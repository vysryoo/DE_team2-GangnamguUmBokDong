"""하루치 silver를 날짜당 parquet 하나로 묶어 archive 계층에 적재한다.

5분 소스는 하루 288개 파일을 쓰므로 하류가 하루치를 읽으려면 수백 개를 열어야 한다.
이 단계는 Silver를 지우지 않는다. 최신 authority가 아닌 Silver는 `silver_gc`가 정리한다.

- 시각 보존: `bike_station_realtime`은 행에 시각 컬럼이 없어 파일 경로가 유일한 시각
  정보다. 모든 소스에 `_window_start`를 주입한다.
- 스키마 고정: Silver는 스키마를 추론해 쓰므로 전량 결측 컬럼이 `null` 타입이 된다.
  소스 yaml의 `columns`로 스키마를 강제해 모든 날짜의 archive 스키마를 같게 한다.
- Authority 선택: window마다 최신 source snapshot manifest가 가리키는 Silver 하나만
  입력이다. 최신 상태가 EMPTY이면 이전 Silver를 다시 읽지 않는다.
- 변경 감지: archive의 LastModified가 내용과 무관하게 갱신되면 하류 변경 감지가
  오염되므로, 선택된 object와 authority 상태로 만든 서명이 바뀐 날짜만 다시 쓴다.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from core import layout
from core.gold_publication.storage import S3ImmutableObjectStore
from core.kst import KST
from core.s3 import S3Object, object_uri
from core.source_snapshot import SourceSnapshotStatus

import manifest as manifest_module
import storage
from config.schema import SourceConfig

logger = logging.getLogger(__name__)

# Airflow가 CATCHUP=False라 놓친 스케줄은 다시 돌지 않으므로 검사 범위가 유일한 복구
# 수단이다. D-6 대여이력 correction 뒤 compaction만 실패해도 다음 날 D-7에서 한 번 더
# 복구하도록 오늘을 포함한 8일을 본다.
RECOVERY_DAYS = 8

# validation 엔진은 yaml의 types를 선언 순서대로 시도하므로 types[0]이 실효 타입이다.
_ARROW_TYPES = {
    "str": pa.string(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "precip": pa.float64(),
    "snow": pa.float64(),
    "masked_float": pa.float64(),
}

SOURCE_KIND_COLLECTOR = "collector"
SOURCE_KIND_BOOTSTRAP = "bootstrap"

# `_window_start`는 compaction에서 수집 시각(5분·10분), bootstrap에서 발생 시각(1시간)을
# 뜻해 해상도가 다르므로 `_source_kind`로 출처를 구분함
_META_FIELDS = [
    ("_row_status", pa.string()),
    ("_window_start", pa.string()),
    ("_source_kind", pa.string()),
]

_SILVER_KEY = re.compile(
    r"\Asilver/[a-z][a-z0-9_]*/dt=(?P<day>\d{4}-\d{2}-\d{2})/hh=\d{2}/"
    r"(?P<hhmm>\d{4})/sha256=(?P<checksum>[0-9a-f]{64})\.parquet\Z"
)


def _parse_silver_key(key: str) -> tuple[datetime, str]:
    """Content-addressed Silver key에서 window 시작 시각과 checksum을 읽는다."""
    matched = _SILVER_KEY.fullmatch(key)
    if matched is None:
        raise ValueError(f"content-addressed Silver key 규칙에 맞지 않는다: {key}")
    window_start = datetime.strptime(
        f"{matched.group('day')} {matched.group('hhmm')}", "%Y-%m-%d %H%M"
    ).replace(tzinfo=KST)
    return window_start, matched.group("checksum")


def archive_schema(config: SourceConfig) -> pa.Schema:
    """소스 설정에서 archive parquet의 고정 스키마를 만든다.

    Args:
        config: 대상 소스 설정
    Returns:
        yaml 컬럼 순서를 따르고 메타 컬럼을 뒤에 붙인 pyarrow 스키마
    """
    fields = [
        (name, _ARROW_TYPES[spec.types[0]]) for name, spec in config.columns.items()
    ]
    return pa.schema(fields + _META_FIELDS)


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """테이블을 목표 스키마의 컬럼·타입·순서에 정확히 맞춘다.

    `Table.cast()`는 빠진 컬럼과 다른 순서를 처리하지 못하므로 필드 단위로 맞춘다.

    Args:
        table: silver에서 읽은 테이블
        schema: `archive_schema()`가 만든 목표 스키마
    Returns:
        스키마와 완전히 일치하는 테이블. 없는 컬럼은 null로 채움
    Raises:
        pa.ArrowInvalid: 값을 선언 타입으로 캐스팅할 수 없을 때. yaml 선언과 실제
            데이터가 어긋났다는 뜻이라 조용히 넓히지 않음
    """
    columns = []
    for field in schema:
        if field.name not in table.column_names:
            columns.append(pa.nulls(table.num_rows, field.type))
            continue
        source = table.column(field.name)
        try:
            columns.append(source.cast(field.type))
        except pa.ArrowInvalid as exc:
            # pyarrow 기본 메시지에는 컬럼명이 없어 어느 yaml 선언을 고칠지 알 수 없음
            raise pa.ArrowInvalid(
                f"컬럼 '{field.name}'을 {source.type} → {field.type}로 캐스팅할 수 없다: {exc}"
            ) from exc
    return pa.Table.from_arrays(columns, schema=schema)


def dedup(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """윈도우 반복 수집으로 생긴 중복만 제거하고 가장 이른 `_window_start`를 남긴다.

    같은 응답의 반복은 데이터 컬럼이 완전히 같지만, 원본 자체의 중복은 값이 미세하게
    다르다. `_window_start`를 뺀 전체 컬럼으로 묶어야 원본이 말한 것을 지우지 않는다.

    Args:
        table: `conform()`을 거친 테이블
        schema: 그 목표 스키마
    Returns:
        중복이 제거되고 스키마·컬럼 순서가 그대로인 테이블
    """
    data_columns = [f.name for f in schema if f.name != "_window_start"]
    grouped = table.group_by(data_columns).aggregate([("_window_start", "min")])
    return conform(
        grouped.rename_columns(
            [
                ("_window_start" if n == "_window_start_min" else n)
                for n in grouped.schema.names
            ]
        ),
        schema,
    )


def target_dates(today: date) -> list[date]:
    """오늘을 포함한 검사 날짜를 오름차순으로 반환한다. 당일치도 부분 압축해 둔다.

    Args:
        today: 기준 KST 날짜
    Returns:
        `RECOVERY_DAYS`개의 연속 날짜
    """
    return [today - timedelta(days=offset) for offset in reversed(range(RECOVERY_DAYS))]


@dataclass(frozen=True)
class DateResult:
    """날짜 하나의 압축 결과다.

    `status`는 `compacted`·`skipped`(무변경)·`empty`(silver 없음)·`failed` 중 하나다.
    """

    day: date
    status: str
    rows: int | None = None
    archive_key: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _SelectedSilver:
    """한 window에서 compaction 입력으로 선택된 Silver object다."""

    object: S3Object
    expected_rows: int


@dataclass(frozen=True, slots=True)
class DateAuthority:
    """날짜 하나에서 확정된 compaction 입력과 변경 감지 근거다."""

    selected: tuple[_SelectedSilver, ...]
    markers: tuple[tuple[str, ...], ...]
    completed_windows: int


def _diagnostic_manifest_sha256(source_id: str, window_start: datetime) -> str:
    """Mutable 진단 manifest의 현재 상태를 변경 감지용 SHA-256으로 만든다."""
    diagnostic = manifest_module.load(source_id, window_start)
    if diagnostic is None:
        return "missing"
    payload = json.dumps(
        diagnostic.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _select_date_authority(
    source_id: str,
    objects: list[S3Object],
    snapshot_windows: list[datetime],
) -> DateAuthority:
    """window마다 최신 source snapshot이 가리키는 Silver 하나만 입력으로 고른다.

    Source snapshot이 없는 window의 Silver는 manifest 게시 전에 멈춘 실행의 결과라
    입력으로 쓰지 않는다.
    """
    objects_by_window: dict[datetime, list[S3Object]] = {}
    for silver_object in objects:
        window_start, _ = _parse_silver_key(silver_object.key)
        objects_by_window.setdefault(window_start, []).append(silver_object)

    selected: list[_SelectedSilver] = []
    markers: list[tuple[str, ...]] = []
    published = set(snapshot_windows)
    for window_start in sorted(set(objects_by_window) | published):
        if window_start not in published:
            markers.append(
                (
                    "unpublished_immutable",
                    window_start.isoformat(),
                    _diagnostic_manifest_sha256(source_id, window_start),
                )
            )
            continue

        latest = manifest_module.load_source_snapshots(source_id, window_start)[-1].manifest
        markers.append(
            (
                "source_snapshot",
                window_start.isoformat(),
                str(latest.revision_no),
                latest.sha256,
            )
        )
        if latest.status is SourceSnapshotStatus.EMPTY:
            continue

        exact_key = layout.silver_content_key(
            source_id, window_start, latest.silver_byte_sha256
        )
        by_key = {obj.key: obj for obj in objects_by_window.get(window_start, [])}
        if exact_key not in by_key:
            raise ValueError(
                f"source snapshot이 가리키는 Silver object가 목록에 없다: {exact_key}"
            )
        selected.append(_SelectedSilver(by_key[exact_key], latest.counts.kept))

    return DateAuthority(
        selected=tuple(selected),
        markers=tuple(markers),
        completed_windows=len(published),
    )


def _authority_signature(authority: DateAuthority) -> str:
    """선택된 object와 최신 authority 상태의 변경 감지 서명을 만든다."""
    payload = {
        "objects": [
            (
                item.object.key,
                item.object.size,
                item.object.last_modified.isoformat(),
                item.expected_rows,
            )
            for item in authority.selected
        ],
        "authority": authority.markers,
    }
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def resolve_date_authority(
    config: SourceConfig, day: date
) -> tuple[DateAuthority, str]:
    """날짜의 최신 Silver authority와 변경 감지 서명을 함께 반환한다.

    Args:
        config: 대상 소스 설정
        day: KST 날짜
    Returns:
        선택된 입력과 그 서명
    Raises:
        ValueError: Silver key 규칙이 어긋나거나 authority가 가리키는 object가 없을 때
    """
    objects = storage.list_silver_objects(config.source_id, day)
    windows = storage.list_source_snapshot_windows(config.source_id, day)
    authority = _select_date_authority(config.source_id, objects, windows)
    return authority, _authority_signature(authority)


def _read_selected(item: _SelectedSilver, schema: pa.Schema) -> pa.Table:
    """선택된 Silver를 checksum으로 검증해 읽고 메타 컬럼을 붙여 스키마에 맞춘다."""
    window_start, checksum = _parse_silver_key(item.object.key)
    payload = S3ImmutableObjectStore().read_bytes(object_uri(item.object.key), checksum)
    table = pq.read_table(pa.BufferReader(payload))
    if table.num_rows != item.expected_rows:
        raise ValueError(
            "source snapshot counts.kept와 Silver parquet row 수가 다르다: "
            f"expected={item.expected_rows} actual={table.num_rows} key={item.object.key}"
        )
    table = table.append_column(
        "_window_start",
        pa.array([window_start.isoformat()] * table.num_rows, type=pa.string()),
    )
    table = table.append_column(
        "_source_kind",
        pa.array([SOURCE_KIND_COLLECTOR] * table.num_rows, type=pa.string()),
    )
    return conform(table, schema)


def _build_archive_table(config: SourceConfig, authority: DateAuthority) -> pa.Table:
    """선택된 Silver를 이어 붙여 하루치 archive 테이블을 만든다."""
    schema = archive_schema(config)
    tables = [_read_selected(item, schema) for item in authority.selected]
    if not tables:
        return schema.empty_table()
    table = pa.concat_tables(tables)
    if config.compaction and config.compaction.dedup:
        table = dedup(table, schema)
    return table


def _expected_windows(config: SourceConfig) -> int:
    """하루에 있어야 할 윈도우 수. 주기가 하루보다 길면 1로 본다."""
    return max(1, int(timedelta(days=1) / config.schedule.interval))


def compact_date(config: SourceConfig, day: date, *, force: bool = False) -> DateResult:
    """해당 날짜의 silver를 묶어 archive와 archive manifest에 쓴다.

    입력을 읽다 실패하면 archive도 manifest도 쓰지 않는다. 서명이 남지 않으므로 다음
    실행이 같은 날짜를 다시 시도한다.

    Args:
        config: 대상 소스 설정
        day: 압축할 KST 날짜
        force: 서명이 같아도 다시 압축한다. archive가 손상됐을 때 사용함
    Returns:
        이 날짜의 처리 결과. 입력 단계의 예외는 `failed`로 돌려줌
    """
    try:
        authority, signature = resolve_date_authority(config, day)
        if not authority.markers:
            return DateResult(day=day, status="empty")

        previous = storage.read_archive_manifest(config.source_id, day)
        previous_key = previous.get("archive_key") if previous else None
        if not authority.selected and authority.completed_windows == 0:
            # 게시 전 Silver만 남은 상태로 기존 archive를 빈 파일로 덮어쓰지 않음
            if previous is None:
                return DateResult(day=day, status="empty")
            if not force:
                logger.warning(
                    "stage=compaction status=skipped "
                    f"source={config.source_id} date={day} "
                    "reason=authority_regressed_to_zero_completed_windows"
                )
                return DateResult(day=day, status="skipped", archive_key=previous_key)
        if not force and previous and previous.get("silver_signature") == signature:
            return DateResult(day=day, status="skipped", archive_key=previous_key)

        table = _build_archive_table(config, authority)
    except Exception as exc:  # noqa: BLE001 — 어느 예외든 이 날짜만 실패로 격리한다
        logger.error(
            f"stage=compaction status=failed source={config.source_id} date={day} reason={exc}"
        )
        return DateResult(day=day, status="failed", error=str(exc))

    archive_key = storage.write_archive(config.source_id, day, table)
    expected = _expected_windows(config)
    storage.write_archive_manifest(
        config.source_id,
        day,
        {
            "source_id": config.source_id,
            "date": f"{day:%Y-%m-%d}",
            "archive_key": archive_key,
            "silver_signature": signature,
            "expected_windows": expected,
            "found_windows": authority.completed_windows,
            "completeness": authority.completed_windows / expected,
            "rows": table.num_rows,
            "selected_silver_keys": [item.object.key for item in authority.selected],
            "compacted_at": datetime.now(tz=KST).isoformat(),
        },
    )
    logger.info(
        f"stage=compaction status=compacted source={config.source_id} date={day} "
        f"parts={authority.completed_windows}/{expected} rows={table.num_rows} key={archive_key}"
    )
    return DateResult(
        day=day, status="compacted", rows=table.num_rows, archive_key=archive_key
    )
