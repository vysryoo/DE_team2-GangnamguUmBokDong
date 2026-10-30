"""하루치 silver를 날짜당 parquet 하나로 묶어 archive 계층에 적재한다.

## 왜 필요한가

수집은 윈도우마다 파일 하나를 쓴다. 5분 소스는 하루 288개다. 하루치를 읽으려면 수백
개를 열어야 하고 작은 파일이 계속 쌓인다. 이 모듈은 그것을 날짜당 하나로 묶는다.
이 단계 자체는 Silver를 지우지 않는다. 후속 GC가 검증된 Archive와 Cold Bronze가
있는 날짜에서 최신 authority가 아닌 Silver만 별도로 정리한다.

## 설계상 중요한 네 가지

**시각 보존.** `bike_station_realtime`은 행에 시각 컬럼이 없어서 파일 경로가 유일한
시각 정보다. 그대로 이어붙이면 288개 스냅샷이 구분 불가능해진다. 모든 소스에 예외
없이 `_window_start`를 주입한다.

**스키마 고정.** silver는 `pa.Table.from_pylist()`로 스키마를 추론해 쓰므로, 어떤
윈도우에서 특정 컬럼이 전량 결측이면 그 파일만 `null` 타입이 된다. 소스 yaml의
`columns`에서 스키마를 만들어 강제하면 **모든 날짜의 archive 스키마가 같아진다** —
`concat_tables(promote_options=...)` 계열은 이걸 줄 수 없다. 캐스팅이 실패하면 yaml과
현실이 어긋났다는 뜻이므로 조용히 넓히지 않고 그대로 터뜨린다.

**Authority 선택.** 전환된 윈도우에는 correction마다 immutable Silver가 남지만, 최신
source snapshot manifest가 가리키는 SUCCEEDED object 하나만 유효하다. 최신 상태가
EMPTY이면 이전 object를 다시 읽지 않는다. Source manifest가 없는 historical legacy
window만 기존 key를 호환 입력으로 사용한다.

**변경 감지.** 매일 검사 범위 전체를 다시 압축하면 대부분이 무의미한 재작업이고,
archive의 LastModified가 내용과 무관하게 갱신되어 하류의 변경 감지를 오염시킨다.
선택된 object의 size·last_modified와 최신 authority manifest SHA로 서명을 만들어
바뀐 날짜만 다시 쓴다.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import ceil

import manifest as manifest_module
import pyarrow as pa
import storage
from config.schema import SourceConfig
from core import layout
from core.kst import KST
from core.s3 import S3Object, object_uri, read_parquet
from core.source_snapshot import SourceSnapshotManifest, SourceSnapshotStatus
from manifest import RunStatus, Stage

logger = logging.getLogger(__name__)


# 배치가 안 돈 날을 되채우는 하한. airflow가 CATCHUP=False로 돌아서 놓친 스케줄은
# 다시 실행되지 않으므로, 검사 범위가 그걸 복구하는 유일한 수단이다. D-6 대여이력
# correction 뒤 compaction만 실패해도 다음 날 D-7에서 한 번 더 복구할 수 있도록
# 오늘을 포함한 8일을 본다. 지연 fetch backfill 설정과는 목적이 다르다.
RECOVERY_DAYS = 8

# yaml의 types는 선언 순서대로 시도해 첫 성공을 채택하므로(validation/engine.py:42)
# types[0]이 곧 실효 타입이다. `[str, int]`는 str()이 거의 실패하지 않아 항상 str이 된다.
_ARROW_TYPES = {
    "str": pa.string(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "precip": pa.float64(),  # 강수량 범주 문자열을 mm 실수로 바꾼 결과
    "snow": pa.float64(),  # 적설 범주 문자열을 cm 실수로 바꾼 결과
    "masked_float": pa.float64(),  # 마스킹(`*`)은 결측이 되고 나머지는 실수
}

# 출처 구분: compaction은 실시간 수집 데이터, bootstrap은 초기 로드 과거 데이터.
SOURCE_KIND_COLLECTOR = "collector"
SOURCE_KIND_BOOTSTRAP = "bootstrap"

# 검증 엔진이 붙이는 `_row_status`, 이 모듈이 붙이는 `_window_start`, 그리고 출처를
# 나타내는 `_source_kind`. 셋 다 언더스코어 접두 메타 컬럼이라는 관례를 따른다.
#
# `_source_kind`가 필요한 이유는 `_window_start`의 의미가 출처마다 다르기 때문이다.
# compaction은 "언제 수집했는지"(5분·10분 해상도), bootstrap은 "언제 일어났는지"
# (시간 해상도)를 넣는다. 특히 bike_station_realtime은 행에 다른 시각 컬럼이 없어
# 이 값이 유일한 시각인데, 과거는 시간 단위·현재는 5분 단위가 된다.
_META_FIELDS = [
    ("_row_status", pa.string()),
    ("_window_start", pa.string()),
    ("_source_kind", pa.string()),
]

_SILVER_KEY = re.compile(
    r"\Asilver/(?P<source_id>[a-z][a-z0-9_]*)/"
    r"dt=(?P<day>\d{4}-\d{2}-\d{2})/hh=(?P<partition_hour>\d{2})/"
    r"(?P<hhmm>\d{4})(?:/sha256=(?P<checksum>[0-9a-f]{64}))?\.parquet\Z"
)


@dataclass(frozen=True, slots=True)
class _ParsedSilverKey:
    """Legacy와 immutable Silver key에서 읽은 exact window identity다."""

    source_id: str
    window_start: datetime
    checksum: str | None


def _parse_silver_key(key: str) -> _ParsedSilverKey:
    """Silver key를 파싱하고 partition과 파일 시각이 일치하는지 검증한다."""
    matched = _SILVER_KEY.fullmatch(key)
    if matched is None:
        raise ValueError(f"silver 키 규칙에 맞지 않아 윈도우 시각을 알 수 없다: {key}")

    day = matched.group("day")
    hhmm = matched.group("hhmm")
    if matched.group("partition_hour") != hhmm[:2]:
        raise ValueError(f"silver 키의 hh partition과 파일 시각이 다르다: {key}")
    try:
        window_start = datetime.strptime(f"{day} {hhmm}", "%Y-%m-%d %H%M").replace(
            tzinfo=KST
        )
    except ValueError as exc:
        raise ValueError(f"silver 키의 윈도우 시각이 유효하지 않다: {key}") from exc
    return _ParsedSilverKey(
        source_id=matched.group("source_id"),
        window_start=window_start,
        checksum=matched.group("checksum"),
    )


def window_start_from_key(key: str) -> str:
    """silver 키에서 그 파일이 담당한 윈도우 시작 시각을 뽑아 ISO8601로 반환한다.

    `hh=` 파티션은 시 단위라 분을 잃는다. 파일명의 `HHMM`이 진짜 시각이다.

    args:
        key: legacy `.../1410.parquet` 또는 immutable
            `.../1410/sha256={64hex}.parquet` 형식의 전체 키
    returns:
        `"2026-08-12T14:10:00+09:00"` 형식의 KST ISO8601 문자열
    raises:
        ValueError: 규칙에 맞지 않는 키일 때. 시각을 모르는 행을 archive에 넣는 것보다
            멈추는 편이 낫다.
    """
    return _parse_silver_key(key).window_start.isoformat()


def archive_schema(config: SourceConfig) -> pa.Schema:
    """소스 설정에서 archive parquet의 고정 스키마를 만든다.

    컬럼 집합과 순서가 yaml을 그대로 따르므로, 같은 소스의 archive는 날짜가 달라도
    스키마가 동일하다. 하류가 여러 날짜를 한 번에 읽을 때 스키마 충돌을 만나지 않는다.

    args:
        config: 대상 소스 설정
    returns:
        yaml 컬럼 + 메타 컬럼 2개로 구성된 pyarrow 스키마
    """
    fields = [
        (name, _ARROW_TYPES[spec.types[0]]) for name, spec in config.columns.items()
    ]
    return pa.schema(fields + _META_FIELDS)


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """테이블을 목표 스키마에 정확히 맞춘다.

    `Table.cast()`를 쓰지 않는 이유는 두 가지다. 어떤 윈도우의 silver에는 컬럼이 통째로
    빠져 있을 수 있고(그 윈도우에서 해당 키가 한 번도 등장하지 않은 경우), 컬럼 순서도
    보장되지 않는다. 필드 단위로 맞춰야 둘 다 처리된다.

    args:
        table: silver에서 읽은 테이블
        schema: `archive_schema()`가 만든 목표 스키마
    returns:
        스키마와 컬럼·타입·순서가 완전히 일치하는 테이블
    raises:
        pa.ArrowInvalid: 값을 선언 타입으로 캐스팅할 수 없을 때. yaml 선언과 실제
            데이터가 어긋났다는 뜻이라 조용히 넘기지 않는다.
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
            # pyarrow 기본 메시지에는 컬럼명이 없다. 어느 컬럼의 yaml 선언을 고쳐야
            # 하는지 모르면 이 실패를 진단할 수 없으므로 이름과 양쪽 타입을 붙인다.
            raise pa.ArrowInvalid(
                f"컬럼 '{field.name}'을 {source.type} → {field.type}로 캐스팅할 수 없다: {exc}"
            ) from exc
    return pa.Table.from_arrays(columns, schema=schema)


def dedup(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """윈도우 중복만 제거한다. `_window_start`는 가장 이른 값을 남긴다.

    path_suffix가 날짜 단위인데 주기가 그보다 짧은 소스는, 윈도우마다 같은 구간을
    통째로 다시 받는다. 그렇게 생긴 중복은 **데이터 컬럼이 완전히 동일**하다(같은 응답의
    반복). 반면 원본 자체의 중복은 값이 다르다 — 같은 대여인데 이용시간·이용거리만
    미세하게 다른 사례가 실측으로 확인됐다.

    그래서 `_window_start`를 뺀 전체 컬럼으로 묶는다. 이러면 compaction이 만들어낸
    중복만 사라지고 원본이 말한 것은 전부 남는다. `(BIKE_ID, RENT_DT)` 같은 키로
    묶으면 "그 둘은 같은 대여다"라는 원본 데이터에 대한 판단을 재배치 계층이 내리게
    되는데, 그건 검증·정제의 몫이다.

    남길 `_window_start`로 최솟값을 고르는 이유는 그 기록이 처음 보인 시점이 의미 있는
    값이고, 재압축해도 값이 흔들리지 않기 때문이다.

    args:
        table: `conform()`을 거쳐 스키마가 맞춰진 테이블
        schema: 그 목표 스키마
    returns:
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


def silver_signature(objects: list[S3Object]) -> str:
    """해당 날짜 silver의 현재 상태를 나타내는 서명을 만든다.

    키 목록만으로는 부족하다 — 백필은 **같은 키를 다시 쓰므로** 목록이 그대로다.
    LIST 응답에 이미 들어 있는 size·last_modified를 함께 넣어야 내용 갱신도 잡힌다.

    args:
        objects: `storage.list_silver_objects()`가 반환한 목록
    returns:
        sha256 hex 문자열. 목록 순서에는 영향받지 않는다.
    """
    payload = sorted((o.key, o.size, o.last_modified.isoformat()) for o in objects)
    return hashlib.sha256(json.dumps(payload).encode("utf-8")).hexdigest()


def lookback_days(config: SourceConfig) -> int:
    """이 소스를 며칠 전까지 검사할지 정한다.

    검사 범위가 하는 일은 둘이고 목적이 다르므로 값을 따로 구해 큰 쪽을 쓴다.

    - **백필 창**: 백필은 완결된 윈도우의 silver를 사후에 갱신한다. 마커 만료가
      `first_failed_at + max_age`이고 재시도해도 `first_failed_at`은 유지되므로 상한이
      확정된다. 날짜 경계와 어긋나는 만큼 하루를 더한다.
    - **복구 하한**: 배치 자체가 안 돈 날을 되채운다. 백필 설정과 무관하게 필요하다.

    둘을 한 값에 묶으면 백필이 없는 소스가 복구력까지 잃는다. `bike_station_realtime`이
    그런 경우인데, 이 소스에 백필이 없는 것은 누락이 아니라 API에 시각 파라미터가 없어
    조각 단위 백필이 데이터 오염이 되기 때문이다.

    args:
        config: 대상 소스 설정
    returns:
        오늘로부터 거슬러 올라갈 일수
    """
    return max(_backfill_window_days(config), RECOVERY_DAYS)


def _backfill_window_days(config: SourceConfig) -> int:
    """백필이 이 소스의 silver를 갱신할 수 있는 최대 일수. 백필이 없으면 1."""
    if config.backfill and config.backfill.enabled and config.backfill.max_age:
        return ceil(config.backfill.max_age / timedelta(days=1)) + 1
    return 1


def target_dates(config: SourceConfig, today: date) -> list[date]:
    """검사할 날짜를 오름차순으로 나열한다. 오늘을 포함한다 — 당일치도 부분 압축해 둔다."""
    span = lookback_days(config)
    return [today - timedelta(days=offset) for offset in reversed(range(span))]


@dataclass(frozen=True)
class DateResult:
    """날짜 하나의 압축 결과.

    `status`는 `compacted`·`skipped`(무변경)·`empty`(silver 없음)·`failed` 중 하나다.
    실패해도 예외를 밖으로 던지지 않고 이 값으로 돌려주므로, 한 날짜의 문제가 나머지
    날짜를 막지 않는다.
    """

    day: date
    status: str
    rows: int | None = None
    archive_key: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _SelectedSilver:
    """한 window에서 compaction 입력 권한을 얻은 exact Silver object다."""

    object: S3Object
    expected_rows: int | None


@dataclass(frozen=True, slots=True)
class _DateAuthority:
    """날짜 하나에서 확정된 compaction 입력과 변경 감지 근거다."""

    selected: tuple[_SelectedSilver, ...]
    markers: tuple[tuple[str, ...], ...]
    completed_windows: int


def _mutable_manifest_sha256(value: manifest_module.Manifest | None) -> str:
    """Mutable 진단 manifest의 현재 상태를 변경 감지용 SHA-256으로 만든다."""
    if value is None:
        return "missing"
    payload = json.dumps(
        value.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _snapshot_silver_key(
    snapshot: SourceSnapshotManifest,
    *,
    source_id: str,
    window_start: datetime,
) -> str:
    """SUCCEEDED source snapshot이 가리켜야 할 canonical Silver key를 검증한다."""
    if snapshot.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ValueError("SUCCEEDED source snapshot만 Silver key를 가질 수 있다.")
    if snapshot.source_id != source_id or snapshot.logical_dttm != window_start:
        raise ValueError("source snapshot과 Silver window identity가 다르다.")
    if snapshot.silver_uri is None or snapshot.silver_byte_sha256 is None:
        raise ValueError("SUCCEEDED source snapshot에 Silver identity가 없다.")

    kst_window = snapshot.logical_dttm.astimezone(KST)
    if kst_window.second or kst_window.microsecond:
        raise ValueError("Silver minute key로 표현할 수 없는 source logical_dttm이다.")
    key = layout.silver_content_key(
        source_id, kst_window, snapshot.silver_byte_sha256
    )
    if snapshot.silver_uri != object_uri(key):
        raise ValueError("source snapshot의 Silver URI가 canonical key와 다르다.")
    return key


def _group_silver_objects(
    source_id: str, objects: list[S3Object]
) -> dict[datetime, tuple[S3Object, ...]]:
    """Silver objects를 window별로 묶고 source/key 중복을 검증한다."""
    grouped: dict[datetime, list[S3Object]] = {}
    seen_keys: set[str] = set()
    for silver_object in objects:
        parsed = _parse_silver_key(silver_object.key)
        if parsed.source_id != source_id:
            raise ValueError(
                f"다른 source의 Silver가 compaction 목록에 섞였다: {silver_object.key}"
            )
        if silver_object.key in seen_keys:
            raise ValueError(f"같은 Silver object가 중복 나열됐다: {silver_object.key}")
        seen_keys.add(silver_object.key)
        grouped.setdefault(parsed.window_start, []).append(silver_object)
    return {window: tuple(values) for window, values in grouped.items()}


def _select_date_authority(
    config: SourceConfig,
    objects: list[S3Object],
    source_snapshot_windows: list[datetime],
) -> _DateAuthority:
    """최신 immutable authority 또는 historical legacy 하나만 window 입력으로 고른다.

    Source snapshot revision chain이 하나라도 있으면 mutable manifest와 legacy key는
    더 이상 authority가 아니다. 최신 revision이 SUCCEEDED일 때 그 manifest가 가리키는
    exact content-addressed object만 고르고, EMPTY이면 이전 Silver를 모두 제외한다.
    Immutable object만 있고 source manifest가 없는 manifest-last 중간 상태도 열지 않는다.
    """
    selected: list[_SelectedSilver] = []
    markers: list[tuple[str, ...]] = []
    completed_windows = 0
    grouped_objects = _group_silver_objects(config.source_id, objects)
    authority_windows = set(source_snapshot_windows)

    for window_start in sorted(set(grouped_objects) | authority_windows):
        window_objects = grouped_objects.get(window_start, ())
        by_key = {silver_object.key: silver_object for silver_object in window_objects}
        if window_start in authority_windows:
            snapshots = manifest_module.load_source_snapshots(
                config.source_id, window_start
            )
            if not snapshots:
                raise ValueError(
                    "나열된 source snapshot window의 revision chain이 비어 있다: "
                    f"{window_start.isoformat()}"
                )
            latest = snapshots[-1].manifest
            markers.append(
                (
                    "source_snapshot",
                    window_start.isoformat(),
                    str(latest.revision_no),
                    latest.sha256,
                )
            )
            completed_windows += 1
            if latest.status is SourceSnapshotStatus.EMPTY:
                continue

            exact_key = _snapshot_silver_key(
                latest,
                source_id=config.source_id,
                window_start=window_start,
            )
            silver_object = by_key.get(exact_key)
            if silver_object is None:
                raise ValueError(
                    "source snapshot이 가리키는 exact Silver object가 목록에 없다: "
                    f"{exact_key}"
                )
            selected.append(
                _SelectedSilver(
                    object=silver_object,
                    expected_rows=latest.counts.kept,
                )
            )
            continue

        diagnostic = manifest_module.load(config.source_id, window_start)
        diagnostic_sha = _mutable_manifest_sha256(diagnostic)
        if diagnostic is not None and (
            diagnostic.source_id != config.source_id
            or diagnostic.window_start != window_start
        ):
            raise ValueError("mutable manifest와 Silver window identity가 다르다.")

        immutable_objects = [
            silver_object
            for silver_object in window_objects
            if _parse_silver_key(silver_object.key).checksum is not None
        ]
        if immutable_objects:
            markers.append(
                (
                    "unpublished_immutable",
                    window_start.isoformat(),
                    diagnostic_sha,
                )
            )
            continue

        legacy_objects = list(window_objects)
        if len(legacy_objects) != 1:
            raise ValueError(
                f"window에 legacy Silver key가 하나보다 많다: {window_start}"
            )
        legacy_object = legacy_objects[0]

        if diagnostic is None:
            markers.append(("legacy_without_manifest", window_start.isoformat()))
        elif (
            diagnostic.status is RunStatus.SUCCEEDED
            and diagnostic.stage is Stage.COMPLETED
            and diagnostic.artifacts.silver == legacy_object.key
        ):
            markers.append(
                (
                    "legacy_succeeded",
                    window_start.isoformat(),
                    diagnostic_sha,
                )
            )
        else:
            markers.append(
                (
                    "legacy_not_authoritative",
                    window_start.isoformat(),
                    diagnostic_sha,
                )
            )
            continue

        selected.append(
            _SelectedSilver(
                object=legacy_object,
                expected_rows=None,
            )
        )
        completed_windows += 1

    return _DateAuthority(
        selected=tuple(selected),
        markers=tuple(markers),
        completed_windows=completed_windows,
    )


def _authority_signature(authority: _DateAuthority) -> str:
    """선택된 exact object와 최신 authority 상태의 변경 감지 서명을 만든다."""
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
) -> tuple[_DateAuthority, str]:
    """날짜의 최신 Silver authority와 변경 감지 signature를 함께 반환한다."""
    objects = storage.list_silver_objects(config.source_id, day)
    windows = storage.list_source_snapshot_windows(config.source_id, day)
    authority = _select_date_authority(config, objects, windows)
    return authority, _authority_signature(authority)


def _backfill_window_closed(config: SourceConfig, day: date, today: date) -> bool:
    """이 날짜의 silver가 더 채워질 가능성이 없는지 판정한다.

    닫혔는데도 `completeness`가 낮다면 그것은 확정된 데이터 구멍이다. 하류가 "아직
    채워지는 중"과 구분하려면 이 값이 필요하다.

    백필이 없는 소스는 애초에 사후 갱신 수단이 없으므로 항상 닫힌 것으로 본다.
    `bike_station_realtime`이 그런 경우다.
    """
    if not (config.backfill and config.backfill.enabled and config.backfill.max_age):
        return True
    return (today - day).days > _backfill_window_days(config)


def _expected_windows(config: SourceConfig) -> int:
    """하루에 있어야 할 윈도우 수. 주기가 하루보다 길면 1로 본다."""
    return max(1, int(timedelta(days=1) / config.schedule.interval))


def compact_date(
    config: SourceConfig, day: date, *, today: date, force: bool = False
) -> DateResult:
    """해당 날짜의 silver를 묶어 archive에 쓴다.

    변경이 없으면 parquet 본문을 읽지 않고 object metadata와 authority manifest만
    확인한다. 실패하면 archive도 manifest도 쓰지 않는다 — 서명이 기록되지 않으므로
    다음 실행이 자동으로 재시도하고, 부분 결과가 남지 않는다.

    args:
        config: 대상 소스 설정
        day: 압축할 날짜
        today: 백필 창이 닫혔는지 판정할 기준일
        force: 서명이 같아도 다시 압축한다. archive가 손상됐을 때의 탈출구다.
    returns:
        이 날짜의 처리 결과. 예외를 던지지 않는다.
    """
    try:
        objects = storage.list_silver_objects(config.source_id, day)
        source_snapshot_windows = storage.list_source_snapshot_windows(
            config.source_id, day
        )
        if not objects and not source_snapshot_windows:
            return DateResult(day=day, status="empty")

        authority = _select_date_authority(config, objects, source_snapshot_windows)
        signature = _authority_signature(authority)
        previous = storage.read_archive_manifest(config.source_id, day)
        if not authority.selected and authority.completed_windows == 0:
            if previous is None:
                return DateResult(day=day, status="empty")
            if not force:
                logger.warning(
                    "stage=compaction status=skipped "
                    f"source={config.source_id} date={day} "
                    "reason=authority_regressed_to_zero_completed_windows"
                )
                return DateResult(
                    day=day,
                    status="skipped",
                    archive_key=previous.get("archive_key"),
                )
        if not force and previous and previous.get("silver_signature") == signature:
            return DateResult(
                day=day,
                status="skipped",
                archive_key=previous.get("archive_key"),
            )

        schema = archive_schema(config)
        tables = [_read_selected(item, schema) for item in authority.selected]
        table = pa.concat_tables(tables) if tables else _empty_table(schema)
        if config.compaction and config.compaction.dedup:
            table = dedup(table, schema)
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
            "backfill_window_closed": _backfill_window_closed(config, day, today),
            "rows": table.num_rows,
            "selected_silver_keys": [
                item.object.key for item in authority.selected
            ],
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


def _empty_table(schema: pa.Schema) -> pa.Table:
    """Archive schema를 유지하는 0행 테이블을 만든다."""
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema], schema=schema
    )


def _read_selected(item: _SelectedSilver, schema: pa.Schema) -> pa.Table:
    """선택된 immutable bytes와 row count를 검증한 뒤 archive schema로 읽는다."""
    if item.expected_rows is not None:
        storage.read_immutable_silver_artifact(
            item.object.key, row_count=item.expected_rows
        )
    table = _read_conformed(item.object.key, schema)
    if item.expected_rows is not None and table.num_rows != item.expected_rows:
        raise ValueError(
            "source snapshot counts.kept와 Silver parquet row 수가 다르다: "
            f"expected={item.expected_rows} actual={table.num_rows} key={item.object.key}"
        )
    return table


def _read_conformed(key: str, schema: pa.Schema) -> pa.Table:
    """silver 하나를 읽어 메타 컬럼을 붙이고 목표 스키마에 맞춘다."""
    table = read_parquet(key, as_pandas=False)
    if table is None:
        raise ValueError(f"silver를 읽지 못했다: {key}")
    started = window_start_from_key(key)
    table = table.append_column(
        "_window_start", pa.array([started] * table.num_rows, type=pa.string())
    )
    table = table.append_column(
        "_source_kind",
        pa.array([SOURCE_KIND_COLLECTOR] * table.num_rows, type=pa.string()),
    )
    return conform(table, schema)


def compact_range(
    config: SourceConfig, days: list[date], *, today: date, force: bool = False
) -> list[DateResult]:
    """여러 날짜를 각각 독립적으로 압축한다. 한 날짜의 실패가 나머지를 막지 않는다."""
    return [compact_date(config, day, today=today, force=force) for day in days]
