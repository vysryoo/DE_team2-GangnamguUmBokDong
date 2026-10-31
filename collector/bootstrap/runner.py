"""날짜 하나를 검증해 archive에 적재한다.

`_window_start`의 원천인 `stationDt`는 소스 컬럼이 아니라 `validate_batch`가 떨어뜨린다.
그래서 검증 전에 시간대로 그룹을 나눠 그룹마다 시각을 상수로 두고, 검증 뒤 컬럼으로 붙인다.

quarantine은 쓰지 않는다. 대신 `validate_batch`의 `column_issues`를 manifest에 남겨
어느 컬럼에서 몇 건이 왜 빠졌는지 알 수 있게 한다.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import pyarrow as pa
from core.kst import KST

import storage
from bootstrap.config import BootstrapConfig
from compaction import SOURCE_KIND_BOOTSTRAP, archive_schema, conform
from config.schema import SourceConfig
from validation.engine import validate_batch
from validation.types import RunContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DateResult:
    """날짜 하나의 적재 결과.

    `status`는 `loaded`·`skipped`(이미 archive가 있어 재개로 건너뜀)·`empty`(처리할
    행이 없었거나 검증에서 전량 폐기됨)·`failed` 중 하나다. 재개 중인지와 원래 쓸 게
    없었는지를 요약만 보고 구분하려고 `skipped`와 `empty`를 나눈다.
    """

    day: date
    status: str
    rows: int | None = None
    dropped: int | None = None
    out_of_range: int = 0
    archive_key: str | None = None
    silver_present: bool = False
    error: str | None = None


@dataclass
class _DayCounts:
    """검증 중 누적한 폐기 건수. 중간에 실패해도 그때까지의 값을 결과에 남긴다."""

    dropped: int = 0
    out_of_range: int = 0
    column_issues: dict[str, dict[str, int]] = field(default_factory=dict)


def group_by_window(rows: list[dict], cfg: BootstrapConfig) -> dict[str, list[dict]]:
    """행을 그것이 속한 시간대로 나눈다. 키는 KST ISO8601 문자열이다."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        moment = datetime.strptime(row[cfg.window.from_column], cfg.window.format)
        window = moment.replace(minute=0, second=0, microsecond=0, tzinfo=KST)
        groups[window.isoformat()].append(row)
    return dict(groups)


def _dedup_full_row(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """`_window_start`까지 포함해 완전히 같은 행만 하나로 합친다.

    `compaction.dedup()`은 `_window_start`를 빼고 묶으므로, 재고에 쓰면 다른 시각의 값이
    우연히 같을 때 한 행으로 합쳐져 시계열이 깨진다.
    """
    all_columns = schema.names
    grouped = table.group_by(all_columns).aggregate([(all_columns[0], "count")])
    return conform(grouped, schema)


def _validate_windows(
    scfg: SourceConfig,
    bcfg: BootstrapConfig,
    day: date,
    rows: list[dict],
    schema: pa.Schema,
    counts: _DayCounts,
) -> list[pa.Table]:
    """시간대별로 행을 검증해 archive 스키마 테이블 목록을 만들고 폐기 건수를 누적한다."""
    tables: list[pa.Table] = []
    for window, group in sorted(group_by_window(rows, bcfg).items()):
        started = datetime.fromisoformat(window)
        if started.date() != day:
            # API 경로는 경계 시각에 다른 날짜의 관측을 섞어 줄 수 있음. 그대로 두면
            # compaction이 다른 날짜 데이터를 같은 날짜로 섞음
            counts.out_of_range += len(group)
            logger.warning(
                f"stage=bootstrap source={scfg.source_id} date={day} "
                f"out_of_range_date={started.date()} rows={len(group)} "
                "대상 날짜가 아니라 버림"
            )
            continue
        ctx = RunContext(
            source_id=scfg.source_id,
            window_start=started,
            window_end=started + timedelta(hours=1),
            attempt=1,
        )
        outcome = validate_batch(group, scfg, ctx)
        counts.dropped += outcome.counts.get("dropped", 0)
        _merge_column_issues(counts.column_issues, outcome.column_issues)
        if not outcome.silver_rows:
            continue
        table = pa.Table.from_pylist(outcome.silver_rows)
        table = table.append_column("_window_start", pa.array([window] * table.num_rows, type=pa.string()))
        table = table.append_column(
            "_source_kind", pa.array([SOURCE_KIND_BOOTSTRAP] * table.num_rows, type=pa.string())
        )
        tables.append(conform(table, schema))
    return tables


def _merge_column_issues(
    total: dict[str, dict[str, int]], window_issues: dict[str, dict[str, int]]
) -> None:
    """시간대 하나의 컬럼별 이상치 건수를 날짜 누적치에 더한다."""
    for column, issue_counts in window_issues.items():
        merged = total.setdefault(column, {"missing": 0, "outlier": 0, "type_error": 0})
        for kind, value in issue_counts.items():
            merged[kind] = merged.get(kind, 0) + value


def _write_archive(
    scfg: SourceConfig,
    day: date,
    table: pa.Table,
    counts: _DayCounts,
    *,
    silver_present: bool,
    station_map_stats: dict | None,
    materialized_empty: bool,
) -> DateResult:
    """Archive와 manifest를 쓰고 `loaded` 결과를 반환한다."""
    archive_key = storage.write_archive(scfg.source_id, day, table)
    storage.write_archive_manifest(scfg.source_id, day, {
        "source_id": scfg.source_id,
        "date": f"{day:%Y-%m-%d}",
        "archive_key": archive_key,
        "source_kind": SOURCE_KIND_BOOTSTRAP,
        "rows": table.num_rows,
        "dropped": counts.dropped,
        "out_of_range": counts.out_of_range,
        "column_issues": counts.column_issues,
        "silver_present": silver_present,
        **({"materialized_empty": True} if materialized_empty else {}),
        "loaded_at": datetime.now(tz=KST).isoformat(),
        # 조인하지 않은 소스에는 키 자체를 두지 않는다.
        **({"station_map": station_map_stats} if station_map_stats else {}),
    })
    empty_note = " materialized_empty=true" if materialized_empty else ""
    logger.info(
        f"stage=bootstrap status=loaded source={scfg.source_id} date={day} "
        f"rows={table.num_rows} dropped={counts.dropped} out_of_range={counts.out_of_range}"
        f"{empty_note} key={archive_key}"
    )
    return DateResult(
        day=day, status="loaded", rows=table.num_rows, dropped=counts.dropped,
        out_of_range=counts.out_of_range, archive_key=archive_key, silver_present=silver_present,
    )


def _has_silver(source_id: str, day: date) -> bool:
    """같은 날짜에 운영 수집 Silver가 있는지 확인하고 있으면 경고를 남긴다."""
    if not storage.list_silver_objects(source_id, day):
        return False
    # 대량 적재에서는 로그 한 줄이 묻히므로 결과에도 남겨 실행 요약에 집계함
    logger.warning(
        f"stage=bootstrap source={source_id} date={day} "
        "silver_present=true 다음 compaction이 이 archive를 덮어쓴다"
    )
    return True


def load_date(
    scfg: SourceConfig,
    bcfg: BootstrapConfig,
    day: date,
    rows: list[dict],
    *,
    force: bool = False,
    station_map_stats: dict | None = None,
    materialize_empty: bool = False,
) -> DateResult:
    """그 날짜의 행을 검증해 archive에 쓴다.

    args:
        scfg: collector 소스 설정. 컬럼 스펙과 정책을 여기서 가져온다.
        bcfg: bootstrap 매핑 설정
        day: 대상 날짜
        rows: 물리 컬럼명으로 정규화된 원시 행
        force: archive가 이미 있어도 다시 쓴다
        station_map_stats: 조인 매핑표의 출처 정보. 주면 manifest에 그대로 실린다.
            매핑표 값은 실행 시점 API 스냅샷이라 나중에 출처를 되짚을 수 있어야 한다.
        materialize_empty: 참이면 확인된 0행도 빈 Archive와 manifest로 기록한다.
    returns:
        이 날짜의 처리 결과. 예외를 던지지 않는다.
    """
    if not rows and not materialize_empty:
        return DateResult(day=day, status="empty")
    if not force and storage.archive_exists(scfg.source_id, day):
        return DateResult(day=day, status="skipped")

    silver_present = _has_silver(scfg.source_id, day)

    schema = archive_schema(scfg)
    counts = _DayCounts()
    try:
        tables = _validate_windows(scfg, bcfg, day, rows, schema, counts)
    except Exception as exc:  # noqa: BLE001 — 어느 예외든 이 날짜만 실패로 격리한다
        logger.error(f"stage=bootstrap status=failed source={scfg.source_id} date={day} reason={exc}")
        return DateResult(
            day=day, status="failed", error=str(exc),
            out_of_range=counts.out_of_range, silver_present=silver_present,
        )

    if tables:
        table = pa.concat_tables(tables)
        if bcfg.dedup:
            table = _dedup_full_row(table, schema)
    elif materialize_empty:
        table = schema.empty_table()
    else:
        return DateResult(
            day=day, status="empty", rows=0, dropped=counts.dropped,
            out_of_range=counts.out_of_range, silver_present=silver_present,
        )
    return _write_archive(
        scfg,
        day,
        table,
        counts,
        silver_present=silver_present,
        station_map_stats=station_map_stats,
        materialized_empty=not tables,
    )
