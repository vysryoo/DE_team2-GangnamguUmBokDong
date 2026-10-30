"""날짜 하나를 검증해 archive에 적재한다.

## 왜 시간대로 먼저 그룹핑하는가

`_window_start`의 원천이 `RENT_DT`(대여이력)와 `stationDt`(재고)인데, `stationDt`는
`bike_station_realtime.yaml`의 컬럼이 아니다. `_process_columns`가 `config.columns`만
순회하므로(`validation/engine.py`) `validate_batch`가 이 값을 떨어뜨린다.

검증 전에 시간대로 그룹을 나눠두면 그룹마다 시각이 상수가 되어, 검증이 행을 폐기해도
정렬이 깨지지 않는다. 그룹 처리 후 상수를 컬럼으로 붙이면 된다.

## quarantine을 쓰지 않는다

대신 `validate_batch`가 주는 `column_issues`·`policy_actions`를 manifest에 남긴다.
행 단위 원본은 안 남지만 "어느 컬럼에서 몇 건이 왜 빠졌는지"는 알 수 있다.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
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
    행이 없었거나 검증에서 전량 폐기됨)·`failed` 중 하나다. `skipped`와 `empty`를
    나누는 이유는 "재개 중"과 "이 날짜엔 원래 쓸 게 없었다"를 요약만 보고 구분할 수
    있어야 하기 때문이다(`compaction.py`의 `empty`와 같은 어휘를 쓴다).
    """

    day: date
    status: str
    rows: int | None = None
    dropped: int | None = None
    out_of_range: int = 0
    archive_key: str | None = None
    silver_present: bool = False
    error: str | None = None


def group_by_window(rows: list[dict], cfg: BootstrapConfig) -> dict[str, list[dict]]:
    """행을 그것이 속한 시간대로 나눈다. 키는 KST ISO8601 문자열이다."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        moment = datetime.strptime(row[cfg.window.from_column], cfg.window.format)
        window = moment.replace(minute=0, second=0, microsecond=0, tzinfo=KST)
        groups[window.isoformat()].append(row)
    return dict(groups)


def _dedup_full_row(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """완전히 동일한 행만 하나로 합친다. `_window_start`를 포함한 전체 컬럼으로 묶는다.

    ⚠️ `compaction.dedup()`을 여기 쓰면 안 된다. 그건 `_window_start`를 **뺀** 전체
    컬럼으로 묶는데, compaction에서는 옳다(같은 창 안의 반복 수집을 지우는 것이므로).
    bootstrap 재고에 그대로 쓰면 서로 다른 시각의 값이 우연히 같을 때 두 시각이 한
    행으로 합쳐지고 `_window_start`가 이른 쪽으로 남아 시계열이 파괴된다.

    `_window_start`까지 포함해 묶으면 같은 시각 안의 동일 행만 사라지고, 시각이
    다르면 값이 같아도 각각 남는다.

    args:
        table: `conform()`을 거쳐 스키마가 맞춰진 테이블
        schema: 그 목표 스키마 (`archive_schema(scfg)`)
    returns:
        중복이 제거되고 스키마·컬럼 순서가 그대로인 테이블
    """
    all_columns = schema.names
    grouped = table.group_by(all_columns).aggregate([(all_columns[0], "count")])
    return conform(grouped, schema)


def _materialize_empty(
    scfg: SourceConfig,
    day: date,
    *,
    dropped: int,
    out_of_range: int,
    column_issues: dict[str, dict[str, int]],
    silver_present: bool,
    station_map_stats: dict | None,
) -> DateResult:
    """확인된 0행 날짜를 스키마가 있는 Archive와 manifest로 기록한다."""
    schema = archive_schema(scfg)
    table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )
    archive_key = storage.write_archive(scfg.source_id, day, table)
    storage.write_archive_manifest(
        scfg.source_id,
        day,
        {
            "source_id": scfg.source_id,
            "date": f"{day:%Y-%m-%d}",
            "archive_key": archive_key,
            "source_kind": SOURCE_KIND_BOOTSTRAP,
            "rows": 0,
            "dropped": dropped,
            "out_of_range": out_of_range,
            "column_issues": column_issues,
            "silver_present": silver_present,
            "materialized_empty": True,
            "loaded_at": datetime.now(tz=KST).isoformat(),
            **({"station_map": station_map_stats} if station_map_stats else {}),
        },
    )
    logger.info(
        f"stage=bootstrap status=loaded source={scfg.source_id} date={day} "
        f"rows=0 dropped={dropped} out_of_range={out_of_range} "
        f"materialized_empty=true key={archive_key}"
    )
    return DateResult(
        day=day,
        status="loaded",
        rows=0,
        dropped=dropped,
        out_of_range=out_of_range,
        archive_key=archive_key,
        silver_present=silver_present,
    )


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
            매핑표가 실행 시점 API 스냅샷이라 `rackTotCnt`·`shared`가 "그날의 값"이므로,
            나중에 그 상수 컬럼의 출처를 되짚을 수 있어야 한다.
        materialize_empty: 참이면 확인된 0행도 빈 Archive와 manifest로 기록한다.
    returns:
        이 날짜의 처리 결과. 예외를 던지지 않는다.
    """
    if not rows and not materialize_empty:
        return DateResult(day=day, status="empty")
    if not force and storage.archive_exists(scfg.source_id, day):
        return DateResult(day=day, status="skipped")

    silver_present = bool(storage.list_silver_objects(scfg.source_id, day))
    if silver_present:
        # compaction의 구역이다. 막지는 않지만 로그 한 줄은 대량 적재에서 묻히므로
        # 결과에도 남겨 실행 요약에 집계되게 한다.
        logger.warning(
            f"stage=bootstrap source={scfg.source_id} date={day} "
            "silver_present=true 다음 compaction이 이 archive를 덮어쓴다"
        )

    if not rows:
        return _materialize_empty(
            scfg,
            day,
            dropped=0,
            out_of_range=0,
            column_issues={},
            silver_present=silver_present,
            station_map_stats=station_map_stats,
        )

    schema = archive_schema(scfg)
    tables: list[pa.Table] = []
    dropped = 0
    out_of_range = 0
    column_issues: dict[str, dict[str, int]] = {}

    try:
        for window, group in sorted(group_by_window(rows, bcfg).items()):
            started = datetime.fromisoformat(window)
            if started.date() != day:
                # API 경로는 경계 시각에 다른 날짜의 관측을 섞어 줄 수 있다(`bikeListHist`
                # 실측). silver 키가 `day`로 고정되는 불변식을 이 시점에 지키지 않으면
                # 하류 compaction이 조용히 다른 날짜 데이터를 섞게 된다.
                out_of_range += len(group)
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
            dropped += outcome.counts.get("dropped", 0)
            for column, counts in outcome.column_issues.items():
                merged = column_issues.setdefault(column, {"missing": 0, "outlier": 0, "type_error": 0})
                for kind, value in counts.items():
                    merged[kind] = merged.get(kind, 0) + value
            if not outcome.silver_rows:
                continue
            table = pa.Table.from_pylist(outcome.silver_rows)
            table = table.append_column("_window_start", pa.array([window] * table.num_rows, type=pa.string()))
            table = table.append_column(
                "_source_kind", pa.array([SOURCE_KIND_BOOTSTRAP] * table.num_rows, type=pa.string())
            )
            tables.append(conform(table, schema))
    except Exception as exc:  # noqa: BLE001 — 어느 예외든 이 날짜만 실패로 격리한다
        logger.error(f"stage=bootstrap status=failed source={scfg.source_id} date={day} reason={exc}")
        return DateResult(
            day=day, status="failed", error=str(exc),
            out_of_range=out_of_range, silver_present=silver_present,
        )

    if not tables:
        if materialize_empty:
            return _materialize_empty(
                scfg,
                day,
                dropped=dropped,
                out_of_range=out_of_range,
                column_issues=column_issues,
                silver_present=silver_present,
                station_map_stats=station_map_stats,
            )
        return DateResult(
            day=day, status="empty", rows=0, dropped=dropped,
            out_of_range=out_of_range, silver_present=silver_present,
        )

    table = pa.concat_tables(tables)
    if bcfg.dedup:
        table = _dedup_full_row(table, schema)
    archive_key = storage.write_archive(scfg.source_id, day, table)
    storage.write_archive_manifest(scfg.source_id, day, {
        "source_id": scfg.source_id,
        "date": f"{day:%Y-%m-%d}",
        "archive_key": archive_key,
        "source_kind": SOURCE_KIND_BOOTSTRAP,
        "rows": table.num_rows,
        "dropped": dropped,
        "out_of_range": out_of_range,
        "column_issues": column_issues,
        "silver_present": silver_present,
        "loaded_at": datetime.now(tz=KST).isoformat(),
        # 조인하지 않은 소스에는 키 자체를 두지 않는다.
        **({"station_map": station_map_stats} if station_map_stats else {}),
    })
    logger.info(
        f"stage=bootstrap status=loaded source={scfg.source_id} date={day} "
        f"rows={table.num_rows} dropped={dropped} out_of_range={out_of_range} key={archive_key}"
    )
    return DateResult(
        day=day, status="loaded", rows=table.num_rows, dropped=dropped,
        out_of_range=out_of_range, archive_key=archive_key, silver_present=silver_present,
    )
