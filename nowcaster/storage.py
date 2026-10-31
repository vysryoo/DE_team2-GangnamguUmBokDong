"""`living_population_grid`의 아카이브, authoritative 실측 Silver, nowcast와 cache를 다룬다.

archive와 nowcast 키는 collector·normalizer와 공유하므로 `core.layout`이 갖는다.
"""

from __future__ import annotations

from datetime import date, datetime

# pyrefly: ignore [missing-import]
import pandas as pd

# pyrefly: ignore [missing-import]
import pyarrow as pa

# pyrefly: ignore [missing-import]
from core.layout import archive_key, archive_prefix, nowcast_key

# pyrefly: ignore [missing-import]
from core.s3 import (
    delete_object,
    list_keys,
    read_json,
    read_parquet,
    write_json,
    write_parquet,
)

# pyrefly: ignore [missing-import]
from core.source_snapshot_io import (
    SourceSnapshotNotFoundError,
    read_exact_source_snapshot,
)
from core.sources import LIVING_POPULATION_GRID

_HISTORICAL_CACHE_PREFIX = f"derived/{LIVING_POPULATION_GRID}/historical_avg_cache"
_CACHE_INDEX = ["H_DNG_CD", "CELL_ID", "TT"]


def read_real_grid_silver(logical_dttm: datetime) -> pa.Table | None:
    """Exact authority가 확인된 생활인구 Silver만 실측 테이블로 읽는다.

    완료된 PARTIAL과 authority 게시 전 immutable Silver는 실측으로 승격하지 않는다.
    Exact authority 자체가 없으면 None을 반환하지만, authority나 연결 artifact가
    손상된 경우에는 예외를 그대로 전파해 데이터 오염을 숨기지 않는다.
    """
    try:
        snapshot = read_exact_source_snapshot(LIVING_POPULATION_GRID, logical_dttm)
    except SourceSnapshotNotFoundError:
        return None
    return snapshot.table


def write_archive(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 데이터를 아카이브 parquet으로 저장하고 저장된 key를 반환한다."""
    key = archive_key(LIVING_POPULATION_GRID, target_date)
    write_parquet(table, key)
    return key


def list_archive_dates() -> list[date]:
    """아카이브에 저장된 데이터의 날짜 목록을 오름차순으로 반환한다."""
    prefix = archive_prefix(LIVING_POPULATION_GRID)
    dates = []
    for key in list_keys(prefix):
        if not key.endswith(".parquet"):
            continue
        filename = key[len(prefix):]
        dt_str = filename.removeprefix("dt=").removesuffix(".parquet")
        dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


def read_archive(target_date: date) -> pa.Table | None:
    """해당 날짜의 아카이브를 읽는다. 없으면 None(4주 lookback 중 결측으로 처리)."""
    key = archive_key(LIVING_POPULATION_GRID, target_date)
    return read_parquet(key, as_pandas=False)


def write_nowcast(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 추정치 데이터를 nowcast.parquet 파일로 저장한다."""
    key = nowcast_key(target_date)
    write_parquet(table, key)
    return key


def delete_nowcast(target_date: date) -> None:
    """해당 날짜의 기존 추정치 parquet 파일을 삭제한다."""
    delete_object(nowcast_key(target_date))


def _historical_cache_key(pattern: str, part: str) -> str:
    return f"{_HISTORICAL_CACHE_PREFIX}/{pattern}_{part}"


def read_historical_cache(
    pattern: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, list[str]]:
    """`estimate_day.historical_average_cached`의 패턴별 (합계, 카운트, 반영한 날짜)를 읽는다.

    캐시가 아직 없으면 (None, None, [])을 반환한다.
    """
    sum_df = read_parquet(_historical_cache_key(pattern, "sum.parquet"))
    count_df = read_parquet(_historical_cache_key(pattern, "count.parquet"))
    manifest = read_json(_historical_cache_key(pattern, "dates.json"))
    included = manifest["dates"] if manifest else []
    if sum_df is not None:
        sum_df = sum_df.set_index(_CACHE_INDEX)
    if count_df is not None:
        count_df = count_df.set_index(_CACHE_INDEX)
    return sum_df, count_df, included


def write_historical_cache(
    pattern: str, sum_df: pd.DataFrame, count_df: pd.DataFrame, included_dates: list[str]
) -> None:
    """패턴별 누적 합계·카운트와 반영한 날짜 목록을 갱신한다."""
    write_parquet(sum_df.reset_index(), _historical_cache_key(pattern, "sum.parquet"))
    write_parquet(count_df.reset_index(), _historical_cache_key(pattern, "count.parquet"))
    write_json(_historical_cache_key(pattern, "dates.json"), {"dates": included_dates})
