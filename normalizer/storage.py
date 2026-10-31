"""S3 실버 계층의 생활인구 격자 및 실시간 POI 데이터 I/O를 처리한다."""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta

# pyrefly: ignore [missing-import]
import pyarrow as pa

# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from core import layout
from core.s3 import (
    get_object_bytes,
    get_object_metadata,
    list_keys,
    write_json,
    write_parquet,
)
from core.source_snapshot_io import (
    AvailableSourceSnapshot,
    PartialConsumptionPolicy,
    SourceSnapshotNotFoundError,
    SourceSnapshotReadError,
    read_available_source_snapshot,
    read_exact_source_snapshot,
    read_latest_source_snapshot,
)
from core.sources import (
    BIKE_STATION_MASTER,
    BIKE_STATION_REALTIME,
    LIVING_POPULATION_GRID,
    LIVING_POPULATION_NORMALIZED,
    POPULATION_REALTIME,
    STATION_MASTER_ENRICHED,
)

PARTIAL_POLICY = PartialConsumptionPolicy.REPAIR
_NORMALIZED_SOURCE_WINDOW_METADATA = "normalized_source_window_start"


class PartitionNotFoundError(RuntimeError):
    """요청한 Silver 파티션이 S3에 존재하지 않을 때 발생하는 예외."""


def read_nowcast_grid(target_date: date) -> pa.Table:
    """해당 일자의 nowcaster 추정 격자(`nowcast.parquet`)를 읽는다.

    `living_population_grid` 실측은 수집일보다 4~5일 늦게 관측되므로 오늘과 미래 시각의
    baseline은 nowcaster 추정치(D-3~D+3)만 제공할 수 있다.

    Args:
        target_date: 대상 일자. nowcaster가 D+3까지 만들므로 미래일 수 있음
    Returns:
        읽어온 PyArrow Table
    Raises:
        PartitionNotFoundError: 해당 일자의 추정치 파일이 없을 때
    """
    return _read_nowcast(layout.nowcast_key(target_date))


def _read_nowcast(key: str) -> pa.Table:
    body = get_object_bytes(key)
    if body is None:
        raise PartitionNotFoundError(f"{LIVING_POPULATION_GRID}의 nowcast 추정치 없음: {key}")
    return pq.read_table(io.BytesIO(body))


def read_latest_nowcast_grid(target_date: date) -> tuple[date, pa.Table]:
    """target_date 이전 최신 nowcast를 정적 CELL_ID 목록 용도로 읽는다.

    station master 보강은 인구값이 아니라 CELL_ID 목록만 쓰므로, 같은 날 nowcaster와의
    스케줄 경합으로 실패시키지 않고 미래가 아닌 최신 성공본을 사용한다.

    Args:
        target_date: 이 날짜보다 미래인 snapshot은 선택하지 않는 상한
    Returns:
        실제 선택한 baseline 날짜와 PyArrow Table
    Raises:
        PartitionNotFoundError: 상한 이전 nowcast가 하나도 없을 때
    """
    candidates: list[tuple[date, str]] = []
    for key in list_keys(layout.silver_prefix(LIVING_POPULATION_GRID)):
        try:
            partition = key.split("/dt=", 1)[1].split("/", 1)[0]
            snapshot_date = date.fromisoformat(partition)
        except (IndexError, ValueError):
            continue
        if key == layout.nowcast_key(snapshot_date) and snapshot_date <= target_date:
            candidates.append((snapshot_date, key))
    if not candidates:
        raise PartitionNotFoundError(
            f"{LIVING_POPULATION_GRID}의 {target_date.isoformat()} 이전 nowcast 추정치 없음"
        )
    snapshot_date, key = max(candidates)
    return snapshot_date, _read_nowcast(key)


def read_realtime_snapshot(window_start: datetime) -> AvailableSourceSnapshot:
    """완전·부분·최근 성공 순서로 실시간 POI 인구를 선택한다."""
    try:
        return read_available_source_snapshot(
            POPULATION_REALTIME,
            window_start,
            lookback=timedelta(hours=1),
            partial_policy=PARTIAL_POLICY,
        )
    except SourceSnapshotReadError as exc:
        raise PartitionNotFoundError(str(exc)) from exc


def read_station_master_silver(window_start: datetime) -> pa.Table:
    """Collector가 같은 window에 쓴 대여소 master Silver를 읽는다."""
    try:
        snapshot = read_exact_source_snapshot(BIKE_STATION_MASTER, window_start)
    except SourceSnapshotReadError as exc:
        raise PartitionNotFoundError(str(exc)) from exc
    if snapshot.table is None:
        raise PartitionNotFoundError(
            f"{BIKE_STATION_MASTER} exact source snapshot이 EMPTY입니다: {window_start.isoformat()}"
        )
    return snapshot.table


def read_latest_bike_realtime_silver(window_start: datetime) -> pa.Table | None:
    """window 시각 이전 24시간의 최신 authoritative 대여소 Silver를 읽는다."""
    try:
        snapshot = read_latest_source_snapshot(
            BIKE_STATION_REALTIME,
            window_start,
            lookback=timedelta(hours=24),
        )
    except SourceSnapshotNotFoundError:
        return None
    except SourceSnapshotReadError as exc:
        raise PartitionNotFoundError(str(exc)) from exc
    return snapshot.table


def write_normalized_silver(
    target: datetime,
    table: pa.Table,
    *,
    source_window_start: datetime | None = None,
) -> str | None:
    """새 세대의 정규화 결과만 target Silver Parquet에 저장한다.

    미래 예보는 여러 수집 window가 같은 target 키를 갱신한다. 늦게 실행된 과거
    backfill이 더 최신 window의 예보나 실제 관측을 되돌리지 않도록, S3 user
    metadata에 원본 수집 시각을 기록하고 기존 세대가 더 최신이면 쓰기를 건너뛴다.

    args:
        target: 정규화 값이 의미하는 시각이자 출력 키 시각
        table: 저장할 정규화 격자 테이블
        source_window_start: 이 값을 만든 실시간 수집 window. 생략하면 target과 같음
    returns:
        저장한 S3 키. 더 최신 세대가 이미 있어 건너뛰면 None
    raises:
        ValueError: 기존 객체의 세대 metadata가 손상됐을 때
    """
    generation = source_window_start or target
    key = layout.silver_window_key(LIVING_POPULATION_NORMALIZED, target)
    metadata = get_object_metadata(key)
    if metadata is not None:
        raw_generation = metadata.get(_NORMALIZED_SOURCE_WINDOW_METADATA)
        if raw_generation is not None:
            try:
                existing_generation = datetime.fromisoformat(raw_generation)
            except ValueError as exc:
                raise ValueError(
                    f"정규화 세대 metadata가 잘못됐습니다: key={key}"
                ) from exc
            if existing_generation > generation:
                return None

    write_parquet(
        table,
        key,
        metadata={_NORMALIZED_SOURCE_WINDOW_METADATA: generation.isoformat()},
    )
    return key


def write_enriched_station_master(window_start: datetime, table: pa.Table) -> str:
    """CELL_ID가 보강된 대여소 master를 파티션 Silver로 저장한다."""
    key = layout.silver_window_key(STATION_MASTER_ENRICHED, window_start)
    write_parquet(table, key)
    return key


def write_manifest(
    window_start: datetime,
    data: dict,
    source_id: str = LIVING_POPULATION_NORMALIZED,
) -> str:
    """해당 source의 정규화 실행 메타데이터를 Manifest JSON 파일로 저장한다."""
    key = layout.window_manifest_key(source_id, window_start)
    write_json(key, data)
    return key
