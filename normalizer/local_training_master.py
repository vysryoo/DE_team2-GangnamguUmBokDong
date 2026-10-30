"""실제 과거 Archive에서 로컬 학습용 current station dimension을 만든다."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from urllib.parse import urlsplit

import pyarrow as pa
import station_master
import storage
from core.kst import KST
from core.layout import archive_key
from core.s3 import read_parquet
from core.sources import BIKE_STATION_REALTIME, LIVING_POPULATION_GRID

_LOCAL_S3_HOSTS = frozenset(
    {
        "127.0.0.1",
        "host.docker.internal",
        "localhost",
        "minio",
    }
)


def _require_local_environment() -> None:
    """명시적 opt-in과 로컬 S3 endpoint를 모두 확인한다."""
    if os.environ.get("LOCAL_TRAINING_SMOKE_ALLOW_WRITE") != "1":
        raise ValueError("LOCAL_TRAINING_SMOKE_ALLOW_WRITE=1 opt-in이 필요합니다.")
    endpoint = os.environ.get("S3_ENDPOINT_URL", "")
    parsed = urlsplit(endpoint)
    explicit_host = os.environ.get("LOCAL_TRAINING_SMOKE_S3_HOST")
    local_host = parsed.hostname in _LOCAL_S3_HOSTS or (
        bool(explicit_host) and parsed.hostname == explicit_host
    )
    if parsed.scheme != "http" or not local_host:
        raise ValueError(f"로컬 HTTP S3 endpoint가 필요합니다: {endpoint!r}")


def _master_from_station_archive(table: pa.Table) -> pa.Table:
    """실제 재고 Archive의 ID·이름·좌표를 master 입력으로 변환한다."""
    required = {
        "stationId",
        "stationName",
        "stationLatitude",
        "stationLongitude",
    }
    if missing := required - set(table.column_names):
        raise ValueError(f"station Archive 필수 컬럼이 없습니다: {sorted(missing)}")

    rows_by_id: dict[str, dict[str, object]] = {}
    for raw in table.to_pylist():
        station_id = str(raw.get("stationId") or "").strip()
        if not station_id:
            continue
        rows_by_id[station_id] = {
            "RNTLS_ID": station_id,
            "ADDR1": str(raw.get("stationName") or station_id),
            "ADDR2": "",
            "LAT": raw.get("stationLatitude"),
            "LOT": raw.get("stationLongitude"),
        }
    if not rows_by_id:
        raise ValueError("station Archive에 유효한 stationId가 없습니다.")
    return pa.Table.from_pylist(
        [rows_by_id[station_id] for station_id in sorted(rows_by_id)]
    )


def prepare(source_date: date) -> dict[str, object]:
    """실제 station·population Archive로 enriched current snapshot을 게시한다."""
    _require_local_environment()
    station_table = read_parquet(
        archive_key(BIKE_STATION_REALTIME, source_date),
        as_pandas=False,
    )
    population_table = read_parquet(
        archive_key(LIVING_POPULATION_GRID, source_date),
        as_pandas=False,
    )
    if station_table is None or population_table is None:
        raise FileNotFoundError(f"학습 master 입력 Archive가 없습니다: {source_date}")

    master_table = _master_from_station_archive(station_table)
    enriched, metrics = station_master.enrich_station_master(
        master_table,
        population_table,
        station_table,
    )
    snapshot = datetime.combine(source_date, datetime.min.time(), tzinfo=KST)
    output_key = storage.write_enriched_station_master(snapshot, enriched)
    return {"output_key": output_key, "source_date": source_date.isoformat(), **metrics}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        description="실제 Archive에서 로컬 학습용 station master를 준비한다."
    )
    parser.add_argument("--source-date", required=True, type=date.fromisoformat)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """학습용 station master를 게시하고 결과를 출력한다."""
    args = parse_args(argv)
    try:
        result = prepare(args.source_date)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(" ".join(f"{key}={value}" for key, value in sorted(result.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
