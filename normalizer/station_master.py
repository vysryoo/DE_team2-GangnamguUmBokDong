"""대여소 API 위경도에 생활인구 250m CELL_ID와 기상청 5km 격자를 보강한다."""

from __future__ import annotations

import argparse
import math
import re
import sys
from datetime import datetime

import pyarrow as pa
from core.kst import parse_kst_datetime
from core.sources import STATION_MASTER_ENRICHED
from core.weather_grid import latlon_to_grid
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import Point

import grid
import storage

MIN_GRID_COVERAGE = 0.95
STATION_NO_MIN = 1
STATION_NO_MAX = 32_767

_STATION_ID_PATTERN = re.compile(r"^ST-([0-9]+)$")

_TO_EPSG5179 = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)

_OUTPUT_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("station_no", pa.int16()),
        ("station_name", pa.string()),
        ("capacity", pa.int64()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("grid_id", pa.string()),
        ("weather_nx", pa.int64()),
        ("weather_ny", pa.int64()),
    ]
)


def _number(value: object) -> float | None:
    """값을 유한한 실수로 변환하고 변환할 수 없으면 None을 반환한다."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_wgs84(lat: object, lon: object) -> bool:
    """서울 인근에서 유효한 WGS84 위경도인지 확인한다."""
    latitude = _number(lat)
    longitude = _number(lon)
    return (
        latitude is not None
        and longitude is not None
        and 36.5 <= latitude <= 38.5
        and 125.5 <= longitude <= 128.5
    )


def _station_no(station_id: str) -> int:
    """`ST-<숫자>` station_id에서 모델용 int16 정류소 번호를 추출한다.

    `bike_station_master.ADDR2`는 실제 데이터에서 정류소 번호와 한글 상세주소가
    섞여 있으므로 모델의 범주 키로 사용할 수 없다. 정류소 식별자인 `RNTLS_ID`의
    숫자 suffix만 허용하고, Spark `ShortType` 및 모델 스키마와 같은 양의 int16
    범위를 벗어나면 보강 snapshot 전체를 fail-closed한다.

    raises:
        ValueError: station_id 형식이 잘못됐거나 숫자 suffix가 양의 int16 범위 밖일 때
    """
    match = _STATION_ID_PATTERN.fullmatch(station_id)
    if match is None:
        raise ValueError(
            "bike_station_master RNTLS_ID는 'ST-<숫자>' 형식이어야 합니다: "
            f"{station_id!r}"
        )
    station_no = int(match.group(1))
    if not STATION_NO_MIN <= station_no <= STATION_NO_MAX:
        raise ValueError(
            "bike_station_master RNTLS_ID 숫자 suffix가 모델 int16 범위 밖입니다: "
            f"station_id={station_id!r}, station_no={station_no}, "
            f"allowed={STATION_NO_MIN}..{STATION_NO_MAX}"
        )
    return station_no


def _realtime_by_station(table: pa.Table | None) -> dict[str, dict]:
    """실시간 대여소 표를 stationId 기준 최신 행 사전으로 바꾼다."""
    if table is None:
        return {}
    return {
        str(row["stationId"]): row
        for row in table.to_pylist()
        if row.get("stationId") is not None
    }


def enrich_station_master(
    master_table: pa.Table,
    grid_table: pa.Table,
    realtime_table: pa.Table | None = None,
) -> tuple[pa.Table, dict[str, int | float]]:
    """대여소 위경도를 실제 생활인구 격자 폴리곤과 공간 조인한다.

    API master의 좌표가 0이거나 결측이면 최신 실시간 대여소 좌표를 사용한다.
    격자 목록은 생활인구 baseline의 CELL_ID에서 가져오며 동일 CELL_ID의 24시간
    행은 하나로 중복 제거한다.
    """
    cell_ids = sorted(
        {
            str(row["CELL_ID"])
            for row in grid_table.to_pylist()
            if row.get("CELL_ID") is not None
        }
    )
    if not cell_ids:
        raise ValueError("living_population_grid에 CELL_ID가 없음")

    polygons = [grid.cell_id_to_polygon(cell_id) for cell_id in cell_ids]
    tree = STRtree(polygons)
    realtime = _realtime_by_station(realtime_table)

    rows_by_station: dict[str, dict] = {}
    for raw in master_table.to_pylist():
        if raw.get("RNTLS_ID") is None:
            continue
        station_id = str(raw["RNTLS_ID"])
        live = realtime.get(station_id, {})

        lat = raw.get("LAT")
        lon = raw.get("LOT")
        if not _valid_wgs84(lat, lon) and _valid_wgs84(
            live.get("stationLatitude"), live.get("stationLongitude")
        ):
            lat = live.get("stationLatitude")
            lon = live.get("stationLongitude")

        latitude = _number(lat)
        longitude = _number(lon)
        grid_id = None
        weather_nx = weather_ny = None
        # 좌표가 유효할 때만 계산한다 — LAT/LOT가 0인 대여소는 _number()가 0.0을
        # 돌려주므로, 이 가드 없이는 적도상 엉뚱한 격자 번호가 실린다.
        if _valid_wgs84(latitude, longitude):
            weather_nx, weather_ny = latlon_to_grid(latitude, longitude)
            x, y = _TO_EPSG5179.transform(longitude, latitude)
            point = Point(x, y)
            candidates = tree.query(point)
            matches = [int(index) for index in candidates if polygons[int(index)].covers(point)]
            if matches:
                grid_id = cell_ids[matches[0]]

        capacity_value = _number(live.get("rackTotCnt"))
        rows_by_station[station_id] = {
            "station_id": station_id,
            "station_no": _station_no(station_id),
            "station_name": live.get("stationName") or raw.get("ADDR1") or raw.get("ADDR2"),
            "capacity": int(capacity_value) if capacity_value is not None else None,
            "lat": latitude,
            "lon": longitude,
            "grid_id": grid_id,
            "weather_nx": weather_nx,
            "weather_ny": weather_ny,
        }

    rows = list(rows_by_station.values())
    if not rows:
        raise ValueError("bike_station_master에 유효한 대여소가 없음")
    mapped_count = sum(row["grid_id"] is not None for row in rows)
    coverage = mapped_count / len(rows)
    if coverage < MIN_GRID_COVERAGE:
        raise ValueError(
            f"station master CELL_ID 매핑률이 기준 미달: {coverage:.3%} < {MIN_GRID_COVERAGE:.1%}"
        )
    return pa.Table.from_pylist(rows, schema=_OUTPUT_SCHEMA), {
        "station_count": len(rows),
        "grid_mapped_count": mapped_count,
        "grid_unmapped_count": len(rows) - mapped_count,
        "grid_coverage": coverage,
    }


def run(window_start: datetime) -> int:
    """같은 window의 API master를 보강해 파티션 Silver와 manifest를 쓴다.

    격자 목록은 target date 이전 최신 nowcaster 추정치에서 얻는다. 인구값이 아닌
    정적인 CELL_ID 목록만 쓰므로 당일 nowcaster와의 스케줄 경합은 이전 성공본으로
    안전하게 피하고 미래 snapshot은 선택하지 않는다.
    """
    master_table = storage.read_station_master_silver(window_start)
    baseline_date, grid_table = storage.read_latest_nowcast_grid(window_start.date())
    realtime_table = storage.read_latest_bike_realtime_silver(window_start)
    output, metrics = enrich_station_master(master_table, grid_table, realtime_table)
    output_key = storage.write_enriched_station_master(window_start, output)
    storage.write_manifest(
        window_start,
        {
            "source_id": STATION_MASTER_ENRICHED,
            "baseline_date": baseline_date.isoformat(),
            "output_key": output_key,
            **metrics,
        },
        source_id=STATION_MASTER_ENRICHED,
    )
    print(
        f"station master enriched rows={metrics['station_count']} "
        f"mapped={metrics['grid_mapped_count']} coverage={metrics['grid_coverage']:.3%} "
        f"output={output_key}"
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="station_master.py")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI 실행 오류를 Airflow가 감지할 수 있는 종료 코드로 바꾼다."""
    args = parse_args(argv)
    try:
        return run(parse_kst_datetime(args.window_start))
    except (storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
