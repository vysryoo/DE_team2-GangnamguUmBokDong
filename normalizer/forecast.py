"""실시간 도시데이터의 `FCST_n_*` 예측 슬롯을 미래 시각별 POI 스냅샷으로 바꾼다."""

from __future__ import annotations

from datetime import datetime, timedelta, tzinfo

# pyrefly: ignore [missing-import]
import pyarrow as pa
from core.forecast import POPULATION_FORECAST_SLOT_COUNT

import merge
import poi

# 실시간 도시데이터의 예측 시각 포맷(실측: "2026-08-19 22:00", KST 정시).
_FORECAST_TIME_FORMAT = "%Y-%m-%d %H:%M"
# 이보다 먼 예측은 쓰지 않는다. API가 간격을 바꿔도 미래로 무한히 번지지 않게 하는 상한.
_MAX_HORIZON = timedelta(hours=12)

ForecastsByCode = dict[str, dict[datetime, tuple[float, float]]]


def _parse_forecast_time(raw: object, tzinfo: tzinfo | None) -> datetime | None:
    """`FCST_n_TIME` 문자열을 window_start와 같은 시간대의 datetime으로 바꾼다. 실패하면 None."""
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.strptime(raw, _FORECAST_TIME_FORMAT)  # noqa: DTZ007 - 아래에서 tz를 붙인다
    except ValueError:
        return None
    return parsed.replace(tzinfo=tzinfo)


def _forecast_by_time(row: dict, tzinfo: tzinfo | None) -> dict[datetime, tuple[float, float]]:
    """POI 한 행의 예측 슬롯을 {예측시각: (MIN, MAX)}로 바꾼다.

    슬롯 번호를 "n시간 후"로 해석하지 않는다 — 실측(2026-08-19 20:55 관측)에서 첫 슬롯이
    22:00이었다. 어느 시각의 예측인지는 `FCST_n_TIME`만이 말한다.
    """
    if row.get("FCST_YN") != "Y":
        return {}

    forecasts: dict[datetime, tuple[float, float]] = {}
    for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
        target = _parse_forecast_time(row.get(f"FCST_{slot}_TIME"), tzinfo)
        pop_min = row.get(f"FCST_{slot}_PPLTN_MIN")
        pop_max = row.get(f"FCST_{slot}_PPLTN_MAX")
        if target is None or pop_min is None or pop_max is None:
            continue
        forecasts[target] = (float(pop_min), float(pop_max))
    return forecasts


def collect_forecasts(
    realtime_table: pa.Table, window_start: datetime
) -> ForecastsByCode:
    """POI 코드별 {예측시각: (MIN, MAX)} 맵을 만든다. 과거·12시간 초과 시각은 버린다."""
    horizon_end = window_start + _MAX_HORIZON
    by_code: ForecastsByCode = {}
    for row in realtime_table.to_pylist():
        forecasts = {
            target: bounds
            for target, bounds in _forecast_by_time(row, window_start.tzinfo).items()
            if window_start < target <= horizon_end
        }
        if forecasts:
            by_code[row["AREA_CD"]] = forecasts
    return by_code


def forecast_targets(
    forecasts_by_code: ForecastsByCode
) -> list[datetime]:
    """보정할 미래 시각 목록(오름차순). POI마다 슬롯이 어긋나도 합집합으로 모은다."""
    return sorted({target for forecasts in forecasts_by_code.values() for target in forecasts})


def build_forecast_snapshots(
    poi_areas: tuple[poi.PoiArea, ...],
    forecasts_by_code: ForecastsByCode,
    target: datetime,
) -> dict[str, merge.PoiSnapshot]:
    """해당 미래 시각의 PoiSnapshot 맵을 만든다.

    성비(`male_rate`/`female_rate`)는 넣지 않는다(0.0) — 예측값에 성비가 없어 미래 시각은
    `merge.merge_cell_total_only()`가 총량만 합성하고 성·연령 구성은 baseline을 따른다.

    args:
        poi_areas: POI 영역 지오메트리 목록
        forecasts_by_code: POI 코드별 예측 맵
        target: 대상 예측 시각
    returns:
        POI 코드를 키로 하는 PoiSnapshot 맵(그 시각 예측이 없는 POI는 빠진다)
    """
    snapshots: dict[str, merge.PoiSnapshot] = {}
    for area in poi_areas:
        bounds = forecasts_by_code.get(area.area_cd, {}).get(target)
        if bounds is None:
            continue
        pop_min, pop_max = bounds
        snapshots[area.area_cd] = merge.PoiSnapshot(
            area_cd=area.area_cd,
            male_rate=0.0,
            female_rate=0.0,
            pop_estimate=(pop_min + pop_max) / 2.0,
            geometry=area.geometry,
            area_m2=area.area_m2,
        )
    return snapshots
