"""데이터 레이크에 쓰이는 source id 목록.

S3 경로와 manifest의 `source_id`로 쓰이는 이름이다. Collector 수집 source의 id는
`collector/sources/{source_id}.yaml`과 정확히 같아야 한다.

계약 검증 코드가 `type(value) is str`로 확인하므로 Enum이 아니라 문자열 상수로 둔다.
"""

from __future__ import annotations

BIKE_RENTAL_HISTORY = "bike_rental_history"
BIKE_STATION_MASTER = "bike_station_master"
BIKE_STATION_REALTIME = "bike_station_realtime"
CULTURAL_EVENT = "cultural_event"
LIVING_POPULATION_GRID = "living_population_grid"
PERFORMANCE_EVENT = "performance_event"
POPULATION_REALTIME = "population_realtime"
WEATHER_SHORT_TERM_FORECAST = "weather_short_term_forecast"
WEATHER_ULTRA_SHORT_FORECAST = "weather_ultra_short_forecast"
WEATHER_ULTRA_SHORT_LIVE = "weather_ultra_short_live"

COLLECTOR_SOURCE_IDS = (
    BIKE_RENTAL_HISTORY,
    BIKE_STATION_MASTER,
    BIKE_STATION_REALTIME,
    CULTURAL_EVENT,
    LIVING_POPULATION_GRID,
    PERFORMANCE_EVENT,
    POPULATION_REALTIME,
    WEATHER_SHORT_TERM_FORECAST,
    WEATHER_ULTRA_SHORT_FORECAST,
    WEATHER_ULTRA_SHORT_LIVE,
)

# Collector가 아니라 다른 모듈이 만드는 파생 source다.
LIVING_POPULATION_NORMALIZED = "living_population_normalized"
STATION_MASTER_ENRICHED = "station_master_enriched"
POI_MASTER = "poi_master"
