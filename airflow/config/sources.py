"""DAG가 실행할 collector source_id 목록을 모은다.

값은 `collector/sources/*.yaml`의 `source_id`와 정확히 일치해야 한다.
"""

REALTIME_5MIN_SOURCES = ("bike_rental_history", "bike_station_realtime", "population_realtime")
WEATHER_10MIN_SOURCE = "weather_ultra_short_live"
WEATHER_ULTRA_SHORT_FORECAST_SOURCE = "weather_ultra_short_forecast"
WEATHER_3H_SOURCE = "weather_short_term_forecast"
DAILY_POPULATION_SOURCE = "living_population_grid"
DAILY_EVENT_SOURCE = "cultural_event"
PERFORMANCE_EVENT_SOURCE = "performance_event"
STATION_MASTER_SOURCE = "bike_station_master"

# Hot Bronze는 Archive 가치와 관계없이 모든 수집 revision의 장기 복구 근거라 전 source를
# Cold Bronze로 옮긴다.
COLD_BRONZE_SOURCES = (
    *REALTIME_5MIN_SOURCES,
    WEATHER_10MIN_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
    WEATHER_3H_SOURCE,
    DAILY_POPULATION_SOURCE,
    DAILY_EVENT_SOURCE,
    PERFORMANCE_EVENT_SOURCE,
    STATION_MASTER_SOURCE,
)

# 수집 주기가 1시간 이내인 소스만. hourly_collection_alert DAG가 매시 지난 1시간
# 통계를 확인할 때 이 목록만 본다 — WEATHER_3H_SOURCE(3시간)와 일 1회 소스는 제외.
HOURLY_MONITORED_SOURCES = (
    *REALTIME_5MIN_SOURCES,
    WEATHER_10MIN_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
)

# 하루치 silver를 archive로 묶을 대상. 예보 2종(weather_ultra_short_forecast,
# weather_short_term_forecast)은 사후 재현이 불가해 archive 가치가 낮아 제외한다.
# cultural_event·performance_event·living_population_grid는 하루 1파일이라 묶을 것이 없다.
COMPACTION_SOURCES = (
    "bike_rental_history",
    "bike_station_realtime",
    "population_realtime",
    "weather_ultra_short_live",
)

# bike_rental_history를 몇 시간 전까지 다시 수집할지.
#
# tbCycleRentData는 반납이 완료된 기록만 보여 주므로, 대여 시간대가 끝난 뒤 반납된
# 기록은 이후 재조회에서만 잡힌다. 2시간 재조회 시 잔여 누락 약 0.3% (2026-08-18 실측).
RENTAL_HISTORY_LOOKBACK_HOURS = 2

# 일별 archive를 확정하기 전에 대여이력을 전체 재조회할 지연 일수.
DAILY_ARCHIVE_DELAY_DAYS = 6
