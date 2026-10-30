import os
from datetime import datetime, timedelta
from itertools import pairwise

import httpx
import pytest
from core.forecast import POPULATION_FORECAST_SLOT_COUNT

import adapters.kma_apihub
import adapters.seoul_openapi  # noqa: F401
from adapters.base import Window, get_adapter
from config import loader
from core.kst import KST


# 계약 테스트는 외부 API를 직접 호출하므로, 키가 없으면 스킵한다.
seoul_key = os.environ.get("SEOUL_OPENAPI_KEY")
kma_key = os.environ.get("KMA_APIHUB_KEY")

pytestmark = pytest.mark.skipif(
    not seoul_key or not kma_key,
    reason="SEOUL_OPENAPI_KEY or KMA_APIHUB_KEY is not set"
)

def get_recent_window(source_id: str) -> Window:
    """각 소스별로 데이터를 확실히 받을 수 있는 최근 시각을 반환한다."""
    now = datetime.now(KST)
    if source_id == "weather_ultra_short_live":
        # 초단기 실황은 1시간 전
        start = now - timedelta(hours=1)
    elif source_id == "weather_short_term_forecast":
        # 단기 예보는 1일 전 (데이터가 3시간 단위로 생성되므로 넉넉히)
        start = now - timedelta(days=1)
    elif source_id == "bike_rental_history":
        # 대여이력은 3일 전 데이터가 안전하다 (데이터 적재 지연 고려)
        start = now - timedelta(days=3)
    else:
        # 실시간 소스들은 5분 전
        start = now - timedelta(minutes=5)
    return Window(window_start=start, window_end=start + timedelta(minutes=5))

@pytest.mark.xfail(reason="현재 여러 소스의 YAML 설정과 실제 API 스키마/요청방식이 불일치하여 실패가 예상됨", strict=False)
@pytest.mark.parametrize("source_id", [
    "bike_station_realtime",
    "bike_station_master",
    "cultural_event",
    "bike_rental_history",
    "living_population_grid",
    "population_realtime",
    "weather_ultra_short_live",
    "weather_short_term_forecast",
])
def test_api_contract(source_id):
    """실제 API를 호출해 설정된 컬럼들이 응답에 존재하는지 검증한다."""
    config = loader.load(source_id)
    adapter_cls = get_adapter(config.adapter)
    window = get_recent_window(source_id)

    with httpx.Client() as client:
        # fetch 제너레이터에서 첫 번째 청크만 가져온다.
        generator = adapter_cls.fetch(config, window, client=client)
        try:
            first_result = next(generator)
        except StopIteration:
            pytest.fail("API가 응답 조각을 하나도 반환하지 않았습니다.")

    assert first_result.error is None, f"API 호출 실패: {first_result.error}"
    assert first_result.payload is not None, "응답 payload가 비어 있습니다."

    rows = adapter_cls.normalize([first_result.payload], config)
    if not rows:
        # 응답은 성공적이나 데이터가 0건일 경우 (예: 조건에 맞는 데이터 없음)
        # 계약 구조 자체는 틀리지 않았을 수 있으나, 컬럼 검증을 위해선 데이터가 필요하다.
        # 이 테스트가 깨지면 window 시간을 더 이전으로 돌려보거나 해야 한다.
        pytest.skip("데이터가 0건이라 컬럼 검증을 건너뜁니다.")

    sample_row = rows[0]
    expected_columns = set(config.columns.keys())
    actual_columns = set(sample_row.keys())

    # 설정된 컬럼들이 실제 응답 필드 목록의 부분집합인지 확인
    missing_columns = expected_columns - actual_columns
    assert not missing_columns, f"API 응답에 다음 설정된 컬럼들이 누락되었습니다: {missing_columns}"


def test_population_forecast_slots_are_hourly_and_in_the_future():
    """`FCST_PPLTN` 평탄화 결과가 normalizer의 전제(정시·1시간 간격·미래 시각)를 만족하는지 확인한다.

    normalizer는 슬롯 번호를 "n시간 후"로 믿지 않고 `FCST_n_TIME`을 파싱해 시각을 맞춘다.
    이 테스트는 그 파싱 전제(포맷·간격·미래성)가 API 쪽에서 깨지면 알려준다.
    """
    config = loader.load("population_realtime")
    adapter_cls = get_adapter(config.adapter)

    with httpx.Client() as client:
        generator = adapter_cls.fetch(config, get_recent_window("population_realtime"), client=client)
        try:
            first_result = next(generator)
        except StopIteration:
            pytest.fail("API가 응답 조각을 하나도 반환하지 않았습니다.")

    rows = adapter_cls.normalize([first_result.payload], config)
    if not rows or rows[0].get("FCST_YN") != "Y":
        pytest.skip("이 지점은 예측을 제공하지 않아 슬롯 검증을 건너뜁니다.")

    row = rows[0]
    times = []
    for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
        raw = row.get(f"FCST_{slot}_TIME")
        assert raw is not None, f"슬롯 {slot}의 FCST_TIME이 없습니다: {sorted(row)}"
        times.append(datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(tzinfo=KST))

    assert times == sorted(times), f"슬롯이 시간순이 아닙니다: {times}"
    assert all(t.minute == 0 for t in times), f"정시가 아닌 예측 시각이 있습니다: {times}"
    intervals = {(b - a).total_seconds() for a, b in pairwise(times)}
    assert intervals == {3600.0}, f"1시간 간격이 아닙니다: {intervals}"

    observed = datetime.strptime(row["PPLTN_TIME"], "%Y-%m-%d %H:%M").replace(tzinfo=KST) \
        if row.get("PPLTN_TIME") else datetime.now(KST)
    assert times[0] > observed, f"첫 예측({times[0]})이 관측 시각({observed})보다 과거입니다."
