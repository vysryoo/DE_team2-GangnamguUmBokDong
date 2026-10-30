"""#9 기상청 API 허브 어댑터 테스트: 격자 반복, HTTP 상태 분류, normalize pivot."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

import httpx
import pytest
from adapters.base import FetchErrorKind, Window, fetch_with_rounds
from adapters.kma_apihub import (
    KmaApiHubAdapter,
    adjust_base_time,
    _classify_result_code,
)
from core.kst import KST



class _StubConfig:
    def __init__(self, adapter_params):
        self.adapter_params = adapter_params

    def effective_fetch_budget(self):
        """공통 round loop 테스트용 fetch budget을 반환한다."""
        return timedelta(seconds=1)


def _config(grids=None):
    return _StubConfig(
        {
            "endpoint": "getUltraSrtNcst",
            "root_key": "response.body.items.item",
            "pivot": {"key": "category", "value": "obsrValue"},
            "grids": grids if grids is not None else [[60, 127], [61, 127]],
        }
    )


def _window():
    return Window(
        window_start=datetime(2026, 8, 12, 14, 0, tzinfo=KST),
        window_end=datetime(2026, 8, 12, 14, 10, tzinfo=KST),
    )


def _thirty_four_grids() -> list[list[int]]:
    """KMA production plan과 같은 cardinality의 중복 없는 테스트 grid를 만든다."""
    return [[57 + index // 10, 120 + index % 10] for index in range(34)]


def _body(items=None):
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00"},
                "body": {"items": {"item": items or []}},
            }
        }
    ).encode()


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("KMA_APIHUB_KEY", "secret-key-456")


def test_fetch_calls_once_per_grid_in_order():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=_body())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(KmaApiHubAdapter.fetch(_config(), _window(), client=client))

    assert [r.key for r in results] == ["grid-060x127", "grid-061x127"]
    assert len(calls) == 2


def test_fetch_logs_elapsed_for_each_http_request(caplog):
    """응답마다 인증정보 없이 endpoint·격자·페이지·상태·소요시간을 남긴다."""
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=_body())
        )
    )

    with caplog.at_level(logging.INFO, logger="adapters.kma_apihub"):
        list(
            KmaApiHubAdapter.fetch(
                _config(grids=[[60, 127]]), _window(), client=client
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    request_logs = [message for message in messages if "stage=kma_http_request" in message]
    assert len(request_logs) == 1
    assert "endpoint=getUltraSrtNcst" in request_logs[0]
    assert "grid=060x127" in request_logs[0]
    assert "page=1" in request_logs[0]
    assert "outcome=response" in request_logs[0]
    assert "status_code=200" in request_logs[0]
    assert re.search(r"elapsed_ms=\d+\.\d", request_logs[0])
    assert "secret-key-456" not in request_logs[0]


def test_fetch_logs_elapsed_on_request_error(caplog):
    """응답을 받지 못해도 요청 예외 종류와 경과시간을 남긴다."""
    def handler(request):
        raise httpx.ReadTimeout("timed out", request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))

    with caplog.at_level(logging.INFO, logger="adapters.kma_apihub"):
        results = list(
            KmaApiHubAdapter.fetch(
                _config(grids=[[60, 127]]), _window(), client=client
            )
        )

    request_log = next(
        record.getMessage()
        for record in caplog.records
        if "stage=kma_http_request" in record.getMessage()
    )
    assert results[0].error is FetchErrorKind.TRANSIENT
    assert "endpoint=getUltraSrtNcst" in request_log
    assert "grid=060x127" in request_log
    assert "page=1" in request_log
    assert "outcome=request_error" in request_log
    assert "error_type=ReadTimeout" in request_log
    assert re.search(r"elapsed_ms=\d+\.\d", request_log)
    assert "secret-key-456" not in request_log


def test_planned_parts_fixes_all_34_grids_before_fetch():
    """Deadline으로 iterator가 끊기기 전부터 미방문 grid key를 모두 안다."""
    planned = KmaApiHubAdapter.planned_parts(
        _config(grids=_thirty_four_grids()), _window()
    )

    assert len(planned) == 34
    assert "grid-057x120" in planned
    assert "grid-060x123" in planned


def test_planned_parts_rejects_duplicate_grid():
    """중복 격자는 한 번만 호출되므로 완전 snapshot 근거가 될 수 없다."""
    grids = _thirty_four_grids()[:-1] + [[57, 120]]
    with pytest.raises(ValueError):
        KmaApiHubAdapter.planned_parts(_config(grids=grids), _window())


def test_deadline_marks_all_33_unvisited_kma_grids_missing():
    """첫 grid 뒤 deadline이면 yield하지 못한 나머지 plan도 missing에 남는다."""
    config = _config(grids=_thirty_four_grids())
    planned = KmaApiHubAdapter.planned_parts(config, _window())
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=_body())
        )
    )
    clock = iter((0.0, 0.0, 0.0, 2.0))

    result = fetch_with_rounds(
        KmaApiHubAdapter.fetch,
        config,
        _window(),
        client=client,
        now_fn=lambda: next(clock),
        sleep_fn=lambda seconds: None,
        planned_parts=planned,
    )

    assert tuple(result.chunks) == ("grid-057x120",)
    assert len(result.missing) == 33
    assert set(result.chunks) | set(result.missing) == set(planned)


def test_fetch_uses_window_start_as_base_date_and_time_without_offset():
    captured = []

    def handler(request):
        captured.append(str(request.url))
        return httpx.Response(200, content=_body())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    list(KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client))

    assert "base_date=20260812" in captured[0]
    assert "base_time=1400" in captured[0]


def test_fetch_expected_total_is_always_none():
    def handler(request):
        return httpx.Response(200, content=_body())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert results[0].expected_total is None


def test_fetch_masks_api_key_from_chunk_keys():
    def handler(request):
        return httpx.Response(200, content=_body())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    for r in results:
        assert "secret-key-456" not in r.key


def test_fetch_skips_grids_already_collected():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=_body())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(
            _config(),
            _window(),
            client=client,
            skip=frozenset({"grid-060x127"}),
        )
    )

    assert [r.key for r in results] == ["grid-061x127"]
    assert len(calls) == 1


def test_fetch_classifies_401_and_403_as_fatal_and_stops():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(401, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(KmaApiHubAdapter.fetch(_config(), _window(), client=client))

    assert len(results) == 1
    assert results[0].error is FetchErrorKind.FATAL
    assert len(calls) == 1  # FATAL 뒤 나머지 격자는 호출하지 않는다


def test_fetch_classifies_5xx_as_transient_and_continues():
    def handler(request):
        return httpx.Response(503, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(KmaApiHubAdapter.fetch(_config(), _window(), client=client))

    assert [r.error for r in results] == [
        FetchErrorKind.TRANSIENT,
        FetchErrorKind.TRANSIENT,
    ]


def test_fetch_classifies_404_as_permanent_and_continues():
    def handler(request):
        return httpx.Response(404, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(KmaApiHubAdapter.fetch(_config(), _window(), client=client))

    assert [r.error for r in results] == [
        FetchErrorKind.PERMANENT,
        FetchErrorKind.PERMANENT,
    ]


def test_fetch_yields_raw_response_unmodified():
    raw = _body(items=[{"category": "T1H", "obsrValue": "31.6"}])

    def handler(request):
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert results[0].payload == raw


def _page_body(total_count, page_no, items):
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "dataType": "JSON",
                    "items": {"item": items},
                    "pageNo": page_no,
                    "numOfRows": 1000,
                    "totalCount": total_count,
                },
            }
        }
    ).encode()


def test_fetch_fetches_second_page_when_total_count_exceeds_num_of_rows():
    """totalCount(1052)가 numOfRows(1000)를 넘으면 2페이지를 마저 받아 하나의
    격자 결과로 합쳐야 한다(#버그: 예전엔 1페이지만 받고 나머지 52건을 유실했다)."""
    page1_items = [{"category": "TMP", "fcstValue": str(i)} for i in range(1000)]
    page2_items = [{"category": "REH", "fcstValue": str(i)} for i in range(52)]
    calls = []

    def handler(request):
        params = dict(request.url.params)
        page_no = int(params["pageNo"])
        calls.append(page_no)
        if page_no == 1:
            return httpx.Response(200, content=_page_body(1052, 1, page1_items))
        return httpx.Response(200, content=_page_body(1052, 2, page2_items))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert calls == [1, 2]
    assert len(results) == 1
    assert results[0].key == "grid-060x127"
    assert results[0].error is None

    merged = json.loads(results[0].payload)
    items = merged["response"]["body"]["items"]["item"]
    assert len(items) == 1052


def test_fetch_single_page_when_total_count_missing_stays_unmodified():
    """totalCount가 없는(기존 소스 전부 해당) 응답은 예전처럼 1페이지만 받고,
    원본 바이트를 그대로 넘긴다(회귀 방지)."""
    raw = _body(items=[{"category": "T1H", "obsrValue": "31.6"}])
    calls = []

    def handler(request):
        calls.append(dict(request.url.params)["pageNo"])
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert calls == ["1"]
    assert results[0].payload == raw


def test_fetch_single_page_when_total_count_fits_num_of_rows():
    """totalCount(8)가 numOfRows(1000)보다 작으면(초단기실황 실제 사례) 추가
    페이지를 요청하지 않는다 — 회귀 없음 확인."""
    items = [{"category": f"C{i}", "obsrValue": str(i)} for i in range(8)]
    raw = _page_body(8, 1, items)
    calls = []

    def handler(request):
        calls.append(dict(request.url.params)["pageNo"])
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert calls == ["1"]
    assert results[0].payload == raw


def test_fetch_transient_on_second_page_fails_whole_grid_not_just_that_page():
    """2페이지째가 실패하면 1페이지만 부분 반영하지 않고 격자 전체를 TRANSIENT로
    실패 처리한다 — 다음 라운드가 1페이지부터 다시 받아야 하기 때문이다."""
    page1_items = [{"category": "TMP", "fcstValue": str(i)} for i in range(1000)]

    def handler(request):
        page_no = dict(request.url.params)["pageNo"]
        if page_no == "1":
            return httpx.Response(200, content=_page_body(1052, 1, page1_items))
        return httpx.Response(503, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(_config(grids=[[60, 127]]), _window(), client=client)
    )

    assert len(results) == 1
    assert results[0].key == "grid-060x127"
    assert results[0].error is FetchErrorKind.TRANSIENT
    assert results[0].payload is None


def test_fetch_fatal_on_second_page_stops_entire_fetch():
    page1_items = [{"category": "TMP", "fcstValue": str(i)} for i in range(1000)]

    def handler(request):
        page_no = dict(request.url.params)["pageNo"]
        if page_no == "1":
            return httpx.Response(200, content=_page_body(1052, 1, page1_items))
        return httpx.Response(401, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        KmaApiHubAdapter.fetch(
            _config(grids=[[60, 127], [61, 127]]), _window(), client=client
        )
    )

    assert len(results) == 1
    assert results[0].error is FetchErrorKind.FATAL


def test_normalize_pivots_long_rows_into_wide_records_grouped_by_remaining_fields():
    chunk = _body(
        items=[
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "nx": 60,
                "ny": 127,
                "category": "T1H",
                "obsrValue": "31.6",
            },
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "nx": 60,
                "ny": 127,
                "category": "REH",
                "obsrValue": "42",
            },
        ]
    )

    rows = KmaApiHubAdapter.normalize([chunk], _config())

    assert rows == [
        {
            "baseDate": "20260812",
            "baseTime": "1400",
            "nx": 60,
            "ny": 127,
            "T1H": "31.6",
            "REH": "42",
        }
    ]


def test_normalize_groups_separately_across_chunks_by_grid():
    chunk1 = _body(
        items=[
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "nx": 60,
                "ny": 127,
                "category": "T1H",
                "obsrValue": "31.6",
            }
        ]
    )
    chunk2 = _body(
        items=[
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "nx": 61,
                "ny": 127,
                "category": "T1H",
                "obsrValue": "30.9",
            }
        ]
    )

    rows = KmaApiHubAdapter.normalize([chunk1, chunk2], _config())

    assert rows == [
        {
            "baseDate": "20260812",
            "baseTime": "1400",
            "nx": 60,
            "ny": 127,
            "T1H": "31.6",
        },
        {
            "baseDate": "20260812",
            "baseTime": "1400",
            "nx": 61,
            "ny": 127,
            "T1H": "30.9",
        },
    ]


def test_normalize_absorbs_forecast_endpoint_field_names_via_pivot_config_only():
    """예보 엔드포인트는 값 필드가 fcstValue이고 그룹 키에 fcstDate·fcstTime이 늘어난다.
    코드 분기 없이 adapter_params.pivot 설정만으로 흡수돼야 한다."""
    config = _StubConfig(
        {
            "endpoint": "getUltraSrtFcst",
            "root_key": "response.body.items.item",
            "pivot": {"key": "category", "value": "fcstValue"},
            "grids": [[60, 127]],
        }
    )
    chunk = _body(
        items=[
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "fcstDate": "20260812",
                "fcstTime": "1500",
                "nx": 60,
                "ny": 127,
                "category": "T1H",
                "fcstValue": "30.1",
            },
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "fcstDate": "20260812",
                "fcstTime": "1500",
                "nx": 60,
                "ny": 127,
                "category": "REH",
                "fcstValue": "40",
            },
        ]
    )

    rows = KmaApiHubAdapter.normalize([chunk], config)

    assert rows == [
        {
            "baseDate": "20260812",
            "baseTime": "1400",
            "fcstDate": "20260812",
            "fcstTime": "1500",
            "nx": 60,
            "ny": 127,
            "T1H": "30.1",
            "REH": "40",
        }
    ]


def test_normalize_tolerates_grid_missing_from_chunk_list():
    chunk_without_items = _body(items=[])
    chunk_with_items = _body(
        items=[
            {
                "baseDate": "20260812",
                "baseTime": "1400",
                "nx": 60,
                "ny": 127,
                "category": "T1H",
                "obsrValue": "31.6",
            }
        ]
    )

    rows = KmaApiHubAdapter.normalize(
        [chunk_without_items, chunk_with_items], _config()
    )

    assert rows == [
        {"baseDate": "20260812", "baseTime": "1400", "nx": 60, "ny": 127, "T1H": "31.6"}
    ]


def test_classify_result_code():
    assert _classify_result_code("00") is None
    assert _classify_result_code("03") is FetchErrorKind.TRANSIENT
    assert _classify_result_code("04") is FetchErrorKind.TRANSIENT
    assert _classify_result_code("20") is FetchErrorKind.FATAL
    assert _classify_result_code("33") is FetchErrorKind.FATAL
    assert _classify_result_code("99") is FetchErrorKind.PERMANENT
    assert _classify_result_code(None) is FetchErrorKind.PERMANENT


def test_adjust_base_time_hourly():
    # 40분 전에는 이전 시각 (14:39 -> 13:00)
    dt = datetime(2026, 8, 12, 14, 39, tzinfo=KST)
    assert adjust_base_time(dt, "hourly") == datetime(2026, 8, 12, 13, 0, tzinfo=KST)

    # 40분 이후는 현재 시각 (14:40 -> 14:00)
    dt = datetime(2026, 8, 12, 14, 40, tzinfo=KST)
    assert adjust_base_time(dt, "hourly") == datetime(2026, 8, 12, 14, 0, tzinfo=KST)


def test_adjust_base_time_half_hourly():
    # 30분 전에는 이전 시각 30분 (14:29 -> 13:30)
    dt = datetime(2026, 8, 12, 14, 29, tzinfo=KST)
    assert adjust_base_time(dt, "half_hourly") == datetime(
        2026, 8, 12, 13, 30, tzinfo=KST
    )

    # 30분 이후는 현재 시각 30분 (14:30 -> 14:30)
    dt = datetime(2026, 8, 12, 14, 30, tzinfo=KST)
    assert adjust_base_time(dt, "half_hourly") == datetime(
        2026, 8, 12, 14, 30, tzinfo=KST
    )


def test_adjust_base_time_ten_minutely():
    # 10분 단위로 내림만 한다(발표 지연 보정 없음) — freshness 전용
    dt = datetime(2026, 8, 12, 14, 37, tzinfo=KST)
    assert adjust_base_time(dt, "ten_minutely") == datetime(
        2026, 8, 12, 14, 30, tzinfo=KST
    )

    dt = datetime(2026, 8, 12, 14, 0, tzinfo=KST)
    assert adjust_base_time(dt, "ten_minutely") == datetime(
        2026, 8, 12, 14, 0, tzinfo=KST
    )


def test_adjust_base_time_thirty_minutely():
    # 30분 단위로 내림만 한다(시간당 2슬롯: :00, :30) — half_hourly(시간당
    # 1슬롯, 매시 30분)와 다르다
    dt = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
    assert adjust_base_time(dt, "thirty_minutely") == datetime(
        2026, 8, 12, 14, 0, tzinfo=KST
    )

    dt = datetime(2026, 8, 12, 14, 37, tzinfo=KST)
    assert adjust_base_time(dt, "thirty_minutely") == datetime(
        2026, 8, 12, 14, 30, tzinfo=KST
    )


def test_adjust_base_time_vilage_fcst():
    # 02, 05, 08, 11, 14, 17, 20, 23시 + 10분 발표
    # 05:09 -> 02:00
    dt = datetime(2026, 8, 12, 5, 9, tzinfo=KST)
    assert adjust_base_time(dt, "vilage_fcst") == datetime(
        2026, 8, 12, 2, 0, tzinfo=KST
    )

    # 05:10 -> 05:00
    dt = datetime(2026, 8, 12, 5, 10, tzinfo=KST)
    assert adjust_base_time(dt, "vilage_fcst") == datetime(
        2026, 8, 12, 5, 0, tzinfo=KST
    )

    # 01:00 -> 전날 23:00
    dt = datetime(2026, 8, 12, 1, 0, tzinfo=KST)
    assert adjust_base_time(dt, "vilage_fcst") == datetime(
        2026, 8, 11, 23, 0, tzinfo=KST
    )
