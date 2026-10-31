"""#6 서울 열린데이터광장 어댑터 테스트: 페이지네이션, RESULT.CODE 분류, normalize."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

import httpx
import pytest

from adapters.base import FetchErrorKind, Window, fetch_with_rounds
from adapters.seoul_openapi import NaturalKeyCardinalityError, SeoulOpenApiAdapter
from core.kst import KST



class _StubConfig:
    """어댑터가 읽는 최소 소스 설정을 제공한다."""

    def __init__(self, adapter_params, natural_key=None):
        """adapter params와 선택적 자연키를 보관한다."""
        self.adapter_params = adapter_params
        self.natural_key = natural_key


def _config(page_size=2):
    return _StubConfig(
        {
            "service": "bikeList",
            "page_size": page_size,
            "root_key": "rentBikeStatus.row",
        }
    )


def _probe_config(
    page_size=2,
    max_probe_pages=10,
    natural_key=("stationId",),
):
    """bikeList의 행 수 기반 probe pagination 설정을 만든다."""
    return _StubConfig(
        {
            "service": "bikeList",
            "page_size": page_size,
            "root_key": "rentBikeStatus.row",
            "pagination": "probe_until_empty",
            "max_probe_pages": max_probe_pages,
        },
        natural_key=natural_key,
    )


class _ProbeRoundConfig(_StubConfig):
    """라운드 테스트에 fetch budget까지 제공하는 최소 설정 더블."""

    def effective_fetch_budget(self):
        """테스트 중 만료되지 않는 fetch budget을 반환한다."""
        return timedelta(hours=1)


def _probe_round_config(page_size=2, max_probe_pages=10):
    """probe 설정과 fetch budget을 함께 가진 라운드 테스트 설정을 만든다."""
    config = _probe_config(page_size, max_probe_pages)
    return _ProbeRoundConfig(config.adapter_params, natural_key=config.natural_key)


def _body(code="INFO-000", total=3, rows=None):
    return json.dumps(
        {
            "rentBikeStatus": {
                "list_total_count": total,
                "RESULT": {"CODE": code, "MESSAGE": "ok"},
                "row": rows if rows is not None else [],
            }
        }
    ).encode()


def _empty_body():
    """페이지 범위를 넘겼을 때의 서울 API 최상단 INFO-200 응답을 만든다."""
    return json.dumps(
        {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
    ).encode()


def _probe_handler(rows, page_size, *, calls=None, fail_once=None, raw_total=None):
    """현재 페이지 행 수를 total로 주고 끝에서 INFO-200을 주는 bikeList를 흉내낸다."""
    attempts: dict[int, int] = {}

    def handler(request):
        url = str(request.url)
        if calls is not None:
            calls.append(url)
        start, end = (int(value) for value in re.search(r"/(\d+)/(\d+)/", url).groups())
        attempts[start] = attempts.get(start, 0) + 1
        if fail_once == start and attempts[start] == 1:
            return httpx.Response(500, content=b"temporary")
        page_rows = rows[start - 1 : end]
        if not page_rows:
            return httpx.Response(200, content=_empty_body())
        page_total = len(page_rows) if raw_total is None else raw_total
        return httpx.Response(200, content=_body(total=page_total, rows=page_rows))

    return handler


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "secret-key-123")


def test_fetch_paginates_until_total_is_covered():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if "/1/2/" in str(request.url):
            return httpx.Response(
                200, content=_body(total=3, rows=[{"a": "1"}, {"a": "2"}])
            )
        if "/3/3/" in str(request.url):
            return httpx.Response(200, content=_body(total=3, rows=[{"a": "3"}]))
        raise AssertionError(f"unexpected url: {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client)
    )

    assert [r.key for r in results] == ["page-00001-00002", "page-00003-00003"]
    assert results[0].expected_total == 3
    assert results[1].expected_total is None  # 첫 조각에만 싣는다
    assert all(r.error is None for r in results)
    assert len(calls) == 2


def test_fetch_masks_api_key_from_chunk_keys():
    def handler(request):
        return httpx.Response(200, content=_body(total=1, rows=[{"a": "1"}]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client)
    )

    for r in results:
        assert "secret-key-123" not in r.key


def test_fetch_skips_keys_already_collected(monkeypatch):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=_body(total=3, rows=[{"a": "3"}]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        SeoulOpenApiAdapter.fetch(
            _config(page_size=2),
            window=None,
            client=client,
            skip=frozenset({"page-00001-00002"}),
            expected_total=3,
        )
    )

    assert [r.key for r in results] == ["page-00003-00003"]
    assert len(calls) == 1  # page1은 호출되지 않는다


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("INFO-100", FetchErrorKind.FATAL),
        ("ERROR-500", FetchErrorKind.TRANSIENT),
        ("ERROR-336", FetchErrorKind.PERMANENT),
    ],
)
def test_fetch_classifies_result_code_errors(code, expected):
    def handler(request):
        return httpx.Response(200, content=_body(code=code))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert len(results) == 1
    assert results[0].error is expected


def test_fetch_treats_empty_result_info_200_as_success():
    def handler(request):
        return httpx.Response(200, content=_body(code="INFO-200", total=0, rows=[]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert len(results) == 1
    assert results[0].error is None


def test_fetch_yields_raw_response_unmodified():
    raw = _body(total=1, rows=[{"a": "1"}])

    def handler(request):
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert results[0].payload == raw


@pytest.mark.parametrize("raw_total", ["invalid", -1, True, 1.5])
def test_fetch_rejects_present_malformed_total(raw_total):
    """total 기반 pagination은 malformed total을 unknown으로 축소하지 않는다."""
    raw = _body(total=raw_total, rows=[{"a": "1"}])

    def handler(request):
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert len(results) == 1
    assert results[0].payload is None
    assert results[0].error is FetchErrorKind.PERMANENT


class TestProbeUntilEmptyPagination:
    """페이지별 count가 전체가 아닌 bikeList의 탐색 계약을 검증한다."""

    def test_ignores_page_totals_and_continues_after_short_page(self):
        """짧은 페이지 뒤에도 명시적 빈 row를 확인할 때까지 계속 조회한다."""
        responses = {
            "/1/2/": _body(
                total=2, rows=[{"stationId": "ST-1"}, {"stationId": "ST-2"}]
            ),
            "/3/4/": _body(total=1, rows=[{"stationId": "ST-3"}]),
            "/5/6/": _body(total=1, rows=[{"stationId": "ST-4"}]),
            "/7/8/": _body(total=0, rows=[]),
        }
        calls = []

        def handler(request):
            url = str(request.url)
            calls.append(url)
            return httpx.Response(
                200,
                content=next(
                    payload for marker, payload in responses.items() if marker in url
                ),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        results = list(
            SeoulOpenApiAdapter.fetch(_probe_config(), window=None, client=client)
        )

        assert [result.key for result in results] == [
            "page-00001-00002",
            "page-00003-00004",
            "page-00005-00006",
            "page-00007-00008",
        ]
        assert [result.expected_total for result in results] == [None, None, None, 4]
        assert len(calls) == 4

    def test_missing_total_does_not_stop_probe(self):
        """list_total_count가 없는 비어 있지 않은 페이지도 정상 raw로 보존한다."""
        first = json.dumps(
            {
                "rentBikeStatus": {
                    "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
                    "row": [{"stationId": "ST-1"}],
                }
            }
        ).encode()
        empty = _body(total=0, rows=[])
        calls = []

        def handler(request):
            calls.append(str(request.url))
            return httpx.Response(200, content=first if len(calls) == 1 else empty)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        results = list(
            SeoulOpenApiAdapter.fetch(_probe_config(), window=None, client=client)
        )

        assert results[0].payload == first
        assert results[-1].expected_total == 1
        assert len(calls) == 2

    def test_malformed_page_total_does_not_stop_probe(self):
        """probe source는 신뢰하지 않는 page total 형식과 무관하게 sentinel까지 간다."""
        first = _body(total="invalid", rows=[{"stationId": "ST-1"}])
        empty = _body(total=-1, rows=[])
        calls = []

        def handler(request):
            calls.append(str(request.url))
            return httpx.Response(200, content=first if len(calls) == 1 else empty)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        results = list(
            SeoulOpenApiAdapter.fetch(_probe_config(), window=None, client=client)
        )

        assert [result.error for result in results] == [None, None]
        assert results[-1].expected_total == 1
        assert len(calls) == 2

    def test_exact_multiple_requires_info_200_sentinel(self):
        """마지막 데이터 페이지가 꽉 차도 다음 INFO-200까지 확인한다."""
        sentinel = json.dumps(
            {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
        ).encode()
        calls = []

        def handler(request):
            url = str(request.url)
            calls.append(url)
            if "/1/2/" in url:
                return httpx.Response(
                    200,
                    content=_body(
                        total=2,
                        rows=[{"stationId": "ST-1"}, {"stationId": "ST-2"}],
                    ),
                )
            if "/3/4/" in url:
                return httpx.Response(
                    200,
                    content=_body(
                        total=2,
                        rows=[{"stationId": "ST-3"}, {"stationId": "ST-4"}],
                    ),
                )
            return httpx.Response(200, content=sentinel)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        results = list(
            SeoulOpenApiAdapter.fetch(_probe_config(), window=None, client=client)
        )

        assert [result.key for result in results] == [
            "page-00001-00002",
            "page-00003-00004",
            "page-00005-00006",
        ]
        assert results[-1].payload == sentinel
        assert results[-1].expected_total == 4
        assert len(calls) == 3

    def test_duplicate_natural_key_fails_cardinality_validation(self):
        """raw 행 수보다 고유 stationId 수가 작으면 serving 입력 생성을 거부한다."""
        chunk = _body(
            total=2,
            rows=[{"stationId": "ST-1"}, {"stationId": "ST-1"}],
        )

        with pytest.raises(NaturalKeyCardinalityError, match=r"rows=2, unique=1"):
            SeoulOpenApiAdapter.normalize([chunk], _probe_config())

    @pytest.mark.parametrize("station_id", [None, "", "   "])
    def test_missing_natural_key_fails_before_silver(self, station_id):
        """identity 결손 행은 nullable 비식별 필드와 달리 snapshot을 거부한다."""
        chunk = _body(total=1, rows=[{"stationId": station_id, "stationName": "이름"}])

        with pytest.raises(NaturalKeyCardinalityError, match=r"natural_key.*비어"):
            SeoulOpenApiAdapter.normalize([chunk], _probe_config())

    def test_nullable_fields_and_large_counts_remain_raw(self):
        """identity 외 nullable 값과 큰 비음수 count를 어댑터가 바꾸지 않는다."""
        row = {
            "stationId": "ST-1",
            "stationName": None,
            "rackTotCnt": "0",
            "parkingBikeTotCnt": "500000",
            "shared": "500000",
            "stationLatitude": "37.0",
            "stationLongitude": "127.5",
        }
        raw = _body(total=1, rows=[row])

        normalized = SeoulOpenApiAdapter.normalize([raw], _probe_config())

        assert normalized == [row]
        assert json.loads(raw)["rentBikeStatus"]["row"][0] == row


def test_population_fetch_uses_configured_poi_range_and_does_not_stop_at_gap():
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "root_key_literal": True,
            "poi_start": 117,
            "poi_end": 121,
        }
    )
    calls = []

    def handler(request):
        calls.append(str(request.url))
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        if poi_id == "POI119":
            body = {"RESULT": {"RESULT.CODE": "INFO-200"}}
        else:
            body = {
                "RESULT": {"RESULT.CODE": "INFO-000"},
                "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
            }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert SeoulOpenApiAdapter.planned_parts(config, window=None) == frozenset(
        f"poi-POI{i:03d}" for i in range(117, 122)
    )
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert [result.key for result in results] == [
        "poi-POI117",
        "poi-POI118",
        "poi-POI119",
        "poi-POI120",
        "poi-POI121",
    ]
    # POI 범위는 기대 row 수가 아니다. INFO-200도 정상 성공 조각이므로 pipeline이
    # 조각 기준 completeness를 쓰도록 expected_total은 전달하지 않는다.
    assert all(result.expected_total is None for result in results)
    assert all(result.error is None for result in results)
    assert any("/POI121/" in url for url in calls)


def test_population_fetch_excludes_configured_poi_gaps():
    """공식 POI 코드의 결번은 계획과 실제 HTTP 요청 모두에서 제외한다."""
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "root_key_literal": True,
            "poi_start": 20,
            "poi_end": 23,
            "poi_exclude": [22],
        }
    )
    calls = []

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        calls.append(poi_id)
        body = {
            "RESULT": {"RESULT.CODE": "INFO-000"},
            "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
        }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    planned = SeoulOpenApiAdapter.planned_parts(config, window=None)
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert planned == {
        "poi-POI020",
        "poi-POI021",
        "poi-POI023",
    }
    assert [result.key for result in results] == [
        "poi-POI020",
        "poi-POI021",
        "poi-POI023",
    ]
    assert calls == ["POI020", "POI021", "POI023"]


def test_population_fetch_uses_exact_dynamic_poi_codes_without_legacy_range():
    """S3 Master에서 주입한 코드는 legacy 범위 없이 계획과 요청에 똑같이 쓰인다."""
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "root_key_literal": True,
            "poi_codes": ("POI001", "POI132"),
        }
    )
    calls = []

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        calls.append(poi_id)
        body = {
            "RESULT": {"RESULT.CODE": "INFO-000"},
            "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
        }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    planned = SeoulOpenApiAdapter.planned_parts(config, window=None)
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert planned == {"poi-POI001", "poi-POI132"}
    assert [result.key for result in results] == ["poi-POI001", "poi-POI132"]
    assert calls == ["POI001", "POI132"]


def test_population_fetch_requests_pois_concurrently_and_preserves_order():
    import threading
    import time

    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "root_key_literal": True,
            "poi_start": 1,
            "poi_end": 8,
            "concurrency": 4,
        }
    )
    lock = threading.Lock()
    live = {"now": 0, "max": 0}

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        with lock:
            live["now"] += 1
            live["max"] = max(live["max"], live["now"])
        try:
            time.sleep(0.05)
            body = {
                "RESULT": {"RESULT.CODE": "INFO-000"},
                "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
            }
            return httpx.Response(200, content=json.dumps(body).encode())
        finally:
            with lock:
                live["now"] -= 1

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert live["max"] > 1
    assert [result.key for result in results] == [
        f"poi-POI{i:03d}" for i in range(1, 9)
    ]
    assert all(result.error is None for result in results)


def test_population_fetch_honors_skip_under_concurrency():
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "root_key_literal": True,
            "poi_start": 1,
            "poi_end": 4,
            "concurrency": 2,
        }
    )
    calls = []

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        calls.append(poi_id)
        body = {
            "RESULT": {"RESULT.CODE": "INFO-000"},
            "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
        }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        SeoulOpenApiAdapter.fetch(
            config,
            window=None,
            client=client,
            skip=frozenset({"poi-POI002", "poi-POI003"}),
        )
    )

    assert [result.key for result in results] == ["poi-POI001", "poi-POI004"]
    assert set(calls) == {"POI001", "POI004"}


def test_normalize_concatenates_rows_across_chunks():
    chunk1 = _body(total=3, rows=[{"a": "1"}, {"a": "2"}])
    chunk2 = _body(total=3, rows=[{"a": "3"}])

    rows = SeoulOpenApiAdapter.normalize([chunk1, chunk2], _config())

    assert rows == [{"a": "1"}, {"a": "2"}, {"a": "3"}]


def test_normalize_tolerates_missing_row_key():
    chunk_without_rows = json.dumps(
        {"rentBikeStatus": {"RESULT": {"CODE": "INFO-200"}}}
    ).encode()
    chunk_with_rows = _body(total=1, rows=[{"a": "1"}])

    rows = SeoulOpenApiAdapter.normalize(
        [chunk_without_rows, chunk_with_rows], _config()
    )

    assert rows == [{"a": "1"}]


class TestPathSuffixTemplate:
    """`path_suffix`는 소스마다 다른 URL 꼬리를 yaml로 표현하는 장치다.

    `tbCycleRentData`는 `/{날짜}/{시}`를 받는다. 그런데 윈도우 시작 시각의 시(hour)를
    그대로 쓰면 매시 마지막 5분이 통째로 누락된다 — 19:00 윈도우가 19시대를 요청해
    버려서 18:55~19:00 대여를 아무도 가져가지 않는다. 직전 순간(`window_last`)의 시를
    쓰면 19:00 윈도우가 18시대를 한 번 더 완결시키고 19:05부터 19시대로 넘어간다.
    """

    def _capture(self, params, window):
        calls = []

        def handler(request):
            calls.append(str(request.url))
            return httpx.Response(200, content=_body(total=1, rows=[{"a": "1"}]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        list(
            SeoulOpenApiAdapter.fetch(_StubConfig(params), window=window, client=client)
        )
        return calls

    def _params(self, suffix):
        return {
            "service": "tbCycleRentData",
            "page_size": 1000,
            "root_key": "rentBikeStatus.row",
            "path_suffix": suffix,
        }

    @pytest.mark.parametrize(
        ("window_start", "suffix", "expected_tail"),
        [
            ((18, 18, 55), "/{window_start:%Y-%m-%d}/{window_start:%H}", "/2026-08-18/18/"),
            ((18, 18, 55), "/{window_end:%H}", "/19/"),
            ((18, 19, 0), "/{window_last:%Y-%m-%d}/{window_last:%H}", "/2026-08-18/18/"),
            ((18, 19, 5), "/{window_last:%H}", "/19/"),
            ((19, 0, 0), "/{window_last:%Y-%m-%d}/{window_last:%H}", "/2026-08-18/23/"),
            ((18, 4, 5), "/{window_last:%H}", "/04/"),
        ],
        ids=[
            "window_start",
            "window_end",
            "window_last_is_instant_before_window",
            "window_last_keeps_mid_hour_window",
            "window_last_rolls_date_back_at_midnight",
            "hour_is_zero_padded",
        ],
    )
    def test_suffix_renders_window_fields(self, window_start, suffix, expected_tail):
        day, hour, minute = window_start
        start = datetime(2026, 8, day, hour, minute, tzinfo=KST)
        window = Window(window_start=start, window_end=start + timedelta(minutes=5))

        calls = self._capture(self._params(suffix), window)

        assert calls[0].endswith(expected_tail)


# ---------------------------------------------------------------------------
# 페이지 병렬 조회
#
# 실측(2026-08-19): tbCycleRentData의 1000행 페이지 응답이 시점에 따라 0.6~7.2초로
# 흔들린다. 피크 시간대(18시)는 17페이지라 순차로 72.7초가 걸렸고 fetch 예산은
# 150초다. 같은 페이지 집합을 스레드풀로 받으면 4워커 17.0초(4.3배),
# 8워커 10.5초(6.9배)였고 행 수는 정확히 일치했다.
#
# 순서를 유지하는 이유: 완료 순서대로 내보내면 조각 키 순서가 흔들려 기존 계약
# (그리고 이 파일의 다른 테스트들)이 깨진다. 앞쪽 페이지를 기다리는 동안에도
# 뒤쪽 요청은 이미 나가 있으므로 전체 소요는 병렬과 같다.
# ---------------------------------------------------------------------------


def _concurrent_config(page_size=2, concurrency=4):
    return _StubConfig(
        {
            "service": "bikeList",
            "page_size": page_size,
            "root_key": "rentBikeStatus.row",
            "concurrency": concurrency,
        }
    )


def _paged_handler(
    total, page_size, *, delay=0.0, overrides=None, calls=None, live=None
):
    """total건을 page_size로 나눠 응답하는 핸들러. delay로 지연을 흉내낸다.

    `live`를 주면 동시 진행 중인 요청 수의 최댓값을 `live["max"]`에 기록한다.
    """
    import threading
    import time

    lock = threading.Lock()
    state = {"now": 0}

    def handler(request):
        url = str(request.url)
        if calls is not None:
            with lock:
                calls.append(url)
        if live is not None:
            with lock:
                state["now"] += 1
                live["max"] = max(live.get("max", 0), state["now"])
        try:
            if delay:
                time.sleep(delay)
            for marker, response in (overrides or {}).items():
                if marker in url:
                    return response
            import re

            start, end = (int(x) for x in re.search(r"/(\d+)/(\d+)/", url).groups())
            rows = [{"a": str(i)} for i in range(start, min(end, total) + 1)]
            return httpx.Response(200, content=_body(total=total, rows=rows))
        finally:
            if live is not None:
                with lock:
                    state["now"] -= 1

    return handler


class TestConcurrentPagination:
    def test_pages_are_requested_concurrently(self):
        """지연 0.2초 × 8페이지. 순차면 1.6초, 4워커면 1페이지(순차 발견) + 2배치."""
        import time

        live = {}
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(8, 1, delay=0.2, live=live))
        )
        started = time.monotonic()
        results = list(
            SeoulOpenApiAdapter.fetch(
                _concurrent_config(page_size=1, concurrency=4),
                window=None,
                client=client,
            )
        )
        elapsed = time.monotonic() - started

        assert len(results) == 8
        assert all(r.error is None for r in results)
        assert live["max"] > 1, "요청이 하나씩만 나갔다 — 병렬이 아니다"
        assert elapsed < 1.2, f"순차(1.6s)보다 빨라야 한다: {elapsed:.2f}s"

    def test_page_order_is_preserved(self):
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(8, 1, delay=0.05))
        )
        results = list(
            SeoulOpenApiAdapter.fetch(
                _concurrent_config(page_size=1, concurrency=4),
                window=None,
                client=client,
            )
        )

        assert [r.key for r in results] == [
            f"page-{i:05d}-{i:05d}" for i in range(1, 9)
        ]
        assert results[0].expected_total == 8
        assert all(r.expected_total is None for r in results[1:])

    def test_default_is_sequential_so_other_sources_are_unchanged(self):
        """concurrency를 선언하지 않은 소스는 동작이 그대로여야 한다."""
        live = {}
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(6, 1, delay=0.02, live=live))
        )
        results = list(
            SeoulOpenApiAdapter.fetch(_config(page_size=1), window=None, client=client)
        )

        assert len(results) == 6
        assert live["max"] == 1, "concurrency 미선언 소스는 순차여야 한다"

    def test_skip_is_honored_under_concurrency(self):
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(4, 1, calls=calls))
        )
        results = list(
            SeoulOpenApiAdapter.fetch(
                _concurrent_config(page_size=1, concurrency=4),
                window=None,
                client=client,
                skip=frozenset({"page-00002-00002", "page-00003-00003"}),
                expected_total=4,
            )
        )

        assert [r.key for r in results] == ["page-00001-00001", "page-00004-00004"]
        assert not any("/2/2/" in c or "/3/3/" in c for c in calls)

    def test_transient_on_one_page_reports_only_that_page(self):
        client = httpx.Client(
            transport=httpx.MockTransport(
                _paged_handler(
                    4,
                    1,
                    overrides={"/3/3/": httpx.Response(500, content=b"boom")},
                )
            )
        )
        results = list(
            SeoulOpenApiAdapter.fetch(
                _concurrent_config(page_size=1, concurrency=4),
                window=None,
                client=client,
            )
        )

        by_key = {r.key: r for r in results}
        assert by_key["page-00003-00003"].error is FetchErrorKind.TRANSIENT
        assert by_key["page-00001-00001"].error is None
        assert by_key["page-00004-00004"].error is None

    def test_fatal_stops_the_whole_fetch(self):
        client = httpx.Client(
            transport=httpx.MockTransport(
                _paged_handler(
                    6,
                    1,
                    overrides={
                        "/3/3/": httpx.Response(
                            200, content=_body(code="INFO-100", total=6)
                        )
                    },
                )
            )
        )
        results = list(
            SeoulOpenApiAdapter.fetch(
                _concurrent_config(page_size=1, concurrency=4),
                window=None,
                client=client,
            )
        )

        assert results[-1].error is FetchErrorKind.FATAL
        assert results[-1].key == "page-00003-00003"
        keys = [r.key for r in results]
        assert "page-00004-00004" not in keys, "FATAL 이후 조각을 더 내보내면 안 된다"

    def test_abandoning_the_generator_does_not_block_on_the_pool(self):
        """fetch_with_rounds는 마감 시한을 넘기면 순회를 중단하고 제너레이터를 버린다.
        스레드풀을 `with`로 감싸면 shutdown(wait=True)가 되어 큐에 남은 페이지가
        끝날 때까지 블록되고, 마감 시한 방어가 무력화된다."""
        import time

        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(40, 1, delay=0.3))
        )
        iterator = SeoulOpenApiAdapter.fetch(
            _concurrent_config(page_size=1, concurrency=4), window=None, client=client
        )
        next(iterator)  # 1페이지만 받고 버린다

        started = time.monotonic()
        iterator.close()
        elapsed = time.monotonic() - started

        assert elapsed < 1.5, f"풀 종료가 큐를 기다렸다: {elapsed:.2f}s"


class TestTopLevelResultCode:
    """시작 인덱스가 total을 넘으면 서울 API는 래퍼 없이 최상단에 CODE만 준다
    (실측: `{"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}`).
    래퍼 안의 RESULT.CODE만 보면 None이 되어 PERMANENT로 오판한다."""

    def test_top_level_info_200_is_success_not_permanent(self):
        body = json.dumps(
            {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
        ).encode()
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=body)
            )
        )

        results = list(
            SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client)
        )

        assert [r.error for r in results] == [None]

    def test_top_level_error_code_is_still_classified(self):
        body = json.dumps({"CODE": "INFO-100", "MESSAGE": "인증키 오류"}).encode()
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=body)
            )
        )

        results = list(
            SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client)
        )

        assert results[0].error is FetchErrorKind.FATAL


class TestProbePagination:
    """전체 건수가 아닌 페이지 크기를 주는 bikeList 전용 탐색 계약을 검증한다."""

    def test_ignores_page_sized_total_and_stops_only_after_empty_probe(self):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 6)]
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(_probe_handler(rows, 2, calls=calls))
        )

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2), window=None, client=client
            )
        )

        data_results = [result for result in results if result.expected_total is None]
        terminal = [result for result in results if result.expected_total is not None]
        assert [result.key for result in data_results] == [
            "page-00001-00002",
            "page-00003-00004",
            "page-00005-00006",
        ]
        assert len(terminal) == 1
        assert terminal[0].key == "page-00007-00008"
        assert terminal[0].expected_total == 5
        assert terminal[0].payload is not None
        assert len(calls) == 4
        assert calls[-1].endswith("/7/8/")
        assert (
            SeoulOpenApiAdapter.normalize(
                [result.payload for result in data_results], _probe_config(page_size=2)
            )
            == rows
        )

    def test_exact_page_multiple_uses_terminal_position_as_total(self):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 5)]
        client = httpx.Client(transport=httpx.MockTransport(_probe_handler(rows, 2)))

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2), window=None, client=client
            )
        )

        assert [
            result.expected_total
            for result in results
            if result.expected_total is not None
        ] == [4]
        assert [result.key for result in results if result.expected_total is None] == [
            "page-00001-00002",
            "page-00003-00004",
        ]

    def test_does_not_parse_or_trust_list_total_count(self):
        rows = [{"stationId": "ST-1"}]
        client = httpx.Client(
            transport=httpx.MockTransport(
                _probe_handler(rows, 2, raw_total="not-an-integer")
            )
        )

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2), window=None, client=client
            )
        )

        assert [
            result.expected_total
            for result in results
            if result.expected_total is not None
        ] == [1]
        assert [result.error for result in results] == [None, None]

    def test_transient_last_data_page_is_retried_and_total_is_recovered(self):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 6)]
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(
                _probe_handler(rows, 2, calls=calls, fail_once=5)
            )
        )

        result = fetch_with_rounds(
            SeoulOpenApiAdapter.fetch,
            _probe_round_config(page_size=2),
            window=None,
            client=client,
            sleep_fn=lambda _seconds: None,
        )

        assert sorted(result.chunks) == [
            "page-00001-00002",
            "page-00003-00004",
            "page-00005-00006",
            "page-00007-00008",
        ]
        assert result.missing == {}
        assert result.expected_total == 5
        assert sum("/5/6/" in call for call in calls) == 2

    def test_transient_terminal_probe_recovers_total_without_rewriting_skipped_pages(
        self,
    ):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 5)]
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(
                _probe_handler(rows, 2, calls=calls, fail_once=5)
            )
        )

        result = fetch_with_rounds(
            SeoulOpenApiAdapter.fetch,
            _probe_round_config(page_size=2),
            window=None,
            client=client,
            sleep_fn=lambda _seconds: None,
        )

        assert sorted(result.chunks) == [
            "page-00001-00002",
            "page-00003-00004",
            "page-00005-00006",
        ]
        assert result.missing == {}
        assert result.expected_total == 4
        # 3~4 페이지의 두 번째 호출은 payload 재수집이 아니라 종료 위치 복구용이다.
        assert sum("/3/4/" in call for call in calls) == 2

    def test_terminal_recovery_rejects_shrunk_snapshot_behind_skipped_parts(self):
        """skipped Bronze보다 축소된 snapshot의 terminal을 새 total로 확정하지 않는다."""
        # 직전 라운드에는 1~4가 data page로 저장됐지만 terminal만 실패한 상태다.
        # 재시도 시 snapshot이 2행으로 줄면 /5/6과 복구용 /3/4가 모두 terminal이다.
        # 기존 4행 payload에 expected=2를 붙이면 혼합 snapshot이므로 transient여야 한다.
        rows = [{"stationId": "ST-1"}, {"stationId": "ST-2"}]
        client = httpx.Client(transport=httpx.MockTransport(_probe_handler(rows, 2)))

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2),
                window=None,
                client=client,
                skip=frozenset({"page-00001-00002", "page-00003-00004"}),
            )
        )

        assert len(results) == 1
        assert results[0].key == "page-00005-00006"
        assert results[0].error is FetchErrorKind.TRANSIENT
        assert results[0].expected_total is None
        assert results[0].payload is None

    def test_known_total_retries_only_missing_fixed_width_page(self):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 6)]
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(_probe_handler(rows, 2, calls=calls))
        )

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2),
                window=None,
                client=client,
                skip=frozenset({"page-00001-00002", "page-00005-00006"}),
                expected_total=5,
            )
        )

        assert [result.key for result in results if result.expected_total is None] == [
            "page-00003-00004"
        ]
        assert [
            result.expected_total
            for result in results
            if result.expected_total is not None
        ] == [5]
        assert len(calls) == 2
        assert calls[0].endswith("/3/4/")
        assert calls[1].endswith("/7/8/")

    def test_known_total_retry_collects_rows_appended_after_the_previous_round(self):
        """retry 사이에 snapshot 뒤로 늘어난 행도 terminal 재탐색으로 수집한다."""
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 8)]
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(_probe_handler(rows, 2, calls=calls))
        )

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2),
                window=None,
                client=client,
                skip=frozenset({"page-00001-00002", "page-00005-00006"}),
                expected_total=5,
            )
        )

        assert [result.key for result in results if result.expected_total is None] == [
            "page-00003-00004",
            "page-00007-00008",
        ]
        assert [
            result.expected_total
            for result in results
            if result.expected_total is not None
        ] == [7]
        assert calls[-1].endswith("/9/10/")

    def test_probe_limit_fails_instead_of_silently_truncating(self):
        rows = [{"stationId": f"ST-{index}"} for index in range(1, 6)]
        client = httpx.Client(transport=httpx.MockTransport(_probe_handler(rows, 2)))

        results = list(
            SeoulOpenApiAdapter.fetch(
                _probe_config(page_size=2, max_probe_pages=2),
                window=None,
                client=client,
            )
        )

        assert [result.key for result in results[:2]] == [
            "page-00001-00002",
            "page-00003-00004",
        ]
        assert results[-1].key == "page-00005-00006"
        assert results[-1].error is FetchErrorKind.PERMANENT
        assert results[-1].payload is None


class TestForecastFlatten:
    """citydata_ppltn의 `FCST_PPLTN` 중첩 배열이 슬롯 컬럼으로 펼쳐지는지 확인한다."""

    @staticmethod
    def _config():
        return _StubConfig(
            {
                "service": "citydata_ppltn",
                "page_size": 1000,
                "root_key": "SeoulRtd.citydata_ppltn",
                "root_key_literal": True,
                "flatten_forecast": True,
            }
        )

    @staticmethod
    def _chunk(row: dict) -> bytes:
        return json.dumps({"SeoulRtd.citydata_ppltn": [row]}).encode()

    @staticmethod
    def _forecast(hour: int, pop: int) -> dict:
        return {
            "FCST_TIME": f"2026-08-19 {hour:02d}:00",
            "FCST_CONGEST_LVL": "여유",
            "FCST_PPLTN_MIN": str(pop),
            "FCST_PPLTN_MAX": str(pop + 500),
        }

    def test_twelve_slots_become_flat_columns(self):
        row = {
            "AREA_CD": "POI001",
            "FCST_YN": "Y",
            "FCST_PPLTN": [self._forecast(10 + i, 1000 * i) for i in range(12)],
        }
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert len(rows) == 1
        assert rows[0]["FCST_1_TIME"] == "2026-08-19 10:00"
        assert rows[0]["FCST_1_PPLTN_MIN"] == "0"
        assert rows[0]["FCST_12_TIME"] == "2026-08-19 21:00"
        assert rows[0]["FCST_12_PPLTN_MAX"] == "11500"
        assert rows[0]["FCST_12_CONGEST_LVL"] == "여유"

    def test_nested_key_is_removed_so_parquet_only_sees_scalars(self):
        row = {"AREA_CD": "POI001", "FCST_PPLTN": [self._forecast(10, 100)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert "FCST_PPLTN" not in rows[0]
        assert all(not isinstance(v, (list, dict)) for v in rows[0].values())

    def test_slots_are_numbered_by_time_not_response_order(self):
        row = {
            "AREA_CD": "POI001",
            "FCST_PPLTN": [
                self._forecast(15, 300),
                self._forecast(13, 100),
                self._forecast(14, 200),
            ],
        }
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_1_TIME"] == "2026-08-19 13:00"
        assert rows[0]["FCST_2_TIME"] == "2026-08-19 14:00"
        assert rows[0]["FCST_3_TIME"] == "2026-08-19 15:00"

    def test_short_array_leaves_remaining_slots_absent(self):
        row = {
            "AREA_CD": "POI001",
            "FCST_PPLTN": [self._forecast(10 + i, 100) for i in range(5)],
        }
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_5_TIME"] == "2026-08-19 14:00"
        assert not [
            k for k in rows[0] if k.startswith(("FCST_6_", "FCST_7_", "FCST_12_"))
        ]

    def test_missing_forecast_array_keeps_other_columns(self):
        row = {"AREA_CD": "POI001", "FCST_YN": "N", "AREA_PPLTN_MIN": "100"}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0] == {"AREA_CD": "POI001", "FCST_YN": "N", "AREA_PPLTN_MIN": "100"}

    def test_entries_without_time_are_dropped(self):
        row = {
            "AREA_CD": "POI001",
            "FCST_PPLTN": [{"FCST_PPLTN_MIN": "100"}, self._forecast(11, 200)],
        }
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_1_TIME"] == "2026-08-19 11:00"
        assert "FCST_2_TIME" not in rows[0]

    def test_other_sources_are_untouched(self):
        config = _StubConfig(
            {"service": "bikeList", "page_size": 1000, "root_key": "rentBikeStatus.row"}
        )
        chunk = json.dumps(
            {
                "rentBikeStatus": {
                    "row": [{"stationId": "ST-1", "FCST_PPLTN": [{"x": 1}]}]
                }
            }
        ).encode()

        rows = SeoulOpenApiAdapter.normalize([chunk], config)

        assert rows[0]["FCST_PPLTN"] == [{"x": 1}]
