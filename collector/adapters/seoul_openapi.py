"""서울 열린데이터광장 공용 페이지네이션 규약 어댑터.

RESULT.CODE → 실패 범주:

| 코드 | 범주 | 근거 |
| --- | --- | --- |
| `INFO-000` | 성공 | |
| `INFO-200` (해당 데이터 없음) | 성공(빈 결과) | 정상 상황을 누락으로 세면 완결도가 왜곡된다 |
| `INFO-100` (인증키 오류) | `FATAL` | 모든 조각이 같은 키를 쓰므로 재시도가 무의미 |
| `ERROR-500` 계열 (서버 오류) | `TRANSIENT` | 라운드로 회복 가능 |
| 그 외 요청 오류 | `PERMANENT` | 그 조각만 누락 확정 |

pagination:

| 모드 | 소스 | 방식 |
| --- | --- | --- |
| `total` (기본) | 대부분 | 첫 페이지의 `list_total_count`로 페이지 목록을 만든다 |
| `probe_until_empty` | `bikeList` | `list_total_count`가 현재 페이지 행 수라 빈 페이지까지 순차 탐색 |
| (citydata_ppltn) | 실시간 인구 | 페이지 대신 POI 코드를 하나씩 요청 |

주의:
- 인증키(`SEOUL_OPENAPI_KEY`)는 URL 경로에 실리므로 로그에서 `logging_setup`이 가린다.
- 응답 필드는 전부 문자열이다. 캐스팅은 검증 엔진의 `types`가 맡는다.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import httpx
from core.forecast import POPULATION_FORECAST_SLOT_COUNT

from adapters.base import FetchErrorKind, FetchResult, adapter, run_concurrent

if TYPE_CHECKING:
    from collections.abc import Callable

    from config.schema import SourceConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"

_FATAL_CODES = {"INFO-100"}
_SUCCESS_CODES = {"INFO-000", "INFO-200"}

# citydata_ppltn의 `FCST_PPLTN`은 12개 예측 객체 배열이다. 검증 엔진은 스칼라만 다루므로
# 평탄한 슬롯 컬럼으로 펼친다. 슬롯 번호는 "n시간 후"가 아니며(20:55 관측의 첫 예측이
# 22:00), 소비자는 `FCST_n_TIME` 값으로 시각을 맞춘다.
_FCST_ARRAY_KEY = "FCST_PPLTN"
_FCST_SLOT_FIELDS = (
    "FCST_TIME",
    "FCST_CONGEST_LVL",
    "FCST_PPLTN_MIN",
    "FCST_PPLTN_MAX",
)


class NaturalKeyCardinalityError(ValueError):
    """원천 행 수와 유효한 고유 자연키 수가 일치하지 않을 때 발생한다."""


@dataclass(frozen=True, slots=True)
class _PageOutcome:
    """페이지(또는 POI) 하나의 조회 결과. 스레드풀 워커가 돌려주는 값이다."""

    payload: bytes | None = None
    error: FetchErrorKind | None = None
    total: int | None = None
    row_count: int | None = None
    terminal: bool = False


def _api_key() -> str:
    return os.environ["SEOUL_OPENAPI_KEY"]


def _classify(code: str | None) -> FetchErrorKind | None:
    """RESULT.CODE를 실패 범주로 매핑한다. None은 성공이다."""
    if not isinstance(code, str):
        return FetchErrorKind.PERMANENT
    if code in _SUCCESS_CODES:
        return None
    if code in _FATAL_CODES:
        return FetchErrorKind.FATAL
    if code.startswith("ERROR-5"):
        return FetchErrorKind.TRANSIENT
    return FetchErrorKind.PERMANENT


def _extract(body: dict, wrapper_key: str) -> dict:
    """서울 API 응답은 서비스명을 키로 감싸서 온다."""
    return body.get(wrapper_key, {})


def _result_code(body: dict, wrapper_key: str) -> str | None:
    """본문에서 RESULT.CODE를 꺼낸다.

    시작 인덱스가 `list_total_count`를 넘으면 래퍼 없이 최상단에 `CODE`만 온다
    (`{"CODE": "INFO-200", ...}`). 래퍼만 보면 PERMANENT로 오판한다.
    """
    wrapper = _extract(body, wrapper_key)
    if isinstance(wrapper, dict):
        code = wrapper.get("RESULT", {}).get("CODE")
        if isinstance(code, str):
            return code
    top_level = body.get("CODE")
    return top_level if isinstance(top_level, str) else None


def _page_rows(body: dict, wrapper_key: str, row_path: str) -> list | None:
    """설정된 wrapper와 row 경로에서 원천 행 배열을 꺼낸다."""
    node = _extract(body, wrapper_key)
    for segment in row_path.split(".") if row_path else []:
        if not isinstance(node, dict):
            return None
        node = node.get(segment)
    return node if isinstance(node, list) else None


def _page_key(start: int, end: int) -> str:
    """페이지 인덱스 범위로 조각 키를 만든다."""
    return f"page-{start:05d}-{end:05d}"


def _full_page_key(start: int, page_size: int) -> str:
    """clamp하지 않은 고정 폭 페이지 키를 만든다."""
    return _page_key(start, start + page_size - 1)


def _flatten_forecast(row: dict) -> dict:
    """`FCST_PPLTN` 배열을 `FCST_1_TIME`~`FCST_12_PPLTN_MAX` 슬롯 컬럼으로 펼친다.

    API가 시간순을 보장하지 않아 `FCST_TIME` 오름차순으로 슬롯을 매긴다. 값이 없는
    슬롯은 키를 만들지 않아 엔진이 결측으로 처리하게 둔다.
    """
    forecasts = row.pop(_FCST_ARRAY_KEY, None)
    if not isinstance(forecasts, list):
        return row

    datable = [
        f
        for f in forecasts
        if isinstance(f, dict) and isinstance(f.get("FCST_TIME"), str)
    ]
    for slot, forecast in enumerate(
        sorted(datable, key=lambda f: f["FCST_TIME"])[:POPULATION_FORECAST_SLOT_COUNT],
        start=1,
    ):
        for field in _FCST_SLOT_FIELDS:
            value = forecast.get(field)
            if value is not None:
                row[f"FCST_{slot}_{field.removeprefix('FCST_')}"] = value
    return row


def _request_json(
    client: httpx.Client, url: str
) -> tuple[bytes, dict] | FetchErrorKind:
    """URL을 요청해 JSON 객체로 파싱한다. 실패는 TRANSIENT로 반환한다."""
    try:
        response = client.get(url)
        response.raise_for_status()
        body = json.loads(response.content)
    except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError):
        return FetchErrorKind.TRANSIENT
    if not isinstance(body, dict):
        return FetchErrorKind.TRANSIENT
    return response.content, body


def _parse_total(value: object) -> int | None:
    """`list_total_count`를 음수가 아닌 정수로 읽는다."""
    if value is None:
        return None
    if type(value) is int and value >= 0:
        return value
    if type(value) is str and value.strip().isdigit():
        return int(value.strip())
    raise ValueError("list_total_count는 음수가 아닌 정수여야 합니다")


def _fetch_page(
    client: httpx.Client, url: str, wrapper_key: str, row_path: str
) -> _PageOutcome:
    """`total` 모드 페이지 하나를 받아 실패 범주까지 판정한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(error=result)
    content, body = result

    # 서울 API는 보통 HTTP 200으로 응답하고 논리적 성공/실패를 본문 코드에 담는다.
    code = _result_code(body, wrapper_key)
    category = _classify(code)
    if category is not None:
        return _PageOutcome(error=category)

    rows = _page_rows(body, wrapper_key, row_path)
    terminal = code == "INFO-200" or rows == []
    if rows is None and not terminal:
        return _PageOutcome(error=FetchErrorKind.PERMANENT)
    wrapper = _extract(body, wrapper_key)
    try:
        total = _parse_total(
            wrapper.get("list_total_count") if isinstance(wrapper, dict) else None
        )
    except ValueError:
        return _PageOutcome(error=FetchErrorKind.PERMANENT)
    return _PageOutcome(
        payload=content,
        total=total,
        row_count=len(rows) if rows is not None else 0,
        terminal=terminal,
    )


def _fetch_probe_page(client: httpx.Client, url: str, wrapper_key: str) -> _PageOutcome:
    """`list_total_count`를 무시하고 행 수와 종료 여부만 판정한다.

    INFO-000인데 row가 list가 아니면 빈 끝으로 오인해 수집을 조용히 자를 수 있으므로
    영구 조각 오류로 돌린다.
    """
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(error=result)
    content, body = result

    code = _result_code(body, wrapper_key)
    category = _classify(code)
    if category is not None:
        return _PageOutcome(error=category)
    if code == "INFO-200":
        return _PageOutcome(payload=content, row_count=0, terminal=True)

    wrapper = _extract(body, wrapper_key)
    rows = wrapper.get("row") if isinstance(wrapper, dict) else None
    if not isinstance(rows, list):
        return _PageOutcome(error=FetchErrorKind.PERMANENT)
    return _PageOutcome(payload=content, row_count=len(rows), terminal=not rows)


def _fetch_poi(client: httpx.Client, url: str) -> _PageOutcome:
    """실시간 인구 POI 하나를 받는다. 빈 POI(INFO-200)도 성공 조각으로 보존한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(error=result)
    content, body = result

    # citydata_ppltn은 결과 코드를 최상단 `RESULT.CODE` 키로 내려준다.
    code = body.get("RESULT.CODE") or body.get("RESULT", {}).get("RESULT.CODE")
    category = _classify(code)
    if category is not None:
        return _PageOutcome(error=category)
    return _PageOutcome(payload=content)


def _validate_natural_key(
    rows: list[dict], natural_key: tuple[str, ...] | None
) -> None:
    """모든 raw 행에 자연키가 있고 행 수만큼 고유한지 검증한다."""
    if natural_key is None:
        return

    identities: list[tuple[object, ...]] = []
    for index, row in enumerate(rows):
        identity = tuple(row.get(column) for column in natural_key)
        if any(
            value is None or (isinstance(value, str) and not value.strip())
            for value in identity
        ):
            columns = ", ".join(natural_key)
            raise NaturalKeyCardinalityError(
                f"raw row {index}의 natural_key({columns})가 비어 있습니다"
            )
        try:
            hash(identity)
        except TypeError as exc:
            raise NaturalKeyCardinalityError(
                f"raw row {index}의 natural_key 값은 scalar여야 합니다"
            ) from exc
        identities.append(identity)

    unique_count = len(set(identities))
    if unique_count != len(rows):
        raise NaturalKeyCardinalityError(
            "raw row count와 unique natural_key count가 다릅니다: "
            f"rows={len(rows)}, unique={unique_count}"
        )


def _minimum_total_covered_by_probe_parts(parts: frozenset[str]) -> int:
    """이미 저장된 probe page들이 보장하는 최소 row 위치를 반환한다.

    `page-N-M`이 저장됐다면 그 snapshot에는 적어도 N번째 row가 있었다. 재시도 중 더
    작은 total이 관측되면 서로 다른 snapshot을 섞는 것이므로 성공으로 확정할 수 없다.
    """
    starts: list[int] = []
    for key in parts:
        if not key.startswith("page-"):
            continue
        try:
            starts.append(int(key.split("-", 2)[1]))
        except (IndexError, ValueError):
            continue
    return max(starts, default=0)


def _recover_probe_total(
    *,
    page_start: int,
    page_size: int,
    skip: frozenset[str],
    client: httpx.Client,
    page_url: Callable[[int, int], str],
    wrapper_key: str,
) -> int | None:
    """종료 페이지만 재시도한 경우 직전 성공 페이지를 다시 읽어 실제 행 수를 복구한다.

    데이터 payload는 다시 yield하지 않으므로 Bronze가 중복 저장되지 않는다.
    """
    previous_start = page_start - page_size
    if previous_start < 1:
        return 0
    if _full_page_key(previous_start, page_size) not in skip:
        return None

    outcome = _fetch_probe_page(
        client, page_url(previous_start, previous_start + page_size - 1), wrapper_key
    )
    if outcome.error is not None or outcome.terminal or outcome.row_count is None:
        return None
    return previous_start - 1 + outcome.row_count


def _refetch_known_probe_pages(
    *,
    page_size: int,
    max_probe_pages: int,
    client: httpx.Client,
    page_url: Callable[[int, int], str],
    wrapper_key: str,
    skip: frozenset[str],
    expected_total: int,
) -> Iterator[FetchResult]:
    """이전 라운드가 확정한 total로 고정 폭 페이지를 복원해 실패 페이지만 다시 받는다."""
    page_count = (expected_total + page_size - 1) // page_size
    if page_count > max_probe_pages:
        start = max_probe_pages * page_size + 1
        yield FetchResult(
            key=_full_page_key(start, page_size), error=FetchErrorKind.PERMANENT
        )
        return

    pages = [
        (start, _full_page_key(start, page_size))
        for start in range(1, page_count * page_size + 1, page_size)
    ]
    for index, (start, key) in enumerate(pages):
        if key in skip:
            continue
        outcome = _fetch_probe_page(
            client, page_url(start, start + page_size - 1), wrapper_key
        )
        if outcome.terminal:
            # 확정한 범위 안에서 데이터 페이지가 사라졌으면 호출 사이에 snapshot이 바뀐
            # 것이다. 남은 범위를 transient로 남겨 부분 snapshot을 완결로 오인하지 않는다.
            for _, missing_key in pages[index:]:
                if missing_key not in skip:
                    yield FetchResult(key=missing_key, error=FetchErrorKind.TRANSIENT)
            return
        yield FetchResult(key=key, payload=outcome.payload, error=outcome.error)
        if outcome.error is FetchErrorKind.FATAL:
            return

    # 재시도 사이에 뒤로 늘어난 행을 놓치지 않게 기존 범위 다음 페이지부터 종료 페이지까지
    # 다시 탐색한다. 정적 snapshot이면 추가 호출 한 번으로 끝난다.
    page_start = page_count * page_size + 1
    last_data_start: int | None = None
    last_data_rows: int | None = None
    while True:
        page_number = ((page_start - 1) // page_size) + 1
        key = _full_page_key(page_start, page_size)
        outcome = _fetch_probe_page(
            client, page_url(page_start, page_start + page_size - 1), wrapper_key
        )
        if outcome.error is not None:
            yield FetchResult(key=key, error=outcome.error)
            return
        if outcome.terminal:
            total = (
                expected_total
                if last_data_start is None or last_data_rows is None
                else last_data_start - 1 + last_data_rows
            )
            yield FetchResult(key=key, payload=outcome.payload, expected_total=total)
            return
        if page_number > max_probe_pages:
            yield FetchResult(key=key, error=FetchErrorKind.PERMANENT)
            return
        yield FetchResult(key=key, payload=outcome.payload)
        last_data_start = page_start
        last_data_rows = outcome.row_count
        page_start += page_size


def _discover_probe_pages(
    *,
    page_size: int,
    max_probe_pages: int,
    client: httpx.Client,
    page_url: Callable[[int, int], str],
    wrapper_key: str,
    skip: frozenset[str],
) -> Iterator[FetchResult]:
    """끝을 모르는 상태에서 빈 종료 페이지까지 순차 탐색한다.

    종료 응답도 완결성 증거로 Bronze에 보존하며 실제 expected_total을 함께 전달한다.
    """
    page_start = 1
    last_data_start: int | None = None
    last_data_rows: int | None = None
    received_rows = 0
    skipped_data_page = False
    while True:
        page_number = ((page_start - 1) // page_size) + 1
        key = _full_page_key(page_start, page_size)

        # max_probe_pages개 뒤 한 번 더 호출해 빈 끝을 확인한다. 거기에도 데이터가 있으면
        # 설정 상한이 낮은 것이므로 조용히 자르지 않고 누락으로 실패시킨다.
        is_boundary_probe = page_number == max_probe_pages + 1
        if key in skip and not is_boundary_probe:
            skipped_data_page = True
            page_start += page_size
            continue

        outcome = _fetch_probe_page(
            client, page_url(page_start, page_start + page_size - 1), wrapper_key
        )
        if outcome.error is not None:
            # 실패 page 너머를 탐색하면 중간 공백을 둔 채 뒤의 종료 페이지를 완결 증거로
            # 오인할 수 있다. 다음 라운드가 이 page부터 이어받게 멈춘다.
            yield FetchResult(key=key, error=outcome.error)
            return

        if outcome.terminal:
            if page_start == 1:
                total = 0
            elif not skipped_data_page:
                total = received_rows
            elif (
                last_data_start == page_start - page_size and last_data_rows is not None
            ):
                total = last_data_start - 1 + last_data_rows
            else:
                total = _recover_probe_total(
                    page_start=page_start,
                    page_size=page_size,
                    skip=skip,
                    client=client,
                    page_url=page_url,
                    wrapper_key=wrapper_key,
                )
            if total is None or total < _minimum_total_covered_by_probe_parts(skip):
                # 더 작아진 snapshot의 종료 페이지를 확정하면 fetched>expected인 혼합
                # Bronze가 정상 완료될 수 있다. transient로 남겨 fail-closed한다.
                yield FetchResult(key=key, error=FetchErrorKind.TRANSIENT)
                return
            yield FetchResult(key=key, payload=outcome.payload, expected_total=total)
            return

        if is_boundary_probe:
            yield FetchResult(key=key, error=FetchErrorKind.PERMANENT)
            return

        yield FetchResult(key=key, payload=outcome.payload)
        last_data_start = page_start
        last_data_rows = outcome.row_count
        received_rows += outcome.row_count or 0
        page_start += page_size


def _fetch_total_pages(
    *,
    page_size: int,
    concurrency: int,
    client: httpx.Client,
    page_url: Callable[[int, int], str],
    wrapper_key: str,
    row_path: str,
    skip: frozenset[str],
    expected_total: int | None,
) -> Iterator[FetchResult]:
    """`list_total_count`로 페이지 목록을 만들어 받는다."""
    total = expected_total
    page_start = 1

    # total을 모르면 페이지 목록을 만들 수 없으므로 한 페이지만 순차로 받는다. 범위를
    # 넘겨 요청하면 래퍼 없는 INFO-200이 온다(`_result_code` 참고).
    if total is None:
        while _full_page_key(page_start, page_size) in skip:
            page_start += page_size
        key = _full_page_key(page_start, page_size)
        outcome = _fetch_page(
            client, page_url(page_start, page_start + page_size - 1), wrapper_key, row_path
        )
        if outcome.error is not None:
            yield FetchResult(key=key, error=outcome.error)
            return
        total = outcome.total or 0
        yield FetchResult(
            key=key,
            payload=outcome.payload,
            expected_total=total if page_start == 1 else None,
        )
        page_start += page_size

    # 마지막 페이지를 total로 clamp하므로 조각 키가 total에 의존한다. 라운드 사이에
    # total이 흔들리지 않게 pipeline이 expected_total을 되돌려준다.
    pages: list[tuple[str, int, int]] = []
    start = page_start
    while start <= total:
        end = min(start + page_size - 1, total)
        key = _page_key(start, end)
        if key not in skip:
            pages.append((key, start, end))
        start = end + 1

    def fetch_one_page(item: tuple[str, int, int]) -> _PageOutcome:
        _, start, end = item
        return _fetch_page(client, page_url(start, end), wrapper_key, row_path)

    for (key, start, _end), outcome in run_concurrent(
        pages, concurrency, fetch_one_page, "seoul-page"
    ):
        yield FetchResult(
            key=key,
            payload=outcome.payload,
            error=outcome.error,
            expected_total=total if start == 1 else None,
        )


def _population_poi_codes(params: dict) -> tuple[str, ...]:
    """이번 실행에 요청할 POI 코드를 반환한다.

    `poi_codes`는 CLI가 exact POI Master에서 읽어 주입한 값이다. 없으면 YAML의
    `poi_start`/`poi_end`/`poi_exclude` 범위를 쓴다. 두 입력의 형식은 각각
    `core.poi_master`와 `config.loader`가 검증한다.
    """
    if "poi_codes" in params:
        return params["poi_codes"]
    excluded = frozenset(params.get("poi_exclude", []))
    return tuple(
        f"POI{number:03d}"
        for number in range(params.get("poi_start", 1), params["poi_end"] + 1)
        if number not in excluded
    )


@adapter("seoul_openapi")
class SeoulOpenApiAdapter:
    """서울 열린데이터광장 공용 페이지네이션 규약 어댑터."""

    @staticmethod
    def _root_location(params: dict) -> tuple[str, str]:
        """설정에 따라 응답 wrapper 키와 그 아래 row 경로를 분리한다."""
        root_key = params["root_key"]
        if params.get("root_key_literal", False):
            return root_key, ""
        wrapper_key, _, row_path = root_key.partition(".")
        return wrapper_key, row_path

    @staticmethod
    def planned_parts(config: SourceConfig, window) -> frozenset[str] | None:
        """요청 전에 전체 키를 아는 POI 소스의 조각 계획을 반환한다."""
        params = config.adapter_params
        if params["service"] != "citydata_ppltn":
            return None
        return frozenset(f"poi-{code}" for code in _population_poi_codes(params))

    @staticmethod
    def fetch(
        config: SourceConfig,
        window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ) -> Iterator[FetchResult]:
        params = config.adapter_params
        service = params["service"]
        concurrency = int(params.get("concurrency", 1))
        wrapper_key, row_path = SeoulOpenApiAdapter._root_location(params)

        if service == "citydata_ppltn":
            # POI 범위 크기는 요청 조각 수이고 행 수가 아니므로 expected_total을 싣지
            # 않는다. 완결도는 조각 기준으로 계산된다.
            pois = [
                (f"poi-{code}", f"{_BASE_URL}/{_api_key()}/json/{service}/1/5/{code}/")
                for code in _population_poi_codes(params)
                if f"poi-{code}" not in skip
            ]
            for (key, _url), outcome in run_concurrent(
                pois, concurrency, lambda item: _fetch_poi(client, item[1]), "seoul-poi"
            ):
                yield FetchResult(key=key, payload=outcome.payload, error=outcome.error)
            return

        suffix = ""
        if params.get("path_suffix"):
            # 19:00 window가 window_start의 시를 쓰면 19시대를 요청해 18:55~19:00 데이터를
            # 아무도 가져가지 않는다. 직전 순간(`window_last`)으로 18시대를 완결시킨다.
            suffix = params["path_suffix"].format(
                window_start=window.window_start,
                window_end=window.window_end,
                window_last=window.window_start - timedelta(seconds=1),
            )

        def page_url(start: int, end: int) -> str:
            return f"{_BASE_URL}/{_api_key()}/json/{service}/{start}/{end}{suffix}/"

        page_size = params["page_size"]
        if params.get("pagination") != "probe_until_empty":
            yield from _fetch_total_pages(
                page_size=page_size,
                concurrency=concurrency,
                client=client,
                page_url=page_url,
                wrapper_key=wrapper_key,
                row_path=row_path,
                skip=skip,
                expected_total=expected_total,
            )
            return

        probe_args = {
            "page_size": int(page_size),
            "max_probe_pages": int(params["max_probe_pages"]),
            "client": client,
            "page_url": page_url,
            "wrapper_key": wrapper_key,
            "skip": skip,
        }
        if expected_total is None:
            yield from _discover_probe_pages(**probe_args)
        else:
            yield from _refetch_known_probe_pages(
                **probe_args, expected_total=expected_total
            )

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 순서대로 이어붙여 행 목록으로 만든다."""
        params = config.adapter_params
        wrapper_key, row_path = SeoulOpenApiAdapter._root_location(params)

        rows: list[dict] = []
        for chunk in chunks:
            rows.extend(_page_rows(json.loads(chunk), wrapper_key, row_path) or [])
        if params.get("flatten_forecast", False):
            rows = [_flatten_forecast(row) for row in rows if isinstance(row, dict)]
        _validate_natural_key(rows, config.natural_key)
        return rows
