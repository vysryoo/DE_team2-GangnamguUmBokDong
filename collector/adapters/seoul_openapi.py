"""서울 열린데이터광장 공용 페이지네이션 규약 어댑터.

서울 API는 대개 HTTP 200으로 응답하고 성공 여부를 본문 `RESULT.CODE`에 담는다.
`INFO-000`·`INFO-200`(데이터 없음)은 성공, `INFO-100`(인증키 오류)은 `FATAL`,
`ERROR-5xx`는 `TRANSIENT`, 그 외는 `PERMANENT`로 본다.

요청 방식은 세 가지다. 기본 `total`은 첫 페이지의 `list_total_count`로 페이지 목록을
만들고, `probe_until_empty`(`bikeList`)는 `list_total_count`가 현재 페이지 행 수라 빈
페이지까지 순차 탐색하며, `citydata_ppltn`(실시간 인구)은 POI 코드를 하나씩 요청한다.
인증키는 URL 경로에 실리므로 `logging_setup`이 로그에서 가린다.
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
    row_count: int = 0
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class _Pages:
    """서비스 하나의 페이지 요청 규칙. 고정 폭 페이지 키와 요청을 만든다."""

    client: httpx.Client
    url_for: Callable[[int, int], str]
    wrapper_key: str
    row_path: str
    size: int

    def key(self, start: int) -> str:
        """clamp하지 않은 고정 폭 페이지 키를 만든다."""
        return _page_key(start, start + self.size - 1)

    def fetch(self, start: int, end: int) -> _PageOutcome:
        """`total` 모드 페이지 하나를 받는다."""
        return _fetch_page(
            self.client, self.url_for(start, end), self.wrapper_key, self.row_path
        )

    def probe(self, start: int) -> _PageOutcome:
        """`list_total_count`를 무시하고 고정 폭 페이지 하나를 받는다."""
        return _fetch_page(
            self.client,
            self.url_for(start, start + self.size - 1),
            self.wrapper_key,
            self.row_path,
            read_total=False,
        )


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
    client: httpx.Client,
    url: str,
    wrapper_key: str,
    row_path: str,
    *,
    read_total: bool = True,
) -> _PageOutcome:
    """페이지 하나를 받아 실패 범주와 종료 여부를 판정한다.

    성공 코드인데 row가 list가 아니면 빈 끝으로 오인해 수집을 조용히 자를 수 있으므로
    영구 조각 오류로 돌린다. probe는 `list_total_count`를 쓰지 않아 `read_total=False`다.
    """
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(error=result)
    content, body = result

    code = _result_code(body, wrapper_key)
    category = _classify(code)
    if category is not None:
        return _PageOutcome(error=category)

    rows = _page_rows(body, wrapper_key, row_path)
    terminal = code == "INFO-200" or rows == []
    if rows is None and not terminal:
        return _PageOutcome(error=FetchErrorKind.PERMANENT)

    total = None
    if read_total:
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
        row_count=0 if terminal else len(rows),
        terminal=terminal,
    )


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


def _fetch_total_pages(
    pages: _Pages,
    concurrency: int,
    skip: frozenset[str],
    expected_total: int | None,
) -> Iterator[FetchResult]:
    """`list_total_count`로 페이지 목록을 만들어 받는다."""
    total = expected_total
    page_start = 1

    # total을 모르면 페이지 목록을 만들 수 없으므로 한 페이지만 순차로 받는다. 범위를
    # 넘겨 요청하면 래퍼 없는 INFO-200이 온다(`_result_code` 참고).
    if total is None:
        while pages.key(page_start) in skip:
            page_start += pages.size
        key = pages.key(page_start)
        outcome = pages.fetch(page_start, page_start + pages.size - 1)
        if outcome.error is not None:
            yield FetchResult(key=key, error=outcome.error)
            return
        total = outcome.total or 0
        yield FetchResult(
            key=key,
            payload=outcome.payload,
            expected_total=total if page_start == 1 else None,
        )
        page_start += pages.size

    # 마지막 페이지를 total로 clamp하므로 조각 키가 total에 의존한다. 라운드 사이에
    # total이 흔들리지 않게 pipeline이 expected_total을 되돌려준다.
    ranges: list[tuple[str, int, int]] = []
    start = page_start
    while start <= total:
        end = min(start + pages.size - 1, total)
        key = _page_key(start, end)
        if key not in skip:
            ranges.append((key, start, end))
        start = end + 1

    for (key, start, _end), outcome in run_concurrent(
        ranges, concurrency, lambda item: pages.fetch(item[1], item[2]), "seoul-page"
    ):
        yield FetchResult(
            key=key,
            payload=outcome.payload,
            error=outcome.error,
            expected_total=total if start == 1 else None,
        )


def _discover_probe_pages(
    pages: _Pages, max_probe_pages: int, skip: frozenset[str]
) -> Iterator[FetchResult]:
    """끝을 모르는 상태에서 빈 종료 페이지까지 순차 탐색한다.

    종료 응답도 완결성 증거로 Bronze에 보존하며 실제 expected_total을 함께 전달한다.
    max_probe_pages개 뒤 한 번 더 호출해 빈 끝을 확인하고, 거기에도 데이터가 있으면
    설정 상한이 낮은 것이므로 조용히 자르지 않고 누락으로 실패시킨다.
    """
    page_start = 1
    received_rows = 0
    skipped_data_page = False
    # 직전 페이지를 이번 라운드에 받았으면 그 페이지의 마지막 row 위치.
    previous_end: int | None = None
    while True:
        key = pages.key(page_start)
        is_boundary_probe = (page_start - 1) // pages.size == max_probe_pages
        if key in skip and not is_boundary_probe:
            skipped_data_page = True
            previous_end = None
            page_start += pages.size
            continue

        outcome = pages.probe(page_start)
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
            elif previous_end is not None:
                total = previous_end
            else:
                total = _recover_probe_total(pages, page_start, skip)
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
        received_rows += outcome.row_count
        previous_end = page_start - 1 + outcome.row_count
        page_start += pages.size


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
    pages: _Pages, page_start: int, skip: frozenset[str]
) -> int | None:
    """종료 페이지만 재시도한 경우 직전 성공 페이지를 다시 읽어 실제 행 수를 복구한다.

    데이터 payload는 다시 yield하지 않으므로 Bronze가 중복 저장되지 않는다.
    """
    previous_start = page_start - pages.size
    if previous_start < 1:
        return 0
    if pages.key(previous_start) not in skip:
        return None

    outcome = pages.probe(previous_start)
    if outcome.error is not None or outcome.terminal:
        return None
    return previous_start - 1 + outcome.row_count


def _refetch_known_probe_pages(
    pages: _Pages, max_probe_pages: int, skip: frozenset[str], expected_total: int
) -> Iterator[FetchResult]:
    """이전 라운드가 확정한 total로 고정 폭 페이지를 복원해 실패 페이지만 다시 받는다."""
    page_count = (expected_total + pages.size - 1) // pages.size
    if page_count > max_probe_pages:
        yield FetchResult(
            key=pages.key(max_probe_pages * pages.size + 1),
            error=FetchErrorKind.PERMANENT,
        )
        return

    starts = range(1, page_count * pages.size + 1, pages.size)
    for index, start in enumerate(starts):
        key = pages.key(start)
        if key in skip:
            continue
        outcome = pages.probe(start)
        if outcome.terminal:
            # 확정한 범위 안에서 데이터 페이지가 사라졌으면 호출 사이에 snapshot이 바뀐
            # 것이다. 남은 범위를 transient로 남겨 부분 snapshot을 완결로 오인하지 않는다.
            for missing_start in starts[index:]:
                if pages.key(missing_start) not in skip:
                    yield FetchResult(
                        key=pages.key(missing_start), error=FetchErrorKind.TRANSIENT
                    )
            return
        yield FetchResult(key=key, payload=outcome.payload, error=outcome.error)
        if outcome.error is FetchErrorKind.FATAL:
            return

    # 재시도 사이에 뒤로 늘어난 행을 놓치지 않게 기존 범위 다음 페이지부터 종료 페이지까지
    # 다시 탐색한다. 정적 snapshot이면 추가 호출 한 번으로 끝난다.
    yield from _probe_until_terminal(
        pages, page_count * pages.size + 1, max_probe_pages, expected_total
    )


def _probe_until_terminal(
    pages: _Pages, page_start: int, max_probe_pages: int, known_total: int
) -> Iterator[FetchResult]:
    """`page_start`부터 종료 페이지까지 받는다. 새 데이터가 없으면 `known_total`을 쓴다."""
    previous_end: int | None = None
    while True:
        key = pages.key(page_start)
        outcome = pages.probe(page_start)
        if outcome.error is not None:
            yield FetchResult(key=key, error=outcome.error)
            return
        if outcome.terminal:
            total = known_total if previous_end is None else previous_end
            yield FetchResult(key=key, payload=outcome.payload, expected_total=total)
            return
        if (page_start - 1) // pages.size >= max_probe_pages:
            yield FetchResult(key=key, error=FetchErrorKind.PERMANENT)
            return
        yield FetchResult(key=key, payload=outcome.payload)
        previous_end = page_start - 1 + outcome.row_count
        page_start += pages.size


def _fetch_population_pois(
    client: httpx.Client, params: dict, skip: frozenset[str], concurrency: int
) -> Iterator[FetchResult]:
    """실시간 인구 POI를 하나씩 받는다.

    POI 수는 요청 조각 수이고 행 수가 아니므로 expected_total을 싣지 않는다. 완결도는
    조각 기준으로 계산된다.
    """
    service = params["service"]
    pois = [
        (f"poi-{code}", f"{_BASE_URL}/{_api_key()}/json/{service}/1/5/{code}/")
        for code in _population_poi_codes(params)
        if f"poi-{code}" not in skip
    ]
    for (key, _url), outcome in run_concurrent(
        pois, concurrency, lambda item: _fetch_poi(client, item[1]), "seoul-poi"
    ):
        yield FetchResult(key=key, payload=outcome.payload, error=outcome.error)


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


def _root_location(params: dict) -> tuple[str, str]:
    """설정에 따라 응답 wrapper 키와 그 아래 row 경로를 분리한다."""
    root_key = params["root_key"]
    if params.get("root_key_literal", False):
        return root_key, ""
    wrapper_key, _, row_path = root_key.partition(".")
    return wrapper_key, row_path


def _page_url_builder(params: dict, window) -> Callable[[int, int], str]:
    """페이지 시작·끝 인덱스로 요청 URL을 만드는 함수를 반환한다."""
    service = params["service"]
    suffix = ""
    if params.get("path_suffix"):
        # 19:00 window가 window_start의 시를 쓰면 19시대를 요청해 18:55~19:00 데이터를
        # 아무도 가져가지 않는다. 직전 순간(`window_last`)으로 18시대를 완결시킨다.
        suffix = params["path_suffix"].format(
            window_start=window.window_start,
            window_end=window.window_end,
            window_last=window.window_start - timedelta(seconds=1),
        )
    return lambda start, end: (
        f"{_BASE_URL}/{_api_key()}/json/{service}/{start}/{end}{suffix}/"
    )


@adapter("seoul_openapi")
class SeoulOpenApiAdapter:
    """서울 열린데이터광장 공용 페이지네이션 규약 어댑터."""

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
        concurrency = int(params.get("concurrency", 1))
        if params["service"] == "citydata_ppltn":
            yield from _fetch_population_pois(client, params, skip, concurrency)
            return

        wrapper_key, row_path = _root_location(params)
        pages = _Pages(
            client=client,
            url_for=_page_url_builder(params, window),
            wrapper_key=wrapper_key,
            row_path=row_path,
            size=int(params["page_size"]),
        )
        if params.get("pagination") != "probe_until_empty":
            yield from _fetch_total_pages(pages, concurrency, skip, expected_total)
        elif expected_total is None:
            yield from _discover_probe_pages(
                pages, int(params["max_probe_pages"]), skip
            )
        else:
            yield from _refetch_known_probe_pages(
                pages, int(params["max_probe_pages"]), skip, expected_total
            )

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 순서대로 이어붙여 행 목록으로 만든다."""
        params = config.adapter_params
        wrapper_key, row_path = _root_location(params)

        rows: list[dict] = []
        for chunk in chunks:
            rows.extend(_page_rows(json.loads(chunk), wrapper_key, row_path) or [])
        if params.get("flatten_forecast", False):
            rows = [_flatten_forecast(row) for row in rows if isinstance(row, dict)]
        _validate_natural_key(rows, config.natural_key)
        return rows
