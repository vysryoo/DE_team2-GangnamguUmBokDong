"""과거 조회 API에서 하루치 원시 행을 받아온다.

`bikeListHist`는 시각을 `YYYYMMDDHH`로 받는다. 8자리를 주면 에러 없이 최신 스냅샷을
돌려주므로 형식을 설정(`time_format`)으로 고정한다. 공공 서비스라 기본 병렬도는 보수적으로
4로 둔다. 인증키는 URL 경로에 들어가므로 예외 메시지에서 가린다.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

import httpx

from bootstrap.config import BootstrapConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"
_SUCCESS_CODES = {"INFO-000", "INFO-200"}
_FATAL_CODES = {"INFO-100"}


class FetchFailed(Exception):
    """재시도 후에도 실패해 그 날짜를 포기해야 할 때."""


class _FatalFetchError(Exception):
    """인증키 오류 등 재시도해도 무의미한 실패. `_hour()`에서 즉시 중단시키는 내부 신호다."""


def _api_key() -> str:
    return os.environ["SEOUL_OPENAPI_KEY"]


def _mask(url: str) -> str:
    """URL 경로에 실린 인증키를 가린다."""
    return url.replace(_api_key(), "***")


def _raise_for_code(url: str, code, message) -> None:
    """코드가 실패를 뜻하면 알맞은 예외를 올린다. 성공 코드거나 코드가 없으면 아무 일도 하지 않는다."""
    if code is None or code in _SUCCESS_CODES:
        return
    if code in _FATAL_CODES:
        raise _FatalFetchError(f"{_mask(url)} → {code} {message}")
    raise FetchFailed(f"{_mask(url)} → {code} {message}")


def _page(cfg: BootstrapConfig, stamp: str, start: int, end: int, client: httpx.Client) -> tuple[list[dict], int]:
    """페이지 하나를 받아 (행, 총건수)를 반환한다."""
    url = f"{_BASE_URL}/{_api_key()}/json/{cfg.service}/{start}/{end}/{stamp}/"
    response = client.get(url)
    response.raise_for_status()
    try:
        body = json.loads(response.content)
    except json.JSONDecodeError:
        if "INFO-100" in response.text:
            raise _FatalFetchError(f"{_mask(url)} → INFO-100 인증키가 유효하지 않습니다") from None
        raise

    code = body.get("CODE")
    _raise_for_code(url, code, body.get("MESSAGE"))
    if code == "INFO-200":
        return [], 0

    wrapper = next(iter(body.values()))
    wrapper_result = wrapper.get("RESULT", {})
    _raise_for_code(url, wrapper_result.get("CODE"), wrapper_result.get("MESSAGE"))

    return wrapper.get("row", []), int(wrapper.get("list_total_count", 0))


def _hour(
    cfg: BootstrapConfig,
    stamp: str,
    client: httpx.Client,
    max_retries: int,
    sleep: Callable[[float], None] | None = None,
) -> list[dict]:
    """한 시각을 페이지 끝까지 받아온다. 일시적 실패는 재시도하고, 확정적 실패는 즉시 포기한다.

    재시도 사이에는 쿼터·레이트리밋을 악화시키지 않도록 1초, 2초, ... 지수 백오프로 쉰다.
    """
    effective_sleep = sleep if sleep is not None else time.sleep
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            rows, total = _page(cfg, stamp, 1, cfg.page_size, client)
            start = cfg.page_size + 1
            while start <= total:
                more, _ = _page(cfg, stamp, start, start + cfg.page_size - 1, client)
                rows.extend(more)
                start += cfg.page_size
            return rows
        except _FatalFetchError as exc:
            raise FetchFailed(f"{stamp} 조회 실패: {_mask(str(exc))}") from exc
        except (httpx.HTTPError, json.JSONDecodeError, FetchFailed) as exc:
            last_error = exc
            if attempt < max_retries:
                effective_sleep(2**attempt)
    raise FetchFailed(f"{stamp} 조회 실패: {_mask(str(last_error))}")


def fetch_by_date(
    cfg: BootstrapConfig,
    day: date,
    *,
    client: httpx.Client,
    concurrency: int = 4,
    max_retries: int = 2,
    sleep: Callable[[float], None] | None = None,
) -> list[dict]:
    """그 날짜의 24시간을 병렬로 조회해 행을 모아 반환한다.

    args:
        cfg: 해당 소스의 bootstrap 설정
        day: 조회할 날짜
        client: 재사용할 HTTP 클라이언트
        concurrency: 동시에 처리할 시각 수
        max_retries: 시각 하나당 재시도 횟수
        sleep: 재시도 사이 백오프에 쓸 sleep 함수. 기본은 `time.sleep`.
    returns:
        24시간치 행. 순서는 보장하지 않는다.
    raises:
        FetchFailed: 어느 한 시각이라도 재시도 후 실패했을 때. 부분 결과를 쓰지 않기
            위해 날짜 전체를 포기한다.
    """
    stamps = [datetime(day.year, day.month, day.day, hour).strftime(cfg.time_format) for hour in range(24)]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        # `pool.map()`은 첫 예외에서 남은 future를 취소해 호출 횟수가 들쭉날쭉해짐.
        # 24시각을 모두 시도한 뒤 첫 실패를 올림
        futures = [pool.submit(_hour, cfg, stamp, client, max_retries, sleep) for stamp in stamps]
        chunks: list[list[dict]] = []
        first_error: FetchFailed | None = None
        for future in futures:
            try:
                chunks.append(future.result())
            except FetchFailed as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error
    return [row for chunk in chunks for row in chunk]
