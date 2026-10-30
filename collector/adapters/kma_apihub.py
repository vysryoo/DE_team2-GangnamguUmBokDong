"""기상청 API 허브 어댑터.

초단기실황·예보(10분)와 단기예보(3시간)를 수집한다. 전체 행 수를 알려주지 않아
`expected_total`이 항상 None이고, 완결도는 (성공 격자 / 계획 격자)로 계산된다.

`time_rule`은 window를 API가 허용하는 발표 시각(`base_time`)으로 내린다.
`freshness_rule`은 fetch에는 쓰지 않고 `main.py`의 freshness 판정에만 쓰는 폴링
그리드다. 초단기실황·예보는 발표 그리드(시간당 1슬롯)와 실제 갱신 주기(10분·30분)가
달라, 발표 그리드로 freshness를 판정하면 시간당 1번만 수집된다.

| rule | 용도 | 규칙 | 예시 |
| --- | --- | --- | --- |
| `hourly` | 초단기실황 fetch | 매시 정각(40분 발표) | 14:40 → 14:00, 14:39 → 13:00 |
| `half_hourly` | 초단기예보 fetch | 매시 30분 | 14:40 → 14:30, 14:20 → 13:30 |
| `vilage_fcst` | 단기예보 fetch | 02, 05, 08...시 정각(10분 발표) | 05:05 → 02:00 |
| `ten_minutely` | freshness | 10분 단위 내림 | 14:37 → 14:30 |
| `thirty_minutely` | freshness | 30분 단위 내림 | 14:37 → 14:30 |

격자 하나의 행 수가 `numOfRows`를 넘으면(단기예보 1052건) `totalCount`로 남은 페이지를
받아 격자당 하나의 `FetchResult`로 합친다. 재시도·skip은 격자 단위이므로 페이지 중
하나라도 실패하면 그 격자 전체를 실패로 처리한다.
"""

from __future__ import annotations

import copy
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from adapters.base import (
    FetchErrorKind,
    FetchResult,
    adapter,
    classify_http_status,
    run_concurrent,
)

if TYPE_CHECKING:
    from config.schema import SourceConfig

    from adapters.base import Window

_BASE_URL = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0"
_NUM_OF_ROWS = 1000

logger = logging.getLogger(__name__)


def _api_key() -> str:
    return os.environ["KMA_APIHUB_KEY"]


def _classify_result_code(code: str | None) -> FetchErrorKind | None:
    """본문의 resultCode를 실패 범주로 매핑한다."""
    if not isinstance(code, str):
        return FetchErrorKind.PERMANENT

    if code == "00":
        return None  # NORMAL_SERVICE (성공)
    if code in {"03", "04", "05", "22"}:
        # 03: 데이터 없음(미발표), 04: HTTP 에러, 05: 연결 실패, 22: 제한 초과
        return FetchErrorKind.TRANSIENT
    if code in {"20", "21", "30", "31", "32", "33"}:
        # 20, 21, 30, 31, 32, 33: 인증/권한 에러
        return FetchErrorKind.FATAL

    return FetchErrorKind.PERMANENT


def adjust_base_time(dt: datetime, rule: str | None) -> datetime:
    """`rule` 그리드에서 `dt` 이전 가장 최근 슬롯으로 내린다(모듈 docstring 표 참고)."""
    if not rule:
        return dt

    if rule == "hourly":
        eval_dt = dt if dt.minute >= 40 else dt - timedelta(hours=1)
        return eval_dt.replace(minute=0, second=0, microsecond=0)

    if rule == "half_hourly":
        if dt.minute < 30:
            return (dt - timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)
        return dt.replace(minute=30, second=0, microsecond=0)

    if rule == "vilage_fcst":
        eval_dt = dt if dt.minute >= 10 else dt - timedelta(hours=1)
        if eval_dt.hour < 2:
            return (eval_dt - timedelta(days=1)).replace(
                hour=23, minute=0, second=0, microsecond=0
            )
        base_hour = 2 + 3 * ((eval_dt.hour - 2) // 3)
        return eval_dt.replace(hour=base_hour, minute=0, second=0, microsecond=0)

    if rule == "ten_minutely":
        return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)

    if rule == "thirty_minutely":
        return dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)

    raise ValueError(f"알 수 없는 time_rule: {rule}")


def _grid_key(nx: int, ny: int) -> str:
    """격자 좌표로 조각 키를 만든다."""
    return f"grid-{nx:03d}x{ny:03d}"


def _extract(body: dict, root_key: str) -> list[dict]:
    """root_key의 점 표기 경로를 따라가 행 배열을 꺼낸다. 경로가 없으면 빈 리스트."""
    node: object = body
    for segment in root_key.split("."):
        if not isinstance(node, dict):
            return []
        node = node.get(segment)
    return node if isinstance(node, list) else []


def _merge_pages(bodies: list[dict], root_key: str) -> bytes:
    """여러 페이지의 item 배열을 하나로 이어붙인 JSON 본문(bytes)을 만든다.

    첫 페이지의 envelope(header 등)을 그대로 쓰고 item 배열만 전체 페이지를
    이어붙인 것으로 교체한다.
    """
    merged = copy.deepcopy(bodies[0])
    node = merged
    segments = root_key.split(".")
    for segment in segments[:-1]:
        node = node[segment]
    node[segments[-1]] = [item for body in bodies for item in _extract(body, root_key)]
    return json.dumps(merged).encode()


@dataclass(frozen=True, slots=True)
class _GridOutcome:
    """격자 하나(페이지 여러 장 포함)를 전부 받은 뒤의 결과."""

    payload: bytes | None
    error: FetchErrorKind | None


def _fetch_grid(
    client: httpx.Client,
    endpoint: str,
    root_key: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
) -> _GridOutcome:
    """격자 하나를 필요한 만큼 페이지를 넘겨가며 받아 하나의 결과로 합친다."""
    base_url = (
        f"{_BASE_URL}/{endpoint}?authKey={_api_key()}&dataType=JSON"
        f"&numOfRows={_NUM_OF_ROWS}"
        f"&base_date={base_date}&base_time={base_time}&nx={nx}&ny={ny}"
    )

    page_bodies: list[dict] = []
    first_page_raw: bytes | None = None
    total_pages = 1
    page_no = 1

    while page_no <= total_pages:
        url = f"{base_url}&pageNo={page_no}"
        started_at = time.monotonic()
        try:
            response = client.get(url)
        except httpx.RequestError as exc:
            logger.info(
                "stage=kma_http_request endpoint=%s grid=%03dx%03d page=%d "
                "outcome=request_error error_type=%s elapsed_ms=%.1f",
                endpoint,
                nx,
                ny,
                page_no,
                type(exc).__name__,
                (time.monotonic() - started_at) * 1000,
            )
            return _GridOutcome(payload=None, error=FetchErrorKind.TRANSIENT)

        logger.info(
            "stage=kma_http_request endpoint=%s grid=%03dx%03d page=%d "
            "outcome=response status_code=%d elapsed_ms=%.1f",
            endpoint,
            nx,
            ny,
            page_no,
            response.status_code,
            (time.monotonic() - started_at) * 1000,
        )

        category = classify_http_status(response.status_code)
        if category is not None:
            return _GridOutcome(payload=None, error=category)

        try:
            body = json.loads(response.content)
            if not isinstance(body, dict):
                raise TypeError("응답이 JSON 객체가 아님")
            result_code = body.get("response", {}).get("header", {}).get("resultCode")
        except (json.JSONDecodeError, TypeError):
            return _GridOutcome(payload=None, error=FetchErrorKind.TRANSIENT)

        api_category = _classify_result_code(result_code)
        if api_category is not None:
            return _GridOutcome(payload=None, error=api_category)

        page_bodies.append(body)
        if page_no == 1:
            first_page_raw = response.content
            total_count = body.get("response", {}).get("body", {}).get("totalCount")
            if isinstance(total_count, int) and total_count > _NUM_OF_ROWS:
                total_pages = math.ceil(total_count / _NUM_OF_ROWS)

        page_no += 1

    payload = (
        first_page_raw if len(page_bodies) == 1 else _merge_pages(page_bodies, root_key)
    )
    return _GridOutcome(payload=payload, error=None)


@adapter("kma_apihub")
class KmaApiHubAdapter:
    """기상청 API 허브 공용 격자 반복 어댑터."""

    @staticmethod
    def planned_parts(config: SourceConfig, window: Window) -> frozenset[str]:
        """fetch 전에 격자 요청 계획을 고정한다.

        iterator가 budget으로 첫 미방문 격자 전에 끊겨도 누락을 기록하려면 전체 키를
        미리 알아야 한다. 중복 격자는 한 번만 호출되므로 거부한다.
        """
        keys = [_grid_key(nx, ny) for nx, ny in config.adapter_params["grids"]]
        if len(set(keys)) != len(keys):
            raise ValueError("KMA source의 grids는 중복될 수 없습니다")
        return frozenset(keys)

    @staticmethod
    def fetch(
        config: SourceConfig,
        window: Window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ):
        params = config.adapter_params
        endpoint = params["endpoint"]
        root_key = params["root_key"]
        base_time = adjust_base_time(window.window_start, params.get("time_rule"))
        grid_items = [
            (_grid_key(nx, ny), nx, ny)
            for nx, ny in params["grids"]
            if _grid_key(nx, ny) not in skip
        ]

        def fetch_one_grid(item: tuple[str, int, int]) -> _GridOutcome:
            _key, nx, ny = item
            return _fetch_grid(
                client,
                endpoint,
                root_key,
                base_time.strftime("%Y%m%d"),
                base_time.strftime("%H%M"),
                nx,
                ny,
            )

        for (key, _nx, _ny), outcome in run_concurrent(
            grid_items, int(params.get("concurrency", 1)), fetch_one_grid, "kma-grid"
        ):
            yield FetchResult(key=key, payload=outcome.payload, error=outcome.error)

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 pivot 설정에 따라 합친다."""
        root_key = config.adapter_params["root_key"]
        pivot = config.adapter_params["pivot"]
        key_field, value_field = pivot["key"], pivot["value"]

        groups: dict[tuple, dict] = {}
        for chunk in chunks:
            body = json.loads(chunk)
            for item in _extract(body, root_key):
                group_key = tuple(
                    sorted(
                        (k, v)
                        for k, v in item.items()
                        if k not in (key_field, value_field)
                    )
                )
                groups.setdefault(group_key, dict(group_key))[item[key_field]] = item[
                    value_field
                ]
        return list(groups.values())
