"""API 제공처별 어댑터의 공통 계약과 라운드 재시도 루프.

어댑터 하나는 소스가 아니라 API 제공처 하나를 맡는다. `fetch`는 실패를 예외 대신
`FetchResult.error`로 돌려줘 나머지 조각을 계속 받게 한다. `TRANSIENT`(타임아웃·429·5xx)는
다음 라운드에서 재시도하고, `PERMANENT`(400·404)는 그 조각만 포기하며, `FATAL`(401·403)은
fetch 전체를 멈춘다. 어댑터는 S3 저장과 재시도 횟수를 모르고 원본 응답을 그대로 넘긴다.
`normalize`는 네트워크 없이 조각 목록 전체를 행으로 바꾸며, 재개 시에도 다시 호출된다.

조각 키는 Bronze 파일명이므로 같은 요청이면 실행 간에 같아야 누락 조각을 지목할 수
있다(`page-00001-01000`, `grid-060x127`). `planned_parts`는 요청 전에 전체 키를 아는
어댑터만 제공해, budget으로 끊겨 yield하지 못한 키도 누락으로 남긴다.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    import httpx

    from config.schema import SourceConfig


@dataclass(frozen=True, slots=True)
class Window:
    """수집 대상 시간 창."""

    window_start: datetime
    window_end: datetime


class FetchErrorKind(Enum):
    """조각 실패 범주."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class FetchResult:
    """조각 하나의 결과. 실패도 예외 대신 `error`에 범주를 담아 반환한다.

    key: 조각 식별 키
    payload: 성공 시 원본 응답
    error: 실패 시 범주
    expected_total: 전체 행 수. 알 수 있는 소스가 처음 알아낸 조각에만 싣는다
    persist: False면 payload를 Bronze에 저장하지 않고 expected_total만 전달한다
    """

    key: str
    payload: bytes | None = None
    error: FetchErrorKind | None = None
    expected_total: int | None = None
    persist: bool = True


class Adapter(Protocol):
    """API 제공처 하나에 대응하는 fetch · normalize 계약."""

    @staticmethod
    def fetch(
        config: SourceConfig,
        window: Window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ) -> Iterator[FetchResult]: ...

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]: ...

    @staticmethod
    def planned_parts(config: SourceConfig, window: Window) -> frozenset[str] | None: ...


class UnknownAdapterError(ValueError):
    """등록되지 않은 어댑터 이름을 조회했다."""


class DuplicateAdapterError(ValueError):
    """같은 이름을 두 번 등록했다. 조용히 덮어쓰면 어느 어댑터가 돌았는지 알 수 없다."""


_ADAPTERS: dict[str, type] = {}


def adapter(name: str) -> Callable[[type], type]:
    """어댑터를 이름으로 등록한다. 원본 클래스를 그대로 반환한다."""

    def register(cls: type) -> type:
        if name in _ADAPTERS:
            raise DuplicateAdapterError(
                f"어댑터 '{name}'이 이미 등록되어 있습니다: {_ADAPTERS[name].__qualname__}"
            )
        _ADAPTERS[name] = cls
        return cls

    return register


def get_adapter(name: str) -> type:
    """등록된 어댑터를 이름으로 조회한다."""
    try:
        return _ADAPTERS[name]
    except KeyError:
        listed = ", ".join(sorted(_ADAPTERS)) or "(없음)"
        raise UnknownAdapterError(
            f"어댑터 '{name}'이 등록되어 있지 않습니다. 등록된 이름: {listed}"
        ) from None


_Item = TypeVar("_Item")


class _HasError(Protocol):
    """`run_concurrent`이 FATAL 조기 종료를 판단하는 데만 필요한 최소 계약."""

    error: FetchErrorKind | None


_Outcome = TypeVar("_Outcome", bound=_HasError)


def run_concurrent(
    items: Iterable[_Item],
    concurrency: int,
    fetch_one: Callable[[_Item], _Outcome],
    thread_name_prefix: str,
) -> Iterator[tuple[_Item, _Outcome]]:
    """items 순서를 유지하며 fetch_one(item)을 실행해 (item, outcome)을 yield한다.

    `concurrency`가 1 이하면 순차로 돈다. 그보다 크면 앞 항목을 기다리는 동안 뒤 요청을
    미리 보내되, 완료됐지만 아직 내보내지 않은 응답이 쌓이지 않게 concurrency의 2배까지만
    미리 요청한다. 어느 쪽이든 FATAL을 만나면 나머지 항목을 요청하지 않는다(모든 조각이
    같은 인증키를 쓴다).
    """
    if concurrency <= 1:
        for item in items:
            outcome = fetch_one(item)
            yield item, outcome
            if outcome.error is FetchErrorKind.FATAL:
                return
        return

    pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=thread_name_prefix)
    try:
        queued = iter(items)
        inflight: deque[tuple[_Item, Future[_Outcome]]] = deque()

        def submit_next() -> bool:
            item = next(queued, None)
            if item is None:
                return False
            inflight.append((item, pool.submit(fetch_one, item)))
            return True

        for _ in range(concurrency * 2):
            if not submit_next():
                break

        while inflight:
            item, future = inflight.popleft()
            outcome = future.result()
            submit_next()
            yield item, outcome
            if outcome.error is FetchErrorKind.FATAL:
                return
    finally:
        # `with` 문의 shutdown(wait=True)는 fetch_with_rounds가 마감 시한으로 순회를
        # 버릴 때 대기 큐가 끝날 때까지 블록한다. 대기 큐는 즉시 비운다.
        pool.shutdown(wait=False, cancel_futures=True)


def classify_http_status(status_code: int) -> FetchErrorKind | None:
    """HTTP 상태 코드를 실패 범주로 매핑한다. `None`은 성공(2xx)이다."""
    if 200 <= status_code < 300:
        return None
    if status_code in (401, 403):
        return FetchErrorKind.FATAL
    if status_code == 429 or status_code >= 500:
        return FetchErrorKind.TRANSIENT
    return FetchErrorKind.PERMANENT


@dataclass(frozen=True, slots=True)
class FetchRoundResult:
    """라운드 오케스트레이션의 최종 결과."""

    chunks: dict[str, bytes]
    missing: dict[str, FetchErrorKind]
    expected_total: int | None


# 라운드 0→1, 1→2 대기 시간.
ROUND_WAITS_SECONDS = (15, 30)
MAX_ROUNDS = len(ROUND_WAITS_SECONDS) + 1


def fetch_with_rounds(
    fetch_fn,
    config,
    window,
    *,
    client,
    skip=frozenset(),
    expected_total=None,
    sleep_fn=time.sleep,
    now_fn=time.monotonic,
    on_chunk=None,
    planned_parts: frozenset[str] | None = None,
):
    """TRANSIENT 조각만 모아 대기 후 다시 요청하는 라운드 루프를 돈다.

    fetch budget이 끝나면 새 요청을 시작하지 않는다. FATAL이 나오면 남은 라운드를 돌지
    않는다.

    Args:
        fetch_fn: 어댑터의 fetch 제너레이터 함수.
        config: 소스 설정.
        window: 수집 대상 window.
        client: 어댑터에 넘길 httpx 클라이언트.
        skip: 이미 확보해 건너뛸 조각 키.
        expected_total: 이미 알고 있는 전체 행 수.
        sleep_fn: 라운드 간 대기 함수.
        now_fn: budget 계산용 단조 시계.
        on_chunk: 성공 조각마다 즉시 호출할 콜백(pipeline의 Bronze 저장).
        planned_parts: 요청 전에 아는 전체 조각 키. budget 종료로 iterator가 도달하지
            못한 키도 누락으로 기록한다.
    Returns:
        성공 조각, 누락 조각, 전체 행 수를 담은 FetchRoundResult.
    Raises:
        ValueError: 저장 대상 성공 조각의 payload가 None일 때.
    """
    collected: dict[str, bytes] = {}
    permanent: dict[str, FetchErrorKind] = {}
    transient: dict[str, FetchErrorKind] = {}
    deadline = now_fn() + config.effective_fetch_budget().total_seconds()

    for round_index in range(MAX_ROUNDS):
        if round_index > 0 and not transient:
            break
        if now_fn() >= deadline:
            break

        results = fetch_fn(
            config,
            window,
            client=client,
            skip=frozenset(skip) | collected.keys() | permanent.keys(),
            expected_total=expected_total,
        )
        transient = {}
        aborted, expected_total = _run_round(
            results,
            deadline=deadline,
            now_fn=now_fn,
            on_chunk=on_chunk,
            collected=collected,
            permanent=permanent,
            transient=transient,
            expected_total=expected_total,
        )
        if aborted:
            break
        if round_index < len(ROUND_WAITS_SECONDS) and transient:
            sleep_fn(ROUND_WAITS_SECONDS[round_index])

    missing = {**permanent, **transient}
    if planned_parts is not None:
        covered = frozenset(skip) | collected.keys() | missing.keys()
        for key in planned_parts - covered:
            missing[key] = FetchErrorKind.TRANSIENT
    return FetchRoundResult(chunks=collected, missing=missing, expected_total=expected_total)


def _run_round(
    results: Iterator[FetchResult],
    *,
    deadline: float,
    now_fn: Callable[[], float],
    on_chunk: Callable[[str, bytes], None] | None,
    collected: dict[str, bytes],
    permanent: dict[str, FetchErrorKind],
    transient: dict[str, FetchErrorKind],
    expected_total: int | None,
) -> tuple[bool, int | None]:
    """라운드 하나의 결과를 세 dict에 나눠 담고 (중단 여부, 전체 행 수)를 반환한다.

    budget이 끝났거나 FATAL을 만나면 중단으로 보고 남은 라운드를 돌지 않게 한다.
    """
    while now_fn() < deadline:
        try:
            result = next(results)
        except StopIteration:
            return False, expected_total

        # probe 소스가 재시도 중 늘어난 total을 발견하면 갱신하되, 더 작은 값으로
        # 줄여 기존 Bronze payload를 정상으로 오인하지 않는다.
        if result.expected_total is not None:
            expected_total = max(expected_total or 0, result.expected_total)

        if result.error is None:
            if not result.persist:
                continue
            if result.payload is None:
                raise ValueError("저장할 성공 조각의 payload는 None일 수 없습니다")
            collected[result.key] = result.payload
            if on_chunk is not None:
                on_chunk(result.key, result.payload)
        elif result.error is FetchErrorKind.TRANSIENT:
            transient[result.key] = result.error
        else:
            permanent[result.key] = result.error
            if result.error is FetchErrorKind.FATAL:
                return True, expected_total
    return True, expected_total
