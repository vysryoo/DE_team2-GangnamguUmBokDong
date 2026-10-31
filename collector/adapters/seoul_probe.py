"""서울 API `probe_until_empty` 페이지네이션.

`bikeList`는 `list_total_count`가 현재 페이지 행 수라 전체 행 수를 미리 알 수 없다. 빈
종료 페이지까지 순차로 받고, 그 응답도 완결성 증거로 Bronze에 남긴다. 재시도 라운드는
앞 라운드가 확정한 total로 고정 폭 페이지를 복원해 실패 페이지만 다시 받는다.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from adapters.base import FetchErrorKind, FetchResult

if TYPE_CHECKING:
    from adapters.seoul_openapi import _Pages


def discover_pages(
    pages: _Pages, max_probe_pages: int, skip: frozenset[str]
) -> Iterator[FetchResult]:
    """끝을 모르는 상태에서 빈 종료 페이지까지 순차 탐색한다.

    종료 응답도 완결성 증거로 Bronze에 보존하며 실제 expected_total을 함께 전달한다.
    max_probe_pages개 뒤 한 번 더 호출해 빈 끝을 확인하고, 거기에도 데이터가 있으면
    설정 상한이 낮은 것이므로 조용히 자르지 않고 누락으로 실패시킨다.

    Args:
        pages: 서비스의 페이지 요청 규칙.
        max_probe_pages: 데이터 페이지 상한.
        skip: 앞 라운드에서 이미 받은 페이지 키.
    Returns:
        페이지마다의 FetchResult. 종료 페이지 결과에만 expected_total을 싣는다.
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


def refetch_known_pages(
    pages: _Pages, max_probe_pages: int, skip: frozenset[str], expected_total: int
) -> Iterator[FetchResult]:
    """이전 라운드가 확정한 total로 고정 폭 페이지를 복원해 실패 페이지만 다시 받는다.

    Args:
        pages: 서비스의 페이지 요청 규칙.
        max_probe_pages: 데이터 페이지 상한.
        skip: 앞 라운드에서 이미 받은 페이지 키.
        expected_total: 앞 라운드가 종료 페이지로 확정한 전체 행 수.
    Returns:
        페이지마다의 FetchResult. 종료 페이지 결과에만 expected_total을 싣는다.
    """
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
