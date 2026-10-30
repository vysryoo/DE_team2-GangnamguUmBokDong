"""#6 adapters/base.py 테스트: 타입, 레지스트리, 라운드 오케스트레이션."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from adapters.base import (
    DuplicateAdapterError,
    FetchErrorKind,
    FetchResult,
    UnknownAdapterError,
    Window,
    adapter,
    adapter_names,
    classify_http_status,
    fetch_with_rounds,
    get_adapter,
    is_adapter_registered,
)
from core.kst import KST


class _FakeBudgetConfig:
    """`effective_fetch_budget()`만 필요한 최소 config 더블."""

    def __init__(self, seconds=3600):
        self._seconds = seconds

    def effective_fetch_budget(self):
        from datetime import timedelta

        return timedelta(seconds=self._seconds)




@pytest.fixture
def window():
    return Window(
        window_start=datetime(2026, 8, 12, 14, 10, tzinfo=KST),
        window_end=datetime(2026, 8, 12, 14, 15, tzinfo=KST),
    )


def test_window_holds_start_and_end():
    start = datetime(2026, 8, 12, 14, 10, tzinfo=KST)
    end = datetime(2026, 8, 12, 14, 15, tzinfo=KST)

    window = Window(window_start=start, window_end=end)

    assert window.window_start == start
    assert window.window_end == end


def test_fetch_error_kind_has_three_categories():
    assert FetchErrorKind.TRANSIENT
    assert FetchErrorKind.PERMANENT
    assert FetchErrorKind.FATAL


def test_fetch_result_success_carries_payload():
    result = FetchResult(key="page-00001-01000", payload=b'{"a": 1}', error=None, expected_total=2765)

    assert result.key == "page-00001-01000"
    assert result.payload == b'{"a": 1}'
    assert result.error is None
    assert result.expected_total == 2765


def test_fetch_result_failure_carries_error_kind():
    result = FetchResult(key="page-00001-01000", payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)

    assert result.payload is None
    assert result.error is FetchErrorKind.TRANSIENT


def test_fetch_result_metadata_is_not_persisted_but_updates_expected_total(window):
    """probe 종료 메타데이터는 bronze 조각이 아니지만 전체 행 수는 전달한다."""

    def fetch_fn(config, win, *, client, skip, expected_total):
        yield FetchResult(key="page-1", payload=b"data", error=None, expected_total=None)
        yield FetchResult(
            key="probe-end-2",
            payload=None,
            error=None,
            expected_total=1,
            persist=False,
        )

    on_chunk = MagicMock()
    result = fetch_with_rounds(
        fetch_fn,
        _FakeBudgetConfig(),
        window,
        client=object(),
        on_chunk=on_chunk,
    )

    assert result.chunks == {"page-1": b"data"}
    assert result.expected_total == 1
    on_chunk.assert_called_once_with("page-1", b"data")


def test_fetch_with_rounds_rejects_persisted_success_without_payload(window):
    """어댑터가 metadata 플래그를 빼먹어 빈 bronze를 쓰는 일을 막는다."""

    def fetch_fn(config, win, *, client, skip, expected_total):
        yield FetchResult(key="broken", payload=None, error=None, expected_total=None)

    with pytest.raises(ValueError, match="payload"):
        fetch_with_rounds(fetch_fn, _FakeBudgetConfig(), window, client=object())


def test_adapter_round_trip(clean_adapter_registry):
    @adapter("t_dummy")
    class Dummy:
        pass

    assert get_adapter("t_dummy") is Dummy


def test_duplicate_adapter_is_rejected(clean_adapter_registry):
    @adapter("t_dup")
    class First:
        pass

    with pytest.raises(DuplicateAdapterError, match="t_dup"):
        @adapter("t_dup")
        class Second:
            pass


def test_unknown_adapter_message_lists_registered_names(clean_adapter_registry):
    @adapter("t_registered")
    class Known:
        pass

    with pytest.raises(UnknownAdapterError) as excinfo:
        get_adapter("t_typo")

    message = str(excinfo.value)
    assert "t_typo" in message
    assert "t_registered" in message


def test_is_adapter_registered_does_not_instantiate(clean_adapter_registry):
    @adapter("t_boom")
    class Boom:
        def __init__(self):
            raise AssertionError("등록 확인이 인스턴스를 만들면 안 된다")

    assert is_adapter_registered("t_boom") is True
    assert is_adapter_registered("t_absent") is False


def test_adapter_names_are_sorted(clean_adapter_registry):
    @adapter("t_b")
    class B:
        pass

    @adapter("t_a")
    class A:
        pass

    names = adapter_names()
    assert names.index("t_a") < names.index("t_b")


def test_classify_http_status_treats_2xx_as_success():
    assert classify_http_status(200) is None
    assert classify_http_status(204) is None


def test_classify_http_status_treats_429_and_5xx_as_transient():
    assert classify_http_status(429) is FetchErrorKind.TRANSIENT
    assert classify_http_status(500) is FetchErrorKind.TRANSIENT
    assert classify_http_status(503) is FetchErrorKind.TRANSIENT


def test_classify_http_status_treats_401_and_403_as_fatal():
    assert classify_http_status(401) is FetchErrorKind.FATAL
    assert classify_http_status(403) is FetchErrorKind.FATAL


def test_classify_http_status_treats_other_4xx_as_permanent():
    assert classify_http_status(400) is FetchErrorKind.PERMANENT
    assert classify_http_status(404) is FetchErrorKind.PERMANENT


def test_fetch_with_rounds_collects_all_successes_in_one_round(window):
    def fetch_fn(config, win, *, client, skip, expected_total):
        assert skip == frozenset()
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=2)
        yield FetchResult(key="b", payload=b"2", error=None, expected_total=None)

    sleep_fn = MagicMock()
    result = fetch_with_rounds(
        fetch_fn, _FakeBudgetConfig(), window, client=object(), sleep_fn=sleep_fn,
    )

    assert result.chunks == {"a": b"1", "b": b"2"}
    assert result.missing == {}
    assert result.expected_total == 2
    sleep_fn.assert_not_called()


def test_fetch_with_rounds_permanent_failure_is_missing_without_retry(window):
    call_count = 0

    def fetch_fn(config, win, *, client, skip, expected_total):
        nonlocal call_count
        call_count += 1
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)
        yield FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None)

    sleep_fn = MagicMock()
    result = fetch_with_rounds(
        fetch_fn, _FakeBudgetConfig(), window, client=object(), sleep_fn=sleep_fn,
    )

    assert call_count == 1  # PERMANENT는 재투입되지 않으므로 라운드가 더 안 돈다
    assert result.chunks == {"a": b"1"}
    assert result.missing == {"b": FetchErrorKind.PERMANENT}
    sleep_fn.assert_not_called()


def test_fetch_with_rounds_retries_transient_failure_in_next_round(window):
    calls = []

    def fetch_fn(config, win, *, client, skip, expected_total):
        calls.append(frozenset(skip))
        if len(calls) == 1:
            yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)
            yield FetchResult(key="b", payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)
        else:
            yield FetchResult(key="b", payload=b"2", error=None, expected_total=None)

    sleep_fn = MagicMock()
    result = fetch_with_rounds(
        fetch_fn, _FakeBudgetConfig(), window, client=object(), sleep_fn=sleep_fn,
    )

    assert len(calls) == 2
    assert calls[1] == frozenset({"a"})  # 라운드 1은 이미 확보한 a만 skip한다
    assert result.chunks == {"a": b"1", "b": b"2"}
    assert result.missing == {}
    sleep_fn.assert_called_once_with(15)


def test_fetch_with_rounds_keeps_successes_from_prior_round(window):
    """다음 round는 앞 round 성공분을 유지하고 누락 조각만 다시 받는다."""
    calls = []

    def fetch_fn(config, win, *, client, skip, expected_total):
        """첫 round만 실패하고 두 번째 round에서 바뀐 전체본을 반환한다."""
        calls.append(frozenset(skip))
        if len(calls) == 1:
            yield FetchResult(
                key="a", payload=b"old-a", error=None, expected_total=2
            )
            yield FetchResult(
                key="b",
                payload=None,
                error=FetchErrorKind.TRANSIENT,
                expected_total=None,
            )
        else:
            yield FetchResult(
                key="b", payload=b"new-b", error=None, expected_total=None
            )

    result = fetch_with_rounds(
        fetch_fn,
        _FakeBudgetConfig(),
        window,
        client=object(),
        sleep_fn=lambda seconds: None,
    )

    assert calls == [frozenset(), frozenset({"a"})]
    assert result.chunks == {"a": b"old-a", "b": b"new-b"}


def test_fetch_with_rounds_fatal_aborts_immediately_without_further_calls(window):
    call_count = 0

    def fetch_fn(config, win, *, client, skip, expected_total):
        nonlocal call_count
        call_count += 1
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)
        yield FetchResult(key="b", payload=None, error=FetchErrorKind.FATAL, expected_total=None)
        yield FetchResult(key="c", payload=b"3", error=None, expected_total=None)  # 도달하면 안 된다

    sleep_fn = MagicMock()
    result = fetch_with_rounds(
        fetch_fn, _FakeBudgetConfig(), window, client=object(), sleep_fn=sleep_fn,
    )

    assert call_count == 1
    assert result.chunks == {"a": b"1"}  # FATAL 이전에 성공한 조각은 남는다
    assert "c" not in result.chunks  # FATAL 뒤는 이터레이터를 더 당기지 않는다
    sleep_fn.assert_not_called()


def test_fetch_with_rounds_stops_new_calls_when_budget_exceeded(window):
    calls = []

    def fetch_fn(config, win, *, client, skip, expected_total):
        calls.append(1)
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)
        yield FetchResult(key="b", payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)

    # 라운드 0은 예산 안에서 끝나 재시도(TRANSIENT b)가 잡히지만, 라운드 1 시작 전
    # 체크에서 예산을 넘겨 새 호출을 막는다.
    now_values = iter([0, 0, 0, 0, 0])
    sleep_fn = MagicMock()
    result = fetch_with_rounds(
        fetch_fn, _FakeBudgetConfig(seconds=50), window, client=object(),
        sleep_fn=sleep_fn, now_fn=lambda: next(now_values, 100),
    )

    assert len(calls) == 1  # 라운드 1은 새 호출을 시작하지 못한다
    assert result.chunks == {"a": b"1"}
    assert result.missing == {"b": FetchErrorKind.TRANSIENT}


def test_fetch_with_rounds_marks_unvisited_planned_parts_missing(window):
    def fetch_fn(config, win, *, client, skip, expected_total):
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)

    result = fetch_with_rounds(
        fetch_fn,
        _FakeBudgetConfig(),
        window,
        client=object(),
        planned_parts=frozenset({"a", "b", "c"}),
    )

    assert result.chunks == {"a": b"1"}
    assert result.missing == {
        "b": FetchErrorKind.TRANSIENT,
        "c": FetchErrorKind.TRANSIENT,
    }


def test_fetch_with_rounds_calls_on_chunk_immediately_for_each_success(window):
    def fetch_fn(config, win, *, client, skip, expected_total):
        yield FetchResult(key="a", payload=b"1", error=None, expected_total=None)
        yield FetchResult(key="b", payload=b"2", error=None, expected_total=None)

    on_chunk = MagicMock()
    fetch_with_rounds(fetch_fn, _FakeBudgetConfig(), window, client=object(), on_chunk=on_chunk)

    assert on_chunk.call_args_list == [(("a", b"1"),), (("b", b"2"),)]
