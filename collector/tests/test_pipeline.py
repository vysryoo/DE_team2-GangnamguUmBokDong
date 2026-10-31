"""#7 pipeline.py 테스트: 재개 분기 3가지, 완결도·폐기 게이트, FATAL 즉시 중단."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

import httpx
import pytest

import manifest as manifest_module
import pipeline
import storage
from adapters.base import FetchErrorKind, FetchResult, adapter
from config.schema import Policies, Quality, Schedule, SourceConfig
from config.schema import Storage as StorageConfig
from manifest import FailureReason, RunStatus, Stage
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)


def _config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_pipeline_adapter",
        "adapter_params": {},
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(
            bronze_format="json", silver_format="parquet", partition=("dt", "hh")
        ),
        "quality": Quality(
            max_drop_ratio=0.5, max_missing_ratio=0.0, allow_empty=False
        ),
        "policies": Policies(
            required_missing="drop_row",
            required_outlier="drop_row",
            optional_missing="keep_null",
            optional_outlier="set_null",
        ),
        "columns": {},
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


class _ScriptedAdapter:
    """라운드별로 다른 결과를 내도록 스크립트로 제어하는 테스트 어댑터."""

    fetch_calls: ClassVar[int] = 0
    results: ClassVar[list] = []  # list[list[FetchResult]] — 호출마다 하나씩 소비
    rows_by_key: ClassVar[dict] = {}  # chunk key -> normalize가 반환할 행 하나
    planned: ClassVar[frozenset[str] | None] = None

    @staticmethod
    def planned_parts(config, window):
        return _ScriptedAdapter.planned

    @staticmethod
    def fetch(config, window, *, client, skip=frozenset(), expected_total=None):
        _ScriptedAdapter.fetch_calls += 1
        results = _ScriptedAdapter.results.pop(0) if _ScriptedAdapter.results else []
        for r in results:
            if r.key not in skip:
                yield r

    @staticmethod
    def normalize(chunks, config):
        rows = []
        for chunk in chunks:
            key = chunk.decode()
            rows.append(_ScriptedAdapter.rows_by_key.get(key, {"k": key}))
        return rows


@pytest.fixture
def scripted_adapter(clean_adapter_registry, monkeypatch):
    _ScriptedAdapter.fetch_calls = 0
    _ScriptedAdapter.results = []
    _ScriptedAdapter.rows_by_key = {}
    _ScriptedAdapter.planned = None
    adapter("t_pipeline_adapter")(_ScriptedAdapter)
    return _ScriptedAdapter


@pytest.fixture
def client():
    return httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200)))


def _chunk(key):
    return key.encode()


class TestFreshFetchSuccess:
    """분기 3: manifest 없음 → 전체 fetch → 성공."""

    def test_writes_bronze_silver_and_completed_manifest(
        self, scripted_adapter, client
    ):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=None
                ),
                FetchResult(
                    key="b", payload=_chunk("b"), error=None, expected_total=None
                ),
            ]
        ]
        config = _config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SUCCEEDED
        assert result.stage == Stage.COMPLETED
        assert result.counts.fetched == 2
        assert result.counts.dropped == 0
        assert result.revision == 0
        assert result.artifacts.silver is not None
        assert set(result.artifacts.bronze.parts) == {"a", "b"}

        saved = manifest_module.load(config.source_id, WINDOW_START)
        assert saved.status == RunStatus.SUCCEEDED
        assert storage.read_bronze(
            config.source_id,
            WINDOW_START,
            ["a", "b"],
            result.artifacts.bronze.revision,
        ) == [
            _chunk("a"),
            _chunk("b"),
        ]

    def test_successful_empty_part_is_not_counted_as_missing(
        self, scripted_adapter, client, monkeypatch
    ):
        """INFO-200 같은 정상 빈 조각은 row 부족이 아니라 성공한 POI 조각이다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="poi-POI001",
                    payload=_chunk("row"),
                    error=None,
                    expected_total=None,
                ),
                FetchResult(
                    key="poi-POI002",
                    payload=_chunk("empty"),
                    error=None,
                    expected_total=None,
                ),
            ]
        ]
        scripted_adapter.rows_by_key = {"row": {"k": "row"}, "empty": None}

        original_normalize = scripted_adapter.normalize

        def normalize_without_empty(chunks, config):
            return [
                row for row in original_normalize(chunks, config) if row is not None
            ]

        monkeypatch.setattr(
            scripted_adapter, "normalize", staticmethod(normalize_without_empty)
        )
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=0.0, allow_empty=False
            )
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SUCCEEDED
        assert result.counts.expected is None
        assert result.counts.fetched == 1
        assert result.missing.parts == ()
        assert result.missing.basis == "parts"
        assert result.completeness == 1.0

    def test_round_retry_preserves_successes_from_prior_round(
        self, scripted_adapter, client
    ):
        """round retry는 기존 성공 조각을 유지하고 누락 조각만 다시 받는다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("old-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.TRANSIENT,
                    expected_total=None,
                ),
            ],
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ],
        ]

        result = pipeline.execute_window(
            _config(), WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )

        assert result.status is RunStatus.SUCCEEDED
        assert result.artifacts.bronze.revision == 0
        assert storage.read_bronze(
            "t_source", WINDOW_START, ["a", "b"], revision=0
        ) == [
            _chunk("old-a"),
            _chunk("new-b"),
        ]

    def test_all_sources_preserve_successes_from_prior_transient_round(
        self, scripted_adapter, client
    ):
        """고정 격자 source의 round retry는 이미 성공한 조각을 유지한다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("old-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.TRANSIENT,
                    expected_total=None,
                ),
            ],
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ],
        ]
        config = _config()

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )

        assert result.status is RunStatus.SUCCEEDED
        assert result.artifacts.bronze.revision == 0
        assert storage.read_bronze(
            "t_source", WINDOW_START, ["a", "b"], revision=0
        ) == [
            _chunk("old-a"),
            _chunk("new-b"),
        ]

    def test_unvisited_planned_part_fails_zero_missing_ratio_gate(
        self, scripted_adapter, client
    ):
        """iterator가 조기 종료돼 yield되지 않은 계획 part도 누락으로 판정한다."""
        scripted_adapter.planned = frozenset({"poi-POI001", "poi-POI002"})
        scripted_adapter.results = [
            [
                FetchResult(
                    key="poi-POI001",
                    payload=_chunk("row"),
                    error=None,
                    expected_total=None,
                )
            ]
        ]
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=0.0, allow_empty=False
            )
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.missing.parts == ("poi-POI002",)
        assert result.missing.basis == "parts"


class TestSkipBranch:
    """분기 1: stage=completed & 누락 없음 & !force → SKIPPED, 재호출 없음."""

    def test_returns_existing_manifest_without_refetching(
        self, scripted_adapter, client
    ):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)
        assert scripted_adapter.fetch_calls == 1

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SKIPPED
        assert scripted_adapter.fetch_calls == 1  # 다시 호출되지 않았다

    def test_completed_partial_keeps_existing_skip_contract(
        self, scripted_adapter, client
    ):
        """누락을 허용한 PARTIAL은 전역 의미를 바꾸지 않고 일반 재실행을 건너뛴다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=0.6, allow_empty=True
            )
        )
        partial = pipeline.execute_window(config, WINDOW_START, client=client)
        assert partial.status is RunStatus.PARTIAL

        skipped = pipeline.execute_window(config, WINDOW_START, client=client)

        assert skipped.status is RunStatus.SKIPPED
        assert scripted_adapter.fetch_calls == 1


class TestForceBranch:
    """분기 3 강제: --force는 완결 여부와 무관하게 새 revision을 수집한다."""

    def test_force_refetches_even_when_completed(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=None
                )
            ],
            [
                FetchResult(
                    key="z", payload=_chunk("z"), error=None, expected_total=None
                )
            ],
        ]
        config = _config()
        first = pipeline.execute_window(config, WINDOW_START, client=client)

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, force=True
        )

        assert scripted_adapter.fetch_calls == 2
        assert set(result.artifacts.bronze.parts) == {"z"}
        assert first.artifacts.bronze.revision == 0
        assert result.artifacts.bronze.revision == 1
        assert storage.read_bronze(
            config.source_id, WINDOW_START, ["a"], revision=0
        ) == [_chunk("a")]
        assert storage.read_bronze(
            config.source_id, WINDOW_START, ["z"], revision=1
        ) == [_chunk("z")]


class TestMissingRatioGate:
    """max_missing_ratio 초과 시 silver를 쓰지 않고 FAILED/fetch_error로 끝난다."""

    def test_exceeds_gate_fails_without_silver(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True)
        )

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None

    def test_within_gate_succeeds_as_partial(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=10
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        # expected_total=10, 받은 행 1개 -> missing_ratio = 1 - 1/10 = 0.9
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True
            )
        )

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.PARTIAL
        assert result.artifacts.silver is not None
        assert result.missing.parts == ("b",)

    @pytest.mark.parametrize(
        ("fetched_rows", "expected_status", "has_silver"),
        [
            pytest.param(17, RunStatus.PARTIAL, True, id="exactly-15-percent"),
            pytest.param(16, RunStatus.FAILED, False, id="above-15-percent"),
        ],
    )
    def test_configured_15_percent_fault_boundary(
        self,
        scripted_adapter,
        client,
        monkeypatch,
        fetched_rows,
        expected_status,
        has_silver,
    ):
        """15% 누락은 PARTIAL, 15%를 넘으면 FAILED로 닫힌다."""
        scripted_adapter.results = [[
            FetchResult(
                key="page=1",
                payload=_chunk("page=1"),
                error=None,
                expected_total=20,
            ),
            FetchResult(
                key="page=2",
                payload=None,
                error=FetchErrorKind.PERMANENT,
                expected_total=None,
            ),
        ]]
        monkeypatch.setattr(
            scripted_adapter,
            "normalize",
            staticmethod(
                lambda chunks, config: [
                    {"k": str(index)} for index in range(fetched_rows)
                ]
            ),
        )
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0,
                max_missing_ratio=0.15,
                allow_empty=True,
            )
        )

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )

        assert result.status == expected_status
        assert (result.artifacts.silver is not None) is has_silver
        if has_silver:
            assert result.completeness == pytest.approx(fetched_rows / 20)
        else:
            assert result.completeness is None

    def test_fetched_rows_above_expected_fail_closed_on_snapshot_drift(
        self,
        scripted_adapter,
        client,
        monkeypatch,
    ):
        """축소·재정렬 전 4행 Bronze와 새 expected=3을 정상 완료로 clamp하지 않는다."""
        scripted_adapter.results = [[
            FetchResult(key="old-pages", payload=_chunk("old-pages"), error=None, expected_total=3)
        ]]
        monkeypatch.setattr(
            scripted_adapter,
            "normalize",
            staticmethod(lambda chunks, config: [{"stationId": str(i)} for i in range(4)]),
        )
        # 누락 허용치를 100%로 둬도 overfetch는 비율 문제가 아니라 snapshot 계약
        # 위반이므로 반드시 실패해야 한다.
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=True)
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.counts.expected == 3
        assert result.counts.fetched == 4
        assert result.artifacts.silver is None

    def test_overfetch_within_tolerance_succeeds_for_append_only_source(
        self,
        scripted_adapter,
        client,
        monkeypatch,
    ):
        """카운트가 행 배열보다 하나 뒤처진 응답을 fetch 실패로 만들지 않는다.

        서울 API는 진행 중인 시간대를 조회하면 같은 본문 안에서 list_total_count가
        row 배열보다 적은 값을 주는 일이 있다(레코드 하나가 카운트 계산과 직렬화
        사이에 들어온 경우). 대여이력처럼 행이 쌓이기만 하는 소스에서 그 초과분은
        누락이 아니라 더 최신인 실제 레코드다.
        """
        scripted_adapter.results = [[
            FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=100)
        ]]
        monkeypatch.setattr(
            scripted_adapter,
            "normalize",
            staticmethod(lambda chunks, config: [{"k": str(i)} for i in range(101)]),
        )
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0,
                max_missing_ratio=0.0,
                allow_empty=True,
                max_overfetch_ratio=0.01,
            )
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.failure_reason is None
        assert result.artifacts.silver is not None
        # 카운트가 아니라 실제 행 배열을 이 window의 cardinality로 확정한다.
        # Gold의 SUCCEEDED 계약도 expected==fetched를 요구한다.
        assert result.counts.expected == 101
        assert result.counts.fetched == 101
        assert result.missing.rows == 0

    def test_default_tolerance_allows_small_overfetch(
        self,
        scripted_adapter,
        client,
        monkeypatch,
    ):
        """소스가 따로 지정하지 않아도 기본 허용치(10%) 안의 초과는 통과한다."""
        scripted_adapter.results = [[
            FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=100)
        ]]
        monkeypatch.setattr(
            scripted_adapter,
            "normalize",
            staticmethod(lambda chunks, config: [{"k": str(i)} for i in range(105)]),
        )

        result = pipeline.execute_window(_config(), WINDOW_START, client=client)

        assert result.failure_reason is None
        assert result.counts.expected == 105
        assert result.counts.fetched == 105

    def test_overfetch_beyond_tolerance_still_fails(
        self,
        scripted_adapter,
        client,
        monkeypatch,
    ):
        """허용치를 넘는 초과는 여전히 막는다 — 페이지 중복 병합 같은 진짜 사고다."""
        scripted_adapter.results = [[
            FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=100)
        ]]
        monkeypatch.setattr(
            scripted_adapter,
            "normalize",
            staticmethod(lambda chunks, config: [{"k": str(i)} for i in range(200)]),
        )
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0,
                max_missing_ratio=0.0,
                allow_empty=True,
                max_overfetch_ratio=0.01,
            )
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None


class TestFatalAbortsImmediately:
    def test_fatal_skips_gate_and_fails_with_fetch_error(
        self, scripted_adapter, client
    ):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=None
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.FATAL,
                    expected_total=None,
                ),
            ]
        ]
        config = _config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None


class TestBronzeReuseBranch:
    """분기 2: stage>=bronze_written & !force → bronze를 다시 읽고, fetch는 다시 하지 않는다."""

    def test_resumes_from_bronze_without_refetching(
        self, scripted_adapter, client, monkeypatch
    ):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        config = _config()

        # write_silver가 실패해 stage=bronze_written에서 멈춘 상황을 흉내낸다.
        original_write_silver = storage.write_immutable_silver

        def _boom(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(storage, "write_immutable_silver", _boom)
        first = pipeline.execute_window(config, WINDOW_START, client=client)
        assert first.status == RunStatus.FAILED
        assert first.failure_reason == FailureReason.STORAGE_ERROR
        assert first.stage == Stage.VALIDATED

        monkeypatch.setattr(storage, "write_immutable_silver", original_write_silver)
        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert (
            scripted_adapter.fetch_calls == 1
        )  # 두 번째 실행에서 다시 fetch하지 않았다
        assert result.status == RunStatus.SUCCEEDED
        assert result.stage == Stage.COMPLETED
        assert result.artifacts.bronze.revision == first.artifacts.bronze.revision


class TestFetchErrorRecovery:
    """fetch_error 재실행은 기존 Bronze를 재사용한다."""

    def test_retry_reuses_prior_revision_without_refetch(
        self, scripted_adapter, client
    ):
        """Airflow 재실행은 외부 API를 다시 호출하지 않고 부분 Bronze를 유지한다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("old-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config()
        failed = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )
        assert failed.failure_reason is FailureReason.FETCH_ERROR

        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ]
        ]
        recovered = pipeline.execute_window(config, WINDOW_START, client=client)

        assert recovered.status is RunStatus.FAILED
        assert recovered.failure_reason is FailureReason.FETCH_ERROR
        assert scripted_adapter.fetch_calls == 1
        assert failed.artifacts.bronze.revision == 0
        assert recovered.artifacts.bronze.revision == 0
        assert storage.read_bronze(
            config.source_id, WINDOW_START, ["a"], revision=0
        ) == [_chunk("old-a")]

    def test_retry_reuses_successful_parts(
        self, scripted_adapter, client
    ):
        """안정적인 조각 소스는 기존 성공분을 유지하고 누락분만 다시 받는다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("old-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config()
        failed = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )
        assert failed.failure_reason is FailureReason.FETCH_ERROR

        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ]
        ]
        recovered = pipeline.execute_window(config, WINDOW_START, client=client)

        assert recovered.status is RunStatus.FAILED
        assert scripted_adapter.fetch_calls == 1
        assert failed.artifacts.bronze.revision == 0
        assert recovered.artifacts.bronze.revision == 0
        assert storage.read_bronze(
            config.source_id, WINDOW_START, ["a"], revision=0
        ) == [_chunk("old-a")]

    def test_retry_without_known_missing_still_reuses_bronze(
        self, scripted_adapter, client
    ):
        """누락 key가 없어도 Airflow 재실행은 기존 Bronze를 재사용한다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="old", payload=_chunk("old"), error=None, expected_total=0
                )
            ]
        ]
        config = _config()
        failed = pipeline.execute_window(config, WINDOW_START, client=client)
        assert failed.failure_reason is FailureReason.FETCH_ERROR
        assert failed.missing.parts == ()

        scripted_adapter.results = [
            [
                FetchResult(
                    key="new", payload=_chunk("new"), error=None, expected_total=1
                )
            ]
        ]
        recovered = pipeline.execute_window(config, WINDOW_START, client=client)

        assert recovered.status is RunStatus.FAILED
        assert recovered.artifacts.bronze.parts == ("old",)
        assert failed.artifacts.bronze.revision == 0
        assert recovered.artifacts.bronze.revision == 0
        assert storage.read_bronze(
            config.source_id, WINDOW_START, ["old"], revision=0
        ) == [_chunk("old")]

    def test_daily_window_retry_does_not_refetch_current_snapshot(
        self, scripted_adapter, client, monkeypatch
    ):
        """일일 task 재실행은 실행 날짜와 무관하게 기존 Bronze를 재사용한다."""
        monkeypatch.setattr(pipeline, "_now", lambda: WINDOW_START.replace(hour=15))
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(schedule=Schedule(interval="1d"))
        failed = pipeline.execute_window(config, WINDOW_START, client=client)
        assert failed.failure_reason is FailureReason.FETCH_ERROR

        monkeypatch.setattr(
            pipeline, "_now", lambda: WINDOW_START.replace(day=13, hour=3)
        )
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ]
        ]
        blocked = pipeline.execute_window(config, WINDOW_START, client=client)

        assert blocked.status is RunStatus.FAILED
        assert blocked.failure_reason is FailureReason.FETCH_ERROR
        assert scripted_adapter.fetch_calls == 1
        assert blocked.artifacts.bronze.revision == failed.artifacts.bronze.revision

    def test_daily_window_does_not_refetch_within_same_kst_day(
        self, scripted_adapter, client, monkeypatch
    ):
        """같은 날 Airflow 재실행도 일일 전체본을 다시 수집하지 않는다."""
        now = WINDOW_START.replace(hour=15)
        monkeypatch.setattr(pipeline, "_now", lambda: now)
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("old-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(schedule=Schedule(interval="1d"))
        pipeline.execute_window(config, WINDOW_START, client=client)

        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("new-a"), error=None, expected_total=2
                ),
                FetchResult(
                    key="b", payload=_chunk("new-b"), error=None, expected_total=None
                ),
            ]
        ]
        recovered = pipeline.execute_window(config, WINDOW_START, client=client)

        assert recovered.status is RunStatus.FAILED
        assert recovered.failure_reason is FailureReason.FETCH_ERROR
        assert scripted_adapter.fetch_calls == 1


class TestDropRatioGate:
    def test_exceeds_drop_ratio_fails_with_quality_gate(self, scripted_adapter, client):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        scripted_adapter.rows_by_key = {"a": {"col": None}}
        config = _config(
            quality=Quality(
                max_drop_ratio=0.1, max_missing_ratio=1.0, allow_empty=True
            ),
            columns={"col": pipeline_make_required_str_spec()},
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.QUALITY_GATE
        assert result.artifacts.silver is None

    def test_quality_gate_retry_reuses_bronze(self, scripted_adapter, client):
        """fetch 정책은 deterministic 품질 실패에서 외부 API를 다시 호출하지 않는다."""
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        scripted_adapter.rows_by_key = {"a": {"col": None}}
        config = _config(
            quality=Quality(
                max_drop_ratio=0.0, max_missing_ratio=0.0, allow_empty=False
            ),
            columns={"col": pipeline_make_required_str_spec()},
        )
        first = pipeline.execute_window(config, WINDOW_START, client=client)
        assert first.failure_reason is FailureReason.QUALITY_GATE

        second = pipeline.execute_window(config, WINDOW_START, client=client)

        assert second.failure_reason is FailureReason.QUALITY_GATE
        assert scripted_adapter.fetch_calls == 1
        assert second.artifacts.bronze.revision == first.artifacts.bronze.revision


def pipeline_make_required_str_spec():
    from config.schema import ColumnSpec

    return ColumnSpec(types=("str",), required=True)


class TestLogging:
    """계획서 9절: 단계 경계마다 로그 한 줄, 행·조각 단위 로그는 없다."""

    def test_success_run_logs_bronze_written_validated_and_completed(
        self, scripted_adapter, client, caplog
    ):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=None
                ),
                FetchResult(
                    key="b", payload=_chunk("b"), error=None, expected_total=None
                ),
            ]
        ]
        config = _config()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        messages = [r.message for r in caplog.records]
        assert any("stage=bronze_written" in m for m in messages)
        assert any("stage=validated" in m for m in messages)
        assert any("stage=completed" in m for m in messages)
        assert all(r.levelname == "INFO" for r in caplog.records)

    def test_missing_chunks_logs_bronze_written_at_warning(
        self, scripted_adapter, client, caplog
    ):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=10
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True
            )
        )

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(
                config, WINDOW_START, client=client, sleep_fn=lambda s: None
            )

        bronze_record = next(
            r for r in caplog.records if "stage=bronze_written" in r.message
        )
        assert bronze_record.levelname == "WARNING"
        assert "missing=b" in bronze_record.message

    def test_missing_ratio_gate_failure_logs_error(
        self, scripted_adapter, client, caplog
    ):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True)
        )

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(
                config, WINDOW_START, client=client, sleep_fn=lambda s: None
            )

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) == 1
        assert "failure_reason=fetch_error" in error_records[0].message

    def test_fatal_logs_error_without_bronze_written_line(
        self, scripted_adapter, client, caplog
    ):
        scripted_adapter.results = [
            [
                FetchResult(
                    key="a", payload=_chunk("a"), error=None, expected_total=None
                ),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.FATAL,
                    expected_total=None,
                ),
            ]
        ]
        config = _config()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "ERROR"
        assert "failure_reason=fetch_error" in caplog.records[0].message

    def test_skip_branch_logs_single_info_line(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)
        caplog.clear()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "INFO"
        assert "status=skipped" in caplog.records[0].message

    def test_drop_ratio_gate_failure_logs_error_with_quality_gate(
        self, scripted_adapter, client, caplog
    ):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        scripted_adapter.rows_by_key = {"a": {"col": None}}
        config = _config(
            quality=Quality(
                max_drop_ratio=0.1, max_missing_ratio=1.0, allow_empty=True
            ),
            columns={"col": pipeline_make_required_str_spec()},
        )

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) == 1
        assert "failure_reason=quality_gate" in error_records[0].message


class TestEmptyResult:
    def test_zero_rows_with_allow_empty_is_empty_status(
        self, scripted_adapter, client, monkeypatch
    ):
        """Expected 0과 terminal part가 있는 0행만 confirmed EMPTY다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="page-00001-01000",
                    payload=b"confirmed-empty",
                    error=None,
                    expected_total=0,
                )
            ]
        ]
        monkeypatch.setattr(
            scripted_adapter, "normalize", staticmethod(lambda chunks, config: [])
        )
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=True)
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.EMPTY
        assert result.artifacts.silver is None

    def test_unknown_total_zero_rows_fails_instead_of_opening_empty_authority(
        self, scripted_adapter, client
    ):
        """Unknown-total 0행은 API pagination 완료 증거가 아니므로 EMPTY가 아니다."""
        scripted_adapter.results = [[]]
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=True)
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.QUALITY_GATE

    def test_zero_rows_without_allow_empty_fails_quality_gate(
        self, scripted_adapter, client
    ):
        scripted_adapter.results = [[]]
        config = _config(
            quality=Quality(
                max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=False
            )
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.QUALITY_GATE


class TestSourceSnapshotAuthority:
    """Gold downstream을 여는 immutable source manifest 경계를 검증한다."""

    def test_success_writes_revision_zero_manifest_with_actual_silver_identity(
        self, scripted_adapter, client
    ):
        """첫 완전 snapshot은 실제 Silver checksum을 가진 revision 0을 연다."""
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=1)]
        ]
        config = _config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)
        snapshots = manifest_module.load_source_snapshots(
            config.source_id, WINDOW_START
        )

        assert result.revision == 0
        assert len(snapshots) == 1
        source_manifest = snapshots[0].manifest
        assert source_manifest.revision_no == 0
        assert source_manifest.status.value == "succeeded"
        assert source_manifest.counts.fetched == 1
        assert source_manifest.planned_parts == ("a",)
        assert source_manifest.completed_parts == ("a",)
        assert result.artifacts.silver.endswith(
            f"sha256={source_manifest.silver_byte_sha256}.parquet"
        )

    def test_partial_never_writes_authority_manifest(self, scripted_adapter, client):
        """게이트 이내 PARTIAL Silver가 있어도 downstream authority는 열리지 않는다."""
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(
                    key="b",
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                ),
            ]
        ]
        config = _config(
            quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.6, allow_empty=True)
        )

        result = pipeline.execute_window(
            config, WINDOW_START, client=client, sleep_fn=lambda seconds: None
        )

        assert result.status is RunStatus.PARTIAL
        assert result.artifacts.silver is not None
        assert (
            manifest_module.load_source_snapshots(config.source_id, WINDOW_START) == ()
        )

    def test_probe_without_terminal_marker_fails_before_silver(
        self, scripted_adapter, client
    ):
        """Deadline 전에 sentinel을 못 본 probe 결과는 complete snapshot이 아니다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="page-00001-01000",
                    payload=b"row",
                    error=None,
                    expected_total=None,
                )
            ]
        ]
        config = _config(adapter_params={"pagination": "probe_until_empty"})

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status is RunStatus.FAILED
        assert result.failure_reason is FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None
        assert (
            manifest_module.load_source_snapshots(config.source_id, WINDOW_START) == ()
        )

    def test_probe_uses_full_normalized_cardinality_after_terminal_marker(
        self, scripted_adapter, client
    ):
        """Terminal marker의 round-local 값 대신 전체 Bronze 실제 행 수를 기록한다."""
        scripted_adapter.results = [
            [
                FetchResult(
                    key="page-00001-01000",
                    payload=b"row",
                    error=None,
                    expected_total=999,
                )
            ]
        ]
        config = _config(
            adapter_params={"pagination": "probe_until_empty"},
            columns={"k": pipeline_make_required_str_spec()},
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)
        snapshot = manifest_module.load_source_snapshots(
            config.source_id, WINDOW_START
        )[0]

        assert result.status is RunStatus.SUCCEEDED
        assert result.counts.expected == 1
        assert snapshot.manifest.counts.expected == 1

    def test_exact_replay_reuses_revision_and_changed_content_increments(
        self, scripted_adapter, client
    ):
        """Same content replay는 0을 유지하고 changed correction만 1을 만든다."""
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=1)],
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=1)],
            [FetchResult(key="z", payload=_chunk("z"), error=None, expected_total=1)],
        ]
        config = _config(columns={"k": pipeline_make_required_str_spec()})

        first = pipeline.execute_window(config, WINDOW_START, client=client)
        replay = pipeline.execute_window(
            config, WINDOW_START, client=client, force=True
        )
        correction = pipeline.execute_window(
            config, WINDOW_START, client=client, force=True
        )
        snapshots = manifest_module.load_source_snapshots(
            config.source_id, WINDOW_START
        )

        assert (first.revision, replay.revision, correction.revision) == (0, 0, 1)
        assert tuple(item.manifest.revision_no for item in snapshots) == (0, 1)
        assert (
            snapshots[0].manifest.silver_byte_sha256
            != snapshots[1].manifest.silver_byte_sha256
        )

    def test_authority_manifest_is_written_after_output_and_diagnostic_manifest(
        self, scripted_adapter, client, monkeypatch
    ):
        """Immutable success manifest가 output 완성 뒤 sequence의 마지막 write다."""
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=1)]
        ]
        config = _config()
        events = []
        original_silver = storage.write_immutable_silver
        original_diagnostic = manifest_module.save
        original_authority = storage.write_source_snapshot_manifest

        def write_silver(*args, **kwargs):
            """Immutable Silver write 순서를 기록한다."""
            events.append("silver")
            return original_silver(*args, **kwargs)

        def save_diagnostic(*args, **kwargs):
            """Mutable 진단 manifest write 순서를 기록한다."""
            events.append("diagnostic")
            return original_diagnostic(*args, **kwargs)

        def save_authority(*args, **kwargs):
            """Authority manifest write 순서를 기록한다."""
            events.append("authority")
            return original_authority(*args, **kwargs)

        monkeypatch.setattr(storage, "write_immutable_silver", write_silver)
        monkeypatch.setattr(manifest_module, "save", save_diagnostic)
        monkeypatch.setattr(storage, "write_source_snapshot_manifest", save_authority)

        pipeline.execute_window(config, WINDOW_START, client=client)

        assert events == ["silver", "diagnostic", "authority"]

    def test_authority_write_failure_revalidates_bronze_on_retry(
        self, scripted_adapter, client, monkeypatch
    ):
        """Last PUT 실패 뒤 Bronze를 재검증해야만 authority를 만들 수 있다."""
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=1)]
        ]
        config = _config()
        original_finalize = manifest_module.finalize_source_snapshot

        def fail_finalize(prepared):
            """첫 authority manifest last-write 실패를 재현한다."""
            raise RuntimeError("manifest put unavailable")

        monkeypatch.setattr(manifest_module, "finalize_source_snapshot", fail_finalize)
        failed = pipeline.execute_window(config, WINDOW_START, client=client)

        assert failed.status is RunStatus.FAILED
        assert failed.stage is Stage.VALIDATED
        assert failed.failure_reason is FailureReason.STORAGE_ERROR
        assert (
            manifest_module.load_source_snapshots(config.source_id, WINDOW_START) == ()
        )

        monkeypatch.setattr(
            manifest_module, "finalize_source_snapshot", original_finalize
        )
        recovered = pipeline.execute_window(config, WINDOW_START, client=client)

        assert recovered.status is RunStatus.SUCCEEDED
        assert recovered.stage is Stage.COMPLETED
        assert scripted_adapter.fetch_calls == 1
        snapshots = manifest_module.load_source_snapshots(
            config.source_id, WINDOW_START
        )
        assert tuple(item.manifest.revision_no for item in snapshots) == (0,)


