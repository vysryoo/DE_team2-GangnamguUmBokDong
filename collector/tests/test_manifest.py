"""manifest.py의 상태 어휘와 Manifest·RetryMarker 모델을 검증한다."""

from datetime import date, datetime

import manifest as manifest_module
import pytest
from manifest import (
    Artifacts,
    ColumnIssueCount,
    Counts,
    FailureReason,
    Manifest,
    Missing,
    RetryMarker,
    RunStatus,
    Stage,
    StageField,
    clear_retry_marker,
    load,
    load_retry_markers,
    save,
    save_retry_marker,
)
from pydantic import BaseModel, ValidationError
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)
WINDOW_END = datetime(2026, 8, 12, 14, 15, tzinfo=KST)


def _minimal_manifest(**overrides) -> Manifest:
    base = {
        "source_id": "test_source",
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "status": RunStatus.PARTIAL,
        "stage": Stage.COMPLETED,
        "started_at": WINDOW_START,
        "config_version": "sha256:abc",
    }
    base.update(overrides)
    return Manifest.model_validate(base)


class TestStage:
    def test_ordering(self):
        assert Stage.BRONZE_WRITTEN < Stage.VALIDATED < Stage.COMPLETED
        assert Stage.COMPLETED >= Stage.BRONZE_WRITTEN

    def test_serializes_to_lowercase_name(self):
        class Holder(BaseModel):
            stage: StageField

        dumped = Holder(stage=Stage.BRONZE_WRITTEN).model_dump(mode="json")
        assert dumped == {"stage": "bronze_written"}

    def test_deserializes_from_lowercase_name(self):
        class Holder(BaseModel):
            stage: StageField

        holder = Holder.model_validate({"stage": "completed"})
        assert holder.stage is Stage.COMPLETED

    def test_accepts_stage_instance_directly(self):
        class Holder(BaseModel):
            stage: StageField

        holder = Holder(stage=Stage.VALIDATED)
        assert holder.stage is Stage.VALIDATED


class TestRunStatus:
    def test_values_match_disk_contract(self):
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.SUCCEEDED.value == "succeeded"
        assert RunStatus.PARTIAL.value == "partial"
        assert RunStatus.FAILED.value == "failed"
        assert RunStatus.EMPTY.value == "empty"
        assert RunStatus.SKIPPED.value == "skipped"


class TestFailureReason:
    def test_values_match_disk_contract(self):
        assert FailureReason.FETCH_ERROR.value == "fetch_error"
        assert FailureReason.STORAGE_ERROR.value == "storage_error"
        assert FailureReason.QUALITY_GATE.value == "quality_gate"
        assert FailureReason.CONFIG_ERROR.value == "config_error"


class TestManifestModel:
    def test_minimal_construction(self):
        m = _minimal_manifest()
        assert m.attempt == 1
        assert m.revision == 0
        assert m.artifacts == Artifacts()
        assert m.counts == Counts()
        assert m.missing == Missing()

    def test_forbids_extra_key(self):
        with pytest.raises(ValidationError):
            _minimal_manifest(unknown_key="x")

    def test_full_example_from_plan_doc_validates(self):
        data = {
            "source_id": "bike_station_realtime",
            "window_start": "2026-08-12T14:10:00Z",
            "window_end": "2026-08-12T14:15:00Z",
            "status": "partial",
            "stage": "completed",
            "failure_reason": None,
            "attempt": 2,
            "revision": 1,
            "started_at": "2026-08-12T14:15:00Z",
            "ended_at": "2026-08-12T14:15:04Z",
            "duration_ms": 4310,
            "artifacts": {
                "bronze": {
                    "prefix": "s3://.../bronze/bike_station_realtime/dt=2026-08-12/hh=14/1410/",
                    "parts": ["page-00001-01000", "page-01001-02000", "page-02001-02765"],
                },
                "silver": "s3://.../silver/bike_station_realtime/dt=2026-08-12/hh=14/1410.parquet",
                "quarantine": "s3://.../quarantine/bike_station_realtime/dt=2026-08-12/hh=14/1410.jsonl",
            },
            "counts": {"expected": 2765, "fetched": 2765, "kept": 2740, "repaired": 31, "dropped": 25},
            "missing": {"parts": [], "rows": 0, "basis": "rows"},
            "drop_ratio": 0.009,
            "completeness": 0.991,
            "backfill_status": None,
            "column_issues": {
                "stationId": {"missing": 25, "outlier": 0, "type_error": 0},
                "parkingBikeTotCnt": {"missing": 3, "outlier": 28, "type_error": 0},
            },
            "policy_actions": {"drop_row": 25, "clip_to_range": 28, "set_null": 3},
            "config_version": "sha256:a3f9",
        }

        m = Manifest.model_validate(data)

        assert m.artifacts.bronze.parts == (
            "page-00001-01000",
            "page-01001-02000",
            "page-02001-02765",
        )
        assert m.column_issues["stationId"] == ColumnIssueCount(missing=25, outlier=0, type_error=0)
        assert m.stage is Stage.COMPLETED


class TestLoadSave:
    def test_save_then_load_round_trip(self):
        m = _minimal_manifest()

        save(m)
        loaded = load(m.source_id, m.window_start)

        assert loaded == m

    def test_load_missing_returns_none(self):
        assert load("never_saved", WINDOW_START) is None

    def test_load_keeps_bronze_revision_separate_from_default_authority_revision(
        self, monkeypatch
    ):
        """이전 manifest도 Bronze revision을 보존하고 authority는 기본값을 쓴다."""
        data = _minimal_manifest(revision=3).model_dump(mode="json")
        data.pop("revision")
        data["artifacts"]["bronze"] = {
            "prefix": "bronze/test_source/dt=2026-08-12/hh=14/1410/",
            "parts": ["page-1"],
            "revision": 3,
        }
        original = data.copy()
        monkeypatch.setattr(
            manifest_module.storage, "read_manifest", lambda *_args: data
        )

        loaded = load("test_source", WINDOW_START)

        assert loaded is not None
        assert loaded.revision == 0
        assert loaded.artifacts.bronze is not None
        assert loaded.artifacts.bronze.revision == 3
        assert loaded.artifacts.bronze.parts == ("page-1",)
        assert data == original

    def test_load_accepts_independent_authority_and_bronze_revisions(self, monkeypatch):
        """authority correction과 Hot Bronze revision은 서로 다른 ordinal이다."""
        data = _minimal_manifest(revision=2).model_dump(mode="json")
        data["artifacts"]["bronze"] = {
            "prefix": "bronze/test_source/dt=2026-08-12/hh=14/1410/",
            "parts": [],
            "revision": 3,
        }
        monkeypatch.setattr(
            manifest_module.storage, "read_manifest", lambda *_args: data
        )

        loaded = load("test_source", WINDOW_START)

        assert loaded is not None
        assert loaded.revision == 2
        assert loaded.artifacts.bronze is not None
        assert loaded.artifacts.bronze.revision == 3


class TestLoadWindowManifests:
    def test_returns_all_windows_within_the_day_parsed_as_models(self):
        first = _minimal_manifest(window_start=datetime(2026, 8, 12, 0, 5, tzinfo=KST))
        second = _minimal_manifest(window_start=datetime(2026, 8, 12, 23, 55, tzinfo=KST))
        save(first)
        save(second)

        result = manifest_module.load_window_manifests("test_source", date(2026, 8, 12))

        assert {m.window_start for m in result} == {
            first.window_start,
            second.window_start,
        }
        assert all(isinstance(m, Manifest) for m in result)

    def test_hour_filter_narrows_to_that_hour(self):
        in_hour = _minimal_manifest(window_start=datetime(2026, 8, 12, 7, 0, tzinfo=KST))
        other_hour = _minimal_manifest(window_start=datetime(2026, 8, 12, 8, 0, tzinfo=KST))
        save(in_hour)
        save(other_hour)

        result = manifest_module.load_window_manifests(
            "test_source", date(2026, 8, 12), "07"
        )

        assert [m.window_start for m in result] == [in_hour.window_start]

    def test_no_manifests_returns_empty_list(self):
        assert manifest_module.load_window_manifests("never_saved", date(2026, 8, 12)) == []


class TestSummarizeWindow:
    def test_empty_window_returns_zeroed_summary(self):
        summary = manifest_module.summarize_window([])

        assert summary == {
            "run_count": 0,
            "status_counts": {},
            "fetched_count": 0,
            "kept_count": 0,
            "dropped_count": 0,
            "repaired_count": 0,
        }

    def test_aggregates_status_counts_and_row_counts_across_runs(self):
        succeeded = _minimal_manifest(
            status=RunStatus.SUCCEEDED,
            counts=Counts(fetched=10, kept=9, repaired=1, dropped=1),
            drop_ratio=0.1,
            column_issues={
                "stationName": ColumnIssueCount(missing=2, outlier=0, type_error=0),
            },
        )
        failed = _minimal_manifest(
            status=RunStatus.FAILED,
            counts=Counts(fetched=10, kept=0, dropped=10),
            drop_ratio=1.0,
            column_issues={
                "rackTotCnt": ColumnIssueCount(missing=0, outlier=3, type_error=1),
            },
        )

        summary = manifest_module.summarize_window([succeeded, failed])

        assert summary["run_count"] == 2
        assert summary["status_counts"] == {"succeeded": 1, "failed": 1}
        assert summary["fetched_count"] == 20
        assert summary["kept_count"] == 9
        assert summary["dropped_count"] == 11
        assert summary["repaired_count"] == 1


class TestRetryMarker:
    def test_save_then_load_round_trip(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )

        save_retry_marker(marker)
        loaded = load_retry_markers("test_source")

        assert loaded == [marker]

    def test_default_attempts_is_one(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )
        assert marker.attempts == 1

    def test_clear_removes_marker(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )
        save_retry_marker(marker)

        clear_retry_marker("test_source", WINDOW_START)

        assert load_retry_markers("test_source") == []

    def test_load_with_no_markers_returns_empty_list(self):
        assert load_retry_markers("no_markers_here") == []
