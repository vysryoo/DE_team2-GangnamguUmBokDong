"""Collector source snapshot revision chain과 immutable manifest 저장을 검증한다."""

from __future__ import annotations

from datetime import datetime, timedelta

import manifest as manifest_module
import pyarrow as pa
import pytest
import storage
from core.gold_publication.errors import ObjectCollisionError
from core.source_snapshot import SourceSnapshotContractError, SourceSnapshotStatus
from manifest import Counts
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)
PARTS = ("page-00001-00001",)


def _silver(values: list[int]) -> storage.ImmutableSilverArtifact:
    """테스트용 content-addressed Silver artifact를 기록한다."""
    return storage.write_immutable_silver(
        "test_source", WINDOW_START, pa.table({"value": values})
    )


def _publish(**fields) -> manifest_module.PublishedSourceSnapshot:
    """authority manifest를 준비한 뒤 확정한다."""
    return manifest_module.finalize_source_snapshot(
        manifest_module.prepare_source_snapshot(**fields)
    )


def _publish_succeeded(
    artifact: storage.ImmutableSilverArtifact,
    *,
    config_version: str = "v1",
) -> manifest_module.PublishedSourceSnapshot:
    """테스트용 SUCCEEDED authority manifest를 게시한다."""
    return _publish(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=config_version,
        silver=artifact,
        counts=Counts(
            expected=artifact.row_count,
            fetched=artifact.row_count,
            kept=artifact.row_count,
            repaired=0,
            dropped=0,
        ),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )


def test_first_success_is_revision_zero_and_replay_is_exact() -> None:
    """첫 성공은 0이고 같은 source content 재실행은 manifest를 늘리지 않는다."""
    artifact = _silver([1])

    first = _publish_succeeded(artifact)
    replay = _publish_succeeded(artifact)
    loaded = manifest_module.load_source_snapshots("test_source", WINDOW_START)

    assert first == replay
    assert first.manifest.revision_no == 0
    assert loaded == (first,)


def test_changed_silver_gets_next_revision_without_overwriting_first() -> None:
    """Changed source content는 revision 1을 쓰고 revision 0 bytes를 보존한다."""
    first = _publish_succeeded(_silver([1]))
    correction = _publish_succeeded(_silver([1, 2]))
    loaded = manifest_module.load_source_snapshots("test_source", WINDOW_START)

    assert correction.manifest.revision_no == 1
    assert tuple(item.manifest.revision_no for item in loaded) == (0, 1)
    assert loaded[0] == first
    assert loaded[0].manifest.silver_uri != loaded[1].manifest.silver_uri


def test_logical_times_within_same_minute_have_independent_revision_chains() -> None:
    """초·microsecond가 다른 logical time은 같은 minute prefix에서 섞이지 않는다."""
    artifact = _silver([1])
    _publish_succeeded(artifact)
    other_logical = WINDOW_START + timedelta(seconds=1, microseconds=2)

    other = _publish(
        source_id="test_source",
        logical_dttm=other_logical,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="v1",
        silver=artifact,
        counts=Counts(expected=1, fetched=1, kept=1, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )

    assert other.manifest.revision_no == 0
    assert (
        manifest_module.load_source_snapshots("test_source", WINDOW_START)[
            0
        ].manifest.logical_dttm
        == WINDOW_START
    )
    assert manifest_module.load_source_snapshots("test_source", other_logical) == (
        other,
    )


def test_succeeded_to_confirmed_empty_gets_higher_revision() -> None:
    """같은 logical source의 SUCCEEDED→EMPTY correction은 revision을 증가시킨다."""
    _publish_succeeded(_silver([1]))

    empty = _publish(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.EMPTY,
        config_version="v1",
        silver=None,
        counts=Counts(expected=0, fetched=0, kept=0, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )

    assert empty.manifest.status is SourceSnapshotStatus.EMPTY
    assert empty.manifest.revision_no == 1


def test_confirmed_empty_to_succeeded_gets_higher_revision() -> None:
    """같은 logical source의 EMPTY→SUCCEEDED correction도 revision을 증가시킨다."""
    _publish(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.EMPTY,
        config_version="v1",
        silver=None,
        counts=Counts(expected=0, fetched=0, kept=0, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )

    succeeded = _publish_succeeded(_silver([1]))

    assert succeeded.manifest.status is SourceSnapshotStatus.SUCCEEDED
    assert succeeded.manifest.revision_no == 1


def test_two_different_writers_for_same_next_revision_collide() -> None:
    """동시 correction이 같은 ordinal에 다른 bytes를 쓰면 덮어쓰지 않는다."""
    _publish_succeeded(_silver([1]))
    artifact = _silver([1, 2])
    first_candidate = manifest_module.prepare_source_snapshot(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="v2",
        silver=artifact,
        counts=Counts(expected=2, fetched=2, kept=2, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )
    competing_candidate = manifest_module.prepare_source_snapshot(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="v3",
        silver=artifact,
        counts=Counts(expected=2, fetched=2, kept=2, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )

    manifest_module.finalize_source_snapshot(first_candidate)

    with pytest.raises(ObjectCollisionError):
        manifest_module.finalize_source_snapshot(competing_candidate)


def test_revision_gap_is_rejected() -> None:
    """Revision 0 없이 1만 존재하는 authority chain을 fail closed한다."""
    artifact = _silver([1])
    candidate = manifest_module.prepare_source_snapshot(
        source_id="test_source",
        logical_dttm=WINDOW_START,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="v1",
        silver=artifact,
        counts=Counts(expected=1, fetched=1, kept=1, repaired=0, dropped=0),
        planned_parts=PARTS,
        completed_parts=PARTS,
    )
    revision_one = candidate.manifest.__class__(
        schema_version=candidate.manifest.schema_version,
        source_id=candidate.manifest.source_id,
        logical_dttm=candidate.manifest.logical_dttm,
        revision_no=1,
        status=candidate.manifest.status,
        config_version=candidate.manifest.config_version,
        silver_uri=candidate.manifest.silver_uri,
        silver_byte_sha256=candidate.manifest.silver_byte_sha256,
        counts=candidate.manifest.counts,
        planned_parts=candidate.manifest.planned_parts,
        completed_parts=candidate.manifest.completed_parts,
    )
    storage.write_source_snapshot_manifest(
        "test_source", WINDOW_START, 1, revision_one.canonical_bytes
    )

    with pytest.raises(SourceSnapshotContractError, match="0부터"):
        manifest_module.load_source_snapshots("test_source", WINDOW_START)
