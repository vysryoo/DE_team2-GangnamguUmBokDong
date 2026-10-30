"""Authoritative source snapshot S3 reader를 검증한다."""

import io
import json
from datetime import UTC, datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import EndpointConnectionError

from core.gold_publication.canonical import sha256_hex
from core.kst import KST
from core.source_snapshot_io import (
    PartialConsumptionPolicy,
    SourceDataStatus,
    SourceFreshness,
    SourceSnapshotNotFoundError,
    SourceSnapshotReadError,
    read_available_source_snapshot,
    read_exact_source_snapshot,
    read_exact_source_snapshot_manifest,
    read_latest_source_snapshot,
    read_partial_source_snapshot,
)
from core.testing import TEST_BUCKET, PutSourceSnapshot, put_source_snapshot


def _put_snapshot(
    logical: datetime,
    rows: list[dict],
    *,
    revision: int = 0,
    source_id: str = "population_realtime",
    manifest_rows: int | None = None,
) -> PutSourceSnapshot:
    """테스트용 content-addressed Parquet과 authority manifest를 쓴다."""
    return put_source_snapshot(
        source_id,
        logical,
        pa.Table.from_pylist(rows),
        revision_no=revision,
        manifest_rows=manifest_rows,
    )


def test_exact_window_selects_latest_contiguous_correction() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical, [{"value": 1}], revision=0)
    _put_snapshot(logical, [{"value": 2}], revision=1)

    result = read_exact_source_snapshot("population_realtime", logical)

    assert result.manifest.revision_no == 1
    assert result.table is not None
    assert result.table.column("value").to_pylist() == [2]


def test_exact_manifest_ref_stays_on_old_revision_after_correction() -> None:
    """고정된 URI+SHA는 같은 logical window의 후속 correction을 따라가지 않는다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    manifest_uri, manifest_sha256, _silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
        revision=0,
    )
    _put_snapshot(logical, [{"value": 2}], revision=1)

    pinned = read_exact_source_snapshot_manifest(
        manifest_uri,
        manifest_sha256,
    )

    assert pinned.manifest.revision_no == 0
    assert pinned.table is not None
    assert pinned.table.column("value").to_pylist() == [1]
    assert read_exact_source_snapshot(
        "population_realtime", logical
    ).manifest.revision_no == 1


def test_exact_manifest_ref_rejects_wrong_manifest_sha256() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    manifest_uri, _manifest_sha256, _silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
    )

    with pytest.raises(SourceSnapshotReadError, match="manifest"):
        read_exact_source_snapshot_manifest(manifest_uri, "0" * 64)


def test_exact_manifest_ref_requires_canonical_authority_uri() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    manifest_uri, manifest_sha256, _silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
    )
    noncanonical_uri = manifest_uri.replace("revision=0000000000", "revision=0")

    with pytest.raises(ValueError, match="canonical"):
        read_exact_source_snapshot_manifest(noncanonical_uri, manifest_sha256)


def test_exact_manifest_ref_rejects_key_body_source_mismatch() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    manifest_uri, manifest_sha256, _silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
    )
    source_key = manifest_uri.removeprefix(f"s3://{TEST_BUCKET}/")
    target_key = source_key.replace(
        "/population_realtime/",
        "/poi_master/",
    )
    client = boto3.client("s3", region_name="us-east-1")
    body = client.get_object(Bucket=TEST_BUCKET, Key=source_key)["Body"].read()
    client.put_object(Bucket=TEST_BUCKET, Key=target_key, Body=body)

    with pytest.raises(SourceSnapshotReadError, match="identity"):
        read_exact_source_snapshot_manifest(
            f"s3://{TEST_BUCKET}/{target_key}",
            manifest_sha256,
        )


def test_exact_manifest_ref_validates_linked_silver_checksum_and_row_count() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    checksum_uri, checksum_manifest_sha256, silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=b"mutated")

    with pytest.raises(SourceSnapshotReadError, match="checksum"):
        read_exact_source_snapshot_manifest(
            checksum_uri,
            checksum_manifest_sha256,
        )

    row_logical = logical + timedelta(minutes=1)
    row_uri, row_manifest_sha256, _row_silver_key = _put_snapshot(
        row_logical,
        [{"value": 1}],
        manifest_rows=2,
    )
    with pytest.raises(SourceSnapshotReadError, match="행 수"):
        read_exact_source_snapshot_manifest(row_uri, row_manifest_sha256)


def test_exact_manifest_ref_wraps_linked_silver_access_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """연결된 Silver GET의 botocore 오류를 source read 도메인 예외로 변환한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    manifest_uri, manifest_sha256, _silver_key = _put_snapshot(
        logical,
        [{"value": 1}],
    )

    def fail_get(_key: str) -> bytes | None:
        """Silver artifact transport 실패를 발생시킨다."""
        raise EndpointConnectionError(endpoint_url="https://s3.invalid")

    monkeypatch.setattr("core.source_snapshot_io.get_object_bytes", fail_get)

    with pytest.raises(SourceSnapshotReadError, match="Silver") as caught:
        read_exact_source_snapshot_manifest(manifest_uri, manifest_sha256)
    assert isinstance(caught.value.__cause__, EndpointConnectionError)


def test_exact_window_fails_when_content_bytes_change() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical, [{"value": 1}])
    manifest = read_exact_source_snapshot("population_realtime", logical).manifest
    assert manifest.silver_uri is not None
    key = manifest.silver_uri.split(f"s3://{TEST_BUCKET}/", 1)[1]
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=b"changed",
    )

    with pytest.raises(SourceSnapshotReadError, match="checksum"):
        read_exact_source_snapshot("population_realtime", logical)


def test_latest_window_is_bounded_and_ignores_future() -> None:
    cutoff = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    prior = cutoff - timedelta(minutes=5)
    _put_snapshot(prior, [{"value": 1}])
    _put_snapshot(cutoff + timedelta(minutes=5), [{"value": 2}])

    result = read_latest_source_snapshot(
        "population_realtime",
        cutoff,
        lookback=timedelta(hours=1),
    )

    assert result.manifest.logical_dttm == prior


def test_exact_window_requires_timezone_and_authority() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        read_exact_source_snapshot(
            "population_realtime",
            datetime(2026, 8, 20, 13, 50, tzinfo=UTC).replace(tzinfo=None),
        )
    with pytest.raises(SourceSnapshotNotFoundError):
        read_exact_source_snapshot(
            "population_realtime",
            datetime(2026, 8, 20, 13, 50, tzinfo=KST),
        )


def test_partial_window_uses_kst_path_and_validates_content_address() -> None:
    """UTC 인자를 받아도 KST diagnostic 경로를 찾고 artifact hash를 검증한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([{"value": 7}]), buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        f"silver/population_realtime/dt=2026-08-20/hh=13/1350/sha256={checksum}.parquet"
    )
    document = {
        "source_id": "population_realtime",
        "window_start": logical.isoformat(),
        "status": "partial",
        "stage": "completed",
        "failure_reason": None,
        "artifacts": {"silver": silver_key},
        "counts": {"kept": 1},
    }
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET,
        Key="_manifest/population_realtime/dt=2026-08-20/hh=13/1350.json",
        Body=json.dumps(document).encode(),
    )

    table = read_partial_source_snapshot(
        "population_realtime",
        logical.astimezone(UTC),
    )

    assert table.column("value").to_pylist() == [7]

    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=b"mutated")
    with pytest.raises(SourceSnapshotReadError, match="checksum"):
        read_partial_source_snapshot("population_realtime", logical)


def test_available_prefers_current_partial_to_past_complete() -> None:
    """현재 완전이 없으면 현재 PARTIAL을 과거 완전 성공보다 먼저 선택한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical - timedelta(minutes=5), [{"value": 1}])
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([{"value": 2}]), buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        "silver/population_realtime/dt=2026-08-20/hh=13/"
        f"1350/sha256={checksum}.parquet"
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET,
        Key="_manifest/population_realtime/dt=2026-08-20/hh=13/1350.json",
        Body=json.dumps(
            {
                "source_id": "population_realtime",
                "window_start": logical.isoformat(),
                "status": "partial",
                "stage": "completed",
                "failure_reason": None,
                "artifacts": {"silver": silver_key},
                "counts": {"kept": 1},
            }
        ).encode(),
    )

    selected = read_available_source_snapshot(
        "population_realtime",
        logical,
        lookback=timedelta(hours=1),
        partial_policy=PartialConsumptionPolicy.REPAIR,
    )

    assert selected.status is SourceDataStatus.PARTIAL
    assert selected.freshness is SourceFreshness.CURRENT
    assert selected.table.column("value").to_pylist() == [2]
    metadata = selected.selection_metadata(
        "population_realtime",
        logical,
        partial_policy=PartialConsumptionPolicy.REPAIR,
    )
    assert metadata.as_dict() == {
        "freshness": "current",
        "partial_policy": "repair",
        "requested_dttm": logical.astimezone(UTC).isoformat(),
        "resolution": "repaired",
        "selected_dttm": logical.astimezone(UTC).isoformat(),
        "source_id": "population_realtime",
        "status": "partial",
    }

    rejected = read_available_source_snapshot(
        "population_realtime", logical, lookback=timedelta(hours=1)
    )
    assert rejected.status is SourceDataStatus.SUCCESS
    assert rejected.freshness is SourceFreshness.STALE
    assert rejected.table.column("value").to_pylist() == [1]


def test_available_uses_past_complete_after_current_is_missing() -> None:
    """현재 완전·부분이 모두 없으면 freshness 안의 과거 성공을 선택한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    prior = logical - timedelta(minutes=5)
    _put_snapshot(prior, [{"value": 1}])

    selected = read_available_source_snapshot(
        "population_realtime", logical, lookback=timedelta(hours=1)
    )

    assert selected.status is SourceDataStatus.SUCCESS
    assert selected.freshness is SourceFreshness.STALE
    assert selected.logical_dttm == prior.astimezone(UTC)


def test_available_fails_when_current_and_bounded_past_are_unavailable() -> None:
    """현재 및 허용 lookback 안의 과거 성공이 없으면 명시적으로 실패한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical - timedelta(hours=2), [{"value": 1}])

    with pytest.raises(SourceSnapshotNotFoundError):
        read_available_source_snapshot(
            "population_realtime", logical, lookback=timedelta(hours=1)
        )
