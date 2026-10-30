"""storage.py의 S3 I/O를 moto로 검증한다."""

import io
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage
from core.gold_publication.canonical import sha256_hex
from core.source_snapshot_io import SourceSnapshotReadError
from core.kst import KST
from core.sources import LIVING_POPULATION_GRID
from core.testing import TEST_BUCKET, put_source_snapshot


def _s3():
    return boto3.client("s3", region_name="us-east-1")


class TestReadRealGridSilver:
    def test_reads_exact_authoritative_snapshot(self):
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)
        table = pa.table(
            {
                "CELL_ID": ["가가00000000", "가가00000001"],
                "SPOP": [10.0, 20.0],
            }
        )
        put_source_snapshot(LIVING_POPULATION_GRID, logical, table)

        result = storage.read_real_grid_silver(logical)

        assert sorted(result.column("CELL_ID").to_pylist()) == [
            "가가00000000",
            "가가00000001",
        ]

    def test_uses_latest_contiguous_correction(self):
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)
        put_source_snapshot(
            LIVING_POPULATION_GRID,
            logical,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]}),
        )
        put_source_snapshot(
            LIVING_POPULATION_GRID,
            logical,
            pa.table({"CELL_ID": ["가가00000001"], "SPOP": [20.0]}),
            revision_no=1,
        )

        result = storage.read_real_grid_silver(logical)

        assert result.column("CELL_ID").to_pylist() == ["가가00000001"]

    def test_ignores_unpublished_immutable_silver(self):
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)
        table = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [99.0]})
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        checksum = sha256_hex(buffer.getvalue())
        _s3().put_object(
            Bucket=TEST_BUCKET,
            Key=(
                "silver/living_population_grid/dt=2026-08-11/hh=03/0300/"
                f"sha256={checksum}.parquet"
            ),
            Body=buffer.getvalue(),
        )

        assert storage.read_real_grid_silver(logical) is None

    def test_returns_none_when_authority_missing(self):
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)

        assert storage.read_real_grid_silver(logical) is None

    def test_raises_when_authoritative_silver_is_corrupted(self):
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)
        key = put_source_snapshot(
            LIVING_POPULATION_GRID,
            logical,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]}),
        ).silver_key
        _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=b"corrupted")

        with pytest.raises(SourceSnapshotReadError, match="checksum"):
            storage.read_real_grid_silver(logical)

    def test_raises_when_authority_revision_chain_starts_at_one(self):
        """손상된 authority chain을 단순 snapshot 부재로 축소하지 않는다."""
        logical = datetime(2026, 8, 11, 3, tzinfo=KST)
        put_source_snapshot(
            LIVING_POPULATION_GRID,
            logical,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]}),
            revision_no=1,
        )

        with pytest.raises(SourceSnapshotReadError, match="0부터 연속"):
            storage.read_real_grid_silver(logical)


class TestListArchiveDates:
    def test_returns_sorted_dates_of_existing_archive_files(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_archive(date(2026, 8, 12), table)
        storage.write_archive(date(2026, 8, 5), table)

        assert storage.list_archive_dates() == [date(2026, 8, 5), date(2026, 8, 12)]

    def test_returns_empty_list_when_nothing_archived(self):
        assert storage.list_archive_dates() == []


class TestArchive:
    def test_write_then_read_archive_roundtrip(self):
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})

        key = storage.write_archive(date(2026, 8, 11), table)

        assert key == "archive/living_population_grid/dt=2026-08-11.parquet"
        result = storage.read_archive(date(2026, 8, 11))
        assert result.column("CELL_ID").to_pylist() == ["가가00000000"]

    def test_read_archive_returns_none_when_missing(self):
        assert storage.read_archive(date(2026, 8, 11)) is None


class TestNowcastFile:
    def test_write_nowcast_key_and_roundtrip(self):
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42.0], "is_estimated": [True]})

        key = storage.write_nowcast(date(2026, 8, 13), table)

        assert key == "silver/living_population_grid/dt=2026-08-13/hh=00/nowcast.parquet"
        stored = pq.read_table(io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()))
        assert stored.column("SPOP").to_pylist() == [42.0]

    def test_nowcast_exists_true_and_false(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_nowcast(date(2026, 8, 13), table)

        assert storage.nowcast_exists(date(2026, 8, 13)) is True
        assert storage.nowcast_exists(date(2026, 8, 14)) is False

    def test_delete_nowcast_removes_file(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_nowcast(date(2026, 8, 13), table)

        storage.delete_nowcast(date(2026, 8, 13))

        assert storage.nowcast_exists(date(2026, 8, 13)) is False

    def test_delete_nowcast_is_idempotent_when_missing(self):
        storage.delete_nowcast(date(2026, 8, 13))  # 에러 없이 통과해야 함
