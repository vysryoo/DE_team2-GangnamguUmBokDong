"""storage.py의 S3 I/O를 moto로 검증한다."""

import gzip
import json
import re
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core.gold_publication.errors import (
    ObjectChecksumMismatchError,
    ObjectCollisionError,
)
from storage import (
    latest_source_snapshot_logical_dttm,
    list_source_snapshot_windows,
    list_window_manifest_payloads,
    next_bronze_revision,
    read_bronze,
    read_immutable_silver_artifact,
    read_manifest,
    write_bronze_part,
    write_immutable_silver,
    write_manifest,
    write_quarantine,
    write_source_snapshot_manifest,
)
from core.kst import KST
from core.testing import TEST_BUCKET

pytestmark = pytest.mark.usefixtures("s3_bucket")

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)


def _s3():
    return boto3.client("s3", region_name="us-east-1")


class TestBronzeRoundTrip:
    def test_write_then_read_in_parts_order(self):
        write_bronze_part("test_source", WINDOW_START, "page-002", b'{"b": 2}')
        write_bronze_part("test_source", WINDOW_START, "page-001", b'{"a": 1}')

        result = read_bronze("test_source", WINDOW_START, ["page-001", "page-002"])

        assert result == [b'{"a": 1}', b'{"b": 2}']

    def test_bytes_round_trip_exactly(self):
        raw = b'{"weird": "\\u00e9", "n": 1}'
        write_bronze_part("test_source", WINDOW_START, "page-001", raw)

        result = read_bronze("test_source", WINDOW_START, ["page-001"])

        assert result == [raw]

    def test_stored_object_is_gzip_compressed(self):
        raw = b"plain bytes, not json necessarily"
        write_bronze_part("test_source", WINDOW_START, "page-001", raw)

        key = "bronze/test_source/dt=2026-08-12/hh=14/1410/part=page-001.json.gz"
        stored = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()

        assert gzip.decompress(stored) == raw

    def test_read_ignores_objects_not_listed_in_parts(self):
        write_bronze_part("test_source", WINDOW_START, "page-001", b"keep")
        write_bronze_part("test_source", WINDOW_START, "page-002", b"ignore-me")

        result = read_bronze("test_source", WINDOW_START, ["page-001"])

        assert result == [b"keep"]

    def test_immutable_hot_revisions_preserve_changed_payloads(self):
        """강제 재수집은 기존 Hot Bronze를 덮어쓰지 않고 새 revision에 쓴다."""
        write_bronze_part(
            "test_source", WINDOW_START, "page-001", b"first", revision=0
        )
        write_bronze_part(
            "test_source", WINDOW_START, "page-001", b"corrected", revision=1
        )

        assert read_bronze(
            "test_source", WINDOW_START, ["page-001"], revision=0
        ) == [b"first"]
        assert read_bronze(
            "test_source", WINDOW_START, ["page-001"], revision=1
        ) == [b"corrected"]

    def test_immutable_hot_revision_rejects_key_collision(self):
        """같은 revision과 part key에 다른 원본을 덮어쓸 수 없다."""
        write_bronze_part(
            "test_source", WINDOW_START, "page-001", b"first", revision=0
        )

        with pytest.raises(RuntimeError, match="immutable Hot Bronze key 충돌"):
            write_bronze_part(
                "test_source", WINDOW_START, "page-001", b"changed", revision=0
            )

    def test_next_revision_skips_orphan_written_before_manifest(self):
        """중간 장애로 manifest 없는 object가 남아도 그 revision을 재사용하지 않는다."""
        write_bronze_part(
            "test_source", WINDOW_START, "page-001", b"orphan", revision=3
        )

        assert next_bronze_revision("test_source", WINDOW_START) == 4


class TestWriteImmutableSilver:
    """Content-addressed Silver의 put-once와 readback을 검증한다."""

    def test_writes_parquet_and_returns_key(self):
        """Parquet bytes를 hash segment가 있는 object에 기록한다."""
        table = pa.table({"stationId": ["ST-1", "ST-2"], "rackTotCnt": [10, 20]})

        key = write_immutable_silver("test_source", WINDOW_START, table).key

        assert re.fullmatch(
            r"silver/test_source/dt=2026-08-12/hh=14/1410/"
            r"sha256=[0-9a-f]{64}\.parquet",
            key,
        )
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
        roundtrip = pq.read_table(pa.BufferReader(body))
        assert roundtrip.to_pydict() == table.to_pydict()

    def test_changed_bytes_use_a_new_key_without_overwrite(self):
        """Correction parquet은 기존 URI를 덮어쓰지 않고 새 content key를 쓴다."""
        table1 = pa.table({"x": [1]})
        table2 = pa.table({"x": [1, 2]})

        key1 = write_immutable_silver("test_source", WINDOW_START, table1).key
        key2 = write_immutable_silver("test_source", WINDOW_START, table2).key

        assert key1 != key2
        first = _s3().get_object(Bucket=TEST_BUCKET, Key=key1)["Body"].read()
        second = _s3().get_object(Bucket=TEST_BUCKET, Key=key2)["Body"].read()
        assert pq.read_table(pa.BufferReader(first)).num_rows == 1
        assert pq.read_table(pa.BufferReader(second)).num_rows == 2

    def test_same_bytes_replay_the_same_immutable_key(self):
        """동일 table 재실행은 같은 object의 안전한 replay다."""
        table = pa.table({"x": [1]})

        first = write_immutable_silver("test_source", WINDOW_START, table).key
        second = write_immutable_silver("test_source", WINDOW_START, table).key

        assert second == first

    def test_collision_does_not_overwrite_existing_key(self):
        """Content key에 다른 bytes가 있으면 put-once가 hard fail한다."""
        table = pa.table({"x": [1]})
        key = write_immutable_silver("test_source", WINDOW_START, table).key
        _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=b"corrupt")

        with pytest.raises(ObjectCollisionError):
            write_immutable_silver("test_source", WINDOW_START, table)

        assert (
            _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read() == b"corrupt"
        )

    def test_readback_rejects_checksum_mismatch(self):
        """URI hash와 실제 Silver bytes가 다르면 authority 복원이 실패한다."""
        table = pa.table({"x": [1]})
        key = write_immutable_silver("test_source", WINDOW_START, table).key
        _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=b"corrupt")

        with pytest.raises(ObjectChecksumMismatchError):
            read_immutable_silver_artifact(key, row_count=1)


class TestWriteQuarantine:
    def test_empty_rows_writes_nothing_and_returns_none(self):
        result = write_quarantine("test_source", WINDOW_START, [])

        assert result is None
        listed = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="quarantine/test_source/"
        )
        assert listed.get("KeyCount", 0) == 0

    def test_nonempty_rows_writes_jsonl(self):
        rows = [
            {"stationId": "ST-1", "_issues": ["missing:rackTotCnt"]},
            {"stationId": "ST-2"},
        ]

        key = write_quarantine("test_source", WINDOW_START, rows)

        assert key == "quarantine/test_source/dt=2026-08-12/hh=14/1410.jsonl"
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read().decode()
        lines = [json.loads(line) for line in body.strip().split("\n")]
        assert lines == rows


class TestManifestRawIO:
    def test_write_then_read_round_trip(self):
        data = {"source_id": "test_source", "status": "succeeded"}

        write_manifest("test_source", WINDOW_START, data)
        result = read_manifest("test_source", WINDOW_START)

        assert result == data

    def test_read_missing_returns_none(self):
        assert read_manifest("never_written", WINDOW_START) is None


class TestListWindowManifestPayloads:
    def test_lists_all_windows_within_the_day(self):
        write_manifest(
            "test_source", datetime(2026, 8, 12, 0, 5, tzinfo=KST), {"hh": "00"}
        )
        write_manifest(
            "test_source", datetime(2026, 8, 12, 23, 55, tzinfo=KST), {"hh": "23"}
        )
        # 다른 날짜는 포함되지 않는다.
        write_manifest(
            "test_source", datetime(2026, 8, 13, 0, 5, tzinfo=KST), {"hh": "00-next-day"}
        )

        result = list_window_manifest_payloads("test_source", date(2026, 8, 12))

        assert sorted(d["hh"] for d in result) == ["00", "23"]

    def test_hour_filter_narrows_to_that_hour_only(self):
        write_manifest(
            "test_source", datetime(2026, 8, 12, 7, 0, tzinfo=KST), {"hh": "07"}
        )
        write_manifest(
            "test_source", datetime(2026, 8, 12, 7, 55, tzinfo=KST), {"hh": "07-late"}
        )
        write_manifest(
            "test_source", datetime(2026, 8, 12, 8, 0, tzinfo=KST), {"hh": "08"}
        )

        result = list_window_manifest_payloads("test_source", date(2026, 8, 12), "07")

        assert sorted(d["hh"] for d in result) == ["07", "07-late"]

    def test_no_manifests_returns_empty_list(self):
        assert list_window_manifest_payloads("never_written", date(2026, 8, 12)) == []


class TestSourceSnapshotWindowListing:
    """UTC manifest layout을 KST compaction 날짜의 logical window로 발견한다."""

    def test_lists_both_utc_partitions_and_deduplicates_revisions(self) -> None:
        """KST 하루 양끝 window와 여러 revision을 오름차순 한 번씩 반환한다."""
        midnight = datetime(2026, 8, 12, 0, 0, tzinfo=KST)
        late = datetime(2026, 8, 12, 23, 55, tzinfo=KST)
        before = datetime(2026, 8, 11, 23, 55, tzinfo=KST)
        after = datetime(2026, 8, 13, 0, 0, tzinfo=KST)
        write_source_snapshot_manifest("test_source", midnight, 0, b"{}")
        write_source_snapshot_manifest("test_source", midnight, 1, b'{"v":1}')
        write_source_snapshot_manifest("test_source", late, 0, b"{}")
        write_source_snapshot_manifest("test_source", before, 0, b"{}")
        write_source_snapshot_manifest("test_source", after, 0, b"{}")

        result = list_source_snapshot_windows("test_source", date(2026, 8, 12))

        assert result == [midnight, late]

    def test_does_not_mix_sources(self) -> None:
        """같은 logical time의 다른 source manifest를 포함하지 않는다."""
        write_source_snapshot_manifest("source_a", WINDOW_START, 0, b"{}")
        write_source_snapshot_manifest("source_b", WINDOW_START, 0, b"{}")

        result = list_source_snapshot_windows("source_a", date(2026, 8, 12))

        assert result == [WINDOW_START]

    def test_returns_empty_when_date_has_no_manifests(self) -> None:
        """해당 KST 날짜에 manifest가 없으면 빈 목록을 반환한다."""
        assert list_source_snapshot_windows("test_source", date(2026, 8, 12)) == []

    def test_rejects_partition_mismatched_manifest_key(self) -> None:
        """UTC partition이 logical time과 다른 key를 authority로 발견하지 않는다."""
        key = (
            "source_snapshot_manifest/test_source/dt=2026-08-12/hh=13/"
            "logical=20260812T140000000000Z/revision=0000000000.json"
        )
        _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=b"{}")

        with pytest.raises(ValueError, match="파티션"):
            list_source_snapshot_windows("test_source", date(2026, 8, 12))


class TestLatestSourceSnapshotLogicalDttm:
    """Airflow freshness gate가 쓰는 '마지막 성공 수집 시각' 조회를 검증한다."""

    def test_returns_latest_window_at_or_before_as_of(self) -> None:
        older = datetime(2026, 8, 12, 10, 0, tzinfo=KST)
        newer = datetime(2026, 8, 12, 10, 10, tzinfo=KST)
        future = datetime(2026, 8, 12, 10, 20, tzinfo=KST)
        write_source_snapshot_manifest("test_source", older, 0, b"{}")
        write_source_snapshot_manifest("test_source", newer, 0, b"{}")
        write_source_snapshot_manifest("test_source", future, 0, b"{}")

        result = latest_source_snapshot_logical_dttm(
            "test_source", as_of=datetime(2026, 8, 12, 10, 15, tzinfo=KST)
        )

        assert result == newer

    def test_falls_back_to_previous_kst_day_right_after_midnight(self) -> None:
        late_yesterday = datetime(2026, 8, 11, 23, 55, tzinfo=KST)
        write_source_snapshot_manifest("test_source", late_yesterday, 0, b"{}")

        result = latest_source_snapshot_logical_dttm(
            "test_source", as_of=datetime(2026, 8, 12, 0, 2, tzinfo=KST)
        )

        assert result == late_yesterday

    def test_returns_none_when_nothing_found(self) -> None:
        result = latest_source_snapshot_logical_dttm(
            "test_source", as_of=datetime(2026, 8, 12, 10, 15, tzinfo=KST)
        )

        assert result is None


