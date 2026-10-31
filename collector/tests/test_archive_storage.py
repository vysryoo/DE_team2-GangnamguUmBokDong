"""archive 계층 S3 I/O를 moto로 검증한다.

경로 규칙은 nowcaster가 먼저 쓰기 시작한 `archive/{source_id}/dt=YYYY-MM-DD.parquet`을
그대로 따른다(`nowcaster/storage.py:33`). 두 모듈이 같은 계층을 쓰므로 이 형식은
문자열 계약으로 고정한다 — 바뀌면 nowcaster가 조용히 못 읽게 된다.
"""

from datetime import date, datetime

import boto3
import pyarrow as pa
import pytest

import storage as storage_module
from storage import (
    list_silver_objects,
    read_archive_manifest,
    write_archive,
    write_archive_manifest,
)
from core.kst import KST
from core.testing import TEST_BUCKET, put_source_snapshot

pytestmark = pytest.mark.usefixtures("s3_bucket")

DAY = date(2026, 8, 12)


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _table():
    return pa.table({"a": [1, 2], "b": ["x", "y"]})


class TestArchiveKey:
    def test_key_format_is_the_nowcaster_contract(self):
        key = write_archive("living_population_grid", DAY, _table())

        assert key == "archive/living_population_grid/dt=2026-08-12.parquet"

    def test_no_hh_partition_unlike_silver(self):
        key = write_archive("test_source", DAY, _table())

        assert "hh=" not in key

    def test_written_object_is_readable_parquet(self):
        import io

        import pyarrow.parquet as pq

        key = write_archive("test_source", DAY, _table())

        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()

        assert pq.read_table(io.BytesIO(body)).equals(_table())


class TestArchiveManifest:
    def test_round_trip(self):
        data = {"source_id": "test_source", "date": "2026-08-12", "rows": 7}

        write_archive_manifest("test_source", DAY, data)

        assert read_archive_manifest("test_source", DAY) == data

    def test_missing_manifest_returns_none(self):
        assert read_archive_manifest("test_source", DAY) is None

    def test_manifest_key_is_under_archive_manifest_prefix(self):
        write_archive_manifest("test_source", DAY, {"rows": 1})

        keys = [o["Key"] for o in _s3().list_objects_v2(Bucket=TEST_BUCKET)["Contents"]]

        assert "_archive_manifest/test_source/dt=2026-08-12.json" in keys


def _put_silver(day: int) -> str:
    return put_source_snapshot(
        "test_source", datetime(2026, 8, day, 14, 10, tzinfo=KST), _table()
    ).silver_key


class TestListSilverObjects:
    def test_returns_size_and_last_modified_not_just_keys(self):
        key = _put_silver(12)

        objs = list_silver_objects("test_source", DAY)

        assert len(objs) == 1
        assert objs[0].key == key
        assert objs[0].size > 0
        assert objs[0].last_modified is not None

    def test_only_that_days_objects(self):
        key = _put_silver(12)
        _put_silver(13)

        objs = list_silver_objects("test_source", DAY)

        assert [o.key for o in objs] == [key]

    def test_empty_day_returns_empty_list(self):
        assert list_silver_objects("test_source", DAY) == []

    def test_ignores_non_parquet_objects(self):
        key = _put_silver(12)
        _s3().put_object(
            Bucket=TEST_BUCKET,
            Key="silver/test_source/dt=2026-08-12/hh=00/nowcast.json",
            Body=b"{}",
        )

        objs = list_silver_objects("test_source", DAY)

        assert [o.key for o in objs] == [key]


class TestArchiveExists:
    def test_false_when_absent(self):
        assert storage_module.archive_exists("test_source", DAY) is False

    def test_true_after_write(self):
        write_archive("test_source", DAY, _table())

        assert storage_module.archive_exists("test_source", DAY) is True
