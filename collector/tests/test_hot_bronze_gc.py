"""Cold inventory 기반 Hot Bronze GC를 검증한다."""

from datetime import date, datetime, timedelta

import cold_bronze
import hot_bronze_gc
import pytest
import storage
from core.gold_publication.storage import ImmutableObjectError
from core.s3 import list_keys, put_object_bytes, read_json
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

DAY = date(2026, 8, 12)
WINDOW = datetime(2026, 8, 12, 14, 55, tzinfo=KST)


def test_deletes_only_hot_keys_verified_in_cold_inventory():
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    cold_bronze.compact_date("test_source", DAY)

    result = hot_bronze_gc.collect_date(
        "test_source", DAY, now=datetime.now(KST) + timedelta(days=1), retention_days=0
    )

    assert result.deleted == 1
    assert list_keys("bronze/hot/test_source/") == []
    manifest = read_json("_hot_gc_manifest/test_source/dt=2026-08-12.json")
    assert manifest["status"] == "completed"


def test_refuses_hot_object_missing_from_cold_inventory():
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    cold_bronze.compact_date("test_source", DAY)
    unknown = (
        "bronze/hot/test_source/dt=2026-08-12/hh=14/1455/"
        "revision=0000000001/part=page-001.json.gz"
    )
    put_object_bytes(unknown, b"not-compacted")

    with pytest.raises(RuntimeError, match="Cold에 포함되지 않은 Hot"):
        hot_bronze_gc.collect_date(
            "test_source", DAY, now=datetime.now(KST), retention_days=0
        )

    assert unknown in list_keys("bronze/hot/test_source/")


def test_refuses_deletion_when_cold_checksum_mismatches():
    """Cold bytes가 manifest checksum과 다르면 Hot을 지우지 않는다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    result = cold_bronze.compact_date("test_source", DAY)
    put_object_bytes(result.cold_key, b"corrupted")

    with pytest.raises(ImmutableObjectError):
        hot_bronze_gc.collect_date(
            "test_source", DAY, now=datetime.now(KST), retention_days=0
        )

    assert len(list_keys("bronze/hot/test_source/")) == 1


def test_recover_due_skips_dates_completed_with_same_cold():
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    cold_bronze.compact_date("test_source", DAY)
    today = date(2026, 9, 30)

    first = hot_bronze_gc.recover_due("test_source", today=today, retention_days=0)
    second = hot_bronze_gc.recover_due("test_source", today=today, retention_days=0)

    assert first.dates == 1
    assert first.deleted == 1
    assert second.dates == 0
