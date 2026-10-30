"""모든 Hot Bronze revision을 보존하는 날짜 단위 Cold compaction을 검증한다."""

import gzip
from datetime import date, datetime

import cold_bronze
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage
from core.s3 import (
    delete_objects,
    get_object_bytes,
    list_keys,
    read_json,
    write_json,
)
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

DAY = date(2026, 8, 12)
WINDOW = datetime(2026, 8, 12, 14, 55, tzinfo=KST)


def test_compacts_every_revision_and_preserves_exact_stored_bytes():
    """최초 수집과 correction을 모두 한 Cold 파일에서 원본 bytes로 복원한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"corrected", 1)

    result = cold_bronze.compact_date("test_source", DAY)

    assert result.status == "compacted"
    assert result.objects == 2
    payload = get_object_bytes(result.cold_key)
    table = pq.read_table(pa.BufferReader(payload))
    assert table["revision"].to_pylist() == [0, 1]
    assert [gzip.decompress(value.as_py()) for value in table["stored_bytes"]] == [
        b"first",
        b"corrected",
    ]
    manifest = read_json("bronze/cold_manifest/test_source/dt=2026-08-12.json")
    assert manifest["input_objects"] == 2
    assert manifest["cold_key"] == result.cold_key
    assert len(list_keys("_cold_pending/test_source/")) == 2
    assert len(manifest["inventory_keys"]) == 2


def test_unchanged_inputs_skip_rewrite():
    """입력 signature가 같으면 기존 immutable Cold bundle을 재사용한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    first = cold_bronze.compact_date("test_source", DAY)

    second = cold_bronze.compact_date("test_source", DAY)

    assert second.status == "skipped"
    assert second.cold_key == first.cold_key


def test_legacy_unverified_manifest_is_upgraded_before_gc():
    """verified 필드 도입 전 Cold manifest는 readback 검증 후 다시 게시한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    first = cold_bronze.compact_date("test_source", DAY)
    key = "bronze/cold_manifest/test_source/dt=2026-08-12.json"
    manifest = read_json(key)
    manifest.pop("verified")
    write_json(key, manifest)

    upgraded = cold_bronze.compact_date("test_source", DAY)

    assert upgraded.status == "compacted"
    assert upgraded.cold_key == first.cold_key
    assert read_json(key)["verified"] is True


def test_read_bronze_falls_back_to_cold_after_hot_expiration():
    """Hot Lifecycle 삭제 뒤에도 같은 window revision을 Cold에서 복원한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    cold_bronze.compact_date("test_source", DAY)
    delete_objects(list_keys("bronze/hot/test_source/"))

    restored = storage.read_bronze(
        "test_source", WINDOW, ["page-001"], revision=0
    )

    assert restored == [b"first"]


def test_legacy_manifest_without_revision_also_falls_back_to_cold():
    """이관 전 manifest의 revision=None도 Cold의 legacy revision -1로 복원한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"legacy")
    cold_bronze.compact_date("test_source", DAY)
    delete_objects(list_keys("bronze/test_source/"))

    restored = storage.read_bronze("test_source", WINDOW, ["page-001"])

    assert restored == [b"legacy"]


def test_empty_date_is_explicit():
    """Hot Bronze가 없는 날짜는 파일을 만들지 않고 empty로 끝난다."""
    result = cold_bronze.compact_date("test_source", DAY)

    assert result.status == "empty"
    assert result.cold_key is None


def test_pending_recovery_only_processes_dates_after_delay():
    """pending queue는 전체 날짜 sweep 없이 기한이 된 marker 날짜만 처리한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)

    before_due = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 17), delay_days=6
    )
    due = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 18), delay_days=6
    )

    assert before_due.dates == 0
    assert list_keys("_cold_pending/test_source/") == []
    assert due.dates == 1
    assert due.objects == 1
    assert read_json("bronze/cold_manifest/test_source/dt=2026-08-12.json")


def test_pending_marker_is_one_per_revision_not_per_part():
    """part가 많은 source도 같은 window revision에는 marker 하나만 만든다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    storage.write_bronze_part("test_source", WINDOW, "page-002", b"second", 0)

    assert len(list_keys("_cold_pending/test_source/")) == 1


def test_missing_hot_marker_is_ignored_until_due_and_then_cleaned():
    """Hot put 전에 중단된 marker는 D-6 전 task를 막지 않고 기한 뒤 정리된다."""
    missing_hot = (
        "bronze/hot/test_source/dt=2026-08-12/hh=14/1455/"
        "revision=0000000000/part=missing.json.gz"
    )
    cold_bronze.write_pending_marker("test_source", DAY, missing_hot)

    before_due = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 17), delay_days=6
    )
    due = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 18), delay_days=6
    )

    assert before_due.dates == 0
    assert due.dates == 1
    assert due.objects == 0
    assert due.stale_markers == 1
    assert list_keys("_cold_pending/test_source/") == []


def test_late_revision_creates_new_pending_work_after_cold():
    """Cold 완료 날짜의 늦은 revision은 새 marker로 다시 compaction된다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    first = cold_bronze.compact_date("test_source", DAY)
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"late", 1)

    recovered = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 18), delay_days=6
    )
    current = read_json("bronze/cold_manifest/test_source/dt=2026-08-12.json")

    assert recovered.dates == 1
    assert recovered.objects == 2
    assert current["cold_key"] != first.cold_key
    assert list_keys("_cold_pending/test_source/") == []


def test_late_revision_merges_with_cold_after_hot_was_deleted():
    """Hot GC 이후의 늦은 revision도 기존 Cold rows를 잃지 않고 누적한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    first = cold_bronze.compact_date("test_source", DAY)
    delete_objects(list_keys("bronze/hot/test_source/"))
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"late", 1)

    second = cold_bronze.compact_date("test_source", DAY)
    table = pq.read_table(pa.BufferReader(get_object_bytes(second.cold_key)))

    assert second.cold_key != first.cold_key
    assert table["revision"].to_pylist() == [0, 1]


def test_hot_put_during_sweep_keeps_revision_pending(monkeypatch):
    """시작 때 marker만 있던 revision의 sweep 중 PUT을 다음 실행이 복구한다."""
    storage.write_bronze_part("test_source", WINDOW, "page-001", b"first", 0)
    late_key = (
        "bronze/hot/test_source/dt=2026-08-12/hh=14/1455/"
        "revision=0000000001/part=page-001.json.gz"
    )
    cold_bronze.write_pending_marker("test_source", DAY, late_key)
    original = cold_bronze.compact_date

    def _compact_then_finish_late_put(source_id, day):
        result = original(source_id, day)
        storage.write_bronze_part(
            "test_source", WINDOW, "page-001", b"late", revision=1
        )
        return result

    monkeypatch.setattr(cold_bronze, "compact_date", _compact_then_finish_late_put)
    with pytest.raises(RuntimeError, match="Cold 생성 중 Hot 입력이 변경됐다"):
        cold_bronze.recover_pending(
            "test_source", today=date(2026, 8, 18), delay_days=6
        )

    assert list_keys("_cold_pending/test_source/")
    monkeypatch.setattr(cold_bronze, "compact_date", original)
    recovered = cold_bronze.recover_pending(
        "test_source", today=date(2026, 8, 18), delay_days=6
    )

    assert recovered.dates == 1
    assert recovered.objects == 2
    assert list_keys("_cold_pending/test_source/") == []
