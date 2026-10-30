"""Cold·Archive 검증과 30일 보존 뒤 non-authority Silver를 삭제하는 GC를 검증한다."""

from datetime import UTC, datetime, timedelta

import cold_bronze
import pytest
import silver_gc
import storage
from compaction import compact_date
from core.s3 import get_object_bytes, read_json
from tests.test_compaction_run import DAY, TODAY, _config, _publish_succeeded, _window

pytestmark = pytest.mark.usefixtures("s3_bucket")


def _prepare_cold() -> None:
    """같은 window의 최초·correction Hot Bronze를 Cold로 묶는다."""
    storage.write_bronze_part("t_source", _window(5), "part", b"first", revision=0)
    storage.write_bronze_part(
        "t_source", _window(5), "part", b"corrected", revision=1
    )
    assert cold_bronze.compact_date("t_source", DAY).status == "compacted"


def test_deletes_only_non_authority_after_archive_and_cold_are_verified():
    """과거 Silver를 삭제하고 최신 authority와 감사 manifest를 남긴다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    latest = _publish_succeeded(5, [2, 3])
    assert compact_date(config, DAY, today=TODAY).status == "compacted"
    _prepare_cold()

    result = silver_gc.collect_date(
        config, DAY, now=datetime.now(UTC) + timedelta(days=31)
    )

    assert result.status == "completed"
    assert result.deleted == 1
    assert result.retained == 1
    assert get_object_bytes(first.key) is None
    assert get_object_bytes(latest.key) is not None
    manifest = read_json("_silver_gc_manifest/t_source/dt=2026-08-12.json")
    assert manifest["status"] == "completed"
    assert manifest["deleted_keys"] == [first.key]
    assert manifest["retained_authority_keys"] == [latest.key]
    assert manifest["retention_days"] == 30


def test_retains_non_authority_during_thirty_day_grace_period():
    """non-authority라도 생성 후 30일 동안은 직접 재현을 위해 보존한다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    latest = _publish_succeeded(5, [2])
    assert compact_date(config, DAY, today=TODAY).status == "compacted"
    _prepare_cold()

    result = silver_gc.collect_date(config, DAY)

    assert result.status == "skipped"
    assert result.reason == "nothing_to_delete"
    assert get_object_bytes(first.key) is not None
    assert get_object_bytes(latest.key) is not None


def test_refuses_deletion_without_verified_cold_bronze():
    """Archive만 있고 Cold가 없으면 과거 Silver를 보존한다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    latest = _publish_succeeded(5, [2])
    assert compact_date(config, DAY, today=TODAY).status == "compacted"

    result = silver_gc.collect_date(config, DAY)

    assert result.status == "skipped"
    assert result.reason == "cold_unverified"
    assert get_object_bytes(first.key) is not None
    assert get_object_bytes(latest.key) is not None


def test_refuses_deletion_when_archive_signature_is_stale():
    """Archive 뒤 새 correction이 생기면 재compaction 전까지 아무것도 삭제하지 않는다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    assert compact_date(config, DAY, today=TODAY).status == "compacted"
    latest = _publish_succeeded(5, [2])
    _prepare_cold()

    result = silver_gc.collect_date(config, DAY)

    assert result.status == "skipped"
    assert result.reason == "archive_authority_stale"
    assert get_object_bytes(first.key) is not None
    assert get_object_bytes(latest.key) is not None


def test_deletes_non_authority_without_archive_when_cold_is_verified():
    """Archive 비대상 source는 검증된 Cold만으로 30일 지난 revision을 정리한다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    latest = _publish_succeeded(5, [2])
    _prepare_cold()

    result = silver_gc.collect_date(
        config,
        DAY,
        now=datetime.now(UTC) + timedelta(days=31),
        require_archive=False,
    )

    assert result.status == "completed"
    assert get_object_bytes(first.key) is None
    assert get_object_bytes(latest.key) is not None
    manifest = read_json("_silver_gc_manifest/t_source/dt=2026-08-12.json")
    assert manifest["archive_required"] is False
    assert manifest["archive_key"] is None


def test_archive_free_gc_still_requires_verified_cold():
    """Archive 비대상이어도 Cold가 없으면 non-authority를 삭제하지 않는다."""
    config = _config()
    first = _publish_succeeded(5, [1])
    latest = _publish_succeeded(5, [2])

    result = silver_gc.collect_date(
        config,
        DAY,
        now=datetime.now(UTC) + timedelta(days=31),
        require_archive=False,
    )

    assert result.reason == "cold_unverified"
    assert get_object_bytes(first.key) is not None
    assert get_object_bytes(latest.key) is not None
