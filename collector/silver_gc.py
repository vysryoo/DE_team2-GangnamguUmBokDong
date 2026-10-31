"""검증된 날짜에서 최신 authority가 아닌 Silver object를 정리한다.

삭제는 Cold Bronze가 있는 날짜에서만 허용하고, Archive 대상 source는 Archive가 현재
authority와 일치하는지도 확인한다. 과거 source snapshot manifest는 삭제된 Silver URI를
계속 가리키므로 삭제 목록과 복구 근거를 GC manifest에 남긴다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from core import layout
from core.kst import KST
from core.s3 import S3Object

import cold_bronze
import compaction
import storage
from config.schema import SourceConfig
from gc_manifest import delete_with_manifest, gc_manifest_key

NON_AUTHORITY_RETENTION_DAYS = 30


@dataclass(frozen=True, slots=True)
class SilverGcResult:
    """날짜 하나의 non-authority Silver 정리 결과다."""

    status: str
    deleted: int
    retained: int
    reason: str | None = None


def collect_date(
    config: SourceConfig,
    day: date,
    *,
    now: datetime | None = None,
    require_archive: bool = True,
) -> SilverGcResult:
    """Cold와 선택적인 Archive 확인 뒤 30일 지난 non-authority Silver를 삭제한다.

    Args:
        config: 대상 소스 설정
        day: KST 날짜
        now: 보존기간 기준 시각. 생략하면 현재 시각
        require_archive: Archive가 현재 authority와 일치해야만 삭제할지 여부
    Returns:
        삭제 결과. 조건을 만족하지 않으면 `skipped`와 이유를 담음
    Raises:
        RuntimeError: 삭제 뒤에도 object가 남았을 때
    """
    source_id = config.source_id
    objects = storage.list_silver_objects(source_id, day)
    if not objects:
        return SilverGcResult("skipped", deleted=0, retained=0, reason="no_silver")

    cold_manifest = cold_bronze.read_manifest(source_id, day)
    if cold_manifest is None:
        return SilverGcResult("skipped", 0, len(objects), "cold_unverified")

    authority, signature = compaction.resolve_date_authority(config, day)
    archive_manifest = None
    if require_archive:
        archive_manifest = storage.read_archive_manifest(source_id, day)
        archive_problem = _archive_problem(archive_manifest, signature)
        if archive_problem is not None:
            return SilverGcResult("skipped", 0, len(objects), archive_problem)

    authority_keys = {item.object.key for item in authority.selected}
    cutoff = (now or datetime.now(KST)) - timedelta(days=NON_AUTHORITY_RETENTION_DAYS)
    candidates = [
        obj
        for obj in objects
        if obj.key not in authority_keys and obj.last_modified <= cutoff
    ]
    deleted_keys = [obj.key for obj in candidates]
    retained_keys = sorted({obj.key for obj in objects} - set(deleted_keys))
    if not candidates:
        return SilverGcResult("skipped", 0, len(retained_keys), "nothing_to_delete")

    delete_with_manifest(
        gc_manifest_key("silver", source_id, day),
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "archive_key": archive_manifest.get("archive_key") if archive_manifest else None,
            "archive_required": require_archive,
            "archive_silver_signature": signature if require_archive else None,
            "cold_key": cold_manifest["cold_key"],
            "retained_authority_keys": sorted(authority_keys),
            "retained_keys": retained_keys,
            "retention_days": NON_AUTHORITY_RETENTION_DAYS,
            "cutoff": cutoff.isoformat(),
            "deletion_candidates": [_candidate_record(obj) for obj in candidates],
        },
        deleted_keys,
        layout.silver_day_prefix(source_id, day),
        completed={"status": "completed"},
    )
    return SilverGcResult("completed", len(deleted_keys), len(retained_keys))


def _archive_problem(archive_manifest: dict | None, signature: str) -> str | None:
    """Archive가 없거나 현재 authority와 다르면 skip 이유를 반환한다."""
    if not archive_manifest:
        return "archive_manifest_missing"
    if archive_manifest.get("silver_signature") != signature:
        return "archive_authority_stale"
    return None


def _candidate_record(obj: S3Object) -> dict:
    return {
        "key": obj.key,
        "size": obj.size,
        "last_modified": obj.last_modified.isoformat(),
    }
