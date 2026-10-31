"""검증된 Cold에 포함된 보존기한 지난 Hot Bronze만 삭제한다.

S3 Lifecycle은 Cold 포함 여부를 모르므로 Hot Bronze 정리는 애플리케이션이 맡는다.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from core.kst import KST
from core.s3 import list_keys, list_objects, read_json

import cold_bronze
from gc_manifest import delete_with_manifest, gc_manifest_key

_COLD_MANIFEST_DAY = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})\.json\Z")


@dataclass(frozen=True, slots=True)
class HotGcResult:
    """Hot Bronze GC sweep 결과다."""

    dates: int
    deleted: int
    retained: int


def _read_cold_inventory(source_id: str, day: date) -> tuple[dict, set[str]]:
    """Cold manifest와 checksum을 검증한 Cold에 담긴 원본 Hot key 집합을 읽는다."""
    manifest = cold_bronze.read_manifest(source_id, day)
    if manifest is None:
        raise RuntimeError(f"Cold manifest가 없다: {cold_bronze.manifest_key(source_id, day)}")
    cold_table = pq.read_table(
        pa.BufferReader(cold_bronze.read_verified_cold(manifest)),
        columns=["original_key"],
    )
    return manifest, set(cold_table["original_key"].to_pylist())


def collect_date(
    source_id: str,
    day: date,
    *,
    now: datetime | None = None,
    retention_days: int = 30,
) -> HotGcResult:
    """Cold inventory에 있고 보존기간이 지난 Hot 객체만 삭제한다.

    Args:
        source_id: 소스 id
        day: KST 날짜
        now: 보존기간 기준 시각. 생략하면 현재 시각
        retention_days: Hot 객체를 남겨 둘 일수
    Returns:
        삭제·보존한 Hot 객체 수
    Raises:
        RuntimeError: Cold manifest가 없거나, Cold에 없는 Hot 객체가 있거나, 삭제 뒤에도
            객체가 남았을 때
        ImmutableObjectError: Cold checksum 검증에 실패했을 때
    """
    cold_manifest, inventory = _read_cold_inventory(source_id, day)
    hot_prefix = cold_bronze.hot_day_prefix(source_id, day)
    hot_objects = list_objects(hot_prefix)
    unknown = sorted(obj.key for obj in hot_objects if obj.key not in inventory)
    if unknown:
        raise RuntimeError(f"Cold에 포함되지 않은 Hot object가 있다: {unknown[:5]}")

    cutoff = (now or datetime.now(KST)) - timedelta(days=retention_days)
    candidates = sorted(obj.key for obj in hot_objects if obj.last_modified <= cutoff)
    retained = len(hot_objects) - len(candidates)
    delete_with_manifest(
        gc_manifest_key("hot", source_id, day),
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "cold_key": cold_manifest["cold_key"],
            "retention_days": retention_days,
            "cutoff": cutoff.isoformat(),
            "deletion_candidates": candidates,
        },
        candidates,
        hot_prefix,
        completed={
            "status": "completed" if retained == 0 else "retained_recent",
            "retained": retained,
        },
    )
    return HotGcResult(dates=1, deleted=len(candidates), retained=retained)


def _uncollected_days(source_id: str, cutoff_day: date) -> Iterator[date]:
    """보존기한이 지난 Cold 날짜 중 현재 Cold로 GC를 끝내지 않은 날짜를 낸다."""
    for key in list_keys(cold_bronze.manifest_prefix(source_id)):
        matched = _COLD_MANIFEST_DAY.search(key)
        if matched is None:
            raise RuntimeError(f"Cold manifest key 형식이 잘못됐다: {key}")
        day = date.fromisoformat(matched.group(1))
        if day > cutoff_day:
            continue
        # Cold key는 content-addressed라 늦은 revision으로 Cold가 바뀌면 key도 바뀜
        cold = read_json(key) or {}
        completed = read_json(gc_manifest_key("hot", source_id, day)) or {}
        if completed.get("status") == "completed" and completed.get("cold_key") == cold.get("cold_key"):
            continue
        yield day


def recover_due(
    source_id: str,
    *,
    today: date,
    retention_days: int = 30,
) -> HotGcResult:
    """보존기한이 지난 Cold 날짜 중 같은 Cold로 GC를 끝내지 않은 날짜만 처리한다.

    Args:
        source_id: 소스 id
        today: 기준 KST 날짜
        retention_days: Hot 객체를 남겨 둘 일수
    Returns:
        처리한 날짜 수와 삭제·보존한 Hot 객체 수의 합
    """
    results = [
        collect_date(source_id, day, retention_days=retention_days)
        for day in _uncollected_days(source_id, today - timedelta(days=retention_days))
    ]
    return HotGcResult(
        sum(result.dates for result in results),
        sum(result.deleted for result in results),
        sum(result.retained for result in results),
    )
