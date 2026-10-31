"""Hot Bronze 원본 객체를 날짜 단위 immutable Cold Bronze로 묶는다.

JSON을 파싱하거나 다시 직렬화하지 않는다. 각 Hot object의 gzip bytes, 원래 key와
checksum을 Parquet binary 컬럼에 넣어 모든 window와 모든 수집 revision을 복원할 수
있게 한다. Cold manifest만 해당 날짜의 최신 bundle을 가리키는 mutable pointer다.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from core.gold_publication.storage import (
    ImmutableObjectError,
    S3ImmutableObjectStore,
)
from core.s3 import (
    delete_objects,
    get_object_bytes,
    list_keys,
    list_objects,
    object_uri,
    read_json,
    write_json,
)

_HOT_KEY = re.compile(
    r"\Abronze/hot/[a-z][a-z0-9_]*/dt=\d{4}-\d{2}-\d{2}/"
    r"hh=\d{2}/(?P<hhmm>\d{4})/revision=(?P<revision>\d{10})/"
    r"part=(?P<part>.+)\.json\.gz\Z"
)
_PENDING_MARKER_DAY = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/")

_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("window_start", pa.string()),
        ("revision", pa.int64()),
        ("part_key", pa.string()),
        ("original_key", pa.string()),
        ("collected_at", pa.string()),
        ("stored_sha256", pa.string()),
        ("payload_sha256", pa.string()),
        ("stored_bytes", pa.binary()),
    ]
)


@dataclass(frozen=True, slots=True)
class ColdBronzeResult:
    """날짜 하나의 Cold Bronze 생성 결과다."""

    status: str
    objects: int
    cold_key: str | None


@dataclass(frozen=True, slots=True)
class ColdRecoveryResult:
    """pending marker에서 처리한 날짜별 Cold recovery 결과다."""

    dates: int
    objects: int


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def hot_day_prefix(source_id: str, day: date) -> str:
    """날짜의 Hot Bronze prefix를 반환한다.

    Args:
        source_id: 소스 id
        day: KST 날짜
    Returns:
        `bronze/hot/{source_id}/dt=YYYY-MM-DD/`
    """
    return f"bronze/hot/{source_id}/dt={day.isoformat()}/"


def manifest_key(source_id: str, day: date) -> str:
    """날짜의 Cold manifest key를 반환한다.

    Args:
        source_id: 소스 id
        day: KST 날짜
    Returns:
        `bronze/cold_manifest/{source_id}/dt=YYYY-MM-DD.json`
    """
    return f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"


def read_manifest(source_id: str, day: date) -> dict | None:
    """날짜의 Cold manifest를 읽는다.

    Args:
        source_id: 소스 id
        day: KST 날짜
    Returns:
        manifest dict. Cold가 아직 없으면 None
    """
    return read_json(manifest_key(source_id, day))


def read_verified_cold(manifest: dict) -> bytes:
    """Manifest가 가리키는 Cold Parquet bytes를 checksum으로 검증해 읽는다.

    Args:
        manifest: `read_manifest`가 반환한 dict
    Returns:
        Cold Parquet 원본 bytes
    Raises:
        ImmutableObjectError: Cold object가 없거나 checksum이 다를 때
    """
    return S3ImmutableObjectStore().read_bytes(
        object_uri(manifest["cold_key"]), manifest["cold_sha256"]
    )


def write_pending_marker(source_id: str, day: date, hot_key: str) -> None:
    """Hot revision prefix 하나에 대응하는 날짜별 Cold 작업 marker를 기록한다.

    key와 payload가 Hot revision prefix로 정해지므로 여러 번 써도 결과가 같다.

    Args:
        source_id: 소스 id
        day: Hot object의 KST 날짜
        hot_key: 저장할 Hot object key
    """
    revision_prefix = hot_key.rsplit("/part=", 1)[0]
    write_json(
        f"_cold_pending/{source_id}/dt={day.isoformat()}/"
        f"sha256={_sha256(revision_prefix.encode())}.json",
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "hot_revision_prefix": revision_prefix,
        },
    )


def _cold_row(source_id: str, day: date, obj) -> dict:
    """Hot object 하나를 Cold row로 만든다."""
    matched = _HOT_KEY.fullmatch(obj.key)
    if matched is None:
        raise ValueError(f"Cold Bronze 입력 key 규칙에 맞지 않는다: {obj.key}")
    stored = get_object_bytes(obj.key)
    if stored is None:
        raise RuntimeError(f"LIST 뒤 Hot Bronze object가 사라졌다: {obj.key}")
    hhmm = matched.group("hhmm")
    return {
        "source_id": source_id,
        "window_start": f"{day.isoformat()}T{hhmm[:2]}:{hhmm[2:]}:00+09:00",
        "revision": int(matched.group("revision")),
        "part_key": matched.group("part"),
        "original_key": obj.key,
        "collected_at": obj.last_modified.isoformat(),
        "stored_sha256": _sha256(stored),
        "payload_sha256": _sha256(gzip.decompress(stored)),
        "stored_bytes": stored,
    }


def _previous_rows(previous: dict | None) -> dict[str, dict]:
    """기존 Cold bundle의 row를 원본 Hot key별로 읽는다."""
    if previous is None:
        return {}
    table = pq.read_table(pa.BufferReader(read_verified_cold(previous)))
    return {row["original_key"]: row for row in table.to_pylist()}


def _write_cold(source_id: str, day: date, rows: list[dict]) -> tuple[str, str]:
    """Cold row를 content-addressed Parquet으로 쓰고 key와 checksum을 반환한다."""
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), buffer, compression="zstd")
    cold_payload = buffer.getvalue()
    cold_sha256 = _sha256(cold_payload)
    cold_key = f"bronze/cold/{source_id}/dt={day.isoformat()}/sha256={cold_sha256}.parquet"
    S3ImmutableObjectStore().put_verified(object_uri(cold_key), cold_payload)
    return cold_key, cold_sha256


def compact_date(source_id: str, day: date) -> ColdBronzeResult:
    """날짜의 모든 Hot Bronze revision을 원본 bytes 그대로 Cold 파일 하나에 보관한다.

    Hot GC 뒤에 늦은 revision이 들어와도 이전 Cold rows를 잃지 않도록 기존 bundle에
    누적한다.

    Args:
        source_id: 소스 id
        day: KST 날짜
    Returns:
        `compacted`, 입력이 그대로라 기존 bundle을 재사용한 `skipped`, Hot과 Cold가 모두
        없는 `empty` 중 하나
    Raises:
        RuntimeError: 이미 Cold에 들어간 Hot key의 bytes가 달라졌을 때
        ImmutableObjectError: 기존 Cold의 checksum 검증 또는 새 Cold 쓰기에 실패했을 때
    """
    previous = read_manifest(source_id, day)
    rows_by_key = _previous_rows(previous)
    hot_objects = list_objects(hot_day_prefix(source_id, day))
    if not hot_objects and not rows_by_key:
        return ColdBronzeResult(status="empty", objects=0, cold_key=None)

    for obj in hot_objects:
        row = _cold_row(source_id, day, obj)
        previous_row = rows_by_key.get(obj.key)
        if previous_row and previous_row["stored_sha256"] != row["stored_sha256"]:
            raise RuntimeError(f"immutable Hot Bronze key 내용이 변경됐다: {obj.key}")
        rows_by_key[obj.key] = row

    # Python 문자열 정렬은 code point 기준이라 S3 LIST 순서(UTF-8 bytes)와 맞추려 encode함
    inventory_keys = sorted(rows_by_key, key=str.encode)
    if previous is not None and previous.get("inventory_keys") == inventory_keys:
        return ColdBronzeResult("skipped", len(inventory_keys), previous["cold_key"])

    rows = [rows_by_key[key] for key in inventory_keys]
    cold_key, cold_sha256 = _write_cold(source_id, day, rows)
    write_json(
        manifest_key(source_id, day),
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "input_objects": len(rows),
            "inventory_keys": inventory_keys,
            "cold_key": cold_key,
            "cold_sha256": cold_sha256,
            "stored_bytes": sum(len(row["stored_bytes"]) for row in rows),
        },
    )
    return ColdBronzeResult("compacted", len(rows), cold_key)


def _due_marker_keys(
    source_id: str, today: date, delay_days: int
) -> dict[date, list[str]]:
    """안정화 기간이 지난 pending marker key를 날짜별로 묶는다."""
    due: dict[date, list[str]] = {}
    for key in list_keys(f"_cold_pending/{source_id}/"):
        matched = _PENDING_MARKER_DAY.search(key)
        if matched is None:
            raise RuntimeError(f"Cold pending marker key 형식이 잘못됐다: {key}")
        day = date.fromisoformat(matched.group(1))
        if day + timedelta(days=delay_days) <= today:
            due.setdefault(day, []).append(key)
    return due


def _recover_date(source_id: str, day: date, marker_keys: list[str]) -> int:
    """날짜 하나를 Cold로 묶고 처리한 marker를 지운다."""
    result = compact_date(source_id, day)
    delete_objects(marker_keys)

    # marker 삭제 전후로 끝난 Hot PUT은 이번 inventory에 없을 수 있다. writer는 PUT 뒤에
    # marker를 다시 쓰지만, 그 쓰기가 위 삭제보다 먼저 끝났으면 marker를 잃으므로 복원함
    manifest = read_manifest(source_id, day) or {}
    inventory = set(manifest.get("inventory_keys", []))
    late_keys = [
        obj.key
        for obj in list_objects(hot_day_prefix(source_id, day))
        if obj.key not in inventory
    ]
    if late_keys:
        for key in late_keys:
            write_pending_marker(source_id, day, key)
        raise RuntimeError(
            f"Cold 생성 중 Hot 입력이 변경됐다: source={source_id}, day={day}"
        )
    return result.objects


def recover_pending(
    source_id: str,
    *,
    today: date,
    delay_days: int = 6,
) -> ColdRecoveryResult:
    """pending marker 중 안정화 기간이 지난 날짜만 Cold로 묶는다.

    한 날짜의 실패가 나머지 날짜를 막지 않도록 모두 시도한 뒤 실패를 모아 던진다.

    Args:
        source_id: 소스 id
        today: 기준 KST 날짜
        delay_days: marker 날짜로부터 기다릴 일수
    Returns:
        처리한 날짜 수와 Cold에 담긴 object 수
    Raises:
        RuntimeError: 하나 이상의 날짜가 실패했을 때. marker는 남아 다음 실행이 재시도함
    """
    due = _due_marker_keys(source_id, today, delay_days)
    object_count = 0
    failures = []
    for day in sorted(due):
        try:
            object_count += _recover_date(source_id, day, due[day])
        except (ClientError, ImmutableObjectError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"{day}: {exc}")
    if failures:
        raise RuntimeError(f"Cold pending recovery 일부 날짜가 실패했다: {', '.join(failures)}")
    return ColdRecoveryResult(dates=len(due), objects=object_count)


def read_revision(
    source_id: str,
    window_start: datetime,
    revision: int,
    parts: tuple[str, ...],
) -> dict[str, bytes]:
    """Cold bundle에서 window revision의 원본 payload를 복원한다.

    Args:
        source_id: 소스 id
        window_start: 수집 window 시작 시각(KST aware)
        revision: Hot revision 번호. 이관 전 legacy Bronze는 -1
        parts: 복원할 part key 목록
    Returns:
        part key별 gzip 해제된 payload. Cold가 없거나 해당 row가 없으면 빠짐
    Raises:
        ImmutableObjectError: Cold object가 없거나 checksum이 다를 때
    """
    manifest = read_manifest(source_id, window_start.date())
    if not manifest:
        return {}
    table = pq.read_table(
        pa.BufferReader(read_verified_cold(manifest)),
        columns=["window_start", "revision", "part_key", "stored_bytes"],
    )
    expected_window = window_start.isoformat()
    requested = set(parts)
    return {
        row["part_key"]: gzip.decompress(row["stored_bytes"])
        for row in table.to_pylist()
        if row["window_start"] == expected_window
        and row["revision"] == revision
        and row["part_key"] in requested
    }
