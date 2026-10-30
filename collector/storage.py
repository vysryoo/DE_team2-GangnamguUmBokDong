"""S3/MinIO 입출력과 경로 규칙 생성.

S3와 연결되는 창구다. collector만 쓰는 키(bronze, quarantine, archive manifest)는
여기서 만들고, 다른 모듈과 공유하는 키는 `core.layout`이 만든다.
dict·bytes 단위로만 주고받고, 그 값이 무엇을 뜻하는지는 해석하지 않는다.
"""

from __future__ import annotations

import gzip
import io
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pyarrow.parquet as pq
from core import layout
from core.gold_publication.canonical import sha256_hex
from core.gold_publication.storage import S3ImmutableObjectStore
from core.kst import KST
from core.s3 import (
    S3Object,
    delete_object,
    delete_objects,
    get_object_bytes,
    list_keys,
    list_objects,
    object_exists,
    object_uri,
    put_object_bytes,
    read_json,
    write_json,
    write_parquet,
)


@dataclass(frozen=True, slots=True)
class ImmutableSilverArtifact:
    """Content-addressed Silver parquet의 exact object identity다."""

    key: str
    uri: str
    byte_sha256: str
    row_count: int


def _bronze_prefix(
    source_id: str, window_start: datetime, revision: int | None = None
) -> str:
    """Bronze 조각들이 모이는 legacy 또는 immutable Hot prefix를 만든다."""
    window = (
        f"{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}/"
    )
    if revision is None:
        return f"bronze/{window}"
    if revision < 0:
        raise ValueError(f"Bronze revision은 0 이상이어야 한다: {revision}")
    return f"bronze/hot/{window}revision={revision:010d}/"


def bronze_prefix(
    source_id: str, window_start: datetime, revision: int | None = None
) -> str:
    """Manifest에 기록할 canonical Bronze prefix를 반환한다."""
    return _bronze_prefix(source_id, window_start, revision)


def next_bronze_revision(source_id: str, window_start: datetime) -> int:
    """Manifest 저장 전 중단된 orphan까지 피해 다음 immutable revision을 고른다."""
    window_prefix = (
        f"bronze/hot/{source_id}/dt={window_start:%Y-%m-%d}/"
        f"hh={window_start:%H}/{window_start:%H%M}/"
    )
    revisions = []
    for key in list_keys(window_prefix):
        matched = re.match(rf"{re.escape(window_prefix)}revision=(\d{{10}})/", key)
        if matched is not None:
            revisions.append(int(matched.group(1)))
    return 0 if not revisions else max(revisions) + 1


def _bronze_part_key(
    source_id: str,
    window_start: datetime,
    chunk_key: str,
    revision: int | None = None,
) -> str:
    """bronze 조각 하나의 전체 키를 만든다."""
    return (
        f"{_bronze_prefix(source_id, window_start, revision)}"
        f"part={chunk_key}.json.gz"
    )


def write_bronze_part(
    source_id: str,
    window_start: datetime,
    chunk_key: str,
    chunk: bytes,
    revision: int | None = None,
) -> None:
    """API 응답 조각을 legacy 또는 immutable Hot Bronze에 gzip으로 저장한다."""
    key = _bronze_part_key(source_id, window_start, chunk_key, revision)
    payload = gzip.compress(chunk, mtime=0)
    if revision is None:
        put_object_bytes(key, payload)
        return
    import cold_bronze

    cold_bronze.write_pending_marker(source_id, window_start.date(), key)
    existing = get_object_bytes(key)
    if existing is not None and existing != payload:
        raise RuntimeError(f"immutable Hot Bronze key 충돌: {key}")
    if existing is None:
        put_object_bytes(key, payload)
    # Sweep가 pre-write marker를 본 뒤 지웠더라도 PUT 완료 상태를 다시 queue에 남긴다.
    cold_bronze.write_pending_marker(source_id, window_start.date(), key)


def read_bronze(
    source_id: str,
    window_start: datetime,
    parts: Sequence[str],
    revision: int | None = None,
) -> list[bytes]:
    """지정된 bronze 조각들을 읽어 압축을 해제한 뒤 순서대로 반환한다."""
    restored: dict[str, bytes] = {}
    for chunk_key in parts:
        key = _bronze_part_key(source_id, window_start, chunk_key, revision)
        body = get_object_bytes(key)
        if body:
            restored[chunk_key] = gzip.decompress(body)
    missing = tuple(part for part in parts if part not in restored)
    if missing:
        import cold_bronze

        restored.update(
            cold_bronze.read_revision(
                source_id,
                window_start,
                -1 if revision is None else revision,
                missing,
            )
        )
    return [restored[part] for part in parts if part in restored]


def clear_bronze(
    source_id: str, window_start: datetime, revision: int | None = None
) -> None:
    """해당 윈도우의 bronze 조각을 모두 삭제한다.
    수집 파이프라인이 에러로 뻗었을 때, 쓰레기 데이터가 남지 않도록 임시 조각들을 지워주는 역할."""
    prefix = _bronze_prefix(source_id, window_start, revision)
    keys = list_keys(prefix)
    delete_objects(keys)


def _layer_key(layer: str, source_id: str, window_start: datetime, ext: str) -> str:
    """silver·quarantine처럼 윈도우당 파일 하나로 떨어지는 계층의 경로를 만든다."""
    return (
        f"{layer}/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def write_immutable_silver(
    source_id: str,
    window_start: datetime,
    table: pq.Table,
) -> ImmutableSilverArtifact:
    """Silver parquet을 content-addressed immutable object로 기록하고 다시 읽는다.

    같은 parquet bytes의 재실행은 같은 URI에서 안전한 replay가 되고, correction으로
    bytes가 달라지면 새 URI가 된다. 반환 전 exact object를 checksum으로 다시 읽어
    downstream manifest가 아직 완성되지 않은 출력을 가리키지 않게 한다.
    """
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    payload = buffer.getvalue()
    checksum = sha256_hex(payload)
    key = layout.silver_content_key(source_id, window_start, checksum)
    uri = object_uri(key)
    S3ImmutableObjectStore().put_verified(uri, payload)
    return ImmutableSilverArtifact(
        key=key,
        uri=uri,
        byte_sha256=checksum,
        row_count=table.num_rows,
    )


def write_silver(source_id: str, window_start: datetime, table: pq.Table) -> str:
    """Legacy/compaction Silver key에 테이블을 쓰고 key를 반환한다.

    Authoritative Collector pipeline은 이 mutable 호환 경로를 사용하지 않고
    ``write_immutable_silver``의 content-addressed object만 source manifest에 싣는다.
    """
    key = _layer_key("silver", source_id, window_start, "parquet")
    write_parquet(table, key)
    return key


def read_immutable_silver_artifact(
    key: str,
    *,
    row_count: int,
) -> ImmutableSilverArtifact:
    """Content-addressed Silver key를 exact checksum으로 읽어 artifact를 복원한다."""
    if type(key) is not str:
        raise TypeError("immutable Silver key는 문자열이어야 합니다.")
    match = re.search(r"/sha256=([0-9a-f]{64})\.parquet\Z", key)
    if match is None:
        raise ValueError("Silver key가 content-addressed parquet 경로가 아닙니다.")
    checksum = match.group(1)
    uri = object_uri(key)
    S3ImmutableObjectStore().read_bytes(uri, checksum)
    return ImmutableSilverArtifact(
        key=key,
        uri=uri,
        byte_sha256=checksum,
        row_count=row_count,
    )


def write_quarantine(
    source_id: str, window_start: datetime, rows: list[dict]
) -> str | None:
    """검증에 실패한 row들을 jsonl로 저장하고, 저장된 키를 반환한다."""
    if not rows:
        return None
    key = _layer_key("quarantine", source_id, window_start, "jsonl")
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    put_object_bytes(key, body.encode("utf-8"))
    return key


def list_silver_objects(source_id: str, day: date) -> list[S3Object]:
    """해당 날짜의 silver parquet을 메타(size·last_modified)와 함께 나열한다.

    compaction이 "이 날짜가 지난번 압축 이후 바뀌었는가"를 본문 없이 판정하는 근거다.
    parquet이 아닌 객체는 거른다 — nowcaster가 같은 prefix 아래에 자기 산출물을
    두는 경우가 있어(`silver/{src}/dt=.../hh=00/nowcast.parquet`) 확장자만으로는
    부족할 수 있으나, 그 소스는 compaction 대상이 아니다.
    """
    return [
        o
        for o in list_objects(layout.silver_day_prefix(source_id, day))
        if o.key.endswith(".parquet")
    ]


def delete_silver_objects(keys: list[str]) -> None:
    """검증된 exact Silver parquet key만 일괄 삭제한다."""
    for key in keys:
        if not key.startswith("silver/") or not key.endswith(".parquet"):
            raise ValueError(f"Silver GC 삭제 key가 안전 범위를 벗어났다: {key}")
    delete_objects(keys)


def write_archive(source_id: str, day: date, table: pq.Table) -> str:
    """하루치를 묶은 테이블을 archive parquet으로 저장하고 저장된 키를 반환한다.

    경로 규칙은 `core.layout`이 갖는다 — nowcaster도 같은 계층을 읽고 쓰므로
    한쪽만 바뀌면 조용히 어긋난다.
    """
    key = layout.archive_key(source_id, day)
    write_parquet(table, key)
    return key


def archive_exists(source_id: str, day: date) -> bool:
    """해당 날짜의 archive가 이미 있는지 확인한다.

    bootstrap이 재개 판단에 쓴다 — 상태 파일을 따로 두지 않고 결과물의 존재로 판정한다.
    """
    return object_exists(layout.archive_key(source_id, day))


def _archive_manifest_key(source_id: str, day: date) -> str:
    """해당 날짜의 archive manifest 객체 키를 만든다."""
    return f"_archive_manifest/{source_id}/dt={day:%Y-%m-%d}.json"


def write_archive_manifest(source_id: str, day: date, data: dict) -> None:
    """해당 날짜의 압축 결과 요약을 저장한다."""
    write_json(_archive_manifest_key(source_id, day), data)


def read_archive_manifest(source_id: str, day: date) -> dict | None:
    """해당 날짜의 압축 결과 요약을 읽는다. 없으면 None(= 아직 압축한 적 없음)."""
    return read_json(_archive_manifest_key(source_id, day))


def list_source_snapshot_windows(source_id: str, day: date) -> list[datetime]:
    """KST 날짜에 속한 immutable source snapshot logical window를 나열한다.

    Manifest path는 UTC date partition을 사용하지만 compaction archive는 KST 날짜를
    사용한다. KST 하루가 걸치는 두 UTC partition을 나열하고 canonical key에서 exact
    logical identity를 복원한다. Revision이 여러 개인 correction window는 한 번만
    반환하며, 각 revision chain의 검증은 manifest 계층이 담당한다.

    args:
        source_id: 대상 Collector source ID
        day: archive가 표현하는 KST 날짜
    returns:
        오름차순 KST aware datetime 목록
    raises:
        ValueError: 전용 namespace에 canonical하지 않은 manifest key가 있을 때
    """
    start_kst = datetime(day.year, day.month, day.day, tzinfo=KST)
    end_kst = start_kst + timedelta(days=1)
    start_utc = start_kst.astimezone(UTC)
    end_utc = end_kst.astimezone(UTC)
    utc_days = {
        start_utc.date(),
        (end_utc - timedelta(microseconds=1)).date(),
    }

    logical_windows: set[datetime] = set()
    for utc_day in sorted(utc_days):
        for key in list_keys(layout.source_snapshot_day_prefix(source_id, utc_day)):
            parsed = layout.parse_source_snapshot_manifest_key(key)
            if parsed.source_id != source_id:
                raise ValueError(f"다른 source의 manifest key가 섞였다: {key}")
            if start_utc <= parsed.logical_dttm < end_utc:
                logical_windows.add(parsed.logical_dttm.astimezone(KST))
    return sorted(logical_windows)


def latest_source_snapshot_logical_dttm(
    source_id: str, *, as_of: datetime
) -> datetime | None:
    """`as_of` 이전 가장 최근 source snapshot의 logical 시각을 반환한다(없으면 None).

    Airflow의 freshness gate가 "마지막 성공 수집 이후 얼마나 지났는지"를 판단하는
    용도다 — `as_of`가 속한 KST 날짜에서 못 찾으면 자정을 막 넘긴 경우를 위해
    전날도 한 번 더 본다(그 이상 과거는 보지 않는다: 그만큼 오래 비어있다면 이미
    다른 방식으로 감지되어야 할 장애다).

    args:
        source_id: 대상 Collector source ID
        as_of: 기준 시각(aware) — 이 시각 이후의 window는 무시한다
    returns:
        가장 최근 logical 시각(KST aware) 또는 None
    """
    as_of_kst = as_of.astimezone(KST)
    candidates = [w for w in list_source_snapshot_windows(source_id, as_of_kst.date()) if w <= as_of_kst]
    if not candidates:
        yesterday = (as_of_kst - timedelta(days=1)).date()
        candidates = [w for w in list_source_snapshot_windows(source_id, yesterday) if w <= as_of_kst]
    return max(candidates) if candidates else None


def source_snapshot_manifest_uri(
    source_id: str,
    logical_dttm: datetime,
    revision_no: int,
) -> str:
    """Source snapshot manifest revision의 exact S3 URI를 반환한다."""
    return object_uri(
        layout.source_snapshot_manifest_key(source_id, logical_dttm, revision_no)
    )


def write_source_snapshot_manifest(
    source_id: str,
    logical_dttm: datetime,
    revision_no: int,
    payload: bytes,
) -> str:
    """Authority manifest를 revision URI에 put-once하고 exact bytes를 다시 읽는다."""
    uri = source_snapshot_manifest_uri(source_id, logical_dttm, revision_no)
    S3ImmutableObjectStore().put_verified(uri, payload, require_canonical_json=True)
    return uri


def list_source_snapshot_manifest_payloads(
    source_id: str,
    logical_dttm: datetime,
) -> list[tuple[str, bytes]]:
    """논리 source window의 authority manifest exact URI와 bytes를 모두 반환한다."""
    prefix = layout.source_snapshot_logical_prefix(source_id, logical_dttm)
    object_store = S3ImmutableObjectStore()
    result: list[tuple[str, bytes]] = []
    for key in sorted(list_keys(prefix)):
        uri = object_uri(key)
        first_read = get_object_bytes(key)
        if first_read is None:
            raise RuntimeError(
                f"나열된 source snapshot manifest를 읽을 수 없습니다: {uri}"
            )
        checksum = sha256_hex(first_read)
        exact = object_store.read_bytes(
            uri,
            checksum,
            require_canonical_json=True,
        )
        result.append((uri, exact))
    return result


def _retry_marker_key(source_id: str, window_start: datetime) -> str:
    """해당 윈도우의 retry marker 객체 키를 만든다."""
    return f"_retry_queue/{source_id}/{window_start.isoformat()}.json"


def write_manifest(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 manifest를 json으로 저장한다."""
    write_json(layout.window_manifest_key(source_id, window_start), data)


def read_manifest(source_id: str, window_start: datetime) -> dict | None:
    """해당 윈도우의 manifest를 읽는다. 없으면 None을 반환한다."""
    return read_json(layout.window_manifest_key(source_id, window_start))


def list_window_manifest_payloads(
    source_id: str, day: date, hour: str | None = None
) -> list[dict]:
    """해당 KST 날짜(및 선택적으로 시)에 속한 window manifest를 모두 읽어 반환한다.

    window manifest의 `dt=`/`hh=`는 KST 파티션이라 이 prefix 하나가 정확히 KST
    하루(또는 KST 한 시간)와 일치한다. 데이터 수집 모니터링 알림(일별/시간별)이 이
    함수로 해당 기간의 manifest를 모두 모은다.
    """
    prefix = layout.window_manifest_prefix(
        source_id, day, None if hour is None else int(hour)
    )
    payloads = []
    for key in list_keys(prefix):
        if not key.endswith(".json"):
            continue
        data = read_json(key)
        if data:
            payloads.append(data)
    return payloads


def write_retry_marker(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 retry marker를 JSON으로 저장한다."""
    key = _retry_marker_key(source_id, window_start)
    write_json(key, data)


def list_retry_markers(source_id: str) -> list[dict]:
    """해당 소스에 쌓인 retry marker를 모두 읽어 반환한다."""
    prefix = f"_retry_queue/{source_id}/"
    markers = []
    for key in list_keys(prefix):
        data = read_json(key)
        if data:
            markers.append(data)
    return markers


def delete_retry_marker(source_id: str, window_start: datetime) -> None:
    """해당 윈도우의 retry marker를 삭제한다."""
    key = _retry_marker_key(source_id, window_start)
    delete_object(key)
