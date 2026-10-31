"""Gold 입력이 되는 immutable source snapshot manifest 계약을 제공한다."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, cast
from urllib.parse import urlsplit

from .gold_publication.canonical import (
    JsonValue,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
    validate_sha256_hex,
)

SOURCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION = "source-snapshot-manifest-v1"
"""Immutable source snapshot manifest의 schema version이다."""

_SOURCE_ID_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
_MANIFEST_KEYS = frozenset(
    {
        "completed_parts",
        "config_version",
        "counts",
        "logical_dttm",
        "planned_parts",
        "revision_no",
        "schema_version",
        "silver_byte_sha256",
        "silver_uri",
        "source_id",
        "status",
    }
)
_COUNTS_KEYS = frozenset({"dropped", "expected", "fetched", "kept", "repaired"})
_MAX_SAFE_INTEGER = 2**53 - 1


class SourceSnapshotContractError(ValueError):
    """Source snapshot bytes 또는 typed 값이 계약을 위반했다."""


class SourceSnapshotStatus(StrEnum):
    """Gold 입력 권한을 열 수 있는 source snapshot의 최종 상태다."""

    SUCCEEDED = "succeeded"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class SourceSnapshotCounts:
    """Source snapshot의 원천·검증 행 수를 고정한다."""

    expected: int | None
    fetched: int
    kept: int
    repaired: int
    dropped: int

    def __post_init__(self) -> None:
        """count의 exact type과 상호 관계를 검증한다."""
        if self.expected is not None:
            _require_nonnegative_integer(self.expected, "counts.expected")
        _require_nonnegative_integer(self.fetched, "counts.fetched")
        _require_nonnegative_integer(self.kept, "counts.kept")
        _require_nonnegative_integer(self.repaired, "counts.repaired")
        _require_nonnegative_integer(self.dropped, "counts.dropped")
        if self.kept + self.dropped != self.fetched:
            raise SourceSnapshotContractError(
                "counts.kept + counts.dropped은 counts.fetched와 같아야 합니다."
            )
        if self.repaired > self.kept:
            raise SourceSnapshotContractError(
                "counts.repaired는 counts.kept보다 클 수 없습니다."
            )


@dataclass(frozen=True, slots=True)
class SourceSnapshotManifest:
    """Gold downstream이 신뢰할 수 있는 immutable source snapshot을 표현한다."""

    schema_version: str
    source_id: str
    logical_dttm: datetime
    revision_no: int
    status: SourceSnapshotStatus
    config_version: str
    silver_uri: str | None
    silver_byte_sha256: str | None
    counts: SourceSnapshotCounts
    planned_parts: tuple[str, ...]
    completed_parts: tuple[str, ...]

    def __post_init__(self) -> None:
        """Manifest의 exact field type과 authoritative 상태 불변식을 검증한다."""
        validate_source_snapshot_manifest(self)

    @property
    def canonical_bytes(self) -> bytes:
        """Manifest의 exact canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_manifest_document(self))

    @property
    def sha256(self) -> str:
        """Manifest canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


def build_source_snapshot_manifest(
    *,
    source_id: str,
    logical_dttm: datetime,
    revision_no: int,
    status: SourceSnapshotStatus,
    config_version: str,
    silver_uri: str | None,
    silver_byte_sha256: str | None,
    counts: SourceSnapshotCounts,
    planned_parts: tuple[str, ...],
    completed_parts: tuple[str, ...],
) -> SourceSnapshotManifest:
    """검증된 값에서 v1 source snapshot manifest를 만든다."""
    return SourceSnapshotManifest(
        schema_version=SOURCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        source_id=source_id,
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=status,
        config_version=config_version,
        silver_uri=silver_uri,
        silver_byte_sha256=silver_byte_sha256,
        counts=counts,
        planned_parts=planned_parts,
        completed_parts=completed_parts,
    )


def parse_source_snapshot_manifest(payload: bytes) -> SourceSnapshotManifest:
    """Canonical bytes를 exact v1 source snapshot manifest로 파싱한다."""
    try:
        document = _require_object(
            parse_canonical_json(payload), _MANIFEST_KEYS, "source snapshot manifest"
        )
        counts_document = _require_object(
            document["counts"], _COUNTS_KEYS, "source snapshot counts"
        )
        status_value = _require_string(document["status"], "status")
        try:
            status = SourceSnapshotStatus(status_value)
        except ValueError as exc:
            raise SourceSnapshotContractError(
                "status는 succeeded 또는 empty여야 합니다."
            ) from exc
        # count, source_id, revision_no 등의 타입과 값은 생성자 검증이 다시 확인함
        return SourceSnapshotManifest(
            schema_version=document["schema_version"],
            source_id=document["source_id"],
            logical_dttm=parse_utc_dttm(
                _require_string(document["logical_dttm"], "logical_dttm")
            ),
            revision_no=document["revision_no"],
            status=status,
            config_version=document["config_version"],
            silver_uri=_require_optional_string(document["silver_uri"], "silver_uri"),
            silver_byte_sha256=_require_optional_string(
                document["silver_byte_sha256"], "silver_byte_sha256"
            ),
            counts=SourceSnapshotCounts(**counts_document),
            planned_parts=_parse_parts(document["planned_parts"], "planned_parts"),
            completed_parts=_parse_parts(
                document["completed_parts"], "completed_parts"
            ),
        )
    except SourceSnapshotContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceSnapshotContractError(
            "source snapshot manifest를 파싱할 수 없습니다."
        ) from exc


def validate_source_snapshot_manifest(manifest: SourceSnapshotManifest) -> None:
    """Typed source snapshot manifest의 모든 authority 불변식을 검증한다."""
    if type(manifest) is not SourceSnapshotManifest:
        raise SourceSnapshotContractError(
            "manifest는 SourceSnapshotManifest여야 합니다."
        )
    if manifest.schema_version != SOURCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION:
        raise SourceSnapshotContractError(
            "지원하지 않는 source snapshot schema_version입니다."
        )
    _require_source_id(manifest.source_id)
    try:
        format_utc_dttm(manifest.logical_dttm)
    except (TypeError, ValueError) as exc:
        raise SourceSnapshotContractError("logical_dttm이 유효하지 않습니다.") from exc
    _require_nonnegative_integer(manifest.revision_no, "revision_no")
    if type(manifest.status) is not SourceSnapshotStatus:
        raise SourceSnapshotContractError("status는 SourceSnapshotStatus여야 합니다.")
    _require_nonblank_nfc(manifest.config_version, "config_version")
    if type(manifest.counts) is not SourceSnapshotCounts:
        raise SourceSnapshotContractError("counts는 SourceSnapshotCounts여야 합니다.")
    _validate_parts(manifest.planned_parts, "planned_parts")
    _validate_parts(manifest.completed_parts, "completed_parts")
    if manifest.planned_parts != manifest.completed_parts:
        raise SourceSnapshotContractError(
            "authoritative snapshot은 planned_parts와 completed_parts가 같아야 합니다."
        )

    if manifest.status is SourceSnapshotStatus.SUCCEEDED:
        _validate_succeeded(manifest)
    else:
        _validate_empty(manifest)
    canonical_json_bytes(_manifest_document(manifest))


def same_source_snapshot_content(
    left: SourceSnapshotManifest,
    right: SourceSnapshotManifest,
) -> bool:
    """Revision을 제외한 source snapshot identity가 정확히 같은지 반환한다."""
    validate_source_snapshot_manifest(left)
    validate_source_snapshot_manifest(right)
    if (left.source_id, left.logical_dttm) != (right.source_id, right.logical_dttm):
        return False
    return (
        replace(left, revision_no=0).canonical_bytes
        == replace(right, revision_no=0).canonical_bytes
    )


def _validate_succeeded(manifest: SourceSnapshotManifest) -> None:
    """SUCCEEDED source snapshot의 Silver와 count 결합을 검증한다."""
    if manifest.silver_uri is None or manifest.silver_byte_sha256 is None:
        raise SourceSnapshotContractError(
            "SUCCEEDED snapshot은 silver_uri와 silver_byte_sha256이 필요합니다."
        )
    checksum = validate_sha256_hex(manifest.silver_byte_sha256)
    _validate_content_addressed_s3_uri(manifest.silver_uri, checksum)
    if manifest.counts.fetched == 0 or manifest.counts.kept == 0:
        raise SourceSnapshotContractError(
            "SUCCEEDED snapshot은 한 행 이상이어야 합니다."
        )
    if manifest.counts.dropped != 0:
        raise SourceSnapshotContractError(
            "SUCCEEDED snapshot은 dropped=0이어야 합니다."
        )
    if (
        manifest.counts.expected is not None
        and manifest.counts.expected != manifest.counts.fetched
    ):
        raise SourceSnapshotContractError(
            "SUCCEEDED snapshot의 expected와 fetched가 다릅니다."
        )
    if not manifest.completed_parts:
        raise SourceSnapshotContractError(
            "SUCCEEDED snapshot은 하나 이상의 completed part가 필요합니다."
        )


def _validate_empty(manifest: SourceSnapshotManifest) -> None:
    """Confirmed EMPTY source snapshot의 무산출물 증거를 검증한다."""
    if manifest.silver_uri is not None or manifest.silver_byte_sha256 is not None:
        raise SourceSnapshotContractError(
            "EMPTY snapshot은 Silver artifact가 없어야 합니다."
        )
    if manifest.counts != SourceSnapshotCounts(0, 0, 0, 0, 0):
        raise SourceSnapshotContractError(
            "EMPTY snapshot은 expected를 포함한 모든 count가 0이어야 합니다."
        )
    if not manifest.completed_parts:
        raise SourceSnapshotContractError(
            "EMPTY snapshot은 pagination 완료를 증명할 completed part가 필요합니다."
        )


def _manifest_document(manifest: SourceSnapshotManifest) -> dict[str, JsonValue]:
    """Typed manifest를 exact 11-key JSON object로 바꾼다."""
    return {
        "completed_parts": list(manifest.completed_parts),
        "config_version": manifest.config_version,
        "counts": {
            "dropped": manifest.counts.dropped,
            "expected": manifest.counts.expected,
            "fetched": manifest.counts.fetched,
            "kept": manifest.counts.kept,
            "repaired": manifest.counts.repaired,
        },
        "logical_dttm": format_utc_dttm(manifest.logical_dttm),
        "planned_parts": list(manifest.planned_parts),
        "revision_no": manifest.revision_no,
        "schema_version": manifest.schema_version,
        "silver_byte_sha256": manifest.silver_byte_sha256,
        "silver_uri": manifest.silver_uri,
        "source_id": manifest.source_id,
        "status": manifest.status.value,
    }


def _require_object(
    value: JsonValue,
    expected_keys: frozenset[str],
    label: str,
) -> dict[str, JsonValue]:
    """값이 exact key 집합을 가진 builtin dict인지 확인한다."""
    if type(value) is not dict:
        raise SourceSnapshotContractError(f"{label}는 JSON object여야 합니다.")
    document = cast(dict[str, JsonValue], value)
    if frozenset(document) != expected_keys:
        raise SourceSnapshotContractError(f"{label} key가 정확하지 않습니다.")
    return document


def _require_string(value: JsonValue, label: str) -> str:
    """값이 exact builtin string인지 확인한다."""
    if type(value) is not str:
        raise SourceSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    return value


def _require_optional_string(value: JsonValue, label: str) -> str | None:
    """값이 null 또는 exact builtin string인지 확인한다."""
    if value is None:
        return None
    return _require_string(value, label)


def _require_nonnegative_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 0 이상의 builtin integer인지 확인한다."""
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        raise SourceSnapshotContractError(
            f"{label}은 canonical JSON safe range의 0 이상 정수여야 합니다."
        )
    return value


def _require_nonblank_nfc(value: str, label: str) -> str:
    """문자열이 공백 아닌 NFC exact builtin string인지 확인한다."""
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or unicodedata.normalize("NFC", value) != value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise SourceSnapshotContractError(
            f"{label}은 공백·제어 문자 없는 NFC 문자열이어야 합니다."
        )
    return value


def _require_source_id(value: str) -> str:
    """Source ID가 경로에도 안전한 snake_case인지 확인한다."""
    if type(value) is not str or _SOURCE_ID_PATTERN.fullmatch(value) is None:
        raise SourceSnapshotContractError(
            "source_id는 lowercase snake_case여야 합니다."
        )
    return value


def _parse_parts(value: JsonValue, label: str) -> tuple[str, ...]:
    """Canonical JSON array를 part tuple로 바꾼다. 값 검증은 생성자가 한다."""
    if type(value) is not list:
        raise SourceSnapshotContractError(f"{label}는 JSON array여야 합니다.")
    return tuple(value)


def _validate_parts(value: tuple[str, ...], label: str) -> None:
    """Part tuple의 exact type, 값, 정렬, 중복 없음을 검증한다."""
    if type(value) is not tuple:
        raise SourceSnapshotContractError(f"{label}는 tuple이어야 합니다.")
    for part in value:
        _require_nonblank_nfc(part, label)
        if "/" in part or "\\" in part:
            raise SourceSnapshotContractError(
                f"{label}에는 경로 구분자를 쓸 수 없습니다."
            )
    expected = tuple(sorted(value, key=lambda item: item.encode("utf-8")))
    if value != expected or len(set(value)) != len(value):
        raise SourceSnapshotContractError(
            f"{label}는 UTF-8 byte 순으로 정렬되고 중복이 없어야 합니다."
        )


def _validate_content_addressed_s3_uri(uri: str, checksum: str) -> None:
    """Silver URI가 checksum segment를 가진 exact S3 object인지 검증한다."""
    _require_nonblank_nfc(uri, "silver_uri")
    if "?" in uri or "#" in uri:
        raise SourceSnapshotContractError(
            "silver_uri에는 query나 fragment를 쓸 수 없습니다."
        )
    try:
        parsed = urlsplit(uri)
    except ValueError as exc:
        raise SourceSnapshotContractError("silver_uri를 해석할 수 없습니다.") from exc
    segments = parsed.path.removeprefix("/").split("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.path in {"", "/"}
        or "@" in parsed.netloc
        or ":" in parsed.netloc
        or segments[-1] != f"sha256={checksum}.parquet"
    ):
        raise SourceSnapshotContractError(
            "silver_uri는 byte SHA-256으로 content-addressed된 S3 parquet이어야 합니다."
        )
