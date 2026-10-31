"""POI Master activation과 exact source snapshot 소비 계약을 제공한다."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlsplit

import pyarrow as pa
from botocore.exceptions import BotoCoreError, ClientError

from ._polygon_wkb import (
    PolygonWkbError,
    parse_polygon_wkb,
    polygon_area,
    validate_polygon_rings,
)
from .gold_publication.canonical import (
    JsonValue,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
    validate_sha256_hex,
)
from .gold_publication.errors import (
    CanonicalParseError,
    HashFormatError,
    ImmutableObjectError,
    TimestampFormatError,
)
from .gold_publication.storage import S3ImmutableObjectStore
from .s3 import get_object_bytes, list_keys, object_uri
from .source_snapshot import SourceSnapshotStatus
from .source_snapshot_io import (
    SourceSnapshotReadError,
    read_exact_source_snapshot_manifest,
)
from .sources import POI_MASTER

POI_MASTER_SOURCE_ID = POI_MASTER
"""POI Master source snapshot의 고정 source ID다."""

POI_MASTER_SCHEMA_VERSION = "poi-master-v1"
"""Collector와 Normalizer가 소비할 POI Master Table 계약 버전이다."""

POI_MASTER_READABLE_SCHEMA_VERSIONS = frozenset({POI_MASTER_SCHEMA_VERSION})
"""현재 consumer가 같은 물리 schema로 읽을 수 있는 POI Master 버전 집합이다."""

POI_MASTER_ACTIVATION_SCHEMA_VERSION = "poi-master-activation-v1"
"""Append-only POI Master activation pointer의 schema version이다."""

POI_MASTER_POINTER_PREFIX = "source_snapshot_pointer/poi_master/"
"""POI Master activation pointer를 나열하는 전용 S3 prefix다."""

_ACTIVATION_KEYS = frozenset(
    {
        "activated_at",
        "manifest_byte_sha256",
        "manifest_uri",
        "schema_version",
        "source_id",
    }
)
_REF_KEYS = frozenset({"manifest_sha256", "manifest_uri", "mode"})
_ACTIVATION_KEY = re.compile(
    rf"\A{re.escape(POI_MASTER_POINTER_PREFIX)}"
    r"activated=(?P<activated>\d{8}T\d{12}Z)\.json\Z"
)
_POI_MANIFEST_KEY = re.compile(
    r"\Asource_snapshot_manifest/poi_master/"
    r"dt=\d{4}-\d{2}-\d{2}/hh=\d{2}/"
    r"logical=\d{8}T\d{12}Z/revision=\d{10}\.json\Z"
)
_POI_MASTER_SCHEMA = pa.schema(
    [
        ("AREA_CD", pa.string()),
        ("AREA_NM", pa.string()),
        ("CATEGORY", pa.string()),
        ("ENG_NM", pa.string()),
        ("SOURCE_NO", pa.int64()),
        ("GEOMETRY_WKB", pa.binary()),
        ("AREA_M2", pa.float64()),
    ]
)
_GEOMETRY_CRS_METADATA_KEY = b"geometry_crs"
_GEOMETRY_CRS = b"EPSG:5179"
_POI_MASTER_SCHEMA_VERSION_METADATA_KEY = b"poi_master_schema_version"
_AREA_CODE = re.compile(r"\APOI[0-9]{3}\Z")
_REQUIRED_TEXT_COLUMNS = ("AREA_NM", "CATEGORY", "ENG_NM")


class PoiMasterError(Exception):
    """POI Master resolve, read, activation 처리의 기반 예외다."""


class PoiMasterContractError(PoiMasterError, ValueError):
    """POI Master ref, activation 또는 Table이 계약을 위반했다."""


class PoiMasterReadError(PoiMasterError, RuntimeError):
    """고정한 POI Master authority나 artifact를 안전하게 읽을 수 없다."""


class PoiMasterActivationError(PoiMasterError, RuntimeError):
    """POI Master activation pointer를 immutable하게 게시할 수 없다."""


@dataclass(frozen=True, slots=True)
class PoiMasterRef:
    """Static fallback 또는 exact S3 manifest를 가리키는 실행 입력 ref다."""

    mode: str
    manifest_uri: str | None = None
    manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        """Mode별 필드 조합과 S3 manifest identity를 검증한다."""
        if type(self.mode) is not str or self.mode not in {"static", "s3"}:
            raise PoiMasterContractError("POI Master mode는 static 또는 s3여야 합니다.")
        if self.mode == "static":
            if self.manifest_uri is not None or self.manifest_sha256 is not None:
                raise PoiMasterContractError(
                    "Static POI Master ref에는 manifest 정보가 없어야 합니다."
                )
            return
        if type(self.manifest_uri) is not str:
            raise PoiMasterContractError(
                "S3 POI Master ref에는 manifest_uri가 필요합니다."
            )
        _validate_poi_manifest_uri(self.manifest_uri)
        _validated_sha256(self.manifest_sha256, "manifest_sha256")

    def as_dict(self) -> dict[str, str | None]:
        """XCom과 CLI JSON에 쓸 exact 3-key dict를 반환한다."""
        return {
            "mode": self.mode,
            "manifest_uri": self.manifest_uri,
            "manifest_sha256": self.manifest_sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> PoiMasterRef:
        """Exact 3-key builtin dict를 검증해 POI Master ref로 파싱한다."""
        if type(value) is not dict:
            raise PoiMasterContractError("POI Master ref는 JSON object여야 합니다.")
        document = cast(dict[str, Any], value)
        if frozenset(document) != _REF_KEYS:
            raise PoiMasterContractError("POI Master ref key가 정확하지 않습니다.")
        # 필드 타입과 mode별 조합은 __post_init__이 검증함
        return cls(
            mode=document["mode"],
            manifest_uri=document["manifest_uri"],
            manifest_sha256=document["manifest_sha256"],
        )


@dataclass(frozen=True, slots=True)
class PoiMasterActivation:
    """한 시각부터 사용할 exact POI Master manifest를 고정하는 pointer다."""

    schema_version: str
    source_id: str
    activated_at: datetime
    manifest_uri: str
    manifest_byte_sha256: str

    def __post_init__(self) -> None:
        """Activation scalar, 시각, manifest URI와 SHA를 검증한다."""
        if self.schema_version != POI_MASTER_ACTIVATION_SCHEMA_VERSION:
            raise PoiMasterContractError(
                "지원하지 않는 POI Master activation schema_version입니다."
            )
        if self.source_id != POI_MASTER_SOURCE_ID:
            raise PoiMasterContractError(
                "POI Master activation source_id가 잘못됐습니다."
            )
        object.__setattr__(self, "activated_at", _aware_utc(self.activated_at))
        _validate_poi_manifest_uri(self.manifest_uri)
        _validated_sha256(
            self.manifest_byte_sha256,
            "manifest_byte_sha256",
        )

    @property
    def canonical_bytes(self) -> bytes:
        """Activation pointer의 exact canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(
            {
                "activated_at": format_utc_dttm(self.activated_at),
                "manifest_byte_sha256": self.manifest_byte_sha256,
                "manifest_uri": self.manifest_uri,
                "schema_version": self.schema_version,
                "source_id": self.source_id,
            }
        )

    @property
    def ref(self) -> PoiMasterRef:
        """Activation이 고정한 exact S3 POI Master ref를 반환한다."""
        return PoiMasterRef(
            mode="s3",
            manifest_uri=self.manifest_uri,
            manifest_sha256=self.manifest_byte_sha256,
        )


def parse_poi_master_activation(payload: bytes) -> PoiMasterActivation:
    """Canonical bytes를 exact POI Master activation pointer로 파싱한다."""
    try:
        value = parse_canonical_json(payload)
        if type(value) is not dict:
            raise PoiMasterContractError(
                "POI Master activation은 JSON object여야 합니다."
            )
        document = cast(dict[str, JsonValue], value)
        if frozenset(document) != _ACTIVATION_KEYS:
            raise PoiMasterContractError(
                "POI Master activation key가 정확하지 않습니다."
            )
        return PoiMasterActivation(
            schema_version=_required_string(
                document["schema_version"], "schema_version"
            ),
            source_id=_required_string(document["source_id"], "source_id"),
            activated_at=parse_utc_dttm(
                _required_string(document["activated_at"], "activated_at")
            ),
            manifest_uri=_required_string(
                document["manifest_uri"], "manifest_uri"
            ),
            manifest_byte_sha256=_required_string(
                document["manifest_byte_sha256"],
                "manifest_byte_sha256",
            ),
        )
    except PoiMasterContractError:
        raise
    except (CanonicalParseError, TimestampFormatError) as exc:
        raise PoiMasterContractError(
            "POI Master activation을 파싱할 수 없습니다."
        ) from exc


def resolve_poi_master(as_of: datetime) -> PoiMasterRef:
    """기준 시각 이전 최신 activation을 찾고 없으면 static ref를 반환한다.

    Pointer key에서 먼저 activation 시각을 복원하므로 ``as_of``보다 미래인 pointer는
    본문을 읽지 않는다. 선택 시각에 서로 다른 manifest pointer가 둘 이상 있으면
    임의로 하나를 고르지 않고 fail-closed한다.
    """
    cutoff = _aware_utc(as_of)
    try:
        keys = list_keys(POI_MASTER_POINTER_PREFIX)
    except (BotoCoreError, ClientError) as exc:
        raise PoiMasterReadError(
            "POI Master activation pointer 목록을 읽을 수 없습니다."
        ) from exc

    candidates: list[tuple[datetime, str]] = []
    for key in keys:
        activated_at = _activation_time_from_key(key)
        if activated_at <= cutoff:
            candidates.append((activated_at, key))
    if not candidates:
        return PoiMasterRef(mode="static")

    latest_at = max(item[0] for item in candidates)
    latest = [item for item in candidates if item[0] == latest_at]
    if len(latest) != 1:
        raise PoiMasterReadError(
            "같은 시각의 POI Master activation pointer가 둘 이상입니다: "
            f"activated_at={format_utc_dttm(latest_at)}"
        )
    _activated_at, key = latest[0]
    pointer = _read_activation_pointer(key)
    if pointer.activated_at != latest_at or key != _activation_key(pointer):
        raise PoiMasterReadError(
            f"POI Master activation key와 본문 identity가 다릅니다: {key}"
        )
    return pointer.ref


def activate_poi_master(
    *,
    activated_at: datetime,
    manifest_uri: str,
    manifest_sha256: str,
) -> PoiMasterRef:
    """검증 완료 manifest를 append-only activation pointer로 마지막 게시한다.

    Manifest와 연결된 POI Master Parquet을 먼저 exact하게 읽고 전체 schema까지 검증한다.
    그 검증이 끝난 뒤에만 activation pointer를 put-once하며, 기록 직후 같은 URI를 다시
    읽어 canonical bytes와 identity를 확인한다. 동일 시각에 다른 manifest가 이미
    활성화됐으면 기존 pointer를 덮어쓰거나 임의로 우선순위를 정하지 않는다.
    """
    pointer = PoiMasterActivation(
        schema_version=POI_MASTER_ACTIVATION_SCHEMA_VERSION,
        source_id=POI_MASTER_SOURCE_ID,
        activated_at=activated_at,
        manifest_uri=manifest_uri,
        manifest_byte_sha256=manifest_sha256,
    )

    # Pointer는 publication의 마지막 단계다. 대상 authority와 전체 Table 계약을 먼저
    # 검증해서 잘못된 manifest가 discoverable해지는 순간을 만들지 않는다.
    candidate_table = read_poi_master(pointer.ref)
    try:
        _validate_poi_master_topology(candidate_table)
    except PoiMasterContractError as exc:
        raise PoiMasterActivationError(
            "POI Master geometry 위상 계약이 잘못되어 활성화할 수 없습니다."
        ) from exc

    _put_activation_pointer(pointer)
    return pointer.ref


def _put_activation_pointer(pointer: PoiMasterActivation) -> None:
    """Activation pointer를 put-once로 쓰고 readback이 원본과 같은지 확인한다."""
    uri = object_uri(_activation_key(pointer))
    try:
        readback = S3ImmutableObjectStore().put_verified(
            uri,
            pointer.canonical_bytes,
            require_canonical_json=True,
        )
    except ImmutableObjectError as exc:
        detail = "같은 시각의 다른 activation과 충돌했거나 S3 접근에 실패했습니다."
        raise PoiMasterActivationError(
            f"POI Master activation pointer 게시에 실패했습니다: {uri}; {detail}"
        ) from exc
    try:
        parsed = parse_poi_master_activation(readback)
    except PoiMasterContractError as exc:
        raise PoiMasterActivationError(
            f"POI Master activation pointer readback이 손상됐습니다: {uri}"
        ) from exc
    if parsed != pointer:
        raise PoiMasterActivationError(
            f"POI Master activation pointer readback이 원본과 다릅니다: {uri}"
        )


def read_poi_master(
    ref: PoiMasterRef,
    columns: list[str] | None = None,
) -> pa.Table:
    """Exact S3 ref의 POI Master를 읽고 schema·CRS를 검증한다.

    Static ref는 저장소 artifact를 가리키지 않는 bootstrap 신호다. 호출자가 기존 로컬
    fixture 경로로 명시적으로 분기해야 하며 이 함수는 조용히 다른 입력을 선택하지 않는다.
    S3 ref는 projection을 요청해도 전체 Table schema와 metadata를 먼저 검증한 뒤 선택한
    컬럼만 반환한다.
    """
    if type(ref) is not PoiMasterRef:
        raise TypeError("ref는 PoiMasterRef여야 합니다.")
    if ref.mode == "static":
        raise PoiMasterReadError(
            "Static POI Master ref는 호출자가 로컬 fixture로 분기해야 합니다."
        )
    selected_columns = _validated_columns(columns)
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None
    try:
        snapshot = read_exact_source_snapshot_manifest(
            ref.manifest_uri,
            ref.manifest_sha256,
        )
    except (SourceSnapshotReadError, ValueError) as exc:
        raise PoiMasterReadError(
            f"고정한 POI Master source snapshot을 읽을 수 없습니다: {ref.manifest_uri}"
        ) from exc
    if snapshot.manifest.source_id != POI_MASTER_SOURCE_ID:
        raise PoiMasterReadError(
            "고정한 source snapshot이 poi_master authority가 아닙니다."
        )
    if (
        snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED
        or snapshot.table is None
    ):
        raise PoiMasterReadError("POI Master source snapshot은 SUCCEEDED여야 합니다.")
    try:
        _validate_poi_master_table(snapshot.table)
    except PoiMasterContractError as exc:
        raise PoiMasterReadError(
            f"POI Master artifact가 table 계약을 위반했습니다: {ref.manifest_uri}"
        ) from exc
    if selected_columns is None:
        return snapshot.table
    return snapshot.table.select(selected_columns)


def _validate_poi_master_table(table: pa.Table) -> None:
    """POI Master Table의 schema, CRS와 모든 행 불변식을 검증한다."""
    _validate_schema_and_metadata(table)
    if table.num_rows <= 0:
        raise PoiMasterContractError("POI Master에는 한 행 이상 있어야 합니다.")
    _validate_row_values(table)
    _validate_declared_areas(table)


def _validate_schema_and_metadata(table: pa.Table) -> None:
    """Exact schema와 schema version·CRS metadata를 확인한다."""
    if not table.schema.remove_metadata().equals(
        _POI_MASTER_SCHEMA,
        check_metadata=True,
    ):
        raise PoiMasterContractError(
            "POI Master schema가 exact registry+geometry 계약과 다릅니다: "
            f"actual={table.schema.remove_metadata()}"
        )
    metadata = table.schema.metadata or {}
    raw_schema_version = metadata.get(_POI_MASTER_SCHEMA_VERSION_METADATA_KEY)
    try:
        schema_version = (
            raw_schema_version.decode("utf-8")
            if raw_schema_version is not None
            else None
        )
    except UnicodeDecodeError as exc:
        raise PoiMasterContractError(
            "POI Master schema metadata poi_master_schema_version이 UTF-8이 아닙니다."
        ) from exc
    if schema_version not in POI_MASTER_READABLE_SCHEMA_VERSIONS:
        raise PoiMasterContractError(
            "지원하지 않는 POI Master poi_master_schema_version metadata입니다: "
            f"actual={schema_version!r}, "
            f"readable={sorted(POI_MASTER_READABLE_SCHEMA_VERSIONS)}"
        )
    if metadata.get(_GEOMETRY_CRS_METADATA_KEY) != _GEOMETRY_CRS:
        raise PoiMasterContractError(
            "POI Master schema metadata geometry_crs는 EPSG:5179여야 합니다."
        )


def _validate_row_values(table: pa.Table) -> None:
    """코드·이름·번호·면적 값의 형식과 고유성을 확인한다.

    Exact schema를 통과한 뒤라 각 값은 선언 타입이거나 None이다.
    """
    area_codes = table.column("AREA_CD").to_pylist()
    if any(value is None or _AREA_CODE.fullmatch(value) is None for value in area_codes):
        raise PoiMasterContractError(
            "POI Master AREA_CD는 null이 아닌 POI 세 자리 코드여야 합니다."
        )
    if len(area_codes) != len(set(area_codes)):
        raise PoiMasterContractError("POI Master AREA_CD는 고유해야 합니다.")
    if area_codes != sorted(area_codes):
        raise PoiMasterContractError("POI Master AREA_CD는 오름차순이어야 합니다.")

    for column_name in _REQUIRED_TEXT_COLUMNS:
        values = table.column(column_name).to_pylist()
        if any(not value or value != value.strip() for value in values):
            raise PoiMasterContractError(
                f"POI Master {column_name}은 공백 없는 trimmed 문자열이어야 합니다."
            )

    source_numbers = table.column("SOURCE_NO").to_pylist()
    if any(value is None or value <= 0 for value in source_numbers):
        raise PoiMasterContractError("POI Master SOURCE_NO는 양의 정수여야 합니다.")
    if len(source_numbers) != len(set(source_numbers)):
        raise PoiMasterContractError("POI Master SOURCE_NO는 고유해야 합니다.")

    if any(
        value is None or not math.isfinite(value) or value <= 0
        for value in table.column("AREA_M2").to_pylist()
    ):
        raise PoiMasterContractError(
            "POI Master AREA_M2는 유한한 양의 실수여야 합니다."
        )


def _validate_declared_areas(table: pa.Table) -> None:
    """GEOMETRY_WKB가 유효한 2D Polygon이고 그 면적이 AREA_M2와 같은지 확인한다."""
    for area_code, geometry_wkb, declared_area in zip(
        table.column("AREA_CD").to_pylist(),
        table.column("GEOMETRY_WKB").to_pylist(),
        table.column("AREA_M2").to_pylist(),
        strict=True,
    ):
        try:
            computed_area = polygon_area(parse_polygon_wkb(geometry_wkb))
        except PolygonWkbError as exc:
            raise PoiMasterContractError(
                f"POI Master {area_code} GEOMETRY_WKB가 유효한 2D Polygon이 아닙니다."
            ) from exc
        if not math.isclose(
            declared_area,
            computed_area,
            rel_tol=1e-9,
            abs_tol=1e-6,
        ):
            raise PoiMasterContractError(
                f"POI Master {area_code} AREA_M2가 geometry 면적과 다릅니다."
            )


def _validate_poi_master_topology(table: pa.Table) -> None:
    """Activation 직전에 모든 Polygon ring의 단순성과 포함 관계를 검증한다."""
    for area_code, geometry_wkb in zip(
        table.column("AREA_CD").to_pylist(),
        table.column("GEOMETRY_WKB").to_pylist(),
        strict=True,
    ):
        try:
            validate_polygon_rings(parse_polygon_wkb(geometry_wkb))
        except PolygonWkbError as exc:
            raise PoiMasterContractError(
                f"POI Master {area_code} Polygon 위상이 유효하지 않습니다."
            ) from exc


def _validated_columns(columns: list[str] | None) -> list[str] | None:
    """Projection 컬럼이 exact builtin string 목록이고 schema 안에 있는지 검증한다."""
    if columns is None:
        return None
    if type(columns) is not list or any(type(column) is not str for column in columns):
        raise PoiMasterContractError("columns는 문자열 list여야 합니다.")
    if len(columns) != len(set(columns)):
        raise PoiMasterContractError("columns에는 중복 컬럼을 요청할 수 없습니다.")
    available = frozenset(_POI_MASTER_SCHEMA.names)
    unknown = [column for column in columns if column not in available]
    if unknown:
        raise PoiMasterContractError(
            f"POI Master schema에 없는 컬럼입니다: {unknown}"
        )
    return columns


def _read_activation_pointer(key: str) -> PoiMasterActivation:
    """나열된 activation pointer를 exact bytes와 canonical JSON으로 다시 읽는다."""
    uri = object_uri(key)
    try:
        first_read = get_object_bytes(key)
    except (BotoCoreError, ClientError) as exc:
        raise PoiMasterReadError(
            f"POI Master activation pointer를 읽을 수 없습니다: {uri}"
        ) from exc
    if first_read is None:
        raise PoiMasterReadError(
            f"나열된 POI Master activation pointer가 없습니다: {uri}"
        )
    checksum = sha256_hex(first_read)
    try:
        payload = S3ImmutableObjectStore().read_bytes(
            uri,
            checksum,
            require_canonical_json=True,
        )
        return parse_poi_master_activation(payload)
    except (ImmutableObjectError, PoiMasterContractError) as exc:
        raise PoiMasterReadError(
            f"POI Master activation pointer가 손상됐습니다: {uri}"
        ) from exc


def _activation_time_from_key(key: str) -> datetime:
    """Canonical activation key에서 UTC 시각을 복원한다."""
    matched = _ACTIVATION_KEY.fullmatch(key)
    if matched is None:
        raise PoiMasterReadError(
            f"POI Master activation key가 canonical하지 않습니다: {key}"
        )
    try:
        activated_at = datetime.strptime(
            matched.group("activated"),
            "%Y%m%dT%H%M%S%fZ",
        ).replace(tzinfo=UTC)
    except ValueError as exc:
        raise PoiMasterReadError(
            f"POI Master activation key 시각이 유효하지 않습니다: {key}"
        ) from exc
    if _compact_utc(activated_at) != matched.group("activated"):
        raise PoiMasterReadError(
            f"POI Master activation key 시각이 canonical하지 않습니다: {key}"
        )
    return activated_at


def _activation_key(pointer: PoiMasterActivation) -> str:
    """한 activation 시각을 단일 immutable S3 slot key로 변환한다."""
    return f"{POI_MASTER_POINTER_PREFIX}activated={_compact_utc(pointer.activated_at)}.json"


def _compact_utc(value: datetime) -> str:
    """Aware datetime을 경로용 UTC compact microsecond 문자열로 변환한다."""
    utc = _aware_utc(value)
    return f"{utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z"


def _aware_utc(value: datetime) -> datetime:
    """Timezone-aware datetime을 같은 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise TypeError("시각은 datetime이어야 합니다.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("시각은 timezone-aware datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _validate_poi_manifest_uri(uri: str) -> str:
    """URI가 poi_master 전용 canonical source manifest object인지 검증한다."""
    if type(uri) is not str or not uri or "?" in uri or "#" in uri:
        raise PoiMasterContractError(
            "POI Master manifest_uri는 query와 fragment 없는 S3 URI여야 합니다."
        )
    try:
        parsed = urlsplit(uri)
    except ValueError as exc:
        raise PoiMasterContractError(
            "POI Master manifest_uri를 해석할 수 없습니다."
        ) from exc
    key = parsed.path.removeprefix("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or "@" in parsed.netloc
        or ":" in parsed.netloc
        or parsed.path != f"/{key}"
        or _POI_MANIFEST_KEY.fullmatch(key) is None
    ):
        raise PoiMasterContractError(
            "POI Master manifest_uri가 canonical poi_master authority 경로가 아닙니다."
        )
    return uri


def _validated_sha256(value: object, label: str) -> str:
    """값을 lowercase SHA-256으로 검증하고 POI 계약 오류로 변환한다."""
    try:
        return validate_sha256_hex(value)
    except HashFormatError as exc:
        raise PoiMasterContractError(
            f"{label}은 정확히 64자리 lowercase SHA-256이어야 합니다."
        ) from exc


def _required_string(value: Any, label: str) -> str:
    """JSON 값이 exact builtin string인지 확인한다."""
    if type(value) is not str:
        raise PoiMasterContractError(f"{label}은 문자열이어야 합니다.")
    return value


__all__ = [
    "POI_MASTER_ACTIVATION_SCHEMA_VERSION",
    "POI_MASTER_POINTER_PREFIX",
    "POI_MASTER_READABLE_SCHEMA_VERSIONS",
    "POI_MASTER_SCHEMA_VERSION",
    "POI_MASTER_SOURCE_ID",
    "PoiMasterActivation",
    "PoiMasterActivationError",
    "PoiMasterContractError",
    "PoiMasterError",
    "PoiMasterReadError",
    "PoiMasterRef",
    "activate_poi_master",
    "parse_poi_master_activation",
    "read_poi_master",
    "resolve_poi_master",
]
