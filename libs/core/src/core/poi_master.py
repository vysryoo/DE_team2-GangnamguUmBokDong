"""POI Master activation과 exact source snapshot 소비 계약을 제공한다."""

from __future__ import annotations

import math
import re
import struct
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any, cast
from urllib.parse import urlsplit

import pyarrow as pa
from botocore.exceptions import BotoCoreError, ClientError

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
        document = cast(dict[str, object], value)
        if frozenset(document) != _REF_KEYS:
            raise PoiMasterContractError("POI Master ref key가 정확하지 않습니다.")
        mode = document["mode"]
        manifest_uri = document["manifest_uri"]
        manifest_sha256 = document["manifest_sha256"]
        if type(mode) is not str:
            raise PoiMasterContractError("POI Master ref mode는 문자열이어야 합니다.")
        if manifest_uri is not None and type(manifest_uri) is not str:
            raise PoiMasterContractError(
                "POI Master manifest_uri는 문자열 또는 null이어야 합니다."
            )
        if manifest_sha256 is not None and type(manifest_sha256) is not str:
            raise PoiMasterContractError(
                "POI Master manifest_sha256은 문자열 또는 null이어야 합니다."
            )
        return cls(
            mode=mode,
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha256,
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
    return pointer.ref


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
    if type(table) is not pa.Table:
        raise PoiMasterContractError("POI Master artifact는 PyArrow Table이어야 합니다.")
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
    if table.num_rows <= 0:
        raise PoiMasterContractError("POI Master에는 한 행 이상 있어야 합니다.")

    area_codes = table.column("AREA_CD").to_pylist()
    if any(type(value) is not str or _AREA_CODE.fullmatch(value) is None for value in area_codes):
        raise PoiMasterContractError(
            "POI Master AREA_CD는 null이 아닌 POI 세 자리 코드여야 합니다."
        )
    if len(area_codes) != len(set(area_codes)):
        raise PoiMasterContractError("POI Master AREA_CD는 고유해야 합니다.")
    if area_codes != sorted(area_codes):
        raise PoiMasterContractError("POI Master AREA_CD는 오름차순이어야 합니다.")

    for column_name in _REQUIRED_TEXT_COLUMNS:
        values = table.column(column_name).to_pylist()
        if any(
            type(value) is not str or not value or value != value.strip()
            for value in values
        ):
            raise PoiMasterContractError(
                f"POI Master {column_name}은 공백 없는 trimmed 문자열이어야 합니다."
            )

    source_numbers = table.column("SOURCE_NO").to_pylist()
    if any(type(value) is not int or value <= 0 for value in source_numbers):
        raise PoiMasterContractError("POI Master SOURCE_NO는 양의 정수여야 합니다.")
    if len(source_numbers) != len(set(source_numbers)):
        raise PoiMasterContractError("POI Master SOURCE_NO는 고유해야 합니다.")

    areas = table.column("AREA_M2").to_pylist()
    if any(
        type(value) is not float or not math.isfinite(value) or value <= 0
        for value in areas
    ):
        raise PoiMasterContractError(
            "POI Master AREA_M2는 유한한 양의 실수여야 합니다."
        )

    for area_code, geometry_wkb, declared_area in zip(
        area_codes,
        table.column("GEOMETRY_WKB").to_pylist(),
        areas,
        strict=True,
    ):
        try:
            computed_area = _validate_polygon_wkb(geometry_wkb)
        except PoiMasterContractError as exc:
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


def _validate_polygon_wkb(payload: object) -> float:
    """외부 geometry 의존성 없이 2D Polygon WKB 구조를 검증하고 면적을 반환한다."""
    if type(payload) is not bytes or len(payload) < 9:
        raise PoiMasterContractError("Polygon WKB bytes가 없거나 너무 짧습니다.")
    byte_order = payload[0]
    if byte_order not in {0, 1}:
        raise PoiMasterContractError("Polygon WKB byte order가 잘못됐습니다.")
    endian = "<" if byte_order == 1 else ">"
    geometry_type = struct.unpack_from(f"{endian}I", payload, 1)[0]
    if geometry_type != 3:
        raise PoiMasterContractError("Geometry는 SRID/Z/M 없는 2D Polygon이어야 합니다.")
    ring_count = struct.unpack_from(f"{endian}I", payload, 5)[0]
    if ring_count < 1:
        raise PoiMasterContractError("Polygon에는 ring이 하나 이상 있어야 합니다.")

    offset = 9
    ring_areas: list[float] = []
    for _ring_index in range(ring_count):
        if offset + 4 > len(payload):
            raise PoiMasterContractError("Polygon ring header가 잘렸습니다.")
        point_count = struct.unpack_from(f"{endian}I", payload, offset)[0]
        offset += 4
        point_bytes = point_count * 16
        if point_count < 4 or offset + point_bytes > len(payload):
            raise PoiMasterContractError("Polygon ring point 수 또는 길이가 잘못됐습니다.")
        points: list[tuple[float, float]] = []
        for point_offset in range(offset, offset + point_bytes, 16):
            point = struct.unpack_from(f"{endian}dd", payload, point_offset)
            if not all(math.isfinite(coordinate) for coordinate in point):
                raise PoiMasterContractError("Polygon 좌표는 유한해야 합니다.")
            points.append(point)
        if points[0] != points[-1] or len(set(points[:-1])) < 3:
            raise PoiMasterContractError(
                "Polygon ring은 닫혀 있고 서로 다른 점이 세 개 이상이어야 합니다."
            )
        origin_x, origin_y = points[0]
        twice_area = math.fsum(
            (first[0] - origin_x) * (second[1] - origin_y)
            - (second[0] - origin_x) * (first[1] - origin_y)
            for first, second in pairwise(points)
        )
        if twice_area == 0:
            raise PoiMasterContractError("Polygon ring 면적은 0보다 커야 합니다.")
        ring_areas.append(abs(twice_area) / 2.0)
        offset += point_bytes
    if offset != len(payload):
        raise PoiMasterContractError("Polygon WKB 뒤에 불필요한 bytes가 있습니다.")
    polygon_area = ring_areas[0] - math.fsum(ring_areas[1:])
    if not math.isfinite(polygon_area) or polygon_area <= 0:
        raise PoiMasterContractError("Polygon의 ring 면적 합계는 0보다 커야 합니다.")
    return polygon_area


def _validate_poi_master_topology(table: pa.Table) -> None:
    """Activation 직전에 모든 Polygon ring의 단순성과 포함 관계를 검증한다."""
    for area_code, geometry_wkb in zip(
        table.column("AREA_CD").to_pylist(),
        table.column("GEOMETRY_WKB").to_pylist(),
        strict=True,
    ):
        try:
            rings = _polygon_rings(geometry_wkb)
            _validate_polygon_rings(rings)
        except PoiMasterContractError as exc:
            raise PoiMasterContractError(
                f"POI Master {area_code} Polygon 위상이 유효하지 않습니다."
            ) from exc


def _polygon_rings(payload: bytes) -> tuple[tuple[tuple[float, float], ...], ...]:
    """구조 검증을 통과한 2D Polygon WKB의 ring 좌표를 반환한다."""
    byte_order = payload[0]
    endian = "<" if byte_order == 1 else ">"
    ring_count = struct.unpack_from(f"{endian}I", payload, 5)[0]
    offset = 9
    rings: list[tuple[tuple[float, float], ...]] = []
    for _ring_index in range(ring_count):
        point_count = struct.unpack_from(f"{endian}I", payload, offset)[0]
        offset += 4
        points = tuple(
            struct.unpack_from(f"{endian}dd", payload, point_offset)
            for point_offset in range(offset, offset + point_count * 16, 16)
        )
        rings.append(points)
        offset += point_count * 16
    return tuple(rings)


def _validate_polygon_rings(
    rings: tuple[tuple[tuple[float, float], ...], ...],
) -> None:
    """Polygon exterior와 hole이 OGC의 단순 ring·포함 불변식을 지키는지 검사한다."""
    normalized_rings = tuple(
        _collapse_consecutive_points(ring) for ring in rings
    )
    for ring in normalized_rings:
        _validate_simple_ring(ring)

    exterior = normalized_rings[0]
    holes = normalized_rings[1:]
    for hole in holes:
        hole_locations = tuple(
            (
                _point_inside_ring(point, exterior),
                _point_on_ring(point, exterior),
            )
            for point in hole[:-1]
        )
        if (
            _rings_have_invalid_contact(exterior, hole)
            or not any(inside for inside, _on_boundary in hole_locations)
            or not all(inside or on_boundary for inside, on_boundary in hole_locations)
        ):
            raise PoiMasterContractError(
                "Polygon hole은 exterior 안에 있고 경계를 교차하면 안 됩니다."
            )
    for first_index, first_hole in enumerate(holes):
        for second_hole in holes[first_index + 1 :]:
            if (
                _rings_have_invalid_contact(first_hole, second_hole)
                or any(
                    _point_inside_ring(point, second_hole)
                    for point in first_hole[:-1]
                )
                or any(
                    _point_inside_ring(point, first_hole)
                    for point in second_hole[:-1]
                )
            ):
                raise PoiMasterContractError(
                    "Polygon hole은 서로 교차·겹치거나 포함할 수 없습니다."
                )


def _collapse_consecutive_points(
    ring: tuple[tuple[float, float], ...],
) -> tuple[tuple[float, float], ...]:
    """위상을 바꾸지 않는 연속 중복 좌표를 검증용 ring에서 제거한다."""
    return tuple(
        point
        for index, point in enumerate(ring)
        if index == 0 or point != ring[index - 1]
    )


def _validate_simple_ring(ring: tuple[tuple[float, float], ...]) -> None:
    """Ring에 길이 0인 edge나 비인접 edge 교차가 없는지 검사한다."""
    segments = tuple(pairwise(ring))
    if any(start == end for start, end in segments):
        raise PoiMasterContractError("Polygon ring에 길이 0인 edge가 있습니다.")
    last_index = len(segments) - 1
    for first_index, first_segment in enumerate(segments):
        for second_index in range(first_index + 1, len(segments)):
            if second_index == first_index + 1 or (
                first_index == 0 and second_index == last_index
            ):
                continue
            if _segments_intersect(first_segment, segments[second_index]):
                raise PoiMasterContractError(
                    "Polygon ring의 비인접 edge가 교차하거나 맞닿습니다."
                )


def _rings_have_invalid_contact(
    first: tuple[tuple[float, float], ...],
    second: tuple[tuple[float, float], ...],
) -> bool:
    """두 ring이 교차·겹치거나 둘 이상의 점에서 접하는지 반환한다."""
    contacts: set[tuple[float, float]] = set()
    for first_segment in pairwise(first):
        for second_segment in pairwise(second):
            segment_contacts = _segment_contacts(first_segment, second_segment)
            if segment_contacts is None:
                return True
            contacts.update(segment_contacts)
            if len(contacts) > 1:
                return True
    return False


def _segment_contacts(
    first: tuple[tuple[float, float], tuple[float, float]],
    second: tuple[tuple[float, float], tuple[float, float]],
) -> set[tuple[float, float]] | None:
    """두 선분의 끝점 접촉을 반환하고 교차·선 겹침이면 None을 반환한다."""
    first_start, first_end = first
    second_start, second_end = second
    orientations = (
        _orientation(first_start, first_end, second_start),
        _orientation(first_start, first_end, second_end),
        _orientation(second_start, second_end, first_start),
        _orientation(second_start, second_end, first_end),
    )
    if (
        orientations[0] * orientations[1] < 0
        and orientations[2] * orientations[3] < 0
    ):
        return None
    contacts = {
        point
        for point in (first_start, first_end, second_start, second_end)
        if _orientation(first_start, first_end, point) == 0
        and _point_on_segment(point, first_start, first_end)
        and _orientation(second_start, second_end, point) == 0
        and _point_on_segment(point, second_start, second_end)
    }
    if len(contacts) > 1:
        return None
    return contacts


def _segments_intersect(
    first: tuple[tuple[float, float], tuple[float, float]],
    second: tuple[tuple[float, float], tuple[float, float]],
) -> bool:
    """두 닫힌 선분이 교차·접촉·중첩하는지 반환한다."""
    first_start, first_end = first
    second_start, second_end = second
    orientations = (
        _orientation(first_start, first_end, second_start),
        _orientation(first_start, first_end, second_end),
        _orientation(second_start, second_end, first_start),
        _orientation(second_start, second_end, first_end),
    )
    if orientations[0] == 0 and _point_on_segment(
        second_start, first_start, first_end
    ):
        return True
    if orientations[1] == 0 and _point_on_segment(
        second_end, first_start, first_end
    ):
        return True
    if orientations[2] == 0 and _point_on_segment(
        first_start, second_start, second_end
    ):
        return True
    if orientations[3] == 0 and _point_on_segment(
        first_end, second_start, second_end
    ):
        return True
    return orientations[0] != orientations[1] and orientations[2] != orientations[3]


def _orientation(
    first: tuple[float, float],
    second: tuple[float, float],
    third: tuple[float, float],
) -> int:
    """세 점의 방향을 반시계 1, 시계 -1, 일직선 0으로 반환한다."""
    cross = (second[0] - first[0]) * (third[1] - first[1]) - (
        second[1] - first[1]
    ) * (third[0] - first[0])
    return (cross > 0) - (cross < 0)


def _point_on_segment(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> bool:
    """일직선인 점이 닫힌 선분의 bounding box 안에 있는지 반환한다."""
    return (
        min(start[0], end[0]) <= point[0] <= max(start[0], end[0])
        and min(start[1], end[1]) <= point[1] <= max(start[1], end[1])
    )


def _point_inside_ring(
    point: tuple[float, float],
    ring: tuple[tuple[float, float], ...],
) -> bool:
    """점이 ring 경계가 아닌 내부에 있는지 ray casting으로 판정한다."""
    inside = False
    for start, end in pairwise(ring):
        if _orientation(start, end, point) == 0 and _point_on_segment(
            point, start, end
        ):
            return False
        if (start[1] > point[1]) != (end[1] > point[1]):
            crossing_x = start[0] + (
                (point[1] - start[1]) * (end[0] - start[0])
                / (end[1] - start[1])
            )
            if crossing_x > point[0]:
                inside = not inside
    return inside


def _point_on_ring(
    point: tuple[float, float],
    ring: tuple[tuple[float, float], ...],
) -> bool:
    """점이 ring의 어느 닫힌 선분 위에 있는지 반환한다."""
    return any(
        _orientation(start, end, point) == 0
        and _point_on_segment(point, start, end)
        for start, end in pairwise(ring)
    )


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
    if type(key) is not str:
        raise PoiMasterReadError("POI Master activation key는 문자열이어야 합니다.")
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
    if type(value) is not str:
        raise PoiMasterContractError(f"{label}은 문자열이어야 합니다.")
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
