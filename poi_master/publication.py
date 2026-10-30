"""검증된 POI Master를 content-addressed S3 객체와 authority manifest로 게시한다."""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapefile
from core.gold_publication.errors import ImmutableObjectError
from core.gold_publication.storage import S3ImmutableObjectStore
from core.layout import source_snapshot_manifest_key
from core.poi_master import (
    POI_MASTER_SCHEMA_VERSION,
    PoiMasterRef,
    activate_poi_master,
    read_poi_master,
    resolve_poi_master,
)
from core.s3 import object_uri
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from core.source_snapshot_io import (
    SourceSnapshotData,
    SourceSnapshotReadError,
    read_exact_source_snapshot_manifest,
)
from core.sources import POI_MASTER

from registry import RegistryBuild, build_registry
from source import SourceAssets

_PARTS = ("areas_zip", "list_xlsx")
_AREA_CODE = re.compile(r"\APOI[0-9]{3}\Z")
_LEGACY_POI_SHP_PATH = (
    Path(__file__).resolve().parents[1]
    / "normalizer"
    / "data"
    / "poi_areas"
    / "seoul_121_poi_areas.shp"
)


class PoiPublicationError(RuntimeError):
    """POI Master 불변 객체 또는 활성화 게시가 실패했을 때 발생한다."""


@dataclass(frozen=True, slots=True)
class RefreshResult:
    """일일 확인의 변경 여부와 선택된 exact POI Master를 표현한다."""

    status: str
    ref: PoiMasterRef
    row_count: int
    list_sha256: str
    areas_sha256: str

    def as_dict(self) -> dict[str, object]:
        """CLI가 출력할 JSON 호환 문서로 변환한다."""
        return {
            "areas_sha256": self.areas_sha256,
            "list_sha256": self.list_sha256,
            "poi_master": self.ref.as_dict(),
            "row_count": self.row_count,
            "status": self.status,
        }


def _put_once(
    store: S3ImmutableObjectStore,
    key: str,
    payload: bytes,
    *,
    require_canonical_json: bool = False,
) -> None:
    """조건부 PUT으로 exact bytes를 한 번만 쓰고 다시 읽어 확인한다."""
    uri = object_uri(key)
    try:
        store.put_verified(uri, payload, require_canonical_json=require_canonical_json)
    except ImmutableObjectError as exc:
        raise PoiPublicationError(f"POI 불변 객체 게시에 실패했습니다: {uri}") from exc


def _parquet_bytes(table: pa.Table) -> bytes:
    """POI Master table을 고정 옵션의 Parquet bytes로 직렬화한다."""
    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    return buffer.getvalue()


def _raw_metadata_table(build: RegistryBuild) -> tuple[pa.Table, str, str]:
    """영구 보존할 원본 exact URI를 metadata에 더하고 raw key 두 개를 반환한다."""
    list_key = f"source_snapshot_raw/poi_master/list/sha256={build.list_sha256}.xlsx"
    areas_key = f"source_snapshot_raw/poi_master/areas/sha256={build.areas_sha256}.zip"
    metadata = dict(build.table.schema.metadata or {})
    metadata[b"list_uri"] = object_uri(list_key).encode("utf-8")
    metadata[b"areas_uri"] = object_uri(areas_key).encode("utf-8")
    return build.table.replace_schema_metadata(metadata), list_key, areas_key


def _metadata_text(table: pa.Table, key: str) -> str | None:
    """Arrow schema metadata의 UTF-8 값을 안전하게 읽는다."""
    raw = (table.schema.metadata or {}).get(key.encode("utf-8"))
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PoiPublicationError(
            f"활성 POI Master metadata가 UTF-8이 아닙니다: {key}"
        ) from exc


def _source_hashes(assets: SourceAssets) -> tuple[str, str]:
    """방금 내려받은 목록과 영역 파일의 SHA-256을 반환한다."""
    return (
        hashlib.sha256(assets.list_bytes).hexdigest(),
        hashlib.sha256(assets.areas_bytes).hexdigest(),
    )


def _config_version(
    list_sha256: str,
    areas_sha256: str,
    list_declared_place_count: int,
    areas_declared_place_count: int,
) -> str:
    """현재 변환 계약과 source checksum을 결합한 manifest config version을 만든다."""
    digest = hashlib.sha256(
        (
            f"{POI_MASTER_SCHEMA_VERSION}\n{list_sha256}\n{areas_sha256}\n"
            f"{list_declared_place_count}\n{areas_declared_place_count}"
        ).encode()
    ).hexdigest()
    return f"sha256:{digest}"


def _read_refresh_snapshot(ref: PoiMasterRef) -> SourceSnapshotData:
    """갱신 판단용 이전 snapshot을 exact ref로 읽어 schema migration을 허용한다.

    현재 schema version과 다른 이전 정상본은 consumer 계약으로 읽을 수 없지만 새
    artifact의 행 감소 기준에는 필요하다. Manifest와 Silver checksum·행 수는 공통
    source snapshot reader로 그대로 검증하고, 현재 version이면 consumer의 전체 Table
    계약을 별도로 적용한다.
    """
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None
    try:
        snapshot = read_exact_source_snapshot_manifest(
            ref.manifest_uri,
            ref.manifest_sha256,
        )
    except (SourceSnapshotReadError, ValueError) as exc:
        raise PoiPublicationError(
            f"활성 POI Master snapshot을 읽을 수 없습니다: {ref.manifest_uri}"
        ) from exc
    if (
        snapshot.manifest.source_id != POI_MASTER
        or snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED
        or snapshot.table is None
    ):
        raise PoiPublicationError("활성 POI Master snapshot이 SUCCEEDED 계약이 아닙니다.")
    return snapshot


def _stale_schema_previous_count(table: pa.Table) -> int:
    """구버전 Table에서 schema와 독립적인 고유 POI code 수만 감소 기준으로 읽는다."""
    if "AREA_CD" not in table.column_names or table.schema.field("AREA_CD").type != pa.string():
        raise PoiPublicationError(
            "구버전 POI Master에 string AREA_CD identity가 없습니다."
        )
    area_codes = table.column("AREA_CD").to_pylist()
    if (
        not area_codes
        or len(area_codes) != table.num_rows
        or any(
            type(area_code) is not str or _AREA_CODE.fullmatch(area_code) is None
            for area_code in area_codes
        )
        or len(area_codes) != len(set(area_codes))
    ):
        raise PoiPublicationError(
            "구버전 POI Master의 AREA_CD identity가 유효하거나 고유하지 않습니다."
        )
    return max(len(area_codes), _legacy_poi_count())


def _legacy_poi_count() -> int:
    """최초 활성화의 감소 기준이 되는 repository Shapefile 행 수를 반환한다."""
    try:
        with shapefile.Reader(str(_LEGACY_POI_SHP_PATH)) as reader:
            row_count = len(reader)
    except (OSError, shapefile.ShapefileException) as exc:
        raise PoiPublicationError(
            f"기존 POI Shapefile 행 수를 읽을 수 없습니다: {_LEGACY_POI_SHP_PATH}"
        ) from exc
    if row_count <= 0:
        raise PoiPublicationError("기존 POI Shapefile의 행 수가 0 이하입니다.")
    return row_count


def _validate_drop_guard(
    previous_count: int, candidate_count: int, max_drop_ratio: float
) -> None:
    """이전 정상본 대비 지나치게 큰 행 감소를 활성화 전에 차단한다."""
    if isinstance(max_drop_ratio, bool) or not 0 <= max_drop_ratio <= 1:
        raise ValueError("max_drop_ratio는 0과 1 사이여야 합니다.")
    if previous_count <= 0:
        raise PoiPublicationError("활성 POI Master의 행 수가 0 이하입니다.")
    drop_ratio = (previous_count - candidate_count) / previous_count
    if drop_ratio > max_drop_ratio:
        raise PoiPublicationError(
            "새 POI Master의 행 수가 이전 정상본보다 지나치게 적습니다: "
            f"previous={previous_count}, candidate={candidate_count}, "
            f"drop_ratio={drop_ratio:.4f}, max={max_drop_ratio:.4f}"
        )


def refresh_poi_master(
    assets: SourceAssets,
    *,
    activated_at: datetime,
    max_drop_ratio: float = 0.2,
) -> RefreshResult:
    """공식 첨부 내용이 바뀐 경우에만 검증하고 새 POI Master를 활성화한다.

    게시 순서는 immutable source raw 두 개, immutable Silver Parquet, immutable source
    manifest, append-only activation pointer 순이다. 앞 단계가 실패하면 마지막
    pointer가 생기지 않아 기존 활성본은 그대로 유지된다.
    """
    if activated_at.tzinfo is None or activated_at.utcoffset() is None:
        raise ValueError("activated_at은 timezone-aware datetime이어야 합니다.")
    list_sha256, areas_sha256 = _source_hashes(assets)
    expected_config_version = _config_version(
        list_sha256,
        areas_sha256,
        assets.list_attachment.declared_place_count,
        assets.areas_attachment.declared_place_count,
    )
    current_ref = resolve_poi_master(activated_at)
    current_table: pa.Table | None = None
    previous_count: int | None = None
    if current_ref.mode == "s3":
        current_snapshot = _read_refresh_snapshot(current_ref)
        assert current_snapshot.table is not None
        current_schema_version = _metadata_text(
            current_snapshot.table,
            "poi_master_schema_version",
        )
        if current_schema_version == POI_MASTER_SCHEMA_VERSION:
            # 현재 version은 source가 바뀌었더라도 이전 행 수를 신뢰하기 전에 consumer와
            # 같은 전체 계약을 통과해야 한다. 손상된 활성본을 migration으로 우회하지 않는다.
            current_table = read_poi_master(current_ref)
            previous_count = current_table.num_rows
            if (
                current_snapshot.manifest.config_version
                == expected_config_version
                and _metadata_text(current_table, "list_sha256") == list_sha256
                and _metadata_text(current_table, "areas_sha256") == areas_sha256
                and _metadata_text(current_table, "list_declared_place_count")
                == str(assets.list_attachment.declared_place_count)
                and _metadata_text(current_table, "areas_declared_place_count")
                == str(assets.areas_attachment.declared_place_count)
            ):
                return RefreshResult(
                    status="unchanged",
                    ref=current_ref,
                    row_count=current_table.num_rows,
                    list_sha256=list_sha256,
                    areas_sha256=areas_sha256,
                )
        else:
            # 알 수 없는 구버전의 행 계약을 그대로 신뢰하지 않는다. 현재 artifact와
            # repository 정상본 중 더 큰 값을 기준으로 삼아 migration이 감소 guard를
            # 느슨하게 만드는 일을 막는다.
            previous_count = _stale_schema_previous_count(current_snapshot.table)

    build = build_registry(assets)
    if (build.list_sha256, build.areas_sha256) != (list_sha256, areas_sha256):
        raise PoiPublicationError("검증 중 POI source bytes의 checksum이 바뀌었습니다.")
    if previous_count is None:
        previous_count = _legacy_poi_count()
    _validate_drop_guard(previous_count, build.table.num_rows, max_drop_ratio)

    table, list_key, areas_key = _raw_metadata_table(build)
    parquet_payload = _parquet_bytes(table)
    parquet_sha256 = hashlib.sha256(parquet_payload).hexdigest()
    silver_key = f"silver/poi_master/sha256={parquet_sha256}.parquet"
    manifest = build_source_snapshot_manifest(
        source_id=POI_MASTER,
        logical_dttm=activated_at,
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=_config_version(
            build.list_sha256,
            build.areas_sha256,
            assets.list_attachment.declared_place_count,
            assets.areas_attachment.declared_place_count,
        ),
        silver_uri=object_uri(silver_key),
        silver_byte_sha256=parquet_sha256,
        counts=SourceSnapshotCounts(
            expected=table.num_rows,
            fetched=table.num_rows,
            kept=table.num_rows,
            repaired=build.repaired_count,
            dropped=0,
        ),
        planned_parts=_PARTS,
        completed_parts=_PARTS,
    )
    manifest_key = source_snapshot_manifest_key(POI_MASTER, activated_at, 0)

    store = S3ImmutableObjectStore()
    _put_once(store, list_key, assets.list_bytes)
    _put_once(store, areas_key, assets.areas_bytes)
    _put_once(store, silver_key, parquet_payload)
    _put_once(
        store,
        manifest_key,
        manifest.canonical_bytes,
        require_canonical_json=True,
    )
    ref = activate_poi_master(
        manifest_uri=object_uri(manifest_key),
        manifest_sha256=manifest.sha256,
        activated_at=activated_at,
    )
    return RefreshResult(
        status="published",
        ref=ref,
        row_count=table.num_rows,
        list_sha256=list_sha256,
        areas_sha256=areas_sha256,
    )
