"""검증된 POI Master를 content-addressed S3 객체와 authority manifest로 게시한다."""

from __future__ import annotations

import hashlib
import io
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
from core.sources import POI_MASTER

from registry import RegistryBuild, build_registry
from source import SourceAssets

_PARTS = ("areas_zip", "list_xlsx")
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


def _config_version(build: RegistryBuild, assets: SourceAssets) -> str:
    """현재 변환 계약과 source checksum을 결합한 manifest config version을 만든다."""
    digest = hashlib.sha256(
        (
            f"{POI_MASTER_SCHEMA_VERSION}\n{build.list_sha256}\n{build.areas_sha256}\n"
            f"{assets.list_attachment.declared_place_count}\n"
            f"{assets.areas_attachment.declared_place_count}"
        ).encode()
    ).hexdigest()
    return f"sha256:{digest}"


def _source_fingerprint(assets: SourceAssets) -> dict[bytes, bytes]:
    """활성본과 비교할 원천 checksum·선언 장소 수를 Table metadata 형태로 만든다."""
    return {
        b"list_sha256": hashlib.sha256(assets.list_bytes).hexdigest().encode(),
        b"areas_sha256": hashlib.sha256(assets.areas_bytes).hexdigest().encode(),
        b"list_declared_place_count": str(assets.list_attachment.declared_place_count).encode(),
        b"areas_declared_place_count": str(assets.areas_attachment.declared_place_count).encode(),
    }


def _legacy_poi_count() -> int:
    """최초 활성화의 감소 기준이 되는 repository Shapefile 행 수를 반환한다."""
    try:
        with shapefile.Reader(str(_LEGACY_POI_SHP_PATH)) as reader:
            return len(reader)
    except (OSError, shapefile.ShapefileException) as exc:
        raise PoiPublicationError(
            f"기존 POI Shapefile 행 수를 읽을 수 없습니다: {_LEGACY_POI_SHP_PATH}"
        ) from exc


def _validate_drop_guard(
    previous_count: int, candidate_count: int, max_drop_ratio: float
) -> None:
    """이전 정상본 대비 지나치게 큰 행 감소를 활성화 전에 차단한다."""
    if not 0 <= max_drop_ratio <= 1:
        raise ValueError("max_drop_ratio는 0과 1 사이여야 합니다.")
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

    Args:
        assets: 공식 페이지에서 내려받은 목록·영역 첨부
        activated_at: 새 활성본의 logical 시각(timezone-aware)
        max_drop_ratio: 이전 정상본 대비 허용하는 최대 행 감소 비율
    Returns:
        `unchanged` 또는 `published` 상태와 선택된 exact ref
    Raises:
        PoiMasterError: 현재 활성본을 읽거나 검증할 수 없을 때
        PoiRegistryError: 원천 첨부가 registry 계약을 어길 때
        PoiPublicationError: 행 감소 guard를 넘거나 불변 객체 게시에 실패했을 때
    """
    fingerprint = _source_fingerprint(assets)
    current_ref = resolve_poi_master(activated_at)
    if current_ref.mode == "s3":
        # read_poi_master가 현재 schema 계약 전체를 검증하므로 손상된 활성본을 기준으로 삼지 않음
        current_table = read_poi_master(current_ref)
        current_metadata = current_table.schema.metadata or {}
        if all(current_metadata.get(key) == value for key, value in fingerprint.items()):
            return RefreshResult(
                status="unchanged",
                ref=current_ref,
                row_count=current_table.num_rows,
                list_sha256=fingerprint[b"list_sha256"].decode(),
                areas_sha256=fingerprint[b"areas_sha256"].decode(),
            )
        previous_count = current_table.num_rows
    else:
        previous_count = _legacy_poi_count()

    build = build_registry(assets)
    _validate_drop_guard(previous_count, build.table.num_rows, max_drop_ratio)
    return RefreshResult(
        status="published",
        ref=_publish(build, assets, activated_at),
        row_count=build.table.num_rows,
        list_sha256=build.list_sha256,
        areas_sha256=build.areas_sha256,
    )


def _publish(build: RegistryBuild, assets: SourceAssets, activated_at: datetime) -> PoiMasterRef:
    """원천 raw, Silver Parquet, source manifest를 불변 객체로 쓴 뒤 마지막에 활성화한다."""
    table, list_key, areas_key = _raw_metadata_table(build)
    parquet_payload = _parquet_bytes(table)
    parquet_sha256 = hashlib.sha256(parquet_payload).hexdigest()
    silver_key = f"silver/poi_master/sha256={parquet_sha256}.parquet"
    manifest = build_source_snapshot_manifest(
        source_id=POI_MASTER,
        logical_dttm=activated_at,
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=_config_version(build, assets),
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
    return activate_poi_master(
        manifest_uri=object_uri(manifest_key),
        manifest_sha256=manifest.sha256,
        activated_at=activated_at,
    )
