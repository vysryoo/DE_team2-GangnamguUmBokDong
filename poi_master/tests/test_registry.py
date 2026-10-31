"""공식 XLSX·영역 ZIP의 교차 검증과 통합 schema를 검증한다."""

import importlib.util
import io
import sys
import zipfile
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
import shapely
from core import poi_master as core_poi_master
from core.poi_master import POI_MASTER_SCHEMA_VERSION
from shapely.geometry import box

from registry import (
    POI_MASTER_SCHEMA,
    PoiRegistryError,
    _safe_shape_members,
    _validated_geometry,
    build_registry,
)
from tests.conftest import real_source_assets


def test_schema_matches_core_consumer_contract() -> None:
    """게시 schema와 core가 읽을 때 강제하는 schema가 따로 정의돼 있어 어긋나지 않게 고정한다."""
    assert POI_MASTER_SCHEMA.equals(core_poi_master._POI_MASTER_SCHEMA, check_metadata=True)


def test_real_frozen_assets_build_same_121_code_registry() -> None:
    """현재 동결본이 기존 호출 대상과 같은 121개 코드의 유효 영역을 만든다."""
    result = build_registry(real_source_assets())

    assert result.table.schema.remove_metadata() == POI_MASTER_SCHEMA
    assert result.table.num_rows == 121
    codes = result.table.column("AREA_CD").to_pylist()
    assert codes == sorted(codes)
    assert len(set(codes)) == 121
    assert codes[0] == "POI001"
    assert codes[-1] == "POI131"
    assert result.table.schema.metadata[b"geometry_crs"] == b"EPSG:5179"
    assert (
        result.table.schema.metadata[b"poi_master_schema_version"]
        == POI_MASTER_SCHEMA_VERSION.encode("utf-8")
    )


def test_custom_page_url_is_preserved_in_registry_provenance() -> None:
    """운영자가 지정한 원천 페이지 URL을 게시 Table metadata에 보존한다."""
    page_url = "https://example.test/custom-poi-dataset"
    assets = replace(real_source_assets(), page_url=page_url)

    result = build_registry(assets)

    assert result.table.schema.metadata[b"source_page_url"] == page_url.encode()


@pytest.mark.parametrize(
    ("list_count", "areas_count"),
    [(125, 125), (121, 125)],
)
def test_registry_rejects_declared_count_different_from_actual_rows(
    list_count: int,
    areas_count: int,
) -> None:
    """파일명 선언 수와 XLSX·Shape·최종 Master 행 수가 모두 같아야 한다."""
    source = real_source_assets()
    assets = replace(
        source,
        list_attachment=replace(
            source.list_attachment,
            filename=f"서울시 주요 {list_count}장소 목록.xlsx",
            declared_place_count=list_count,
        ),
        areas_attachment=replace(
            source.areas_attachment,
            filename=f"서울시 주요 {areas_count}장소 영역.zip",
            declared_place_count=areas_count,
        ),
    )

    with pytest.raises(PoiRegistryError, match="실제 장소 수가 일치하지 않습니다"):
        build_registry(assets)


def test_real_geometries_are_valid_positive_epsg5179_polygons() -> None:
    """게시 geometry가 Normalizer의 기존 좌표·위상 계약을 지킨다."""
    table = build_registry(real_source_assets()).table

    for row in table.select(["AREA_CD", "GEOMETRY_WKB", "AREA_M2"]).to_pylist():
        geometry = shapely.from_wkb(row["GEOMETRY_WKB"])
        assert geometry.geom_type == "Polygon", row["AREA_CD"]
        assert geometry.is_valid, row["AREA_CD"]
        assert geometry.area == pytest.approx(row["AREA_M2"])
        minx, miny, maxx, maxy = geometry.bounds
        assert 900_000 < minx < maxx < 990_000
        assert 1_920_000 < miny < maxy < 1_980_000


@pytest.mark.parametrize(
    "geometry",
    [
        box(129.0, 35.0, 129.01, 35.01),
        box(0.0, 0.0, 0.01, 0.01),
    ],
)
def test_registry_rejects_valid_polygon_outside_seoul(geometry) -> None:
    """형식과 위상이 정상이더라도 서울 밖 geometry는 자동 게시 입력으로 받지 않는다."""
    with pytest.raises(PoiRegistryError, match="서울 안전 범위"):
        _validated_geometry(geometry, "POI001")


def test_generated_registry_is_geometry_identical_to_legacy_normalizer() -> None:
    """현재 121장소에서 새 S3 WKB가 기존 Shapefile 변환 결과와 정확히 같다."""
    root = Path(__file__).resolve().parents[2]
    module_path = root / "normalizer" / "poi.py"
    spec = importlib.util.spec_from_file_location("legacy_normalizer_poi", module_path)
    assert spec is not None and spec.loader is not None
    legacy_poi = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = legacy_poi
    try:
        spec.loader.exec_module(legacy_poi)
        legacy = {
            area.area_cd: area
            for area in legacy_poi.load_poi_areas(legacy_poi.DEFAULT_POI_SHP_PATH)
        }
    finally:
        sys.modules.pop(spec.name, None)

    rows = build_registry(real_source_assets()).table.to_pylist()

    assert {row["AREA_CD"] for row in rows} == set(legacy)
    for row in rows:
        area = legacy[row["AREA_CD"]]
        generated = shapely.from_wkb(row["GEOMETRY_WKB"])
        assert generated.equals_exact(area.geometry, tolerance=0)
        assert row["AREA_NM"] == area.area_nm
        assert row["AREA_M2"] == area.area_m2


@pytest.mark.parametrize("filename", ["../escape.shp", "/absolute.shp", "C:/evil.shp"])
def test_zip_rejects_path_traversal_and_absolute_members(filename: str) -> None:
    """ZIP member를 디스크에 풀지 않더라도 위험한 경로 자체를 거부한다."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(filename, b"x")

    with pytest.raises(PoiRegistryError, match="안전하지 않은 경로"):
        _safe_shape_members(buffer.getvalue())


def test_registry_schema_uses_binary_wkb_and_metric_area() -> None:
    """소비자가 의존하는 exact 물리 타입을 고정한다."""
    schema = build_registry(real_source_assets()).table.schema.remove_metadata()

    assert schema.field("GEOMETRY_WKB").type == pa.binary()
    assert schema.field("AREA_M2").type == pa.float64()
    assert schema.field("SOURCE_NO").type == pa.int64()
