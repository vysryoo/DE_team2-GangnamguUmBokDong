"""poi.py: POI shapefile 로딩, 위상 오류 복구, 캐싱 테스트."""

import pyarrow as pa
import pytest
import shapely
from core.poi_master import PoiMasterRef
from shapely.geometry import GeometryCollection, LineString, Point, box

import poi

EXPECTED_MISSING_CODES = {
    "POI022", "POI028", "POI057", "POI062", "POI065",
    "POI069", "POI075", "POI097", "POI099", "POI113",
}


def test_load_real_shapefile_reads_121_areas():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    assert len(areas) == 121
    assert len({a.area_cd for a in areas}) == 121


def test_area_codes_have_expected_gaps():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    codes = {a.area_cd for a in areas}
    all_expected = {f"POI{i:03d}" for i in range(1, 132)} - EXPECTED_MISSING_CODES
    assert codes == all_expected


def test_all_geometries_are_valid_polygons_in_epsg5179():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    for area in areas:
        assert area.geometry.geom_type == "Polygon"
        assert area.geometry.is_valid
        assert area.area_m2 > 0
        # 서울 지역 EPSG:5179 좌표는 대략 900,000~980,000 / 1,930,000~1,970,000 범위에 있어야 한다.
        minx, miny, maxx, maxy = area.geometry.bounds
        assert 900_000 < minx < maxx < 990_000
        assert 1_920_000 < miny < maxy < 1_980_000


def test_poi070_topology_error_is_recovered():
    """실측: POI070("쌍문역")은 shapely가 TopologyException을 던질 정도로 자기교차한다."""
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    poi070 = next(a for a in areas if a.area_cd == "POI070")
    assert poi070.area_nm == "쌍문역"
    assert poi070.geometry.geom_type == "Polygon"
    assert poi070.geometry.is_valid
    assert poi070.area_m2 > 0


def test_load_poi_areas_is_cached():
    first = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    second = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    assert first is second  # lru_cache로 동일 튜플 객체가 반환돼야 함


def test_select_largest_polygon_raises_when_no_polygon_remains():
    """make_valid 결과에 Polygon 조각이 하나도 없으면 명확히 실패해야 한다."""
    collection = GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])])
    with pytest.raises(ValueError, match="Polygon 조각이 없음"):
        poi._select_largest_polygon(collection, area_cd="POI999")


def test_select_largest_polygon_picks_biggest_piece_from_multipolygon():
    from shapely.geometry import MultiPolygon, box

    small = box(0, 0, 1, 1)  # area 1
    big = box(10, 10, 13, 13)  # area 9
    multi = MultiPolygon([small, big])
    result = poi._select_largest_polygon(multi, area_cd="POI999")
    assert result.equals(big)


def _master_table(rows: list[dict]) -> pa.Table:
    """S3 POI Master의 exact schema로 테스트 table을 만든다."""
    schema = pa.schema(
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
    return pa.Table.from_pylist(rows, schema=schema)


def _master_row(area_cd: str, geometry=None) -> dict:
    """유효한 EPSG:5179 POI Master 행 하나를 만든다."""
    selected = geometry or box(950_000, 1_940_000, 950_100, 1_940_100)
    return {
        "AREA_CD": area_cd,
        "AREA_NM": f"장소 {area_cd}",
        "CATEGORY": "공원",
        "ENG_NM": f"Place {area_cd}",
        "SOURCE_NO": int(area_cd[-3:]),
        "GEOMETRY_WKB": shapely.to_wkb(selected),
        "AREA_M2": selected.area,
    }


def test_load_poi_master_areas_decodes_one_exact_ref_once(monkeypatch):
    """Normalizer가 중앙 ref의 WKB를 읽고 cache하며 latest를 별도 조회하지 않는다."""
    ref = PoiMasterRef(
        mode="s3",
        manifest_uri=(
            "s3://test/source_snapshot_manifest/poi_master/dt=2026-08-25/hh=01/"
            "logical=20260825T010000000000Z/revision=0000000000.json"
        ),
        manifest_sha256="a" * 64,
    )
    calls = []
    table = _master_table([_master_row("POI001"), _master_row("POI132")])

    def fake_read(selected_ref):
        """고정 ref 호출을 기록하고 fixture table을 반환한다."""
        calls.append(selected_ref)
        return table

    poi.load_poi_master_areas.cache_clear()
    monkeypatch.setattr(poi, "read_poi_master", fake_read)

    first = poi.load_poi_master_areas(ref)
    second = poi.load_poi_master_areas(ref)

    assert calls == [ref]
    assert first is second
    assert [area.area_cd for area in first] == ["POI001", "POI132"]
    assert all(area.geometry.is_valid and area.area_m2 > 0 for area in first)
