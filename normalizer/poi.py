"""고정된 S3 POI Master 또는 bootstrap Shapefile의 EPSG:5179 영역을 로드한다."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# pyrefly: ignore [missing-import]
import shapefile
import shapely

# pyrefly: ignore [missing-import]
from core.poi_master import PoiMasterRef, read_poi_master

# pyrefly: ignore [missing-import]
from pyproj import Transformer
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform

DEFAULT_POI_SHP_PATH = str(Path(__file__).parent / "data" / "poi_areas" / "seoul_121_poi_areas.shp")

_TO_EPSG5179 = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)


@dataclass(frozen=True)
class PoiArea:
    """EPSG:5179로 변환 및 위상 복구가 완료된 POI 영역 지오메트리 정보."""

    area_cd: str
    area_nm: str
    geometry: BaseGeometry
    area_m2: float


def _select_largest_polygon(geom: BaseGeometry, area_cd: str) -> BaseGeometry:
    """make_valid가 남긴 미세 파편을 버리고 면적이 가장 큰 Polygon만 남긴다."""
    if geom.geom_type == "Polygon":
        return geom
    polygons = [g for g in getattr(geom, "geoms", ()) if g.geom_type == "Polygon"]
    if not polygons:
        raise ValueError(
            f"{area_cd}: make_valid 이후 Polygon 조각이 없음(geom_type={geom.geom_type})"
        )
    return max(polygons, key=lambda p: p.area)


@lru_cache(maxsize=4)
def load_poi_areas(shp_path: str) -> tuple[PoiArea, ...]:
    """POI Shapefile을 읽어 위상 오류를 복구하고 EPSG:5179로 변환한다.

    Args:
        shp_path: Shapefile(.shp) 경로
    Returns:
        PoiArea 튜플. 캐시되므로 호출자가 바꿀 수 없게 튜플로 반환함
    Raises:
        ValueError: 레코드가 없거나 복구 후 Polygon이 없을 때
    """
    areas: list[PoiArea] = []
    for shape_record in shapefile.Reader(shp_path).iterShapeRecords():
        record = shape_record.record.as_dict()
        geometry = shape(shape_record.shape.__geo_interface__)
        if not geometry.is_valid:
            geometry = _select_largest_polygon(shapely.make_valid(geometry), record["AREA_CD"])
        geometry = shapely_transform(_TO_EPSG5179.transform, geometry)
        areas.append(
            PoiArea(
                area_cd=record["AREA_CD"],
                area_nm=record["AREA_NM"],
                geometry=geometry,
                area_m2=geometry.area,
            )
        )
    if not areas:
        raise ValueError(f"POI shapefile에 레코드가 없음: {shp_path}")
    return tuple(areas)


@lru_cache(maxsize=8)
def load_poi_master_areas(ref: PoiMasterRef) -> tuple[PoiArea, ...]:
    """중앙에서 고정한 ref의 POI 영역을 읽어 `PoiArea` 튜플로 변환한다.

    `static`은 로컬 Shapefile로 최초 배포와 명시적 롤백을 지원한다. `s3`은 ref가
    가리키는 exact manifest만 읽으며, schema·코드·면적 검증은 `read_poi_master`가 맡는다.

    Args:
        ref: Airflow가 고정한 POI Master ref
    Returns:
        AREA_CD 오름차순 PoiArea 튜플
    Raises:
        PoiMasterError: S3 POI Master를 읽거나 검증할 수 없을 때
    """
    if ref.mode == "static":
        return load_poi_areas(DEFAULT_POI_SHP_PATH)
    return tuple(
        PoiArea(
            area_cd=row["AREA_CD"],
            area_nm=row["AREA_NM"],
            geometry=shapely.from_wkb(row["GEOMETRY_WKB"]),
            area_m2=row["AREA_M2"],
        )
        for row in read_poi_master(ref).to_pylist()
    )
