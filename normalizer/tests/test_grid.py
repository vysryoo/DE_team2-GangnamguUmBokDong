"""grid.py: CELL_ID <-> EPSG:5179 좌표 변환, 격자 폴리곤 생성 테스트."""

import pytest

from grid import GRID_AREA_M2, cell_id_to_epsg5179_sw_corner, cell_id_to_polygon


@pytest.mark.parametrize(
    ("cell_id", "corner"),
    [
        # plan.md에 실측으로 검증된 좌표
        ("다사53815262", (953810.0, 1952620.0)),
        # 격자 좌표계의 원점
        ("가가00000000", (700000.0, 1300000.0)),
    ],
)
def test_sw_corner(cell_id, corner):
    assert cell_id_to_epsg5179_sw_corner(cell_id) == corner


def test_polygon_is_250m_square_at_sw_corner():
    poly = cell_id_to_polygon("다사53815262")
    assert poly.bounds == (953810.0, 1952620.0, 954060.0, 1952870.0)


def test_polygon_area_is_62500_m2():
    poly = cell_id_to_polygon("다사53815262")
    assert poly.area == GRID_AREA_M2 == 62500.0


@pytest.mark.parametrize(
    "cell_id",
    ["다사5381526", "다사5381526X", "ZZ53815262"],
    ids=["short_length", "non_digit", "unknown_letter"],
)
def test_invalid_cell_id_raises_value_error(cell_id):
    with pytest.raises(ValueError):
        cell_id_to_epsg5179_sw_corner(cell_id)
