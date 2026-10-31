"""CELL_ID를 EPSG:5179 좌표로 변환하고 250m 격자 폴리곤을 생성한다."""

from __future__ import annotations

from shapely.geometry import Polygon, box

GRID_SIZE_M = 250.0
GRID_AREA_M2 = GRID_SIZE_M * GRID_SIZE_M  # 62500.0

_GRID_LETTERS = "가나다라마바사아자차카타파하"


def cell_id_to_epsg5179_sw_corner(cell_id: str) -> tuple[float, float]:
    """CELL_ID를 EPSG:5179 남서쪽 꼭짓점 좌표로 변환한다.

    국가지점번호 체계는 격자의 좌하단(남서쪽, X·Y 최소점)을 기준점으로 코드를 부여함.

    args:
        cell_id: 한글 2자 + 숫자 8자리 국가지점번호 형식 (예: "다사53815262")
    returns:
        (x, y) EPSG:5179 남서쪽 꼭짓점 좌표
    raises:
        ValueError: CELL_ID 형식 또는 문자가 유효하지 않을 때
    """
    if len(cell_id) != 10:
        raise ValueError(f"CELL_ID 형식이 아님(길이 10): {cell_id!r}")

    ew_letter, ns_letter = cell_id[0], cell_id[1]
    digits = cell_id[2:]
    if not digits.isdigit():
        raise ValueError(f"CELL_ID 형식이 아님(숫자 8자리): {cell_id!r}")

    try:
        ew_idx = _GRID_LETTERS.index(ew_letter)
        ns_idx = _GRID_LETTERS.index(ns_letter)
    except ValueError as exc:
        raise ValueError(f"CELL_ID의 한글 문자가 격자 문자표에 없음: {cell_id!r}") from exc

    ew_num = int(digits[:4])
    ns_num = int(digits[4:])

    x = 700_000 + ew_idx * 100_000 + ew_num * 10
    y = 1_300_000 + ns_idx * 100_000 + ns_num * 10
    return (float(x), float(y))


def cell_id_to_polygon(cell_id: str) -> Polygon:
    """CELL_ID에 대응하는 250m x 250m 정사각 격자 폴리곤(EPSG:5179)을 생성한다."""
    x, y = cell_id_to_epsg5179_sw_corner(cell_id)
    return box(x, y, x + GRID_SIZE_M, y + GRID_SIZE_M)
