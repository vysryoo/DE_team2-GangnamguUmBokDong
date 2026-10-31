"""공간 교차(STRtree)를 통해 격자 인구와 실시간 POI 인구를 합성하고 연령·성별을 재분배한다."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from grid import GRID_AREA_M2
from shapely import STRtree
from shapely.geometry.base import BaseGeometry

MALE_AGE_COLUMNS = (
    "M00", "M10", "M15", "M20", "M25", "M30", "M35",
    "M40", "M45", "M50", "M55", "M60", "M65", "M70",
)
FEMALE_AGE_COLUMNS = (
    "F00", "F10", "F15", "F20", "F25", "F30", "F35",
    "F40", "F45", "F50", "F55", "F60", "F65", "F70",
)
AGE_COLUMNS = MALE_AGE_COLUMNS + FEMALE_AGE_COLUMNS


@dataclass(frozen=True)
class GridCell:
    """생활인구 격자 데이터의 단일 셀 정보."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]
    geometry: BaseGeometry


@dataclass(frozen=True)
class PoiSnapshot:
    """실시간 POI 인구 관측치와 지오메트리가 결합된 스냅샷."""

    area_cd: str
    male_rate: float
    female_rate: float
    pop_estimate: float
    geometry: BaseGeometry
    area_m2: float


@dataclass(frozen=True)
class MergedCell:
    """POI 인구 합성이 완료된 격자 셀 정보(반올림 전 소수점 유지)."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]


class OverlappingGeometry(Protocol):
    """교차 계산에 필요한 최소 계약 — POI 코드와 지오메트리."""

    area_cd: str
    geometry: BaseGeometry


def find_overlap_areas(
    cells: list[GridCell], pois: list[OverlappingGeometry]
) -> dict[str, list[tuple[str, float]]]:
    """STRtree 공간 인덱스로 격자와 POI의 교차 면적을 계산한다(POI 코드 기준).

    지오메트리만 보므로 결과가 시각과 무관해 여러 보정 시각에 재사용할 수 있다.

    args:
        cells: 대상 GridCell 목록(여러 시각의 셀 합집합을 넣어도 된다)
        pois: `area_cd`와 `geometry`를 가진 POI 목록
    returns:
        CELL_ID를 키로 하고 (POI 코드, 교차면적_m2) 튜플 목록을 값으로 갖는 딕셔너리
    """
    if not pois:
        return {}

    tree = STRtree([p.geometry for p in pois])
    overlaps: dict[str, list[tuple[str, float]]] = {}

    for cell in cells:
        candidate_idx = tree.query(cell.geometry, predicate="intersects")
        pairs: list[tuple[str, float]] = []
        for i in candidate_idx:
            candidate = pois[i]
            intersection_area = cell.geometry.intersection(candidate.geometry).area
            if intersection_area > 0:
                pairs.append((candidate.area_cd, intersection_area))
        if pairs:
            overlaps[cell.cell_id] = pairs

    return overlaps


def bind_snapshots(
    overlap_areas: list[tuple[str, float]], snapshots_by_code: dict[str, PoiSnapshot]
) -> list[tuple[PoiSnapshot, float]]:
    """교차 면적 목록에 해당 시각의 PoiSnapshot을 붙인다.

    그 시각의 관측·예측값이 없는 POI는 제외된다 — 예측을 제공하지 않는 지점
    (`FCST_YN='N'`)이 겹친 셀은 baseline 값이 그대로 남는다.

    args:
        overlap_areas: (POI 코드, 교차면적_m2) 목록
        snapshots_by_code: 그 시각의 POI 코드별 스냅샷
    returns:
        (PoiSnapshot, 교차면적_m2) 목록
    """
    return [
        (snapshots_by_code[area_cd], area)
        for area_cd, area in overlap_areas
        if area_cd in snapshots_by_code
    ]


def _update_density(current_spop: float, poi: PoiSnapshot, intersection_area: float) -> float:
    """겹친 면적 비율만큼 POI 밀도로, 나머지는 기존 밀도로 가중평균한 격자 인구를 계산한다."""
    overlap_ratio = intersection_area / GRID_AREA_M2
    density = (1 - overlap_ratio) * (current_spop / GRID_AREA_M2) + overlap_ratio * (
        poi.pop_estimate / poi.area_m2
    )
    return density * GRID_AREA_M2


def _scale_group(
    original_ages: dict[str, float], columns: tuple[str, ...], new_total: float
) -> dict[str, float]:
    """한 성별의 연령 비중을 유지하며 새 총인구에 맞춘다. 기존 인구가 0이면 균등 배분함."""
    original_total = sum(original_ages[c] for c in columns)
    if original_total > 0:
        return {c: new_total * (original_ages[c] / original_total) for c in columns}
    return dict.fromkeys(columns, new_total / len(columns))


def _redistribute_ages(
    original_ages: dict[str, float], spop_new: float, male_rate: float, female_rate: float
) -> dict[str, float]:
    """합성된 생활인구 총합과 POI 성비(%)로 성별 총인구를 정하고 연령대에 재분배한다."""
    return {
        **_scale_group(original_ages, MALE_AGE_COLUMNS, spop_new * male_rate / 100.0),
        **_scale_group(original_ages, FEMALE_AGE_COLUMNS, spop_new * female_rate / 100.0),
    }


def _scale_ages(original_ages: dict[str, float], spop_new: float) -> dict[str, float]:
    """성·연령 구성비를 그대로 두고 총량만 `spop_new`에 맞춰 비례 조정한다.

    미래 시각 보정에 쓴다. `FCST_PPLTN`은 인구 수만 주고 성비를 주지 않으므로, 그 시각의
    성·연령 구조는 baseline(nowcaster 추정치)이 가진 것을 신뢰한다 — 관측 시점의 성비를
    12시간 뒤에 씌우면 있는 정보를 오히려 버린다.

    args:
        original_ages: baseline 격자의 연령·성별 인구
        spop_new: 합성된 새 생활인구 총합
    returns:
        총합이 `spop_new`가 되도록 조정된 연령·성별 인구
    """
    total_orig = sum(original_ages[c] for c in AGE_COLUMNS)
    # baseline이 0인 셀은 비율을 알 수 없다. `_redistribute_ages()`와 같은 규칙으로
    # 균등 배분해 SPOP == sum(ages) 불변식을 지킨다.
    if total_orig <= 0:
        even_share = spop_new / len(AGE_COLUMNS)
        return {c: even_share for c in AGE_COLUMNS}
    ratio = spop_new / total_orig
    return {c: original_ages[c] * ratio for c in AGE_COLUMNS}


def _ordered_by_area(
    overlapping: list[tuple[PoiSnapshot, float]]
) -> list[tuple[PoiSnapshot, float]]:
    """광역 POI를 먼저 반영하고 국소 POI가 최종 덮어쓰도록 면적 내림차순(동률 시 코드순) 정렬한다."""
    return sorted(overlapping, key=lambda pair: (-pair[0].area_m2, pair[0].area_cd))


def _composite_spop(cell: GridCell, ordered: list[tuple[PoiSnapshot, float]]) -> float:
    """큰 POI부터 작은 POI 순으로 밀도를 누적 갱신한 생활인구 총합을 반환한다."""
    spop = cell.spop
    for poi, intersection_area in ordered:
        spop = _update_density(spop, poi, intersection_area)
    return spop


def merge_cell_total_only(
    cell: GridCell, overlapping: list[tuple[PoiSnapshot, float]]
) -> MergedCell:
    """총량만 POI 예측 인구로 합성하고 성·연령 구성비는 baseline 그대로 유지한다.

    미래 시각(`FCST_n_*`)용이다. `merge_cell()`은 POI의 실측 성비로 28개 컬럼을
    재분배하지만, 예측값에는 성비가 없어 그 경로를 쓸 수 없다.

    args:
        cell: 대상 GridCell 객체(baseline = nowcaster 추정치)
        overlapping: (PoiSnapshot, 교차면적_m2) 목록. `pop_estimate`가 예측 인구다
    returns:
        합성된 MergedCell 객체
    """
    if not overlapping:
        return MergedCell(
            cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=cell.spop, ages=dict(cell.ages)
        )

    spop = _composite_spop(cell, _ordered_by_area(overlapping))
    return MergedCell(
        cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=spop, ages=_scale_ages(cell.ages, spop)
    )


def merge_cell(cell: GridCell, overlapping: list[tuple[PoiSnapshot, float]]) -> MergedCell:
    """단일 격자에 겹치는 POI들을 면적 내림차순으로 적용해 인구와 성·연령 분포를 합성한다.

    광역 POI의 배경 밀도를 먼저 반영하고 국소 POI가 최종 값을 덮어쓰도록 큰 POI부터 적용한다.

    args:
        cell: 대상 GridCell 객체
        overlapping: (PoiSnapshot, 교차면적_m2) 목록
    returns:
        합성된 MergedCell 객체
    """
    if not overlapping:
        return MergedCell(
            cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=cell.spop, ages=dict(cell.ages)
        )

    ordered = _ordered_by_area(overlapping)
    spop = _composite_spop(cell, ordered)

    # 가장 국소적인 마지막 POI의 성비가 그 격자의 실제 구성에 가장 가까움
    last_poi = ordered[-1][0]
    ages = _redistribute_ages(cell.ages, spop, last_poi.male_rate, last_poi.female_rate)

    return MergedCell(cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=spop, ages=ages)


def round_output_row(merged: MergedCell) -> dict[str, int | str]:
    """합성된 실수형 인구 데이터를 정수형(int64) 출력 레코드 형태로 반올림한다."""
    row: dict[str, int | str] = {
        "CELL_ID": merged.cell_id,
        "H_DNG_CD": merged.h_dng_cd,
        "SPOP": round(merged.spop),
    }
    for c in AGE_COLUMNS:
        row[c] = round(merged.ages[c])
    return row
