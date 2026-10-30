"""merge.py: 공간 조인, 밀도 순차 갱신, 연령/성별 재분배 테스트.

수치 예시는 spec 3.5/plan.md 4절과 동일한 시나리오("격자 A의 30% 영역에 POI가
교차", "SPOP 100->130, 성비 55/45")를 정확히 재현하도록 역산해서 만들었다:

    D_A = 100/62500 = 0.0016
    POI 면적 62500 m^2, 인구추정치 200 -> D_B = 200/62500 = 0.0032
    교차면적 18750 m^2(그리드의 30%) -> w=0.3
    D_new = 0.7*0.0016 + 0.3*0.0032 = 0.00208 -> SPOP_new = 0.00208*62500 = 130.0 (정확히)
"""

import pytest
from merge import (
    AGE_COLUMNS,
    FEMALE_AGE_COLUMNS,
    MALE_AGE_COLUMNS,
    GridCell,
    PoiSnapshot,
    find_overlap_areas,
    merge_cell,
    merge_cell_total_only,
    round_output_row,
)
from shapely.geometry import box


def _zero_ages() -> dict[str, float]:
    return {c: 0.0 for c in MALE_AGE_COLUMNS + FEMALE_AGE_COLUMNS}


def _grid_cell(spop: float, ages: dict[str, float] | None = None) -> GridCell:
    return GridCell(
        cell_id="다사53815262",
        h_dng_cd="1100053",
        spop=spop,
        ages=ages or _zero_ages(),
        geometry=box(953810.0, 1952620.0, 954060.0, 1952870.0),
    )


class TestSinglePoiDensityUpdate:
    def test_matches_worked_example_100_to_130(self):
        cell = _grid_cell(spop=100.0)
        poi = PoiSnapshot(
            area_cd="POI001",
            male_rate=55.0,
            female_rate=45.0,
            pop_estimate=200.0,
            geometry=box(0, 0, 250, 250),  # area_m2=62500 (grid와 동일 크기로 단순화)
            area_m2=62500.0,
        )
        intersection_area = 18750.0  # 그리드 면적의 30%

        result = merge_cell(cell, [(poi, intersection_area)])

        assert result.spop == pytest.approx(130.0)

    def test_no_overlap_passes_through_unchanged(self):
        ages = _zero_ages()
        ages["M00"] = 40.0
        cell = _grid_cell(spop=100.0, ages=ages)

        result = merge_cell(cell, [])

        assert result.spop == 100.0
        assert result.ages == ages
        assert result.ages is not ages  # 원본과 별개 dict여야 함(방어적 복사)


class TestAgeGenderRedistribution:
    def test_spop_100_to_130_with_male_55_female_45(self):
        ages = _zero_ages()
        ages["M00"], ages["M10"] = 40.0, 10.0
        ages["F00"], ages["F10"] = 30.0, 20.0
        cell = _grid_cell(spop=100.0, ages=ages)
        poi = PoiSnapshot(
            area_cd="POI001", male_rate=55.0, female_rate=45.0,
            pop_estimate=200.0, geometry=box(0, 0, 250, 250), area_m2=62500.0,
        )

        result = merge_cell(cell, [(poi, 18750.0)])
        row = round_output_row(result)

        assert row["SPOP"] == 130
        assert row["M00"] == 57  # 71.5 * (40/50) = 57.2 -> round 57
        assert row["M10"] == 14  # 71.5 * (10/50) = 14.3 -> round 14
        assert row["F00"] == 35  # 58.5 * (30/50) = 35.1 -> round 35
        assert row["F10"] == 23  # 58.5 * (20/50) = 23.4 -> round 23
        # 반올림 오차(spec 3.5에서 허용): 129 vs SPOP 130, 오차 1
        age_sum = sum(row[c] for c in MALE_AGE_COLUMNS + FEMALE_AGE_COLUMNS)
        assert abs(age_sum - row["SPOP"]) <= 2

    def test_masked_null_ages_treated_as_zero(self):
        """living_population_grid에서 마스킹(*)으로 null 처리된 연령대는 0으로 취급한다."""
        ages = _zero_ages()
        ages["M00"] = 100.0  # 다른 모든 연령대는 null -> 호출자가 0.0으로 치환해서 넘김
        cell = _grid_cell(spop=100.0, ages=ages)
        poi = PoiSnapshot(
            area_cd="POI001", male_rate=100.0, female_rate=0.0,
            pop_estimate=200.0, geometry=box(0, 0, 250, 250), area_m2=62500.0,
        )

        result = merge_cell(cell, [(poi, 18750.0)])

        assert result.ages["M00"] == pytest.approx(130.0)
        assert all(result.ages[c] == 0.0 for c in MALE_AGE_COLUMNS if c != "M00")
        assert all(result.ages[c] == 0.0 for c in FEMALE_AGE_COLUMNS)

    def test_zero_original_gender_total_splits_evenly(self):
        """원본 남성(또는 여성) 연령대 합이 0인데 새 성비가 그 성별에 인구를 배정하면
        14개 연령대에 균등 분배한다(spec에 없는 0-division 예외 상황에 대한 이 계획의 방어적 설계)."""
        ages = _zero_ages()
        ages["F00"] = 100.0  # 남성 연령대 전부 0
        cell = _grid_cell(spop=100.0, ages=ages)
        poi = PoiSnapshot(
            area_cd="POI001", male_rate=50.0, female_rate=50.0,
            pop_estimate=200.0, geometry=box(0, 0, 250, 250), area_m2=62500.0,
        )

        result = merge_cell(cell, [(poi, 18750.0)])

        male_total_new = 130.0 * 0.5
        expected_each = male_total_new / len(MALE_AGE_COLUMNS)
        for c in MALE_AGE_COLUMNS:
            assert result.ages[c] == pytest.approx(expected_each)


class TestMultiPoiSequentialUpdate:
    def test_processes_area_descending_regardless_of_input_order(self):
        """큰 POI(면적 100000, 인구추정 1000) 먼저 -> 작은 POI(면적 10000, 인구추정 50) 나중.
        입력 리스트 순서를 뒤섞어도 결과가 같아야 한다(내부에서 면적 기준 정렬하므로).

            1차(큰 POI, 전체 겹침 w=1.0): D_new = D_poi_large = 1000/100000 = 0.01
                -> spop = 0.01*62500 = 625.0
            2차(작은 POI, 절반 겹침 w=0.5): D_new = 0.5*0.01 + 0.5*(50/10000)
                = 0.005+0.0025 = 0.0075 -> spop = 0.0075*62500 = 468.75
        """
        cell = _grid_cell(spop=100.0)
        big = PoiSnapshot(
            area_cd="POI_BIG", male_rate=50.0, female_rate=50.0,
            pop_estimate=1000.0, geometry=box(0, 0, 316, 316), area_m2=100000.0,
        )
        small = PoiSnapshot(
            area_cd="POI_SMALL", male_rate=10.0, female_rate=90.0,
            pop_estimate=50.0, geometry=box(0, 0, 100, 100), area_m2=10000.0,
        )

        result_order_1 = merge_cell(cell, [(big, 62500.0), (small, 31250.0)])
        result_order_2 = merge_cell(cell, [(small, 31250.0), (big, 62500.0)])

        assert result_order_1.spop == pytest.approx(468.75)
        assert result_order_2.spop == pytest.approx(468.75)

    def test_last_applied_poi_is_the_smaller_area_one(self):
        """겹치는 POI가 여럿이면 성비는 면적이 가장 작은(마지막 적용) POI 값을 쓴다."""
        ages = _zero_ages()
        ages["M00"] = 100.0
        cell = _grid_cell(spop=100.0, ages=ages)
        big = PoiSnapshot(
            area_cd="POI_BIG", male_rate=0.0, female_rate=100.0,
            pop_estimate=1000.0, geometry=box(0, 0, 316, 316), area_m2=100000.0,
        )
        small = PoiSnapshot(
            area_cd="POI_SMALL", male_rate=100.0, female_rate=0.0,
            pop_estimate=50.0, geometry=box(0, 0, 100, 100), area_m2=10000.0,
        )

        result = merge_cell(cell, [(big, 62500.0), (small, 31250.0)])

        # 마지막(small)의 male_rate=100이 적용됐다면 여성 합계는 0이어야 한다.
        assert all(result.ages[c] == 0.0 for c in FEMALE_AGE_COLUMNS)
        assert sum(result.ages[c] for c in MALE_AGE_COLUMNS) == pytest.approx(result.spop)

    def test_tie_break_by_area_cd_when_areas_equal(self):
        """면적이 정확히 같으면 AREA_CD 사전순으로 마지막 적용 POI를 결정한다(spec 3.4)."""
        ages = _zero_ages()
        ages["M00"] = 100.0
        cell = _grid_cell(spop=100.0, ages=ages)
        poi_a = PoiSnapshot(
            area_cd="POI_AAA", male_rate=0.0, female_rate=100.0,
            pop_estimate=100.0, geometry=box(0, 0, 100, 100), area_m2=10000.0,
        )
        poi_z = PoiSnapshot(
            area_cd="POI_ZZZ", male_rate=100.0, female_rate=0.0,
            pop_estimate=100.0, geometry=box(0, 0, 100, 100), area_m2=10000.0,
        )

        result = merge_cell(cell, [(poi_z, 10000.0), (poi_a, 10000.0)])

        # AREA_CD 사전순으로 정렬하면 POI_AAA가 먼저, POI_ZZZ가 마지막(덮어씀) -> male_rate=100 적용
        assert all(result.ages[c] == 0.0 for c in FEMALE_AGE_COLUMNS)


class TestOverlapAreaCalculation:
    def test_finds_intersecting_pairs_with_correct_area(self):
        cell = GridCell(
            cell_id="C1", h_dng_cd="H1", spop=100.0, ages=_zero_ages(),
            geometry=box(0, 0, 250, 250),
        )
        overlapping_poi = PoiSnapshot(
            area_cd="POI001", male_rate=50.0, female_rate=50.0,
            pop_estimate=100.0, geometry=box(100, 100, 400, 400), area_m2=90000.0,
        )
        far_poi = PoiSnapshot(
            area_cd="POI002", male_rate=50.0, female_rate=50.0,
            pop_estimate=100.0, geometry=box(10000, 10000, 10100, 10100), area_m2=10000.0,
        )

        result = find_overlap_areas([cell], [overlapping_poi, far_poi])

        assert set(result.keys()) == {"C1"}
        [(area_cd, area)] = result["C1"]
        assert area_cd == "POI001"
        assert area == pytest.approx(150.0 * 150.0)  # 겹치는 사각형: (100,100)-(250,250)

    def test_no_pois_returns_empty_dict(self):
        cell = GridCell(cell_id="C1", h_dng_cd="H1", spop=1.0, ages=_zero_ages(), geometry=box(0, 0, 1, 1))
        assert find_overlap_areas([cell], []) == {}


class TestMergeCellTotalOnly:
    """미래 시각용: 총량만 예측값으로 합성하고 성·연령 구성비는 baseline을 따른다."""

    @staticmethod
    def _forecast_poi(pop_estimate: float) -> PoiSnapshot:
        # 성비는 예측에 없어 0.0으로 두고, 이 경로는 성비를 쓰지 않는다.
        return PoiSnapshot(
            area_cd="POI001", male_rate=0.0, female_rate=0.0,
            pop_estimate=pop_estimate,
            geometry=box(953810.0, 1952620.0, 954060.0, 1952870.0), area_m2=62500.0,
        )

    def test_total_follows_density_update_like_the_observed_path(self):
        cell = _grid_cell(100.0, {**_zero_ages(), "M20": 60.0, "F20": 40.0})

        result = merge_cell_total_only(cell, [(self._forecast_poi(200.0), 18750.0)])

        assert result.spop == pytest.approx(130.0)

    def test_gender_and_age_ratio_is_preserved(self):
        cell = _grid_cell(100.0, {**_zero_ages(), "M20": 60.0, "F20": 40.0})

        result = merge_cell_total_only(cell, [(self._forecast_poi(200.0), 18750.0)])

        assert result.ages["M20"] == pytest.approx(78.0)  # 130 * 0.6
        assert result.ages["F20"] == pytest.approx(52.0)  # 130 * 0.4
        assert sum(result.ages.values()) == pytest.approx(result.spop)

    def test_ignores_poi_rates_entirely(self):
        """성비가 들어와도 무시한다 — 예측 시각의 성비는 baseline이 결정한다."""
        cell = _grid_cell(100.0, {**_zero_ages(), "M20": 60.0, "F20": 40.0})
        skewed = PoiSnapshot(
            area_cd="POI001", male_rate=90.0, female_rate=10.0, pop_estimate=200.0,
            geometry=box(953810.0, 1952620.0, 954060.0, 1952870.0), area_m2=62500.0,
        )

        result = merge_cell_total_only(cell, [(skewed, 18750.0)])

        assert result.ages["M20"] == pytest.approx(78.0)
        assert result.ages["F20"] == pytest.approx(52.0)

    def test_zero_baseline_ages_split_evenly(self):
        cell = _grid_cell(0.0, _zero_ages())

        result = merge_cell_total_only(cell, [(self._forecast_poi(200.0), 62500.0)])

        assert result.spop == pytest.approx(200.0)
        assert all(v == pytest.approx(200.0 / len(AGE_COLUMNS)) for v in result.ages.values())
        assert sum(result.ages.values()) == pytest.approx(result.spop)

    def test_no_overlap_passes_baseline_through(self):
        cell = _grid_cell(50.0, {**_zero_ages(), "M20": 30.0, "F20": 20.0})

        result = merge_cell_total_only(cell, [])

        assert result.spop == 50.0
        assert result.ages["M20"] == 30.0


class TestFindOverlapAreas:
    """교차 계산은 지오메트리만 본다 — 그래서 13개 시각이 결과를 공유할 수 있다."""

    def test_returns_area_codes_so_snapshots_can_be_swapped_per_target(self):
        cell = _grid_cell(100.0)
        poi_geom = PoiSnapshot(
            area_cd="POI001", male_rate=50.0, female_rate=50.0, pop_estimate=0.0,
            geometry=box(953810.0, 1952620.0, 954060.0, 1952870.0), area_m2=62500.0,
        )

        result = find_overlap_areas([cell], [poi_geom])

        assert list(result) == [cell.cell_id]
        assert result[cell.cell_id][0][0] == "POI001"
        assert result[cell.cell_id][0][1] == pytest.approx(62500.0)

    def test_bind_snapshots_drops_pois_without_a_value_for_that_target(self):
        from merge import bind_snapshots

        available = PoiSnapshot(
            area_cd="POI001", male_rate=0.0, female_rate=0.0, pop_estimate=100.0,
            geometry=box(0, 0, 1, 1), area_m2=1.0,
        )

        bound = bind_snapshots(
            [("POI001", 100.0), ("POI002", 200.0)], {"POI001": available}
        )

        assert [pair[0].area_cd for pair in bound] == ["POI001"]
