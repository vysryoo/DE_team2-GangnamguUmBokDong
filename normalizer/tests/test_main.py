"""main.py: CLI 인자 파싱과 예측 시각 해석 테스트."""

from datetime import datetime, timedelta

import main
import poi
import pyarrow as pa
import pytest
from core.source_snapshot_io import (
    AvailableSourceSnapshot,
    SourceDataStatus,
    SourceFreshness,
)
from shapely.geometry import box

from core.kst import KST


class TestParseArgs:
    def test_requires_window_start(self):
        with pytest.raises(SystemExit):
            main.parse_args([])

    def test_takes_window_start_only(self):
        """baseline 모드 인자는 없다 — baseline은 항상 nowcaster 추정치다."""
        args = main.parse_args(["--window-start", "2026-08-12T14:05:00+09:00"])

        assert args.window_start == "2026-08-12T14:05:00+09:00"
        assert args.poi_master_mode == "static"
        assert not hasattr(args, "baseline_date_mode")

    def test_takes_exact_s3_poi_master_ref(self):
        """Airflow가 고정한 S3 manifest URI와 SHA를 그대로 파싱한다."""
        args = main.parse_args([
            "--window-start", "2026-08-12T14:05:00+09:00",
            "--poi-master-mode", "s3",
            "--poi-master-manifest-uri",
            (
                "s3://bucket/source_snapshot_manifest/poi_master/dt=2026-08-12/"
                "hh=05/logical=20260812T050500000000Z/revision=0000000000.json"
            ),
            "--poi-master-manifest-sha256", "a" * 64,
        ])

        assert args.poi_master_mode == "s3"
        assert args.poi_master_manifest_sha256 == "a" * 64

    def test_rejects_removed_baseline_mode_flag(self):
        with pytest.raises(SystemExit):
            main.parse_args([
                "--window-start", "2026-08-12T14:05:00+09:00",
                "--baseline-date-mode", "latest",
            ])


def _forecast_row(area_cd: str, first_hour: int, slots: int = 12, *, fcst_yn: str = "Y") -> dict:
    """`FCST_n_*` 슬롯이 채워진 population_realtime 행 한 개를 만든다."""
    row = {
        "AREA_CD": area_cd,
        "AREA_PPLTN_MIN": 1000,
        "AREA_PPLTN_MAX": 2000,
        "MALE_PPLTN_RATE": 50.0,
        "FEMALE_PPLTN_RATE": 50.0,
        "FCST_YN": fcst_yn,
    }
    for slot in range(1, slots + 1):
        hour = first_hour + slot - 1
        stamp = datetime(2026, 8, 12, 0, 0, tzinfo=KST) + timedelta(hours=hour)
        row[f"FCST_{slot}_TIME"] = stamp.strftime("%Y-%m-%d %H:%M")
        row[f"FCST_{slot}_PPLTN_MIN"] = 100 * slot
        row[f"FCST_{slot}_PPLTN_MAX"] = 100 * slot + 500
    return row


class TestCollectForecasts:
    """슬롯 번호가 아니라 `FCST_n_TIME`이 시각을 결정한다."""

    def test_uses_declared_times_not_slot_numbers(self):
        window_start = datetime(2026, 8, 12, 20, 55, tzinfo=KST)
        # 실측과 같은 형태: 20:55 관측인데 첫 슬롯이 22:00이다(21:00이 아니다).
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=22)])

        result = main._collect_forecasts(table, window_start)

        assert set(result) == {"POI001"}
        assert min(result["POI001"]) == datetime(2026, 8, 12, 22, 0, tzinfo=KST)
        assert result["POI001"][datetime(2026, 8, 12, 22, 0, tzinfo=KST)] == (100.0, 600.0)

    def test_drops_past_and_beyond_horizon_times(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        # 18:00부터 12슬롯 → 18/19/20시는 과거이거나 현재, 나머지는 12시간 안에 든다.
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=18)])

        result = main._collect_forecasts(table, window_start)

        targets = sorted(result["POI001"])
        assert targets[0] == datetime(2026, 8, 12, 21, 0, tzinfo=KST)
        assert all(t <= window_start + timedelta(hours=12) for t in targets)

    def test_skips_pois_that_do_not_forecast(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=21, fcst_yn="N")])

        assert main._collect_forecasts(table, window_start) == {}

    def test_targets_are_the_sorted_union_across_pois(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        table = pa.Table.from_pylist([
            _forecast_row("POI001", first_hour=21, slots=3),
            _forecast_row("POI002", first_hour=22, slots=3),
        ])

        forecasts = main._collect_forecasts(table, window_start)
        targets = main._forecast_targets(forecasts)

        assert targets == [
            datetime(2026, 8, 12, 21, 0, tzinfo=KST),
            datetime(2026, 8, 12, 22, 0, tzinfo=KST),
            datetime(2026, 8, 12, 23, 0, tzinfo=KST),
            datetime(2026, 8, 13, 0, 0, tzinfo=KST),
        ]

    def test_unparsable_time_is_dropped_without_failing(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        row = _forecast_row("POI001", first_hour=21, slots=2)
        row["FCST_1_TIME"] = "어제쯤"
        table = pa.Table.from_pylist([row])

        result = main._collect_forecasts(table, window_start)

        assert sorted(result["POI001"]) == [datetime(2026, 8, 12, 22, 0, tzinfo=KST)]


class TestBuildForecastSnapshots:
    def test_uses_min_max_average_and_leaves_rates_unused(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        target = datetime(2026, 8, 12, 21, 0, tzinfo=KST)
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=21, slots=1)])
        forecasts = main._collect_forecasts(table, window_start)
        areas = (
            poi.PoiArea(area_cd="POI001", area_nm="가", geometry=box(0, 0, 100, 100), area_m2=10000.0),
            poi.PoiArea(area_cd="POI002", area_nm="나", geometry=box(0, 0, 100, 100), area_m2=10000.0),
        )

        snapshots = main._build_forecast_snapshots(areas, forecasts, target)

        assert set(snapshots) == {"POI001"}
        assert snapshots["POI001"].pop_estimate == pytest.approx((100.0 + 600.0) / 2)
        assert snapshots["POI001"].male_rate == 0.0


def _poi_area(area_cd: str) -> poi.PoiArea:
    """POI 입력 계약 테스트용 유효 영역을 만든다."""
    geometry = box(0, 0, 100, 100)
    return poi.PoiArea(
        area_cd=area_cd,
        area_nm=f"장소 {area_cd}",
        geometry=geometry,
        area_m2=geometry.area,
    )


class TestRealtimePoiContract:
    """실시간 POI 코드가 한 실행에 고정된 Master와 일치하는지 검증한다."""

    @pytest.mark.parametrize(
        "raw_code",
        [None, "", " POI001", "POI1", "POI１２３", 1, True],
    )
    def test_rejects_null_or_invalid_area_codes(self, raw_code):
        table = pa.table({"AREA_CD": [raw_code]})

        with pytest.raises(main.RealtimePoiContractError, match="ASCII 숫자 3자리"):
            main._validate_realtime_poi_contract(table, (_poi_area("POI001"),))

    def test_rejects_missing_area_code_column(self):
        with pytest.raises(main.RealtimePoiContractError, match="AREA_CD 컬럼"):
            main._validate_realtime_poi_contract(
                pa.table({"AREA_NM": ["장소"]}),
                (_poi_area("POI001"),),
            )

    def test_rejects_duplicate_area_codes(self):
        table = pa.table({"AREA_CD": ["POI001", "POI001"]})

        with pytest.raises(main.RealtimePoiContractError, match="중복"):
            main._validate_realtime_poi_contract(table, (_poi_area("POI001"),))

    def test_rejects_codes_absent_from_selected_master(self):
        table = pa.table({"AREA_CD": ["POI001", "POI132"]})

        with pytest.raises(main.RealtimePoiContractError, match="POI132"):
            main._validate_realtime_poi_contract(table, (_poi_area("POI001"),))

    def test_allows_realtime_to_omit_master_codes(self):
        table = pa.table({"AREA_CD": ["POI001"]})
        areas = (_poi_area("POI001"), _poi_area("POI002"))

        assert main._validate_realtime_poi_contract(table, areas) == frozenset(
            {"POI001"}
        )

    def test_run_rejects_unknown_code_before_baseline_or_output(self, monkeypatch):
        """Master 불일치를 baseline 계산이나 Silver 쓰기 전에 차단한다."""
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        realtime = pa.Table.from_pylist(
            [_forecast_row("POI132", first_hour=15, slots=1)]
        )
        monkeypatch.setattr(
            main.storage,
            "read_realtime_snapshot",
            lambda _window: AvailableSourceSnapshot(
                status=SourceDataStatus.SUCCESS,
                freshness=SourceFreshness.CURRENT,
                logical_dttm=_window,
                table=realtime,
                manifest=None,
            ),
        )
        monkeypatch.setattr(
            main.poi,
            "load_poi_master_areas",
            lambda _ref: (_poi_area("POI001"),),
        )
        monkeypatch.setattr(
            main,
            "_load_baseline_cells",
            lambda *_args: pytest.fail("Master 불일치 뒤 baseline을 읽으면 안 됩니다."),
        )
        monkeypatch.setattr(
            main.storage,
            "write_normalized_silver",
            lambda *_args, **_kwargs: pytest.fail("잘못된 입력으로 출력하면 안 됩니다."),
        )

        with pytest.raises(main.RealtimePoiContractError, match="POI132"):
            main.run(window_start)


class TestFilterGridRowsForHour:
    def test_keeps_only_matching_tt_and_sums_h_dng_components_by_cell_id(self):
        table = pa.table({
            "CELL_ID": ["다사53815262", "다사53815262", "다사53815262"],
            "TT": ["13", "14", "14"],
            "H_DNG_CD": ["1100059", "1100054", "1100053"],
            "SPOP": [10.0, 20.0, 30.0],
            **{
                c: [None, 2.0 if c == "M00" else None, 3.0 if c == "M00" else None]
                for c in main.merge.AGE_COLUMNS
            },
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert set(result.keys()) == {"다사53815262"}
        assert result["다사53815262"].spop == 50.0
        assert result["다사53815262"].ages["M00"] == 5.0
        assert result["다사53815262"].h_dng_cd == "1100053"

    @pytest.mark.parametrize(
        ("raw_tt", "hour"),
        [("0 ", 0), ("9", 9), ("09", 9), (9, 9)],
    )
    def test_accepts_supported_tt_representations(self, raw_tt, hour):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": [raw_tt],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=hour)

        assert result["다사53815262"].spop == 10.0

    @pytest.mark.parametrize("raw_tt", [None, "9.5", 9.5, "24", 24, "아홉", True])
    def test_rejects_invalid_tt(self, raw_tt):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": [raw_tt],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        with pytest.raises(ValueError, match="TT가 잘못됐습니다"):
            main._filter_grid_rows_for_hour(table, hour=9)

    def test_rejects_invalid_tt_even_when_another_row_matches_selected_hour(self):
        table = pa.table({
            "CELL_ID": ["다사53815262", "다사53815263"],
            "TT": ["14", "잘못됨"],
            "H_DNG_CD": ["1100053", "1100054"],
            "SPOP": [10.0, 20.0],
            **{c: [None, None] for c in main.merge.AGE_COLUMNS},
        })

        with pytest.raises(ValueError, match="TT가 잘못됐습니다"):
            main._filter_grid_rows_for_hour(table, hour=14)

    @pytest.mark.parametrize("raw_h_dng_cd", [None, "", "  ", "H1", "１２３"])
    def test_rejects_invalid_h_dng_cd(self, raw_h_dng_cd):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": [raw_h_dng_cd],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        with pytest.raises(ValueError, match="H_DNG_CD가 잘못됐습니다"):
            main._filter_grid_rows_for_hour(table, hour=14)

    def test_trims_valid_ascii_h_dng_cd(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": [" 1100053 "],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert result["다사53815262"].h_dng_cd == "1100053"

    @pytest.mark.parametrize("raw_cell_id", [None, "", "  "])
    def test_rejects_null_or_blank_cell_id(self, raw_cell_id):
        table = pa.table({
            "CELL_ID": [raw_cell_id],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        with pytest.raises(ValueError, match="CELL_ID가 잘못됐습니다"):
            main._filter_grid_rows_for_hour(table, hour=14)

    def test_null_spop_becomes_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": pa.array([None], type=pa.float64()),
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert result["다사53815262"].spop == 0.0

    def test_null_ages_become_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert all(v == 0.0 for v in result["다사53815262"].ages.values())


def test_run_rejects_empty_current_window_cells(monkeypatch):
    """현재 target이 비면 미래와 달리 성공 manifest를 남기지 않고 실패한다."""
    window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
    monkeypatch.setattr(
        main.storage,
        "read_realtime_snapshot",
        lambda _window: AvailableSourceSnapshot(
            status=SourceDataStatus.SUCCESS,
            freshness=SourceFreshness.CURRENT,
            logical_dttm=_window,
            table=pa.table({"AREA_CD": pa.array([], type=pa.string())}),
            manifest=None,
        ),
    )
    monkeypatch.setattr(main.poi, "load_poi_master_areas", lambda _ref: ())
    monkeypatch.setattr(main.storage, "read_nowcast_grid", lambda _date: None)
    monkeypatch.setattr(main, "_filter_grid_rows_for_hour", lambda _table, _hour: {})

    with pytest.raises(ValueError, match="현재 window"):
        main.run(window_start)
