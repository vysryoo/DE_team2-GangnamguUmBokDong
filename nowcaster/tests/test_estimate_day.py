"""estimate_day.py: 격자·시간대(H_DNG_CD/CELL_ID/TT)별로 후보 주차 archive를 조인해 추정 테이블을 만든다."""

from datetime import date

import pandas as pd
import pytest

import estimate_day


def _frame(rows):
    return pd.DataFrame(rows)


_NAN = float("nan")


def _spop_frame(value):
    """SPOP 하나짜리 프레임. None이면 그 주차 archive가 없음, NaN이면 행만 있고 값이 결측."""
    if value is None:
        return None
    return _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": value}])


@pytest.mark.parametrize(
    ("candidates", "extended", "historical", "expected", "method"),
    [
        ([100.0, 200.0, 300.0, 400.0], [], None, 100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1, "weighted_avg"),
        ([100.0, None, 300.0, 400.0], [], None, (100.0 * 0.4 + 300.0 * 0.2 + 400.0 * 0.1) / 0.7, "reweighted_avg"),
        ([100.0, None, None, 400.0], [], None, (100.0 * 0.4 + 400.0 * 0.1) / 0.5, "reweighted_avg"),
        ([None, None, 300.0, None], [], None, 300.0, "single_week_fallback"),
        ([_NAN, None, None, None], [None, 250.0, 260.0], None, 250.0, "extended_lookback_fallback"),
        ([_NAN, None, None, None], [None, None], 180.0, 180.0, "grid_historical_avg"),
        ([_NAN, None, None, None], [None], None, 0.0, "no_data"),
    ],
)
def test_single_cell_estimation_rule(candidates, extended, historical, expected, method):
    """후보 가중평균 → 재조정 → 단일 주차 → 확장 주차 → 과거 평균 → 0 순서를 따른다."""
    result = estimate_day.build_nowcast_table(
        [_spop_frame(value) for value in candidates],
        extended_frames=[_spop_frame(value) for value in extended],
        historical_avg_frame=_spop_frame(historical),
    )

    row = result.iloc[0]
    assert row["SPOP"] == pytest.approx(expected)
    assert row["estimation_method"] == method


class TestBuildNowcastTable:
    def test_all_four_weeks_present_uses_weighted_avg(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])
        week2 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0}])

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        row = result.iloc[0]
        assert row["SPOP"] == pytest.approx(100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1)
        assert row["is_estimated"] == True
        assert row["estimation_method"] == "weighted_avg"

    def test_one_week_missing_key_renormalizes(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0}])

        result = estimate_day.build_nowcast_table([week1, None, week3, week4])

        row = result.iloc[0]
        expected = (100.0 * 0.4 + 300.0 * 0.2 + 400.0 * 0.1) / 0.7
        assert row["SPOP"] == pytest.approx(expected)
        assert row["estimation_method"] == "reweighted_avg"

    def test_key_absent_from_all_four_weeks_uses_extended_fallback(self):
        empty = _frame([])
        extended_hit = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 250.0}])

        result = estimate_day.build_nowcast_table(
            [empty, empty, empty, empty], extended_frames=[None, extended_hit]
        )

        row = result.iloc[0]
        assert row["SPOP"] == 250.0
        assert row["estimation_method"] == "extended_lookback_fallback"

    def test_multiple_value_columns_all_estimated_consistently(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0, "M00": 10.0}])
        week2 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0, "M00": 20.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0, "M00": 30.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0, "M00": 40.0}])

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        row = result.iloc[0]
        assert row["M00"] == pytest.approx(10.0 * 0.4 + 20.0 * 0.3 + 30.0 * 0.2 + 40.0 * 0.1)

class TestHistoricalAverageCached:
    def _fake_cache_store(self):
        """load_cache/save_cache 계약을 지키는 인메모리 캐시 저장소를 만든다."""
        store = {}

        def load_cache(pattern):
            if pattern not in store:
                return None, None, []
            return store[pattern]

        def save_cache(pattern, sum_df, count_df, included):
            store[pattern] = (sum_df, count_df, included)

        return store, load_cache, save_cache

    def test_first_call_reads_every_date_and_populates_cache(self):
        d1, d2 = date(2026, 1, 5), date(2026, 1, 12)
        frames = {
            d1: _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}]),
            d2: _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}]),
        }
        calls = []

        def read_frame(d):
            calls.append(d)
            return frames[d]

        store, load_cache, save_cache = self._fake_cache_store()
        result = estimate_day.historical_average_cached("weekday", [d1, d2], read_frame, load_cache, save_cache)

        assert result.loc[("H1", "C1", "10")]["SPOP"] == 200.0
        assert sorted(calls) == [d1, d2]
        assert sorted(store["weekday"][2]) == [d1.isoformat(), d2.isoformat()]

    def test_second_call_only_reads_dates_not_yet_cached(self):
        d1, d2 = date(2026, 1, 5), date(2026, 1, 12)
        frames = {
            d1: _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}]),
            d2: _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}]),
        }
        store, load_cache, save_cache = self._fake_cache_store()

        def read_frame_first(d):
            return frames[d]

        estimate_day.historical_average_cached("weekday", [d1], read_frame_first, load_cache, save_cache)

        calls = []

        def read_frame_second(d):
            calls.append(d)
            return frames[d]

        result = estimate_day.historical_average_cached(
            "weekday", [d1, d2], read_frame_second, load_cache, save_cache
        )

        # d1은 이미 캐시에 있으니 다시 읽지 않고 d2만 새로 읽는다.
        assert calls == [d2]
        assert result.loc[("H1", "C1", "10")]["SPOP"] == 200.0

    def test_no_new_dates_does_not_call_save_cache(self):
        d1 = date(2026, 1, 5)
        store, load_cache, save_cache_calls = self._fake_cache_store()

        def read_frame(d):
            return _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])

        estimate_day.historical_average_cached("weekday", [d1], read_frame, load_cache, save_cache_calls)
        save_calls = []
        estimate_day.historical_average_cached(
            "weekday", [d1], read_frame, load_cache, lambda *a: save_calls.append(a)
        )

        assert save_calls == []


class TestBuildNowcastTableUsesHistoricalAverage:
    def test_falls_back_to_historical_avg_when_no_other_source(self):
        empty = _frame([])
        historical = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 180.0}])

        result = estimate_day.build_nowcast_table(
            [empty, empty, empty, empty],
            extended_frames=[None],
            historical_avg_frame=historical,
        )

        row = result.iloc[0]
        assert row["SPOP"] == 180.0
        assert row["estimation_method"] == "grid_historical_avg"


class TestMultipleKeysEstimatedIndependently:
    def test_multiple_keys_estimated_independently(self):
        week1 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 500.0},
            ]
        )
        week2 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 600.0},
            ]
        )
        week3 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 700.0},
            ]
        )
        week4 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 800.0},
            ]
        )

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        by_cell = result.set_index("CELL_ID")
        assert by_cell.loc["C1", "SPOP"] == pytest.approx(100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1)
        assert by_cell.loc["C2", "SPOP"] == pytest.approx(500.0 * 0.4 + 600.0 * 0.3 + 700.0 * 0.2 + 800.0 * 0.1)
