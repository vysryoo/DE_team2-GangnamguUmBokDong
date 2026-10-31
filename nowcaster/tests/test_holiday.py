"""holiday.py: 공휴일/일요일 판정과 후보 날짜 선정 로직."""

from datetime import date

import pytest

import holiday


class TestIsSpecialDay:
    @pytest.mark.parametrize(
        ("day", "expected"),
        [
            (date(2026, 8, 16), True),  # 일요일
            (date(2026, 8, 12), False),  # 수요일
            (date(2026, 5, 5), True),  # 어린이날(화요일)
            (date(2026, 8, 22), False),  # 토요일(공휴일 아님)
        ],
    )
    def test_sunday_and_public_holiday_are_special(self, day, expected):
        assert holiday.is_special_day(day) is expected


class TestCandidateDates:
    def test_regular_weekday_returns_last_4_same_weekdays(self):
        target = date(2026, 8, 12)  # 수요일

        result = holiday.candidate_dates(target)

        assert result == [
            date(2026, 8, 5),
            date(2026, 7, 29),
            date(2026, 7, 22),
            date(2026, 7, 15),
        ]

    def test_special_day_target_scans_backward_for_special_days(self):
        target = date(2026, 5, 5)  # 어린이날(화요일, 공휴일)

        result = holiday.candidate_dates(target)

        assert len(result) == 4
        assert all(holiday.is_special_day(d) for d in result)
        assert result == sorted(result, reverse=True)


class TestMatchesTargetPattern:
    def test_true_when_both_regular_weekdays(self):
        assert holiday.matches_target_pattern(date(2026, 8, 5), date(2026, 8, 12)) is True

    def test_false_when_candidate_is_holiday_but_target_is_not(self):
        assert holiday.matches_target_pattern(date(2026, 5, 5), date(2026, 5, 12)) is False


class TestExtendedCandidateDates:
    def test_returns_weeks_5_to_8_for_regular_weekday(self):
        target = date(2026, 8, 12)  # 수요일

        result = holiday.extended_candidate_dates(target)

        assert result == [
            date(2026, 7, 8),
            date(2026, 7, 1),
            date(2026, 6, 24),
            date(2026, 6, 17),
        ]
