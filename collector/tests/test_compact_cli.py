"""compact.py 테스트: 인자 파싱, 날짜 범위 해석, 종료 코드 매핑."""

from __future__ import annotations

from datetime import date

import pytest

import compact
from compaction import RECOVERY_DAYS, DateResult
from config.schema import ColumnSpec, Policies, Quality, Schedule, SourceConfig
from config.schema import Storage as StorageConfig

TODAY = date(2026, 8, 13)


def _config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_adapter",
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(bronze_format="json", silver_format="parquet", partition=("dt", "hh")),
        "quality": Quality(max_drop_ratio=0.5, max_missing_ratio=0.0, allow_empty=False),
        "policies": Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
        ),
        "columns": {"a": ColumnSpec(types=("str",))},
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


class TestParseArgs:
    def test_source_is_required(self):
        with pytest.raises(SystemExit):
            compact.parse_args([])

    def test_defaults_have_no_date_selection(self):
        args = compact.parse_args(["--source", "t_source"])

        assert args.date is None
        assert getattr(args, "from") is None
        assert args.to is None
        assert args.force is False

    def test_date_and_range_together_exits(self):
        with pytest.raises(SystemExit):
            compact.parse_args(
                ["--source", "t_source", "--date", "2026-08-12", "--from", "2026-08-01", "--to", "2026-08-02"]
            )

    def test_from_without_to_exits(self):
        with pytest.raises(SystemExit):
            compact.parse_args(["--source", "t_source", "--from", "2026-08-01"])

    def test_to_without_from_exits(self):
        with pytest.raises(SystemExit):
            compact.parse_args(["--source", "t_source", "--to", "2026-08-01"])


class TestResolveDates:
    def test_single_date(self):
        args = compact.parse_args(["--source", "t_source", "--date", "2026-08-12"])

        assert compact.resolve_dates(args, _config(), TODAY) == [date(2026, 8, 12)]

    def test_explicit_range_is_inclusive(self):
        args = compact.parse_args(
            ["--source", "t_source", "--from", "2026-08-01", "--to", "2026-08-03"]
        )

        assert compact.resolve_dates(args, _config(), TODAY) == [
            date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 3),
        ]

    def test_range_ignores_lookback(self):
        """도입 시점 일괄 압축은 검사 범위보다 훨씬 길 수 있어야 한다."""
        args = compact.parse_args(
            ["--source", "t_source", "--from", "2026-01-01", "--to", "2026-08-13"]
        )

        assert len(compact.resolve_dates(args, _config(), TODAY)) == 225

    def test_default_uses_config_lookback(self):
        args = compact.parse_args(["--source", "t_source"])

        assert len(compact.resolve_dates(args, _config(), TODAY)) == RECOVERY_DAYS

    def test_reversed_range_exits(self):
        args = compact.parse_args(
            ["--source", "t_source", "--from", "2026-08-05", "--to", "2026-08-01"]
        )

        with pytest.raises(SystemExit):
            compact.resolve_dates(args, _config(), TODAY)


class TestExitCode:
    def test_zero_when_all_succeeded(self):
        results = [
            DateResult(day=date(2026, 8, 12), status="compacted", rows=3),
            DateResult(day=date(2026, 8, 11), status="skipped"),
            DateResult(day=date(2026, 8, 10), status="empty"),
        ]

        assert compact.exit_code_for(results) == 0

    def test_nonzero_when_any_failed(self):
        results = [
            DateResult(day=date(2026, 8, 12), status="compacted", rows=3),
            DateResult(day=date(2026, 8, 11), status="failed", error="boom"),
        ]

        assert compact.exit_code_for(results) != 0

    def test_zero_for_empty_result_list(self):
        assert compact.exit_code_for([]) == 0
