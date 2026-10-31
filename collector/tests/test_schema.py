"""config.schema 모델 단위 테스트."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from config.schema import (
    ColumnSpec,
    Fetch,
    Policies,
    Quality,
    Range,
    Schedule,
    SourceConfig,
    Storage,
    _parse_duration,
)


class TestParseDuration:
    def test_minutes(self):
        assert _parse_duration("5m") == timedelta(minutes=5)

    def test_hours(self):
        assert _parse_duration("3h") == timedelta(hours=3)

    def test_days(self):
        assert _parse_duration("1d") == timedelta(days=1)

    def test_compound(self):
        assert _parse_duration("2m30s") == timedelta(minutes=2, seconds=30)

    def test_passthrough_timedelta(self):
        value = timedelta(hours=1)
        assert _parse_duration(value) is value

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            _parse_duration("")

    def test_garbage_rejected(self):
        with pytest.raises(ValueError):
            _parse_duration("five minutes")


class TestRange:
    def test_allows_min_only(self):
        bounds = Range(min=0)

        assert bounds.min == 0
        assert bounds.max == float("inf")

    def test_allows_max_only(self):
        bounds = Range(max=200)

        assert bounds.min == float("-inf")
        assert bounds.max == 200

    def test_requires_at_least_one_boundary(self):
        with pytest.raises(ValidationError):
            Range()

    def test_valid(self):
        r = Range(min=0, max=200)
        assert r.min == 0
        assert r.max == 200

    def test_forbids_extra_keys(self):
        with pytest.raises(ValidationError):
            Range(min=0, max=200, step=1)

    def test_min_equal_to_max_is_allowed(self):
        r = Range(min=100, max=100)
        assert r.min == r.max == 100

    def test_min_greater_than_max_rejected(self):
        with pytest.raises(ValidationError):
            Range(min=100, max=0)


class TestColumnSpec:
    def test_minimal(self):
        spec = ColumnSpec(types=["int"])
        assert spec.types == ("int",)
        assert spec.required is False
        assert spec.range is None

    def test_rejects_range_and_enum_together(self):
        with pytest.raises(ValidationError):
            ColumnSpec(types=["int"], range={"min": 0, "max": 200}, enum=[0, 1, 2])

    def test_allows_range_only(self):
        spec = ColumnSpec(types=["int"], range={"min": 0, "max": 200})
        assert spec.range.max == 200

    def test_allows_enum_only(self):
        spec = ColumnSpec(types=["int"], enum=[0, 1, 2, 3])
        assert spec.enum == (0, 1, 2, 3)

    def test_requires_at_least_one_type(self):
        with pytest.raises(ValidationError):
            ColumnSpec(types=[])


class TestSchedule:
    def test_parses_interval(self):
        s = Schedule(interval="5m")
        assert s.interval == timedelta(minutes=5)

    def test_forbids_extra_keys(self):
        with pytest.raises(ValidationError):
            Schedule(interval="5m", cron="*/5 * * * *")


class TestStorage:
    def test_valid(self):
        s = Storage(
            bronze_format="json", silver_format="parquet", partition=["dt", "hh"]
        )
        assert s.partition == ("dt", "hh")

    def test_requires_nonempty_partition(self):
        with pytest.raises(ValidationError):
            Storage(bronze_format="json", silver_format="parquet", partition=[])


class TestQuality:
    def test_defaults(self):
        q = Quality(max_drop_ratio=0.05)
        assert q.max_missing_ratio == 0.0
        assert q.allow_empty is False

    def test_max_drop_ratio_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            Quality(max_drop_ratio=1.5)

    def test_max_missing_ratio_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            Quality(max_drop_ratio=0.05, max_missing_ratio=-0.1)


class TestFetch:
    def test_budget_optional(self):
        assert Fetch().budget is None

    def test_budget_parses(self):
        assert Fetch(budget="2m30s").budget == timedelta(minutes=2, seconds=30)

def _minimal_source_config(**overrides):
    base = {
        "source_id": "test_source",
        "description": "test",
        "adapter": "seoul_openapi",
        "schedule": {"interval": "5m"},
        "storage": {
            "bronze_format": "json",
            "silver_format": "parquet",
            "partition": ["dt", "hh"],
        },
        "quality": {"max_drop_ratio": 0.05},
        "policies": {
            "required_missing": "drop_row",
            "required_outlier": "drop_row",
            "optional_missing": "keep_null",
            "optional_outlier": "set_null",
        },
        "columns": {"stationId": {"types": ["str"], "required": True}},
    }
    base.update(overrides)
    return SourceConfig.model_validate(base)


class TestPolicies:
    def test_row_optional(self):
        p = Policies(
            required_missing="drop_row",
            required_outlier="drop_row",
            optional_missing="keep_null",
            optional_outlier="set_null",
        )
        assert p.row is None
        assert p.row_params is None


class TestSourceConfig:
    def test_minimal_valid(self):
        config = _minimal_source_config()
        assert config.source_id == "test_source"
        assert config.adapter_params == {}
        assert config.config_version == ""

    @pytest.mark.parametrize(
        "extra", [{"unknown_key": "x"}, {"backfill": {"enabled": False}}]
    )
    def test_forbids_extra_top_level_key(self, extra):
        with pytest.raises(ValidationError):
            _minimal_source_config(**extra)

    def test_effective_fetch_budget_derives_when_unset(self):
        config = _minimal_source_config(schedule={"interval": "10m"})
        assert config.effective_fetch_budget() == timedelta(minutes=5)

    def test_effective_fetch_budget_caps_at_30_minutes(self):
        config = _minimal_source_config(schedule={"interval": "3h"})
        assert config.effective_fetch_budget() == timedelta(minutes=30)

    def test_effective_fetch_budget_uses_explicit_value(self):
        config = _minimal_source_config(
            schedule={"interval": "10m"}, fetch={"budget": "2m30s"}
        )
        assert config.effective_fetch_budget() == timedelta(minutes=2, seconds=30)
