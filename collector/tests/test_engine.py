"""validation.engine 단위 테스트."""

import pytest

from config.schema import Policies, SourceConfig
from validation.engine import (
    BatchOutcome,
    BatchValidationFailed,
    _judge_column,
    _parse_bool,
    _process_columns,
    _resolve_policy_name,
    _resolve_row_params,
    _Tally,
    _try_cast,
    validate_batch,
)
from validation.types import IssueKind


@pytest.fixture
def policies():
    return Policies(
        required_missing="drop_row",
        required_outlier="drop_row",
        optional_missing="keep_null",
        optional_outlier="set_null",
    )


def _config(columns: dict, **policy_overrides):
    policies = {
        "required_missing": "drop_row",
        "required_outlier": "drop_row",
        "optional_missing": "keep_null",
        "optional_outlier": "set_null",
    }
    policies.update(policy_overrides)
    return SourceConfig.model_validate(
        {
            "source_id": "test_source",
            "description": "test",
            "adapter": "seoul_openapi",
            "schedule": {"interval": "5m"},
            "storage": {"bronze_format": "json", "silver_format": "parquet", "partition": ["dt", "hh"]},
            "quality": {"max_drop_ratio": 0.05},
            "policies": policies,
            "columns": columns,
        }
    )


class TestParseBool:
    def test_true_string(self):
        assert _parse_bool("true") is True

    def test_false_string(self):
        assert _parse_bool("false") is False

    def test_case_insensitive(self):
        assert _parse_bool("True") is True
        assert _parse_bool("FALSE") is False

    def test_actual_bool_passthrough(self):
        assert _parse_bool(True) is True
        assert _parse_bool(False) is False

    def test_zero_one_rejected(self):
        # 내장 bool()과 달리 "0"/"1"은 인정하지 않는다 — 오직 "true"/"false" 문자열만.
        with pytest.raises(ValueError):
            _parse_bool("0")
        with pytest.raises(ValueError):
            _parse_bool("1")

    def test_garbage_rejected(self):
        with pytest.raises(ValueError):
            _parse_bool("yes")


class TestTryCast:
    def test_single_type_success(self):
        value, ok = _try_cast("15", ("int",))
        assert ok is True
        assert value == 15
        assert isinstance(value, int)

    def test_single_type_failure(self):
        value, ok = _try_cast("abc", ("int",))
        assert ok is False
        assert value == "abc"

    def test_tries_in_declared_order(self):
        # int 시도가 실패하면 float로 넘어간다(선언 순서 그대로).
        value, ok = _try_cast("3.5", ("int", "float"))
        assert ok is True
        assert value == 3.5
        assert isinstance(value, float)

    def test_str_always_succeeds(self):
        value, ok = _try_cast(123, ("str",))
        assert ok is True
        assert value == "123"

    def test_all_fail(self):
        value, ok = _try_cast("abc", ("int", "float"))
        assert ok is False
        assert value == "abc"

    def test_bool_uses_dedicated_parser(self):
        value, ok = _try_cast("false", ("bool",))
        assert ok is True
        assert value is False


class TestJudgeColumn:
    def test_none_is_missing(self, make_spec):
        spec = make_spec(types=("int",), required=True)
        value, issue = _judge_column(None, "col", spec)
        assert value is None
        assert issue.kind is IssueKind.MISSING
        assert issue.required is True
        assert issue.raw_value is None

    def test_empty_string_is_missing(self, make_spec):
        spec = make_spec(types=("int",))
        value, issue = _judge_column("", "col", spec)
        assert value is None
        assert issue.kind is IssueKind.MISSING
        assert issue.raw_value == ""

    def test_whitespace_string_is_not_missing(self, make_spec):
        # strip 없음 — 정확히 ""만 결측으로 본다.
        spec = make_spec(types=("str",))
        value, issue = _judge_column("  ", "col", spec)
        assert issue is None
        assert value == "  "

    def test_cast_failure_is_type_error(self, make_spec):
        spec = make_spec(types=("int",))
        value, issue = _judge_column("abc", "col", spec)
        assert value == "abc"  # 정책에는 캐스팅 실패 원시값을 그대로 넘긴다
        assert issue.kind is IssueKind.TYPE_ERROR
        assert issue.raw_value == "abc"

    def test_successful_cast_no_issue(self, make_spec):
        spec = make_spec(types=("int",))
        value, issue = _judge_column("15", "col", spec)
        assert value == 15
        assert issue is None

    def test_range_violation_is_outlier(self, make_spec):
        spec = make_spec(types=("int",), range=(0, 200))
        value, issue = _judge_column("250", "col", spec)
        assert value == 250  # 캐스팅은 성공했으므로 casted 값을 넘긴다
        assert issue.kind is IssueKind.OUTLIER
        assert issue.raw_value == "250"

    def test_range_boundary_is_not_outlier(self, make_spec):
        spec = make_spec(types=("int",), range=(0, 200))
        _, issue_min = _judge_column("0", "col", spec)
        _, issue_max = _judge_column("200", "col", spec)
        assert issue_min is None
        assert issue_max is None

    def test_enum_violation_is_outlier(self, make_spec):
        spec = make_spec(types=("int",), enum=(0, 1, 2, 3))
        value, issue = _judge_column("9", "col", spec)
        assert value == 9
        assert issue.kind is IssueKind.OUTLIER

    def test_enum_membership_is_not_outlier(self, make_spec):
        spec = make_spec(types=("int",), enum=(0, 1, 2, 3))
        _, issue = _judge_column("2", "col", spec)
        assert issue is None

    def test_no_range_no_enum_always_passes(self, make_spec):
        spec = make_spec(types=("str",))
        value, issue = _judge_column("anything", "col", spec)
        assert value == "anything"
        assert issue is None


class TestResolvePolicyName:
    def test_required_missing_uses_quadrant_default(self, make_spec, make_issue, policies):
        spec = make_spec(required=True)
        issue = make_issue(IssueKind.MISSING, spec=spec)
        assert _resolve_policy_name(issue, policies) == "drop_row"

    def test_optional_missing_uses_quadrant_default(self, make_spec, make_issue, policies):
        spec = make_spec(required=False)
        issue = make_issue(IssueKind.MISSING, spec=spec)
        assert _resolve_policy_name(issue, policies) == "keep_null"

    def test_required_outlier_uses_quadrant_default(self, make_spec, make_issue, policies):
        spec = make_spec(required=True)
        issue = make_issue(IssueKind.OUTLIER, spec=spec)
        assert _resolve_policy_name(issue, policies) == "drop_row"

    def test_optional_outlier_uses_quadrant_default(self, make_spec, make_issue, policies):
        spec = make_spec(required=False)
        issue = make_issue(IssueKind.OUTLIER, spec=spec)
        assert _resolve_policy_name(issue, policies) == "set_null"

    def test_on_missing_override_wins(self, make_spec, make_issue, policies):
        spec = make_spec(required=True, on_missing="fill_zero")
        issue = make_issue(IssueKind.MISSING, spec=spec)
        assert _resolve_policy_name(issue, policies) == "fill_zero"

    def test_on_outlier_override_wins_for_outlier(self, make_spec, make_issue, policies):
        spec = make_spec(required=False, on_outlier="clip_to_range")
        issue = make_issue(IssueKind.OUTLIER, spec=spec)
        assert _resolve_policy_name(issue, policies) == "clip_to_range"

    def test_type_error_ignores_on_outlier_override_required(self, make_spec, make_issue, policies):
        # TYPE_ERROR는 컬럼 오버라이드를 무시하고 4분면 기본값만 쓴다.
        spec = make_spec(required=True, on_outlier="clip_to_range")
        issue = make_issue(IssueKind.TYPE_ERROR, spec=spec)
        assert _resolve_policy_name(issue, policies) == "drop_row"

    def test_type_error_ignores_on_outlier_override_optional(self, make_spec, make_issue, policies):
        spec = make_spec(required=False, on_outlier="clip_to_range")
        issue = make_issue(IssueKind.TYPE_ERROR, spec=spec)
        assert _resolve_policy_name(issue, policies) == "set_null"


class TestProcessColumns:
    def test_no_issues_resolved_matches_casted_values(self, ctx):
        config = _config({"rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}}})
        row = {"rackTotCnt": "15"}
        result = _process_columns(row, config, ctx, _Tally())
        assert result.resolved == {"rackTotCnt": 15}
        assert result.issues == []
        assert result.repaired is False
        assert result.dropped_by_column is False

    def test_optional_missing_keep_null_not_repaired(self, ctx):
        config = _config({"note": {"types": ["str"], "required": False}})
        result = _process_columns({"note": ""}, config, ctx, _Tally())
        assert result.resolved == {"note": None}
        assert result.repaired is False  # keep_null은 값을 바꾸지 않는다

    def test_optional_outlier_set_null_is_repaired(self, ctx):
        config = _config({"temp": {"types": ["int"], "range": {"min": -50, "max": 50}}})
        result = _process_columns({"temp": "999"}, config, ctx, _Tally())
        assert result.resolved == {"temp": None}
        assert result.repaired is True  # set_null이 999 -> None으로 값을 바꿨다

    def test_required_missing_drop_row_marks_dropped(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        result = _process_columns({"stationId": ""}, config, ctx, _Tally())
        assert result.dropped_by_column is True

    def test_dropped_column_does_not_stop_remaining_columns(self, ctx):
        # 폐기가 확정돼도 나머지 컬럼을 계속 판정해 quarantine에 전체 이슈를 남긴다.
        config = _config(
            {
                "stationId": {"types": ["str"], "required": True},
                "rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}},
            }
        )
        result = _process_columns({"stationId": "", "rackTotCnt": "999"}, config, ctx, _Tally())
        assert result.dropped_by_column is True
        assert len(result.issues) == 2
        assert {entry["column"] for entry in result.issue_entries} == {"stationId", "rackTotCnt"}

    def test_issue_entry_shape(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        result = _process_columns({"stationId": ""}, config, ctx, _Tally())
        assert result.issue_entries == [
            {"column": "stationId", "kind": "missing", "required": True, "action": "drop_row"}
        ]

    def test_column_issue_counts_accumulate(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        tally = _Tally()
        _process_columns({"stationId": ""}, config, ctx, tally)
        assert tally.column_issues == {"stationId": {"missing": 1, "outlier": 0, "type_error": 0}}

    def test_type_error_counted_separately_from_outlier(self, ctx):
        config = _config({"rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}}})
        tally = _Tally()
        _process_columns({"rackTotCnt": "abc"}, config, ctx, tally)
        assert tally.column_issues == {"rackTotCnt": {"missing": 0, "outlier": 0, "type_error": 1}}

    def test_policy_action_counts_accumulate(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        tally = _Tally()
        _process_columns({"stationId": ""}, config, ctx, tally)
        assert tally.policy_actions == {"drop_row": 1}

    def test_fail_batch_raises(self, ctx):
        config = _config(
            {"stationId": {"types": ["str"], "required": True}},
            required_missing="fail_batch",
        )
        with pytest.raises(BatchValidationFailed):
            _process_columns({"stationId": ""}, config, ctx, _Tally())

    def test_original_row_not_mutated(self, ctx):
        config = _config({"rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}}})
        row = {"rackTotCnt": "15"}
        _process_columns(row, config, ctx, _Tally())
        assert row == {"rackTotCnt": "15"}

    def test_policy_receives_original_raw_row_not_partially_resolved(self, ctx, monkeypatch):
        # 두 번째 컬럼을 처리할 때, 정책에 넘어가는 row는 첫 번째 컬럼이 교정되기 전의
        # 원본 raw row와 같아야 한다(3.8절 결정) — 부분 교정된 dict가 아니다.
        config = _config(
            {
                "temp": {"types": ["int"], "range": {"min": -50, "max": 50}},  # 먼저 교정된다
                "rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}},
            }
        )
        row = {"temp": "999", "rackTotCnt": "250"}
        seen_rows = []

        import validation.engine as engine_module
        original_get_policy = engine_module.get_policy

        def _spy(name):
            fn = original_get_policy(name)

            def _wrapped(value, issue, row_arg, ctx_arg):
                seen_rows.append(dict(row_arg))
                return fn(value, issue, row_arg, ctx_arg)

            return _wrapped

        monkeypatch.setattr(engine_module, "get_policy", _spy)
        _process_columns(row, config, ctx, _Tally())

        assert seen_rows == [
            {"temp": "999", "rackTotCnt": "250"},
            {"temp": "999", "rackTotCnt": "250"},
        ]


class TestResolveRowParams:
    def test_no_row_policy_returns_none(self):
        policies = Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
            row=None,
        )
        assert _resolve_row_params(policies) is None

    def test_row_policy_without_params_returns_none(self):
        policies = Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
            row="keep_always",
        )
        assert _resolve_row_params(policies) is None

    def test_row_policy_with_params_parses_model(self):
        policies = Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
            row="drop_if_issue_count_exceeds",
            row_params={"max_issues": 3},
        )
        params = _resolve_row_params(policies)
        assert params.max_issues == 3


class TestValidateBatch:
    def test_all_rows_kept_ok_status(self, ctx):
        config = _config({"rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}}})
        outcome = validate_batch([{"rackTotCnt": "15"}, {"rackTotCnt": "20"}], config, ctx)
        assert isinstance(outcome, BatchOutcome)
        assert outcome.silver_rows == [
            {"rackTotCnt": 15, "_row_status": "ok"},
            {"rackTotCnt": 20, "_row_status": "ok"},
        ]
        assert outcome.quarantine_records == []
        assert outcome.counts == {"fetched": 2, "kept": 2, "repaired": 0, "dropped": 0}
        assert outcome.drop_ratio == 0.0

    def test_repaired_row_status(self, ctx):
        config = _config({"temp": {"types": ["int"], "range": {"min": -50, "max": 50}}})
        outcome = validate_batch([{"temp": "999"}], config, ctx)
        assert outcome.silver_rows == [{"temp": None, "_row_status": "repaired"}]
        assert outcome.counts["repaired"] == 1

    def test_dropped_by_column_policy_goes_to_quarantine(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        outcome = validate_batch([{"stationId": ""}], config, ctx)
        assert outcome.silver_rows == []
        assert len(outcome.quarantine_records) == 1
        record = outcome.quarantine_records[0]
        assert record["stationId"] is None
        assert record["_row_index"] == 0
        assert record["_issues"] == [
            {"column": "stationId", "kind": "missing", "required": True, "action": "drop_row"}
        ]
        assert outcome.counts == {"fetched": 1, "kept": 0, "repaired": 0, "dropped": 1}
        assert outcome.drop_ratio == 1.0

    def test_quarantine_keeps_raw_value_for_non_missing_columns(self, ctx):
        config = _config(
            {
                "stationId": {"types": ["str"], "required": True},
                "rackTotCnt": {"types": ["int"], "range": {"min": 0, "max": 200}},
            }
        )
        outcome = validate_batch([{"stationId": "", "rackTotCnt": "10"}], config, ctx)
        record = outcome.quarantine_records[0]
        assert record["stationId"] is None      # MISSING -> 정규화된 None
        assert record["rackTotCnt"] == "10"       # 이슈 없는 컬럼은 원본 raw 값 그대로

    def test_row_policy_can_drop_row_that_column_policies_kept(self, ctx):
        config = _config(
            {
                "a": {"types": ["int"], "required": True},
                "b": {"types": ["int"], "required": True},
                "c": {"types": ["int"], "required": True},
            },
            required_missing="keep_null",  # 컬럼 레벨에서는 폐기하지 않는다
            row="drop_if_issue_count_exceeds",
            row_params={"max_issues": 2},
        )
        # 이슈 3개 -> 임계값(2) 초과 -> 행 정책이 폐기한다
        outcome = validate_batch([{"a": "", "b": "", "c": ""}], config, ctx)
        assert outcome.silver_rows == []
        assert outcome.counts["dropped"] == 1

    def test_row_policy_not_called_when_column_already_dropped(self, ctx, monkeypatch):
        config = _config(
            {"stationId": {"types": ["str"], "required": True}},
            row="keep_always",
        )
        calls = []
        from validation import registry

        original = registry.get_row_policy

        def _spy(name):
            fn = original(name)

            def _wrapped(*args, **kwargs):
                calls.append(name)
                return fn(*args, **kwargs)

            return _wrapped

        monkeypatch.setattr("validation.engine.get_row_policy", _spy)
        outcome = validate_batch([{"stationId": ""}], config, ctx)
        assert outcome.counts["dropped"] == 1
        assert calls == []  # 컬럼에서 이미 DROP_ROW가 나왔으니 행 정책은 호출되지 않는다

    def test_fail_batch_stops_iteration(self, ctx):
        config = _config(
            {"stationId": {"types": ["str"], "required": True}},
            required_missing="fail_batch",
        )
        with pytest.raises(BatchValidationFailed):
            validate_batch([{"stationId": "ok"}, {"stationId": ""}, {"stationId": "ok"}], config, ctx)

    def test_column_issues_and_policy_actions_shape(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        outcome = validate_batch([{"stationId": ""}], config, ctx)
        assert outcome.column_issues == {"stationId": {"missing": 1, "outlier": 0, "type_error": 0}}
        assert outcome.policy_actions == {"drop_row": 1}

    def test_empty_batch(self, ctx):
        config = _config({"stationId": {"types": ["str"], "required": True}})
        outcome = validate_batch([], config, ctx)
        assert outcome.counts == {"fetched": 0, "kept": 0, "repaired": 0, "dropped": 0}
        assert outcome.drop_ratio == 0.0
