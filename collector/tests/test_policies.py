"""정책 함수는 순수 함수다. 로그도 S3도 없다."""

import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from validation.policies import (
    IssueCountParams,
    clip_to_range,
    drop_if_any_required_issue,
    drop_if_issue_count_exceeds,
    drop_row,
    fail_batch,
    fill_default,
    fill_zero,
    keep_always,
    keep_null,
    set_null,
)
from validation.registry import (
    get_policy,
    get_row_policy,
    get_row_policy_params_model,
    policy_names,
    row_policy_names,
)
from validation.types import Action, IssueKind, RowVerdict


def test_all_column_policies_are_registered():
    assert set(policy_names()) >= {
        "keep_null",
        "set_null",
        "fill_zero",
        "fill_default",
        "clip_to_range",
        "drop_row",
        "fail_batch",
    }


@pytest.mark.parametrize(
    ("name", "fn"),
    [
        ("keep_null", keep_null),
        ("set_null", set_null),
        ("fill_zero", fill_zero),
        ("fill_default", fill_default),
        ("clip_to_range", clip_to_range),
        ("drop_row", drop_row),
        ("fail_batch", fail_batch),
    ],
)
def test_column_policy_name_resolves_to_the_right_function(name, fn):
    # 엔진은 이름으로만 정책에 도달한다. 데코레이터 이름이 뒤바뀌어도
    # 직접 호출 테스트는 전부 통과하므로 이 단정이 유일한 방어다.
    assert get_policy(name) is fn


@pytest.mark.parametrize(
    ("name", "fn"),
    [
        ("drop_if_any_required_issue", drop_if_any_required_issue),
        ("drop_if_issue_count_exceeds", drop_if_issue_count_exceeds),
        ("keep_always", keep_always),
    ],
)
def test_row_policy_name_resolves_to_the_right_function(name, fn):
    assert get_row_policy(name) is fn


def test_row_policy_params_models_are_registered():
    # #2 loader가 이 값으로 config의 row_params를 검증한다.
    assert get_row_policy_params_model("drop_if_issue_count_exceeds") is IssueCountParams
    # None은 "등록됐고 params를 받지 않는다"는 뜻이다 — 미등록(예외)과 구별된다.
    assert get_row_policy_params_model("drop_if_any_required_issue") is None
    assert get_row_policy_params_model("keep_always") is None


def test_keep_null_keeps_missing_as_none(make_issue, ctx):
    # MISSING이면 엔진이 정규화된 None을 넘긴다. 원시값은 issue.raw_value에 있다.
    issue = make_issue(IssueKind.MISSING, raw="")
    assert keep_null(None, issue, {}, ctx) == (None, Action.KEEP)


def test_keep_null_returns_value_unchanged(make_issue, ctx):
    # 값을 바꾸지 않는 것이 이 정책의 본질이다 — 엔진의 repaired 판정에서 제외된다.
    issue = make_issue(IssueKind.OUTLIER, raw="250")
    assert keep_null(250, issue, {}, ctx) == (250, Action.KEEP)


def test_set_null_replaces_value(make_issue, ctx):
    issue = make_issue(IssueKind.OUTLIER, raw="250")
    assert set_null(250, issue, {}, ctx) == (None, Action.KEEP)


def test_fill_zero_fills_missing(make_issue, ctx):
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert fill_zero(None, issue, {}, ctx) == (0, Action.KEEP)


def test_fill_zero_fills_outlier(make_issue, ctx):
    issue = make_issue(IssueKind.OUTLIER, raw="250")
    assert fill_zero(250, issue, {}, ctx) == (0, Action.KEEP)


def test_fill_zero_defends_type_error(make_issue, ctx):
    # 해석 불가한 값에 0을 채우면 안 된다 — set_null과 같은 효과로 빠진다
    issue = make_issue(IssueKind.TYPE_ERROR, raw="31.6xyz")
    assert fill_zero("31.6xyz", issue, {}, ctx) == (None, Action.KEEP)


def test_fill_default_uses_spec_default(make_spec, make_issue, ctx):
    spec = make_spec(default=-1)
    issue = make_issue(IssueKind.MISSING, spec=spec, raw=None)
    assert fill_default(None, issue, {}, ctx) == (-1, Action.KEEP)


def test_fill_default_defends_type_error(make_spec, make_issue, ctx):
    spec = make_spec(default=-1)
    issue = make_issue(IssueKind.TYPE_ERROR, spec=spec, raw="abc")
    assert fill_default("abc", issue, {}, ctx) == (None, Action.KEEP)


def test_fill_default_without_declared_default_returns_none(make_issue, ctx):
    # 로드 시점에 막을지는 #2의 결정이다. #3에서는 set_null과 같은 효과로 둔다
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert fill_default(None, issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_clips_above_max(make_spec, make_issue, ctx):
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="250")
    assert clip_to_range(250, issue, {}, ctx) == (200, Action.KEEP)


def test_clip_to_range_clips_below_min(make_spec, make_issue, ctx):
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="-5")
    assert clip_to_range(-5, issue, {}, ctx) == (0, Action.KEEP)


def test_clip_to_range_defends_type_error(make_spec, make_issue, ctx):
    # 4분면 기본값이 교정형인 소스에서 TYPE_ERROR가 이쪽으로 디스패치된다
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.TYPE_ERROR, spec=spec, raw="31.6xyz")
    assert clip_to_range("31.6xyz", issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_defends_missing(make_spec, make_issue, ctx):
    # `on_missing: clip_to_range`는 YAML에서 합법이다
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.MISSING, spec=spec, raw=None)
    assert clip_to_range(None, issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_defends_missing_range(make_spec, make_issue, ctx):
    # enum만 선언한 컬럼(PTY)에 on_outlier: clip_to_range를 걸었을 때
    spec = make_spec(enum=(0, 1, 2, 3, 5, 6, 7))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="9")
    assert clip_to_range(9, issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_passes_enum_violation_that_is_within_range(make_spec, make_issue, ctx):
    # range와 enum 동시 선언 허용 여부는 #2의 결정이다. 허용되면 enum 위반이면서
    # range 안인 값은 교정되지 않고 통과한다 — #2가 금지해야 할 조합의 표적.
    spec = make_spec(range=(0, 200), enum=(0, 1, 2))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="100")
    assert clip_to_range(100, issue, {}, ctx) == (100, Action.KEEP)


def test_drop_row_returns_drop_action(make_issue, ctx):
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert drop_row(None, issue, {}, ctx) == (None, Action.DROP_ROW)


def test_fail_batch_returns_fail_action(make_issue, ctx):
    issue = make_issue(IssueKind.TYPE_ERROR, raw="junk")
    assert fail_batch("junk", issue, {}, ctx) == ("junk", Action.FAIL_BATCH)


def test_all_row_policies_are_registered():
    assert set(row_policy_names()) >= {
        "drop_if_any_required_issue",
        "drop_if_issue_count_exceeds",
        "keep_always",
    }


def test_drop_if_any_required_issue_drops_on_required(make_spec, make_issue, ctx):
    issues = [make_issue(IssueKind.MISSING, spec=make_spec(required=True), raw=None)]
    assert drop_if_any_required_issue({}, issues, ctx, None) is RowVerdict.DROP


def test_drop_if_any_required_issue_keeps_optional_only(make_issue, ctx):
    issues = [make_issue(IssueKind.OUTLIER, raw="250")]  # spec 기본값 required=False
    assert drop_if_any_required_issue({}, issues, ctx, None) is RowVerdict.KEEP


def test_drop_if_any_required_issue_keeps_clean_row(ctx):
    assert drop_if_any_required_issue({}, [], ctx, None) is RowVerdict.KEEP


def test_drop_if_issue_count_exceeds_drops_above_threshold(make_issue, ctx):
    issues = [make_issue(IssueKind.OUTLIER, raw="1") for _ in range(4)]
    params = IssueCountParams(max_issues=3)
    assert drop_if_issue_count_exceeds({}, issues, ctx, params) is RowVerdict.DROP


def test_drop_if_issue_count_exceeds_keeps_at_threshold(make_issue, ctx):
    # 경계값 — "넘으면" 폐기이므로 같으면 유지다
    issues = [make_issue(IssueKind.OUTLIER, raw="1") for _ in range(3)]
    params = IssueCountParams(max_issues=3)
    assert drop_if_issue_count_exceeds({}, issues, ctx, params) is RowVerdict.KEEP


def test_drop_if_issue_count_exceeds_keeps_below_threshold(make_issue, ctx):
    issues = [make_issue(IssueKind.OUTLIER, raw="1")]
    params = IssueCountParams(max_issues=3)
    assert drop_if_issue_count_exceeds({}, issues, ctx, params) is RowVerdict.KEEP


def test_keep_always_keeps(make_issue, ctx):
    issues = [make_issue(IssueKind.MISSING, raw=None)]
    assert keep_always({}, issues, ctx, None) is RowVerdict.KEEP


def test_issue_count_params_rejects_field_name_typo():
    # #2 loader의 3단계 검증이 `max_issue: 3` 오타를 잡을 수 있는 근거가 이 설정이다
    with pytest.raises(ValidationError):
        IssueCountParams(max_issue=3)


def test_issue_count_params_rejects_negative():
    with pytest.raises(ValidationError):
        IssueCountParams(max_issues=-1)


def test_importing_registry_alone_populates_policies():
    """등록은 import 부수효과다. 패키지 __init__이 그 보장을 맡는다."""
    code = (
        "from validation.registry import policy_names, row_policy_names; "
        "print(','.join(policy_names() + row_policy_names()))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    names = result.stdout.strip().split(",")
    assert "clip_to_range" in names
    assert "keep_always" in names
