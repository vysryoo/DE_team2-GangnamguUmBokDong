"""값 이슈 발견 시 처리 방식을 정의하는 공통 정책 함수 모음.

소스 지식 없이 동작하며, 소스별 차이는 YAML 설정(4분면 기본값, 컬럼별 오버라이드)으로만 제어한다.
컬럼 정책 7종·행 정책 3종이며, 모두 로그·S3 접근이 없는 순수 함수다.

## 컬럼 정책 7종: `(value, issue, row, ctx) -> tuple[Any, Action]`

| 이름 | 반환값 | repaired | 방어 가드 |
| --- | --- | --- | --- |
| `keep_null` | 원래 값 그대로 | 아니오 | — |
| `set_null` | None | 예 | — |
| `fill_zero` | 0 | 예 | `TYPE_ERROR` 제외 |
| `fill_default` | spec에 선언된 기본값 | 예 | `TYPE_ERROR` 제외 |
| `clip_to_range` | 정상 범위의 경계로 자른 값 | 예 | `OUTLIER`·`range` 선언 컬럼만 |
| `drop_row` | — (행 폐기) | — | — |
| `fail_batch` | — (배치 실패) | — | — |

`keep_null`과 `set_null`은 값을 바꾸는지 여부로 구분한다. 값을 바꾸지 않는 `keep_null`은
`repaired`가 아니고, None으로 교체하는 `set_null`은 `repaired`다. 이 기준을 엔진의
`_row_status` 판정과 맞춘다.

`fail_batch`는 최후 수단이다. 4분면 기본값에 걸면 한 행 때문에 배치 전체가 죽는다.
스키마 계약 위반처럼 데이터 전체를 의심할 때만 쓴다.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from validation.registry import policy, row_policy
from validation.types import Action, Issue, IssueKind, RowVerdict, RunContext


@policy("keep_null")
def keep_null(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 바꾸지 않고 유지한다."""
    return value, Action.KEEP


@policy("set_null")
def set_null(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 None으로 교체한다."""
    return None, Action.KEEP


@policy("fill_zero")
def fill_zero(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 0으로 채운다. 타입 에러에는 적용하지 않고 None으로 대신한다."""
    if issue.kind is IssueKind.TYPE_ERROR:
        return None, Action.KEEP
    return 0, Action.KEEP


@policy("fill_default")
def fill_default(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """spec에 선언된 기본값으로 채운다.

    `default`를 선언하지 않은 컬럼에서는 None이 되어 `set_null`과 같은 효과다.
    """
    if issue.kind is IssueKind.TYPE_ERROR:
        return None, Action.KEEP
    return issue.spec.default, Action.KEEP


@policy("clip_to_range")
def clip_to_range(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 정상 범위의 경계로 잘라낸다. 적용 조건이 안 맞으면 None으로 대신한다."""
    bounds = issue.spec.range
    if issue.kind is not IssueKind.OUTLIER or bounds is None:
        return None, Action.KEEP
    return min(max(value, bounds.min), bounds.max), Action.KEEP


@policy("drop_row")
def drop_row(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """이 행을 silver에서 제외하고 quarantine으로 보낸다."""
    return value, Action.DROP_ROW


@policy("fail_batch")
def fail_batch(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """배치 전체를 실패로 만든다. 스키마 계약 위반처럼 데이터 전체를 의심할 때만 쓴다."""
    return value, Action.FAIL_BATCH


class IssueCountParams(BaseModel):
    """drop_if_issue_count_exceeds의 인자."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    max_issues: int = Field(ge=0)


@row_policy("drop_if_any_required_issue")
def drop_if_any_required_issue(
    row: dict, issues: list[Issue], ctx: RunContext, params: None
) -> RowVerdict:
    """필수 컬럼에 문제가 하나라도 있으면 행을 폐기한다."""
    return RowVerdict.DROP if any(issue.required for issue in issues) else RowVerdict.KEEP


@row_policy("drop_if_issue_count_exceeds", params=IssueCountParams)
def drop_if_issue_count_exceeds(
    row: dict, issues: list[Issue], ctx: RunContext, params: IssueCountParams
) -> RowVerdict:
    """이슈 개수가 임계값을 넘으면 행을 폐기한다."""
    return RowVerdict.DROP if len(issues) > params.max_issues else RowVerdict.KEEP


@row_policy("keep_always")
def keep_always(row: dict, issues: list[Issue], ctx: RunContext, params: None) -> RowVerdict:
    """항상 유지한다."""
    return RowVerdict.KEEP
