"""YAML의 정책 이름을 정책 함수로 연결하는 레지스트리.

등록은 데코레이터의 import 부수효과로 이뤄지므로 `validation.policies`가 먼저 import돼야
한다. 정책 이름은 manifest의 `policy_actions`에 영구 기록되므로 함부로 바꾸지 않는다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from validation.types import Action, Issue, RowVerdict, RunContext

if TYPE_CHECKING:
    from pydantic import BaseModel

PolicyFn = Callable[[Any, Issue, dict, RunContext], tuple[Any, Action]]
RowPolicyFn = Callable[[dict, list[Issue], RunContext, Any], RowVerdict]


class UnknownPolicyError(ValueError):
    """등록되지 않은 정책 이름을 조회했다."""


class DuplicatePolicyError(ValueError):
    """같은 이름을 두 번 등록했다. 조용히 덮어쓰면 어느 함수가 돌았는지 알 수 없다."""


@dataclass(frozen=True, slots=True)
class RowPolicyEntry:
    """행 정책 함수와 params 모델. params가 없는 정책은 params_model이 None이다."""

    fn: RowPolicyFn
    params_model: type[BaseModel] | None


_POLICIES: dict[str, PolicyFn] = {}
_ROW_POLICIES: dict[str, RowPolicyEntry] = {}


def policy(name: str) -> Callable[[PolicyFn], PolicyFn]:
    """컬럼 정책을 등록한다. 원본 함수를 그대로 반환한다."""

    def register(fn: PolicyFn) -> PolicyFn:
        if name in _POLICIES:
            raise DuplicatePolicyError(
                f"컬럼 정책 '{name}'이 이미 등록돼 있습니다: {_POLICIES[name].__qualname__}"
            )
        _POLICIES[name] = fn
        return fn

    return register


def row_policy(
    name: str, *, params: type[BaseModel] | None = None
) -> Callable[[RowPolicyFn], RowPolicyFn]:
    """행 정책을 등록한다. 원본 함수를 그대로 반환한다."""

    def register(fn: RowPolicyFn) -> RowPolicyFn:
        if name in _ROW_POLICIES:
            raise DuplicatePolicyError(
                f"행 정책 '{name}'이 이미 등록돼 있습니다: {_ROW_POLICIES[name].fn.__qualname__}"
            )
        _ROW_POLICIES[name] = RowPolicyEntry(fn=fn, params_model=params)
        return fn

    return register


def get_policy(name: str) -> PolicyFn:
    """등록된 컬럼 정책을 이름으로 조회한다."""
    try:
        return _POLICIES[name]
    except KeyError:
        raise UnknownPolicyError(_unknown_message("컬럼 정책", name, policy_names())) from None


def get_row_policy(name: str) -> RowPolicyFn:
    """등록된 행 정책을 이름으로 조회한다."""
    return _row_entry(name).fn


def get_row_policy_params_model(name: str) -> type[BaseModel] | None:
    """행 정책의 params Pydantic 모델을 반환한다. params가 없는 정책이면 None."""
    return _row_entry(name).params_model


def is_policy_registered(name: str) -> bool:
    """컬럼 정책 이름이 등록돼 있는지 본다."""
    return name in _POLICIES


def is_row_policy_registered(name: str) -> bool:
    """행 정책 이름이 등록돼 있는지 본다."""
    return name in _ROW_POLICIES


def policy_names() -> tuple[str, ...]:
    """등록된 컬럼 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_POLICIES))


def row_policy_names() -> tuple[str, ...]:
    """등록된 행 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_ROW_POLICIES))


def _row_entry(name: str) -> RowPolicyEntry:
    """이름으로 행 정책 항목을 조회한다."""
    try:
        return _ROW_POLICIES[name]
    except KeyError:
        raise UnknownPolicyError(_unknown_message("행 정책", name, row_policy_names())) from None


def _unknown_message(kind: str, name: str, registered: tuple[str, ...]) -> str:
    """미등록 이름 조회 시 예외 메시지를 만든다."""
    listed = ", ".join(registered) or "(없음)"
    return f"{kind} '{name}'이 등록돼 있지 않습니다. 등록된 이름: {listed}"
