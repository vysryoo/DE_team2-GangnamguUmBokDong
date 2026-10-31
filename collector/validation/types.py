"""검증 엔진·정책·레지스트리가 함께 쓰는 타입."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config.schema import ColumnSpec


class Action(Enum):
    """컬럼 정책이 엔진에 돌려주는 처리 지시."""

    KEEP = "keep"
    DROP_ROW = "drop_row"
    FAIL_BATCH = "fail_batch"


class IssueKind(Enum):
    """판정 이슈의 종류."""

    MISSING = "missing"
    TYPE_ERROR = "type_error"
    OUTLIER = "outlier"


class RowVerdict(Enum):
    """행 정책의 최종 판정."""

    KEEP = "keep"
    DROP = "drop"


@dataclass(frozen=True, slots=True)
class Issue:
    """컬럼 하나의 판정 이슈. 정책 함수에 넘긴다.

    엔진이 함께 넘기는 value는 MISSING이면 None, TYPE_ERROR면 원시값, OUTLIER면
    캐스팅된 값이다.
    """

    column: str
    kind: IssueKind
    required: bool
    raw_value: Any  # 캐스팅 전 원시값 — quarantine에 남길 때 필요하다
    spec: ColumnSpec


@dataclass(frozen=True, slots=True)
class RunContext:
    """정책이 참조할 수 있는 실행 맥락."""

    source_id: str
    window_start: datetime
    window_end: datetime
    attempt: int
