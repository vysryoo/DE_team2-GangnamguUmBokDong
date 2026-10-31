"""`sources/*.yaml` 수집 설정의 Pydantic 스키마."""

from __future__ import annotations

import math
import re
from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

# 예: 1d, 12h, 5m, 30s, 1d12h, 2h30m, 1d2h3m4s
_DURATION_RE = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def _parse_duration(value: object) -> timedelta:
    """`1d12h`, `5m` 같은 duration 문자열을 timedelta로 변환한다."""

    if isinstance(value, timedelta):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"duration은 비어있지 않은 문자열이어야 합니다: {value!r}")

    match = _DURATION_RE.fullmatch(value)
    # 모든 단위가 생략 가능해 빈 매치도 fullmatch를 통과한다.
    if match is None or not any(match.groups()):
        raise ValueError(f"duration 형식이 틀렸습니다.: {value!r}")
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


Duration = Annotated[timedelta, BeforeValidator(_parse_duration)]


class Range(BaseModel):
    """컬럼의 정상 범위이다. 한쪽 경계만 선언한 열린 범위도 허용한다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float = -math.inf
    max: float = math.inf

    @model_validator(mode="after")
    def _min_le_max(self) -> Range:
        """경계가 하나도 없거나 min이 max보다 큰 range를 막는다."""

        if not self.model_fields_set.intersection({"min", "max"}):
            raise ValueError("range는 min 또는 max 중 하나 이상을 선언해야 합니다.")
        if self.min > self.max:
            raise ValueError(
                f"range.min({self.min})이 range.max({self.max})보다 클 수 없습니다."
            )
        return self


class ColumnSpec(BaseModel):
    """컬럼 하나의 스펙. range와 enum은 배타적으로만 사용할 수 있다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # precip·snow·masked_float 캐스터는 `validation.engine._CASTERS` 참고.
    types: tuple[
        Literal["str", "int", "float", "bool", "precip", "snow", "masked_float"], ...
    ] = Field(min_length=1)
    required: bool = False
    range: Range | None = None
    enum: tuple[Any, ...] | None = None
    on_missing: str | None = None
    on_outlier: str | None = None
    default: Any = None

    @model_validator(mode="after")
    def _range_xor_enum(self) -> ColumnSpec:
        """range와 enum을 동시에 선언한 spec을 막는다."""
        if self.range is not None and self.enum is not None:
            raise ValueError("range와 enum을 동시에 선언할 수 없습니다.")
        return self


class Schedule(BaseModel):
    """수집 주기."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    interval: Duration


class Storage(BaseModel):
    """bronze·silver 저장 형식과 파티션 키."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bronze_format: str
    silver_format: str
    partition: tuple[str, ...] = Field(min_length=1)


class Compaction(BaseModel):
    """하루치 silver를 archive로 묶을 때의 소스별 옵션."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # window마다 같은 구간을 다시 받는 소스(bike_rental_history)에서만 켠다. 스냅샷
    # 소스는 연속 window가 같은 값을 내는 것이 정상이라 지우면 시계열이 무너진다.
    dedup: bool = False


class Quality(BaseModel):
    """배치를 버릴지 판단하는 완결성 기준."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_drop_ratio: float = Field(ge=0, le=1)
    max_missing_ratio: float = Field(default=0.0, ge=0, le=1)
    allow_empty: bool = False

    # 소스가 알려준 전체 건수보다 실제 받은 행이 많을 때 용인할 비율. 진행 중인
    # window는 카운트 계산과 직렬화 사이에 레코드가 들어와 total을 넘을 수 있다
    # (대여이력 rows=989 expected=988). 막아야 할 페이지 중복 병합은 최소 +100%라
    # 0.1로도 걸린다. 10% 안쪽의 snapshot 혼합은 놓치는 것을 감수한다.
    max_overfetch_ratio: float = Field(default=0.1, ge=0, le=1)


class Fetch(BaseModel):
    """window 하나의 fetch 시간 예산. 생략하면 `SourceConfig.effective_fetch_budget` 기본값."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    budget: Duration | None = None


class Policies(BaseModel):
    """컬럼 정책 4분면 기본값과 선택적 행 정책."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    required_missing: str
    required_outlier: str
    optional_missing: str
    optional_outlier: str
    row: str | None = None
    row_params: dict[str, Any] | None = None


class SourceConfig(BaseModel):
    """소스 하나의 수집 설정.

    source_id: 소스 ID
    description: 소스에 대한 설명
    adapter: 어댑터 이름
    adapter_params: 어댑터 파라미터
    schedule: 주기
    storage: 저장 방식
    quality: 품질 기준
    fetch: fetch 시간 예산
    compaction: 하루치 압축 옵션
    policies: 컬럼 정책과 행 정책
    columns: 데이터 스키마(key=컬럼명, value=컬럼 스펙)
    natural_key: 원천 snapshot에서 행별 identity를 이루는 컬럼
    config_version: 설정 버전
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    description: str
    adapter: str
    adapter_params: dict[str, Any] = {}
    schedule: Schedule
    storage: Storage
    quality: Quality
    fetch: Fetch | None = None
    compaction: Compaction | None = None
    policies: Policies
    columns: dict[str, ColumnSpec]
    natural_key: tuple[str, ...] | None = Field(default=None, min_length=1)
    config_version: str = ""

    def effective_fetch_budget(self) -> timedelta:
        """fetch.budget이 없으면 주기의 절반과 30분 중 작은 값을 기본 예산으로 쓴다."""

        if self.fetch and self.fetch.budget:
            return self.fetch.budget
        return min(self.schedule.interval / 2, timedelta(minutes=30))
