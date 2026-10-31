"""bootstrap 매핑 설정 스키마와 로더.

한 번 쓰고 버리는 설정이라 오래 유지되는 운영 yaml(`collector/sources/*.yaml`)과 파일을
나눈다. 검증에 쓰는 `columns`·`policies`는 운영 yaml에서 `config.loader.load()`로 가져온다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


class WindowSpec(BaseModel):
    """행이 속한 시간대를 어느 컬럼에서 어떻게 읽을지."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_column: str
    format: str


class DerivedTimeSpec(BaseModel):
    """시각 컬럼 하나를 여러 물리 컬럼으로 분해하는 규칙(1 -> N).

    ASOS의 `일시` 하나를 `baseDate`·`baseTime`으로 나누는 경우에 쓴다. 설정 파일이 코드가
    되지 않도록 `parse`로 읽어 `into` 형식으로 다시 찍는 strftime 재포맷만 허용한다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    parse: str
    into: dict[str, str] = Field(min_length=1)


class ComposedTimeSpec(BaseModel):
    """여러 시각 컬럼을 물리 컬럼 하나로 합치는 규칙(N -> 1).

    재고 CSV의 `일시`·`시간대`를 `stationDt` 하나로 합치는 경우에 쓴다. `from` 값을 공백으로
    이어 `parse`로 읽고 `format`으로 다시 찍는 strftime 재포맷만 허용한다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    # `from`은 파이썬 예약어라 필드명을 `from_`으로 두고 yaml 키를 alias로 받는다.
    from_: tuple[str, ...] = Field(alias="from", min_length=1)
    parse: str
    format: str


class JoinKeySpec(BaseModel):
    """매핑표를 찾을 때 쓰는 물리 컬럼들. 번호로 먼저 찾고 없으면 이름으로 찾는다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    number: str
    name: str


class JoinSpec(BaseModel):
    """CSV에 없는 `stationId`·거치대 수·좌표를 외부 매핑표(`station_join`)에서 채우는 규칙.

    오타가 나면 조인이 조용히 전부 실패해 행이 전량 폐기되므로 `provider`와 `fills`를
    Literal로 막아 설정 로딩에서 끊는다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["bike_station"]
    by: JoinKeySpec
    fills: tuple[
        Literal[
            "stationId",
            "rackTotCnt",
            "shared",
            "stationLatitude",
            "stationLongitude",
        ],
        ...,
    ] = Field(min_length=1)


class DerivedWindSpec(BaseModel):
    """풍속·풍향 물리 컬럼에서 동서(`u`)·남북(`v`) 성분 컬럼을 계산하는 규칙.

    운영 수집은 두 성분을 주지만 과거 ASOS CSV에는 없어 `core.wind`의 같은 공식으로 채운다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    speed: str
    direction: str
    u: str
    v: str


class BootstrapConfig(BaseModel):
    """소스 하나의 초기 로드 설정. `kind`별 필수 필드는 validator가 가른다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["csv", "history_api"]
    window: WindowSpec
    dedup: bool = False

    # kind: csv
    encoding: str = "utf-8"
    # 한 디렉터리에 다른 소스 CSV가 섞이면 그 파일을 열어 시각 파싱에서 실패하므로 거름
    file_pattern: str = "*.csv"
    na_values: tuple[str, ...] = ()
    column_map: dict[str, str] = {}
    value_map: dict[str, dict[str, str]] = {}
    # 단일 관측 지점 CSV에 없는 격자 좌표 등을 고정값으로 채움. 캐스팅은 검증 엔진이
    # 맡으므로 문자열로 둠
    constants: dict[str, str] = {}
    # 시각 컬럼 하나를 여러 물리 컬럼으로 분해한다(키는 CSV 헤더).
    derived_time: dict[str, DerivedTimeSpec] = {}
    # 여러 시각 컬럼을 물리 컬럼 하나로 합친다(키는 물리 컬럼명).
    composed_time: dict[str, ComposedTimeSpec] = {}
    # CSV에 없는 컬럼을 외부 매핑표에서 채운다.
    join: JoinSpec | None = None
    # 풍속·풍향에서 UUU·VVV를 계산한다.
    derived_wind: DerivedWindSpec | None = None

    # kind: history_api
    service: str | None = None
    time_format: str | None = None
    page_size: int = 1000

    @model_validator(mode="after")
    def _require_kind_fields(self) -> BootstrapConfig:
        if self.kind == "csv" and not self.column_map:
            raise ValueError("kind=csv면 column_map이 필수다")
        if self.kind == "history_api":
            if not self.service:
                raise ValueError("kind=history_api면 service가 필수다")
            if not self.time_format:
                raise ValueError("kind=history_api면 time_format이 필수다")
        return self

    @model_validator(mode="after")
    def _no_duplicate_targets(self) -> BootstrapConfig:
        """어느 값이 남는지가 적용 순서에 달리지 않도록 한 물리 컬럼을 두 경로가 채우는 설정을 막는다."""
        seen: dict[str, str] = {}
        sources = [
            ("column_map", self.column_map.values()),
            ("constants", self.constants.keys()),
            *(
                (f"derived_time[{header}]", spec.into.keys())
                for header, spec in self.derived_time.items()
            ),
            *(
                [("derived_wind", (self.derived_wind.u, self.derived_wind.v))]
                if self.derived_wind is not None
                else []
            ),
            ("composed_time", self.composed_time.keys()),
            *([("join", self.join.fills)] if self.join is not None else []),
        ]
        for origin, targets in sources:
            for target in targets:
                if target in seen:
                    raise ValueError(
                        f"물리 컬럼 '{target}'을 {seen[target]}과 {origin}이 함께 채운다"
                    )
                seen[target] = origin
        return self


def load(source_id: str) -> BootstrapConfig:
    """해당 소스의 bootstrap 설정을 읽는다.

    args:
        source_id: 소스 id. `mappings/{source_id}.yaml`을 찾는다.
    returns:
        검증된 설정
    raises:
        FileNotFoundError: 그 소스의 매핑 파일이 없을 때. 초기 로드 대상이 아니라는 뜻이다.
    """
    path = _MAPPINGS_DIR / f"{source_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"bootstrap 매핑 설정이 없다: {path}")
    return BootstrapConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
