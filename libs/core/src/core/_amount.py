"""기상청 강수·적설 범주 표기를 실수로 바꾸는 공통 규칙.

`PCP`(mm)와 `SNO`(cm)는 표기 형태가 같고 단위와 없음 레이블만 다르다. 다른 단위 표기나
레이블을 받으면 거부한다. `"1.0mm 미만"`의 대표값 0.5를 cm 컬럼에 쓰는 식으로 단위가
섞여도 값만으로는 알아챌 수 없기 때문이다.
"""

from __future__ import annotations

from typing import Any

# "1.0mm 미만" / "1.0cm 미만"의 실제 구간은 0.1~1.0(해당 단위)이다. 그 대표값.
BELOW_THRESHOLD = 0.5

# 자기 단위가 아닌 것이 값에 보이면 잘못된 파서에 넘어온 것이므로 거부한다.
_KNOWN_UNITS = ("mm", "cm")


def parse_amount(value: Any, *, unit: str, none_label: str) -> float:
    """범주 표기를 `unit` 단위 실수로 바꾼다.

    범위형은 하한을 취한다. 상한이 없는 `"50.0mm 이상"`은 평균을 정의할 수 없어 같은
    하한 규칙을 쓴다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
        unit: 이 표기의 단위 문자열(`"mm"` 또는 `"cm"`). 값에서 제거한다.
        none_label: "없음"을 뜻하는 레이블(`"강수없음"` 또는 `"적설없음"`).
    returns:
        `unit` 단위 실수.
    raises:
        ValueError: 어느 규칙에도 해당하지 않아 숫자로 읽을 수 없을 때. 다른 단위의
            표기를 넘긴 경우도 여기 걸린다.
        TypeError: `value`가 `None`처럼 문자열로 다룰 수 없는 값일 때.
    """
    if value is None:
        raise TypeError(f"{none_label} 계열 값이 None이다")

    text = str(value).strip()
    if text == none_label:
        return 0.0

    # "1.0cm 미만"처럼 다른 단위의 표기는 아래 "미만" 분기가 삼켜버리므로 먼저 막는다.
    for foreign in (u for u in _KNOWN_UNITS if u != unit):
        if foreign in text:
            raise ValueError(f"{unit} 파서에 {foreign} 표기가 들어왔다: {value!r}")

    if "미만" in text:
        return BELOW_THRESHOLD

    # "50.0mm 이상" → "50.0", "30.0~50.0mm" → "30.0~50.0"
    text = text.replace(unit, "").replace("이상", "").strip()
    if "~" in text:
        text = text.split("~", 1)[0].strip()
    return float(text)
