"""기상청 적설 범주 표기를 cm 실수로 바꾼다.

단기예보 `SNO`는 `PCP`와 표기 형태가 같지만 단위가 cm라 함수를 나눈다. 형태 규칙은
`core._amount.parse_amount`가 공유한다. collector 캐스터로 쓰이므로 해석 실패는 `None`이
아니라 예외다.
"""

from __future__ import annotations

from typing import Any

from core._amount import parse_amount


def parse_snow(value: Any) -> float:
    """적설 표기를 cm 단위 실수로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        cm 단위 실수.
    raises:
        ValueError: 어느 규칙에도 해당하지 않을 때. 강수 표기(`"강수없음"`, `"2.0mm"`)를
            넘긴 경우도 여기 걸린다.
        TypeError: `value`가 `None`처럼 문자열로 다룰 수 없는 값일 때.
    """
    return parse_amount(value, unit="cm", none_label="적설없음")
