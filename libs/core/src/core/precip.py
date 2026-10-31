"""기상청 강수량 범주 표기를 mm 실수로 바꾼다.

단기예보 `PCP`·초단기예보 `RN1`은 `"강수없음"`, `"1.0mm 미만"`, `"30.0~50.0mm"`,
`"50.0mm 이상"` 같은 범주 문자열을 준다. collector Silver와 loader RDB가 같은 값을 내야
하므로 규칙을 `core`에 둔다. 적설(`SNO`)은 단위가 cm라 `core.snow`가 따로 처리한다.
collector 캐스터로 쓰이므로 해석 실패는 `None`이 아니라 예외다.
"""

from __future__ import annotations

from typing import Any

from core._amount import parse_amount


def parse_precip(value: Any) -> float:
    """강수량 표기를 mm 실수로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        mm 단위 실수.
    raises:
        ValueError: 어느 규칙에도 해당하지 않아 숫자로 읽을 수 없을 때. 적설 표기
            (`"적설없음"`, `"5.0cm"`)를 넘긴 경우도 여기 걸린다.
        TypeError: `value`가 `None`처럼 문자열로 다룰 수 없는 값일 때.
    """
    return parse_amount(value, unit="mm", none_label="강수없음")
