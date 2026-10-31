"""서울 생활인구의 비식별 마스킹 표기(`*`)를 결측으로 판정시키는 캐스터.

`living_population_grid`는 소수 인구를 `*`로 가려서 보낸다. `float`로 캐스팅하면
TYPE_ERROR가 되어 `column_issues`에서 진짜 형식 오류와 구분되지 않고 `optional_outlier`
정책을 타므로, `MaskedValue`를 던져 검증 엔진이 결측(MISSING)으로 판정하게 한다.
"""

from __future__ import annotations

from typing import Any

# 서울 열린데이터광장이 소수 인구를 가릴 때 쓰는 표기.
_MASK_MARKER = "*"


class MaskedValue(Exception):
    """비식별 마스킹된 값. 검증 엔진이 결측(MISSING)으로 판정한다.

    `_try_cast`가 `ValueError`/`TypeError`를 삼키므로 `Exception`을 직접 상속한다.
    """


def parse_masked_float(value: Any) -> float:
    """마스킹 표기를 결측으로 넘기고, 나머지는 float으로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        float 값.
    raises:
        MaskedValue: 값이 마스킹 표기(`*`)일 때. 결측으로 판정된다.
        ValueError: 마스킹도 아니고 숫자로도 읽을 수 없을 때. 진짜 형식 오류이므로
            TYPE_ERROR로 남아야 한다.
        TypeError: `value`가 `None`처럼 float으로 다룰 수 없는 값일 때.
    """
    if isinstance(value, str) and value.strip() == _MASK_MARKER:
        raise MaskedValue("비식별 마스킹된 값이다")
    return float(value)
