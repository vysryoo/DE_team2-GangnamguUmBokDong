"""풍속·풍향을 동서(`UUU`)·남북(`VVV`) 성분으로 분해한다.

실황 API는 `UUU`·`VVV`를 계산해서 주지만 과거 ASOS CSV에는 풍속·풍향만 있어, bootstrap이
같은 공식으로 채워 운영 수집분과 컬럼을 맞춘다. 풍향은 바람이 불어오는 방향이라 성분에
음수가 붙는다(`u = -WSD * sin(VEC)`, `v = -WSD * cos(VEC)`). 실API 대조 표본은
`collector/tests/test_wind.py`에 회귀로 남아 있다.
"""

from __future__ import annotations

import math
from typing import Any


def wind_components(speed: Any, direction: Any) -> tuple[float, float]:
    """풍속·풍향을 (동서 성분, 남북 성분)으로 분해한다.

    args:
        speed: 풍속(m/s). `WSD`에 해당한다.
        direction: 풍향(도, 0~360). `VEC`에 해당하며 바람이 불어오는 방향이다.
    returns:
        `(u, v)` — 각각 `UUU`·`VVV`에 해당하는 m/s 실수.
    raises:
        ValueError: 숫자로 읽을 수 없는 값일 때.
        TypeError: `None`처럼 실수로 다룰 수 없는 값일 때.
    """
    speed_value = float(speed)
    radians = math.radians(float(direction))
    return -speed_value * math.sin(radians), -speed_value * math.cos(radians)
