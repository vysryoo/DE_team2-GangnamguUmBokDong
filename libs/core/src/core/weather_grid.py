"""기상청 5km 격자(nx, ny)와 WGS84 위경도를 상호 변환한다.

람베르트 등각원추투영(LCC) 공식과 계수는 기상청 단기예보 API 가이드 기준이다.
loader(gold `stations`/`weather_*` 적재), normalizer(보강 station master),
`loader/scripts/generate_weather_grids.py`(수집 격자 목록 생성)가 모두 이 모듈을
쓴다 — 계수를 여러 곳에 복사해 두면 한쪽만 고쳐졌을 때 조용히 어긋난다.
"""

from __future__ import annotations

import math

# 기상청 5km 격자(nx, ny) <-> 위경도 변환을 위한 람베르트 등각원추투영 공식 계수
_RE = 6371.00877  # 지구 반경(km)
_GRID = 5.0  # 격자 간격(km)
_SLAT1 = 30.0  # 투영 표준위도1(degree)
_SLAT2 = 60.0  # 투영 표준위도2(degree)
_OLON = 126.0  # 기준점 경도(degree)
_OLAT = 38.0  # 기준점 위도(degree)
_XO = 43  # 기준점 X 격자좌표
_YO = 136  # 기준점 Y 격자좌표

_DEGRAD = math.pi / 180.0


def _projection_constants() -> tuple[float, float, float, float]:
    """두 변환이 공유하는 투영 상수 (re, sn, sf, ro)를 계산한다. import 때 한 번만 호출한다."""
    re = _RE / _GRID
    slat1 = _SLAT1 * _DEGRAD
    slat2 = _SLAT2 * _DEGRAD
    olat = _OLAT * _DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = sf**sn * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / ro**sn
    return re, sn, sf, ro


_RE_GRID, _SN, _SF, _RO = _projection_constants()
_OLON_RAD = _OLON * _DEGRAD


def grid_to_latlon(nx: float, ny: float) -> tuple[float, float]:
    """기상청 격자 좌표(nx, ny)를 WGS84 위경도 좌표(lat, lon)로 변환한다.

    args:
        nx: 기상청 X 격자 좌표
        ny: 기상청 Y 격자 좌표
    returns:
        (lat, lon) 위경도 좌표 튜플
    """
    xn = nx - _XO
    yn = _RO - (ny - _YO)
    ra = math.hypot(xn, yn)
    if _SN < 0.0:
        ra = -ra
    alat = (_RE_GRID * _SF / ra) ** (1.0 / _SN)
    alat = 2.0 * math.atan(alat) - math.pi * 0.5
    theta = math.atan2(xn, yn) if (xn, yn) != (0.0, 0.0) else 0.0
    alon = theta / _SN + _OLON_RAD

    return math.degrees(alat), math.degrees(alon)


def latlon_to_grid(lat: float, lon: float) -> tuple[int, int]:
    """WGS84 위경도 좌표를 가장 가까운 기상청 격자 좌표(nx, ny)로 변환한다.

    `grid_to_latlon`의 정확한 역변환이다(같은 람베르트 등각원추투영 계수를 쓴다).

    args:
        lat: 위도 (WGS84)
        lon: 경도 (WGS84)
    returns:
        가장 가까운 격자의 (nx, ny) 정수 좌표
    """
    ra = math.tan(math.pi * 0.25 + (lat * _DEGRAD) * 0.5)
    ra = _RE_GRID * _SF / ra**_SN
    theta = lon * _DEGRAD - _OLON_RAD
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= _SN

    xn = ra * math.sin(theta)
    yn = ra * math.cos(theta)
    nx = xn + _XO
    ny = _YO + _RO - yn
    return round(nx), round(ny)
