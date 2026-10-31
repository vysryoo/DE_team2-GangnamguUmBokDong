"""재고 CSV의 `대여소번호`를 collector 스키마의 대여소 컬럼들로 옮기는 매핑표.

재고 CSV는 `00102` 같은 번호만 주지만 `bike_station_realtime`은 `stationId`를 required로
요구한다. `bikeList` API의 이름 완전일치, 이름 접두 번호, 대여이력 CSV의 번호↔ID 순으로
겹쳐 채운다. 대여이력에서만 찾은 폐쇄 대여소는 `station_id`만 채우고 나머지는 비운다.
`bikeStationMaster`는 `ADDR2`가 상세주소라 번호로 조인할 수 없다.

`bikeList`는 현재 상태를 주므로 거치대 수·거치율은 매핑표를 만든 시각의 값이다. 출처를
되짚을 수 있게 `stats`를 archive manifest에 싣는다.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx
from core.kst import KST

from bootstrap.globbing import glob_normalized

logger = logging.getLogger(__name__)

_BASE_URL = "http://openapi.seoul.go.kr:8088"
_SERVICE = "bikeList"
_PAGE_SIZE = 1000
# 페이지를 무한히 도는 것을 막는 상한
_MAX_PAGES = 20


# 대여소명 앞에 붙는 번호. `"102. 망원역 1번출구 앞"` -> `102`.
_NAME_PREFIX = re.compile(r"^\s*(\d+)\s*\.")

# 대여이력 CSV만 고른다. `--csv-dir`에 재고 CSV·기상 CSV가 함께 있을 수 있다.
_HISTORY_PATTERN = "서울특별시 공공자전거 대여이력 정보_*.csv"
_HISTORY_ENCODING = "cp949"
# (대여소번호 컬럼, 대여소ID 컬럼) 쌍. 한 행이 대여·반납 두 대여소를 담는다.
_HISTORY_COLUMN_PAIRS = (("대여 대여소번호", "대여대여소ID"), ("반납대여소번호", "반납대여소ID"))
_MAX_CONFLICT_SAMPLE = 5


@dataclass(frozen=True)
class StationInfo:
    """대여소 하나에 대해 매핑표가 아는 값들.

    캐스팅은 검증 엔진이 맡으므로 전부 문자열이고, 모르는 값은 결측으로 판정될 빈 문자열이다.
    """

    station_id: str
    rack_tot_cnt: str = ""
    shared: str = ""
    latitude: str = ""
    longitude: str = ""


@dataclass(frozen=True)
class StationMap:
    """번호·이름 어느 쪽으로도 찾을 수 있는 매핑표."""

    by_name: dict[str, StationInfo] = field(default_factory=dict)
    by_number: dict[str, StationInfo] = field(default_factory=dict)
    stats: dict = field(default_factory=dict)

    def lookup(self, number: str, name: str) -> StationInfo | None:
        """대여소 하나를 찾는다. 이름 완전일치를 먼저 보고 없으면 번호로 본다.

        이름을 먼저 보는 이유는 그쪽이 5개 컬럼을 모두 채우는 API 출처이기 때문이다.
        번호 사전에는 API 접두 매칭과 대여이력이 함께 들어 있고, 겹치면 API가 이긴다.

        args:
            number: CSV의 `대여소번호`. 5자리 zero-padded를 기대한다.
            name: CSV의 `대여소명`
        returns:
            찾은 대여소 정보. 어느 쪽으로도 못 찾으면 None.
        """
        return self.by_name.get(name) or self.by_number.get(_pad(number))


def _pad(number: str) -> str:
    """대여소번호를 CSV와 같은 5자리 zero-padded 표기로 맞춘다."""
    return number.strip().zfill(5)


def _api_key() -> str:
    return os.environ["SEOUL_OPENAPI_KEY"]


def _fetch_stations(client: httpx.Client) -> list[dict]:
    """`bikeList`를 페이지 끝까지 받아 행을 모은다.

    이 서비스의 `list_total_count`는 전체 건수가 아니라 그 페이지의 행 수라 끝 판단에 쓸 수
    없다. 페이지가 가득 차지 않으면 마지막 페이지로 본다.
    """
    rows: list[dict] = []
    for page in range(_MAX_PAGES):
        start = page * _PAGE_SIZE + 1
        url = f"{_BASE_URL}/{_api_key()}/json/{_SERVICE}/{start}/{start + _PAGE_SIZE - 1}/"
        response = client.get(url)
        response.raise_for_status()
        wrapper = json.loads(response.content).get("rentBikeStatus", {})
        page_rows = wrapper.get("row") or []
        rows.extend(page_rows)
        if len(page_rows) < _PAGE_SIZE:
            break
    return rows


def _info_from_api(row: dict) -> StationInfo:
    """`bikeList` 응답 행 하나를 매핑표 항목으로 바꾼다."""
    return StationInfo(
        station_id=str(row.get("stationId") or ""),
        rack_tot_cnt=str(row.get("rackTotCnt") or ""),
        shared=str(row.get("shared") or ""),
        latitude=str(row.get("stationLatitude") or ""),
        longitude=str(row.get("stationLongitude") or ""),
    )


def _read_history_ids(csv_dir: Path) -> tuple[dict[str, str], list[str]]:
    """대여이력 CSV들에서 `대여소번호 -> 대여소ID` 매핑을 모으고 충돌을 함께 기록한다.

    대여소번호는 폐기 후 다른 대여소에 재사용될 수 있다. 파일명순(시간순)으로 읽어 최신
    기간의 id를 채택하되, 덮어쓴 사실을 기간과 함께 남긴다.

    returns:
        (매핑, 충돌 설명 목록). 충돌 설명은 `"00102: ST-4(2412) -> ST-999(2501)"`
        형식이고 대여소번호순으로 정렬된다(결정적).
    """
    mapping: dict[str, str] = {}
    origin: dict[str, str] = {}
    trail: dict[str, list[str]] = {}
    for path in glob_normalized(csv_dir, _HISTORY_PATTERN):
        period = path.stem.rsplit("_", 1)[-1]
        with path.open(encoding=_HISTORY_ENCODING, errors="replace", newline="") as handle:
            for row in csv.DictReader(handle):
                for number_column, id_column in _HISTORY_COLUMN_PAIRS:
                    number, station_id = row.get(number_column), row.get(id_column)
                    if not (number and station_id):
                        continue
                    padded = _pad(number)
                    previous = mapping.get(padded)
                    if previous == station_id:
                        continue
                    if previous is not None:
                        trail.setdefault(padded, [f"{previous}({origin[padded]})"]).append(
                            f"{station_id}({period})"
                        )
                    mapping[padded] = station_id
                    origin[padded] = period
    conflicts = [
        f"{number}: {' -> '.join(steps)}" for number, steps in sorted(trail.items())
    ]
    return mapping, conflicts


def build(csv_dir: Path | None, *, client: httpx.Client | None = None) -> StationMap:
    """대여소 매핑표를 만든다.

    args:
        csv_dir: 대여이력 CSV를 찾을 디렉터리. None이면 대여이력 단계를 건너뛴다.
        client: 주입용 httpx 클라이언트. 생략하면 새로 만든다.
    returns:
        번호·이름으로 조회 가능한 매핑표. `stats`에 출처별 건수와 생성 시각이 담긴다.
    """
    owned = client is None
    client = client or httpx.Client(timeout=30.0)
    try:
        api_rows = _fetch_stations(client)
    finally:
        if owned:
            client.close()

    by_name, by_number = _index_api_rows(api_rows)
    history, conflicts = _read_history_ids(csv_dir) if csv_dir is not None else ({}, [])
    for number, station_id in history.items():
        # API가 이긴다 — 그쪽만 5개 컬럼을 모두 채운다.
        by_number.setdefault(number, StationInfo(station_id=station_id))

    stats = {
        "built_at": datetime.now(tz=KST).isoformat(),
        "api_stations": len(api_rows),
        "history_stations": len(history),
        "history_id_conflicts": len(conflicts),
        "history_id_conflict_sample": conflicts[:_MAX_CONFLICT_SAMPLE],
    }
    station_map = StationMap(by_name=by_name, by_number=by_number, stats=stats)
    _log_build(station_map, conflicts)
    return station_map


def _index_api_rows(
    api_rows: list[dict],
) -> tuple[dict[str, StationInfo], dict[str, StationInfo]]:
    """API 대여소를 이름과 이름 앞의 대여소번호로 색인한다."""
    by_name: dict[str, StationInfo] = {}
    by_number: dict[str, StationInfo] = {}
    for row in api_rows:
        name = str(row.get("stationName") or "")
        info = _info_from_api(row)
        if name:
            by_name[name] = info
        prefix = _NAME_PREFIX.match(name)
        if prefix:
            by_number[_pad(prefix.group(1))] = info
    return by_name, by_number


def _log_build(station_map: StationMap, conflicts: list[str]) -> None:
    """매핑표 출처별 건수와 대여소번호 충돌을 로그로 남긴다."""
    stats = station_map.stats
    logger.info(
        f"stage=bootstrap_station_join api_stations={stats['api_stations']} "
        f"history_stations={stats['history_stations']} by_name={len(station_map.by_name)} "
        f"by_number={len(station_map.by_number)} "
        f"history_id_conflicts={stats['history_id_conflicts']}"
    )
    if conflicts:
        logger.warning(
            f"stage=bootstrap_station_join 대여소번호가 기간에 따라 다른 station_id를 가리킴 "
            f"count={len(conflicts)} sample={stats['history_id_conflict_sample']} "
            "— 최신 기간의 id를 채택했다"
        )
