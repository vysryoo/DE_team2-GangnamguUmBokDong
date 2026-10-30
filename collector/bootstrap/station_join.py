"""재고 CSV의 `대여소번호`를 collector 스키마의 대여소 컬럼들로 옮기는 매핑표.

## 왜 필요한가

재고 CSV(`대여소별 공공자전거 대여가능 수량`)는 대여소를 `00102` 형식의 번호로
가리키는데 `bike_station_realtime`은 `stationId`(`ST-4`)를 **required**로 요구한다.
거치대 수·거치율·좌표도 CSV에 없다. 세 소스를 겹쳐 그 값들을 만든다.

| 단계 | 소스 | 채우는 값 | 실측 매칭 |
|---|---|---|---|
| 1 | `bikeList` API — `stationName` 완전일치 | 5개 전부 | 2,711 |
| 2 | `bikeList` API — 이름 접두 번호(`"102. ..."` -> `00102`) | 5개 전부 | 8 |
| 3 | 대여이력 CSV — `대여소번호`↔`대여대여소ID` | `station_id`만 | 61 |

행 207만 기준 미매칭이 API 단독 2.79%, 대여이력까지 겹치면 **0.68%**다. 살아나는
43,856행(2.11%)은 2025-12 이후 폐쇄되어 `bikeList`에 더는 없는 대여소이므로
`station_id`만 채워지고 나머지는 빈 값이 된다 — 지어내지 않는다.

매핑 신뢰도는 교차 검증으로 확인했다. 1단계와 3단계가 모두 맞은 2,636건에서
`stationId`가 **100% 일치**했다. 단 이 교차검증은 *API와 대여이력 사이*의 일치를 본
것이고, *대여이력 파일들 사이*의 번호 재사용은 검증 대상이 아니었다 — 대여소번호는
폐기 후 재사용될 수 있어 station_id와 1:1이 아니다. 3단계는 파일명순(=시간순)으로
읽어 최신 기간의 id를 채택하고, 덮어쓴 사실은 `history_id_conflicts` 통계와 경고
로그로 남긴다(`_read_history_ids` 참고).

`bikeStationMaster`는 쓸 수 없다. `ADDR2`가 대여소 번호가 아니라 상세주소
(`"더샵스타시티 C동 앞"`)라서 번호로 조인할 방법이 없다.

## 값의 시점

`bikeList`는 **현재** 재고 상태를 준다. 즉 여기서 얻는 `rack_tot_cnt`·`shared`·좌표는
매핑표를 만든 그 시각의 값이고, 과거 날짜에 적재되면 그 기간 내내 상수가 된다.
좌표는 대여소가 이전하지 않는 한 맞지만 거치대 수와 거치율은 그렇지 않다.
`stats`를 archive manifest에 실어 나중에 출처를 되짚을 수 있게 한다.
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
# 페이지를 무한히 도는 것을 막는 상한. 대여소는 실측 2,737개소이고 열린데이터광장이
# 총건수를 잘못 주더라도 여기서 끊긴다.
_MAX_PAGES = 20


# 대여소명 앞에 붙는 번호. `"102. 망원역 1번출구 앞"` -> `102`.
_NAME_PREFIX = re.compile(r"^\s*(\d+)\s*\.")

# 대여이력 CSV만 고른다. `--csv-dir`에 재고 CSV·기상 CSV가 함께 있을 수 있다.
_HISTORY_PATTERN = "서울특별시 공공자전거 대여이력 정보_*.csv"
_HISTORY_ENCODING = "cp949"
# (대여소번호 컬럼, 대여소ID 컬럼) 쌍. 한 행이 대여·반납 두 대여소를 담는다.
_HISTORY_COLUMN_PAIRS = (("대여 대여소번호", "대여대여소ID"), ("반납대여소번호", "반납대여소ID"))
# 충돌 로그/stats에 남기는 샘플 개수. manifest를 비대하게 만들지 않으면서 "어느 번호가
# 충돌했나"를 바로 짚을 수 있는 최소한.
_MAX_CONFLICT_SAMPLE = 5


@dataclass(frozen=True)
class StationInfo:
    """대여소 하나에 대해 매핑표가 아는 값들.

    전부 문자열이다 — `csv_source`가 모든 컬럼을 문자열로 넘기고 캐스팅은 검증 엔진의
    `types`가 맡는다는 규약을 지킨다. 모르는 값은 빈 문자열이고, 검증 엔진이 그것을
    결측으로 판정해 `optional_missing` 정책이 걸린다.
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

    ⚠️ 끝 판단에 `list_total_count`를 쓰면 안 된다. 이 서비스는 그 값에 **전체 대여소
    수가 아니라 그 페이지의 행 수**를 담는다(실측: 1/1000 -> 1000, 1001/2000 -> 1000,
    2001/3000 -> 739). 그것으로 끝내면 첫 페이지에서 멈춰 1,000개소만 받고, 나머지
    1,737개소가 조인에 실패해 그 행들이 조용히 폐기된다. 같은 열린데이터광장이라도
    `bikeStationMaster`는 전체(3428)를 주므로 서비스마다 의미가 다르다.

    페이지가 가득 차지 않으면 마지막 페이지다.
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

    대여·반납 두 컬럼 쌍을 모두 읽는다 — 한쪽에만 나오는 대여소가 있다.

    ⚠️ 대여소번호는 station_id와 1:1이 **아니다**. 폐기된 대여소의 번호가 나중에 다른
    대여소에 재사용될 수 있어서, 같은 `00102`가 2024년 파일에서는 `ST-4`, 2025년
    파일에서는 `ST-999`로 나올 수 있다. 파일을 파일명순(=사실상 시간순)으로 읽으므로
    나중 파일이 이긴다 — 최신 id가 지금 살아있는 대여소를 가리킬 가능성이 높아 이 동작
    자체는 유지하되, 덮어쓴 사실을 조용히 삼키지 않고 어느 기간에서 어느 기간으로
    바뀌었는지 남긴다. 이 매핑은 `bikeList`에 없는 대여소(폐쇄분)에만 쓰이므로 충돌이
    그대로 적재 결과가 되는 경로다.

    returns:
        (매핑, 충돌 설명 목록). 충돌 설명은 `"00102: ST-4(2412) -> ST-999(2501)"`
        형식이고 대여소번호순으로 정렬된다(결정적).
    """
    mapping: dict[str, str] = {}
    # 현재 매핑값이 어느 파일(기간)에서 왔는지 — 충돌 로그에 시점을 남기기 위한 것.
    origin: dict[str, str] = {}
    trail: dict[str, list[str]] = {}
    # csv_source.glob_normalized 참고: macOS는 파일명을 NFD로 저장해 Path.glob()의
    # NFC 패턴이 조용히 0건 매칭된다. 여기서도 같은 문제라 같은 방식으로 우회한다.
    for path in glob_normalized(csv_dir, _HISTORY_PATTERN):
        # "...대여이력 정보_2501.csv" -> "2501"
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
        csv_dir: 대여이력 CSV를 찾을 디렉터리. None이면 3단계를 건너뛴다(커버리지가
            실측 99.32%에서 97.1%로 떨어지지만 실행은 성공한다).
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

    history, conflicts = _read_history_ids(csv_dir) if csv_dir is not None else ({}, [])
    for number, station_id in history.items():
        # API가 이긴다 — 그쪽만 5개 컬럼을 모두 채운다.
        by_number.setdefault(number, StationInfo(station_id=station_id))

    stats = {
        "built_at": datetime.now(tz=KST).isoformat(),
        "api_stations": len(api_rows),
        "history_stations": len(history),
        # 대여소번호 하나가 기간에 따라 서로 다른 station_id로 나타난 건수. manifest에
        # 실려 남으므로 나중에 "그때 번호가 재사용됐었나"를 되짚을 수 있다.
        "history_id_conflicts": len(conflicts),
        "history_id_conflict_sample": conflicts[:_MAX_CONFLICT_SAMPLE],
    }
    logger.info(
        f"stage=bootstrap_station_join api_stations={stats['api_stations']} "
        f"history_stations={stats['history_stations']} by_name={len(by_name)} "
        f"by_number={len(by_number)} history_id_conflicts={stats['history_id_conflicts']}"
    )
    if conflicts:
        logger.warning(
            f"stage=bootstrap_station_join 대여소번호가 기간에 따라 다른 station_id를 가리킴 "
            f"count={len(conflicts)} sample={stats['history_id_conflict_sample']} "
            "— 최신 기간의 id를 채택했다"
        )
    return StationMap(by_name=by_name, by_number=by_number, stats=stats)
