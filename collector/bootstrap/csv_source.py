"""과거 CSV를 한 번씩만 읽어 날짜별 Arrow 테이블로 나눈다.

월 파일이 수백 MB라 날짜마다 다시 읽지 않고 한 번에 날짜별로 버킷팅한다. 행이 시각순으로
정렬돼 있지 않아 파일 끝까지 읽어야 한 날짜가 확정된다.

메모리는 두 가지로 억제한다. 파일명의 `_YYMM`이 요청 범위와 겹치지 않으면 파일을 열지
않고(YYMM을 못 뽑으면 데이터를 빠뜨리지 않도록 읽음), 청크마다 날짜별 몫을 문자열
`pa.RecordBatch`로 바꿔 파이썬 객체를 바로 해제한다. 타입 캐스팅은 `validate_batch`가 한다.
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa

from bootstrap.config import BootstrapConfig
from bootstrap.globbing import glob_normalized
from bootstrap.station_join import StationMap
from core.wind import wind_components

logger = logging.getLogger(__name__)

_CHUNK_ROWS = 100_000

# "..._2606 (2).csv"의 2606처럼 앞뒤로 숫자가 붙지 않은 4자리만 잡음
_YYMM_PATTERN = re.compile(r"(?<!\d)_(\d{4})(?!\d)")


def _file_year_month(path: Path) -> tuple[int, int] | None:
    """파일명에서 YYMM을 뽑아 (연, 월)로 반환한다. 못 뽑거나 월이 부적절하면 None."""
    match = _YYMM_PATTERN.search(path.stem)
    if not match:
        return None
    yy, mm = int(match.group(1)[:2]), int(match.group(1)[2:])
    if not 1 <= mm <= 12:
        return None
    return (2000 + yy, mm)


def read_by_date(
    cfg: BootstrapConfig,
    csv_dir: Path,
    days: set[date],
    station_map: StationMap | None = None,
) -> dict[date, pa.Table]:
    """디렉터리의 CSV들을 한 번씩 훑어 요청한 날짜별 Arrow 테이블로 나눈다.

    헤더는 `column_map`으로 물리 컬럼명이 되고, `value_map`에 있는 값은 변환된다.
    `na_values`에 해당하는 값은 검증 엔진이 결측으로 보는 빈 문자열이 된다.

    args:
        cfg: 해당 소스의 bootstrap 설정
        csv_dir: CSV들이 있는 디렉터리. `cfg.file_pattern`에 맞는 파일만 읽는다.
        days: 담을 날짜 집합. 여기 없는 날짜의 행은 버린다.
        station_map: `join`이 선언된 설정에서 조인에 쓸 매핑표. 선언되지 않았으면 무시된다.
    returns:
        `{날짜: pa.Table}`. 모든 컬럼은 문자열 타입이다. 행이 하나도 없는 날짜는
        키 자체가 없다.
    raises:
        ValueError: 시각 컬럼을 설정된 형식으로 파싱할 수 없을 때, 또는 `join`이
            선언됐는데 `station_map`이 없을 때.
    """
    if cfg.join is not None and station_map is None:
        # 그냥 두면 required인 stationId가 모두 비어 archive가 빈 채로 성공함
        raise ValueError(f"join(provider={cfg.join.provider})이 선언됐는데 station_map이 없다")

    months = {(d.year, d.month) for d in days}
    na_values = set(cfg.na_values)
    batches: dict[date, list[pa.RecordBatch]] = defaultdict(list)

    for path in glob_normalized(csv_dir, cfg.file_pattern):
        year_month = _file_year_month(path)
        if year_month is not None and year_month not in months:
            logger.info(
                f"stage=bootstrap_csv file={path.name} 요청 범위와 겹치지 않아 건너뜀"
            )
            continue
        if year_month is None:
            logger.warning(
                f"stage=bootstrap_csv file={path.name} 파일명에서 YYMM을 뽑을 수 없어 "
                "전체를 읽는다"
            )
        _read_file_into_batches(path, cfg, days, na_values, batches, station_map)

    return {day: pa.Table.from_batches(day_batches) for day, day_batches in batches.items()}


def _read_file_into_batches(
    path: Path,
    cfg: BootstrapConfig,
    days: set[date],
    na_values: set[str],
    batches: dict[date, list[pa.RecordBatch]],
    station_map: StationMap | None = None,
) -> None:
    """CSV 한 파일을 청크 단위로 읽어 `batches`에 날짜별 RecordBatch를 쌓는다."""
    physical_columns = _physical_columns(cfg)
    with path.open(encoding=cfg.encoding, errors="replace", newline="") as handle:
        chunk: dict[date, dict[str, list[str]]] = {}
        rows_in_chunk = 0
        for raw in csv.DictReader(handle):
            converted = _physical_row(raw, cfg, days, na_values, station_map)
            if converted is None:
                continue
            day, row = converted
            day_columns = chunk.setdefault(day, {name: [] for name in physical_columns})
            for name in physical_columns:
                day_columns[name].append(row[name])

            rows_in_chunk += 1
            if rows_in_chunk >= _CHUNK_ROWS:
                _flush_chunk(chunk, batches)
                chunk = {}
                rows_in_chunk = 0

        if chunk:
            _flush_chunk(chunk, batches)


def _physical_columns(cfg: BootstrapConfig) -> list[str]:
    """설정의 모든 채움 경로가 만드는 물리 컬럼 목록을 반환한다."""
    return [
        name
        for name in (
            *cfg.column_map.values(),
            *cfg.constants,
            *(target for spec in cfg.derived_time.values() for target in spec.into),
            *cfg.composed_time,
            *(cfg.join.fills if cfg.join else ()),
            *((cfg.derived_wind.u, cfg.derived_wind.v) if cfg.derived_wind else ()),
        )
        # `_station_no` 같은 조인용 임시 값은 `conform()`이 어차피 버리므로 쌓지 않음
        if not name.startswith("_")
    ]


def _physical_row(
    raw: dict,
    cfg: BootstrapConfig,
    days: set[date],
    na_values: set[str],
    station_map: StationMap | None,
) -> tuple[date, dict[str, str]] | None:
    """CSV 원본 한 행을 (날짜, 물리 행)으로 바꾼다. 요청 날짜가 아니면 None을 반환한다."""
    row = {
        physical: ("" if raw.get(header) in na_values else (raw.get(header) or ""))
        for header, physical in cfg.column_map.items()
    }
    row.update(cfg.constants)
    # 날짜 버킷팅이 분해·결합 결과를 읽으므로 그보다 먼저 계산함
    row.update(_derived_time_values(raw, cfg))
    row.update(_composed_time_values(raw, cfg))
    day = _row_date(row, cfg)
    if day not in days:
        return None
    for column, mapping in cfg.value_map.items():
        if column in row and row[column] in mapping:
            row[column] = mapping[row[column]]

    # value_map이 풍속·풍향과 조인 키를 고칠 수 있으므로 파생과 조인은 그 뒤에 함
    row.update(_derived_wind_values(row, cfg))
    row.update(_join_values(row, cfg, station_map))
    return day, row


def _flush_chunk(
    chunk: dict[date, dict[str, list[str]]],
    batches: dict[date, list[pa.RecordBatch]],
) -> None:
    """청크의 날짜별 파이썬 열을 RecordBatch로 바꿔 누적하고 청크를 비운다."""
    for day, columns in chunk.items():
        arrays = {name: pa.array(values, type=pa.string()) for name, values in columns.items()}
        batches[day].append(pa.RecordBatch.from_pydict(arrays))


def _derived_time_values(raw: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`derived_time` 규칙으로 매핑 전 헤더의 시각을 여러 물리 컬럼으로 분해한다.

    읽을 수 없는 시각을 빈 값으로 넘기면 required 결측으로 행이 조용히 폐기되므로
    ValueError로 끊는다.
    """
    values: dict[str, str] = {}
    for header, spec in cfg.derived_time.items():
        text = (raw.get(header) or "").strip()
        try:
            parsed = datetime.strptime(text, spec.parse)
        except ValueError as exc:
            raise ValueError(
                f"시각 컬럼 '{header}'을 '{spec.parse}' 형식으로 읽을 수 없다: {text!r}"
            ) from exc
        for target, fmt in spec.into.items():
            values[target] = parsed.strftime(fmt)
    return values


def _composed_time_values(raw: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`composed_time` 규칙으로 여러 시각 헤더를 공백으로 이어 물리 컬럼 하나로 합친다.

    `"2025-12-01 0"`처럼 이어 붙여도 `%H`가 한 자리 시각을 받는다. 읽을 수 없으면
    `derived_time`과 같은 이유로 ValueError로 끊는다.
    """
    values: dict[str, str] = {}
    for target, spec in cfg.composed_time.items():
        text = " ".join((raw.get(header) or "").strip() for header in spec.from_)
        try:
            parsed = datetime.strptime(text, spec.parse)
        except ValueError as exc:
            raise ValueError(
                f"시각 컬럼 {list(spec.from_)}을 합친 '{text}'을 '{spec.parse}' 형식으로 "
                f"읽을 수 없다 (대상 컬럼 '{target}')"
            ) from exc
        values[target] = parsed.strftime(spec.format)
    return values


def _join_values(row: dict, cfg: BootstrapConfig, station_map: StationMap | None) -> dict[str, str]:
    """`join` 규칙으로 매핑표에서 `fills` 컬럼 값을 가져온다.

    매핑표에 없는 대여소는 빈 문자열로 두고 따로 거르지 않는다. 검증 엔진이 그 행을
    폐기하고 건수를 manifest의 `column_issues`에 남긴다.
    """
    spec = cfg.join
    if spec is None or station_map is None:
        return {}

    info = station_map.lookup(row.get(spec.by.number, ""), row.get(spec.by.name, ""))
    available = {
        "stationId": info.station_id if info else "",
        "rackTotCnt": info.rack_tot_cnt if info else "",
        "shared": info.shared if info else "",
        "stationLatitude": info.latitude if info else "",
        "stationLongitude": info.longitude if info else "",
    }
    return {column: available.get(column, "") for column in spec.fills}


def _derived_wind_values(row: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`derived_wind` 규칙으로 풍속·풍향에서 UUU·VVV를 계산한다.

    둘 중 하나라도 결측이면 두 성분을 빈 문자열(결측)로 둔다. 0으로 채우면 무풍이 된다.
    """
    spec = cfg.derived_wind
    if spec is None:
        return {}

    speed, direction = row.get(spec.speed, ""), row.get(spec.direction, "")
    if not str(speed).strip() or not str(direction).strip():
        return {spec.u: "", spec.v: ""}

    u, v = wind_components(speed, direction)
    return {spec.u: _format_component(u), spec.v: _format_component(v)}


def _format_component(value: float) -> str:
    """성분을 운영 수집분과 같은 소수 첫째 자리 문자열로 찍는다.

    북풍이나 반올림으로 생기는 `-0.0`이 `"0.0"`과 섞이지 않도록 `+ 0.0`으로 없앤다.
    """
    return f"{round(value, 1) + 0.0:.1f}"


def _row_date(row: dict, cfg: BootstrapConfig) -> date:
    """행이 속한 날짜를 시각 컬럼에서 뽑는다."""
    raw = row.get(cfg.window.from_column, "")
    try:
        return datetime.strptime(raw, cfg.window.format).date()
    except ValueError as exc:
        raise ValueError(
            f"시각 컬럼 '{cfg.window.from_column}'을 '{cfg.window.format}' 형식으로 "
            f"읽을 수 없다: {raw!r}"
        ) from exc
