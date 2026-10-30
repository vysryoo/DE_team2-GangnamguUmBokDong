"""서울 생활인구 격자 베이스라인과 실시간 POI 인구를 공간 정규화하는 CLI 진입점.

한 번 실행하면 **현재 시각 + 실시간 도시데이터가 주는 향후 12시간 예측 시각**을 각각
보정해 그 시각의 tick 키에 쓴다. 미래 시각의 baseline은 nowcaster가 만든 추정치이고
(`storage.read_nowcast_grid`), 보정값은 `FCST_n_*` 컬럼이다. 소비자(추론기)는 미래
시각으로 조회하면 그 파일을 그대로 읽는다 — 조회 경로가 현재분과 같다.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta, tzinfo

import grid
import merge
import poi

# pyrefly: ignore [missing-import]
import pyarrow as pa
import storage
from core.forecast import POPULATION_FORECAST_SLOT_COUNT
from core.kst import parse_kst_datetime
from core.poi_master import PoiMasterError, PoiMasterRef
from core.sources import POPULATION_REALTIME

# 실시간 도시데이터의 예측 시각 포맷(실측: "2026-08-19 22:00", KST 정시).
_FORECAST_TIME_FORMAT = "%Y-%m-%d %H:%M"
# 이보다 먼 예측은 쓰지 않는다. API가 간격을 바꿔도 미래로 무한히 번지지 않게 하는 상한.
_MAX_HORIZON = timedelta(hours=12)
_POI_CODE_PATTERN = re.compile(r"POI[0-9]{3}\Z")


class RealtimePoiContractError(ValueError):
    """실시간 생활인구의 POI 식별자 계약이 선택된 Master와 맞지 않는다."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    args:
        argv: 파싱할 인자 목록 (생략 시 sys.argv 사용)
    returns:
        window_start가 포함된 Namespace
    """
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument(
        "--poi-master-mode",
        choices=("static", "s3"),
        default="static",
        help="Airflow가 중앙에서 고정한 POI Master 소비 모드",
    )
    parser.add_argument("--poi-master-manifest-uri")
    parser.add_argument("--poi-master-manifest-sha256")
    return parser.parse_args(argv)


def _filter_grid_rows_for_hour(grid_table: pa.Table, hour: int) -> dict[str, merge.GridCell]:
    """해당 시간대(TT)의 행을 CELL별로 합산해 GridCell 맵으로 변환한다.

    nowcaster 입력의 ``TT``는 과거 원천에 따라 ``"0 "``처럼 뒤 공백이 붙거나
    ``"9"``/``"09"``/정수 ``9``처럼 표현이 다를 수 있다. 표현만 정규화하고
    0~23 정수 계약은 모든 행에 엄격히 적용한다. 같은 CELL·시간에 여러
    ``H_DNG_CD`` component가 있으면 training의 인구 집계와 같은 의미가 되도록
    SPOP과 28개 age를 합산한다.

    args:
        grid_table: 24시간 생활인구 격자 테이블
        hour: 대상 시각 (0~23)
    returns:
        CELL_ID를 키로 하는 GridCell 딕셔너리
    """
    def _parse_hour(raw: object) -> int:
        """허용된 TT 표현을 정수 시각으로 바꾸고 잘못된 값은 실패시킨다."""
        if isinstance(raw, bool):
            parsed = None
        elif isinstance(raw, int):
            parsed = raw
        elif isinstance(raw, str):
            text = raw.strip()
            is_ascii_integer = (
                1 <= len(text) <= 2 and all("0" <= character <= "9" for character in text)
            )
            parsed = int(text) if is_ascii_integer else None
        else:
            parsed = None
        if parsed is None or not 0 <= parsed <= 23:
            raise ValueError(
                "living_population_grid TT가 잘못됐습니다: "
                f"TT={raw!r} (공백 제거 후 0..23 정수 필수)"
            )
        return parsed

    def _required_text(raw: object, field: str, *, ascii_digits: bool = False) -> str:
        """필수 식별자를 trim하고 빈 값·잘못된 행정동 코드를 실패시킨다."""
        text = "" if raw is None else str(raw).strip()
        digits_valid = not ascii_digits or (
            bool(text) and all("0" <= character <= "9" for character in text)
        )
        if not text or not digits_valid:
            suffix = (
                " (공백이 아닌 ASCII 숫자 문자열 필수)"
                if ascii_digits
                else " (공백이 아닌 문자열 필수)"
            )
            raise ValueError(
                f"living_population_grid {field}가 잘못됐습니다: {field}={raw!r}{suffix}"
            )
        return text

    # 하루치 테이블(25만 행대)을 매 target(최대 13개)마다 통째로 to_pylist하면 비용이
    # 13배로 쌓인다. TT 컬럼만 먼저 pylist로 훑어 대상 시각 행의 인덱스를 추리고,
    # 전체 컬럼 변환은 그 부분집합에만 적용한다 — TT 검증은 여전히 전 행에 적용된다.
    matching_indices = [
        index
        for index, raw in enumerate(grid_table.column("TT").to_pylist())
        if _parse_hour(raw) == hour
    ]

    spop_by_cell: dict[str, float] = {}
    ages_by_cell: dict[str, dict[str, float]] = {}
    h_dng_codes_by_cell: dict[str, set[str]] = {}
    for row in grid_table.take(matching_indices).to_pylist():
        cell_id = _required_text(row.get("CELL_ID"), "CELL_ID")
        h_dng_cd = _required_text(row.get("H_DNG_CD"), "H_DNG_CD", ascii_digits=True)
        spop_by_cell[cell_id] = spop_by_cell.get(cell_id, 0.0) + float(row["SPOP"] or 0.0)
        if cell_id not in ages_by_cell:
            ages_by_cell[cell_id] = {column: 0.0 for column in merge.AGE_COLUMNS}
            h_dng_codes_by_cell[cell_id] = set()
        for column in merge.AGE_COLUMNS:
            ages_by_cell[cell_id][column] += float(row.get(column) or 0.0)
        h_dng_codes_by_cell[cell_id].add(h_dng_cd)

    cells: dict[str, merge.GridCell] = {}
    for cell_id in sorted(spop_by_cell):
        # 출력 schema는 H_DNG_CD 한 값만 담는다. 단일 component면 원값을 보존하고,
        # 여러 component를 합친 CELL은 정렬상 최소 코드를 deterministic 대표값으로 쓴다.
        representative_h_dng_cd = min(h_dng_codes_by_cell[cell_id])
        cells[cell_id] = merge.GridCell(
            cell_id=cell_id,
            h_dng_cd=representative_h_dng_cd,
            spop=spop_by_cell[cell_id],
            ages=ages_by_cell[cell_id],
            geometry=grid.cell_id_to_polygon(cell_id),
        )
    return cells


def _build_poi_snapshots(
    poi_areas: tuple[poi.PoiArea, ...], realtime_table: pa.Table
) -> list[merge.PoiSnapshot]:
    """POI 지오메트리와 실시간 인구 관측치를 결합하여 PoiSnapshot 목록을 생성한다.

    args:
        poi_areas: POI 영역 지오메트리 목록
        realtime_table: 실시간 POI 인구 수집 테이블
    returns:
        결합된 PoiSnapshot 목록
    """
    realtime_by_code = {row["AREA_CD"]: row for row in realtime_table.to_pylist()}
    snapshots: list[merge.PoiSnapshot] = []
    for area in poi_areas:
        realtime_row = realtime_by_code.get(area.area_cd)
        if realtime_row is None:
            continue
        pop_estimate = (realtime_row["AREA_PPLTN_MIN"] + realtime_row["AREA_PPLTN_MAX"]) / 2.0
        snapshots.append(
            merge.PoiSnapshot(
                area_cd=area.area_cd,
                male_rate=float(realtime_row["MALE_PPLTN_RATE"]),
                female_rate=float(realtime_row["FEMALE_PPLTN_RATE"]),
                pop_estimate=pop_estimate,
                geometry=area.geometry,
                area_m2=area.area_m2,
            )
        )
    return snapshots


def _validate_realtime_poi_contract(
    realtime_table: pa.Table,
    poi_areas: tuple[poi.PoiArea, ...],
) -> frozenset[str]:
    """실시간 AREA_CD가 유효하고 선택된 POI Master의 부분집합인지 검증한다.

    API의 부분 실패로 Master 일부 코드가 실시간 snapshot에 없는 것은 허용한다. 반대로
    실시간 snapshot에만 있는 코드는 Collector와 Normalizer가 서로 다른 Master를 썼거나
    입력 authority가 손상됐다는 뜻이므로 출력 전에 실패시킨다.

    args:
        realtime_table: Collector가 게시한 실시간 POI 인구 테이블
        poi_areas: 이번 실행에 고정된 POI Master 영역
    returns:
        검증된 실시간 AREA_CD 집합
    raises:
        RealtimePoiContractError: AREA_CD 컬럼·값·고유성 또는 Master 부분집합 계약이 깨질 때
    """
    if type(realtime_table) is not pa.Table:
        raise RealtimePoiContractError(
            "population_realtime 입력은 pyarrow.Table이어야 합니다."
        )
    if "AREA_CD" not in realtime_table.column_names:
        raise RealtimePoiContractError(
            "population_realtime 입력에 AREA_CD 컬럼이 없습니다."
        )

    raw_codes = realtime_table.column("AREA_CD").to_pylist()
    invalid_codes = [
        code
        for code in raw_codes
        if type(code) is not str or _POI_CODE_PATTERN.fullmatch(code) is None
    ]
    if invalid_codes:
        raise RealtimePoiContractError(
            "population_realtime AREA_CD는 POI와 ASCII 숫자 3자리 형식이어야 합니다: "
            f"invalid={invalid_codes!r}"
        )
    if len(raw_codes) != len(set(raw_codes)):
        duplicates = sorted(
            code for code in set(raw_codes) if raw_codes.count(code) > 1
        )
        raise RealtimePoiContractError(
            "population_realtime AREA_CD에 중복 코드가 있습니다: "
            f"duplicates={duplicates}"
        )

    realtime_codes = frozenset(raw_codes)
    master_codes = frozenset(area.area_cd for area in poi_areas)
    unknown_codes = sorted(realtime_codes - master_codes)
    if unknown_codes:
        raise RealtimePoiContractError(
            "population_realtime에 선택된 POI Master에 없는 AREA_CD가 있습니다: "
            f"unknown={unknown_codes}"
        )
    return realtime_codes


def _parse_forecast_time(raw: object, tzinfo: tzinfo | None) -> datetime | None:
    """`FCST_n_TIME` 문자열을 window_start와 같은 시간대의 datetime으로 바꾼다. 실패하면 None."""
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.strptime(raw, _FORECAST_TIME_FORMAT)  # noqa: DTZ007 - 아래에서 tz를 붙인다
    except ValueError:
        return None
    return parsed.replace(tzinfo=tzinfo)


def _forecast_by_time(row: dict, tzinfo: tzinfo | None) -> dict[datetime, tuple[float, float]]:
    """POI 한 행의 예측 슬롯을 {예측시각: (MIN, MAX)}로 바꾼다.

    슬롯 번호를 "n시간 후"로 해석하지 않는다 — 실측(2026-08-19 20:55 관측)에서 첫 슬롯이
    22:00이었다. 어느 시각의 예측인지는 `FCST_n_TIME`만이 말한다.
    """
    if row.get("FCST_YN") != "Y":
        return {}

    forecasts: dict[datetime, tuple[float, float]] = {}
    for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
        target = _parse_forecast_time(row.get(f"FCST_{slot}_TIME"), tzinfo)
        pop_min = row.get(f"FCST_{slot}_PPLTN_MIN")
        pop_max = row.get(f"FCST_{slot}_PPLTN_MAX")
        if target is None or pop_min is None or pop_max is None:
            continue
        forecasts[target] = (float(pop_min), float(pop_max))
    return forecasts


def _collect_forecasts(
    realtime_table: pa.Table, window_start: datetime
) -> dict[str, dict[datetime, tuple[float, float]]]:
    """POI 코드별 {예측시각: (MIN, MAX)} 맵을 만든다. 과거·12시간 초과 시각은 버린다."""
    horizon_end = window_start + _MAX_HORIZON
    by_code: dict[str, dict[datetime, tuple[float, float]]] = {}
    for row in realtime_table.to_pylist():
        forecasts = {
            target: bounds
            for target, bounds in _forecast_by_time(row, window_start.tzinfo).items()
            if window_start < target <= horizon_end
        }
        if forecasts:
            by_code[row["AREA_CD"]] = forecasts
    return by_code


def _forecast_targets(
    forecasts_by_code: dict[str, dict[datetime, tuple[float, float]]]
) -> list[datetime]:
    """보정할 미래 시각 목록(오름차순). POI마다 슬롯이 어긋나도 합집합으로 모은다."""
    return sorted({target for forecasts in forecasts_by_code.values() for target in forecasts})


def _build_forecast_snapshots(
    poi_areas: tuple[poi.PoiArea, ...],
    forecasts_by_code: dict[str, dict[datetime, tuple[float, float]]],
    target: datetime,
) -> dict[str, merge.PoiSnapshot]:
    """해당 미래 시각의 PoiSnapshot 맵을 만든다.

    성비(`male_rate`/`female_rate`)는 넣지 않는다(0.0) — 예측값에 성비가 없어 미래 시각은
    `merge.merge_cell_total_only()`가 총량만 합성하고 성·연령 구성은 baseline을 따른다.

    args:
        poi_areas: POI 영역 지오메트리 목록
        forecasts_by_code: POI 코드별 예측 맵
        target: 대상 예측 시각
    returns:
        POI 코드를 키로 하는 PoiSnapshot 맵(그 시각 예측이 없는 POI는 빠진다)
    """
    snapshots: dict[str, merge.PoiSnapshot] = {}
    for area in poi_areas:
        bounds = forecasts_by_code.get(area.area_cd, {}).get(target)
        if bounds is None:
            continue
        pop_min, pop_max = bounds
        snapshots[area.area_cd] = merge.PoiSnapshot(
            area_cd=area.area_cd,
            male_rate=0.0,
            female_rate=0.0,
            pop_estimate=(pop_min + pop_max) / 2.0,
            geometry=area.geometry,
            area_m2=area.area_m2,
        )
    return snapshots


_OUTPUT_SCHEMA = pa.schema(
    [("CELL_ID", pa.string()), ("H_DNG_CD", pa.string()), ("SPOP", pa.int64())]
    + [(c, pa.int64()) for c in merge.AGE_COLUMNS]
)


def _baseline_cells(
    baseline_cache: dict[date, pa.Table], target: datetime
) -> dict[str, merge.GridCell]:
    """해당 시각의 baseline 격자(nowcaster 추정치)를 GridCell 맵으로 돌려준다.

    날짜별 테이블을 캐시한다 — 12시간 앞이 자정을 넘으면 두 날짜(오늘/내일)를 읽게 되고,
    시각마다 다시 읽으면 같은 파일을 13번 내려받는다.
    """
    target_date = target.date()
    if target_date not in baseline_cache:
        baseline_cache[target_date] = storage.read_nowcast_grid(target_date)
    return _filter_grid_rows_for_hour(baseline_cache[target_date], target.hour)


def _write_normalized(
    target: datetime,
    source_window_start: datetime,
    cells_by_id: dict[str, merge.GridCell],
    overlap_areas: dict[str, list[tuple[str, float]]],
    snapshots_by_code: dict[str, merge.PoiSnapshot],
    merge_cell: Callable[[merge.GridCell, list[tuple[merge.PoiSnapshot, float]]], merge.MergedCell],
) -> str | None:
    """한 시각의 보정 결과를 더 최신 세대를 보존하며 tick 키에 쓴다."""
    output_rows = [
        merge.round_output_row(
            merge_cell(cell, merge.bind_snapshots(overlap_areas.get(cell_id, []), snapshots_by_code))
        )
        for cell_id, cell in cells_by_id.items()
    ]
    table = pa.Table.from_pylist(output_rows, schema=_OUTPUT_SCHEMA)
    return storage.write_normalized_silver(
        target,
        table,
        source_window_start=source_window_start,
    )


def run(
    window_start: datetime,
    poi_master_ref: PoiMasterRef | None = None,
) -> int:
    """현재 시각과 향후 12시간 예측 시각을 각각 보정해 그 시각의 Silver tick에 쓴다.

    현재 시각은 실측 POI 인구와 성비로 성·연령까지 재분배하고(`merge.merge_cell`), 미래
    시각은 예측 인구로 총량만 합성한다(`merge.merge_cell_total_only`) — 예측값에는 성비가
    없다. 시각과 무관한 교차 면적은 한 번만 계산해 모든 시각에 재사용한다.

    args:
        window_start: 수집 기준 시각
    returns:
        종료 코드 (성공 시 0)
    """
    realtime_snapshot = storage.read_realtime_snapshot(window_start)
    realtime_table = realtime_snapshot.table
    source_selection = realtime_snapshot.selection_metadata(
        POPULATION_REALTIME,
        window_start,
        partial_policy=storage.PARTIAL_POLICY,
    )
    selected_ref = poi_master_ref or PoiMasterRef(mode="static")
    poi_areas = poi.load_poi_master_areas(selected_ref)
    _validate_realtime_poi_contract(realtime_table, poi_areas)

    forecasts_by_code = _collect_forecasts(realtime_table, window_start)
    targets = _forecast_targets(forecasts_by_code)

    baseline_cache: dict[date, pa.Table] = {}
    cells_by_target: dict[datetime, dict[str, merge.GridCell]] = {}
    skipped: list[str] = []
    for target in [window_start, *targets]:
        try:
            cells = _baseline_cells(baseline_cache, target)
        except storage.PartitionNotFoundError:
            # 현재 시각의 baseline이 없으면 보정 자체가 불가능하므로 실패로 올린다.
            # 미래 날짜만 없으면 그 시각을 건너뛰고 나머지를 쓴다.
            if target == window_start:
                raise
            skipped.append(target.isoformat())
            continue
        if not cells:
            # 현재 tick이 없는데 성공 처리하면 inference가 이 실행의 normalized
            # population 없이 진행한다. 미래 target만 fallback 가능 대상으로 건너뛴다.
            if target == window_start:
                raise ValueError(
                    "현재 window의 living_population_grid nowcast에 대상 시각 행이 없습니다: "
                    f"target={target.isoformat()}"
                )
            skipped.append(target.isoformat())
            continue
        cells_by_target[target] = cells

    # 교차 면적은 지오메트리만 보므로 시각과 무관하다 — 모든 시각의 셀 합집합으로 1회 계산한다.
    union_cells: dict[str, merge.GridCell] = {}
    for cells in cells_by_target.values():
        for cell_id, cell in cells.items():
            union_cells.setdefault(cell_id, cell)
    overlap_areas = merge.find_overlap_areas(list(union_cells.values()), list(poi_areas))

    observed_snapshots = _build_poi_snapshots(poi_areas, realtime_table)
    written: dict[str, str] = {}
    stale_generation_skipped: list[str] = []
    if window_start in cells_by_target:
        output_key = _write_normalized(
            window_start,
            window_start,
            cells_by_target[window_start],
            overlap_areas,
            {snapshot.area_cd: snapshot for snapshot in observed_snapshots},
            merge.merge_cell,
        )
        if output_key is None:
            stale_generation_skipped.append(window_start.isoformat())
        else:
            written[window_start.isoformat()] = output_key

    for target in targets:
        if target not in cells_by_target:
            continue
        output_key = _write_normalized(
            target,
            window_start,
            cells_by_target[target],
            overlap_areas,
            _build_forecast_snapshots(poi_areas, forecasts_by_code, target),
            merge.merge_cell_total_only,
        )
        if output_key is None:
            stale_generation_skipped.append(target.isoformat())
        else:
            written[target.isoformat()] = output_key

    storage.write_manifest(
        window_start,
        {
            "input_status": realtime_snapshot.status.value,
            "input_freshness": realtime_snapshot.freshness.value,
            "partial_policy": "repair",
            "resolution": (
                "repaired"
                if realtime_snapshot.status.value == "partial"
                else "observed"
            ),
            "source_selection": source_selection.as_dict(),
            "source_observed_at": realtime_snapshot.logical_dttm.isoformat(),
            "baseline_dates": sorted(d.isoformat() for d in baseline_cache),
            "cell_count": len(union_cells),
            "poi_matched_count": len(observed_snapshots),
            "poi_forecast_count": len(forecasts_by_code),
            "poi_master": selected_ref.as_dict(),
            "poi_master_row_count": len(poi_areas),
            "forecast_horizons": len(targets),
            "written_keys": written,
            "skipped_targets": skipped,
            "stale_generation_skipped_targets": stale_generation_skipped,
        },
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점으로 인자를 파싱하고 정규화 파이프라인을 실행한다."""
    args = parse_args(argv)
    window_start = parse_kst_datetime(args.window_start)
    try:
        ref = PoiMasterRef(
            mode=args.poi_master_mode,
            manifest_uri=args.poi_master_manifest_uri or None,
            manifest_sha256=args.poi_master_manifest_sha256 or None,
        )
        return run(window_start, ref)
    except (PoiMasterError, storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
