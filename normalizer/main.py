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
from collections import Counter
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


MergeCell = Callable[[merge.GridCell, list[tuple[merge.PoiSnapshot, float]]], merge.MergedCell]


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


def _parse_hour(raw: object) -> int:
    """허용된 TT 표현("0 ", "09", 9 등)을 0~23 정수로 바꾸고 잘못된 값은 실패시킨다."""
    text = raw.strip() if isinstance(raw, str) else None
    if text is not None and text.isascii() and text.isdigit() and len(text) <= 2:
        hour = int(text)
    elif type(raw) is int:
        hour = raw
    else:
        hour = None
    if hour is None or not 0 <= hour <= 23:
        raise ValueError(
            f"living_population_grid TT가 잘못됐습니다: TT={raw!r} (공백 제거 후 0..23 정수 필수)"
        )
    return hour


def _required_text(raw: object, field: str, *, ascii_digits: bool = False) -> str:
    """필수 식별자를 trim하고 빈 값·숫자가 아닌 행정동 코드를 실패시킨다."""
    text = "" if raw is None else str(raw).strip()
    if not text or (ascii_digits and not (text.isascii() and text.isdigit())):
        raise ValueError(f"living_population_grid {field}가 잘못됐습니다: {field}={raw!r}")
    return text


def _filter_grid_rows_for_hour(grid_table: pa.Table, hour: int) -> dict[str, merge.GridCell]:
    """해당 시간대(TT)의 행을 CELL별로 합산해 GridCell 맵으로 변환한다.

    같은 CELL·시간에 여러 `H_DNG_CD` component가 있으면 training의 인구 집계와 같은
    의미가 되도록 SPOP과 28개 age를 합산한다. TT 검증은 대상 시각이 아닌 행에도 적용한다.

    Args:
        grid_table: 24시간 생활인구 격자 테이블
        hour: 대상 시각 (0~23)
    Returns:
        CELL_ID를 키로 하는 GridCell 딕셔너리
    Raises:
        ValueError: TT·CELL_ID·H_DNG_CD 값이 계약을 어길 때
    """
    # 하루치(25만 행대) 전체를 target마다 to_pylist하지 않도록 TT로 대상 행만 먼저 추림
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
        ages = ages_by_cell.setdefault(cell_id, dict.fromkeys(merge.AGE_COLUMNS, 0.0))
        for column in merge.AGE_COLUMNS:
            ages[column] += float(row.get(column) or 0.0)
        h_dng_codes_by_cell.setdefault(cell_id, set()).add(h_dng_cd)

    # 출력 schema는 H_DNG_CD 한 값만 담으므로 여러 component면 최소 코드를 대표값으로 씀
    return {
        cell_id: merge.GridCell(
            cell_id=cell_id,
            h_dng_cd=min(h_dng_codes_by_cell[cell_id]),
            spop=spop_by_cell[cell_id],
            ages=ages_by_cell[cell_id],
            geometry=grid.cell_id_to_polygon(cell_id),
        )
        for cell_id in sorted(spop_by_cell)
    }


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

    API 부분 실패로 Master 일부 코드가 빠지는 것은 허용한다. 실시간에만 있는 코드는
    Collector와 Normalizer가 서로 다른 Master를 썼다는 뜻이라 출력 전에 실패시킨다.

    Args:
        realtime_table: Collector가 게시한 실시간 POI 인구 테이블
        poi_areas: 이번 실행에 고정된 POI Master 영역
    Returns:
        검증된 실시간 AREA_CD 집합
    Raises:
        RealtimePoiContractError: AREA_CD 컬럼·형식·고유성 또는 Master 부분집합 계약이 깨질 때
    """
    if "AREA_CD" not in realtime_table.column_names:
        raise RealtimePoiContractError("population_realtime 입력에 AREA_CD 컬럼이 없습니다.")

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
    duplicates = sorted(code for code, count in Counter(raw_codes).items() if count > 1)
    if duplicates:
        raise RealtimePoiContractError(
            f"population_realtime AREA_CD에 중복 코드가 있습니다: duplicates={duplicates}"
        )

    realtime_codes = frozenset(raw_codes)
    unknown_codes = sorted(realtime_codes - {area.area_cd for area in poi_areas})
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


def _load_baseline_cells(
    window_start: datetime, targets: list[datetime]
) -> tuple[dict[datetime, dict[str, merge.GridCell]], list[str], list[str]]:
    """현재와 미래 시각의 baseline 격자(nowcaster 추정치)를 시각별로 읽는다.

    현재 시각의 baseline이 없으면 보정이 불가능하고 inference가 이 실행의 결과 없이
    진행되므로 실패시킨다. 미래 시각만 없으면 그 시각을 건너뛴다.

    Returns:
        시각별 GridCell 맵, 건너뛴 시각, 읽은 baseline 날짜
    Raises:
        storage.PartitionNotFoundError: 현재 시각 날짜의 nowcast가 없을 때
        ValueError: 현재 시각의 nowcast 행이 없을 때
    """
    # 12시간 앞이 자정을 넘으면 두 날짜를 읽으므로 시각마다 같은 파일을 다시 받지 않게 캐시함
    tables: dict[date, pa.Table] = {}
    cells_by_target: dict[datetime, dict[str, merge.GridCell]] = {}
    skipped: list[str] = []
    for target in [window_start, *targets]:
        is_current = target == window_start
        try:
            if target.date() not in tables:
                tables[target.date()] = storage.read_nowcast_grid(target.date())
        except storage.PartitionNotFoundError:
            if is_current:
                raise
            skipped.append(target.isoformat())
            continue
        cells = _filter_grid_rows_for_hour(tables[target.date()], target.hour)
        if not cells:
            if is_current:
                raise ValueError(
                    "현재 window의 living_population_grid nowcast에 대상 시각 행이 없습니다: "
                    f"target={target.isoformat()}"
                )
            skipped.append(target.isoformat())
            continue
        cells_by_target[target] = cells
    return cells_by_target, skipped, sorted(day.isoformat() for day in tables)


def _write_normalized(
    target: datetime,
    source_window_start: datetime,
    cells_by_id: dict[str, merge.GridCell],
    overlap_areas: dict[str, list[tuple[str, float]]],
    snapshots_by_code: dict[str, merge.PoiSnapshot],
    merge_cell: MergeCell,
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


def _merge_plan(
    window_start: datetime,
    targets: list[datetime],
    observed_snapshots: list[merge.PoiSnapshot],
    poi_areas: tuple[poi.PoiArea, ...],
    forecasts_by_code: dict[str, dict[datetime, tuple[float, float]]],
    cells_by_target: dict[datetime, dict[str, merge.GridCell]],
) -> dict[datetime, tuple[dict[str, merge.PoiSnapshot], MergeCell]]:
    """시각마다 쓸 POI 스냅샷과 합성 함수를 정한다. baseline이 없는 미래 시각은 뺀다.

    현재 시각은 실측 성비로 성·연령까지 재분배하고, 미래 시각은 예측값에 성비가 없어
    총량만 합성한다.
    """
    plan: dict[datetime, tuple[dict[str, merge.PoiSnapshot], MergeCell]] = {
        window_start: (
            {snapshot.area_cd: snapshot for snapshot in observed_snapshots},
            merge.merge_cell,
        )
    }
    for target in targets:
        if target in cells_by_target:
            plan[target] = (
                _build_forecast_snapshots(poi_areas, forecasts_by_code, target),
                merge.merge_cell_total_only,
            )
    return plan


def _write_merge_plan(
    merge_plan: dict[datetime, tuple[dict[str, merge.PoiSnapshot], MergeCell]],
    window_start: datetime,
    cells_by_target: dict[datetime, dict[str, merge.GridCell]],
    overlap_areas: dict[str, list[tuple[str, float]]],
) -> tuple[dict[str, str], list[str]]:
    """시각별 보정 결과를 쓰고 쓴 키와 더 최신 세대가 있어 건너뛴 시각을 반환한다."""
    written: dict[str, str] = {}
    stale_generation_skipped: list[str] = []
    for target, (snapshots_by_code, merge_cell) in merge_plan.items():
        output_key = _write_normalized(
            target,
            window_start,
            cells_by_target[target],
            overlap_areas,
            snapshots_by_code,
            merge_cell,
        )
        if output_key is None:
            stale_generation_skipped.append(target.isoformat())
        else:
            written[target.isoformat()] = output_key
    return written, stale_generation_skipped


def run(
    window_start: datetime,
    poi_master_ref: PoiMasterRef | None = None,
) -> int:
    """현재 시각과 향후 12시간 예측 시각을 각각 보정해 그 시각의 Silver tick에 쓴다.

    Args:
        window_start: 수집 기준 시각
        poi_master_ref: Airflow가 고정한 POI Master. 생략하면 static
    Returns:
        종료 코드 (성공 시 0)
    """
    realtime_snapshot = storage.read_realtime_snapshot(window_start)
    realtime_table = realtime_snapshot.table
    selected_ref = poi_master_ref or PoiMasterRef(mode="static")
    poi_areas = poi.load_poi_master_areas(selected_ref)
    _validate_realtime_poi_contract(realtime_table, poi_areas)

    forecasts_by_code = _collect_forecasts(realtime_table, window_start)
    targets = _forecast_targets(forecasts_by_code)
    cells_by_target, skipped, baseline_dates = _load_baseline_cells(window_start, targets)

    # 교차 면적은 지오메트리만 보므로 모든 시각의 셀 합집합으로 한 번만 계산함
    union_cells: dict[str, merge.GridCell] = {}
    for cells in cells_by_target.values():
        for cell_id, cell in cells.items():
            union_cells.setdefault(cell_id, cell)
    overlap_areas = merge.find_overlap_areas(list(union_cells.values()), list(poi_areas))

    observed_snapshots = _build_poi_snapshots(poi_areas, realtime_table)
    merge_plan = _merge_plan(
        window_start, targets, observed_snapshots, poi_areas, forecasts_by_code, cells_by_target
    )
    written, stale_generation_skipped = _write_merge_plan(
        merge_plan, window_start, cells_by_target, overlap_areas
    )

    is_partial = realtime_snapshot.status.value == "partial"
    storage.write_manifest(
        window_start,
        {
            "input_status": realtime_snapshot.status.value,
            "input_freshness": realtime_snapshot.freshness.value,
            "partial_policy": "repair",
            "resolution": "repaired" if is_partial else "observed",
            "source_selection": realtime_snapshot.selection_metadata(
                POPULATION_REALTIME,
                window_start,
                partial_policy=storage.PARTIAL_POLICY,
            ).as_dict(),
            "source_observed_at": realtime_snapshot.logical_dttm.isoformat(),
            "baseline_dates": baseline_dates,
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
