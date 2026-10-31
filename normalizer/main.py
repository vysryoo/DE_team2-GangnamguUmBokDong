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
from datetime import datetime

import baseline
import forecast
import merge
import poi

# pyrefly: ignore [missing-import]
import pyarrow as pa
import storage
from core.kst import parse_kst_datetime
from core.poi_master import PoiMasterError, PoiMasterRef
from core.sources import POPULATION_REALTIME

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


_OUTPUT_SCHEMA = pa.schema(
    [("CELL_ID", pa.string()), ("H_DNG_CD", pa.string()), ("SPOP", pa.int64())]
    + [(c, pa.int64()) for c in merge.AGE_COLUMNS]
)


def _union_cells(
    cells_by_target: dict[datetime, dict[str, merge.GridCell]],
) -> dict[str, merge.GridCell]:
    """시각별 격자 셀을 CELL_ID 기준 합집합으로 모은다. 같은 id는 먼저 나온 셀을 쓴다."""
    union: dict[str, merge.GridCell] = {}
    for cells in cells_by_target.values():
        for cell_id, cell in cells.items():
            union.setdefault(cell_id, cell)
    return union


def _write_targets(
    window_start: datetime,
    targets: list[datetime],
    observed_snapshots: list[merge.PoiSnapshot],
    poi_areas: tuple[poi.PoiArea, ...],
    forecasts_by_code: forecast.ForecastsByCode,
    cells_by_target: dict[datetime, dict[str, merge.GridCell]],
    overlap_areas: dict[str, list[tuple[str, float]]],
) -> tuple[dict[str, str], list[str]]:
    """시각별 보정 결과를 tick 키에 쓰고, 쓴 키와 더 최신 세대가 있어 건너뛴 시각을 반환한다.

    현재 시각은 실측 성비로 성·연령까지 재분배하고, 미래 시각은 예측값에 성비가 없어
    총량만 합성한다. baseline이 없는 미래 시각은 쓰지 않는다.
    """
    written: dict[str, str] = {}
    stale_generation_skipped: list[str] = []
    for target in [window_start, *targets]:
        if target == window_start:
            snapshots_by_code = {snapshot.area_cd: snapshot for snapshot in observed_snapshots}
            merge_cell = merge.merge_cell
        elif target in cells_by_target:
            snapshots_by_code = forecast.build_forecast_snapshots(
                poi_areas, forecasts_by_code, target
            )
            merge_cell = merge.merge_cell_total_only
        else:
            continue
        output_rows = [
            merge.round_output_row(
                merge_cell(cell, merge.bind_snapshots(overlap_areas.get(cell_id, []), snapshots_by_code))
            )
            for cell_id, cell in cells_by_target[target].items()
        ]
        output_key = storage.write_normalized_silver(
            target,
            pa.Table.from_pylist(output_rows, schema=_OUTPUT_SCHEMA),
            source_window_start=window_start,
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

    forecasts_by_code = forecast.collect_forecasts(realtime_table, window_start)
    targets = forecast.forecast_targets(forecasts_by_code)
    cells_by_target, skipped, baseline_dates = baseline.load_baseline_cells(window_start, targets)

    # 교차 면적은 지오메트리만 보므로 모든 시각의 셀 합집합으로 한 번만 계산함
    union_cells = _union_cells(cells_by_target)
    overlap_areas = merge.find_overlap_areas(list(union_cells.values()), list(poi_areas))

    observed_snapshots = _build_poi_snapshots(poi_areas, realtime_table)
    written, stale_generation_skipped = _write_targets(
        window_start,
        targets,
        observed_snapshots,
        poi_areas,
        forecasts_by_code,
        cells_by_target,
        overlap_areas,
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
