"""nowcaster가 만든 생활인구 격자 추정치를 보정 대상 시각별 GridCell로 읽는다."""

from __future__ import annotations

from datetime import date, datetime

# pyrefly: ignore [missing-import]
import pyarrow as pa

import grid
import merge
import storage


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


def filter_grid_rows_for_hour(grid_table: pa.Table, hour: int) -> dict[str, merge.GridCell]:
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


def load_baseline_cells(
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
        cells = filter_grid_rows_for_hour(tables[target.date()], target.hour)
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
