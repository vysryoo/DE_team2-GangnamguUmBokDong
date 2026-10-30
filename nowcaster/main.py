"""생활인구 Archive 적재(backfill-archive, bootstrap-lookback)와 나우캐스팅(estimate) CLI."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# pyrefly: ignore [missing-import]
import pyarrow as pa
from core.kst import KST, parse_kst_datetime, today_kst

import backfill
import estimate_day
import holiday
import storage


def _dedup_grid_rows(table: pa.Table) -> pd.DataFrame:
    """같은 격자·시간대 행이 여러 번 실린 원천에서 첫 행만 남긴다."""
    return table.to_pandas().drop_duplicates(subset=estimate_day.KEY_COLS)


def _read_archive_as_frame(target_date: date) -> pd.DataFrame | None:
    """지정한 일자의 아카이브를 중복 제거한 DataFrame으로 읽는다. 없으면 None."""
    table = storage.read_archive(target_date)
    return _dedup_grid_rows(table) if table is not None else None


def _write_actual_archive(target_date: date, table: pa.Table) -> None:
    """실측 테이블에 실측 메타데이터를 붙여 중복 제거 후 Archive로 쓴다."""
    frame = _dedup_grid_rows(backfill.add_estimation_columns(table))
    storage.write_archive(target_date, pa.Table.from_pandas(frame, preserve_index=False))


def _promote_actual(source_window_start: datetime) -> None:
    """Authority가 확인된 실측 Silver를 실제 발생 일자의 Archive로 승격한다.

    Collector의 dt= 파티션은 수집 실행일이라 Silver 안의 YMD로 실제 날짜를 정한다.
    실측이 들어온 날짜의 임시 추정치는 지운다.
    """
    real = storage.read_real_grid_silver(source_window_start)
    if real is None:
        return
    for biz_date, day_table in backfill.group_rows_by_date(real).items():
        _write_actual_archive(biz_date, day_table)
        storage.delete_nowcast(biz_date)


def _estimate_date(target: date) -> None:
    """실측 Archive가 없는 날짜 하나의 추정치를 만들어 nowcast로 쓴다."""
    # 1~4주 전 후보는 주차별 가중치 슬롯이 고정이라 패턴이 다른 날짜도 None으로 자리를 채움
    candidate_frames = [
        _read_archive_as_frame(candidate) if holiday.matches_target_pattern(candidate, target) else None
        for candidate in holiday.candidate_dates(target)
    ]
    candidate_frames += [None] * (4 - len(candidate_frames))
    extended_frames = [_read_archive_as_frame(d) for d in holiday.extended_candidate_dates(target)]

    pattern = "special" if holiday.is_special_day(target) else "weekday"
    historical_dates = [d for d in storage.list_archive_dates() if holiday.matches_target_pattern(d, target)]
    historical_avg = estimate_day.historical_average_cached(
        pattern,
        historical_dates,
        _read_archive_as_frame,
        storage.read_historical_cache,
        storage.write_historical_cache,
    )

    nowcast_df = estimate_day.build_nowcast_table(
        candidate_frames,
        extended_frames=extended_frames,
        historical_avg_frame=historical_avg.reset_index() if historical_avg is not None else None,
    )
    if not nowcast_df.empty:
        storage.write_nowcast(target, pa.Table.from_pandas(nowcast_df, preserve_index=False))


def run_estimate(
    today: date,
    *,
    source_window_start: datetime | None = None,
) -> int:
    """권위가 확인된 실측을 반영하고 D-3부터 D+3까지 생활인구를 추정한다.

    Collector와 같은 exact logical window가 주어진 경우에만 실측을 승격한다. PARTIAL처럼
    authority가 없거나 window가 생략되면 승격만 건너뛰고 추정은 계속한다.

    Args:
        today: 추정 기준 KST 날짜
        source_window_start: Collector의 logical window(KST aware). 생략하면 승격 생략
    Returns:
        종료 코드 (성공 시 0)
    Raises:
        ValueError: source_window_start의 KST 날짜가 today와 다를 때
    """
    if source_window_start is not None:
        if source_window_start.astimezone(KST).date() != today:
            raise ValueError("source_window_start의 KST 날짜가 target date와 다르다")
        _promote_actual(source_window_start)

    for offset in range(-3, 4):
        target = today + timedelta(days=offset)
        if storage.read_archive(target) is None:
            _estimate_date(target)
    return 0


def run_backfill_archive(csv_dir: str) -> int:
    """지정한 디렉터리의 원본 CSV 파일들을 읽어 아카이브 Parquet으로 일괄 적재한다."""
    for csv_path in sorted(Path(csv_dir).glob("*.csv")):
        table = backfill.add_estimation_columns(backfill.read_source_csv(csv_path))
        for target_date, day_table in backfill.group_rows_by_date(table).items():
            storage.write_archive(target_date, day_table)
    return 0


def required_lookback_dates(today: date, horizon_days: int = 3) -> list[date]:
    """나우캐스팅 구간이 참조하는 1~4주 전 후보 날짜를 반환한다.

    ``run_estimate()``는 D-3~D+3을 한 번에 만들기 때문에 기본값도 같은 범위를
    사용한다. 공휴일/일요일은 단순히 7·14·21·28일 전을 고르는 대신 실제 추정과
    동일한 ``holiday.candidate_dates()`` 규칙을 사용해야, 백필은 성공했는데
    estimate가 다른 날짜를 찾는 불일치가 생기지 않는다.
    """
    if horizon_days < 0:
        raise ValueError("horizon_days는 0 이상이어야 한다")

    required: set[date] = set()
    for offset in range(-horizon_days, horizon_days + 1):
        required.update(holiday.candidate_dates(today + timedelta(days=offset)))
    return sorted(required)


def run_bootstrap_lookback(
    csv_dir: str,
    today: date,
    *,
    horizon_days: int = 3,
    force: bool = False,
) -> int:
    """과거 CSV에서 현재 나우캐스팅에 필요한 1~4주 Archive만 초기 적재한다.

    서울시 ``Se250MSpopLocalResd`` API는 과거 날짜 파라미터를 받지 않고 항상 최신
    공표일 한 날짜만 반환한다. 따라서 이 초기 적재는 서울 열린데이터광장에서 받은
    과거 생활인구 CSV를 입력으로 사용한다.

    이미 존재하는 Archive는 기본적으로 유지한다. 필요한 날짜가 CSV와 기존 Archive
    어디에도 없으면 non-zero를 반환해 불완전한 초기화를 성공으로 오인하지 않게 한다.
    """
    csv_paths = sorted(Path(csv_dir).glob("*.csv"))
    if not csv_paths:
        print(f"생활인구 CSV를 찾지 못했습니다: {csv_dir}", file=sys.stderr)
        return 1

    required = set(required_lookback_dates(today, horizon_days))
    available = {day for day in required if storage.read_archive(day) is not None}
    tables_by_date: dict[date, list[pa.Table]] = {}

    for csv_path in csv_paths:
        table = backfill.read_source_csv(csv_path)
        for target_date, day_table in backfill.group_rows_by_date(table).items():
            if target_date in required and (force or target_date not in available):
                tables_by_date.setdefault(target_date, []).append(day_table)

    for target_date, tables in sorted(tables_by_date.items()):
        _write_actual_archive(target_date, pa.concat_tables(tables))
        available.add(target_date)

    missing = sorted(required - available)
    print(
        f"population_lookback target={today} required={len(required)} "
        f"available={len(available)} missing={len(missing)}"
    )
    if missing:
        print(
            "누락 날짜: " + ", ".join(day.isoformat() for day in missing),
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 인자를 파싱하여 적재 또는 나우캐스팅 추정 명령을 실행한다."""
    parser = argparse.ArgumentParser(prog="seoul-pop-nowcasting")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_parser = subparsers.add_parser("backfill-archive")
    backfill_parser.add_argument("--csv-dir", required=True)

    lookback_parser = subparsers.add_parser("bootstrap-lookback")
    lookback_parser.add_argument("--csv-dir", required=True)
    lookback_parser.add_argument("--target-date")
    lookback_parser.add_argument(
        "--horizon-days",
        type=int,
        default=3,
        help="target-date 앞뒤 추정 범위(기본 3 = D-3~D+3)",
    )
    lookback_parser.add_argument(
        "--force", action="store_true", help="기존 Archive도 CSV 값으로 다시 쓴다"
    )

    estimate_parser = subparsers.add_parser("estimate")
    estimate_parser.add_argument("--target-date")
    estimate_parser.add_argument(
        "--source-window-start",
        help="Collector와 동일한 ISO 8601 logical window (timezone offset 필수)",
    )

    args = parser.parse_args(argv)
    if args.command == "backfill-archive":
        return run_backfill_archive(args.csv_dir)

    today = date.fromisoformat(args.target_date) if args.target_date else today_kst()
    if args.command == "bootstrap-lookback":
        return run_bootstrap_lookback(
            args.csv_dir,
            today,
            horizon_days=args.horizon_days,
            force=args.force,
        )

    source_window_start = None
    if args.source_window_start:
        source_window_start = parse_kst_datetime(args.source_window_start)
    else:
        print(
            "--source-window-start가 없어 actual Archive 승격을 생략합니다.",
            file=sys.stderr,
        )
    return run_estimate(today, source_window_start=source_window_start)


if __name__ == "__main__":
    sys.exit(main())
