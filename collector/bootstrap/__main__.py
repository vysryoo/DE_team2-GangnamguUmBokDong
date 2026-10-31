"""과거 데이터를 archive에 한 번 적재하는 수동 실행 CLI다.

    cd collector
    uv run --frozen python -m bootstrap --source bike_rental_history \
        --from 2025-01-01 --to 2025-12-31 --csv-dir ../data --csv-batch-by-month

이미 archive가 있는 날짜는 건너뛰고(`--force`로 무시), 실패한 날짜가 있으면 non-zero로
끝난다. 날짜 단위로 다 만든 뒤 쓰므로 다시 실행하면 실패한 날짜만 재시도된다.
`bike_station_realtime`은 대여소 매핑표용 대여이력 CSV도 `--csv-dir`에서 찾는다.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import httpx

import config.loader as config_loader
from bootstrap import api_source, csv_source, runner, station_join
from bootstrap import config as bootstrap_config
from logging_setup import configure_batch_logging

logger = logging.getLogger(__name__)

# history_api에서 연속 실패가 이만큼이면 인증키 오류·쿼터 소진으로 보고 남은 날짜를 중단함.
# 일시적 장애로 몇 날짜만 실패하는 경우와 구분하는 값
_MAX_CONSECUTIVE_FAILURES = 5


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="python -m bootstrap")
    parser.add_argument("--source", required=True, help="소스 id")
    parser.add_argument("--from", dest="from", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--to", required=True, help="끝 날짜, 포함 (YYYY-MM-DD)")
    parser.add_argument("--csv-dir", help="kind=csv일 때 CSV들이 있는 디렉터리")
    parser.add_argument("--concurrency", type=int, default=4, help="kind=history_api의 동시 조회 수")
    parser.add_argument(
        "--csv-batch-by-month",
        action="store_true",
        help="대용량 CSV 범위를 월별로 읽고 적재해 peak memory를 제한한다",
    )
    parser.add_argument(
        "--materialize-empty-archive",
        action="store_true",
        help="확인된 0행 날짜도 스키마가 있는 빈 Archive와 manifest로 기록한다",
    )
    parser.add_argument("--force", action="store_true", help="archive가 있어도 다시 쓴다")
    return parser.parse_args(argv)


def resolve_dates(args: argparse.Namespace) -> list[date]:
    """처리할 날짜를 오름차순으로 만든다.

    raises:
        SystemExit: `--from`이 `--to`보다 뒤일 때.
    """
    first, last = date.fromisoformat(getattr(args, "from")), date.fromisoformat(args.to)
    if first > last:
        raise SystemExit(f"--from({first})이 --to({last})보다 뒤다")
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def exit_code_for(results: list[runner.DateResult]) -> int:
    """실패한 날짜가 있으면 non-zero."""
    return 1 if any(r.status == "failed" for r in results) else 0


def _csv_day_batches(days: list[date], batch_by_month: bool) -> list[list[date]]:
    """CSV 날짜를 전체 한 묶음 또는 달력 월별 묶음으로 반환한다.

    월별 모드는 날짜별 Arrow table을 다음 달 처리 전에 해제해 1년치가 메모리에 동시에
    남지 않게 한다.
    """
    if not days or not batch_by_month:
        return [days]
    batches: list[list[date]] = []
    for day in days:
        if not batches or (batches[-1][0].year, batches[-1][0].month) != (day.year, day.month):
            batches.append([])
        batches[-1].append(day)
    return batches


def _csv_dir(args: argparse.Namespace) -> Path:
    """`--csv-dir`를 검증해 경로로 반환한다."""
    if not args.csv_dir:
        raise SystemExit("kind=csv 소스에는 --csv-dir가 필요하다")
    csv_dir = Path(args.csv_dir)
    if not csv_dir.exists() or not csv_dir.is_dir():
        raise SystemExit(f"--csv-dir 경로가 없거나 디렉터리가 아니다: {csv_dir}")
    if not any(csv_dir.glob("*.csv")):
        logger.warning(
            f"stage=bootstrap source={args.source} csv_dir={csv_dir} "
            "csv 파일을 하나도 찾지 못했다"
        )
    return csv_dir


def _load_csv_days(args, scfg, bcfg, days: list[date]) -> list[runner.DateResult]:
    """CSV를 배치마다 한 번만 읽어 날짜별로 적재한다."""
    csv_dir = _csv_dir(args)
    # 매핑표는 API를 여러 번 부르므로 월 배치 밖에서 범위 전체에 한 번만 만든다.
    station_map = station_join.build(csv_dir) if bcfg.join else None
    station_map_stats = station_map.stats if station_map else None
    day_batches = _csv_day_batches(days, args.csv_batch_by_month)
    results: list[runner.DateResult] = []
    for batch_number, day_batch in enumerate(day_batches, start=1):
        if args.csv_batch_by_month:
            logger.info(
                f"stage=bootstrap_csv source={args.source} batch={batch_number}/"
                f"{len(day_batches)} month={day_batch[0]:%Y-%m} days={len(day_batch)}"
            )
        by_date = csv_source.read_by_date(bcfg, csv_dir, set(day_batch), station_map)
        for day in day_batch:
            table = by_date.get(day)
            results.append(
                runner.load_date(
                    scfg,
                    bcfg,
                    day,
                    table.to_pylist() if table is not None else [],
                    force=args.force,
                    station_map_stats=station_map_stats,
                    materialize_empty=args.materialize_empty_archive,
                )
            )
    return results


def _load_api_days(args, scfg, bcfg, days: list[date]) -> tuple[list[runner.DateResult], bool]:
    """날짜별로 API를 조회해 적재하고, 연속 실패가 한도에 닿으면 중단한다."""
    results: list[runner.DateResult] = []
    consecutive_failures = 0
    with httpx.Client(timeout=60.0) as client:
        for day in days:
            try:
                rows = api_source.fetch_by_date(
                    bcfg, day, client=client, concurrency=args.concurrency
                )
            except api_source.FetchFailed as exc:
                results.append(runner.DateResult(day=day, status="failed", error=str(exc)))
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        f"stage=bootstrap source={args.source} "
                        f"연속 {consecutive_failures}회 실패해 중단한다. "
                        f"남은 {len(days) - len(results)}개 날짜는 시도하지 않았다"
                    )
                    return results, True
                continue
            consecutive_failures = 0
            results.append(runner.load_date(scfg, bcfg, day, rows, force=args.force))
    return results, False


def _summary(source: str, days: list[date], results: list[runner.DateResult], aborted: bool) -> str:
    """실행 요약 한 줄을 만든다."""
    tally = Counter(result.status for result in results)
    statuses = " ".join(f"{status}={count}" for status, count in sorted(tally.items()))
    overlapped = sum(1 for r in results if r.silver_present)
    dropped_total = sum(r.dropped or 0 for r in results)
    out_of_range_total = sum(r.out_of_range for r in results)
    out_of_range_note = f" out_of_range={out_of_range_total}" if out_of_range_total else ""
    abort_note = " aborted=true" if aborted else ""
    return (
        f"source={source} dates={len(days)} {statuses} "
        f"silver_overlap={overlapped} dropped={dropped_total}{out_of_range_note}{abort_note}"
    )


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 대상 날짜를 적재하고 결과를 종료 코드로 반환한다."""
    args = parse_args(argv)
    configure_batch_logging(args.source)

    scfg = config_loader.load(args.source)
    bcfg = bootstrap_config.load(args.source)
    days = resolve_dates(args)
    if bcfg.kind == "csv":
        results, aborted = _load_csv_days(args, scfg, bcfg, days), False
    else:
        results, aborted = _load_api_days(args, scfg, bcfg, days)
    print(_summary(args.source, days, results, aborted))
    return exit_code_for(results)


if __name__ == "__main__":
    sys.exit(main())
