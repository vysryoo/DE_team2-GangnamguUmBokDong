"""CLI 진입점으로 인자를 파싱해 하루치 silver를 archive로 압축한다.

## 실행 방법

    cd collector
    uv run --frozen python compact.py --source bike_station_realtime
    uv run --frozen python compact.py --source bike_rental_history --date 2026-08-12
    uv run --frozen python compact.py --source weather_ultra_short_live \
        --from 2026-07-01 --to 2026-08-17 [--force]

- Airflow는 소스별 태스크에서 인자 없이 호출한다. 검사 범위는 소스 설정에서 유도한다.
- `--date`는 하루만, `--from/--to`는 지정 구간만 처리한다. 둘은 함께 줄 수 없다.
- `--force`는 변경이 없어도 다시 압축한다.

## 날짜 선택

기본값은 `compaction.target_dates()`가 계산한다 — 백필 창과 배치 복구 하한 중 큰 쪽이다.
`--from/--to`는 이 계산을 우회한다. 도입 시점에 이미 쌓여 있는 silver를 일괄 압축하거나
임의 구간을 재처리할 때 쓰며, 검사 범위보다 훨씬 길 수 있다.

## 수집 CLI와 분리한 이유

`main.py`는 `--source --window-start`로 윈도우 하나를 수집하는 전용 CLI다. 날짜 단위
배치를 같은 진입점에 넣으면 인자 체계와 의미가 섞인다.

## 종료 코드

한 날짜라도 실패하면 non-zero다. 변경 감지 덕에 실패는 그 날짜에만 국한되고, 고치기
전까지 매일 같은 날짜만 재시도된다.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import compaction
import config.loader as config_loader
from core.kst import today_kst
from logging_setup import configure_batch_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    `--date`와 `--from/--to`는 날짜를 고르는 방식이 서로 달라 함께 주면 막는다.
    `--from`과 `--to`는 한쪽만 주면 구간이 성립하지 않으므로 역시 막는다.

    args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 그대로 쓴다.
    returns:
        `source` · `date` · `from` · `to` · `force` 필드를 담은 네임스페이스.
    raises:
        SystemExit: 필수 인자가 없거나 날짜 선택 방식이 충돌할 때.
    """
    parser = argparse.ArgumentParser(prog="compact.py")
    parser.add_argument("--source", required=True, help="소스 id (sources/{source_id}.yaml)")
    parser.add_argument("--date", help="이 날짜 하나만 압축 (YYYY-MM-DD)")
    parser.add_argument("--from", dest="from", help="구간 시작 (YYYY-MM-DD), --to와 함께")
    parser.add_argument("--to", help="구간 끝, 포함 (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="변경이 없어도 다시 압축")
    args = parser.parse_args(argv)

    start, end = getattr(args, "from"), args.to
    if args.date and (start or end):
        parser.error("--date와 --from/--to는 함께 줄 수 없다")
    if bool(start) != bool(end):
        parser.error("--from과 --to는 함께 줘야 한다")

    return args


def resolve_dates(args: argparse.Namespace, config, today: date) -> list[date]:
    """인자와 소스 설정에서 처리할 날짜 목록을 오름차순으로 만든다.

    args:
        args: `parse_args`가 반환한 네임스페이스
        config: 대상 소스 설정. 기본 검사 범위를 유도하는 데 쓴다.
        today: 기준일
    returns:
        처리할 날짜 목록
    raises:
        SystemExit: `--from`이 `--to`보다 뒤일 때.
    """
    if args.date:
        return [date.fromisoformat(args.date)]

    start = getattr(args, "from")
    if start:
        first, last = date.fromisoformat(start), date.fromisoformat(args.to)
        if first > last:
            raise SystemExit(f"--from({first})이 --to({last})보다 뒤다")
        return [first + timedelta(days=n) for n in range((last - first).days + 1)]

    return compaction.target_dates(config, today)


def exit_code_for(results: list[compaction.DateResult]) -> int:
    """압축 결과를 프로세스 종료 코드로 바꾼다. 하나라도 실패했으면 non-zero."""
    return 1 if any(r.status == "failed" for r in results) else 0


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 대상 날짜를 압축하고 그 결과를 종료 코드로 반환한다."""
    args = parse_args(argv)

    configure_batch_logging(args.source)
    config = config_loader.load(args.source)

    today = today_kst()
    days = resolve_dates(args, config, today)
    results = compaction.compact_range(config, days, today=today, force=args.force)

    tally: dict[str, int] = {}
    for result in results:
        tally[result.status] = tally.get(result.status, 0) + 1
    summary = " ".join(f"{status}={count}" for status, count in sorted(tally.items()))
    print(f"source={args.source} dates={len(days)} {summary}")

    return exit_code_for(results)


if __name__ == "__main__":
    sys.exit(main())
