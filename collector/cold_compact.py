"""날짜별 Cold Bronze compaction CLI다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from core.kst import today_kst

import cold_bronze


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Source와 대상 날짜 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="cold_compact.py")
    parser.add_argument("--source", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--date", type=date.fromisoformat)
    mode.add_argument("--recover-pending", action="store_true")
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument("--delay-days", type=int, default=6)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Cold Bronze를 생성하고 기계 판독 가능한 결과 한 줄을 출력한다."""
    args = parse_args(argv)
    if args.recover_pending:
        today = args.today or today_kst()
        result = cold_bronze.recover_pending(
            args.source, today=today, delay_days=args.delay_days
        )
        print(
            json.dumps(
                {"status": "completed", "dates": result.dates, "objects": result.objects}
            )
        )
        return 0
    result = cold_bronze.compact_date(args.source, args.date)
    print(json.dumps({"status": result.status, "objects": result.objects, "cold_key": result.cold_key}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
