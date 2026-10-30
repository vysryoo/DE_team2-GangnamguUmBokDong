"""검증된 Cold manifest 기반 Hot Bronze GC CLI다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from core.kst import today_kst

import hot_bronze_gc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hot_bronze_gc_cli.py")
    parser.add_argument("--source", required=True)
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument("--retention-days", type=int, default=30)
    args = parser.parse_args(argv)
    today = args.today or today_kst()
    result = hot_bronze_gc.recover_due(
        args.source, today=today, retention_days=args.retention_days
    )
    print(
        json.dumps(
            {
                "status": "completed",
                "dates": result.dates,
                "deleted": result.deleted,
                "retained": result.retained,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
