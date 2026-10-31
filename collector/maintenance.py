"""레이크 유지보수 작업(Archive·Cold Bronze·GC)을 실행하는 CLI다.

    cd collector
    uv run --frozen python maintenance.py compact --source bike_station_realtime [--date D] [--force]
    uv run --frozen python maintenance.py cold-compact --source X --today D --delay-days 6
    uv run --frozen python maintenance.py gc-hot-bronze --source X --today D --retention-days 30
    uv run --frozen python maintenance.py gc-silver --source X --date D [--require-archive]

각 명령은 결과를 JSON 한 줄로 출력한다. 실패한 날짜가 있으면 non-zero로 끝난다.

- `compact`(`compaction.py`): 날짜의 authority Silver를 Archive parquet 하나로 묶음.
  authority는 window마다 최신 source snapshot이 가리키는 Silver이고, 그 목록으로 만든
  `silver_signature`가 archive manifest와 같으면 다시 쓰지 않음
- `cold-compact`(`cold_bronze.py`): Hot Bronze를 쓸 때 남긴 pending marker 중 기한이 된
  날짜의 모든 Hot revision을 원본 bytes 그대로 Cold Bronze parquet 하나에 보관함.
  Cold manifest가 그 날짜의 최신 Cold를 가리킴
- `gc-hot-bronze`(`hot_bronze_gc.py`): Cold inventory에 들어 있고 보존기한이 지난 Hot
  Bronze만 삭제함
- `gc-silver`(`silver_gc.py`): Cold가 있고 Archive가 현재 authority와 같은 날짜에서 보존기한이
  지난 non-authority Silver만 삭제함

두 GC는 삭제 전후로 GC manifest(`gc_manifest.py`)를 남긴다. Airflow는 source마다
`cold-compact` → `gc-hot-bronze` → `gc-silver` 순서로 실행한다.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict
from datetime import date

from core.kst import today_kst

import cold_bronze
import compaction
import config.loader as config_loader
import hot_bronze_gc
import silver_gc
from logging_setup import configure_batch_logging


def _compact(args: argparse.Namespace) -> int:
    config = config_loader.load(args.source)
    days = [args.date] if args.date else compaction.target_dates(args.today)
    results = [compaction.compact_date(config, day, force=args.force) for day in days]
    statuses = Counter(result.status for result in results)
    print(json.dumps({"source": args.source, "dates": len(days), **statuses}))
    return 1 if statuses["failed"] else 0


def _cold_compact(args: argparse.Namespace) -> int:
    result = cold_bronze.recover_pending(
        args.source, today=args.today, delay_days=args.delay_days
    )
    print(json.dumps(asdict(result)))
    return 0


def _gc_hot_bronze(args: argparse.Namespace) -> int:
    result = hot_bronze_gc.recover_due(
        args.source, today=args.today, retention_days=args.retention_days
    )
    print(json.dumps(asdict(result)))
    return 0


def _gc_silver(args: argparse.Namespace) -> int:
    result = silver_gc.collect_date(
        config_loader.load(args.source), args.date, require_archive=args.require_archive
    )
    print(json.dumps(asdict(result)))
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """하위 명령과 인자를 파싱한다.

    Args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`
    Returns:
        `run`에 실행할 함수를 담은 네임스페이스
    Raises:
        SystemExit: 하위 명령이나 필수 인자가 없을 때
    """
    parser = argparse.ArgumentParser(prog="maintenance.py")
    commands = parser.add_subparsers(required=True)

    def add_command(name, run, help_text):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--source", required=True, help="소스 id (sources/{source_id}.yaml)")
        command.add_argument("--today", type=date.fromisoformat, default=today_kst(), help="기준 KST 날짜")
        command.set_defaults(run=run)
        return command

    compact = add_command("compact", _compact, "최근 날짜의 Silver를 Archive로 압축")
    compact.add_argument("--date", type=date.fromisoformat, help="이 날짜 하나만 압축")
    compact.add_argument("--force", action="store_true", help="변경이 없어도 다시 압축")

    cold = add_command("cold-compact", _cold_compact, "기한이 된 Hot Bronze 날짜를 Cold로 묶음")
    cold.add_argument("--delay-days", type=int, default=6)

    hot_gc = add_command("gc-hot-bronze", _gc_hot_bronze, "Cold에 포함된 오래된 Hot Bronze 삭제")
    hot_gc.add_argument("--retention-days", type=int, default=30)

    silver = add_command("gc-silver", _gc_silver, "오래된 non-authority Silver 삭제")
    silver.add_argument("--date", type=date.fromisoformat, required=True)
    silver.add_argument("--require-archive", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """하위 명령을 실행하고 종료 코드를 반환한다."""
    args = parse_args(argv)
    configure_batch_logging(args.source)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
