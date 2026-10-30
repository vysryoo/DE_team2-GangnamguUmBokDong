"""서울시 POI Master의 일일 refresh와 exact version resolve CLI를 제공한다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

import httpx
from core.kst import KST, parse_kst_datetime
from core.poi_master import PoiMasterError, resolve_poi_master

from publication import PoiPublicationError, refresh_poi_master
from registry import PoiRegistryError
from source import DEFAULT_DATASET_PAGE_URL, PoiSourceError, fetch_source_assets

_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=60.0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """refresh 또는 resolve 하위 명령의 CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="main.py")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh_parser = subparsers.add_parser(
        "refresh", help="공식 첨부를 확인하고 변경된 정상본만 게시한다"
    )
    refresh_parser.add_argument("--page-url", default=DEFAULT_DATASET_PAGE_URL)
    refresh_parser.add_argument("--max-drop-ratio", type=float, default=0.2)

    resolve_parser = subparsers.add_parser(
        "resolve", help="기준 시각에 활성인 exact POI Master ref를 출력한다"
    )
    resolve_parser.add_argument(
        "--as-of", required=True, help="ISO8601 timezone offset를 포함한 기준 시각"
    )
    return parser.parse_args(argv)


def _run_refresh(args: argparse.Namespace) -> int:
    """공식 첨부를 실제로 내려받아 content checksum 기준 refresh를 실행한다."""
    now = datetime.now(KST)
    with httpx.Client(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": "Gangnamgu-POI-Master/1.0"},
    ) as client:
        assets = fetch_source_assets(client, page_url=args.page_url)
    result = refresh_poi_master(
        assets,
        activated_at=now,
        max_drop_ratio=args.max_drop_ratio,
    )
    print(json.dumps(result.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0


def _run_resolve(args: argparse.Namespace) -> int:
    """기준 시각의 append-only activation 이력에서 exact ref 하나를 출력한다."""
    as_of = parse_kst_datetime(args.as_of)
    ref = resolve_poi_master(as_of)
    print(json.dumps(ref.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 하위 명령을 실행하고 운영자가 읽을 수 있는 오류와 종료 코드를 반환한다."""
    args = parse_args(argv)
    try:
        if args.command == "refresh":
            return _run_refresh(args)
        return _run_resolve(args)
    except (
        PoiMasterError,
        PoiPublicationError,
        PoiRegistryError,
        PoiSourceError,
        ValueError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
