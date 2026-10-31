"""collector CLI 진입점.

- `--window-start`: window 하나 수집. `--force`면 새 Hot Bronze revision에 전체 재수집
- `--check-due-after-seconds N`: freshness gate 판정 JSON 출력
- `--report-window-stats --window-day [--window-hour]`: 수집 통계 JSON 출력

수집 모드의 종료 코드는 `SUCCEEDED`·`PARTIAL`·`EMPTY`·`SKIPPED`가 0, `FAILED`가
non-zero다. 인증키는 환경변수(`SEOUL_OPENAPI_KEY`, `KMA_APIHUB_KEY`)에서만 읽는다.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime

import adapters  # noqa: F401 (어댑터 레지스트리 로드용)
import config.loader as config_loader
import httpx
import manifest as manifest_module
import pipeline
import storage
from adapters.kma_apihub import adjust_base_time
from config.schema import SourceConfig
from core.kst import KST, parse_kst_datetime
from core.poi_master import PoiMasterRef, read_poi_master
from core.sources import POPULATION_REALTIME
from logging_setup import configure_httpx_request_logging, configure_logging
from manifest import RunStatus

_OK_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED})

# 서울 1000행 페이지 응답이 최대 7초 안팎까지 걸려 httpx 기본값(5초)이면 라운드 재시도로
# fetch 예산을 태운다. read는 그 4배로 잡는다.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    Args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 쓴다.
    Returns:
        실행 모드와 POI Master ref 필드를 담은 네임스페이스.
    Raises:
        SystemExit: 모드에 필요한 인자가 없거나 POI Master 인자 조합이 잘못됐을 때.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Airflow는 static 모드에서도 같은 명령 형태로 빈 ref 값을 넘긴다.
    for field in ("poi_master_manifest_uri", "poi_master_manifest_sha256"):
        value = getattr(args, field)
        if value is not None and not value.strip():
            setattr(args, field, None)

    if (
        args.check_due_after_seconds is None
        and not args.report_window_stats
        and not args.window_start
    ):
        parser.error(
            "the following arguments are required: --window-start "
            "(unless --check-due-after-seconds/--report-window-stats is used)"
        )
    if args.report_window_stats and not args.window_day:
        parser.error("--report-window-stats에는 --window-day가 필요하다")
    _check_poi_master_args(parser, args)
    return args


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--source", required=True, help="소스 id (sources/{source_id}.yaml)")
    parser.add_argument("--window-start", help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument(
        "--force",
        action="store_true",
        help="재개 분기를 무시하고 새 Hot Bronze revision에 전체 재수집",
    )
    parser.add_argument(
        "--report-window-stats",
        action="store_true",
        help="해당 KST 날짜(및 선택적으로 시)의 수집 통계를 JSON으로 출력하고 종료",
    )
    parser.add_argument("--window-day", help="--report-window-stats에 사용할 KST 날짜(YYYY-MM-DD)")
    parser.add_argument(
        "--window-hour", help="--report-window-stats에 사용할 KST 시(HH). 생략하면 하루 전체를 본다"
    )
    parser.add_argument(
        "--poi-master-mode",
        choices=("static", "s3"),
        default="static",
        help="population_realtime 호출 대상을 기존 YAML(static) 또는 exact S3 POI Master에서 읽는다",
    )
    parser.add_argument(
        "--poi-master-manifest-uri",
        help="s3 모드에서 사용할 immutable POI Master manifest의 exact URI",
    )
    parser.add_argument(
        "--poi-master-manifest-sha256",
        help="s3 모드에서 사용할 POI Master manifest bytes의 SHA-256",
    )
    parser.add_argument(
        "--check-due-after-seconds",
        type=int,
        default=None,
        help="마지막 성공 수집 이후 이 초만큼 안 지났으면 수집을 건너뛰어도 되는지 JSON으로 출력하고 종료",
    )
    return parser


def _check_poi_master_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    has_manifest_ref = (
        args.poi_master_manifest_uri is not None
        or args.poi_master_manifest_sha256 is not None
    )
    if args.source != POPULATION_REALTIME and (
        args.poi_master_mode != "static" or has_manifest_ref
    ):
        parser.error("POI Master 인자는 population_realtime 소스에만 사용할 수 있다")
    if args.poi_master_mode == "static" and has_manifest_ref:
        parser.error("static 모드에는 POI Master manifest ref를 지정할 수 없다")
    if args.poi_master_mode == "s3" and (
        args.poi_master_manifest_uri is None
        or args.poi_master_manifest_sha256 is None
    ):
        parser.error("s3 모드에는 POI Master manifest URI와 SHA-256이 모두 필요하다")


def _config_with_poi_master(config: SourceConfig, ref: PoiMasterRef) -> SourceConfig:
    """Exact S3 POI Master의 코드 목록을 실행 설정에 고정한다.

    Master manifest SHA를 YAML 설정 hash와 함께 다시 hash해 source snapshot의
    `config_version`에 호출 목록의 외부 의존성을 반영한다. AREA_CD 형식·고유성·정렬은
    `read_poi_master`가 검증한다.

    Args:
        config: YAML에서 읽은 SourceConfig.
        ref: Airflow가 이번 tick에 고정한 exact S3 POI Master 참조.
    Returns:
        `poi_codes`와 합성 config version이 주입된 새 SourceConfig.
    """
    table = read_poi_master(ref, columns=["AREA_CD"])
    poi_codes = tuple(table.column("AREA_CD").to_pylist())
    version_material = (
        f"collector_config={config.config_version}\n"
        f"poi_master_manifest_sha256={ref.manifest_sha256}\n"
    ).encode("ascii")
    return config.model_copy(
        update={
            "adapter_params": {**config.adapter_params, "poi_codes": poi_codes},
            "config_version": f"sha256:{hashlib.sha256(version_material).hexdigest()}",
        }
    )


def exit_code_for(status: RunStatus) -> int:
    """manifest의 최종 status를 프로세스 종료 코드로 바꾼다.

    Args:
        status: pipeline이 반환한 manifest의 최종 status.
    Returns:
        `SUCCEEDED`·`PARTIAL`·`EMPTY`·`SKIPPED`면 0, 그 외는 1.
    """
    return 0 if status in _OK_STATUSES else 1


def _report_window_stats(args: argparse.Namespace) -> None:
    """KST 날짜(및 시)의 window manifest 통계를 JSON으로 출력한다."""
    day = date.fromisoformat(args.window_day)
    manifests = manifest_module.load_window_manifests(args.source, day, args.window_hour)
    summary = manifest_module.summarize_window(manifests)
    summary.update({"source_id": args.source, "day": args.window_day, "hour": args.window_hour})
    print(json.dumps(summary))


def _is_due(
    config: SourceConfig, last: datetime | None, now: datetime, due_after_seconds: int
) -> bool:
    """마지막 authority 이후 새 수집이 필요한지 판정한다."""
    if last is None:
        return True

    # 실행 시각이 아니라 마지막 성공이 담당한 슬롯으로 비교해야 한 번 늦은 성공의 지연이
    # 다음 판정에 누적되지 않는다. 초단기실황·예보는 API 발표 그리드(`time_rule`)와 실제
    # 갱신 주기가 달라 `freshness_rule`을 따로 둔다.
    rule = config.adapter_params.get("freshness_rule") or config.adapter_params.get("time_rule")
    if rule:
        time_based_due = adjust_base_time(now, rule) > adjust_base_time(last, rule)
    else:
        time_based_due = (now - last).total_seconds() >= due_after_seconds
    # 설정이 바뀌어 배포됐으면 같은 슬롯이라도 새 설정으로 다시 수집한다.
    return time_based_due or not _latest_source_uses_config(
        config.source_id, last, config.config_version
    )


def _latest_source_uses_config(
    source_id: str, logical_dttm: datetime, config_version: str
) -> bool:
    """가장 최근 authority가 현재 배포 source 설정으로 수집됐는지 확인한다."""
    snapshots = manifest_module.load_source_snapshots(source_id, logical_dttm)
    return bool(snapshots and snapshots[-1].manifest.config_version == config_version)


def _check_due(args: argparse.Namespace) -> None:
    """freshness gate 판정 결과를 JSON으로 출력한다."""
    config = config_loader.load(args.source)
    now = datetime.now(KST)
    last = storage.latest_source_snapshot_logical_dttm(args.source, as_of=now)
    print(json.dumps({
        "source_id": args.source,
        "due": _is_due(config, last, now, args.check_due_after_seconds),
        "last_logical_dttm": None if last is None else last.isoformat(),
        "elapsed_seconds": None if last is None else (now - last).total_seconds(),
    }))


def _collect(args: argparse.Namespace) -> int:
    """window 하나를 수집하고 종료 코드를 반환한다."""
    window_start = parse_kst_datetime(args.window_start)
    # config 로드보다 먼저 설정해야 이후 모든 로그에 고정 필드가 붙는다.
    configure_logging(args.source, window_start, attempt=1)
    config = config_loader.load(args.source)
    configure_httpx_request_logging(config.adapter)
    if args.poi_master_mode == "s3":
        ref = PoiMasterRef(
            mode="s3",
            manifest_uri=args.poi_master_manifest_uri,
            manifest_sha256=args.poi_master_manifest_sha256,
        )
        config = _config_with_poi_master(config, ref)

    with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
        result = pipeline.execute_window(
            config, window_start, client=client, force=args.force
        )
    return exit_code_for(result.status)


def main(argv: list[str] | None = None) -> int:
    """인자로 고른 모드를 실행하고 종료 코드를 반환한다.

    Args:
        argv: `parse_args`에 그대로 전달할 인자 목록.
    Returns:
        Airflow가 태스크 성공/실패를 판단할 프로세스 종료 코드.
    """
    args = parse_args(argv)
    if args.report_window_stats:
        _report_window_stats(args)
        return 0
    if args.check_due_after_seconds is not None:
        _check_due(args)
        return 0
    return _collect(args)


if __name__ == "__main__":
    sys.exit(main())
