"""CLI 진입점 — 인자 파싱 후 pipeline을 호출한다.

    cd collector
    uv run python main.py --source bike_station_realtime \
        --window-start 2026-08-12T23:10:00+09:00 [--force]

Airflow는 소스별 태스크에서 논리 시각을 KST로 변환해 `--window-start`로 넘긴다.
일반 재실행은 manifest가 가리키는 기존 Bronze를 재사용한다.
`window_end`는 config의 `schedule.interval`로 계산하며, collector 자체는 스케줄을
모른다.

`--force`는 재개 분기를 무시하고 새 Hot Bronze revision에 전체 재수집한다.

종료 코드는 `SUCCEEDED`·`PARTIAL`·`EMPTY`·`SKIPPED`가 0, `FAILED`가 non-zero다.
누락이 있어도 source별 허용 게이트를 통과했으면 `PARTIAL`(0)로 downstream task를
스케줄한다. PARTIAL Silver의 실제 사용 여부는 소비자별 정책이다. 누락 허용치를
초과하면 `FAILED/fetch_error`가 되며, 이를 포함한 모든 `FAILED`와 처리되지 않은 예외는
non-zero로 Airflow retry 대상이다.

주의:
- 스택 트레이스를 그대로 뱉지 않는다. 실패는 manifest에 남기고 정리된 메시지와
  종료 코드로 전달한다.
- 인증키를 인자로 받지 않는다. 키는 환경변수(`SEOUL_OPENAPI_KEY`, `KMA_APIHUB_KEY`)
  에서만 읽는다.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime

import adapters  # noqa: F401 (어댑터 레지스트리 로드용)
import config.loader as config_loader
import httpx
import manifest as manifest_module
import pipeline

# pyrefly: ignore [missing-import]
import pyarrow as pa
import storage
from config.schema import SourceConfig
from core.kst import KST, parse_kst_datetime
from core.poi_master import PoiMasterRef, read_poi_master
from core.sources import POPULATION_REALTIME
from logging_setup import configure_httpx_request_logging, configure_logging
from manifest import RunStatus

_OK_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED})
_POI_CODE_PATTERN = re.compile(r"POI[0-9]{3}\Z")

# httpx 기본값은 모든 단계에 5초다. 서울 열린데이터광장의 1000행 페이지 응답 시간을
# 실측하면 시점에 따라 0.6~7.2초로 흔들려서, 기본값이면 느린 시점에 페이지마다
# ReadTimeout으로 5초를 버리고 라운드 재시도(15s·30s 대기)로 넘어간다. 데이터를
# 잃지는 않지만 fetch 예산(`effective_fetch_budget`)을 헛되게 태운다.
#
# read를 30초로 두는 근거는 실측 최댓값(7.2초)의 4배 여유다. connect는 짧게 둔다 —
# 연결이 안 되는 상황은 기다려서 나아지지 않고, TRANSIENT로 라운드가 재시도한다.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)


def _latest_source_uses_config(
    source_id: str, logical_dttm: datetime, config_version: str
) -> bool:
    """가장 최근 authority가 현재 배포 source 설정으로 수집됐는지 확인한다."""

    snapshots = manifest_module.load_source_snapshots(source_id, logical_dttm)
    return bool(
        snapshots and snapshots[-1].manifest.config_version == config_version
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 그대로 쓴다.
    returns:
        실행 window와 선택한 POI Master ref 필드를 담은 네임스페이스.
    raises:
        SystemExit: 필수 인자가 없거나 POI Master 인자 조합이 잘못됐을 때.
    """
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
    args = parser.parse_args(argv)

    # Airflow는 static/S3를 같은 command shape으로 호출하고 static ref 필드는 빈
    # 환경변수로 넘긴다. 빈 문자열을 "ref가 지정됨"으로 오인하지 않도록 여기서
    # 생략값과 같은 None으로 정규화한다.
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

    return args


def _validated_master_poi_codes(table: pa.Table) -> tuple[str, ...]:
    """POI Master의 AREA_CD를 검증하고 코드 오름차순의 불변 튜플로 반환한다.

    args:
        table: exact POI Master manifest가 가리키는 Parquet 테이블
    returns:
        중복 없이 정렬된 POI 코드 튜플
    raises:
        ValueError: 테이블 타입·컬럼·코드 형식·중복 또는 빈 목록이 잘못됐을 때
    """
    if type(table) is not pa.Table:
        raise ValueError("POI Master reader는 pyarrow.Table을 반환해야 합니다")
    if table.column_names != ["AREA_CD"]:
        raise ValueError(
            "POI Master 코드 조회 결과는 AREA_CD 컬럼 하나여야 합니다: "
            f"columns={table.column_names}"
        )

    raw_codes = table.column("AREA_CD").to_pylist()
    if not raw_codes:
        raise ValueError("POI Master의 AREA_CD 목록이 비어 있습니다")
    if any(
        type(code) is not str or _POI_CODE_PATTERN.fullmatch(code) is None
        for code in raw_codes
    ):
        raise ValueError("POI Master AREA_CD는 POI와 ASCII 숫자 3자리 형식이어야 합니다")
    if len(set(raw_codes)) != len(raw_codes):
        raise ValueError("POI Master AREA_CD에 중복 코드가 있습니다")
    return tuple(sorted(raw_codes))


def _config_with_poi_master(
    config: SourceConfig,
    ref: PoiMasterRef,
) -> SourceConfig:
    """Exact S3 POI Master를 한 번 읽어 Collector 실행 설정에 고정한다.

    Master manifest SHA를 YAML 설정 hash와 함께 다시 hash하여 source snapshot의
    ``config_version``에도 호출 목록의 외부 의존성이 반영되게 한다.

    args:
        config: YAML에서 읽은 frozen SourceConfig
        ref: Airflow가 이번 tick에 고정한 exact S3 POI Master 참조
    returns:
        정렬된 ``poi_codes``와 합성 config version이 주입된 새 SourceConfig
    """
    table = read_poi_master(ref, columns=["AREA_CD"])
    poi_codes = _validated_master_poi_codes(table)
    manifest_sha256 = ref.manifest_sha256
    if manifest_sha256 is None:
        raise ValueError("s3 POI Master ref에는 manifest SHA-256이 필요합니다")

    version_material = (
        f"collector_config={config.config_version}\n"
        f"poi_master_manifest_sha256={manifest_sha256}\n"
    ).encode("ascii")
    config_version = f"sha256:{hashlib.sha256(version_material).hexdigest()}"
    adapter_params = {**config.adapter_params, "poi_codes": poi_codes}
    return config.model_copy(
        update={
            "adapter_params": adapter_params,
            "config_version": config_version,
        }
    )


def exit_code_for(status: RunStatus) -> int:
    """manifest의 최종 status를 프로세스 종료 코드로 바꾼다.

    `SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0, `FAILED`는 non-zero다.
    누락을 허용하지 않는 소스는 pipeline 완결도 게이트에서 `FAILED`가 되므로
    이 매핑을 바꾸지 않아도 Airflow retry가 동작한다.

    args:
        status: pipeline이 반환한 manifest의 최종 status.
    returns:
        Airflow 태스크 성공/실패를 가르는 프로세스 종료 코드.
    """
    return 0 if status in _OK_STATUSES else 1


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 window 하나를 수집하고 그 결과를 종료 코드로 반환한다.

    처리 순서는 인자 파싱 → 로깅 초기화 → config 로드 → pipeline 실행 → 종료
    코드 반환이다. 로깅 초기화를 config 로드보다 먼저 하는 이유는, 
    이후 pipeline이 남기는 모든 로그에 고정 필드(source_id·window·attempt)가
    붙게 하기 위해서다 — 순서를 바꾸면 pipeline이 로드 직후에 남기는 초반 로그에는 고정 필드가 빠진다.

    args:
        argv: `parse_args`에 그대로 전달할 인자 목록.
    returns:
        Airflow가 태스크 성공/실패를 판단할 프로세스 종료 코드.
    """
    args = parse_args(argv)

    if args.report_window_stats:
        import json
        from datetime import date as date_cls

        day = date_cls.fromisoformat(args.window_day)
        manifests = manifest_module.load_window_manifests(args.source, day, args.window_hour)
        summary = manifest_module.summarize_window(manifests)
        summary.update({
            "source_id": args.source,
            "day": args.window_day,
            "hour": args.window_hour,
        })
        print(json.dumps(summary))
        return 0

    if args.check_due_after_seconds is not None:
        import json

        config = config_loader.load(args.source)
        now = datetime.now(KST)
        last = storage.latest_source_snapshot_logical_dttm(args.source, as_of=now)
        elapsed_seconds = None if last is None else (now - last).total_seconds()
        time_rule = config.adapter_params.get("time_rule")
        # freshness 판단은 fetch용 time_rule(API가 허용하는 발표 그리드)이 아니라
        # 이 소스가 실제로 갱신되는 폴링 그리드를 봐야 한다 — 단기예보(vilage_fcst,
        # 3시간)는 둘이 같지만, 초단기실황/예보는 API 파라미터 그리드(hourly/
        # half_hourly, 시간당 1슬롯)와 실제 갱신 주기(10분/30분)가 다르다. 이
        # 둘을 혼동해서 hourly/half_hourly를 freshness에 그대로 썼다가 실제
        # 운영에서 확인(2026-08-26): 시간당 6번(10분 간격) 잘 수집되던 게 시간당
        # 1번으로 뚝 떨어졌다 — 그래서 `freshness_rule`을 별도 필드로 분리해
        # ten_minutely/thirty_minutely 같은 순수 폴링 그리드를 쓰게 한다
        # (`adapters.kma_apihub` 모듈 docstring 표 참고).
        freshness_rule = config.adapter_params.get("freshness_rule") or time_rule

        if last is None:
            time_based_due = True
        elif freshness_rule:
            # "마지막 성공이 wall-clock으로 언제 실행됐는지"가 아니라 "그 실행이
            # 실제로 담당했던 그리드 슬롯이 무엇인지"로 판단해야, 한 번이라도
            # 늦게 성공할 때마다 그 지연이 다음 판단 기준에 그대로 누적돼 실제
            # 슬롯과 점점 어긋나는 문제를 막는다 — 실제 운영에서 확인
            # (2026-08-26, weather_short_term_forecast manifest logical_dttm이
            # 09:00→12:00→15:00→16:00→19:00→22:00→01:00→04:00→07:00(KST)처럼
            # 3시간 간격의 wall-clock 성공 시각만 계속 이어져서, 08:00 발표분이
            # 10:00이 돼서야 잡혔다).
            from adapters.kma_apihub import adjust_base_time

            time_based_due = adjust_base_time(now, freshness_rule) > adjust_base_time(
                last, freshness_rule
            )
        else:
            time_based_due = elapsed_seconds >= args.check_due_after_seconds

        due = time_based_due or not _latest_source_uses_config(
            args.source, last, config.config_version
        )
        print(json.dumps({
            "source_id": args.source,
            "due": due,
            "last_logical_dttm": None if last is None else last.isoformat(),
            "elapsed_seconds": elapsed_seconds,
        }))
        return 0

    window_start = parse_kst_datetime(args.window_start)

    configure_logging(args.source, window_start, attempt=1)
    config = config_loader.load(args.source)
    configure_httpx_request_logging(getattr(config, "adapter", ""))
    if args.poi_master_mode == "s3":
        ref = PoiMasterRef(
            mode="s3",
            manifest_uri=args.poi_master_manifest_uri,
            manifest_sha256=args.poi_master_manifest_sha256,
        )
        config = _config_with_poi_master(config, ref)

    # httpx.Client를 여기서 만들어 실행 하나에 재사용한다
    # 어댑터는 연결을 모르고 `client` 인자로 주입받기만 한다
    with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
        result = pipeline.execute_window(
            config, window_start, client=client, force=args.force,
        )

    return exit_code_for(result.status)


if __name__ == "__main__":
    sys.exit(main())
