"""collector CLI 태스크 빌더.

Airflow는 ``--source``와 시각만 넘기고 페이지 번호, API URL 등 collector 내부 세부사항은
알지 않는다.
"""

from __future__ import annotations

import json
import subprocess
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    EXECUTION_TIMEOUT_OVERRIDES,
)
from orchestration.refine_task import POI_MASTER_ARGS, poi_master_ref_env
from orchestration.task_builder import (
    REPO_ROOT,
    build_module_task,
    module_subprocess_env,
    uv_python_command,
)
from orchestration.templates import KST_WINDOW_START, kst_day_hour_replay_days_ago

COLLECTOR_DIR = str(REPO_ROOT / "collector")
_FRESHNESS_CHECK_TIMEOUT_SECONDS = 30
_STATS_EXECUTION_TIMEOUT = timedelta(seconds=60)


def build_collector_task(
    dag,
    source_id: str,
    *,
    task_id: str | None = None,
    window_start: str = KST_WINDOW_START,
    force: bool = False,
    poi_master_task_id: str | None = None,
    retries: int = DEFAULT_RETRIES,
    retry_delay: timedelta = DEFAULT_RETRY_DELAY,
    execution_timeout: timedelta | None = None,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
) -> BashOperator:
    """``main.py --source --window-start``로 한 윈도우를 수집하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        source_id: 수집할 source ID
        task_id: 태스크 ID. 기본값은 ``collect_{source_id}``
        window_start: ``--window-start`` 템플릿. 기본값은 현재 tick의 KST 윈도우
        force: ``--force``로 완결된 윈도우도 다시 수집할지 여부
        poi_master_task_id: 지정하면 이 resolver 태스크가 고정한 POI Master 인자를 전달
        retries: 재시도 횟수
        retry_delay: 재시도 간격
        execution_timeout: 실행 제한 시간. 기본값은 source별 override 또는 공통 기본값
        trigger_rule: 상위 태스크 상태에 따른 실행 조건
    Returns:
        구성된 BashOperator
    """
    args = ["main.py", "--source", source_id, "--window-start", window_start]
    if force:
        args.append("--force")
    env = None
    if poi_master_task_id is not None:
        args.extend(POI_MASTER_ARGS)
        env = poi_master_ref_env(poi_master_task_id)
    return build_module_task(
        dag,
        task_id or f"collect_{source_id}",
        COLLECTOR_DIR,
        uv_python_command(*args),
        execution_timeout=execution_timeout
        or EXECUTION_TIMEOUT_OVERRIDES.get(source_id, DEFAULT_EXECUTION_TIMEOUT),
        retries=retries,
        retry_delay=retry_delay,
        trigger_rule=trigger_rule,
        env=env,
    )


def build_daily_history_replay_task(dag, hour: int, days_back: int) -> BashOperator:
    """``days_back``일 전 대여이력 한 시간대를 ``--force``로 다시 수집하는 태스크를 만든다.

    시간대 하나의 실패가 나머지 시간대를 막지 않도록 ``ALL_DONE``으로 실행함.

    Args:
        dag: 태스크를 붙일 DAG
        hour: 대상 시(0~23)
        days_back: 며칠 전 날짜인지. 양수
    Returns:
        구성된 BashOperator
    """
    return build_collector_task(
        dag,
        "bike_rental_history",
        task_id=f"replay_bike_rental_history_{hour:02d}h",
        window_start=kst_day_hour_replay_days_ago(days_back, hour),
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def _check_source_due(source_id: str, min_interval_seconds: int) -> bool:
    # logical_date는 이전 run이 늦게 끝나면 실제 시각보다 뒤처지므로, 경과 시간은
    # collector가 실제 현재 시각 기준으로 판단함
    result = subprocess.run(
        [
            "uv", "run", "--frozen", "python", "main.py",
            "--source", source_id,
            "--check-due-after-seconds", str(min_interval_seconds),
        ],
        cwd=COLLECTOR_DIR,
        env=module_subprocess_env(COLLECTOR_DIR),
        capture_output=True,
        text=True,
        timeout=_FRESHNESS_CHECK_TIMEOUT_SECONDS,
        check=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    return bool(payload["due"])


def build_weather_freshness_gate_task(
    dag, source_id: str, *, min_interval: timedelta
) -> ShortCircuitOperator:
    """마지막 성공 수집이 ``min_interval`` 안이면 이 source의 수집만 건너뛰는 게이트를 만든다.

    ``ignore_downstream_trigger_rules=False``로 직접 하위인 collect 태스크만 스킵함.
    기본값(True)이면 ``weather_ready_gate`` 이후 serving 체인 전체가 스킵됨.

    Args:
        dag: 태스크를 붙일 DAG
        source_id: 확인할 source ID
        min_interval: 이 시간 안에 성공 수집이 있으면 스킵
    Returns:
        구성된 ShortCircuitOperator
    """
    return ShortCircuitOperator(
        task_id=f"freshness_gate_{source_id}",
        python_callable=lambda: _check_source_due(source_id, int(min_interval.total_seconds())),
        ignore_downstream_trigger_rules=False,
        retries=0,
        execution_timeout=timedelta(seconds=_FRESHNESS_CHECK_TIMEOUT_SECONDS),
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag,
    )


def build_window_stats_task(
    dag,
    source_id: str,
    *,
    day_template: str,
    hour_template: str | None = None,
) -> BashOperator:
    """KST 날짜(선택적으로 시)의 수집 통계를 JSON XCom으로 남기는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        source_id: 통계를 볼 source ID
        day_template: ``--window-day`` 템플릿
        hour_template: ``--window-hour`` 템플릿. 없으면 하루 전체
    Returns:
        구성된 BashOperator
    """
    args = [
        "main.py", "--report-window-stats",
        "--source", source_id,
        "--window-day", f'"{day_template}"',
    ]
    if hour_template is not None:
        args.extend(["--window-hour", f'"{hour_template}"'])
    return build_module_task(
        dag,
        f"collection_stats_{source_id}",
        COLLECTOR_DIR,
        uv_python_command(*args),
        output_processor=json.loads,
        execution_timeout=_STATS_EXECUTION_TIMEOUT,
        retries=1,
    )
