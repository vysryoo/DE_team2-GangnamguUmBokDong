"""5분마다 도는 단일 realtime 체인.

날씨는 source별 freshness gate가 실제 시각 기준으로 수집 필요 여부를 판단한다. DAG를
cron별로 나누면 DAG마다 ``max_active_runs``가 따로 관리돼 지연된 tick끼리 겹칠 수
있으므로 한 DAG에서 태스크 의존성으로 순서를 강제한다.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import BaseOperator, chain
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    REALTIME_TICK_CRON,
    REALTIME_TICK_MAX_START_DELAY,
    REALTIME_TICK_RUN_TIMEOUT,
    TIMEZONE,
)
from config.sources import (
    BIKE_STATION_REALTIME_SOURCE,
    POPULATION_REALTIME_SOURCE,
    RENTAL_HISTORY_LOOKBACK_HOURS,
    RENTAL_HISTORY_SOURCE,
    WEATHER_3H_SOURCE,
    WEATHER_10MIN_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
)
from notifications.slack import de2_group_mention, send_message
from orchestration.collector_task import (
    build_collector_task,
    build_weather_freshness_gate_task,
)
from orchestration.refine_task import (
    build_normalizer_task,
    build_poi_master_resolve_task,
)
from orchestration.serving_task import (
    build_finalize_serving_task,
    build_inference_task,
    build_prepare_serving_task,
    build_routes_task,
    build_urgency_task,
)
from orchestration.templates import kst_window_start_shifted

logger = logging.getLogger(__name__)

# 기상청이 초단기(실황+예보)는 10분, 단기예보는 3시간보다 자주 새 값을 내지 않음
_ULTRA_WEATHER_MIN_INTERVAL = timedelta(minutes=10)
_SHORT_TERM_WEATHER_MIN_INTERVAL = timedelta(hours=3)
_WEATHER_MIN_INTERVAL_BY_SOURCE = {
    WEATHER_10MIN_SOURCE: _ULTRA_WEATHER_MIN_INTERVAL,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE: _ULTRA_WEATHER_MIN_INTERVAL,
    WEATHER_3H_SOURCE: _SHORT_TERM_WEATHER_MIN_INTERVAL,
}

# 재시도 없이 짧게 실패시켜 날씨 수집이 뒤의 serving 체인을 붙잡지 않게 함
_WEATHER_COLLECTOR_TIMEOUT = timedelta(seconds=60)


def _now() -> pendulum.DateTime:
    """현재 UTC 시각을 반환한다."""
    return pendulum.now("UTC")


def _send_slack_safely(message: str, *, log_message: str) -> None:
    """Slack 장애가 realtime 복구 흐름을 막지 않도록 전송 실패를 로그로 남긴다."""
    try:
        send_message(message)
    except Exception:
        logger.exception(log_message)


def _allow_current_tick(**context: Any) -> bool:
    """정시에 가까운 scheduled run과 모든 수동 run만 실행한다."""
    dag_run = context["dag_run"]
    if dag_run.run_type != "scheduled":
        return True
    if getattr(dag_run, "clear_number", 0):
        return True

    logical_date = dag_run.logical_date
    if logical_date is None:
        return True

    run_started_at = dag_run.start_date or _now()
    start_delay = run_started_at - logical_date
    if start_delay <= REALTIME_TICK_MAX_START_DELAY:
        return True

    message = (
        ":information_source: realtime_tick 지연 Run을 건너뜁니다.\n"
        f"• run_id: `{dag_run.run_id}`\n"
        f"• 예약 시각: `{logical_date.isoformat()}`\n"
        f"• 시작 지연: `{int(start_delay.total_seconds())}초`\n"
        "• 조치: 밀린 Run 전체를 skip하고 다음 5분 tick에서 재개합니다."
    )
    _send_slack_safely(
        message,
        log_message="realtime_tick 지연 Slack 알림 전송에 실패했습니다",
    )
    return False


def _on_realtime_tick_failure(context: dict) -> None:
    """task 콜백이 잡지 못하는 DAG Run timeout을 포함한 실패를 Slack으로 알린다."""
    dag_run = context["dag_run"]
    message = (
        f"{de2_group_mention()} :rotating_light: realtime_tick DAG Run이 실패했습니다.\n"
        f"• run_id: `{dag_run.run_id}`\n"
        f"• 예약 시각: `{dag_run.logical_date}`\n"
        f"• timeout: `{REALTIME_TICK_RUN_TIMEOUT}`\n"
        "• Airflow에서 실패 원인과 serving 게시 누락 여부를 확인해주세요."
    )
    _send_slack_safely(
        message,
        log_message="realtime_tick 실패 Slack 알림 전송에 실패했습니다",
    )


def _build_realtime_tick_dag() -> DAG:
    """stale run gate와 날씨 freshness gate를 둔 realtime 체인을 만든다."""
    with DAG(
        dag_id="realtime_tick",
        schedule=CronTriggerTimetable(REALTIME_TICK_CRON, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        dagrun_timeout=REALTIME_TICK_RUN_TIMEOUT,
        on_failure_callback=_on_realtime_tick_failure,
        tags=["realtime"],
    ) as dag:
        run_normalizer = _build_population_chain(dag)
        collect_station = build_collector_task(dag, BIKE_STATION_REALTIME_SOURCE)
        collect_rental = build_collector_task(dag, RENTAL_HISTORY_SOURCE)
        weather_ready = _build_weather_chain(dag)
        _build_serving_chain(
            dag,
            plan_upstream=[collect_station, weather_ready],
            inference_upstream=[collect_rental, run_normalizer],
        )
        _build_rental_replay_chain(dag, collect_rental)
        _gate_root_tasks(dag)
    return dag


def _build_population_chain(dag: DAG) -> BaseOperator:
    """POI Master를 고정하고 실시간 인구 수집과 normalizer가 같은 버전을 쓰게 한다."""
    resolve_poi_master = build_poi_master_resolve_task(dag)
    collect_population = build_collector_task(
        dag,
        POPULATION_REALTIME_SOURCE,
        poi_master_task_id=resolve_poi_master.task_id,
    )
    run_normalizer = build_normalizer_task(
        dag,
        poi_master_task_id=resolve_poi_master.task_id,
    )
    resolve_poi_master >> collect_population >> run_normalizer
    return run_normalizer


def _build_weather_chain(dag: DAG) -> BaseOperator:
    """source별 freshness gate 뒤에 날씨를 수집하고 항상 통과하는 합류 게이트를 반환한다."""
    weather_collect_tasks = []
    for source_id, min_interval in _WEATHER_MIN_INTERVAL_BY_SOURCE.items():
        freshness_gate = build_weather_freshness_gate_task(
            dag, source_id, min_interval=min_interval
        )
        collect = build_collector_task(
            dag,
            source_id,
            retries=0,
            execution_timeout=_WEATHER_COLLECTOR_TIMEOUT,
        )
        freshness_gate >> collect
        weather_collect_tasks.append(collect)

    # 날씨 수집이 실패하거나 스킵돼도 이 게이트는 성공하므로 prepare는 이전
    # 날씨 스냅샷으로 진행함
    weather_gate = EmptyOperator(
        task_id="weather_ready_gate",
        trigger_rule=TriggerRule.ALL_DONE,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag,
    )
    weather_collect_tasks >> weather_gate
    return weather_gate


def _build_serving_chain(
    dag: DAG,
    *,
    plan_upstream: list[BaseOperator],
    inference_upstream: list[BaseOperator],
) -> None:
    """serving plan → inference → finalize → urgency → routes 순서의 게시 체인을 만든다."""
    prepare_plan = build_prepare_serving_task(
        dag,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    run_inference = build_inference_task(dag, plan_task_id=prepare_plan.task_id)
    finalize_release = build_finalize_serving_task(
        dag,
        plan_task_id=prepare_plan.task_id,
        inference_task_id=run_inference.task_id,
    )
    publish_urgency = build_urgency_task(
        dag,
        final_task_id=finalize_release.task_id,
    )
    publish_routes = build_routes_task(
        dag,
        urgency_task_id=publish_urgency.task_id,
    )
    plan_upstream >> prepare_plan
    [prepare_plan, *inference_upstream] >> run_inference
    run_inference >> finalize_release >> publish_urgency >> publish_routes


def _build_rental_replay_chain(dag: DAG, collect_rental: BaseOperator) -> None:
    """대여 시간대가 끝난 뒤 반납된 기록을 회수하는 비차단 side chain을 만든다.

    완결된 윈도우를 다시 받아야 하므로 ``--force``를 쓰고, 현재 tick 성패와 무관하게
    실행되도록 ``ALL_DONE``으로 둠.
    """
    replays = [
        build_collector_task(
            dag,
            RENTAL_HISTORY_SOURCE,
            task_id=f"collect_bike_rental_history_replay_{hours_back}h",
            window_start=kst_window_start_shifted(hours_back),
            force=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        for hours_back in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]
    chain(collect_rental, *replays)


def _gate_root_tasks(dag: DAG) -> None:
    """지연된 scheduled run이 실제 작업을 시작하지 않도록 모든 root 앞에 gate를 둔다."""
    root_tasks = [task for task in dag.tasks if not task.upstream_task_ids]
    allow_current_tick = ShortCircuitOperator(
        task_id="allow_current_tick",
        python_callable=_allow_current_tick,
        retries=0,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag,
    )
    allow_current_tick >> root_tasks


dag = _build_realtime_tick_dag()
