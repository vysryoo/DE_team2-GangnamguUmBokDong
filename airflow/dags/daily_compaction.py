"""1일 주기: D-6 대여이력을 재수집하고 Archive·Cold Bronze를 만든 뒤 오래된 객체를 정리한다.

대여이력 API는 반납 완료 건만 보여 최초 수집 때 장기 대여가 빠질 수 있다. D-6의
24개 시간대를 `--force`로 순차 재조회하고, 모든 시간대의 시도가 끝난 뒤 대여이력
compaction을 실행한다. 다른 source의 compaction은 replay와 독립적으로 실행한다.

source마다 Cold Bronze → Hot Bronze GC → Silver GC 순서로 실행해, 복구 근거가 검증된
뒤에만 원본을 지운다.
"""

import pendulum
from airflow import DAG
from airflow.sdk import BaseOperator, chain
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, COMPACTION_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import (
    COLD_BRONZE_SOURCES,
    COMPACTION_SOURCES,
    DAILY_ARCHIVE_DELAY_DAYS,
    RENTAL_HISTORY_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.compaction_task import (
    build_cold_bronze_compaction_task,
    build_compaction_task,
    build_hot_bronze_gc_task,
    build_silver_gc_task,
)
from orchestration.templates import (
    KST_DATE,
    kst_date_days_ago,
    kst_day_hour_replay_days_ago,
)

SILVER_GC_RETENTION_DAYS = 30
HOT_BRONZE_RETENTION_DAYS = 30


def _build_daily_compaction_dag() -> DAG:
    """D-6 대여이력 replay, Archive compaction, Cold Bronze와 GC 체인을 만든다."""
    with DAG(
        dag_id="daily_compaction",
        schedule=CronTriggerTimetable(COMPACTION_CRON, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        tags=["daily", "archive"],
    ) as dag:
        replay_end = _build_rental_replay_chain(dag)
        _build_compaction_tasks(dag, replay_end)
        _build_cold_bronze_and_gc_chains(dag, replay_end)
    return dag


def _build_rental_replay_chain(dag: DAG) -> BaseOperator:
    """D-6의 24개 시간대를 순차 재수집하고 마지막 replay 태스크를 반환한다."""
    # 시간대 하나의 실패가 나머지 시간대를 막지 않도록 ALL_DONE으로 순차 실행함
    replays = [
        build_collector_task(
            dag,
            RENTAL_HISTORY_SOURCE,
            task_id=f"replay_bike_rental_history_{hour:02d}h",
            window_start=kst_day_hour_replay_days_ago(DAILY_ARCHIVE_DELAY_DAYS, hour),
            force=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        for hour in range(24)
    ]
    chain(*replays)
    return replays[-1]


def _build_compaction_tasks(dag: DAG, replay_end: BaseOperator) -> None:
    """대여이력은 replay가 모두 끝난 뒤, 나머지 source는 독립적으로 압축한다."""
    for source_id in COMPACTION_SOURCES:
        if source_id == RENTAL_HISTORY_SOURCE:
            replay_end >> build_compaction_task(
                dag, source_id, trigger_rule=TriggerRule.ALL_DONE
            )
        else:
            build_compaction_task(dag, source_id)


def _build_cold_bronze_and_gc_chains(dag: DAG, replay_end: BaseOperator) -> None:
    """source마다 Cold Bronze → Hot Bronze GC → Silver GC 순서로 잇는다."""
    silver_gc_date = kst_date_days_ago(DAILY_ARCHIVE_DELAY_DAYS + SILVER_GC_RETENTION_DAYS)
    for source_id in COLD_BRONZE_SOURCES:
        is_rental = source_id == RENTAL_HISTORY_SOURCE
        cold = build_cold_bronze_compaction_task(
            dag,
            source_id,
            today=KST_DATE,
            delay_days=DAILY_ARCHIVE_DELAY_DAYS,
            # 한 시간대 replay 실패가 그 날짜의 Cold 보존을 막지 않게 함
            trigger_rule=TriggerRule.ALL_DONE if is_rental else TriggerRule.ALL_SUCCESS,
        )
        if is_rental:
            replay_end >> cold
        hot_gc = build_hot_bronze_gc_task(
            dag,
            source_id,
            today=KST_DATE,
            retention_days=HOT_BRONZE_RETENTION_DAYS,
        )
        silver_gc = build_silver_gc_task(
            dag,
            source_id,
            silver_gc_date,
            require_archive=source_id in COMPACTION_SOURCES,
        )
        cold >> hot_gc >> silver_gc


dag = _build_daily_compaction_dag()
