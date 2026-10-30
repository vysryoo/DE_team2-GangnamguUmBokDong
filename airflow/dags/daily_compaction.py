"""1일 주기: D-6 대여이력을 재수집하고 Archive·Cold Bronze를 만든 뒤 오래된 객체를 정리한다.

대여이력 API는 반납 완료 건만 보여 최초 수집 때 장기 대여가 빠질 수 있다. D-6의
24개 시간대를 `--force`로 순차 재조회하고, 모든 시간대의 시도가 끝난 뒤 대여이력
compaction을 실행한다. 다른 source의 compaction은 replay와 독립적으로 실행한다.

source마다 Cold Bronze → Hot Bronze GC → Silver GC 순서로 실행해, 복구 근거가 검증된
뒤에만 원본을 지운다.
"""

import pendulum
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, COMPACTION_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import (
    COLD_BRONZE_SOURCES,
    COMPACTION_SOURCES,
    DAILY_ARCHIVE_DELAY_DAYS,
)
from orchestration.collector_task import build_daily_history_replay_task
from orchestration.compaction_task import (
    build_cold_bronze_compaction_task,
    build_compaction_task,
    build_hot_bronze_gc_task,
    build_silver_gc_task,
)
from orchestration.templates import KST_DATE, kst_date_days_ago

from airflow import DAG

SILVER_GC_RETENTION_DAYS = 30
HOT_BRONZE_RETENTION_DAYS = 30
RENTAL_HISTORY_SOURCE = "bike_rental_history"

with DAG(
    dag_id="daily_compaction",
    schedule=CronTriggerTimetable(COMPACTION_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily", "archive"],
) as dag:
    replay_chain = None
    for hour in range(24):
        replay = build_daily_history_replay_task(
            dag, hour, DAILY_ARCHIVE_DELAY_DAYS
        )
        if replay_chain is not None:
            replay_chain >> replay
        replay_chain = replay

    for source_id in COMPACTION_SOURCES:
        if source_id == RENTAL_HISTORY_SOURCE:
            replay_chain >> build_compaction_task(
                dag, source_id, trigger_rule=TriggerRule.ALL_DONE
            )
        else:
            build_compaction_task(dag, source_id)

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
            replay_chain >> cold
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
