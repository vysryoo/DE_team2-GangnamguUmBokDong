"""1일 주기 생활인구 추정과 source-scoped event publication DAG.

생활인구와 두 행사 source 브랜치는 서로 독립이다.
"""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    CATCHUP,
    DAILY_CRON,
    MAX_ACTIVE_RUNS,
    TIMEZONE,
)
from config.sources import (
    DAILY_EVENT_SOURCE,
    DAILY_POPULATION_SOURCE,
    PERFORMANCE_EVENT_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.refine_task import build_nowcasting_task
from orchestration.serving_task import build_event_publication_task

with DAG(
    dag_id="daily_population_and_events",
    schedule=CronTriggerTimetable(DAILY_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily"],
) as dag:
    collect_population = build_collector_task(dag, DAILY_POPULATION_SOURCE)
    run_nowcasting = build_nowcasting_task(dag)
    collect_population >> run_nowcasting

    collect_events = build_collector_task(dag, DAILY_EVENT_SOURCE)
    publish_events = build_event_publication_task(dag, DAILY_EVENT_SOURCE)
    collect_events >> publish_events

    collect_performance = build_collector_task(dag, PERFORMANCE_EVENT_SOURCE)
    publish_performance = build_event_publication_task(dag, PERFORMANCE_EVENT_SOURCE)
    collect_performance >> publish_performance
