"""서울시 POI 목록·영역 변경을 확인해 검증된 Master를 게시하는 일일 DAG."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    POI_MASTER_REFRESH_CRON,
    TIMEZONE,
)
from orchestration.refine_task import build_poi_master_refresh_task

with DAG(
    dag_id="poi_master_refresh",
    schedule=CronTriggerTimetable(POI_MASTER_REFRESH_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 25, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    is_paused_upon_creation=False,
    tags=["daily", "master", "poi"],
) as dag:
    refresh_poi_master = build_poi_master_refresh_task(dag)
