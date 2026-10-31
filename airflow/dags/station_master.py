"""따릉이 대여소 마스터를 하루 한 번 수집하는 DAG."""

import pendulum
from airflow import DAG

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, STATION_MASTER_CRON, TIMEZONE
from config.sources import STATION_MASTER_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.refine_task import build_station_master_enrichment_task

with DAG(
    dag_id="station_master",
    schedule=STATION_MASTER_CRON,
    start_date=pendulum.datetime(2026, 8, 17, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["collector", "daily", "station"],
) as dag:
    collect_station_master = build_collector_task(dag, STATION_MASTER_SOURCE)
    # realtime_tick의 prepare_serving_plan과 run_inference가
    # silver/station_master_enriched를 필수로 읽음
    enrich_station_master = build_station_master_enrichment_task(dag)
    collect_station_master >> enrich_station_master
