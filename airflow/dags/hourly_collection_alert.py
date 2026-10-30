"""수집 주기가 1시간 이내인 소스에 대해 매시 정각(KST) 지난 1시간 수집 통계를 확인하고,
이상이 있는 소스가 있을 때만 @de2조 그룹을 태그해 문제 소스와 수치를 Slack에 알린다.

정상이면 Slack에 아무것도 보내지 않는다 — 위험 판단 기준은 daily_collection_report와
같은 `config/alert_policy.yaml`을 공유한다.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    CATCHUP,
    HOURLY_COLLECTION_ALERT_CRON,
    MAX_ACTIVE_RUNS,
    TIMEZONE,
)
from config.sources import HOURLY_MONITORED_SOURCES
from notifications.collection_report import (
    build_hourly_alert_message,
    evaluate_source_stats,
)
from notifications.slack import send_message
from orchestration.collector_task import build_window_stats_task
from orchestration.templates import kst_hours_ago_date, kst_hours_ago_hour

_HOUR_AGO_DATE = kst_hours_ago_date(1)
_HOUR_AGO_HOUR = kst_hours_ago_hour(1)


def _send_hourly_alert(day: str, hour: str, **context: Any) -> None:
    ti = context["ti"]
    evaluations = [
        evaluate_source_stats(
            source_id, ti.xcom_pull(task_ids=f"collection_stats_{source_id}")
        )
        for source_id in HOURLY_MONITORED_SOURCES
    ]
    risky = [e for e in evaluations if e["is_risky"]]
    message = build_hourly_alert_message(f"{day} {hour}시", risky)
    if message is not None:
        send_message(message)


with DAG(
    dag_id="hourly_collection_alert",
    schedule=CronTriggerTimetable(HOURLY_COLLECTION_ALERT_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 27, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    is_paused_upon_creation=False,
    tags=["hourly", "monitoring", "slack"],
) as dag:
    stats_tasks = [
        build_window_stats_task(
            dag, source_id, day_template=_HOUR_AGO_DATE, hour_template=_HOUR_AGO_HOUR
        )
        for source_id in HOURLY_MONITORED_SOURCES
    ]
    send_alert = PythonOperator(
        task_id="send_hourly_alert",
        python_callable=_send_hourly_alert,
        op_kwargs={"day": _HOUR_AGO_DATE, "hour": _HOUR_AGO_HOUR},
        dag=dag,
    )
    stats_tasks >> send_alert
