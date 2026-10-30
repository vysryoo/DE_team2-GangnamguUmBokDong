"""전날(00:00~24:00 KST) 데이터 수집 통계(성공/실패/결측치/이상치)를 매일 07:00 KST에
Slack으로 보고한다.

위험 판단 기준(실패율·결측 비율·이상치 비율)은 `config/alert_policy.yaml`에서 소스별로
관리한다 — 기준을 초과한 소스가 있으면 메시지 맨 아래 @de2조 그룹을 태그한다.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    CATCHUP,
    DAILY_COLLECTION_REPORT_CRON,
    MAX_ACTIVE_RUNS,
    TIMEZONE,
)
from config.sources import COLD_BRONZE_SOURCES
from notifications.collection_report import (
    build_daily_report_message,
    evaluate_source_stats,
)
from notifications.slack import send_message
from orchestration.collector_task import build_window_stats_task
from orchestration.templates import kst_date_days_ago

_YESTERDAY = kst_date_days_ago(1)


def _send_daily_report(day: str, **context: Any) -> None:
    ti = context["ti"]
    evaluations = [
        evaluate_source_stats(
            source_id, ti.xcom_pull(task_ids=f"collection_stats_{source_id}")
        )
        for source_id in COLD_BRONZE_SOURCES
    ]
    send_message(build_daily_report_message(day, evaluations))


with DAG(
    dag_id="daily_collection_report",
    schedule=CronTriggerTimetable(DAILY_COLLECTION_REPORT_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 27, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    is_paused_upon_creation=False,
    tags=["daily", "monitoring", "slack"],
) as dag:
    stats_tasks = [
        build_window_stats_task(dag, source_id, day_template=_YESTERDAY)
        for source_id in COLD_BRONZE_SOURCES
    ]
    send_report = PythonOperator(
        task_id="send_daily_report",
        python_callable=_send_daily_report,
        op_kwargs={"day": _YESTERDAY},
        dag=dag,
    )
    stats_tasks >> send_report
