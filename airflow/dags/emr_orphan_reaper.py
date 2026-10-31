"""monthly_retrain의 teardown이 실행되지 못해 남은 EMR 클러스터를 종료하는 안전망.

스케줄러 장애 등으로 teardown이 실행되지 않는 경우에 대비해 실제 EMR 상태를 주기적으로
조회한다. Airflow 3 Task SDK는 태스크 안에서 다른 DAG run 상태를 조회할 경로가 없어,
재학습 진행 여부는 EMR 활성 스텝 유무로 판단한다.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from config.schedules import CATCHUP, EMR_ORPHAN_REAPER_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from orchestration.aws_infra_task import (
    EMR_IDLE_GRACE_MINUTES,
    get_cluster_step_activity,
    list_active_emr_clusters,
    terminate_emr_cluster,
)

logger = logging.getLogger(__name__)


def _reap_orphan_emr_clusters(**context: Any) -> dict[str, Any]:
    """활성 스텝이 없고 유휴 유예 시간이 지난 월간 재학습 클러스터를 종료한다.

    활성 스텝이 있는 클러스터는 경과 시간과 무관하게 건드리지 않음. 정상 학습도
    24시간 이상 걸린 기록이 있어 시간 상한으로 종료하지 않음.

    Returns:
        ``{"total", "reaped", "still_active", "still_within_grace"}`` 요약
    """
    clusters = list_active_emr_clusters()
    now = datetime.now(UTC)
    reaped: list[str] = []
    still_active: list[str] = []
    still_within_grace: list[str] = []

    for cluster in clusters:
        activity = get_cluster_step_activity(cluster["id"])

        if activity["has_active_step"]:
            still_active.append(cluster["id"])
            continue

        idle_since = activity["last_step_completed_at"] or cluster["created_at"]
        idle_minutes = (now - idle_since).total_seconds() / 60
        if idle_minutes >= EMR_IDLE_GRACE_MINUTES:
            logger.warning(
                "[emr-reaper] 클러스터 '%s'(%s)가 활성 스텝 없이 %.1f분째 유휴 상태라 "
                "(기준 %.1f분) 강제 종료합니다",
                cluster["name"],
                cluster["id"],
                idle_minutes,
                EMR_IDLE_GRACE_MINUTES,
            )
            terminate_emr_cluster(cluster["id"])
            reaped.append(cluster["id"])
        else:
            still_within_grace.append(cluster["id"])

    logger.info(
        "[emr-reaper] 점검 완료 — 전체 %d개, 종료 %d개, 재학습 중 %d개, 유예 중 %d개",
        len(clusters),
        len(reaped),
        len(still_active),
        len(still_within_grace),
    )
    return {
        "total": len(clusters),
        "reaped": reaped,
        "still_active": still_active,
        "still_within_grace": still_within_grace,
    }


with DAG(
    dag_id="emr_orphan_reaper",
    schedule=EMR_ORPHAN_REAPER_CRON,
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["ml", "emr", "maintenance"],
) as dag:
    reap_orphan_emr_clusters = PythonOperator(
        task_id="reap_orphan_emr_clusters",
        python_callable=_reap_orphan_emr_clusters,
    )
