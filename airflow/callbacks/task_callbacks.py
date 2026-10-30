"""모듈 태스크 성공·실패를 구조화 로그로 남기는 공통 callback."""

from __future__ import annotations

import logging

logger = logging.getLogger("airflow.task")


def _context_fields(context: dict) -> dict:
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    return {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "logical_date": str(dag_run.logical_date),
        "run_id": dag_run.run_id,
        "try_number": task_instance.try_number,
    }


def on_success_callback(context: dict) -> None:
    logger.info("task succeeded", extra=_context_fields(context))


def on_failure_callback(context: dict) -> None:
    fields = _context_fields(context)
    fields["exception"] = str(context.get("exception"))
    logger.error("task failed", extra=fields)
