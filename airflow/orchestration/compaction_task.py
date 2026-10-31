"""Collector 레이크 유지보수 CLI(`maintenance.py`)를 실행하는 태스크를 만든다.

Archive compaction은 ``--source``만 넘겨 Collector가 검사 범위를 정하게 한다. DAG가
돌지 않았거나 압축이 실패한 날짜도 다음 실행의 recovery sweep이 다시 검사한다.
"""

from __future__ import annotations

from airflow.task.trigger_rule import TriggerRule
from config.schedules import COMPACTION_EXECUTION_TIMEOUT

from orchestration.task_builder import REPO_ROOT, build_module_task

COLLECTOR_DIR = str(REPO_ROOT / "collector")


def _build_maintenance_task(
    dag, command: str, source_id: str, options: str = "", *, trigger_rule: str
):
    task_prefix = command.replace("-", "_")
    return build_module_task(
        dag,
        f"{task_prefix}_{source_id}",
        COLLECTOR_DIR,
        f"uv run --frozen python maintenance.py {command} --source {source_id}{options}",
        execution_timeout=COMPACTION_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )


def build_compaction_task(dag, source_id: str, *, trigger_rule: str = TriggerRule.ALL_SUCCESS):
    """소스의 최근 날짜 Silver를 Archive로 압축하는 태스크를 만든다."""
    return _build_maintenance_task(dag, "compact", source_id, trigger_rule=trigger_rule)


def build_cold_bronze_compaction_task(
    dag,
    source_id: str,
    *,
    today: str,
    delay_days: int,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """기한이 된 pending Hot revision 날짜를 Cold 파일로 묶는 태스크를 만든다."""
    return _build_maintenance_task(
        dag,
        "cold-compact",
        source_id,
        f" --today {today} --delay-days {delay_days}",
        trigger_rule=trigger_rule,
    )


def build_hot_bronze_gc_task(
    dag,
    source_id: str,
    *,
    today: str,
    retention_days: int,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """Cold에 포함된 보존기한 지난 Hot Bronze를 정리하는 태스크를 만든다."""
    return _build_maintenance_task(
        dag,
        "gc-hot-bronze",
        source_id,
        f" --today {today} --retention-days {retention_days}",
        trigger_rule=trigger_rule,
    )


def build_silver_gc_task(
    dag,
    source_id: str,
    target_date: str,
    *,
    require_archive: bool,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """Cold와 선택적인 Archive 확인 뒤 보존기간 지난 Silver를 정리하는 태스크를 만든다."""
    archive_option = " --require-archive" if require_archive else ""
    return _build_maintenance_task(
        dag,
        "gc-silver",
        source_id,
        f" --date {target_date}{archive_option}",
        trigger_rule=trigger_rule,
    )
