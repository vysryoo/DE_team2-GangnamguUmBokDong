"""Gold 게시 CLI(loader, ml inference) 태스크 빌더.

realtime serving 체인은 앞 태스크 JSON XCom의 URI·SHA만 다음 태스크 환경변수로
넘긴다. 같은 ref로 단독 재시도해도 drift가 해소되지 않는 단계는 retry를 0으로 둔다.
"""

from __future__ import annotations

import json

from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from config.schedules import (
    INFERENCE_EXECUTION_TIMEOUT,
    ROUTES_EXECUTION_TIMEOUT,
    SERVING_EXECUTION_TIMEOUT,
    URGENCY_EXECUTION_TIMEOUT,
)
from orchestration.task_builder import REPO_ROOT, build_module_task, uv_python_command
from orchestration.templates import KST_WINDOW_START

LOADER_DIR = str(REPO_ROOT / "loader")
ML_DIR = str(REPO_ROOT / "ml")


def xcom_ref(task_id: str, key: str, field: str) -> str:
    """JSON XCom의 ``[key][field]`` 값을 읽는 Jinja 템플릿을 만든다.

    Args:
        task_id: XCom을 남긴 태스크 ID
        key: 최상위 키
        field: 중첩 필드 이름
    Returns:
        Jinja 템플릿 문자열
    """
    return "{{ ti.xcom_pull(task_ids='" + task_id + "')['" + key + "']['" + field + "'] }}"


def _ref_env(task_id: str, refs: dict[str, str]) -> dict[str, str]:
    env = {}
    for prefix, key in refs.items():
        env[f"{prefix}_URI"] = xcom_ref(task_id, key, "uri")
        env[f"{prefix}_SHA256"] = xcom_ref(task_id, key, "byte_sha256")
    return env


def build_event_publication_task(dag, source_id: str) -> BashOperator:
    """행사 source authority를 Gold로 게시하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        source_id: ``cultural_event`` 또는 ``performance_event``
    Returns:
        구성된 BashOperator
    """
    return build_module_task(
        dag,
        f"publish_event_{source_id}",
        LOADER_DIR,
        uv_python_command(
            "gold_cli.py",
            "--publication", f"event:{source_id}",
            "--window-start", KST_WINDOW_START,
        ),
        execution_timeout=SERVING_EXECUTION_TIMEOUT,
    )


def build_prepare_serving_task(
    dag,
    *,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
) -> BashOperator:
    """최신 source와 current model support를 immutable plan으로 고정하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        trigger_rule: 상위 태스크 상태에 따른 실행 조건
    Returns:
        ``{"plan": {"uri", "byte_sha256"}}`` JSON을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "prepare_serving_plan",
        LOADER_DIR,
        uv_python_command("serving_cli.py", "prepare", "--logical-dttm", '"$SERVING_LOGICAL_DTTM"'),
        env={"SERVING_LOGICAL_DTTM": KST_WINDOW_START},
        output_processor=json.loads,
        execution_timeout=SERVING_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )


def build_inference_task(dag, *, plan_task_id: str = "prepare_serving_plan") -> BashOperator:
    """plan ref로 inference 결과를 게시하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        plan_task_id: plan을 만든 태스크 ID
    Returns:
        ``{"inference": {...}}`` JSON을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "run_inference",
        ML_DIR,
        uv_python_command(
            "-m", "inference.publication_cli",
            "--plan-uri", '"$PLAN_URI"', "--plan-sha256", '"$PLAN_SHA256"',
            project="inference",
        ),
        execution_timeout=INFERENCE_EXECUTION_TIMEOUT,
        env=_ref_env(plan_task_id, {"PLAN": "plan"}),
        output_processor=json.loads,
        uv_environment_name="ml-inference",
    )


def build_finalize_serving_task(
    dag,
    *,
    plan_task_id: str,
    inference_task_id: str,
) -> BashOperator:
    """plan과 inference를 station·stock·demand·weather release로 원자 게시하는 태스크를 만든다.

    source/catalog drift는 같은 ref로 재시도해도 회복되지 않아 retry를 0으로 둠.
    새 plan부터 DAG run 전체를 다시 시작해야 함.

    Args:
        dag: 태스크를 붙일 DAG
        plan_task_id: plan을 만든 태스크 ID
        inference_task_id: inference를 게시한 태스크 ID
    Returns:
        release ref JSON을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "finalize_serving_release",
        LOADER_DIR,
        uv_python_command(
            "serving_cli.py", "finalize",
            "--plan-uri", '"$PLAN_URI"', "--plan-sha256", '"$PLAN_SHA256"',
            "--inference-uri", '"$INFERENCE_URI"',
            "--inference-sha256", '"$INFERENCE_SHA256"',
        ),
        env={
            **_ref_env(plan_task_id, {"PLAN": "plan"}),
            **_ref_env(inference_task_id, {"INFERENCE": "inference"}),
        },
        output_processor=json.loads,
        execution_timeout=SERVING_EXECUTION_TIMEOUT,
        retries=0,
    )


def build_urgency_task(dag, *, final_task_id: str = "finalize_serving_release") -> BashOperator:
    """final release의 station·demand·stock ref로 urgency를 게시하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        final_task_id: release를 게시한 태스크 ID
    Returns:
        ``{"station_urgency": {...}}`` JSON을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "publish_station_urgency",
        LOADER_DIR,
        uv_python_command(
            "serving_cli.py", "urgency",
            "--station-uri", '"$STATION_URI"', "--station-sha256", '"$STATION_SHA256"',
            "--demand-uri", '"$DEMAND_URI"', "--demand-sha256", '"$DEMAND_SHA256"',
            "--stock-uri", '"$STOCK_URI"', "--stock-sha256", '"$STOCK_SHA256"',
        ),
        execution_timeout=URGENCY_EXECUTION_TIMEOUT,
        env=_ref_env(
            final_task_id,
            {
                "STATION": "station",
                "DEMAND": "station_demand_forecast",
                "STOCK": "station_stock",
            },
        ),
        output_processor=json.loads,
        retries=0,
    )


def build_routes_task(dag, *, urgency_task_id: str = "publish_station_urgency") -> BashOperator:
    """urgency ref로 rebalance route를 게시하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        urgency_task_id: urgency를 게시한 태스크 ID
    Returns:
        route ref JSON을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "publish_rebalance_route",
        LOADER_DIR,
        uv_python_command(
            "serving_cli.py", "route",
            "--urgency-uri", '"$URGENCY_URI"', "--urgency-sha256", '"$URGENCY_SHA256"',
        ),
        execution_timeout=ROUTES_EXECUTION_TIMEOUT,
        env=_ref_env(urgency_task_id, {"URGENCY": "station_urgency"}),
        output_processor=json.loads,
        retries=0,
    )
