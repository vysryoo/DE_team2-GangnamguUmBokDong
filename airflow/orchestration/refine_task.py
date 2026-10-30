"""정제 모듈(poi_master, normalizer, nowcaster) CLI 태스크 빌더."""

from __future__ import annotations

import json

from airflow.providers.standard.operators.bash import BashOperator

from config.schedules import (
    NORMALIZER_EXECUTION_TIMEOUT,
    NOWCASTING_EXECUTION_TIMEOUT,
    POI_MASTER_REFRESH_EXECUTION_TIMEOUT,
    POI_MASTER_RESOLVE_EXECUTION_TIMEOUT,
)
from orchestration.task_builder import REPO_ROOT, build_module_task, uv_python_command
from orchestration.templates import KST_DATE, KST_WINDOW_START

POI_MASTER_DIR = str(REPO_ROOT / "poi_master")
NORMALIZER_DIR = str(REPO_ROOT / "normalizer")
NOWCASTING_DIR = str(REPO_ROOT / "nowcaster")

# poi_master_ref_env()가 채우는 환경변수를 읽는 CLI 인자
POI_MASTER_ARGS = (
    "--poi-master-mode", '"$POI_MASTER_MODE"',
    "--poi-master-manifest-uri", '"$POI_MASTER_MANIFEST_URI"',
    "--poi-master-manifest-sha256", '"$POI_MASTER_MANIFEST_SHA256"',
)


def poi_master_ref_env(task_id: str) -> dict[str, str]:
    """resolver XCom의 POI Master ref를 하위 CLI 환경변수 템플릿으로 만든다.

    Args:
        task_id: ``resolve_poi_master`` 태스크 ID
    Returns:
        ``POI_MASTER_MODE``, ``POI_MASTER_MANIFEST_URI``, ``POI_MASTER_MANIFEST_SHA256``
        Jinja 템플릿. static 모드에서는 URI·SHA가 빈 문자열로 렌더링됨
    """
    payload = f"ti.xcom_pull(task_ids='{task_id}')"
    return {
        "POI_MASTER_MODE": "{{ " + payload + "['mode'] }}",
        "POI_MASTER_MANIFEST_URI": "{{ " + payload + ".get('manifest_uri') or '' }}",
        "POI_MASTER_MANIFEST_SHA256": "{{ " + payload + ".get('manifest_sha256') or '' }}",
    }


def build_poi_master_refresh_task(dag) -> BashOperator:
    """서울시 POI 파일 변경을 검증하고 새 POI Master를 게시하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
    Returns:
        ``main.py refresh`` JSON 출력을 XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "refresh_poi_master",
        POI_MASTER_DIR,
        uv_python_command("main.py", "refresh"),
        execution_timeout=POI_MASTER_REFRESH_EXECUTION_TIMEOUT,
        output_processor=json.loads,
        uv_environment_name="poi-master",
    )


def build_poi_master_resolve_task(dag) -> BashOperator:
    """한 realtime run이 사용할 POI Master ref를 고정하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
    Returns:
        ``main.py resolve`` JSON 출력(``mode``, ``manifest_uri``, ``manifest_sha256``)을
        XCom으로 남기는 BashOperator
    """
    return build_module_task(
        dag,
        "resolve_poi_master",
        POI_MASTER_DIR,
        uv_python_command("main.py", "resolve", "--as-of", '"$POI_MASTER_AS_OF"'),
        env={"POI_MASTER_AS_OF": KST_WINDOW_START},
        execution_timeout=POI_MASTER_RESOLVE_EXECUTION_TIMEOUT,
        output_processor=json.loads,
        uv_environment_name="poi-master",
    )


def build_normalizer_task(
    dag,
    task_id: str = "run_normalizer",
    *,
    poi_master_task_id: str,
) -> BashOperator:
    """현재와 향후 12시간 생활인구를 한 번에 보정하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        task_id: 태스크 ID
        poi_master_task_id: POI Master ref를 고정한 resolver 태스크 ID
    Returns:
        구성된 BashOperator
    """
    return build_module_task(
        dag,
        task_id,
        NORMALIZER_DIR,
        uv_python_command("main.py", "--window-start", KST_WINDOW_START, *POI_MASTER_ARGS),
        execution_timeout=NORMALIZER_EXECUTION_TIMEOUT,
        env=poi_master_ref_env(poi_master_task_id),
    )


def build_station_master_enrichment_task(dag) -> BashOperator:
    """대여소 master에 생활인구 CELL_ID를 보강하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
    Returns:
        구성된 BashOperator
    """
    return build_module_task(
        dag,
        "enrich_station_master",
        NORMALIZER_DIR,
        uv_python_command("station_master.py", "--window-start", KST_WINDOW_START),
    )


def build_nowcasting_task(dag) -> BashOperator:
    """collector와 같은 logical window로 생활인구 nowcast를 추정하는 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
    Returns:
        구성된 BashOperator
    """
    return build_module_task(
        dag,
        "run_nowcasting_estimate",
        NOWCASTING_DIR,
        uv_python_command(
            "main.py", "estimate",
            "--target-date", KST_DATE,
            "--source-window-start", KST_WINDOW_START,
        ),
        execution_timeout=NOWCASTING_EXECUTION_TIMEOUT,
    )
