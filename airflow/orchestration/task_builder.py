"""모듈 CLI를 독립 uv 환경에서 실행하는 BashOperator 팩토리.

Airflow는 각 모듈을 CLI로만 호출하고 모듈 코드를 import하지 않는다. 모듈 venv는
Docker named volume 아래에 두어, bind mount된 호스트 ``.venv``를 macOS와 Linux
컨테이너가 서로 덮어쓰지 않게 한다.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
)

# docker-compose가 저장소 전체를 /workspace에 마운트하므로 컨테이너 안에서도 성립함
REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_UV_ENV_ROOT = "/opt/venvs/modules"


def _module_project_environment(module_dir: str, uv_environment_name: str | None) -> str:
    environment_name = uv_environment_name or Path(module_dir).name
    if re.fullmatch(r"[a-z][a-z0-9-]*", environment_name) is None:
        raise ValueError("uv_environment_name은 lowercase 영숫자와 하이픈만 허용합니다.")
    return f"{_MODULE_UV_ENV_ROOT}/{environment_name}"


def module_subprocess_env(
    module_dir: str, *, uv_environment_name: str | None = None
) -> dict[str, str]:
    """PythonOperator 안에서 모듈 CLI를 ``subprocess``로 부를 때 쓸 환경변수를 만든다.

    Args:
        module_dir: 모듈 디렉터리 경로
        uv_environment_name: 모듈 venv 이름. 기본값은 ``module_dir``의 마지막 경로명
    Returns:
        Airflow의 ``VIRTUAL_ENV``를 지우고 모듈 venv를 가리키는 환경변수
    Raises:
        ValueError: venv 이름이 lowercase 영숫자와 하이픈 형식이 아닌 경우
    """
    env = dict(os.environ)
    env.pop("VIRTUAL_ENV", None)
    env["UV_PROJECT_ENVIRONMENT"] = _module_project_environment(
        module_dir, uv_environment_name
    )
    return env


def build_module_task(
    dag,
    task_id: str,
    module_dir: str,
    bash_command: str,
    *,
    retries: int = DEFAULT_RETRIES,
    retry_delay=DEFAULT_RETRY_DELAY,
    execution_timeout=DEFAULT_EXECUTION_TIMEOUT,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
    env: dict[str, str] | None = None,
    output_processor: Callable[[str], Any] | None = None,
    uv_environment_name: str | None = None,
) -> BashOperator:
    """독립 uv 환경을 사용하는 모듈 CLI 태스크를 만든다.

    Args:
        dag: 태스크를 붙일 DAG
        task_id: 태스크 ID
        module_dir: 명령을 실행할 모듈 디렉터리
        bash_command: 실행할 셸 명령
        retries: 재시도 횟수
        retry_delay: 재시도 간격
        execution_timeout: 실행 제한 시간
        trigger_rule: 상위 태스크 상태에 따른 실행 조건
        env: 추가 환경변수. Airflow 환경변수에 덧붙임
        output_processor: 마지막 stdout 줄을 XCom 값으로 바꾸는 함수
        uv_environment_name: 모듈 venv 이름. ``--project``가 작업 디렉터리와 다른
            프로젝트를 가리킬 때만 지정
    Returns:
        구성된 BashOperator
    Raises:
        ValueError: venv 이름이 lowercase 영숫자와 하이픈 형식이 아닌 경우
    """
    project_environment = _module_project_environment(module_dir, uv_environment_name)
    arguments: dict[str, Any] = {
        "task_id": task_id,
        "bash_command": (
            f"env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT={project_environment} {bash_command}"
        ),
        "cwd": module_dir,
        "retries": retries,
        "retry_delay": retry_delay,
        "execution_timeout": execution_timeout,
        "trigger_rule": trigger_rule,
        "append_env": True,
        "on_success_callback": on_success_callback,
        "on_failure_callback": on_failure_callback,
        "dag": dag,
    }
    if env is not None:
        arguments["env"] = env
    if output_processor is not None:
        arguments["output_processor"] = output_processor
    return BashOperator(**arguments)
