"""Airflow Compose의 cross-platform 가상환경 격리 계약을 검증한다."""

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_LOCAL_ADAPTER_PATH = _ROOT / "ops" / "compose" / "docker-compose.yml"
_PROD_COMPOSE_PATH = _ROOT / "ops" / "compose" / "docker-compose.prod.yml"
_ENV_EXAMPLE_PATH = _ROOT / ".env.example"
_ENTRYPOINT_PATH = _ROOT / "ops" / "compose" / "airflow-entrypoint.sh"
_AIRFLOW_SERVICES = (
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "airflow-dag-processor",
)
_MODULE_ENV_VOLUME = "airflow-module-venvs:/opt/venvs/modules"


def test_airflow_services_share_container_only_module_environments():
    """운영 원본의 모든 Airflow 서비스가 같은 named volume을 사용한다."""
    production = yaml.safe_load(_PROD_COMPOSE_PATH.read_text(encoding="utf-8"))
    local_text = _LOCAL_ADAPTER_PATH.read_text(encoding="utf-8")

    assert "airflow-module-venvs" in production["volumes"]
    assert _MODULE_ENV_VOLUME in production["x-airflow-common"]["volumes"]
    for service_name in _AIRFLOW_SERVICES:
        service_section = local_text.split(f"  {service_name}:", 1)[1].split("\n  ", 1)[0]
        assert "volumes:" not in service_section


def test_airflow_init_prewarms_every_bash_operator_project():
    """초기화 단계가 추론과 재배치를 포함한 독립 uv 프로젝트를 모두 예열한다."""
    entrypoint = _ENTRYPOINT_PATH.read_text(encoding="utf-8")

    assert (
        "for proj in collector poi_master normalizer nowcaster ml/inference loader rebalance; do"
        in entrypoint
    )
    assert 'env_name="${env_name//_/-}"' in entrypoint
    assert 'UV_PROJECT_ENVIRONMENT="/opt/venvs/modules/$env_name"' in entrypoint


def test_local_and_production_use_same_airflow_concurrency_contract():
    """로컬 adapter가 운영 원본의 안전 동시성 계약을 덮어쓰지 않는다."""
    production = yaml.safe_load(_PROD_COMPOSE_PATH.read_text(encoding="utf-8"))
    local_text = _LOCAL_ADAPTER_PATH.read_text(encoding="utf-8")
    expected = {
        "AIRFLOW__CORE__PARALLELISM": "${AIRFLOW__CORE__PARALLELISM:-3}",
        "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": (
            "${AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG:-3}"
        ),
    }

    production_environment = production["x-airflow-common"]["environment"]
    assert {
        key: production_environment[key] for key in expected
    } == expected
    assert "AIRFLOW__CORE__PARALLELISM" not in local_text
    assert "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG" not in local_text

    env_example = _ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    assert "AIRFLOW__CORE__PARALLELISM=3" in env_example
    assert "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=3" in env_example


def test_local_airflow_starts_project_dags_paused_without_examples():
    """운영 원본이 빈 metadata DB에서도 프로젝트 DAG를 pause 생성한다."""
    production = yaml.safe_load(_PROD_COMPOSE_PATH.read_text(encoding="utf-8"))
    environment = production["x-airflow-common"]["environment"]

    assert environment["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "true"
    assert environment["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"


def test_local_adapter_does_not_redefine_production_runtime_images_or_commands():
    """로컬은 managed service 연결만 바꾸고 운영 runtime 정의는 재사용한다."""
    local_text = _LOCAL_ADAPTER_PATH.read_text(encoding="utf-8")

    for service_name in (*_AIRFLOW_SERVICES, "api", "mlflow", "web"):
        service_section = local_text.split(f"  {service_name}:", 1)[1].split("\n  ", 1)[0]
        assert "build:" not in service_section
        assert "image:" not in service_section
        assert "command:" not in service_section
        assert "restart:" not in service_section
