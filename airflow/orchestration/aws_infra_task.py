"""월간 재학습용 상시 EMR 클러스터의 생성, 스텝 실행, 종료를 제어한다.

AWS 자격증명이 없는 로컬·테스트 환경에서는 mock으로 동작한다.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
EMR_RELEASE_LABEL = os.environ.get("AWS_EMR_RELEASE_LABEL", "emr-7.13.0")
# 이 AWS 계정은 EMR에 m4.large 외 인스턴스 타입을 허용하지 않음
EMR_MASTER_INSTANCE_TYPE = os.environ.get("AWS_EMR_MASTER_INSTANCE_TYPE", "m4.large")
EMR_CORE_INSTANCE_TYPE = os.environ.get("AWS_EMR_CORE_INSTANCE_TYPE", "m4.large")
EMR_CORE_INSTANCE_COUNT = int(os.environ.get("AWS_EMR_CORE_INSTANCE_COUNT", "2"))
# m4.large는 VPC 서브넷 없이 뜨지 않음. terraform이 config/prod.env에 채움
EMR_SUBNET_ID = os.environ.get("AWS_EMR_SUBNET_ID", "")
# 보안그룹을 넘기지 않으면 EMR이 직접 만들려다 VPC 태그 조건에 막힘.
# terraform/emr.tf가 만든 보안그룹을 사용함
EMR_MASTER_SG_ID = os.environ.get("AWS_EMR_MASTER_SG_ID", "")
EMR_CORE_SG_ID = os.environ.get("AWS_EMR_CORE_SG_ID", "")
# EMR 노드에서는 docker 네트워크 이름(mlflow)이 풀리지 않아 사설 IP 주소를 주입함
EMR_MLFLOW_TRACKING_URI = os.environ.get("AWS_EMR_MLFLOW_TRACKING_URI", "")
# terraform/emr.tf가 만드는 역할 이름(var.project 기본값 gng-ubd 기준)
EMR_SERVICE_ROLE = os.environ.get("AWS_EMR_SERVICE_ROLE", "gng-ubd-emr-service")
EMR_JOB_FLOW_ROLE = os.environ.get("AWS_EMR_JOB_FLOW_ROLE", "gng-ubd-emr-ec2")

# libs/ml_core/paths.py와 같은 값이어야 함. Airflow venv에는 ml_core가 없어 따로 정의함
S3_BUCKET = os.environ.get("S3_BUCKET", "gangnamgu")
MODELS_PREFIX = os.environ.get("MODELS_PREFIX", "models")
TRAINING_RUNS_PREFIX = os.environ.get("TRAINING_RUNS_PREFIX", f"{MODELS_PREFIX}/training-runs")

# Makefile의 emr-package가 업로드하는 위치와 같아야 함. 노드에 training 패키지를 설치함
EMR_BOOTSTRAP_SCRIPT_S3_URI = os.environ.get(
    "AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI", f"s3://{S3_BUCKET}/emr/bootstrap.sh"
)
EMR_PYFILES_S3_BUCKET = os.environ.get("AWS_EMR_PYFILES_S3_BUCKET", S3_BUCKET)

# 다른 용도의 EMR 클러스터를 reaper가 건드리지 않도록 이름 prefix로 거름
MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX = os.environ.get(
    "AWS_MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX", "ml-monthly-retrain-"
)
# 스텝 사이 짧은 간격을 유휴로 오판하지 않기 위한 여유
EMR_IDLE_GRACE_MINUTES = float(os.environ.get("AWS_EMR_IDLE_GRACE_MINUTES", "15"))

MOCK_OVERRIDE_FORCE_MOCK = "force_mock"
MOCK_OVERRIDE_FORCE_REAL = "force_real"


def is_emr_mock_mode(override: str | None = None) -> bool:
    """EMR 호출을 mock으로 대체할지 판별한다.

    운영은 access key 없이 instance profile로 인증하므로, 키가 없다는 사실이 아니라
    로컬 MinIO 등의 더미 키일 때만 mock으로 봄.

    Args:
        override: ``MOCK_OVERRIDE_FORCE_MOCK``, ``MOCK_OVERRIDE_FORCE_REAL`` 또는 그 외
            값. 그 외 값은 환경변수로 판별함
    Returns:
        mock이면 True
    """
    if override == MOCK_OVERRIDE_FORCE_MOCK:
        return True
    if override == MOCK_OVERRIDE_FORCE_REAL:
        return False
    if os.environ.get("MOCK_AWS_INFRA", "").lower() in ("1", "true", "yes"):
        return True
    return os.environ.get("AWS_ACCESS_KEY_ID", "") in ("minioadmin", "test", "dummy")


def _get_boto3_client(service_name: str) -> Any:
    import boto3

    return boto3.client(service_name, region_name=DEFAULT_REGION)


def read_s3_json(key: str) -> dict | None:
    """``S3_BUCKET``의 JSON 객체를 읽는다.

    EMR 스텝은 stdout을 돌려주지 않으므로 스텝 결과 요약을 이 함수로 읽음.

    Args:
        key: 읽을 S3 객체 키
    Returns:
        파싱된 JSON. 키가 없으면 None
    Raises:
        botocore.exceptions.ClientError: ``NoSuchKey`` 외의 S3 오류
    """
    import boto3
    from botocore.exceptions import ClientError

    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    if endpoint_url:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
    else:
        client = boto3.client("s3", region_name=DEFAULT_REGION)

    try:
        body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)


def terminate_emr_cluster(cluster_id: str | None, *, mock_override: str | None = None) -> None:
    """EMR 클러스터를 종료한다.

    teardown에서 호출되므로 AWS 오류는 로그만 남기고 올리지 않음.

    Args:
        cluster_id: 대상 클러스터 ID. 없으면 아무것도 하지 않음
        mock_override: ``is_emr_mock_mode()`` 참고
    """
    if is_emr_mock_mode(mock_override):
        logger.info("[Mock EMR] 클러스터 '%s' 종료 (Mock)", cluster_id)
        return
    if not cluster_id:
        return

    emr = _get_boto3_client("emr")
    try:
        logger.info("[EMR] 클러스터 '%s' 종료 요청", cluster_id)
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
    except Exception as exc:  # noqa: BLE001 - 자원 정리 실패가 상위 태스크를 깨뜨리지 않도록 로깅만 수행
        logger.error("[EMR] 클러스터 '%s' 종료 중 오류 발생: %s", cluster_id, exc)


def list_active_emr_clusters(
    name_prefix: str = MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX,
    *,
    mock_override: str | None = None,
) -> list[dict[str, Any]]:
    """``name_prefix``로 시작하는 종료되지 않은 EMR 클러스터를 나열한다.

    Args:
        name_prefix: 클러스터 이름 prefix
        mock_override: ``is_emr_mock_mode()`` 참고. mock이면 빈 목록
    Returns:
        ``{"id", "name", "state", "created_at"}`` 목록. ``created_at``은 tz-aware datetime
    """
    if is_emr_mock_mode(mock_override):
        return []

    emr = _get_boto3_client("emr")
    clusters: list[dict[str, Any]] = []
    for page in emr.get_paginator("list_clusters").paginate(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]
    ):
        for cluster in page["Clusters"]:
            if not cluster["Name"].startswith(name_prefix):
                continue
            clusters.append({
                "id": cluster["Id"],
                "name": cluster["Name"],
                "state": cluster["Status"]["State"],
                "created_at": cluster["Status"]["Timeline"]["CreationDateTime"],
            })
    return clusters


def get_cluster_step_activity(
    cluster_id: str, *, mock_override: str | None = None
) -> dict[str, Any]:
    """클러스터에 활성 스텝이 있는지와 마지막 스텝 종료 시각을 조회한다.

    Airflow 3 Task SDK는 태스크 안에서 다른 DAG run 상태를 조회할 경로가 없어,
    재학습 진행 여부를 EMR 스텝 상태로 판단함.

    Args:
        cluster_id: 대상 클러스터 ID
        mock_override: ``is_emr_mock_mode()`` 참고
    Returns:
        ``{"has_active_step": bool, "last_step_completed_at": datetime | None}``.
        끝난 스텝이 없으면 ``last_step_completed_at``은 None
    """
    if is_emr_mock_mode(mock_override):
        return {"has_active_step": False, "last_step_completed_at": None}

    steps = _get_boto3_client("emr").list_steps(ClusterId=cluster_id)["Steps"]
    active_states = {"PENDING", "CANCEL_PENDING", "RUNNING"}
    end_times = [
        step["Status"]["Timeline"]["EndDateTime"]
        for step in steps
        if step["Status"]["Timeline"].get("EndDateTime") is not None
    ]
    return {
        "has_active_step": any(step["Status"]["State"] in active_states for step in steps),
        "last_step_completed_at": max(end_times) if end_times else None,
    }


def create_emr_cluster(
    *,
    cluster_name: str | None = None,
    core_instance_count: int | None = None,
    core_instance_type: str | None = None,
    timeout_seconds: int = 1200,
    mock_override: str | None = None,
) -> str:
    """스텝 없이 유지되는 상시 EMR 클러스터를 만들고 WAITING까지 기다린다.

    자동 종료되지 않으므로 호출부가 ``terminate_emr_cluster()``를 반드시 호출해야 함.

    Args:
        cluster_name: 클러스터 이름. 기본값은 타임스탬프로 생성
        core_instance_count: core 노드 수. 기본값은 ``AWS_EMR_CORE_INSTANCE_COUNT``
        core_instance_type: core 노드 타입. 기본값은 ``AWS_EMR_CORE_INSTANCE_TYPE``
        timeout_seconds: WAITING 대기 상한(초). 넘기면 클러스터를 종료함
        mock_override: ``is_emr_mock_mode()`` 참고
    Returns:
        클러스터(JobFlow) ID
    Raises:
        RuntimeError: 서브넷·보안그룹 설정이 없거나, WAITING 전에 클러스터가 종료된 경우
        TimeoutError: 대기 상한을 넘긴 경우
    """
    if is_emr_mock_mode(mock_override):
        mock_cluster_id = f"mock-j-{int(time.time())}"
        logger.info("[Mock EMR] 클러스터 '%s' 생성 (Mock: %s)", cluster_name, mock_cluster_id)
        return mock_cluster_id

    _require_network_config()
    core_count = core_instance_count or EMR_CORE_INSTANCE_COUNT
    flow_name = cluster_name or f"ml-monthly-retrain-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    emr = _get_boto3_client("emr")
    logger.info("[EMR] 클러스터 '%s' 생성 요청(core=%d)", flow_name, core_count)
    cluster_id = emr.run_job_flow(
        **_job_flow_request(flow_name, core_count, core_instance_type or EMR_CORE_INSTANCE_TYPE)
    )["JobFlowId"]
    logger.info("[EMR] 클러스터 생성됨: %s", cluster_id)

    if _wait_for_cluster_waiting(emr, cluster_id, timeout_seconds):
        return cluster_id
    # 예외로 끝나면 cluster_id가 XCom에 남지 않아 teardown이 찾지 못하므로 직접 종료함
    terminate_emr_cluster(cluster_id, mock_override=mock_override)
    raise TimeoutError(
        f"EMR 클러스터 '{cluster_id}'가 {timeout_seconds}초 내에 WAITING 상태가 되지 못했습니다."
    )


def _require_network_config() -> None:
    if not EMR_SUBNET_ID:
        raise RuntimeError(
            "AWS_EMR_SUBNET_ID가 비어 있습니다. terraform output -raw subnet_id 값을 "
            "config/prod.env(AWS_EMR_SUBNET_ID)에 채우세요."
        )
    if not EMR_MASTER_SG_ID or not EMR_CORE_SG_ID:
        raise RuntimeError(
            "AWS_EMR_MASTER_SG_ID/AWS_EMR_CORE_SG_ID가 비어 있습니다. terraform이 만든 "
            "aws_security_group.emr_master/emr_core의 ID를 config/prod.env에 채우세요."
        )


def _instance_group(role: str, instance_type: str, count: int) -> dict[str, Any]:
    return {
        "Name": role.capitalize(),
        "Market": "ON_DEMAND",
        "InstanceRole": role,
        "InstanceType": instance_type,
        "InstanceCount": count,
    }


def _job_flow_request(name: str, core_count: int, core_instance_type: str) -> dict[str, Any]:
    request: dict[str, Any] = {
        "Name": name,
        "ReleaseLabel": EMR_RELEASE_LABEL,
        "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
        "Instances": {
            "InstanceGroups": [
                _instance_group("MASTER", EMR_MASTER_INSTANCE_TYPE, 1),
                _instance_group("CORE", core_instance_type, core_count),
            ],
            "Ec2SubnetId": EMR_SUBNET_ID,
            "EmrManagedMasterSecurityGroup": EMR_MASTER_SG_ID,
            "EmrManagedSlaveSecurityGroup": EMR_CORE_SG_ID,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "JobFlowRole": EMR_JOB_FLOW_ROLE,
        "ServiceRole": EMR_SERVICE_ROLE,
        # LogUri가 없으면 스텝 실패 원인이 "Unknown Error"로만 남음
        "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
        "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
    }
    if EMR_BOOTSTRAP_SCRIPT_S3_URI:
        request["BootstrapActions"] = [
            {
                "Name": "install-training-env",
                "ScriptBootstrapAction": {
                    "Path": EMR_BOOTSTRAP_SCRIPT_S3_URI,
                    "Args": [EMR_PYFILES_S3_BUCKET] if EMR_PYFILES_S3_BUCKET else [],
                },
            }
        ]
    return request


def _wait_for_cluster_waiting(emr: Any, cluster_id: str, timeout_seconds: int) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        status = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]
        state = status["State"]
        logger.info("[EMR] 클러스터 '%s' 상태: %s", cluster_id, state)
        if state == "WAITING":
            return True
        if state in ("TERMINATED", "TERMINATED_WITH_ERRORS"):
            reason = status.get("StateChangeReason", {})
            raise RuntimeError(
                f"EMR 클러스터 '{cluster_id}' 생성 중 비정상 종료: {state} "
                f"(code={reason.get('Code')}, message={reason.get('Message')})"
            )
        time.sleep(30)
    return False


def submit_emr_step(
    cluster_id: str,
    name: str,
    command: list[str],
    *,
    action_on_failure: str = "CONTINUE",
    timeout_seconds: int = 345600,
    mock_override: str | None = None,
) -> dict[str, Any]:
    """상시 클러스터에 스텝 하나를 제출하고 완료까지 기다린다.

    Args:
        cluster_id: 대상 클러스터 ID
        name: 스텝 이름
        command: ``command-runner.jar``에 넘길 인자 목록
        action_on_failure: 기본값 ``CONTINUE``. 스텝이 실패해도 다음 후보 프로필을
            시도할 수 있도록 클러스터를 유지함
        timeout_seconds: 완료 대기 상한(초)
        mock_override: ``is_emr_mock_mode()`` 참고
    Returns:
        ``{"StepId": ..., "State": "COMPLETED"}``
    Raises:
        RuntimeError: 스텝이 실패, 취소, 중단된 경우
        TimeoutError: 대기 상한을 넘긴 경우
    """
    if is_emr_mock_mode(mock_override):
        mock_step_id = f"mock-s-{int(time.time())}"
        logger.info("[Mock EMR] 클러스터 '%s'에 스텝 '%s' 완료 (Mock: %s)", cluster_id, name, mock_step_id)
        return {"StepId": mock_step_id, "State": "COMPLETED"}

    emr = _get_boto3_client("emr")
    logger.info("[EMR] 클러스터 '%s'에 스텝 '%s' 제출", cluster_id, name)
    step_id = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": name,
                "ActionOnFailure": action_on_failure,
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": command},
            }
        ],
    )["StepIds"][0]

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        status = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]["Status"]
        state = status["State"]
        logger.info("[EMR] 스텝 '%s'(%s) 상태: %s", name, step_id, state)
        if state == "COMPLETED":
            return {"StepId": step_id, "State": state}
        if state in ("FAILED", "CANCELLED", "INTERRUPTED"):
            raise RuntimeError(
                f"EMR 스텝 '{name}'({step_id}) 실패({state}): {status.get('FailureDetails', {})}"
            )
        time.sleep(15)

    raise TimeoutError(f"EMR 스텝 '{name}'({step_id})이 {timeout_seconds}초 내에 완료되지 못했습니다.")
