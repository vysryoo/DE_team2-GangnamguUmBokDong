"""aws_infra_task.py의 mock 판별과 EMR 상시 클러스터·스텝 함수를 검증한다.

Airflow venv에 moto가 없어 `botocore.stub.Stubber`로 boto3 호출 형태를 검증한다.
"""

from datetime import UTC, datetime

import boto3
import pytest
from botocore.stub import Stubber

import orchestration.aws_infra_task as infra


@pytest.fixture(autouse=True)
def _no_dummy_access_key(monkeypatch):
    """`is_emr_mock_mode()`가 access key로 mock을 추론하지 않도록 더미 값이 아닌
    값으로 고정한다 — 이 파일의 목적은 "실제 호출 경로"를 Stubber로 검증하는
    것이라 자동으로 mock으로 빠지면 안 된다."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "unittest-real")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "unittest-real-secret")
    monkeypatch.delenv("MOCK_AWS_INFRA", raising=False)


@pytest.fixture(autouse=True)
def _default_emr_subnet(monkeypatch):
    """EMR_SUBNET_ID는 모듈 import 시점에 환경변수로 고정되므로 monkeypatch.setenv가
    안 먹는다 — 대신 모듈 속성 자체를 덮어써서 대부분의 테스트가 "서브넷이
    설정된 정상 상태"를 기본으로 가정하게 한다. 빈 값일 때의 동작만 검증하는
    테스트는 이 값을 다시 ""로 덮어쓴다."""
    monkeypatch.setattr(infra, "EMR_SUBNET_ID", "subnet-0123456789abcdef0")
    monkeypatch.setattr(infra, "EMR_MASTER_SG_ID", "sg-master0123456789")
    monkeypatch.setattr(infra, "EMR_CORE_SG_ID", "sg-core0123456789ab")


def _stub_emr_client(monkeypatch) -> Stubber:
    client = boto3.client("emr", region_name="ap-northeast-2")
    stubber = Stubber(client)
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name: client)
    return stubber


# --- is_emr_mock_mode() ---


def test_is_emr_mock_mode_defaults_to_real_with_non_dummy_key():
    assert infra.is_emr_mock_mode(None) is False


def test_is_emr_mock_mode_override_wins_over_environment(monkeypatch):
    monkeypatch.setenv("MOCK_AWS_INFRA", "true")
    assert infra.is_emr_mock_mode(infra.MOCK_OVERRIDE_FORCE_REAL) is False
    assert infra.is_emr_mock_mode(infra.MOCK_OVERRIDE_FORCE_MOCK) is True


def test_is_emr_mock_mode_env_var_forces_mock_without_override(monkeypatch):
    monkeypatch.setenv("MOCK_AWS_INFRA", "true")
    assert infra.is_emr_mock_mode("auto") is True


def test_is_emr_mock_mode_treats_local_dummy_key_as_mock(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    assert infra.is_emr_mock_mode(None) is True


# --- create_emr_cluster() ---


def test_create_emr_cluster_requests_persistent_cluster_with_required_tag(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response(
        "describe_cluster",
        {"Cluster": {"Id": "j-created", "Name": "n", "Status": {"State": "WAITING"}}},
    )
    stubber.activate()

    cluster_id = infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert cluster_id == "j-created"
    stubber.assert_no_pending_responses()


def test_create_emr_cluster_sets_keep_alive_and_managed_policy_tag(monkeypatch):
    captured = {}
    client = boto3.client("emr", region_name="ap-northeast-2")

    def _capture_run_job_flow(**kwargs):
        captured.update(kwargs)
        return {"JobFlowId": "j-created", "ResponseMetadata": {}}

    monkeypatch.setattr(client, "run_job_flow", _capture_run_job_flow)
    monkeypatch.setattr(
        client,
        "describe_cluster",
        lambda **kwargs: {"Cluster": {"Status": {"State": "WAITING"}}},
    )
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name: client)

    infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert captured["Instances"]["KeepJobFlowAliveWhenNoSteps"] is True
    assert {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"} in captured["Tags"]
    assert captured["Instances"]["InstanceGroups"][1]["InstanceCount"] == 3
    # m4.large는 서브넷 없이 못 뜬다 — 첫 실제 실행에서 이게 빠져 "Subnet is
    # required" VALIDATION_ERROR로 실패했다(2026-08-25).
    assert captured["Instances"]["Ec2SubnetId"] == infra.EMR_SUBNET_ID
    # 보안그룹을 명시하지 않으면 EMR이 스스로 만들려다 VPC 태그 조건에 막혀
    # "insufficient EC2 permissions: ec2:CreateSecurityGroup"로 실패했다
    # (2026-08-25, 두 번째 실제 실행에서 실측).
    assert captured["Instances"]["EmrManagedMasterSecurityGroup"] == infra.EMR_MASTER_SG_ID
    assert captured["Instances"]["EmrManagedSlaveSecurityGroup"] == infra.EMR_CORE_SG_ID
    # LogUri가 없으면 스텝이 실패해도 "Unknown Error"만 뜨고 실제 stdout/stderr을
    # 볼 방법이 없다(2026-08-25, evaluate_rental 첫 실패에서 실측 확인).
    assert captured["LogUri"] == f"s3://{infra.S3_BUCKET}/emr-logs/"
    # 기본값(AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI 미설정)에서도 BootstrapActions가
    # 실려야 한다 — 예전엔 빈 문자열이 기본값이라 아무도 안 채우면 training
    # 패키지가 안 깔린 클러스터가 뜨고 첫 스텝이 ImportError로 조용히
    # 실패했다(2026-08 리뷰에서 발견). S3_BUCKET 기본값에서 유도한 경로가 온다.
    bootstrap_action = captured["BootstrapActions"][0]["ScriptBootstrapAction"]
    assert bootstrap_action["Path"] == f"s3://{infra.S3_BUCKET}/emr/bootstrap.sh"
    assert bootstrap_action["Args"] == [infra.S3_BUCKET]


def test_create_emr_cluster_adds_bootstrap_action_when_configured(monkeypatch):
    """AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI를 override하면 그 값을 그대로 쓴다."""
    monkeypatch.setattr(infra, "EMR_BOOTSTRAP_SCRIPT_S3_URI", "s3://my-bucket/emr/bootstrap.sh")
    monkeypatch.setattr(infra, "EMR_PYFILES_S3_BUCKET", "my-bucket")
    captured = {}
    client = boto3.client("emr", region_name="ap-northeast-2")

    def _capture_run_job_flow(**kwargs):
        captured.update(kwargs)
        return {"JobFlowId": "j-created", "ResponseMetadata": {}}

    monkeypatch.setattr(client, "run_job_flow", _capture_run_job_flow)
    monkeypatch.setattr(
        client,
        "describe_cluster",
        lambda **kwargs: {"Cluster": {"Status": {"State": "WAITING"}}},
    )
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name: client)

    infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    bootstrap_action = captured["BootstrapActions"][0]["ScriptBootstrapAction"]
    assert bootstrap_action["Path"] == "s3://my-bucket/emr/bootstrap.sh"
    assert bootstrap_action["Args"] == ["my-bucket"]


def test_create_emr_cluster_skips_bootstrap_action_when_explicitly_disabled(monkeypatch):
    """AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI를 빈 문자열로 명시적으로 비우면(로컬/모의
    환경 등 training 패키지가 굳이 안 깔려도 되는 경우) BootstrapActions 없이 뜬다."""
    monkeypatch.setattr(infra, "EMR_BOOTSTRAP_SCRIPT_S3_URI", "")
    captured = {}
    client = boto3.client("emr", region_name="ap-northeast-2")

    def _capture_run_job_flow(**kwargs):
        captured.update(kwargs)
        return {"JobFlowId": "j-created", "ResponseMetadata": {}}

    monkeypatch.setattr(client, "run_job_flow", _capture_run_job_flow)
    monkeypatch.setattr(
        client,
        "describe_cluster",
        lambda **kwargs: {"Cluster": {"Status": {"State": "WAITING"}}},
    )
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name: client)

    infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert "BootstrapActions" not in captured


def test_create_emr_cluster_raises_on_early_termination(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response(
        "describe_cluster",
        {"Cluster": {"Status": {"State": "TERMINATED_WITH_ERRORS"}}},
    )
    stubber.activate()

    with pytest.raises(RuntimeError, match="비정상 종료"):
        infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_create_emr_cluster_terminates_itself_on_waiting_timeout(monkeypatch):
    """WAITING 전에 타임아웃되면 cluster_id가 XCom에 남지 않으므로 직접 종료한다."""
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response("terminate_job_flows", {})
    stubber.activate()

    with pytest.raises(TimeoutError, match="WAITING 상태가 되지 못했습니다"):
        infra.create_emr_cluster(
            core_instance_count=3, timeout_seconds=0, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
        )

    stubber.assert_no_pending_responses()


def test_create_emr_cluster_raises_immediately_when_subnet_id_missing(monkeypatch):
    """AWS_EMR_SUBNET_ID가 비어 있으면 boto3를 부르기도 전에 즉시 실패해야 한다 —
    안 그러면 AWS가 훨씬 늦게(클러스터 생성 요청 자체가 거부된 뒤) 알려줘서
    원인 파악이 느려진다(2026-08-25 첫 실제 실행에서 실측)."""
    monkeypatch.setattr(infra, "EMR_SUBNET_ID", "")

    def _fail(*args, **kwargs):
        raise AssertionError("서브넷 검증에서 걸러졌어야 하므로 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    with pytest.raises(RuntimeError, match="AWS_EMR_SUBNET_ID"):
        infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_create_emr_cluster_raises_immediately_when_security_groups_missing(monkeypatch):
    """AWS_EMR_MASTER_SG_ID/AWS_EMR_CORE_SG_ID가 비어 있으면 boto3를 부르기도
    전에 즉시 실패해야 한다 — 비워두면 EMR이 기본 보안그룹을 스스로 만들려다
    VPC 태그 조건에 막혀 실패한다(2026-08-25, 두 번째 실제 실행에서 실측)."""
    monkeypatch.setattr(infra, "EMR_MASTER_SG_ID", "")

    def _fail(*args, **kwargs):
        raise AssertionError("보안그룹 검증에서 걸러졌어야 하므로 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    with pytest.raises(RuntimeError, match="AWS_EMR_MASTER_SG_ID"):
        infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_create_emr_cluster_mock_mode_returns_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    cluster_id = infra.create_emr_cluster(mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK)

    assert cluster_id.startswith("mock-j-")


# --- submit_emr_step() ---


def test_submit_emr_step_returns_completed_state(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response(
        "describe_step",
        {"Step": {"Status": {"State": "COMPLETED"}}},
    )
    stubber.activate()

    result = infra.submit_emr_step(
        "j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
    )

    assert result == {"StepId": "s-1", "State": "COMPLETED"}


def test_submit_emr_step_polls_through_pending_and_running(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "PENDING"}}})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "RUNNING"}}})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "COMPLETED"}}})
    stubber.activate()
    monkeypatch.setattr(infra.time, "sleep", lambda _seconds: None)

    result = infra.submit_emr_step(
        "j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
    )

    assert result["State"] == "COMPLETED"


def test_submit_emr_step_raises_on_failure(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response(
        "describe_step",
        {"Step": {"Status": {"State": "FAILED", "FailureDetails": {"Reason": "boom"}}}},
    )
    stubber.activate()

    with pytest.raises(RuntimeError, match="실패"):
        infra.submit_emr_step("j-1", "Eval", ["bash", "-c", "false"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_submit_emr_step_mock_mode_returns_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    result = infra.submit_emr_step("j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK)

    assert result["State"] == "COMPLETED"
    assert result["StepId"].startswith("mock-s-")


# --- list_active_emr_clusters() ---


def test_list_active_emr_clusters_filters_by_name_prefix(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    created_at = datetime(2026, 8, 1, tzinfo=UTC)
    stubber.add_response(
        "list_clusters",
        {
            "Clusters": [
                {
                    "Id": "j-1",
                    "Name": "ml-monthly-retrain-rental",
                    "Status": {"State": "WAITING", "Timeline": {"CreationDateTime": created_at}},
                },
                {
                    "Id": "j-2",
                    "Name": "some-other-cluster",
                    "Status": {"State": "RUNNING", "Timeline": {"CreationDateTime": created_at}},
                },
            ]
        },
    )
    stubber.activate()

    clusters = infra.list_active_emr_clusters(mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert [c["id"] for c in clusters] == ["j-1"]
    assert clusters[0]["name"] == "ml-monthly-retrain-rental"
    assert clusters[0]["created_at"] == created_at


def test_list_active_emr_clusters_mock_mode_returns_empty_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    assert infra.list_active_emr_clusters(mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK) == []


# --- get_cluster_step_activity() ---


def test_get_cluster_step_activity_detects_active_step(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response(
        "list_steps",
        {
            "Steps": [
                {"Id": "s-old", "Status": {"State": "COMPLETED", "Timeline": {"EndDateTime": datetime(2026, 8, 25, tzinfo=UTC)}}},
                {"Id": "s-running", "Status": {"State": "RUNNING", "Timeline": {}}},
            ]
        },
    )
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity["has_active_step"] is True


def test_get_cluster_step_activity_returns_last_completed_time_when_idle(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    earlier = datetime(2026, 8, 25, 10, 0, tzinfo=UTC)
    later = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
    stubber.add_response(
        "list_steps",
        {
            "Steps": [
                {"Id": "s-1", "Status": {"State": "COMPLETED", "Timeline": {"EndDateTime": earlier}}},
                {"Id": "s-2", "Status": {"State": "FAILED", "Timeline": {"EndDateTime": later}}},
            ]
        },
    )
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity["has_active_step"] is False
    assert activity["last_step_completed_at"] == later


def test_get_cluster_step_activity_no_steps_yet(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("list_steps", {"Steps": []})
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity == {"has_active_step": False, "last_step_completed_at": None}


def test_get_cluster_step_activity_mock_mode_returns_idle_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    assert infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK) == {
        "has_active_step": False,
        "last_step_completed_at": None,
    }
