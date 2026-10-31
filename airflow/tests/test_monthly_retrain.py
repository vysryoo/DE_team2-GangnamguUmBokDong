"""monthly_retrain DAG 구조와 단일 EMR 클러스터 생애주기 오케스트레이션을 검증한다."""

from datetime import timedelta
from functools import partial
from typing import NamedTuple
from unittest.mock import MagicMock

import pytest
from airflow.task.trigger_rule import TriggerRule

import dags.monthly_retrain as monthly_dag

_DAG = monthly_dag.dag


class _SubmittedStep(NamedTuple):
    cluster_id: str
    name: str
    command: list[str]
    timeout_seconds: int | None


@pytest.fixture
def submitted_steps(monkeypatch) -> list[_SubmittedStep]:
    """`submit_emr_step` 호출을 순서대로 기록하고 항상 COMPLETED를 반환한다."""
    steps: list[_SubmittedStep] = []

    def _submit(cluster_id, name, command, **kwargs):
        steps.append(_SubmittedStep(cluster_id, name, command, kwargs.get("timeout_seconds")))
        return {"StepId": f"s-{len(steps)}", "State": "COMPLETED"}

    monkeypatch.setattr(monthly_dag, "submit_emr_step", _submit)
    return steps


def _names(steps: list[_SubmittedStep]) -> list[str]:
    return [step.name for step in steps]


def _commands(steps: list[_SubmittedStep]) -> dict[str, list[str]]:
    return {step.name: step.command for step in steps}


def test_monthly_retrain_is_a_single_combined_dag() -> None:
    """단일 EMR 클러스터 위에서 대여/반납 두 모델 체인이 순차적으로 실행되어야 한다."""
    assert _DAG.dag_id == "monthly_retrain"
    expected_tasks = {
        f"{name}_{model}"
        for model in ("rental", "return")
        for name in (
            "refresh_feature_mart",
            "evaluate",
            "check_retrain_branch",
            "orchestrate_retrain_loop",
            "skip_monthly_retrain",
        )
    } | {"create_cluster", "terminate_cluster", "start_mlflow", "stop_mlflow"}
    assert set(_DAG.task_ids) == expected_tasks


def test_monthly_retrain_runs_rental_fully_before_return_starts() -> None:
    """대여 사이클이 완전히 끝난 뒤에만 반납 사이클이 시작돼야 한다."""
    assert monthly_dag.MODEL_EXECUTION_ORDER == ("rental", "return")
    return_start = _DAG.get_task("refresh_feature_mart_return")
    rental_ends = {"orchestrate_retrain_loop_rental", "skip_monthly_retrain_rental"}
    assert return_start.upstream_task_ids == rental_ends


def test_start_stop_mlflow_wrap_the_whole_dag() -> None:
    """mlflow는 클러스터 생성 전에 켜고 종료 뒤에 끄며, stop은 teardown이다."""
    start_mlflow = _DAG.get_task("start_mlflow")
    stop_mlflow = _DAG.get_task("stop_mlflow")
    assert start_mlflow.downstream_task_ids == {"create_cluster", "stop_mlflow"}
    assert stop_mlflow.upstream_task_ids == {"terminate_cluster", "start_mlflow"}
    assert stop_mlflow.is_teardown is True


def test_monthly_retrain_total_timeout_covers_both_models_sequentially() -> None:
    """두 모델을 순차 실행하므로 전체 timeout은 모델 하나 몫의 2배다."""
    assert _DAG.dagrun_timeout == timedelta(hours=240)
    assert _DAG.dagrun_timeout >= 2 * monthly_dag.MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT


def test_monthly_retrain_terminate_cluster_is_a_real_teardown() -> None:
    """DAG run을 수동으로 실패 처리하면 일반 태스크는 강제 skip되므로 클러스터 종료는 teardown이어야 한다."""
    terminate_cluster = _DAG.get_task("terminate_cluster")
    create_cluster = _DAG.get_task("create_cluster")
    assert terminate_cluster.is_teardown is True
    assert terminate_cluster.trigger_rule == TriggerRule.ALL_DONE_SETUP_SUCCESS
    assert create_cluster.is_setup is True
    assert "create_cluster" in terminate_cluster.upstream_task_ids


def test_monthly_retrain_evaluate_is_not_a_setup() -> None:
    """evaluate가 setup이면 평가 실패 시 teardown 조건을 못 채워 클러스터가 남는다."""
    for model_name in ("rental", "return"):
        evaluate = _DAG.get_task(f"evaluate_{model_name}")
        assert evaluate.is_setup is False


def test_refresh_feature_mart_runs_between_create_cluster_and_evaluate() -> None:
    """evaluate가 최근 구간을 읽기 전에 feature mart를 먼저 최신화해야 한다."""
    refresh_rental = _DAG.get_task("refresh_feature_mart_rental")
    assert refresh_rental.upstream_task_ids == {"create_cluster"}
    assert refresh_rental.downstream_task_ids == {"evaluate_rental"}

    refresh_return = _DAG.get_task("refresh_feature_mart_return")
    assert refresh_return.upstream_task_ids == {"orchestrate_retrain_loop_rental", "skip_monthly_retrain_rental"}
    assert refresh_return.downstream_task_ids == {"evaluate_return"}


def test_check_retrain_branch_decisions() -> None:
    """needs_retrain 값에 따라 올바른(모델 접미사가 붙은) 다운스트림 태스크로 분기한다."""
    mock_ti = MagicMock()
    branch_fn = partial(monthly_dag._choose_retrain_branch, "rental")

    # Case 1: 재학습 필요 시 orchestrate_retrain_loop_rental로 분기
    mock_ti.xcom_pull.return_value = True
    branch = branch_fn(ti=mock_ti)
    assert branch == "orchestrate_retrain_loop_rental"

    # Case 2: 재학습 불필요 시 skip_monthly_retrain_rental로 분기
    mock_ti.xcom_pull.return_value = False
    branch = branch_fn(ti=mock_ti)
    assert branch == "skip_monthly_retrain_rental"


def test_create_cluster_pushes_cluster_id(monkeypatch) -> None:
    """클러스터를 생성하고 cluster_id만 xcom에 남긴다 — 평가는 별도 태스크(evaluate)다."""
    mock_ti = MagicMock()
    monkeypatch.setattr(monthly_dag, "create_emr_cluster", lambda **kwargs: "j-created")

    task_fn = monthly_dag._create_cluster
    result = task_fn(ti=mock_ti, params={})

    assert result == "j-created"
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["cluster_id"] == "j-created"


def test_refresh_feature_mart_submits_feature_mart_steps_for_champion_profile(monkeypatch, submitted_steps) -> None:
    """챔피언 프로필로 run_pipeline과 build_multi_horizon 스텝을 제출한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "champion_profile_name", lambda model_name: "leaves127")


    task_fn = partial(monthly_dag._refresh_feature_mart, "rental")
    task_fn(ti=mock_ti, params={})

    assert {step.cluster_id for step in submitted_steps} == {"j-created"}
    assert "Spark-RunPipeline-leaves127" in _names(submitted_steps)
    assert "Spark-BuildMultiHorizon-leaves127" in _names(submitted_steps)
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["profile"] == "leaves127"


def test_refresh_feature_mart_waits_for_yarn_nodes_before_spark_steps(monkeypatch, submitted_steps) -> None:
    """WAITING 직후에는 YARN이 AM 등록을 받지 못할 수 있어 첫 Spark 스텝 전에 노드를 기다린다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "champion_profile_name", lambda model_name: "leaves127")


    task_fn = partial(monthly_dag._refresh_feature_mart, "rental")
    task_fn(ti=mock_ti, params={})

    assert _names(submitted_steps)[0] == "Wait-YARN-Nodes"
    assert _names(submitted_steps).index("Wait-YARN-Nodes") < _names(submitted_steps).index("Spark-RunPipeline-leaves127")


def test_refresh_feature_mart_falls_back_to_builtin_default_when_no_champion(monkeypatch, submitted_steps) -> None:
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "champion_profile_name", lambda model_name: None)


    task_fn = partial(monthly_dag._refresh_feature_mart, "rental")
    task_fn(ti=mock_ti, params={})

    assert any("builtin-default" in s for s in _names(submitted_steps))


def test_refresh_feature_mart_skips_champion_build_when_test_profile_only(monkeypatch, submitted_steps) -> None:
    """test_profile_only=True면 챔피언 feature mart는 만들 필요가 없다 —
    orchestrate_retrain_loop가 TEST_ONLY_PROFILE_NAME으로 어차피 다시 만든다.
    다만 Wait-YARN-Nodes는 그대로 제출해야 한다 — 뒤에 오는 retrain 루프의 첫
    스텝이 AM 등록 타임아웃으로 죽지 않으려면 이 대기가 반드시 필요하다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"


    task_fn = partial(monthly_dag._refresh_feature_mart, "rental")
    task_fn(ti=mock_ti, params={"test_profile_only": True})

    assert _names(submitted_steps) == ["Wait-YARN-Nodes"]
    assert not any("Spark-RunPipeline" in s for s in _names(submitted_steps))


def test_refresh_feature_mart_gives_wait_for_yarn_nodes_a_short_timeout(monkeypatch, submitted_steps) -> None:
    """YARN 노드 대기는 스텝 기본 timeout 대신 짧은 상한을 써서 피처마트 갱신 전체 timeout을 넘기지 않는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"

    task_fn = partial(monthly_dag._refresh_feature_mart, "rental")
    task_fn(ti=mock_ti, params={"test_profile_only": True})

    assert {step.name: step.timeout_seconds for step in submitted_steps}["Wait-YARN-Nodes"] == 1200


def test_evaluate_uses_same_profile_refresh_feature_mart_just_built(monkeypatch, submitted_steps) -> None:
    """evaluate는 refresh_feature_mart가 XCom에 남긴 프로필로 방금 갱신한 피처마트를 읽는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "cluster_id": "j-created",
        "profile": "champion-profile-x",
    }.get(key)
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: {"needs_retrain": False})

    task_fn = partial(monthly_dag._evaluate, "rental")
    task_fn(ti=mock_ti, params={}, run_id="run-1")

    script = submitted_steps[-1].command[2]
    assert "-shell_env ML_PROFILE=champion-profile-x" in script


def test_evaluate_defaults_profile_to_builtin_when_refresh_pushed_nothing(monkeypatch, submitted_steps) -> None:
    """refresh_feature_mart가 profile을 xcom에 못 남긴 경우(예: 과거 실행 기록이
    없는 등)에도 evaluate가 죽지 않고 builtin-default로 안전하게 fallback한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {"cluster_id": "j-created"}.get(key)
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: {"needs_retrain": False})

    task_fn = partial(monthly_dag._evaluate, "rental")
    task_fn(ti=mock_ti, params={}, run_id="run-1")

    script = submitted_steps[-1].command[2]
    assert "-shell_env ML_PROFILE=builtin-default" in script


def test_evaluate_pushes_needs_retrain(monkeypatch, submitted_steps) -> None:
    """create_cluster가 xcom에 남긴 cluster_id로 평가 스텝을 제출하고, 결과(S3 JSON)를
    읽어 needs_retrain/candidate_profiles를 xcom에 남긴다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(
        monthly_dag,
        "read_s3_json",
        lambda key: {"needs_retrain": True, "candidate_profiles": ["rental-profile-1"]},
    )

    task_fn = partial(monthly_dag._evaluate, "rental")
    result = task_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["needs_retrain"] is True
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["needs_retrain"] is True
    assert pushed["candidate_profiles"] == ["rental-profile-1"]
    # master 노드 OOM(exitCode 137) 이후 evaluate는 core 노드의 YARN
    # distributed-shell 컨테이너에서 돌아야 한다 — _yarn_python_module_step()
    # docstring 참고.
    assert "org.apache.hadoop.yarn.applications.distributedshell.Client" in submitted_steps[-1].command[2]


def test_evaluate_passes_eval_num_workers_matching_provisioned_core_count(monkeypatch, submitted_steps) -> None:
    """평가(predict)는 학습과 달리 워커끼리 통신할 필요가 없는 embarrassingly
    parallel 작업이라(monitor_performance.combine_evaluation_shards() 참고),
    create_cluster가 이미 띄워둔 core 노드 수만큼 그대로 나눠 돌릴 수 있다 —
    실제로 띄운 노드 수(emr_core_instance_count override 포함)와 어긋나면
    존재하지 않는 컨테이너를 기다리다 barrier가 타임아웃난다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: {"needs_retrain": False})

    task_fn = partial(monthly_dag._evaluate, "rental")
    task_fn(ti=mock_ti, params={}, run_id="run-1")

    script = submitted_steps[-1].command[2]
    expected = monthly_dag.CORE_INSTANCE_COUNT - monthly_dag._WRAPPER_NODE_RESERVATION
    assert f"--eval-num-workers {expected}" in script


@pytest.mark.parametrize("core_count,expected_workers", [(4, 1), (5, 2), (8, 5)])
def test_resolve_wrapper_worker_count_boundary_values(core_count, expected_workers) -> None:
    """outer AM/워커 컨테이너 + inner AM 최악 배치 예약분(3)을 뺀 값 — 4가
    inner 워커를 최소 1개라도 띄울 수 있는 가장 작은 유효 core 수다."""
    assert monthly_dag._resolve_wrapper_worker_count(core_count) == expected_workers


@pytest.mark.parametrize("core_count", [1, 2, 3])
def test_resolve_wrapper_worker_count_rejects_too_small_core_count(core_count) -> None:
    """core 노드가 wrapper 예약분 이하이면 학습 워커가 남지 않으므로 즉시 실패한다."""
    with pytest.raises(ValueError, match="너무 작습니다"):
        monthly_dag._resolve_wrapper_worker_count(core_count)


def test_emr_core_instance_count_param_minimum_matches_wrapper_reservation() -> None:
    """DAG Param의 minimum도 런타임 fail-fast(`_resolve_wrapper_worker_count`)와
    같은 기준(`_WRAPPER_NODE_RESERVATION + 1`)이어야 UI에서부터 막힌다."""
    param = _DAG.params.get_param("emr_core_instance_count")
    assert param.schema["minimum"] == monthly_dag._WRAPPER_NODE_RESERVATION + 1


def test_evaluate_eval_num_workers_respects_core_instance_count_override(monkeypatch, submitted_steps) -> None:
    """override(5) - _WRAPPER_NODE_RESERVATION(3) = 2 — outer AM/워커 컨테이너 +
    inner distributed-shell 자신의 AM까지, 최악의 배치(co-locate 안 됨)를
    가정하면 노드 3개가 필요하므로 그만큼 빼야 barrier가 존재하지 않는
    컨테이너를 기다리다 타임아웃나지 않는다(실제 EMR 실행에서 확인,
    2026-08-26)."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: {"needs_retrain": False})

    task_fn = partial(monthly_dag._evaluate, "rental")
    task_fn(ti=mock_ti, params={"emr_core_instance_count": 5}, run_id="run-1")

    script = submitted_steps[-1].command[2]
    assert "--eval-num-workers 2" in script


def test_evaluate_defaults_to_needs_retrain_when_no_result(monkeypatch, submitted_steps) -> None:
    """스텝이 결과를 못 남겼을 때(주로 mock 모드) 재학습 루프 구조가 계속 검증되도록
    needs_retrain=True로 기본값을 잡는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "mock-j-1"
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: None)

    task_fn = partial(monthly_dag._evaluate, "rental")
    result = task_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["needs_retrain"] is True
    assert result["candidate_profiles"] == ["builtin-default"]


def test_evaluate_forces_test_profile_when_test_profile_only(monkeypatch, submitted_steps) -> None:
    """test_profile_only=True면 실제 EMR 성능 점검 스텝을 아예 제출하지 않고,
    바로 TEST_ONLY_PROFILE_NAME 하나만 재학습 대상으로 xcom에 남긴다 — 재평가를
    건너뛰고 작은 프로필로 전체 파이프라인을 빠르게 스모크 테스트하는 용도."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"

    task_fn = partial(monthly_dag._evaluate, "rental")
    result = task_fn(ti=mock_ti, params={"test_profile_only": True}, run_id="run-1")

    assert submitted_steps == []
    assert result["needs_retrain"] is True
    assert result["candidate_profiles"] == [monthly_dag.TEST_ONLY_PROFILE_NAME]
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["needs_retrain"] is True
    assert pushed["candidate_profiles"] == [monthly_dag.TEST_ONLY_PROFILE_NAME]


def test_orchestrate_retrain_loop_submits_feature_mart_then_trains_without_resize(monkeypatch, submitted_steps) -> None:
    """재학습 루프가 프로필마다 피처마트 스텝을 제출하고 YARN distributed-shell 학습
    스텝을 제출한다 — 클러스터가 이미 학습 단계 노드 수로 생성돼 있으므로(resize
    로직이 진행 중이던 작업을 죽이거나 목표치까지 못 올라가는 사례가 의심돼
    2026-08-26에 제거) 이 루프는 더 이상 resize를 태우지 않는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "cluster_id": "j-1",
        "candidate_profiles": ["profile-a", "profile-b"],
    }.get(key)

    # 첫 프로필은 승격 실패, 두 번째 프로필은 승격 성공 -> 루프가 거기서 멈춰야 한다.
    monkeypatch.setattr(
        monthly_dag,
        "read_s3_json",
        lambda key: {"promoted": {"rental": "profile-b" in key}},
    )

    loop_fn = partial(monthly_dag._orchestrate_retrain_loop, "rental")
    result = loop_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["status"] == "completed"
    assert result["profiles"]["profile-a"]["promoted"] is False
    assert result["profiles"]["profile-b"]["promoted"] is True
    assert any("Spark-RunPipeline-profile-a" in s for s in _names(submitted_steps))
    assert any("Spark-RunPipeline-profile-b" in s for s in _names(submitted_steps))
    assert any("Train-rental-profile-a" in s for s in _names(submitted_steps))
    assert any("Train-rental-profile-b" in s for s in _names(submitted_steps))
    # 학습 오케스트레이터도 master가 아니라 core 노드 YARN distributed-shell
    # 컨테이너에서 돌아야 한다(evaluate와 같은 이유 — _yarn_python_module_step()
    # docstring 참고).
    train_command = _commands(submitted_steps)["Train-rental-profile-a"][2]
    assert "org.apache.hadoop.yarn.applications.distributedshell.Client" in train_command
    assert "--performance-already-checked" in train_command
    expected_machines = monthly_dag.CORE_INSTANCE_COUNT - monthly_dag._WRAPPER_NODE_RESERVATION
    assert f"-shell_env LGB_NUM_MACHINES={expected_machines}" in train_command


def test_orchestrate_retrain_loop_passes_no_promote_when_test_profile_only(monkeypatch, submitted_steps) -> None:
    """test_profile_only는 "재평가만 건너뛴다"는 뜻이지 "승격을 막는다"는 뜻이
    아니다 — 테스트 프로필이 우연히 챔피언보다 좋게 나오면 이 플래그 없이는
    진짜 챔피언이 바뀐다. 학습 스텝에 --no-promote를 반드시 같이 실어야 한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "cluster_id": "j-1",
        "candidate_profiles": [monthly_dag.TEST_ONLY_PROFILE_NAME],
    }.get(key)

    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: {"promoted": {"rental": False}})

    loop_fn = partial(monthly_dag._orchestrate_retrain_loop, "rental")
    loop_fn(ti=mock_ti, params={"test_profile_only": True}, run_id="run-1")

    train_command = _commands(submitted_steps)[f"Train-rental-{monthly_dag.TEST_ONLY_PROFILE_NAME}"][2]
    assert "--no-promote" in train_command


def test_terminate_cluster_task_calls_terminate_when_cluster_exists(monkeypatch) -> None:
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-1"
    terminated = []
    monkeypatch.setattr(monthly_dag, "terminate_emr_cluster", lambda cluster_id, **kwargs: terminated.append(cluster_id))

    task_fn = monthly_dag._terminate_cluster
    task_fn(ti=mock_ti, params={})

    assert terminated == ["j-1"]


def test_terminate_cluster_task_no_ops_when_cluster_id_missing(monkeypatch) -> None:
    """클러스터 생성 자체가 실패해 cluster_id가 없으면 종료를 시도하지 않는다
    (터미네이트 대상이 아예 없으므로 에러가 아니라 정상 no-op)."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = None
    terminated = []
    monkeypatch.setattr(monthly_dag, "terminate_emr_cluster", lambda cluster_id, **kwargs: terminated.append(cluster_id))

    task_fn = monthly_dag._terminate_cluster
    task_fn(ti=mock_ti, params={})

    assert terminated == []


@pytest.mark.parametrize(
    "task_fn,mock_mode,expected_calls",
    [
        (monthly_dag._start_mlflow, None, ["start"]),
        (monthly_dag._start_mlflow, "force_mock", []),
        (monthly_dag._stop_mlflow, None, ["stop"]),
    ],
)
def test_mlflow_tasks_call_docker_action_unless_force_mock(
    monkeypatch, task_fn, mock_mode, expected_calls
) -> None:
    calls = []
    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", lambda action: calls.append(action))

    task_fn(params={} if mock_mode is None else {"mock_mode": mock_mode})

    assert calls == expected_calls


def test_stop_mlflow_does_not_raise_when_docker_action_fails(monkeypatch) -> None:
    """mlflow 정지 실패가 DAG teardown 체인 전체를 실패시키면 안 된다 — 부가
    기능(모니터링)일 뿐이라 실패를 삼키고 경고만 남긴다."""

    def _boom(action):
        raise RuntimeError("docker socket 없음")

    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", _boom)

    monthly_dag._stop_mlflow(params={})  # raise하지 않아야 한다
