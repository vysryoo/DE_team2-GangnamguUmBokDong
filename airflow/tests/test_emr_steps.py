"""월간 재학습 EMR 스텝 명령 조립을 검증한다."""

from orchestration import emr_steps


def test_wait_for_yarn_nodes_step_checks_running_count() -> None:
    name, command = emr_steps.wait_for_yarn_nodes_step(3)
    assert name == "Wait-YARN-Nodes"
    assert command[:2] == ["bash", "-c"]
    assert "yarn node -list -all" in command[2]
    assert "-ge 3" in command[2]


def test_feature_mart_steps_use_runpy_launcher_not_raw_script_path() -> None:
    """패키지 상대 import를 쓰는 스크립트라 파일 경로 대신 runpy launcher로 제출한다."""
    run_pipeline_step, multi_horizon_step = emr_steps.feature_mart_steps("some-profile", 4)

    for _name, command in (run_pipeline_step, multi_horizon_step):
        assert command[:2] == ["bash", "-c"]
        script = command[2]
        # spark-submit의 마지막 인자(primary python file)는 launcher(/tmp/_spark_entry*.py)여야
        # 한다 — feature_engine 안의 실제 스크립트 경로를 직접 넘기면 안 된다.
        primary_file = script.strip().splitlines()[-1].split()[-1]
        assert primary_file.startswith("/tmp/_spark_entry")
        assert "feature_engine/spark/run_pipeline.py" not in script
        assert "feature_engine/spark/build_multi_horizon_features.py" not in script

    assert 'runpy.run_module("feature_engine.spark.run_pipeline"' in run_pipeline_step[1][2]
    assert 'runpy.run_module("feature_engine.spark.build_multi_horizon_features"' in multi_horizon_step[1][2]
    # 이 두 모듈은 자기 코드 안에서 get_spark()로 SparkSession을 직접 만드므로,
    # launcher가 대신 더미 SparkSession을 끼워 넣을 필요가 없다(그건
    # yarn_python_module_step()처럼 Spark를 전혀 안 쓰는 모듈에만 필요하다).
    assert "SparkSession.builder.getOrCreate()" not in run_pipeline_step[1][2]
    assert "SparkSession.builder.getOrCreate()" not in multi_horizon_step[1][2]


def test_feature_mart_steps_num_executors_scales_with_core_count() -> None:
    """`--num-executors`는 실제 클러스터 core 노드 수를 따라간다."""
    small_step, _ = emr_steps.feature_mart_steps("some-profile", 4)
    scaled_step, _ = emr_steps.feature_mart_steps("some-profile", 8)

    assert "--num-executors 3" in small_step[1][2]
    assert "--num-executors 7" in scaled_step[1][2]


def test_yarn_python_module_step_wraps_in_distributed_shell_with_small_am_heap() -> None:
    """master OOM을 피해 YARN distributed-shell로 실행하고, 기본 AM 힙이 부족하므로 명시한다."""
    name, command = emr_steps.yarn_python_module_step(
        "Evaluate-rental",
        "training.scripts.monthly_retrain_check",
        ["--check-only", "--models", "rental", "--result-s3-key", "some/key.json"],
    )
    assert name == "Evaluate-rental"
    assert command[:2] == ["bash", "-c"]
    script = command[2]
    assert "org.apache.hadoop.yarn.applications.distributedshell.Client" in script
    assert "-num_containers 1" in script
    assert f"-master_memory {emr_steps.YARN_AM_MEMORY_MB}" in script
    assert f"-master_vcores {emr_steps.YARN_AM_VCORES}" in script
    assert "-shell_command 'cd /opt/gng && PYTHONPATH=/opt/gng python3.11 -m training.scripts.monthly_retrain_check --check-only --models rental --result-s3-key some/key.json'" in script


def test_yarn_python_module_step_passes_env_via_shell_env() -> None:
    """학습 스텝의 LGB_NUM_MACHINES/LGB_TREE_LEARNER는 distributed-shell
    컨테이너가 부모 프로세스 환경을 상속하지 않으므로 -shell_env로 명시해야
    한다(training/scripts/monthly_retrain_check.py의 같은 제약과 동일)."""
    _name, command = emr_steps.yarn_python_module_step(
        "Train-rental-profile-a",
        "training.scripts.monthly_retrain_check",
        ["--execute"],
        env={"LGB_NUM_MACHINES": "8", "LGB_TREE_LEARNER": "data"},
    )
    script = command[2]
    assert "-shell_env LGB_NUM_MACHINES=8" in script
    assert "-shell_env LGB_TREE_LEARNER=data" in script
    assert f"-shell_env S3_BUCKET={emr_steps.S3_BUCKET}" in script
    assert f"-shell_env PYTHONPATH={emr_steps.EMR_PYTHONPATH}" in script
