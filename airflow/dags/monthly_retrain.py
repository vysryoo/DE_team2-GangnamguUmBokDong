"""대여·반납 모델의 월별 점검, 피처마트 갱신, YARN 분산 학습, 승격을 EMR 클러스터 하나에서 실행하는 DAG."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import UTC, datetime
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import Param
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    MONTHLY_CLUSTER_CREATE_TIMEOUT,
    MONTHLY_EVALUATION_TIMEOUT,
    MONTHLY_FEATURE_REFRESH_TIMEOUT,
    MONTHLY_RETRAIN_CRON,
    MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    MONTHLY_RETRAIN_TOTAL_TIMEOUT,
    MONTHLY_TRAINING_TIMEOUT,
    TIMEZONE,
)
from orchestration.aws_infra_task import (
    EMR_CORE_INSTANCE_TYPE,
    EMR_MLFLOW_TRACKING_URI,
    MOCK_OVERRIDE_FORCE_MOCK,
    MODELS_PREFIX,
    S3_BUCKET,
    TRAINING_RUNS_PREFIX,
    create_emr_cluster,
    read_s3_json,
    submit_emr_step,
    terminate_emr_cluster,
)

logger = logging.getLogger(__name__)

# 한 번에 EMR 클러스터 하나만 쓰도록 대여 -> 반납 순서로 실행함
MODEL_EXECUTION_ORDER = ("rental", "return")

CORE_INSTANCE_COUNT = 8
# YARN distributed-shell의 outer AM, outer worker, inner AM이 차지하는 노드 수
_WRAPPER_NODE_RESERVATION = 3
_EMR_PYTHONPATH = "/opt/gng"
_DEFAULT_PROFILE = "builtin-default"
# 스모크 테스트용 프로필. 격리된 S3 경로를 사용함
TEST_ONLY_PROFILE_NAME = "a-test-sparse-flat"

_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS = ("/usr/lib/hadoop-yarn", "/usr/lib/hadoop", "/opt/hadoop")
# 기본 AM 힙(100MB)으로는 distributed-shell AM이 죽음
_YARN_AM_MEMORY_MB = 1024
_YARN_AM_VCORES = 1

_MLFLOW_COMPOSE_SERVICE_LABEL = "com.docker.compose.service=mlflow"


def _mock_override(context: dict[str, Any]) -> str | None:
    return context["params"].get("mock_mode")


def _core_instance_count(context: dict[str, Any]) -> int:
    return context["params"].get("emr_core_instance_count") or CORE_INSTANCE_COUNT


def _resolve_wrapper_worker_count(core_instance_count: int) -> int:
    worker_count = core_instance_count - _WRAPPER_NODE_RESERVATION
    if worker_count < 1:
        raise ValueError(
            f"emr_core_instance_count={core_instance_count}는 너무 작습니다 — "
            f"최소 {_WRAPPER_NODE_RESERVATION + 1}개 이상이어야 합니다."
        )
    return worker_count


def _task_id(model_name: str, name: str) -> str:
    return f"{name}_{model_name}"


def _result_s3_key(run_id: str, name: str) -> str:
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/{name}.json"


def _cluster_id(ti: Any) -> str | None:
    return ti.xcom_pull(task_ids="create_cluster", key="cluster_id")


def _spark_module_launcher_command(
    module: str, spark_args: list[str], app_args: list[str]
) -> list[str]:
    # feature_engine 스크립트는 패키지 상대 import를 쓰므로 파일 경로로 제출하면
    # __main__으로 실행돼 실패함. runpy launcher로 python -m과 같게 실행함
    entry_path = f"/tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py"
    launcher = (
        f"cat > {entry_path} <<'PYEOF'\n"
        "import runpy\n"
        f'runpy.run_module("{module}", run_name="__main__")\n'
        "PYEOF\n"
    )
    spark_submit = " ".join(
        ["spark-submit", "--deploy-mode", "cluster", "--master", "yarn", *spark_args, entry_path, *app_args]
    )
    return ["bash", "-c", launcher + spark_submit]


def _yarn_python_module_step(
    name: str, module: str, app_args: list[str], env: dict[str, str] | None = None
) -> tuple[str, list[str]]:
    # master 노드에서 직접 실행하면 EMR 데몬과 메모리를 다퉈 OOM이 나므로 core 노드의
    # YARN 컨테이너에서 실행함. 컨테이너는 부모 환경을 상속하지 않아 -shell_env로 넘김
    shell_env = {"S3_BUCKET": S3_BUCKET, "PYTHONPATH": _EMR_PYTHONPATH, **(env or {})}
    if EMR_MLFLOW_TRACKING_URI:
        shell_env["MLFLOW_TRACKING_URI"] = EMR_MLFLOW_TRACKING_URI
    shell_env_args = " ".join(f"-shell_env {key}={value}" for key, value in shell_env.items())
    shell_command = (
        f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} "
        f"python3.11 -m {module} {' '.join(app_args)}"
    )
    search_roots = " ".join(_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS)
    script = (
        f"JAR=$(find {search_roots} -iname '*distributedshell*.jar' 2>/dev/null | head -1); "
        'if [ -z "$JAR" ]; then echo "distributed-shell jar를 찾을 수 없습니다" >&2; exit 1; fi; '
        "yarn org.apache.hadoop.yarn.applications.distributedshell.Client "
        f'-jar "$JAR" -shell_command \'{shell_command}\' '
        "-num_containers 1 -container_memory 6144 -container_vcores 2 "
        f"-master_memory {_YARN_AM_MEMORY_MB} -master_vcores {_YARN_AM_VCORES} "
        "-timeout 345600000 "
        f"{shell_env_args}"
    )
    return name, ["bash", "-c", script]


def _feature_mart_spark_steps(
    profile: str,
    core_instance_count: int = CORE_INSTANCE_COUNT,
    model_name: str | None = None,
    force: bool = False,
) -> tuple[tuple[str, list[str]], tuple[str, list[str]]]:
    # run_pipeline은 watermark 기반 증분이라 반복 호출해도 안전함
    spark_args = [
        # get_spark()가 YARN 실행임을 판단하는 유일한 신호
        "--conf", "spark.yarn.appMasterEnv.SPARK_ON_YARN=1",
        "--conf", "spark.executorEnv.SPARK_ON_YARN=1",
        "--conf", f"spark.yarn.appMasterEnv.ML_PROFILE={profile}",
        "--conf", f"spark.executorEnv.ML_PROFILE={profile}",
        # 없으면 ml/feature_engine/spark/config.py가 local-dev 버킷을 씀
        "--conf", f"spark.yarn.appMasterEnv.S3_BUCKET={S3_BUCKET}",
        "--conf", f"spark.executorEnv.S3_BUCKET={S3_BUCKET}",
        "--conf", f"spark.yarn.appMasterEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf", f"spark.executorEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf", "spark.pyspark.python=/usr/bin/python3.11",
        "--conf", "spark.pyspark.driver.python=/usr/bin/python3.11",
        # m4.large 노드당 YARN 컨테이너 상한이 약 6GB임. 기본 2g 힙은 watermark 없는
        # 최초 실행에서 죽었고, executor를 노드 수보다 하나 적게 잡아 AM 자리를 남김
        "--driver-memory", "4g",
        "--executor-memory", "4g",
        "--num-executors", str(max(core_instance_count - 1, 1)),
        "--conf", "spark.yarn.am.waitTime=1800s",
        "--conf", "spark.sql.shuffle.partitions=24",
        "--conf", "spark.hadoop.fs.s3a.buffer.dir=/mnt/tmp",
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    ]
    force_args = ["--force"] if force else []
    model_args = ["--models", model_name] if model_name else []
    return (
        (
            f"Spark-RunPipeline-{profile}",
            _spark_module_launcher_command(
                "feature_engine.spark.run_pipeline", spark_args, force_args
            ),
        ),
        (
            f"Spark-BuildMultiHorizon-{profile}",
            _spark_module_launcher_command(
                "feature_engine.spark.build_multi_horizon_features",
                spark_args,
                model_args + force_args,
            ),
        ),
    )


def _wait_for_yarn_nodes_step(count: int) -> tuple[str, list[str]]:
    # 클러스터가 WAITING이 된 직후에는 YARN이 AM 등록을 받지 못해 첫 스텝이 죽을 수 있음
    return "Wait-YARN-Nodes", [
        "bash",
        "-c",
        f"until [ $(yarn node -list -all 2>/dev/null | grep -c RUNNING) -ge {count} ]; do sleep 15; done",
    ]


def _champion_profile_name(model_name: str) -> str | None:
    pointer = read_s3_json(f"{MODELS_PREFIX}/champion/{model_name}.json")
    archive_prefix = (pointer or {}).get("archive_prefix")
    if not archive_prefix:
        return None
    payload = read_s3_json(f"{archive_prefix}/{model_name}_profile.json")
    return (payload or {}).get("profile_name")


def _first_profile_value(profile_data: dict, keys: tuple[str, ...], default: int) -> int:
    return next((profile_data[key] for key in keys if profile_data.get(key)), default)


def _extract_profile_feature_params(profile_data: dict) -> tuple[str, int]:
    # 프로필 JSON은 대문자 키와 소문자 키가 섞여 있음
    window = _first_profile_value(
        profile_data, ("ROLLING_WINDOW_MINUTES", "rolling_window_minutes"), 60
    )
    embargo = _first_profile_value(
        profile_data, ("ROLLING_EMBARGO_MINUTES", "rolling_embargo_minutes"), 40
    )
    tick = _first_profile_value(
        profile_data, ("ROLLING_TICK_MINUTES", "rolling_tick_minutes"), 20
    )
    anchor_tick = _first_profile_value(
        profile_data,
        (
            "TRAIN_ANCHOR_TICK_MINUTES",
            "train_anchor_tick_minutes",
            "GRID_TICK_MINUTES",
            "grid_tick_minutes",
        ),
        20,
    )
    return f"w{window}_e{embargo}_t{tick}", int(anchor_tick)


def _is_feature_mart_fresh(profile: str, model_name: str, max_age_hours: float = 24.0) -> bool:
    try:
        profile_data = (
            read_s3_json(f"profiles/{profile}.json")
            or read_s3_json(f"{MODELS_PREFIX}/profiles/{profile}.json")
            or {}
        )
        combo_id, anchor_tick = _extract_profile_feature_params(profile_data)
        base_wm = read_s3_json(f"processed/features/{combo_id}/_watermark.json")
        model_wm = read_s3_json(
            f"processed/features/{combo_id}/training_anchor_a{anchor_tick}/"
            f"_multi_horizon_{model_name}_watermark.json"
        )
        watermarks = (base_wm or {}, model_wm or {})
        if not all(wm.get("updated_at") for wm in watermarks):
            return False
        now = datetime.now(UTC)
        for wm in watermarks:
            if (now - datetime.fromisoformat(wm["updated_at"])).total_seconds() > max_age_hours * 3600:
                return False
        return base_wm.get("max_hour_ts") == model_wm.get("max_hour_ts")
    except Exception:  # noqa: BLE001 - 판정에 실패하면 피처마트를 다시 만듦
        return False


def _create_cluster(**context: Any) -> str:
    params = context["params"]
    cluster_id = create_emr_cluster(
        cluster_name="ml-monthly-retrain",
        core_instance_count=_core_instance_count(context),
        core_instance_type=params.get("emr_core_instance_type") or None,
        mock_override=_mock_override(context),
    )
    context["ti"].xcom_push(key="cluster_id", value=cluster_id)
    return cluster_id


def _refresh_feature_mart(model_name: str, **context: Any) -> None:
    ti = context["ti"]
    params = context["params"]
    mock_override = _mock_override(context)
    cluster_id = _cluster_id(ti)
    core_count = _core_instance_count(context)

    submit_emr_step(
        cluster_id,
        *_wait_for_yarn_nodes_step(core_count),
        timeout_seconds=1200,
        mock_override=mock_override,
    )
    if params.get("test_profile_only"):
        logger.info("[%s] test_profile_only — 챔피언 피처마트 갱신 스킵", model_name)
        return

    force = bool(params.get("force_refresh_feature_mart", False))
    profile = _champion_profile_name(model_name) or _DEFAULT_PROFILE
    if not force and _is_feature_mart_fresh(profile, model_name):
        logger.info("[%s] 프로필 '%s' 피처마트가 24시간 이내 갱신됨 — Spark 스텝 스킵", model_name, profile)
    else:
        logger.info("[%s] 프로필 '%s' 피처마트 %s", model_name, profile, "전체 재생성" if force else "증분 갱신")
        for step_name, spark_args in _feature_mart_spark_steps(
            profile, core_count, model_name=model_name, force=force
        ):
            submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
    ti.xcom_push(key="profile", value=profile)


def _evaluate(model_name: str, **context: Any) -> dict[str, Any]:
    ti = context["ti"]
    if context["params"].get("test_profile_only"):
        logger.info("[%s] test_profile_only — 평가 없이 '%s' 프로필로 재학습", model_name, TEST_ONLY_PROFILE_NAME)
        summary = {"needs_retrain": True, "candidate_profiles": [TEST_ONLY_PROFILE_NAME]}
        ti.xcom_push(key="needs_retrain", value=True)
        ti.xcom_push(key="candidate_profiles", value=[TEST_ONLY_PROFILE_NAME])
        return summary

    result_key = _result_s3_key(context["run_id"], f"eval-{model_name}")
    eval_num_workers = _resolve_wrapper_worker_count(_core_instance_count(context))
    profile = (
        ti.xcom_pull(task_ids=_task_id(model_name, "refresh_feature_mart"), key="profile")
        or _DEFAULT_PROFILE
    )
    name, command = _yarn_python_module_step(
        f"Evaluate-{model_name}",
        "training.scripts.monthly_retrain_check",
        [
            "--check-only", "--models", model_name, "--result-s3-key", result_key,
            "--eval-num-workers", str(eval_num_workers),
        ],
        env={"ML_PROFILE": profile},
    )
    submit_emr_step(_cluster_id(ti), name, command, mock_override=_mock_override(context))

    summary = read_s3_json(result_key) or {
        "needs_retrain": True,
        "retrain_models": [model_name],
        "candidate_profiles": [_DEFAULT_PROFILE],
    }
    ti.xcom_push(key="needs_retrain", value=summary.get("needs_retrain", False))
    ti.xcom_push(key="candidate_profiles", value=summary.get("candidate_profiles") or [_DEFAULT_PROFILE])
    return summary


def _choose_retrain_branch(model_name: str, **context: Any) -> str:
    needs_retrain = context["ti"].xcom_pull(task_ids=_task_id(model_name, "evaluate"), key="needs_retrain")
    if needs_retrain:
        return _task_id(model_name, "orchestrate_retrain_loop")
    return _task_id(model_name, "skip_monthly_retrain")


def _train_profile(
    model_name: str, profile: str, cluster_id: str, context: dict[str, Any]
) -> bool:
    params = context["params"]
    mock_override = _mock_override(context)
    core_count = _core_instance_count(context)
    for step_name, spark_args in _feature_mart_spark_steps(
        profile,
        core_count,
        model_name=model_name,
        force=bool(params.get("force_refresh_feature_mart", False)),
    ):
        submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)

    result_key = _result_s3_key(context["run_id"], f"train-{model_name}-{profile}")
    train_args = [
        "--execute",
        "--performance-already-checked",
        "--skip-feature-pipeline",
        "--profile-name", profile,
        "--models", model_name,
        "--result-s3-key", result_key,
    ]
    if params.get("test_profile_only"):
        train_args.append("--no-promote")
    name, command = _yarn_python_module_step(
        f"Train-{model_name}-{profile}",
        "training.scripts.monthly_retrain_check",
        train_args,
        env={
            "LGB_NUM_MACHINES": str(_resolve_wrapper_worker_count(core_count)),
            "LGB_TREE_LEARNER": "data",
        },
    )
    submit_emr_step(
        cluster_id,
        name,
        command,
        timeout_seconds=int(MONTHLY_TRAINING_TIMEOUT.total_seconds()),
        mock_override=mock_override,
    )
    summary = read_s3_json(result_key) or {"promoted": {model_name: False}}
    return bool(summary.get("promoted", {}).get(model_name))


def _orchestrate_retrain_loop(model_name: str, **context: Any) -> dict[str, Any]:
    ti = context["ti"]
    cluster_id = _cluster_id(ti)
    candidate_profiles = (
        ti.xcom_pull(task_ids=_task_id(model_name, "evaluate"), key="candidate_profiles")
        or [_DEFAULT_PROFILE]
    )
    _resolve_wrapper_worker_count(_core_instance_count(context))
    logger.info("[%s] 챌린저 재학습 시작 (후보 프로필: %s)", model_name, candidate_profiles)

    # 한 프로필이 실패해도 다음 후보를 시도하고, 승격되면 멈춤
    results_by_profile: dict[str, Any] = {}
    for profile in candidate_profiles:
        try:
            promoted = _train_profile(model_name, profile, cluster_id, context)
        except (RuntimeError, OSError, ValueError, TimeoutError) as exc:
            logger.error("[%s 프로필: %s] 재학습 중 오류 발생: %s", model_name, profile, exc)
            results_by_profile[profile] = {"status": "failed", "error": str(exc)}
            continue
        results_by_profile[profile] = {"status": "success", "promoted": promoted}
        if promoted:
            logger.info("[%s 프로필: %s] 챔피언 승격 성공", model_name, profile)
            break
    return {"status": "completed", "profiles": results_by_profile}


def _terminate_cluster(**context: Any) -> None:
    cluster_id = _cluster_id(context["ti"])
    if not cluster_id:
        logger.warning("cluster_id 없음(생성 실패) — 종료할 클러스터 없음")
        return
    terminate_emr_cluster(cluster_id, mock_override=_mock_override(context))


def _docker_mlflow_container_action(action: str) -> None:
    find_cmd = [
        "curl", "-sf", "--unix-socket", "/var/run/docker.sock",
        f"http://localhost/containers/json?all=1&filters=%7B%22label%22%3A%5B%22{_MLFLOW_COMPOSE_SERVICE_LABEL}%22%5D%7D",
    ]
    result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)
    containers = json.loads(result.stdout)
    if not containers:
        logger.warning("[mlflow %s] label=%s인 컨테이너를 못 찾음 — 건너뜀", action, _MLFLOW_COMPOSE_SERVICE_LABEL)
        return
    container_id = containers[0]["Id"]
    subprocess.run(
        ["curl", "-sf", "-X", "POST", "--unix-socket", "/var/run/docker.sock", f"http://localhost/containers/{container_id}/{action}"],
        check=True,
    )
    logger.info("[mlflow %s] 컨테이너 %s에 적용 완료", action, container_id[:12])


def _start_mlflow(**context: Any) -> None:
    if _mock_override(context) == MOCK_OVERRIDE_FORCE_MOCK:
        return
    try:
        _docker_mlflow_container_action("start")
    except Exception:
        logger.exception("[mlflow] 시작 실패 — 재학습은 계속 진행")


def _stop_mlflow(**context: Any) -> None:
    if _mock_override(context) == MOCK_OVERRIDE_FORCE_MOCK:
        return
    try:
        _docker_mlflow_container_action("stop")
    except Exception:
        logger.exception("[mlflow] 정지 실패")


def _build_model_chain(model_name: str, trigger_rule: str) -> tuple[PythonOperator, list]:
    model_kwargs = {"model_name": model_name}
    refresh_feature_mart = PythonOperator(
        task_id=_task_id(model_name, "refresh_feature_mart"),
        python_callable=_refresh_feature_mart,
        op_kwargs=model_kwargs,
        execution_timeout=MONTHLY_FEATURE_REFRESH_TIMEOUT,
        trigger_rule=trigger_rule,
    )
    evaluate = PythonOperator(
        task_id=_task_id(model_name, "evaluate"),
        python_callable=_evaluate,
        op_kwargs=model_kwargs,
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )
    check_retrain_branch = BranchPythonOperator(
        task_id=_task_id(model_name, "check_retrain_branch"),
        python_callable=_choose_retrain_branch,
        op_kwargs=model_kwargs,
    )
    orchestrate_retrain_loop = PythonOperator(
        task_id=_task_id(model_name, "orchestrate_retrain_loop"),
        python_callable=_orchestrate_retrain_loop,
        op_kwargs=model_kwargs,
        execution_timeout=MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    )
    skip_monthly_retrain = EmptyOperator(task_id=_task_id(model_name, "skip_monthly_retrain"))

    ends = [orchestrate_retrain_loop, skip_monthly_retrain]
    refresh_feature_mart >> evaluate >> check_retrain_branch >> ends
    return refresh_feature_mart, ends


with DAG(
    dag_id="monthly_retrain",
    schedule=CronTriggerTimetable(MONTHLY_RETRAIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    dagrun_timeout=MONTHLY_RETRAIN_TOTAL_TIMEOUT,
    tags=["ml", "monthly", "retrain", "emr", "yarn", "rental", "return"],
    params={
        "mock_mode": Param(
            "auto",
            type="string",
            enum=["auto", "force_mock", "force_real"],
            description=(
                "auto: 환경변수로 자동 판별. "
                "force_mock: EMR 호출 없이 mock. "
                "force_real: 실제 AWS EMR 호출."
            ),
        ),
        "emr_core_instance_type": Param(
            EMR_CORE_INSTANCE_TYPE,
            type="string",
            description="EMR core 노드 인스턴스 타입.",
        ),
        "emr_core_instance_count": Param(
            CORE_INSTANCE_COUNT,
            type="integer",
            minimum=_WRAPPER_NODE_RESERVATION + 1,
            maximum=10,
            description="클러스터 Core 노드 수.",
        ),
        "force_refresh_feature_mart": Param(
            False,
            type="boolean",
            description="피처마트 강제 전체 재생성 여부.",
        ),
        "test_profile_only": Param(
            False,
            type="boolean",
            description=f"스모크 테스트용 프로필({TEST_ONLY_PROFILE_NAME}) 단독 실행 여부.",
        ),
    },
) as dag:
    start_mlflow = PythonOperator(task_id="start_mlflow", python_callable=_start_mlflow)
    stop_mlflow = PythonOperator(
        task_id="stop_mlflow",
        python_callable=_stop_mlflow,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    stop_mlflow.as_teardown(setups=start_mlflow)

    create_cluster = PythonOperator(
        task_id="create_cluster",
        python_callable=_create_cluster,
        execution_timeout=MONTHLY_CLUSTER_CREATE_TIMEOUT,
    )
    # DAG run을 수동으로 실패 처리해도 실행되도록 teardown으로 둠. evaluate 등을
    # setup에 넣으면 그 태스크가 실패했을 때 teardown이 실행되지 않음
    terminate_cluster = PythonOperator(
        task_id="terminate_cluster",
        python_callable=_terminate_cluster,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    terminate_cluster.as_teardown(setups=create_cluster)

    start_mlflow >> create_cluster
    previous_ends: list = [create_cluster]
    for index, model_name in enumerate(MODEL_EXECUTION_ORDER):
        chain_start, chain_ends = _build_model_chain(
            model_name,
            TriggerRule.ALL_SUCCESS if index == 0 else TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        previous_ends >> chain_start
        previous_ends = chain_ends
    previous_ends >> terminate_cluster
    terminate_cluster >> stop_mlflow
