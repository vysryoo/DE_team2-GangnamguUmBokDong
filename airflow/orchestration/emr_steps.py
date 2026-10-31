"""상시 EMR 클러스터에 제출할 스텝 명령을 만든다.

각 스텝은 ``(이름, command-runner.jar 인자)`` 쌍이다. 클러스터 생성·제출·대기는
``aws_infra_task``가 맡고, 이 모듈은 명령 문자열만 조립한다.
"""

from __future__ import annotations

from orchestration.aws_infra_task import EMR_MLFLOW_TRACKING_URI, S3_BUCKET

EmrStep = tuple[str, list[str]]

EMR_PYTHONPATH = "/opt/gng"
_PYTHON = "/usr/bin/python3.11"

_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS = ("/usr/lib/hadoop-yarn", "/usr/lib/hadoop", "/opt/hadoop")
# 기본 AM 힙(100MB)으로는 distributed-shell AM이 죽음
YARN_AM_MEMORY_MB = 1024
YARN_AM_VCORES = 1


def wait_for_yarn_nodes_step(count: int) -> EmrStep:
    """RUNNING YARN 노드가 ``count``개 이상이 될 때까지 기다리는 스텝을 만든다.

    클러스터가 WAITING이 된 직후에는 YARN이 AM 등록을 받지 못해 첫 스텝이 죽을 수 있음.

    Args:
        count: 기다릴 RUNNING 노드 수
    Returns:
        ``Wait-YARN-Nodes`` 스텝
    """
    return "Wait-YARN-Nodes", [
        "bash",
        "-c",
        f"until [ $(yarn node -list -all 2>/dev/null | grep -c RUNNING) -ge {count} ]; do sleep 15; done",
    ]


def feature_mart_steps(
    profile: str,
    core_instance_count: int,
    *,
    model_name: str | None = None,
    force: bool = False,
) -> tuple[EmrStep, EmrStep]:
    """프로필의 피처마트를 갱신하는 Spark 스텝 2개를 실행 순서대로 만든다.

    run_pipeline은 watermark 기반 증분이라 반복 호출해도 안전함.

    Args:
        profile: ``ML_PROFILE``로 넘길 프로필 이름
        core_instance_count: 클러스터 core 노드 수. executor 수를 정함
        model_name: 지정하면 multi-horizon 피처를 이 모델만 만듦
        force: 참이면 두 스텝 모두 ``--force``로 전체 재생성
    Returns:
        ``(run_pipeline 스텝, build_multi_horizon_features 스텝)``
    """
    spark_args = [
        *_spark_env_conf({
            # get_spark()가 YARN 실행임을 판단하는 유일한 신호
            "SPARK_ON_YARN": "1",
            "ML_PROFILE": profile,
            # 없으면 ml/feature_engine/spark/config.py가 local-dev 버킷을 씀
            "S3_BUCKET": S3_BUCKET,
            "PYTHONPATH": EMR_PYTHONPATH,
        }),
        "--conf", f"spark.pyspark.python={_PYTHON}",
        "--conf", f"spark.pyspark.driver.python={_PYTHON}",
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
            _spark_module_command("feature_engine.spark.run_pipeline", spark_args, force_args),
        ),
        (
            f"Spark-BuildMultiHorizon-{profile}",
            _spark_module_command(
                "feature_engine.spark.build_multi_horizon_features",
                spark_args,
                model_args + force_args,
            ),
        ),
    )


def yarn_python_module_step(
    name: str, module: str, app_args: list[str], env: dict[str, str] | None = None
) -> EmrStep:
    """Spark를 쓰지 않는 Python 모듈을 core 노드의 YARN 컨테이너에서 실행하는 스텝을 만든다.

    master 노드에서 직접 실행하면 EMR 데몬과 메모리를 다퉈 OOM이 남. 컨테이너는 부모
    환경을 상속하지 않아 환경변수를 ``-shell_env``로 넘김.

    Args:
        name: 스텝 이름
        module: ``python -m``으로 실행할 모듈
        app_args: 모듈 인자
        env: 컨테이너에 추가로 넘길 환경변수
    Returns:
        distributed-shell 스텝
    """
    shell_env = {"S3_BUCKET": S3_BUCKET, "PYTHONPATH": EMR_PYTHONPATH, **(env or {})}
    if EMR_MLFLOW_TRACKING_URI:
        shell_env["MLFLOW_TRACKING_URI"] = EMR_MLFLOW_TRACKING_URI
    shell_env_args = " ".join(f"-shell_env {key}={value}" for key, value in shell_env.items())
    shell_command = (
        f"cd {EMR_PYTHONPATH} && PYTHONPATH={EMR_PYTHONPATH} "
        f"python3.11 -m {module} {' '.join(app_args)}"
    )
    search_roots = " ".join(_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS)
    script = (
        f"JAR=$(find {search_roots} -iname '*distributedshell*.jar' 2>/dev/null | head -1); "
        'if [ -z "$JAR" ]; then echo "distributed-shell jar를 찾을 수 없습니다" >&2; exit 1; fi; '
        "yarn org.apache.hadoop.yarn.applications.distributedshell.Client "
        f'-jar "$JAR" -shell_command \'{shell_command}\' '
        "-num_containers 1 -container_memory 6144 -container_vcores 2 "
        f"-master_memory {YARN_AM_MEMORY_MB} -master_vcores {YARN_AM_VCORES} "
        "-timeout 345600000 "
        f"{shell_env_args}"
    )
    return name, ["bash", "-c", script]


def _spark_env_conf(env: dict[str, str]) -> list[str]:
    # driver(AM)와 executor가 같은 환경변수를 봐야 하므로 항상 쌍으로 넘김
    args: list[str] = []
    for key, value in env.items():
        args += [
            "--conf", f"spark.yarn.appMasterEnv.{key}={value}",
            "--conf", f"spark.executorEnv.{key}={value}",
        ]
    return args


def _spark_module_command(module: str, spark_args: list[str], app_args: list[str]) -> list[str]:
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
