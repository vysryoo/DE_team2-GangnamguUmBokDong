"""Orchestration task builder의 production CLI·JSON XCom 계약을 검증한다."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import jinja2
import pytest
from airflow.sdk.execution_time import macros
from airflow.task.trigger_rule import TriggerRule

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from orchestration import collector_task
from orchestration.collector_task import (
    COLLECTOR_DIR,
    build_collector_task,
    build_weather_freshness_gate_task,
    build_window_stats_task,
)
from orchestration.refine_task import (
    NORMALIZER_DIR,
    NOWCASTING_DIR,
    POI_MASTER_DIR,
    build_normalizer_task,
    build_nowcasting_task,
    build_poi_master_refresh_task,
    build_poi_master_resolve_task,
    build_station_master_enrichment_task,
)
from orchestration.serving_task import (
    LOADER_DIR,
    ML_DIR,
    build_event_publication_task,
    build_finalize_serving_task,
    build_inference_task,
    build_prepare_serving_task,
    build_routes_task,
    build_urgency_task,
)
from orchestration.task_builder import REPO_ROOT, build_module_task, uv_python_command
from orchestration.templates import (
    KST_WINDOW_START,
    kst_date_days_ago,
    kst_day_hour_replay_days_ago,
    kst_hours_ago_date,
    kst_hours_ago_hour,
    kst_window_start_shifted,
)


def test_repo_root_resolves_to_repository_root() -> None:
    """Task cwd 기준 저장소 루트를 실제 module directory와 결합한다."""
    assert (REPO_ROOT / "collector").is_dir()
    assert (REPO_ROOT / "poi_master").is_dir()
    assert (REPO_ROOT / "loader").is_dir()
    assert (REPO_ROOT / "ml" / "inference").is_dir()


def test_existing_task_keeps_provider_default_output_processor(dag) -> None:
    """JSON 출력이 없는 기존 task에 None processor를 덮어쓰지 않는다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert task.cwd == COLLECTOR_DIR
    assert task.append_env is True
    assert callable(task.output_processor)
    assert task.output_processor("plain output") == "plain output"


def test_collector_task_uses_kst_window_and_own_project_environment(dag) -> None:
    """Collector wrapper가 frozen project와 KST 5분 window 계약을 유지한다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector "
    )
    assert (
        "uv run --frozen python main.py --source bike_station_realtime"
        in task.bash_command
    )
    assert "astimezone" in task.bash_command
    assert "// 5" in task.bash_command
    assert task.cwd == COLLECTOR_DIR


def test_collector_task_forwards_source_retry_policy(dag) -> None:
    """Collector wrapper가 source별 retry 횟수와 간격을 Operator에 전달한다."""
    task = build_collector_task(
        dag,
        "living_population_grid",
        retries=4,
        retry_delay=timedelta(minutes=10),
    )

    assert task.retries == 4
    assert task.retry_delay == timedelta(minutes=10)


def test_weather_freshness_gate_only_skips_direct_downstream(dag) -> None:
    """다운스트림 전체를 강제 스킵하면 weather_ready_gate 이후 체인이 통째로 멈춘다."""
    from airflow.providers.standard.operators.python import ShortCircuitOperator

    gate = build_weather_freshness_gate_task(
        dag, "weather_ultra_short_live", min_interval=timedelta(minutes=10)
    )

    assert isinstance(gate, ShortCircuitOperator)
    assert gate.ignore_downstream_trigger_rules is False
    assert gate.retries == 0
    assert gate.execution_timeout == timedelta(seconds=30)
    assert gate.task_id == "freshness_gate_weather_ultra_short_live"


def test_weather_freshness_gate_callable_delegates_with_seconds(monkeypatch, dag) -> None:
    """min_interval을 초 단위로 바꿔 _check_source_due에 그대로 넘긴다."""
    captured = {}

    def fake_check(source_id, min_interval_seconds):
        captured["source_id"] = source_id
        captured["min_interval_seconds"] = min_interval_seconds
        return True

    monkeypatch.setattr(collector_task, "_check_source_due", fake_check)

    gate = build_weather_freshness_gate_task(
        dag, "weather_short_term_forecast", min_interval=timedelta(hours=3)
    )

    assert gate.python_callable() is True
    assert captured == {
        "source_id": "weather_short_term_forecast",
        "min_interval_seconds": 10800,
    }


def test_check_source_due_parses_collector_cli_json(monkeypatch) -> None:
    """collector CLI의 --check-due-after-seconds JSON 출력을 그대로 신뢰한다."""
    captured_kwargs = {}

    def fake_run(cmd, **kwargs):
        captured_kwargs["cmd"] = cmd
        captured_kwargs.update(kwargs)
        return SimpleNamespace(stdout='{"due": false, "elapsed_seconds": 42.0}\n')

    monkeypatch.setattr(collector_task.subprocess, "run", fake_run)

    result = collector_task._check_source_due("weather_ultra_short_live", 600)

    assert result is False
    assert captured_kwargs["cwd"] == COLLECTOR_DIR
    assert captured_kwargs["timeout"] == 30
    assert captured_kwargs["env"]["UV_PROJECT_ENVIRONMENT"] == "/opt/venvs/modules/collector"
    assert captured_kwargs["cmd"][-4:] == [
        "--source", "weather_ultra_short_live",
        "--check-due-after-seconds", "600",
    ]


def test_module_task_rejects_unsafe_environment_name(dag) -> None:
    """모듈 환경 이름이 고정 volume 경로를 벗어나지 못하게 한다."""
    with pytest.raises(ValueError, match="lowercase"):
        build_module_task(
            dag,
            "unsafe_environment",
            COLLECTOR_DIR,
            "true",
            uv_environment_name="../collector",
        )


def test_module_task_runs_command_directly_in_module_environment(dag) -> None:
    """모듈 명령을 다른 프로세스로 감싸지 않고 모듈 venv에서 바로 실행한다."""
    task = build_module_task(dag, "plain", COLLECTOR_DIR, "printf 'done'")

    assert task.bash_command == (
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector printf 'done'"
    )


def test_uv_python_command_joins_args_and_optional_project() -> None:
    """uv 명령 조립 형태를 고정한다."""
    assert uv_python_command("main.py", "--source", "x") == (
        "uv run --frozen python main.py --source x"
    )
    assert uv_python_command("-m", "pkg.cli", project="inference") == (
        "uv --project inference run --frozen python -m pkg.cli"
    )


def test_normalizer_task_contract(dag) -> None:
    """Population normalizer는 기존 frozen project와 all-success 계약을 유지한다."""
    task = build_normalizer_task(dag, poi_master_task_id="resolve_x")
    assert task.task_id == "run_normalizer"
    assert task.cwd == NORMALIZER_DIR
    assert "python main.py --window-start" in task.bash_command
    assert '--poi-master-mode "$POI_MASTER_MODE"' in task.bash_command
    assert task.env is not None
    assert "resolve_x" in task.env["POI_MASTER_MODE"]
    assert "--baseline-date-mode" not in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS


def test_population_collector_consumes_exact_poi_master_ref(dag) -> None:
    """poi_master_task_id를 주면 resolver의 mode·manifest exact ref를 전달한다."""
    task = build_collector_task(
        dag, "population_realtime", poi_master_task_id="resolve_x"
    )

    assert task.task_id == "collect_population_realtime"
    assert task.cwd == COLLECTOR_DIR
    assert "--source population_realtime" in task.bash_command
    assert '--poi-master-mode "$POI_MASTER_MODE"' in task.bash_command
    assert '--poi-master-manifest-uri "$POI_MASTER_MANIFEST_URI"' in task.bash_command
    assert (
        '--poi-master-manifest-sha256 "$POI_MASTER_MANIFEST_SHA256"'
        in task.bash_command
    )
    assert task.env == {
        "POI_MASTER_MODE": "{{ ti.xcom_pull(task_ids='resolve_x')['mode'] }}",
        "POI_MASTER_MANIFEST_URI": (
            "{{ ti.xcom_pull(task_ids='resolve_x').get('manifest_uri') or '' }}"
        ),
        "POI_MASTER_MANIFEST_SHA256": (
            "{{ ti.xcom_pull(task_ids='resolve_x').get('manifest_sha256') or '' }}"
        ),
    }


def test_poi_master_refresh_and_resolve_builder_contracts(dag) -> None:
    """POI Master 앱의 refresh·resolve CLI와 JSON XCom 계약을 고정한다."""
    refresh = build_poi_master_refresh_task(dag)
    resolve = build_poi_master_resolve_task(dag)

    assert refresh.cwd == resolve.cwd == POI_MASTER_DIR
    for task in (refresh, resolve):
        assert task.bash_command.startswith(
            "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/poi-master "
        )
    assert "python main.py refresh" in refresh.bash_command
    assert "python main.py resolve" in resolve.bash_command
    assert '--as-of "$POI_MASTER_AS_OF"' in resolve.bash_command
    assert resolve.env == {"POI_MASTER_AS_OF": KST_WINDOW_START}
    assert refresh.output_processor('{"status":"unchanged"}') == {
        "status": "unchanged"
    }
    assert resolve.output_processor('{"mode":"static"}') == {"mode": "static"}


def test_station_master_enrichment_builder_contract(dag) -> None:
    """대여소 master enrichment CLI 계약을 고정한다."""
    task = build_station_master_enrichment_task(dag)

    assert task.cwd == NORMALIZER_DIR
    assert "python station_master.py" in task.bash_command
    assert "--baseline-date-mode" not in task.bash_command
    assert "astimezone" in task.bash_command


def test_nowcasting_task_uses_date_and_exact_source_window(dag) -> None:
    """Nowcasting은 추정 기준일과 Collector의 exact logical time을 함께 받는다."""
    task = build_nowcasting_task(dag)

    assert task.cwd == NOWCASTING_DIR
    assert "main.py estimate --target-date" in task.bash_command
    assert "--source-window-start" in task.bash_command
    assert KST_WINDOW_START in task.bash_command
    assert "strftime" in task.bash_command


def test_prepare_task_emits_json_and_uses_templated_env(dag) -> None:
    """Prepare는 shell interpolation 대신 templated env로 logical time을 받는다."""
    task = build_prepare_serving_task(dag)

    assert task.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py prepare" in task.bash_command
    assert task.env is not None
    assert "astimezone" in task.env["SERVING_LOGICAL_DTTM"]
    assert "ti.xcom_pull" not in task.bash_command
    assert task.append_env is True
    assert task.output_processor('{"plan":{"uri":"s3://b/k"}}') == {
        "plan": {"uri": "s3://b/k"}
    }


def test_inference_task_consumes_only_plan_ref_json(dag) -> None:
    """Inference는 plan URI·SHA만 XCom으로 받고 immutable producer CLI를 실행한다."""
    task = build_inference_task(dag, plan_task_id="prepare_x")

    assert task.cwd == ML_DIR
    assert task.cwd.endswith("/ml")
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/ml-inference "
    )
    assert "--frozen python -m inference.publication_cli" in task.bash_command
    assert "predict_single" not in task.bash_command
    assert task.env == {
        "PLAN_URI": "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['uri'] }}",
        "PLAN_SHA256": (
            "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in task.bash_command
    assert set(task.env) == {"PLAN_URI", "PLAN_SHA256"}
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS
    assert task.output_processor(json.dumps({"inference": {}})) == {"inference": {}}


def test_finalize_task_uses_exact_refs_and_disables_single_task_retry(dag) -> None:
    """Drift 실패는 같은 refs 재시도로 치유되지 않아 final 단독 retry를 금지한다."""
    task = build_finalize_serving_task(
        dag,
        plan_task_id="prepare_x",
        inference_task_id="inference_x",
    )

    assert task.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py finalize" in task.bash_command
    assert task.env == {
        "PLAN_URI": "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['uri'] }}",
        "PLAN_SHA256": (
            "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['byte_sha256'] }}"
        ),
        "INFERENCE_URI": (
            "{{ ti.xcom_pull(task_ids='inference_x')['inference']['uri'] }}"
        ),
        "INFERENCE_SHA256": (
            "{{ ti.xcom_pull(task_ids='inference_x')['inference']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in task.bash_command
    assert task.retries == 0
    assert callable(task.output_processor)
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader "
    )


def test_urgency_and_route_use_release_refs_without_legacy_compute(dag) -> None:
    """Derived downstream task가 loader publisher만 호출하고 stale retry를 하지 않는다."""
    urgency = build_urgency_task(dag, final_task_id="final_x")
    route = build_routes_task(dag, urgency_task_id="urgency_x")

    assert urgency.cwd == route.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py urgency" in urgency.bash_command
    assert "uv run --frozen python serving_cli.py route" in route.bash_command
    assert "rebalance" not in urgency.cwd
    assert urgency.retries == route.retries == 0
    for task in (urgency, route):
        assert task.bash_command.startswith(
            "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader "
        )
    assert urgency.env == {
        "STATION_URI": "{{ ti.xcom_pull(task_ids='final_x')['station']['uri'] }}",
        "STATION_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')['station']['byte_sha256'] }}"
        ),
        "DEMAND_URI": (
            "{{ ti.xcom_pull(task_ids='final_x')['station_demand_forecast']['uri'] }}"
        ),
        "DEMAND_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')"
            "['station_demand_forecast']['byte_sha256'] }}"
        ),
        "STOCK_URI": ("{{ ti.xcom_pull(task_ids='final_x')['station_stock']['uri'] }}"),
        "STOCK_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')['station_stock']['byte_sha256'] }}"
        ),
    }
    assert route.env == {
        "URGENCY_URI": (
            "{{ ti.xcom_pull(task_ids='urgency_x')['station_urgency']['uri'] }}"
        ),
        "URGENCY_SHA256": (
            "{{ ti.xcom_pull(task_ids='urgency_x')['station_urgency']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in urgency.bash_command
    assert "ti.xcom_pull" not in route.bash_command


def test_event_publication_task_contract(dag) -> None:
    """행사 source만 gold_cli의 event publication으로 게시한다."""
    task = build_event_publication_task(dag, "cultural_event")
    assert task.task_id == "publish_event_cultural_event"
    assert task.cwd == LOADER_DIR
    assert "uv run --frozen python gold_cli.py" in task.bash_command
    assert "--publication event:cultural_event" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS


def test_replay_collector_remains_nonblocking_side_chain(dag) -> None:
    """과거 rental 보강은 force/all-done 계약을 유지한다."""
    task = build_collector_task(
        dag,
        "bike_rental_history",
        task_id="collect_bike_rental_history_replay_1h",
        window_start=kst_window_start_shifted(1),
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    assert task.task_id == "collect_bike_rental_history_replay_1h"
    assert task.cwd == COLLECTOR_DIR
    assert "--source bike_rental_history" in task.bash_command
    assert "--force" in task.bash_command
    assert "timedelta(hours=1)" in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_DONE


def test_module_wrappers_attach_success_and_failure_callbacks(dag) -> None:
    """공용 wrapper가 운영 성공·실패 callback을 유지한다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert on_success_callback in task.on_success_callback
    assert on_failure_callback in task.on_failure_callback


def test_plan_and_source_tasks_share_same_five_minute_template(dag) -> None:
    """Plan logical time과 source task가 동일한 5분 KST floor를 사용한다."""
    collector = build_collector_task(dag, "bike_station_realtime")
    normalizer = build_normalizer_task(
        dag,
        "normalize_for_template",
        poi_master_task_id="resolve_for_template",
    )
    prepare = build_prepare_serving_task(dag)

    for task in (collector, normalizer):
        assert "// 5" in task.bash_command
        assert "second=0" in task.bash_command
    assert prepare.env == {"SERVING_LOGICAL_DTTM": KST_WINDOW_START}


def test_replay_template_renders_to_a_whole_hour_earlier() -> None:
    """Replay template가 5분 floor 뒤 정확히 whole hour만큼 이동한다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 18, 19, 33, 12, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    base = environment.from_string(KST_WINDOW_START).render(context)
    shifted = environment.from_string(kst_window_start_shifted(1)).render(context)

    assert datetime.fromisoformat(base) == datetime(2026, 8, 18, 19, 30, tzinfo=kst)
    assert datetime.fromisoformat(shifted) == datetime(2026, 8, 18, 18, 30, tzinfo=kst)
    assert datetime.fromisoformat(
        environment.from_string(kst_window_start_shifted(2)).render(context)
    ) == datetime(2026, 8, 18, 17, 30, tzinfo=kst)


def test_replay_template_rejects_non_positive_hours() -> None:
    """Replay offset은 양의 시간만 허용한다."""
    with pytest.raises(ValueError):
        kst_window_start_shifted(0)


def test_daily_replay_templates_render_d_minus_six_boundaries() -> None:
    """D-6의 첫·마지막 API window boundary를 정확히 렌더링한다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 19, 4, 30, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    target_date = environment.from_string(kst_date_days_ago(6)).render(context)
    first_end = environment.from_string(kst_day_hour_replay_days_ago(6, 0)).render(
        context
    )
    last_end = environment.from_string(kst_day_hour_replay_days_ago(6, 23)).render(
        context
    )

    assert target_date == "2026-08-13"
    assert datetime.fromisoformat(first_end) == datetime(2026, 8, 13, 0, 55, tzinfo=kst)
    assert datetime.fromisoformat(last_end) == datetime(2026, 8, 13, 23, 55, tzinfo=kst)


def test_window_stats_task_reports_a_full_day_without_hour_flag(dag) -> None:
    """day_template만 주면 --window-hour 없이 하루 전체를 요청한다."""
    task = build_window_stats_task(
        dag, "bike_station_realtime", day_template=kst_date_days_ago(1)
    )

    assert task.task_id == "collection_stats_bike_station_realtime"
    assert task.cwd == COLLECTOR_DIR
    assert "--report-window-stats" in task.bash_command
    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-day" in task.bash_command
    assert "--window-hour" not in task.bash_command
    assert callable(task.output_processor)
    assert task.output_processor('{"run_count": 0}') == {"run_count": 0}


def test_window_stats_task_adds_hour_flag_when_given(dag) -> None:
    """hour_template을 주면 --window-hour가 함께 붙는다."""
    task = build_window_stats_task(
        dag,
        "weather_ultra_short_live",
        day_template=kst_hours_ago_date(1),
        hour_template=kst_hours_ago_hour(1),
    )

    assert "--window-day" in task.bash_command
    assert "--window-hour" in task.bash_command


def test_hours_ago_templates_render_one_hour_earlier() -> None:
    """kst_hours_ago_date/hour가 기준 시각에서 정확히 N시간 전 날짜·시를 렌더링한다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 27, 8, 0, 0, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    date_rendered = environment.from_string(kst_hours_ago_date(1)).render(context)
    hour_rendered = environment.from_string(kst_hours_ago_hour(1)).render(context)

    assert date_rendered == "2026-08-27"
    assert hour_rendered == "07"


def test_hours_ago_templates_cross_day_boundary() -> None:
    """자정 직후 기준 시각이면 전날 23시로 넘어간다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 27, 0, 0, 0, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    date_rendered = environment.from_string(kst_hours_ago_date(1)).render(context)
    hour_rendered = environment.from_string(kst_hours_ago_hour(1)).render(context)

    assert date_rendered == "2026-08-26"
    assert hour_rendered == "23"


def test_hours_ago_templates_reject_non_positive_hours() -> None:
    with pytest.raises(ValueError):
        kst_hours_ago_date(0)
    with pytest.raises(ValueError):
        kst_hours_ago_hour(0)
