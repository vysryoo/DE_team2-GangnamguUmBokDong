"""단일 realtime tick DAG(5분 격자 + 날씨 freshness gate)를 검증한다."""

from datetime import datetime, timedelta, timezone
from itertools import pairwise
from types import SimpleNamespace

import pytest
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

import dags.realtime_tick as realtime_tick_dag
from config.schedules import REALTIME_TICK_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from dags.realtime_tick import dag

_WEATHER_SOURCES = (
    "weather_ultra_short_live",
    "weather_ultra_short_forecast",
    "weather_short_term_forecast",
)


def test_schedule_is_five_minute_grid_without_catchup() -> None:
    """5분 격자로 매번 돌고, single active run에 catchup은 없다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert REALTIME_TICK_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(minutes=15)
    assert dag.on_failure_callback is realtime_tick_dag._on_realtime_tick_failure


def _core_task_ids() -> set[str]:
    return (
        {f"collect_{source}" for source in REALTIME_5MIN_SOURCES}
        | {
            "allow_current_tick",
            "run_normalizer",
            "resolve_poi_master",
            "prepare_serving_plan",
            "run_inference",
            "finalize_serving_release",
            "publish_station_urgency",
            "publish_rebalance_route",
            "weather_ready_gate",
        }
        | {f"collect_{source}" for source in _WEATHER_SOURCES}
        | {f"freshness_gate_{source}" for source in _WEATHER_SOURCES}
        | {
            f"collect_bike_rental_history_replay_{hour}h"
            for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
        }
    )


def test_all_realtime_roots_wait_for_current_tick_gate() -> None:
    """실제 작업의 모든 root는 stale run 판정 뒤에만 실행된다."""
    gate = dag.get_task("allow_current_tick")
    assert isinstance(gate, ShortCircuitOperator)
    assert gate.retries == 0
    assert realtime_tick_dag.on_success_callback in gate.on_success_callback
    assert realtime_tick_dag.on_failure_callback in gate.on_failure_callback
    assert gate.ignore_downstream_trigger_rules is True
    assert gate.upstream_task_ids == set()
    assert gate.downstream_task_ids == {
        "resolve_poi_master",
        "collect_bike_rental_history",
        "collect_bike_station_realtime",
        *{f"freshness_gate_{source}" for source in _WEATHER_SOURCES},
    }


def test_current_scheduled_tick_is_allowed(monkeypatch) -> None:
    """worker가 늦게 떠도 DAG Run이 정시에 시작됐으면 정상 실행한다."""
    logical_date = datetime(2026, 8, 27, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        realtime_tick_dag,
        "_now",
        lambda: datetime(2026, 8, 27, 0, 2, tzinfo=timezone.utc),
    )
    messages = []
    monkeypatch.setattr(realtime_tick_dag, "send_message", messages.append)

    assert realtime_tick_dag._allow_current_tick(
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-08-27T00:00:00+00:00",
            run_type="scheduled",
            clear_number=0,
            logical_date=logical_date,
            start_date=datetime(2026, 8, 27, 0, 0, 30, tzinfo=timezone.utc),
        )
    ) is True
    assert messages == []


def test_stale_scheduled_tick_is_skipped_and_alerted(monkeypatch) -> None:
    """1분 넘게 밀린 scheduled run은 Slack 알림 후 스킵한다."""
    logical_date = datetime(2026, 8, 27, 0, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(
        realtime_tick_dag,
        "_now",
        lambda: datetime(2026, 8, 27, 0, 8, tzinfo=timezone.utc),
    )
    messages = []
    monkeypatch.setattr(realtime_tick_dag, "send_message", messages.append)
    monkeypatch.setattr(realtime_tick_dag, "de2_group_mention", lambda: "@de2조")

    assert realtime_tick_dag._allow_current_tick(
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-08-27T00:05:00+00:00",
            run_type="scheduled",
            clear_number=0,
            logical_date=logical_date,
            start_date=datetime(2026, 8, 27, 0, 7, tzinfo=timezone.utc),
        )
    ) is False
    assert len(messages) == 1
    assert "120초" in messages[0]
    assert "scheduled__2026-08-27T00:05:00+00:00" in messages[0]
    assert "@de2조" not in messages[0]


def test_manual_tick_is_always_allowed(monkeypatch) -> None:
    """의도적으로 실행한 오래된 logical date는 스킵하지 않는다."""
    monkeypatch.setattr(
        realtime_tick_dag,
        "_now",
        lambda: datetime(2026, 8, 27, 1, 0, tzinfo=timezone.utc),
    )

    assert realtime_tick_dag._allow_current_tick(
        dag_run=SimpleNamespace(
            run_id="manual__2026-08-27T00:00:00+00:00",
            run_type="manual",
            clear_number=0,
            logical_date=datetime(2026, 8, 27, 0, 0, tzinfo=timezone.utc),
            start_date=datetime(2026, 8, 27, 1, 0, tzinfo=timezone.utc),
        )
    ) is True


def test_cleared_scheduled_tick_is_always_allowed() -> None:
    """UI나 CLI에서 Clear한 과거 scheduled run은 복구 실행한다."""
    assert realtime_tick_dag._allow_current_tick(
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-08-27T00:05:00+00:00",
            run_type="scheduled",
            clear_number=1,
            logical_date=datetime(2026, 8, 27, 0, 5, tzinfo=timezone.utc),
            start_date=datetime(2026, 8, 27, 1, 0, tzinfo=timezone.utc),
        )
    ) is True


def test_stale_tick_is_still_skipped_when_slack_fails(monkeypatch, caplog) -> None:
    """Slack 장애가 stale run 정리를 막지 않는다."""
    logical_date = datetime(2026, 8, 27, 0, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(
        realtime_tick_dag,
        "_now",
        lambda: datetime(2026, 8, 27, 0, 8, tzinfo=timezone.utc),
    )

    def fail_to_send(_message: str) -> None:
        raise RuntimeError("slack unavailable")

    monkeypatch.setattr(realtime_tick_dag, "send_message", fail_to_send)

    assert realtime_tick_dag._allow_current_tick(
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-08-27T00:05:00+00:00",
            run_type="scheduled",
            clear_number=0,
            logical_date=logical_date,
            start_date=datetime(2026, 8, 27, 0, 7, tzinfo=timezone.utc),
        )
    ) is False
    assert "Slack 알림 전송에 실패" in caplog.text


def test_dag_failure_alert_mentions_team(monkeypatch) -> None:
    """DAG Run timeout을 포함한 실패는 팀 멘션과 함께 알린다."""
    messages = []
    monkeypatch.setattr(realtime_tick_dag, "send_message", messages.append)
    monkeypatch.setattr(realtime_tick_dag, "de2_group_mention", lambda: "@de2조")

    realtime_tick_dag._on_realtime_tick_failure(
        {
            "dag_run": SimpleNamespace(
                run_id="scheduled__2026-08-27T00:05:00+00:00",
                logical_date=datetime(2026, 8, 27, 0, 5, tzinfo=timezone.utc),
            )
        }
    )

    assert len(messages) == 1
    assert "@de2조" in messages[0]
    assert "0:15:00" in messages[0]


def test_dag_has_exactly_the_expected_tasks() -> None:
    assert set(dag.task_ids) == _core_task_ids()


@pytest.mark.parametrize("source_id", _WEATHER_SOURCES)
def test_weather_collector_sits_behind_its_own_freshness_gate(source_id: str) -> None:
    """날씨 collector마다 freshness gate 하나가 직접 상위에 있다."""
    gate = dag.get_task(f"freshness_gate_{source_id}")
    collect = dag.get_task(f"collect_{source_id}")

    assert isinstance(gate, ShortCircuitOperator)
    assert gate.retries == 0
    # 다운스트림 전체를 강제로 스킵시키면(기본값 True) weather_ready_gate 이후
    # 체인까지 통째로 멈춘다 — 직접 하위(collect)만 스킵돼야 한다.
    assert gate.ignore_downstream_trigger_rules is False
    assert collect.upstream_task_ids == {f"freshness_gate_{source_id}"}

    # 재시도 없이 60초 안에 실패시킨다.
    assert f"--source {source_id}" in collect.bash_command
    assert collect.retries == 0
    assert collect.execution_timeout == timedelta(seconds=60)


def test_weather_ready_gate_tolerates_skipped_or_failed_collectors() -> None:
    """freshness gate로 건너뛰어졌거나 실패한 날씨 collector가 있어도 게이트는 진행한다."""
    gate = dag.get_task("weather_ready_gate")
    assert isinstance(gate, EmptyOperator)
    assert gate.trigger_rule == TriggerRule.ALL_DONE
    assert gate.upstream_task_ids == {f"collect_{source}" for source in _WEATHER_SOURCES}


def test_prepare_plan_waits_for_station_and_weather_gate() -> None:
    prepare = dag.get_task("prepare_serving_plan")
    assert prepare.upstream_task_ids == {"collect_bike_station_realtime", "weather_ready_gate"}
    assert prepare.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


def test_inference_waits_for_plan_normalizer_and_rental_input() -> None:
    inference = dag.get_task("run_inference")
    assert inference.upstream_task_ids == {
        "prepare_serving_plan",
        "collect_bike_rental_history",
        "run_normalizer",
    }
    assert dag.get_task("run_normalizer").upstream_task_ids == {
        "collect_population_realtime"
    }
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS


def test_population_and_normalizer_share_one_pinned_poi_master() -> None:
    """실시간 인구 수집과 공간 보정이 같은 resolver XCom만 사용한다."""
    resolver = dag.get_task("resolve_poi_master")
    population = dag.get_task("collect_population_realtime")
    normalizer = dag.get_task("run_normalizer")

    assert population.upstream_task_ids == {resolver.task_id}
    assert normalizer.upstream_task_ids == {population.task_id}
    assert population.env == normalizer.env == {
        "POI_MASTER_MODE": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')['mode'] }}"
        ),
        "POI_MASTER_MANIFEST_URI": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')"
            ".get('manifest_uri') or '' }}"
        ),
        "POI_MASTER_MANIFEST_SHA256": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')"
            ".get('manifest_sha256') or '' }}"
        ),
    }
    for task in (population, normalizer):
        assert '--poi-master-mode "$POI_MASTER_MODE"' in task.bash_command
        assert (
            '--poi-master-manifest-uri "$POI_MASTER_MANIFEST_URI"'
            in task.bash_command
        )
        assert (
            '--poi-master-manifest-sha256 "$POI_MASTER_MANIFEST_SHA256"'
            in task.bash_command
        )


def test_other_collectors_do_not_receive_poi_master_arguments() -> None:
    """POI Master 계약이 population 외 Collector의 CLI를 바꾸지 않는다."""
    for source_id in REALTIME_5MIN_SOURCES:
        if source_id == "population_realtime":
            continue
        task = dag.get_task(f"collect_{source_id}")
        assert "--poi-master-" not in task.bash_command
        assert task.env is None


def test_derived_publication_is_one_strict_chain() -> None:
    assert dag.get_task("finalize_serving_release").upstream_task_ids == {
        "run_inference"
    }
    assert dag.get_task("publish_station_urgency").upstream_task_ids == {
        "finalize_serving_release"
    }
    assert dag.get_task("publish_rebalance_route").upstream_task_ids == {
        "publish_station_urgency"
    }


def test_json_tasks_use_bash_env_contract() -> None:
    """Production task는 append_env와 JSON output processor를 사용한다."""
    for task_id in (
        "prepare_serving_plan",
        "run_inference",
        "finalize_serving_release",
        "publish_station_urgency",
        "publish_rebalance_route",
    ):
        task = dag.get_task(task_id)
        assert isinstance(task, BashOperator)
        assert task.append_env is True
        assert callable(task.output_processor)
    assert dag.get_task("finalize_serving_release").retries == 0
    assert dag.get_task("publish_station_urgency").retries == 0
    assert dag.get_task("publish_rebalance_route").retries == 0


def test_collector_task_execution_contract() -> None:
    """현재 station collector의 retry·timeout·window 계약을 유지한다."""
    task = dag.get_task("collect_bike_station_realtime")

    assert isinstance(task, BashOperator)
    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(seconds=240)
    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector "
    )


def test_living_population_grid_timeout_override_not_used() -> None:
    assert "collect_living_population_grid" not in dag.task_ids


def _replay_tasks() -> list[BashOperator]:
    return [
        dag.get_task(f"collect_bike_rental_history_replay_{hour}h")
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_lookback_is_two_hours() -> None:
    assert RENTAL_HISTORY_LOOKBACK_HOURS == 2


def test_replay_tasks_exist_for_each_lookback_hour() -> None:
    assert [task.task_id for task in _replay_tasks()] == [
        f"collect_bike_rental_history_replay_{hour}h"
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_replay_uses_force_for_late_returns() -> None:
    for task in _replay_tasks():
        assert "--force" in task.bash_command
        assert "--backfill" not in task.bash_command
        assert "--source bike_rental_history" in task.bash_command


def test_replay_runs_sequentially_to_cap_api_concurrency() -> None:
    chain = [dag.get_task("collect_bike_rental_history"), *_replay_tasks()]

    for upstream, downstream in pairwise(chain):
        assert downstream.task_id in upstream.downstream_task_ids


def test_replay_failure_does_not_block_inference() -> None:
    for task in _replay_tasks():
        assert task.trigger_rule == TriggerRule.ALL_DONE
        assert "run_inference" not in task.downstream_task_ids


def test_inference_waits_only_for_current_rental_collection() -> None:
    upstream = dag.get_task("run_inference").upstream_task_ids

    assert "collect_bike_rental_history" in upstream
    for task in _replay_tasks():
        assert task.task_id not in upstream
