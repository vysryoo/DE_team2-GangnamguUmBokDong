"""일 배치 compaction DAG와 태스크 빌더를 검증한다."""

from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import COMPACTION_CRON
from config.sources import COLD_BRONZE_SOURCES, COMPACTION_SOURCES
from dags.daily_compaction import dag
from orchestration.compaction_task import COLLECTOR_DIR, build_compaction_task


class TestCompactionTask:
    def test_runs_collector_maintenance_cli(self, dag):
        task = build_compaction_task(dag, "bike_station_realtime")

        assert task.task_id == "compact_bike_station_realtime"
        assert task.cwd == COLLECTOR_DIR
        assert (
            "uv run --frozen python maintenance.py compact --source bike_station_realtime"
            in task.bash_command
        )

    def test_passes_no_date_so_collector_derives_its_own_range(self, dag):
        """검사 범위는 Collector가 유도한다 — Airflow가 recovery 범위를 알 필요가 없다."""
        task = build_compaction_task(dag, "bike_rental_history")

        assert "--date" not in task.bash_command
        assert "--from" not in task.bash_command

    def test_no_virtual_env_leak(self, dag):
        task = build_compaction_task(dag, "bike_station_realtime")

        assert task.bash_command.startswith("env -u VIRTUAL_ENV ")


class TestCompactionSources:
    def test_forecast_sources_are_excluded(self):
        """예보는 사후 재현이 불가해 archive 가치가 낮다."""
        assert "weather_ultra_short_forecast" not in COMPACTION_SOURCES
        assert "weather_short_term_forecast" not in COMPACTION_SOURCES

    def test_covers_the_four_target_sources(self):
        assert set(COMPACTION_SOURCES) == {
            "bike_rental_history",
            "bike_station_realtime",
            "population_realtime",
            "weather_ultra_short_live",
        }

    def test_cold_bronze_covers_every_collector_source(self):
        """공통 Hot Lifecycle 전에 모든 Collector source를 Cold로 보존한다."""
        assert set(COLD_BRONZE_SOURCES) == {
            "bike_rental_history",
            "bike_station_realtime",
            "population_realtime",
            "weather_ultra_short_live",
            "weather_ultra_short_forecast",
            "weather_short_term_forecast",
            "living_population_grid",
            "cultural_event",
            "performance_event",
            "bike_station_master",
        }


class TestDag:
    def test_schedule(self):
        assert isinstance(dag.timetable, CronTriggerTimetable)
        assert dag.catchup is False
        assert dag.max_active_runs == 1

    def test_runs_after_the_daily_collectors(self):
        """일별 API 부하가 낮은 04:30에 D-6 확정 배치를 실행한다."""
        assert COMPACTION_CRON == "30 4 * * *"

    def test_replays_all_24_rental_hours(self):
        replay_ids = {
            f"replay_bike_rental_history_{hour:02d}h" for hour in range(24)
        }
        assert replay_ids <= set(dag.task_ids)

        for hour in range(24):
            task = dag.get_task(f"replay_bike_rental_history_{hour:02d}h")
            assert "--source bike_rental_history" in task.bash_command
            assert "--force" in task.bash_command
            assert "macros.timedelta(days=6)" in task.bash_command
            assert task.trigger_rule == TriggerRule.ALL_DONE

    def test_replay_tasks_are_sequential(self):
        for hour in range(23):
            current = dag.get_task(f"replay_bike_rental_history_{hour:02d}h")
            following = dag.get_task(
                f"replay_bike_rental_history_{hour + 1:02d}h"
            )
            assert following.task_id in current.downstream_task_ids

    def test_compaction_uses_recovery_sweep(self):
        for source in COMPACTION_SOURCES:
            task = dag.get_task(f"compact_{source}")
            assert "--date" not in task.bash_command
            assert "--from" not in task.bash_command

    def test_rental_compaction_waits_for_replay_attempts(self):
        task = dag.get_task("compact_bike_rental_history")

        assert task.upstream_task_ids == {"replay_bike_rental_history_23h"}
        assert task.trigger_rule == TriggerRule.ALL_DONE

    def test_other_compactions_are_independent_from_replay(self):
        for source in (
            "bike_station_realtime",
            "population_realtime",
            "weather_ultra_short_live",
        ):
            task = dag.get_task(f"compact_{source}")
            assert task.upstream_task_ids == set()
            assert task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_cold_bronze_runs_for_every_collector_source(self):
        for source in COLD_BRONZE_SOURCES:
            task = dag.get_task(f"cold_compact_{source}")
            assert "maintenance.py cold-compact" in task.bash_command
            assert "--delay-days 6" in task.bash_command
            expected_upstream = (
                {"replay_bike_rental_history_23h"}
                if source == "bike_rental_history"
                else set()
            )
            assert task.upstream_task_ids == expected_upstream

    def test_rental_cold_recovery_is_not_blocked_by_replay_failure(self):
        task = dag.get_task("cold_compact_bike_rental_history")

        assert task.trigger_rule == TriggerRule.ALL_DONE

    def test_non_authority_silver_gc_runs_after_thirty_day_retention(self):
        for source in COLD_BRONZE_SOURCES:
            task = dag.get_task(f"gc_silver_{source}")
            assert task.upstream_task_ids == {f"gc_hot_bronze_{source}"}
            assert "maintenance.py gc-silver" in task.bash_command
            assert "macros.timedelta(days=36)" in task.bash_command
            assert ("--require-archive" in task.bash_command) is (
                source in COMPACTION_SOURCES
            )

    def test_hot_bronze_gc_runs_after_cold_recovery(self):
        for source in COLD_BRONZE_SOURCES:
            task = dag.get_task(f"gc_hot_bronze_{source}")
            assert task.upstream_task_ids == {f"cold_compact_{source}"}
            assert "maintenance.py gc-hot-bronze" in task.bash_command
            assert "--retention-days 30" in task.bash_command

    def test_expected_task_count(self):
        assert len(dag.task_ids) == (
            24 + len(COMPACTION_SOURCES) + 3 * len(COLD_BRONZE_SOURCES)
        )
