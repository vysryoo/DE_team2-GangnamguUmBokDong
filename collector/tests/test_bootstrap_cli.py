"""bootstrap CLI 테스트: 인자 파싱, 날짜 범위, 종료 코드, main() 전체 배선."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from bootstrap import __main__ as cli
from bootstrap.api_source import FetchFailed
from bootstrap.runner import DateResult


class TestParseArgs:
    def test_source_and_range_are_required(self):
        with pytest.raises(SystemExit):
            cli.parse_args(["--source", "bike_rental_history"])

    def test_parses_range_and_options(self):
        args = cli.parse_args([
            "--source", "bike_rental_history", "--from", "2026-06-01", "--to", "2026-06-03",
            "--csv-dir", "data", "--concurrency", "8", "--csv-batch-by-month", "--force",
            "--materialize-empty-archive",
        ])

        assert args.source == "bike_rental_history"
        assert getattr(args, "from") == "2026-06-01"
        assert args.csv_dir == "data"
        assert args.concurrency == 8
        assert args.csv_batch_by_month is True
        assert args.materialize_empty_archive is True
        assert args.force is True

    def test_default_concurrency_is_four(self):
        """공공 API라 기본을 보수적으로 둔다."""
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-01"])

        assert args.concurrency == 4


class TestResolveDates:
    def test_range_is_inclusive(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-03"])

        assert cli.resolve_dates(args) == [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]

    def test_single_day_range(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-01"])

        assert cli.resolve_dates(args) == [date(2026, 6, 1)]

    def test_reversed_range_exits(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-05", "--to", "2026-06-01"])

        with pytest.raises(SystemExit):
            cli.resolve_dates(args)


class TestExitCode:
    def test_zero_when_nothing_failed(self):
        results = [DateResult(day=date(2026, 6, 1), status="loaded", rows=3),
                   DateResult(day=date(2026, 6, 2), status="skipped")]

        assert cli.exit_code_for(results) == 0

    def test_nonzero_when_any_failed(self):
        results = [DateResult(day=date(2026, 6, 1), status="loaded", rows=3),
                   DateResult(day=date(2026, 6, 2), status="failed", error="boom")]

        assert cli.exit_code_for(results) != 0

    def test_zero_for_empty_results(self):
        assert cli.exit_code_for([]) == 0


class TestMain:
    """`main()` 자체를 e2e로 검증한다. S3·네트워크는 타지 않는다."""

    @pytest.fixture
    def stub_scfg(self):
        return SimpleNamespace(source_id="bike_rental_history")

    @pytest.fixture(autouse=True)
    def _stub_config_loader(self, monkeypatch, stub_scfg):
        monkeypatch.setattr(cli.config_loader, "load", lambda source_id: stub_scfg)

    def test_csv_kind_reads_file_once_and_loads_each_date(self, monkeypatch, stub_scfg, tmp_path):
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        (tmp_path / "data.csv").write_text("x")

        read_calls = []

        def fake_read_by_date(cfg, csv_dir, days, station_map=None):
            read_calls.append((cfg, csv_dir, frozenset(days)))
            return {
                date(2026, 6, 1): pa.Table.from_pylist([{"row": "d1"}]),
                date(2026, 6, 2): pa.Table.from_pylist([{"row": "d2"}]),
            }

        load_calls = []

        def fake_load_date(scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False):
            load_calls.append((scfg, bcfg_arg, day, rows, force))
            return DateResult(day=day, status="loaded", rows=len(rows))

        monkeypatch.setattr(cli.csv_source, "read_by_date", fake_read_by_date)
        monkeypatch.setattr(cli.runner, "load_date", fake_load_date)

        code = cli.main([
            "--source", "bike_rental_history",
            "--from", "2026-06-01", "--to", "2026-06-02",
            "--csv-dir", str(tmp_path),
        ])

        assert code == 0
        assert len(read_calls) == 1  # 날짜마다 파일을 다시 읽지 않는다
        assert len(load_calls) == 2  # 날짜 수만큼 적재 시도
        assert load_calls[0][2] == date(2026, 6, 1)
        assert load_calls[0][3] == [{"row": "d1"}]
        assert load_calls[1][2] == date(2026, 6, 2)
        assert load_calls[1][3] == [{"row": "d2"}]

    def test_csv_month_batches_bound_resident_tables(self, monkeypatch, tmp_path):
        """월별 모드는 날짜 범위를 달력 경계로 나눠 각각 한 번씩 읽는다."""
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        (tmp_path / "data.csv").write_text("x")
        read_calls = []

        def fake_read_by_date(cfg, csv_dir, days, station_map=None):
            read_calls.append(tuple(sorted(days)))
            return {}

        monkeypatch.setattr(cli.csv_source, "read_by_date", fake_read_by_date)
        monkeypatch.setattr(
            cli.runner,
            "load_date",
            lambda scfg, bcfg_arg, day, rows, **kwargs: DateResult(
                day=day, status="empty"
            ),
        )

        code = cli.main(
            [
                "--source",
                "bike_rental_history",
                "--from",
                "2025-01-31",
                "--to",
                "2025-03-01",
                "--csv-dir",
                str(tmp_path),
                "--csv-batch-by-month",
            ]
        )

        assert code == 0
        assert [batch[0].month for batch in read_calls] == [1, 2, 3]
        assert [len(batch) for batch in read_calls] == [1, 28, 1]

    def test_csv_empty_materialization_option_reaches_loader(
        self, monkeypatch, tmp_path
    ):
        """명시적 0행 보존 옵션만 runner의 materialize 경로를 활성화한다."""
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        (tmp_path / "data.csv").write_text("x")
        monkeypatch.setattr(cli.csv_source, "read_by_date", lambda *args, **kwargs: {})
        seen = {}

        def fake_load_date(scfg, bcfg_arg, day, rows, **kwargs):
            seen.update(kwargs)
            return DateResult(day=day, status="loaded", rows=0)

        monkeypatch.setattr(cli.runner, "load_date", fake_load_date)

        code = cli.main(
            [
                "--source",
                "bike_station_realtime",
                "--from",
                "2025-01-09",
                "--to",
                "2025-01-09",
                "--csv-dir",
                str(tmp_path),
                "--materialize-empty-archive",
            ]
        )

        assert code == 0
        assert seen["materialize_empty"] is True

    def test_csv_kind_without_csv_dir_exits(self, monkeypatch):
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        monkeypatch.setattr(cli.csv_source, "read_by_date", lambda *a, **k: {})
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: None)

        with pytest.raises(SystemExit):
            cli.main([
                "--source", "bike_rental_history",
                "--from", "2026-06-01", "--to", "2026-06-01",
            ])

    def test_csv_dir_nonexistent_path_exits(self, monkeypatch, tmp_path):
        """오타·잘못된 경로를 주면 glob이 조용히 빈 결과를 내고 전부 skipped가 되어
        종료 코드 0으로 끝난다 — 하나도 못 읽은 것과 정상 재개를 구별할 수 없다."""
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: None)

        missing = tmp_path / "does-not-exist"
        with pytest.raises(SystemExit):
            cli.main([
                "--source", "bike_rental_history",
                "--from", "2026-06-01", "--to", "2026-06-01",
                "--csv-dir", str(missing),
            ])

    def test_csv_dir_is_a_file_not_a_directory_exits(self, monkeypatch, tmp_path):
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: None)

        not_a_dir = tmp_path / "file.txt"
        not_a_dir.write_text("x")
        with pytest.raises(SystemExit):
            cli.main([
                "--source", "bike_rental_history",
                "--from", "2026-06-01", "--to", "2026-06-01",
                "--csv-dir", str(not_a_dir),
            ])

    def test_csv_dir_with_no_csv_files_warns(self, monkeypatch, tmp_path, capsys):
        """로그는 `configure_batch_logging`이 root 핸들러를 stdout으로 바꿔치므로
        caplog가 아니라 capsys로 확인한다."""
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        monkeypatch.setattr(cli.csv_source, "read_by_date", lambda cfg, csv_dir, days, station_map=None: {})
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: DateResult(day=date(2026, 6, 1), status="empty"))

        cli.main([
            "--source", "bike_rental_history",
            "--from", "2026-06-01", "--to", "2026-06-01",
            "--csv-dir", str(tmp_path),
        ])

        out = capsys.readouterr().out
        assert "WARNING" in out
        assert "csv" in out.lower()

    def test_history_api_kind_fetches_and_loads_each_date(self, monkeypatch):
        bcfg = SimpleNamespace(kind="history_api")
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)

        fetch_calls = []

        def fake_fetch_by_date(cfg, day, *, client, concurrency):
            fetch_calls.append((cfg, day, concurrency))
            return [{"row": str(day)}]

        load_calls = []

        def fake_load_date(scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False):
            load_calls.append((day, rows))
            return DateResult(day=day, status="loaded", rows=len(rows))

        monkeypatch.setattr(cli.api_source, "fetch_by_date", fake_fetch_by_date)
        monkeypatch.setattr(cli.runner, "load_date", fake_load_date)

        code = cli.main([
            "--source", "bike_station_realtime",
            "--from", "2026-06-01", "--to", "2026-06-02",
        ])

        assert code == 0
        assert [day for _, day, _ in fetch_calls] == [date(2026, 6, 1), date(2026, 6, 2)]
        assert load_calls == [
            (date(2026, 6, 1), [{"row": "2026-06-01"}]),
            (date(2026, 6, 2), [{"row": "2026-06-02"}]),
        ]

    def test_api_failure_is_isolated_to_that_date(self, monkeypatch):
        """한 날짜의 API 실패가 나머지 날짜 처리를 막으면 안 된다."""
        bcfg = SimpleNamespace(kind="history_api")
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)

        def fake_fetch_by_date(cfg, day, *, client, concurrency):
            if day == date(2026, 6, 2):
                raise FetchFailed("boom")
            return [{"row": str(day)}]

        load_calls = []

        def fake_load_date(scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False):
            load_calls.append(day)
            return DateResult(day=day, status="loaded", rows=len(rows))

        monkeypatch.setattr(cli.api_source, "fetch_by_date", fake_fetch_by_date)
        monkeypatch.setattr(cli.runner, "load_date", fake_load_date)

        code = cli.main([
            "--source", "bike_station_realtime",
            "--from", "2026-06-01", "--to", "2026-06-03",
        ])

        assert code != 0
        # 실패한 날짜는 load_date까지 가지 않고, 나머지 날짜는 정상 처리된다
        assert load_calls == [date(2026, 6, 1), date(2026, 6, 3)]

    def test_aborts_after_max_consecutive_failures(self, monkeypatch):
        """인증키 오류·쿼터 소진 상황을 시뮬레이션한다: 모든 날짜가 실패하면 남은
        날짜는 시도조차 하지 않고 중단해야 한다."""
        bcfg = SimpleNamespace(kind="history_api")
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)

        fetch_calls = []

        def fake_fetch_by_date(cfg, day, *, client, concurrency):
            fetch_calls.append(day)
            raise FetchFailed("boom")

        monkeypatch.setattr(cli.api_source, "fetch_by_date", fake_fetch_by_date)
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: (_ for _ in ()).throw(AssertionError("호출되면 안 됨")))

        code = cli.main([
            "--source", "bike_station_realtime",
            "--from", "2026-01-01", "--to", "2026-01-31",
        ])

        assert code != 0
        assert len(fetch_calls) == cli._MAX_CONSECUTIVE_FAILURES

    def test_counter_resets_on_success_and_keeps_going(self, monkeypatch):
        bcfg = SimpleNamespace(kind="history_api")
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)

        fetch_calls = []

        def fake_fetch_by_date(cfg, day, *, client, concurrency):
            fetch_calls.append(day)
            # 4번 실패 -> 성공(리셋) -> 다시 4번 실패, 총합은 상한(5)을 넘지만
            # 연속으로는 한 번도 상한에 닿지 않으므로 끝까지 진행돼야 한다.
            index = len(fetch_calls) - 1
            if index == 4:
                return [{"row": "ok"}]
            raise FetchFailed("boom")

        monkeypatch.setattr(cli.api_source, "fetch_by_date", fake_fetch_by_date)
        monkeypatch.setattr(
            cli.runner, "load_date",
            lambda scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False: DateResult(day=day, status="loaded", rows=len(rows)),
        )

        days = [date(2026, 1, 1) + __import__("datetime").timedelta(days=n) for n in range(9)]
        code = cli.main([
            "--source", "bike_station_realtime",
            "--from", str(days[0]), "--to", str(days[-1]),
        ])

        assert len(fetch_calls) == 9  # 중단되지 않고 전체 날짜를 시도했다
        assert code != 0  # 실패한 날짜는 여전히 있으므로 non-zero

    def test_prints_summary_with_source_status_and_overlap(self, monkeypatch, capsys, tmp_path):
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        (tmp_path / "data.csv").write_text("x")
        monkeypatch.setattr(
            cli.csv_source, "read_by_date",
            lambda cfg, csv_dir, days, station_map=None: {date(2026, 6, 1): pa.Table.from_pylist([{"row": "d1"}])},
        )
        monkeypatch.setattr(
            cli.runner, "load_date",
            lambda scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False: DateResult(
                day=day, status="loaded", rows=len(rows), silver_present=True,
            ),
        )

        cli.main([
            "--source", "bike_rental_history",
            "--from", "2026-06-01", "--to", "2026-06-01",
            "--csv-dir", str(tmp_path),
        ])

        out = capsys.readouterr().out
        assert "source=bike_rental_history" in out
        assert "loaded=1" in out
        assert "silver_overlap=1" in out

    def test_summary_reports_dropped_total(self, monkeypatch, capsys, tmp_path):
        bcfg = SimpleNamespace(kind="csv", join=None)
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        (tmp_path / "data.csv").write_text("x")
        monkeypatch.setattr(
            cli.csv_source, "read_by_date",
            lambda cfg, csv_dir, days, station_map=None: {date(2026, 6, 1): pa.Table.from_pylist([{"row": "d1"}])},
        )
        monkeypatch.setattr(
            cli.runner, "load_date",
            lambda scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False: DateResult(
                day=day, status="loaded", rows=len(rows), dropped=3,
            ),
        )

        cli.main([
            "--source", "bike_rental_history",
            "--from", "2026-06-01", "--to", "2026-06-01",
            "--csv-dir", str(tmp_path),
        ])

        out = capsys.readouterr().out
        assert "dropped=3" in out

    def test_summary_shows_aborted_flag(self, monkeypatch, capsys):
        bcfg = SimpleNamespace(kind="history_api")
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        monkeypatch.setattr(
            cli.api_source, "fetch_by_date",
            lambda cfg, day, *, client, concurrency: (_ for _ in ()).throw(FetchFailed("boom")),
        )
        monkeypatch.setattr(cli.runner, "load_date", lambda *a, **k: None)

        cli.main([
            "--source", "bike_station_realtime",
            "--from", "2026-01-01", "--to", "2026-01-31",
        ])

        out = capsys.readouterr().out
        assert "aborted=true" in out


class TestStationJoinWiring:
    """`join`이 선언된 CSV 소스는 매핑표를 만들어 read_by_date와 manifest에 넘긴다."""

    @pytest.fixture(autouse=True)
    def _stub_config_loader(self, monkeypatch):
        monkeypatch.setattr(cli.config_loader, "load",
                            lambda source_id: SimpleNamespace(source_id="bike_station_realtime"))

    @pytest.fixture
    def joined_bcfg(self, monkeypatch):
        bcfg = SimpleNamespace(kind="csv", join=SimpleNamespace(provider="bike_station"))
        monkeypatch.setattr(cli.bootstrap_config, "load", lambda source_id: bcfg)
        return bcfg

    def _stub_table(self, monkeypatch, stats=None):
        table = SimpleNamespace(stats=stats or {"built_at": "2026-08-19T18:40:00+09:00",
                                                "api_stations": 2737, "history_stations": 2831})
        monkeypatch.setattr(cli.station_join, "build", lambda csv_dir, **kwargs: table)
        return table

    def test_passes_station_map_to_reader(self, monkeypatch, joined_bcfg, tmp_path):
        (tmp_path / "stock.csv").write_text("x")
        table = self._stub_table(monkeypatch)
        seen = {}

        def fake_read_by_date(cfg, csv_dir, days, station_map=None):
            seen["station_map"] = station_map
            return {}

        monkeypatch.setattr(cli.csv_source, "read_by_date", fake_read_by_date)
        monkeypatch.setattr(cli.runner, "load_date",
                            lambda *a, **k: DateResult(day=date(2025, 12, 1), status="empty"))

        cli.main(["--source", "bike_station_realtime", "--from", "2025-12-01",
                  "--to", "2025-12-01", "--csv-dir", str(tmp_path)])

        assert seen["station_map"] is table

    def test_passes_stats_to_loader(self, monkeypatch, joined_bcfg, tmp_path):
        (tmp_path / "stock.csv").write_text("x")
        table = self._stub_table(monkeypatch)
        seen = {}

        monkeypatch.setattr(cli.csv_source, "read_by_date",
                            lambda cfg, csv_dir, days, station_map=None: {
                                date(2025, 12, 1): pa.Table.from_pylist([{"row": "d1"}])})

        def fake_load_date(scfg, bcfg_arg, day, rows, *, force, station_map_stats=None, materialize_empty=False):
            seen["stats"] = station_map_stats
            return DateResult(day=day, status="loaded", rows=len(rows))

        monkeypatch.setattr(cli.runner, "load_date", fake_load_date)

        cli.main(["--source", "bike_station_realtime", "--from", "2025-12-01",
                  "--to", "2025-12-01", "--csv-dir", str(tmp_path)])

        assert seen["stats"] == table.stats

    def test_builds_map_once_for_the_whole_range(self, monkeypatch, joined_bcfg, tmp_path):
        """매핑표는 API 4회 호출이라 날짜마다 다시 만들면 안 된다."""
        (tmp_path / "stock.csv").write_text("x")
        calls = []
        monkeypatch.setattr(cli.station_join, "build",
                            lambda csv_dir, **kwargs: calls.append(csv_dir) or SimpleNamespace(stats={}))
        monkeypatch.setattr(cli.csv_source, "read_by_date",
                            lambda cfg, csv_dir, days, station_map=None: {})
        monkeypatch.setattr(cli.runner, "load_date",
                            lambda *a, **k: DateResult(day=date(2025, 12, 1), status="empty"))

        cli.main(["--source", "bike_station_realtime", "--from", "2025-11-30",
                  "--to", "2025-12-31", "--csv-dir", str(tmp_path),
                  "--csv-batch-by-month"])

        assert len(calls) == 1

    def test_does_not_build_map_when_join_is_absent(self, monkeypatch, tmp_path):
        (tmp_path / "data.csv").write_text("x")
        monkeypatch.setattr(cli.bootstrap_config, "load",
                            lambda source_id: SimpleNamespace(kind="csv", join=None))
        calls = []
        monkeypatch.setattr(cli.station_join, "build", lambda csv_dir, **kwargs: calls.append(1))
        monkeypatch.setattr(cli.csv_source, "read_by_date",
                            lambda cfg, csv_dir, days, station_map=None: {})
        monkeypatch.setattr(cli.runner, "load_date",
                            lambda *a, **k: DateResult(day=date(2025, 12, 1), status="empty"))

        cli.main(["--source", "bike_rental_history", "--from", "2025-12-01",
                  "--to", "2025-12-01", "--csv-dir", str(tmp_path)])

        assert calls == []
