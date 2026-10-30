"""maintenance.py 테스트: 하위 명령 파싱, 날짜 범위, JSON 출력과 종료 코드."""

from __future__ import annotations

import json
from datetime import date

import pytest

import maintenance
from compaction import RECOVERY_DAYS, DateResult

TODAY = date(2026, 8, 13)


class TestParseArgs:
    def test_subcommand_is_required(self):
        with pytest.raises(SystemExit):
            maintenance.parse_args([])

    def test_source_is_required(self):
        with pytest.raises(SystemExit):
            maintenance.parse_args(["compact"])

    def test_compact_defaults(self):
        args = maintenance.parse_args(["compact", "--source", "t_source"])

        assert args.date is None
        assert args.force is False
        assert args.run is maintenance._compact

    def test_gc_silver_requires_date(self):
        with pytest.raises(SystemExit):
            maintenance.parse_args(["gc-silver", "--source", "t_source"])

    def test_parses_dates(self):
        args = maintenance.parse_args(
            ["cold-compact", "--source", "t_source", "--today", "2026-08-13", "--delay-days", "6"]
        )

        assert args.today == TODAY
        assert args.delay_days == 6


class TestCompactCommand:
    @pytest.fixture
    def compacted_days(self, monkeypatch):
        days: list[date] = []
        statuses = iter(["compacted", "skipped", "failed"] + ["empty"] * RECOVERY_DAYS)

        def fake_compact(config, day, *, force):
            days.append(day)
            return DateResult(day=day, status=next(statuses))

        monkeypatch.setattr(maintenance.config_loader, "load", lambda source_id: None)
        monkeypatch.setattr(maintenance.compaction, "compact_date", fake_compact)
        return days

    def test_default_range_is_recovery_window(self, compacted_days, capsys):
        args = maintenance.parse_args(["compact", "--source", "t_source", "--today", "2026-08-13"])

        exit_code = args.run(args)

        assert len(compacted_days) == RECOVERY_DAYS
        assert compacted_days[-1] == TODAY
        assert exit_code == 1
        summary = json.loads(capsys.readouterr().out)
        assert summary["failed"] == 1
        assert summary["dates"] == RECOVERY_DAYS

    def test_single_date(self, compacted_days, capsys):
        args = maintenance.parse_args(["compact", "--source", "t_source", "--date", "2026-08-01"])

        exit_code = args.run(args)

        assert compacted_days == [date(2026, 8, 1)]
        assert exit_code == 0
        assert json.loads(capsys.readouterr().out)["compacted"] == 1
