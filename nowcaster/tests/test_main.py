"""main.py: CLI 서브커맨드 및 estimate/backfill-archive 오케스트레이션."""

import io
from datetime import date, datetime, timedelta

import boto3
import main
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage
from botocore.exceptions import ClientError
from core.kst import KST
from core.layout import nowcast_key
from core.s3 import object_exists
from core.testing import TEST_BUCKET


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _key_exists(key: str) -> bool:
    try:
        _s3().head_object(Bucket=TEST_BUCKET, Key=key)
        return True
    except ClientError:
        return False


class TestRunEstimateArchivesD4Data:
    def test_archives_authoritative_data_using_actual_ymd(self, monkeypatch):
        today = date(2026, 8, 20)
        source_window_start = datetime(2026, 8, 20, 3, tzinfo=KST)
        # collector의 dt= 파티션은 "수집 실행일"이다. 실제 대상(biz) 날짜는
        # 그 안의 YMD 컬럼 값으로만 알 수 있다(예: 오늘 실행분의 YMD가 D-4).
        biz_date = today - timedelta(days=4)  # 2026-08-16
        source_table = pa.table(
            {
                "YMD": [f"{biz_date:%Y%m%d}"],
                "H_DNG_CD": ["H1"],
                "CELL_ID": ["C1"],
                "TT": ["10"],
                "SPOP": [123.0],
            }
        )
        observed = {}

        def read_real(logical):
            """요청한 exact logical window를 기록하고 authoritative fixture를 반환한다."""
            observed["logical"] = logical
            return source_table

        monkeypatch.setattr(storage, "read_real_grid_silver", read_real)
        # 예전에 이 날짜를 추정치로 채워뒀던 잔재
        storage.write_nowcast(biz_date, pa.table({"CELL_ID": ["C1"], "SPOP": [999.0]}))

        main.run_estimate(today, source_window_start=source_window_start)

        archived = storage.read_archive(biz_date)
        assert archived is not None
        assert archived.column("is_estimated").to_pylist() == [False]
        assert object_exists(nowcast_key(biz_date)) is False
        assert observed["logical"] == source_window_start

    def test_skips_actual_promotion_without_exact_window(self, monkeypatch):
        today = date(2026, 8, 20)

        def unexpected_read(_logical):
            """Exact window가 없을 때 Silver prefix scan을 테스트 실패로 바꾼다."""
            pytest.fail("exact source window 없이 실측 Silver를 읽었다")

        monkeypatch.setattr(storage, "read_real_grid_silver", unexpected_read)

        assert main.run_estimate(today) == 0

    def test_rejects_source_window_from_another_kst_date(self):
        with pytest.raises(ValueError, match="KST 날짜"):
            main.run_estimate(
                date(2026, 8, 20),
                source_window_start=datetime(2026, 8, 19, 3, tzinfo=KST),
            )


class TestRunEstimateWritesNowcast:
    def test_weighted_average_written_for_missing_date_with_full_history(self):
        today = date(2026, 8, 10)  # 월요일
        target = today + timedelta(days=3)  # 2026-08-13(목, 평일) - D+3

        for weeks_ago, value in [(1, 100.0), (2, 200.0), (3, 300.0), (4, 400.0)]:
            candidate = target - timedelta(weeks=weeks_ago)
            storage.write_archive(
                candidate,
                pa.table({"H_DNG_CD": ["H1"], "CELL_ID": ["C1"], "TT": ["10"], "SPOP": [value]}),
            )

        main.run_estimate(
            today,
            source_window_start=datetime(2026, 8, 10, 3, tzinfo=KST),
        )

        key = f"silver/living_population_grid/dt={target:%Y-%m-%d}/hh=00/nowcast.parquet"
        assert _key_exists(key)
        stored = pq.read_table(io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()))
        assert stored.column("SPOP").to_pylist() == pytest.approx(
            [100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1]
        )
        assert stored.column("estimation_method").to_pylist() == ["weighted_avg"]

    def test_skips_date_that_already_has_archived_actual(self):
        today = date(2026, 8, 12)
        target = today + timedelta(days=3)
        storage.write_archive(
            target, pa.table({"H_DNG_CD": ["H1"], "CELL_ID": ["C1"], "TT": ["10"], "SPOP": [500.0]})
        )

        main.run_estimate(today)

        key = f"silver/living_population_grid/dt={target:%Y-%m-%d}/hh=00/nowcast.parquet"
        assert _key_exists(key) is False


_PORTAL_HEADER = (
    '"일자","시간","행정동코드","250M격자","생활인구합계","남자 0~9세","남자 10~14세",'
    '"남자 15~19세","남자 20~24세","남자 25~29세","남자 30~34세","남자 35~39세",'
    '"남자 40~44세","남자 45~49세","남자 50~54세","남자 55~59세","남자 60~64세",'
    '"남자 65~69세","남자 70세 이상","여자 0~9세","여자 10~14세","여자 15~19세",'
    '"여자 20~24세","여자 25~29세","여자 30~34세","여자 35~39세","여자 40~44세",'
    '"여자 45~49세","여자 50~54세","여자 55~59세","여자 60~64세","여자 65~69세",'
    '"여자 70세 이상"'
)


def _portal_row(ymd: str, cell_id: str, spop: str) -> str:
    return f'"{ymd}","10","1100053","{cell_id}","{spop}"' + ',"*"' * 28


class TestRunBackfillArchive:
    def test_reads_real_portal_csv_and_writes_one_archive_file_per_date(self, tmp_path):
        csv_path = tmp_path / "250_LOCAL_RESD_sample.csv"
        content = "\n".join(
            [
                _PORTAL_HEADER,
                _portal_row("20260810", "C1", "111.0"),
                _portal_row("20260811", "C1", "222.0"),
            ]
        )
        csv_path.write_bytes((content + "\n").encode("euc-kr"))

        main.run_backfill_archive(str(tmp_path))

        first = storage.read_archive(date(2026, 8, 10))
        second = storage.read_archive(date(2026, 8, 11))
        assert first.column("SPOP").to_pylist() == [111.0]
        assert first.column("is_estimated").to_pylist() == [False]
        assert second.column("SPOP").to_pylist() == [222.0]


class TestBootstrapLookback:
    def test_exact_four_weeks_for_single_weekday_target(self):
        target = date(2026, 8, 20)  # 목요일

        assert main.required_lookback_dates(target, horizon_days=0) == [
            date(2026, 7, 23),
            date(2026, 7, 30),
            date(2026, 8, 6),
            date(2026, 8, 13),
        ]

    def test_writes_only_required_dates_and_marks_actual(self, tmp_path):
        target = date(2026, 8, 20)
        required = main.required_lookback_dates(target, horizon_days=0)
        unrelated = date(2026, 8, 12)
        csv_path = tmp_path / "250_LOCAL_RESD_lookback.csv"
        rows = [
            _portal_row(f"{day:%Y%m%d}", "C1", str(index * 100.0))
            for index, day in enumerate(required, start=1)
        ]
        rows.append(_portal_row(f"{unrelated:%Y%m%d}", "C1", "999.0"))
        csv_path.write_bytes(
            (_PORTAL_HEADER + "\n" + "\n".join(rows) + "\n").encode("euc-kr")
        )

        result = main.run_bootstrap_lookback(
            str(tmp_path), target, horizon_days=0
        )

        assert result == 0
        for day in required:
            archived = storage.read_archive(day)
            assert archived is not None
            assert archived.column("is_estimated").to_pylist() == [False]
        assert storage.read_archive(unrelated) is None

    def test_returns_failure_when_a_required_date_is_missing(self, tmp_path):
        target = date(2026, 8, 20)
        required = main.required_lookback_dates(target, horizon_days=0)
        csv_path = tmp_path / "250_LOCAL_RESD_incomplete.csv"
        csv_path.write_bytes(
            (
                _PORTAL_HEADER
                + "\n"
                + "\n".join(
                    _portal_row(f"{day:%Y%m%d}", "C1", "100.0")
                    for day in required[:-1]
                )
                + "\n"
            ).encode("euc-kr")
        )

        assert (
            main.run_bootstrap_lookback(str(tmp_path), target, horizon_days=0)
            == 1
        )


class TestCliDispatch:
    def test_estimate_command_calls_run_estimate_with_parsed_date(self, monkeypatch):
        captured = {}

        def fake_run(today, *, source_window_start):
            """Estimate CLI가 파싱한 날짜와 exact source window를 기록한다."""
            captured.update(today=today, source_window_start=source_window_start)
            return 0

        monkeypatch.setattr(main, "run_estimate", fake_run)

        main.main(
            [
                "estimate",
                "--target-date",
                "2026-08-12",
                "--source-window-start",
                "2026-08-12T03:00:00+09:00",
            ]
        )

        assert captured == {
            "today": date(2026, 8, 12),
            "source_window_start": datetime(2026, 8, 12, 3, tzinfo=KST),
        }

    def test_estimate_command_rejects_naive_source_window(self):
        with pytest.raises(ValueError, match="오프셋"):
            main.main(
                [
                    "estimate",
                    "--target-date",
                    "2026-08-12",
                    "--source-window-start",
                    "2026-08-12T03:00:00",
                ]
            )

    def test_estimate_without_source_window_warns_about_skipped_actual(
        self,
        monkeypatch,
        capsys,
    ):
        """하위호환 수동 실행이 actual 미승격을 조용히 숨기지 않는다."""
        observed = {}

        def fake_run(today, *, source_window_start):
            """Nowcast-only dispatch 인자를 기록한다."""
            observed.update(today=today, source_window_start=source_window_start)
            return 0

        monkeypatch.setattr(main, "run_estimate", fake_run)

        assert main.main(["estimate", "--target-date", "2026-08-12"]) == 0
        assert observed == {
            "today": date(2026, 8, 12),
            "source_window_start": None,
        }
        assert "actual Archive 승격을 생략" in capsys.readouterr().err

    def test_backfill_archive_command_calls_run_backfill_archive_with_csv_dir(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            main, "run_backfill_archive", lambda csv_dir: captured.setdefault("csv_dir", csv_dir)
        )

        main.main(["backfill-archive", "--csv-dir", "/tmp/some-dir"])

        assert captured["csv_dir"] == "/tmp/some-dir"

    def test_bootstrap_lookback_dispatches_target_and_options(self, monkeypatch):
        captured = {}

        def fake_run(csv_dir, today, *, horizon_days, force):
            captured.update(
                csv_dir=csv_dir,
                today=today,
                horizon_days=horizon_days,
                force=force,
            )
            return 0

        monkeypatch.setattr(main, "run_bootstrap_lookback", fake_run)

        assert main.main(
            [
                "bootstrap-lookback",
                "--csv-dir",
                "/data/population",
                "--target-date",
                "2026-08-20",
                "--horizon-days",
                "1",
                "--force",
            ]
        ) == 0
        assert captured == {
            "csv_dir": "/data/population",
            "today": date(2026, 8, 20),
            "horizon_days": 1,
            "force": True,
        }
