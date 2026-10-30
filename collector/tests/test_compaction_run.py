"""compaction 실행 경로를 moto S3로 검증한다 — 압축·건너뛰기·격리·manifest."""

from datetime import date, datetime, timedelta

import pyarrow as pa
import pytest
from core.s3 import put_object_bytes, read_parquet
from core.source_snapshot import SourceSnapshotStatus

import manifest as manifest_module
import storage
from compaction import RECOVERY_DAYS, compact_date, compact_range, target_dates
from config.schema import (
    Backfill,
    ColumnSpec,
    Policies,
    Quality,
    Schedule,
    SourceConfig,
)
from config.schema import Compaction as CompactionConfig
from config.schema import Storage as StorageConfig
from manifest import Artifacts, Counts, Manifest, RunStatus, Stage
from storage import read_archive_manifest, write_silver
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

DAY = date(2026, 8, 12)
TODAY = date(2026, 8, 13)


def _config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_adapter",
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(
            bronze_format="json", silver_format="parquet", partition=("dt", "hh")
        ),
        "quality": Quality(
            max_drop_ratio=0.5, max_missing_ratio=0.0, allow_empty=False
        ),
        "policies": Policies(
            required_missing="drop_row",
            required_outlier="drop_row",
            optional_missing="keep_null",
            optional_outlier="set_null",
        ),
        "columns": {
            "sta": ColumnSpec(types=("str",)),
            "cnt": ColumnSpec(types=("int",)),
        },
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


def _put_silver(source_id, minute, rows=2, day=DAY):
    """해당 날짜 hh=09의 HH:MM 윈도우에 silver 하나를 쓴다."""
    table = pa.table(
        {
            "sta": [f"ST-{i}" for i in range(rows)],
            "cnt": list(range(rows)),
            "_row_status": ["ok"] * rows,
        }
    )
    write_silver(
        source_id, datetime(day.year, day.month, day.day, 9, minute, tzinfo=KST), table
    )


def _table(values: list[int]) -> pa.Table:
    """구분 가능한 cnt 값을 가진 테스트 Silver table을 만든다."""
    return pa.table(
        {
            "sta": [f"ST-{value}" for value in values],
            "cnt": values,
            "_row_status": ["ok"] * len(values),
        }
    )


def _window(minute: int) -> datetime:
    """테스트 날짜의 KST 09시 source logical window를 만든다."""
    return datetime(DAY.year, DAY.month, DAY.day, 9, minute, tzinfo=KST)


def _publish_succeeded(
    minute: int, values: list[int]
) -> storage.ImmutableSilverArtifact:
    """Immutable Silver와 그 SUCCEEDED source snapshot manifest를 게시한다."""
    logical = _window(minute)
    artifact = storage.write_immutable_silver("t_source", logical, _table(values))
    row_count = len(values)
    manifest_module.publish_source_snapshot(
        source_id="t_source",
        logical_dttm=logical,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="v1",
        silver=artifact,
        counts=Counts(
            expected=row_count,
            fetched=row_count,
            kept=row_count,
            repaired=0,
            dropped=0,
        ),
        planned_parts=("part",),
        completed_parts=("part",),
    )
    return artifact


def _publish_empty(minute: int) -> None:
    """같은 logical window의 최신 correction을 confirmed EMPTY로 게시한다."""
    manifest_module.publish_source_snapshot(
        source_id="t_source",
        logical_dttm=_window(minute),
        status=SourceSnapshotStatus.EMPTY,
        config_version="v1",
        silver=None,
        counts=Counts(expected=0, fetched=0, kept=0, repaired=0, dropped=0),
        planned_parts=("sentinel",),
        completed_parts=("sentinel",),
    )


def _save_mutable_diagnostic(minute: int, status: RunStatus, silver_key: str) -> None:
    """Silver key 옆에 mutable 진단 manifest를 기록한다."""
    logical = _window(minute)
    manifest_module.save(
        Manifest(
            source_id="t_source",
            window_start=logical,
            window_end=logical + timedelta(minutes=5),
            status=status,
            stage=Stage.COMPLETED,
            started_at=logical,
            ended_at=logical + timedelta(seconds=1),
            artifacts=Artifacts(silver=silver_key),
            counts=Counts(expected=1, fetched=1, kept=1),
            config_version="v1",
        )
    )


class TestCompactDate:
    def test_row_count_is_preserved(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver("t_source", minute, rows=2)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 6

    def test_archive_is_readable_with_declared_schema(self):
        config = _config()
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.schema.names == [
            "sta",
            "cnt",
            "_row_status",
            "_window_start",
            "_source_kind",
        ]
        assert table.schema.field("cnt").type == pa.int64()

    def test_marks_rows_as_collector_sourced(self):
        config = _config()
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"collector"}

    def test_window_start_distinguishes_source_files(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver("t_source", minute, rows=2)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert sorted(set(table.column("_window_start").to_pylist())) == [
            "2026-08-12T09:05:00+09:00",
            "2026-08-12T09:10:00+09:00",
            "2026-08-12T09:15:00+09:00",
        ]

    def test_empty_day_writes_nothing(self):
        config = _config()

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "empty"
        assert result.archive_key is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_dates_with_different_null_patterns_share_one_schema(self):
        """전량 결측 컬럼이 있는 날과 없는 날의 archive 스키마가 같아야 한다."""
        config = _config()
        _put_silver("t_source", 5)
        write_silver(
            "t_source",
            datetime(2026, 8, 11, 9, 5, tzinfo=KST),
            pa.table(
                {
                    "sta": ["ST-0"],
                    "cnt": pa.array([None], type=pa.null()),
                    "_row_status": ["ok"],
                }
            ),
        )

        compact_date(config, DAY, today=TODAY)
        compact_date(config, date(2026, 8, 11), today=TODAY)

        a = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        b = read_parquet("archive/t_source/dt=2026-08-11.parquet", as_pandas=False)
        assert a.schema == b.schema


class TestSourceSnapshotAuthority:
    """Immutable source snapshot의 최신 correction만 archive 입력을 열어야 한다."""

    def test_compacts_manifest_referenced_immutable_silver(self) -> None:
        """SUCCEEDED manifest가 가리킨 immutable Silver만 압축한다."""
        config = _config()
        _publish_succeeded(5, [7, 8])

        result = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.status == "compacted"
        assert result.rows == 2
        assert archive.column("cnt").to_pylist() == [7, 8]
        assert archive.column("_window_start").to_pylist() == [
            "2026-08-12T09:05:00+09:00",
            "2026-08-12T09:05:00+09:00",
        ]

    def test_same_window_legacy_is_not_merged_with_immutable_authority(self) -> None:
        """같은 window의 legacy와 immutable을 동시에 합치지 않는다."""
        config = _config()
        write_silver("t_source", _window(5), _table([999]))
        _publish_succeeded(5, [1, 2])

        result = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.rows == 2
        assert archive.column("cnt").to_pylist() == [1, 2]

    def test_latest_succeeded_correction_replaces_older_immutable_object(self) -> None:
        """여러 immutable revision 중 latest SUCCEEDED만 선택한다."""
        config = _config()
        _publish_succeeded(5, [1])
        _publish_succeeded(5, [20, 30])

        result = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.rows == 2
        assert archive.column("cnt").to_pylist() == [20, 30]

    def test_unpublished_partial_attempt_keeps_last_published_success(self) -> None:
        """새 PARTIAL object는 직전 published SUCCEEDED authority를 대체하지 않는다."""
        config = _config()
        _publish_succeeded(5, [1])
        compact_date(config, DAY, today=TODAY)
        partial = storage.write_immutable_silver("t_source", _window(5), _table([999]))
        _save_mutable_diagnostic(5, RunStatus.PARTIAL, partial.key)

        result = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.status == "skipped"
        assert archive.column("cnt").to_pylist() == [1]

    def test_initial_empty_manifest_is_counted_without_silver(self) -> None:
        """Silver가 한 번도 없는 최초 EMPTY도 completed window로 압축 기록한다."""
        config = _config()
        _publish_empty(5)

        result = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        archive_manifest = read_archive_manifest("t_source", DAY)
        assert result.status == "compacted"
        assert result.rows == 0
        assert archive.num_rows == 0
        assert archive_manifest["found_windows"] == 1
        assert archive_manifest["completeness"] == pytest.approx(1 / 288)

    def test_empty_correction_clears_previously_compacted_rows(self) -> None:
        """Latest EMPTY correction은 과거 archive row를 빈 table로 교정한다."""
        config = _config()
        _publish_succeeded(5, [1])
        first = compact_date(config, DAY, today=TODAY)
        _publish_empty(5)

        corrected = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        archive_manifest = read_archive_manifest("t_source", DAY)
        assert first.rows == 1
        assert corrected.status == "compacted"
        assert corrected.rows == 0
        assert archive.num_rows == 0
        assert archive_manifest["found_windows"] == 1

    def test_zero_completed_authority_regression_preserves_previous_archive(
        self, monkeypatch
    ) -> None:
        """authority discovery가 0 completed로 퇴행하면 기존 archive를 보존한다."""
        config = _config()
        _publish_succeeded(5, [1, 2])
        first = compact_date(config, DAY, today=TODAY)
        previous_manifest = read_archive_manifest("t_source", DAY)
        monkeypatch.setattr(storage, "list_source_snapshot_windows", lambda *_: [])

        regressed = compact_date(config, DAY, today=TODAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert first.rows == 2
        assert regressed.status == "skipped"
        assert archive.column("cnt").to_pylist() == [1, 2]
        assert read_archive_manifest("t_source", DAY) == previous_manifest

    @pytest.mark.parametrize(
        "status", [RunStatus.PARTIAL, RunStatus.FAILED, RunStatus.EMPTY]
    )
    def test_non_succeeded_mutable_legacy_manifest_does_not_open_authority(
        self, status: RunStatus
    ) -> None:
        """Mutable legacy의 비성공 최종 상태는 archive 입력을 열지 않는다."""
        config = _config()
        legacy_key = write_silver("t_source", _window(5), _table([1]))
        _save_mutable_diagnostic(5, status, legacy_key)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "empty"
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_succeeded_historical_legacy_manifest_remains_compatible(self) -> None:
        """전환 전 SUCCEEDED legacy window는 계속 압축할 수 있다."""
        config = _config()
        legacy_key = write_silver("t_source", _window(5), _table([4]))
        _save_mutable_diagnostic(5, RunStatus.SUCCEEDED, legacy_key)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 1

    def test_unpublished_immutable_object_does_not_fall_back_to_legacy(self) -> None:
        """Manifest-last 중간 상태에서는 같은 window legacy도 열지 않는다."""
        config = _config()
        write_silver("t_source", _window(5), _table([999]))
        storage.write_immutable_silver("t_source", _window(5), _table([1]))

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "empty"
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_manifest_row_count_mismatch_fails_closed(self) -> None:
        """Manifest count와 parquet row가 다르면 archive를 기록하지 않는다."""
        config = _config()
        logical = _window(5)
        artifact = storage.write_immutable_silver("t_source", logical, _table([1]))
        manifest_module.publish_source_snapshot(
            source_id="t_source",
            logical_dttm=logical,
            status=SourceSnapshotStatus.SUCCEEDED,
            config_version="v1",
            silver=artifact,
            counts=Counts(expected=2, fetched=2, kept=2),
            planned_parts=("part",),
            completed_parts=("part",),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "failed"
        assert "counts.kept" in result.error
        assert read_archive_manifest("t_source", DAY) is None

    def test_manifest_referenced_missing_silver_fails_closed(self) -> None:
        """Manifest만 있고 exact SUCCEEDED Silver가 없으면 날짜를 실패시킨다."""
        config = _config()
        logical = _window(5)
        checksum = "a" * 64
        missing_key = (
            f"silver/t_source/dt=2026-08-12/hh=09/0905/sha256={checksum}.parquet"
        )
        missing = storage.ImmutableSilverArtifact(
            key=missing_key,
            uri=storage.object_uri(missing_key),
            byte_sha256=checksum,
            row_count=1,
        )
        manifest_module.publish_source_snapshot(
            source_id="t_source",
            logical_dttm=logical,
            status=SourceSnapshotStatus.SUCCEEDED,
            config_version="v1",
            silver=missing,
            counts=Counts(expected=1, fetched=1, kept=1),
            planned_parts=("part",),
            completed_parts=("part",),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "failed"
        assert "exact Silver object" in result.error
        assert read_archive_manifest("t_source", DAY) is None

    def test_manifest_referenced_checksum_mismatch_fails_closed(self) -> None:
        """Content-addressed key의 bytes가 변조되면 archive를 기록하지 않는다."""
        config = _config()
        artifact = _publish_succeeded(5, [1])
        put_object_bytes(artifact.key, b"corrupted parquet bytes")

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )


class TestChangeDetection:
    def test_second_run_skips_when_nothing_changed(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "skipped"

    def test_new_silver_file_triggers_recompaction(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        _put_silver("t_source", 10)
        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 4

    def test_overwritten_silver_file_triggers_recompaction(self):
        """백필은 같은 키를 다시 쓴다 — 키 목록만 보면 못 잡는다."""
        config = _config()
        _put_silver("t_source", 5, rows=2)
        compact_date(config, DAY, today=TODAY)

        _put_silver("t_source", 5, rows=5)
        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 5

    def test_force_recompacts_unchanged_date(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        result = compact_date(config, DAY, today=TODAY, force=True)

        assert result.status == "compacted"


class TestArchiveManifest:
    def test_records_completeness_against_expected_windows(self):
        config = _config()
        for minute in (5, 10):
            _put_silver("t_source", minute)

        compact_date(config, DAY, today=TODAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["expected_windows"] == 288
        assert manifest["found_windows"] == 2
        assert manifest["completeness"] == pytest.approx(2 / 288)

    def test_records_rows_and_archive_key(self):
        config = _config()
        _put_silver("t_source", 5, rows=3)

        compact_date(config, DAY, today=TODAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["rows"] == 3
        assert manifest["archive_key"] == "archive/t_source/dt=2026-08-12.parquet"

    def test_backfill_window_open_inside_max_age(self):
        config = _config(backfill=Backfill(enabled=True, max_age="7d"))
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is False

    def test_backfill_window_closed_beyond_max_age(self):
        config = _config(backfill=Backfill(enabled=True, max_age="6h"))
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=date(2026, 8, 20))

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is True

    def test_backfill_window_always_closed_without_backfill(self):
        config = _config(backfill=None)
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is True


class TestFailureIsolation:
    def test_cast_failure_does_not_write_archive_or_manifest(self):
        """부분 결과를 남기지 않아야 다음 실행이 자동 재시도한다."""
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_one_bad_date_does_not_block_others(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )
        write_silver(
            "t_source",
            datetime(2026, 8, 11, 9, 5, tzinfo=KST),
            pa.table({"cnt": [1], "_row_status": ["ok"]}),
        )

        results = compact_range(config, [date(2026, 8, 11), DAY], today=TODAY)

        by_day = {r.day: r.status for r in results}
        assert by_day[date(2026, 8, 11)] == "compacted"
        assert by_day[DAY] == "failed"

    def test_failure_records_the_reason(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert "cnt" in result.error


class TestTargetDates:
    def test_spans_lookback_window_ending_today(self):
        config = _config(backfill=None)

        days = target_dates(config, TODAY)

        assert len(days) == RECOVERY_DAYS
        assert days[-1] == TODAY

    def test_longer_backfill_window_widens_the_range(self):
        config = _config(backfill=Backfill(enabled=True, max_age="7d"))

        assert len(target_dates(config, TODAY)) == 8

    def test_dates_are_ascending_and_contiguous(self):
        config = _config(backfill=None)

        days = target_dates(config, TODAY)

        assert days == sorted(days)
        assert (days[-1] - days[0]).days == len(days) - 1


class TestDedup:
    """`bike_rental_history`는 path_suffix가 날짜 단위인데 5분마다 돌아, 윈도우마다
    같은 날 데이터를 통째로 다시 받는다. 윈도우 중복은 완전히 동일한 행을 만들고,
    원본 자체의 중복은 값이 다르다(같은 대여인데 이용시간·이용거리가 미세하게 다른
    사례가 실측으로 확인됨). 이 차이를 이용해 무손실로 제거한다.
    """

    def _put(self, minute, rows):
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, minute, tzinfo=KST),
            pa.table(
                {
                    "sta": [r[0] for r in rows],
                    "cnt": [r[1] for r in rows],
                    "_row_status": ["ok"] * len(rows),
                }
            ),
        )

    def test_off_by_default_keeps_window_duplicates(self):
        """스냅샷 소스는 연속 윈도우가 같은 값을 내는 게 정상이라 지우면 안 된다."""
        config = _config()
        self._put(5, [("ST-1", 3)])
        self._put(10, [("ST-1", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 2

    def test_collapses_rows_repeated_across_windows(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])
        self._put(10, [("ST-1", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 1

    def test_keeps_earliest_window_start(self):
        """그 기록이 처음 보인 시점이 의미 있는 값이다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(10, [("ST-1", 3)])
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.column("_window_start").to_pylist() == [
            "2026-08-12T09:05:00+09:00"
        ]

    def test_preserves_rows_differing_in_any_data_column(self):
        """원본의 진짜 중복(값이 다름)은 compaction이 판단해 지울 것이 아니다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 31), ("ST-1", 32)])
        self._put(10, [("ST-1", 31), ("ST-1", 32)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 2

    def test_preserves_declared_schema(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.schema.names == [
            "sta",
            "cnt",
            "_row_status",
            "_window_start",
            "_source_kind",
        ]
        assert table.schema.field("cnt").type == pa.int64()

    def test_distinct_rows_are_untouched(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 1), ("ST-2", 2)])
        self._put(10, [("ST-3", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 3
