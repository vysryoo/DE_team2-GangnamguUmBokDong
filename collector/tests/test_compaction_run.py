"""compaction 실행 경로를 moto S3로 검증한다 — 압축·건너뛰기·격리·manifest."""

from datetime import date, datetime

import pyarrow as pa
import pytest
from core import layout
from core.gold_publication.canonical import sha256_hex
from core.kst import KST
from core.s3 import delete_object, list_keys, put_object_bytes, read_parquet
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from core.testing import (
    PutSourceSnapshot,
    put_partial_source_snapshot,
    put_source_snapshot,
)

import storage
from compaction import RECOVERY_DAYS, compact_date, target_dates
from config.schema import (
    ColumnSpec,
    Policies,
    Quality,
    Schedule,
    SourceConfig,
)
from config.schema import Compaction as CompactionConfig
from config.schema import Storage as StorageConfig
from storage import read_archive_manifest

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


def _table(values: list[int]) -> pa.Table:
    """구분 가능한 cnt 값을 가진 테스트 Silver table을 만든다."""
    return pa.table(
        {
            "sta": [f"ST-{value}" for value in values],
            "cnt": values,
            "_row_status": ["ok"] * len(values),
        }
    )


def _window(minute: int, day: date = DAY) -> datetime:
    """테스트 날짜의 KST 09시 source logical window를 만든다."""
    return datetime(day.year, day.month, day.day, 9, minute, tzinfo=KST)


def _next_revision(logical: datetime) -> int:
    return len(list_keys(layout.source_snapshot_logical_prefix("t_source", logical)))


def _publish_table(logical: datetime, table: pa.Table, **kwargs) -> PutSourceSnapshot:
    """Immutable Silver와 다음 revision의 SUCCEEDED source snapshot을 게시한다."""
    return put_source_snapshot(
        "t_source", logical, table, revision_no=_next_revision(logical), **kwargs
    )


def _publish_succeeded(minute: int, values: list[int]) -> PutSourceSnapshot:
    return _publish_table(_window(minute), _table(values))


def _put_silver(minute: int, rows: int = 2, day: date = DAY) -> PutSourceSnapshot:
    """해당 날짜 09시 HH:MM 윈도우에 rows행 Silver를 게시한다."""
    return _publish_table(_window(minute, day), _table(list(range(rows))))


def _publish_empty(minute: int) -> None:
    """같은 logical window의 최신 correction을 confirmed EMPTY로 게시한다."""
    logical = _window(minute)
    revision = _next_revision(logical)
    manifest = build_source_snapshot_manifest(
        source_id="t_source",
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.EMPTY,
        config_version="v1",
        silver_uri=None,
        silver_byte_sha256=None,
        counts=SourceSnapshotCounts(0, 0, 0, 0, 0),
        planned_parts=("sentinel",),
        completed_parts=("sentinel",),
    )
    put_object_bytes(
        layout.source_snapshot_manifest_key("t_source", logical, revision),
        manifest.canonical_bytes,
    )


class TestCompactDate:
    def test_row_count_is_preserved(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver(minute, rows=2)

        result = compact_date(config, DAY)

        assert result.status == "compacted"
        assert result.rows == 6

    def test_archive_is_readable_with_declared_schema(self):
        config = _config()
        _put_silver(5)

        compact_date(config, DAY)

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
        _put_silver(5)

        compact_date(config, DAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"collector"}

    def test_window_start_distinguishes_source_files(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver(minute, rows=2)

        compact_date(config, DAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert sorted(set(table.column("_window_start").to_pylist())) == [
            "2026-08-12T09:05:00+09:00",
            "2026-08-12T09:10:00+09:00",
            "2026-08-12T09:15:00+09:00",
        ]

    def test_empty_day_writes_nothing(self):
        config = _config()

        result = compact_date(config, DAY)

        assert result.status == "empty"
        assert result.archive_key is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_dates_with_different_null_patterns_share_one_schema(self):
        """전량 결측 컬럼이 있는 날과 없는 날의 archive 스키마가 같아야 한다."""
        config = _config()
        _put_silver(5)
        _publish_table(
            _window(5, date(2026, 8, 11)),
            pa.table(
                {
                    "sta": ["ST-0"],
                    "cnt": pa.array([None], type=pa.null()),
                    "_row_status": ["ok"],
                }
            ),
        )

        compact_date(config, DAY)
        compact_date(config, date(2026, 8, 11))

        a = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        b = read_parquet("archive/t_source/dt=2026-08-11.parquet", as_pandas=False)
        assert a.schema == b.schema


class TestSourceSnapshotAuthority:
    """Immutable source snapshot의 최신 correction만 archive 입력을 열어야 한다."""

    def test_compacts_manifest_referenced_immutable_silver(self) -> None:
        """SUCCEEDED manifest가 가리킨 immutable Silver만 압축한다."""
        config = _config()
        _publish_succeeded(5, [7, 8])

        result = compact_date(config, DAY)

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

    def test_latest_succeeded_correction_replaces_older_immutable_object(self) -> None:
        """여러 immutable revision 중 latest SUCCEEDED만 선택한다."""
        config = _config()
        _publish_succeeded(5, [1])
        _publish_succeeded(5, [20, 30])

        result = compact_date(config, DAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.rows == 2
        assert archive.column("cnt").to_pylist() == [20, 30]

    def test_unpublished_partial_attempt_keeps_last_published_success(self) -> None:
        """새 PARTIAL object는 직전 published SUCCEEDED authority를 대체하지 않는다."""
        config = _config()
        _publish_succeeded(5, [1])
        compact_date(config, DAY)
        put_partial_source_snapshot("t_source", _window(5), _table([999]))

        result = compact_date(config, DAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert result.status == "skipped"
        assert archive.column("cnt").to_pylist() == [1]

    def test_initial_empty_manifest_is_counted_without_silver(self) -> None:
        """Silver가 한 번도 없는 최초 EMPTY도 completed window로 압축 기록한다."""
        config = _config()
        _publish_empty(5)

        result = compact_date(config, DAY)

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
        first = compact_date(config, DAY)
        _publish_empty(5)

        corrected = compact_date(config, DAY)

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
        first = compact_date(config, DAY)
        previous_manifest = read_archive_manifest("t_source", DAY)
        monkeypatch.setattr(storage, "list_source_snapshot_windows", lambda *_: [])

        regressed = compact_date(config, DAY)

        archive = read_parquet(
            "archive/t_source/dt=2026-08-12.parquet", as_pandas=False
        )
        assert first.rows == 2
        assert regressed.status == "skipped"
        assert archive.column("cnt").to_pylist() == [1, 2]
        assert read_archive_manifest("t_source", DAY) == previous_manifest

    def test_unpublished_immutable_object_is_not_compacted(self) -> None:
        """Manifest-last 중간 상태의 Silver는 archive 입력을 열지 않는다."""
        config = _config()
        body = b"unpublished parquet"
        put_object_bytes(
            layout.silver_content_key("t_source", _window(5), sha256_hex(body)), body
        )

        result = compact_date(config, DAY)

        assert result.status == "empty"
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_legacy_mutable_silver_key_fails_closed(self) -> None:
        """Source snapshot 이전 mutable key가 섞인 날짜는 archive를 덮어쓰지 않는다."""
        config = _config()
        _publish_succeeded(5, [1])
        put_object_bytes("silver/t_source/dt=2026-08-12/hh=09/0910.parquet", b"legacy")

        result = compact_date(config, DAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None

    def test_manifest_row_count_mismatch_fails_closed(self) -> None:
        """Manifest count와 parquet row가 다르면 archive를 기록하지 않는다."""
        config = _config()
        _publish_table(_window(5), _table([1]), manifest_rows=2)

        result = compact_date(config, DAY)

        assert result.status == "failed"
        assert "counts.kept" in result.error
        assert read_archive_manifest("t_source", DAY) is None

    def test_manifest_referenced_missing_silver_fails_closed(self) -> None:
        """Manifest만 있고 exact SUCCEEDED Silver가 없으면 날짜를 실패시킨다."""
        config = _config()
        published = _publish_succeeded(5, [1])
        delete_object(published.silver_key)

        result = compact_date(config, DAY)

        assert result.status == "failed"
        assert "Silver object가 목록에 없다" in result.error
        assert read_archive_manifest("t_source", DAY) is None

    def test_manifest_referenced_checksum_mismatch_fails_closed(self) -> None:
        """Content-addressed key의 bytes가 변조되면 archive를 기록하지 않는다."""
        config = _config()
        published = _publish_succeeded(5, [1])
        put_object_bytes(published.silver_key, b"corrupted parquet bytes")

        result = compact_date(config, DAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )


class TestChangeDetection:
    def test_second_run_skips_when_nothing_changed(self):
        config = _config()
        _put_silver(5)
        compact_date(config, DAY)

        result = compact_date(config, DAY)

        assert result.status == "skipped"

    def test_new_silver_file_triggers_recompaction(self):
        config = _config()
        _put_silver(5)
        compact_date(config, DAY)

        _put_silver(10)
        result = compact_date(config, DAY)

        assert result.status == "compacted"
        assert result.rows == 4

    def test_correction_triggers_recompaction(self):
        config = _config()
        _put_silver(5, rows=2)
        compact_date(config, DAY)

        _put_silver(5, rows=5)
        result = compact_date(config, DAY)

        assert result.status == "compacted"
        assert result.rows == 5

    def test_force_recompacts_unchanged_date(self):
        config = _config()
        _put_silver(5)
        compact_date(config, DAY)

        result = compact_date(config, DAY, force=True)

        assert result.status == "compacted"


class TestArchiveManifest:
    def test_records_completeness_against_expected_windows(self):
        config = _config()
        for minute in (5, 10):
            _put_silver(minute)

        compact_date(config, DAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["expected_windows"] == 288
        assert manifest["found_windows"] == 2
        assert manifest["completeness"] == pytest.approx(2 / 288)

    def test_records_rows_and_archive_key(self):
        config = _config()
        _put_silver(5, rows=3)

        compact_date(config, DAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["rows"] == 3
        assert manifest["archive_key"] == "archive/t_source/dt=2026-08-12.parquet"


class TestFailureIsolation:
    def test_cast_failure_does_not_write_archive_or_manifest(self):
        """부분 결과를 남기지 않아야 다음 실행이 자동 재시도한다."""
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        _publish_table(
            _window(5), pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]})
        )

        result = compact_date(config, DAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None
        assert (
            read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
            is None
        )

    def test_one_bad_date_does_not_block_others(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        _publish_table(
            _window(5), pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]})
        )
        _publish_table(
            _window(5, date(2026, 8, 11)),
            pa.table({"cnt": [1], "_row_status": ["ok"]}),
        )

        results = [compact_date(config, day) for day in (date(2026, 8, 11), DAY)]

        by_day = {r.day: r.status for r in results}
        assert by_day[date(2026, 8, 11)] == "compacted"
        assert by_day[DAY] == "failed"

    def test_failure_records_the_reason(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        _publish_table(
            _window(5), pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]})
        )

        result = compact_date(config, DAY)

        assert "cnt" in result.error


class TestTargetDates:
    def test_spans_recovery_window_ending_today(self):
        days = target_dates(TODAY)

        assert len(days) == RECOVERY_DAYS
        assert days[-1] == TODAY

    def test_dates_are_ascending_and_contiguous(self):
        days = target_dates(TODAY)

        assert days == sorted(days)
        assert (days[-1] - days[0]).days == len(days) - 1


class TestDedup:
    """`bike_rental_history`는 path_suffix가 날짜 단위인데 5분마다 돌아, 윈도우마다
    같은 날 데이터를 통째로 다시 받는다. 윈도우 중복은 완전히 동일한 행을 만들고,
    원본 자체의 중복은 값이 다르다(같은 대여인데 이용시간·이용거리가 미세하게 다른
    사례가 실측으로 확인됨). 이 차이를 이용해 무손실로 제거한다.
    """

    def _put(self, minute, rows):
        _publish_table(
            _window(minute),
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

        result = compact_date(config, DAY)

        assert result.rows == 2

    def test_collapses_rows_repeated_across_windows(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])
        self._put(10, [("ST-1", 3)])

        result = compact_date(config, DAY)

        assert result.rows == 1

    def test_keeps_earliest_window_start(self):
        """그 기록이 처음 보인 시점이 의미 있는 값이다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(10, [("ST-1", 3)])
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.column("_window_start").to_pylist() == [
            "2026-08-12T09:05:00+09:00"
        ]

    def test_preserves_rows_differing_in_any_data_column(self):
        """원본의 진짜 중복(값이 다름)은 compaction이 판단해 지울 것이 아니다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 31), ("ST-1", 32)])
        self._put(10, [("ST-1", 31), ("ST-1", 32)])

        result = compact_date(config, DAY)

        assert result.rows == 2

    def test_preserves_declared_schema(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY)

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

        result = compact_date(config, DAY)

        assert result.rows == 3
