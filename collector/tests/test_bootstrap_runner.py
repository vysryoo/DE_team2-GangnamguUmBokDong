"""bootstrap runner 테스트: 그룹핑·검증 재사용·재개·silver 겹침·스키마 일치."""

from datetime import date, datetime

import pyarrow as pa
import pytest

from bootstrap.config import BootstrapConfig
from bootstrap.runner import group_by_window, load_date
from compaction import archive_schema
from config.schema import ColumnSpec, Policies, Quality, Schedule, SourceConfig
from config.schema import Storage as StorageConfig
from core.s3 import read_parquet
from storage import read_archive_manifest
from core.kst import KST
from core.testing import put_source_snapshot

pytestmark = pytest.mark.usefixtures("s3_bucket")

DAY = date(2026, 6, 1)


def _source_config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_adapter",
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(bronze_format="json", silver_format="parquet", partition=("dt", "hh")),
        "quality": Quality(max_drop_ratio=0.9, max_missing_ratio=0.0, allow_empty=False),
        "policies": Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
        ),
        "columns": {
            "BIKE_ID": ColumnSpec(types=("str",), required=True),
            "RENT_DT": ColumnSpec(types=("str",), required=True),
        },
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


def _bootstrap_config(**overrides):
    fields = {
        "kind": "csv",
        "column_map": {"자전거번호": "BIKE_ID", "대여일시": "RENT_DT"},
        "window": {"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
    }
    fields.update(overrides)
    return BootstrapConfig.model_validate(fields)


def _rows(*times):
    return [{"BIKE_ID": f"SPB-{i}", "RENT_DT": t} for i, t in enumerate(times)]


class TestGroupByWindow:
    def test_groups_by_hour(self):
        rows = _rows("2026-06-01 09:05:00", "2026-06-01 09:55:00", "2026-06-01 10:01:00")

        groups = group_by_window(rows, _bootstrap_config())

        assert sorted(groups) == ["2026-06-01T09:00:00+09:00", "2026-06-01T10:00:00+09:00"]
        assert len(groups["2026-06-01T09:00:00+09:00"]) == 2

    def test_window_is_kst_iso8601(self):
        groups = group_by_window(_rows("2026-06-01 00:00:00"), _bootstrap_config())

        assert list(groups) == ["2026-06-01T00:00:00+09:00"]


class TestLoadDate:
    def test_writes_archive_with_bootstrap_source_kind(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.status == "loaded"
        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"bootstrap"}

    def test_window_start_comes_from_the_record_hour(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.column("_window_start").to_pylist() == ["2026-06-01T09:00:00+09:00"]

    def test_schema_matches_what_compaction_produces(self):
        """같은 소스의 archive는 출처가 달라도 스키마가 같아야 한다."""
        scfg = _source_config()
        load_date(scfg, _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.schema == archive_schema(scfg)

    def test_required_missing_row_is_dropped(self):
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"},
                {"BIKE_ID": "SPB-1", "RENT_DT": "2026-06-01 09:05:00"}]

        result = load_date(_source_config(), _bootstrap_config(), DAY, rows)

        assert result.rows == 1
        assert result.dropped == 1

    def test_manifest_records_counts_and_issues(self):
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"},
                {"BIKE_ID": "SPB-1", "RENT_DT": "2026-06-01 09:05:00"}]

        load_date(_source_config(), _bootstrap_config(), DAY, rows)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["source_kind"] == "bootstrap"
        assert manifest["rows"] == 1
        assert manifest["dropped"] == 1
        assert manifest["column_issues"]["BIKE_ID"]["missing"] == 1
        assert "silver_signature" not in manifest

    def test_manifest_records_station_map_snapshot(self):
        """rackTotCnt·shared가 "실행한 날의 값"이라 출처를 되짚을 수 있어야 한다."""
        stats = {"built_at": "2026-08-19T18:40:00+09:00", "api_stations": 2737,
                 "history_stations": 2831}

        load_date(_source_config(), _bootstrap_config(), DAY,
                  _rows("2026-06-01 09:05:00"), station_map_stats=stats)

        assert read_archive_manifest("t_source", DAY)["station_map"] == stats

    def test_manifest_omits_station_map_when_not_joined(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert "station_map" not in read_archive_manifest("t_source", DAY)

    def test_skips_when_archive_already_exists(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 10:05:00"))

        assert result.status == "skipped"

    def test_force_overwrites_existing_archive(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        result = load_date(
            _source_config(), _bootstrap_config(), DAY,
            _rows("2026-06-01 10:05:00", "2026-06-01 11:05:00"), force=True,
        )

        assert result.status == "loaded"
        assert result.rows == 2

    def test_flags_silver_overlap_but_still_writes(self):
        """compaction 구역을 침범해도 막지는 않는다 — 결과 요약에 남긴다."""
        put_source_snapshot("t_source", datetime(2026, 6, 1, 9, 5, tzinfo=KST),
                            pa.table({"BIKE_ID": ["SPB-9"], "RENT_DT": ["2026-06-01 09:05:00"]}))

        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.status == "loaded"
        assert result.silver_present is True

    def test_no_silver_means_no_flag(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.silver_present is False

    def test_empty_rows_writes_nothing(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, [])

        assert result.status == "empty"
        assert read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False) is None

    def test_materialized_empty_has_schema_and_manifest(self):
        """알려진 원천 공백은 누락과 구분되는 0행 Archive로 보존한다."""
        scfg = _source_config()

        result = load_date(
            scfg,
            _bootstrap_config(),
            DAY,
            [],
            materialize_empty=True,
        )

        table = read_parquet(
            "archive/t_source/dt=2026-06-01.parquet", as_pandas=False
        )
        manifest = read_archive_manifest("t_source", DAY)
        assert result.status == "loaded"
        assert result.rows == 0
        assert table.num_rows == 0
        assert table.schema == archive_schema(scfg)
        assert manifest["materialized_empty"] is True
        assert manifest["rows"] == 0

    def test_materializes_all_rows_dropped_when_requested(self):
        """검증으로 전량 폐기된 날짜도 dropped 근거와 빈 스키마를 함께 남긴다."""
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"}]

        result = load_date(
            _source_config(),
            _bootstrap_config(),
            DAY,
            rows,
            materialize_empty=True,
        )

        manifest = read_archive_manifest("t_source", DAY)
        assert result.status == "loaded"
        assert result.dropped == 1
        assert manifest["dropped"] == 1
        assert manifest["column_issues"]["BIKE_ID"]["missing"] == 1

    def test_out_of_range_window_is_dropped_and_counted(self):
        """API가 경계 시각에 다른 날짜의 관측을 섞어 줄 수 있다 — target day가 아닌
        시간대 그룹은 archive에 넣지 않고 out_of_range로 집계한다."""
        rows = _rows("2026-06-01 09:05:00") + [
            {"BIKE_ID": "SPB-OOR", "RENT_DT": "2026-05-31 23:30:00"},
        ]

        result = load_date(_source_config(), _bootstrap_config(), DAY, rows)

        assert result.status == "loaded"
        assert result.rows == 1
        assert result.out_of_range == 1
        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.column("BIKE_ID").to_pylist() == ["SPB-0"]
        assert read_archive_manifest("t_source", DAY)["out_of_range"] == 1

    def test_all_rows_out_of_range_is_empty(self):
        rows = [{"BIKE_ID": "SPB-OOR", "RENT_DT": "2026-05-31 23:30:00"}]

        result = load_date(_source_config(), _bootstrap_config(), DAY, rows)

        assert result.status == "empty"
        assert result.out_of_range == 1

    def test_no_out_of_range_rows_records_zero(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.out_of_range == 0

    def test_all_rows_dropped_by_validation_is_empty_not_skipped(self):
        """행은 있었으나 검증에서 전량 폐기된 경우도 '처리할 게 없었다'는 empty다.

        이미 archive가 있어 건너뛴 진짜 skip과는 다른 상황이라 상태를 나눈다.
        """
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"}]

        result = load_date(_source_config(), _bootstrap_config(), DAY, rows)

        assert result.status == "empty"
        assert result.dropped == 1


class TestDedup:
    """대여소 재고는 시각당 스테이션마다 완전 동일한 행이 2개씩 온다(실측).

    `_window_start`를 포함한 전체 컬럼으로 묶어야 한다 — 뺀 채로 묶으면(`compaction.dedup`)
    서로 다른 시각의 값이 우연히 같을 때 시계열이 파괴된다.
    """

    def _rows_with_dt(self, *entries):
        """entries: (BIKE_ID, RENT_DT) 튜플들."""
        return [{"BIKE_ID": bike_id, "RENT_DT": dt} for bike_id, dt in entries]

    def test_identical_rows_in_the_same_window_are_merged(self):
        rows = self._rows_with_dt(
            ("SPB-1", "2026-06-01 09:05:00"),
            ("SPB-1", "2026-06-01 09:05:00"),  # 완전히 동일한 행 (실측: 시각당 스테이션마다 2행)
        )

        result = load_date(
            _source_config(), _bootstrap_config(dedup=True), DAY, rows,
        )

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert result.rows == 1
        assert table.num_rows == 1

    def test_same_window_different_values_are_both_kept(self):
        rows = self._rows_with_dt(
            ("SPB-1", "2026-06-01 09:05:00"),
            ("SPB-2", "2026-06-01 09:06:00"),  # 같은 창이지만 BIKE_ID가 달라 서로 다른 행
        )

        result = load_date(
            _source_config(), _bootstrap_config(dedup=True), DAY, rows,
        )

        assert result.rows == 2

    def test_different_windows_with_identical_values_are_both_kept(self):
        """compaction.dedup()을 그대로 쓰면 깨지는 케이스: 시각이 다르면 값이 같아도 남아야 한다."""
        rows = self._rows_with_dt(
            ("SPB-1", "2026-06-01 09:05:00"),
            ("SPB-1", "2026-06-01 10:05:00"),  # 09시·10시 값이 우연히 같음
        )

        result = load_date(
            _source_config(), _bootstrap_config(dedup=True), DAY, rows,
        )

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert result.rows == 2
        assert sorted(table.column("_window_start").to_pylist()) == [
            "2026-06-01T09:00:00+09:00", "2026-06-01T10:00:00+09:00",
        ]

    def test_dedup_false_keeps_everything(self):
        rows = self._rows_with_dt(
            ("SPB-1", "2026-06-01 09:05:00"),
            ("SPB-1", "2026-06-01 09:05:00"),
        )

        result = load_date(
            _source_config(), _bootstrap_config(dedup=False), DAY, rows,
        )

        assert result.rows == 2

    def test_schema_matches_archive_schema_after_dedup(self):
        scfg = _source_config()
        rows = self._rows_with_dt(
            ("SPB-1", "2026-06-01 09:05:00"),
            ("SPB-1", "2026-06-01 09:05:00"),
        )

        load_date(scfg, _bootstrap_config(dedup=True), DAY, rows)

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.schema == archive_schema(scfg)
