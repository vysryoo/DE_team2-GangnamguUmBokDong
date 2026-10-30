"""compaction 순수 로직 검증 — 경로 파싱·스키마 정합·변경 감지·검사 범위.

S3를 타는 통합 경로는 test_compaction_run.py에서 따로 본다. 여기서는 네트워크 없이
계산되는 부분만 다룬다.
"""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from compaction import (
    RECOVERY_DAYS,
    archive_schema,
    conform,
    lookback_days,
    silver_signature,
    window_start_from_key,
)
from config import loader as config_loader
from config.schema import (
    Backfill,
    ColumnSpec,
    Policies,
    Quality,
    Schedule,
    SourceConfig,
)
from config.schema import Storage as StorageConfig
from core.s3 import S3Object
from core.kst import KST

SOURCES_DIR = Path(__file__).resolve().parents[1] / "sources"


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
        "columns": {},
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


def _obj(key, size=100, minute=0):
    return S3Object(
        key=key, size=size, last_modified=datetime(2026, 8, 12, 4, minute, tzinfo=KST)
    )


class TestWindowStartFromKey:
    """silver 키가 유일한 시각 정보인 소스가 있으므로(bike_station_realtime) 정확해야 한다."""

    def test_parses_date_and_time_into_kst_iso8601(self):
        key = "silver/test_source/dt=2026-08-12/hh=14/1410.parquet"

        assert window_start_from_key(key) == "2026-08-12T14:10:00+09:00"

    def test_midnight(self):
        key = "silver/test_source/dt=2026-08-12/hh=00/0000.parquet"

        assert window_start_from_key(key) == "2026-08-12T00:00:00+09:00"

    def test_parses_content_addressed_immutable_key(self) -> None:
        """Content-addressed key에서도 legacy와 같은 minute를 복원한다."""
        checksum = "a" * 64
        key = f"silver/test_source/dt=2026-08-12/hh=14/1410/sha256={checksum}.parquet"

        assert window_start_from_key(key) == "2026-08-12T14:10:00+09:00"

    def test_uses_filename_not_hh_partition(self):
        """hh=는 시 단위라 분을 잃는다. 파일명 HHMM이 진짜 시각이다."""
        key = "silver/test_source/dt=2026-08-12/hh=23/2355.parquet"

        assert window_start_from_key(key) == "2026-08-12T23:55:00+09:00"

    def test_rejects_mismatched_hour_partition(self) -> None:
        """Partition hour와 파일 minute의 hour가 다르면 거부한다."""
        key = "silver/test_source/dt=2026-08-12/hh=13/1410.parquet"

        with pytest.raises(ValueError, match="partition"):
            window_start_from_key(key)

    def test_rejects_noncanonical_immutable_checksum(self) -> None:
        """Lowercase 64자 SHA-256이 아닌 immutable key를 거부한다."""
        key = "silver/test_source/dt=2026-08-12/hh=14/1410/sha256=ABC.parquet"

        with pytest.raises(ValueError):
            window_start_from_key(key)

    def test_rejects_unparseable_key(self):
        with pytest.raises(ValueError):
            window_start_from_key(
                "silver/test_source/dt=2026-08-12/hh=14/nowcast.parquet"
            )


class TestArchiveSchema:
    """yaml의 types[0]이 실효 타입이다 — _try_cast가 선언 순서대로 첫 성공을 채택하므로."""

    def test_single_declared_type_maps_directly(self):
        config = _config(
            columns={
                "a": ColumnSpec(types=("float",)),
                "b": ColumnSpec(types=("int",)),
            }
        )

        schema = archive_schema(config)

        assert schema.field("a").type == pa.float64()
        assert schema.field("b").type == pa.int64()

    def test_multi_type_declaration_resolves_to_first(self):
        """[str, int]는 str()이 거의 실패하지 않아 항상 str이 된다."""
        config = _config(columns={"USE_MIN": ColumnSpec(types=("str", "int"))})

        assert archive_schema(config).field("USE_MIN").type == pa.string()

    def test_meta_columns_appended(self):
        config = _config(columns={"a": ColumnSpec(types=("str",))})

        schema = archive_schema(config)

        assert schema.names == ["a", "_row_status", "_window_start", "_source_kind"]
        assert schema.field("_window_start").type == pa.string()

    def test_source_kind_is_part_of_the_schema(self):
        """archive에 두 출처가 섞이므로 구분 컬럼이 스키마에 있어야 한다."""
        config = _config(columns={"a": ColumnSpec(types=("str",))})

        schema = archive_schema(config)

        assert schema.names == ["a", "_row_status", "_window_start", "_source_kind"]
        assert schema.field("_source_kind").type == pa.string()

    def test_column_order_follows_yaml(self):
        config = _config(
            columns={
                "z": ColumnSpec(types=("str",)),
                "a": ColumnSpec(types=("str",)),
            }
        )

        assert archive_schema(config).names[:2] == ["z", "a"]


class TestConform:
    def test_casts_all_null_column_to_declared_type(self):
        """전량 결측 윈도우는 pyarrow가 null 타입으로 추론한다 — 이게 날짜 간 스키마 drift의 원인."""
        schema = pa.schema([("rn1", pa.float64())])
        table = pa.table({"rn1": pa.array([None, None], type=pa.null())})

        assert conform(table, schema).schema.field("rn1").type == pa.float64()

    def test_fills_missing_column_with_nulls(self):
        schema = pa.schema([("a", pa.string()), ("b", pa.int64())])
        table = pa.table({"a": ["x", "y"]})

        result = conform(table, schema)

        assert result.column("b").to_pylist() == [None, None]

    def test_reorders_columns_to_schema(self):
        schema = pa.schema([("a", pa.string()), ("b", pa.string())])
        table = pa.table({"b": ["2"], "a": ["1"]})

        assert conform(table, schema).schema.names == ["a", "b"]

    def test_drops_columns_not_in_schema(self):
        schema = pa.schema([("a", pa.string())])
        table = pa.table({"a": ["1"], "extra": ["drop me"]})

        assert conform(table, schema).schema.names == ["a"]

    def test_raises_when_value_cannot_cast(self):
        """yaml 선언과 실제가 어긋났다는 뜻이므로 조용히 넘기지 않는다."""
        schema = pa.schema([("n", pa.int64())])
        table = pa.table({"n": ["not a number"]})

        with pytest.raises(pa.ArrowInvalid):
            conform(table, schema)

    def test_conformed_tables_are_concatenable(self):
        schema = pa.schema([("a", pa.float64())])
        typed = pa.table({"a": pa.array([1.5], type=pa.float64())})
        nulls = pa.table({"a": pa.array([None], type=pa.null())})

        merged = pa.concat_tables([conform(typed, schema), conform(nulls, schema)])

        assert merged.column("a").to_pylist() == [1.5, None]


class TestSilverSignature:
    def test_same_objects_give_same_signature(self):
        objs = [_obj("a.parquet"), _obj("b.parquet")]

        assert silver_signature(objs) == silver_signature(list(objs))

    def test_order_does_not_matter(self):
        a, b = _obj("a.parquet"), _obj("b.parquet")

        assert silver_signature([a, b]) == silver_signature([b, a])

    def test_added_file_changes_signature(self):
        before = [_obj("a.parquet")]
        after = [_obj("a.parquet"), _obj("b.parquet")]

        assert silver_signature(before) != silver_signature(after)

    def test_same_key_with_new_size_changes_signature(self):
        """백필은 같은 키를 덮어쓴다 — 키 목록만 보면 못 잡는다."""
        before = [_obj("a.parquet", size=100)]
        after = [_obj("a.parquet", size=250)]

        assert silver_signature(before) != silver_signature(after)

    def test_same_key_with_new_mtime_changes_signature(self):
        before = [_obj("a.parquet", minute=0)]
        after = [_obj("a.parquet", minute=30)]

        assert silver_signature(before) != silver_signature(after)

    def test_empty_list_is_stable(self):
        assert silver_signature([]) == silver_signature([])


class TestLookbackDays:
    """검사 범위는 (가) 백필 창과 (나) 배치 복구 하한 중 큰 쪽이다."""

    def test_backfill_window_wins_when_longer(self):
        config = _config(backfill=Backfill(enabled=True, max_age="7d"))

        assert lookback_days(config) == 8

    def test_recovery_floor_wins_when_backfill_is_short(self):
        config = _config(backfill=Backfill(enabled=True, max_age="6h"))

        assert lookback_days(config) == RECOVERY_DAYS

    def test_recovery_floor_applies_without_backfill(self):
        """백필이 없다고 배치 복구력까지 잃으면 안 된다 — CATCHUP=False라 놓친 날은 안 돌아온다."""
        config = _config(backfill=None)

        assert lookback_days(config) == RECOVERY_DAYS

    def test_recovery_floor_keeps_d6_correction_for_next_day_retry(self):
        """D-6 correction 당일 압축이 실패해도 다음 날 D-7에서 다시 검사한다."""

        assert RECOVERY_DAYS == 8

    def test_rental_production_config_keeps_d7_in_recovery_sweep(self):
        """운영 대여이력에서 지연 backfill을 꺼도 Archive 복구 범위는 줄지 않는다."""
        config = config_loader.load("bike_rental_history", base_dir=SOURCES_DIR)

        assert config.backfill is None
        assert lookback_days(config) == 8

    def test_sub_day_max_age_rounds_up_and_adds_boundary_day(self):
        config = _config(backfill=Backfill(enabled=True, max_age="30h"))

        assert lookback_days(config) == max(3, RECOVERY_DAYS)

    def test_disabled_backfill_is_treated_as_absent(self):
        config = _config(backfill=Backfill(enabled=False))

        assert lookback_days(config) == RECOVERY_DAYS
