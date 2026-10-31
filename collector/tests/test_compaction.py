"""compaction 순수 로직 검증 — 경로 파싱·스키마 정합.

S3를 타는 통합 경로는 test_compaction_run.py에서 따로 본다. 여기서는 네트워크 없이
계산되는 부분만 다룬다.
"""

from datetime import datetime

import pyarrow as pa
import pytest
from compaction import _parse_silver_key, archive_schema, conform
from config.schema import (
    ColumnSpec,
    Policies,
    Quality,
    Schedule,
    SourceConfig,
)
from config.schema import Storage as StorageConfig
from core.kst import KST


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


class TestParseSilverKey:
    """silver 키가 유일한 시각 정보인 소스가 있으므로(bike_station_realtime) 정확해야 한다."""

    def test_parses_content_addressed_key_into_kst_window(self) -> None:
        checksum = "a" * 64
        key = f"silver/test_source/dt=2026-08-12/hh=23/2355/sha256={checksum}.parquet"

        assert _parse_silver_key(key) == (
            datetime(2026, 8, 12, 23, 55, tzinfo=KST),
            checksum,
        )

    @pytest.mark.parametrize(
        "key",
        [
            # Source snapshot authority 이전 mutable key
            "silver/test_source/dt=2026-08-12/hh=14/1410.parquet",
            "silver/test_source/dt=2026-08-12/hh=14/1410/sha256=ABC.parquet",
            "silver/test_source/dt=2026-08-12/hh=14/nowcast.parquet",
        ],
        ids=["legacy_mutable", "noncanonical_checksum", "unparseable"],
    )
    def test_rejects_non_content_addressed_key(self, key) -> None:
        with pytest.raises(ValueError):
            _parse_silver_key(key)


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
        """archive에 두 출처가 섞이므로 `_source_kind`도 메타 컬럼으로 붙는다."""
        config = _config(columns={"a": ColumnSpec(types=("str",))})

        schema = archive_schema(config)

        assert schema.names == ["a", "_row_status", "_window_start", "_source_kind"]
        assert schema.field("_window_start").type == pa.string()
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
