"""storage.py의 S3 I/O를 moto로 검증한다."""

import io
from datetime import date, datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage

from core.kst import KST
from core.testing import (
    TEST_BUCKET,
    put_partial_source_snapshot,
    put_source_snapshot,
)


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


class TestReadNowcastGrid:
    """baseline은 nowcaster 추정치다 — 실측 원본은 관측일이 4~5일 늦어 쓸 수 없다."""

    def test_reads_nowcast_parquet_for_the_date(self):
        nowcast = pa.table({"CELL_ID": ["가가00000000"], "TT": ["14"], "SPOP": [10.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-12/hh=00/nowcast.parquet", nowcast
        )

        result = storage.read_nowcast_grid(date(2026, 8, 12))

        assert result.to_pylist() == nowcast.to_pylist()

    def test_reads_future_dates_too(self):
        """nowcaster가 D+3까지 만들므로 12시간 앞(자정 넘김)도 읽을 수 있어야 한다."""
        nowcast = pa.table({"CELL_ID": ["가가00000000"], "TT": ["06"], "SPOP": [7.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-13/hh=00/nowcast.parquet", nowcast
        )

        assert (
            storage.read_nowcast_grid(date(2026, 8, 13)).to_pylist()
            == nowcast.to_pylist()
        )

    def test_ignores_measured_parquet_in_the_same_prefix(self):
        measured = pa.table({"CELL_ID": ["가가99999999"], "SPOP": [999.0]})
        nowcast = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-12/hh=14/1400.parquet", measured
        )
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-12/hh=00/nowcast.parquet", nowcast
        )

        assert (
            storage.read_nowcast_grid(date(2026, 8, 12)).to_pylist()
            == nowcast.to_pylist()
        )

    def test_raises_when_nowcast_missing_even_if_measured_exists(self):
        measured = pa.table({"CELL_ID": ["가가99999999"], "SPOP": [999.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-12/hh=14/1400.parquet", measured
        )

        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_nowcast_grid(date(2026, 8, 12))

    def test_raises_when_nothing_written(self):
        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_nowcast_grid(date(2026, 8, 12))

    def test_latest_nowcast_for_station_geometry_uses_prior_success_and_ignores_future(
        self,
    ):
        prior = pa.table({"CELL_ID": ["가가00000001"], "TT": ["00"], "SPOP": [1.0]})
        future = pa.table({"CELL_ID": ["가가00000002"], "TT": ["00"], "SPOP": [2.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-11/hh=00/nowcast.parquet", prior
        )
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-13/hh=00/nowcast.parquet", future
        )

        snapshot_date, result = storage.read_latest_nowcast_grid(date(2026, 8, 12))

        assert snapshot_date == date(2026, 8, 11)
        assert result.column("CELL_ID").to_pylist() == ["가가00000001"]


class TestReadRealtimeSilver:
    def test_reads_exact_window_key(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        table = pa.table({"AREA_CD": ["POI001"]})
        put_source_snapshot("population_realtime", window_start, table)

        result = storage.read_realtime_snapshot(window_start).table

        assert result.column("AREA_CD").to_pylist() == ["POI001"]

    def test_reads_completed_partial_window_from_diagnostic_manifest(self):
        """운영 정책상 허용된 POI 일부 누락은 authority 없이도 명시적으로 읽는다."""
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        table = pa.table({"AREA_CD": ["POI001"]})
        put_partial_source_snapshot("population_realtime", window_start, table)

        result = storage.read_realtime_snapshot(window_start).table

        assert result.column("AREA_CD").to_pylist() == ["POI001"]

    def test_raises_when_window_file_missing(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_realtime_snapshot(window_start)

    def test_uses_recent_complete_after_current_complete_and_partial_are_missing(self):
        """현재 입력이 없으면 보정 전에 freshness 안의 과거 성공을 선택한다."""
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        prior = window_start - timedelta(minutes=5)
        put_source_snapshot(
            "population_realtime", prior, pa.table({"AREA_CD": ["POI001"]})
        )

        result = storage.read_realtime_snapshot(window_start)

        assert result.status.value == "success"
        assert result.freshness.value == "stale"
        assert result.table.column("AREA_CD").to_pylist() == ["POI001"]


class TestWriteNormalizedSilverAndManifest:
    def test_write_normalized_silver_key_and_roundtrip(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42]})

        key = storage.write_normalized_silver(window_start, table)

        assert (
            key
            == "silver/living_population_normalized/dt=2026-08-12/hh=14/1405.parquet"
        )
        stored = pq.read_table(
            io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        )
        assert stored.column("SPOP").to_pylist() == [42]

    def test_older_generation_cannot_overwrite_newer_target(self):
        """늦은 backfill은 같은 target에 이미 저장된 최신 예보를 되돌리지 않는다."""
        target = datetime(2026, 8, 12, 18, 0, tzinfo=KST)
        newer = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        older = datetime(2026, 8, 12, 13, 5, tzinfo=KST)

        key = storage.write_normalized_silver(
            target,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42]}),
            source_window_start=newer,
        )
        skipped = storage.write_normalized_silver(
            target,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [1]}),
            source_window_start=older,
        )

        assert key is not None
        assert skipped is None
        stored = pq.read_table(
            io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        )
        assert stored.column("SPOP").to_pylist() == [42]

    def test_newer_generation_replaces_older_forecast(self):
        """target 시각의 실제 관측은 이전 window가 쓴 예보를 정상적으로 갱신한다."""
        target = datetime(2026, 8, 12, 18, 0, tzinfo=KST)
        older = datetime(2026, 8, 12, 14, 5, tzinfo=KST)

        storage.write_normalized_silver(
            target,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [1]}),
            source_window_start=older,
        )
        key = storage.write_normalized_silver(
            target,
            pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42]}),
            source_window_start=target,
        )

        assert key is not None
        stored = pq.read_table(
            io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        )
        assert stored.column("SPOP").to_pylist() == [42]

    def test_write_manifest_key_and_content(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)

        key = storage.write_manifest(
            window_start,
            {"baseline_date": "2026-08-12", "baseline_date_mode": "strict"},
        )

        assert (
            key
            == "_manifest/living_population_normalized/dt=2026-08-12/hh=14/1405.json"
        )
        import json

        body = json.loads(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        assert body == {"baseline_date": "2026-08-12", "baseline_date_mode": "strict"}
