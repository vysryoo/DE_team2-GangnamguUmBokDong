"""데이터 레이크 키 규칙이 기존 S3 객체 경로와 바이트 단위로 같은지 고정한다."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from core import layout
from core.kst import KST

WINDOW = datetime(2026, 8, 12, 9, 5, tzinfo=KST)
SHA = "a" * 64


def test_silver_keys_use_kst_partition() -> None:
    assert layout.silver_prefix("bike_station_realtime") == "silver/bike_station_realtime/"
    assert layout.silver_day_prefix("bike_station_realtime", date(2026, 8, 12)) == (
        "silver/bike_station_realtime/dt=2026-08-12/"
    )
    assert layout.silver_window_key("station_master_enriched", WINDOW) == (
        "silver/station_master_enriched/dt=2026-08-12/hh=09/0905.parquet"
    )
    assert layout.silver_content_key("bike_station_realtime", WINDOW, SHA) == (
        f"silver/bike_station_realtime/dt=2026-08-12/hh=09/0905/sha256={SHA}.parquet"
    )


def test_partition_converts_other_offsets_to_kst() -> None:
    same_instant = WINDOW.astimezone(UTC)

    assert layout.silver_window_key("s", same_instant) == layout.silver_window_key(
        "s", WINDOW
    )
    assert layout.window_manifest_key(
        "s", WINDOW.astimezone(timezone(timedelta(hours=-3)))
    ) == layout.window_manifest_key("s", WINDOW)


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        layout.silver_window_key("s", datetime(2026, 8, 12, 9, 5))
    with pytest.raises(ValueError, match="timezone-aware"):
        layout.source_snapshot_logical_prefix("s", datetime(2026, 8, 12, 9, 5))


def test_window_manifest_keys() -> None:
    assert layout.window_manifest_key("population_realtime", WINDOW) == (
        "_manifest/population_realtime/dt=2026-08-12/hh=09/0905.json"
    )
    assert layout.window_manifest_prefix("population_realtime", date(2026, 8, 12)) == (
        "_manifest/population_realtime/dt=2026-08-12/"
    )
    assert layout.window_manifest_prefix(
        "population_realtime", date(2026, 8, 12), 9
    ) == ("_manifest/population_realtime/dt=2026-08-12/hh=09/")


def test_nowcast_key() -> None:
    assert layout.nowcast_key(date(2026, 8, 12)) == (
        "silver/living_population_grid/dt=2026-08-12/hh=00/nowcast.parquet"
    )


def test_source_snapshot_manifest_keys_use_utc_partition() -> None:
    logical = datetime(2026, 8, 12, 8, 5, tzinfo=KST)

    assert layout.source_snapshot_day_prefix("poi_master", date(2026, 8, 11)) == (
        "source_snapshot_manifest/poi_master/dt=2026-08-11/"
    )
    assert layout.source_snapshot_hour_prefix("poi_master", logical) == (
        "source_snapshot_manifest/poi_master/dt=2026-08-11/hh=23/"
    )
    assert layout.source_snapshot_logical_prefix("poi_master", logical) == (
        "source_snapshot_manifest/poi_master/dt=2026-08-11/hh=23/"
        "logical=20260811T230500000000Z/"
    )
    assert layout.source_snapshot_manifest_key("poi_master", logical, 3) == (
        "source_snapshot_manifest/poi_master/dt=2026-08-11/hh=23/"
        "logical=20260811T230500000000Z/revision=0000000003.json"
    )


def test_source_snapshot_manifest_key_rejects_negative_revision() -> None:
    with pytest.raises(ValueError, match="revision_no"):
        layout.source_snapshot_manifest_key("s", WINDOW, -1)


def test_parse_source_snapshot_manifest_key_round_trips() -> None:
    logical = datetime(2026, 8, 12, 8, 5, 1, 250, tzinfo=KST)
    key = layout.source_snapshot_manifest_key("bike_station_realtime", logical, 2)

    parsed = layout.parse_source_snapshot_manifest_key(key)

    assert parsed == ("bike_station_realtime", logical.astimezone(UTC), 2)


@pytest.mark.parametrize(
    "key",
    [
        "source_snapshot_manifest/s/dt=2026-08-11/hh=23/logical=20260811T230500000000Z/revision=1.json",
        "source_snapshot_manifest/s/dt=2026-08-12/hh=23/logical=20260811T230500000000Z/revision=0000000000.json",
        "source_snapshot_manifest/s/dt=2026-08-11/hh=22/logical=20260811T230500000000Z/revision=0000000000.json",
        "source_snapshot_manifest/s/dt=2026-08-11/hh=23/logical=20261399T230500000000Z/revision=0000000000.json",
    ],
)
def test_parse_source_snapshot_manifest_key_rejects_non_canonical(key: str) -> None:
    with pytest.raises(ValueError):
        layout.parse_source_snapshot_manifest_key(key)


def test_archive_keys() -> None:
    assert layout.archive_key("bike_rental_history", date(2026, 8, 12)) == (
        "archive/bike_rental_history/dt=2026-08-12.parquet"
    )
    assert layout.archive_prefix("bike_rental_history") == "archive/bike_rental_history/"
