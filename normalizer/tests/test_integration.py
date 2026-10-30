"""Task 1~6을 실제 데이터(POI shapefile)와 moto S3로 엮은 통합/회귀 테스트.

과거 시도의 실제 파이프라인 출력 fixture는 이 리포지토리에 남아있지 않아
재사용할 수 없었다(이번 조사에서 git 이력·파일시스템 모두 확인). 대신 Task 1에서
커밋한 실제 POI shapefile(POI001="강남 MICE 관광특구")을 그대로 쓰고, 그 폴리곤과
실제로 겹치는/겹치지 않는 CELL_ID를 골라 회귀 시나리오로 고정한다.
"""

import io
import json
from datetime import UTC, datetime, timedelta

import boto3
import grid
import main
import merge
import poi
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage
from core.forecast import POPULATION_FORECAST_SLOT_COUNT

from core.kst import KST
from core.testing import (
    TEST_BUCKET,
    put_partial_source_snapshot,
    put_source_snapshot,
)

# POI001("강남 MICE 관광특구")와 실제로 크게 겹치는 격자(약 97.9% 겹침, 이번 조사에서 확인).
OVERLAPPING_CELL_ID = "다사61004575"
# POI001과 전혀 겹치지 않는(충분히 먼) 격자.
PASS_THROUGH_CELL_ID = "다바10759525"

WINDOW_START = datetime(2026, 8, 15, 14, 5, tzinfo=KST)
CURRENT_KEY = "silver/living_population_normalized/dt=2026-08-15/hh=14/1405.parquet"


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


def _grid_table(hours: list[str], spop: tuple[float, float]) -> pa.Table:
    """두 격자(겹침/비겹침)에 대해 지정한 TT들을 채운 baseline 테이블을 만든다."""
    cells = [OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID]
    rows = {"CELL_ID": [], "TT": [], "H_DNG_CD": [], "SPOP": []}
    for c in merge.AGE_COLUMNS:
        rows[c] = []
    for hour in hours:
        for cell_id, cell_spop in zip(cells, spop):
            rows["CELL_ID"].append(cell_id)
            rows["TT"].append(hour)
            rows["H_DNG_CD"].append("1168064")
            rows["SPOP"].append(cell_spop)
            for c in merge.AGE_COLUMNS:
                rows[c].append(cell_spop / 2 if c in ("M00", "F00") else None)
    return pa.table(rows)


def _seed_nowcast_baseline() -> None:
    """nowcaster 추정치를 오늘·내일 두 날짜에 심는다(12시간 앞이 자정을 넘는다)."""
    today_hours = [f"{h:02d}" for h in range(14, 24)]
    tomorrow_hours = [f"{h:02d}" for h in range(4)]
    _put_parquet(
        "silver/living_population_grid/dt=2026-08-15/hh=00/nowcast.parquet",
        _grid_table(today_hours, (200.0, 50.0)),
    )
    _put_parquet(
        "silver/living_population_grid/dt=2026-08-16/hh=00/nowcast.parquet",
        _grid_table(tomorrow_hours, (200.0, 50.0)),
    )


def _seed_realtime_silver(
    *, with_forecast: bool = True, partial: bool = False
) -> None:
    row = {
        "AREA_NM": "강남 MICE 관광특구",
        "AREA_CD": "POI001",
        "AREA_CONGEST_LVL": "보통",
        "AREA_PPLTN_MIN": 1000,
        "AREA_PPLTN_MAX": 1400,
        "MALE_PPLTN_RATE": 52.0,
        "FEMALE_PPLTN_RATE": 48.0,
        "FCST_YN": "Y" if with_forecast else "N",
    }
    for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
        stamp = WINDOW_START.replace(minute=0) + timedelta(hours=slot)
        row[f"FCST_{slot}_TIME"] = (
            stamp.strftime("%Y-%m-%d %H:%M") if with_forecast else None
        )
        row[f"FCST_{slot}_PPLTN_MIN"] = 3000 if with_forecast else None
        row[f"FCST_{slot}_PPLTN_MAX"] = 3400 if with_forecast else None
    writer = put_partial_source_snapshot if partial else put_source_snapshot
    writer("population_realtime", WINDOW_START, pa.Table.from_pylist([row]))


def _read_normalized(key: str) -> dict[str, dict]:
    body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return {row["CELL_ID"]: row for row in table.to_pylist()}, table


class TestPoi001RealGeometryFixture:
    """이번 조사에서 확인한 실제 겹침/비겹침 관계를, 하드코딩 없이 실제 함수로 재검증한다."""

    def test_overlapping_cell_actually_intersects_poi001(self):
        areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
        poi001 = next(a for a in areas if a.area_cd == "POI001")
        cell_geom = grid.cell_id_to_polygon(OVERLAPPING_CELL_ID)

        intersection_area = cell_geom.intersection(poi001.geometry).area

        assert intersection_area / grid.GRID_AREA_M2 > 0.9  # 90% 이상 겹침

    def test_pass_through_cell_does_not_intersect_poi001(self):
        areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
        poi001 = next(a for a in areas if a.area_cd == "POI001")
        cell_geom = grid.cell_id_to_polygon(PASS_THROUGH_CELL_ID)

        assert not cell_geom.intersects(poi001.geometry)


class TestEndToEndRun:
    def test_current_tick_updates_overlapping_cell_and_passes_through_the_rest(self):
        _seed_nowcast_baseline()
        _seed_realtime_silver()

        assert main.run(WINDOW_START) == 0

        rows, table = _read_normalized(CURRENT_KEY)
        assert set(rows) == {OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID}

        # 겹치지 않는 격자는 baseline 그대로 pass-through.
        passthrough = rows[PASS_THROUGH_CELL_ID]
        assert passthrough["SPOP"] == 50
        assert passthrough["M00"] == 25
        assert passthrough["F00"] == 25
        assert passthrough["H_DNG_CD"] == "1168064"

        # 크게 겹치는 격자는 실측 POI 인구((1000+1400)/2=1200) 쪽으로 이동한다.
        overlapping = rows[OVERLAPPING_CELL_ID]
        assert overlapping["SPOP"] != 200
        assert abs(overlapping["SPOP"] - 1200) < abs(200 - 1200)

        # 출력 스키마 회귀 방어: 추론기가 이 파일을 지금과 같은 코드로 읽는다.
        assert set(table.column_names) == {
            "CELL_ID",
            "H_DNG_CD",
            "SPOP",
            *merge.AGE_COLUMNS,
        }
        for name in ("SPOP", *merge.AGE_COLUMNS):
            assert table.schema.field(name).type == pa.int64()

    def test_writes_a_tick_for_every_forecast_hour_including_after_midnight(self):
        _seed_nowcast_baseline()
        _seed_realtime_silver()

        main.run(WINDOW_START)

        for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
            target = WINDOW_START.replace(minute=0) + timedelta(hours=slot)
            key = (
                f"silver/living_population_normalized/dt={target:%Y-%m-%d}/"
                f"hh={target:%H}/{target:%H%M}.parquet"
            )
            rows, table = _read_normalized(key)
            assert set(rows) == {OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID}
            # 미래 파일도 현재분과 같은 스키마여야 한다 — 추론기가 같은 코드로 읽는다.
            assert set(table.column_names) == {
                "CELL_ID",
                "H_DNG_CD",
                "SPOP",
                *merge.AGE_COLUMNS,
            }

        # 자정 넘김: 다음 날 파티션이 실제로 쓰였다.
        assert "dt=2026-08-16" in str(
            _s3().list_objects_v2(
                Bucket=TEST_BUCKET,
                Prefix="silver/living_population_normalized/dt=2026-08-16/",
            )
        )

    def test_forecast_tick_scales_total_and_keeps_baseline_gender_ratio(self):
        _seed_nowcast_baseline()
        _seed_realtime_silver()

        main.run(WINDOW_START)

        target = WINDOW_START.replace(minute=0) + timedelta(hours=1)
        rows, _ = _read_normalized(
            f"silver/living_population_normalized/dt={target:%Y-%m-%d}/"
            f"hh={target:%H}/{target:%H%M}.parquet"
        )
        overlapping = rows[OVERLAPPING_CELL_ID]

        # 예측 인구((3000+3400)/2=3200) 쪽으로 이동한다.
        assert abs(overlapping["SPOP"] - 3200) < abs(200 - 3200)
        # 성비는 baseline 그대로(M00:F00 = 1:1), 총량만 커진다.
        assert overlapping["M00"] == overlapping["F00"]
        assert overlapping["M00"] + overlapping["F00"] == pytest.approx(
            overlapping["SPOP"], abs=1
        )

    def test_without_forecasts_only_the_current_tick_is_written(self):
        _seed_nowcast_baseline()
        _seed_realtime_silver(with_forecast=False)

        assert main.run(WINDOW_START) == 0

        rows, _ = _read_normalized(CURRENT_KEY)
        assert set(rows) == {OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID}
        listing = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="silver/living_population_normalized/"
        )
        assert [item["Key"] for item in listing["Contents"]] == [CURRENT_KEY]

    def test_manifest_records_targets_and_baseline_dates(self):
        _seed_nowcast_baseline()
        _seed_realtime_silver()

        main.run(WINDOW_START)

        key = "_manifest/living_population_normalized/dt=2026-08-15/hh=14/1405.json"
        body = json.loads(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())

        assert body["baseline_dates"] == ["2026-08-15", "2026-08-16"]
        assert body["input_status"] == "success"
        assert body["input_freshness"] == "current"
        assert body["partial_policy"] == "repair"
        assert body["resolution"] == "observed"
        assert body["source_selection"] == {
            "freshness": "current",
            "partial_policy": "repair",
            "requested_dttm": WINDOW_START.astimezone(UTC).isoformat(),
            "resolution": "observed",
            "selected_dttm": WINDOW_START.astimezone(UTC).isoformat(),
            "source_id": "population_realtime",
            "status": "success",
        }
        assert body["source_observed_at"] == WINDOW_START.astimezone(UTC).isoformat()
        assert body["cell_count"] == 2
        assert body["poi_matched_count"] == 1
        assert body["poi_master"] == {
            "mode": "static",
            "manifest_uri": None,
            "manifest_sha256": None,
        }
        assert body["poi_master_row_count"] == 121
        assert body["poi_forecast_count"] == 1
        assert body["forecast_horizons"] == 12
        assert len(body["written_keys"]) == 13
        assert body["skipped_targets"] == []

    def test_partial_input_is_explicitly_repaired_and_recorded(self):
        """Normalizer만 PARTIAL을 opt-in하고 baseline merge 결과를 repaired로 남긴다."""
        _seed_nowcast_baseline()
        _seed_realtime_silver(partial=True)

        main.run(WINDOW_START)

        key = "_manifest/living_population_normalized/dt=2026-08-15/hh=14/1405.json"
        body = json.loads(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        assert body["input_status"] == "partial"
        assert body["input_freshness"] == "current"
        assert body["partial_policy"] == "repair"
        assert body["resolution"] == "repaired"
        assert body["source_selection"]["resolution"] == "repaired"

    def test_missing_future_baseline_skips_that_hour_only(self):
        """내일 추정치가 없으면 자정 이후 시각만 건너뛰고 나머지는 쓴다."""
        today_hours = [f"{h:02d}" for h in range(14, 24)]
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-15/hh=00/nowcast.parquet",
            _grid_table(today_hours, (200.0, 50.0)),
        )
        _seed_realtime_silver()

        assert main.run(WINDOW_START) == 0

        listing = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="silver/living_population_normalized/"
        )
        written = [item["Key"] for item in listing["Contents"]]
        assert CURRENT_KEY in written
        assert not [k for k in written if "dt=2026-08-16" in k]

        manifest_key = (
            "_manifest/living_population_normalized/dt=2026-08-15/hh=14/1405.json"
        )
        body = json.loads(
            _s3().get_object(Bucket=TEST_BUCKET, Key=manifest_key)["Body"].read()
        )
        assert len(body["skipped_targets"]) == 3  # 다음 날 01/02/03시

    def test_raises_when_the_current_baseline_is_missing(self):
        _seed_realtime_silver()  # nowcast 추정치를 심지 않음

        with pytest.raises(storage.PartitionNotFoundError):
            main.run(WINDOW_START)
