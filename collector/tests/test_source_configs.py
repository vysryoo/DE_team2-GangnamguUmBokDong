"""Collector 소스 YAML 10종 테스트.

`sources/*.yaml` 전부가 config loader를 통과하는지, 이번에 늘어난 3개 키
(`max_missing_ratio` · `fetch` · `backfill`)가 생략 가능한지, 그리고 실제
어댑터(`seoul_openapi` · `kma_apihub`)를 공통 코드 수정 없이 pipeline까지
end-to-end로 통과시킬 수 있는지 확인한다.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree

import httpx
import pyarrow.parquet as pq
import pytest
from core.forecast import POPULATION_FORECAST_SLOT_COUNT
from core.kst import KST

pytestmark = pytest.mark.usefixtures("s3_bucket")

from core.s3 import get_object_bytes
from core.sources import COLLECTOR_SOURCE_IDS

import config.loader as config_loader
import pipeline
import storage
from adapters import (  # noqa: F401 — @adapter 등록을 위한 import
    kma_apihub,
    seoul_openapi,
)
from manifest import RunStatus

SOURCES_DIR = Path(__file__).resolve().parent.parent / "sources"
SOURCE_IDS = sorted(p.stem for p in SOURCES_DIR.glob("*.yaml"))
OFFICIAL_POI_LIST = (
    Path(__file__).resolve().parents[2]
    / "normalizer"
    / "data"
    / "서울시 주요 121장소 목록.xlsx"
)


def _official_poi_codes() -> frozenset[str]:
    """저장소에 고정한 서울시 공식 XLSX에서 AREA_CD 집합을 읽는다."""
    namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(OFFICIAL_POI_LIST) as workbook:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            shared_root = ElementTree.fromstring(
                workbook.read("xl/sharedStrings.xml")
            )
            shared_strings = [
                "".join(node.text or "" for node in item.findall(".//main:t", namespace))
                for item in shared_root.findall("main:si", namespace)
            ]

        sheet = ElementTree.fromstring(
            workbook.read("xl/worksheets/sheet1.xml")
        )
        values: list[str] = []
        for cell in sheet.findall(".//main:c", namespace):
            value_node = cell.find("main:v", namespace)
            if value_node is None or value_node.text is None:
                continue
            value = value_node.text
            if cell.get("t") == "s":
                value = shared_strings[int(value)]
            values.append(value)

    return frozenset(value for value in values if re.fullmatch(r"POI\d{3}", value))


@pytest.fixture(autouse=True)
def _api_keys(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "test-seoul-key")
    monkeypatch.setenv("KMA_APIHUB_KEY", "test-kma-key")


class TestAllSourcesLoad:
    def test_ten_sources_exist(self):
        assert len(SOURCE_IDS) == 10

    @pytest.mark.parametrize("source_id", SOURCE_IDS)
    def test_loads_without_error(self, source_id):
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)

        assert config.source_id == source_id
        assert config.adapter in ("seoul_openapi", "kma_apihub")
        assert config.config_version.startswith("sha256:")

    @pytest.mark.parametrize("source_id", SOURCE_IDS)
    def test_no_source_uses_delayed_backfill(self, source_id):
        """운영 소스는 소비 DAG가 없는 지연 backfill 큐를 만들지 않는다."""
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)

        assert config.backfill is None

    @pytest.mark.parametrize(
        ("source_id", "expected_ratio"),
        (
            ("living_population_grid", 0.1),
            ("cultural_event", 0.1),
            ("performance_event", 0.1),
        ),
    )
    def test_daily_sources_keep_agreed_partial_tolerance(
        self, source_id, expected_ratio
    ):
        """일일 source는 합의된 누락 허용치 안에서 PARTIAL 처리를 유지한다."""
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)

        assert config.quality.max_missing_ratio == expected_ratio

    def test_no_source_declares_response_pagination_meta(self):
        """`RNUM`·`START_INDEX`·`END_INDEX`는 데이터가 아니라 요청/응답 메타다.

        실측에서 `START_INDEX`/`END_INDEX`는 전 행이 `(0, 0)`이고 `RNUM`은 그 응답
        안에서의 행 번호다. 선언하면 두 가지가 나빠진다.

        1. archive에 의미 없는 컬럼이 쌓인다(`docs/collector/bootstrap-design.md`도
           "archive에도 의미가 없다"고 적어뒀다 — CSV bootstrap은 채울 수조차 없다).
        2. `compaction.dedup`이 `_window_start`를 뺀 **전체 데이터 컬럼**으로 묶으므로
           `RNUM`이 dedup 키에 들어간다. 같은 시간대를 여러 윈도우가 반복 수집할 때
           목록에 지연 등록이 끼어들어 `RNUM`이 한 칸 밀리면, 같은 대여가 서로 다른
           행으로 남아 중복이 걷히지 않는다.
        """
        forbidden = {"RNUM", "START_INDEX", "END_INDEX"}
        for source_id in SOURCE_IDS:
            config = config_loader.load(source_id, base_dir=SOURCES_DIR)
            declared = forbidden & set(config.columns)
            assert not declared, (
                f"{source_id}에 응답 메타 컬럼이 선언돼 있다: {sorted(declared)}"
            )

    def test_bike_rental_history_preserves_string_compatible_types(self):
        config = config_loader.load("bike_rental_history", base_dir=SOURCES_DIR)

        # 윈도우 직전 순간(window_last)의 시를 요청한다 — 매시 끝자락 누락 방지.
        # `RNUM`은 의도적으로 선언하지 않는다(위 응답 메타 테스트 참고).
        assert config.adapter_params["path_suffix"] == (
            "/{window_last:%Y-%m-%d}/{window_last:%H}"
        )
        assert config.columns["USE_MIN"].types == ("str", "int")
        assert config.columns["USE_DST"].types == ("str", "float")
        assert config.columns["BIRTH_YEAR"].types == ("str", "int")

    def test_bike_realtime_declares_bounded_probe_identity_and_counts(self):
        """bikeList의 완결성·identity·DDL 안전 경계를 source config에 고정한다."""
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)

        assert config.adapter_params["pagination"] == "probe_until_empty"
        assert config.adapter_params["page_size"] == 1000
        assert config.adapter_params["max_probe_pages"] == 10
        assert config.natural_key == ("stationId",)
        assert config.columns["stationId"].required is True
        assert config.columns["stationName"].required is False
        for column in ("rackTotCnt", "parkingBikeTotCnt", "shared"):
            assert config.columns[column].range.min == 0
            assert config.columns[column].range.max == float("inf")
        assert config.columns["parkingBikeTotCnt"].on_outlier is None
        assert config.columns["stationLatitude"].range.model_dump() == {
            "min": 37.0,
            "max": 38.0,
        }
        assert config.columns["stationLongitude"].range.model_dump() == {
            "min": 126.5,
            "max": 127.5,
        }

    @pytest.mark.parametrize(
        ("adapter_params", "message"),
        [
            ({}, "adapter_params.page_size"),
            (
                {
                    "service": "bikeList",
                    "page_size": 1000,
                    "root_key": "rentBikeStatus.row",
                    "pagination": "unknown",
                },
                "adapter_params.pagination",
            ),
            (
                {
                    "service": "bikeList",
                    "page_size": 1000,
                    "root_key": "rentBikeStatus.row",
                    "pagination": "probe_until_empty",
                },
                "adapter_params.max_probe_pages",
            ),
            (
                {
                    "service": "bikeList",
                    "page_size": 1000,
                    "root_key": "rentBikeStatus.row",
                    "pagination": "probe_until_empty",
                    "max_probe_pages": 0,
                },
                "adapter_params.max_probe_pages",
            ),
            (
                {
                    "service": "bikeList",
                    "page_size": 0,
                    "root_key": "rentBikeStatus.row",
                },
                "adapter_params.page_size",
            ),
        ],
    )
    def test_seoul_pagination_config_is_validated(
        self, tmp_path, adapter_params, message
    ):
        source = tmp_path / "invalid_pagination.yaml"
        source.write_text(
            "\n".join(
                [
                    "source_id: invalid_pagination",
                    "description: invalid pagination config",
                    "adapter: seoul_openapi",
                    f"adapter_params: {json.dumps(adapter_params)}",
                    "schedule: {interval: 5m}",
                    "storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}",
                    "quality: {max_drop_ratio: 0.05}",
                    "policies: {required_missing: drop_row, required_outlier: drop_row, optional_missing: keep_null, optional_outlier: set_null}",
                    "columns: {}",
                ]
            )
        )

        with pytest.raises(config_loader.ConfigError, match=message):
            config_loader.load("invalid_pagination", base_dir=tmp_path)

    def test_cultural_coordinates_match_gold_safety_box(self):
        """문화행사 좌표 경계를 Gold Point DDL과 동일하게 유지한다."""
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)

        assert config.columns["LAT"].range.model_dump() == {"min": 37.0, "max": 38.0}
        assert config.columns["LOT"].range.model_dump() == {
            "min": 126.5,
            "max": 127.5,
        }

    def test_population_realtime_matches_official_poi_list(self):
        """실시간 인구 요청 대상을 저장소의 서울시 공식 목록과 동일하게 유지한다."""
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)

        assert config.adapter_params["poi_start"] == 1
        assert config.adapter_params["poi_end"] == 131
        assert config.adapter_params["poi_exclude"] == [
            22,
            28,
            57,
            62,
            65,
            69,
            75,
            97,
            99,
            113,
        ]
        assert config.adapter_params["concurrency"] == 8
        assert config.adapter_params["root_key_literal"] is True
        assert config.adapter_params["flatten_forecast"] is True
        planned = seoul_openapi.SeoulOpenApiAdapter.planned_parts(
            config, window=None
        )
        assert planned is not None
        planned_codes = {part.removeprefix("poi-") for part in planned}
        assert planned_codes == _official_poi_codes()

    def test_population_realtime_declares_all_twelve_forecast_slots(self):
        """어댑터가 평탄화한 `FCST_n_*` 컬럼이 전부 선언돼 있어야 silver까지 살아 남는다."""
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)

        for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
            assert f"FCST_{slot}_TIME" in config.columns
            assert f"FCST_{slot}_CONGEST_LVL" in config.columns
            assert f"FCST_{slot}_PPLTN_MIN" in config.columns
            assert f"FCST_{slot}_PPLTN_MAX" in config.columns
        assert "FCST_YN" in config.columns

    def test_forecast_columns_are_optional_so_non_forecast_pois_survive(self):
        """`FCST_YN='N'` 지점이 required_missing(drop_row)에 걸려 사라지면 현재 인구 보정까지 잃는다."""
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)

        forecast_columns = {
            k: v for k, v in config.columns.items() if k.startswith("FCST_")
        }
        assert forecast_columns
        assert not [k for k, spec in forecast_columns.items() if spec.required]

    def test_forecast_population_bounds_match_the_observed_columns(self):
        """예측 인구도 관측 인구와 같은 범위를 쓴다 — 둘이 갈리면 같은 값에 다른 판정이 난다."""
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)
        observed = config.columns["AREA_PPLTN_MIN"].range

        for slot in range(1, POPULATION_FORECAST_SLOT_COUNT + 1):
            assert config.columns[f"FCST_{slot}_PPLTN_MIN"].range == observed
            assert config.columns[f"FCST_{slot}_PPLTN_MAX"].range == observed
            assert (
                config.columns[f"FCST_{slot}_CONGEST_LVL"].enum
                == config.columns["AREA_CONGEST_LVL"].enum
            )

    @pytest.mark.parametrize(
        "adapter_params",
        [
            {
                "service": "citydata_ppltn",
                "page_size": 1000,
                "root_key": "SeoulRtd.citydata_ppltn",
            },
            {
                "service": "citydata_ppltn",
                "page_size": 1000,
                "root_key": "SeoulRtd.citydata_ppltn",
                "poi_start": 10,
                "poi_end": 9,
            },
            {
                "service": "citydata_ppltn",
                "page_size": 1000,
                "root_key": "SeoulRtd.citydata_ppltn",
                "root_key_literal": True,
                "flatten_forecast": True,
                "poi_start": 1,
                "poi_end": 131,
                "poi_exclude": [22, 22],
            },
            {
                "service": "citydata_ppltn",
                "page_size": 1000,
                "root_key": "SeoulRtd.citydata_ppltn",
                "root_key_literal": True,
                "flatten_forecast": True,
                "poi_start": 1,
                "poi_end": 131,
                "poi_exclude": [132],
            },
        ],
    )
    def test_population_poi_range_is_validated_at_config_load(
        self, tmp_path, adapter_params
    ):
        source = tmp_path / "invalid_population.yaml"
        source.write_text(
            "\n".join(
                [
                    "source_id: invalid_population",
                    "description: invalid population config",
                    "adapter: seoul_openapi",
                    f"adapter_params: {json.dumps(adapter_params)}",
                    "schedule: {interval: 5m}",
                    "storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}",
                    "quality: {max_drop_ratio: 0.05}",
                    "policies: {required_missing: drop_row, required_outlier: drop_row, optional_missing: keep_null, optional_outlier: set_null}",
                    "columns: {}",
                ]
            )
        )

        with pytest.raises(config_loader.ConfigError, match="adapter_params.poi"):
            config_loader.load("invalid_population", base_dir=tmp_path)

    def test_unknown_pagination_mode_is_rejected_at_config_load(self, tmp_path):
        """오타가 total 기반 조회로 조용히 폴백하지 못하게 한다."""
        source = tmp_path / "invalid_pagination.yaml"
        source.write_text(
            """source_id: invalid_pagination
description: invalid pagination config
adapter: seoul_openapi
adapter_params: {service: bikeList, page_size: 1000, root_key: rentBikeStatus.row, pagination: probe}
schedule: {interval: 5m}
storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}
quality: {max_drop_ratio: 0.05}
policies: {required_missing: drop_row, required_outlier: drop_row, optional_missing: keep_null, optional_outlier: set_null}
columns: {stationId: {types: [str], required: true}}
"""
        )

        with pytest.raises(config_loader.ConfigError, match="pagination"):
            config_loader.load("invalid_pagination", base_dir=tmp_path)

    def test_natural_key_must_reference_required_column(self, tmp_path):
        """nullable 컬럼을 snapshot identity로 선언하지 못하게 한다."""
        source = tmp_path / "invalid_identity.yaml"
        source.write_text(
            """source_id: invalid_identity
description: invalid identity config
adapter: seoul_openapi
adapter_params: {service: bikeList, page_size: 1000, root_key: rentBikeStatus.row}
schedule: {interval: 5m}
storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}
quality: {max_drop_ratio: 0.05}
policies: {required_missing: drop_row, required_outlier: drop_row, optional_missing: keep_null, optional_outlier: set_null}
natural_key: [stationName]
columns: {stationName: {types: [str]}}
"""
        )

        with pytest.raises(config_loader.ConfigError, match="required=true"):
            config_loader.load("invalid_identity", base_dir=tmp_path)


class TestOptionalKeysOmittable:
    """새로 늘어난 3개 키가 생략 가능하고, 생략 시 기존과 같이 동작하는지 확인한다."""

    def test_fetch_omitted_falls_back_to_effective_budget(self):
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)

        assert config.fetch is None
        assert config.effective_fetch_budget() == config.schedule.interval / 2

    def test_backfill_omitted_defaults_to_disabled(self):
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)

        assert config.backfill is None

    def test_cultural_event_keeps_explicit_partial_tolerance(self):
        """문화행사는 합의된 범위의 catalog 누락을 PARTIAL로 처리한다."""
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)

        assert config.quality.max_missing_ratio == 0.1

    def test_max_missing_ratio_truly_omitted_defaults_to_zero(self, tmp_path):
        # 3개 키를 아예 안 쓴 최소 YAML로 기본값 자체를 확인한다.
        minimal = tmp_path / "minimal_source.yaml"
        minimal.write_text(
            """
source_id: minimal_source
description: 최소 설정 테스트용
adapter: seoul_openapi
adapter_params: {service: bikeList, page_size: 1000, root_key: rentBikeStatus.row}
schedule: {interval: 5m}
storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}
quality: {max_drop_ratio: 0.05}
policies:
  required_missing: drop_row
  required_outlier: drop_row
  optional_missing: keep_null
  optional_outlier: set_null
columns: {}
"""
        )
        config = config_loader.load("minimal_source", base_dir=tmp_path)

        assert config.quality.max_missing_ratio == 0.0
        assert config.quality.allow_empty is False
        assert config.fetch is None
        assert config.backfill is None


def _seoul_response(
    wrapper_key: str, rows: list[dict], total: int | None = None
) -> bytes:
    body = {wrapper_key: {"RESULT": {"CODE": "INFO-000"}, "row": rows}}
    if total is not None:
        body[wrapper_key]["list_total_count"] = total
    return json.dumps(body).encode()


class TestSeoulSourcesEndToEnd:
    """seoul_openapi 어댑터 하나로 소스 2개(bikeList·culturalEventInfo)가 그대로 도는지 확인한다.

    `cultural_event.yaml`은 공통 코드를 한 줄도 고치지 않고 YAML만 추가한 두 번째
    서울 열린데이터광장 소스다 — 이게 되면 "소스가 늘어도 공통 코드는 바뀌지
    않는다"는 설계 목표가 충족된 것으로 본다.
    """

    def test_bike_station_realtime_end_to_end(self, monkeypatch, tmp_path):
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)
        rows = [
            {
                "stationId": "ST-1",
                "stationName": None,
                "rackTotCnt": None,
                "parkingBikeTotCnt": "500000",
                "shared": "500000",
                "stationLatitude": "37.0",
                "stationLongitude": "127.5",
            },
            {
                "stationId": "ST-2",
                "stationName": "영점 거치대",
                "rackTotCnt": "0",
                "parkingBikeTotCnt": "500001",
                "shared": "500001",
                "stationLatitude": "38.0",
                "stationLongitude": "126.5",
            },
        ]

        def handler(request):
            if "/1/1000/" in str(request.url):
                return httpx.Response(
                    200,
                    content=_seoul_response("rentBikeStatus", rows, total=len(rows)),
                )
            return httpx.Response(
                200,
                content=json.dumps(
                    {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
                ).encode(),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, 14, 10, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None
        assert result.counts.expected == 2
        table = pq.read_table(io.BytesIO(get_object_bytes(result.artifacts.silver)))
        assert table.to_pylist() == [
            {
                "stationId": "ST-1",
                "stationName": None,
                "rackTotCnt": None,
                "parkingBikeTotCnt": 500000,
                "shared": 500000,
                "stationLatitude": 37.0,
                "stationLongitude": 127.5,
                "_row_status": "ok",
            },
            {
                "stationId": "ST-2",
                "stationName": "영점 거치대",
                "rackTotCnt": 0,
                "parkingBikeTotCnt": 500001,
                "shared": 500001,
                "stationLatitude": 38.0,
                "stationLongitude": 126.5,
                "_row_status": "ok",
            },
        ]

    def test_bike_negative_count_keeps_bronze_and_records_silver_issue(self):
        """음수 원문은 Bronze에 남기고 Silver에서는 clip 없이 품질 이슈로 분리한다."""
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)
        row = {
            "stationId": "ST-NEG",
            "stationName": "음수 재고",
            "rackTotCnt": "10",
            "parkingBikeTotCnt": "-1",
            "shared": "-10",
            "stationLatitude": "37.5",
            "stationLongitude": "127.0",
        }
        raw = _seoul_response("rentBikeStatus", [row], total=1)

        def handler(request):
            if "/1/1000/" in str(request.url):
                return httpx.Response(200, content=raw)
            return httpx.Response(
                200,
                content=json.dumps(
                    {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
                ).encode(),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, 14, 15, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda seconds: None
        )

        bronze = storage.read_bronze(
            config.source_id,
            window_start,
            result.artifacts.bronze.parts,
            result.artifacts.bronze.revision,
        )
        assert json.loads(bronze[0])["rentBikeStatus"]["row"] == [row]
        assert result.status is RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None
        assert result.column_issues["parkingBikeTotCnt"].outlier == 1
        assert result.column_issues["shared"].outlier == 1
        table = pq.read_table(io.BytesIO(get_object_bytes(result.artifacts.silver)))
        silver_row = table.to_pylist()[0]
        assert silver_row["parkingBikeTotCnt"] is None
        assert silver_row["shared"] is None
        assert silver_row["_row_status"] == "repaired"

    def test_cultural_event_end_to_end_via_yaml_only(self, monkeypatch):
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)
        rows = [
            {
                "TITLE": "한강 밤도깨비 야시장",
                "CODENAME": "축제",
                "GUNAME": "영등포구",
                "PLACE": "여의도한강공원",
                "STRTDATE": "2026-08-01",
                "END_DATE": "2026-08-31",
                "IS_FREE": "무료",
                "LOT": "126.93",
                "LAT": "37.53",
            }
        ]

        def handler(request):
            return httpx.Response(
                200, content=_seoul_response("culturalEventInfo", rows, total=len(rows))
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None

    def test_cultural_event_allows_zero_rows(self):
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)

        def handler(request):
            return httpx.Response(
                200, content=_seoul_response("culturalEventInfo", [], total=0)
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 13, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.EMPTY

    def test_performance_event_end_to_end(self):
        config = config_loader.load("performance_event", base_dir=SOURCES_DIR)
        rows = [
            {
                "SCH_SEQ": "1234",
                "TITLE": "잠실 야구 경기",
                "SDATE": "2026-08-20",
                "EDATE": "2026-08-20",
                "USE_TIME": "18:30",
                "USE_AGE": "전체 관람가",
                "USE_TARGET": "시민",
                "USE_PAY": "유료",
                "LINK_URL": "https://example.com/event",
                "REG_DATE": "2026-08-01",
                "UPD_DATE": "2026-08-10",
                "SCH_CODE_A": "1",
                "SCH_CODE_B": "8",
                "CODE_TITLE_A": "스포츠경기",
                "CODE_TITLE_B": "잠실야구장",
            }
        ]

        def handler(request):
            return httpx.Response(
                200,
                content=_seoul_response("stadiumScheduleInfo", rows, total=len(rows)),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 18, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.SUCCEEDED
        # allow_empty: true라서 status만 보면 스키마가 어긋나 전 행이 폐기돼도
        # 통과한다. 행이 실제로 살아남았는지까지 고정한다.
        assert result.counts.kept == 1
        assert result.drop_ratio == 0.0
        assert result.artifacts.silver is not None


class TestKmaSourceEndToEnd:
    def test_weather_ultra_short_live_end_to_end(self):
        config = config_loader.load("weather_ultra_short_live", base_dir=SOURCES_DIR)

        def handler(request):
            params = dict(request.url.params)
            common = {
                "nx": int(params["nx"]),
                "ny": int(params["ny"]),
                "baseDate": params["base_date"],
                "baseTime": params["base_time"],
            }
            items = [
                {**common, "category": "T1H", "obsrValue": "28.5"},
                {**common, "category": "REH", "obsrValue": "55"},
                {**common, "category": "WSD", "obsrValue": "2.1"},
                {**common, "category": "RN1", "obsrValue": "0"},
                {**common, "category": "PTY", "obsrValue": "0"},
            ]
            body = {
                "response": {
                    "header": {"resultCode": "00"},
                    "body": {"items": {"item": items}},
                }
            }
            return httpx.Response(200, content=json.dumps(body).encode())

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, 14, 0, tzinfo=KST)

        result = pipeline.execute_window(
            config, window_start, client=client, sleep_fn=lambda s: None
        )

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None

    @pytest.mark.parametrize(
        "source_id", ["weather_ultra_short_live", "weather_short_term_forecast"]
    )
    def test_weather_grids_cover_all_real_stations_without_duplicates(self, source_id):
        """grids 목록은 `loader/scripts/generate_weather_grids.py`가 실제 대여소
        좌표(`apps/api/seed_data/stations_seoul.json`) 전부를 `latlon_to_grid`로
        변환해 만든 고유 격자 집합이다(현재 34개). 더는 "구당 격자 1개" 하드코딩
        테이블에 맞출 필요가 없으므로, 여기서는 "중복 없음"만 회귀로 잡는다."""
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)
        grids = config.adapter_params["grids"]

        assert len(grids) == len({tuple(g) for g in grids})


def test_source_yaml_ids_match_core_sources() -> None:
    """`core.sources`의 수집 source 목록이 YAML 파일과 정확히 같은지 확인한다."""
    assert SOURCE_IDS == sorted(COLLECTOR_SOURCE_IDS)
