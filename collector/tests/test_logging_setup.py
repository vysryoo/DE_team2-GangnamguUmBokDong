"""logging_setup.configure_logging 테스트.

`source_id`·`window`·`attempt`가 모든 로그 줄에 자동으로 붙는지, 호출부가 넘긴
추가 필드(`extra=`)가 `key=value` 형식으로 뒤에 붙는지, 그리고 `pipeline.py`처럼
자기 이름의 로거로 남긴 로그도 root의 핸들러를 통해 같은 필드를 받는지 확인한다.
"""

import io
import logging
from datetime import datetime

import logging_setup
from logging_setup import configure_httpx_request_logging, configure_logging
from core.kst import KST



def _configure(stream, **kwargs):
    return configure_logging(
        "bike_station_realtime",
        datetime(2026, 8, 12, 14, 10, tzinfo=KST),
        attempt=1,
        stream=stream,
        **kwargs,
    )


class TestFixedFields:
    def test_injects_source_id_window_attempt(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.info("done")

        line = stream.getvalue()
        assert "source_id=bike_station_realtime" in line
        assert "window=2026-08-12T14:10:00+09:00" in line
        assert "attempt=1" in line

    def test_fixed_fields_appear_before_extra_fields(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.info("done", extra={"stage": "bronze_written"})

        line = stream.getvalue()
        assert line.index("attempt=1") < line.index("stage=bronze_written")


class TestExtraFields:
    def test_extra_fields_rendered_as_key_value(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.info("bronze written", extra={"stage": "bronze_written", "parts": "3/3", "rounds": 1})

        line = stream.getvalue()
        assert "stage=bronze_written" in line
        assert "parts=3/3" in line
        assert "rounds=1" in line

    def test_no_extra_fields_still_logs_fixed_fields_only(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.warning("missing chunks")

        line = stream.getvalue()
        assert "WARNING" in line
        assert "missing chunks" in line
        assert "source_id=bike_station_realtime" in line


class TestLevel:
    def test_includes_level_name(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.error("failed", extra={"failure_reason": "quality_gate"})

        line = stream.getvalue()
        assert line.startswith("ERROR")
        assert "failure_reason=quality_gate" in line


class TestHttpxRequestLogging:
    def test_kma_suppresses_duplicate_httpx_info_log(self):
        configure_httpx_request_logging("kma_apihub")

        assert logging.getLogger("httpx").level == logging.WARNING

    def test_other_adapter_restores_inherited_httpx_level(self):
        configure_httpx_request_logging("kma_apihub")
        configure_httpx_request_logging("seoul_openapi")

        assert logging.getLogger("httpx").level == logging.NOTSET


class TestPropagationToNamedLogger:
    """pipeline.py는 root가 아니라 `logging.getLogger(__name__)`으로 로그를 남긴다."""

    def test_named_logger_inherits_fixed_fields(self):
        stream = io.StringIO()
        _configure(stream)
        pipeline_logger = logging.getLogger("pipeline")

        pipeline_logger.info("bronze written", extra={"stage": "bronze_written"})

        line = stream.getvalue()
        assert "source_id=bike_station_realtime" in line
        assert "stage=bronze_written" in line


class TestSecretRedaction:
    def test_masks_service_key_query_param(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.error("fetch failed: https://api.example.com/data?serviceKey=SUPERSECRET&page=1")

        line = stream.getvalue()
        assert "SUPERSECRET" not in line
        assert "serviceKey=***" in line

    def test_masks_seoul_api_path_segment(self):
        stream = io.StringIO()
        root = _configure(stream)

        root.error("fetch failed: http://openapi.seoul.go.kr:8088/SEOULSECRETKEY123/json/bikeList/1/1000/")

        line = stream.getvalue()
        assert "SEOULSECRETKEY123" not in line
        assert "openapi.seoul.go.kr:8088/***" in line


class TestIsolation:
    def test_second_configure_call_replaces_handlers_not_stacks(self):
        stream1 = io.StringIO()
        _configure(stream1)
        stream2 = io.StringIO()
        root2 = _configure(stream2)

        root2.info("only once")

        assert stream1.getvalue() == ""
        assert stream2.getvalue().count("only once") == 1


class TestBatchLogging:
    """compaction 같은 날짜 단위 배치는 window·attempt가 없다.

    없는 개념을 억지로 채우면(now()를 window로 넣는 등) 로그를 읽는 사람이 그 값을
    실제 수집 윈도우로 오해한다. 배치는 source_id만 고정 필드로 갖는다.
    """

    def test_injects_source_id(self):
        stream = io.StringIO()
        root = logging_setup.configure_batch_logging("bike_rental_history", stream=stream)

        root.info("stage=compaction status=compacted")

        assert "source_id=bike_rental_history" in stream.getvalue()

    def test_omits_window_and_attempt(self):
        stream = io.StringIO()
        root = logging_setup.configure_batch_logging("bike_rental_history", stream=stream)

        root.info("stage=compaction status=compacted")

        line = stream.getvalue()
        assert "window=" not in line
        assert "attempt=" not in line

    def test_extra_fields_still_render_as_key_value(self):
        stream = io.StringIO()
        root = logging_setup.configure_batch_logging("t_source", stream=stream)

        root.info("stage=compaction", extra={"rows": 42})

        assert "rows=42" in stream.getvalue()
