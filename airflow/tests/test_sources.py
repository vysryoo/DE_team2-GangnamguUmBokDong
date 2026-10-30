"""`config/sources.py`의 source id가 collector source 설정과 일치하는지 검증한다.

Airflow venv에는 core가 없으므로 `collector/sources/*.yaml`을 직접 읽는다.
"""

from pathlib import Path

import yaml

from config import sources

_COLLECTOR_SOURCES_DIR = Path(__file__).resolve().parents[2] / "collector" / "sources"


def _collector_source_ids() -> set[str]:
    return {
        yaml.safe_load(path.read_text(encoding="utf-8"))["source_id"]
        for path in _COLLECTOR_SOURCES_DIR.glob("*.yaml")
    }


def _airflow_source_ids() -> set[str]:
    source_ids: set[str] = set()
    for name, value in vars(sources).items():
        if name.startswith("_") or not name.isupper():
            continue
        if isinstance(value, str):
            source_ids.add(value)
        elif isinstance(value, tuple):
            source_ids.update(item for item in value if isinstance(item, str))
    return source_ids


def test_collector_yaml_file_name_matches_source_id() -> None:
    for path in _COLLECTOR_SOURCES_DIR.glob("*.yaml"):
        assert yaml.safe_load(path.read_text(encoding="utf-8"))["source_id"] == path.stem


def test_every_airflow_source_id_exists_in_collector() -> None:
    assert _airflow_source_ids() <= _collector_source_ids()


def test_every_collector_source_is_cold_bronze_compacted() -> None:
    assert set(sources.COLD_BRONZE_SOURCES) == _collector_source_ids()
