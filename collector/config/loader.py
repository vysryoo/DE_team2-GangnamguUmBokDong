"""`sources/{source_id}.yaml`을 읽어 검증한 SourceConfig로 만든다.

네트워크를 타기 전에 잘못된 설정을 잡도록 스키마 밖의 논리 검증(정책 이름, adapter
파라미터, 자연키)도 여기서 끝낸다.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import yaml
from pydantic import ValidationError
from validation.registry import (
    get_row_policy_params_model,
    is_policy_registered,
    is_row_policy_registered,
    policy_names,
    row_policy_names,
)

from config.schema import SourceConfig


class ConfigError(ValueError):
    """스키마는 통과했지만 논리적으로 잘못된 설정."""


def load(source_id: str, base_dir: Path = Path("sources")) -> SourceConfig:
    """`sources/{source_id}.yaml`을 읽어 검증한 SourceConfig를 반환한다.

    Args:
        source_id: 소스 ID. 파일명과 YAML 안의 `source_id`가 같아야 함
        base_dir: YAML 디렉터리
    Returns:
        원본 YAML bytes의 SHA-256을 `config_version`에 담은 SourceConfig
    Raises:
        pydantic.ValidationError: 스키마 위반
        ConfigError: 정책 이름, adapter 파라미터 등 논리적 설정 오류
    """

    raw_bytes = (base_dir / f"{source_id}.yaml").read_bytes()
    config_version = f"sha256:{hashlib.sha256(raw_bytes).hexdigest()}"
    config = SourceConfig.model_validate(yaml.safe_load(raw_bytes))

    errors: list[str] = []
    if config.source_id != source_id:
        errors.append(
            f"파일명 '{source_id}'와 YAML 안의 source_id '{config.source_id}'가 다릅니다."
        )
    errors += _check_policy_names(config)
    errors += _check_row_params(config)
    errors += _check_adapter_params(config)
    errors += _check_natural_key(config)
    if errors:
        raise ConfigError("\n".join(errors))

    return config.model_copy(update={"config_version": config_version})


def _is_int(value: object) -> bool:
    """bool을 제외한 정수인지 판별한다."""
    return isinstance(value, int) and not isinstance(value, bool)


def _check_adapter_params(config: SourceConfig) -> list[str]:
    """네트워크나 기존 bronze를 건드리기 전에 서울 API adapter 설정을 검증한다."""
    params = config.adapter_params
    if config.adapter != "seoul_openapi":
        return []

    errors: list[str] = []
    for required in ("service", "root_key"):
        value = params.get(required)
        if not isinstance(value, str) or not value.strip():
            errors.append(
                f"adapter_params.{required}: 비어있지 않은 문자열이 필수입니다."
            )
    page_size = params.get("page_size")
    if not _is_int(page_size) or page_size < 1:
        errors.append("adapter_params.page_size: 1 이상의 정수여야 합니다.")

    pagination = params.get("pagination", "total")
    if pagination not in {"total", "probe_until_empty"}:
        errors.append(
            "adapter_params.pagination: 'total' 또는 'probe_until_empty'여야 합니다."
        )

    for option in ("root_key_literal", "flatten_forecast"):
        if option in params and not isinstance(params[option], bool):
            errors.append(f"adapter_params.{option}: boolean이어야 합니다.")

    if params.get("service") == "citydata_ppltn":
        return errors + _check_population_params(params, pagination)

    if pagination == "probe_until_empty":
        return errors + _check_probe_params(config)
    if "max_probe_pages" in params:
        errors.append(
            "adapter_params.max_probe_pages: probe pagination에서만 사용할 수 있습니다."
        )
    return errors


def _check_probe_params(config: SourceConfig) -> list[str]:
    """빈 페이지까지 탐색하는 probe pagination 설정을 검증한다."""
    errors: list[str] = []
    if config.natural_key is None:
        errors.append(
            "adapter_params.pagination: probe pagination에는 natural_key가 필수입니다."
        )
    max_probe_pages = config.adapter_params.get("max_probe_pages")
    if not _is_int(max_probe_pages) or max_probe_pages < 1:
        errors.append(
            "adapter_params.max_probe_pages: probe pagination이면 "
            "1 이상의 정수가 필수입니다."
        )
    return errors


def _check_population_params(params: dict, pagination: str) -> list[str]:
    """실시간 인구(citydata_ppltn)의 응답 형태와 POI 범위 설정을 검증한다."""
    errors: list[str] = []
    if pagination != "total":
        errors.append(
            "adapter_params.pagination: citydata_ppltn에는 probe를 사용할 수 없습니다."
        )
    for option in ("root_key_literal", "flatten_forecast"):
        if params.get(option) is not True:
            errors.append(f"adapter_params.{option}: citydata_ppltn에는 true가 필수입니다.")

    poi_start = params.get("poi_start", 1)
    poi_end = params.get("poi_end")
    if not _is_int(poi_start):
        errors.append("adapter_params.poi_start: 1 이상의 정수여야 합니다.")
    if not _is_int(poi_end):
        errors.append("adapter_params.poi_end: 필수이며 정수여야 합니다.")
    valid_range = _is_int(poi_start) and _is_int(poi_end) and 1 <= poi_start <= poi_end
    if _is_int(poi_start) and _is_int(poi_end) and not valid_range:
        errors.append(
            "adapter_params.poi_start/poi_end: 1 <= poi_start <= poi_end여야 합니다."
        )

    poi_exclude = params.get("poi_exclude", [])
    if not isinstance(poi_exclude, list):
        errors.append("adapter_params.poi_exclude: 정수 목록이어야 합니다.")
        return errors
    excludes = [value for value in poi_exclude if _is_int(value)]
    if len(excludes) != len(poi_exclude):
        errors.append("adapter_params.poi_exclude: 모든 값이 정수여야 합니다.")
    if len(set(excludes)) != len(excludes):
        errors.append("adapter_params.poi_exclude: 중복 번호를 선언할 수 없습니다.")
    if valid_range:
        if any(value < poi_start or value > poi_end for value in excludes):
            errors.append("adapter_params.poi_exclude: POI 범위 안의 번호만 허용됩니다.")
        if len(set(excludes)) >= poi_end - poi_start + 1:
            errors.append("adapter_params.poi_exclude: 모든 POI를 제외할 수 없습니다.")
    return errors


def _check_natural_key(config: SourceConfig) -> list[str]:
    """자연키가 선언 컬럼을 정확히 가리키고 식별 필드가 필수인지 확인한다."""
    natural_key = config.natural_key
    if natural_key is None:
        return []

    errors: list[str] = []
    if len(set(natural_key)) != len(natural_key):
        errors.append("natural_key: 중복 컬럼을 선언할 수 없습니다.")

    for column in natural_key:
        spec = config.columns.get(column)
        if spec is None:
            errors.append(f"natural_key: columns에 없는 '{column}'을 참조합니다.")
        elif not spec.required:
            errors.append(
                f"natural_key: 식별 컬럼 '{column}'은 required=true여야 합니다."
            )
    return errors


def _check_policy_names(config: SourceConfig) -> list[str]:
    """기본 컬럼 정책, 개별 컬럼 오버라이드 정책, 행 정책 이름이 등록됐는지 확인한다."""
    policies = config.policies
    column_policies = [
        (f"policies.{field}", getattr(policies, field))
        for field in (
            "required_missing",
            "required_outlier",
            "optional_missing",
            "optional_outlier",
        )
    ]
    column_policies += [
        (f"columns.{column_name}.{field}", getattr(spec, field))
        for column_name, spec in config.columns.items()
        for field in ("on_missing", "on_outlier")
        if getattr(spec, field) is not None
    ]
    errors = [
        _unregistered_message(location, name, policy_names())
        for location, name in column_policies
        if not is_policy_registered(name)
    ]
    if policies.row is not None and not is_row_policy_registered(policies.row):
        errors.append(
            _unregistered_message("policies.row", policies.row, row_policy_names())
        )
    return errors


def _unregistered_message(location: str, name: str, registered: tuple[str, ...]) -> str:
    """미등록 정책 이름 오류 메시지를 만든다."""
    listed = ", ".join(registered) or "(없음)"
    return f"{location}: '{name}'이 등록돼 있지 않습니다. 등록된 이름: {listed}"


def _check_row_params(config: SourceConfig) -> list[str]:
    """행 정책의 `row_params`가 그 정책의 params 모델에 맞는지 확인한다."""

    row_name = config.policies.row
    if row_name is None or not is_row_policy_registered(row_name):
        return []  # 미등록 이름은 _check_policy_names가 이미 보고한다.

    params_model = get_row_policy_params_model(row_name)
    row_params = config.policies.row_params

    if params_model is None:
        if row_params is not None:
            return [
                f"policies.row_params: '{row_name}'은 params를 받지 않는데, row_params가 주어졌습니다: {row_params}"
            ]
        return []

    try:
        params_model.model_validate(row_params or {})
    except ValidationError as exc:
        return [f"policies.row_params: '{row_name}'의 params 형식이 아닙니다.: {exc}"]
    return []
