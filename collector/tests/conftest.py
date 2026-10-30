"""#2·#3·#4 테스트 공용 픽스처.

`make_spec`은 `ColumnSpec.model_construct()`로 만든다 — 일반 생성자를 쓰면 validator가
돌아서 `range`의 부분 선언이나 `range`+`enum` 동시 선언 같은 "일부러 위반된" spec을
만들 수 없다. 정책 함수(#3)의 방어 코드는 이런 잘못된 spec에서도 안전한지를 테스트해야
하므로, 이 픽스처만 validator를 우회한다. 실제 YAML 로딩 경로(config.loader.load)는
`SourceConfig.model_validate`를 쓰므로 이 우회와 무관하게 여전히 막힌다.

moto S3 bucket은 `core.testing`의 `s3_bucket` fixture를 쓴다.
"""

from datetime import datetime

import pytest
from core.kst import KST

from adapters import base as adapter_base
from config.schema import ColumnSpec, Range
from validation import registry
from validation.types import Issue, RunContext

pytest_plugins = ("core.testing",)


@pytest.fixture
def make_spec():
    """ColumnSpec을 만든다. `range=(0, 200)`처럼 튜플로 주면 Range로 감싼다."""

    def _make(types=("int",), required=False, range=None, enum=None, default=None, on_missing=None, on_outlier=None):
        bounds = Range.model_construct(min=range[0], max=range[1]) if isinstance(range, tuple) else range
        return ColumnSpec.model_construct(
            types=tuple(types),
            required=required,
            range=bounds,
            enum=enum,
            default=default,
            on_missing=on_missing,
            on_outlier=on_outlier,
        )

    return _make


@pytest.fixture
def make_issue(make_spec):
    """Issue를 만든다. `required`를 생략하면 spec의 값을 따른다."""

    def _make(kind, spec=None, column="col", raw=None, required=None):
        spec = make_spec() if spec is None else spec
        return Issue(
            column=column,
            kind=kind,
            required=spec.required if required is None else required,
            raw_value=raw,
            spec=spec,
        )

    return _make


@pytest.fixture
def ctx():
    return RunContext(
        source_id="bike_station_realtime",
        window_start=datetime(2026, 8, 12, 14, 10, tzinfo=KST),
        window_end=datetime(2026, 8, 12, 14, 15, tzinfo=KST),
        attempt=1,
    )


@pytest.fixture
def clean_registry():
    """레지스트리는 전역 상태다. 테스트가 등록한 이름이 다른 테스트로 새지 않게 복원한다."""
    saved_policies = dict(registry._POLICIES)
    saved_row_policies = dict(registry._ROW_POLICIES)
    yield registry
    registry._POLICIES.clear()
    registry._POLICIES.update(saved_policies)
    registry._ROW_POLICIES.clear()
    registry._ROW_POLICIES.update(saved_row_policies)


@pytest.fixture
def clean_adapter_registry():
    """어댑터 레지스트리도 전역 상태다. 테스트가 등록한 이름이 다른 테스트로 새지 않게 복원한다."""
    saved = dict(adapter_base._ADAPTERS)
    yield adapter_base
    adapter_base._ADAPTERS.clear()
    adapter_base._ADAPTERS.update(saved)


@pytest.fixture(autouse=True)
def _isolated_s3_env(s3_env):
    """환경과 process-local S3 client cache를 테스트마다 격리한다."""
