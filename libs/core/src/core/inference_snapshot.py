"""Gold demand가 소비하는 immutable inference success manifest 계약을 제공한다."""

from __future__ import annotations

import io
import math
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .gold_publication.canonical import (
    JsonValue,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
)
from .gold_publication.contract import Dependency, IdSet, InputArtifact
from .gold_publication.errors import ContractViolation
from .kst import KST
from .model_snapshot import (
    IdSetArtifactRef,
    ModelKind,
    ModelSnapshotManifest,
    validate_content_addressed_s3_uri,
    validate_model_snapshot_manifest,
)

LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION = "ml-inference-snapshot-manifest-v1"
"""Mean-only dual-read를 지원하는 구 inference manifest schema version이다."""

INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION = "ml-inference-snapshot-manifest-v2"
"""신규 inference producer가 쓰는 current manifest schema version이다."""

_SUPPORTED_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSIONS = frozenset(
    {
        LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    }
)

INFERENCE_HORIZON_COUNT = 12
"""Gold demand publication이 요구하는 exact horizon 수다."""

INFERENCE_OUTPUT_COLUMN_NAMES = (
    "station_id",
    "date",
    "hour",
    "minute",
    "horizon",
    "rental_pred_mean",
    "rental_pred_p10",
    "rental_pred_p50",
    "rental_pred_p90",
    "return_pred_mean",
    "return_pred_p10",
    "return_pred_p50",
    "return_pred_p90",
)
"""Gold demand가 소비하는 inference output의 exact column 순서다."""

INFERENCE_OUTPUT_ARROW_SCHEMA = pa.schema(
    (
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        pa.field("hour", pa.uint8(), nullable=False),
        pa.field("minute", pa.uint8(), nullable=False),
        pa.field("horizon", pa.uint8(), nullable=False),
        pa.field("rental_pred_mean", pa.float64(), nullable=False),
        pa.field("rental_pred_p10", pa.float64(), nullable=False),
        pa.field("rental_pred_p50", pa.float64(), nullable=False),
        pa.field("rental_pred_p90", pa.float64(), nullable=False),
        pa.field("return_pred_mean", pa.float64(), nullable=False),
        pa.field("return_pred_p10", pa.float64(), nullable=False),
        pa.field("return_pred_p50", pa.float64(), nullable=False),
        pa.field("return_pred_p90", pa.float64(), nullable=False),
    )
)
"""Inference authority Parquet의 metadata 없는 exact non-null Arrow schema다."""

LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES = (
    "station_id",
    "date",
    "hour",
    "minute",
    "horizon",
    "rental_pred_mean",
    "return_pred_mean",
)
"""Mean-only v1 inference output의 exact column 순서다."""

LEGACY_INFERENCE_OUTPUT_ARROW_SCHEMA = pa.schema(
    (
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        pa.field("hour", pa.uint8(), nullable=False),
        pa.field("minute", pa.uint8(), nullable=False),
        pa.field("horizon", pa.uint8(), nullable=False),
        pa.field("rental_pred_mean", pa.float64(), nullable=False),
        pa.field("return_pred_mean", pa.float64(), nullable=False),
    )
)
"""Dual-read가 허용하는 v1 inference output Arrow schema다."""

_MANIFEST_KEYS = frozenset(
    {
        "counts",
        "expected_sta_ids",
        "horizon_count",
        "inputs",
        "logical_dttm",
        "output",
        "producer_version",
        "rental_model_manifest",
        "return_model_manifest",
        "revision_no",
        "schema_version",
        "serving_release",
        "serving_plan",
        "station_dependency",
        "status",
    }
)
_DOCUMENT_REF_KEYS = frozenset(
    {"byte_sha256", "effective_contract_version", "release_version", "uri"}
)
_MODEL_REF_KEYS = frozenset(
    {
        "byte_sha256",
        "effective_contract_version",
        "model_kind",
        "model_version",
        "uri",
    }
)
_INPUT_REF_KEYS = frozenset({"byte_sha256", "role", "uri"})
_SERVING_PLAN_REF_KEYS = frozenset({"byte_sha256", "uri"})
_ID_SET_REF_KEYS = frozenset({"byte_sha256", "id_count", "schema_version", "uri"})
_OUTPUT_REF_KEYS = frozenset({"byte_sha256", "row_count", "uri"})
_COUNTS_KEYS = frozenset(
    {
        "actual_row_count",
        "actual_station_count",
        "expected_row_count",
        "expected_station_count",
        "failed_row_count",
        "failed_station_count",
    }
)
_DEPENDENCY_KEYS = frozenset(
    {
        "artifact_set_sha256",
        "input_fingerprint_sha256",
        "logical_dttm",
        "manifest_uri",
        "publication_key",
        "revision_no",
    }
)
_ROLE_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_CONTENT_VERSION_PATTERN = re.compile(r"sha256:(?P<checksum>[0-9a-f]{64})\Z")
_MAX_SAFE_INTEGER = 2**53 - 1


class InferenceSnapshotContractError(ContractViolation):
    """Inference snapshot bytes 또는 typed 값이 계약을 위반했다."""


class InferenceSnapshotStatus(StrEnum):
    """Gold authority로 사용할 수 있는 inference 최종 상태다."""

    SUCCEEDED = "succeeded"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class ServingReleaseRef:
    """Batch 시작에 한 번 pin한 serving-release manifest identity다."""

    byte_sha256: str
    effective_contract_version: str
    release_version: str
    uri: str

    def __post_init__(self) -> None:
        """Release version, effective contract와 content-addressed URI를 검증한다."""
        _require_sha256(self.byte_sha256, "serving release byte_sha256")
        _require_content_version(
            self.effective_contract_version,
            "serving release effective_contract_version",
        )
        _require_content_version(self.release_version, "serving release version")
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="json",
        )


@dataclass(frozen=True, slots=True)
class ServingPlanRef:
    """Inference expected scope를 결정한 exact serving-plan artifact identity다."""

    byte_sha256: str
    uri: str

    def __post_init__(self) -> None:
        """Serving-plan checksum과 content-addressed JSON URI를 검증한다."""
        _require_sha256(self.byte_sha256, "serving plan byte_sha256")
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="json",
        )


@dataclass(frozen=True, slots=True)
class ModelManifestRef:
    """Inference가 실제로 읽은 per-model manifest의 pinned identity다."""

    byte_sha256: str
    effective_contract_version: str
    model_kind: ModelKind
    model_version: str
    uri: str

    def __post_init__(self) -> None:
        """Model kind/version과 content-addressed manifest URI를 검증한다."""
        _require_sha256(self.byte_sha256, "model manifest byte_sha256")
        _require_content_version(
            self.effective_contract_version,
            "model manifest effective_contract_version",
        )
        if type(self.model_kind) is not ModelKind:
            raise InferenceSnapshotContractError(
                "model manifest model_kind는 exact ModelKind여야 합니다."
            )
        _require_content_version(
            self.model_version,
            "model manifest model_version",
        )
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="json",
        )


@dataclass(frozen=True, slots=True)
class ImmutableInputRef:
    """Inference 결과를 바꾸는 generic immutable upstream object다."""

    byte_sha256: str
    role: str
    uri: str

    def __post_init__(self) -> None:
        """Input role, checksum과 content-addressed S3 URI를 검증한다."""
        _require_sha256(self.byte_sha256, "inference input byte_sha256")
        _require_role(self.role, "inference input role")
        validate_content_addressed_s3_uri(self.uri, self.byte_sha256)


@dataclass(frozen=True, slots=True)
class InferenceSnapshotCounts:
    """Inference expected/actual/failed station과 row 완전성 수치다."""

    expected_station_count: int
    actual_station_count: int
    failed_station_count: int
    expected_row_count: int
    actual_row_count: int
    failed_row_count: int

    def __post_init__(self) -> None:
        """모든 count가 bool이 아닌 canonical-safe integer인지 검증한다."""
        for label, value in (
            ("expected_station_count", self.expected_station_count),
            ("actual_station_count", self.actual_station_count),
            ("failed_station_count", self.failed_station_count),
            ("expected_row_count", self.expected_row_count),
            ("actual_row_count", self.actual_row_count),
            ("failed_row_count", self.failed_row_count),
        ):
            _require_nonnegative_integer(value, f"counts.{label}")


@dataclass(frozen=True, slots=True)
class ParquetOutputRef:
    """완전 검증된 inference output Parquet의 exact identity다."""

    byte_sha256: str
    row_count: int
    uri: str

    def __post_init__(self) -> None:
        """Output row count, checksum과 content-addressed Parquet URI를 검증한다."""
        _require_sha256(self.byte_sha256, "inference output byte_sha256")
        _require_nonnegative_integer(self.row_count, "inference output row_count")
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="parquet",
        )


@dataclass(frozen=True, slots=True)
class InferenceSnapshotManifest:
    """Manifest-last로 공개하는 완전한 inference snapshot authority다."""

    schema_version: str
    logical_dttm: datetime
    revision_no: int
    status: InferenceSnapshotStatus
    producer_version: str
    serving_release: ServingReleaseRef
    serving_plan: ServingPlanRef
    rental_model_manifest: ModelManifestRef
    return_model_manifest: ModelManifestRef
    station_dependency: Dependency
    inputs: tuple[ImmutableInputRef, ...]
    expected_sta_ids: IdSetArtifactRef
    counts: InferenceSnapshotCounts
    horizon_count: int
    output: ParquetOutputRef | None

    def __post_init__(self) -> None:
        """시각을 UTC로 고정하고 모든 completeness 관계를 검증한다."""
        object.__setattr__(self, "logical_dttm", _utc_dttm(self.logical_dttm))
        validate_inference_snapshot_manifest(self)

    @property
    def canonical_bytes(self) -> bytes:
        """Manifest의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_manifest_document(self))

    @property
    def sha256(self) -> str:
        """Manifest canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


def build_model_manifest_ref(
    manifest: ModelSnapshotManifest,
    uri: str,
) -> ModelManifestRef:
    """Model snapshot manifest와 저장 URI를 pinned inference ref로 묶는다."""
    validate_model_snapshot_manifest(manifest)
    validate_content_addressed_s3_uri(
        uri,
        manifest.sha256,
        expected_extension="json",
    )
    return ModelManifestRef(
        byte_sha256=manifest.sha256,
        effective_contract_version=manifest.effective_contract_version,
        model_kind=manifest.model_kind,
        model_version=manifest.model_version,
        uri=uri,
    )


def validate_model_manifest_binding(
    reference: ModelManifestRef,
    manifest: ModelSnapshotManifest,
) -> None:
    """Pinned model ref가 읽은 manifest bytes와 metadata에 정확히 결합됐는지 검증한다."""
    if type(reference) is not ModelManifestRef:
        raise InferenceSnapshotContractError(
            "reference는 exact ModelManifestRef여야 합니다."
        )
    validate_model_snapshot_manifest(manifest)
    if (
        reference.byte_sha256 != manifest.sha256
        or reference.model_kind is not manifest.model_kind
        or reference.model_version != manifest.model_version
        or reference.effective_contract_version != manifest.effective_contract_version
    ):
        raise InferenceSnapshotContractError(
            "model manifest ref가 실제 manifest bytes/metadata와 다릅니다."
        )


def build_inference_snapshot_manifest(
    *,
    logical_dttm: datetime,
    revision_no: int,
    status: InferenceSnapshotStatus,
    producer_version: str,
    serving_release: ServingReleaseRef,
    serving_plan: ServingPlanRef,
    rental_model_manifest: ModelManifestRef,
    return_model_manifest: ModelManifestRef,
    station_dependency: Dependency,
    inputs: Iterable[ImmutableInputRef],
    expected_sta_ids: IdSetArtifactRef,
    counts: InferenceSnapshotCounts,
    horizon_count: int,
    output: ParquetOutputRef | None,
) -> InferenceSnapshotManifest:
    """Validated inputs를 canonical 순서로 묶어 v2 inference manifest를 만든다."""
    values = tuple(inputs)
    _require_instances(values, ImmutableInputRef, "inference input")
    ordered = tuple(
        sorted(
            values,
            key=lambda item: (_utf8_key(item.role), _utf8_key(item.uri)),
        )
    )
    return InferenceSnapshotManifest(
        schema_version=INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=status,
        producer_version=producer_version,
        serving_release=serving_release,
        serving_plan=serving_plan,
        rental_model_manifest=rental_model_manifest,
        return_model_manifest=return_model_manifest,
        station_dependency=station_dependency,
        inputs=ordered,
        expected_sta_ids=expected_sta_ids,
        counts=counts,
        horizon_count=horizon_count,
        output=output,
    )


def canonicalize_inference_output_table(
    table: pa.Table | pd.DataFrame,
    *,
    logical_dttm: datetime,
    expected_sta_ids: IdSet,
) -> pa.Table:
    """Producer row를 Gold가 소비하는 13-column authority로 정규화한다.

    Extra producer metadata column은 authority에서 제외하고, station×12
    완전성과 KST target time을 검증한 뒤 station UTF-8 byte·horizon
    순으로 정렬한다.
    """
    if type(expected_sta_ids) is not IdSet:
        raise InferenceSnapshotContractError(
            "expected_sta_ids는 exact IdSet이어야 합니다."
        )
    base_dttm = _utc_dttm(logical_dttm)
    if base_dttm.second != 0 or base_dttm.microsecond != 0:
        raise InferenceSnapshotContractError(
            "logical_dttm은 date/hour/minute로 표현 가능한 분 경계여야 합니다."
        )
    arrow_table = _to_arrow_table(table)
    column_names = tuple(arrow_table.column_names)
    if len(set(column_names)) != len(column_names):
        raise InferenceSnapshotContractError(
            "inference output column name은 중복될 수 없습니다."
        )
    missing = tuple(
        name for name in INFERENCE_OUTPUT_COLUMN_NAMES if name not in column_names
    )
    if missing:
        raise InferenceSnapshotContractError(
            f"inference output에 필수 column이 없습니다: {missing}"
        )

    selected = arrow_table.select(INFERENCE_OUTPUT_COLUMN_NAMES)
    null_columns = tuple(
        name
        for name, column in zip(selected.column_names, selected.columns, strict=True)
        if column.null_count
    )
    if null_columns:
        raise InferenceSnapshotContractError(
            f"inference output 필수 column은 non-null이어야 합니다: {null_columns}"
        )
    records: list[dict[str, str | date | int | float]] = []
    observed_keys: set[tuple[str, int]] = set()
    horizons_by_station: dict[str, set[int]] = {}
    expected_ids = set(expected_sta_ids.ids)
    kst_base = base_dttm.astimezone(KST)
    for index, raw in enumerate(selected.to_pylist()):
        station_id = _require_nonblank_nfc(
            raw["station_id"],
            f"inference output row {index} station_id",
        )
        horizon = _require_output_integer(
            raw["horizon"],
            f"inference output row {index} horizon",
            minimum=1,
            maximum=INFERENCE_HORIZON_COUNT,
        )
        key = (station_id, horizon)
        if key in observed_keys:
            raise InferenceSnapshotContractError(
                f"inference output에 중복 station/horizon이 있습니다: {key}"
            )
        observed_keys.add(key)
        horizons_by_station.setdefault(station_id, set()).add(horizon)

        row_date = _require_output_date(
            raw["date"],
            f"inference output row {index} date",
        )
        hour = _require_output_integer(
            raw["hour"],
            f"inference output row {index} hour",
            minimum=0,
            maximum=23,
        )
        minute = _require_output_integer(
            raw["minute"],
            f"inference output row {index} minute",
            minimum=0,
            maximum=59,
        )
        expected_target = kst_base + timedelta(hours=horizon - 1)
        if (
            row_date != expected_target.date()
            or hour != expected_target.hour
            or minute != expected_target.minute
        ):
            raise InferenceSnapshotContractError(
                "inference output date/hour/minute는 KST "
                "logical_dttm + (horizon - 1)시간이어야 합니다."
            )

        records.append(
            {
                "station_id": station_id,
                "date": row_date,
                "hour": hour,
                "minute": minute,
                "horizon": horizon,
                "rental_pred_mean": _require_prediction(
                    raw["rental_pred_mean"],
                    f"inference output row {index} rental_pred_mean",
                ),
                "rental_pred_p10": _require_quantile_prediction(
                    raw["rental_pred_p10"],
                    f"inference output row {index} rental_pred_p10",
                ),
                "rental_pred_p50": _require_quantile_prediction(
                    raw["rental_pred_p50"],
                    f"inference output row {index} rental_pred_p50",
                ),
                "rental_pred_p90": _require_quantile_prediction(
                    raw["rental_pred_p90"],
                    f"inference output row {index} rental_pred_p90",
                ),
                "return_pred_mean": _require_prediction(
                    raw["return_pred_mean"],
                    f"inference output row {index} return_pred_mean",
                ),
                "return_pred_p10": _require_quantile_prediction(
                    raw["return_pred_p10"],
                    f"inference output row {index} return_pred_p10",
                ),
                "return_pred_p50": _require_quantile_prediction(
                    raw["return_pred_p50"],
                    f"inference output row {index} return_pred_p50",
                ),
                "return_pred_p90": _require_quantile_prediction(
                    raw["return_pred_p90"],
                    f"inference output row {index} return_pred_p90",
                ),
            }
        )

    actual_ids = set(horizons_by_station)
    if actual_ids != expected_ids:
        raise InferenceSnapshotContractError(
            "inference output station 집합이 expected ID set과 다릅니다: "
            f"missing={sorted(expected_ids - actual_ids)}, "
            f"extra={sorted(actual_ids - expected_ids)}"
        )
    expected_horizons = set(range(1, INFERENCE_HORIZON_COUNT + 1))
    incomplete = sorted(
        station_id
        for station_id, horizons in horizons_by_station.items()
        if horizons != expected_horizons
    )
    if incomplete:
        raise InferenceSnapshotContractError(
            "모든 expected station은 horizon 1..12를 정확히 가져야 합니다: "
            f"{incomplete}"
        )

    records.sort(
        key=lambda record: (
            cast(str, record["station_id"]).encode("utf-8"),
            cast(int, record["horizon"]),
        )
    )
    canonical = pa.Table.from_pylist(
        records,
        schema=INFERENCE_OUTPUT_ARROW_SCHEMA,
    )
    _require_exact_output_schema(canonical)
    return canonical


def serialize_inference_output_parquet(table: pa.Table) -> bytes:
    """Exact 13-column authority table을 고정 writer option의 Parquet bytes로 만든다."""
    _require_exact_output_schema(table)
    _require_canonical_output_order(table)
    sink = pa.BufferOutputStream()
    pq.write_table(
        table.combine_chunks(),
        sink,
        compression="zstd",
        data_page_version="2.0",
        use_dictionary=False,
        version="2.6",
        write_statistics=True,
    )
    return sink.getvalue().to_pybytes()


def _canonicalize_legacy_inference_output_table(
    table: pa.Table,
    *,
    logical_dttm: datetime,
    expected_sta_ids: IdSet,
) -> pa.Table:
    """v1 mean-only table을 current 공통 불변식으로 검증해 canonicalize한다."""
    _require_exact_legacy_output_schema(table)
    augmented_rows = []
    for row in table.to_pylist():
        augmented_rows.append(
            {
                **row,
                "rental_pred_p10": row["rental_pred_mean"],
                "rental_pred_p50": row["rental_pred_mean"],
                "rental_pred_p90": row["rental_pred_mean"],
                "return_pred_p10": row["return_pred_mean"],
                "return_pred_p50": row["return_pred_mean"],
                "return_pred_p90": row["return_pred_mean"],
            }
        )
    current = canonicalize_inference_output_table(
        pa.Table.from_pylist(augmented_rows),
        logical_dttm=logical_dttm,
        expected_sta_ids=expected_sta_ids,
    )
    legacy = current.select(LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES)
    _require_exact_legacy_output_schema(legacy)
    return legacy


def parse_inference_output_parquet(
    payload: bytes,
    *,
    logical_dttm: datetime,
    expected_sta_ids: IdSet,
    schema_version: str = INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
) -> pa.Table:
    """v1/v2 Parquet bytes를 schema·KST target·completeness 검증 후 읽는다."""
    if type(payload) is not bytes:
        raise InferenceSnapshotContractError(
            "inference output Parquet payload는 exact bytes여야 합니다."
        )
    try:
        table = pq.read_table(io.BytesIO(payload))
    except (OSError, ValueError, pa.ArrowException) as exc:
        raise InferenceSnapshotContractError(
            "inference output Parquet bytes를 읽을 수 없습니다."
        ) from exc
    _require_supported_inference_schema_version(schema_version)
    if schema_version == LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION:
        canonical = _canonicalize_legacy_inference_output_table(
            table,
            logical_dttm=logical_dttm,
            expected_sta_ids=expected_sta_ids,
        )
    else:
        _require_exact_output_schema(table)
        canonical = canonicalize_inference_output_table(
            table,
            logical_dttm=logical_dttm,
            expected_sta_ids=expected_sta_ids,
        )
    if not table.equals(canonical, check_metadata=True):
        raise InferenceSnapshotContractError(
            "inference output Parquet row는 canonical station/horizon 순서여야 합니다."
        )
    return canonical


def parse_inference_snapshot_manifest(payload: bytes) -> InferenceSnapshotManifest:
    """Canonical bytes를 exact-key v1/v2 inference manifest로 파싱한다."""
    document = _require_exact_object(
        parse_canonical_json(payload),
        _MANIFEST_KEYS,
        "inference snapshot manifest",
    )
    status_text = _require_string(document["status"], "status")
    try:
        status = InferenceSnapshotStatus(status_text)
    except ValueError as exc:
        raise InferenceSnapshotContractError(
            "status는 succeeded 또는 empty여야 합니다."
        ) from exc

    input_values = _require_array(document["inputs"], "inputs")
    output_value = document["output"]
    return InferenceSnapshotManifest(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        logical_dttm=parse_utc_dttm(
            _require_string(document["logical_dttm"], "logical_dttm")
        ),
        revision_no=_require_nonnegative_integer(
            document["revision_no"], "revision_no"
        ),
        status=status,
        producer_version=_require_string(
            document["producer_version"], "producer_version"
        ),
        serving_release=_parse_serving_release(document["serving_release"]),
        serving_plan=_parse_serving_plan_ref(document["serving_plan"]),
        rental_model_manifest=_parse_model_ref(
            document["rental_model_manifest"], "rental_model_manifest"
        ),
        return_model_manifest=_parse_model_ref(
            document["return_model_manifest"], "return_model_manifest"
        ),
        station_dependency=_parse_dependency(document["station_dependency"]),
        inputs=tuple(_parse_input_ref(value) for value in input_values),
        expected_sta_ids=_parse_id_set_ref(document["expected_sta_ids"]),
        counts=_parse_counts(document["counts"]),
        horizon_count=_require_nonnegative_integer(
            document["horizon_count"], "horizon_count"
        ),
        output=None if output_value is None else _parse_output_ref(output_value),
    )


def validate_inference_snapshot_manifest(
    manifest: InferenceSnapshotManifest,
) -> None:
    """Inference manifest의 pinned provenance와 완전성 불변식을 검증한다."""
    if type(manifest) is not InferenceSnapshotManifest:
        raise InferenceSnapshotContractError(
            "manifest는 exact InferenceSnapshotManifest여야 합니다."
        )
    _require_supported_inference_schema_version(manifest.schema_version)
    _utc_dttm(manifest.logical_dttm)
    _require_nonnegative_integer(manifest.revision_no, "revision_no")
    if type(manifest.status) is not InferenceSnapshotStatus:
        raise InferenceSnapshotContractError(
            "status는 exact InferenceSnapshotStatus여야 합니다."
        )
    _require_nonblank_nfc(manifest.producer_version, "producer_version")
    _require_exact_instance(
        manifest.serving_release,
        ServingReleaseRef,
        "serving_release",
    )
    _require_exact_instance(
        manifest.serving_plan,
        ServingPlanRef,
        "serving_plan",
    )
    _require_exact_instance(
        manifest.rental_model_manifest,
        ModelManifestRef,
        "rental_model_manifest",
    )
    _require_exact_instance(
        manifest.return_model_manifest,
        ModelManifestRef,
        "return_model_manifest",
    )
    if manifest.rental_model_manifest.model_kind is not ModelKind.RENTAL:
        raise InferenceSnapshotContractError(
            "rental_model_manifest는 rental model이어야 합니다."
        )
    if manifest.return_model_manifest.model_kind is not ModelKind.RETURN:
        raise InferenceSnapshotContractError(
            "return_model_manifest는 return model이어야 합니다."
        )
    if (
        manifest.rental_model_manifest.effective_contract_version
        != manifest.serving_release.effective_contract_version
        or manifest.return_model_manifest.effective_contract_version
        != manifest.serving_release.effective_contract_version
    ):
        raise InferenceSnapshotContractError(
            "두 model과 serving release의 effective contract가 같아야 합니다."
        )
    if manifest.rental_model_manifest.uri == manifest.return_model_manifest.uri:
        raise InferenceSnapshotContractError(
            "rental과 return model manifest URI는 달라야 합니다."
        )

    _require_exact_instance(
        manifest.station_dependency,
        Dependency,
        "station_dependency",
    )
    if manifest.station_dependency.publication_key != "station":
        raise InferenceSnapshotContractError(
            "station_dependency publication_key는 station이어야 합니다."
        )
    if manifest.station_dependency.logical_dttm > manifest.logical_dttm:
        raise InferenceSnapshotContractError(
            "station_dependency는 inference logical_dttm보다 미래일 수 없습니다."
        )

    if type(manifest.inputs) is not tuple:
        raise InferenceSnapshotContractError("inputs는 tuple이어야 합니다.")
    _require_instances(manifest.inputs, ImmutableInputRef, "inference input")
    input_roles = tuple(item.role for item in manifest.inputs)
    input_uris = tuple(item.uri for item in manifest.inputs)
    input_keys = tuple(
        (item.role.encode("utf-8"), item.uri.encode("utf-8"))
        for item in manifest.inputs
    )
    if input_keys != tuple(sorted(input_keys)):
        raise InferenceSnapshotContractError(
            "inputs는 (role, uri) UTF-8 순으로 정렬되어야 합니다."
        )
    if len(set(input_roles)) != len(input_roles):
        raise InferenceSnapshotContractError(
            "inference input role은 중복될 수 없고 각 의미당 한 번만 나와야 합니다."
        )
    if len(set(input_uris)) != len(input_uris):
        raise InferenceSnapshotContractError(
            "inference input URI는 여러 role이 공유할 수 없습니다."
        )
    if manifest.serving_plan.uri in input_uris:
        raise InferenceSnapshotContractError(
            "serving plan explicit ref는 generic input에 중복될 수 없습니다."
        )

    _require_exact_instance(
        manifest.expected_sta_ids,
        IdSetArtifactRef,
        "expected_sta_ids",
    )
    _require_exact_instance(manifest.counts, InferenceSnapshotCounts, "counts")
    if (
        type(manifest.horizon_count) is not int
        or manifest.horizon_count != INFERENCE_HORIZON_COUNT
    ):
        raise InferenceSnapshotContractError(
            f"horizon_count는 exact integer {INFERENCE_HORIZON_COUNT}여야 합니다."
        )
    if manifest.expected_sta_ids.id_count != manifest.counts.expected_station_count:
        raise InferenceSnapshotContractError(
            "expected ID set count와 expected_station_count가 다릅니다."
        )
    expected_rows = manifest.counts.expected_station_count * manifest.horizon_count
    actual_rows = manifest.counts.actual_station_count * manifest.horizon_count
    if manifest.counts.expected_row_count != expected_rows:
        raise InferenceSnapshotContractError(
            "expected_row_count는 expected station × horizon이어야 합니다."
        )
    if manifest.counts.actual_row_count != actual_rows:
        raise InferenceSnapshotContractError(
            "actual_row_count는 actual station × horizon이어야 합니다."
        )
    if (
        manifest.counts.failed_station_count != 0
        or manifest.counts.failed_row_count != 0
    ):
        raise InferenceSnapshotContractError(
            "Authority inference manifest는 failed count가 모두 0이어야 합니다."
        )

    if manifest.status is InferenceSnapshotStatus.SUCCEEDED:
        _validate_succeeded(manifest)
    else:
        _validate_empty(manifest)
    canonical_json_bytes(_manifest_document(manifest))


def inference_output_input_artifact(
    manifest: InferenceSnapshotManifest,
    uri: str,
) -> InputArtifact:
    """Success manifest bytes 자체를 Gold ``inference_output`` input으로 만든다."""
    validate_inference_snapshot_manifest(manifest)
    validate_content_addressed_s3_uri(
        uri,
        manifest.sha256,
        expected_extension="json",
    )
    return InputArtifact(
        byte_sha256=manifest.sha256,
        role="inference_output",
        uri=uri,
    )


def _validate_succeeded(manifest: InferenceSnapshotManifest) -> None:
    """SUCCEEDED가 nonempty exact projection과 Parquet을 소유하게 한다."""
    counts = manifest.counts
    if counts.expected_station_count == 0:
        raise InferenceSnapshotContractError(
            "SUCCEEDED inference는 한 개 이상의 expected station이 필요합니다."
        )
    if (
        counts.actual_station_count != counts.expected_station_count
        or counts.actual_row_count != counts.expected_row_count
    ):
        raise InferenceSnapshotContractError(
            "SUCCEEDED inference의 actual과 expected count가 같아야 합니다."
        )
    if not manifest.inputs:
        raise InferenceSnapshotContractError(
            "SUCCEEDED inference는 하나 이상의 immutable input이 필요합니다."
        )
    if type(manifest.output) is not ParquetOutputRef:
        raise InferenceSnapshotContractError(
            "SUCCEEDED inference는 exact ParquetOutputRef가 필요합니다."
        )
    if manifest.output.row_count != counts.actual_row_count:
        raise InferenceSnapshotContractError(
            "Output row_count와 actual_row_count가 다릅니다."
        )


def _validate_empty(manifest: InferenceSnapshotManifest) -> None:
    """EMPTY가 0 기대 집합과 무산출물만 표현하게 한다."""
    counts = manifest.counts
    if counts != InferenceSnapshotCounts(0, 0, 0, 0, 0, 0):
        raise InferenceSnapshotContractError(
            "EMPTY inference의 station/row count는 모두 0이어야 합니다."
        )
    if manifest.output is not None:
        raise InferenceSnapshotContractError(
            "EMPTY inference는 output Parquet을 가질 수 없습니다."
        )


def _manifest_document(
    manifest: InferenceSnapshotManifest,
) -> dict[str, JsonValue]:
    """Typed inference manifest를 exact canonical JSON object로 바꾼다."""
    return {
        "counts": {
            "actual_row_count": manifest.counts.actual_row_count,
            "actual_station_count": manifest.counts.actual_station_count,
            "expected_row_count": manifest.counts.expected_row_count,
            "expected_station_count": manifest.counts.expected_station_count,
            "failed_row_count": manifest.counts.failed_row_count,
            "failed_station_count": manifest.counts.failed_station_count,
        },
        "expected_sta_ids": _id_set_ref_document(manifest.expected_sta_ids),
        "horizon_count": manifest.horizon_count,
        "inputs": [_input_ref_document(value) for value in manifest.inputs],
        "logical_dttm": format_utc_dttm(manifest.logical_dttm),
        "output": (
            None if manifest.output is None else _output_ref_document(manifest.output)
        ),
        "producer_version": manifest.producer_version,
        "rental_model_manifest": _model_ref_document(manifest.rental_model_manifest),
        "return_model_manifest": _model_ref_document(manifest.return_model_manifest),
        "revision_no": manifest.revision_no,
        "schema_version": manifest.schema_version,
        "serving_release": _serving_release_document(manifest.serving_release),
        "serving_plan": _serving_plan_ref_document(manifest.serving_plan),
        "station_dependency": _dependency_document(manifest.station_dependency),
        "status": manifest.status.value,
    }


def _serving_release_document(value: ServingReleaseRef) -> dict[str, JsonValue]:
    """Serving release ref를 canonical JSON object로 바꾼다."""
    return {
        "byte_sha256": value.byte_sha256,
        "effective_contract_version": value.effective_contract_version,
        "release_version": value.release_version,
        "uri": value.uri,
    }


def _model_ref_document(value: ModelManifestRef) -> dict[str, JsonValue]:
    """Model manifest ref를 canonical JSON object로 바꾼다."""
    return {
        "byte_sha256": value.byte_sha256,
        "effective_contract_version": value.effective_contract_version,
        "model_kind": value.model_kind.value,
        "model_version": value.model_version,
        "uri": value.uri,
    }


def _serving_plan_ref_document(value: ServingPlanRef) -> dict[str, JsonValue]:
    """Serving plan ref를 canonical JSON object로 바꾼다."""
    return {"byte_sha256": value.byte_sha256, "uri": value.uri}


def _input_ref_document(value: ImmutableInputRef) -> dict[str, JsonValue]:
    """Generic inference input ref를 canonical JSON object로 바꾼다."""
    return {"byte_sha256": value.byte_sha256, "role": value.role, "uri": value.uri}


def _id_set_ref_document(value: IdSetArtifactRef) -> dict[str, JsonValue]:
    """Expected ID set ref를 canonical JSON object로 바꾼다."""
    return {
        "byte_sha256": value.byte_sha256,
        "id_count": value.id_count,
        "schema_version": value.schema_version,
        "uri": value.uri,
    }


def _output_ref_document(value: ParquetOutputRef) -> dict[str, JsonValue]:
    """Output ref를 canonical JSON object로 바꾼다."""
    return {
        "byte_sha256": value.byte_sha256,
        "row_count": value.row_count,
        "uri": value.uri,
    }


def _dependency_document(value: Dependency) -> dict[str, JsonValue]:
    """Gold station dependency를 기존 6-tuple 문서로 바꾼다."""
    return {
        "artifact_set_sha256": value.artifact_set_sha256,
        "input_fingerprint_sha256": value.input_fingerprint_sha256,
        "logical_dttm": format_utc_dttm(value.logical_dttm),
        "manifest_uri": value.manifest_uri,
        "publication_key": value.publication_key,
        "revision_no": value.revision_no,
    }


def _parse_serving_release(value: JsonValue) -> ServingReleaseRef:
    """JSON object를 exact ServingReleaseRef로 파싱한다."""
    document = _require_exact_object(value, _DOCUMENT_REF_KEYS, "serving_release")
    return ServingReleaseRef(
        byte_sha256=_require_string(
            document["byte_sha256"], "serving_release.byte_sha256"
        ),
        effective_contract_version=_require_string(
            document["effective_contract_version"],
            "serving_release.effective_contract_version",
        ),
        release_version=_require_string(
            document["release_version"], "serving_release.release_version"
        ),
        uri=_require_string(document["uri"], "serving_release.uri"),
    )


def _parse_model_ref(value: JsonValue, label: str) -> ModelManifestRef:
    """JSON object를 exact ModelManifestRef로 파싱한다."""
    document = _require_exact_object(value, _MODEL_REF_KEYS, label)
    kind_text = _require_string(document["model_kind"], f"{label}.model_kind")
    try:
        kind = ModelKind(kind_text)
    except ValueError as exc:
        raise InferenceSnapshotContractError(
            f"{label}.model_kind가 유효하지 않습니다."
        ) from exc
    return ModelManifestRef(
        byte_sha256=_require_string(document["byte_sha256"], f"{label}.byte_sha256"),
        effective_contract_version=_require_string(
            document["effective_contract_version"],
            f"{label}.effective_contract_version",
        ),
        model_kind=kind,
        model_version=_require_string(
            document["model_version"], f"{label}.model_version"
        ),
        uri=_require_string(document["uri"], f"{label}.uri"),
    )


def _parse_serving_plan_ref(value: JsonValue) -> ServingPlanRef:
    """JSON object를 exact ServingPlanRef로 파싱한다."""
    document = _require_exact_object(
        value,
        _SERVING_PLAN_REF_KEYS,
        "serving_plan",
    )
    return ServingPlanRef(
        byte_sha256=_require_string(
            document["byte_sha256"],
            "serving_plan.byte_sha256",
        ),
        uri=_require_string(document["uri"], "serving_plan.uri"),
    )


def _parse_input_ref(value: JsonValue) -> ImmutableInputRef:
    """JSON object를 exact ImmutableInputRef로 파싱한다."""
    document = _require_exact_object(value, _INPUT_REF_KEYS, "inference input")
    return ImmutableInputRef(
        byte_sha256=_require_string(
            document["byte_sha256"], "inference input byte_sha256"
        ),
        role=_require_string(document["role"], "inference input role"),
        uri=_require_string(document["uri"], "inference input URI"),
    )


def _parse_id_set_ref(value: JsonValue) -> IdSetArtifactRef:
    """JSON object를 exact expected IdSetArtifactRef로 파싱한다."""
    document = _require_exact_object(value, _ID_SET_REF_KEYS, "expected_sta_ids")
    return IdSetArtifactRef(
        byte_sha256=_require_string(
            document["byte_sha256"], "expected_sta_ids.byte_sha256"
        ),
        id_count=_require_nonnegative_integer(
            document["id_count"], "expected_sta_ids.id_count"
        ),
        schema_version=_require_string(
            document["schema_version"], "expected_sta_ids.schema_version"
        ),
        uri=_require_string(document["uri"], "expected_sta_ids.uri"),
    )


def _parse_counts(value: JsonValue) -> InferenceSnapshotCounts:
    """JSON object를 exact InferenceSnapshotCounts로 파싱한다."""
    document = _require_exact_object(value, _COUNTS_KEYS, "counts")
    return InferenceSnapshotCounts(
        expected_station_count=_require_nonnegative_integer(
            document["expected_station_count"], "counts.expected_station_count"
        ),
        actual_station_count=_require_nonnegative_integer(
            document["actual_station_count"], "counts.actual_station_count"
        ),
        failed_station_count=_require_nonnegative_integer(
            document["failed_station_count"], "counts.failed_station_count"
        ),
        expected_row_count=_require_nonnegative_integer(
            document["expected_row_count"], "counts.expected_row_count"
        ),
        actual_row_count=_require_nonnegative_integer(
            document["actual_row_count"], "counts.actual_row_count"
        ),
        failed_row_count=_require_nonnegative_integer(
            document["failed_row_count"], "counts.failed_row_count"
        ),
    )


def _parse_output_ref(value: JsonValue) -> ParquetOutputRef:
    """JSON object를 exact ParquetOutputRef로 파싱한다."""
    document = _require_exact_object(value, _OUTPUT_REF_KEYS, "output")
    return ParquetOutputRef(
        byte_sha256=_require_string(document["byte_sha256"], "output.byte_sha256"),
        row_count=_require_nonnegative_integer(
            document["row_count"], "output.row_count"
        ),
        uri=_require_string(document["uri"], "output.uri"),
    )


def _parse_dependency(value: JsonValue) -> Dependency:
    """JSON object를 exact Gold station Dependency로 파싱한다."""
    document = _require_exact_object(value, _DEPENDENCY_KEYS, "station_dependency")
    return Dependency(
        artifact_set_sha256=_require_string(
            document["artifact_set_sha256"],
            "station_dependency.artifact_set_sha256",
        ),
        input_fingerprint_sha256=_require_string(
            document["input_fingerprint_sha256"],
            "station_dependency.input_fingerprint_sha256",
        ),
        logical_dttm=parse_utc_dttm(
            _require_string(document["logical_dttm"], "station_dependency.logical_dttm")
        ),
        manifest_uri=_require_string(
            document["manifest_uri"], "station_dependency.manifest_uri"
        ),
        publication_key=_require_string(
            document["publication_key"], "station_dependency.publication_key"
        ),
        revision_no=_require_nonnegative_integer(
            document["revision_no"], "station_dependency.revision_no"
        ),
    )


def _to_arrow_table(value: pa.Table | pd.DataFrame) -> pa.Table:
    """Exact Arrow Table 또는 pandas DataFrame을 Arrow Table로 바꾼다."""
    if type(value) is pa.Table:
        return value
    if type(value) is pd.DataFrame:
        try:
            return pa.Table.from_pandas(value, preserve_index=False)
        except (TypeError, ValueError, pa.ArrowException) as exc:
            raise InferenceSnapshotContractError(
                "inference output DataFrame을 Arrow Table로 바꿀 수 없습니다."
            ) from exc
    raise InferenceSnapshotContractError(
        "inference output은 exact pyarrow.Table 또는 pandas.DataFrame이어야 합니다."
    )


def _require_exact_output_schema(table: pa.Table) -> None:
    """Arrow table이 metadata 없는 exact 13-column non-null schema인지 확인한다."""
    if type(table) is not pa.Table:
        raise InferenceSnapshotContractError(
            "inference output authority는 exact pyarrow.Table이어야 합니다."
        )
    if not table.schema.equals(
        INFERENCE_OUTPUT_ARROW_SCHEMA,
        check_metadata=True,
    ):
        raise InferenceSnapshotContractError(
            "inference output Arrow schema가 exact 13-column contract와 다릅니다."
        )
    if any(column.null_count for column in table.columns):
        raise InferenceSnapshotContractError(
            "inference output authority column은 null을 가질 수 없습니다."
        )


def _require_exact_legacy_output_schema(table: pa.Table) -> None:
    """Arrow table이 metadata 없는 exact v1 7-column schema인지 확인한다."""
    if type(table) is not pa.Table:
        raise InferenceSnapshotContractError(
            "legacy inference output authority는 exact pyarrow.Table이어야 합니다."
        )
    if not table.schema.equals(
        LEGACY_INFERENCE_OUTPUT_ARROW_SCHEMA,
        check_metadata=True,
    ):
        raise InferenceSnapshotContractError(
            "legacy inference output Arrow schema가 exact 7-column contract와 다릅니다."
        )
    if any(column.null_count for column in table.columns):
        raise InferenceSnapshotContractError(
            "legacy inference output authority column은 null을 가질 수 없습니다."
        )


def _require_supported_inference_schema_version(value: object) -> str:
    """Inference manifest/output schema version이 v1 또는 v2인지 확인한다."""
    if (
        type(value) is not str
        or value not in _SUPPORTED_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSIONS
    ):
        raise InferenceSnapshotContractError(
            "inference snapshot schema_version은 v1 또는 v2여야 합니다."
        )
    return value


def _require_canonical_output_order(table: pa.Table) -> None:
    """Authority table의 row 타입, station/horizon 집합과 순서를 검증한다."""
    keys: list[tuple[bytes, int]] = []
    horizons_by_station: dict[str, set[int]] = {}
    for index, raw in enumerate(table.to_pylist()):
        station_id = _require_nonblank_nfc(
            raw["station_id"],
            f"inference output row {index} station_id",
        )
        _require_output_date(raw["date"], f"inference output row {index} date")
        _require_output_integer(
            raw["hour"],
            f"inference output row {index} hour",
            minimum=0,
            maximum=23,
        )
        _require_output_integer(
            raw["minute"],
            f"inference output row {index} minute",
            minimum=0,
            maximum=59,
        )
        horizon = _require_output_integer(
            raw["horizon"],
            f"inference output row {index} horizon",
            minimum=1,
            maximum=INFERENCE_HORIZON_COUNT,
        )
        _require_prediction(
            raw["rental_pred_mean"],
            f"inference output row {index} rental_pred_mean",
        )
        for name in (
            "rental_pred_p10",
            "rental_pred_p50",
            "rental_pred_p90",
            "return_pred_p10",
            "return_pred_p50",
            "return_pred_p90",
        ):
            _require_quantile_prediction(
                raw[name],
                f"inference output row {index} {name}",
            )
        _require_prediction(
            raw["return_pred_mean"],
            f"inference output row {index} return_pred_mean",
        )
        keys.append((station_id.encode("utf-8"), horizon))
        horizons_by_station.setdefault(station_id, set()).add(horizon)

    if len(set(keys)) != len(keys) or keys != sorted(keys):
        raise InferenceSnapshotContractError(
            "inference output row는 unique station_id/horizon UTF-8 순서여야 합니다."
        )
    expected_horizons = set(range(1, INFERENCE_HORIZON_COUNT + 1))
    if any(horizons != expected_horizons for horizons in horizons_by_station.values()):
        raise InferenceSnapshotContractError(
            "inference output의 각 station은 horizon 1..12를 정확히 가져야 합니다."
        )


def _require_output_date(value: Any, label: str) -> date:
    """Date32로 표현할 exact date 또는 ISO date string을 읽는다."""
    if type(value) is date:
        return value
    if type(value) is str:
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise InferenceSnapshotContractError(
                f"{label}은 YYYY-MM-DD여야 합니다."
            ) from exc
        if parsed.isoformat() == value:
            return parsed
    raise InferenceSnapshotContractError(
        f"{label}은 exact date 또는 YYYY-MM-DD 문자열이어야 합니다."
    )


def _require_output_integer(
    value: Any,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    """Output scalar가 bool이 아닌 exact builtin bounded integer인지 확인한다."""
    if type(value) is not int or not minimum <= value <= maximum:
        raise InferenceSnapshotContractError(
            f"{label}은 {minimum}..{maximum} exact integer여야 합니다."
        )
    return value


def _require_prediction(value: Any, label: str) -> float:
    """Prediction scalar를 finite nonnegative float64로 정규화한다."""
    if type(value) not in {int, float} or type(value) is bool:
        raise InferenceSnapshotContractError(
            f"{label}은 float64-compatible 숫자여야 합니다."
        )
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise InferenceSnapshotContractError(
            f"{label}은 finite nonnegative float64여야 합니다."
        )
    return 0.0 if normalized == 0 else normalized


def _require_quantile_prediction(value: Any, label: str) -> float:
    """Quantile scalar를 부호와 순서를 바꾸지 않고 finite float64로 정규화한다."""
    if type(value) not in {int, float} or type(value) is bool:
        raise InferenceSnapshotContractError(
            f"{label}은 float64-compatible 숫자여야 합니다."
        )
    normalized = float(value)
    if not math.isfinite(normalized):
        raise InferenceSnapshotContractError(f"{label}은 finite float64여야 합니다.")
    return 0.0 if normalized == 0 else normalized


def _require_exact_object(
    value: JsonValue,
    expected_keys: frozenset[str],
    label: str,
) -> dict[str, JsonValue]:
    """JSON 값이 exact builtin object와 key 집합인지 확인한다."""
    if type(value) is not dict:
        raise InferenceSnapshotContractError(f"{label}는 JSON object여야 합니다.")
    document = cast(dict[str, JsonValue], value)
    actual_keys = frozenset(document)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys.difference(actual_keys))
        extra = sorted(actual_keys.difference(expected_keys))
        raise InferenceSnapshotContractError(
            f"{label} key가 정확하지 않습니다: missing={missing}, extra={extra}"
        )
    return document


def _require_array(value: JsonValue, label: str) -> list[JsonValue]:
    """JSON 값이 exact builtin array인지 확인한다."""
    if type(value) is not list:
        raise InferenceSnapshotContractError(f"{label}는 JSON array여야 합니다.")
    return cast(list[JsonValue], value)


def _require_string(value: JsonValue, label: str) -> str:
    """JSON 값이 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise InferenceSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    return _require_nfc(value, label)


def _require_nonblank_nfc(value: Any, label: str) -> str:
    """값이 공백·제어 문자 없는 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise InferenceSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    normalized = _require_nfc(value, label)
    if (
        not normalized
        or normalized != normalized.strip()
        or any(
            ord(character) < 0x20 or ord(character) == 0x7F for character in normalized
        )
    ):
        raise InferenceSnapshotContractError(
            f"{label}은 공백·제어 문자 없는 NFC 문자열이어야 합니다."
        )
    return normalized


def _require_nfc(value: str, label: str) -> str:
    """문자열이 surrogate·noncharacter 없는 NFC인지 확인한다."""
    if type(value) is not str:
        raise InferenceSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    if unicodedata.normalize("NFC", value) != value:
        raise InferenceSnapshotContractError(f"{label}은 Unicode NFC여야 합니다.")
    for character in value:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise InferenceSnapshotContractError(
                f"{label}에 Unicode surrogate를 쓸 수 없습니다."
            )
        if 0xFDD0 <= code_point <= 0xFDEF or code_point & 0xFFFF in {
            0xFFFE,
            0xFFFF,
        }:
            raise InferenceSnapshotContractError(
                f"{label}에 Unicode noncharacter를 쓸 수 없습니다."
            )
    return value


def _require_exact_string(value: Any, expected: str, label: str) -> str:
    """값이 exact builtin string이며 contract 고정값과 같은지 확인한다."""
    if type(value) is not str or value != expected:
        raise InferenceSnapshotContractError(
            f"{label}은 정확히 {expected!r}이어야 합니다."
        )
    return value


def _require_sha256(value: Any, label: str) -> str:
    """값이 exact lowercase SHA-256 string인지 확인한다."""
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise InferenceSnapshotContractError(
            f"{label}은 정확히 64자리 lowercase hex여야 합니다."
        )
    return value


def _require_content_version(value: Any, label: str) -> str:
    """Version이 ``sha256:<lowercase digest>`` exact string인지 확인한다."""
    if type(value) is not str or _CONTENT_VERSION_PATTERN.fullmatch(value) is None:
        raise InferenceSnapshotContractError(
            f"{label}은 sha256:<64 lowercase hex> 형식이어야 합니다."
        )
    return value


def _require_nonnegative_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 canonical-safe nonnegative builtin integer인지 확인한다."""
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        raise InferenceSnapshotContractError(
            f"{label}은 0 이상 {_MAX_SAFE_INTEGER} 이하 integer여야 합니다."
        )
    return value


def _require_role(value: Any, label: str) -> str:
    """Role이 lowercase snake_case exact builtin string인지 확인한다."""
    if type(value) is not str or _ROLE_PATTERN.fullmatch(value) is None:
        raise InferenceSnapshotContractError(
            f"{label}은 lowercase snake_case여야 합니다."
        )
    return value


def _require_exact_instance(value: Any, expected_type: type[Any], label: str) -> None:
    """값이 subclass가 아닌 exact dataclass instance인지 확인한다."""
    if type(value) is not expected_type:
        raise InferenceSnapshotContractError(
            f"{label}은 exact {expected_type.__name__}이어야 합니다."
        )


def _require_instances(
    values: tuple[Any, ...],
    expected_type: type[Any],
    label: str,
) -> None:
    """Tuple 원소가 subclass가 아닌 exact dataclass인지 확인한다."""
    if any(type(value) is not expected_type for value in values):
        raise InferenceSnapshotContractError(
            f"모든 {label} 값은 exact {expected_type.__name__}이어야 합니다."
        )


def _utf8_key(value: str) -> bytes:
    """Contract 배열 정렬에 쓰는 NFC string의 UTF-8 bytes를 반환한다."""
    return _require_nonblank_nfc(value, "sort key").encode("utf-8")


def _utc_dttm(value: datetime) -> datetime:
    """Exact aware datetime을 UTC instant로 정규화한다."""
    try:
        format_utc_dttm(value)
    except (ContractViolation, TypeError, ValueError) as exc:
        raise InferenceSnapshotContractError(
            "logical_dttm은 timezone-aware exact datetime이어야 합니다."
        ) from exc
    return value.astimezone(UTC)
