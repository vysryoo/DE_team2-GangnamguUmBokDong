"""Inference success manifest의 provenance와 completeness byte 계약을 검증한다."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core.gold_publication.canonical import canonical_json_bytes
from core.gold_publication.contract import Dependency, build_id_set
from core.gold_publication.errors import CanonicalParseError
from core.inference_snapshot import (
    INFERENCE_HORIZON_COUNT,
    INFERENCE_OUTPUT_ARROW_SCHEMA,
    INFERENCE_OUTPUT_COLUMN_NAMES,
    INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    LEGACY_INFERENCE_OUTPUT_ARROW_SCHEMA,
    LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES,
    LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    ImmutableInputRef,
    InferenceSnapshotContractError,
    InferenceSnapshotCounts,
    InferenceSnapshotManifest,
    InferenceSnapshotStatus,
    ModelManifestRef,
    ParquetOutputRef,
    ServingPlanRef,
    ServingReleaseRef,
    build_inference_snapshot_manifest,
    build_model_manifest_ref,
    canonicalize_inference_output_table,
    inference_output_input_artifact,
    parse_inference_output_parquet,
    parse_inference_snapshot_manifest,
    serialize_inference_output_parquet,
    validate_model_manifest_binding,
)
from core.model_snapshot import (
    MODEL_ARTIFACT_ROLES,
    IdSetArtifactRef,
    ModelArtifact,
    ModelKind,
    ModelSnapshotContractError,
    ModelSnapshotManifest,
    build_model_snapshot_manifest,
)
from core.kst import KST

LOGICAL_DTTM = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
RELEASE_SHA256 = "a" * 64
RENTAL_SHA256 = "b" * 64
RETURN_SHA256 = "c" * 64
EXPECTED_IDS_SHA256 = "d" * 64
POPULATION_SHA256 = "e" * 64
WEATHER_SHA256 = "f" * 64
OUTPUT_SHA256 = "0" * 64
SERVING_PLAN_SHA256 = "9" * 64
EFFECTIVE_CONTRACT_VERSION = "sha256:" + "6" * 64
RENTAL_MODEL_VERSION = "sha256:" + "1" * 64
RETURN_MODEL_VERSION = "sha256:" + "2" * 64
RELEASE_VERSION = "sha256:" + "3" * 64


def _serving_release(**overrides: object) -> ServingReleaseRef:
    """테스트용 pinned serving release ref를 만든다."""
    fields: dict[str, object] = {
        "byte_sha256": RELEASE_SHA256,
        "effective_contract_version": EFFECTIVE_CONTRACT_VERSION,
        "release_version": RELEASE_VERSION,
        "uri": f"s3://fixture/releases/sha256={RELEASE_SHA256}.json",
    }
    fields.update(overrides)
    return ServingReleaseRef(**fields)  # type: ignore[arg-type]


def _model_ref(kind: ModelKind, **overrides: object) -> ModelManifestRef:
    """테스트용 rental 또는 return model manifest ref를 만든다."""
    checksum = RENTAL_SHA256 if kind is ModelKind.RENTAL else RETURN_SHA256
    fields: dict[str, object] = {
        "byte_sha256": checksum,
        "effective_contract_version": EFFECTIVE_CONTRACT_VERSION,
        "model_kind": kind,
        "model_version": (
            RENTAL_MODEL_VERSION if kind is ModelKind.RENTAL else RETURN_MODEL_VERSION
        ),
        "uri": f"s3://fixture/model-manifests/{kind.value}/sha256={checksum}.json",
    }
    fields.update(overrides)
    return ModelManifestRef(**fields)  # type: ignore[arg-type]


def _serving_plan(**overrides: object) -> ServingPlanRef:
    """테스트용 exact serving plan artifact ref를 만든다."""
    fields: dict[str, object] = {
        "byte_sha256": SERVING_PLAN_SHA256,
        "uri": (f"s3://fixture/serving-plans/sha256={SERVING_PLAN_SHA256}.json"),
    }
    fields.update(overrides)
    return ServingPlanRef(**fields)  # type: ignore[arg-type]


def _station_dependency(**overrides: object) -> Dependency:
    """테스트용 Gold station publication dependency tuple을 만든다."""
    fields: dict[str, object] = {
        "artifact_set_sha256": "1" * 64,
        "input_fingerprint_sha256": "2" * 64,
        "logical_dttm": LOGICAL_DTTM - timedelta(minutes=5),
        "manifest_uri": "s3://fixture/gold/station/revision=0000000000.json",
        "publication_key": "station",
        "revision_no": 0,
    }
    fields.update(overrides)
    return Dependency(**fields)  # type: ignore[arg-type]


def _expected_ids(id_count: int = 2) -> IdSetArtifactRef:
    """테스트용 expected Gold station ID-set ref를 만든다."""
    return IdSetArtifactRef(
        byte_sha256=EXPECTED_IDS_SHA256,
        id_count=id_count,
        schema_version="gold-id-set-v1",
        uri=f"s3://fixture/inference/expected/sha256={EXPECTED_IDS_SHA256}.json",
    )


def _inputs() -> tuple[ImmutableInputRef, ...]:
    """Builder 정렬도 검증하도록 역순의 generic input tuple을 반환한다."""
    return (
        ImmutableInputRef(
            WEATHER_SHA256,
            "weather_snapshot",
            f"s3://fixture/inference/weather/sha256={WEATHER_SHA256}.parquet",
        ),
        ImmutableInputRef(
            POPULATION_SHA256,
            "population_snapshot",
            f"s3://fixture/inference/population/sha256={POPULATION_SHA256}.parquet",
        ),
    )


def _counts(stations: int = 2) -> InferenceSnapshotCounts:
    """Station마다 12개 horizon이 완성된 success count를 만든다."""
    rows = stations * INFERENCE_HORIZON_COUNT
    return InferenceSnapshotCounts(stations, stations, 0, rows, rows, 0)


def _output(row_count: int = 24) -> ParquetOutputRef:
    """테스트용 content-addressed inference Parquet ref를 만든다."""
    return ParquetOutputRef(
        OUTPUT_SHA256,
        row_count,
        f"s3://fixture/inference/output/sha256={OUTPUT_SHA256}.parquet",
    )


def _producer_output_frame(
    *,
    logical_dttm: datetime = LOGICAL_DTTM,
) -> pd.DataFrame:
    """Extra audit column과 역순 row를 가진 현재 producer 형태를 만든다."""
    rows = []
    kst_base = logical_dttm.astimezone(KST)
    for station_id in ("ST-2", "ST-1"):
        for horizon in reversed(range(1, INFERENCE_HORIZON_COUNT + 1)):
            target = kst_base + timedelta(hours=horizon - 1)
            rows.append(
                {
                    "station_id": station_id,
                    "date": target.date().isoformat(),
                    "hour": target.hour,
                    "minute": target.minute,
                    "horizon": horizon,
                    "rental_pred_mean": horizon + 0.25,
                    "rental_pred_p10": -0.25,
                    "rental_pred_p50": horizon + 0.5,
                    "rental_pred_p90": horizon + 0.25,
                    "return_pred_mean": horizon + 0.75,
                    "return_pred_p10": -0.75,
                    "return_pred_p50": horizon + 0.5,
                    "return_pred_p90": horizon + 1.0,
                    "lag_data_freshness": 1.0,
                }
            )
    return pd.DataFrame(rows)


def _succeeded(**overrides: object) -> InferenceSnapshotManifest:
    """테스트용 정상 SUCCEEDED inference manifest를 만든다."""
    fields: dict[str, object] = {
        "logical_dttm": LOGICAL_DTTM,
        "revision_no": 0,
        "status": InferenceSnapshotStatus.SUCCEEDED,
        "producer_version": "inference-producer-v1",
        "serving_release": _serving_release(),
        "serving_plan": _serving_plan(),
        "rental_model_manifest": _model_ref(ModelKind.RENTAL),
        "return_model_manifest": _model_ref(ModelKind.RETURN),
        "station_dependency": _station_dependency(),
        "inputs": _inputs(),
        "expected_sta_ids": _expected_ids(),
        "counts": _counts(),
        "horizon_count": INFERENCE_HORIZON_COUNT,
        "output": _output(),
    }
    fields.update(overrides)
    return build_inference_snapshot_manifest(**fields)  # type: ignore[arg-type]


def _empty(**overrides: object) -> InferenceSnapshotManifest:
    """테스트용 정상 EMPTY inference manifest를 만든다."""
    fields: dict[str, object] = {
        "logical_dttm": LOGICAL_DTTM,
        "revision_no": 0,
        "status": InferenceSnapshotStatus.EMPTY,
        "producer_version": "inference-producer-v1",
        "serving_release": _serving_release(),
        "serving_plan": _serving_plan(),
        "rental_model_manifest": _model_ref(ModelKind.RENTAL),
        "return_model_manifest": _model_ref(ModelKind.RETURN),
        "station_dependency": _station_dependency(),
        "inputs": (),
        "expected_sta_ids": _expected_ids(0),
        "counts": InferenceSnapshotCounts(0, 0, 0, 0, 0, 0),
        "horizon_count": INFERENCE_HORIZON_COUNT,
        "output": None,
    }
    fields.update(overrides)
    return build_inference_snapshot_manifest(**fields)  # type: ignore[arg-type]


def test_inference_manifest_has_stable_exact_canonical_bytes() -> None:
    """Pinned refs, counts, UTC와 output을 포함한 manifest bytes를 golden 고정한다."""
    manifest = _succeeded()

    assert manifest.canonical_bytes == (
        b'{"counts":{"actual_row_count":24,"actual_station_count":2,'
        b'"expected_row_count":24,"expected_station_count":2,'
        b'"failed_row_count":0,"failed_station_count":0},'
        b'"expected_sta_ids":{"byte_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",'
        b'"id_count":2,"schema_version":"gold-id-set-v1",'
        b'"uri":"s3://fixture/inference/expected/'
        b'sha256=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.json"},'
        b'"horizon_count":12,"inputs":['
        b'{"byte_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",'
        b'"role":"population_snapshot","uri":"s3://fixture/inference/population/'
        b'sha256=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee.parquet"},'
        b'{"byte_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",'
        b'"role":"weather_snapshot","uri":"s3://fixture/inference/weather/'
        b'sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.parquet"}],'
        b'"logical_dttm":"2026-08-20T01:00:00.000000Z",'
        b'"output":{"byte_sha256":"0000000000000000000000000000000000000000000000000000000000000000",'
        b'"row_count":24,"uri":"s3://fixture/inference/output/'
        b'sha256=0000000000000000000000000000000000000000000000000000000000000000.parquet"},'
        b'"producer_version":"inference-producer-v1",'
        b'"rental_model_manifest":{"byte_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
        b'"effective_contract_version":"sha256:'
        b'6666666666666666666666666666666666666666666666666666666666666666",'
        b'"model_kind":"rental","model_version":"sha256:'
        b'1111111111111111111111111111111111111111111111111111111111111111",'
        b'"uri":"s3://fixture/model-manifests/rental/'
        b'sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"},'
        b'"return_model_manifest":{"byte_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
        b'"effective_contract_version":"sha256:'
        b'6666666666666666666666666666666666666666666666666666666666666666",'
        b'"model_kind":"return","model_version":"sha256:'
        b'2222222222222222222222222222222222222222222222222222222222222222",'
        b'"uri":"s3://fixture/model-manifests/return/'
        b'sha256=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.json"},'
        b'"revision_no":0,"schema_version":"ml-inference-snapshot-manifest-v2",'
        b'"serving_plan":{"byte_sha256":"9999999999999999999999999999999999999999999999999999999999999999",'
        b'"uri":"s3://fixture/serving-plans/'
        b'sha256=9999999999999999999999999999999999999999999999999999999999999999.json"},'
        b'"serving_release":{"byte_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"effective_contract_version":"sha256:'
        b'6666666666666666666666666666666666666666666666666666666666666666",'
        b'"release_version":"sha256:'
        b'3333333333333333333333333333333333333333333333333333333333333333",'
        b'"uri":"s3://fixture/releases/'
        b'sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"},'
        b'"station_dependency":{"artifact_set_sha256":"1111111111111111111111111111111111111111111111111111111111111111",'
        b'"input_fingerprint_sha256":"2222222222222222222222222222222222222222222222222222222222222222",'
        b'"logical_dttm":"2026-08-20T00:55:00.000000Z",'
        b'"manifest_uri":"s3://fixture/gold/station/revision=0000000000.json",'
        b'"publication_key":"station","revision_no":0},"status":"succeeded"}'
    )
    assert parse_inference_snapshot_manifest(manifest.canonical_bytes) == manifest


def test_empty_manifest_round_trips_without_output() -> None:
    """Expected set이 0으로 증명된 EMPTY는 output 없이 authority가 된다."""
    manifest = _empty()

    assert parse_inference_snapshot_manifest(manifest.canonical_bytes) == manifest
    assert manifest.output is None
    assert manifest.expected_sta_ids.id_count == 0


def test_builder_sorts_generic_inputs_by_role_and_uri() -> None:
    """Generic input refs가 caller 순서가 아니라 contract 순서로 직렬화된다."""
    manifest = _succeeded()

    assert tuple(value.role for value in manifest.inputs) == (
        "population_snapshot",
        "weather_snapshot",
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"horizon_count": 11}, "horizon_count"),
        (
            {
                "counts": InferenceSnapshotCounts(2, 2, 0, 23, 24, 0),
            },
            "expected_row_count",
        ),
        (
            {
                "counts": InferenceSnapshotCounts(2, 1, 0, 24, 12, 0),
            },
            "actual과 expected",
        ),
        (
            {
                "counts": InferenceSnapshotCounts(2, 2, 1, 24, 24, 12),
            },
            "failed count",
        ),
        ({"expected_sta_ids": _expected_ids(3)}, "ID set count"),
        ({"output": None}, "ParquetOutputRef"),
        ({"output": _output(23)}, "Output row_count"),
        ({"inputs": ()}, "immutable input"),
    ],
)
def test_succeeded_rejects_partial_or_unbound_projection(
    overrides: dict[str, object],
    message: str,
) -> None:
    """SUCCEEDED가 partial, 잘못된 horizon/count, 무입력·무산출물을 받지 않는다."""
    with pytest.raises(InferenceSnapshotContractError, match=message):
        _succeeded(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"expected_sta_ids": _expected_ids(1)},
        {"counts": InferenceSnapshotCounts(1, 1, 0, 12, 12, 0)},
        {"output": _output(0)},
    ],
)
def test_empty_rejects_nonzero_evidence_or_output(
    overrides: dict[str, object],
) -> None:
    """EMPTY는 0 expected ID/count와 no-output 조합만 허용한다."""
    with pytest.raises(InferenceSnapshotContractError):
        _empty(**overrides)


def test_count_types_reject_bool_and_unsafe_integer() -> None:
    """Bool과 canonical JSON safe 범위 밖 count가 integer를 가장하지 못한다."""
    with pytest.raises(InferenceSnapshotContractError):
        InferenceSnapshotCounts(True, 0, 0, 0, 0, 0)  # type: ignore[arg-type]
    with pytest.raises(InferenceSnapshotContractError):
        InferenceSnapshotCounts(2**53, 0, 0, 0, 0, 0)


def test_model_pair_must_match_kind_and_release_contract() -> None:
    """Rental/return kind 또는 effective contract가 섞인 pair를 거부한다."""
    with pytest.raises(InferenceSnapshotContractError, match="rental model"):
        _succeeded(rental_model_manifest=_model_ref(ModelKind.RETURN))
    with pytest.raises(InferenceSnapshotContractError, match="effective contract"):
        _succeeded(
            return_model_manifest=_model_ref(
                ModelKind.RETURN,
                effective_contract_version="sha256:" + "7" * 64,
            )
        )
    with pytest.raises(InferenceSnapshotContractError, match="URI는 달라야"):
        _succeeded(
            return_model_manifest=_model_ref(
                ModelKind.RETURN,
                byte_sha256=RENTAL_SHA256,
                uri=f"s3://fixture/model-manifests/rental/sha256={RENTAL_SHA256}.json",
            )
        )


def test_station_dependency_is_exact_station_tuple_not_future() -> None:
    """다른 Gold key나 inference 이후 topology state를 provenance로 쓰지 않는다."""
    with pytest.raises(InferenceSnapshotContractError, match="publication_key"):
        _succeeded(
            station_dependency=_station_dependency(publication_key="weather_grid")
        )
    with pytest.raises(InferenceSnapshotContractError, match="미래"):
        _succeeded(
            station_dependency=_station_dependency(
                logical_dttm=LOGICAL_DTTM + timedelta(microseconds=1)
            )
        )


def test_direct_typed_manifest_rejects_unsorted_duplicate_inputs() -> None:
    """Direct constructor는 generic input ref 순서와 중복을 fail closed한다."""
    manifest = _succeeded()
    with pytest.raises(InferenceSnapshotContractError, match="정렬"):
        replace(manifest, inputs=tuple(reversed(manifest.inputs)))
    with pytest.raises(InferenceSnapshotContractError, match="중복"):
        replace(manifest, inputs=(manifest.inputs[0], manifest.inputs[0]))

    first, second = manifest.inputs
    duplicate_role = ImmutableInputRef(
        second.byte_sha256,
        first.role,
        second.uri,
    )
    with pytest.raises(InferenceSnapshotContractError, match="role은 중복"):
        replace(manifest, inputs=(first, duplicate_role))

    duplicate_uri = ImmutableInputRef(
        first.byte_sha256,
        second.role,
        first.uri,
    )
    with pytest.raises(InferenceSnapshotContractError, match="URI는 여러 role"):
        replace(manifest, inputs=(first, duplicate_uri))

    duplicate_plan = ImmutableInputRef(
        manifest.serving_plan.byte_sha256,
        "serving_plan_duplicate",
        manifest.serving_plan.uri,
    )
    with pytest.raises(InferenceSnapshotContractError, match="explicit ref"):
        replace(
            manifest,
            inputs=tuple(
                sorted(
                    (*manifest.inputs, duplicate_plan),
                    key=lambda value: (value.role.encode(), value.uri.encode()),
                )
            ),
        )


def test_all_provenance_versions_are_content_derived_sha256_strings() -> None:
    """Release, model과 effective contract version이 semantic label로 되돌아가지 않는다."""
    with pytest.raises(InferenceSnapshotContractError, match="sha256"):
        _serving_release(release_version="release-v1")
    with pytest.raises(InferenceSnapshotContractError, match="sha256"):
        _serving_release(effective_contract_version="contract-v1")
    with pytest.raises(InferenceSnapshotContractError, match="sha256"):
        _model_ref(ModelKind.RENTAL, model_version="rental-v1")


def test_parser_rejects_unknown_nested_field_and_noncanonical_bytes() -> None:
    """Parser가 nested schema 확장과 공백 JSON을 fail closed한다."""
    payload = _succeeded().canonical_bytes
    document = json.loads(payload)
    document["counts"]["extra"] = 1

    with pytest.raises(InferenceSnapshotContractError, match="extra"):
        parse_inference_snapshot_manifest(canonical_json_bytes(document))
    with pytest.raises(CanonicalParseError):
        parse_inference_snapshot_manifest(payload.replace(b'":', b'": ', 1))


def test_exact_builtin_and_dataclass_types_reject_subclasses() -> None:
    """String, tuple, input dataclass subclass가 typed manifest를 우회하지 못한다."""

    class StringSubclass(str):
        """Exact string 검증용 subclass다."""

    class InputSubclass(ImmutableInputRef):
        """Exact input ref 검증용 subclass다."""

    with pytest.raises(InferenceSnapshotContractError):
        _succeeded(producer_version=StringSubclass("producer-v1"))
    with pytest.raises(InferenceSnapshotContractError, match="tuple"):
        replace(_succeeded(), inputs=list(_succeeded().inputs))  # type: ignore[arg-type]

    original = _succeeded().inputs[0]
    subclass = InputSubclass(original.byte_sha256, original.role, original.uri)
    with pytest.raises(InferenceSnapshotContractError, match="exact ImmutableInputRef"):
        replace(_succeeded(), inputs=(subclass, _succeeded().inputs[1]))


def _actual_model_manifest(kind: ModelKind, digit: str) -> ModelSnapshotManifest:
    """Model binding helper 검증용 실제 canonical model manifest를 만든다."""
    artifacts = []
    for index, role in enumerate(MODEL_ARTIFACT_ROLES):
        checksum = f"{(int(digit, 16) + index) % 16:x}" * 64
        extension = "txt" if role.startswith("booster_") else "json"
        artifacts.append(
            ModelArtifact(
                checksum,
                role,
                f"s3://fixture/models/{kind.value}/{role}/sha256={checksum}.{extension}",
            )
        )
    support_checksum = "9" * 64 if kind is ModelKind.RENTAL else "8" * 64
    return build_model_snapshot_manifest(
        model_kind=kind,
        effective_contract_version=(
            "sha256:"
            + artifacts[MODEL_ARTIFACT_ROLES.index("effective_profile")].byte_sha256
        ),
        artifacts=tuple(artifacts),
        support_sta_ids=IdSetArtifactRef(
            support_checksum,
            2,
            "gold-id-set-v1",
            f"s3://fixture/support/{kind.value}/sha256={support_checksum}.json",
        ),
    )


def test_model_manifest_ref_binds_actual_canonical_bytes_and_metadata() -> None:
    """Inference model ref가 manifest SHA/kind/version/contract를 함께 고정한다."""
    model = _actual_model_manifest(ModelKind.RENTAL, "1")
    uri = f"s3://fixture/model-manifests/sha256={model.sha256}.json"
    reference = build_model_manifest_ref(model, uri)

    validate_model_manifest_binding(reference, model)
    assert reference.byte_sha256 == model.sha256
    assert reference.model_kind is ModelKind.RENTAL

    with pytest.raises(InferenceSnapshotContractError, match="다릅니다"):
        validate_model_manifest_binding(
            replace(reference, model_version="sha256:" + "0" * 64),
            model,
        )


def test_model_manifest_ref_requires_content_addressed_manifest_uri() -> None:
    """실제 model manifest를 mutable current URI로 pin하지 못한다."""
    model = _actual_model_manifest(ModelKind.RENTAL, "1")

    with pytest.raises(ModelSnapshotContractError, match="content-addressed"):
        build_model_manifest_ref(model, "s3://fixture/model-manifests/current.json")


def test_manifest_bytes_become_gold_inference_output_not_raw_parquet() -> None:
    """Gold inference_output role이 output Parquet이 아니라 success manifest를 hash한다."""
    manifest = _succeeded()
    uri = f"s3://fixture/inference/manifests/sha256={manifest.sha256}.json"

    artifact = inference_output_input_artifact(manifest, uri)

    assert artifact.role == "inference_output"
    assert artifact.byte_sha256 == manifest.sha256
    assert artifact.byte_sha256 != manifest.output.byte_sha256  # type: ignore[union-attr]
    assert artifact.uri == uri


def test_output_table_canonicalizes_exact_columns_and_round_trips() -> None:
    """Producer extra metadata를 제외하고 exact Arrow authority를 Parquet으로 읽는다."""
    expected_ids = build_id_set(("ST-2", "ST-1"))

    table = canonicalize_inference_output_table(
        _producer_output_frame(),
        logical_dttm=LOGICAL_DTTM,
        expected_sta_ids=expected_ids,
    )
    payload = serialize_inference_output_parquet(table)
    parsed = parse_inference_output_parquet(
        payload,
        logical_dttm=LOGICAL_DTTM,
        expected_sta_ids=expected_ids,
    )

    assert table.schema.equals(INFERENCE_OUTPUT_ARROW_SCHEMA, check_metadata=True)
    assert tuple(table.column_names) == INFERENCE_OUTPUT_COLUMN_NAMES
    assert table.num_rows == 24
    assert table.column("station_id").to_pylist()[:12] == ["ST-1"] * 12
    assert table.column("horizon").to_pylist()[:12] == list(range(1, 13))
    assert table.column("rental_pred_p10").to_pylist()[0] == -0.25
    assert (
        table.column("rental_pred_p50").to_pylist()[0]
        > table.column("rental_pred_p90").to_pylist()[0]
    )
    assert parsed.equals(table, check_metadata=True)


def test_v1_manifest_and_mean_only_output_remain_dual_readable() -> None:
    """기존 v1 manifest와 7-column output을 mean-only authority로 읽는다."""
    current = _succeeded()
    legacy_payload = current.canonical_bytes.replace(
        INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION.encode(),
        LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION.encode(),
    )
    legacy_manifest = parse_inference_snapshot_manifest(legacy_payload)
    expected_ids = build_id_set(("ST-1", "ST-2"))
    current_table = canonicalize_inference_output_table(
        _producer_output_frame(),
        logical_dttm=LOGICAL_DTTM,
        expected_sta_ids=expected_ids,
    )
    legacy_table = current_table.select(LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES)
    buffer = BytesIO()
    pq.write_table(legacy_table, buffer)

    parsed = parse_inference_output_parquet(
        buffer.getvalue(),
        logical_dttm=LOGICAL_DTTM,
        expected_sta_ids=expected_ids,
        schema_version=LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    )

    assert legacy_manifest.schema_version == (
        LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION
    )
    assert parsed.schema.equals(
        LEGACY_INFERENCE_OUTPUT_ARROW_SCHEMA,
        check_metadata=True,
    )
    assert tuple(parsed.column_names) == LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES


def test_output_table_uses_kst_target_across_date_boundary() -> None:
    """Arrow authority의 date/hour/minute가 UTC가 아닌 KST base+h-1을 따른다."""
    logical_dttm = datetime(2026, 8, 20, 15, 30, tzinfo=UTC)
    frame = _producer_output_frame(logical_dttm=logical_dttm)

    table = canonicalize_inference_output_table(
        frame,
        logical_dttm=logical_dttm,
        expected_sta_ids=build_id_set(("ST-1", "ST-2")),
    )

    first = table.slice(0, 1).to_pylist()[0]
    assert first["date"].isoformat() == "2026-08-21"
    assert first["hour"] == 0
    assert first["minute"] == 30


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda frame: frame.iloc[:-1].copy(), "horizon 1..12"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "중복 station/horizon",
        ),
        (
            lambda frame: frame.assign(hour=frame["hour"].where(frame.index != 0, 0)),
            "KST",
        ),
        (
            lambda frame: frame.assign(
                rental_pred_mean=frame["rental_pred_mean"].where(
                    frame.index != 0,
                    float("nan"),
                )
            ),
            "non-null",
        ),
        (
            lambda frame: frame.assign(
                return_pred_mean=frame["return_pred_mean"].where(
                    frame.index != 0,
                    float("inf"),
                )
            ),
            "finite nonnegative",
        ),
    ],
)
def test_output_table_rejects_partial_duplicate_time_or_nonfinite_prediction(
    mutate: object,
    message: str,
) -> None:
    """Incomplete·duplicate·wrong-target·nonfinite row는 authority가 되지 못한다."""
    frame = mutate(_producer_output_frame())  # type: ignore[operator]

    with pytest.raises(InferenceSnapshotContractError, match=message):
        canonicalize_inference_output_table(
            frame,
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=build_id_set(("ST-1", "ST-2")),
        )


def test_output_parser_rejects_extra_schema_and_noncanonical_row_order() -> None:
    """Stored Parquet은 exact 13 columns과 canonical station/horizon order를 유지한다."""
    expected_ids = build_id_set(("ST-1", "ST-2"))
    table = canonicalize_inference_output_table(
        _producer_output_frame(),
        logical_dttm=LOGICAL_DTTM,
        expected_sta_ids=expected_ids,
    )
    with_extra = table.append_column(
        "audit",
        pa.array(["x"] * table.num_rows, type=pa.string()),
    )
    extra_buffer = BytesIO()
    pq.write_table(with_extra, extra_buffer)
    with pytest.raises(InferenceSnapshotContractError, match="schema"):
        parse_inference_output_parquet(
            extra_buffer.getvalue(),
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=expected_ids,
        )

    reversed_table = table.take(pa.array(list(reversed(range(table.num_rows)))))
    reversed_buffer = BytesIO()
    pq.write_table(reversed_table, reversed_buffer)
    with pytest.raises(InferenceSnapshotContractError, match="canonical"):
        parse_inference_output_parquet(
            reversed_buffer.getvalue(),
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=expected_ids,
        )
    with pytest.raises(InferenceSnapshotContractError, match="UTF-8"):
        serialize_inference_output_parquet(reversed_table)


def test_output_contract_rejects_missing_column_wrong_expected_ids_and_bytes_subclass() -> (
    None
):
    """Required column, expected ID authority와 exact payload type을 fail closed한다."""
    frame = _producer_output_frame()
    with pytest.raises(InferenceSnapshotContractError, match="필수 column"):
        canonicalize_inference_output_table(
            frame.drop(columns=["horizon"]),
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=build_id_set(("ST-1", "ST-2")),
        )
    with pytest.raises(InferenceSnapshotContractError, match="expected ID set"):
        canonicalize_inference_output_table(
            frame,
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=build_id_set(("ST-1",)),
        )

    class BytesSubclass(bytes):
        """Exact payload type 검증용 subclass다."""

    with pytest.raises(InferenceSnapshotContractError, match="exact bytes"):
        parse_inference_output_parquet(
            BytesSubclass(b"not parquet"),
            logical_dttm=LOGICAL_DTTM,
            expected_sta_ids=build_id_set(()),
        )


def test_schema_and_horizon_constants_are_disk_contract() -> None:
    """Inference manifest schema와 12 horizon을 회귀 고정한다."""
    assert (
        INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION
        == "ml-inference-snapshot-manifest-v2"
    )
    assert INFERENCE_HORIZON_COUNT == 12
    assert (
        LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION
        == "ml-inference-snapshot-manifest-v1"
    )
    assert INFERENCE_OUTPUT_COLUMN_NAMES == (
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
    assert INFERENCE_OUTPUT_ARROW_SCHEMA == pa.schema(
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
