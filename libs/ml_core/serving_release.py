"""Rental/return 모델 쌍을 하나로 고정하는 serving release 계약을 제공한다.

개별 champion pointer는 모델 하나 안의 booster와 부속 파일이 섞이는 문제만 막는다.
이 모듈은 두 model snapshot, station fallback profile, effective serving contract를
immutable release manifest 하나로 묶고, 검증이 모두 끝난 뒤 단일 generation
pointer를 compare-and-swap하여 모델 쌍 사이의 중간 상태도 노출하지 않는다.
"""

from __future__ import annotations

import io
import json
import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from core import s3 as s3_io
from core.gold_publication import (
    IdSet,
    ImmutableObjectStore,
    S3ImmutableObjectStore,
    canonical_json_bytes,
    parse_canonical_json,
    parse_id_set,
    sha256_hex,
)
from core.gold_publication.canonical import JsonValue
from core.gold_publication.errors import ContractViolation
from core.model_snapshot import (
    MODEL_ARTIFACT_ROLES,
    IdSetArtifactRef,
    ModelArtifact,
    ModelKind,
    ModelSnapshotManifest,
    StationCrosswalk,
    StationCrosswalkEntry,
    build_id_set_artifact_ref,
    build_model_snapshot_manifest,
    build_model_support_sta_ids,
    build_station_crosswalk,
    canonical_station_categories_bytes,
    derive_model_support_sta_ids,
    extract_serving_feature_contract_bytes,
    parse_model_snapshot_manifest,
    parse_station_categories,
    parse_station_crosswalk,
    validate_content_addressed_s3_uri,
    validate_model_effective_contract_binding,
    validate_model_snapshot_manifest,
)

from .paths import (
    model_snapshot_artifact_key,
    model_snapshot_manifest_key,
    model_support_id_set_key,
    serving_release_artifact_key,
    serving_release_manifest_key,
    serving_release_pointer_key,
)
from .profile_contract import (
    SUPPORTED_MODEL_GRID_TICK_MINUTES,
    validate_model_grid_contract,
    validate_train_anchor_contract,
)
from .serving_contract import (
    SERVING_FEATURE_PROFILE_KEYS,
    extract_serving_feature_contract,
)

SERVING_RELEASE_IDENTITY_SCHEMA_VERSION = "ml-serving-release-identity-v1"
"""Release version을 계산할 때 사용하는 versionless identity schema다."""

SERVING_RELEASE_MANIFEST_SCHEMA_VERSION = "ml-serving-release-manifest-v1"
"""Immutable pair serving release manifest schema version이다."""

SERVING_RELEASE_POINTER_SCHEMA_VERSION = "ml-serving-release-pointer-v1"
"""단일 mutable pair release pointer schema version이다."""

_MODEL_REF_KEYS = frozenset(
    {
        "byte_sha256",
        "effective_contract_version",
        "model_kind",
        "model_version",
        "uri",
    }
)
_ARTIFACT_REF_KEYS = frozenset({"byte_sha256", "uri"})
_CONTRACT_REF_KEYS = frozenset({"byte_sha256", "uri", "version"})
_RELEASE_MANIFEST_KEYS = frozenset(
    {
        "effective_contract",
        "release_version",
        "rental_model_manifest",
        "return_model_manifest",
        "schema_version",
        "station_profile",
    }
)
_POINTER_KEYS = frozenset(
    {
        "generation",
        "release_manifest_byte_sha256",
        "release_manifest_uri",
        "schema_version",
    }
)
_VERSION_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ROLE_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
_MAX_SAFE_INTEGER = 2**53 - 1
_MISSING_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_CONFLICT_CODES = frozenset(
    {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}
)
_MODEL_ARTIFACT_EXTENSION = {
    "booster_poisson": "txt",
    "booster_q10": "txt",
    "booster_q50": "txt",
    "booster_q90": "txt",
    "conformal_correction": "json",
    "effective_profile": "json",
    "metrics": "json",
    "station_categories": "json",
    "station_crosswalk": "json",
}
_STATION_PROFILE_COLUMN_NAMES = (
    "station_no",
    "minute",
    "dow",
    "month",
    "rental_mean",
    "rental_std",
    "return_mean",
    "return_std",
    "n_samples",
)
_STATION_PROFILE_KEY_COLUMNS = ("station_no", "minute", "dow", "month")
_STATION_PROFILE_STAT_COLUMNS = (
    "rental_mean",
    "rental_std",
    "return_mean",
    "return_std",
)
_GOLD_INFERENCE_HORIZON_COUNT = 12


class ServingReleaseContractError(ContractViolation):
    """Serving release typed 값이나 canonical bytes가 계약을 위반했다."""


class ServingReleasePreflightError(RuntimeError):
    """Release가 가리키는 bytes나 model/profile 결합이 검증되지 않았다."""


class ServingReleasePointerError(RuntimeError):
    """Mutable serving release pointer 입출력이 안전하게 완료되지 않았다."""


class ServingReleasePointerConflictError(ServingReleasePointerError):
    """다른 writer가 먼저 pointer generation을 변경했다."""


class CrossContractServingReleaseError(RuntimeError):
    """명시적 maintenance 승인 없이 serving contract를 바꾸려 했다."""


@dataclass(frozen=True, slots=True)
class ModelManifestRef:
    """Release가 고정하는 per-model snapshot manifest identity다."""

    byte_sha256: str
    effective_contract_version: str
    model_kind: ModelKind
    model_version: str
    uri: str

    def __post_init__(self) -> None:
        """Model metadata, checksum과 content-addressed URI를 검증한다."""
        _require_sha256(self.byte_sha256, "model manifest byte_sha256")
        _require_version(
            self.effective_contract_version,
            "model manifest effective_contract_version",
        )
        if type(self.model_kind) is not ModelKind:
            raise ServingReleaseContractError(
                "model manifest model_kind는 exact ModelKind여야 합니다."
            )
        _require_version(self.model_version, "model manifest model_version")
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="json",
        )


@dataclass(frozen=True, slots=True)
class ImmutableArtifactRef:
    """Station profile처럼 release가 직접 고정하는 immutable object identity다."""

    byte_sha256: str
    uri: str

    def __post_init__(self) -> None:
        """Checksum과 content-addressed URI를 검증한다."""
        _require_sha256(self.byte_sha256, "artifact byte_sha256")
        validate_content_addressed_s3_uri(self.uri, self.byte_sha256)


@dataclass(frozen=True, slots=True)
class EffectiveContractRef:
    """Canonical effective-serving-contract object와 derived version을 고정한다."""

    byte_sha256: str
    uri: str
    version: str

    def __post_init__(self) -> None:
        """Contract URI/SHA와 ``sha256:<SHA>`` version 결합을 검증한다."""
        digest = _require_sha256(
            self.byte_sha256,
            "effective contract byte_sha256",
        )
        validate_content_addressed_s3_uri(
            self.uri,
            digest,
            expected_extension="json",
        )
        _require_version(self.version, "effective contract version")
        if self.version != f"sha256:{digest}":
            raise ServingReleaseContractError(
                "effective contract version은 exact contract bytes SHA여야 합니다."
            )


@dataclass(frozen=True, slots=True)
class ServingReleaseManifest:
    """두 model과 필수 serving artifact를 고정하는 immutable release다."""

    schema_version: str
    release_version: str
    rental_model_manifest: ModelManifestRef
    return_model_manifest: ModelManifestRef
    station_profile: ImmutableArtifactRef
    effective_contract: EffectiveContractRef

    def __post_init__(self) -> None:
        """Exact model kind, contract equality와 derived release version을 검증한다."""
        validate_serving_release_manifest(self)

    @property
    def canonical_bytes(self) -> bytes:
        """Release manifest의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_release_manifest_document(self))

    @property
    def sha256(self) -> str:
        """Release manifest canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class ServingReleasePointer:
    """현재 release manifest 하나를 가리키는 generation pointer다."""

    schema_version: str
    generation: int
    release_manifest_byte_sha256: str
    release_manifest_uri: str

    def __post_init__(self) -> None:
        """Generation과 content-addressed release manifest ref를 검증한다."""
        _require_exact_string(
            self.schema_version,
            SERVING_RELEASE_POINTER_SCHEMA_VERSION,
            "serving release pointer schema_version",
        )
        _require_nonnegative_integer(self.generation, "pointer generation")
        _require_sha256(
            self.release_manifest_byte_sha256,
            "release manifest byte_sha256",
        )
        validate_content_addressed_s3_uri(
            self.release_manifest_uri,
            self.release_manifest_byte_sha256,
            expected_extension="json",
        )

    @property
    def canonical_bytes(self) -> bytes:
        """Pointer의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_pointer_document(self))


@dataclass(frozen=True, slots=True)
class PointerRead:
    """Pointer bytes와 CAS에 사용할 object-store version token을 함께 보관한다."""

    payload: bytes | None
    version_token: str | None

    def __post_init__(self) -> None:
        """Missing 또는 existing pointer 상태가 서로 모순되지 않게 검증한다."""
        if (self.payload is None) != (self.version_token is None):
            raise ServingReleasePointerError(
                "pointer payload와 version token은 함께 존재하거나 함께 없어야 합니다."
            )
        if self.payload is not None and type(self.payload) is not bytes:
            raise ServingReleasePointerError("pointer payload는 bytes여야 합니다.")
        if self.version_token is not None:
            _require_nonblank_nfc(self.version_token, "pointer version token")


@dataclass(frozen=True, slots=True)
class VerifiedModelArtifact:
    """SHA 검증을 통과한 model artifact ref와 exact payload bytes다."""

    reference: ModelArtifact
    payload: bytes

    def __post_init__(self) -> None:
        """Artifact ref type과 payload actual SHA를 검증한다."""
        if type(self.reference) is not ModelArtifact:
            raise ServingReleasePreflightError(
                "verified artifact reference는 exact ModelArtifact여야 합니다."
            )
        if type(self.payload) is not bytes:
            raise ServingReleasePreflightError(
                "verified artifact payload는 bytes여야 합니다."
            )
        if sha256_hex(self.payload) != self.reference.byte_sha256:
            raise ServingReleasePreflightError(
                f"verified artifact payload SHA가 ref와 다릅니다: {self.reference.role}"
            )


@dataclass(frozen=True, slots=True)
class VerifiedModelSnapshot:
    """Inference가 재조회 없이 사용할 exact model manifest와 transitive bytes다."""

    manifest: ModelSnapshotManifest
    manifest_payload: bytes
    artifacts: tuple[VerifiedModelArtifact, ...]
    support_sta_ids: IdSet
    support_sta_ids_payload: bytes

    def __post_init__(self) -> None:
        """Manifest, artifact 순서와 support bytes가 같은 snapshot인지 검증한다."""
        if type(self.manifest) is not ModelSnapshotManifest:
            raise ServingReleasePreflightError(
                "verified model manifest는 exact ModelSnapshotManifest여야 합니다."
            )
        if type(self.manifest_payload) is not bytes:
            raise ServingReleasePreflightError(
                "verified model manifest payload는 bytes여야 합니다."
            )
        if parse_model_snapshot_manifest(self.manifest_payload) != self.manifest:
            raise ServingReleasePreflightError(
                "verified model manifest payload와 typed manifest가 다릅니다."
            )
        if type(self.artifacts) is not tuple or any(
            type(artifact) is not VerifiedModelArtifact for artifact in self.artifacts
        ):
            raise ServingReleasePreflightError(
                "verified model artifacts는 exact tuple이어야 합니다."
            )
        if tuple(artifact.reference for artifact in self.artifacts) != (
            self.manifest.artifacts
        ):
            raise ServingReleasePreflightError(
                "verified model artifact 순서/ref가 manifest와 다릅니다."
            )
        if type(self.support_sta_ids) is not IdSet:
            raise ServingReleasePreflightError(
                "verified model support는 exact IdSet이어야 합니다."
            )
        if type(self.support_sta_ids_payload) is not bytes:
            raise ServingReleasePreflightError(
                "verified model support payload는 bytes여야 합니다."
            )
        if (
            parse_id_set(self.support_sta_ids_payload) != self.support_sta_ids
            or self.support_sta_ids.sha256 != self.manifest.support_sta_ids.byte_sha256
        ):
            raise ServingReleasePreflightError(
                "verified model support payload와 manifest ref가 다릅니다."
            )

    def artifact_payload(self, role: str) -> bytes:
        """검증된 snapshot에서 exact role의 payload를 재조회 없이 반환한다."""
        matches = tuple(
            artifact.payload
            for artifact in self.artifacts
            if artifact.reference.role == role
        )
        if len(matches) != 1:
            raise KeyError(
                f"verified model artifact role이 정확히 한 개가 아닙니다: {role}"
            )
        return matches[0]


@dataclass(frozen=True, slots=True)
class VerifiedStationProfile:
    """검증된 station fallback profile의 exact bytes와 bounded metadata다."""

    payload: bytes
    row_count: int
    station_nos: tuple[int, ...]
    minute_values: tuple[int, ...]
    grid_tick_minutes: int

    def __post_init__(self) -> None:
        """Retained payload와 검증 결과 metadata의 exact scalar type을 확인한다."""
        if type(self.payload) is not bytes:
            raise ServingReleasePreflightError(
                "verified station profile payload는 bytes여야 합니다."
            )
        _require_positive_integer(self.row_count, "station profile row_count")
        if type(self.station_nos) is not tuple or not self.station_nos:
            raise ServingReleasePreflightError(
                "verified station profile station_nos는 nonempty tuple이어야 합니다."
            )
        if any(type(value) is not int for value in self.station_nos):
            raise ServingReleasePreflightError(
                "verified station profile station_nos는 exact integer tuple이어야 합니다."
            )
        if type(self.minute_values) is not tuple or not self.minute_values:
            raise ServingReleasePreflightError(
                "verified station profile minute_values는 nonempty tuple이어야 합니다."
            )
        if any(type(value) is not int for value in self.minute_values):
            raise ServingReleasePreflightError(
                "verified station profile minute_values는 exact integer tuple이어야 합니다."
            )
        _require_positive_integer(
            self.grid_tick_minutes,
            "station profile grid_tick_minutes",
        )


@dataclass(frozen=True, slots=True)
class ServingReleasePreflight:
    """Release의 모든 transitive bytes를 보존한 검증 완료 model snapshot 쌍이다."""

    rental_snapshot: VerifiedModelSnapshot
    return_snapshot: VerifiedModelSnapshot
    effective_contract_payload: bytes
    station_profile: VerifiedStationProfile

    def __post_init__(self) -> None:
        """Model kind와 retained release artifact payload type을 검증한다."""
        if (
            type(self.rental_snapshot) is not VerifiedModelSnapshot
            or self.rental_snapshot.manifest.model_kind is not ModelKind.RENTAL
        ):
            raise ServingReleasePreflightError(
                "preflight rental snapshot은 exact rental snapshot이어야 합니다."
            )
        if (
            type(self.return_snapshot) is not VerifiedModelSnapshot
            or self.return_snapshot.manifest.model_kind is not ModelKind.RETURN
        ):
            raise ServingReleasePreflightError(
                "preflight return snapshot은 exact return snapshot이어야 합니다."
            )
        if type(self.effective_contract_payload) is not bytes:
            raise ServingReleasePreflightError(
                "preflight effective contract payload는 bytes여야 합니다."
            )
        if type(self.station_profile) is not VerifiedStationProfile:
            raise ServingReleasePreflightError(
                "preflight station profile은 exact VerifiedStationProfile이어야 합니다."
            )

    @property
    def rental_model(self) -> ModelSnapshotManifest:
        """검증된 rental typed manifest를 반환한다."""
        return self.rental_snapshot.manifest

    @property
    def return_model(self) -> ModelSnapshotManifest:
        """검증된 return typed manifest를 반환한다."""
        return self.return_snapshot.manifest

    @property
    def station_profile_payload(self) -> bytes:
        """검증된 station profile exact bytes를 하위 호환 이름으로 반환한다."""
        return self.station_profile.payload


@dataclass(frozen=True, slots=True)
class PinnedServingRelease:
    """한 번 읽은 pointer에서 고정한 release와 모든 exact transitive bytes다."""

    pointer: ServingReleasePointer
    pointer_payload: bytes
    manifest: ServingReleaseManifest
    manifest_payload: bytes
    preflight: ServingReleasePreflight

    def __post_init__(self) -> None:
        """Retained pointer/release/transitive bytes가 하나의 snapshot인지 검증한다."""
        if type(self.pointer) is not ServingReleasePointer:
            raise ServingReleasePreflightError(
                "pinned pointer는 exact ServingReleasePointer여야 합니다."
            )
        if (
            type(self.pointer_payload) is not bytes
            or parse_serving_release_pointer(self.pointer_payload) != self.pointer
        ):
            raise ServingReleasePreflightError(
                "pinned pointer payload와 typed pointer가 다릅니다."
            )
        if type(self.manifest) is not ServingReleaseManifest:
            raise ServingReleasePreflightError(
                "pinned manifest는 exact ServingReleaseManifest여야 합니다."
            )
        if (
            type(self.manifest_payload) is not bytes
            or parse_serving_release_manifest(self.manifest_payload) != self.manifest
        ):
            raise ServingReleasePreflightError(
                "pinned manifest payload와 typed manifest가 다릅니다."
            )
        if self.pointer.release_manifest_byte_sha256 != self.manifest.sha256:
            raise ServingReleasePreflightError(
                "pinned pointer의 release manifest SHA가 실제 manifest와 다릅니다."
            )
        if type(self.preflight) is not ServingReleasePreflight:
            raise ServingReleasePreflightError(
                "pinned preflight는 exact ServingReleasePreflight여야 합니다."
            )
        if (
            sha256_hex(self.preflight.effective_contract_payload)
            != self.manifest.effective_contract.byte_sha256
            or sha256_hex(self.preflight.station_profile_payload)
            != self.manifest.station_profile.byte_sha256
        ):
            raise ServingReleasePreflightError(
                "pinned release artifact payload SHA가 manifest ref와 다릅니다."
            )


@dataclass(frozen=True, slots=True)
class ExactServingRelease:
    """Mutable pointer 없이 exact manifest ref에서 검증한 serving release다."""

    manifest_uri: str
    manifest: ServingReleaseManifest
    manifest_payload: bytes
    preflight: ServingReleasePreflight

    def __post_init__(self) -> None:
        """Manifest URI·bytes·preflight가 동일한 release인지 검증한다."""
        if type(self.manifest) is not ServingReleaseManifest:
            raise ServingReleasePreflightError(
                "exact serving release manifest 타입이 잘못됐습니다."
            )
        if (
            type(self.manifest_payload) is not bytes
            or parse_serving_release_manifest(self.manifest_payload) != self.manifest
        ):
            raise ServingReleasePreflightError(
                "exact serving release manifest bytes가 typed manifest와 다릅니다."
            )
        validate_content_addressed_s3_uri(
            self.manifest_uri,
            self.manifest.sha256,
            expected_extension="json",
        )
        if type(self.preflight) is not ServingReleasePreflight:
            raise ServingReleasePreflightError(
                "exact serving release preflight 타입이 잘못됐습니다."
            )
        if (
            sha256_hex(self.preflight.effective_contract_payload)
            != self.manifest.effective_contract.byte_sha256
            or sha256_hex(self.preflight.station_profile_payload)
            != self.manifest.station_profile.byte_sha256
        ):
            raise ServingReleasePreflightError(
                "exact serving release artifact가 manifest ref와 다릅니다."
            )


@dataclass(frozen=True, slots=True)
class PinnedServingPlanRelease:
    """Serving plan에 필요한 pointer와 model manifest만 고정한 경량 snapshot이다."""

    pointer: ServingReleasePointer
    pointer_payload: bytes
    manifest: ServingReleaseManifest
    manifest_payload: bytes
    rental_model: ModelSnapshotManifest
    rental_model_payload: bytes
    return_model: ModelSnapshotManifest
    return_model_payload: bytes

    def __post_init__(self) -> None:
        """Retained bytes와 상위 manifest reference가 같은 snapshot인지 검증한다."""
        if (
            type(self.pointer) is not ServingReleasePointer
            or type(self.pointer_payload) is not bytes
            or parse_serving_release_pointer(self.pointer_payload) != self.pointer
        ):
            raise ServingReleasePreflightError(
                "plan용 pinned pointer payload와 typed pointer가 다릅니다."
            )
        if (
            type(self.manifest) is not ServingReleaseManifest
            or type(self.manifest_payload) is not bytes
            or parse_serving_release_manifest(self.manifest_payload) != self.manifest
            or self.pointer.release_manifest_byte_sha256 != self.manifest.sha256
        ):
            raise ServingReleasePreflightError(
                "plan용 pinned release manifest가 pointer와 다릅니다."
            )
        pairs = (
            (
                self.manifest.rental_model_manifest,
                self.rental_model,
                self.rental_model_payload,
            ),
            (
                self.manifest.return_model_manifest,
                self.return_model,
                self.return_model_payload,
            ),
        )
        for reference, model, payload in pairs:
            if (
                type(model) is not ModelSnapshotManifest
                or type(payload) is not bytes
                or parse_model_snapshot_manifest(payload) != model
                or model.sha256 != reference.byte_sha256
                or model.model_kind is not reference.model_kind
                or model.model_version != reference.model_version
                or model.effective_contract_version
                != reference.effective_contract_version
            ):
                raise ServingReleasePreflightError(
                    "plan용 pinned model manifest가 release reference와 다릅니다."
                )


@dataclass(frozen=True, slots=True)
class ExplicitImmutablePayload:
    """Caller가 출처를 명시한 immutable source bytes와 identity다."""

    payload: bytes
    byte_sha256: str
    uri: str

    def __post_init__(self) -> None:
        """Payload actual SHA와 content-addressed source URI를 검증한다."""
        if type(self.payload) is not bytes:
            raise TypeError("immutable source payload는 bytes여야 합니다.")
        digest = _require_sha256(self.byte_sha256, "immutable source byte_sha256")
        if sha256_hex(self.payload) != digest:
            raise ServingReleaseContractError(
                "immutable source payload의 actual SHA가 명시된 SHA와 다릅니다."
            )
        validate_content_addressed_s3_uri(self.uri, digest)


@dataclass(frozen=True, slots=True)
class PublishedModelSnapshot:
    """Put-once/readback을 마친 model snapshot manifest와 release ref다."""

    manifest: ModelSnapshotManifest
    manifest_ref: ModelManifestRef
    support_sta_ids: IdSet
    station_crosswalk: StationCrosswalk


class ServingReleasePointerStore(Protocol):
    """Mutable pointer가 요구하는 read와 compare-and-swap 경계다."""

    def read(self, key: str) -> PointerRead:
        """Pointer bytes와 동일 object version token을 한 번에 읽는다."""
        ...

    def compare_and_swap(
        self,
        key: str,
        expected_version_token: str | None,
        payload: bytes,
    ) -> None:
        """Expected version이 그대로일 때만 pointer bytes를 한 번 갱신한다."""
        ...


class S3PointerClient(Protocol):
    """S3 pointer CAS adapter가 사용하는 최소 boto3 client 계약이다."""

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """S3 object 본문과 ETag를 함께 읽는다."""
        ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        """If-Match 또는 If-None-Match 조건으로 pointer를 쓴다."""
        ...


class S3ServingReleasePointerStore:
    """S3 ETag 조건부 PUT으로 mutable pointer를 CAS하는 adapter다."""

    def __init__(
        self,
        client: S3PointerClient | None = None,
        bucket: str | None = None,
    ) -> None:
        """주입된 client/bucket 또는 기존 ML S3 환경 설정을 사용한다."""
        self._client = (
            client if client is not None else cast(S3PointerClient, s3_io._client())
        )
        self._bucket = bucket if bucket is not None else s3_io.bucket_name()

    def read(self, key: str) -> PointerRead:
        """단일 GET 응답에서 pointer bytes와 ETag를 함께 읽는다."""
        _require_pointer_key(key)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            code = _client_error_code(exc)
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in _MISSING_CODES or status == 404:
                return PointerRead(payload=None, version_token=None)
            raise ServingReleasePointerError(
                f"serving release pointer 읽기에 실패했습니다: key={key}, code={code}"
            ) from exc
        except BotoCoreError as exc:
            raise ServingReleasePointerError(
                f"serving release pointer 읽기에 실패했습니다: key={key}"
            ) from exc

        body: Any | None = None
        try:
            body = response["Body"]
            content_length = int(response["ContentLength"])
            payload = body.read()
            version_token = response["ETag"]
        except (BotoCoreError, KeyError, TypeError, ValueError, OSError) as exc:
            raise ServingReleasePointerError(
                f"serving release pointer 응답이 완전하지 않습니다: key={key}"
            ) from exc
        finally:
            if body is not None:
                try:
                    body.close()
                except (AttributeError, OSError, BotoCoreError):
                    pass

        if type(payload) is not bytes or len(payload) != content_length:
            raise ServingReleasePointerError(
                f"serving release pointer 응답 길이가 다릅니다: key={key}"
            )
        if type(version_token) is not str or not version_token:
            raise ServingReleasePointerError(
                f"serving release pointer ETag가 없습니다: key={key}"
            )
        return PointerRead(payload=payload, version_token=version_token)

    def compare_and_swap(
        self,
        key: str,
        expected_version_token: str | None,
        payload: bytes,
    ) -> None:
        """S3 conditional PUT으로 stale writer의 pointer overwrite를 거부한다."""
        _require_pointer_key(key)
        if type(payload) is not bytes:
            raise TypeError("pointer payload는 bytes여야 합니다.")
        kwargs: dict[str, Any] = {
            "Body": payload,
            "Bucket": self._bucket,
            "ContentType": "application/json",
            "Key": key,
        }
        if expected_version_token is None:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = _require_nonblank_nfc(
                expected_version_token,
                "expected pointer version token",
            )
        try:
            self._client.put_object(**kwargs)
        except ClientError as exc:
            code = _client_error_code(exc)
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in _CONFLICT_CODES or status in {409, 412}:
                raise ServingReleasePointerConflictError(
                    "serving release pointer가 preflight 뒤 다른 writer에 의해 변경됐습니다."
                ) from exc
            raise ServingReleasePointerError(
                f"serving release pointer 쓰기에 실패했습니다: key={key}, code={code}"
            ) from exc
        except BotoCoreError as exc:
            raise ServingReleasePointerError(
                f"serving release pointer 쓰기에 실패했습니다: key={key}"
            ) from exc


def build_effective_serving_contract(profile: Mapping[str, object]) -> bytes:
    """Full effective profile에서 serving 의미를 바꾸는 7-key canonical 문서를 만든다."""
    values = extract_serving_feature_contract(profile, source="effective profile")
    normalized: dict[str, int] = {}
    for key in SERVING_FEATURE_PROFILE_KEYS:
        value = values[key]
        if type(value) is not int or value <= 0:
            raise ServingReleaseContractError(
                f"effective serving contract {key}는 양의 integer여야 합니다."
            )
        normalized[key] = value
    _validate_effective_contract_relations(normalized)
    return canonical_json_bytes(normalized)


def parse_effective_serving_contract(payload: bytes) -> dict[str, int]:
    """Canonical serving-only contract bytes를 exact 7-key 값으로 파싱한다."""
    values = _require_exact_object(
        parse_canonical_json(payload),
        frozenset(SERVING_FEATURE_PROFILE_KEYS),
        "effective serving contract values",
    )
    result: dict[str, int] = {}
    for key in SERVING_FEATURE_PROFILE_KEYS:
        value = values[key]
        if type(value) is not int or value <= 0:
            raise ServingReleaseContractError(
                f"effective serving contract {key}는 양의 integer여야 합니다."
            )
        result[key] = value
    _validate_effective_contract_relations(result)
    return result


def _validate_effective_contract_relations(values: Mapping[str, int]) -> None:
    """Serving contract의 기존 model-grid/anchor 관계와 Gold horizon을 검증한다."""
    try:
        validate_model_grid_contract(
            values["GRID_TICK_MINUTES"],
            values["ROLLING_TICK_MINUTES"],
            values["TARGET_HORIZON_MINUTES"],
            "effective-serving-contract",
        )
        validate_train_anchor_contract(
            values["GRID_TICK_MINUTES"],
            values["TRAIN_ANCHOR_TICK_MINUTES"],
            "effective-serving-contract",
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ServingReleaseContractError(
            "effective serving contract의 grid/anchor 관계가 지원되지 않습니다."
        ) from exc
    if values["HORIZON_COUNT"] != _GOLD_INFERENCE_HORIZON_COUNT:
        raise ServingReleaseContractError(
            "effective serving contract HORIZON_COUNT는 Gold demand 계약의 exact "
            f"{_GOLD_INFERENCE_HORIZON_COUNT}여야 합니다."
        )


def effective_contract_version(payload: bytes) -> str:
    """검증된 canonical effective contract bytes의 content-derived version을 반환한다."""
    parse_effective_serving_contract(payload)
    return f"sha256:{sha256_hex(payload)}"


def build_model_manifest_ref(
    manifest: ModelSnapshotManifest,
    uri: str,
) -> ModelManifestRef:
    """Validated model snapshot manifest와 content-addressed URI를 release ref로 묶는다."""
    validate_model_snapshot_manifest(manifest)
    return ModelManifestRef(
        byte_sha256=manifest.sha256,
        effective_contract_version=manifest.effective_contract_version,
        model_kind=manifest.model_kind,
        model_version=manifest.model_version,
        uri=uri,
    )


def build_effective_contract_ref(payload: bytes, uri: str) -> EffectiveContractRef:
    """Canonical effective contract bytes와 URI를 exact release ref로 묶는다."""
    version = effective_contract_version(payload)
    return EffectiveContractRef(
        byte_sha256=sha256_hex(payload),
        uri=uri,
        version=version,
    )


def publish_release_artifact(
    payload: bytes,
    *,
    role: str,
    extension: str,
    object_store: ImmutableObjectStore | None = None,
    bucket: str | None = None,
) -> ImmutableArtifactRef:
    """Release-owned actual bytes를 content-addressed put-once/readback한다."""
    if type(payload) is not bytes:
        raise TypeError("release artifact payload는 bytes여야 합니다.")
    _require_role(role)
    _require_extension(extension)
    digest = sha256_hex(payload)
    uri = _s3_uri(
        serving_release_artifact_key(role, digest, extension),
        bucket=bucket,
    )
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    _put_once_and_readback(
        immutable,
        uri,
        payload,
        require_canonical_json=extension == "json" and role == "effective_contract",
    )
    return ImmutableArtifactRef(byte_sha256=digest, uri=uri)


def publish_effective_contract(
    effective_profile_payload: bytes,
    *,
    object_store: ImmutableObjectStore | None = None,
    bucket: str | None = None,
) -> EffectiveContractRef:
    """Full profile에서 serving-only canonical object를 만들어 immutable하게 공개한다."""
    contract_payload = extract_serving_feature_contract_bytes(effective_profile_payload)
    effective_contract_version(contract_payload)
    artifact = publish_release_artifact(
        contract_payload,
        role="effective_contract",
        extension="json",
        object_store=object_store,
        bucket=bucket,
    )
    return build_effective_contract_ref(contract_payload, artifact.uri)


def validate_station_profile_payload(
    payload: bytes,
    *,
    expected_grid_tick_minutes: int | None = None,
    station_crosswalk: StationCrosswalk | None = None,
    required_station_nos: tuple[int, ...] = (),
) -> VerifiedStationProfile:
    """Station fallback Parquet의 schema, 값과 model/crosswalk 결합을 검증한다.

    전체 station×minute×dow×month Cartesian coverage는 강제하지 않는다. 기존
    inference는 없는 조합을 NaN fallback으로 처리하므로, 여기서는 실제로 존재하는
    행의 의미와 global minute grid, 두 model이 지원하는 station의 최소 coverage만
    고정한다.

    args:
        payload: 검증할 exact Parquet bytes
        expected_grid_tick_minutes: release contract가 고정한 model grid
        station_crosswalk: rental/return model이 공유하는 exact crosswalk
        required_station_nos: 두 model category station_no의 합집합
    returns:
        exact payload와 bounded metadata를 보존한 검증 완료 snapshot
    raises:
        ServingReleasePreflightError: schema, 값, grid 또는 model 결합이 잘못됐을 때
    """
    if type(payload) is not bytes:
        raise TypeError("station profile payload는 bytes여야 합니다.")
    if type(required_station_nos) is not tuple or any(
        type(value) is not int for value in required_station_nos
    ):
        raise TypeError("required_station_nos는 exact integer tuple이어야 합니다.")
    if expected_grid_tick_minutes is not None:
        _require_positive_integer(
            expected_grid_tick_minutes,
            "expected station profile grid tick",
        )
    if (
        station_crosswalk is not None
        and type(station_crosswalk) is not StationCrosswalk
    ):
        raise TypeError("station_crosswalk는 exact StationCrosswalk여야 합니다.")
    if required_station_nos and station_crosswalk is None:
        raise ServingReleasePreflightError(
            "model category coverage 검증에는 shared station crosswalk가 필요합니다."
        )

    try:
        table = pq.read_table(io.BytesIO(payload))
    except (pa.ArrowInvalid, pa.ArrowTypeError, OSError) as exc:
        raise ServingReleasePreflightError(
            "station profile payload를 Parquet table로 읽을 수 없습니다."
        ) from exc
    if tuple(table.column_names) != _STATION_PROFILE_COLUMN_NAMES:
        raise ServingReleasePreflightError(
            "station profile column은 exact 9-column 순서여야 합니다: "
            f"expected={_STATION_PROFILE_COLUMN_NAMES}, actual={tuple(table.column_names)}"
        )
    if table.num_rows <= 0:
        raise ServingReleasePreflightError(
            "station profile은 한 개 이상의 row가 필요합니다."
        )
    null_columns = tuple(
        name
        for name, column in zip(table.column_names, table.columns, strict=True)
        if column.null_count
    )
    if null_columns:
        raise ServingReleasePreflightError(
            f"station profile column은 non-null이어야 합니다: {null_columns}"
        )

    _validate_station_profile_integer_column(table, "station_no", 0, 32767)
    _validate_station_profile_integer_column(table, "minute", 0, 1439)
    _validate_station_profile_integer_column(table, "dow", 0, 6)
    _validate_station_profile_integer_column(table, "month", 1, 12)
    _validate_station_profile_integer_column(table, "n_samples", 1, 2**31 - 1)
    for name in _STATION_PROFILE_STAT_COLUMNS:
        _validate_station_profile_stat_column(table, name)

    encoded_key = pc.cast(table.column("station_no"), pa.int64(), safe=True)
    for name, cardinality, offset in (
        ("minute", 1440, 0),
        ("dow", 7, 0),
        ("month", 12, 1),
    ):
        values = pc.cast(table.column(name), pa.int64(), safe=True)
        if offset:
            values = pc.subtract(values, offset)
        encoded_key = pc.add(pc.multiply(encoded_key, cardinality), values)
    distinct_key_count = pc.count_distinct(encoded_key).as_py()
    if distinct_key_count != table.num_rows:
        raise ServingReleasePreflightError(
            "station profile logical key(station_no,minute,dow,month)는 unique여야 합니다."
        )

    station_nos = tuple(sorted(pc.unique(table.column("station_no")).to_pylist()))
    minute_values = tuple(sorted(pc.unique(table.column("minute")).to_pylist()))
    inferred_grid = _station_profile_grid_tick(minute_values)
    if expected_grid_tick_minutes is not None:
        expected_minutes = tuple(range(0, 1440, expected_grid_tick_minutes))
        if minute_values != expected_minutes:
            raise ServingReleasePreflightError(
                "station profile global minute set이 release model grid와 다릅니다: "
                f"expected_grid={expected_grid_tick_minutes}, "
                f"observed_grid={inferred_grid}"
            )

    if station_crosswalk is not None:
        crosswalk_station_nos = {
            entry.station_no for entry in station_crosswalk.entries
        }
        unknown = tuple(
            value for value in station_nos if value not in crosswalk_station_nos
        )
        if unknown:
            raise ServingReleasePreflightError(
                "station profile station_no가 shared model crosswalk에 없습니다: "
                f"{unknown}"
            )
        profile_station_nos = set(station_nos)
        missing = tuple(
            value
            for value in sorted(set(required_station_nos))
            if value not in profile_station_nos
        )
        if missing:
            raise ServingReleasePreflightError(
                "station profile이 rental/return model category station_no를 모두 "
                f"포함하지 않습니다: {missing}"
            )

    return VerifiedStationProfile(
        payload=payload,
        row_count=table.num_rows,
        station_nos=station_nos,
        minute_values=minute_values,
        grid_tick_minutes=inferred_grid,
    )


def _validate_station_profile_integer_column(
    table: pa.Table,
    name: str,
    minimum: int,
    maximum: int,
) -> None:
    """Profile integer column의 Arrow type과 bounded value 범위를 검증한다."""
    column = table.column(name)
    if not pa.types.is_integer(column.type):
        raise ServingReleasePreflightError(
            f"station profile {name}은 integer column이어야 합니다: {column.type}"
        )
    actual_minimum = pc.min(column).as_py()
    actual_maximum = pc.max(column).as_py()
    if actual_minimum < minimum or actual_maximum > maximum:
        raise ServingReleasePreflightError(
            f"station profile {name} 범위는 {minimum}..{maximum}이어야 합니다: "
            f"min={actual_minimum}, max={actual_maximum}"
        )


def _validate_station_profile_stat_column(table: pa.Table, name: str) -> None:
    """Profile mean/std column이 nonnegative finite numeric인지 검증한다."""
    column = table.column(name)
    if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
        raise ServingReleasePreflightError(
            f"station profile {name}은 numeric column이어야 합니다: {column.type}"
        )
    values = pc.cast(column, pa.float64(), safe=True)
    if pc.any(pc.invert(pc.is_finite(values))).as_py():
        raise ServingReleasePreflightError(
            f"station profile {name}은 finite number만 가져야 합니다."
        )
    actual_minimum = pc.min(values).as_py()
    if not math.isfinite(actual_minimum) or actual_minimum < 0:
        raise ServingReleasePreflightError(
            f"station profile {name}은 nonnegative여야 합니다: min={actual_minimum}"
        )


def _station_profile_grid_tick(minute_values: tuple[int, ...]) -> int:
    """Global minute set이 자정부터 시작하는 exact uniform grid인지 검증한다."""
    if not minute_values or minute_values[0] != 0:
        raise ServingReleasePreflightError(
            "station profile global minute set은 0분부터 시작해야 합니다."
        )
    grid_tick = 1440 if len(minute_values) == 1 else minute_values[1] - minute_values[0]
    expected = tuple(range(0, 1440, grid_tick))
    if grid_tick not in SUPPORTED_MODEL_GRID_TICK_MINUTES or minute_values != expected:
        raise ServingReleasePreflightError(
            "station profile global minute set은 supported model grid의 exact set이어야 "
            f"합니다: observed={minute_values[:10]}, inferred_grid={grid_tick}"
        )
    return grid_tick


def publish_station_profile(
    station_profile_payload: bytes,
    *,
    object_store: ImmutableObjectStore | None = None,
    bucket: str | None = None,
) -> ImmutableArtifactRef:
    """Station fallback profile actual Parquet bytes를 immutable release artifact로 만든다."""
    validate_station_profile_payload(station_profile_payload)
    return publish_release_artifact(
        station_profile_payload,
        role="station_profile",
        extension="parquet",
        object_store=object_store,
        bucket=bucket,
    )


def publish_model_snapshot(
    *,
    model_kind: ModelKind,
    artifact_payloads: Mapping[str, bytes],
    station_source: ExplicitImmutablePayload,
    object_store: ImmutableObjectStore | None = None,
    bucket: str | None = None,
) -> PublishedModelSnapshot:
    """Legacy archive bytes와 explicit station source를 immutable model snapshot으로 만든다.

    ``station_source``는 feature build가 실제 사용한 station master Parquet 또는 이미
    canonical한 station crosswalk여야 한다. Mutable default path를 여기서 다시 읽거나
    ``station_no``를 문자열 규칙으로 ``sta_id``로 추정하지 않는다.
    """
    if type(model_kind) is not ModelKind:
        raise ServingReleaseContractError("model_kind는 exact ModelKind여야 합니다.")
    if type(station_source) is not ExplicitImmutablePayload:
        raise ServingReleaseContractError(
            "station_source는 exact ExplicitImmutablePayload여야 합니다."
        )
    if not isinstance(artifact_payloads, Mapping):
        raise ServingReleaseContractError("artifact_payloads는 mapping이어야 합니다.")
    expected_roles = set(MODEL_ARTIFACT_ROLES).difference({"station_crosswalk"})
    actual_roles = set(artifact_payloads)
    if actual_roles != expected_roles:
        raise ServingReleaseContractError(
            "model artifact payload role이 정확하지 않습니다: "
            f"missing={sorted(expected_roles - actual_roles)}, "
            f"extra={sorted(actual_roles - expected_roles)}"
        )
    if set(_MODEL_ARTIFACT_EXTENSION) != set(MODEL_ARTIFACT_ROLES):
        raise ServingReleaseContractError(
            "ml_core와 core.model_snapshot의 artifact role 계약이 다릅니다."
        )

    normalized_payloads: dict[str, bytes] = {}
    for role in expected_roles:
        payload = artifact_payloads[role]
        if type(payload) is not bytes:
            raise TypeError(f"model artifact {role} payload는 bytes여야 합니다.")
        normalized_payloads[role] = (
            _canonicalize_station_categories(payload)
            if role == "station_categories"
            else payload
        )
    profile_payload = normalized_payloads["effective_profile"]
    contract_payload = extract_serving_feature_contract_bytes(profile_payload)
    contract_version = effective_contract_version(contract_payload)

    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    crosswalk = _station_crosswalk_from_source(station_source)
    crosswalk_payload = crosswalk.canonical_bytes
    _put_once_and_readback(
        immutable,
        station_source.uri,
        station_source.payload,
    )
    normalized_payloads["station_crosswalk"] = crosswalk_payload

    model_name = model_kind.value
    artifacts: list[ModelArtifact] = []
    for role in MODEL_ARTIFACT_ROLES:
        payload = normalized_payloads[role]
        extension = _MODEL_ARTIFACT_EXTENSION[role]
        digest = sha256_hex(payload)
        uri = _s3_uri(
            model_snapshot_artifact_key(
                model_name,
                role,
                digest,
                extension,
            ),
            bucket=bucket,
        )
        _put_once_and_readback(immutable, uri, payload)
        artifacts.append(ModelArtifact(byte_sha256=digest, role=role, uri=uri))

    categories = parse_station_categories(normalized_payloads["station_categories"])
    support = build_model_support_sta_ids(categories, crosswalk)
    support_uri = _s3_uri(
        model_support_id_set_key(model_name, support.sha256),
        bucket=bucket,
    )
    _put_once_and_readback(
        immutable,
        support_uri,
        support.canonical_bytes,
        require_canonical_json=True,
    )
    support_ref: IdSetArtifactRef = build_id_set_artifact_ref(support, support_uri)

    manifest = build_model_snapshot_manifest(
        model_kind=model_kind,
        effective_contract_version=contract_version,
        artifacts=artifacts,
        support_sta_ids=support_ref,
    )
    derive_model_support_sta_ids(
        manifest,
        normalized_payloads["station_categories"],
        crosswalk_payload,
    )
    validate_model_effective_contract_binding(manifest, profile_payload)

    manifest_uri = _s3_uri(
        model_snapshot_manifest_key(model_name, manifest.sha256),
        bucket=bucket,
    )
    _put_once_and_readback(
        immutable,
        manifest_uri,
        manifest.canonical_bytes,
        require_canonical_json=True,
    )
    readback = immutable.read_bytes(
        manifest_uri,
        manifest.sha256,
        require_canonical_json=True,
    )
    if parse_model_snapshot_manifest(readback) != manifest:
        raise ServingReleasePreflightError(
            f"{model_name} model manifest readback이 입력 manifest와 다릅니다."
        )
    return PublishedModelSnapshot(
        manifest=manifest,
        manifest_ref=build_model_manifest_ref(manifest, manifest_uri),
        support_sta_ids=support,
        station_crosswalk=crosswalk,
    )


def build_serving_release_manifest(
    *,
    rental_model_manifest: ModelManifestRef,
    return_model_manifest: ModelManifestRef,
    station_profile: ImmutableArtifactRef,
    effective_contract: EffectiveContractRef,
) -> ServingReleaseManifest:
    """검증된 pair input으로 content-derived version의 release manifest를 만든다."""
    values = (
        rental_model_manifest,
        return_model_manifest,
        station_profile,
        effective_contract,
    )
    expected_types = (
        ModelManifestRef,
        ModelManifestRef,
        ImmutableArtifactRef,
        EffectiveContractRef,
    )
    if any(
        type(value) is not expected for value, expected in zip(values, expected_types)
    ):
        raise ServingReleaseContractError(
            "release input은 exact ref dataclass여야 합니다."
        )
    release_version = _release_version(
        rental_model_manifest,
        return_model_manifest,
        station_profile,
        effective_contract,
    )
    return ServingReleaseManifest(
        schema_version=SERVING_RELEASE_MANIFEST_SCHEMA_VERSION,
        release_version=release_version,
        rental_model_manifest=rental_model_manifest,
        return_model_manifest=return_model_manifest,
        station_profile=station_profile,
        effective_contract=effective_contract,
    )


def validate_serving_release_manifest(manifest: ServingReleaseManifest) -> None:
    """Release의 exact schema, model kind, shared contract와 derived version을 검증한다."""
    if type(manifest) is not ServingReleaseManifest:
        raise ServingReleaseContractError(
            "manifest는 exact ServingReleaseManifest여야 합니다."
        )
    _require_exact_string(
        manifest.schema_version,
        SERVING_RELEASE_MANIFEST_SCHEMA_VERSION,
        "serving release schema_version",
    )
    if type(manifest.rental_model_manifest) is not ModelManifestRef:
        raise ServingReleaseContractError(
            "rental_model_manifest는 exact ModelManifestRef여야 합니다."
        )
    if type(manifest.return_model_manifest) is not ModelManifestRef:
        raise ServingReleaseContractError(
            "return_model_manifest는 exact ModelManifestRef여야 합니다."
        )
    if type(manifest.station_profile) is not ImmutableArtifactRef:
        raise ServingReleaseContractError(
            "station_profile은 exact ImmutableArtifactRef여야 합니다."
        )
    validate_content_addressed_s3_uri(
        manifest.station_profile.uri,
        manifest.station_profile.byte_sha256,
        expected_extension="parquet",
    )
    if type(manifest.effective_contract) is not EffectiveContractRef:
        raise ServingReleaseContractError(
            "effective_contract는 exact EffectiveContractRef여야 합니다."
        )
    if manifest.rental_model_manifest.model_kind is not ModelKind.RENTAL:
        raise ServingReleaseContractError(
            "rental_model_manifest는 rental model이어야 합니다."
        )
    if manifest.return_model_manifest.model_kind is not ModelKind.RETURN:
        raise ServingReleaseContractError(
            "return_model_manifest는 return model이어야 합니다."
        )
    expected_contract = manifest.effective_contract.version
    actual_contracts = {
        manifest.rental_model_manifest.effective_contract_version,
        manifest.return_model_manifest.effective_contract_version,
    }
    if actual_contracts != {expected_contract}:
        raise ServingReleaseContractError(
            "rental/return model과 release effective contract version이 같아야 합니다."
        )
    top_level_uris = {
        manifest.rental_model_manifest.uri,
        manifest.return_model_manifest.uri,
        manifest.station_profile.uri,
        manifest.effective_contract.uri,
    }
    if len(top_level_uris) != 4:
        raise ServingReleaseContractError(
            "release의 top-level artifact URI는 서로 달라야 합니다."
        )
    expected_version = _release_version(
        manifest.rental_model_manifest,
        manifest.return_model_manifest,
        manifest.station_profile,
        manifest.effective_contract,
    )
    if manifest.release_version != expected_version:
        raise ServingReleaseContractError(
            "release_version은 versionless release identity bytes SHA여야 합니다."
        )
    canonical_json_bytes(_release_manifest_document(manifest))


def parse_serving_release_manifest(payload: bytes) -> ServingReleaseManifest:
    """Canonical bytes를 exact-key serving release manifest로 파싱한다."""
    document = _require_exact_object(
        parse_canonical_json(payload),
        _RELEASE_MANIFEST_KEYS,
        "serving release manifest",
    )
    return ServingReleaseManifest(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        release_version=_require_string(document["release_version"], "release_version"),
        rental_model_manifest=_parse_model_ref(document["rental_model_manifest"]),
        return_model_manifest=_parse_model_ref(document["return_model_manifest"]),
        station_profile=_parse_artifact_ref(document["station_profile"]),
        effective_contract=_parse_contract_ref(document["effective_contract"]),
    )


def parse_serving_release_pointer(payload: bytes) -> ServingReleasePointer:
    """Canonical bytes를 exact-key serving release pointer로 파싱한다."""
    document = _require_exact_object(
        parse_canonical_json(payload),
        _POINTER_KEYS,
        "serving release pointer",
    )
    return ServingReleasePointer(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        generation=_require_nonnegative_integer(
            document["generation"],
            "pointer generation",
        ),
        release_manifest_byte_sha256=_require_string(
            document["release_manifest_byte_sha256"],
            "release manifest byte_sha256",
        ),
        release_manifest_uri=_require_string(
            document["release_manifest_uri"],
            "release manifest URI",
        ),
    )


def preflight_serving_release(
    manifest: ServingReleaseManifest,
    object_store: ImmutableObjectStore,
    *,
    trust_published_station_profile: bool = False,
) -> ServingReleasePreflight:
    """Release와 model manifests가 참조하는 모든 bytes/SHA와 profile 결합을 검증한다.

    게시 경계는 station profile 전체를 검증한다. 이미 이 경계를 통과해 immutable
    release에 고정된 profile을 매 5분 추론에서 다시 여는 경우에는 exact SHA와
    Parquet footer만 확인해 수천만 행 materialization을 피할 수 있다.
    """
    if type(trust_published_station_profile) is not bool:
        raise TypeError("trust_published_station_profile은 bool이어야 합니다.")
    validate_serving_release_manifest(manifest)

    contract_payload = object_store.read_bytes(
        manifest.effective_contract.uri,
        manifest.effective_contract.byte_sha256,
        require_canonical_json=True,
    )
    contract = parse_effective_serving_contract(contract_payload)
    if (
        effective_contract_version(contract_payload)
        != manifest.effective_contract.version
    ):
        raise ServingReleasePreflightError(
            "effective contract bytes와 release contract version이 다릅니다."
        )

    station_profile_payload = object_store.read_bytes(
        manifest.station_profile.uri,
        manifest.station_profile.byte_sha256,
    )
    rental = _preflight_model_manifest(
        manifest.rental_model_manifest,
        contract_payload,
        object_store,
    )
    returned = _preflight_model_manifest(
        manifest.return_model_manifest,
        contract_payload,
        object_store,
    )
    rental_crosswalk = _model_artifact_for_role(
        rental.manifest,
        "station_crosswalk",
    )
    return_crosswalk = _model_artifact_for_role(
        returned.manifest,
        "station_crosswalk",
    )
    if rental_crosswalk.byte_sha256 != return_crosswalk.byte_sha256:
        raise ServingReleasePreflightError(
            "rental/return model은 exact same station crosswalk bytes를 사용해야 합니다."
        )
    try:
        shared_crosswalk = parse_station_crosswalk(
            rental.artifact_payload("station_crosswalk")
        )
        rental_categories = parse_station_categories(
            rental.artifact_payload("station_categories")
        )
        return_categories = parse_station_categories(
            returned.artifact_payload("station_categories")
        )
    except ContractViolation as exc:
        raise ServingReleasePreflightError(
            "station profile 결합에 필요한 model category/crosswalk가 잘못됐습니다."
        ) from exc
    required_station_nos = tuple(
        sorted({*rental_categories, *return_categories})
    )
    if trust_published_station_profile:
        try:
            row_count = pq.ParquetFile(io.BytesIO(station_profile_payload)).metadata.num_rows
        except (pa.ArrowInvalid, pa.ArrowTypeError, OSError) as exc:
            raise ServingReleasePreflightError(
                "station profile payload의 Parquet footer를 읽을 수 없습니다."
            ) from exc
        grid_tick = contract["GRID_TICK_MINUTES"]
        station_profile = VerifiedStationProfile(
            payload=station_profile_payload,
            row_count=row_count,
            station_nos=required_station_nos,
            minute_values=tuple(range(0, 1440, grid_tick)),
            grid_tick_minutes=grid_tick,
        )
    else:
        station_profile = validate_station_profile_payload(
            station_profile_payload,
            expected_grid_tick_minutes=contract["GRID_TICK_MINUTES"],
            station_crosswalk=shared_crosswalk,
            required_station_nos=required_station_nos,
        )
    return ServingReleasePreflight(
        rental_snapshot=rental,
        return_snapshot=returned,
        effective_contract_payload=contract_payload,
        station_profile=station_profile,
    )


def publish_serving_release(
    manifest: ServingReleaseManifest,
    *,
    station_source: ExplicitImmutablePayload | None = None,
    object_store: ImmutableObjectStore | None = None,
    pointer_store: ServingReleasePointerStore | None = None,
    release_manifest_uri: str | None = None,
    pointer_key: str | None = None,
    allow_contract_change: bool = False,
) -> ServingReleasePointer:
    """Preflight와 manifest readback 뒤 단일 CAS를 마지막 쓰기로 수행한다.

    같은 release의 재실행은 기존 pointer generation을 그대로 반환한다. 현재 release와
    effective contract가 다른 migration은 monthly/automatic 경로에서 수행할 수 없고,
    승인된 maintenance caller만 ``allow_contract_change=True``를 명시해야 한다.
    """
    if type(allow_contract_change) is not bool:
        raise TypeError("allow_contract_change는 bool이어야 합니다.")
    if type(station_source) is not ExplicitImmutablePayload:
        raise ServingReleasePreflightError(
            "pair promotion에는 feature build가 사용한 explicit immutable "
            "station master/crosswalk payload가 필요합니다."
        )
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    mutable = (
        pointer_store if pointer_store is not None else S3ServingReleasePointerStore()
    )
    resolved_pointer_key = pointer_key or serving_release_pointer_key()
    _require_pointer_key(resolved_pointer_key)

    preflight = preflight_serving_release(manifest, immutable)
    _validate_station_source_binding(preflight, station_source, immutable)
    release_payload = manifest.canonical_bytes
    resolved_manifest_uri = release_manifest_uri or (
        f"s3://{s3_io.bucket_name()}/{serving_release_manifest_key(manifest.sha256)}"
    )
    validate_content_addressed_s3_uri(
        resolved_manifest_uri,
        manifest.sha256,
        expected_extension="json",
    )

    current_read = mutable.read(resolved_pointer_key)
    current_pointer: ServingReleasePointer | None = None
    if current_read.payload is not None:
        current_pointer = parse_serving_release_pointer(current_read.payload)
        current_payload = immutable.read_bytes(
            current_pointer.release_manifest_uri,
            current_pointer.release_manifest_byte_sha256,
            require_canonical_json=True,
        )
        current_manifest = parse_serving_release_manifest(current_payload)
        if (
            current_pointer.release_manifest_byte_sha256 == manifest.sha256
            and current_pointer.release_manifest_uri == resolved_manifest_uri
        ):
            return current_pointer
        if (
            current_manifest.effective_contract.version
            != manifest.effective_contract.version
            and not allow_contract_change
        ):
            raise CrossContractServingReleaseError(
                "개별/자동 승격으로 effective contract를 바꿀 수 없습니다. "
                "승인된 pair maintenance migration에서 allow_contract_change=True를 "
                "명시해야 합니다."
            )

    immutable.put_once(
        resolved_manifest_uri,
        release_payload,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    readback = immutable.read_bytes(
        resolved_manifest_uri,
        manifest.sha256,
        require_canonical_json=True,
    )
    if parse_serving_release_manifest(readback) != manifest:
        raise ServingReleasePreflightError(
            "put-once 뒤 readback한 release manifest가 입력 manifest와 다릅니다."
        )

    generation = 0 if current_pointer is None else current_pointer.generation + 1
    pointer = ServingReleasePointer(
        schema_version=SERVING_RELEASE_POINTER_SCHEMA_VERSION,
        generation=generation,
        release_manifest_byte_sha256=manifest.sha256,
        release_manifest_uri=resolved_manifest_uri,
    )
    mutable.compare_and_swap(
        resolved_pointer_key,
        current_read.version_token,
        pointer.canonical_bytes,
    )
    return pointer


def load_current_serving_release(
    *,
    object_store: ImmutableObjectStore | None = None,
    pointer_store: ServingReleasePointerStore | None = None,
    pointer_key: str | None = None,
) -> PinnedServingRelease:
    """Pointer를 한 번 읽고 exact release와 모든 transitive bytes를 fail-fast 검증한다."""
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    mutable = (
        pointer_store if pointer_store is not None else S3ServingReleasePointerStore()
    )
    resolved_pointer_key = pointer_key or serving_release_pointer_key()
    pointer_read = mutable.read(resolved_pointer_key)
    if pointer_read.payload is None:
        raise FileNotFoundError(
            f"serving release pointer가 없습니다: {resolved_pointer_key}"
        )
    pointer = parse_serving_release_pointer(pointer_read.payload)
    manifest_payload = immutable.read_bytes(
        pointer.release_manifest_uri,
        pointer.release_manifest_byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_serving_release_manifest(manifest_payload)
    preflight = preflight_serving_release(manifest, immutable)
    return PinnedServingRelease(
        pointer=pointer,
        pointer_payload=pointer_read.payload,
        manifest=manifest,
        manifest_payload=manifest_payload,
        preflight=preflight,
    )


def load_current_serving_release_for_inference(
    *,
    object_store: ImmutableObjectStore | None = None,
    pointer_store: ServingReleasePointerStore | None = None,
    pointer_key: str | None = None,
) -> PinnedServingRelease:
    """게시 시 검증된 profile을 재검증하지 않고 inference snapshot을 고정한다.

    Pointer, release, model artifact와 station profile은 기존 loader와 동일하게 exact
    SHA로 읽는다. 차이는 station profile의 수천만 행 validation을 반복하지 않고
    Parquet footer만 확인한다는 점이다. 최초 release 게시에는 이 경로를 사용하지
    않으며 항상 ``publish_serving_release``의 전체 preflight를 통과해야 한다.
    """
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    mutable = (
        pointer_store if pointer_store is not None else S3ServingReleasePointerStore()
    )
    resolved_pointer_key = pointer_key or serving_release_pointer_key()
    pointer_read = mutable.read(resolved_pointer_key)
    if pointer_read.payload is None:
        raise FileNotFoundError(
            f"serving release pointer가 없습니다: {resolved_pointer_key}"
        )
    pointer = parse_serving_release_pointer(pointer_read.payload)
    exact = load_exact_serving_release_for_inference(
        release_manifest_uri=pointer.release_manifest_uri,
        release_manifest_byte_sha256=pointer.release_manifest_byte_sha256,
        object_store=immutable,
    )
    return PinnedServingRelease(
        pointer=pointer,
        pointer_payload=pointer_read.payload,
        manifest=exact.manifest,
        manifest_payload=exact.manifest_payload,
        preflight=exact.preflight,
    )


def load_exact_serving_release_for_inference(
    *,
    release_manifest_uri: str,
    release_manifest_byte_sha256: str,
    object_store: ImmutableObjectStore | None = None,
) -> ExactServingRelease:
    """Plan이 pin한 exact release를 mutable pointer 재조회 없이 연다."""
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    digest = _require_sha256(
        release_manifest_byte_sha256,
        "exact serving release manifest byte_sha256",
    )
    validate_content_addressed_s3_uri(
        release_manifest_uri,
        digest,
        expected_extension="json",
    )
    manifest_payload = immutable.read_bytes(
        release_manifest_uri,
        digest,
        require_canonical_json=True,
    )
    manifest = parse_serving_release_manifest(manifest_payload)
    if manifest.sha256 != digest:
        raise ServingReleasePreflightError(
            "exact serving release manifest actual SHA가 plan ref와 다릅니다."
        )
    preflight = preflight_serving_release(
        manifest,
        immutable,
        trust_published_station_profile=True,
    )
    return ExactServingRelease(
        manifest_uri=release_manifest_uri,
        manifest=manifest,
        manifest_payload=manifest_payload,
        preflight=preflight,
    )


def load_current_serving_release_for_plan(
    *,
    object_store: ImmutableObjectStore | None = None,
    pointer_store: ServingReleasePointerStore | None = None,
    pointer_key: str | None = None,
) -> PinnedServingPlanRelease:
    """Plan 준비에 필요한 pointer/release/model manifest만 exact-read해 고정한다.

    Station profile 전체 검증과 booster artifact 로드는 release 게시 및 실제 inference
    loader의 책임으로 남긴다. Serving plan은 두 model의 content-addressed support
    reference만 사용하므로, 매 5분 1천만 행대 Parquet을 materialize하지 않는다.
    """
    immutable = object_store if object_store is not None else S3ImmutableObjectStore()
    mutable = (
        pointer_store if pointer_store is not None else S3ServingReleasePointerStore()
    )
    resolved_pointer_key = pointer_key or serving_release_pointer_key()
    pointer_read = mutable.read(resolved_pointer_key)
    if pointer_read.payload is None:
        raise FileNotFoundError(
            f"serving release pointer가 없습니다: {resolved_pointer_key}"
        )
    pointer = parse_serving_release_pointer(pointer_read.payload)
    manifest_payload = immutable.read_bytes(
        pointer.release_manifest_uri,
        pointer.release_manifest_byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_serving_release_manifest(manifest_payload)
    rental_payload, rental = _read_model_manifest(
        manifest.rental_model_manifest,
        immutable,
    )
    return_payload, returned = _read_model_manifest(
        manifest.return_model_manifest,
        immutable,
    )
    return PinnedServingPlanRelease(
        pointer=pointer,
        pointer_payload=pointer_read.payload,
        manifest=manifest,
        manifest_payload=manifest_payload,
        rental_model=rental,
        rental_model_payload=rental_payload,
        return_model=returned,
        return_model_payload=return_payload,
    )


def _read_model_manifest(
    reference: ModelManifestRef,
    object_store: ImmutableObjectStore,
) -> tuple[bytes, ModelSnapshotManifest]:
    """Model manifest bytes를 SHA 검증하고 release reference와 결합한다."""
    payload = object_store.read_bytes(
        reference.uri,
        reference.byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_model_snapshot_manifest(payload)
    if (
        manifest.sha256 != reference.byte_sha256
        or manifest.model_kind is not reference.model_kind
        or manifest.model_version != reference.model_version
        or manifest.effective_contract_version != reference.effective_contract_version
    ):
        raise ServingReleasePreflightError(
            f"{reference.model_kind.value} model ref가 실제 manifest metadata와 다릅니다."
        )
    return payload, manifest


def _preflight_model_manifest(
    reference: ModelManifestRef,
    effective_contract_payload: bytes,
    object_store: ImmutableObjectStore,
) -> VerifiedModelSnapshot:
    """Model manifest와 그 모든 transitive artifact를 exact bytes로 검증한다."""
    payload, manifest = _read_model_manifest(reference, object_store)

    artifact_payloads: dict[str, bytes] = {}
    verified_artifacts: list[VerifiedModelArtifact] = []
    for artifact in manifest.artifacts:
        artifact_payload = object_store.read_bytes(
            artifact.uri,
            artifact.byte_sha256,
        )
        artifact_payloads[artifact.role] = artifact_payload
        verified_artifacts.append(
            VerifiedModelArtifact(reference=artifact, payload=artifact_payload)
        )
    support_payload = object_store.read_bytes(
        manifest.support_sta_ids.uri,
        manifest.support_sta_ids.byte_sha256,
        require_canonical_json=True,
    )
    support = parse_id_set(support_payload)
    if len(support.ids) != manifest.support_sta_ids.id_count:
        raise ServingReleasePreflightError(
            f"{reference.model_kind.value} support ID count가 manifest와 다릅니다."
        )

    profile_payload = artifact_payloads.get("effective_profile")
    if profile_payload is None:
        raise ServingReleasePreflightError(
            f"{reference.model_kind.value} model에 effective_profile artifact가 없습니다."
        )
    try:
        actual_contract_payload = validate_model_effective_contract_binding(
            manifest,
            profile_payload,
        )
    except ContractViolation as exc:
        raise ServingReleasePreflightError(
            f"{reference.model_kind.value} effective_profile/model contract binding이 "
            "잘못되었습니다."
        ) from exc
    if actual_contract_payload != effective_contract_payload:
        raise ServingReleasePreflightError(
            f"{reference.model_kind.value} effective_profile의 serving contract가 "
            "release contract와 다릅니다."
        )

    _validate_model_support_binding(manifest, artifact_payloads)
    return VerifiedModelSnapshot(
        manifest=manifest,
        manifest_payload=payload,
        artifacts=tuple(verified_artifacts),
        support_sta_ids=support,
        support_sta_ids_payload=support_payload,
    )


def _validate_model_support_binding(
    manifest: ModelSnapshotManifest,
    artifact_payloads: Mapping[str, bytes],
) -> None:
    """Category/crosswalk로 support ID set을 재계산하는 core adapter 경계를 호출한다."""
    categories_payload = artifact_payloads.get("station_categories")
    crosswalk_payload = artifact_payloads.get("station_crosswalk")
    if categories_payload is None or crosswalk_payload is None:
        raise ServingReleasePreflightError(
            f"{manifest.model_kind.value} model에 station_categories/crosswalk가 없습니다."
        )
    try:
        derive_model_support_sta_ids(
            manifest,
            categories_payload,
            crosswalk_payload,
        )
    except ContractViolation as exc:
        raise ServingReleasePreflightError(
            f"{manifest.model_kind.value} model support ID binding이 잘못되었습니다."
        ) from exc


def _validate_station_source_binding(
    preflight: ServingReleasePreflight,
    station_source: ExplicitImmutablePayload,
    object_store: ImmutableObjectStore,
) -> None:
    """Feature build의 explicit station source를 두 model crosswalk bytes에 결합한다."""
    _put_once_and_readback(
        object_store,
        station_source.uri,
        station_source.payload,
    )
    expected_crosswalk = _station_crosswalk_from_source(station_source)
    expected_sha256 = expected_crosswalk.sha256
    for model in (preflight.rental_model, preflight.return_model):
        artifact = _model_artifact_for_role(model, "station_crosswalk")
        if artifact.byte_sha256 != expected_sha256:
            raise ServingReleasePreflightError(
                f"{model.model_kind.value} station crosswalk가 명시된 feature-build "
                "station source에서 재생성한 bytes와 다릅니다."
            )


def _model_artifact_for_role(
    manifest: ModelSnapshotManifest,
    role: str,
) -> ModelArtifact:
    """Validated model manifest에서 exact role artifact 하나를 반환한다."""
    matches = tuple(
        artifact for artifact in manifest.artifacts if artifact.role == role
    )
    if len(matches) != 1:
        raise ServingReleasePreflightError(
            f"{manifest.model_kind.value} model artifact role이 정확히 한 개가 아닙니다: {role}"
        )
    return matches[0]


def _station_crosswalk_from_source(
    source: ExplicitImmutablePayload,
) -> StationCrosswalk:
    """Explicit canonical crosswalk 또는 station master Parquet에서 exact mapping을 만든다."""
    if source.uri.endswith(".json"):
        try:
            return parse_station_crosswalk(source.payload)
        except ContractViolation as exc:
            raise ServingReleasePreflightError(
                "명시된 station crosswalk JSON이 canonical contract와 다릅니다."
            ) from exc
    if not source.uri.endswith(".parquet"):
        raise ServingReleasePreflightError(
            "station source는 content-addressed .json 또는 .parquet object여야 합니다."
        )
    try:
        table = pq.read_table(
            io.BytesIO(source.payload),
            columns=["station_id", "station_no"],
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError, OSError) as exc:
        raise ServingReleasePreflightError(
            "station master Parquet에서 station_id/station_no를 읽을 수 없습니다."
        ) from exc
    station_ids = table.column("station_id").to_pylist()
    station_nos = table.column("station_no").to_pylist()
    entries: list[StationCrosswalkEntry] = []
    for index, (station_id, station_no) in enumerate(zip(station_ids, station_nos)):
        if type(station_id) is not str or type(station_no) is not int:
            raise ServingReleasePreflightError(
                "station master crosswalk 값은 non-null string/int여야 합니다: "
                f"row={index}, station_id={station_id!r}, station_no={station_no!r}"
            )
        entries.append(StationCrosswalkEntry(station_no=station_no, sta_id=station_id))
    try:
        return build_station_crosswalk(entries)
    except ContractViolation as exc:
        raise ServingReleasePreflightError(
            "station master의 station_id/station_no가 1:1 crosswalk가 아닙니다."
        ) from exc


def _canonicalize_station_categories(payload: bytes) -> bytes:
    """Legacy JSON whitespace는 허용하되 integer category 의미를 canonical bytes로 고정한다."""
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_json_object_without_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ServingReleasePreflightError(
            "station_categories JSON을 읽을 수 없습니다."
        ) from exc
    if type(value) is not list:
        raise ServingReleasePreflightError(
            "station_categories는 JSON integer array여야 합니다."
        )
    try:
        return canonical_station_categories_bytes(value)
    except ContractViolation as exc:
        raise ServingReleasePreflightError(
            "station_categories가 unique int16 categorical order 계약을 위반했습니다."
        ) from exc


def _put_once_and_readback(
    object_store: ImmutableObjectStore,
    uri: str,
    payload: bytes,
    *,
    require_canonical_json: bool = False,
) -> None:
    """Actual bytes를 put-once한 뒤 같은 SHA로 즉시 readback한다."""
    digest = sha256_hex(payload)
    object_store.put_once(
        uri,
        payload,
        expected_sha256=digest,
        require_canonical_json=require_canonical_json,
    )
    readback = object_store.read_bytes(
        uri,
        digest,
        require_canonical_json=require_canonical_json,
    )
    if readback != payload:
        raise ServingReleasePreflightError(
            f"immutable object readback bytes가 입력과 다릅니다: {uri}"
        )


def _s3_uri(key: str, *, bucket: str | None) -> str:
    """Bucket-relative key를 현재 또는 명시된 bucket의 canonical S3 URI로 만든다."""
    resolved_bucket = bucket if bucket is not None else s3_io.bucket_name()
    _require_nonblank_nfc(resolved_bucket, "S3 bucket")
    if "/" in resolved_bucket or ":" in resolved_bucket:
        raise ServingReleaseContractError("S3 bucket 이름이 유효하지 않습니다.")
    return f"s3://{resolved_bucket}/{key}"


def _release_version(
    rental: ModelManifestRef,
    returned: ModelManifestRef,
    station_profile: ImmutableArtifactRef,
    effective_contract: EffectiveContractRef,
) -> str:
    """Version 필드를 제외한 release identity canonical bytes의 SHA version을 만든다."""
    identity: dict[str, JsonValue] = {
        "effective_contract": _contract_ref_document(effective_contract),
        "rental_model_manifest": _model_ref_document(rental),
        "return_model_manifest": _model_ref_document(returned),
        "schema_version": SERVING_RELEASE_IDENTITY_SCHEMA_VERSION,
        "station_profile": _artifact_ref_document(station_profile),
    }
    return f"sha256:{sha256_hex(canonical_json_bytes(identity))}"


def _release_manifest_document(
    manifest: ServingReleaseManifest,
) -> dict[str, JsonValue]:
    """Typed serving release manifest를 exact canonical JSON object로 바꾼다."""
    return {
        "effective_contract": _contract_ref_document(manifest.effective_contract),
        "release_version": manifest.release_version,
        "rental_model_manifest": _model_ref_document(manifest.rental_model_manifest),
        "return_model_manifest": _model_ref_document(manifest.return_model_manifest),
        "schema_version": manifest.schema_version,
        "station_profile": _artifact_ref_document(manifest.station_profile),
    }


def _model_ref_document(reference: ModelManifestRef) -> dict[str, JsonValue]:
    """Model manifest ref를 exact JSON object로 바꾼다."""
    return {
        "byte_sha256": reference.byte_sha256,
        "effective_contract_version": reference.effective_contract_version,
        "model_kind": reference.model_kind.value,
        "model_version": reference.model_version,
        "uri": reference.uri,
    }


def _artifact_ref_document(reference: ImmutableArtifactRef) -> dict[str, JsonValue]:
    """Immutable artifact ref를 exact JSON object로 바꾼다."""
    return {"byte_sha256": reference.byte_sha256, "uri": reference.uri}


def _contract_ref_document(reference: EffectiveContractRef) -> dict[str, JsonValue]:
    """Effective contract ref를 exact JSON object로 바꾼다."""
    return {
        "byte_sha256": reference.byte_sha256,
        "uri": reference.uri,
        "version": reference.version,
    }


def _pointer_document(pointer: ServingReleasePointer) -> dict[str, JsonValue]:
    """Typed pointer를 exact canonical JSON object로 바꾼다."""
    return {
        "generation": pointer.generation,
        "release_manifest_byte_sha256": pointer.release_manifest_byte_sha256,
        "release_manifest_uri": pointer.release_manifest_uri,
        "schema_version": pointer.schema_version,
    }


def _parse_model_ref(value: JsonValue) -> ModelManifestRef:
    """Exact JSON object를 model manifest ref로 파싱한다."""
    document = _require_exact_object(value, _MODEL_REF_KEYS, "model manifest ref")
    kind_text = _require_string(document["model_kind"], "model_kind")
    try:
        model_kind = ModelKind(kind_text)
    except ValueError as exc:
        raise ServingReleaseContractError(
            "model_kind는 rental 또는 return이어야 합니다."
        ) from exc
    return ModelManifestRef(
        byte_sha256=_require_string(document["byte_sha256"], "byte_sha256"),
        effective_contract_version=_require_string(
            document["effective_contract_version"],
            "effective_contract_version",
        ),
        model_kind=model_kind,
        model_version=_require_string(document["model_version"], "model_version"),
        uri=_require_string(document["uri"], "uri"),
    )


def _parse_artifact_ref(value: JsonValue) -> ImmutableArtifactRef:
    """Exact JSON object를 immutable artifact ref로 파싱한다."""
    document = _require_exact_object(value, _ARTIFACT_REF_KEYS, "artifact ref")
    return ImmutableArtifactRef(
        byte_sha256=_require_string(document["byte_sha256"], "byte_sha256"),
        uri=_require_string(document["uri"], "uri"),
    )


def _parse_contract_ref(value: JsonValue) -> EffectiveContractRef:
    """Exact JSON object를 effective contract ref로 파싱한다."""
    document = _require_exact_object(
        value,
        _CONTRACT_REF_KEYS,
        "effective contract ref",
    )
    return EffectiveContractRef(
        byte_sha256=_require_string(document["byte_sha256"], "byte_sha256"),
        uri=_require_string(document["uri"], "uri"),
        version=_require_string(document["version"], "version"),
    )


def _require_exact_object(
    value: JsonValue,
    expected_keys: frozenset[str],
    label: str,
) -> dict[str, JsonValue]:
    """JSON 값이 exact builtin object와 key 집합인지 확인한다."""
    if type(value) is not dict:
        raise ServingReleaseContractError(f"{label}는 JSON object여야 합니다.")
    document = cast(dict[str, JsonValue], value)
    actual_keys = frozenset(document)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys.difference(actual_keys))
        extra = sorted(actual_keys.difference(expected_keys))
        raise ServingReleaseContractError(
            f"{label} key가 정확하지 않습니다: missing={missing}, extra={extra}"
        )
    return document


def _require_string(value: JsonValue, label: str) -> str:
    """JSON 값이 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise ServingReleaseContractError(f"{label}은 문자열이어야 합니다.")
    return _require_nonblank_nfc(value, label)


def _require_nonblank_nfc(value: Any, label: str) -> str:
    """값이 공백·제어 문자 없는 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise ServingReleaseContractError(f"{label}은 문자열이어야 합니다.")
    if (
        not value
        or value != value.strip()
        or unicodedata.normalize("NFC", value) != value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ServingReleaseContractError(
            f"{label}은 공백·제어 문자 없는 NFC 문자열이어야 합니다."
        )
    return value


def _require_exact_string(value: Any, expected: str, label: str) -> str:
    """값이 contract 고정 문자열과 정확히 같은지 확인한다."""
    if type(value) is not str or value != expected:
        raise ServingReleaseContractError(
            f"{label}은 정확히 {expected!r}이어야 합니다."
        )
    return value


def _require_sha256(value: Any, label: str) -> str:
    """값이 exact lowercase SHA-256 string인지 확인한다."""
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ServingReleaseContractError(
            f"{label}은 정확히 64자리 lowercase hex여야 합니다."
        )
    return value


def _require_version(value: Any, label: str) -> str:
    """값이 exact ``sha256:<lowercase digest>`` version인지 확인한다."""
    if type(value) is not str or _VERSION_PATTERN.fullmatch(value) is None:
        raise ServingReleaseContractError(
            f"{label}은 정확히 sha256:<64자리 lowercase hex>여야 합니다."
        )
    return value


def _require_role(value: Any) -> str:
    """Artifact role이 lowercase snake_case exact string인지 확인한다."""
    if type(value) is not str or _ROLE_PATTERN.fullmatch(value) is None:
        raise ServingReleaseContractError(
            "artifact role은 lowercase snake_case여야 합니다."
        )
    return value


def _require_extension(value: Any) -> str:
    """Artifact extension이 URI path에 안전한 exact string인지 확인한다."""
    if type(value) is not str or re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ServingReleaseContractError("artifact extension이 유효하지 않습니다.")
    return value


def _require_nonnegative_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 canonical-safe nonnegative builtin integer인지 확인한다."""
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        raise ServingReleaseContractError(
            f"{label}은 0 이상 {_MAX_SAFE_INTEGER} 이하 integer여야 합니다."
        )
    return value


def _require_positive_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 canonical-safe positive builtin integer인지 확인한다."""
    if type(value) is not int or not 1 <= value <= _MAX_SAFE_INTEGER:
        raise ServingReleaseContractError(
            f"{label}은 1 이상 {_MAX_SAFE_INTEGER} 이하 integer여야 합니다."
        )
    return value


def _require_pointer_key(value: Any) -> str:
    """Pointer key가 bucket-relative exact object key인지 확인한다."""
    key = _require_nonblank_nfc(value, "pointer key")
    if key.startswith("/") or key.endswith("/") or "//" in key:
        raise ServingReleasePointerError(
            "pointer key는 정확한 bucket-relative object key여야 합니다."
        )
    return key


def _json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Profile JSON object를 만들며 duplicate key를 거부한다."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    """Profile JSON의 NaN/Infinity 비표준 상수를 거부한다."""
    raise ValueError(f"invalid JSON constant: {value}")


def _client_error_code(exc: ClientError) -> str:
    """Botocore ClientError의 서비스 오류 코드를 안정적으로 반환한다."""
    return str(exc.response.get("Error", {}).get("Code", "Unknown"))
