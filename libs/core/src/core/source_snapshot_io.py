"""권위 manifest를 따라 immutable source Silver snapshot을 읽는다."""

from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError

from . import layout
from .gold_publication.canonical import (
    format_utc_dttm,
    sha256_hex,
    validate_sha256_hex,
)
from .gold_publication.errors import HashFormatError, ImmutableObjectError
from .gold_publication.storage import S3ImmutableObjectStore
from .s3 import bucket_name, get_object_bytes, list_keys
from .source_snapshot import (
    SourceSnapshotContractError,
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    parse_source_snapshot_manifest,
)

_SOURCE_ID = re.compile(r"[a-z][a-z0-9_]*\Z")
_SILVER_CHECKSUM = re.compile(r"/sha256=([0-9a-f]{64})\.parquet\Z")


class SourceSnapshotReadError(RuntimeError):
    """Source authority 또는 연결된 Silver artifact가 올바르지 않다."""


class SourceSnapshotNotFoundError(SourceSnapshotReadError):
    """요청 범위에 authoritative source snapshot이 없다."""


@dataclass(frozen=True, slots=True)
class SourceSnapshotData:
    """검증한 source manifest와 선택적인 Parquet table을 결합한다."""

    manifest: SourceSnapshotManifest
    table: pa.Table | None


class SourceDataStatus(StrEnum):
    """Producer가 판정한 source 데이터의 객관적 상태다."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class PartialConsumptionPolicy(StrEnum):
    """Consumer가 PARTIAL 입력을 처리하는 명시적 정책이다."""

    REJECT = "reject"
    REPAIR = "repair"


class SourceFreshness(StrEnum):
    """선택된 source 데이터가 현재 window인지 과거 window인지 나타낸다."""

    CURRENT = "current"
    STALE = "stale"


class SourceResolution(StrEnum):
    """Consumer가 source 입력을 실제로 처리한 결과다."""

    OBSERVED = "observed"
    REPAIRED = "repaired"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class SourceSelectionMetadata:
    """추가 I/O 없이 consumer 경계에서 전파하는 공통 source 선택 metadata다."""

    source_id: str
    status: SourceDataStatus
    freshness: SourceFreshness
    partial_policy: PartialConsumptionPolicy
    resolution: SourceResolution
    requested_dttm: datetime
    selected_dttm: datetime | None

    def __post_init__(self) -> None:
        """필드 타입과 시각 관계를 검증하고 시각을 UTC로 고정한다."""
        source = _validated_source_id(self.source_id)
        if type(self.status) is not SourceDataStatus:
            raise TypeError("status는 SourceDataStatus여야 합니다.")
        if type(self.freshness) is not SourceFreshness:
            raise TypeError("freshness는 SourceFreshness여야 합니다.")
        if type(self.partial_policy) is not PartialConsumptionPolicy:
            raise TypeError("partial_policy는 PartialConsumptionPolicy여야 합니다.")
        if type(self.resolution) is not SourceResolution:
            raise TypeError("resolution은 SourceResolution이어야 합니다.")
        requested = _aware_utc(self.requested_dttm)
        selected = (
            None if self.selected_dttm is None else _aware_utc(self.selected_dttm)
        )
        if self.status is SourceDataStatus.FAILED:
            if selected is not None or self.resolution is not SourceResolution.UNAVAILABLE:
                raise ValueError("FAILED metadata는 selected_dttm 없이 unavailable이어야 합니다.")
        elif selected is None:
            raise ValueError("SUCCESS/PARTIAL metadata에는 selected_dttm이 필요합니다.")
        if selected is not None and selected > requested:
            raise ValueError("selected_dttm은 requested_dttm보다 미래일 수 없습니다.")
        object.__setattr__(self, "source_id", source)
        object.__setattr__(self, "requested_dttm", requested)
        object.__setattr__(self, "selected_dttm", selected)

    def as_dict(self) -> dict[str, str | None]:
        """Manifest와 구조화 로그에 함께 쓰는 안정적인 JSON object를 반환한다."""
        return {
            "freshness": self.freshness.value,
            "partial_policy": self.partial_policy.value,
            "requested_dttm": self.requested_dttm.isoformat(),
            "resolution": self.resolution.value,
            "selected_dttm": (
                None if self.selected_dttm is None else self.selected_dttm.isoformat()
            ),
            "source_id": self.source_id,
            "status": self.status.value,
        }


@dataclass(frozen=True, slots=True)
class AvailableSourceSnapshot:
    """상태와 freshness를 섞지 않고 선택한 serving 입력을 표현한다."""

    status: SourceDataStatus
    freshness: SourceFreshness
    logical_dttm: datetime
    table: pa.Table
    manifest: SourceSnapshotManifest | None

    def selection_metadata(
        self,
        source_id: str,
        requested_dttm: datetime,
        *,
        partial_policy: PartialConsumptionPolicy,
    ) -> SourceSelectionMetadata:
        """이미 선택한 snapshot에서 추가 조회 없이 공통 metadata를 만든다."""
        resolution = (
            SourceResolution.REPAIRED
            if self.status is SourceDataStatus.PARTIAL
            else SourceResolution.OBSERVED
        )
        return SourceSelectionMetadata(
            source_id=source_id,
            status=self.status,
            freshness=self.freshness,
            partial_policy=partial_policy,
            resolution=resolution,
            requested_dttm=requested_dttm,
            selected_dttm=self.logical_dttm,
        )


def read_available_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    lookback: timedelta,
    partial_policy: PartialConsumptionPolicy = PartialConsumptionPolicy.REJECT,
    columns: list[str] | None = None,
) -> AvailableSourceSnapshot:
    """Consumer 정책에 따라 현재 입력을 판정한 뒤 bounded 과거 성공을 선택한다.

    PARTIAL 허용 비율은 이 reader가 다시 해석하지 않는다. Collector가 source별
    ``max_missing_ratio`` 품질 게이트를 통과시켜 completed/partial로 확정한 결과만
    ``read_partial_source_snapshot``이 열 수 있으므로 그 계약을 단일 기준으로 쓴다.
    PARTIAL은 기본적으로 거부하며, ``REPAIR``를 명시한 consumer에만 반환한다.
    반환된 PARTIAL의 누락 보정은 source 의미를 아는 호출자가 책임진다.
    """
    _require_positive_lookback(lookback)
    if type(partial_policy) is not PartialConsumptionPolicy:
        raise TypeError("partial_policy는 PartialConsumptionPolicy여야 합니다.")
    logical = _aware_utc(logical_dttm)
    try:
        exact = read_exact_source_snapshot(source_id, logical, columns=columns)
    except SourceSnapshotNotFoundError:
        exact = None
    if exact is not None and exact.table is not None:
        return AvailableSourceSnapshot(
            SourceDataStatus.SUCCESS,
            SourceFreshness.CURRENT,
            exact.manifest.logical_dttm,
            exact.table,
            exact.manifest,
        )

    if partial_policy is PartialConsumptionPolicy.REPAIR:
        try:
            partial = read_partial_source_snapshot(source_id, logical, columns=columns)
        except SourceSnapshotNotFoundError:
            partial = None
        if partial is not None:
            return AvailableSourceSnapshot(
                SourceDataStatus.PARTIAL,
                SourceFreshness.CURRENT,
                logical,
                partial,
                None,
            )
    stale = read_latest_source_snapshot(
        source_id,
        logical - timedelta(microseconds=1),
        lookback=lookback,
        columns=columns,
    )
    return AvailableSourceSnapshot(
        SourceDataStatus.SUCCESS,
        SourceFreshness.STALE,
        stale.manifest.logical_dttm,
        stale.table,
        stale.manifest,
    )


def read_exact_source_snapshot_manifest(
    manifest_uri: str,
    expected_sha256: str,
    *,
    columns: list[str] | None = None,
) -> SourceSnapshotData:
    """고정한 manifest URI와 SHA로 특정 source revision을 정확히 읽는다.

    Logical window의 최신 revision을 다시 선택하지 않는다. 호출자가 고정한 manifest
    object 자체를 checksum과 canonical JSON으로 검증하고, key에 표현된 source·logical
    time·revision이 본문 identity와 정확히 같은지 확인한 뒤 연결된 Silver의 checksum과
    행 수도 검증한다. 따라서 같은 logical window에 더 최신 correction이 게시되어도
    이 함수의 결과는 바뀌지 않는다.

    args:
        manifest_uri: 정확한 ``s3://bucket/source_snapshot_manifest/...json`` URI
        expected_sha256: manifest canonical bytes의 lowercase SHA-256
        columns: Silver에서 선택할 컬럼 목록. 생략하면 전체 컬럼을 읽는다.
    returns:
        고정한 manifest와 선택적인 Silver Table
    raises:
        ValueError: URI 또는 expected SHA 형식이 잘못됐을 때
        SourceSnapshotReadError: manifest나 Silver가 계약을 위반하거나 읽히지 않을 때
    """
    key = _exact_manifest_key_from_uri(manifest_uri)
    try:
        checksum = validate_sha256_hex(expected_sha256)
    except HashFormatError as exc:
        raise ValueError("expected_sha256 형식이 잘못됐습니다.") from exc

    try:
        payload = S3ImmutableObjectStore().read_bytes(
            manifest_uri,
            checksum,
            require_canonical_json=True,
        )
    except ImmutableObjectError as exc:
        raise SourceSnapshotReadError(
            f"고정한 source manifest를 검증해 읽을 수 없습니다: {manifest_uri}"
        ) from exc

    try:
        manifest = parse_source_snapshot_manifest(payload)
    except SourceSnapshotContractError as exc:
        raise SourceSnapshotReadError(
            f"고정한 source manifest 본문이 계약을 위반했습니다: {manifest_uri}"
        ) from exc
    if key != _manifest_key(manifest):
        raise SourceSnapshotReadError(
            f"고정한 source manifest key와 본문 identity가 다릅니다: {manifest_uri}"
        )
    return _snapshot_data(manifest, columns)


def read_exact_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    columns: list[str] | None = None,
) -> SourceSnapshotData:
    """한 logical window의 최신 correction과 연결된 Parquet을 읽는다."""
    source = _validated_source_id(source_id)
    logical = _aware_utc(logical_dttm)
    keys = tuple(sorted(list_keys(layout.source_snapshot_logical_prefix(source, logical))))
    if not keys:
        raise SourceSnapshotNotFoundError(
            f"{source} exact source authority window가 없습니다: {format_utc_dttm(logical)}"
        )

    manifests = [_read_listed_manifest(key, source, logical) for key in keys]
    revisions = [manifest.revision_no for manifest in manifests]
    if revisions != list(range(len(revisions))):
        raise SourceSnapshotReadError(
            f"source authority revision chain이 0부터 연속되지 않습니다: {revisions}"
        )
    return _snapshot_data(manifests[-1], columns)


def read_latest_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    lookback: timedelta,
    columns: list[str] | None = None,
) -> SourceSnapshotData:
    """기준 시각 이전 bounded 범위의 최신 SUCCEEDED snapshot을 읽는다."""
    _require_positive_lookback(lookback)
    source = _validated_source_id(source_id)
    cutoff = _aware_utc(logical_dttm)
    lower = cutoff - lookback
    current = cutoff.replace(minute=0, second=0, microsecond=0)
    final = lower.replace(minute=0, second=0, microsecond=0)
    while current >= final:
        keys = list_keys(layout.source_snapshot_hour_prefix(source, current))
        logicals = sorted(
            {
                parsed
                for key in keys
                if lower <= (parsed := _logical_from_key(key, source)) <= cutoff
            },
            reverse=True,
        )
        for logical in logicals:
            snapshot = read_exact_source_snapshot(source, logical, columns=columns)
            if snapshot.table is not None:
                return snapshot
        current -= timedelta(hours=1)
    raise SourceSnapshotNotFoundError(
        f"{source} SUCCEEDED source authority가 lookback 안에 없습니다."
    )


def read_partial_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    columns: list[str] | None = None,
) -> pa.Table:
    """명시적으로 허용된 PARTIAL 실행의 content-addressed Silver를 읽는다.

    PARTIAL은 Gold authority를 열 수 없지만 실시간 운영 소비자는 source별 정책에
    따라 제한적으로 사용할 수 있다. 호출자가 그 정책을 결정하고, 이 함수는 exact
    diagnostic manifest가 completed/partial인지와 artifact checksum·행 수를 검증한다.
    """
    source = _validated_source_id(source_id)
    logical = _aware_utc(logical_dttm)
    manifest_key = layout.window_manifest_key(source, logical)
    payload = get_object_bytes(manifest_key)
    if payload is None:
        raise SourceSnapshotNotFoundError(
            f"{source} partial diagnostic manifest가 없습니다: {manifest_key}"
        )
    silver_key, kept = _completed_partial_silver(payload, manifest_key, source, logical)
    match = _SILVER_CHECKSUM.search(silver_key)
    if match is None or silver_key != layout.silver_content_key(
        source, logical, match.group(1)
    ):
        raise SourceSnapshotReadError(
            f"partial Silver key가 logical window 또는 content address와 다릅니다: {silver_key}"
        )
    return _read_content_addressed_parquet(
        silver_key,
        checksum=match.group(1),
        expected_rows=kept,
        columns=columns,
    )


def _completed_partial_silver(
    payload: bytes, manifest_key: str, source: str, logical: datetime
) -> tuple[str, int]:
    """Diagnostic manifest가 completed PARTIAL인지 확인하고 Silver key와 행 수를 반환한다."""
    try:
        document = json.loads(payload)
        manifest_logical = datetime.fromisoformat(document["window_start"])
        artifacts = document["artifacts"]
        counts = document["counts"]
        silver_key = artifacts["silver"]
        kept = counts["kept"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SourceSnapshotReadError(
            f"partial diagnostic manifest 형식이 잘못됐습니다: {manifest_key}"
        ) from exc
    if (
        type(document) is not dict
        or document.get("source_id") != source
        or manifest_logical.tzinfo is None
        or manifest_logical.astimezone(UTC) != logical
        or document.get("status") != "partial"
        or document.get("stage") != "completed"
        or document.get("failure_reason") is not None
        or type(artifacts) is not dict
        or type(counts) is not dict
        or type(silver_key) is not str
        or type(kept) is not int
        or kept <= 0
    ):
        raise SourceSnapshotReadError(
            f"partial diagnostic manifest가 completed PARTIAL 계약을 위반했습니다: {manifest_key}"
        )
    return silver_key, kept


def _read_listed_manifest(key: str, source: str, logical: datetime) -> SourceSnapshotManifest:
    """LIST로 찾은 revision key의 manifest를 읽고 key와 본문 identity가 같은지 확인한다."""
    try:
        revision = layout.parse_source_snapshot_manifest_key(key).revision_no
    except ValueError as exc:
        raise SourceSnapshotReadError(
            f"source authority revision key가 잘못됐습니다: {key}"
        ) from exc
    payload = get_object_bytes(key)
    if payload is None:
        raise SourceSnapshotReadError(
            f"LIST된 source authority를 읽을 수 없습니다: {key}"
        )
    manifest = parse_source_snapshot_manifest(payload)
    if (
        manifest.source_id != source
        or manifest.logical_dttm.astimezone(UTC) != logical
        or manifest.revision_no != revision
        or key != _manifest_key(manifest)
    ):
        raise SourceSnapshotReadError(
            f"source authority key와 manifest identity가 다릅니다: {key}"
        )
    return manifest


def _snapshot_data(
    manifest: SourceSnapshotManifest, columns: list[str] | None
) -> SourceSnapshotData:
    """EMPTY는 table 없이, SUCCEEDED는 검증한 Silver와 함께 반환한다."""
    if manifest.status is SourceSnapshotStatus.EMPTY:
        return SourceSnapshotData(manifest=manifest, table=None)
    return SourceSnapshotData(
        manifest=manifest,
        table=_read_manifest_parquet(manifest, columns=columns),
    )


def _require_positive_lookback(lookback: timedelta) -> None:
    if type(lookback) is not timedelta or lookback <= timedelta(0):
        raise ValueError("lookback은 양수 timedelta여야 합니다.")


def _read_manifest_parquet(
    manifest: SourceSnapshotManifest,
    *,
    columns: list[str] | None,
) -> pa.Table:
    """Manifest URI의 content-addressed bytes와 행 수를 검증해 읽는다."""
    if manifest.silver_uri is None or manifest.silver_byte_sha256 is None:
        raise SourceSnapshotReadError(
            "SUCCEEDED manifest에 Silver artifact가 없습니다."
        )
    parsed = urlsplit(manifest.silver_uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket_name():
        raise SourceSnapshotReadError(
            f"source Silver bucket이 현재 런타임과 다릅니다: {manifest.silver_uri}"
        )
    key = parsed.path.removeprefix("/")
    return _read_content_addressed_parquet(
        key,
        checksum=manifest.silver_byte_sha256,
        expected_rows=manifest.counts.kept,
        columns=columns,
    )


def _read_content_addressed_parquet(
    key: str,
    *,
    checksum: str,
    expected_rows: int,
    columns: list[str] | None,
) -> pa.Table:
    """Exact key의 checksum·Parquet·행 수를 검증한다."""
    try:
        body = get_object_bytes(key)
    except (BotoCoreError, ClientError) as exc:
        raise SourceSnapshotReadError(
            f"source Silver를 읽을 수 없습니다: {key}"
        ) from exc
    if body is None:
        raise SourceSnapshotReadError(f"source Silver가 없습니다: {key}")
    if sha256_hex(body) != checksum:
        raise SourceSnapshotReadError(
            f"source Silver checksum이 manifest와 다릅니다: {key}"
        )
    try:
        table = pq.read_table(io.BytesIO(body), columns=columns)
    except (pa.ArrowException, OSError) as exc:
        raise SourceSnapshotReadError(
            f"source Silver Parquet을 읽을 수 없습니다: {key}"
        ) from exc
    if table.num_rows != expected_rows:
        raise SourceSnapshotReadError(
            f"source Silver 행 수가 manifest와 다릅니다: key={key}, "
            f"actual={table.num_rows}, expected={expected_rows}"
        )
    return table


def _validated_source_id(value: str) -> str:
    """Authority 경로에 쓸 source ID를 검증한다."""
    if type(value) is not str or _SOURCE_ID.fullmatch(value) is None:
        raise ValueError("source_id는 lowercase snake_case여야 합니다.")
    return value


def _aware_utc(value: datetime) -> datetime:
    """Aware datetime을 같은 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise TypeError("logical_dttm은 datetime이어야 합니다.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("logical_dttm은 timezone-aware datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _exact_manifest_key_from_uri(uri: str) -> str:
    """현재 bucket의 canonical source manifest URI에서 exact key를 반환한다."""
    if type(uri) is not str or not uri or "?" in uri or "#" in uri:
        raise ValueError("manifest_uri는 query와 fragment 없는 S3 URI여야 합니다.")
    try:
        parsed = urlsplit(uri)
    except ValueError as exc:
        raise ValueError("manifest_uri를 해석할 수 없습니다.") from exc
    if parsed.scheme != "s3" or parsed.netloc != bucket_name():
        raise ValueError("manifest_uri bucket이 현재 런타임과 다릅니다.")
    key = parsed.path.removeprefix("/")
    if parsed.path != f"/{key}":
        raise ValueError("manifest_uri가 canonical source manifest 경로가 아닙니다.")
    try:
        layout.parse_source_snapshot_manifest_key(key)
    except ValueError as exc:
        raise ValueError("manifest_uri가 canonical source manifest 경로가 아닙니다.") from exc
    return key


def _manifest_key(manifest: SourceSnapshotManifest) -> str:
    """Manifest identity의 canonical authority key를 반환한다."""
    return layout.source_snapshot_manifest_key(
        manifest.source_id, manifest.logical_dttm, manifest.revision_no
    )


def _logical_from_key(key: str, source_id: str) -> datetime:
    """해당 source의 canonical authority key에서 logical UTC 시각을 읽는다."""
    try:
        parsed = layout.parse_source_snapshot_manifest_key(key)
    except ValueError as exc:
        raise SourceSnapshotReadError(
            f"source authority key 형식이 잘못됐습니다: {key}"
        ) from exc
    if parsed.source_id != source_id:
        raise SourceSnapshotReadError(f"source authority key가 prefix 밖입니다: {key}")
    return parsed.logical_dttm
