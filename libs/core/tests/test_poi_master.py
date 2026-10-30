"""POI Master exact ref, activation pointer와 Table 읽기 계약을 검증한다."""

import io
import json
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import EndpointConnectionError

import core.poi_master as poi_master_module
from core.gold_publication.canonical import sha256_hex
from core.gold_publication.errors import ObjectStoreAccessError
from core.gold_publication.storage import S3ImmutableObjectStore
from core.poi_master import (
    POI_MASTER_ACTIVATION_SCHEMA_VERSION,
    POI_MASTER_POINTER_PREFIX,
    POI_MASTER_READABLE_SCHEMA_VERSIONS,
    POI_MASTER_SCHEMA_VERSION,
    POI_MASTER_SOURCE_ID,
    PoiMasterActivation,
    PoiMasterActivationError,
    PoiMasterContractError,
    PoiMasterError,
    PoiMasterReadError,
    PoiMasterRef,
    activate_poi_master,
    read_poi_master,
    resolve_poi_master,
)
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)

TEST_BUCKET = "test-bucket"
BASE_TIME = datetime(2026, 8, 25, 0, 0, tzinfo=UTC)
MANIFEST_PREFIX = "source_snapshot_manifest/poi_master"

POI_SCHEMA = pa.schema(
    [
        ("AREA_CD", pa.string()),
        ("AREA_NM", pa.string()),
        ("CATEGORY", pa.string()),
        ("ENG_NM", pa.string()),
        ("SOURCE_NO", pa.int64()),
        ("GEOMETRY_WKB", pa.binary()),
        ("AREA_M2", pa.float64()),
    ],
    metadata={
        b"geometry_crs": b"EPSG:5179",
        b"poi_master_schema_version": POI_MASTER_SCHEMA_VERSION.encode("utf-8"),
    },
)


def _polygon_wkb(
    points: tuple[tuple[float, float], ...] | None = None,
) -> bytes:
    """주어진 exterior ring의 canonical little-endian 2D Polygon WKB를 만든다."""
    if points is None:
        points = (
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        )
    return _polygon_rings_wkb((points,))


def _polygon_rings_wkb(
    rings: tuple[tuple[tuple[float, float], ...], ...],
) -> bytes:
    """Exterior와 hole ring을 canonical little-endian 2D Polygon WKB로 만든다."""
    return b"".join(
        [
            struct.pack("<BII", 1, 3, len(rings)),
            *(
                b"".join(
                    [
                        struct.pack("<I", len(ring)),
                        *(struct.pack("<dd", x, y) for x, y in ring),
                    ]
                )
                for ring in rings
            ),
        ]
    )


def _poi_row(**overrides: object) -> dict[str, object]:
    """행 단위 계약 테스트에 쓸 기본 POI 행을 반환한다."""
    row: dict[str, object] = {
        "AREA_CD": "POI001",
        "AREA_NM": "장소",
        "CATEGORY": "관광특구",
        "ENG_NM": "Place",
        "SOURCE_NO": 1,
        "GEOMETRY_WKB": _polygon_wkb(),
        "AREA_M2": 4.0,
    }
    row.update(overrides)
    return row


def _poi_table(
    name: str = "장소",
    *,
    schema: pa.Schema = POI_SCHEMA,
    rows: list[dict[str, object]] | None = None,
    **overrides: object,
) -> pa.Table:
    """필드 또는 전체 행을 바꿀 수 있는 테스트 POI Master Table을 만든다."""
    if rows is None:
        row_overrides = {"AREA_NM": name, **overrides}
        rows = [_poi_row(**row_overrides)]
    return pa.Table.from_pylist(rows, schema=schema)


def _manifest_key(logical: datetime, revision: int) -> str:
    """테스트 logical identity의 canonical manifest key를 반환한다."""
    utc = logical.astimezone(UTC)
    compact = f"{utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z"
    return (
        f"{MANIFEST_PREFIX}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"
        f"logical={compact}/revision={revision:010d}.json"
    )


def _put_poi_snapshot(
    logical: datetime,
    table: pa.Table,
    *,
    revision: int = 0,
) -> PoiMasterRef:
    """테스트용 POI Master Silver와 exact source manifest를 저장한다."""
    output = io.BytesIO()
    pq.write_table(table, output)
    parquet_payload = output.getvalue()
    parquet_sha256 = sha256_hex(parquet_payload)
    silver_key = f"silver/poi_master/sha256={parquet_sha256}.parquet"
    manifest = build_source_snapshot_manifest(
        source_id=POI_MASTER_SOURCE_ID,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="poi-master-test-v1",
        silver_uri=f"s3://{TEST_BUCKET}/{silver_key}",
        silver_byte_sha256=parquet_sha256,
        counts=SourceSnapshotCounts(
            table.num_rows,
            table.num_rows,
            table.num_rows,
            0,
            0,
        ),
        planned_parts=("areas_zip", "list_xlsx"),
        completed_parts=("areas_zip", "list_xlsx"),
    )
    manifest_key = _manifest_key(logical, revision)
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(
        Bucket=TEST_BUCKET,
        Key=silver_key,
        Body=parquet_payload,
    )
    client.put_object(
        Bucket=TEST_BUCKET,
        Key=manifest_key,
        Body=manifest.canonical_bytes,
    )
    return PoiMasterRef(
        mode="s3",
        manifest_uri=f"s3://{TEST_BUCKET}/{manifest_key}",
        manifest_sha256=manifest.sha256,
    )


def _compact(value: datetime) -> str:
    """테스트 activation key의 UTC compact 시각을 반환한다."""
    utc = value.astimezone(UTC)
    return f"{utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z"


def _put_pointer(
    activated_at: datetime,
    manifest_sha256: str,
    *,
    manifest_uri: str | None = None,
    key_activated_at: datetime | None = None,
    payload: bytes | None = None,
) -> str:
    """본문 또는 key identity를 독립 제어할 수 있는 activation pointer를 쓴다."""
    uri = manifest_uri or (
        f"s3://{TEST_BUCKET}/{MANIFEST_PREFIX}/dt=2026-08-25/hh=00/"
        "logical=20260825T000000000000Z/revision=0000000000.json"
    )
    pointer = PoiMasterActivation(
        schema_version=POI_MASTER_ACTIVATION_SCHEMA_VERSION,
        source_id=POI_MASTER_SOURCE_ID,
        activated_at=activated_at,
        manifest_uri=uri,
        manifest_byte_sha256=manifest_sha256,
    )
    key_time = key_activated_at or activated_at
    key = f"{POI_MASTER_POINTER_PREFIX}activated={_compact(key_time)}.json"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=pointer.canonical_bytes if payload is None else payload,
    )
    return key


def _pointer_keys() -> list[str]:
    """현재 테스트 bucket의 activation pointer key를 정렬해 반환한다."""
    response = boto3.client("s3", region_name="us-east-1").list_objects_v2(
        Bucket=TEST_BUCKET,
        Prefix=POI_MASTER_POINTER_PREFIX,
    )
    return sorted(item["Key"] for item in response.get("Contents", []))


def test_ref_is_strict_exact_three_key_json_contract() -> None:
    static = PoiMasterRef(mode="static")
    static_document = {
        "mode": "static",
        "manifest_uri": None,
        "manifest_sha256": None,
    }
    assert static.as_dict() == static_document
    assert PoiMasterRef.from_dict(static_document) == static

    s3_ref = PoiMasterRef(
        mode="s3",
        manifest_uri=(
            f"s3://{TEST_BUCKET}/{MANIFEST_PREFIX}/dt=2026-08-25/hh=00/"
            "logical=20260825T000000000000Z/revision=0000000000.json"
        ),
        manifest_sha256="a" * 64,
    )
    assert PoiMasterRef.from_dict(s3_ref.as_dict()) == s3_ref

    invalid_documents = [
        {"mode": "static"},
        {**static_document, "extra": None},
        {**static_document, "mode": True},
        {**static_document, "manifest_uri": 1},
    ]
    for document in invalid_documents:
        with pytest.raises(PoiMasterContractError):
            PoiMasterRef.from_dict(document)
    with pytest.raises(PoiMasterContractError):
        PoiMasterRef(mode="static", manifest_sha256="a" * 64)
    with pytest.raises(PoiMasterContractError):
        PoiMasterRef(
            mode="s3",
            manifest_uri=s3_ref.manifest_uri,
            manifest_sha256="A" * 64,
        )


def test_resolve_without_eligible_pointer_returns_static_fallback() -> None:
    future = BASE_TIME + timedelta(days=1)
    _put_pointer(future, "a" * 64, payload=b"not-json")

    ref = resolve_poi_master(BASE_TIME)

    assert ref == PoiMasterRef(mode="static")
    with pytest.raises(PoiMasterReadError, match="Static"):
        read_poi_master(ref)


def test_resolve_selects_latest_at_or_before_as_of_and_ignores_future_body() -> None:
    first_at = BASE_TIME
    second_at = BASE_TIME + timedelta(hours=1)
    future_at = BASE_TIME + timedelta(hours=3)
    first_sha256 = "1" * 64
    second_sha256 = "2" * 64
    _put_pointer(first_at, first_sha256)
    _put_pointer(second_at, second_sha256)
    _put_pointer(future_at, "3" * 64, payload=b"not-json")

    selected = resolve_poi_master(BASE_TIME + timedelta(hours=2))

    assert selected.mode == "s3"
    assert selected.manifest_sha256 == second_sha256


def test_resolve_rejects_pointer_key_body_identity_mismatch() -> None:
    _put_pointer(
        BASE_TIME,
        "1" * 64,
        key_activated_at=BASE_TIME + timedelta(hours=1),
    )

    with pytest.raises(PoiMasterReadError, match="identity"):
        resolve_poi_master(BASE_TIME + timedelta(hours=1))


def test_resolve_rejects_noncanonical_pointer_json() -> None:
    fields = {
        "activated_at": "2026-08-25T00:00:00.000000Z",
        "manifest_byte_sha256": "1" * 64,
        "manifest_uri": (
            f"s3://{TEST_BUCKET}/{MANIFEST_PREFIX}/dt=2026-08-25/hh=00/"
            "logical=20260825T000000000000Z/revision=0000000000.json"
        ),
        "schema_version": POI_MASTER_ACTIVATION_SCHEMA_VERSION,
        "source_id": POI_MASTER_SOURCE_ID,
    }
    pretty_payload = json.dumps(fields, indent=2).encode("utf-8")
    _put_pointer(BASE_TIME, "1" * 64, payload=pretty_payload)

    with pytest.raises(PoiMasterReadError, match="손상"):
        resolve_poi_master(BASE_TIME)


def test_activation_pins_exact_revision_and_is_idempotent() -> None:
    old_ref = _put_poi_snapshot(BASE_TIME, _poi_table("이전"), revision=0)
    _put_poi_snapshot(BASE_TIME, _poi_table("수정"), revision=1)
    activated_at = BASE_TIME + timedelta(hours=1)
    assert old_ref.manifest_uri is not None
    assert old_ref.manifest_sha256 is not None

    first = activate_poi_master(
        activated_at=activated_at,
        manifest_uri=old_ref.manifest_uri,
        manifest_sha256=old_ref.manifest_sha256,
    )
    replay = activate_poi_master(
        activated_at=activated_at,
        manifest_uri=old_ref.manifest_uri,
        manifest_sha256=old_ref.manifest_sha256,
    )

    assert first == replay == old_ref
    assert resolve_poi_master(activated_at) == old_ref
    assert read_poi_master(old_ref).column("AREA_NM").to_pylist() == ["이전"]
    assert len(_pointer_keys()) == 1


def test_activation_rejects_different_manifest_in_same_time_slot() -> None:
    first_ref = _put_poi_snapshot(BASE_TIME, _poi_table("첫째"), revision=0)
    second_ref = _put_poi_snapshot(BASE_TIME, _poi_table("둘째"), revision=1)
    activated_at = BASE_TIME + timedelta(hours=1)
    assert first_ref.manifest_uri is not None
    assert first_ref.manifest_sha256 is not None
    assert second_ref.manifest_uri is not None
    assert second_ref.manifest_sha256 is not None
    activate_poi_master(
        activated_at=activated_at,
        manifest_uri=first_ref.manifest_uri,
        manifest_sha256=first_ref.manifest_sha256,
    )

    with pytest.raises(PoiMasterActivationError, match="같은 시각"):
        activate_poi_master(
            activated_at=activated_at,
            manifest_uri=second_ref.manifest_uri,
            manifest_sha256=second_ref.manifest_sha256,
        )

    assert resolve_poi_master(activated_at) == first_ref
    assert len(_pointer_keys()) == 1


def test_concurrent_activation_allows_only_one_manifest_per_time_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """두 writer가 동시에 시작해도 S3 단일 slot에는 승자 하나만 남긴다."""
    first_ref = _put_poi_snapshot(BASE_TIME, _poi_table("첫째"), revision=0)
    second_ref = _put_poi_snapshot(BASE_TIME, _poi_table("둘째"), revision=1)
    activated_at = BASE_TIME + timedelta(hours=1)
    barrier = threading.Barrier(2)
    original_put_once = S3ImmutableObjectStore.put_once

    def synchronized_put_once(
        self: S3ImmutableObjectStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        """두 activation writer의 conditional PUT 시작점을 맞춘다."""
        barrier.wait(timeout=5)
        return original_put_once(self, *args, **kwargs)

    monkeypatch.setattr(S3ImmutableObjectStore, "put_once", synchronized_put_once)

    def activate(ref: PoiMasterRef) -> PoiMasterRef:
        """주어진 exact manifest를 같은 activation slot에 게시한다."""
        assert ref.manifest_uri is not None
        assert ref.manifest_sha256 is not None
        return activate_poi_master(
            activated_at=activated_at,
            manifest_uri=ref.manifest_uri,
            manifest_sha256=ref.manifest_sha256,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(activate, ref) for ref in (first_ref, second_ref)]
    results: list[PoiMasterRef] = []
    failures: list[BaseException] = []
    for future in futures:
        try:
            results.append(future.result())
        except BaseException as exc:  # noqa: BLE001 - 경쟁 결과 자체를 분류한다.
            failures.append(exc)

    assert len(results) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], PoiMasterActivationError)
    assert resolve_poi_master(activated_at) == results[0]
    assert len(_pointer_keys()) == 1


def test_activation_reconciles_put_response_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """조건부 PUT이 반영된 뒤 응답만 유실된 경우 exact readback으로 성공 처리한다."""
    ref = _put_poi_snapshot(BASE_TIME, _poi_table())
    activated_at = BASE_TIME + timedelta(hours=1)

    class AppliedThenFailedStore(S3ImmutableObjectStore):
        """객체를 실제로 쓴 뒤 transport 실패를 흉내 내는 store다."""

        def put_once(self, *args: object, **kwargs: object) -> object:
            """실제 conditional PUT을 완료한 뒤 응답 유실 오류를 낸다."""
            super().put_once(*args, **kwargs)
            raise ObjectStoreAccessError("응답 유실")

    monkeypatch.setattr(
        poi_master_module,
        "S3ImmutableObjectStore",
        AppliedThenFailedStore,
    )
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None

    activated = activate_poi_master(
        activated_at=activated_at,
        manifest_uri=ref.manifest_uri,
        manifest_sha256=ref.manifest_sha256,
    )

    assert activated == ref
    assert len(_pointer_keys()) == 1


def test_read_validates_full_schema_and_crs_before_projection() -> None:
    ref = _put_poi_snapshot(BASE_TIME, _poi_table())

    full = read_poi_master(ref)
    projected = read_poi_master(ref, columns=["AREA_CD", "SOURCE_NO"])

    assert full.schema.remove_metadata().equals(POI_SCHEMA.remove_metadata())
    assert full.schema.metadata is not None
    assert full.schema.metadata[b"geometry_crs"] == b"EPSG:5179"
    assert (
        full.schema.metadata[b"poi_master_schema_version"]
        == POI_MASTER_SCHEMA_VERSION.encode("utf-8")
    )
    assert projected.column_names == ["AREA_CD", "SOURCE_NO"]
    assert projected.to_pylist() == [{"AREA_CD": "POI001", "SOURCE_NO": 1}]

    with pytest.raises(PoiMasterContractError, match="없는 컬럼"):
        read_poi_master(ref, columns=["UNKNOWN"])
    with pytest.raises(PoiMasterContractError, match="중복"):
        read_poi_master(ref, columns=["AREA_CD", "AREA_CD"])


def test_read_rejects_wrong_poi_master_schema_version() -> None:
    """물리 schema가 같아도 지원하지 않는 POI Master 계약 버전은 거부한다."""
    metadata = dict(POI_SCHEMA.metadata or {})
    metadata[b"poi_master_schema_version"] = b"poi-master-v0"
    ref = _put_poi_snapshot(
        BASE_TIME,
        _poi_table(schema=POI_SCHEMA.with_metadata(metadata)),
    )

    with pytest.raises(PoiMasterReadError, match="table 계약") as caught:
        read_poi_master(ref)

    assert caught.value.__cause__ is not None
    assert "poi_master_schema_version" in str(caught.value.__cause__)


def test_read_allows_explicitly_compatible_previous_schema_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """배포 전환 중 명시적으로 유지한 동일 물리 schema의 이전 버전은 읽을 수 있다."""
    previous_version = "poi-master-v0"
    metadata = dict(POI_SCHEMA.metadata or {})
    metadata[b"poi_master_schema_version"] = previous_version.encode("utf-8")
    ref = _put_poi_snapshot(
        BASE_TIME,
        _poi_table(schema=POI_SCHEMA.with_metadata(metadata)),
    )
    monkeypatch.setattr(
        poi_master_module,
        "POI_MASTER_READABLE_SCHEMA_VERSIONS",
        POI_MASTER_READABLE_SCHEMA_VERSIONS | {previous_version},
    )

    assert read_poi_master(ref).num_rows == 1


@pytest.mark.parametrize(
    ("table", "message"),
    [
        (_poi_table(AREA_CD=None), "AREA_CD"),
        (_poi_table(AREA_CD="poi001"), "AREA_CD"),
        (
            _poi_table(
                rows=[
                    _poi_row(AREA_CD="POI001", SOURCE_NO=1),
                    _poi_row(AREA_CD="POI001", SOURCE_NO=2),
                ]
            ),
            "AREA_CD는 고유",
        ),
        (
            _poi_table(
                rows=[
                    _poi_row(AREA_CD="POI002", SOURCE_NO=2),
                    _poi_row(AREA_CD="POI001", SOURCE_NO=1),
                ]
            ),
            "오름차순",
        ),
        (_poi_table(AREA_NM=" 장소"), "AREA_NM"),
        (_poi_table(CATEGORY=None), "CATEGORY"),
        (_poi_table(ENG_NM=""), "ENG_NM"),
        (_poi_table(SOURCE_NO=0), "SOURCE_NO"),
        (
            _poi_table(
                rows=[
                    _poi_row(AREA_CD="POI001", SOURCE_NO=1),
                    _poi_row(AREA_CD="POI002", SOURCE_NO=1),
                ]
            ),
            "SOURCE_NO는 고유",
        ),
        (_poi_table(AREA_M2=float("nan")), "AREA_M2"),
        (_poi_table(AREA_M2=0.0), "AREA_M2"),
        (_poi_table(GEOMETRY_WKB=b"not-wkb"), "GEOMETRY_WKB"),
        (_poi_table(AREA_M2=5.0), "geometry 면적"),
    ],
)
def test_read_rejects_row_contract_corruption(
    table: pa.Table,
    message: str,
) -> None:
    """Schema가 맞더라도 소비 불가능한 행은 exact read 단계에서 거부한다."""
    ref = _put_poi_snapshot(BASE_TIME, table)

    with pytest.raises(PoiMasterReadError, match="table 계약") as caught:
        read_poi_master(ref)

    assert caught.value.__cause__ is not None
    assert message in str(caught.value.__cause__)


def test_table_contract_rejects_empty_master() -> None:
    """Core의 최종 Table gate는 빈 POI Master 자체를 허용하지 않는다."""
    with pytest.raises(PoiMasterContractError, match="한 행"):
        poi_master_module._validate_poi_master_table(_poi_table(rows=[]))


def test_activation_rejects_invalid_table_before_pointer_creation() -> None:
    """행 계약을 어긴 manifest는 activation pointer를 만들기 전에 차단한다."""
    ref = _put_poi_snapshot(BASE_TIME, _poi_table(GEOMETRY_WKB=b"bad"))
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None

    with pytest.raises(PoiMasterReadError, match="table 계약"):
        activate_poi_master(
            activated_at=BASE_TIME + timedelta(hours=1),
            manifest_uri=ref.manifest_uri,
            manifest_sha256=ref.manifest_sha256,
        )

    assert _pointer_keys() == []


def test_activation_rejects_self_intersecting_polygon_before_pointer() -> None:
    """구조와 면적이 맞아도 자기교차 Polygon은 중앙 activation gate가 거부한다."""
    self_intersecting = (
        (0.0, 0.0),
        (4.0, 0.0),
        (0.0, 3.0),
        (3.0, 3.0),
        (0.0, 0.0),
    )
    ref = _put_poi_snapshot(
        BASE_TIME,
        _poi_table(
            GEOMETRY_WKB=_polygon_wkb(self_intersecting),
            AREA_M2=1.5,
        ),
    )
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None
    # 구조·면적 검증만 필요한 일반 exact read는 통과하지만 discoverable activation은
    # 더 비싼 topology gate를 한 번 거친다.
    assert read_poi_master(ref).num_rows == 1

    with pytest.raises(PoiMasterActivationError, match="위상"):
        activate_poi_master(
            activated_at=BASE_TIME + timedelta(hours=1),
            manifest_uri=ref.manifest_uri,
            manifest_sha256=ref.manifest_sha256,
        )

    assert _pointer_keys() == []


def test_activation_accepts_consecutive_duplicate_polygon_vertex() -> None:
    """면적과 위상을 바꾸지 않는 원천 연속 중복점은 활성화를 막지 않는다."""
    duplicate_vertex = (
        (0.0, 0.0),
        (2.0, 0.0),
        (2.0, 0.0),
        (2.0, 2.0),
        (0.0, 2.0),
        (0.0, 0.0),
    )
    ref = _put_poi_snapshot(
        BASE_TIME,
        _poi_table(GEOMETRY_WKB=_polygon_wkb(duplicate_vertex)),
    )
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None

    activated = activate_poi_master(
        activated_at=BASE_TIME + timedelta(hours=1),
        manifest_uri=ref.manifest_uri,
        manifest_sha256=ref.manifest_sha256,
    )

    assert activated == ref
    assert resolve_poi_master(BASE_TIME + timedelta(hours=1)) == ref


def test_activation_accepts_hole_touching_exterior_at_one_point() -> None:
    """OGC가 허용하는 hole과 exterior의 단일 점 접촉은 활성화를 막지 않는다."""
    exterior = (
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
    )
    touching_hole = (
        (4.0, 2.0),
        (3.0, 1.5),
        (3.0, 2.5),
        (4.0, 2.0),
    )
    ref = _put_poi_snapshot(
        BASE_TIME,
        _poi_table(
            GEOMETRY_WKB=_polygon_rings_wkb((exterior, touching_hole)),
            AREA_M2=15.5,
        ),
    )
    assert ref.manifest_uri is not None
    assert ref.manifest_sha256 is not None

    activated = activate_poi_master(
        activated_at=BASE_TIME + timedelta(hours=1),
        manifest_uri=ref.manifest_uri,
        manifest_sha256=ref.manifest_sha256,
    )

    assert activated == ref
    assert resolve_poi_master(BASE_TIME + timedelta(hours=1)) == ref


def test_resolve_wraps_s3_list_and_pointer_read_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raw botocore 예외가 public POI Master read 경계 밖으로 새지 않게 한다."""

    def fail_list(_prefix: str) -> list[str]:
        """S3 LIST transport 실패를 발생시킨다."""
        raise EndpointConnectionError(endpoint_url="https://s3.invalid")

    monkeypatch.setattr(poi_master_module, "list_keys", fail_list)
    with pytest.raises(PoiMasterReadError, match="목록") as list_error:
        resolve_poi_master(BASE_TIME)
    assert isinstance(list_error.value.__cause__, EndpointConnectionError)

    key = _put_pointer(BASE_TIME, "1" * 64)
    monkeypatch.setattr(poi_master_module, "list_keys", lambda _prefix: [key])

    def fail_get(_key: str) -> bytes | None:
        """S3 GET transport 실패를 발생시킨다."""
        raise EndpointConnectionError(endpoint_url="https://s3.invalid")

    monkeypatch.setattr(poi_master_module, "get_object_bytes", fail_get)
    with pytest.raises(PoiMasterReadError, match="읽을 수 없습니다") as get_error:
        resolve_poi_master(BASE_TIME)
    assert isinstance(get_error.value.__cause__, EndpointConnectionError)


@pytest.mark.parametrize(
    "table",
    [
        _poi_table(
            schema=pa.schema(
                [
                    ("AREA_CD", pa.string()),
                    ("AREA_NM", pa.string()),
                    ("CATEGORY", pa.string()),
                    ("ENG_NM", pa.string()),
                    ("SOURCE_NO", pa.int32()),
                    ("GEOMETRY_WKB", pa.binary()),
                    ("AREA_M2", pa.float64()),
                ],
                metadata={
                    b"geometry_crs": b"EPSG:5179",
                    b"poi_master_schema_version": POI_MASTER_SCHEMA_VERSION.encode(
                        "utf-8"
                    ),
                },
            )
        ),
        _poi_table(schema=POI_SCHEMA.remove_metadata()),
    ],
)
def test_read_rejects_schema_or_crs_corruption(table: pa.Table) -> None:
    ref = _put_poi_snapshot(BASE_TIME, table)

    with pytest.raises(PoiMasterReadError, match="table 계약") as caught:
        read_poi_master(ref)
    assert isinstance(caught.value, PoiMasterError)
