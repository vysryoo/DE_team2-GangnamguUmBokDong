"""Gold publication immutable S3 object 경계를 검증한다."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from core.gold_publication.canonical import sha256_hex
from core.gold_publication.errors import (
    HashFormatError,
    InvalidObjectUriError,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
    ObjectNotCanonicalError,
    ObjectPartialReadError,
    ObjectStoreAccessError,
)
from core.gold_publication.storage import (
    ImmutablePutOutcome,
    S3ImmutableObjectStore,
)

_BUCKET = "test-bucket"


class _ShadowUri(str):
    """query 검사를 숨기는 악성 str subclass를 표현한다."""

    def __contains__(self, _value: object) -> bool:
        """URI delimiter 탐색을 항상 실패시킨다."""
        return False


@pytest.fixture
def s3_client() -> Any:
    """moto가 가로채는 boto3 S3 client를 제공한다."""
    return boto3.client("s3", region_name="us-east-1")


def _put_fixture(client: Any, key: str, payload: bytes) -> None:
    """조건부 계약과 무관한 테스트 선행 객체를 저장한다."""
    client.put_object(Bucket=_BUCKET, Key=key, Body=payload)


def _client_error(code: str, status_code: int, operation: str) -> ClientError:
    """원하는 S3 오류 코드의 botocore ClientError를 만든다."""
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status_code},
        },
        operation,
    )


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "https://test-bucket/object.json",
        "s3://",
        "s3://test-bucket",
        "s3://test-bucket/",
        "s3://user@test-bucket/object.json",
        "s3://test-bucket:9000/object.json",
        "s3://test-bucket/object.json?versionId=1",
        "s3://test-bucket/object.json#fragment",
        "s3://test-bucket/object name.json",
        "s3://[invalid/object.json",
        "s3://test-bucket/object\x00.json",
    ],
)
def test_read_rejects_invalid_uri_before_client_call(uri: str) -> None:
    """정확한 S3 object URI가 아니면 네트워크 호출 전에 거부한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(InvalidObjectUriError):
        store.read_bytes(uri, "0" * 64)

    client.get_object.assert_not_called()


def test_read_rejects_str_subclass_before_client_call() -> None:
    """override로 query 판정을 숨길 수 있는 str subclass를 거부한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(InvalidObjectUriError):
        store.read_bytes(
            _ShadowUri("s3://test-bucket/object.json?shadow"),
            "0" * 64,
        )

    client.get_object.assert_not_called()


def test_read_rejects_non_lowercase_hash_before_client_call() -> None:
    """expected checksum은 정확히 64자리 lowercase hex여야 한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(HashFormatError):
        store.read_bytes("s3://test-bucket/object.bin", "A" * 64)

    client.get_object.assert_not_called()


def test_read_returns_exact_object_bytes_with_injected_client(s3_client: Any) -> None:
    """주입한 boto3 client로 정확한 bucket과 key의 bytes만 읽는다."""
    payload = b"immutable-payload"
    _put_fixture(s3_client, "artifacts/output.bin", payload)
    store = S3ImmutableObjectStore(client=s3_client)

    result = store.read_bytes(
        "s3://test-bucket/artifacts/output.bin",
        sha256_hex(payload),
    )

    assert result == payload


def test_default_client_uses_existing_environment_configuration() -> None:
    """client를 주입하지 않아도 core.s3의 환경변수 기반 client를 사용한다."""
    payload = b"default-client"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=_BUCKET,
        Key="artifacts/default.bin",
        Body=payload,
    )

    result = S3ImmutableObjectStore().read_bytes(
        "s3://test-bucket/artifacts/default.bin",
        sha256_hex(payload),
    )

    assert result == payload


def test_read_never_falls_back_to_matching_prefix(s3_client: Any) -> None:
    """exact key가 없으면 prefix 아래 part가 있어도 결합해 읽지 않는다."""
    payload = b"part"
    _put_fixture(s3_client, "artifacts/table/part-00000.parquet", payload)
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(ObjectMissingError):
        store.read_bytes("s3://test-bucket/artifacts/table", sha256_hex(payload))


def test_read_missing_object_raises_domain_error(s3_client: Any) -> None:
    """정확한 object가 없으면 None 대신 공통 missing 예외를 발생시킨다."""
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(ObjectMissingError, match="s3://test-bucket/missing.bin"):
        store.read_bytes("s3://test-bucket/missing.bin", "0" * 64)


def test_read_rejects_checksum_mismatch(s3_client: Any) -> None:
    """실제 object bytes의 SHA-256이 기대값과 다르면 hard fail한다."""
    _put_fixture(s3_client, "artifacts/mismatch.bin", b"actual")
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(ObjectChecksumMismatchError, match="expected=.*actual="):
        store.read_bytes("s3://test-bucket/artifacts/mismatch.bin", "0" * 64)


def test_read_rejects_partial_body() -> None:
    """ContentLength보다 짧은 응답은 완전한 immutable bytes로 인정하지 않는다."""
    body = Mock()
    body.read.return_value = b"short"
    client = Mock()
    client.get_object.return_value = {"Body": body, "ContentLength": 10}
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(ObjectPartialReadError, match="응답 길이"):
        store.read_bytes("s3://test-bucket/partial.bin", sha256_hex(b"short"))


def test_read_accepts_exact_canonical_json(s3_client: Any) -> None:
    """canonical JSON 요청은 canonical 원본 bytes를 그대로 반환한다."""
    payload = b'{"artifacts":[],"schema_version":"gold-artifact-set-v1"}'
    _put_fixture(s3_client, "manifests/artifact-set.json", payload)
    store = S3ImmutableObjectStore(client=s3_client)

    result = store.read_bytes(
        "s3://test-bucket/manifests/artifact-set.json",
        sha256_hex(payload),
        require_canonical_json=True,
    )

    assert result == payload


def test_read_rejects_noncanonical_json_even_with_matching_checksum(
    s3_client: Any,
) -> None:
    """유효한 JSON이어도 공백 등 원본 bytes가 canonical과 다르면 거부한다."""
    payload = b'{ "schema_version": "gold-artifact-set-v1", "artifacts": [] }'
    _put_fixture(s3_client, "manifests/noncanonical.json", payload)
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(ObjectNotCanonicalError) as caught:
        store.read_bytes(
            "s3://test-bucket/manifests/noncanonical.json",
            sha256_hex(payload),
            require_canonical_json=True,
        )

    assert caught.value.__cause__ is not None


def test_put_once_uses_if_none_match_condition() -> None:
    """새 immutable object는 무조건 IfNoneMatch 별표 조건으로만 쓴다."""
    client = Mock()
    client.put_object.return_value = {}
    store = S3ImmutableObjectStore(client=client)

    outcome = store.put_once("s3://test-bucket/new.bin", b"new")

    assert outcome is ImmutablePutOutcome.CREATED
    client.put_object.assert_called_once_with(
        Bucket=_BUCKET,
        Key="new.bin",
        Body=b"new",
        IfNoneMatch="*",
    )


def test_put_once_creates_new_object(s3_client: Any) -> None:
    """비어 있는 URI에는 payload를 생성하고 CREATED를 반환한다."""
    payload = b"new immutable bytes"
    store = S3ImmutableObjectStore(client=s3_client)

    outcome = store.put_once(
        "s3://test-bucket/artifacts/new.bin",
        payload,
        expected_sha256=sha256_hex(payload),
    )

    assert outcome is ImmutablePutOutcome.CREATED
    assert (
        s3_client.get_object(Bucket=_BUCKET, Key="artifacts/new.bin")["Body"].read()
        == payload
    )


def test_put_once_treats_same_existing_bytes_as_safe_retry(s3_client: Any) -> None:
    """동일 URI의 동일 bytes는 overwrite 없이 안전한 retry 결과를 반환한다."""
    payload = b"stable"
    _put_fixture(s3_client, "artifacts/stable.bin", payload)
    store = S3ImmutableObjectStore(client=s3_client)

    outcome = store.put_once("s3://test-bucket/artifacts/stable.bin", payload)

    assert outcome is ImmutablePutOutcome.ALREADY_EXISTS
    assert (
        s3_client.get_object(Bucket=_BUCKET, Key="artifacts/stable.bin")["Body"].read()
        == payload
    )


def test_put_once_rejects_different_existing_bytes_without_overwrite(
    s3_client: Any,
) -> None:
    """동일 URI의 다른 bytes는 collision으로 실패하고 기존 bytes를 보존한다."""
    existing = b"existing"
    _put_fixture(s3_client, "artifacts/collision.bin", existing)
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(
        ObjectCollisionError, match="existing_sha256=.*incoming_sha256="
    ):
        store.put_once("s3://test-bucket/artifacts/collision.bin", b"incoming")

    stored = s3_client.get_object(Bucket=_BUCKET, Key="artifacts/collision.bin")[
        "Body"
    ].read()
    assert stored == existing


def test_put_verified_returns_readback_bytes(s3_client: Any) -> None:
    """새 object를 쓰고 readback으로 확인한 bytes를 반환한다."""
    store = S3ImmutableObjectStore(client=s3_client)

    readback = store.put_verified("s3://test-bucket/artifacts/verified.bin", b"v")

    assert readback == b"v"


def test_put_verified_accepts_lost_put_response(s3_client: Any) -> None:
    """PUT이 반영된 뒤 응답만 유실돼도 readback이 같으면 성공한다."""

    class _AppliedThenFailedStore(S3ImmutableObjectStore):
        def put_once(self, *args: Any, **kwargs: Any) -> ImmutablePutOutcome:
            super().put_once(*args, **kwargs)
            raise ObjectStoreAccessError("응답 유실")

    store = _AppliedThenFailedStore(client=s3_client)

    assert store.put_verified("s3://test-bucket/artifacts/lost.bin", b"x") == b"x"


def test_put_verified_raises_put_error_when_readback_fails(s3_client: Any) -> None:
    """다른 bytes와 충돌하면 readback 오류가 아니라 쓰기 오류를 던진다."""
    _put_fixture(s3_client, "artifacts/conflict.bin", b"existing")
    store = S3ImmutableObjectStore(client=s3_client)

    with pytest.raises(ObjectCollisionError):
        store.put_verified("s3://test-bucket/artifacts/conflict.bin", b"incoming")


def test_put_once_validates_expected_checksum_before_client_call() -> None:
    """payload checksum 불일치는 조건부 PUT을 호출하기 전에 거부한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(ObjectChecksumMismatchError):
        store.put_once(
            "s3://test-bucket/artifacts/mismatch.bin",
            b"payload",
            expected_sha256="0" * 64,
        )

    client.put_object.assert_not_called()


def test_put_once_rejects_non_lowercase_hash_before_client_call() -> None:
    """쓰기 expected checksum도 정확한 lowercase 형식부터 검증한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(HashFormatError):
        store.put_once(
            "s3://test-bucket/artifacts/output.bin",
            b"payload",
            expected_sha256="A" * 64,
        )

    client.put_object.assert_not_called()


def test_put_once_rejects_noncanonical_json_before_client_call() -> None:
    """noncanonical JSON payload는 object store에 일부라도 쓰기 전에 거부한다."""
    client = Mock()
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(ObjectNotCanonicalError):
        store.put_once(
            "s3://test-bucket/manifests/noncanonical.json",
            b'{"b": 2, "a": 1}',
            require_canonical_json=True,
        )

    client.put_object.assert_not_called()


def test_read_wraps_unexpected_s3_error() -> None:
    """권한 등 예상하지 않은 S3 오류를 공통 access 예외로 전달한다."""
    client = Mock()
    client.get_object.side_effect = _client_error("AccessDenied", 403, "GetObject")
    store = S3ImmutableObjectStore(client=client)

    with pytest.raises(ObjectStoreAccessError, match="AccessDenied") as caught:
        store.read_bytes("s3://test-bucket/private.bin", "0" * 64)

    assert isinstance(caught.value.__cause__, ClientError)
