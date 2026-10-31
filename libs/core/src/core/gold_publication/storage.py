"""Gold publication immutable object의 정확한 S3 입출력을 제공한다."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError, IncompleteReadError

from ..s3 import _client as default_s3_client
from .canonical import parse_canonical_json, sha256_hex, validate_sha256_hex
from .errors import (
    CanonicalParseError,
    ImmutableObjectError,
    InvalidObjectUriError,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
    ObjectNotCanonicalError,
    ObjectPartialReadError,
    ObjectStoreAccessError,
)

_MISSING_ERROR_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_CONDITIONAL_CONFLICT_CODES = frozenset(
    {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}
)
_CONDITIONAL_RETRY_LIMIT = 2


class S3Client(Protocol):
    """immutable object store가 사용하는 최소 boto3 S3 client 계약이다."""

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """S3 객체 하나를 정확한 bucket과 key로 읽는다."""
        ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        """S3 객체 하나를 조건부로 쓴다."""
        ...


class ImmutablePutOutcome(StrEnum):
    """immutable object 조건부 쓰기의 성공 결과를 나타낸다."""

    CREATED = "created"
    ALREADY_EXISTS = "already_exists"


class ImmutableObjectStore(Protocol):
    """publisher가 의존하는 immutable object 입출력 경계다."""

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """URI의 정확한 bytes를 읽고 checksum과 선택적 canonical JSON을 검증한다."""
        ...

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """URI가 비었을 때만 쓰고 기존 bytes와 같으면 안전한 재시도로 판정한다."""
        ...


@dataclass(frozen=True, slots=True)
class _S3Location:
    """검증된 S3 bucket과 정확한 object key를 보관한다."""

    bucket: str
    key: str


class S3ImmutableObjectStore:
    """boto3 S3 client로 immutable publication object를 처리한다."""

    def __init__(self, client: S3Client | None = None) -> None:
        """주입된 client 또는 기존 환경변수 기반 기본 client를 사용한다."""
        self._client = (
            client if client is not None else cast(S3Client, default_s3_client())
        )

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """URI의 단일 객체 bytes를 checksum까지 검증해 반환한다.

        args:
            uri: prefix가 아닌 정확한 ``s3://bucket/key`` URI
            expected_sha256: 기대하는 64자리 lowercase SHA-256
            require_canonical_json: true면 원본 bytes 자체가 canonical JSON인지 검증
        returns:
            checksum과 선택한 canonical 검증을 통과한 원본 bytes
        raises:
            InvalidObjectUriError: URI가 정확한 S3 object를 나타내지 않을 때
            HashFormatError: expected_sha256 형식이 contract와 다를 때
            ObjectMissingError: 정확한 object key가 존재하지 않을 때
            ObjectPartialReadError: 응답 본문을 완전하게 읽지 못했을 때
            ObjectChecksumMismatchError: 실제 bytes의 SHA-256이 기대값과 다를 때
            ObjectNotCanonicalError: canonical JSON을 요청했지만 원본 bytes가 다를 때
            ObjectStoreAccessError: 그 밖의 object store 접근 오류가 발생했을 때
        """
        location = _parse_s3_uri(uri)
        expected = validate_sha256_hex(expected_sha256)
        payload = self._read_exact(uri, location)
        actual = sha256_hex(payload)
        if actual != expected:
            raise ObjectChecksumMismatchError(
                f"immutable object checksum이 다릅니다: uri={uri}, "
                f"expected={expected}, actual={actual}"
            )
        if require_canonical_json:
            _validate_canonical_json(uri, payload)
        return payload

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """``If-None-Match: *``로 object를 한 번만 생성한다.

        이미 같은 bytes가 있으면 안전한 retry로 ``ALREADY_EXISTS``를 반환한다. 같은
        URI에 다른 bytes가 있으면 기존 값을 덮어쓰지 않고 hard fail한다.

        args:
            uri: 쓸 정확한 ``s3://bucket/key`` URI
            payload: 저장할 원본 bytes
            expected_sha256: 주어지면 payload와 일치해야 하는 lowercase SHA-256
            require_canonical_json: true면 쓰기 전에 canonical JSON bytes인지 검증
        returns:
            새로 생성했으면 CREATED, 같은 bytes가 이미 있으면 ALREADY_EXISTS
        raises:
            InvalidObjectUriError: URI가 정확한 S3 object를 나타내지 않을 때
            HashFormatError: expected_sha256 형식이 contract와 다를 때
            ObjectChecksumMismatchError: payload가 expected_sha256과 다를 때
            ObjectNotCanonicalError: canonical JSON을 요청했지만 payload가 다를 때
            ObjectCollisionError: 같은 URI에 다른 bytes가 이미 있을 때
            ObjectStoreAccessError: 그 밖의 object store 접근 오류가 발생했을 때
        """
        location = _parse_s3_uri(uri)
        actual_sha256 = _validated_payload_sha256(
            uri, payload, expected_sha256, require_canonical_json
        )
        for attempt in range(_CONDITIONAL_RETRY_LIMIT):
            if not self._put_if_absent(uri, location, payload):
                return ImmutablePutOutcome.CREATED
            # 충돌 직후 기존 object가 사라진 경우만 조건부 PUT을 다시 시도함
            existing = self._read_after_conflict(
                uri, location, is_last_attempt=attempt + 1 == _CONDITIONAL_RETRY_LIMIT
            )
            if existing is None:
                continue
            if existing == payload:
                return ImmutablePutOutcome.ALREADY_EXISTS
            raise ObjectCollisionError(
                f"immutable URI에 다른 bytes가 이미 있습니다: uri={uri}, "
                f"existing_sha256={sha256_hex(existing)}, incoming_sha256={actual_sha256}"
            )

        raise ObjectStoreAccessError(
            f"immutable object 조건부 쓰기를 완료하지 못했습니다: {uri}"
        )

    def _read_after_conflict(
        self, uri: str, location: _S3Location, *, is_last_attempt: bool
    ) -> bytes | None:
        """조건부 쓰기 충돌 뒤 기존 bytes를 읽는다. 사라졌으면 재시도할 수 있게 None을 반환한다."""
        try:
            return self._read_exact(uri, location)
        except ObjectMissingError as exc:
            if not is_last_attempt:
                return None
            raise ObjectStoreAccessError(
                f"조건부 쓰기 충돌 뒤 object를 확인할 수 없습니다: uri={uri}"
            ) from exc

    def put_verified(
        self,
        uri: str,
        payload: bytes,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """object를 한 번만 쓰고 같은 URI를 checksum으로 다시 읽어 확인한다.

        downstream manifest가 아직 완성되지 않은 object를 가리키지 않도록, 반환 전에
        exact bytes를 읽어 확인한다. 조건부 PUT이 반영된 뒤 응답만 유실되는 경우가
        있어, 쓰기 오류가 나도 readback이 같은 bytes면 성공으로 본다.

        args:
            uri: 쓸 정확한 ``s3://bucket/key`` URI
            payload: 저장할 원본 bytes
            require_canonical_json: true면 쓰기·읽기 모두 canonical JSON인지 검증
        returns:
            readback으로 확인한 bytes
        raises:
            ImmutableObjectError: readback이 실패했을 때. 쓰기 오류가 있었으면 그 오류를 던진다
        """
        payload_sha256 = sha256_hex(payload)
        put_error: ImmutableObjectError | None = None
        try:
            self.put_once(
                uri,
                payload,
                expected_sha256=payload_sha256,
                require_canonical_json=require_canonical_json,
            )
        except ImmutableObjectError as exc:
            put_error = exc
        try:
            return self.read_bytes(
                uri,
                payload_sha256,
                require_canonical_json=require_canonical_json,
            )
        except ImmutableObjectError as exc:
            if put_error is not None:
                raise put_error from exc
            raise

    def _read_exact(self, uri: str, location: _S3Location) -> bytes:
        """prefix fallback 없이 정확한 bucket/key의 전체 bytes만 읽는다."""
        try:
            response = self._client.get_object(Bucket=location.bucket, Key=location.key)
        except ClientError as exc:
            error_code, status_code = _client_error(exc)
            if error_code in _MISSING_ERROR_CODES or status_code == 404:
                raise ObjectMissingError(f"immutable object가 없습니다: {uri}") from exc
            raise ObjectStoreAccessError(
                f"immutable object 읽기에 실패했습니다: uri={uri}, code={error_code}"
            ) from exc
        except BotoCoreError as exc:
            raise ObjectStoreAccessError(
                f"immutable object 읽기에 실패했습니다: {uri}"
            ) from exc

        body: Any | None = None
        try:
            body = response["Body"]
            content_length = int(response["ContentLength"])
            payload = body.read()
        except IncompleteReadError as exc:
            raise ObjectPartialReadError(
                f"immutable object를 일부만 읽었습니다: {uri}"
            ) from exc
        except (KeyError, TypeError, ValueError, OSError, BotoCoreError) as exc:
            raise ObjectPartialReadError(
                f"immutable object의 완전한 응답 본문을 확인할 수 없습니다: {uri}"
            ) from exc
        finally:
            if body is not None:
                try:
                    body.close()
                except (AttributeError, OSError, BotoCoreError):
                    pass

        if (
            type(payload) is not bytes
            or content_length < 0
            or len(payload) != content_length
        ):
            actual_length = len(payload) if type(payload) is bytes else "non-bytes"
            raise ObjectPartialReadError(
                f"immutable object 응답 길이가 다릅니다: uri={uri}, "
                f"expected={content_length}, actual={actual_length}"
            )
        return payload

    def _put_if_absent(
        self,
        uri: str,
        location: _S3Location,
        payload: bytes,
    ) -> bool:
        """조건부 PUT을 실행하고 기존 object와 충돌했는지 반환한다."""
        try:
            self._client.put_object(
                Bucket=location.bucket,
                Key=location.key,
                Body=payload,
                IfNoneMatch="*",
            )
        except ClientError as exc:
            error_code, status_code = _client_error(exc)
            if error_code in _CONDITIONAL_CONFLICT_CODES or status_code in {409, 412}:
                return True
            raise ObjectStoreAccessError(
                f"immutable object 조건부 쓰기에 실패했습니다: uri={uri}, code={error_code}"
            ) from exc
        except BotoCoreError as exc:
            raise ObjectStoreAccessError(
                f"immutable object 조건부 쓰기에 실패했습니다: {uri}"
            ) from exc
        return False


def _parse_s3_uri(uri: str) -> _S3Location:
    """정확한 S3 object URI를 bucket과 key로 분리한다."""
    if (
        type(uri) is not str
        or not uri
        or any(
            character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
            for character in uri
        )
    ):
        raise InvalidObjectUriError(
            "immutable object URI는 공백이나 제어 문자 없는 문자열이어야 합니다."
        )
    if "?" in uri or "#" in uri:
        raise InvalidObjectUriError(
            "immutable object URI에는 query나 fragment를 쓸 수 없습니다."
        )

    try:
        parsed = urlsplit(uri)
    except ValueError as exc:
        raise InvalidObjectUriError(
            "immutable object URI는 정확한 s3://bucket/key 형식이어야 합니다."
        ) from exc
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or "@" in parsed.netloc
        or ":" in parsed.netloc
    ):
        raise InvalidObjectUriError(
            "immutable object URI는 정확한 s3://bucket/key 형식이어야 합니다."
        )
    return _S3Location(bucket=parsed.netloc, key=parsed.path[1:])


def _validated_payload_sha256(
    uri: str,
    payload: bytes,
    expected_sha256: str | None,
    require_canonical_json: bool,
) -> str:
    """쓰기 전 payload의 타입, 기대 checksum, canonical JSON을 확인하고 SHA-256을 반환한다."""
    if type(payload) is not bytes:
        raise TypeError("immutable object payload는 bytes여야 합니다.")
    actual_sha256 = sha256_hex(payload)
    if expected_sha256 is not None:
        expected = validate_sha256_hex(expected_sha256)
        if actual_sha256 != expected:
            raise ObjectChecksumMismatchError(
                f"immutable object payload checksum이 다릅니다: uri={uri}, "
                f"expected={expected}, actual={actual_sha256}"
            )
    if require_canonical_json:
        _validate_canonical_json(uri, payload)
    return actual_sha256


def _validate_canonical_json(uri: str, payload: bytes) -> None:
    """원본 bytes가 다시 직렬화할 필요 없는 exact canonical JSON인지 검증한다."""
    try:
        parse_canonical_json(payload)
    except CanonicalParseError as exc:
        raise ObjectNotCanonicalError(
            f"immutable JSON object가 canonical bytes가 아닙니다: {uri}"
        ) from exc


def _client_error(exc: ClientError) -> tuple[str, int | None]:
    """botocore ClientError의 서비스 오류 코드 문자열과 HTTP status를 반환한다."""
    return (
        str(exc.response.get("Error", {}).get("Code", "Unknown")),
        exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
    )
