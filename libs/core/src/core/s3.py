"""S3/MinIO 제네릭 입출력 모듈.

pandas DataFrame, dict(JSON), pyarrow.parquet 등의 데이터를 S3로 읽고 씁니다.
"""

from __future__ import annotations

import io
import json
import os
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


class ParquetSchemaMismatchError(ValueError):
    """여러 Parquet 조각의 스키마를 무손실로 통일할 수 없을 때 발생한다."""


class S3InputDriftError(RuntimeError):
    """한 capture 구간에서 같은 S3 key가 서로 다른 상태로 관측됐다."""


@dataclass(frozen=True, slots=True)
class CapturedS3Object:
    """계산이 실제로 읽은 S3 object key와 그 exact bytes다."""

    key: str
    payload: bytes


class S3ReadCapture:
    """동시 multipart GET까지 한 inference run의 object 관측을 모은다."""

    def __init__(self) -> None:
        """빈 thread-safe observation map을 만든다."""
        self._lock = threading.Lock()
        self._observations: dict[str, bytes | None] = {}

    def record(self, key: str, payload: bytes | None) -> None:
        """같은 key의 반복 관측이 exact same bytes 또는 같은 missing인지 확인한다."""
        if type(key) is not str or not key:
            raise TypeError("capture할 S3 key는 non-empty string이어야 합니다.")
        if payload is not None and type(payload) is not bytes:
            raise TypeError("capture할 S3 payload는 bytes 또는 None이어야 합니다.")
        with self._lock:
            if key in self._observations and self._observations[key] != payload:
                raise S3InputDriftError(
                    f"같은 S3 key가 inference run 중 변경됐습니다: {key}"
                )
            self._observations[key] = payload

    @property
    def objects(self) -> tuple[CapturedS3Object, ...]:
        """존재한 object만 key UTF-8 byte 순으로 정렬해 반환한다."""
        with self._lock:
            values = tuple(self._observations.items())
        return tuple(
            CapturedS3Object(key=key, payload=payload)
            for key, payload in sorted(values, key=lambda item: item[0].encode("utf-8"))
            if payload is not None
        )


_ACTIVE_READ_CAPTURE: ContextVar[S3ReadCapture | None] = ContextVar(
    "core_s3_active_read_capture",
    default=None,
)
_CLIENT_CACHE_LOCK = threading.Lock()
_CLIENT_CACHE: dict[tuple[object, ...], Any] = {}
_S3_MAX_WORKERS = 16
_CLIENT_IDENTITY_ENV_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_EC2_METADATA_DISABLED",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
    "AWS_CA_BUNDLE",
    "AWS_RETRY_MODE",
    "AWS_MAX_ATTEMPTS",
)


@contextmanager
def capture_object_reads() -> Iterator[S3ReadCapture]:
    """현재 context와 ``read_parquet_many`` worker가 읽는 exact object bytes를 모은다.

    Capture는 의도적으로 중첩할 수 없다. Authority producer가 계산 경계를 한 번만
    열어야 model/release preflight bytes와 실제 non-model 계산 input이 섞이지 않는다.
    """
    if _ACTIVE_READ_CAPTURE.get() is not None:
        raise RuntimeError("S3 object read capture는 중첩할 수 없습니다.")
    capture = S3ReadCapture()
    token = _ACTIVE_READ_CAPTURE.set(capture)
    try:
        yield capture
    finally:
        _ACTIVE_READ_CAPTURE.reset(token)


def _record_object_read(key: str, payload: bytes | None) -> None:
    """활성 capture가 있을 때만 S3 GET 결과를 기록한다."""
    capture = _ACTIVE_READ_CAPTURE.get()
    if capture is not None:
        capture.record(key, payload)


def concat_compatible_tables(
    tables: list[pa.Table], required_columns: list[str] | None = None
) -> pa.Table:
    """호환 가능한 Parquet 스키마 변화만 안전하게 승격해 테이블을 결합한다.

    Spark가 날짜 파티션을 증분 덮어쓰는 동안 구버전과 신버전 part가 잠시 공존할 수
    있다. 이때 정수 폭, float 폭, 전량 NULL 컬럼처럼 논리적 의미는 같지만 물리 타입만
    다른 스키마는 Arrow의 공통 상위 타입으로 맞춘 뒤 결합한다. 모든 캐스트에는
    ``safe=True``를 사용하므로 overflow, float 정밀도 손실, 소수 절삭은 허용하지
    않는다.

    새 컬럼이 추가된 part와 아직 그 컬럼이 없는 part는 공통 스키마의 정확한 타입으로
    NULL을 채운다. 반대로 string/int처럼 공통 논리 타입이 없는 경우는 어느 한쪽을
    문자열 등으로 임의 변환하지 않고, 스키마를 함께 표시한 명시적 예외로 중단한다.

    args:
        tables: 같은 논리 테이블을 구성하는 하나 이상의 Arrow Table
        required_columns: 지정하면 결합 후 이 컬럼들이 모두 존재하는지 확인하고
            요청 순서로 선택한다. 어떤 part에도 없는 컬럼은 명시적으로 실패한다.
    returns:
        컬럼 순서와 타입이 하나의 공통 스키마로 정렬된 결합 테이블
    raises:
        ValueError: 빈 목록이 전달됐을 때
        ParquetSchemaMismatchError: 안전한 공통 타입으로 변환할 수 없을 때
    """
    if not tables:
        raise ValueError("결합할 Parquet 테이블이 없습니다.")

    first_schema = tables[0].schema
    if all(table.schema.equals(first_schema, check_metadata=True) for table in tables[1:]):
        # 학습 정상 경로는 대부분 모든 part 스키마가 이미 같다. 수억 행을 읽을 때
        # 매 컬럼 cast/Table 재조립을 거치지 않도록 기존 zero-copy 결합 성능을 보존한다.
        combined = pa.concat_tables(tables)
        return _select_required_columns(combined, required_columns) if required_columns is not None else combined

    schemas = []
    for index, table in enumerate(tables):
        if len(table.column_names) != len(set(table.column_names)):
            raise ParquetSchemaMismatchError(
                f"Parquet part[{index}]에 중복 컬럼명이 있어 이름 기준으로 안전하게 결합할 수 없습니다: "
                f"columns={table.column_names}"
            )
        schemas.append(table.schema)

    try:
        common_schema = pa.unify_schemas(schemas, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        schema_summary = "; ".join(f"part[{i}]={schema.remove_metadata()}" for i, schema in enumerate(schemas))
        raise ParquetSchemaMismatchError(
            f"Parquet part 스키마에 안전한 공통 타입이 없습니다: {schema_summary}"
        ) from exc

    conformed: list[pa.Table] = []
    for index, table in enumerate(tables):
        columns: list[pa.Array | pa.ChunkedArray] = []
        for field in common_schema:
            field_index = table.schema.get_field_index(field.name)
            if field_index == -1:
                columns.append(pa.nulls(table.num_rows, type=field.type))
                continue

            source = table.column(field_index)
            try:
                casted = source.cast(field.type, safe=True)
                # Arrow 버전에 따라 safe int→float cast의 정밀도 검사가 달라질 수
                # 있으므로 왕복해서 실제 값이 같은지도 직접 확인한다. 예를 들어
                # 2**53+1은 float64로 반올림되므로 이 검사에서 거부된다.
                if pa.types.is_integer(source.type) and pa.types.is_floating(field.type):
                    restored = casted.cast(source.type, safe=True)
                    if not source.equals(restored):
                        raise pa.ArrowInvalid("int→float 변환 후 원래 정숫값으로 왕복되지 않음")
                columns.append(casted)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
                raise ParquetSchemaMismatchError(
                    f"Parquet part[{index}] 컬럼 '{field.name}'을 공통 타입으로 무손실 변환할 수 없습니다: "
                    f"source={source.type}, target={field.type}"
                ) from exc

        conformed.append(pa.Table.from_arrays(columns, schema=common_schema))

    combined = pa.concat_tables(conformed)
    return _select_required_columns(combined, required_columns) if required_columns is not None else combined


def _select_required_columns(table: pa.Table, columns: list[str]) -> pa.Table:
    """요청 컬럼을 순서대로 선택하고 전체 part에 없는 컬럼은 명시적으로 거부한다."""
    missing = [column for column in columns if column not in table.column_names]
    if missing:
        raise ParquetSchemaMismatchError(
            f"요청한 컬럼이 어떤 Parquet part에도 없습니다: missing={missing}, "
            f"available={table.column_names}. 서로 다른 스키마 세대인지 확인하세요."
        )
    return table.select(columns)


def _client(timeout_seconds: float | None = None):
    """S3 호환 클라이언트를 process-local로 생성해 재사용한다.

    `S3_ENDPOINT_URL`이 있으면 MinIO 등 S3 호환 스토리지로 보고 환경변수의 자격증명을
    명시적으로 넘긴다(로컬 개발 경로 — 기존 동작 그대로). 없으면 실제 AWS S3로 보고,
    환경 credential이 이미 명시된 경우에만 그 exact access/secret/token을 client에
    고정한다. 환경 credential이 없으면 boto3 credential chain에 맡겨 EC2 instance
    profile이나 web identity 같은 refreshable provider를 그대로 사용한다.

    args:
        timeout_seconds: 지정하면 connect/read timeout을 이 값으로 두고 재시도를
            끈다(`retries={"max_attempts": 1}`) — 기본(None)은 boto3 기본 재시도
            정책 그대로(연결 자체가 안 되는 엔드포인트에서 실측 8.5초 소요). 프로필처럼
            "S3가 응답 안 하면 즉시 내장 기본값으로 폴백"해야 하는 드문 호출에서만 쓴다
            — 대부분의 호출(학습 데이터 로드 등)은 느리더라도 끝까지 재시도하는 게
            맞아 기본값을 안 바꾼다.
    """
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    session_token = os.environ.get("AWS_SESSION_TOKEN") or os.environ.get(
        "AWS_SECURITY_TOKEN"
    )
    client_environment = tuple(
        (name, os.environ.get(name)) for name in _CLIENT_IDENTITY_ENV_NAMES
    )
    cache_key = (
        os.getpid(),
        endpoint_url,
        timeout_seconds,
        client_environment,
    )
    with _CLIENT_CACHE_LOCK:
        cached = _CLIENT_CACHE.get(cache_key)
        if cached is not None:
            return cached

        config_kwargs: dict[str, Any] = {
            "max_pool_connections": _S3_MAX_WORKERS,
        }
        if timeout_seconds is not None:
            config_kwargs.update(
                connect_timeout=timeout_seconds,
                read_timeout=timeout_seconds,
                retries={"max_attempts": 1},
            )
        config = BotoConfig(**config_kwargs)
        profile_name = (
            os.environ.get("AWS_PROFILE")
            or os.environ.get("AWS_DEFAULT_PROFILE")
            or None
        )
        region_name = (
            os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or None
        )
        client_factory = boto3
        client_kwargs: dict[str, Any] = {"config": config}
        if profile_name is not None:
            client_factory = boto3.session.Session(
                profile_name=profile_name,
                region_name=region_name,
            )
        elif region_name is not None:
            client_kwargs["region_name"] = region_name
        if endpoint_url:
            client_kwargs.update(
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
            )
        elif os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        ):
            # 명시적인 환경 credential은 client 설정에 고정해야 같은 process에서
            # 값이 바뀌어도 boto3 default session의 옛 credential을 재사용하지 않는다.
            # 환경 credential이 없으면 이 kwargs들을 생략해 instance role/web
            # identity 같은 refreshable provider를 credential chain에 그대로 맡긴다.
            client_kwargs.update(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                aws_session_token=session_token,
            )
        client = client_factory.client("s3", **client_kwargs)
        _CLIENT_CACHE[cache_key] = client
        return client


def _clear_client_cache() -> None:
    """Cache를 비우고 분리한 client의 HTTP connection pool을 닫는다."""
    with _CLIENT_CACHE_LOCK:
        clients = list(_CLIENT_CACHE.values())
        _CLIENT_CACHE.clear()
    for client in clients:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def bucket_name() -> str:
    """대상 S3 버킷 이름을 환경 변수에서 읽어 반환한다."""
    return os.environ.get("S3_BUCKET", "gangnamgu")


def object_uri(key: str) -> str:
    """현재 버킷의 객체 키를 `s3://bucket/key` URI로 만든다."""
    return f"s3://{bucket_name()}/{key}"


def get_object_bytes(key: str, timeout_seconds: float | None = None) -> bytes | None:
    """S3 객체를 bytes로 읽는다.

    args:
        key: 읽을 객체의 전체 키
        timeout_seconds: `_client()` 참고 — 지정하면 짧은 timeout+재시도 없음으로 조회한다.
    returns:
        객체 본문 bytes, 키가 없으면 None
    raises:
        ClientError: NoSuchKey가 아닌 다른 S3 오류가 발생했을 때
        botocore.exceptions.EndpointConnectionError 등: timeout_seconds 지정 시
            엔드포인트 자체가 응답하지 않으면 그대로 던진다 — 호출부가 폴백을 결정한다.
    """
    try:
        payload = _client(timeout_seconds).get_object(Bucket=bucket_name(), Key=key)["Body"].read()
        _record_object_read(key, payload)
        return payload
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            _record_object_read(key, None)
            return None
        raise


def put_object_bytes(
    key: str,
    body: bytes,
    *,
    metadata: dict[str, str] | None = None,
) -> None:
    """bytes와 선택적인 S3 user metadata를 객체로 저장한다."""
    kwargs = {"Bucket": bucket_name(), "Key": key, "Body": body}
    if metadata is not None:
        kwargs["Metadata"] = metadata
    _client().put_object(**kwargs)


def get_object_metadata(key: str) -> dict[str, str] | None:
    """S3 객체 본문을 받지 않고 user metadata만 조회한다. 키가 없으면 None."""
    try:
        return dict(_client().head_object(Bucket=bucket_name(), Key=key).get("Metadata", {}))
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return None
        raise


def _to_pandas_or_table(table: pq.Table, as_pandas: bool) -> pd.DataFrame | pq.Table:
    """`table.to_pandas()` 변환 시 순간 메모리가 2배로 뜨는 걸 줄인다.

    기본 `to_pandas()`는 원본 Arrow Table을 그대로 둔 채 새 pandas 배열을
    만들어서, 변환이 끝나기 전까지 둘 다(Arrow Table + 새 DataFrame) 메모리에
    떠 있다 — 학습 테이블처럼 큰 데이터에서 순간 피크가 최종 크기의 2배까지
    간다(2026-08 리뷰 지적). `self_destruct=True`(+`split_blocks=True`, 이
    조합이어야 실제 절감 효과가 있음 — pyarrow 문서)는 컬럼을 pandas로 옮기는
    족족 Arrow 쪽 버퍼를 즉시 해제한다 — 대신 호출부가 변환 후 원본 `table`을
    다시 쓰면 안 된다(이 함수의 모든 호출부는 변환 직후 바로 반환하므로 안전).
    """
    if not as_pandas:
        return table
    return table.to_pandas(self_destruct=True, split_blocks=True)


def read_parquet(
    key: str,
    columns: list[str] | None = None,
    as_pandas: bool = True,
    date_range: tuple[str, str] | None = None,
    dates: list[str] | None = None,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
    _allow_missing_columns: bool = False,
) -> pd.DataFrame | pq.Table | None:
    """S3의 parquet을 pandas DataFrame 또는 pyarrow Table로 읽는다 — 파일 1개짜리 객체와 다중 파트 "디렉터리" 둘 다 지원한다.

    args:
        date_range: (start, end) "YYYY-MM-DD" 문자열(둘 다 포함) — 지정하면 `key`가
            Spark `partitionBy("date")`로 쓰인 `key/date=YYYY-MM-DD/part-*.parquet`
            레이아웃이라고 보고 `_read_parquet_by_dates()`로 위임한다. prefix
            전체를 나열하는 대신 이 범위의 date= 서브prefix만 나열/다운로드해서,
            쌓인 전체 히스토리가 아니라 실제로 필요한 기간 크기에만 비용이 비례하게
            한다(ml/training이 매달 전체를 다시 받는 문제 대응). None이면(기본)
            prefix 전체를 읽는다 — 파티션 없는 데이터셋은 계속 이 경로를 쓴다.
        dates: date_range(연속 구간) 대신 특정 날짜만 골라서 읽고 싶을 때 쓴다 —
            예: 짝수날만/특정 요일만처럼 불연속 날짜 목록. "YYYY-MM-DD" 문자열
            리스트, date_range와 동시에 줄 수 없다(training._split()의 짝/홀수
            day-of-month 표본처럼, 애초에 필요한 파티션만 골라 읽어서 로드 자체의
            메모리를 줄이는 용도 — `_split()`이 다운스트림에서 걸러도 이미 전체를
            읽어버린 뒤라 소용없다).
        filters: `date`/`dates`는 파티션(폴더) 단위 필터라 그걸로 못 거르는, 파일
            *내용* 안의 일반 컬럼 값 필터(예: `[("horizon", "<=", 6)]`) — pyarrow의
            `pq.read_table(..., filters=...)` 그대로 각 조각 파일에 적용된다(row-group
            통계로 스킵 가능하면 스킵, 아니면 읽은 뒤 걸러서 최종 결과에만 안 남김).
            여러 조건은 AND로 묶인다(`[("a", "<=", 1), ("b", ">", 2)]`). training의
            multi-horizon 테이블처럼 같은 날짜 파티션 안에 여러 horizon이 섞여 있어
            날짜만으로는 못 줄일 때 쓴다.
        on_complete: 여러 파일 중 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로
            호출된다(`date_range`/`dates`/prefix 다중 파트처럼 파일이 여러 개일
            때만 의미가 있음 — 단일 객체 GET 한 번으로 끝나는 경로에서는 안 불림).
            대용량 학습 테이블 로드처럼 오래 걸리는 호출의 진행 상황을 로깅하고
            싶을 때만 넘기면 된다.
    """
    if date_range is not None and dates is not None:
        raise ValueError("date_range와 dates는 동시에 지정할 수 없습니다.")
    if date_range is not None:
        date_strs = [day.strftime("%Y-%m-%d") for day in pd.date_range(date_range[0], date_range[1], freq="D")]
        return _read_parquet_by_dates(
            key, date_strs, columns=columns, as_pandas=as_pandas, filters=filters, on_complete=on_complete
        )
    if dates is not None:
        return _read_parquet_by_dates(
            key, dates, columns=columns, as_pandas=as_pandas, filters=filters, on_complete=on_complete
        )

    body = get_object_bytes(key)
    if body is not None:
        source = io.BytesIO(body)
        file_columns = columns
        if _allow_missing_columns and columns is not None:
            # 다중 part 결합 중에는 새 컬럼이 아직 없는 옛 part도 읽어야 한다.
            # 전체 컬럼을 읽으면 station_no 하나만 스캔하는 학습 사전 단계에서
            # 수십 배 메모리를 쓰므로, 이 파일에 실제로 존재하는 요청 컬럼만 읽고
            # concat_compatible_tables()가 나중에 정확한 타입의 NULL을 보충한다.
            available = set(pq.read_schema(source).names)
            source.seek(0)
            file_columns = [column for column in columns if column in available]
        table = pq.read_table(source, columns=file_columns, filters=filters)
        return _to_pandas_or_table(table, as_pandas)

    prefix = key if key.endswith("/") else f"{key}/"
    part_keys = sorted(k for k in list_keys(prefix) if k.endswith(".parquet"))
    if not part_keys:
        return None

    # 조각마다 pandas DataFrame으로 각각 변환해 리스트에 쌓아뒀다가 pd.concat()하면,
    # 그 순간 "조각 전부 + 새로 만든 합본"이 동시에 메모리에 떠 있는다(최종 크기의
    # 최대 2배). 항상 Arrow Table로만 모아서(as_pandas=False) Arrow 레벨에서 먼저
    # 합치고(zero-copy — ChunkedArray가 기존 버퍼를 복사 없이 이어붙임), pandas
    # 변환은 최종 결과 하나에 대해 딱 한 번만 한다. 조각 목록은 합치자마자 바로
    # 참조를 끊어서(del) 가비지 컬렉션이 즉시 회수할 수 있게 한다.
    tables = [
        t
        for t in read_parquet_many(part_keys, columns=columns, as_pandas=False, filters=filters, on_complete=on_complete)
        if t is not None
    ]
    if not tables:
        return None
    combined = concat_compatible_tables(tables, required_columns=columns)
    del tables

    return _to_pandas_or_table(combined, as_pandas)


def _read_parquet_by_dates(
    key: str,
    date_strs: list[str],
    columns: list[str] | None,
    as_pandas: bool,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> pd.DataFrame | pq.Table | None:
    """`date=` Hive 파티션 레이아웃에서 지정한 날짜들의 서브prefix만 나열/다운로드한다.

    연속 구간(`date_range`)이든 불연속 목록(`dates`, 예: 짝수날만)이든 호출부
    (`read_parquet()`)가 이미 정확한 날짜 문자열 리스트로 펼쳐서 넘겨준다 — 여기는
    그 목록을 그대로 순회할 뿐 범위/불연속 여부를 신경 쓰지 않는다.

    Spark는 파티션 컬럼 값을 파일 내용에 넣지 않고 디렉터리명(`date=YYYY-MM-DD/`)
    으로만 표현한다(Hive 컨벤션) — 그래서 파일에서 읽은 결과엔 "date" 컬럼이 아예
    없다. 여기서는 그 값을 파일 내용에서 역추출할 필요 없이, 애초에 그 값으로 직접
    만든 서브prefix를 순회하는 것이므로 반복 중인 날짜 문자열을 그대로 붙인다.

    args:
        key: Spark `partitionBy("date")` 출력의 prefix(파티션 폴더들의 부모)
        date_strs: 읽을 날짜("YYYY-MM-DD") 목록 — 연속/불연속 상관없음
        columns: 읽을 컬럼(None이면 전체) — `None`이거나 명시적으로 `"date"`를
            포함해야 결과에 복원된 "date"가 있다(포함 안 하면 애초에 안 만듦,
            아래 참고). 지정 시 그 컬럼 순서를 그대로 맞춰 반환한다
        as_pandas: True면 DataFrame, False면 pyarrow Table 반환("date" 복원은
            내부적으로 항상 pandas로 하고 필요할 때만 마지막에 변환)
        filters: `read_parquet()` docstring 참고 — 각 날짜 파티션 파일에 그대로 적용된다
        on_complete: `read_parquet()` docstring 참고 — 날짜 전체에 걸친 파일 목록
            기준으로 진행 상황을 보고한다(날짜 경계와 무관하게 하나의 카운터)
    returns:
        지정한 날짜들에 데이터가 하나도 없으면 None
    """
    prefix = key if key.endswith("/") else f"{key}/"
    file_columns = [c for c in columns if c != "date"] if columns is not None else None

    # 날짜별 LIST부터 병렬로 끝낸다 — 순서대로 하나씩 "LIST한 뒤 그 날짜 파일들을
    # 내려받고, 그게 끝나야 다음 날짜 LIST를 시작"하는 계단식 패턴이면 학습처럼
    # 몇 달치를 한 번에 읽을 때 그 사이 대기 시간이 그대로 쌓인다 — LIST 자체는
    # 가벼운 호출이라 read_parquet_many()와 같은 동시성으로 먼저 다 끝내둔다.
    with ThreadPoolExecutor(max_workers=_S3_MAX_WORKERS) as pool:
        keys_by_date = list(
            pool.map(
                lambda date_str: sorted(k for k in list_keys(f"{prefix}date={date_str}/") if k.endswith(".parquet")),
                date_strs,
            )
        )

    # 실제 파일 다운로드는 day 단위로 나눠 부르지 않고 전체 날짜 범위를 합친
    # 키 목록 하나로 read_parquet_many()를 한 번만 호출한다 — 그래야 그 안의
    # ThreadPoolExecutor가 날짜 경계와 무관하게 전체 구간에 대해 동시성을 최대로
    # 활용한다(day마다 별도 풀을 새로 만들어 반복하면 그 사이 유휴 시간이 생김).
    part_keys: list[str] = []
    key_dates: list[str] = []
    for date_str, day_keys in zip(date_strs, keys_by_date):
        part_keys.extend(day_keys)
        key_dates.extend([date_str] * len(day_keys))

    if not part_keys:
        return None

    # read_parquet()와 같은 이유로 Arrow Table 상태로만 모은다 — "date" 컬럼도
    # pandas .copy()+할당 대신 Arrow 레벨에서 상수 컬럼으로 붙여서, 조각마다 pandas
    # DataFrame을 따로 만들지 않는다(다중 파티션 읽기라 이 함수가 가장 큰 데이터를
    # 다룰 가능성이 높음 — training의 multi-horizon 테이블 읽기).
    #
    # **"date"는 호출부가 실제로 요청했을 때만 붙인다(2026-08 수정)** — 예전엔
    # `columns`에 "date"가 없어도 일단 전체 구간에 대해 행 수만큼 문자열 배열을
    # 만들고 나중에(아래 `combined.select(columns)`에서) 버렸다. `lazy_train_dataset.
    # station_categories_for_dates()`처럼 `columns=["station_no"]`로 1년 전체(약
    # 8억 행)를 읽는 호출에서, 그 순간만 쓰고 버리는 date 문자열 배열이 수 GB
    # 규모로 일시에 잡혀 있었다(리뷰 지적) — 스트리밍 경로를 만든 취지와 정반대라
    # 애초에 요청 안 했으면 만들지 않는다.
    want_date = columns is None or "date" in columns
    tables = []
    for date_str, table in zip(
        key_dates,
        read_parquet_many(part_keys, columns=file_columns, as_pandas=False, filters=filters, on_complete=on_complete),
    ):
        if table is None or table.num_rows == 0:
            continue
        if want_date:
            date_column = pa.array([date_str] * table.num_rows)
            table = table.append_column("date", date_column)
        tables.append(table)

    if not tables:
        return None
    combined = concat_compatible_tables(tables, required_columns=columns)
    del tables
    return _to_pandas_or_table(combined, as_pandas)


def read_parquet_many(
    keys: list[str],
    columns: list[str] | None = None,
    max_workers: int = _S3_MAX_WORKERS,
    as_pandas: bool = True,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> list[pd.DataFrame | pq.Table | None]:
    """여러 parquet 키를 스레드로 병렬 조회한다.

    args:
        on_complete: 키 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로 호출된다.
            여러 워커 스레드가 동시에 끝낼 수 있어 카운터 증가는 락으로 보호한다.
            None(기본)이면 아무 것도 안 한다 — 오래 걸리는 대량 로드의 진행 상황을
            로깅하고 싶을 때만 넘기면 된다.
    """
    if not keys:
        return []

    total = len(keys)
    completed = 0
    lock = threading.Lock()

    def _read(key: str) -> pd.DataFrame | pq.Table | None:
        nonlocal completed
        result = read_parquet(
            key,
            columns=columns,
            as_pandas=as_pandas,
            filters=filters,
            _allow_missing_columns=True,
        )
        if on_complete is not None:
            with lock:
                completed += 1
                current = completed
            on_complete(current, total)
        return result

    capture = _ACTIVE_READ_CAPTURE.get()
    if capture is None:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            return list(pool.map(_read, keys))

    # ContextVar는 새 worker thread로 자동 전파되지 않는다. 각 작업에 현재 context의
    # 독립 copy를 넘기되, 그 안의 S3ReadCapture 객체는 같은 thread-safe recorder를
    # 가리키게 해서 multipart GET도 authority run 하나에 빠짐없이 포함한다.
    contexts = [copy_context() for _ in keys]

    def _read_in_context(item: tuple[Context, str]) -> pd.DataFrame | pq.Table | None:
        """복사한 caller context 안에서 단일 parquet key를 읽는다."""
        context, key = item
        return context.run(_read, key)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(_read_in_context, zip(contexts, keys)))


def write_parquet(
    data: pd.DataFrame | pq.Table,
    key: str,
    *,
    metadata: dict[str, str] | None = None,
) -> None:
    """pandas/Arrow 데이터를 Parquet과 선택적인 S3 user metadata로 저장한다."""
    buffer = io.BytesIO()
    if isinstance(data, pd.DataFrame):
        data.to_parquet(buffer, index=False)
    else:
        pq.write_table(data, buffer)
    put_object_bytes(key, buffer.getvalue(), metadata=metadata)


def read_json(key: str, timeout_seconds: float | None = None):
    """S3의 JSON 객체를 읽는다 (dict 또는 list — JSON 최상위 값 그대로). 키가 없으면 None.

    args:
        timeout_seconds: `_client()` 참고.
    """
    body = get_object_bytes(key, timeout_seconds=timeout_seconds)
    if body is None:
        return None
    return json.loads(body)


def write_json(key: str, data) -> None:
    """dict 또는 list를 JSON으로 직렬화해 S3에 저장한다."""
    put_object_bytes(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))


def list_keys(prefix: str, delimiter: str = "") -> list[str]:
    """주어진 prefix 아래 모든 객체 키를 나열한다.

    args:
        prefix: 나열할 키 prefix
        delimiter: S3 폴더 구분자 (예: "/")
    returns:
        prefix로 시작하는 모든 객체 키 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket_name(), Prefix=prefix, Delimiter=delimiter)
        for obj in page.get("Contents", [])
    ]


@dataclass(frozen=True)
class S3Object:
    """LIST 응답이 주는 객체 메타.

    `size`·`last_modified`는 본문을 읽지 않고 변경을 감지할 때 쓴다. 같은 키를
    덮어쓴 경우 키 목록은 그대로지만 이 둘이 바뀐다.
    """

    key: str
    size: int
    last_modified: datetime


def list_objects(prefix: str, delimiter: str = "") -> list[S3Object]:
    """주어진 prefix 아래 객체를 메타와 함께 나열한다.

    `list_keys`는 키만 주므로 "내용이 바뀌었는지"를 알 수 없다. 이 함수는 LIST 응답에
    이미 들어 있는 `Size`·`LastModified`를 버리지 않고 그대로 넘긴다.

    args:
        prefix: 나열할 키 prefix
        delimiter: S3 폴더 구분자 (예: "/")
    returns:
        prefix로 시작하는 객체 메타 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        S3Object(key=obj["Key"], size=obj["Size"], last_modified=obj["LastModified"])
        for page in paginator.paginate(Bucket=bucket_name(), Prefix=prefix, Delimiter=delimiter)
        for obj in page.get("Contents", [])
    ]


def object_exists(key: str) -> bool:
    """S3 객체가 존재하는지 확인한다."""
    try:
        _client().head_object(Bucket=bucket_name(), Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def delete_object(key: str) -> None:
    """S3 객체를 삭제한다."""
    _client().delete_object(Bucket=bucket_name(), Key=key)


def delete_objects(keys: list[str]) -> None:
    """여러 S3 객체를 일괄 삭제한다."""
    if not keys:
        return
    client = _client()
    bucket = bucket_name()
    # delete_objects 한 번에 최대 1000개 삭제 가능
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
