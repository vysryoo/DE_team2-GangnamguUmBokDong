"""S3/MinIO 제네릭 입출력 모듈.

pandas DataFrame, dict(JSON), pyarrow.parquet 등의 데이터를 S3로 읽고 쓴다. 어떤 키에
무엇이 있는지는 `layout.py`가 정하고, 이 모듈은 버킷 접근과 직렬화만 다룬다.
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
    있다. 정수·float 폭이나 전량 NULL 컬럼처럼 물리 타입만 다른 경우는 공통 상위
    타입으로 맞추고, 일부 part에 없는 컬럼은 그 타입의 NULL로 채운다. 캐스트는 모두
    무손실이어야 하며 string/int처럼 공통 타입이 없으면 예외로 중단한다.

    args:
        tables: 같은 논리 테이블을 구성하는 하나 이상의 Arrow Table
        required_columns: 지정하면 결합 후 이 컬럼들을 요청 순서로 선택한다.
            어떤 part에도 없는 컬럼이 있으면 실패한다.
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
        # 대부분의 정상 경로는 스키마가 같으므로 cast 없이 zero-copy로 결합함
        combined = pa.concat_tables(tables)
    else:
        common_schema = _unify_part_schemas(tables)
        combined = pa.concat_tables(
            [_conform_to_schema(table, index, common_schema) for index, table in enumerate(tables)]
        )
    if required_columns is None:
        return combined
    return _select_required_columns(combined, required_columns)


def _unify_part_schemas(tables: list[pa.Table]) -> pa.Schema:
    for index, table in enumerate(tables):
        if len(table.column_names) != len(set(table.column_names)):
            raise ParquetSchemaMismatchError(
                f"Parquet part[{index}]에 중복 컬럼명이 있어 이름 기준으로 안전하게 결합할 수 없습니다: "
                f"columns={table.column_names}"
            )
    schemas = [table.schema for table in tables]
    try:
        return pa.unify_schemas(schemas, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        schema_summary = "; ".join(f"part[{i}]={schema.remove_metadata()}" for i, schema in enumerate(schemas))
        raise ParquetSchemaMismatchError(
            f"Parquet part 스키마에 안전한 공통 타입이 없습니다: {schema_summary}"
        ) from exc


def _conform_to_schema(table: pa.Table, index: int, schema: pa.Schema) -> pa.Table:
    columns: list[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        field_index = table.schema.get_field_index(field.name)
        if field_index == -1:
            columns.append(pa.nulls(table.num_rows, type=field.type))
            continue
        source = table.column(field_index)
        try:
            columns.append(_cast_losslessly(source, field.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise ParquetSchemaMismatchError(
                f"Parquet part[{index}] 컬럼 '{field.name}'을 공통 타입으로 무손실 변환할 수 없습니다: "
                f"source={source.type}, target={field.type}"
            ) from exc
    return pa.Table.from_arrays(columns, schema=schema)


def _cast_losslessly(source: pa.ChunkedArray, target: pa.DataType) -> pa.ChunkedArray:
    casted = source.cast(target, safe=True)
    # Arrow 버전에 따라 safe int→float cast의 정밀도 검사가 달라 왕복으로 직접 확인함.
    # 예: 2**53+1은 float64로 반올림되므로 여기서 거부됨
    is_int_to_float = pa.types.is_integer(source.type) and pa.types.is_floating(target)
    if is_int_to_float and not source.equals(casted.cast(source.type, safe=True)):
        raise pa.ArrowInvalid("int→float 변환 후 원래 정숫값으로 왕복되지 않음")
    return casted


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

    `S3_ENDPOINT_URL`이 있으면 MinIO 등으로 보고 환경변수 자격증명을 넘긴다. 실제 AWS
    S3에서는 환경 credential이 명시된 경우에만 client에 고정하고, 없으면 instance
    profile 같은 refreshable provider를 boto3 credential chain에 맡긴다.

    args:
        timeout_seconds: 지정하면 connect/read timeout을 이 값으로 두고 재시도를 끈다.
            S3가 응답하지 않을 때 곧바로 기본값으로 폴백해야 하는 호출에서만 쓴다.
            기본(None)은 boto3 기본 재시도 정책을 따른다.
    """
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    cache_key = (
        os.getpid(),
        endpoint_url,
        timeout_seconds,
        tuple((name, os.environ.get(name)) for name in _CLIENT_IDENTITY_ENV_NAMES),
    )
    with _CLIENT_CACHE_LOCK:
        cached = _CLIENT_CACHE.get(cache_key)
        if cached is not None:
            return cached
        client = _create_client(endpoint_url, timeout_seconds)
        _CLIENT_CACHE[cache_key] = client
        return client


def _create_client(endpoint_url: str | None, timeout_seconds: float | None):
    config_kwargs: dict[str, Any] = {"max_pool_connections": _S3_MAX_WORKERS}
    if timeout_seconds is not None:
        config_kwargs.update(
            connect_timeout=timeout_seconds,
            read_timeout=timeout_seconds,
            retries={"max_attempts": 1},
        )
    profile_name = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE") or None
    region_name = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or None
    session_token = os.environ.get("AWS_SESSION_TOKEN") or os.environ.get("AWS_SECURITY_TOKEN")

    client_factory = boto3
    client_kwargs: dict[str, Any] = {"config": BotoConfig(**config_kwargs)}
    if profile_name is not None:
        client_factory = boto3.session.Session(profile_name=profile_name, region_name=region_name)
    elif region_name is not None:
        client_kwargs["region_name"] = region_name
    if endpoint_url:
        client_kwargs.update(
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            aws_session_token=session_token,
        )
    elif os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        # 같은 process에서 환경 credential이 바뀌어도 boto3 default session의 옛 값을
        # 재사용하지 않도록 client에 고정함
        client_kwargs.update(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=session_token,
        )
    return client_factory.client("s3", **client_kwargs)


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
        timeout_seconds: `_client()` 참고. 지정하면 짧은 timeout과 재시도 없음으로 조회한다.
    returns:
        객체 본문 bytes, 키가 없으면 None
    raises:
        ClientError: NoSuchKey가 아닌 다른 S3 오류가 발생했을 때
        botocore.exceptions.EndpointConnectionError 등: 엔드포인트가 응답하지 않을 때.
            폴백 여부는 호출부가 결정한다.
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
    """변환하면서 Arrow 버퍼를 바로 해제해 순간 메모리가 2배로 뜨지 않게 한다.

    `self_destruct=True`는 `split_blocks=True`와 함께여야 효과가 있다. 변환 뒤 원본
    `table`은 쓸 수 없으므로 호출부는 변환 결과를 곧바로 반환해야 한다.
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
    """S3의 parquet을 pandas DataFrame 또는 pyarrow Table로 읽는다.

    `key`가 객체 하나이면 그 파일을, 아니면 `key/` 아래 다중 part 파일을 모두 읽어
    결합한다. `date_range`나 `dates`를 주면 `key/date=YYYY-MM-DD/` 파티션 중 해당
    날짜만 나열해 읽고, 파일에 없는 `date` 컬럼을 디렉터리 이름에서 복원한다.

    args:
        key: 객체 키 또는 다중 part의 prefix
        columns: 읽을 컬럼. None이면 전체
        as_pandas: True면 DataFrame, False면 pyarrow Table 반환
        date_range: ("YYYY-MM-DD", "YYYY-MM-DD") 양 끝 포함 구간. `dates`와 함께 줄 수 없다
        dates: 불연속 날짜 목록("YYYY-MM-DD"). `date_range`와 함께 줄 수 없다
        filters: 파일 내용에 적용할 pyarrow `filters`. 여러 조건은 AND로 묶인다
            (예: `[("horizon", "<=", 6)]`)
        on_complete: 파일이 여러 개일 때 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로
            호출된다. 단일 객체 경로에서는 호출되지 않는다
        _allow_missing_columns: 다중 part 결합 중 내부 호출 전용. 이 파일에 없는 요청
            컬럼은 건너뛰고 `concat_compatible_tables()`가 NULL로 채운다
    returns:
        읽은 데이터. 키나 part가 없으면 None
    raises:
        ValueError: `date_range`와 `dates`를 함께 지정했을 때
        ParquetSchemaMismatchError: 다중 part 스키마를 안전하게 결합할 수 없을 때
    """
    if date_range is not None and dates is not None:
        raise ValueError("date_range와 dates는 동시에 지정할 수 없습니다.")
    if date_range is not None:
        dates = [day.strftime("%Y-%m-%d") for day in pd.date_range(date_range[0], date_range[1], freq="D")]
    if dates is not None:
        return _read_parquet_by_dates(
            key, dates, columns=columns, as_pandas=as_pandas, filters=filters, on_complete=on_complete
        )

    body = get_object_bytes(key)
    if body is not None:
        table = _read_parquet_bytes(body, columns, filters, allow_missing_columns=_allow_missing_columns)
        return _to_pandas_or_table(table, as_pandas)

    part_keys = _list_parquet_keys(key)
    if not part_keys:
        return None
    # 조각은 Arrow Table로만 모아 한 번에 결합한 뒤 pandas로 한 번만 변환함.
    # 조각마다 DataFrame을 만들어 pd.concat()하면 최종 크기의 2배까지 메모리를 씀
    tables = read_parquet_many(part_keys, columns=columns, as_pandas=False, filters=filters, on_complete=on_complete)
    return _concat_parts([table for table in tables if table is not None], columns, as_pandas)


def _read_parquet_bytes(
    body: bytes,
    columns: list[str] | None,
    filters: list[tuple] | None,
    *,
    allow_missing_columns: bool,
) -> pq.Table:
    source = io.BytesIO(body)
    if allow_missing_columns and columns is not None:
        # 전체 컬럼을 읽으면 컬럼 하나만 스캔하는 학습 사전 단계에서 메모리를 수십 배
        # 쓰므로, 이 파일에 있는 요청 컬럼만 읽음
        available = set(pq.read_schema(source).names)
        source.seek(0)
        columns = [column for column in columns if column in available]
    return pq.read_table(source, columns=columns, filters=filters)


def _list_parquet_keys(prefix: str) -> list[str]:
    prefix = prefix if prefix.endswith("/") else f"{prefix}/"
    return sorted(key for key in list_keys(prefix) if key.endswith(".parquet"))


def _concat_parts(
    tables: list[pa.Table], columns: list[str] | None, as_pandas: bool
) -> pd.DataFrame | pq.Table | None:
    if not tables:
        return None
    combined = concat_compatible_tables(tables, required_columns=columns)
    # 조각 목록 참조를 먼저 끊어 pandas 변환 전에 회수될 수 있게 함
    tables.clear()
    return _to_pandas_or_table(combined, as_pandas)


def _read_parquet_by_dates(
    key: str,
    date_strs: list[str],
    columns: list[str] | None,
    as_pandas: bool,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> pd.DataFrame | pq.Table | None:
    """`date=` Hive 파티션 중 지정한 날짜의 part만 읽고 `date` 컬럼을 복원한다.

    Spark `partitionBy("date")`는 파티션 값을 파일에 넣지 않으므로 디렉터리 이름의
    날짜를 컬럼으로 붙인다. `columns`에 `date`가 없으면 수억 행짜리 문자열 배열을
    만들지 않도록 붙이지 않는다.
    """
    prefix = key if key.endswith("/") else f"{key}/"
    file_columns = [c for c in columns if c != "date"] if columns is not None else None

    # 날짜별 LIST를 병렬로 먼저 끝내고, 다운로드는 전체 키 목록 하나로 묶어 날짜
    # 경계와 무관하게 동시성을 유지함
    with ThreadPoolExecutor(max_workers=_S3_MAX_WORKERS) as pool:
        keys_by_date = list(pool.map(lambda date_str: _list_parquet_keys(f"{prefix}date={date_str}"), date_strs))
    part_keys = [part_key for day_keys in keys_by_date for part_key in day_keys]
    key_dates = [date_str for date_str, day_keys in zip(date_strs, keys_by_date) for _ in day_keys]
    if not part_keys:
        return None

    want_date = columns is None or "date" in columns
    tables = []
    for date_str, table in zip(
        key_dates,
        read_parquet_many(part_keys, columns=file_columns, as_pandas=False, filters=filters, on_complete=on_complete),
    ):
        if table is None or table.num_rows == 0:
            continue
        if want_date:
            table = table.append_column("date", pa.array([date_str] * table.num_rows))
        tables.append(table)
    return _concat_parts(tables, columns, as_pandas)


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
        keys: 읽을 객체 키 목록
        columns: 읽을 컬럼. 파일에 없는 요청 컬럼은 건너뛴다
        max_workers: 동시 조회 스레드 수
        as_pandas: True면 DataFrame, False면 pyarrow Table
        filters: `read_parquet()` 참고
        on_complete: 키 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로 호출된다
    returns:
        `keys`와 같은 순서의 결과 목록. 없는 키는 None
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

    # ContextVar는 worker thread로 전파되지 않으므로 context copy를 넘김. copy 안의
    # S3ReadCapture는 같은 객체라 multipart GET도 한 capture에 모임
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
