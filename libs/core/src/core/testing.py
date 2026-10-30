"""moto S3 fixture와 source snapshot 테스트 헬퍼를 제공하는 pytest plugin.

각 프로젝트의 `tests/conftest.py`에서 `pytest_plugins = ("core.testing",)`로 등록한다.
`moto`와 `pytest`는 각 프로젝트의 dev 의존성에만 있으므로 운영 코드에서 import하지 않는다.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from datetime import datetime
from typing import NamedTuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from . import layout, s3
from .gold_publication.canonical import sha256_hex
from .source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)

TEST_BUCKET = "test-bucket"


class PutSourceSnapshot(NamedTuple):
    """테스트용으로 저장한 source snapshot의 위치다."""

    manifest_uri: str
    manifest_sha256: str
    silver_key: str


@pytest.fixture
def s3_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """S3 환경변수와 process-local client cache를 테스트마다 격리한다."""
    s3._clear_client_cache()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)
    yield
    s3._clear_client_cache()


@pytest.fixture
def s3_bucket(s3_env: None) -> Iterator[str]:
    """moto 안에 빈 테스트 bucket을 만들고 이름을 반환한다."""
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield TEST_BUCKET


def _put_object(key: str, body: bytes) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=body
    )


def _parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def put_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    table: pa.Table,
    *,
    revision_no: int = 0,
    manifest_rows: int | None = None,
) -> PutSourceSnapshot:
    """Collector가 게시하는 것과 같은 immutable Silver와 SUCCEEDED manifest를 쓴다.

    args:
        source_id: 소스 id
        logical_dttm: 논리 시각(aware)
        table: Silver로 저장할 테이블
        revision_no: manifest correction 순번
        manifest_rows: manifest에 기록할 행 수. 생략하면 실제 행 수
    returns:
        manifest URI·SHA-256과 Silver 키
    """
    body = _parquet_bytes(table)
    checksum = sha256_hex(body)
    silver_key = layout.silver_content_key(source_id, logical_dttm, checksum)
    rows = table.num_rows if manifest_rows is None else manifest_rows
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="fixture-v1",
        silver_uri=s3.object_uri(silver_key),
        silver_byte_sha256=checksum,
        counts=SourceSnapshotCounts(rows, rows, rows, 0, 0),
        planned_parts=("page=1",),
        completed_parts=("page=1",),
    )
    manifest_key = layout.source_snapshot_manifest_key(
        source_id, logical_dttm, revision_no
    )
    _put_object(silver_key, body)
    _put_object(manifest_key, manifest.canonical_bytes)
    return PutSourceSnapshot(s3.object_uri(manifest_key), manifest.sha256, silver_key)


def put_partial_source_snapshot(
    source_id: str, logical_dttm: datetime, table: pa.Table
) -> str:
    """PARTIAL Collector 실행의 diagnostic window manifest와 immutable Silver를 쓴다.

    args:
        source_id: 소스 id
        logical_dttm: 논리 시각(aware)
        table: Silver로 저장할 테이블
    returns:
        저장한 Silver 키
    """
    body = _parquet_bytes(table)
    silver_key = layout.silver_content_key(source_id, logical_dttm, sha256_hex(body))
    document = {
        "source_id": source_id,
        "window_start": logical_dttm.isoformat(),
        "status": "partial",
        "stage": "completed",
        "failure_reason": None,
        "artifacts": {"silver": silver_key},
        "counts": {"kept": table.num_rows},
    }
    _put_object(silver_key, body)
    _put_object(
        layout.window_manifest_key(source_id, logical_dttm),
        json.dumps(document).encode(),
    )
    return silver_key
