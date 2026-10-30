"""manifest 스키마와 읽기 쓰기, 상태 어휘.

한 번의 실행이 무엇을 했고 어디까지 갔는지를 남긴다.
실제 S3 접근은 storage.py에 위임하고, 이 모듈은 dict ↔ 모델 변환과 상태 어휘만 담당한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, IntEnum
from typing import Annotated, Literal

import storage
from core.source_snapshot import (
    SourceSnapshotContractError,
    SourceSnapshotCounts,
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
    parse_source_snapshot_manifest,
    same_source_snapshot_content,
)
from pydantic import BaseModel, BeforeValidator, ConfigDict, PlainSerializer


class RunStatus(str, Enum):
    """한 번의 실행이 최종적으로 도달한 상태."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    EMPTY = "empty"
    SKIPPED = "skipped"


class Stage(IntEnum):
    """실행이 어디까지 진행됐는지를 나타내는 단계. 값 순서가 진행 순서다."""

    BRONZE_WRITTEN = 1
    VALIDATED = 2
    COMPLETED = 3


def _stage_from_json(value: object) -> Stage:
    """저장된 Stage 문자열을 Stage 객체로 변환한다."""

    if isinstance(value, Stage):
        return value
    return Stage[str(value).upper()]


StageField = Annotated[
    Stage,
    BeforeValidator(_stage_from_json),
    PlainSerializer(lambda s: s.name.lower(), return_type=str),
]


class FailureReason(str, Enum):
    """실행이 FAILED 상태로 끝났을 때, 어느 단계에서 실패했는지 구분한다."""

    FETCH_ERROR = "fetch_error"
    STORAGE_ERROR = "storage_error"
    QUALITY_GATE = "quality_gate"
    CONFIG_ERROR = "config_error"


class BronzeArtifacts(BaseModel):
    """bronze 계층에 쓰인 조각들의 위치와 Hot Bronze revision 정보."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    prefix: str
    parts: tuple[str, ...] = ()
    revision: int | None = None


class Artifacts(BaseModel):
    """실행 중 각 계층에 남긴 산출물 위치."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    bronze: BronzeArtifacts | None = None
    silver: str | None = None
    quarantine: str | None = None


class Counts(BaseModel):
    """수집·검증 단계를 거치며 집계한 row 개수."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    expected: int | None = None
    fetched: int = 0
    kept: int = 0
    repaired: int = 0
    dropped: int = 0


class Missing(BaseModel):
    """부분 실패로 채우지 못한 부분에 대한 정보."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parts: tuple[str, ...] = ()
    rows: int | None = None
    basis: Literal["rows", "parts"] = "rows"


class ColumnIssueCount(BaseModel):
    """컬럼 하나에서 발견된 이상치 유형별 개수."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    missing: int = 0
    outlier: int = 0
    type_error: int = 0


class Manifest(BaseModel):
    """한 번의 실행이 남기는 실행 기록 전체.

    source_id: 소스 ID
    window_start: 윈도우 시작 시간
    window_end: 윈도우 종료 시간
    status: 실행 상태
    stage: 실행 단계
    failure_reason: 실패 이유
    attempt: 재시도 횟수
    revision: authoritative source correction ordinal
    started_at: 시작 시간
    ended_at: 종료 시간
    duration_ms: 실행 시간
    artifacts: 실행 결과물
    counts: 개수 정보
    missing: 누락 정보
    drop_ratio: 드롭 비율
    completeness: 완료 비율
    backfill_status: 폐기된 지연 백필의 상태. 저장된 manifest 호환용으로 항상 None
    column_issues: 컬럼 이슈
    policy_actions: 정책 실행
    config_version: 설정 버전
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    window_start: datetime
    window_end: datetime
    status: RunStatus
    stage: StageField
    failure_reason: FailureReason | None = None
    attempt: int = 1
    revision: int = 0
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: int | None = None
    artifacts: Artifacts = Artifacts()
    counts: Counts = Counts()
    missing: Missing = Missing()
    drop_ratio: float | None = None
    completeness: float | None = None
    backfill_status: Literal["pending", "expired"] | None = None
    column_issues: dict[str, ColumnIssueCount] = {}
    policy_actions: dict[str, int] = {}
    config_version: str


@dataclass(frozen=True, slots=True)
class PublishedSourceSnapshot:
    """마지막 기록까지 검증한 authority manifest와 exact URI를 묶는다."""

    manifest: SourceSnapshotManifest
    manifest_uri: str


def load(source_id: str, window_start: datetime) -> Manifest | None:
    """해당 윈도우의 manifest를 읽어 모델로 변환한다. 없으면 None을 반환한다."""

    data = storage.read_manifest(source_id, window_start)
    return None if data is None else Manifest.model_validate(data)


def save(manifest: Manifest) -> None:
    """manifest를 dict로 직렬화해 저장한다."""

    storage.write_manifest(
        manifest.source_id, manifest.window_start, manifest.model_dump(mode="json")
    )


def load_window_manifests(
    source_id: str, day: date, hour: str | None = None
) -> list[Manifest]:
    """해당 KST 날짜(및 선택적으로 시)에 속한 manifest를 모두 읽어 모델로 변환한다."""

    return [
        Manifest.model_validate(d)
        for d in storage.list_window_manifest_payloads(source_id, day, hour)
    ]


def summarize_window(manifests: list[Manifest]) -> dict:
    """manifest 목록을 데이터 수집 모니터링 알림용 통계로 요약한다.

    행(row) 단위로만 집계한다 — 컬럼 값 단위 `column_issues`(결측·이상치)는 소스마다
    "정상 기준선"이 크게 달라(예: 비회원 대여의 성별·생년 컬럼은 평소에도 결측이
    30%에 가깝다) 일률적인 임계값을 적용하면 노이즈가 크다. 대신 collector가 이미
    각 실행마다 정확히 남기는 `RunStatus`와 `Counts`(fetched·kept·dropped)를 그대로
    합산한다 — quality gate(collector/pipeline.py)가 내린 성공/실패 판정과
    "실제로 몇 행을 살렸고 몇 행을 버렸는지"만 본다.
    """
    status_counts: dict[str, int] = {}
    fetched = kept = dropped = repaired = 0
    for m in manifests:
        status_counts[m.status.value] = status_counts.get(m.status.value, 0) + 1
        fetched += m.counts.fetched
        kept += m.counts.kept
        dropped += m.counts.dropped
        repaired += m.counts.repaired
    return {
        "run_count": len(manifests),
        "status_counts": status_counts,
        "fetched_count": fetched,
        "kept_count": kept,
        "dropped_count": dropped,
        "repaired_count": repaired,
    }


def load_source_snapshots(
    source_id: str,
    logical_dttm: datetime,
) -> tuple[PublishedSourceSnapshot, ...]:
    """논리 source window의 immutable manifest revision chain을 검증해 읽는다."""
    snapshots: list[PublishedSourceSnapshot] = []
    for uri, payload in storage.list_source_snapshot_manifest_payloads(
        source_id, logical_dttm
    ):
        parsed = parse_source_snapshot_manifest(payload)
        if parsed.source_id != source_id or parsed.logical_dttm != logical_dttm:
            raise SourceSnapshotContractError(
                "source snapshot manifest의 source/logical identity가 경로와 다릅니다."
            )
        expected_uri = storage.source_snapshot_manifest_uri(
            source_id, logical_dttm, parsed.revision_no
        )
        if uri != expected_uri:
            raise SourceSnapshotContractError(
                "source snapshot manifest revision URI가 canonical 경로와 다릅니다."
            )
        snapshots.append(PublishedSourceSnapshot(parsed, uri))

    snapshots.sort(key=lambda item: item.manifest.revision_no)
    revisions = tuple(item.manifest.revision_no for item in snapshots)
    if revisions != tuple(range(len(snapshots))):
        raise SourceSnapshotContractError(
            "source snapshot revision은 0부터 빈틈없이 증가해야 합니다."
        )
    return tuple(snapshots)


def publish_source_snapshot(
    *,
    source_id: str,
    logical_dttm: datetime,
    status: SourceSnapshotStatus,
    config_version: str,
    silver: storage.ImmutableSilverArtifact | None,
    counts: Counts,
    planned_parts: tuple[str, ...],
    completed_parts: tuple[str, ...],
) -> PublishedSourceSnapshot:
    """Output 완성 뒤 source snapshot revision을 결정해 manifest를 마지막 기록한다.

    최초 authoritative 결과는 revision 0이다. 직전 결과와 status, Silver checksum,
    count, plan, config가 모두 같으면 같은 revision의 exact replay이며, 하나라도 달라진
    correction은 다음 revision을 사용한다. PARTIAL/FAILED는 이 함수의 status 타입으로
    표현할 수 없어 authority manifest를 열 수 없다.
    """
    prepared = prepare_source_snapshot(
        source_id=source_id,
        logical_dttm=logical_dttm,
        status=status,
        config_version=config_version,
        silver=silver,
        counts=counts,
        planned_parts=planned_parts,
        completed_parts=completed_parts,
    )
    return finalize_source_snapshot(prepared)


def prepare_source_snapshot(
    *,
    source_id: str,
    logical_dttm: datetime,
    status: SourceSnapshotStatus,
    config_version: str,
    silver: storage.ImmutableSilverArtifact | None,
    counts: Counts,
    planned_parts: tuple[str, ...],
    completed_parts: tuple[str, ...],
) -> PublishedSourceSnapshot:
    """Output identity를 검증하고 쓸 authority revision을 부작용 없이 준비한다."""
    if type(counts) is not Counts:
        raise SourceSnapshotContractError("counts는 collector Counts여야 합니다.")
    typed_counts = SourceSnapshotCounts(
        expected=counts.expected,
        fetched=counts.fetched,
        kept=counts.kept,
        repaired=counts.repaired,
        dropped=counts.dropped,
    )
    revisions = load_source_snapshots(source_id, logical_dttm)
    candidate_revision = 0 if not revisions else revisions[-1].manifest.revision_no + 1
    candidate = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical_dttm,
        revision_no=candidate_revision,
        status=status,
        config_version=config_version,
        silver_uri=None if silver is None else silver.uri,
        silver_byte_sha256=None if silver is None else silver.byte_sha256,
        counts=typed_counts,
        planned_parts=planned_parts,
        completed_parts=completed_parts,
    )

    if revisions and same_source_snapshot_content(revisions[-1].manifest, candidate):
        candidate = revisions[-1].manifest

    return PublishedSourceSnapshot(
        candidate,
        storage.source_snapshot_manifest_uri(
            source_id, logical_dttm, candidate.revision_no
        ),
    )


def finalize_source_snapshot(
    prepared: PublishedSourceSnapshot,
) -> PublishedSourceSnapshot:
    """준비한 authority manifest를 last put-once/readback으로 확정한다."""
    if type(prepared) is not PublishedSourceSnapshot:
        raise SourceSnapshotContractError(
            "prepared snapshot은 PublishedSourceSnapshot이어야 합니다."
        )
    candidate = prepared.manifest

    manifest_uri = storage.write_source_snapshot_manifest(
        candidate.source_id,
        candidate.logical_dttm,
        candidate.revision_no,
        candidate.canonical_bytes,
    )
    if manifest_uri != prepared.manifest_uri:
        raise SourceSnapshotContractError(
            "준비한 source snapshot manifest URI와 기록 URI가 다릅니다."
        )
    return PublishedSourceSnapshot(candidate, manifest_uri)
