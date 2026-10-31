"""window 하나를 수집해 Bronze·Silver·manifest로 남기는 파이프라인.

흐름: 재개 분기 → fetch(조각마다 Bronze 즉시 저장) → 완결도 게이트 → normalize·검증
→ quarantine·Silver 저장 → window manifest·source snapshot 기록.

- window manifest는 실행마다 덮어쓰는 진단 기록이고, source snapshot은 SUCCEEDED·EMPTY일
  때만 쓰는 immutable authority다. window manifest의 `revision`은 snapshot 정정 순번이다.
- 완료 window는 snapshot만 다시 확인하고 `SKIPPED`를 반환한다. `stage>=bronze_written`이면
  기존 Bronze를 재사용한다. 실시간 API는 지난 시점 데이터를 다시 받을 수 없기 때문이다.
- fetch 실패는 `BRONZE_WRITTEN`, 저장·품질 실패는 `VALIDATED`에서 멈춘다. `COMPLETED`로
  올리면 다음 실행이 `SKIPPED`로 빠져 실패가 성공처럼 보인다.
- 게이트에 걸리면 Silver를 쓰지 않는다. quarantine은 원인 분석용이라 폐기 행이 있으면 쓴다.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa
from core.kst import KST
from core.source_snapshot import SourceSnapshotStatus

import manifest as manifest_module
import storage
from adapters.base import (
    FetchErrorKind,
    FetchRoundResult,
    Window,
    fetch_with_rounds,
    get_adapter,
)
from config.schema import SourceConfig
from manifest import (
    Artifacts,
    BronzeArtifacts,
    ColumnIssueCount,
    Counts,
    FailureReason,
    Manifest,
    Missing,
    RunStatus,
    Stage,
)
from validation.engine import BatchOutcome, BatchValidationFailed, validate_batch
from validation.types import RunContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _WindowRun:
    """fetch(또는 Bronze 재사용)를 마친 실행 하나의 상태."""

    config: SourceConfig
    window: Window
    started_at: datetime
    attempt: int
    snapshot_revision: int
    bronze: BronzeArtifacts
    chunks: dict[str, bytes]
    missing_keys: dict[str, FetchErrorKind]
    expected_total: int | None
    planned_parts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Completeness:
    """행 수로 확정한 cardinality와 누락 정보. `gate_failure`가 있으면 fetch 실패다."""

    expected_total: int | None
    ratio: float
    missing: Missing
    gate_failure: str | None

    def measured(self, fetched_rows: int) -> _Measurements:
        """검증 결과 없이 마감할 때 쓸 manifest 측정값을 만든다."""
        counts = Counts(expected=self.expected_total, fetched=fetched_rows)
        return _Measurements(missing=self.missing, counts=counts)


@dataclass(frozen=True, slots=True)
class _Measurements:
    """window manifest에 남길 누락·행 집계·검증 이슈."""

    missing: Missing = field(default_factory=Missing)
    counts: Counts = field(default_factory=Counts)
    column_issues: dict[str, ColumnIssueCount] = field(default_factory=dict)
    policy_actions: dict[str, int] = field(default_factory=dict)
    drop_ratio: float | None = None


def execute_window(
    config: SourceConfig,
    window_start: datetime,
    *,
    client,
    force: bool = False,
    sleep_fn=time.sleep,
) -> Manifest:
    """window 하나를 재개 분기에 따라 처리하고 최종 manifest를 반환한다.

    실패해도 예외를 올리지 않고 FAILED manifest를 반환해, 호출자(CLI)가 `status`만
    보고 종료 코드를 정하게 한다. 단, manifest 저장 자체가 실패하면 그대로 올린다.

    Args:
        config: 소스 설정.
        window_start: 수집 대상 window의 시작 시각.
        client: 어댑터에 주입할 httpx 클라이언트.
        force: 재개 분기를 무시하고 새 Bronze revision에 전체 재수집한다.
        sleep_fn: 라운드 간 대기 함수.
    Returns:
        이번 실행이 도달한 최종 manifest.
    """
    window = Window(
        window_start=window_start, window_end=window_start + config.schedule.interval
    )
    adapter_cls = get_adapter(config.adapter)
    planned_parts = adapter_cls.planned_parts(config, window)
    existing = manifest_module.load(config.source_id, window_start)

    if existing and existing.stage == Stage.COMPLETED and not force:
        _reconfirm_source_snapshot(config, window_start, existing, planned_parts)
        logger.info("stage=completed status=skipped")
        return existing.model_copy(update={"status": RunStatus.SKIPPED})

    run = _load_bronze(
        config,
        window,
        existing,
        adapter_cls=adapter_cls,
        planned_parts=planned_parts,
        force=force,
        client=client,
        sleep_fn=sleep_fn,
    )
    abort_reason = _fetch_abort_reason(run)
    if abort_reason is not None:
        missing = _build_missing(run.missing_keys, run.expected_total, 0)
        return _fail_fetch(run, abort_reason, _Measurements(missing=missing))

    rows = adapter_cls.normalize([run.chunks[key] for key in sorted(run.chunks)], config)
    completeness = _measure_completeness(run, len(rows))
    if completeness.gate_failure is not None:
        return _fail_fetch(run, completeness.gate_failure, completeness.measured(len(rows)))
    _log_bronze_written(run, len(rows), completeness)

    if not rows:
        return _finish_empty(run, completeness)
    return _validate_and_store(run, rows, completeness)


def _reconfirm_source_snapshot(
    config: SourceConfig,
    window_start: datetime,
    completed: Manifest,
    planned_parts: frozenset[str] | None,
) -> None:
    """완료 window manifest가 기록된 source snapshot과 같은지 검증하고 다시 확정한다.

    직전 실행이 window manifest만 쓰고 source snapshot 기록 전에 죽었으면 여기서 채운다.
    """
    if completed.status not in {RunStatus.SUCCEEDED, RunStatus.EMPTY}:
        return

    silver = None
    if completed.status is RunStatus.SUCCEEDED:
        if completed.artifacts.silver is None:
            raise RuntimeError("완료 SUCCEEDED manifest에 Silver key가 없습니다.")
        try:
            silver = storage.read_immutable_silver_artifact(
                completed.artifacts.silver, row_count=completed.counts.kept
            )
        except ValueError:
            # 전환 전 mutable Silver는 source snapshot으로 승격하지 않는다.
            return

    snapshots = manifest_module.load_source_snapshots(config.source_id, window_start)
    if not snapshots:
        raise RuntimeError(
            "content-addressed 완료 run에 source authority manifest가 없습니다."
        )
    snapshot = manifest_module.prepare_source_snapshot(
        source_id=config.source_id,
        logical_dttm=window_start,
        status=(
            SourceSnapshotStatus.SUCCEEDED
            if completed.status is RunStatus.SUCCEEDED
            else SourceSnapshotStatus.EMPTY
        ),
        config_version=completed.config_version,
        silver=silver,
        counts=completed.counts,
        planned_parts=_effective_plan(planned_parts, completed.artifacts.bronze.parts),
        completed_parts=completed.artifacts.bronze.parts,
    )
    if snapshot.manifest != snapshots[-1].manifest:
        raise RuntimeError("mutable 진단 manifest가 확정된 source authority와 다릅니다.")
    manifest_module.finalize_source_snapshot(snapshot)


def _load_bronze(
    config: SourceConfig,
    window: Window,
    existing: Manifest | None,
    *,
    adapter_cls: type,
    planned_parts: frozenset[str] | None,
    force: bool,
    client,
    sleep_fn,
) -> _WindowRun:
    """기존 Bronze를 재사용하거나 새 Bronze revision에 fetch해 실행 상태를 만든다."""
    source_id = config.source_id
    window_start = window.window_start
    started_at = _now()

    if existing and existing.stage >= Stage.BRONZE_WRITTEN and not force:
        bronze_revision = existing.artifacts.bronze.revision
        fetched = _reuse_bronze(source_id, window_start, existing)
    else:
        bronze_revision = storage.next_bronze_revision(source_id, window_start)
        fetched = fetch_with_rounds(
            adapter_cls.fetch,
            config,
            window,
            client=client,
            sleep_fn=sleep_fn,
            on_chunk=lambda key, payload: storage.write_bronze_part(
                source_id, window_start, key, payload, bronze_revision
            ),
            planned_parts=planned_parts,
        )

    bronze_parts = tuple(sorted(fetched.chunks))
    return _WindowRun(
        config=config,
        window=window,
        started_at=started_at,
        attempt=(existing.attempt + 1) if existing else 1,
        snapshot_revision=existing.revision if existing else 0,
        bronze=BronzeArtifacts(
            prefix=storage.bronze_prefix(source_id, window_start, bronze_revision),
            parts=bronze_parts,
            revision=bronze_revision,
        ),
        chunks=fetched.chunks,
        missing_keys=fetched.missing,
        expected_total=fetched.expected_total,
        planned_parts=_effective_plan(
            planned_parts, bronze_parts, tuple(sorted(fetched.missing))
        ),
    )


def _reuse_bronze(
    source_id: str, window_start: datetime, existing: Manifest
) -> FetchRoundResult:
    """manifest가 가리키는 Bronze revision을 읽어 fetch 결과를 복원한다."""
    parts = existing.artifacts.bronze.parts
    payloads = storage.read_bronze(
        source_id, window_start, parts, existing.artifacts.bronze.revision
    )
    # 조각을 다시 받지 않으므로 실패 종류는 게이트 계산에 영향이 없다.
    return FetchRoundResult(
        chunks=dict(zip(parts, payloads)),
        missing={key: FetchErrorKind.PERMANENT for key in existing.missing.parts},
        expected_total=existing.counts.expected,
    )


def _fetch_abort_reason(run: _WindowRun) -> str | None:
    """normalize 전에 끝내야 하는 fetch 결과면 실패 사유를 반환한다."""
    if FetchErrorKind.FATAL in run.missing_keys.values():
        # 인증 오류는 나머지 조각도 같은 이유로 실패하므로 게이트·검증 없이 끝낸다.
        return "reason=fatal"
    if _is_probe(run.config) and run.expected_total is None:
        # 종료 페이지를 보기 전에 끝나면 전체 cardinality를 모른다. 잘린 대여소
        # snapshot이 삭제·비활성화 authority가 되지 않게 막는다.
        return "reason=pagination_unconfirmed"
    return None


def _measure_completeness(run: _WindowRun, fetched_rows: int) -> _Completeness:
    """행 수로 cardinality를 확정하고 overfetch·누락 게이트를 판정한다."""
    quality = run.config.quality
    # probe의 expected_total은 종료 페이지를 봤다는 표시일 뿐이다. 재시도 라운드는
    # 이미 받은 page를 건너뛰므로 전체 Bronze의 실제 행 수로 확정한다.
    expected_total = fetched_rows if _is_probe(run.config) else run.expected_total
    gate_failure = None

    if expected_total is not None and fetched_rows > expected_total:
        gate_failure = _overfetch_failure(run, fetched_rows, expected_total)
        if gate_failure is None:
            # Gold의 SUCCEEDED 계약이 expected==fetched를 요구한다.
            expected_total = fetched_rows

    ratio = _missing_ratio(
        len(run.missing_keys), expected_total, fetched_rows, len(run.chunks)
    )
    if (
        gate_failure is None
        and ratio > quality.max_missing_ratio
        and not math.isclose(ratio, quality.max_missing_ratio, rel_tol=0.0, abs_tol=1e-12)
    ):
        gate_failure = f"parts={_parts_summary(run)} missing_ratio={ratio:.3f}"

    return _Completeness(
        expected_total=expected_total,
        ratio=ratio,
        missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
        gate_failure=gate_failure,
    )


def _overfetch_failure(
    run: _WindowRun, fetched_rows: int, expected_total: int
) -> str | None:
    """expected보다 많이 받은 행이 허용치를 넘으면 실패 사유를, 이내면 None을 반환한다.

    진행 중인 window는 list_total_count보다 많은 행이 올 수 있다. 페이지 중복 병합 같은
    사고는 최소 +100%라 작은 허용치로도 걸러진다.
    """
    allowed_total = expected_total * (1 + run.config.quality.max_overfetch_ratio)
    detail = (
        f"parts={_parts_summary(run)} rows={fetched_rows} expected={expected_total} "
        f"allowed={allowed_total:.0f}"
    )
    if fetched_rows > allowed_total:
        return f"reason=fetched_exceeds_expected {detail}"
    logger.warning(
        f"stage=bronze_written reason=fetched_exceeds_expected_within_tolerance {detail}"
    )
    return None


def _log_bronze_written(
    run: _WindowRun, fetched_rows: int, completeness: _Completeness
) -> None:
    """bronze_written 단계 경계 로그 한 줄을 남긴다."""
    bronze_bytes = sum(len(v) for v in run.chunks.values())
    summary = (
        f"stage=bronze_written parts={_parts_summary(run)} rows={fetched_rows} "
        f"bytes={bronze_bytes}"
    )
    if run.missing_keys:
        logger.warning(
            f"{summary} missing={','.join(sorted(run.missing_keys))} "
            f"completeness={1.0 - completeness.ratio:.3f}"
        )
    else:
        logger.info(summary)


def _finish_empty(run: _WindowRun, completeness: _Completeness) -> Manifest:
    """행이 0건인 window를 EMPTY 또는 quality_gate 실패로 마감한다."""
    confirmed_empty = (
        completeness.expected_total == 0
        and not completeness.missing.parts
        and bool(run.bronze.parts)
        and run.planned_parts == run.bronze.parts
    )
    measured = completeness.measured(0)
    if run.config.quality.allow_empty and confirmed_empty:
        logger.info("stage=completed status=empty")
        return _finish(
            run,
            measured,
            status=RunStatus.EMPTY,
            stage=Stage.COMPLETED,
            completeness=1.0,
            snapshot_status=SourceSnapshotStatus.EMPTY,
        )
    return _fail_validated(
        run, FailureReason.QUALITY_GATE, "rows=0 reason=unconfirmed_empty", measured
    )


def _validate_and_store(
    run: _WindowRun, rows: list[dict], completeness: _Completeness
) -> Manifest:
    """행을 검증하고 quarantine을 저장한 뒤, 폐기 게이트를 통과하면 Silver로 넘긴다."""
    config = run.config
    try:
        outcome = validate_batch(
            rows,
            config,
            RunContext(
                source_id=config.source_id,
                window_start=run.window.window_start,
                window_end=run.window.window_end,
                attempt=run.attempt,
            ),
        )
    except BatchValidationFailed:
        # 같은 Bronze·config면 재시도해도 결과가 같으므로 config를 고쳐야 한다.
        return _fail_validated(
            run,
            FailureReason.QUALITY_GATE,
            "reason=fail_batch",
            completeness.measured(len(rows)),
        )

    exceeds_drop_gate = outcome.drop_ratio > config.quality.max_drop_ratio
    _log_validated(outcome, completeness, exceeds_drop_gate)
    measured = _Measurements(
        missing=completeness.missing,
        counts=Counts(expected=completeness.expected_total, **outcome.counts),
        column_issues={
            col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()
        },
        policy_actions=outcome.policy_actions,
        drop_ratio=outcome.drop_ratio,
    )
    try:
        quarantine_key = storage.write_quarantine(
            config.source_id, run.window.window_start, outcome.quarantine_records
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        return _fail_validated(
            run, FailureReason.STORAGE_ERROR, "reason=quarantine_write", measured
        )

    artifacts = Artifacts(bronze=run.bronze, quarantine=quarantine_key)
    if exceeds_drop_gate:
        return _finish(
            run,
            measured,
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            artifacts=artifacts,
        )
    return _store_silver(run, outcome, completeness, measured, artifacts)


def _store_silver(
    run: _WindowRun,
    outcome: BatchOutcome,
    completeness: _Completeness,
    measured: _Measurements,
    artifacts: Artifacts,
) -> Manifest:
    """Silver를 저장하고 SUCCEEDED 또는 PARTIAL로 마감한다."""
    try:
        silver = storage.write_immutable_silver(
            run.config.source_id,
            run.window.window_start,
            pa.Table.from_pylist(outcome.silver_rows),
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        return _fail_validated(
            run,
            FailureReason.STORAGE_ERROR,
            "reason=silver_write",
            measured,
            artifacts=artifacts,
        )

    succeeded = outcome.counts["dropped"] == 0 and completeness.ratio == 0.0
    status = RunStatus.SUCCEEDED if succeeded else RunStatus.PARTIAL
    logger.info(f"stage=completed status={status.value} key={silver.key}")
    # PARTIAL은 source snapshot을 열지 않는다.
    return _finish(
        run,
        measured,
        status=status,
        stage=Stage.COMPLETED,
        completeness=1.0 - completeness.ratio,
        artifacts=artifacts.model_copy(update={"silver": silver.key}),
        snapshot_status=SourceSnapshotStatus.SUCCEEDED if succeeded else None,
        snapshot_silver=silver if succeeded else None,
    )


def _log_validated(
    outcome: BatchOutcome, completeness: _Completeness, exceeds_drop_gate: bool
) -> None:
    """validated 단계 경계 로그 한 줄을 남긴다."""
    counts = outcome.counts
    summary = (
        f"stage=validated kept={counts['kept']} repaired={counts['repaired']} "
        f"dropped={counts['dropped']} drop_ratio={outcome.drop_ratio:.3f} "
        f"completeness={1.0 - completeness.ratio:.3f}"
    )
    if exceeds_drop_gate:
        logger.error(f"{summary} status=failed failure_reason=quality_gate")
    elif counts["dropped"] or completeness.missing.parts:
        logger.warning(f"{summary} status=partial")
    else:
        logger.info(f"{summary} status=succeeded")


def _fail_fetch(run: _WindowRun, detail: str, measured: _Measurements) -> Manifest:
    """fetch_error로 실패한 manifest를 BRONZE_WRITTEN 단계로 마감한다."""
    logger.error(
        f"stage=bronze_written status=failed failure_reason=fetch_error {detail}"
    )
    return _finish(run, measured, failure_reason=FailureReason.FETCH_ERROR)


def _fail_validated(
    run: _WindowRun,
    failure_reason: FailureReason,
    detail: str,
    measured: _Measurements,
    *,
    artifacts: Artifacts | None = None,
) -> Manifest:
    """검증 이후 단계의 실패를 로그로 남기고 VALIDATED 단계로 마감한다."""
    logger.error(
        f"stage=validated status=failed failure_reason={failure_reason.value} {detail}"
    )
    return _finish(
        run,
        measured,
        stage=Stage.VALIDATED,
        failure_reason=failure_reason,
        artifacts=artifacts,
    )


def _finish(
    run: _WindowRun,
    measured: _Measurements,
    *,
    status: RunStatus = RunStatus.FAILED,
    stage: Stage = Stage.BRONZE_WRITTEN,
    failure_reason: FailureReason | None = None,
    artifacts: Artifacts | None = None,
    completeness: float | None = None,
    snapshot_status: SourceSnapshotStatus | None = None,
    snapshot_silver: storage.ImmutableSilverArtifact | None = None,
) -> Manifest:
    """window manifest를 저장하고, source snapshot 대상이면 snapshot을 마지막에 쓴다.

    snapshot revision을 먼저 결정해 window manifest의 `revision`에 싣는다. snapshot
    확정에 실패하면 window manifest를 `storage_error`로 다시 저장한다.
    """
    snapshot = None
    if snapshot_status is not None:
        snapshot = _prepare_snapshot(run, measured, snapshot_status, snapshot_silver)

    result = Manifest(
        source_id=run.config.source_id,
        window_start=run.window.window_start,
        window_end=run.window.window_end,
        status=status,
        stage=stage,
        failure_reason=failure_reason,
        attempt=run.attempt,
        revision=run.snapshot_revision if snapshot is None else snapshot.manifest.revision_no,
        started_at=run.started_at,
        ended_at=_now(),
        artifacts=Artifacts(bronze=run.bronze) if artifacts is None else artifacts,
        counts=measured.counts,
        missing=measured.missing,
        drop_ratio=measured.drop_ratio,
        completeness=completeness,
        column_issues=measured.column_issues,
        policy_actions=measured.policy_actions,
        config_version=run.config.config_version,
    )
    manifest_module.save(result)
    if snapshot is None:
        return result

    try:
        manifest_module.finalize_source_snapshot(snapshot)
    except Exception as exc:  # noqa: BLE001 — snapshot 부재를 명시적 실패로 남긴다
        return _fail_snapshot_write(result, exc)
    return result


def _prepare_snapshot(
    run: _WindowRun,
    measured: _Measurements,
    status: SourceSnapshotStatus,
    silver: storage.ImmutableSilverArtifact | None,
) -> manifest_module.PublishedSourceSnapshot:
    """이번 실행 결과로 쓸 source snapshot revision을 준비한다."""
    return manifest_module.prepare_source_snapshot(
        source_id=run.config.source_id,
        logical_dttm=run.window.window_start,
        status=status,
        config_version=run.config.config_version,
        silver=silver,
        counts=measured.counts,
        planned_parts=run.planned_parts,
        completed_parts=run.bronze.parts,
    )


def _fail_snapshot_write(result: Manifest, exc: Exception) -> Manifest:
    """source snapshot 기록 실패를 window manifest에 storage_error로 다시 저장한다."""
    failed = result.model_copy(
        update={
            "status": RunStatus.FAILED,
            "stage": Stage.VALIDATED,
            "failure_reason": FailureReason.STORAGE_ERROR,
        }
    )
    manifest_module.save(failed)
    logger.error(
        "stage=validated status=failed failure_reason=storage_error "
        f"reason=authority_manifest_write error={type(exc).__name__}"
    )
    return failed


def _now() -> datetime:
    """manifest의 started_at·ended_at에 쓸 KST 현재 시각을 반환한다."""
    return datetime.now(KST)


def _is_probe(config: SourceConfig) -> bool:
    """빈 종료 페이지까지 탐색하는 서울 API 소스인지 판별한다."""
    return config.adapter_params.get("pagination") == "probe_until_empty"


def _parts_summary(run: _WindowRun) -> str:
    """받은 조각 수와 전체 조각 수를 `받음/전체`로 나타낸다."""
    return f"{len(run.chunks)}/{len(run.chunks) + len(run.missing_keys)}"


def _effective_plan(
    planned_parts: frozenset[str] | None,
    completed: tuple[str, ...],
    missing: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """adapter 계획이 없으면 관측한 전체 part 집합을 계획으로 쓴다."""
    if planned_parts is None:
        return tuple(sorted(frozenset(completed) | frozenset(missing)))
    return tuple(sorted(planned_parts))


def _missing_ratio(
    missing_count: int,
    expected_total: int | None,
    fetched_rows: int,
    collected_count: int,
) -> float:
    """누락 비율을 계산한다. `expected_total`을 알면 행 기준, 모르면 조각 기준이다."""
    if expected_total is not None:
        return max(0.0, 1 - (fetched_rows / expected_total)) if expected_total else 0.0
    planned = collected_count + missing_count
    return (missing_count / planned) if planned else 0.0


def _build_missing(
    missing_keys: dict, expected_total: int | None, fetched_rows: int
) -> Missing:
    """manifest에 남길 Missing 필드를 만든다."""
    parts = tuple(sorted(missing_keys))
    if expected_total is not None:
        return Missing(
            parts=parts, rows=max(0, expected_total - fetched_rows), basis="rows"
        )
    return Missing(parts=parts, rows=None, basis="parts")
