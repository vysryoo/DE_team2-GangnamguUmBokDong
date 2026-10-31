"""window 하나를 수집해 Bronze·Silver·manifest로 남기는 파이프라인.

흐름: manifest 로드 → 재개 분기 → fetch(조각마다 Bronze 즉시 저장) → 완결도 게이트
→ normalize → 검증 → quarantine·Silver 저장 → manifest 마감.

- 완료 window는 authority만 확인하고 `SKIPPED`를 반환한다. `stage>=bronze_written`이면
  기존 Bronze를 재사용한다. 실시간 API는 지난 시점 데이터를 다시 받을 수 없기 때문이다.
- fetch 실패는 `BRONZE_WRITTEN`, 저장·품질 실패는 `VALIDATED`에서 멈춘다. `quality_gate`를
  `COMPLETED`로 올리면 다음 실행이 `SKIPPED`로 빠져 실패가 성공처럼 보인다.
- 게이트에 걸리면 Silver를 쓰지 않는다. quarantine은 원인 분석용이라 폐기 행이 있으면 쓴다.
- `attempt`는 진단 실행 횟수, `revision`은 authority content의 correction ordinal이다.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
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
    revision: int
    bronze: BronzeArtifacts
    chunks: dict[str, bytes]
    missing_keys: dict[str, FetchErrorKind]
    expected_total: int | None
    planned_parts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Completeness:
    """완결도 게이트를 통과한 뒤 확정한 cardinality와 누락 정보."""

    expected_total: int | None
    ratio: float
    missing: Missing


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
        force: 재개 분기를 무시하고 새 Hot revision에 전체 재수집한다.
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
        _verify_completed_authority(config, window_start, existing, planned_parts)
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
    aborted = _fail_if_fetch_aborted(run)
    if aborted is not None:
        return aborted

    rows = adapter_cls.normalize([run.chunks[key] for key in sorted(run.chunks)], config)
    completeness = _check_completeness(run, len(rows))
    if isinstance(completeness, Manifest):
        return completeness
    if not rows:
        return _finish_empty(run, completeness)
    return _validate_and_store(run, rows, completeness)


def _verify_completed_authority(
    config: SourceConfig,
    window_start: datetime,
    value: Manifest,
    planned_parts: frozenset[str] | None,
) -> None:
    """완료 진단 manifest가 이미 확정된 authority와 같은지 검증하고 다시 확정한다.

    직전 실행이 진단 manifest만 쓰고 authority 기록 전에 죽었으면 여기서 채운다.
    """
    if value.status not in {RunStatus.SUCCEEDED, RunStatus.EMPTY}:
        return

    silver = None
    if value.status is RunStatus.SUCCEEDED:
        if value.artifacts.silver is None:
            raise RuntimeError("완료 SUCCEEDED manifest에 Silver key가 없습니다.")
        try:
            silver = storage.read_immutable_silver_artifact(
                value.artifacts.silver, row_count=value.counts.kept
            )
        except ValueError:
            # 전환 전 mutable Silver는 authority로 승격하지 않는다.
            return

    snapshots = manifest_module.load_source_snapshots(config.source_id, window_start)
    if not snapshots:
        raise RuntimeError(
            "content-addressed 완료 run에 source authority manifest가 없습니다."
        )
    prepared = manifest_module.prepare_source_snapshot(
        source_id=config.source_id,
        logical_dttm=window_start,
        status=(
            SourceSnapshotStatus.SUCCEEDED
            if value.status is RunStatus.SUCCEEDED
            else SourceSnapshotStatus.EMPTY
        ),
        config_version=value.config_version,
        silver=silver,
        counts=value.counts,
        planned_parts=_effective_plan(planned_parts, value.artifacts.bronze.parts),
        completed_parts=value.artifacts.bronze.parts,
    )
    if prepared.manifest != snapshots[-1].manifest:
        raise RuntimeError("mutable 진단 manifest가 확정된 source authority와 다릅니다.")
    manifest_module.finalize_source_snapshot(prepared)


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
    """기존 Bronze를 재사용하거나 새 Hot revision에 fetch해 실행 상태를 만든다."""
    source_id = config.source_id
    window_start = window.window_start
    started_at = _now()

    if existing and existing.stage >= Stage.BRONZE_WRITTEN and not force:
        revision = existing.artifacts.bronze.revision
        fetched = _reuse_bronze(source_id, window_start, existing)
    else:
        revision = storage.next_bronze_revision(source_id, window_start)
        fetched = fetch_with_rounds(
            adapter_cls.fetch,
            config,
            window,
            client=client,
            sleep_fn=sleep_fn,
            on_chunk=lambda key, payload: storage.write_bronze_part(
                source_id, window_start, key, payload, revision
            ),
            planned_parts=planned_parts,
        )

    bronze_parts = tuple(sorted(fetched.chunks))
    return _WindowRun(
        config=config,
        window=window,
        started_at=started_at,
        attempt=(existing.attempt + 1) if existing else 1,
        revision=existing.revision if existing else 0,
        bronze=BronzeArtifacts(
            prefix=storage.bronze_prefix(source_id, window_start, revision),
            parts=bronze_parts,
            revision=revision,
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


def _fail_if_fetch_aborted(run: _WindowRun) -> Manifest | None:
    """normalize 전에 끝내야 하는 fetch 결과면 FAILED manifest를 반환한다."""
    if FetchErrorKind.FATAL in run.missing_keys.values():
        # 인증 오류는 나머지 조각도 같은 이유로 실패하므로 게이트·검증 없이 끝낸다.
        detail = "reason=fatal"
    elif _is_probe(run.config) and run.expected_total is None:
        # 종료 페이지를 보기 전에 끝나면 전체 cardinality를 모른다. 잘린 대여소
        # snapshot이 삭제·비활성화 authority가 되지 않게 막는다.
        detail = "reason=pagination_unconfirmed"
    else:
        return None
    return _fail_fetch(
        run,
        detail,
        missing=_build_missing(run.missing_keys, run.expected_total, 0),
        counts=Counts(),
    )


def _check_completeness(run: _WindowRun, fetched_rows: int) -> _Completeness | Manifest:
    """행 수로 cardinality를 확정하고 누락 게이트를 판정한다.

    게이트에 걸리면 FAILED manifest를, 통과하면 확정한 완결도 정보를 반환한다.
    """
    quality = run.config.quality
    # probe의 expected_total은 종료 페이지를 봤다는 표시일 뿐이다. 재시도 라운드는
    # 이미 받은 page를 건너뛰므로 전체 Bronze의 실제 행 수로 확정한다.
    expected_total = fetched_rows if _is_probe(run.config) else run.expected_total
    parts_summary = f"{len(run.chunks)}/{len(run.chunks) + len(run.missing_keys)}"

    def fail(detail: str) -> Manifest:
        return _fail_fetch(
            run,
            detail,
            missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
            counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    if expected_total is not None and fetched_rows > expected_total:
        # 진행 중인 window는 list_total_count보다 많은 행이 올 수 있다. 페이지 중복
        # 병합 같은 사고는 최소 +100%라 작은 허용치로도 걸러진다.
        allowed_total = expected_total * (1 + quality.max_overfetch_ratio)
        detail = (
            f"parts={parts_summary} rows={fetched_rows} expected={expected_total} "
            f"allowed={allowed_total:.0f}"
        )
        if fetched_rows > allowed_total:
            return fail(f"reason=fetched_exceeds_expected {detail}")
        logger.warning(
            f"stage=bronze_written reason=fetched_exceeds_expected_within_tolerance {detail}"
        )
        # Gold의 SUCCEEDED 계약이 expected==fetched를 요구한다.
        expected_total = fetched_rows

    ratio = _missing_ratio(
        len(run.missing_keys), expected_total, fetched_rows, len(run.chunks)
    )
    if ratio > quality.max_missing_ratio and not math.isclose(
        ratio, quality.max_missing_ratio, rel_tol=0.0, abs_tol=1e-12
    ):
        return fail(f"parts={parts_summary} missing_ratio={ratio:.3f}")

    _log_bronze_written(run, parts_summary, fetched_rows, ratio)
    return _Completeness(
        expected_total=expected_total,
        ratio=ratio,
        missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
    )


def _log_bronze_written(
    run: _WindowRun, parts_summary: str, fetched_rows: int, ratio: float
) -> None:
    """bronze_written 단계 경계 로그 한 줄을 남긴다."""
    bronze_bytes = sum(len(v) for v in run.chunks.values())
    summary = f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes}"
    if run.missing_keys:
        logger.warning(
            f"{summary} missing={','.join(sorted(run.missing_keys))} "
            f"completeness={1.0 - ratio:.3f}"
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
    counts = Counts(expected=completeness.expected_total)
    if run.config.quality.allow_empty and confirmed_empty:
        logger.info("stage=completed status=empty")
        return _finish(
            run,
            status=RunStatus.EMPTY,
            stage=Stage.COMPLETED,
            missing=completeness.missing,
            counts=counts,
            completeness=1.0,
            authority_status=SourceSnapshotStatus.EMPTY,
        )
    return _fail_validated(
        run,
        FailureReason.QUALITY_GATE,
        "rows=0 reason=unconfirmed_empty",
        missing=completeness.missing,
        counts=counts,
    )


def _validate_and_store(
    run: _WindowRun, rows: list[dict], completeness: _Completeness
) -> Manifest:
    """행을 검증해 폐기 게이트를 판정한 뒤 산출물 저장으로 넘긴다."""
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
            missing=completeness.missing,
            counts=Counts(expected=completeness.expected_total, fetched=len(rows)),
        )

    exceeds_drop_gate = outcome.drop_ratio > config.quality.max_drop_ratio
    _log_validated(outcome, completeness, exceeds_drop_gate)
    return _store_outputs(run, outcome, completeness, exceeds_drop_gate)


def _store_outputs(
    run: _WindowRun,
    outcome: BatchOutcome,
    completeness: _Completeness,
    exceeds_drop_gate: bool,
) -> Manifest:
    """quarantine을 저장하고, 폐기 게이트를 통과했으면 Silver 저장으로 넘긴다."""
    fields = {
        "missing": completeness.missing,
        "counts": Counts(expected=completeness.expected_total, **outcome.counts),
        "column_issues": {
            col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()
        },
        "policy_actions": outcome.policy_actions,
        "drop_ratio": outcome.drop_ratio,
    }
    try:
        quarantine_key = storage.write_quarantine(
            run.config.source_id, run.window.window_start, outcome.quarantine_records
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        return _fail_validated(
            run, FailureReason.STORAGE_ERROR, "reason=quarantine_write", **fields
        )

    artifacts = Artifacts(bronze=run.bronze, quarantine=quarantine_key)
    if exceeds_drop_gate:
        return _finish(
            run,
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            artifacts=artifacts,
            **fields,
        )
    return _store_silver(run, outcome, completeness, artifacts, fields)


def _store_silver(
    run: _WindowRun,
    outcome: BatchOutcome,
    completeness: _Completeness,
    artifacts: Artifacts,
    fields: dict,
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
            artifacts=artifacts,
            **fields,
        )

    succeeded = outcome.counts["dropped"] == 0 and completeness.ratio == 0.0
    status = RunStatus.SUCCEEDED if succeeded else RunStatus.PARTIAL
    logger.info(f"stage=completed status={status.value} key={silver.key}")
    # PARTIAL은 authority manifest를 열지 않는다.
    return _finish(
        run,
        status=status,
        stage=Stage.COMPLETED,
        completeness=1.0 - completeness.ratio,
        artifacts=artifacts.model_copy(update={"silver": silver.key}),
        authority_status=SourceSnapshotStatus.SUCCEEDED if succeeded else None,
        authority_silver=silver if succeeded else None,
        **fields,
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


def _fail_fetch(
    run: _WindowRun, detail: str, *, missing: Missing, counts: Counts
) -> Manifest:
    """fetch_error로 실패한 manifest를 BRONZE_WRITTEN 단계로 마감한다."""
    logger.error(
        f"stage=bronze_written status=failed failure_reason=fetch_error {detail}"
    )
    return _finish(
        run, failure_reason=FailureReason.FETCH_ERROR, missing=missing, counts=counts
    )


def _fail_validated(
    run: _WindowRun, failure_reason: FailureReason, detail: str, **fields
) -> Manifest:
    """검증 이후 단계의 실패를 로그로 남기고 VALIDATED 단계로 마감한다."""
    logger.error(
        f"stage=validated status=failed failure_reason={failure_reason.value} {detail}"
    )
    return _finish(
        run, stage=Stage.VALIDATED, failure_reason=failure_reason, **fields
    )


def _finish(
    run: _WindowRun,
    *,
    authority_status: SourceSnapshotStatus | None = None,
    authority_silver: storage.ImmutableSilverArtifact | None = None,
    **fields,
) -> Manifest:
    """진단 manifest를 저장하고, authority 대상이면 authority manifest를 마지막에 쓴다.

    authority revision을 먼저 결정해 진단 manifest의 `revision`에 싣는다. authority
    확정에 실패하면 진단 manifest를 `storage_error`로 다시 저장한다.
    """
    prepared = None
    if authority_status is not None:
        prepared = manifest_module.prepare_source_snapshot(
            source_id=run.config.source_id,
            logical_dttm=run.window.window_start,
            status=authority_status,
            config_version=run.config.config_version,
            silver=authority_silver,
            counts=fields["counts"],
            planned_parts=run.planned_parts,
            completed_parts=run.bronze.parts,
        )
        fields["revision"] = prepared.manifest.revision_no

    result = _build_manifest(run, **fields)
    manifest_module.save(result)
    if prepared is None:
        return result

    try:
        manifest_module.finalize_source_snapshot(prepared)
    except Exception as exc:  # noqa: BLE001 — authority 부재를 명시적 실패로 남긴다
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
    return result


def _build_manifest(run: _WindowRun, **fields) -> Manifest:
    """가장 이른 실패 지점(FAILED, BRONZE_WRITTEN)을 기본값으로 manifest를 만든다."""
    defaults = {
        "source_id": run.config.source_id,
        "window_start": run.window.window_start,
        "window_end": run.window.window_end,
        "status": RunStatus.FAILED,
        "stage": Stage.BRONZE_WRITTEN,
        "attempt": run.attempt,
        "revision": run.revision,
        "started_at": run.started_at,
        "ended_at": _now(),
        "artifacts": Artifacts(bronze=run.bronze),
        "config_version": run.config.config_version,
    }
    return Manifest(**{**defaults, **fields})


def _now() -> datetime:
    """manifest의 started_at·ended_at에 쓸 KST 현재 시각을 반환한다."""
    return datetime.now(KST)


def _is_probe(config: SourceConfig) -> bool:
    """빈 종료 페이지까지 탐색하는 서울 API 소스인지 판별한다."""
    return config.adapter_params.get("pagination") == "probe_until_empty"


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
