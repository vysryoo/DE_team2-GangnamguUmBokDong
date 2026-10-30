"""window 하나를 수집해 Bronze·Silver·manifest로 남기는 파이프라인.

실행 순서는 manifest 로드, 재개 분기, fetch(조각마다 Bronze 즉시 저장), 완결도
게이트, normalize, 검증, Silver·quarantine 저장, manifest 마감이다. 단계를 넘어갈
때마다 manifest의 `stage`를 갱신해 다음 실행이 어디서부터 재개할지 알 수 있게 한다.

재개 분기:

| 조건 | 동작 |
| --- | --- |
| `stage=completed` (`--force` 아님) | authority 검증 후 `SKIPPED` 반환 |
| `stage>=bronze_written` (`--force` 아님) | 기존 revision의 Bronze 재사용(fetch 건너뜀) |
| 그 외 (또는 `--force`) | 새 immutable Hot revision에 전체 fetch |

실시간 API는 몇 분만 지나도 그 시점 데이터를 다시 받을 수 없으므로, 저장 실패 뒤
재실행은 반드시 기존 Bronze를 재사용한다.

status 결정 규칙:

| 조건 | status | failure_reason |
| --- | --- | --- |
| 행 0건 + `allow_empty=true` | `EMPTY` | — |
| 행 0건 + `allow_empty=false` | `FAILED` | `quality_gate` |
| `dropped==0` && 누락 없음 | `SUCCEEDED` | — |
| `dropped>0` 또는 누락 존재(게이트 이내) | `PARTIAL` | — |
| `missing_ratio > max_missing_ratio` | `FAILED` | `fetch_error` |
| `drop_ratio > max_drop_ratio` | `FAILED` | `quality_gate` |
| `FATAL`·전체 API 오류 | `FAILED` | `fetch_error` |
| S3 쓰기 실패 | `FAILED` | `storage_error` |
| 정책이 `FAIL_BATCH` 반환 | `FAILED` | `quality_gate` |

게이트에 걸리면 Silver를 쓰지 않는다(`artifacts.silver`는 null). 하류가 manifest
확인 없이 FAILED window의 Silver를 읽지 않게 하기 위해서다. quarantine은 실패 원인
분석용이라 이 경우에도 쓰되, 폐기 행이 0건이면 만들지 않는다.

`stage`의 최종 정지점:
- `fetch_error`·`FATAL` — `BRONZE_WRITTEN`. 다음 실행이 기존 Bronze를 재사용한다.
- `storage_error`·`quality_gate` — `VALIDATED`. `quality_gate`를 `COMPLETED`로 올리면
  다음 실행이 곧장 `SKIPPED`(종료 코드 0)로 빠져 실패가 성공처럼 보인다.
- `SUCCEEDED`·`PARTIAL`·`EMPTY` — `COMPLETED`.

예외 처리 경계: quarantine·Silver 쓰기 예외와 `BatchValidationFailed`, authority
manifest 확정 실패만 `FAILED` manifest로 바꾼다. 그 외 예외(예: `manifest.save`
실패)는 결과를 남길 방법이 없으므로 그대로 올린다.

`attempt`는 mutable 진단 실행 횟수이고, `revision`은 authority content의 correction
ordinal이다(최초 성공 0, exact replay 유지, 바뀐 content만 증가).
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
from adapters.base import FetchErrorKind, Window, fetch_with_rounds, get_adapter
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


def _now() -> datetime:
    """manifest의 started_at·ended_at에 쓸 KST 현재 시각을 반환한다."""
    return datetime.now(KST)


def _missing_ratio(
    missing_count: int,
    expected_total: int | None,
    fetched_rows: int,
    collected_count: int,
) -> float:
    """성공 조각만으로 누락 비율을 계산한다.

    `expected_total`을 아는 소스(서울)는 행 기준, 모르는 소스(기상청)는 조각 기준이다.
    """
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


def _effective_plan(
    planned_parts: frozenset[str] | None,
    completed: tuple[str, ...],
    missing: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """adapter 계획이 없으면 관측한 전체 part 집합을 계획으로 쓴다."""
    if planned_parts is None:
        return tuple(sorted(frozenset(completed) | frozenset(missing)))
    return tuple(sorted(planned_parts))


def _is_probe(config: SourceConfig) -> bool:
    """빈 종료 페이지까지 탐색하는 서울 API 소스인지 판별한다."""
    return config.adapter_params.get("pagination") == "probe_until_empty"


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
    보고 종료 코드를 정하게 한다.

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

    if FetchErrorKind.FATAL in run.missing_keys.values():
        # 인증 오류는 나머지 조각도 같은 이유로 실패하므로 게이트·검증 없이 끝낸다.
        return _fail_fetch(
            run,
            "reason=fatal",
            missing=_build_missing(run.missing_keys, run.expected_total, 0),
            counts=Counts(),
        )
    if _is_probe(config) and run.expected_total is None:
        # 종료 페이지를 보기 전에 끝나면 전체 cardinality를 모른다. 잘린 대여소
        # snapshot이 삭제·비활성화 authority가 되지 않게 normalize 전에 막는다.
        return _fail_fetch(
            run,
            "reason=pagination_unconfirmed",
            missing=_build_missing(run.missing_keys, None, 0),
            counts=Counts(),
        )

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
        parts = existing.artifacts.bronze.parts
        revision = existing.artifacts.bronze.revision
        chunks = dict(
            zip(parts, storage.read_bronze(source_id, window_start, parts, revision))
        )
        # 조각을 다시 받지 않으므로 실패 종류는 게이트 계산에 영향이 없다.
        missing_keys = {key: FetchErrorKind.PERMANENT for key in existing.missing.parts}
        expected_total = existing.counts.expected
    else:
        revision = storage.next_bronze_revision(source_id, window_start)
        round_result = fetch_with_rounds(
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
        chunks = round_result.chunks
        missing_keys = round_result.missing
        expected_total = round_result.expected_total

    bronze_parts = tuple(sorted(chunks))
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
        chunks=chunks,
        missing_keys=missing_keys,
        expected_total=expected_total,
        planned_parts=_effective_plan(
            planned_parts, bronze_parts, tuple(sorted(missing_keys))
        ),
    )


def _check_completeness(run: _WindowRun, fetched_rows: int) -> _Completeness | Manifest:
    """행 수로 cardinality를 확정하고 누락 게이트를 판정한다.

    게이트에 걸리면 FAILED manifest를, 통과하면 확정한 완결도 정보를 반환한다.
    """
    quality = run.config.quality
    expected_total = run.expected_total
    if _is_probe(run.config):
        # probe adapter의 expected_total은 종료 페이지를 봤다는 표시일 뿐이다. 재시도
        # 라운드는 이미 받은 page를 건너뛰므로 전체 Bronze의 실제 행 수로 확정한다.
        expected_total = fetched_rows
    parts_summary = f"{len(run.chunks)}/{len(run.chunks) + len(run.missing_keys)}"

    if expected_total is not None and fetched_rows > expected_total:
        # 진행 중인 window를 조회하면 API가 list_total_count보다 많은 행을 주는 일이
        # 있다(대여이력 rows=989 expected=988). 페이지 중복 병합 같은 사고는 최소
        # +100%라 작은 허용치로도 걸러지므로, 허용치 안이면 실제 행 수로 확정한다.
        allowed_total = expected_total * (1 + quality.max_overfetch_ratio)
        detail = (
            f"parts={parts_summary} rows={fetched_rows} expected={expected_total} "
            f"allowed={allowed_total:.0f}"
        )
        if fetched_rows > allowed_total:
            return _fail_fetch(
                run,
                f"reason=fetched_exceeds_expected {detail}",
                missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
                counts=Counts(expected=expected_total, fetched=fetched_rows),
            )
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
        return _fail_fetch(
            run,
            f"parts={parts_summary} missing_ratio={ratio:.3f}",
            missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
            counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    bronze_bytes = sum(len(v) for v in run.chunks.values())
    summary = f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes}"
    if run.missing_keys:
        logger.warning(
            f"{summary} missing={','.join(sorted(run.missing_keys))} "
            f"completeness={1.0 - ratio:.3f}"
        )
    else:
        logger.info(summary)
    return _Completeness(
        expected_total=expected_total,
        ratio=ratio,
        missing=_build_missing(run.missing_keys, expected_total, fetched_rows),
    )


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
    logger.error(
        "stage=validated status=failed failure_reason=quality_gate "
        "rows=0 reason=unconfirmed_empty"
    )
    return _finish(
        run,
        stage=Stage.VALIDATED,
        failure_reason=FailureReason.QUALITY_GATE,
        missing=completeness.missing,
        counts=counts,
    )


def _validate_and_store(
    run: _WindowRun, rows: list[dict], completeness: _Completeness
) -> Manifest:
    """행을 검증해 폐기 게이트를 판정하고 quarantine·Silver를 저장한다."""
    config = run.config
    missing = completeness.missing
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
        logger.error(
            "stage=validated status=failed failure_reason=quality_gate reason=fail_batch"
        )
        return _finish(
            run,
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            missing=missing,
            counts=Counts(expected=completeness.expected_total, fetched=len(rows)),
        )

    exceeds_drop_gate = outcome.drop_ratio > config.quality.max_drop_ratio
    _log_validated(outcome, completeness, exceeds_drop_gate)
    common = {
        "missing": missing,
        "counts": Counts(expected=completeness.expected_total, **outcome.counts),
        "column_issues": {
            col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()
        },
        "policy_actions": outcome.policy_actions,
        "drop_ratio": outcome.drop_ratio,
    }
    window_start = run.window.window_start

    try:
        quarantine_key = storage.write_quarantine(
            config.source_id, window_start, outcome.quarantine_records
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error(
            "stage=validated status=failed failure_reason=storage_error reason=quarantine_write"
        )
        return _finish(
            run, stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common
        )

    if exceeds_drop_gate:
        return _finish(
            run,
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            artifacts=Artifacts(bronze=run.bronze, quarantine=quarantine_key),
            **common,
        )

    try:
        silver = storage.write_immutable_silver(
            config.source_id, window_start, pa.Table.from_pylist(outcome.silver_rows)
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error(
            "stage=validated status=failed failure_reason=storage_error reason=silver_write"
        )
        return _finish(
            run,
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.STORAGE_ERROR,
            artifacts=Artifacts(bronze=run.bronze, quarantine=quarantine_key),
            **common,
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
        artifacts=Artifacts(
            bronze=run.bronze, silver=silver.key, quarantine=quarantine_key
        ),
        authority_status=SourceSnapshotStatus.SUCCEEDED if succeeded else None,
        authority_silver=silver if succeeded else None,
        **common,
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


def _base_manifest(run: _WindowRun, **over) -> Manifest:
    """가장 이른 실패 지점(FAILED, BRONZE_WRITTEN)을 기본값으로 manifest를 만든다."""
    fields = {
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
    fields.update(over)
    return Manifest(**fields)


def _finish(
    run: _WindowRun,
    *,
    authority_status: SourceSnapshotStatus | None = None,
    authority_silver: storage.ImmutableSilverArtifact | None = None,
    **over,
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
            counts=over["counts"],
            planned_parts=run.planned_parts,
            completed_parts=run.bronze.parts,
        )
        over["revision"] = prepared.manifest.revision_no

    result = _base_manifest(run, **over)
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
