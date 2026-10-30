"""데이터 수집 파이프라인.

실행 순서는 config 로드, manifest 로드, 재개 분기, (라운드를 돌며 조각마다 fetch
후 bronze 즉시 저장), 완결도 게이트, normalize, 검증·정책 적용, silver·quarantine
저장, manifest 마감 순이다. 단계를 넘어갈
때마다 manifest의 `stage`를 갱신해, 중간에 죽어도 다음 실행이 어디서부터 재개할지
알 수 있게 한다.

재개 분기:

| 조건 | 동작 |
| --- | --- |
| `stage=completed` (일반 실행) | authority 검증 후 `SKIPPED` 반환 |
| `stage>=bronze_written` (일반 재실행) | 기존 revision의 bronze 로드(fetch 건너뜀) |
| 그 외 (또는 `--force`) | 새 immutable Hot revision에 전체 fetch |

bronze 재사용이 재개의 핵심인 이유: 실시간 API(5분 주기)는 몇 분만 지나도 그 시점
데이터를 영영 받을 수 없다. 저장 실패 후 fetch부터 다시 하면 지금 시점의 다른
데이터로 덮어쓰게 된다.

    1회차: bronze ✓ → 정제 ✓ → silver ✗   stage=validated, status=FAILED
    2회차: fetch 건너뜀(bronze 재사용) → 정제 ✓ → silver ✓   stage=completed, SUCCEEDED

그 외 알아둘 점:
- 조각 저장은 pipeline의 책임이다. 어댑터는 `yield`만 하고 저장소를 모른다.
- `stage`는 fetch를 마친 뒤(라운드 소진 또는 예산 종료 시점)에만 올린다. 조각이
  다 모였는지는 `stage`가 아니라 `completeness`·`missing`이 따로 표현하므로,
  `stage=completed`이면서 불완전한 window도 있을 수 있다.
- 수집 실행마다 별도 Hot revision을 쓰므로 조각 수가 달라져도 이전 revision의
  유령 조각이 섞이지 않는다. manifest가 가리키는 revision과 `parts`만 읽는다.
- `normalize`는 bronze 재사용 여부와 무관하게 항상 수행한다. 네트워크를 타지
  않는 순수 변환이라 비용이 없다.
- 검증은 window 전체가 모인 뒤 배치로 하므로 조각을 메모리에서 놓지 않는다(ADR 0003).

조각 실패 처리: 조각 하나가 실패해도 window 전체를 버리지 않는다. 라운드 루프는
`adapters/base`가 돌리고, pipeline은 성공 조각 저장 콜백과 종료 후 판정만 맡는다.

    fetch 종료(라운드 소진 또는 예산 초과), 성공 조각으로 missing_ratio 계산,
      max_missing_ratio 이내면 normalize → 검증 → drop_ratio 판정 → silver,
      초과하면 silver 없이 FAILED(failure_reason=fetch_error)

`FATAL`(인증 오류)만 예외다. 같은 실행의 추가 round를 돌지 않고 즉시
`fetch_error`로 끝낸다.

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

게이트에 걸리면 silver를 쓰지 않는다(`artifacts.silver`는 null) — FAILED window에
silver가 있으면 하류가 manifest 확인 없이 읽을 위험이 있다. quarantine은 실패
원인 분석에 필요해 이 경우에도 쓰지만, 폐기 행이 0건이면 만들지 않는다. 두
게이트는 독립이고 `drop_ratio`의 분모는 `fetched`다. `quality_gate` 실패는 같은
bronze·config로 재시도해도 결과가 같아 config를 고쳐야 하고, `fetch_error`는 수집
누락을 뜻한다 — 그래서 `failure_reason`으로 둘을 구분해 남긴다.

`stage`의 최종 정지점은 실패 종류별로 다르다.
- `fetch_error`·`FATAL` — fetch 단계에서 끝났으므로 `BRONZE_WRITTEN`. 다음
  실행은 manifest가 가리키는 기존 Bronze를 재사용한다.
- `storage_error`·`quality_gate` — normalize·검증까지는 끝났으므로 `VALIDATED`.
  `quality_gate`는 재시도해도 결과가 같지만 `COMPLETED`로 올리지 않는다.
  그러면 다음 실행이 `missing.parts`가 빈 채로 곧장 `SKIPPED`(종료 코드 0)로
  빠져 실패가 조용히 성공처럼 보이기 때문이다.
- `SUCCEEDED`·`PARTIAL`·`EMPTY` — 더 할 일이 없으므로 `COMPLETED`.

예외 처리 경계: `write_silver`·`write_quarantine`·`write_bronze_part`의 예외와
`validate_batch`의 `BatchValidationFailed`만 여기서 잡아 `FAILED` manifest로
바꾼다. 그 외 예외(예: `manifest.save` 자체의 실패)는 삼키지 않고 그대로 올린다.
실행 결과를 남길 방법이 없는 예외까지 잡으면 원인을 추적할 수 없기 때문이다.

주의:
- `attempt`는 mutable 진단 실행 횟수다. `revision`은 authoritative source content의
  correction ordinal이며 최초 성공은 0, exact replay는 유지, changed content만 증가한다.
- 각 단계 경계에서 로그 한 줄씩만 남긴다. 조각마다, 라운드마다 남기지 않는다.
- 부분 성공의 종료 코드는 0이다. source별 누락 허용치 이내면 `PARTIAL`로 downstream
  task를 스케줄하고, PARTIAL Silver를 실제 입력으로 사용할지는 각 소비자가 명시적으로
  결정한다. 누락 허용치 초과를 포함한 모든 `FAILED`와 처리되지 않은 예외는 non-zero로
  Airflow retry 대상이다.
"""

from __future__ import annotations

import logging
import math
import time
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
from validation.engine import BatchValidationFailed, validate_batch
from validation.types import RunContext

_HTTP_TIMEOUT_SECONDS = 10.0

logger = logging.getLogger(__name__)


def _sorted_chunks(chunks: dict[str, bytes]) -> list[bytes]:
    """조각을 키의 사전순으로 정렬해 값 목록만 반환한다."""
    return [chunks[key] for key in sorted(chunks)]


def _missing_ratio(
    missing_count: int,
    expected_total: int | None,
    fetched_rows: int,
    collected_count: int,
) -> float:
    """성공 조각만으로 누락 비율을 계산한다.

    `expected_total`을 아는 소스(서울, 행 기준)는 실제 받은 행 수를 `normalize`로 세어 `1 - fetched_rows/expected_total`로 계산한다.
    모르는 소스(기상청, 조각 기준)는 `누락 조각 수 / 계획된 조각 수`로 계산한다.
    행을 세는 방식을 쓰는 이유는 어댑터마다 다른 조각 키 형식을 pipeline이 몰라도 되게하기 위해서다.

    args:
        missing_count: 라운드를 다 써도 못 받은 조각 수.
        expected_total: 소스가 알려준 전체 행 수. 모르면 None.
        fetched_rows: 성공한 조각을 normalize했을 때 나온 행 수.
        collected_count: 성공한 조각 수.
    returns:
        0.0(누락 없음) ~ 1.0(전부 누락) 사이의 비율.
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


def _now() -> datetime:
    """KST aware 현재 시각. manifest의 started_at·ended_at에 쓴다."""
    return datetime.now(KST)


def execute_window(
    config,
    window_start: datetime,
    *,
    client,
    force: bool = False,
    sleep_fn=time.sleep,
) -> Manifest:
    """window 하나를 재개 분기에 따라 처리하고 최종 manifest를 반환한다.

    fetch(또는 bronze 재사용) → 완결도 게이트 → normalize → 검증 → 폐기 게이트 →
    immutable Silver·quarantine 저장 → mutable 진단 manifest → authority manifest
    last-write 순으로 진행한다.
    실패하더라도 예외를 올리지 않고 FAILED manifest를 만들어 반환한다
    호출자(CLI)가 `status`만 보고 종료 코드를 정할 수 있게 하기 위해서다.

    args:
        config: 소스 설정.
        window_start: 수집 대상 window의 시작 시각.
        client: 어댑터에 주입할 httpx 클라이언트.
        force: 재개 분기를 모두 무시하고 처음부터 다시 수집한다.
        sleep_fn: 라운드 간 대기 함수.
    returns:
        이번 실행이 도달한 최종 manifest.
    """
    window_end = window_start + config.schedule.interval
    started_at = _now()
    existing = manifest_module.load(config.source_id, window_start)
    adapter_cls = get_adapter(config.adapter)
    window = Window(window_start=window_start, window_end=window_end)
    planned_parts = (
        adapter_cls.planned_parts(config, window)
        if hasattr(adapter_cls, "planned_parts")
        else None
    )
    def _effective_plan(
        completed: tuple[str, ...],
        missing: tuple[str, ...] = (),
    ) -> tuple[str, ...]:
        """Adapter plan 또는 관측한 전체 part 집합을 정렬된 tuple로 반환한다."""
        return tuple(
            sorted(
                planned_parts
                if planned_parts is not None
                else frozenset(completed) | frozenset(missing)
            )
        )

    def _verify_completed_authority(value: Manifest) -> Manifest:
        """완료 진단 manifest가 이미 확정된 authority와 같은지 검증한다."""
        if value.status not in {RunStatus.SUCCEEDED, RunStatus.EMPTY}:
            return value

        authority_status = (
            SourceSnapshotStatus.SUCCEEDED
            if value.status is RunStatus.SUCCEEDED
            else SourceSnapshotStatus.EMPTY
        )
        silver = None
        if authority_status is SourceSnapshotStatus.SUCCEEDED:
            if value.artifacts.silver is None:
                raise RuntimeError("완료 SUCCEEDED manifest에 Silver key가 없습니다.")
            try:
                silver = storage.read_immutable_silver_artifact(
                    value.artifacts.silver,
                    row_count=value.counts.kept,
                )
            except ValueError:
                # 전환 전 mutable Silver는 authority로 승격하지 않는다. Gold downstream은
                # source snapshot manifest만 읽으므로 기존 진단 run은 계속 SKIPPED다.
                return value

        snapshots = manifest_module.load_source_snapshots(
            config.source_id, window_start
        )
        if not snapshots:
            raise RuntimeError(
                "content-addressed 완료 run에 source authority manifest가 없습니다."
            )
        prepared = manifest_module.prepare_source_snapshot(
            source_id=config.source_id,
            logical_dttm=window_start,
            status=authority_status,
            config_version=value.config_version,
            silver=silver,
            counts=value.counts,
            planned_parts=_effective_plan(value.artifacts.bronze.parts),
            completed_parts=value.artifacts.bronze.parts,
        )
        if prepared.manifest != snapshots[-1].manifest:
            raise RuntimeError(
                "mutable 진단 manifest가 확정된 source authority와 다릅니다."
            )
        manifest_module.finalize_source_snapshot(prepared)
        return value

    if existing and existing.stage == Stage.COMPLETED and not force:
        # 분기 1: 이미 완결됐으면 아무것도 하지 않는다. 확정된 authority manifest는 exact
        # replay로 다시 검증하되 mutable 진단 파일에서 새 authority를 만들지는 않는다.
        existing = _verify_completed_authority(existing)
        logger.info("stage=completed status=skipped")
        return existing.model_copy(update={"status": RunStatus.SKIPPED})

    if (
        existing
        and existing.stage.value >= Stage.BRONZE_WRITTEN.value
        and not force
    ):
        # 분기 2: bronze 재사용 — 이전 실행이 fetch까지는 끝냈지만(예: silver 쓰기
        # 실패로 VALIDATED에서 멈췄음) 그 뒤에서 죽은 경우다. 지금 다시 fetch하면
        # 실시간 API에서는 그 시점의 다른 데이터를 받게 되므로, 반드시 이전에 저장된
        # bronze 조각을 그대로 재사용한다.
        parts = existing.artifacts.bronze.parts
        bronze_revision = existing.artifacts.bronze.revision
        chunks = dict(
            zip(
                parts,
                storage.read_bronze(
                    config.source_id, window_start, parts, bronze_revision
                ),
            )
        )
        # 조각 자체는 재시도하지 않으므로 실제 실패 종류는 중요하지 않다 — 아래
        # 게이트 계산이 "재시도 불가능한 누락"으로만 취급하면 되므로 PERMANENT로 채운다.
        missing_keys = {key: FetchErrorKind.PERMANENT for key in existing.missing.parts}
        expected_total = existing.counts.expected
        attempt = existing.attempt + 1
        revision_base = existing.revision
    else:
        # 분기 3(또는 --force): 처음부터 전체 fetch하되 이전 원본은 지우지 않는다.
        # 새 immutable revision 경로라 조각 수가 줄어도 이전 실행의 유령 조각이
        # 현재 snapshot에 섞이지 않는다.
        bronze_revision = storage.next_bronze_revision(
            config.source_id, window_start
        )
        round_result = fetch_with_rounds(
            adapter_cls.fetch,
            config,
            window,
            client=client,
            sleep_fn=sleep_fn,
            on_chunk=lambda key, payload: storage.write_bronze_part(
                config.source_id, window_start, key, payload, bronze_revision
            ),
            planned_parts=planned_parts,
        )
        chunks = round_result.chunks
        missing_keys = round_result.missing
        expected_total = round_result.expected_total
        attempt = (existing.attempt + 1) if existing else 1
        revision_base = existing.revision if existing else 0

    bronze_parts = tuple(sorted(chunks))
    authority_planned_parts = _effective_plan(bronze_parts, tuple(sorted(missing_keys)))
    artifacts = Artifacts(
        bronze=BronzeArtifacts(
            prefix=storage.bronze_prefix(
                config.source_id, window_start, bronze_revision
            ),
            parts=bronze_parts,
            revision=bronze_revision,
        )
    )

    def _base_manifest(**over) -> Manifest:
        # 실패 케이스가 대부분 같은 필드(source_id·window·attempt 등)를 반복하므로,
        # 기본값은 "가장 이른 실패 지점"(FAILED, BRONZE_WRITTEN)으로 깔고 각
        # 호출부가 필요한 값만 덮어쓴다.
        fields = {
            "source_id": config.source_id,
            "window_start": window_start,
            "window_end": window_end,
            "status": RunStatus.FAILED,
            "stage": Stage.BRONZE_WRITTEN,
            "attempt": attempt,
            "revision": revision_base,
            "started_at": started_at,
            "ended_at": _now(),
            "artifacts": artifacts,
            "config_version": config.config_version,
        }
        fields.update(over)
        return Manifest(**fields)

    def _finish(
        *,
        authority_status: SourceSnapshotStatus | None = None,
        authority_silver: storage.ImmutableSilverArtifact | None = None,
        **over,
    ) -> Manifest:
        """manifest를 만들어 저장한 뒤 그대로 반환한다.

        아래 모든 종료 지점이 공유한다. Authority 대상이면 immutable Silver를 이미
        완성한 뒤 revision을 먼저 결정하고, mutable 진단 manifest를 기록한 다음
        authority manifest를 마지막에 쓴다.
        """
        prepared_authority = None
        if authority_status is not None:
            provisional = _base_manifest(**over)
            if authority_status is SourceSnapshotStatus.SUCCEEDED:
                if authority_silver is None:
                    raise RuntimeError(
                        "SUCCEEDED authority에 immutable Silver가 없습니다."
                    )
                if provisional.artifacts.silver != authority_silver.key:
                    raise RuntimeError(
                        "진단 manifest와 authority Silver key가 다릅니다."
                    )
                if provisional.counts.kept != authority_silver.row_count:
                    raise RuntimeError(
                        "Silver 실제 row 수와 authority count가 다릅니다."
                    )
            elif authority_silver is not None:
                raise RuntimeError("EMPTY authority에는 Silver가 없어야 합니다.")
            prepared_authority = manifest_module.prepare_source_snapshot(
                source_id=config.source_id,
                logical_dttm=window_start,
                status=authority_status,
                config_version=config.config_version,
                silver=authority_silver,
                counts=provisional.counts,
                planned_parts=authority_planned_parts,
                completed_parts=bronze_parts,
            )
            over["revision"] = prepared_authority.manifest.revision_no

        result = _base_manifest(**over)
        manifest_module.save(result)

        if prepared_authority is not None:
            try:
                manifest_module.finalize_source_snapshot(prepared_authority)
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

    if FetchErrorKind.FATAL in missing_keys.values():
        # FATAL(인증 오류 등)은 나머지 조각도 같은 이유로 실패할 게 뻔하므로, 게이트
        # 계산도 검증도 건너뛰고 즉시 끝낸다. stage는 기본값 BRONZE_WRITTEN 그대로
        # 둔다 — 실행 내부 round는 중단하고 Airflow 재실행 정책에 판정을 넘긴다.
        logger.error(
            "stage=bronze_written status=failed failure_reason=fetch_error reason=fatal"
        )
        missing = _build_missing(missing_keys, expected_total, fetched_rows=0)
        return _finish(failure_reason=FailureReason.FETCH_ERROR, missing=missing)

    if (
        config.adapter_params.get("pagination") == "probe_until_empty"
        and expected_total is None
    ):
        # Sentinel을 보기 전에 deadline이 끝나면 지금까지 받은 page가 정상처럼 보여도
        # 전체 cardinality를 모른다. 이 상태를 completeness=1로 간주하면 잘린 station
        # snapshot이 삭제·비활성화 authority가 되므로 normalize/Silver 전에 차단한다.
        logger.error(
            "stage=bronze_written status=failed failure_reason=fetch_error "
            "reason=pagination_unconfirmed"
        )
        missing = _build_missing(missing_keys, expected_total, fetched_rows=0)
        return _finish(
            failure_reason=FailureReason.FETCH_ERROR,
            missing=missing,
            counts=Counts(expected=None, fetched=0),
        )

    # normalize는 bronze 재사용 여부와 무관하게 항상 다시 수행한다(네트워크를
    # 타지 않는 순수 변환이라 비용이 없다). 그 결과로 나온 행 수를 "성공 조각으로
    # 계산하는 missing_ratio"에도 그대로 재사용한다.
    rows = adapter_cls.normalize(_sorted_chunks(chunks), config)
    fetched_rows = len(rows)
    if config.adapter_params.get("pagination") == "probe_until_empty":
        # Probe adapter의 non-None expected_total은 빈 sentinel을 실제로 봤다는 완료
        # marker다. Retry round는 이미 확보한 page를 skip할 수 있으므로 adapter 내부의
        # 이번-round 누적값이 아니라 전체 Bronze를 normalize한 실제 cardinality로
        # source count를 확정한다.
        expected_total = fetched_rows
    ratio = _missing_ratio(len(missing_keys), expected_total, fetched_rows, len(chunks))
    missing = _build_missing(missing_keys, expected_total, fetched_rows)

    parts_summary = f"{len(chunks)}/{len(chunks) + len(missing_keys)}"

    if expected_total is not None and fetched_rows > expected_total:
        # 1 - fetched/expected를 0으로 clamp하면 probe 재시도 사이 snapshot이
        # 축소·재정렬돼 옛 Bronze payload와 새 total이 섞인 상태를 완결로 오인한다.
        # 초과분을 임의로 자를 근거도 없으므로 Silver를 쓰지 않고 force 재수집이
        # 필요한 명시적 fetch 실패로 남긴다.
        #
        # 단, 행이 쌓이기만 하는 소스는 진행 중인 window를 조회할 때 API가 같은
        # 본문 안에서 list_total_count보다 많은 row를 주는 일이 있다(실측: 대여이력
        # rows=989 expected=988). 그 초과분은 잘린 데이터가 아니라 카운트 계산과
        # 직렬화 사이에 들어온 실제 레코드이므로, max_overfetch_ratio를 연 소스는
        # 경고만 남기고 통과시킨다. 페이지 중복 병합 같은 진짜 사고는 최소 +100%라
        # 작은 허용치로도 계속 걸린다.
        allowed_total = expected_total * (1 + config.quality.max_overfetch_ratio)
        if fetched_rows > allowed_total:
            logger.error(
                "stage=bronze_written status=failed failure_reason=fetch_error "
                f"reason=fetched_exceeds_expected parts={parts_summary} "
                f"rows={fetched_rows} expected={expected_total} "
                f"allowed={allowed_total:.0f}"
            )
            return _finish(
                failure_reason=FailureReason.FETCH_ERROR,
                missing=missing,
                counts=Counts(expected=expected_total, fetched=fetched_rows),
            )
        logger.warning(
            f"stage=bronze_written reason=fetched_exceeds_expected_within_tolerance "
            f"parts={parts_summary} rows={fetched_rows} expected={expected_total} "
            f"allowed={allowed_total:.0f}"
        )
        # 허용치 안의 초과는 "카운트가 뒤처졌다"고 판정한 것이므로, 실제 행 수를 이
        # window의 cardinality로 확정한다. Gold의 SUCCEEDED 계약이 expected==fetched를
        # 요구하기도 한다(core.source_snapshot._validate_succeeded).
        expected_total = fetched_rows
        ratio = _missing_ratio(
            len(missing_keys), expected_total, fetched_rows, len(chunks)
        )
        missing = _build_missing(missing_keys, expected_total, fetched_rows)

    exceeds_missing_gate = ratio > config.quality.max_missing_ratio and not math.isclose(
        ratio,
        config.quality.max_missing_ratio,
        rel_tol=0.0,
        abs_tol=1e-12,
    )
    if exceeds_missing_gate:
        # 완결도 게이트 초과 — silver를 쓰지 않고 fetch_error로 끝낸다. stage는
        # BRONZE_WRITTEN 그대로라 재실행하면 기존 Bronze 재사용 분기로 들어간다.
        logger.error(
            "stage=bronze_written status=failed failure_reason=fetch_error "
            f"parts={parts_summary} missing_ratio={ratio:.3f}"
        )
        return _finish(
            failure_reason=FailureReason.FETCH_ERROR,
            missing=missing,
            counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    # 여기 도달했다는 것은 완결도 게이트를 통과했다는 뜻이다 — fetch 단계 경계의
    # 로그 한 줄을 여기서 남긴다. 누락이 남아 있으면 WARNING, 없으면 INFO.
    bronze_bytes = sum(len(v) for v in chunks.values())
    if missing_keys:
        logger.warning(
            f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes} "
            f"missing={','.join(sorted(missing_keys))} completeness={1.0 - ratio:.3f}"
        )
    else:
        logger.info(
            f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes}"
        )

    if fetched_rows == 0:
        # 행이 0건이면 검증을 돌릴 대상이 없으므로 여기서 바로 갈린다. allow_empty인
        # 소스(행사 등)만 정상 종료로 인정하고, 그 외에는 quality_gate로 묶는다.
        confirmed_empty = (
            expected_total == 0
            and not missing.parts
            and bool(bronze_parts)
            and authority_planned_parts == bronze_parts
        )
        if config.quality.allow_empty and confirmed_empty:
            logger.info("stage=completed status=empty")
            return _finish(
                status=RunStatus.EMPTY,
                stage=Stage.COMPLETED,
                failure_reason=None,
                missing=missing,
                counts=Counts(expected=expected_total),
                completeness=1.0,
                authority_status=SourceSnapshotStatus.EMPTY,
            )
        logger.error(
            "stage=validated status=failed failure_reason=quality_gate "
            "rows=0 reason=unconfirmed_empty"
        )
        return _finish(
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            missing=missing,
            counts=Counts(expected=expected_total),
        )

    ctx = RunContext(
        source_id=config.source_id,
        window_start=window_start,
        window_end=window_end,
        attempt=attempt,
    )
    try:
        outcome = validate_batch(rows, config, ctx)
    except BatchValidationFailed:
        # 컬럼 정책이 FAIL_BATCH를 반환했다 — 그 시점 이후 행은 처리되지 않았으므로
        # silver·quarantine 어느 쪽도 쓸 수 없다. 같은 bronze + 같은 config면
        # 재시도해도 결과가 같으므로 config를 고쳐야 하는 quality_gate로 남긴다.
        logger.error(
            "stage=validated status=failed failure_reason=quality_gate reason=fail_batch"
        )
        return _finish(
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            missing=missing,
            counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    counts = Counts(expected=expected_total, **outcome.counts)
    column_issues = {
        col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()
    }
    # 아래 세 종료 지점(quarantine 실패·drop_ratio 초과·silver 실패)이 공통으로 남길
    # 필드. artifacts만 지점마다 달라(quarantine 키가 언제 붙는지) 따로 넘긴다.
    common = {
        "missing": missing,
        "counts": counts,
        "column_issues": column_issues,
        "policy_actions": outcome.policy_actions,
        "drop_ratio": outcome.drop_ratio,
    }

    # 데이터 품질 관점의 validated 단계 경계 로그. 폐기 게이트 초과 여부는 이 시점에
    # 이미 알 수 있으므로 여기서 ERROR로 확정해 남긴다 — 뒤에 storage_error가 나면
    # 그건 별개 사건이라 각자 자기 ERROR 줄을 남긴다(둘이 겹쳐도 서로 다른 사유다).
    validated_summary = (
        f"stage=validated kept={outcome.counts['kept']} repaired={outcome.counts['repaired']} "
        f"dropped={outcome.counts['dropped']} drop_ratio={outcome.drop_ratio:.3f} completeness={1.0 - ratio:.3f}"
    )
    if outcome.drop_ratio > config.quality.max_drop_ratio:
        logger.error(f"{validated_summary} status=failed failure_reason=quality_gate")
    elif outcome.counts["dropped"] or missing.parts:
        logger.warning(f"{validated_summary} status=partial")
    else:
        logger.info(f"{validated_summary} status=succeeded")

    try:
        # 폐기 행이 없으면 quarantine 객체 자체를 만들지 않는다(빈 객체를 굳이 남기지 않는다).
        quarantine_key = (
            storage.write_quarantine(
                config.source_id, window_start, outcome.quarantine_records
            )
            if outcome.quarantine_records
            else None
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error(
            "stage=validated status=failed failure_reason=storage_error reason=quarantine_write"
        )
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common
        )

    if outcome.drop_ratio > config.quality.max_drop_ratio:
        # 폐기 게이트 초과 — silver는 쓰지 않지만 quarantine은 남긴다(왜 실패했는지
        # 분석하려면 폐기된 행이 필요하다).
        return _finish(
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.QUALITY_GATE,
            **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    try:
        silver_artifact = storage.write_immutable_silver(
            config.source_id,
            window_start,
            pa.Table.from_pylist(outcome.silver_rows),
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error(
            "stage=validated status=failed failure_reason=storage_error reason=silver_write"
        )
        return _finish(
            stage=Stage.VALIDATED,
            failure_reason=FailureReason.STORAGE_ERROR,
            **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    # 폐기·누락이 전혀 없을 때만 SUCCEEDED이고, 그 외(둘 중 하나라도 게이트 이내로
    # 존재)에는 PARTIAL이다. Source correction ordinal은 mutable 실행 횟수가 아니라
    # authority content 비교로 정하므로 여기서 임의로 증가시키지 않는다.
    status = (
        RunStatus.SUCCEEDED
        if outcome.counts["dropped"] == 0 and ratio == 0.0
        else RunStatus.PARTIAL
    )
    logger.info(f"stage=completed status={status.value} key={silver_artifact.key}")
    return _finish(
        status=status,
        stage=Stage.COMPLETED,
        failure_reason=None,
        completeness=1.0 - ratio,
        **common,
        artifacts=Artifacts(
            bronze=artifacts.bronze,
            silver=silver_artifact.key,
            quarantine=quarantine_key,
        ),
        authority_status=(
            SourceSnapshotStatus.SUCCEEDED if status is RunStatus.SUCCEEDED else None
        ),
        authority_silver=(silver_artifact if status is RunStatus.SUCCEEDED else None),
    )
