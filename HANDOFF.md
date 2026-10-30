# 리팩토링 인수인계

"에이전트 1/2/3"으로 지정받은 에이전트가 읽는 문서다. 지정받은 에이전트는 이 문서와 `AGENTS.md`를 읽은 뒤, 사람의 추가 지시나 허가 없이 설계부터 PR 생성까지 한 번에 진행한다.

## 1. 목표

- 지금 코드는 복잡도가 너무 높아 이해와 유지보수가 어려움. 이번 작업의 목표는 다음 두 가지
  - 불필요하거나 과한 기능을 과감하게 덜어냄
  - 남는 코드를 처음 보는 사람도 이해할 수 있게 단순하게 만듦
- 판단 기준
  - 운영 파이프라인(DAG, `Makefile`, `ops/` 스크립트)이 호출하지 않는 코드는 삭제
  - 현재 운영 설정에서 꺼져 있는 legacy 호환 경로는 삭제
  - 실제 장애 시나리오가 아니라 가정에 대비한 방어 계층, 한 곳에서만 쓰는 추상화, 중복된 검증은 걷어냄
  - 긴 함수는 한 가지 일만 하는 함수로 나눔. 새 추상화를 만들기보다 호출 단계를 줄이는 쪽을 택함
  - 주석과 docstring은 "왜"만 짧게. 코드를 다시 설명하는 긴 서술, 실측 일지, 변경 이력은 지움
- 지키는 것
  - 운영 S3에 이미 쌓인 데이터의 키 규칙 (`libs/core/tests/test_layout.py` golden test)
  - 수집·정제 결과의 의미(스키마, 상태 판정 규칙)
- 이번 범위: 데이터 수집·정제(`collector/`, `normalizer/`, `nowcaster/`, `poi_master/`)와 오케스트레이션(`airflow/`)
- 범위 밖: 인프라(`Makefile`, `.github/`, `ops/`, `terraform/`), ML, 재배치, 서빙·대시보드

## 2. 작업 방식

### 자율 진행

- 중간에 사람에게 확인이나 허가를 구하지 않는다. 선택지가 있으면 1절 기준으로 스스로 결정하고, 결정과 근거는 PR 본문의 "작업 내용 및 고민"에 남긴다
- 순서: 3절 준비 → 현황 파악 → 설계 → 구현 → 검증 → 커밋 → push → PR 생성
- 설계 문서 파일을 따로 만들지 않는다. 설계 내용은 PR 본문에 쓴다
- 이 문서(`HANDOFF.md`)는 수정하지 않는다. 다른 에이전트와 충돌한다. 후속 작업은 PR 본문 "리뷰 포인트"에 적는다

### 하드 리밋 (어떤 경우에도 하지 않음)

- 인프라 파일 수정: `Makefile`, `.github/`, `ops/`, `terraform/`
- `libs/core` 수정. 공통 helper가 필요하면 자기 모듈 안에 두고 PR 본문에 "core 이전 후보"로 적음
- 4절 소유권 밖의 파일 수정
- 실제 AWS 리소스나 운영 S3 조작: `terraform`, `aws` CLI 쓰기·삭제, `make deploy-*`, `make train-*`, `make emr-*`
- PR 머지, `develop`/`main`에 직접 push, force push
- 운영 S3 데이터 키 규칙 변경

### 커밋·PR 규칙

- 코드 수정 전 `.agent/coding.md`, 커밋 전 `.agent/commit.md`, PR 생성 전 `.agent/pr.md`를 읽고 따른다
- PR 대상: `origin`(`vysryoo/DE_team2-GangnamguUmBokDong`)의 `develop`. `gh pr create --repo vysryoo/DE_team2-GangnamguUmBokDong --base develop`
- 커밋 메시지와 PR 본문에 AI 서명을 넣지 않는다

## 3. 준비

현재 디렉터리는 에이전트 전용 git worktree다.

1. Phase 1(공통 기반) 포함 여부 확인: `git fetch origin && git merge-base --is-ancestor 771ac19 origin/develop`
   - 성공하면 base는 `origin/develop`, 실패하면 base는 `origin/refactor/common-base`
2. 작업 브랜치 생성: `git switch -c <브랜치> <base>`
   - 에이전트 1: `refactor/agent1-collector-ingest`
   - 에이전트 2: `refactor/agent2-lake-and-refine`
   - 에이전트 3: `refactor/agent3-airflow`
3. 담당 프로젝트마다 `uv sync`를 실행하고 테스트·lint baseline을 기록한다 (8절)
4. Docker(`make up` 등)는 에이전트 3만 사용한다. worktree끼리 같은 포트를 쓰므로 동시에 띄우면 충돌한다

## 4. 에이전트별 소유권과 작업

### 에이전트 1: collector 수집 경로

- 소유
  - `collector/` 중 에이전트 2 소유를 뺀 전부: `main.py`, `pipeline.py`, `manifest.py`, `logging_setup.py`, `adapters/`, `config/`, `validation/`, `sources/`, 해당 테스트
  - `collector/storage.py` 중 에이전트 2 소유 함수를 뺀 전부 (bronze, silver 쓰기, quarantine, source snapshot manifest, window manifest, retry marker)
- 작업
  - legacy backfill 경로 삭제. 어떤 `collector/sources/*.yaml`도 `backfill`을 켜지 않음
    - `main.py`: `--backfill`, `--list-backfill-targets`
    - `pipeline.py`: `ForceAndBackfillError`, `_sync_retry_marker`, `get_backfill_targets`, backfill 재개 분기
    - `manifest.py`: `RetryMarker`, `save/load/clear_retry_marker`
    - `storage.py`: retry marker 함수들
    - `config/schema.py`: `Backfill`, `SourceConfig.backfill`
  - `pipeline.execute_window`(약 580줄 단일 함수)를 단계 함수로 분리. manifest `stage`와 status 결정 규칙(모듈 docstring 표)은 유지
  - `main.py`의 모드(수집, freshness 확인, 통계 리포트) 정리
  - adapters(`seoul_openapi.py` 929줄, `kma_apihub.py`), `validation/`, `config/loader.py`의 과한 분기와 중복 정리
- 유지
  - 6절 CLI 계약 중 collector `main.py` 부분 (`--backfill`, `--list-backfill-targets` 제외)
  - `manifest`의 `Manifest`, `RunStatus`, `Stage`, `load`, `load_source_snapshots`와 `storage.read_immutable_silver_artifact`, `storage.list_source_snapshot_windows`의 시그니처 (에이전트 2의 compaction이 사용)
  - `storage.write_bronze_part`, `storage.read_bronze`가 `cold_bronze.write_pending_marker`, `cold_bronze.read_revision`을 호출하는 구조. 에이전트 2가 두 함수 시그니처를 유지함

### 에이전트 2: collector 레이크 유지보수 + 정제

- 소유
  - `collector/` 유지보수 모듈: `compaction.py`, `compact.py`, `cold_bronze.py`, `cold_compact.py`, `hot_bronze_gc.py`, `hot_bronze_gc_cli.py`, `silver_gc.py`, `silver_gc_cli.py`, `bootstrap/`, 해당 테스트
  - `collector/storage.py`의 `write_silver`, `list_silver_objects`, `delete_silver_objects`, `write_archive`, `archive_exists`, `_archive_manifest_key`, `write_archive_manifest`, `read_archive_manifest` (유지보수 모듈 안으로 옮겨도 됨)
  - `airflow/orchestration/compaction_task.py`, `airflow/dags/daily_compaction.py`, `airflow/config/sources.py`
  - `normalizer/`, `nowcaster/`, `poi_master/`
- 작업 (레이크 유지보수)
  - Hot/Cold Bronze와 GC 3종(`cold_bronze`, `hot_bronze_gc`, `silver_gc`) 단순화. S3 lifecycle(`terraform/`)은 바꿀 수 없으므로 Hot Bronze 정리는 코드에 남김
  - 유지보수 CLI 4종의 반복 boilerplate(argparse, JSON 결과 출력) 통합. CLI를 바꾸면 `compaction_task.py`, `daily_compaction.py`를 같은 PR에서 맞춤
  - `storage.write_silver`(mutable silver, 테스트에서만 호출)와 compaction의 legacy silver key 처리 삭제 검토
  - `airflow/config/sources.py`의 `--backfill` 언급 등 낡은 주석 정리
- 작업 (정제)
  - `normalizer/poi.py`의 `_select_largest_polygon`과 `poi_master/registry.py`의 `_largest_polygon` 중복 제거
  - `normalizer/main.py`의 `run()` 등 긴 함수 정리
  - normalizer·nowcaster·poi_master의 과한 방어 코드와 장황한 주석 정리
- 유지
  - `cold_bronze.write_pending_marker(source_id, day, hot_key)`, `cold_bronze.read_revision(source_id, window_start, revision, parts)` 시그니처 (에이전트 1의 `storage.py`가 호출)
  - 6절 CLI 계약 중 normalizer·nowcaster·poi_master 부분
  - `bootstrap/`, `nowcaster`의 `backfill-archive`·`bootstrap-lookback`, `normalizer/local_training_master.py`는 `ops/training_smoke.sh`가 사용함
  - POI static 모드: `libs/core/src/core/poi_master.py`와 collector까지 함께 바꿔야 하므로 이번에는 유지 (7절 후속 작업)

### 에이전트 3: Airflow

- 소유
  - `airflow/` 전체 (에이전트 2 소유 3개 파일 제외)
- 작업
  - 미사용 코드 삭제
    - `airflow/orchestration/db_loader_task.py`, `config/schedules.py`의 `DB_LOADER_EXECUTION_TIMEOUT`
    - `airflow/orchestration/aws_infra_task.py`의 `start_ec2_instance`, `stop_ec2_instance`, `run_command_on_ec2`, `run_emr_feature_mart_job`, `resize_emr_cluster`, `get_core_instance_group_id`. 삭제 뒤 `is_mock_mode` 등 남은 사용처 확인
    - `airflow/pyproject.toml`의 미사용 `ml_core` uv source
  - `resource_probe` 래핑(`task_builder._profiled_bash_command`) 제거. `ops/resource_probe.py` 파일 자체는 인프라 범위라 남김 (7절 후속 작업)
  - `collector_task.py`의 빌더 4개(일반, population, replay, daily replay)를 옵션을 받는 빌더 1개로 통합
  - `"uv run --frozen python {script} {args}"` 명령 조립 헬퍼 도입
  - 20~90줄짜리 `*_task.py`를 도메인별 모듈로 통합. `compaction_task.py`는 건드리지 않음
  - `airflow/config/sources.py`의 source id와 `collector/sources/*.yaml` 일치 테스트 추가 (Airflow venv에는 core가 없으므로 YAML을 직접 읽음)
  - 낡은 주석 정리: `templates.py`의 `loader/s3_reader.py`(존재하지 않는 파일), `collector_task.py`의 `--backfill` 언급
  - `monthly_retrain.py`(812줄)와 `aws_infra_task.py`의 과한 분기 정리. EMR 동작 자체는 유지
- 유지
  - `task_builder.build_module_task`, `REPO_ROOT`, `templates.py` 공개 이름의 시그니처 (에이전트 2의 `compaction_task.py`가 사용)
  - DAG id, 스케줄, 태스크 의존 관계, 각 모듈 CLI 호출 형태
  - `ops/compose/airflow-entrypoint.sh`가 기대하는 Airflow 실행 방식

## 5. Phase 1 공통 인터페이스 (`libs/core`, 수정 금지)

| 모듈 | 제공 | 용도 |
| --- | --- | --- |
| `core.layout` | `silver_prefix`, `silver_day_prefix`, `silver_window_key`, `silver_content_key`, `nowcast_key`, `window_manifest_key`, `window_manifest_prefix`, `source_snapshot_day_prefix`, `source_snapshot_hour_prefix`, `source_snapshot_logical_prefix`, `source_snapshot_manifest_key`, `parse_source_snapshot_manifest_key`, `archive_key`, `archive_prefix` | 여러 모듈이 공유하는 S3 키 |
| `core.kst` | `KST`, `parse_kst_datetime`, `today_kst` | 시간대 상수, CLI 시각 인자 파싱 |
| `core.sources` | source id 문자열 상수, `COLLECTOR_SOURCE_IDS` | source id 하드코딩 대체 |
| `core.s3` | `bucket_name`, `object_uri` | 버킷 이름, `s3://` URI |
| `core.gold_publication.storage` | `S3ImmutableObjectStore.put_verified` | put-once 후 checksum readback 확인 |
| `core.testing` | `s3_env`, `s3_bucket` fixture, `TEST_BUCKET`, `put_source_snapshot`, `put_partial_source_snapshot` | 테스트 공용 pytest plugin |

- `dt=`/`hh=` 파티션은 KST. `source_snapshot_manifest/`만 UTC 파티션
- `core.layout` 함수는 timezone-aware datetime만 받음
- 한 모듈만 쓰는 키(bronze, quarantine, archive manifest, GC manifest)는 해당 모듈 안에 둠
- 테스트 conftest는 `pytest_plugins = ("core.testing",)`로 fixture를 가져옴 (`normalizer/tests/conftest.py` 참고)

## 6. 모듈 CLI 계약 (Airflow·Makefile 호출 기준)

에이전트 2가 소유하는 collector 유지보수 CLI를 제외하면 이번 작업에서 바꾸지 않는다.

| 모듈 | 명령 | 호출 위치 |
| --- | --- | --- |
| collector | `main.py --source --window-start [--force] [--poi-master-mode --poi-master-manifest-uri --poi-master-manifest-sha256]` | `collector_task.py`, `Makefile`의 `seed-e2e` |
| collector | `main.py --source --check-due-after-seconds N` | `collector_task.py` (freshness gate) |
| collector | `main.py --source --report-window-stats --window-day [--window-hour]` | `collection_stats_task.py` |
| collector | `compact.py`, `cold_compact.py`, `hot_bronze_gc_cli.py`, `silver_gc_cli.py` | `compaction_task.py` (에이전트 2가 함께 변경 가능) |
| collector | `python -m bootstrap`, `python -m bootstrap.zip_stage` | `ops/training_smoke.sh` |
| normalizer | `main.py --window-start --poi-master-*`, `station_master.py --window-start` | `normalizer_task.py` |
| normalizer | `local_training_master.py --source-date` | `ops/training_smoke.sh` |
| nowcaster | `main.py estimate --target-date --source-window-start`, `main.py backfill-archive --csv-dir` | `nowcasting_task.py`, `ops/training_smoke.sh` |
| poi_master | `main.py refresh`, `main.py resolve --as-of` (출력 JSON의 `mode`, `manifest_uri`, `manifest_sha256`) | `poi_master_task.py` |

## 7. 세 PR 머지 후 후속 작업 (이번에는 하지 않음)

- POI static 모드 제거: `core.poi_master`, `collector/main.py`의 `--poi-master-mode`, `population_realtime.yaml`의 POI 범위, `normalizer/poi.py`의 `load_poi_areas`, `poi_master/publication.py`의 `_LEGACY_POI_SHP_PATH`, `poi_master_task.py`의 `POI_MASTER_MODE`. 최초 배포에서 activation이 없을 때의 처리 방식을 먼저 정해야 함
- 각 PR에 적힌 "core 이전 후보" helper의 `libs/core` 이전
- 인프라 정리 (별도 라운드)
  - `ops/resource_probe.py`, `ops/tests/test_resource_probe.py` 삭제 (에이전트 3이 래핑 제거 후 미사용)
  - `Makefile` 프로젝트 목록 4회 반복 통합, 호환용 migration alias 타깃과 비활성화된 `seed` 타깃 정리
  - CI arm64 job 하드코딩 목록 정리, `airflow-entrypoint.sh` prewarm 목록에서 `rebalance` 제거
  - 학습 EC2(`terraform/compute_train.tf`, `Makefile`의 `train-*`) 정리. Airflow는 학습에 EMR만 사용함
  - Hot Bronze 정리를 S3 lifecycle로 대체할지 검토
- 범위 밖 트랙(ML, 재배치, 서빙)
  - `rebalance/` 전체가 legacy. `rebalance/urgency.py`는 `loader/tests/gold/test_urgency.py`의 동등성 비교에서만 사용됨
  - `loader/main.py`, `transform.py`, `reader.py`, `tables.yaml`, `core.upsert`는 DAG가 호출하지 않는 legacy DB 적재 경로로 보임
  - `loader/reader.py`, `rebalance/reader.py`, `libs/ml_core/silver_schema.py`의 S3 키 조립을 `core.layout`으로 이전
  - `Makefile`의 `emr-features`와 `monthly_retrain.py`의 EMR 생성 로직 중복
  - `libs/core`의 서빙·ML 도메인 모듈(`inference_snapshot`, `model_snapshot`, `inference_catalog`, `serving_plan_input`, `gold_publication`)은 패키지 분리 후보

## 8. 검증

- 프로젝트별 테스트: `cd <project> && uv run --frozen --with pytest pytest -q`
- lint: `uvx ruff check .`. 기존 위반이 남아 있으므로(collector 24건 등) 변경 전후를 비교해 새 위반만 없앰. `--fix`가 무관한 파일을 고치면 되돌림
- 삭제한 기능의 테스트는 함께 삭제. 남은 기능의 테스트가 깨지면 고칠 대상은 코드
- 에이전트 1: collector 전체 테스트, `libs/core/tests/test_layout.py`
- 에이전트 2: collector 전체 테스트, normalizer·nowcaster·poi_master 테스트. `compaction_task.py`·`daily_compaction.py`를 바꿨으면 `airflow/tests`의 관련 테스트도 수정
- 에이전트 3: `make up` 후 `docker compose --env-file ops/compose/local.defaults.env --env-file .env.example -f ops/compose/docker-compose.prod.yml -f ops/compose/docker-compose.yml exec -T airflow-scheduler sh -lc 'cd /workspace/airflow && uv run pytest -q'`, 가능하면 `make e2e-smoke`. 끝나면 `make down`
- 에이전트 1과 2는 `collector/`를 나눠 가지므로 PR 생성 전 `git fetch origin`으로 상대 PR이 먼저 머지됐는지 확인하고, 머지됐으면 rebase 후 collector 테스트를 다시 돌림
- macOS에 `libomp`가 없으면 LightGBM import 실패로 loader·ml_core·ml/inference 일부 테스트가 수집 단계에서 실패함. 담당 범위가 아니면 `--continue-on-collection-errors`로 나머지만 확인

### Phase 1 이후 테스트 기준

| 프로젝트 | 결과 |
| --- | --- |
| `libs/core` | 367 passed, 13 skipped |
| `collector` | 881 passed, 9 skipped |
| `normalizer` | 116 passed |
| `nowcaster` | 62 passed |
| `poi_master` | 31 passed |
| `rebalance` | 67 passed |
| `apps/api` | 85 passed, 9 skipped |
| `loader` | 417 passed, 22 skipped (LightGBM 의존 3개 파일 제외) |
| `libs/ml_core` | 137 passed (LightGBM 의존 3개 파일 제외) |
