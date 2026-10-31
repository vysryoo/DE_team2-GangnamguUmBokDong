# 리팩토링 2라운드 인수인계

"에이전트 1/2/3"으로 지정받은 에이전트가 읽는 문서다. 지정받은 에이전트는 이 문서와 `AGENTS.md`를 읽은 뒤, 사람의 추가 지시나 허가 없이 설계부터 PR 생성까지 한 번에 진행한다. `HANDOFF.md`는 1라운드 기록이며 이번 작업 지시가 아니다.

## 1. 목표

- 전체 리팩토링 범위는 ML(`ml/`, `libs/ml_core`)을 뺀 나머지. 이번 2라운드는 1라운드에서 손댄 영역을 한 번 더 단순화하는 단계
  - 대상: `libs/core` 공통 모듈, 데이터 수집·정제(`collector/`, `normalizer/`, `nowcaster/`, `poi_master/`), 오케스트레이션(`airflow/`)
  - 이번에 하지 않음: 인프라, 재배치(`rebalance/`), 서빙·대시보드(`loader/`, `apps/`). 다음 라운드에서 진행
- 원칙: **기능은 그대로 두고, 코드를 최대한 쉽고 짧게 만든다**
  - 1라운드는 "기능 덜어내기"가 중심이었음. 이번에는 남은 기능을 유지한 채 읽기 쉬운 구조로 바꾸는 것이 중심
  - 처음 보는 사람이 모듈 하나를 위에서 아래로 읽고 흐름을 이해할 수 있어야 함
- 판단 기준
  - 한 함수는 한 가지 일만 함. 50줄을 넘는 함수는 나눌 수 있는지 먼저 검토
  - 호출 단계가 깊은 구조보다 평평한 구조를 택함. 한 곳에서만 쓰는 래퍼, 전달만 하는 함수, 상수 별칭은 걷어냄
  - 검증은 시스템 경계(외부 API 응답, S3에서 읽은 bytes, CLI 인자, 설정 파일)에서 한 번만 함. 내부 함수끼리 같은 값을 다시 검증하는 코드, `type(x) is not str` 같은 내부 인자 방어 코드는 걷어냄
  - 주석과 docstring은 "왜"만 짧게 남김. 코드를 다시 설명하는 서술, 실측 일지, 날짜가 붙은 경위, 표 형태의 긴 설명은 지우거나 한두 줄로 줄임. 모듈 docstring은 10줄 안팎
  - 이름만 보고 역할을 알 수 있게 함. 이름을 바꾸면 docstring과 주석을 줄일 수 있는 곳은 이름을 바꿈
  - 테스트도 같은 기준으로 정리. 같은 시나리오의 반복 케이스는 `pytest.mark.parametrize`로 합치고, 테스트 전용 헬퍼 중복은 `core.testing`이나 conftest로 모음. 검증하는 동작은 줄이지 않음
- 지키는 것 (바꾸면 안 됨)
  - 외부 동작: CLI 인자와 출력 JSON, 종료 코드, 로그 외 산출물, DAG id·스케줄·태스크 의존 관계
  - 저장 포맷: 운영 S3 키 규칙(`libs/core/tests/test_layout.py`), parquet 스키마, manifest JSON 필드
  - 판정 규칙: 수집 status·stage 결정, 품질 게이트, compaction·GC 대상 선택, 정규화·추정 계산 결과

## 2. 작업 방식

### 자율 진행

- 중간에 사람에게 확인이나 허가를 구하지 않는다. 선택지가 있으면 1절 기준으로 스스로 결정하고, 결정과 근거는 PR 본문의 "작업 내용 및 고민"에 남긴다
- 순서: 3절 준비 → 현황 파악 → 설계 → 구현 → 검증 → 커밋 → push → PR 생성
- 설계 문서 파일을 따로 만들지 않는다. 설계 내용은 PR 본문에 쓴다
- `HANDOFF.md`, `HANDOFF2.md`는 수정하지 않는다. 후속 작업은 PR 본문 "리뷰 포인트"에 적는다

### 하드 리밋 (어떤 경우에도 하지 않음)

- 1절 "지키는 것" 변경
- 4절 소유권 밖의 파일 수정
- 인프라 파일 수정: `Makefile`, `.github/`, `ops/`, `terraform/`, `.env.example`, `.gitignore`, `.dockerignore`
- `libs/core` 공개 API의 이름·시그니처·반환값·예외 종류 변경. 내부 구현 단순화만 허용 (4절 각 에이전트 담당 모듈 한정)
- 실제 AWS 리소스나 운영 S3 조작: `terraform`, `aws` CLI 쓰기·삭제, `make deploy-*`, `make train-*`, `make emr-*`
- PR 머지, `develop`/`main`에 직접 push, force push

### 커밋·PR 규칙

- 코드 수정 전 `.agent/coding.md`, 커밋 전 `.agent/commit.md`, PR 생성 전 `.agent/pr.md`를 읽고 따른다
- 동작을 바꾸지 않는 변경이므로 커밋 type은 `refactor`, `test`, `docs`, `style` 중에서 고름
- PR 대상: `origin`(`vysryoo/DE_team2-GangnamguUmBokDong`)의 `develop`. `gh pr create --repo vysryoo/DE_team2-GangnamguUmBokDong --base develop`
- 커밋 메시지와 PR 본문에 AI 서명을 넣지 않는다

## 3. 준비

현재 디렉터리는 에이전트 전용 git worktree다.

1. `git fetch origin` 후 `origin/develop`에 1라운드 머지 커밋이 있는지 확인: `git merge-base --is-ancestor e2c0f95 origin/develop`
2. 작업 브랜치 생성: `git switch -c <브랜치> origin/develop`
   - 에이전트 1: `refactor/r2-agent1-collector-ingest`
   - 에이전트 2: `refactor/r2-agent2-lake-and-refine`
   - 에이전트 3: `refactor/r2-agent3-airflow-and-core`
3. 담당 프로젝트마다 `uv sync`를 실행하고 테스트·lint baseline을 기록한다 (6절)
4. Docker(`make up` 등)는 에이전트 3만 사용한다. worktree끼리 같은 포트를 쓰므로 동시에 띄우면 충돌한다

## 4. 에이전트별 소유권과 작업

### 에이전트 1: collector 수집 경로

- 소유
  - `collector/main.py`, `pipeline.py`, `manifest.py`, `storage.py`, `logging_setup.py`, `adapters/`, `config/`, `validation/`, `sources/`, 해당 테스트
  - `libs/core/src/core/{masked,precip,snow,wind,_amount}.py`와 그 테스트 (내부 구현만. `core.precip`은 `loader/`도 사용함)
  - 문서: `docs/collector/` 중 `bootstrap-design.md`, `bootstrap-implementation-plan.md`, `archive-zip-staging.md`를 뺀 전부
- 현재 상태 (`develop` 기준 측정)
  - 긴 함수: `pipeline._validate_and_store`(96줄), `adapters/base.fetch_with_rounds`(93줄), `main.parse_args`(85줄), `kma_apihub._fetch_grid`(79줄), `seoul_openapi._discover_probe_pages`(74줄), `pipeline.execute_window`(70줄)
  - 큰 파일: `adapters/seoul_openapi.py`(666줄), `pipeline.py`(632줄)
  - docstring이 코드 줄 수의 약 17%. `pipeline.py` 모듈 docstring은 90줄이 넘음
- 작업
  - 위 함수들을 단계별로 나누고 `pipeline.py`의 흐름이 모듈 상단부터 한 번에 읽히게 정리
  - `seoul_openapi.py`의 페이지네이션 방식별 분기를 정리하고 공통 요청·파싱 코드를 합침
  - `validation/`의 registry·policy 구조가 실제 사용 방식보다 일반화돼 있으면 단순화
  - 1라운드 잔여물: `config/schema.py`의 `Backfill`과 `SourceConfig.backfill` 삭제. 더 이상 읽는 코드가 없음
  - `Manifest.backfill_status`는 운영 S3의 기존 manifest가 이 키를 갖고 있어 유지
  - 소유 문서에 남은 `--backfill` 등 낡은 서술 정리
- 유지
  - `collector/main.py` CLI 전체 (5절)
  - 에이전트 2가 사용하는 `manifest`의 `Manifest`, `RunStatus`, `Stage`, `load`, `load_source_snapshots`와 `storage`의 `read_immutable_silver_artifact`, `list_source_snapshot_windows`, `write_bronze_part`, `read_bronze`, `list_silver_objects`, `write_archive`, `archive_exists`, `write_archive_manifest`, `read_archive_manifest` 시그니처
  - `storage.py`의 legacy Bronze 경로(`revision=None`)는 에이전트 2의 `cold_bronze` 테스트가 사용하므로 유지 (7절 후속)

### 에이전트 2: collector 레이크 유지보수 + 정제

- 소유
  - `collector/compaction.py`, `maintenance.py`, `cold_bronze.py`, `hot_bronze_gc.py`, `silver_gc.py`, `bootstrap/`, 해당 테스트
  - `normalizer/`, `nowcaster/`, `poi_master/`
  - `libs/core/src/core/{poi_master,weather_grid}.py`와 그 테스트 (내부 구현만. `core.weather_grid`는 `loader/`도 사용함)
  - 문서: `docs/collector/bootstrap-design.md`, `bootstrap-implementation-plan.md`, `archive-zip-staging.md`, `docs/normalizer/`, `docs/nowcaster/`, `normalizer/README.md`, `nowcaster/README.md`
- 현재 상태
  - 긴 함수: `bootstrap/runner.load_date`(139줄), `bootstrap/__main__.main`(93줄), `bootstrap/zip_stage._select`(86줄), `silver_gc.collect_date`(85줄), `hot_bronze_gc.collect_date`(68줄), `compaction.compact_date`(66줄), `poi_master/publication.refresh_poi_master`(90줄), `normalizer/main.run`(90줄), `normalizer/station_master.enrich_station_master`(83줄), `nowcaster/estimate_day.build_nowcast_table`(81줄), `poi_master/registry._read_list_rows`(78줄)
  - `core.poi_master`: `_validate_poi_master_table`(93줄) 등 내부 방어 코드가 많음
- 작업
  - 위 함수들을 단계별로 나누고 GC 3종(`cold_bronze`, `hot_bronze_gc`, `silver_gc`)에 공통인 "대상 나열 → 검증 → manifest 기록 → 삭제" 흐름의 중복 코드를 합침
  - `bootstrap/`의 CSV·ZIP 처리 흐름 정리
  - `poi_master`와 `core.poi_master`의 중복 검증 정리. 경계(첨부 파일 bytes, S3 readback) 검증은 유지
  - normalizer `poi.py`의 `_select_largest_polygon`과 `poi_master/registry.py`의 `_largest_polygon`은 POI static 모드가 남아 있는 동안 둘 다 필요함. 둘 중 하나로 합칠 수 있으면 합침 (`libs/core` 공개 API 추가 금지)
  - 소유 문서에서 `cold_compact.py`, `hot_bronze_gc_cli.py`, `silver_gc_cli.py` 등 옛 CLI 이름을 `maintenance.py`로 갱신
- 유지
  - `maintenance.py`, normalizer·nowcaster·poi_master CLI 전체 (5절)
  - `cold_bronze.write_pending_marker(source_id, day, hot_key)`, `cold_bronze.read_revision(source_id, window_start, revision, parts)` 시그니처 (에이전트 1의 `storage.py`가 호출)
  - POI static 모드 (7절 후속)

### 에이전트 3: Airflow + core 공통 모듈

- 소유
  - `airflow/` 전체 (`compaction_task.py`, `daily_compaction.py`, `config/sources.py` 포함)
  - `libs/core/src/core/{layout,kst,sources,s3,testing,source_snapshot,source_snapshot_io}.py`, `libs/core/src/core/gold_publication/storage.py`와 그 테스트 (내부 구현만)
  - 문서: `docs/airflow/`, `libs/core/README.md`
- 현재 상태
  - 긴 함수: `realtime_tick._build_realtime_tick_dag`(115줄), `aws_infra_task.create_emr_cluster`(111줄), `aws_infra_task.submit_emr_step`(57줄), `task_builder.build_module_task`(55줄), `emr_orphan_reaper._reap_orphan_emr_clusters`(51줄)
  - core 공통 모듈: `s3._read_parquet_by_dates`(91줄), `s3.read_parquet`(88줄), `s3._client`(88줄), `s3.concat_compatible_tables`(82줄), `gold_publication/storage.put_once`(67줄). `type(x) is not ...` 내부 방어 검사가 core 쪽에 약 60곳
- 작업
  - DAG 정의를 읽으면 태스크 흐름이 보이도록 `realtime_tick`, `monthly_retrain` 빌더 함수 분리
  - EMR 제어(`aws_infra_task.py`)의 클러스터 설정 조립을 데이터 구조와 짧은 함수로 정리. EMR 동작은 유지
  - 1라운드 잔여물: `daily_compaction.py`의 `build_daily_history_replay_task` 호출을 통합 빌더 `build_collector_task`로 바꾸고 전용 빌더 삭제
  - `docs/airflow/`에서 삭제된 `*_task.py` 모듈명, `db_loader`, resource probe 서술 정리. `TASK_RESOURCE_PROFILES.md`는 resource probe가 Airflow에서 빠졌다는 사실만 짧게 남김
  - core 공통 모듈의 내부 단순화: `s3.py`의 parquet 읽기 경로 정리, 공개 API 입력에서 같은 값을 반복 검증하는 코드 정리
- core 수정 시 추가 검증 (공개 API를 쓰는 다른 프로젝트)
  - `collector`, `normalizer`, `nowcaster`, `poi_master`, `loader`, `rebalance`, `apps/api`, `libs/ml_core`, `ml/inference` 테스트 전체를 실행
  - `libs/core/tests/test_layout.py` golden test는 수정하지 않음
- 유지
  - Airflow가 호출하는 모듈 CLI 형태 (5절), DAG id·스케줄·태스크 의존 관계
  - `ops/compose/airflow-entrypoint.sh`가 기대하는 실행 방식

### 소유자가 없는 영역 (이번 라운드에서 수정 금지)

- `libs/core`의 나머지 모듈: `db`, `upsert`, `regions`, `forecast`, `scoring_config`, `gold_publication/`(`storage.py` 제외), `inference_snapshot`, `inference_catalog`, `model_snapshot`, `serving_plan_input`
- `loader/`, `apps/`, `rebalance/`, `ml/`, `libs/ml_core/`, 인프라 파일
- `docs/adr/`: 결정 기록이므로 수정하지 않음

## 5. 모듈 CLI 계약 (이번 라운드에서 변경 없음)

| 모듈 | 명령 | 호출 위치 |
| --- | --- | --- |
| collector | `main.py --source --window-start [--force] [--poi-master-mode --poi-master-manifest-uri --poi-master-manifest-sha256]` | `airflow/orchestration/collector_task.py`, `Makefile`의 `seed-e2e` |
| collector | `main.py --source --check-due-after-seconds N` | `collector_task.py` (freshness gate) |
| collector | `main.py --source --report-window-stats --window-day [--window-hour]` | `collector_task.py` |
| collector | `maintenance.py compact / cold-compact / gc-hot-bronze / gc-silver` | `compaction_task.py` |
| collector | `python -m bootstrap`, `python -m bootstrap.zip_stage` | `ops/training_smoke.sh` |
| normalizer | `main.py --window-start --poi-master-*`, `station_master.py --window-start`, `local_training_master.py --source-date` | `refine_task.py`, `ops/training_smoke.sh` |
| nowcaster | `main.py estimate --target-date --source-window-start`, `main.py backfill-archive --csv-dir` | `refine_task.py`, `ops/training_smoke.sh` |
| nowcaster | `main.py bootstrap-lookback` | 운영 초기 수동 실행 (`docs/nowcaster/implementation-plan.md`) |
| poi_master | `main.py refresh`, `main.py resolve --as-of` (출력 JSON의 `mode`, `manifest_uri`, `manifest_sha256`) | `refine_task.py` |

호출 위치의 파일명은 1라운드 통합 이후 기준이다. 정확한 위치는 `grep -rn "main.py\|maintenance.py" airflow/orchestration`으로 확인한다.

## 6. 검증

- 프로젝트별 테스트: `cd <project> && uv run --frozen --with pytest pytest -q`
- 시작 전 baseline과 비교해 실패가 늘지 않아야 함. 테스트를 합쳐 개수가 줄면 PR 본문에 어떤 케이스를 어디로 합쳤는지 적음
- lint: `uvx ruff check .`. 기존 위반은 변경 전후를 비교해 새 위반만 없앰. `--fix`가 무관한 파일을 고치면 되돌림
- 에이전트 1: `collector` 전체, `libs/core`(담당 모듈 수정 시), `loader`(`core.precip` 수정 시)
- 에이전트 2: `collector` 전체, `normalizer`, `nowcaster`, `poi_master`, `libs/core`(담당 모듈 수정 시), `loader`(`core.weather_grid` 수정 시)
- 에이전트 3: Airflow 컨테이너 테스트(`make up` 후 `docker compose --env-file ops/compose/local.defaults.env --env-file .env.example -f ops/compose/docker-compose.prod.yml -f ops/compose/docker-compose.yml exec -T airflow-scheduler sh -lc 'cd /workspace/airflow && uv run pytest -q'`), 가능하면 `make e2e-smoke`, 끝나면 `make down`. core 수정 시 4절의 다른 프로젝트 전체
- 에이전트 1과 2는 `collector/`를 나눠 가지므로 PR 생성 전 `git fetch origin`으로 상대 PR이 먼저 머지됐는지 확인하고, 머지됐으면 rebase 후 collector 테스트를 다시 돌림
- macOS에 `libomp`가 없으면 LightGBM import 실패로 `loader`·`libs/ml_core`·`ml/inference` 일부 테스트가 수집 단계에서 실패함. 이번 변경과 무관하면 `--continue-on-collection-errors`로 나머지를 확인하고 PR 본문에 적음

## 7. 이번 라운드 후속 작업 (하지 않음)

- POI static 모드 제거: `core.poi_master`, `collector/main.py`, `population_realtime.yaml`, `normalizer/poi.py`, `poi_master/publication.py`, `airflow`의 `POI_MASTER_MODE`에 걸쳐 있음
- `storage.py` legacy Bronze 경로(`revision=None`)와 그 `cold_bronze` 테스트 삭제. 운영 S3에서 legacy Bronze가 lifecycle로 모두 만료됐는지 확인 후 진행
- `Manifest.backfill_status` 삭제. 기존 manifest 호환 방식 결정 필요
- 다음 라운드 대상
  - 인프라: resource probe 잔재(`ops/resource_probe.py`, `.env.example`, `.gitignore`·`.dockerignore`의 `airflow/resource-profiles`), `Makefile` 프로젝트 목록 통합, CI arm64 목록, prewarm 목록의 `rebalance`, 학습 EC2 terraform
  - 재배치: `rebalance/` 전체가 legacy. `loader/tests/gold/test_urgency.py`의 동등성 비교에서만 사용됨
  - 서빙·대시보드: `loader/main.py`, `transform.py`, `reader.py`, `tables.yaml`, `core.upsert`의 legacy 적재 경로, `loader/gold/station_release.py`(2,706줄)·`serving_plan.py`(2,491줄), `loader`·`rebalance`의 S3 키 조립을 `core.layout`으로 이전
