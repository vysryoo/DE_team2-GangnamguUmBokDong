# 리팩토링 인수인계

데이터 수집·정제(Phase 2)와 오케스트레이션·인프라(Phase 3)를 병렬로 진행하는 에이전트가 읽는 문서다. 작업 시작 전 이 문서 전체와 `AGENTS.md`를 읽는다.

## 1. 목표와 범위

- 목표: 중복 코드 제거, 이해하기 쉬운 구조, 불필요하거나 과한 기능 제거
- 이번 리팩토링 범위: (1) 공통 기반 정리 (2) 데이터 수집·정제 (3) 오케스트레이션·인프라
- 범위 밖(추후 진행): ML(`ml/`, `libs/ml_core`), 재배치(`rebalance/`), 서빙·대시보드(`loader/`, `apps/`)
- 사용자 방침: 불필요하거나 과한 기능은 과감하게 제거해도 됨
- 단, 아래는 실행 전 사용자 확인 필요
  - 운영 데이터 보존 정책이 바뀌는 변경 (S3 lifecycle, 장기 보관 계층 삭제)
  - `terraform apply` 등 실제 AWS 리소스 생성·삭제

## 2. Phase 1 완료 사항 (`refactor/common-base`)

`libs/core`에 공통 인터페이스를 추가하고 collector·normalizer·nowcaster·poi_master를 이전함. 이후 트랙은 이 인터페이스를 그대로 사용한다.

| 모듈 | 제공 | 용도 |
| --- | --- | --- |
| `core.layout` | `silver_prefix`, `silver_day_prefix`, `silver_window_key`, `silver_content_key`, `nowcast_key`, `window_manifest_key`, `window_manifest_prefix`, `source_snapshot_day_prefix`, `source_snapshot_hour_prefix`, `source_snapshot_logical_prefix`, `source_snapshot_manifest_key`, `parse_source_snapshot_manifest_key`, `archive_key`, `archive_prefix` | 여러 모듈이 공유하는 S3 키 |
| `core.kst` | `KST`, `parse_kst_datetime`, `today_kst` | 시간대 상수, CLI 시각 인자 파싱 |
| `core.sources` | source id 문자열 상수, `COLLECTOR_SOURCE_IDS` | source id 하드코딩 대체 |
| `core.s3` | `bucket_name`, `object_uri` (기존 `_bucket` 대체) | 버킷 이름, `s3://` URI |
| `core.gold_publication.storage` | `S3ImmutableObjectStore.put_verified` | put-once 후 checksum readback 확인 |
| `core.testing` | `s3_env`, `s3_bucket` fixture, `TEST_BUCKET`, `put_source_snapshot`, `put_partial_source_snapshot` | 테스트 공용 pytest plugin |

### 규칙

- `dt=`/`hh=` 파티션은 KST. 예외는 `source_snapshot_manifest/`로 UTC 파티션
- `core.layout` 함수는 timezone-aware datetime만 받음. naive면 `ValueError`
- 한 모듈만 쓰는 키(bronze, quarantine, archive manifest, GC manifest)는 해당 모듈에 둠. 두 모듈 이상이 쓰게 되면 `core.layout`으로 옮김
- `core.sources`는 Enum이 아니라 문자열 상수. 계약 검증 코드가 `type(value) is str`로 확인하기 때문
- 테스트 conftest는 `pytest_plugins = ("core.testing",)`로 fixture를 가져옴. moto bucket이 모든 테스트에 필요하면 `s3_bucket`을 autouse fixture로 감쌈 (`normalizer/tests/conftest.py` 참고)
- 키 규칙 golden test: `libs/core/tests/test_layout.py`. 키를 바꾸는 변경은 운영 S3 데이터와 어긋나므로 금지

### 동작이 바뀐 부분

- `--window-start`, `--as-of`, `--source-window-start`: 오프셋 없는 시각 거부, 다른 오프셋은 KST로 변환. Airflow는 항상 `+09:00`을 넘김
- window manifest와 mutable silver 키: 넘겨받은 오프셋 대신 항상 KST로 파티션 계산. KST 입력에서는 기존과 동일
- collector immutable 쓰기: PUT 응답만 유실된 경우 readback으로 성공 처리 (poi_master는 기존에도 같은 동작)

## 3. 병렬 작업 전 공통 규칙

- `develop`에서 Phase 1 PR 머지 후 트랙별 브랜치 생성
- `libs/core` 공개 API 변경은 트랙 PR에 섞지 않고 별도 PR로 분리. 추가만 하는 변경도 동일
- Airflow가 호출하는 모듈 CLI 계약(4절)을 바꾸는 트랙은 같은 PR에서 Airflow 호출부도 함께 수정
- 커밋·PR 작성 전 `.agent/commit.md`, `.agent/pr.md`를 읽음
- 코드 수정 전 `.agent/coding.md`를 읽음

### 파일 소유권

| 경로 | 트랙 |
| --- | --- |
| `libs/core/**` | 별도 합의 PR |
| `collector/` 중 아래 2B 파일 제외 전부 | 2A |
| `collector/{compaction,compact,cold_bronze,cold_compact,hot_bronze_gc,hot_bronze_gc_cli,silver_gc,silver_gc_cli}.py`, `airflow/orchestration/compaction_task.py`, `airflow/dags/daily_compaction.py` | 2B |
| `normalizer/`, `nowcaster/`, `poi_master/` | 2C |
| `Makefile`, `.github/`, `ops/` (`ops/resource_probe.py` 제외), `terraform/` | 3A |
| `airflow/` 중 2B 소유 제외 전부, `ops/resource_probe.py`, `ops/tests/test_resource_probe.py` | 3B |

## 4. 모듈 CLI 계약 (Airflow 호출 기준, Phase 1에서 변경 없음)

| 모듈 | 명령 | 호출 위치 |
| --- | --- | --- |
| collector | `main.py --source --window-start [--force] [--poi-master-mode --poi-master-manifest-uri --poi-master-manifest-sha256]` | `airflow/orchestration/collector_task.py` |
| collector | `main.py --source --check-due-after-seconds N` | `collector_task.py` (freshness gate, subprocess) |
| collector | `main.py --source --report-window-stats --window-day [--window-hour]` | `airflow/orchestration/collection_stats_task.py` |
| collector | `compact.py --source [--date]` | `compaction_task.py` |
| collector | `cold_compact.py --source --recover-pending --today --delay-days` | `compaction_task.py` |
| collector | `hot_bronze_gc_cli.py --source --today --retention-days` | `compaction_task.py` |
| collector | `silver_gc_cli.py --source --date [--require-archive]` | `compaction_task.py` |
| normalizer | `main.py --window-start --poi-master-*`, `station_master.py --window-start` | `airflow/orchestration/normalizer_task.py` |
| nowcaster | `main.py estimate --target-date --source-window-start` | `airflow/orchestration/nowcasting_task.py` |
| poi_master | `main.py refresh`, `main.py resolve --as-of` | `airflow/orchestration/poi_master_task.py` |

## 5. 트랙별 작업 목록

### 2A. collector 수집 경로

- legacy backfill 경로 삭제. 어떤 `collector/sources/*.yaml`도 `backfill`을 켜지 않음
  - `main.py`: `--backfill`, `--list-backfill-targets`
  - `pipeline.py`: `ForceAndBackfillError`, `_sync_retry_marker`, `get_backfill_targets`, backfill 재개 분기
  - `manifest.py`: `RetryMarker`, `save/load/clear_retry_marker`
  - `storage.py`: `_retry_marker_key`, `write/list/delete_retry_marker`
  - `config/schema.py`: `Backfill`, `SourceConfig.backfill`
- `pipeline.execute_window` (약 580줄 단일 함수)를 단계 함수로 분리. manifest `stage` 의미와 status 결정 규칙(모듈 docstring 표)은 유지
- `main.py`의 모드(수집, freshness 확인, 통계 리포트) 정리. 4절 CLI 플래그 이름은 유지
- `storage.write_silver`(mutable silver)는 테스트에서만 호출됨. compaction이 legacy silver key를 아직 읽으므로 2B와 함께 제거 여부 판단
- `airflow/config/sources.py`, `airflow/orchestration/collector_task.py`의 `--backfill` 언급 주석은 3B가 정리

### 2B. collector 레이크 유지보수

- Hot/Cold Bronze와 GC 3종(`cold_bronze`, `hot_bronze_gc`, `silver_gc`) 단순화
  - 추천안: Cold 아카이브만 남기고 Hot Bronze 삭제는 S3 lifecycle(30일)로 대체
  - 보존 정책 변경이므로 구현 전 사용자 확인
  - lifecycle 규칙은 `terraform/data.tf`(3A 소유)에 있으므로 3A와 조율
- 유지보수 CLI 4개의 반복 boilerplate(argparse, JSON 결과 출력) 통합
- CLI를 바꾸면 같은 PR에서 `compaction_task.py`, `daily_compaction.py` 수정

### 2C. normalizer / nowcaster / poi_master

- POI Master static 모드 제거 검토. 추천안: s3 모드만 유지
  - 영향 범위: `collector/main.py`의 `--poi-master-mode`(2A 소유), `collector/sources/population_realtime.yaml`의 `poi_start/poi_end/poi_exclude`, `normalizer/poi.py`의 `load_poi_areas`, `normalizer/main.py`, `poi_master/publication.py`의 `_LEGACY_POI_SHP_PATH`(최초 refresh drop guard), `airflow/orchestration/poi_master_task.py`(3B 소유)
  - activation이 하나도 없을 때(최초 배포)의 처리 방식을 먼저 정해야 함
  - `poi_master/tests/conftest.py`가 `normalizer/data/`의 xlsx·zip을 테스트 fixture로 사용함. 파일 삭제 시 fixture 위치 이동 필요
  - 2A와 파일이 겹치므로 2A 머지 후 진행하거나 2A와 순서 합의
- `normalizer/poi.py`의 `_select_largest_polygon`과 `poi_master/registry.py`의 `_largest_polygon` 중복 제거
- `normalizer/main.py`의 `run()` 정리
- 1회성 도구(`nowcaster main.py backfill-archive`/`bootstrap-lookback`, `normalizer/local_training_master.py`)는 `ops/training_smoke.sh`가 사용 중. 삭제하지 않음

### 3A. 인프라 (Phase 1 머지 직후 시작 가능)

- `Makefile`: `PROJECTS` 목록 4회 반복을 하나로 통합, 호환용 migration alias 타깃 정리, 비활성화된 `seed` 타깃 정리
- `.github/workflows/ci.yml`: arm64 job의 하드코딩된 프로젝트 목록을 make 타깃으로 대체
- `ops/compose/airflow-entrypoint.sh`: prewarm 목록에서 `rebalance` 제거 (Airflow가 호출하지 않음)
- 학습 EC2 제거 검토: Airflow는 학습 EC2를 쓰지 않고 EMR을 사용함. 대상은 `terraform/compute_train.tf`, IAM `train` 역할, `Makefile`의 `train-*`/`ssh-train`. `terraform apply`는 사용자 확인 후 별도 진행
- 2B의 lifecycle 변경 요청 반영

### 3B. Airflow (2A·2B의 CLI 변경 PR 머지 후 시작)

- 미사용 코드 삭제
  - `airflow/orchestration/db_loader_task.py`와 `config/schedules.py`의 `DB_LOADER_EXECUTION_TIMEOUT`
  - `airflow/orchestration/aws_infra_task.py`: `start_ec2_instance`, `stop_ec2_instance`, `run_command_on_ec2`, `run_emr_feature_mart_job`, `resize_emr_cluster`, `get_core_instance_group_id`. 삭제 후 `is_mock_mode`·`is_emr_mock_mode`를 쓰는 곳이 남는지 확인
  - `airflow/pyproject.toml`의 미사용 `ml_core` uv source
- `collector_task.py`의 빌더 4개(일반, population, replay, daily replay)를 옵션을 받는 빌더 1개로 통합
- `"uv run --frozen python {script} {args}"` 명령 조립 헬퍼 도입
- 20~90줄짜리 `*_task.py` 14개를 도메인별 3~4개 모듈로 통합
- `resource_probe` 래핑(`task_builder._profiled_bash_command`)과 `ops/resource_probe.py` 제거 검토
- `airflow/config/sources.py`와 `collector/sources/*.yaml`의 source id 일치 테스트 추가. Airflow venv는 core를 설치하지 않으므로 YAML을 직접 읽음
- 낡은 주석 정리: `templates.py`의 `loader/s3_reader.py`(존재하지 않는 파일) 언급, `--backfill` 언급

## 6. 검증 방법

- 프로젝트별: `cd <project> && uv run --frozen --with pytest pytest -q`
- lint: `uvx ruff check .`. 기존 위반이 남아 있으므로(collector 24건 등) 변경 전후 결과를 비교해 새 위반만 확인
- Airflow 테스트: 컨테이너 안에서만 실행됨. `make up` 후 `make test`의 airflow 단계 또는 `docker compose ... exec airflow-scheduler sh -lc 'cd /workspace/airflow && uv run pytest -q'`
- 오케스트레이션 변경 후: `make up`, `make e2e-smoke`
- macOS에 `libomp`가 없으면 LightGBM import 실패로 loader·ml_core·ml/inference 일부 테스트 수집이 실패함. 이번 변경과 무관하며 `--continue-on-collection-errors`로 나머지를 실행하거나 CI 결과로 확인

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

## 7. 범위 밖 기록 (추후 트랙)

- `rebalance/` 전체가 legacy. 실제 파이프라인은 `loader/serving_cli.py`와 `loader/gold/*`를 사용하고, `rebalance/urgency.py`는 `loader/tests/gold/test_urgency.py`의 동등성 비교에서만 사용됨
- `loader/main.py`, `loader/transform.py`, `loader/reader.py`, `loader/tables.yaml`, `core.upsert`는 DAG에서 호출되지 않는 legacy DB 적재 경로로 보임
- `loader/reader.py`, `rebalance/reader.py`, `libs/ml_core/silver_schema.py`에 S3 키 조립이 남아 있음. 해당 트랙에서 `core.layout`으로 이전
- `Makefile`의 `emr-features`와 `airflow/dags/monthly_retrain.py`의 EMR 생성 로직 중복
- `libs/core`의 서빙·ML 도메인 모듈(`inference_snapshot`, `model_snapshot`, `inference_catalog`, `serving_plan_input`, `gold_publication`)은 패키지 분리 후보
