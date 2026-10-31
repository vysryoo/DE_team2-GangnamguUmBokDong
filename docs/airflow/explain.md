# Airflow 운영 구조와 데이터 흐름

이 문서는 현재 코드에 구현된 Airflow DAG를 설명한다. 기준 코드는 `airflow/dags/`, `airflow/config/schedules.py`, `airflow/config/sources.py`와 `airflow/orchestration/`이다.

## 한눈에 보기

```text
외부 API
  → Collector: Bronze JSON + Silver Parquet + source manifest
  → Normalizer/Nowcaster: 추론용 파생 Silver
  → Serving plan: 입력·모델의 URI와 SHA 고정
  → LightGBM inference: 대여·반납 12시간 예측
  → Gold PostgreSQL/PostGIS: 동일 기준시각의 projection 원자 게시
  → 긴급도·재배치 경로
  → FastAPI → React 대시보드
```

Airflow는 계산을 직접 구현하지 않는다. `BashOperator`로 각 애플리케이션의 CLI를 실행하고 순서, 재시도, timeout과 성공·실패 상태를 관리한다. 월별 재학습과 EMR 정리 DAG만 `PythonOperator`로 AWS EMR 클러스터와 스텝을 제어한다.

## DAG와 실제 주기

모든 시간은 KST(`Asia/Seoul`) 기준이며 `catchup=False`, `max_active_runs=1`이다.

| DAG | cron | 역할 |
| --- | --- | --- |
| `realtime_tick` | 5분마다 | 실시간 수집, 필요한 날씨 수집, serving 체인 실행 |
| `poi_master_refresh` | 매일 02:04 | 서울시 POI 파일 변경 확인 후 POI Master 게시 |
| `daily_population_and_events` | 매일 03:00 | 생활인구 수집·nowcast와 두 행사 source 게시 |
| `station_master` | 매일 03:04 | 대여소 마스터 수집 후 공간·격자 정보 보강 |
| `daily_compaction` | 매일 04:30 | D-6 대여이력 재수집, Archive·Cold Bronze 생성, Hot Bronze·Silver 정리 |
| `daily_collection_report` | 매일 07:00 | 전날 수집 통계 Slack 보고 |
| `hourly_collection_alert` | 매시 정각 | 지난 1시간 수집 통계가 위험 기준을 넘으면 Slack 알림 |
| `monthly_retrain` | 매월 1일 03:00 | EMR 클러스터 하나에서 대여 → 반납 순으로 평가 및 조건부 재학습 |
| `emr_orphan_reaper` | 15분마다 | teardown이 실행되지 못해 남은 재학습 EMR 클러스터 종료 |

`station_master`의 03:04는 03:00 realtime 실행 중 source authority가 바뀌어 Gold finalize가 중단되는 것을 피하기 위한 운영값이다. realtime 처리시간이 5분에 가까워지면 이 간격을 다시 검토해야 한다.

## 날씨 수집을 realtime 체인에 넣은 방식

예전에는 날씨 수집 DAG가 따로 돌고 realtime DAG의 sensor가 S3 manifest를 polling했다. 이 방식은 완료 관계가 DAG 그래프에 드러나지 않고, sensor가 LocalExecutor 슬롯을 점유했다. 현재는 `realtime_tick` 하나가 매 tick 날씨 collector를 포함하고, `weather_ready_gate → prepare_serving_plan`을 명시적인 task dependency로 연결한다.

```text
freshness_gate_{source} → collect_{source} ─→ weather_ready_gate ─┐
bike_station_realtime ────────────────────────────────────────────┴→ prepare → inference → Gold
```

- 날씨 source마다 `freshness_gate_{source}`가 collector CLI(`--check-due-after-seconds`)로 마지막 성공 수집 이후 경과 시간을 확인한다. 초단기실황·초단기예보는 10분, 단기예보는 3시간이 지나지 않았으면 그 source의 수집만 건너뛴다.
- 경과 시간은 Airflow `logical_date`가 아니라 collector가 실제 현재 시각으로 판단한다. 이전 run이 늦게 끝나면 `logical_date`가 실제 시각보다 뒤처지기 때문이다.
- gate는 `ignore_downstream_trigger_rules=False`라 직접 하위 collect만 스킵한다.
- `weather_ready_gate`는 `TriggerRule.ALL_DONE`이므로 날씨 수집이 실패하거나 스킵돼도 prepare는 이전의 유효한 weather snapshot으로 진행한다. 각 날씨 collector는 `retries=0`, timeout 60초로 제한한다.

DAG를 cron별로 나누면 DAG마다 `max_active_runs`가 따로 관리돼 지연된 tick끼리 겹칠 수 있으므로 한 DAG에서 태스크 의존성으로 순서를 강제한다. 또 모든 root 태스크 앞의 `allow_current_tick`이 예약 시각보다 1분 넘게 늦게 시작한 scheduled run을 Slack 알림과 함께 건너뛴다. 수동 run과 clear된 run은 항상 실행한다.

- `airflow/dags/realtime_tick.py`: DAG 구성
- `airflow/tests/test_realtime_tick.py`: gate, 날씨 의존성, serving 체인, replay 검증

## Realtime serving 체인

```text
bike_station_realtime ─────────────────────────────────────┐
freshness gate → weather collectors → weather_ready_gate ──┴→ prepare_serving_plan
                                                                    ↓
bike_rental_history ───────────────────────────────────────────→ run_inference
resolve_poi_master → population_realtime → run_normalizer ─────────↗   ↓
                                                         finalize_serving_release
                                                                    ↓
                                                         publish_station_urgency
                                                                    ↓
                                                         publish_rebalance_route

bike_rental_history → 1시간 전 replay → 2시간 전 replay  (serving과 독립된 side chain)
```

| 단계 | 실제 책임 | 실패 시 경계 |
| --- | --- | --- |
| Collector | API 응답을 Bronze/Silver로 저장하고 검증된 source manifest 게시 | 필수 실시간 source가 실패하면 downstream 중단 |
| 날씨 gate | 날씨 태스크의 성공 여부와 무관하게 종료 | 새 날씨가 없으면 prepare가 이전의 유효한 snapshot 사용 가능 |
| Normalizer | 일별 격자 인구를 실시간 POI 인구로 보정 | 추론 중단 |
| Serving plan | station, stock, weather, model 입력을 immutable URI+SHA로 고정 | Gold는 변경되지 않음 |
| Inference | 공통 지원 대여소별 1~12시간 대여·반납량 생성 | 이전 정상 Gold release 유지 |
| Finalize | station, stock, demand, weather projection을 한 트랜잭션으로 게시 | 입력 drift 또는 불완전 결과면 전체 게시 중단 |
| Urgency·route | 새 release로 긴급도와 센터별 제안 경로 계산 | serving release는 유지되고 파생 정보만 갱신되지 않음 |

날씨 collector는 재시도 없이 60초 안에 끝내도록 제한한다. 실패해도 `ALL_DONE` gate를 통과시켜 이전의 유효한 날씨로 serving을 계속할 수 있게 한다. 반면 재고, 대여이력과 정규화 인구는 inference의 필수 upstream이다.

## 일·월 단위 체인

### 일별 생활인구와 행사

세 branch는 서로 독립이다.

- `living_population_grid → run_nowcasting`
- `cultural_event → event:cultural_event Gold 게시`
- `performance_event → event:performance_event Gold 게시`

생활인구 Collector가 `PARTIAL`이면 authority가 없으므로 actual Archive 승격은 건너뛰되,
기존 Archive 기반 nowcast 추정 로직은 계속 실행한다. Exact authority가 비어 있는 것과
달리 revision·manifest·Silver checksum이 손상되면 Nowcaster는 fail-closed한다. 행사
`PARTIAL`은 새 Gold를 게시하지 않는다. 기존 `publication_state`와 content-addressed
manifest가 일치하면 Gold 행과 state를 변경하지 않고 CLI는 `stale`로 성공하지만, 최초
실행처럼 유지할 state가 없거나 기존 manifest가 손상됐으면 해당 행사 branch가 실패한다.
세 branch는 독립이므로 한 행사 실패가 생활인구나 다른 행사 branch를 막지 않는다.

### 대여소 마스터

`bike_station_master`를 수집한 뒤 `station_master_enriched`를 만든다. realtime plan과 inference가 이 보강 결과를 필수 입력으로 사용한다.

### 일별 compaction

D-6의 대여이력 24개 시간대를 순차적으로 강제 재조회한다. 각 시간대는 `ALL_DONE`으로 다음 시간대를 계속 시도하며, 마지막에는 성공 여부와 무관하게 대여이력 compaction과 Cold recovery를 각각 실행한다. 두 작업은 서로 의존하지 않는다. 대여소 실시간·실시간 생활인구·초단기실황 compaction도 Cold와 독립적이다. Cold worker는 전체 날짜 범위를 훑지 않고 모든 Collector source의 `_cold_pending` marker 중 6일이 지난 날짜만 처리한다. 그 뒤 Hot GC가 Cold manifest 중 미완료 날짜만 확인해 검증된 inventory의 exact key를 생성 30일 뒤 삭제한다. IAM object tagging과 태그 기반 Lifecycle은 사용하지 않는다. D-36 날짜에서는 모든 source의 30일 지난 non-authority Silver를 정리한다. Archive 대상 4개는 Cold와 Archive를 모두 검증하고, 나머지는 Cold를 검증하며 최신 authority는 항상 유지한다.

### 월별 모델 점검

대여와 반납을 한 DAG에서 순서대로 실행해 EMR 클러스터가 동시에 두 개 뜨지 않게 한다.

```text
start_mlflow → create_cluster → [rental 체인] → [return 체인] → terminate_cluster → stop_mlflow

모델 체인: refresh_feature_mart → evaluate → check_retrain_branch ─┬→ orchestrate_retrain_loop
                                                                  └→ skip_monthly_retrain
```

`terminate_cluster`와 `stop_mlflow`는 teardown이라 DAG run을 수동으로 실패 처리해도 실행된다. 평가·학습은 EMR core 노드의 YARN 컨테이너에서 실행하고, Airflow scheduler 프로세스에서 모델을 직접 학습하지 않는다. teardown이 실행되지 못한 경우는 `emr_orphan_reaper`가 활성 스텝 없이 유휴 유예 시간이 지난 클러스터를 종료한다.

## 데이터 계층과 소비자

| 계층 | 저장소 | 내용 | 주요 소비자 |
| --- | --- | --- | --- |
| Bronze | S3/MinIO JSON | 외부 API 원문에 가까운 page/chunk | 재처리·감사 |
| Silver | S3/MinIO Parquet | 타입·결측·범위 검증을 통과한 source window | normalizer, inference, loader |
| Source manifest | S3/MinIO JSON | 선택된 Silver의 URI, SHA, logical time, revision | 모든 downstream |
| Derived/Archive | S3/MinIO Parquet | 보정 인구, nowcast, 일별 중복 제거 결과 | inference·학습 |
| Serving plan/output | S3/MinIO JSON·Parquet | 고정 입력 계약과 12시간 예측 | Gold finalize |
| Gold | PostgreSQL/PostGIS | 현재 station, stock, forecast, weather, event, urgency, route | FastAPI |

API와 화면은 S3를 직접 읽지 않는다. Gold의 freshness와 공통 기준시각 검사를 통과한 데이터만 제공하며, stale하거나 서로 다른 release의 projection을 섞지 않는다.

## 새 환경의 선행 조건

1. PostgreSQL/PostGIS schema 적용
2. `dispatch_center`, `weather_grid` 기준정보 bootstrap
3. 검증된 rental/return model serving release 등록
4. station master와 생활인구 등 선행 source 생성
5. realtime DAG 활성화

기준정보와 model release가 없으면 DAG 자체는 로드되더라도 serving plan 또는 downstream publication이 성공할 수 없다.

## 운영 확인 위치

- 스케줄·timeout: `airflow/config/schedules.py`
- source 그룹: `airflow/config/sources.py`
- 태스크 명령: `airflow/orchestration/`
- DAG 구조 회귀 테스트: `airflow/tests/`
