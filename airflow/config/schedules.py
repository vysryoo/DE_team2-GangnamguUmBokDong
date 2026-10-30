"""Airflow 스케줄·재시도 관련 공통 설정. DAG 파일이 cron 문자열이나 retry/timeout
값을 직접 반복해서 적지 않도록 한 곳에 모은다.
"""

from datetime import timedelta

TIMEZONE = "Asia/Seoul"

# realtime tick은 5분마다 돈다. 한때는 날씨 필요 여부(분·시 나머지 연산으로 고정된
# 경계)에 따라 이 격자를 4개 cron으로 쪼개서, 필요한 시각에만 날씨 collector가 같은
# DAG에 존재하게 하는 방식을 썼다 — 그런데 그러면 서로 다른 DAG가 max_active_runs를
# 각자 따로 관리해서, 한 tick이 늦어져 여러 DAG의 실행 시각이 겹치면(2026-08-22
# 실측: `realtime_5min` 단일 DAG였던 시절에도 tick 하나가 530초까지 늘어나 CPU가
# 부족해진 적이 있다) 약한 인스턴스에서 CPU 경합으로 60초 타임아웃(retries=0)인 날씨
# collector가 죽을 위험이 있었다. 지금은 다시 DAG 하나로 합치고, 날씨 collector마다
# `orchestration.collector_task.build_weather_freshness_gate_task()`로 "마지막
# 성공 수집 이후 충분히 지났는지"를 실제 시각 기준으로 매 tick마다 직접 물어서
# 스킵 여부를 정한다(초단기 10분/단기 3시간 — `dags/realtime_tick.py` 참고). 이러면
# 3시간짜리 수집이 실패해도 다음 5분 tick에서 바로 재시도되어, cron 경계에 걸려
# 최대 3시간을 기다리던 예전보다 복구가 훨씬 빠르다.
REALTIME_TICK_CRON = "*/5 * * * *"
# 정상 tick을 너무 빡빡하게 죽이지 않되 비정상 실행은 유한하게 끝내도록 넉넉한
# 상한을 둔다. 그 사이 밀린 scheduled run은 DAG 시작 gate에서 버린다.
REALTIME_TICK_RUN_TIMEOUT = timedelta(minutes=15)
# scheduler 자체의 짧은 지연은 허용하되, 이전 tick 때문에 1분 넘게 늦게 시작한
# scheduled run은 남은 5분 격자를 다시 점유하지 않고 복구용 skip으로 소비한다.
REALTIME_TICK_MAX_START_DELAY = timedelta(minutes=1)
# 서울시 POI 목록·영역 파일의 변경 여부를 매일 확인한다. 5분 realtime tick의
# 정각 경계와 03:00 일별 배치를 피해 가벼운 기준정보 갱신을 독립 실행한다.
POI_MASTER_REFRESH_CRON = "4 2 * * *"
# living_population_grid는 그날 데이터를 하루 1개 파일로 발행한다 — 실제 발행 시각을
# 확인해 필요하면 조정한다.
DAILY_CRON = "0 3 * * *"

# station_master는 DAILY_CRON(03:00)을 쓰면 안 된다. realtime tick이 5분마다
# 돌아서 03:00도 그 tick 중 하나이므로 두 DAG이 동시에 시작하고, station_master가
# 약 88초 뒤 bike_station_master authority를
# 게시한다. 그 시각이 realtime tick의
# prepare_serving_plan(고정)과 finalize_serving_release(재검증) 사이에 들어가면
# "locked station master authority가 바뀌었습니다"로 그 tick의 Gold 게시가 실패한다
# (2026-08-22 실측: 06:15 tick, prepare 06:15:46 종료 -> master 게시 06:18:01 ->
# finalize 06:18:23 실패). 이건 버그가 아니라 의도된 방어라 코드로 우회하지 않고
# 시각을 비켜놓는다.
#
# 03:04를 고른 이유: tick 하나가 약 3분 45초라 03:00 tick은 03:03:45에 끝나고
# 03:05 tick은 아직 시작 전이다. 03:02는 03:00 tick의 finalize 창(03:02:20~03:03:00)과
# 여전히 겹칠 수 있어 부족하다. **tick 소요가 다시 5분에 가까워지면 이 여유가
# 사라지므로 그때 다시 계산해야 한다.**
STATION_MASTER_CRON = "4 3 * * *"

# D-6 대여이력 재수집 후 같은 날짜의 silver를 archive로 묶는 배치.
COMPACTION_CRON = "30 4 * * *"

# 매달 1일 03:00 KST — 대여(Rental) 챔피언 점검·재학습을 먼저 끝내고, 그 EMR
# 클러스터가 완전히 종료된 뒤에 반납(Return)을 이어서 시작한다(단일 DAG,
# `monthly_retrain.py`). 두 모델이 각자 최대 8노드 EMR 클러스터를 띄울 수 있어
# 예전처럼 두 DAG를 따로 스케줄하면(대여 03:00·반납 06:00) 재시도나 지연으로
# 겹칠 때 클러스터 2개가 동시에 뜰 수 있었다 — 한 DAG 안에서 순서를 강제해
# 항상 하나만 뜨게 한다.
MONTHLY_RETRAIN_CRON = "0 3 1 * *"

# monthly_retrain_* DAG의 자체 정리 태스크(terminate_cluster, is_teardown=True)는
# 그 DAG 실행의 스케줄러 처리 자체가 계속될 때만 보장된다 — 운영자가 DAG Run을
# 수동으로 통째로 "Mark Failed" 처리하는 경우까지 커버하려면 그 실행 그래프와
# 무관하게 실제 AWS 상태를 직접 확인하는 별도 안전망이 필요하다
# (`emr_orphan_reaper.py`). 이 reaper는 나이가 아니라 EMR 스텝 활동으로 판단하므로
# (활성 스텝이 있으면 절대 안 건드림 — 큰 데이터로 학습이 오래 걸려도 안전),
# 15분 주기는 그저 "유휴 유예 시간(15분)이 지난 클러스터를 얼마나 빨리 알아채는가"만
# 결정한다 — 재학습 사이클 자체의 정상 소요 시간과는 무관하다.
EMR_ORPHAN_REAPER_CRON = "*/15 * * * *"

# 전날(00:00~24:00 KST) 전체 소스 수집 통계를 Slack에 보고한다. 새벽 배치(03:00대)가
# 끝나 하루치 결과가 안정된 뒤인 07:00으로 잡는다.
DAILY_COLLECTION_REPORT_CRON = "0 7 * * *"
# 수집 주기가 1시간 이내인 소스만 매시 정각 지난 1시간 통계를 확인해 이상이 있을
# 때만 Slack에 경고한다.
HOURLY_COLLECTION_ALERT_CRON = "0 * * * *"

DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(seconds=30)
DEFAULT_EXECUTION_TIMEOUT = timedelta(seconds=240)

# living_population_grid는 서울 전체 250m 격자 x 24시간을 페이지네이션으로 받아오므로
# 다른 실시간 소스보다 훨씬 오래 걸린다(로컬 테스트 실측 기준).
EXECUTION_TIMEOUT_OVERRIDES = {
    "living_population_grid": timedelta(seconds=1200),
}

NOWCASTING_EXECUTION_TIMEOUT = timedelta(seconds=600)
# normalizer는 tick 1회에 현재 + 향후 12시간, 최대 13개 시각을 보정한다. 가장 무거운
# 교차 계산은 시각과 무관해 1회만 돌므로 시각 수에 비례하지 않는다. 로컬 실측
# (2026-08-19 21:40 tick, 격자 8,564개 x 13시각, baseline 2개 날짜 = nowcast 27MB):
# 10.7초. 5분 tick에 여유가 크지만 baseline 크기가 늘 수 있어 상한은 300초로 둔다.
NORMALIZER_EXECUTION_TIMEOUT = timedelta(seconds=300)
# POI master refresh는 외부 파일 2개를 내려받아 geometry까지 검증한다. resolver는
# 이미 게시된 작은 manifest 하나를 선택하는 작업이라 짧게 실패시킨다.
POI_MASTER_REFRESH_EXECUTION_TIMEOUT = timedelta(seconds=300)
POI_MASTER_RESOLVE_EXECUTION_TIMEOUT = timedelta(seconds=30)
# 변경이 없는 날짜는 LIST 한 번으로 끝나지만, 대여이력 D-6 correction 등으로 Silver
# 입력이 바뀐 날은 하루치 parquet을 전부 다시 읽는다. bike_rental_history 기준
# 288개가 상한이다.
COMPACTION_EXECUTION_TIMEOUT = timedelta(seconds=900)
# 실측 데이터 없음(placeholder) — 로컬에서 --all-stations 1회 실행 시간을 재본 뒤 조정.
INFERENCE_EXECUTION_TIMEOUT = timedelta(seconds=300)
# AWS 실측: prepare_serving_plan(serving_cli.py prepare)이 station/stock/weather
# projection과 여러 S3 put/readback을 순차로 하는데, 로컬 MinIO가 아니라 실제 AWS
# S3·RDS 네트워크 왕복이 되니 콜드 스타트(재사용할 이전 산출물이 없는 최초 실행)
# 기준 165초가 걸렸다(2026-08-21, CPU는 21초뿐이라 대부분 네트워크 I/O 대기).
# 기존 120초는 이 실측 전의 placeholder였다 — 여유를 두고 300초로 올린다.
SERVING_EXECUTION_TIMEOUT = timedelta(seconds=300)
# 실측 데이터 없음(placeholder) — S3 tick 5~6개 + 예측 결과 1개만 읽는 순수 계산이라
# 추론보다는 가볍게 잡았다. 로컬에서 1회 실행 시간을 재본 뒤 조정.
URGENCY_EXECUTION_TIMEOUT = timedelta(seconds=180)
# 실측 데이터 없음(placeholder) — urgency 계산을 다시 하고 dispatched 넷팅을 위한
# RDS 조회 하나가 추가되는 정도라 URGENCY_EXECUTION_TIMEOUT과 비슷하게 잡았다.
ROUTES_EXECUTION_TIMEOUT = timedelta(seconds=180)

# 월별 ML 재학습 타임아웃. MONTHLY_TRAINING_TIMEOUT/MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT은
# 원래 각각 3시간/6시간이었으나, 이 프로젝트는 1년치 데이터를 48GB RAM 단일
# 머신으로 학습하는 데 실측 24시간이 걸린 이력이 있어(2026-08) 너무 타이트했다
# — 짧은 타임아웃이 발동하면 Airflow 태스크만 죽고 그 밑에서 실제로 돌던 YARN
# distributed-shell 애플리케이션은 안 죽은 채(클라이언트 프로세스와 완전히
# 독립적으로 RM/NM이 관리) 백그라운드에 orphan으로 남는다 — 정상적으로 오래
# 걸리는 학습을 죽이지 않는 쪽을 우선해 넉넉하게 잡는다.
MONTHLY_EVALUATION_TIMEOUT = timedelta(minutes=30)
# create_emr_cluster()의 내부 WAITING 대기(기본 1200초=20분, 그 안에 못 뜨면
# 자기 종료)보다 살짝 여유 있게 잡은 Airflow 레벨 백스톱 — 클러스터 생성만
# 별도 태스크로 분리해 teardown의 setup 성공 조건이 "평가 성공 여부"에 오염되지
# 않게 한다(2026-08, PR 리뷰 지적: 평가가 멈추면 클러스터 생성이 성공했어도
# teardown이 스킵되던 문제).
MONTHLY_CLUSTER_CREATE_TIMEOUT = timedelta(minutes=25)
EMR_FEATURE_MART_TIMEOUT = timedelta(minutes=90)
# refresh_feature_mart(evaluate 직전, 챔피언 프로필로 feature mart 증분 갱신)의
# Airflow 레벨 execution_timeout. run_pipeline.py + build_multi_horizon_features.py
# 두 EMR 스텝을 순서대로 제출하는데, aws_infra_task.submit_emr_step()의 스텝별
# 기본 대기 한도가 이미 90분(EMR_FEATURE_MART_TIMEOUT과 같은 값)이라 두 스텝이면
# 최대 180분까지 걸릴 수 있다 — EMR_FEATURE_MART_TIMEOUT을 그대로 쓰면 두 번째
# 스텝이 끝나기 전에 Airflow가 먼저 태스크를 죽인다. 180분보다 넉넉하게 잡는다.
MONTHLY_FEATURE_REFRESH_TIMEOUT = timedelta(hours=4)
MONTHLY_TRAINING_TIMEOUT = timedelta(hours=120)
# 모델 하나(대여 또는 반납)의 재학습 루프 태스크 자체에 거는 Airflow execution_timeout.
MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT = timedelta(hours=120)
# 대여 → 반납을 한 DAG 안에서 순차 실행하므로(동시에 두 EMR 클러스터가 뜨는 걸
# 막기 위함), 전체 DAG Run의 dagrun_timeout은 두 모델 몫을 합친 240시간으로 잡는다
# (평가 30분씩은 각 모델의 재학습 루프 타임아웃 안에 흡수될 만큼 작아 더하지 않음).
MONTHLY_RETRAIN_TOTAL_TIMEOUT = timedelta(hours=240)

MAX_ACTIVE_RUNS = 1
CATCHUP = False
