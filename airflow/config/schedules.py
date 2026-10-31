"""Airflow 스케줄·재시도·timeout 공통 설정."""

from datetime import timedelta

TIMEZONE = "Asia/Seoul"

REALTIME_TICK_CRON = "*/5 * * * *"
# 비정상 실행을 유한하게 끝내는 상한. 그 사이 밀린 scheduled run은 시작 gate에서 버림
REALTIME_TICK_RUN_TIMEOUT = timedelta(minutes=15)
# 이보다 늦게 시작한 scheduled run은 다음 5분 격자를 점유하지 않도록 skip함
REALTIME_TICK_MAX_START_DELAY = timedelta(minutes=1)
# realtime tick의 정각 경계와 03:00 일별 배치를 피함
POI_MASTER_REFRESH_CRON = "4 2 * * *"
DAILY_CRON = "0 3 * * *"

# 03:00 tick의 prepare_serving_plan과 finalize_serving_release 사이에
# bike_station_master authority가 바뀌면 finalize가 실패함. 03:00 tick(약 3분 45초)이
# 끝나고 03:05 tick이 시작하기 전인 03:04로 비켜 둠. tick 소요가 5분에 가까워지면
# 다시 계산해야 함
STATION_MASTER_CRON = "4 3 * * *"

COMPACTION_CRON = "30 4 * * *"

# 대여와 반납 모델을 한 DAG에서 순차 실행해 EMR 클러스터가 동시에 두 개 뜨지 않게 함
MONTHLY_RETRAIN_CRON = "0 3 1 * *"

# monthly_retrain의 teardown이 실행되지 못한 경우를 위한 안전망. 활성 스텝이 없는
# 클러스터를 유휴 유예 시간 뒤에 알아채는 주기임
EMR_ORPHAN_REAPER_CRON = "*/15 * * * *"

# 새벽 배치(03:00대)가 끝나 전날 결과가 안정된 뒤 보고함
DAILY_COLLECTION_REPORT_CRON = "0 7 * * *"
HOURLY_COLLECTION_ALERT_CRON = "0 * * * *"

DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(seconds=30)
DEFAULT_EXECUTION_TIMEOUT = timedelta(seconds=240)

# living_population_grid는 서울 전체 250m 격자 x 24시간을 페이지네이션으로 받음
EXECUTION_TIMEOUT_OVERRIDES = {
    "living_population_grid": timedelta(seconds=1200),
}

NOWCASTING_EXECUTION_TIMEOUT = timedelta(seconds=600)
# baseline(nowcast.parquet) 크기가 늘 수 있어 5분 tick 안에서 여유를 둠
NORMALIZER_EXECUTION_TIMEOUT = timedelta(seconds=300)
# refresh는 외부 파일 2개를 받아 geometry까지 검증하고, resolve는 manifest 하나만 고름
POI_MASTER_REFRESH_EXECUTION_TIMEOUT = timedelta(seconds=300)
POI_MASTER_RESOLVE_EXECUTION_TIMEOUT = timedelta(seconds=30)
# 대여이력 D-6 correction으로 입력이 바뀐 날은 하루치 parquet(최대 288개)을 다시 읽음
COMPACTION_EXECUTION_TIMEOUT = timedelta(seconds=900)
INFERENCE_EXECUTION_TIMEOUT = timedelta(seconds=300)
# prepare_serving_plan은 AWS S3·RDS 왕복이 많아 재사용할 산출물이 없는 첫 실행에서
# 165초가 걸린 기록이 있음
SERVING_EXECUTION_TIMEOUT = timedelta(seconds=300)
URGENCY_EXECUTION_TIMEOUT = timedelta(seconds=180)
ROUTES_EXECUTION_TIMEOUT = timedelta(seconds=180)

MONTHLY_EVALUATION_TIMEOUT = timedelta(minutes=30)
# create_emr_cluster()의 WAITING 대기 상한(20분)보다 약간 길게 둠
MONTHLY_CLUSTER_CREATE_TIMEOUT = timedelta(minutes=25)
# EMR 스텝 2개를 순서대로 제출하고 스텝마다 최대 90분을 기다림
MONTHLY_FEATURE_REFRESH_TIMEOUT = timedelta(hours=4)
# Airflow 태스크가 timeout으로 죽어도 YARN 애플리케이션은 남으므로, 오래 걸리는
# 정상 학습을 죽이지 않도록 넉넉하게 둠
MONTHLY_TRAINING_TIMEOUT = timedelta(hours=120)
MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT = timedelta(hours=120)
# 대여와 반납을 순차 실행하므로 모델 하나 몫의 2배
MONTHLY_RETRAIN_TOTAL_TIMEOUT = timedelta(hours=240)

MAX_ACTIVE_RUNS = 1
CATCHUP = False
