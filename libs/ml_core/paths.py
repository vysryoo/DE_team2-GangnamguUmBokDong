"""`feature_engine`/`training`/`inference` 세 인스턴스가 공통으로 참조하는 S3 키.

로컬 개발도 항상 S3(MinIO, `dev/start_minio.sh`)를 거친다 — 더 이상 로컬
파일시스템 폴백이 없다. 여기 정의된 값은 전부 **S3 버킷 안의 상대 키**(문자열)
이지 로컬 경로가 아니다 — 실제 버킷 이름은 `S3_BUCKET` 환경변수(`ml_core.
s3_io.bucket_name()`가 읽음)로 정해지고, 이 파일은 그 버킷 "안"에서 어떤 키를
쓸지만 정의한다.

키를 조합할 때 `pathlib.Path`나 `"/".join(...)`을 쓰지 않는다 — f-string으로
그때그때 직접 만든다(`collector/storage.py`와 같은 컨벤션).

**주의**: `feature_engine/spark/config.py`는 이 파일을 import하지 않고
같은 이름의 상수를 독립적으로 다시 정의한다(EMR에 그 패키지만 올릴 때
`ml_core`의 무거운 의존성 없이도 동작하게 하려는 기존 설계) — 두 파일이
가리키는 실제 키는 **반드시 같아야 하므로**, 한쪽을 고치면 다른 쪽도 같이
고칠 것.
"""

import os
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

from core import s3 as s3_io

from . import common_config, profile_contract

# 로컬 subprocess로 형제 패키지의 venv 실행파일을 찾을 때만 쓰는 로컬 경로
# 개념(예: training/scripts/monthly_retrain_check.py가 feature_engine/training의
# .venv/bin/python을 실행) — 데이터 저장 위치와는 무관, 코드 자체는 여전히
# 로컬(또는 EMR/EC2) 프로세스로 실행되므로 이 개념만 남겨둔다.
#
# `Path.cwd()`가 아니라 이 파일 위치 기준으로 고정한다 — cwd 기준이면 스케줄러
# (cron/systemd 등)가 "ml/"로 cd하지 않고 절대경로로 이 스크립트를 실행할 때
# ML_ROOT가 엉뚱한 디렉터리가 되고, 그 아래 "feature_engine/.venv/bin/python"을
# 못 찾아 RuntimeError가 난다 — 실행 위치와 무관하게 항상 이 저장소의 "ml/"을
# 가리켜야 한다. 이 파일은 "libs/ml_core/paths.py"에 있으므로 parents[2]가
# 저장소 루트다.
ML_ROOT = Path(__file__).resolve().parents[2] / "ml"

FEATURE_PARAM_COMBO_ID = os.environ.get(
    "FEATURE_PARAM_COMBO_ID",
    f"w{common_config.ROLLING_WINDOW_MINUTES}_e{common_config.ROLLING_EMBARGO_MINUTES}_t{common_config.ROLLING_TICK_MINUTES}",
)
# custom FEATURE_PARAM_COMBO_ID는 자동 w/e/t 격리를 우회하므로 서로 다른 base
# 조합에 재사용하면 안 된다. 최종 학습 anchor는 아래 별도 namespace로 격리한다.
# feature_engine의 2차 정제 산출물 위치 — dev/S3_DATA_CATALOG.md의
# `processed/features/` prefix를 따른다. 파라미터 조합마다 결과가 달라지므로
# 조합 ID를 키에 넣어 서로 안 덮어쓰게 한다.
_FEATURE_ENGINEERING_OUTPUT_PREFIX = os.environ.get("FEATURE_ENGINEERING_OUTPUT_PREFIX", "processed/features")
FEATURE_ENGINEERING_OUTPUT_DIR = f"{_FEATURE_ENGINEERING_OUTPUT_PREFIX}/{FEATURE_PARAM_COMBO_ID}"

TRAIN_YEAR = 2025
TRAIN_MONTHS = [f"{TRAIN_YEAR % 100:02d}{m:02d}" for m in range(1, 13)]

# 대여이력 원본(트립 단위) parquet 디렉터리 — feature_engine(타겟/rolling
# 계산)이 여전히 이걸 읽는다고 가정(이번 phase는 1차정제 이전 상태 유지, 2단계
# 에서 raw Silver `rental` 조회로 대체 예정). `inference`는 이번 phase부터
# `silver_schema`를 통해 Silver `rental`을 직접 읽으므로 이 상수를 안 쓴다.
RENTAL_PARQUET_DIR = os.environ.get("RENTAL_PARQUET_DIR", "parquet")

# training이 만들고(학습), inference가 읽는(서빙) 모델 아티팩트 — dev/
# S3_DATA_CATALOG.md에 정의된 `models/` prefix를 그대로 쓴다. 학습은 항상 아래
# 아카이브 prefix에 쓴다 — 이 prefix에 booster/JSON을 직접 쓰는 코드 경로는
# 없다. 대신 `champion/{model_name}.json` 포인터가 "지금 챔피언은 어느
# archive_prefix인지"를 가리키고(아래 참고), 실제 파일은 항상 archive에만 있다.
MODELS_PREFIX = os.environ.get("MODELS_PREFIX", "models")

# 학습한 모든 모델(챔피언이 됐는지와 무관하게)을 보존하는 아카이브 — 날짜/프로필별로
# 나뉘어 있어 "언제 어떤 프로필로 학습했는지"를 그대로 찾을 수 있다.
MODELS_ARCHIVE_PREFIX = os.environ.get("MODELS_ARCHIVE_PREFIX", f"{MODELS_PREFIX}/archive")

# YARN distributed-shell 워커들이 서로의 host:port를 찾는 barrier 등록 위치
# (`training/scripts/yarn_worker_bootstrap.py`, ADR-0007 참고). 학습 아티팩트가
# 아니라 한 학습 시도 동안만 쓰고 버리는 임시 조율(coordination) 데이터라
# MODELS_ARCHIVE_PREFIX와 분리한다.
TRAINING_RUNS_PREFIX = os.environ.get("TRAINING_RUNS_PREFIX", f"{MODELS_PREFIX}/training-runs")


def training_run_worker_key(run_id: str, worker_id: str) -> str:
    """YARN distributed-shell 워커 하나가 자기 host:port를 등록하는 barrier 파일 키.

    분산 학습(`LGB_NUM_MACHINES>1`) 워커들은 서로의 주소를 미리 알 수 없다 — 각자
    이 키에 자기 정보를 쓰고, `run_id` 하나가 공유하는 워커 등록이
    `LGB_NUM_MACHINES`개 다 모일 때까지 폴링한다(`yarn_worker_bootstrap.py` 참고).
    `run_id`는 학습 시도 하나(모델 하나, 프로필 하나) 전체가 공유하는 값이어야
    하고, `worker_id`는 그 시도 안에서 워커마다 달라야 한다(YARN `CONTAINER_ID` 등).

    args:
        run_id: 이 학습 시도 전체가 공유하는 식별자
        worker_id: 워커(컨테이너)마다 고유한 식별자
    returns:
        str: "{TRAINING_RUNS_PREFIX}/{run_id}/workers/{worker_id}.json"
    """
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/workers/{worker_id}.json"


def eval_shard_key(run_id: str, model_name: str, worker_id: str) -> str:
    """분산 평가(`yarn_eval_worker.py`) 워커 하나가 자기 부분합을 쓰는 키.

    학습 barrier(`training_run_worker_key()`)와 개념은 같지만(같은 `run_id`가
    공유하는 워커들이 다 쓸 때까지 오케스트레이터가 기다렸다가 합산), 평가는
    워커끼리 서로의 주소를 알 필요가 없어(embarrassingly parallel — 소켓 통신
    없음) 페이로드가 host/port가 아니라 부분합(n_rows, sum_*)이다. `training-runs`
    prefix는 공유하되 `eval-shards/{model_name}/` 아래로 분리해 학습 barrier
    파일과 네임스페이스가 섞이지 않게 한다.

    args:
        run_id: 이 평가 시도 전체가 공유하는 식별자
        model_name: "rental" 또는 "return"
        worker_id: 워커(컨테이너)마다 고유한 식별자
    returns:
        str: "{TRAINING_RUNS_PREFIX}/{run_id}/eval-shards/{model_name}/{worker_id}.json"
    """
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/eval-shards/{model_name}/{worker_id}.json"


def archive_models_prefix(date: str, profile_name: str) -> str:
    """한 번의 학습 시도(날짜 + 프로필 조합)가 쓸 아카이브 prefix를 만든다.

    이 prefix를 `train_common.train_target(..., models_prefix=...)`에 그대로
    넘기면, `model_key`/`model_json_key`가 만드는 파일명(예: "rental_poisson.txt")
    자체는 챔피언 경로와 완전히 동일하게 유지되고 위치만 여기로 바뀐다 — 나중에
    챔피언으로 승격할 때는 이 경로를 `write_champion_pointer()`로 가리키기만
    하면 된다(파일 복사 없음, 아래 참고).

    args:
        date: "YYYY-MM-DD" — 학습을 실행한 날짜
        profile_name: 이 학습에 쓴 프로필 이름(common_config.PROFILE_NAME)
    returns:
        str: "{MODELS_ARCHIVE_PREFIX}/dt={date}/{profile_name}"
    """
    return f"{MODELS_ARCHIVE_PREFIX}/dt={date}/{profile_name}"


def model_key(model_name: str, suffix: str, models_prefix: str | None = None) -> str:
    """모델 아티팩트 하나의 S3 키를 만든다 (예: model_key("rental", "poisson", archive_prefix) -> "{archive_prefix}/rental_poisson.txt").

    args:
        model_name: "rental" 또는 "return"
        suffix: "poisson"/"q10"/"q50"/"q90"
        models_prefix: None이면 정적 MODELS_PREFIX를 그대로 쓴다 — 이제 챔피언
            아티팩트는 여기 없으므로(위 MODELS_PREFIX 설명 참고), "지금 챔피언"을
            읽고 싶으면 호출부가 먼저 `read_champion_prefix(model_name)`으로
            archive_prefix를 구해 명시적으로 넘겨야 한다. 하이퍼파라미터 스윕 등
            실험 실행도 마찬가지로 자신만의 prefix(예: "models/experiments/{run_id}")를
            명시적으로 넘긴다.
    """
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_{suffix}.txt"


def model_json_key(model_name: str, kind: str, models_prefix: str | None = None) -> str:
    """모델 부속 JSON(conformal_correction/station_categories/metrics)의 S3 키를 만든다.

    args: model_key() 참고 — models_prefix=None의 의미가 동일하다.
    """
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_{kind}.json"


def champion_pointer_key(model_name: str) -> str:
    """model_name(rental/return)의 챔피언 포인터 위치."""
    return f"{MODELS_PREFIX}/champion/{model_name}.json"


SERVING_RELEASE_PREFIX = f"{MODELS_PREFIX}/serving-release"
"""Pair serving release의 immutable manifest와 mutable pointer 공통 prefix다."""

MODEL_SNAPSHOT_PREFIX = f"{MODELS_PREFIX}/snapshots"
"""Content-addressed model snapshot artifact와 manifest의 공통 prefix다."""


def model_snapshot_artifact_key(
    model_name: str,
    role: str,
    byte_sha256: str,
    extension: str,
) -> str:
    """Model snapshot이 소유하는 content-addressed artifact 키를 만든다."""
    return (
        f"{MODEL_SNAPSHOT_PREFIX}/{model_name}/artifacts/{role}/"
        f"sha256={byte_sha256}.{extension}"
    )


def model_snapshot_manifest_key(model_name: str, byte_sha256: str) -> str:
    """Model snapshot manifest의 content-addressed S3 상대 키를 만든다."""
    return (
        f"{MODEL_SNAPSHOT_PREFIX}/{model_name}/manifests/"
        f"sha256={byte_sha256}.json"
    )


def model_support_id_set_key(model_name: str, byte_sha256: str) -> str:
    """Model support Gold ID set의 content-addressed S3 상대 키를 만든다."""
    return (
        f"{MODEL_SNAPSHOT_PREFIX}/{model_name}/support/"
        f"sha256={byte_sha256}.json"
    )


def serving_release_manifest_key(byte_sha256: str) -> str:
    """Content-addressed serving release manifest의 S3 상대 키를 만든다."""
    return f"{SERVING_RELEASE_PREFIX}/manifests/sha256={byte_sha256}.json"


def serving_release_artifact_key(
    role: str,
    byte_sha256: str,
    extension: str,
) -> str:
    """Serving release가 직접 소유하는 content-addressed artifact 키를 만든다."""
    return (
        f"{SERVING_RELEASE_PREFIX}/artifacts/{role}/"
        f"sha256={byte_sha256}.{extension}"
    )


def serving_release_pointer_key() -> str:
    """현재 pair serving release를 가리키는 단일 mutable pointer 키를 반환한다."""
    return f"{SERVING_RELEASE_PREFIX}/current.json"


PROFILES_PREFIX = profile_contract.PROFILES_PREFIX
profile_path = profile_contract.profile_path


@cache
def read_champion_prefix(model_name: str) -> str:
    """지금 챔피언이 가리키는 archive_prefix를 읽는다.

    **왜 파일 복사가 아니라 포인터인가**: 예전엔 승격할 때 archive의 파일 8개
    (booster 4개 + station_categories/conformal_correction/metrics/profile)를
    챔피언 prefix로 하나씩 복사했다 — S3는 여러 키에 걸친 트랜잭션을 지원하지
    않으므로, 복사가 절반쯤 끝난 순간 inference가 실행되면 booster는 새
    버전인데 station_categories는 옛 버전인 식으로 섞인 모델을 읽을 수 있었다
    (station_id 카테고리 코드가 학습 시점의 정렬 순서에 의존하므로, 이렇게
    섞이면 성능 저하가 아니라 엉뚱한 정류소에 대한 예측이 조용히 나간다).
    archive 자체는 학습이 끝난 뒤 다시 안 바뀌는 immutable 산출물이므로, "지금
    챔피언이 어느 archive_prefix인지"를 가리키는 포인터 객체 하나만 원자적으로
    바꾸면 파일을 복사할 필요가 아예 없다 — 단일 키에 대한 PUT은 원자적이라,
    어느 시점에 이 함수를 부르든 완전히 예전 archive_prefix 또는 완전히 새
    archive_prefix 둘 중 하나만 보이고 중간 상태는 존재하지 않는다.

    **`@cache`가 필요한 이유(프로세스 "내" 일관성 — 프로세스 "간"이 아님)**: 이
    함수는 여러 모듈(`ml_core.scoring`의 `load_boosters()`/`load_conformal_correction()`,
    `ml_core.model_contract`의 `load_station_dtype()`)이 같은 import로 나눠
    부른다. 캐시가 없으면 한 프로세스 안에서도 이 셋을 부르는 시점 사이에
    승격이 끼어들 경우 서로 다른 archive_prefix를 읽어버릴 수 있다 — 이 함수를
    `@cache`로 감싸면 이 프로세스가 사는 동안 최초 호출 시점의 값 하나로
    고정되어, 그 프로세스 안에서 booster/correction/station_categories가
    항상 같은 archive_prefix에서 나온다. 다른 프로세스(다음 5분 주기 inference 등)가
    승격 이후 새 값을 보는 것은 정상이고 문제없다 — 막아야 하는 건 프로세스
    "하나"가 자기 안에서 신/구 버전을 섞어 쓰는 경우뿐이다.

    **"학습해봤더니 구려서 같은 프로세스 안에서 재학습→재승격을 반복"하는
    코드는 어떻게 되나(2026-08)**: `training.promotion.promote_challenger()`가
    승격할 때 이 캐시와 `ml_core.scoring`의 `load_boosters()`/
    `load_conformal_correction()` 캐시까지 셋을 한꺼번에 비운다 — 그래야 재승격
    직후 다음 채점부터는 새 archive를 보면서도, 셋 중 일부만 새 값을 보고
    나머지는 옛 값에 머무는 불일치가 안 생긴다(`promote_challenger()` docstring
    참고). 이 함수를 `write_champion_pointer()`로 직접 부르기만 하고
    `promote_challenger()`를 안 거치면(테스트 외) 이 캐시가 안 비워진다 —
    `read_champion_prefix.cache_clear()`를 직접 불러야 한다.

    args:
        model_name: "rental" 또는 "return"
    returns:
        str: 챔피언이 가리키는 archive_prefix
    raises:
        FileNotFoundError: 아직 한 번도 승격된 적 없음(포인터 자체가 없음)
    """
    pointer = s3_io.read_json(champion_pointer_key(model_name))
    if pointer is None:
        raise FileNotFoundError(f"챔피언 포인터 없음: {champion_pointer_key(model_name)} (아직 승격된 적 없음)")
    return pointer["archive_prefix"]


def write_champion_pointer(model_name: str, archive_prefix: str) -> dict:
    """model_name의 챔피언이 archive_prefix를 가리키도록 원자적으로 전환한다.

    더 이상 archive 파일을 챔피언 자리로 복사하지 않는다(`read_champion_prefix()`
    docstring 참고) — 포인터 하나만 바꾸면 승격이 끝난다.

    **이 함수 자신은 캐시를 안 비운다 — 일부러다.** `read_champion_prefix()`뿐
    아니라 `ml_core.scoring.load_boosters()`/`load_conformal_correction()`도
    같은 archive_prefix를 각자 따로 캐시하는데, 이 함수는 `paths.py`에 있어서
    `scoring.py`(순환 import 방지로 이 모듈을 모름)의 캐시까지는 못 비운다. 여기서
    `read_champion_prefix`만 비우면 셋 중 하나만 새 값을 보고 나머지 둘은 옛
    값을 유지하는 **불일치**가 생긴다(실측 확인됨, 2026-08) — 그게 바로 이
    캐시 설계가 막으려던 문제 그 자체다. 그래서 세 캐시를 전부 아는 유일한
    지점인 `training.promotion.promote_challenger()`(이 함수의 유일한 실제
    호출부)가 셋을 한꺼번에 비운다 — 이 함수를 단독으로(테스트 외에) 호출하는
    코드는 없어야 한다.

    args:
        model_name: "rental" 또는 "return"
        archive_prefix: 새로 챔피언이 될 archive_prefix(`archive_models_prefix()`가 만든 값)
    returns:
        dict: 실제로 기록된 포인터 내용
    """
    record = {"archive_prefix": archive_prefix, "promoted_at": datetime.now(UTC).isoformat()}
    s3_io.write_json(champion_pointer_key(model_name), record)
    return record


# --- feature_engine 1차 정제 산출물 (이번 phase는 "이미 어딘가 있다"고
# 가정 — 2단계에서 raw Silver로부터 직접 만드는 로직으로 대체될 예정. prefix
# 이름은 로컬 개발 때 쓰던 것을 그대로 유지해 마이그레이션 부담을 줄인다) ---
PROCESSED_V2_PREFIX = os.environ.get("PROCESSED_V2_PREFIX", "processed_v2")
STATION_MASTER_PARQUET = f"{PROCESSED_V2_PREFIX}/station_master.parquet"
TARGETS_PARQUET = f"{PROCESSED_V2_PREFIX}/targets_2025.parquet"
RETURN_TARGETS_PARQUET = f"{PROCESSED_V2_PREFIX}/return_targets_2025.parquet"
STATION_STATUS_PARQUET = f"{PROCESSED_V2_PREFIX}/station_status_2025.parquet"
WEATHER_PARQUET = f"{PROCESSED_V2_PREFIX}/weather_2025.parquet"
POPULATION_PARQUET = f"{PROCESSED_V2_PREFIX}/population_2025.parquet"

# --- feature_engine 2차 정제 산출물(Spark) — training/inference가 그대로
# 읽는다. `feature_engine/spark/config.py`의 같은 이름 상수와 반드시
# 같은 값이어야 한다(위 모듈 docstring 참고) ---
MERGED_TABLE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hour_merged_2025.parquet"
FEATURES_TABLE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hour_features_2025.parquet"
# FEATURES_TABLE_PARQUET의 각 행(T0, 모델 tick)을 horizon=1..HORIZON_COUNT만큼
# self-join한 학습 테이블. base feature/profile은 w/e/t 조합에서 재사용하고 최종
# multi-horizon 결과만 실제 학습 anchor별 namespace로 격리한다.
_TRAINING_ANCHOR_OUTPUT_DIR = (
    f"{FEATURE_ENGINEERING_OUTPUT_DIR}/training_anchor_a{common_config.TRAIN_ANCHOR_TICK_MINUTES}"
)
RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET = (
    f"{_TRAINING_ANCHOR_OUTPUT_DIR}/station_hour_features_multihorizon_rental_2025.parquet"
)
RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET = (
    f"{_TRAINING_ANCHOR_OUTPUT_DIR}/station_hour_features_multihorizon_return_2025.parquet"
)
ROLLING_RENTAL_FEATURES_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/rolling_rental_features_2025.parquet"

# --- inference가 만드는 fallback 프로필 ---
# station profile은 위 MERGED_TABLE_PARQUET의 모델 tick별 통계이므로 feature
# 파라미터 조합(예: t5/t20)과 반드시 같이 격리한다. 공용 processed_v2 키 하나를 쓰면
# A/B 빌드가 서로 덮어써 모델 grid와 profile grid가 조용히 갈라진다.
STATION_HOURLY_PROFILE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hourly_profile.parquet"
# population profile은 원본 시간별 population에서 만들며 모델 grid와 무관하다.
POPULATION_HOURLY_PROFILE_PARQUET = f"{PROCESSED_V2_PREFIX}/population_hourly_profile.parquet"
