# ML → collector 확인/요청 사항

> **보관 문서:** 2026-08-15~16 ML 연동 과정에서 작성한 질문·조사 기록이다. 해결 표시가 없는 항목도 현재 결함을 뜻하지 않는다. 현재 Silver 소비 계약은 `libs/ml_core/silver_schema.py`, Collector 생산 계약은 `collector/sources/*.yaml`과 source manifest 코드를 기준으로 한다.

이번 S3/MinIO 데이터 파이프라인 전환(`feature_engine`/`training`/`inference`가
로컬 `ml/data/processed_v2/*.parquet` 대신 collector가 Silver로 쌓는 S3 데이터를
직접 읽도록 바꾸는 작업)을 진행하며 `collector` 쪽 확정이 필요한 사항을 정리한다.
`collector` 모듈(어댑터/파이프라인/CLI)은 이번 작업에서 손대지 않았다 — 전부 이
문서로만 남긴다.

**2026-08-15 갱신**: collector 팀이 실제 수집 예시 데이터를 `ml/data/silver/`에
넣어줘서, 아래 1~3번(source_id·컬럼명·수집 주기)은 추측이 아니라 실제 데이터로
확인하고 코드도 그에 맞춰 고쳤다. 그 과정에서 예상 못 한 스키마 차이가 몇 개
더 나와 6~9번으로 추가했다 — 특히 8번(생활인구 스키마)은 코드 수정만으로는
완전히 해소되지 않는, 모델 feature 설계 자체에 관련된 사항이라 확인이 필요하다.

**2026-08-16 갱신**: `KMA_APIHUB_KEY`를 실제로 넣고 `weather_ultra_short_live`/
`weather_short_term_forecast` 두 소스를 직접(수동으로 `collector/main.py` 실행)
돌려서 raw 응답과 Silver 결과를 대조했다. 그 결과 6번(강수량 없음)이 실은 잘못된
결론이었음을 확인해 정정했고(11번), 코드 버그 하나(12번)와 두 소스 다 애초에
자동 수집 스케줄 자체가 없다는 것(13번, 영향이 가장 큼)을 새로 발견했다.

## 배경 — 왜 `dev/seed_s3_from_local.py`를 기준으로 개발했는가 (지금은 실제 예시로 대체됨)

`collector`의 실제 수집 어댑터(`collector/adapters/`, `collector/pipeline.py` 등)는
아직 docstring만 있고 실제로 동작하는 코드가 없다 — 소스 YAML config
(`collector/sources/*.yaml`)도 존재하지 않는다. 처음엔 `dev/seed_s3_from_local.py`
(`ml/data/processed_v2/*.parquet`를 Silver 파티션 구조로 변환해 MinIO에 넣어주는
로컬 시딩 스크립트)를 기준 스키마로 개발했는데, 이번에 collector 팀이 실제
예시 데이터(`ml/data/silver/`, 2026-08-15)를 줘서 비교해보니 소스 이름·컬럼명·
수집 주기가 상당히 달랐다. `libs/ml_core/silver_schema.py`는 이제 실제 예시
데이터를 기준으로 다시 맞췄다 — `dev/seed_s3_from_local.py`는 아직 옛 스키마로
로컬 MinIO에 데이터를 넣으므로, 이 스크립트로 시딩한 데이터로 테스트하면 다시
어긋난다(별도 후속 작업 필요, 8번 참고 아래 목록에는 없음 — 이 문서는 collector
쪽에 요청할 사항만 다룬다).

## 1. (해결됨) `rent_sta_id`/`rtn_sta_id`가 raw 숫자인지 `"ST-"` 접두 문자열인지

`DataSchema.md`의 `rental` 절(259~276행)은 이 두 컬럼에 대해 "유효한 station 식별
규칙을 만족하지 않음. **물리 FK 적용 여부는 과거 폐쇄 대여소 확인 후 결정**"이라고
적혀 있어 공식적으로는 미정이었다.

- **실제 예시 데이터로 확인**: `ml/data/silver/bike_rental_history/`의
  `RENT_STATION_ID`/`RETURN_STATION_ID`는 이미 `"ST-2565"`처럼 `bike_station_realtime`의
  `stationId`와 동일한 형식이었다(raw 숫자 대여소번호가 아님). 실제로 두 소스의
  ID가 서로 겹치는 것도 직접 확인했다(`ST-83`, `ST-280` 등 다수 공통).
- **ML 쪽 반영**: `inference/predict_single.py`의 `_resolve_rental_stations()`를
  station_no 크로스워크(`normalize_station_no()`) 없이 station_id로 직접 매칭하도록
  단순화했다. (배치 학습 쪽 `feature_engine/spark/build_targets.py`가 읽는
  과거 이력 CSV는 여전히 raw 숫자라 그쪽 `_normalize_station_no()`는 그대로 둔다 —
  다른 원본 포맷이라 서로 영향 없음.)
- **남은 확인 요청**: 이 형식이 앞으로도 계속 유지되는지(예를 들어 신규
  대여소 추가 시에도 같은 규칙인지)만 변경 있으면 알려달라.

## 2. (해결됨) `rental`의 실제 수집 주기 — 실제로는 5분(계획 문서와 일치)

`implementation-plan.md`의 소스 목록(26~33행)에는 "따릉이 대여이력 정보"가 **5분**
주기로 명시돼 있었는데, 예전 기준이던 `dev/seed_s3_from_local.py`는 1시간
단위로 시뮬레이션해서 실제로 어느 쪽이 맞는지 불확실했다.

- **실제 예시 데이터로 확인**: `ml/data/silver/bike_rental_history/dt=2026-08-15/hh=15/`
  아래 `1500/1505/1510/1515/1520/1525.parquet`처럼 정확히 5분 간격으로 쌓여
  있었다 — 계획 문서가 맞았다.
- **ML 쪽 반영**: `silver_schema.py`에 `rental_tick_keys()`(5분 tick 전용 키
  생성 함수)를 추가하고 `predict_single.py`가 이걸 쓰도록 바꿨다. 다만 5분
  간격이면 lag_168h(7일) 기준 lookback 하나에 키가 2천 개 가까이 돼서, 순차
  조회 대신 `s3_io.read_parquet_many()`(스레드 병렬 조회)로 바꿨다 — 실제
  운영에서 이 정도 지연이 감당 가능한 수준인지는 아직 실측 못 해봤다.

## 3. (해결됨, 단 예상과 다름) `weather_forecast`/`living_population_per_population_grid`의 정확한 `source_id`

계획 문서에는 이 두 소스의 정확한 `source_id` 문자열이 없어 dev 시딩 스크립트
기준(`weather_forecast`, `living_population_per_population_grid`)으로 추측했었다.

- **실제 예시 데이터로 확인한 결과, 둘 다 예상과 달랐고 구조도 더 복잡했다**:
  - 날씨는 소스가 **2개**였다: `weather_ultra_short_live`(초단기실황, 10분 간격,
    컬럼 `T1H`/`REH`/`WSD`/`RN1`/`PTY`)와 `weather_short_term_forecast`(단기예보,
    3시간 간격, 컬럼 `TMP`/`REH`/`WSD`/`POP`/`SKY`/`PTY`). ~~예보 쪽은 강수량(mm)이
    아니라 강수확률(%, `POP`)만 있어 `precip`과 단위가 안 맞는다~~ — **(2026-08-16
    정정) 이 결론은 틀렸다, raw엔 강수량(`PCP`)이 실제로 있고 YAML에만 안
    선언돼 있었다, 6번 참고.** 지금은 `weather_ultra_short_live`(관측치)만 쓰고
    예보 쪽은 안 쓴다.
  - 인구도 소스가 **2개**였고 둘 다 우리가 가정한 이름·구조와 달랐다:
    `population_realtime`(실시간 인구, `AREA_NM`/`AREA_CD`/`AREA_CONGEST_LVL` 등
    관광특구·주요장소 단위 혼잡도 데이터, `DataSchema.md`의 `main_spot_living_population`에
    해당)와 `living_population_grid`(격자 단위, `CELL_ID`/`SPOP`/`M00`~`M70`/`F00`~`F70`
    나이대x성별 인구). 격자 기반인 `living_population_grid`를 썼다 — 자세한 스키마
    차이는 8번 참고.
- **ML 쪽 반영**: `silver_schema.py`의 `WEATHER_SOURCE_ID`/`POPULATION_SOURCE_ID`와
  컬럼 매핑을 전부 실제 값으로 교체했다.

## 4. Silver 읽기 헬퍼 — `read_silver()` 대응 함수가 `collector/storage.py`에 없음

`collector/storage.py`는 `write_silver()`(및 bronze/quarantine 관련 쓰기 함수)는
있지만, 그 반대인 "silver 파티션을 읽어오는" 함수는 없다 — collector 입장에서는
쓰기 전용이라 당연할 수 있지만, ML은 정확히 같은 키 규칙으로 다시 읽어야 해서
`libs/ml_core/silver_schema.py` + `libs/ml_core/s3_io.py`에 **키 생성 규칙을
독립적으로 복제**해 자체 구현했다(`collector`를 import하지 않음 — 서로 다른
인스턴스에 독립 배포되는 모듈이라 의존 관계를 만들지 않는 게 이번 설계 원칙).

- **문제**: 키 규칙(`silver/{source_id}/dt=.../hh=.../HHMM.parquet`)이 두 곳에
  중복돼 있어, `collector` 쪽에서 이 규칙이 바뀌면 ML도 같이 고쳐야 하는데 그
  변경을 ML이 자동으로 알 방법이 없다.
- **요청**: `collector` 쪽에서 이 키 규칙을 바꿀 계획이 있다면 미리 공유해달라.
  장기적으로는 `collector`가 `read_silver(source_id, window_start) -> bytes | None`
  같은 공개 헬퍼를 제공하고 ML이 그걸 그대로 쓰는 편이 중복을 없앨 수 있는데,
  이건 두 모듈 간 의존성을 새로 만드는 결정이라 이번 phase 범위 밖으로 두고
  여기서는 필요성만 남긴다.

## 5. Silver 불변성 — backfill 재작성과 `revision`을 ML이 신경써야 하는가

> 현재: 지연 백필은 제거됐고 Silver는 content-addressed immutable 객체다. 아래는 당시 기록이다.

`implementation-plan.md`(739~754행)에 따르면 **silver는 불변이 아니다** — 백필로
같은 window가 덮어써지고 `manifest.revision`이 올라간다("하류는 `revision`을 보고
멱등 재처리한다"). 그런데 ML의 실시간 조회 함수들
(`_get_recent_weather()`/`_get_recent_bike_status()`/`_get_recent_population()`,
`inference/predict_single.py`)은 매번 그 시각의 silver parquet을 그냥 읽기만 하고
`_manifest/{source_id}/dt=.../hh=.../HHMM.json`의 `revision`/`stage`는 전혀 보지
않는다.

- **지금 괜찮은 이유(추정)**: 추론은 "방금 지난 시각"을 짧은 수명의 프로세스가
  한 번 읽고 끝나는 구조라, 그 시점에 이미 `stage=completed`로 확정된 최신
  silver를 읽을 가능성이 높다 — 학습(`feature_engine`, 1년치 배치)과 달리
  "이미 백필로 여러 번 바뀐 과거 데이터를 다시 집계"하는 경로가 아니다.
- **확인하고 싶은 것**: 그래도 실시간 추론이 매우 최근(예: 방금 지난 5분) 시각을
  읽을 때, 그 window가 아직 `PARTIAL`이거나 재시도 중이어서 완결되지 않은
  상태로 남아있을 가능성이 있는지, 있다면 ML이 `manifest.stage`/`status`를
  확인하고 넘어가야 하는지 확인 요청 — 만약 필요하다면 `_get_recent_*` 함수들에
  manifest 확인 스텝을 추가해야 한다(현재는 안 함).

## 6. (2026-08-16 정정) `weather_short_term_forecast`에 강수량(mm)이 없다고 했던 결론이 틀렸다

~~지금 `_get_recent_weather()`는 사실 "예보"가 아니라 "가장 최근 관측값"을 쓴다
(`weather_ultra_short_live`, 10분 간격 관측치). horizon이 커져(예: 6시간 뒤)
target_ts가 미래로 멀어지면 원래는 진짜 예보(`weather_short_term_forecast`,
3시간 간격)를 써야 더 정확할 텐데, 그 소스엔 강수량이 없고 강수확률(`POP`, %)만
있다.~~ (아래에서 정정)

**뭐가 틀렸었나**: `KMA_APIHUB_KEY`를 실제로 넣고 `getVilageFcst`(단기예보)를
직접 호출해보니, raw 응답에 `PCP`(1시간 강수량, mm)와 `SNO`(적설)가 실제로
있었다 — `weather_short_term_forecast.yaml`의 `columns:`에 선언이 안 돼 있어서
Silver로 넘어올 때 조용히 버려지고 있었을 뿐이다.

**왜 잘못 판단했었나**: 위 결론은 collector 팀이 준 예시 Silver 데이터
(컬럼이 `TMP/REH/WSD/POP/SKY/PTY`뿐)와 실제 구현된 YAML만 보고 낸 것이었다 —
"지금 실제로 뭐가 나오나"만 확인하고 "원래 뭘 만들기로 계획했었나"까지 거슬러
올라가지 않았다. `DataSchema.md`의 `weather_forecast` 절(53~64행)에는 애초에
`precipitation_amount`가 "일반" 컬럼으로 계획돼 있었고 "원천 특수 표현은
normalize 정책 필요"라는 문구까지 있다 — 이게 정확히 `PCP`의 실제 형식
(`"강수없음"`, `"1mm 미만"`, `"10.0mm"`, `"0"`이 섞여 나옴)과 들어맞는다. 즉
계획 문서는 이 필드를 정확히 예견하고 있었는데, 실제 YAML 구현 단계에서
빠진 것으로 보인다.

- **지금 ML이 하는 일**: (정정 전과 동일) horizon과 무관하게 항상
  `weather_ultra_short_live`의 "가장 최근 값"만 쓴다 — 이건 그대로 유지.
- **요청**:
  1. `weather_short_term_forecast.yaml`에 `PCP`(강수량)를 `columns:`로 추가해달라
     — 다만 raw 값이 순수 숫자가 아니라 `"강수없음"`/`"1mm 미만"`/`"10.0mm"`처럼
     텍스트가 섞여 있어(13번 참고) 그냥 `types: [float]`로는 캐스팅이 깨진다,
     전용 normalize 정책이 필요해 보인다(`DataSchema.md`가 이미 예견한 부분).
  2. 추가되면 ML도 `_get_recent_weather()`가 horizon에 따라 진짜 예보
     (`weather_short_term_forecast`)를 쓰도록 개선할 예정.

## 7. `bike_station_realtime` 예시 파일이 항상 1,000행 — 페이지네이션으로 잘린 건 아닌지

실제 예시 데이터(`ml/data/silver/bike_station_realtime/`)의 모든 파일이 예외 없이
정확히 1,000행이었다(실제 전체 대여소는 약 2,977개). `implementation-plan.md`의
YAML 예시(210행 근처)에 `adapter_params.page_size: 1000`이 있어, 혹시 이게 예시
데이터 생성 시 페이지 1개만 반영된 거라면 실제 운영에서는 Silver가 전체 정류소를
포함한 값일 거라 추정하지만, 예시만으로는 확신할 수 없었다.

- **요청**: 실제 운영 Silver `bike_station_realtime`이 매 window마다 전체 정류소
  (~2,977개)를 담는 게 맞는지 확인 요청. 아니라면(예: 진짜로 페이지당 별도 파일로
  쪼개진다면) `_get_recent_bike_status()`가 한 파일만 읽는 지금 방식을 여러
  파일을 합치는 방식으로 고쳐야 한다.

## 8. `living_population_grid` — 국적별 breakdown이 없고, 스냅샷 자체가 며칠 지연됨

이번에 발견한 것 중 가장 영향이 큰 차이다. ML 모델은 생활인구를 `pop_resd`
(내국인)/`pop_long_foreign`(장기체류외국인)/`pop_short_foreign`(단기체류외국인)/
`pop_total` 4개 컬럼으로 쓰도록 학습돼 있는데(`build_population_profile.py` 등),
실제 `living_population_grid` 예시 데이터엔 이 구분이 아예 없고 나이대(10살
단위)x성별(`M00`~`M70`/`F00`~`F70`) 인구와 총합(`SPOP`)만 있다.

- **지금 ML이 하는 일**: `_get_recent_population()`에서 `SPOP`을 `pop_total`로
  쓰고, `pop_resd`는 `pop_total`과 같다고 근사(전부 내국인으로 간주)한 뒤
  `pop_long_foreign`/`pop_short_foreign`은 0으로 채운다 — 4개 컬럼 자리는
  채워지지만 실제 국적별 구성은 반영되지 않는, 임시방편에 가까운 근사다.
- **추가로 발견한 것**: 이 소스는 다른 실시간 소스와 달리 **하루 1개 파일**만
  쌓이고(그 안에서 `YMD`+`TT` 컬럼으로 24시간을 구분), 그 파일이 실제로 담고
  있는 날짜(`YMD`)가 수집일(경로의 `dt=`)보다 **4일 정도 지연**돼 있었다(2026-08-15에
  수집된 파일의 내용이 2026-08-11자 데이터). 그래서 ML은 "정확히 그 날짜"를
  맞추려 하지 않고 "가장 최근 수집분에서 같은 시간대(`TT`)"만 골라 쓰도록 짰다.
- **요청**:
  1. 국적별(내국인/장기체류외국인/단기체류외국인) 구분이 가능한 다른 필드나
     소스가 따로 있는지, 없다면 이 4-컬럼 feature 설계를 이 데이터에 맞게
     바꿔야 하는지는 ML 모델링 쪽 판단이 필요해 이 문서만으로는 결론 못 냄 —
     별도로 상의 필요.
  2. "며칠 지연"이 항상 4일 고정인지, 변동이 있는지(운영 안정화 이후 짧아질
     수 있는지) 확인 요청 — 지금 fallback(`lookback_days=7`)이 이 지연을
     충분히 커버하는지 판단에 필요하다.

## 9. 이번에 처음 본, 아직 안 쓰는 소스 2개

예시 데이터에 `population_realtime`(관광특구·주요장소 혼잡도, `DataSchema.md`의
`main_spot_living_population`에 해당)과 `cultural_event`(문화행사, `event`/
`event_spot`에 해당)가 있었는데, 둘 다 지금 ML 코드 어디서도 쓰지 않는다(요청
사항 없음, 참고용 기록) — 나중에 혼잡도·행사 feature를 추가하게 되면 이 두
소스를 쓰면 된다.

## 10. `bike_rental_history`의 각 tick 파일이 델타(incremental)인지 누적(cumulative)인지

예시 데이터로 직접 비교해보니 `dt=2026-08-15/hh=14/1445.parquet`과
`.../1450.parquet`이 **완전히 동일**했고(행 수·내용 전부 일치), `hh=19/1900.parquet`은
행이 1개만 더 있었다 — 즉 같은 날 여러 tick 파일이 거의 그대로 반복되는
모양이었다. 다만 이 예시 데이터 자체가 데모용으로 정적 스냅샷을 여러 시각에
복제해 넣은 것일 수 있어(실제로 시간마다 새 트립이 반영된 게 아니라), 이것만
보고 실제 운영에서도 tick마다 "그날 트립 전체를 다시 통째로" 담는(누적) 방식인지,
"그 5분 동안 새로 생긴 트립만" 담는(델타) 방식인지는 확정할 수 없었다.

- **지금 ML이 하는 일**: 안전하게 양쪽 다 대응하도록
  `_fetch_recent_rental_trips()`에서 `(bike_id, start_dt)` 기준으로 중복 제거를
  추가했다 — 델타면 애초에 중복이 없어 영향 없고, 누적이면 여러 tick을 이어붙일
  때 생기는 중복을 걸러낸다.
- **요청**: 실제로 어느 방식인지 확인해달라 — 델타가 맞다면 위 중복 제거는
  안전장치로 그냥 둬도 되고, 누적이라면 오히려 "가장 최신 tick 파일 하나만
  읽어도 그날 전체 트립을 다 얻는다"는 뜻이라 여러 tick을 안 읽고 최신 파일
  하나만 읽는 쪽으로 최적화할 수 있다(지금처럼 5분 tick 수백~수천 개를 병렬로
  긁을 필요가 없어짐).

## 11. (2026-08-16 신규) 날씨 두 소스 다 자동 수집 스케줄이 없다

`airflow/dags/`를 전부 확인해봤는데 `weather_ultra_short_live`(10분 주기)/
`weather_short_term_forecast`(3시간 주기)를 실제로 자동으로 도는 스케줄에
올려주는 DAG가 하나도 없다.

- `realtime_collection.py`(5분 주기)의 `airflow/config/sources.py`
  `REALTIME_SOURCES`엔 `bike_station_realtime`/`population_realtime`만 있고
  날씨는 없다.
- (당시 기록. 현재 `collector_backfill` DAG는 없다.) `collector_backfill`(`airflow/dags/backfill.py`)에 두 날씨 source_id가
  들어있긴 한데 `schedule=None`(수동 트리거 전용)이고, "이미 도는 1차 수집의
  누락 조각만 채우는" 백필 용도라 1차 수집 자체가 없으면 채울 게 없다.
- 실제로 지금은 사람이 직접 `collector/main.py --source weather_... --window-start ...`를
  실행해야만 데이터가 들어간다(이번 검증도 그렇게 했다).

- **요청**: 두 소스를 각자 YAML의 `schedule.interval`(10분/3시간)에 맞는
  스케줄로 자동 수집하는 DAG(들)를 만들어달라. 기존 5분 DAG에 억지로
  끼워넣지 말고 별도 스케줄 그룹으로(`sources.py` 모듈 docstring이 이미
  이 방식을 권장하고 있다).

## 12. (2026-08-16 신규) `kma_apihub` 어댑터에 페이지네이션이 없어 단기예보가 매번 잘림

`collector/adapters/kma_apihub.py`의 `fetch()`가 `numOfRows=1000&pageNo=1`을
하드코딩하고, 응답의 `totalCount`를 아예 안 읽는다 — 2페이지 이상을 가져오는
로직이 없다.

- 실제로 돌려본 결과 `weather_short_term_forecast`(`getVilageFcst`)는
  **격자 25개 전부** `totalCount=1052`인데 1000개만 받아왔다 — 격자당 52건씩
  유실, 이 엔드포인트 특성상(3일치×14개 카테고리) 거의 항상 재현되는 문제로
  보인다.
- 잘리는 건 항상 그 격자의 가장 먼 미래 예보 시각 — 그 시각의 `REH`(습도) 등
  일부 카테고리가 통째로 빠진다.
- **아무 경고도 안 뜬다**: `REH`가 `required: true`가 아니라
  `optional_missing: keep_null`로 조용히 null 처리되고, manifest는
  `kept=2075 dropped=0 completeness=1.000`으로 찍힌다 — raw와 직접 대조하지
  않는 한 이 유실을 알 방법이 없다.
- `weather_ultra_short_live`(`getUltraSrtNcst`)은 지금은 격자당 8건뿐이라
  1000 밑이라 안 잘리는데, 코드가 맞아서가 아니라 이 엔드포인트 페이로드가
  작아서 우연히 안전한 것뿐이다 — 같은 코드를 쓰는 한 이 소스도 똑같이
  취약하다.

- **요청**: `totalCount`를 읽어서 필요하면 `pageNo`를 늘려가며 전체를 받아오는
  루프를 추가해달라.

## 13. (2026-08-16 신규) YAML `columns:`에 없어서 조용히 버려지는 raw 필드들 + description 오류

raw 응답과 YAML `columns:` 선언을 대조한 결과:

- `weather_ultra_short_live.yaml`의 `description`은 "초단기 실황·**예보**"라고
  돼 있지만 실제로 부르는 건 `getUltraSrtNcst`(실황)뿐이다. `getUltraSrtFcst`
  (진짜 초단기예보)는 코드 전체에서 `collector/tests/test_kma_apihub.py`의
  예시 테스트에만 등장하고, 실제 소스로 설정된 적이 없다 — description이
  실제 동작과 안 맞는다.
- 두 소스 다 raw엔 있는데 YAML `columns:`엔 없어서 조용히 버려지는 필드가
  있다: `weather_ultra_short_live`은 `UUU`/`VEC`/`VVV`(바람 성분·풍향),
  `weather_short_term_forecast`는 `PCP`(강수량, 6번 참고)/`SNO`(적설).
- `PCP`의 실제 raw 값 종류(참고용): `"강수없음"`, `"1mm 미만"`, `"1.0mm"`~
  `"17.0mm"`(숫자+"mm" 텍스트), 그리고 `"0"`(단위 없는 숫자 문자열)까지 섞여
  나온다 — 하나의 정책으로 캐스팅하기 까다로운 형태다.

- **요청**: `description`을 실제 동작에 맞게 수정. `PCP`/`UUU`/`VEC`/`VVV`/
  `SNO`를 feature로 쓸 계획이 있는지 확인하고, 쓴다면(특히 `PCP`) YAML에
  추가 + 전용 normalize 정책을 정의해달라.
