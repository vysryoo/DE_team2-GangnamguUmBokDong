# nowcaster

서울 열린데이터광장 생활인구 격자(250m) 데이터의 공표 지연으로 인한 실시간/단기 미래의 피처 공백을 해결하기 위해, 과거 시계열 패턴을 기반으로 D-3 ~ D+3 일자의 생활인구를 추정(Nowcasting)하고 아카이브를 관리하는 모듈입니다.

---

## 1. 배경 및 도입 목적

### 공공데이터 공표 지연
- 서울 열린데이터광장의 250m 격자 생활인구(`living_population_grid`) 데이터는 통계 정제 및 집계 작업으로 인해 **실제 발생일 기준 4일 뒤에 공개**됩니다.
  - *예시*: 수집기(Collector)가 8월 17일에 수집한 파일 내부의 실제 데이터는 8월 13일 실측치임.

### 실시간 추론 피처 공백 (Feature Gap)
- 따릉이 수요 예측 머신러닝 모델(`ml/inference`)은 **실시간(D-0) 및 단기 미래(D+1 ~ D+3)**의 대여소 주변 격자 생활인구(`pop_total`)를 핵심 피처로 사용합니다.
- 하지만 공공데이터의 공표 지연으로 인해 **최근 며칠과 미래 시점에는 실측 데이터가 아예 존재하지 않는 공백 문제**가 발생합니다.

### 해결 방안: nowcasting 파이프라인
1. 실측값이 아직 없는 최근 및 미래 구간(D-3 ~ D+3)에 대해 과거 동일 요일·휴일 패턴을 기반으로 **생활인구를 추정(`nowcast.parquet`)**합니다.
2. 모델 추론 파이프라인은 이 `nowcast.parquet`을 읽어 결측 없이 실시간 피처를 생성합니다.
3. 추후 서울시에서 해당 날짜의 실제 데이터가 수집되면, **아카이브(`archive/`)로 영구 승격**시키고 기존 임시 추정치는 자동으로 삭제하여 실측 정답으로 대체합니다.

---

## 2. 핵심 역할

1. **아카이브 백필 (`backfill-archive`, `bootstrap-lookback`)**:
   - 수집된 원본 CSV 파일들을 읽어 표준 물리 스키마(SPOP, 연령대별 M00~M70, F00~F70)의 Parquet으로 정규화하고 `archive/`에 일자별로 적재합니다.
   - 초기 운영에는 `bootstrap-lookback`으로 현재 추정 구간이 참조하는 1~4주 전 날짜만 선별 적재하고, 필요한 날짜가 하나라도 없으면 실패시킵니다.
2. **일일 추정 및 실측 승격 (`estimate`)**:
   - **실측 승격**: Collector와 같은 exact logical window의 source authority가 확인된 실측만 실제 발생일자(`biz_date`) 아카이브로 영구 보관하고, 기존에 생성해 두었던 해당 일자의 임시 추정치를 자동 삭제합니다. PARTIAL과 authority 게시 전 Silver는 actual로 승격하지 않습니다.
   - **손상 방어**: Exact authority가 단순히 없으면 승격만 생략하지만, revision·manifest·Silver checksum 또는 row count가 손상됐으면 task를 실패시킵니다.
   - **나우캐스팅 추정**: 기준일(D-0) 전후 일주일(D-3 ~ D+3) 중 실측이 없는 날짜들에 대해 250m 격자·시간대(00~23)별 인구를 추정하여 `silver/` 경로에 저장합니다.

---

## 3. 파일별 역할

| 파일 | 역할 |
|---|---|
| `main.py` | CLI 진입점 (`backfill-archive`, `estimate`) 및 파이프라인 제어 |
| `estimate_day.py` | 주차별 가중평균 연산, 다단계 폴백 판정, 결합 테이블 벡터화 생성 |
| `holiday.py` | 공휴일·주말 패턴 판별 및 1~4주 전 / 5~8주 전 후보 일자 매핑 |
| `storage.py` | S3 `archive/` 및 `silver/` 파티션 읽기/쓰기/삭제 I/O |
| `backfill.py` | 원본 CSV 정규화 및 YMD 기준 일자별 테이블 분리 |

---

## 4. 추정 알고리즘 및 단계별 폴백

250m 격자·시간대(`H_DNG_CD`, `CELL_ID`, `TT`)별로 다음 우선순위에 따라 인구를 추정합니다:

```text
[0단계] 1~4주 전 동일 요일/휴일 가중평균 (가중치: 0.4, 0.3, 0.2, 0.1)
   └── 결측 격자 발생 시 ⬇
[1차 폴백] 5~8주 전 확장 데이터 중 가장 최근 값 (extended_lookback_fallback)
   └── 결측 격자 발생 시 ⬇
[2차 폴백] 아카이브 내 전체 동일 패턴 일자들의 격자별 역사적 평균 (grid_historical_avg)
```

- **평일**: 과거 1~4주 전 같은 요일의 데이터를 사용합니다.
- **주말/공휴일**: 과거 1~4주 전의 주말 또는 공휴일 데이터를 매핑하여 특수일 패턴을 보존합니다.

---

## 5. S3 저장소 구조

```text
s3://<bucket>/
├── archive/living_population_grid/
│   └── dt=YYYY-MM-DD.parquet                  # [영구] 실제 발생일 기준 실측 데이터 (is_estimated=False)
│
└── silver/living_population_grid/
    ├── dt={수집실행일}/hh=HH/HHMM/sha256=....parquet # Collector immutable Silver
    └── dt={추정대상일}/hh=00/nowcast.parquet     # [임시] 모델 추론용 인구 추정치 (is_estimated=True)
```

> **Note**: `nowcast.parquet`은 추후 해당 날짜의 authoritative 실측이 확인되면
> 실측값으로 대체되면서 자동 삭제됩니다. PARTIAL만 있으면 삭제하지 않습니다.

---

## 6. 타임존(Timezone) 정책

- 서울시 생활인구 원본 데이터 및 비즈니스 기준 시각은 **KST (`Asia/Seoul`)**로 통일합니다.
- CLI에서 `--target-date` 미지정 시 KST 기준 당일 자정을 기준으로 D-3 ~ D+3 범위를 계산합니다.

---

## 7. 실행 방법

```bash
# 1. 의존성 설치
uv sync

# 2. 과거 원본 CSV 일괄 백필
uv run python main.py backfill-archive --csv-dir /path/to/csv/

# 3. 초기 운영: D-3~D+3 추정에 필요한 1~4주 전 데이터만 선별 백필
# 생활인구 API는 과거 날짜를 지정할 수 없으므로 공식 과거 CSV가 필요합니다.
uv run python main.py bootstrap-lookback \
  --csv-dir /path/to/csv/ \
  --target-date 2026-08-21

# 특정 하루의 1·2·3·4주 전 네 날짜만 검사/적재하려면:
uv run python main.py bootstrap-lookback \
  --csv-dir /path/to/csv/ \
  --target-date 2026-08-21 \
  --horizon-days 0

# 4. D-3 ~ D+3 나우캐스팅만 실행 (source window가 없으므로 실측 승격 안 함)
uv run python main.py estimate

# 특정 기준일의 나우캐스팅만 실행
uv run python main.py estimate --target-date 2026-08-17

# Collector와 같은 exact window의 authoritative 실측도 함께 승격
uv run python main.py estimate \
  --target-date 2026-08-17 \
  --source-window-start 2026-08-17T03:00:00+09:00

# 5. 단위 테스트 실행
uv run pytest
```
