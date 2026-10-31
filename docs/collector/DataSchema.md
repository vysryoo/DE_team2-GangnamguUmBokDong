# Collector 데이터 계약

> 상태: 현재 코드 기준<br>
> 코드 확인일: 2026-08-25

이 문서는 Collector가 외부 API 응답을 어떤 schema와 저장 계층으로 생산하는지 설명한다. Column별 type, required, range, enum과 정책의 최종 기준은 `collector/sources/*.yaml`이다. Gold/PostGIS schema는 `docs/gold/`에서 별도로 관리한다.

## 계약의 구성

Source 하나의 계약은 다음 항목으로 구성된다.

| 항목 | 정의 위치 | 역할 |
| --- | --- | --- |
| Source ID·adapter | source YAML | 외부 API와 응답 해석 방식 선택 |
| `columns` | source YAML | Silver에 허용할 column, type, required, range, enum |
| `policies` | source YAML | missing·type error·outlier 처리 |
| `quality` | source YAML | 수집 누락과 폐기 행 허용 비율 |
| Pydantic schema | `collector/config/schema.py` | 알 수 없는 설정과 잘못된 조합을 로딩 단계에서 거부 |
| Validation engine | `collector/validation/` | raw 값을 실제 Silver 값과 quarantine 판정으로 변환 |
| Storage layout | `collector/storage.py` | Bronze, Silver, manifest, Archive key 생성 |

Source YAML에 선언되지 않은 raw field는 Silver에서 제거된다. 새 field가 필요하면 YAML과 downstream 소비 계약을 함께 변경해야 한다.

## 저장 계층

### Bronze

```text
bronze/hot/<source_id>/dt=YYYY-MM-DD/hh=HH/HHMM/revision=NNNNNNNNNN/part=<part_key>.json.gz
```

- API page, 기상 격자 또는 POI 같은 fetch part별 gzip JSON이다.
- 응답 원문에 가깝게 보존하며 validation 이전의 재개 지점이다.
- part가 도착할 때마다 즉시 쓴다.
- 일반 Airflow 재실행은 실패 종류와 실행 날짜에 관계없이 manifest가 가리키는 Hot
  Bronze revision을 재사용한다.
- 최초 수집과 `--force`는 기존 원본을 지우지 않고 새 immutable revision을 만든다.
- Hot object를 쓰기 전에 `_cold_pending/<source_id>/dt=YYYY-MM-DD/` 아래 window
  revision별 immutable marker를 만든다. part가 많아도 같은 revision은 marker 하나를
  공유한다. Cold worker는 marker key의 날짜를 먼저 판별하고 안정화 기간 6일이 지난
  pending 날짜만 실제 Hot 조회 대상으로 처리한다. compaction 전후 Hot signature가
  달라지면 marker를 유지·복원하고 실패해 다음 실행에서 다시 처리한다. Hot 없이 남은
  marker는 stale 건수로 결과에 명시한 뒤 정리한다.
- Silver Archive 대상 여부와 관계없이 모든 Collector source의 검증된 날짜 revision은
  `bronze/cold/<source_id>/dt=YYYY-MM-DD/sha256=...parquet`에 원본 gzip bytes 그대로
  장기 보관한다.
- Cold object의 checksum·row count readback이 끝나면 marker를 제거한다. 기존 Cold
  bundle이 있는 날짜에 늦은 revision이 들어오면 기존 rows와 새 Hot을 합쳐 최신 Cold
  manifest가 모든 revision을 계속 가리키게 한다.
- Hot GC는 Cold manifest 목록만 작은 recovery queue로 조회한다. 30일이 지난 날짜에서
  검증된 Cold inventory에 정확히 포함된 Hot key만 삭제하고
  `_hot_gc_manifest/<source_id>/dt=YYYY-MM-DD.json`에 준비·완료 상태를 남긴다. Cold에
  포함되지 않은 late revision이 하나라도 있으면 삭제하지 않고 실패한다.
- 모든 source에서 Cold Bronze가 검증되면 최신 authority가 아닌 Silver를 객체 생성 후
  30일간 보존한 다음 삭제한다. 일 단위 Archive 대상 source는 Archive와 현재 authority
  signature 일치도 추가로 검증하고
  `_silver_gc_manifest/<source_id>/dt=YYYY-MM-DD.json`에 삭제 key와 복구 근거를 남긴다.
- 과거 Source Snapshot manifest는 immutable 감사 기록으로 유지되지만 GC된 Silver
  URI는 직접 읽을 수 없다. 과거 데이터를 다시 만들 때는 Cold Bronze를 입력으로 쓴다.

### Silver

```text
silver/<source_id>/dt=YYYY-MM-DD/hh=HH/HHMM/sha256=<digest>.parquet
```

- source YAML에 선언된 column만 가진다.
- 선언 type으로 casting되고 policy 적용을 마친 row다.
- content-addressed immutable object이며 같은 bytes는 같은 key를 사용한다.
- `_row_status`가 `ok` 또는 `repaired`로 각 row의 검증 결과를 나타낸다.

Downstream은 Silver prefix에서 최신 파일처럼 보이는 객체를 직접 고르지 않는다. Source snapshot manifest의 exact URI와 SHA를 사용한다.

### Quarantine

```text
quarantine/<source_id>/dt=YYYY-MM-DD/hh=HH/HHMM.jsonl
```

폐기된 row가 있을 때만 생성된다. 원래 raw field에 다음 진단 field를 붙인다.

| Field | 의미 |
| --- | --- |
| `_row_index` | window batch 안에서의 원래 row 위치 |
| `_issues` | 문제가 발생한 column, 종류, raw value와 적용 action |

Quality gate가 실패해 Silver를 게시하지 않더라도 폐기 원인을 분석할 수 있도록 quarantine은 남길 수 있다.

### 실행 Manifest

```text
_manifest/<source_id>/dt=YYYY-MM-DD/hh=HH/HHMM.json
```

한 번의 실행이 어디까지 진행됐는지 나타내는 mutable 진단 기록이다.

| Field | 의미 |
| --- | --- |
| `status` | `running`, `succeeded`, `partial`, `failed`, `empty`, `skipped` |
| `stage` | `bronze_written`, `validated`, `completed` |
| `failure_reason` | `fetch_error`, `storage_error`, `quality_gate`, `config_error` |
| `attempt` | 해당 window의 진단 실행 횟수 |
| `revision` | authoritative content correction 번호 |
| `counts` | expected, fetched, kept, repaired, dropped row 수 |
| `missing` | 누락 part 또는 row와 계산 기준 |
| `column_issues` | column별 missing, type error, outlier 집계 |
| `policy_actions` | 적용된 policy action 집계 |
| `artifacts` | Bronze prefix·parts, Silver, quarantine 위치 |

### Source Authority Manifest

```text
source_snapshot_manifest/<source_id>/dt=<UTC-date>/hh=<UTC-hour>/
logical=<UTC-timestamp>/revision=<10-digit>.json
```

검증된 source snapshot의 immutable authority다. 논리 시각, config version, exact Silver URI·SHA, row count와 완료 part를 고정한다.

- 최초 authoritative 결과는 revision 0이다.
- 같은 content의 재실행은 같은 revision을 재사용한다.
- content 또는 상태가 달라진 correction만 revision이 증가한다.
- revision은 0부터 빈틈없이 이어져야 한다.

진단 manifest는 “실행이 어떻게 됐는가”, authority manifest는 “downstream이 어떤 content를 읽어야 하는가”에 답한다.

### Archive

```text
archive/<source_id>/dt=YYYY-MM-DD.parquet
_archive_manifest/<source_id>/dt=YYYY-MM-DD.json
```

날짜별 Silver를 고정 schema로 묶은 학습·재현 계층이다. Source column 뒤에 다음 meta column이 추가된다.

| Column | 의미 |
| --- | --- |
| `_row_status` | Silver validation 결과 |
| `_window_start` | row가 관측된 KST window |
| `_source_kind` | 운영 compaction은 `collector`, 초기 적재는 `bootstrap` |

## 검증 상태

### Column issue

| Issue | 조건 |
| --- | --- |
| `MISSING` | 값이 `None`, 빈 문자열 또는 마스킹된 값 |
| `TYPE_ERROR` | 선언된 type을 순서대로 시도했지만 모두 실패 |
| `OUTLIER` | casting 성공 후 range 또는 enum 위반 |

현재 지원 caster는 `str`, `int`, `float`, `bool`, `precip`, `snow`, `masked_float`다.

### Row status

| 상태 | 의미 |
| --- | --- |
| `ok` | issue나 값 교정 없이 유지 |
| `repaired` | null 교체, 기본값, 범위 clipping 같은 policy가 값을 변경 |
| dropped | Silver에 넣지 않고 quarantine으로 이동 |

`drop_row` 또는 row policy로 폐기된 row는 Silver에 row status로 남지 않는다.

### 기본 Policy

모든 현재 source는 같은 네 가지 기본값을 사용한다.

| 조건 | Policy |
| --- | --- |
| required missing | `drop_row` |
| required outlier/type error | `drop_row` |
| optional missing | `keep_null` |
| optional outlier/type error | `set_null` |

Column별 override나 row policy가 선언된 경우 YAML 설정이 우선한다.

## Source별 Silver column

아래는 column 집합을 빠르게 찾기 위한 요약이다. 정확한 type과 constraint는 각 YAML을 확인한다.

### Bike

| Source | Column |
| --- | --- |
| `bike_station_master` | `RNTLS_ID`, `ADDR1`, `ADDR2`, `LAT`, `LOT` |
| `bike_station_realtime` | `stationId`, `stationName`, `rackTotCnt`, `parkingBikeTotCnt`, `shared`, `stationLatitude`, `stationLongitude` |
| `bike_rental_history` | `BIKE_ID`, `RENT_DT`, `RENT_ID`, `RENT_NM`, `RENT_HOLD`, `RTN_DT`, `RTN_ID`, `RTN_NM`, `RTN_HOLD`, `USE_MIN`, `USE_DST`, `USR_CLS_CD`, `SEX_CD`, `BIRTH_YEAR`, `RENT_STATION_ID`, `RETURN_STATION_ID`, `BIKE_SE_CD` |

### Population

| Source | Column |
| --- | --- |
| `living_population_grid` | `YMD`, `TT`, `H_DNG_CD`, `CELL_ID`, `SPOP`, 성별·연령별 `M00..M70`, `F00..F70` |
| `population_realtime` | `AREA_*`, 성별 인구 비율, `FCST_YN`, 1~12번 `FCST_n_TIME`, `CONGEST_LVL`, `PPLTN_MIN`, `PPLTN_MAX` |

`living_population_grid`의 `*`는 개인정보 보호 마스킹이며 `masked_float` caster가 `MISSING`으로 처리한다. `population_realtime`의 forecast 번호는 target 시간 순서일 뿐 “n시간 후”를 뜻하지 않으므로 `FCST_n_TIME`을 사용한다.

### Weather

| Source | Identity·time | 주요 값 |
| --- | --- | --- |
| `weather_ultra_short_live` | `nx`, `ny`, `baseDate`, `baseTime` | `T1H`, `REH`, `WSD`, `RN1`, `PTY`, `UUU`, `VVV`, `VEC` |
| `weather_ultra_short_forecast` | 위 + `fcstDate`, `fcstTime` | `T1H`, `RN1`, `SKY`, `PTY`, `REH`, `WSD`, `LGT`, `POP`, `UUU`, `VVV`, `VEC` |
| `weather_short_term_forecast` | 위 + `fcstDate`, `fcstTime` | `TMP`, `REH`, `WSD`, `POP`, `PCP`, `SKY`, `PTY`, `UUU`, `VVV`, `VEC`, `WAV`, `SNO` |

초단기실황 `RN1`은 숫자 mm이고, 예보의 `RN1`·`PCP`는 “강수없음”, “1mm 미만”, 범위 표현 등이 섞여 있어 `precip` caster로 mm 값으로 정규화한다.

### Event

| Source | Column |
| --- | --- |
| `cultural_event` | `TITLE`, `CODENAME`, `GUNAME`, `PLACE`, `STRTDATE`, `END_DATE`, `IS_FREE`, `LOT`, `LAT` |
| `performance_event` | `SCH_SEQ`, `TITLE`, `SDATE`, `EDATE`, 이용 조건·요금·URL, 등록·수정일, 분류 code와 title |

두 행사 source만 `allow_empty=true`다. 행사가 없는 window는 실패가 아니라 정상 `EMPTY`가 될 수 있다.

## Schema 변경 절차

1. 외부 API raw 응답에서 field와 값 범위를 확인한다.
2. 해당 source YAML의 `columns`, 정책과 quality 값을 수정한다.
3. adapter가 구조 변환을 해야 하는 field인지 판단한다.
4. downstream의 `libs/ml_core/silver_schema.py`, normalizer, loader 사용 여부를 검색한다.
5. source config·adapter·pipeline 테스트를 실행한다.
6. 기존 Archive와 schema가 달라지는 경우 재생성 또는 호환 전략을 정한다.

```bash
uv run --project collector --frozen pytest \
  collector/tests/test_source_configs.py \
  collector/tests/test_api_contracts.py \
  collector/tests/test_pipeline.py \
  collector/tests/test_source_snapshot_manifest.py \
  collector/tests/test_compaction.py -q
```

## 코드 기준 위치

- Source schema: `collector/sources/*.yaml`
- Config model: `collector/config/schema.py`
- Validation: `collector/validation/engine.py`, `collector/validation/policies.py`
- 저장 key와 I/O: `collector/storage.py`
- 실행 manifest와 source authority: `collector/manifest.py`
- Archive schema: `collector/compaction.py`
