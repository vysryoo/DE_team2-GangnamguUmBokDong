# collector 모듈 구현 계획

> **보관 문서:** Collector 구현 전 작성한 상세 계획이다. 현재 API·manifest·품질 gate 계약은 `collector/` 코드와 `collector/sources/*.yaml`을 기준으로 한다.
>
> 지연 백필(`--backfill`, `_retry_queue` 마커, source 설정의 `backfill` 키, 백필 DAG)은 제거됐다. 현재 재실행은 manifest가 가리키는 Hot Bronze를 재사용하고, `--force`만 새 revision에 전체를 다시 받는다. Silver는 content-addressed immutable 객체다. 아래 백필 서술은 당시 계획으로만 읽는다. manifest의 `backfill_status`는 기존 manifest 호환을 위해 남아 있으며 항상 `null`이다.

> 설계 결정의 배경과 근거는 [ADR 0001](../adr/0001-collector-module-design.md)을 참고한다. 이 문서는 그 결정을 어떻게 구현할지를 다룬다.

## 목차

1. [배경과 목표](#1-배경과-목표)
2. [설계 결정 요약](#2-설계-결정-요약)
3. [디렉토리 구조](#3-디렉토리-구조)
4. [config 스키마](#4-config-스키마)
5. [검증 엔진](#5-검증-엔진)
6. [상태 어휘와 manifest](#6-상태-어휘와-manifest)
7. [재개 로직](#7-재개-로직)
8. [부분 실패와 백필](#8-부분-실패와-백필)
9. [로깅](#9-로깅)
10. [실행 인터페이스](#10-실행-인터페이스)
11. [구현 순서](#11-구현-순서)
12. [검증 방법](#12-검증-방법)

---

## 1. 배경과 목표

`collector/`는 아키텍처의 **Extract 단계**를 담당한다. 서로 다른 7개 소스를 각기 다른 주기로 수집한다.

| 소스 | 제공처 | 주기 |
| --- | --- | --- |
| 따릉이 실시간 대여정보 | 서울 열린데이터광장 | 5분 |
| 따릉이 대여이력 정보 | 서울 열린데이터광장 | 5분 |
| 서울 실시간 인구 데이터 | 서울 열린데이터광장 | 5분 |
| 기상청 초단기 실황·예보 | 기상청 API 허브 | 10분 |
| 기상청 단기예보 | 기상청 API 허브 | 3시간 |
| 서울시 문화행사·공연행사 | 서울 열린데이터광장 | 1일 |
| 서울 생활인구(250m) | 서울 열린데이터광장 | 1일 |

**설계 목표: 소스가 늘어도 공통 코드는 바뀌지 않는다.** 상태 어휘·재개·검증·manifest·로깅은 소스와 무관하게 완전히 동일하고, 소스마다 달라지는 것은 **fetch 어댑터 + YAML config** 뿐이다.

수집 흐름은 한 프로세스 안에서 완결된다. **bronze는 응답이 도착할 때마다 조각으로 즉시
저장하고, 검증과 silver는 window 전체를 모아 배치로 처리한다.**

```
       ┌─ 호출 1 → 응답 도착 ─→ bronze/…/1410/part=000.json.gz   (즉시 저장)
fetch ─┼─ 호출 2 → 응답 도착 ─→ bronze/…/1410/part=001.json.gz
       └─ 호출 N → 응답 도착 ─→ bronze/…/1410/part=00N.json.gz
              │
              └─ 조각은 메모리에도 계속 쌓인다 → window 전체가 모인 상태
                       │
                       └→ normalize() → rows → 검증·정책 적용 ─┬→ silver/     (Parquet 1개)
                                                              └→ quarantine/ (JSONL)

                                                   → _manifest/ (실행 상태·집계·아티팩트 경로)
```

조각을 저장한 뒤에도 원본을 메모리에서 놓지 않으므로 **피크 메모리는 줄지 않는다.**
이 방식이 노리는 것은 메모리가 아니라 ① 중간에 죽어도 받은 응답이 S3에 남는 것,
② 실패 원인을 응답 원본으로 확인할 수 있는 것, ③ 큰 응답의 업로드 실패 범위를 조각
하나로 좁히는 것이다. 근거는 [ADR 0003](../adr/0003-bronze-streaming-and-scaling-boundaries.md).

**조각 하나가 실패해도 window 전체를 버리지 않는다.** 실패분은 라운드로 재시도하고, 그래도
남으면 누락을 기록한 채 성공분으로 silver까지 진행한다. 근거는 [ADR 0004](../adr/0004-partial-fetch-and-backfill.md),
상세는 [8절](#8-부분-실패와-백필).

---

## 2. 설계 결정 요약

| 항목 | 결정 |
| --- | --- |
| bronze | API 응답 **원본 그대로** 무손실 저장. 정책이 바뀌어도 재수집 없이 재처리 가능 |
| bronze 저장 시점 | **응답이 도착할 때마다 조각으로 즉시 저장.** 마지막에 몰아 쓰지 않는다 |
| bronze 완결 판정 | `stage`는 **실행 진행도**만 뜻한다. 조각이 다 모였는지는 `completeness` · `missing`이 표현한다 |
| 조각 실패 재시도 | 즉시 중단하지 않고 실패분을 모아 **최대 3라운드** 재시도 (라운드 간 15s → 30s 대기) |
| 실패 분류 | `TRANSIENT`(재투입) · `PERMANENT`(그 조각만 포기) · `FATAL`(fetch 즉시 중단) |
| fetch 안전장치 | `fetch_budget`(window 단위 시간 예산) |
| fetch 부분 실패 | 라운드 후에도 남은 누락은 **기록하고 진행한다.** `max_missing_ratio` 게이트가 판정 |
| silver 저장 시점 | **window 전체를 모아 배치로 1회.** Parquet 파일 1개 |
| 게이트 2종 | 수집(`max_missing_ratio`)과 폐기(`max_drop_ratio`)는 **독립.** `drop_ratio` 분모는 `fetched` |
| 품질 게이트 실패 | 어느 쪽이든 초과 시 **silver를 쓰지 않는다.** `artifacts.silver = null` |
| 완결도 | `completeness = kept / expected`를 manifest에 **정보로** 기록(게이트 아님) |
| 빈 quarantine | 폐기 행이 0건이면 **객체를 만들지 않는다.** `artifacts.quarantine = null` |
| 병렬화 경계 | 프로세스는 window 단위 그대로. 필요해지면 `fetch` **내부만** async로 (프로세스를 호출 단위로 쪼개지 않는다) |
| 검증 정책 배치 | **소스 기본 4분면 + 컬럼별 오버라이드** |
| 정책 2단계 | ① 컬럼 정책이 값 교정 → ② 행 정책이 최종 판정 (행 정책은 선택, 생략 시 스킵) |
| 실패 시 재개 | **단계 체크포인트 + 멱등 재실행.** bronze가 있으면 fetch 스킵 |
| 멱등 키 | `(source_id, window_start)` — S3 경로와 manifest 경로를 결정 |
| 교정 행 표시 | silver에 메타 컬럼 `_row_status` (`ok` / `repaired`) 하나만 추가 |
| manifest 저장소 | **S3 단독.** collector가 DB 커넥션 없이 동작 |
| 코드 위치 | **`collector/` 단독.** 소비자가 생기면 그때 `libs/core`로 승격 |
| config 형식 | **YAML + 기동 시 검증.** 함수는 문자열 이름 → 레지스트리 조회 |
| 행 처리 | **행 단위 dict 순회.** pandas 벡터화 아님 |
| 어댑터 계약 | `fetch`(`FetchResult`를 흘려보냄) + `normalize`(행 = 레코드 변환) **2단계.** 소스별 응답 구조 차이를 여기서 흡수 |
| 실패 전달 | 어댑터는 예외가 아니라 **값**(`FetchResult.error`)으로 실패를 알린다. 라운드는 어댑터 밖에서 돈다 |
| 저장 책임 | 조각을 S3에 쓰는 것은 **pipeline**이다. 어댑터는 저장소를 알지 못한다 |
| 어댑터 개수 | API **제공처 단위** 2개로 소스 7개를 모두 수용 |
| 조각 ID | 파일명은 **요청 파생 키**(`part=page-00001-01000` · `part=grid-060x127`). 읽는 순서는 manifest `parts`가 결정 |

---

## 3. 디렉토리 구조

[개발 환경 가이드](../getting-started.md)의 규약대로 wheel 빌드 없이 `uv run python main.py`로 실행한다. 하위 디렉토리는 `sys.path[0] == collector/` 이므로 그대로 import된다.

```
collector/
├── main.py                   # CLI 진입점 (인자 파싱 → pipeline 호출)
├── pipeline.py               # fetch→bronze→validate→silver 오케스트레이션 + 재개 분기
├── manifest.py               # manifest 스키마, 읽기/쓰기, 상태 어휘(RunStatus/Stage/FailureReason)
├── storage.py                # S3/MinIO 입출력, 경로 규칙 생성
├── logging_setup.py          # 구조화 로그 설정 (고정 필드 주입)
├── config/
│   ├── schema.py             # SourceConfig / ColumnSpec / Policies (pydantic)
│   └── loader.py             # YAML 로드 + 스키마 검증 + 정책 이름·row_params 검증 + 해시
├── validation/
│   ├── types.py              # Action / Issue / RowVerdict / RunContext (공유 계약 타입)
│   ├── registry.py           # @policy / @row_policy 데코레이터, 이름→함수 매핑
│   ├── policies.py           # 공통 정책 함수 구현체 (소스 무관)
│   └── engine.py             # 행 순회 → 판정 → 정책 디스패치 → 결과 집계
├── adapters/
│   ├── base.py               # Adapter 프로토콜 + 어댑터 레지스트리
│   ├── seoul_openapi.py      # 서울 열린데이터광장 공통 (소스 5종이 공유)
│   ├── seoul_probe.py        # 서울 probe_until_empty 페이지네이션 (bikeList)
│   └── kma_apihub.py         # 기상청 API 허브 (소스 2종이 공유)
├── sources/                  # 소스별 YAML — 새 소스는 원칙적으로 여기만 추가
│   ├── bike_station_realtime.yaml
│   └── ... (총 7개)
└── tests/
```

**어댑터는 소스 수만큼 필요하지 않다.** 서울 열린데이터광장 5종은 `{인증키}/{포맷}/{서비스명}/{시작}/{끝}/` 형태의 동일한 페이지네이션 규약을 쓰므로 어댑터 하나로 커버되고, 소스별 차이는 config의 `adapter_params`(서비스명·페이지 크기·응답 루트 키)로 흡수한다. **어댑터 2개로 소스 7개를 모두 수용한다.**

### 어댑터 계약

어댑터는 두 단계로 나뉜다. **소스마다 다른 응답 구조를 흡수하는 것이 어댑터의 책임**이고, 검증 엔진은 항상 "행 = 레코드"인 `list[dict]`만 받는다.

```python
@dataclass(frozen=True)
class FetchResult:
    key: str                        # 조각 식별 키 (page-00001-01000 · grid-060x127)
    payload: RawChunk | None        # 성공 시 원본 응답
    error: FetchErrorKind | None    # 실패 시 TRANSIENT | PERMANENT | FATAL
    expected_total: int | None      # 소스가 알려주거나 probe로 확정한 전체 행 수


class Adapter(Protocol):
    def fetch(self, config: SourceConfig, window: Window, *,
              skip: frozenset[str] = frozenset(),
              expected_total: int | None = None) -> Iterator[FetchResult]:
        """API를 호출해 조각을 하나씩 흘려보낸다.

        한 번의 호출 = 한 조각이다. 응답을 가공하지 않으며, 조각을 S3에 저장하는 것은
        pipeline의 책임이다. 어댑터는 저장소를 알지 못한다.

        실패는 예외가 아니라 error가 채워진 FetchResult로 알린다. 라운드 재시도를
        어댑터 밖에서 돌리기 위해서다. FATAL만 즉시 중단 신호로 쓴다.
        """

    def normalize(self, chunks: list[RawChunk]) -> list[dict]:
        """조각 목록을 행 = 레코드 형태로 변환한다."""
```

`fetch`가 값을 반환하지 않고 `yield`하는 이유는 **저장 책임을 어댑터에서 떼어내기
위해서**다. 어댑터가 `storage`를 직접 부르면 어댑터 단위 테스트에 S3 목이 필요해지고,
"어댑터는 네트워크만 안다"는 경계가 무너진다.

`skip`은 **이미 확보한 조각 키**다. 라운드 재순회가 이 인자를 쓴다. 어댑터는
해당 키의 호출을 건너뛴다.

`expected_total`은 서울 API가 성공 페이지를 skip해도 계획을 세울 수 있게 하는 힌트다.
일반 페이지는 라운드 0의 `list_total_count`, `pagination: probe`는 빈 종료 페이지의
위치와 직전 페이지 row 수로 계산한다. probe 종료 응답도 완결성 증거로 bronze에
남기고 그 결과에 expected를 싣는다. 라운드 루프가 이 값을 기억했다가 다음 라운드에
되돌려주며, manifest의 `counts.expected`와 완결도 계산에 쓴다. 기상청처럼 전체 행 수를 알 수 없는 소스는 `None`이다.

**어댑터가 예외 대신 값으로 실패를 알리는 이유**는 라운드 루프가 어댑터 밖에 있기
때문이다. 예외로 올리면 첫 실패에서 이터레이터가 끊겨 나머지 조각을 시도할 수 없다.

`normalize`는 조각 하나가 아니라 **조각 목록 전체**를 받는다. 서울 API는 페이지를 이어
붙이고, 기상청은 격자별 응답을 pivot 그룹 키로 합쳐야 하므로 조각을 가로질러 봐야 한다.

기상청 응답은 기온·습도·풍속이 **각각 별도 행으로 쌓인 long format**이라 `normalize`에서 pivot이 필요하다.

```python
# fetch 원본 (bronze에 저장되는 형태)
{"category": "T1H", "obsrValue": "31.6", "nx": 60, "ny": 127, "baseDate": "20260812", "baseTime": "1400"}
{"category": "REH", "obsrValue": "42",   ...}
{"category": "WSD", "obsrValue": "3.2",  ...}

# normalize 결과 (검증 엔진이 받는 형태)
{"baseDate": "20260812", "baseTime": "1400", "nx": 60, "ny": 127,
 "T1H": "31.6", "REH": "42", "WSD": "3.2", "PTY": "0", "RN1": "0", "UUU": "-3", "VVV": "-0.9", "VEC": "72"}
```

이 정규화 덕분에 config가 `T1H`·`REH`처럼 **컬럼별로 서로 다른 정상 범위**를 자연스럽게 선언할 수 있고, 검증 엔진에는 조건부 range 같은 개념이 필요 없다.

**어댑터가 공통으로 처리할 것**

- **응답 코드 검사**: 서울 API는 HTTP 200으로 응답하면서 본문에 에러 코드를 담는다. `rentBikeStatus.RESULT.CODE`가 `INFO-000`이 아니면 실패로 처리한다.
- **페이지네이션**: 서울 API 기본 모드는 `list_total_count`와 `{시작}/{끝}` 인덱스로
  순회한다. `bikeList`는 total이 현재 페이지 행 수이므로 설정으로 켠 probe 모드가
  빈 페이지까지 순차 탐색한다. 한 번에 최대 1,000건.
- **반복 호출**: 기상청은 격자(`nx`, `ny`)마다 별도 호출이 필요하다. 서울 전역을 커버할 격자 목록은 `adapter_params.grids`에 둔다.
- **재시도**: 호출 단위로 짧은 백오프 2회. 그 위에 조각 집합 단위 라운드가 얹히므로 백오프 횟수는 줄여 잡는다([8절](#8-부분-실패와-백필)).
- **실패 범주 판정**: HTTP 상태와 응답 본문 코드를 `TRANSIENT` · `PERMANENT` · `FATAL` 중 하나로 매핑한다. 서울 API의 `RESULT.CODE`처럼 제공처마다 다른 규약이 여기서 흡수된다.

---

## 4. config 스키마

```yaml
# collector/sources/bike_station_realtime.yaml
source_id: bike_station_realtime
description: 따릉이 실시간 대여정보

adapter: seoul_openapi              # adapters 레지스트리 키
adapter_params:
  service: bikeList
  page_size: 1000
  root_key: rentBikeStatus.row      # 응답에서 행 배열을 꺼낼 경로
  pagination: probe                 # list_total_count 대신 빈 페이지로 종료 판정
  max_probe_pages: 10               # 10,000행 뒤에도 끝이 없으면 실패

schedule:
  interval: 5m                      # 문서화·window 계산용 (실제 스케줄러는 Airflow)

storage:
  bronze_format: json               # 원본 그대로 + gzip
  silver_format: parquet
  partition: [dt, hh]               # dt=YYYY-MM-DD/hh=HH

quality:
  max_drop_ratio: 0.05              # 폐기 비율. 분모는 fetched. 초과 시 PARTIAL이 아니라 FAILED
  max_missing_ratio: 0.0            # 수집 누락 비율. 기본 0.0 = 조각 하나만 빠져도 FAILED
  allow_empty: false                # 행 0건이면 FAILED (행사 소스는 true)

fetch:                              # 선택. 생략하면 전부 기본값
  budget: 2m30s                     # window 하나의 fetch 전체 예산
                                    # 미지정 시 min(interval × 0.5, 30m)

policies:                           # 4분면 기본값
  required_missing: drop_row
  required_outlier: drop_row
  optional_missing:  keep_null
  optional_outlier:  set_null
  row: null                         # 행 정책 (선택). null이면 이 단계 스킵
  row_params: null                  # 행 정책에 넘길 인자 (선택)
                                    # 예: row: drop_if_issue_count_exceeds
                                    #     row_params: { max_issues: 3 }

columns:                            # 실제 API 응답 기준 (전 필드가 문자열로 내려온다)
  stationId:
    types: [str]
    required: true
  stationName:
    types: [str]
    required: true
  rackTotCnt:
    types: [int]                    # 문자열 "15"로 와도 int로 해석되면 통과 + 캐스팅
    range: { min: 0, max: 200 }
  parkingBikeTotCnt:
    types: [int]
    range: { min: 0, max: 200 }
    on_outlier: clip_to_range       # 이 컬럼만 기본값 오버라이드
  shared:
    types: [int]
    range: { min: 0, max: 1000 }
  stationLatitude:
    types: [float]
    range: { min: 37.4, max: 37.7 }
  stationLongitude:
    types: [float]
    range: { min: 126.7, max: 127.2 }
```

기상청 소스는 어댑터의 `normalize`가 pivot한 뒤의 컬럼을 기준으로 선언한다. **`obsrValue` 하나가 아니라 관측 항목마다 별도 컬럼**이므로 범위를 정확히 걸 수 있다.

```yaml
# collector/sources/kma_ultra_srt_ncst.yaml (발췌)
adapter: kma_apihub
adapter_params:
  endpoint: getUltraSrtNcst
  root_key: response.body.items.item
  pivot: { key: category, value: obsrValue }   # long → wide 변환 기준
  grids: [[60, 127], [61, 127]]                # 서울 커버 격자 목록

columns:
  T1H: { types: [float], range: { min: -50, max: 50  } }   # 기온
  REH: { types: [float], range: { min: 0,   max: 100 } }   # 습도
  WSD: { types: [float], range: { min: 0,   max: 50  } }   # 풍속
  RN1: { types: [float], range: { min: 0,   max: 500 } }   # 1시간 강수량
  PTY: { types: [int],   enum: [0, 1, 2, 3, 5, 6, 7] }     # 강수형태
```

### 신규 키의 기본값은 전부 "현행 동작"이다

`max_missing_ratio` · `fetch` 두 키는 모두 **생략 가능**하고, 생략했을 때의 동작이 부분 실패 허용 이전과 같다. `max_missing_ratio: 0.0`은 조각 하나만 빠져도 FAILED다. 이렇게 두면 "소스를 추가할 때 YAML 하나만 쓰면 된다"는 목표가 유지되고, 부분 성공을 받아들일 소스만 명시적으로 연다.

소스별 권장 초기값이다. 조각 수가 적은 소스에서는 비율 게이트가 사실상 이진 스위치라는 점을 감안한다(따릉이 실시간은 3페이지라 1개 실패 = 27~36%). 그래도 비율로 통일하는 이유는 `list_total_count`가 변하면 조각 수도 달라져 절대 개수 임계치가 더 불안정하기 때문이다.

| 소스 | `max_missing_ratio` |
| --- | --- |
| 따릉이 실시간 대여정보 | 0.0 |
| 서울 실시간 인구 데이터 | 0.0 |
| 기상청 초단기 실황·예보 | 0.1 |
| 기상청 단기예보 | 0.1 |
| 따릉이 대여이력 | 0.05 |
| 서울 생활인구(250m) | 0.05 |
| 문화행사·공연행사 | 0.05 |

라운드 수 · 라운드 간 대기는 **config에 노출하지 않는다.** 소스별로 다를 이유가 없고, 설정 키를 늘리면 소스 YAML 7개에 그대로 곱해진다. `adapters/base.py` 상수로 두고 필요해지면 그때 연다.

`loader.py`는 로드 시점에 다음을 수행한다.

1. pydantic 스키마 검증
2. `on_missing`·`on_outlier`·`row`에 적힌 이름이 레지스트리에 실제로 등록돼 있는지 검증
3. `row_params`를 해당 행 정책에 등록된 params 모델로 검증
4. 파일 내용 SHA-256을 `config_version`으로 계산

3단계는 두 방향 모두 오류로 막는다 — params 모델이 있는 정책인데 `row_params`가 없거나 필드가 틀린 경우, 그리고 params를 받지 않는 정책에 `row_params`가 들어온 경우다. 이 단계가 없으면 `max_issue: 3` 같은 오타가 런타임까지 살아남는다.

**config 오타는 수집이 시작되기 전에 죽는다.**

---

## 5. 검증 엔진

### 판정 순서 (컬럼 하나당)

```
원시값 → ① 결측 판정 (None / "" / 센티널)
       → ② 타입 해석 (types 목록으로 캐스팅 시도)
       → ③ 범위·enum 판정
```

**엔진이 정책에 넘기는 `value`는 `kind`에 따라 다르다** — `MISSING`이면 정규화된
`None`, `TYPE_ERROR`면 캐스팅에 실패한 원시값, `OUTLIER`면 캐스팅에 성공한 값이다.
캐스팅 전 원시값은 언제나 `Issue.raw_value`에 남는다.

`types`는 "이 타입으로 해석 가능해야 한다"는 뜻이다. 서울 API는 숫자도 문자열로 주므로, 해석에 성공하면 **캐스팅된 값이 silver에 들어간다**(정규화 겸용). 실패하면 `TYPE_ERROR`로 판정한다.

쓸 수 있는 이름은 `str | int | float | bool | precip`이다. 앞의 넷은 파이썬 생성자를 그대로 부르고, `precip`은 기상청 강수량 범주 표기(`"강수없음"`, `"1.0mm 미만"`, `"30.0~50.0mm"`, `"50.0mm 이상"`, `"2.0mm"`, `"0"`이 한 응답에 섞여 나온다)를 mm 실수로 바꾸는 전용 캐스터다. 범위형은 하한을 취한다. 규칙을 `libs/core`의 `core.precip`에 둔 것은 loader도 같은 함수를 써야 silver와 RDB의 값이 갈리지 않기 때문이다 — collector가 숫자로 저장하기 전에 쌓인 silver를 loader가 다시 읽는 경로가 남아 있다.

판정 결과는 `Issue(column, kind, required, raw_value, spec)`이고, `kind`는 `MISSING | TYPE_ERROR | OUTLIER` 중 하나다. 이 타입들은 `validation/types.py`에 모아 둔다. registry · policies · engine · loader 네 곳이 공유하는 어휘이므로 레지스트리와 분리한다.
이 `Issue`는 집계용 기록이 아니라 **컬럼 정책의 두 번째 인자로 그대로 전달된다.**
정책이 `issue.kind`를 볼 수 있어야 교정형 정책이 캐스팅 실패 값을 방어할 수 있다.

### TYPE_ERROR의 디스패치

4분면은 `missing` · `outlier` 2축뿐이므로 `TYPE_ERROR`의 목적지를 정해야 한다.

**`TYPE_ERROR`는 outlier 계열로 보낸다.** 단 컬럼별 `on_outlier` 오버라이드는 적용하지 않고 소스 4분면 기본값(`required_outlier` · `optional_outlier`)만 쓴다.

- missing 계열로 보내면 깨진다. `optional_missing`의 기본값 `keep_null`은 값을 그대로 두는 정책이고, 캐스팅 실패 값은 원본 문자열이다. silver 스키마가 config `types`로 고정된 상태에서 `"31.6xyz"`를 int 컬럼에 쓸 수 없다. outlier 계열은 `optional_outlier`가 `set_null`이라 자연히 정리된다.
- 오버라이드를 제외하는 이유는 `clip_to_range` · `fill_zero`처럼 값을 교정하는 정책이 **캐스팅되지 않은 값에는 적용 자체가 불가능**하기 때문이다. `on_outlier`는 "타입은 맞지만 범위를 벗어난 값"을 겨냥한 설정이다.
- 4분면 기본값 자체가 교정형인 경우가 남는다. 교정형 정책은 `issue.kind`가 `TYPE_ERROR`이면
`(None, Action.KEEP)`을 반환해 방어한다. `set_null`과 같은 효과가 되므로 규칙을 늘리지 않는다.
값의 타입을 다시 검사하지 않고 `kind`를 보는 이유는, 엔진이 이미 수행한 캐스팅 판정을
정책이 되짚으면 두 곳의 기준이 어긋날 수 있기 때문이다. `clip_to_range`는 같은 가드로
결측값과 `range` 미선언 컬럼까지 함께 막는다.

`column_issues` 집계에는 `type_error`를 **별도 항목으로 유지한다.** 디스패치는 outlier와 합치지만 집계는 구분해야 원인을 추적할 수 있다.

### 정책 계약

```python
class Action(Enum):
    KEEP = "keep"              # 반환값으로 치환하고 행 유지
    DROP_ROW = "drop_row"      # 이 행을 silver에서 제외 → quarantine
    FAIL_BATCH = "fail_batch"  # 배치 전체 실패

@policy("clip_to_range")
def clip_to_range(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 정상 범위의 경계로 잘라낸다."""
    bounds = issue.spec.range
    if (
        issue.kind is not IssueKind.OUTLIER
        or bounds is None
        or bounds.min is None
        or bounds.max is None
    ):
        return None, Action.KEEP          # 캐스팅 실패 · 결측 · range 미선언 · 부분 range를 함께 막는다
    return min(max(value, bounds.min), bounds.max), Action.KEEP
```

**초기 컬럼 정책 함수** (소스 무관, 전부 `validation/policies.py`)

`keep_null` · `set_null` · `fill_zero` · `fill_default` · `clip_to_range` · `drop_row` · `fail_batch`

**초기 행 정책 함수**

`drop_if_any_required_issue` · `drop_if_issue_count_exceeds` · `keep_always`

행 정책은 `params`를 네 번째 인자로 받는다. 파라미터를 쓰지 않는 정책도 같은 시그니처를 유지해 엔진이 정책마다 호출 방식을 분기하지 않게 한다.

```python
@row_policy("drop_if_any_required_issue")
def drop_if_any_required_issue(
    row: dict, issues: list[Issue], ctx: RunContext, params: None
) -> RowVerdict:
    """필수 컬럼에 문제가 하나라도 있으면 행을 폐기한다."""
```

파라미터가 필요한 정책은 pydantic 모델을 함께 등록한다. `loader.py`가 이 모델로 config의 `row_params`를 검증한다.

```python
class IssueCountParams(BaseModel):
    max_issues: int

@row_policy("drop_if_issue_count_exceeds", params=IssueCountParams)
def drop_if_issue_count_exceeds(
    row: dict, issues: list[Issue], ctx: RunContext, params: IssueCountParams
) -> RowVerdict:
    """이슈 개수가 임계값을 넘으면 행을 폐기한다."""
```

새 정책이 필요하면 **함수 하나를 추가하고 YAML에서 이름으로 부른다.** 엔진 코드는 건드리지 않는다.

### 행 결과

값이 하나라도 교정됐으면 `_row_status = "repaired"`, 아니면 `"ok"`. 폐기된 행은 이슈 목록과 함께 quarantine으로 간다.

```jsonl
{"_issues":[{"column":"stationId","kind":"missing","required":true,"action":"drop_row"}],
 "_row_index":417,"stationId":null,"rackTotCnt":"10","parkingBikeTotCnt":"7"}
```

---

## 6. 상태 어휘와 manifest

| 상태 | 의미 |
| --- | --- |
| `RUNNING` | 실행 중 (시작 시 기록) |
| `SUCCEEDED` | silver까지 완료, 폐기된 행 없고 누락된 조각도 없음 |
| `PARTIAL` | silver까지 완료, 일부 행 quarantine **또는 일부 조각 누락** (두 게이트 모두 이내) |
| `FAILED` | 단계 실패, 또는 게이트 초과(`max_drop_ratio` · `max_missing_ratio`), 또는 `allow_empty=false`인데 0건 |
| `EMPTY` | 행 0건이지만 `allow_empty=true`라 정상 |
| `SKIPPED` | 같은 멱등 키가 이미 `completed` — 아무것도 하지 않고 종료 |

`Stage`(재개 근거): `bronze_written` → `validated` → `completed`

**`fetched` 단계는 두지 않는다.** 조각을 도착 즉시 저장하므로 실행이 `fetch₁ → save₁ → fetch₂ → save₂ → …`로 흐르고, 마지막 조각을 저장한 순간 두 상태가 동시에 달성된다. 도달할 수 없는 중간 단계를 어휘에 남기지 않는다.

**`stage`는 실행이 어디까지 갔는지만 뜻한다.** 조각이 다 모였는지는 `completeness` · `missing`이 따로 표현하므로, `stage=completed`이면서 불완전한 window가 존재한다. `bronze_written`은 이제 "계획한 조각을 전부 받았다"가 아니라 "이번 실행이 fetch 단계를 마쳤다"는 뜻이고, 마쳤다는 판정에는 라운드 소진 · 예산 초과가 모두 포함된다.

`FailureReason`(선택, `FAILED`일 때만): 재시도가 의미 있는 실패인지 구분한다.

| 값 | 의미 | 재시도가 도움이 되는가 |
| --- | --- | --- |
| `fetch_error` | API 호출 실패, `max_missing_ratio` 초과, `FATAL`(인증 오류) | 예 (`FATAL`은 키를 고친 뒤) |
| `storage_error` | bronze·silver·quarantine 쓰기 실패 | 예 |
| `quality_gate` | `max_drop_ratio` 초과, 0건인데 `allow_empty=false`, `FAIL_BATCH` | **아니오** |
| `config_error` | 스키마·정책 이름·`row_params` 검증 실패 | 아니오 |

`quality_gate` 실패는 같은 bronze에 같은 config를 적용하므로 재개해도 결과가 같다. config를 고쳐 재처리해야 한다. status를 늘리지 않고 사유만 부가 정보로 남기므로 재개 분기는 이 필드를 보지 않는다.

**게이트가 곧 `failure_reason`을 결정한다.** 수집 게이트에 걸리면 `fetch_error`(`--force` 재수집으로 회복), 폐기 게이트에 걸리면 `quality_gate`(config 수정)다. 두 게이트를 하나로 합치지 않은 이유가 이것이다 — 합치면 71%라는 결과만 보고 재시도할 실패인지 config를 고칠 실패인지 판단할 수 없다.

```json
{
  "source_id": "bike_station_realtime",
  "window_start": "2026-08-12T14:10:00+09:00",
  "window_end":   "2026-08-12T14:15:00+09:00",
  "status": "PARTIAL",
  "stage":  "completed",
  "failure_reason": null,
  "attempt": 2,
  "revision": 1,
  "started_at": "...", "ended_at": "...", "duration_ms": 4310,
  "artifacts": {
    "bronze": {
      "prefix": "s3://.../bronze/bike_station_realtime/dt=2026-08-12/hh=14/1410/",
      "parts":  ["page-00001-01000", "page-01001-02000", "page-02001-02765"]
    },
    "silver":     "s3://.../silver/bike_station_realtime/dt=2026-08-12/hh=14/1410.parquet",
    "quarantine": "s3://.../quarantine/bike_station_realtime/dt=2026-08-12/hh=14/1410.jsonl"
  },
  "counts": { "expected": 2765, "fetched": 2765, "kept": 2740, "repaired": 31, "dropped": 25 },
  "missing": { "parts": [], "rows": 0, "basis": "rows" },
  "drop_ratio":   0.009,
  "completeness": 0.991,
  "backfill_status": null,
  "column_issues": {
    "stationId":         { "missing": 25, "outlier": 0,  "type_error": 0 },
    "parkingBikeTotCnt": { "missing": 3,  "outlier": 28, "type_error": 0 }
  },
  "policy_actions": { "drop_row": 25, "clip_to_range": 28, "set_null": 3 },
  "config_version": "sha256:a3f9…"
}
```

### 수집 완결도 필드

`counts.expected`부터 `completeness`까지가 **무엇이 빠졌는지**를 남기는 자리다. 부분 성공을 허용하면서도 침묵한 손실을 만들지 않기 위해 존재한다.

| 필드 | 의미 |
| --- | --- |
| `counts.expected` | 전체 행 수. 서울 기본 모드는 `list_total_count`, probe는 탐색한 실제 끝, 기상청은 `null` |
| `missing.parts` | 끝내 받지 못한 조각 키 목록 |
| `missing.rows` | 누락 행 수(`expected - fetched`). `expected`가 `null`이면 `null` |
| `missing.basis` | `rows`(행 기준) 또는 `parts`(조각 기준). 기상청은 `parts` |
| `completeness` | `kept / expected`. **게이트가 아니라 정보다** |
| `revision` | authority content의 correction ordinal. 최초 성공 0, exact replay는 유지, 바뀐 content만 +1 |
| `backfill_status` | 폐기된 필드. 기존 manifest 호환용으로 항상 `null` |

`revision`과 `attempt`를 구분한다. `attempt`는 **실행 횟수**이고 `revision`은 **silver 내용이 바뀐 횟수**다. 실패한 재실행은 `attempt`만 올리고 `revision`은 그대로다. 하류가 재처리 여부를 판단할 때 보는 값은 `revision`이다.

`completeness`를 게이트로 쓰지 않는 이유는 두 게이트(`max_missing_ratio` · `max_drop_ratio`)가 이미 각 단계를 막고 있기 때문이다. 세 번째 임계치를 두면 소스 7개마다 튜닝할 값이 하나 더 늘어난다. 대신 게이트 둘을 각각 통과했는데 손실이 곱해져 최종 68%가 되는 경우를 하류가 알 수 있게 값만 남긴다.

`config_version`은 **이 silver가 어떤 정책으로 만들어졌는지**를 남긴다. 나중에 범위 기준을 바꾸면 이 해시로 재처리 대상을 골라낼 수 있다.

`artifacts`의 세 항목은 형태와 null 가능 여부가 다르다.

| 키 | 형태 | null이 되는 경우 |
| --- | --- | --- |
| `bronze` | `{prefix, parts, revision}` | `bronze_written` 미도달 |
| `silver` | 단일 키 | 게이트(`max_missing_ratio` · `max_drop_ratio`) 초과로 silver를 쓰지 않았을 때 |
| `quarantine` | 단일 키 | 폐기 행이 0건이라 객체를 만들지 않았을 때 |

`bronze`가 revision과 조각 목록을 갖는 이유는 `read_bronze`가 어느 원본 묶음을 어떤 순서로 읽어야 하는지 알아야 하고, S3 LIST에 의존하지 않도록 명시적으로 기록하기 때문이다. **manifest가 가리키는 revision의 `parts`에 없는 조각은 읽지 않는다.** 따라서 재수집 때 기존 원본을 삭제하지 않아도 이전 조각이 현재 snapshot에 섞이지 않는다. `quarantine`은 폐기 0건인 실행이 대부분이므로 빈 객체를 만들지 않는다 — 5분 주기 소스 3종이면 하루 864개가 쌓인다. 이 파일을 읽는 쪽은 부재를 정상으로 처리한다.

경로 규칙은 `storage.py` 한 곳에서만 만든다. bronze는 조각으로 저장되므로 window마다
디렉토리를 하나 더 갖는다.

```
bronze              bronze/hot/{source_id}/dt={date}/hh={hour}/{HHMM}/revision={revision:010d}/part={chunk_key}.json.gz
silver·quarantine   {layer}/{source_id}/dt={date}/hh={hour}/{HHMM}.{ext}
_manifest           _manifest/{source_id}/dt={date}/hh={hour}/{HHMM}.json
```

`{chunk_key}`는 **요청을 식별하는 키**다. 어댑터가 요청 파라미터에서 만든다.

```
part=page-00001-01000.json.gz     서울 — 페이지 인덱스 범위
part=grid-060x127.json.gz         기상청 — 격자 좌표
```

순번(`part={NNN}`)을 쓰지 않는 이유는 실행 간에 안정적이지 않기 때문이다. `list_total_count`가 2,765에서 2,770으로 변하면 마지막 페이지 범위가 달라져 같은 번호가 다른 요청을 가리키고, 그러면 재시도 라운드가 누락 조각을 지목할 수 없다. 순번이 맡던 **순서 보장은 이미 `artifacts.bronze.parts` 목록이 하고 있으므로** 잃는 것이 없다. 페이지 인덱스를 제로 패딩하면 문자열 정렬도 호출 순서와 일치하고, 기상청 격자는 pivot 그룹 키에 `nx`·`ny`가 들어가 순서와 무관하다.

---

## 7. 재개 로직

```python
manifest = manifest.load(source_id, window_start)   # 없으면 None

if completed_authority and not force:
    verify_authority()
    return SKIPPED

if manifest and manifest.stage >= BRONZE_WRITTEN and not force:
    bronze_revision = manifest.artifacts.bronze.revision
    chunks = read_manifest_revision()                         # 외부 API 호출 없음

else:                                                         # 최초·force
    bronze_revision = next_bronze_revision()
    chunks, missing = fetch_with_rounds()                     # 성공분 유지, 누락만 재시도

rows = adapter.normalize(chunks)                              # 항상 다시 수행
# 이후 게이트 → 검증 → immutable silver → authority manifest 갱신
```

분기는 셋이다.

| # | 조건 | 동작 |
| --- | --- | --- |
| 1 | 완료 authority & 일반 재실행 | authority 검증 후 SKIPPED |
| 2 | `stage>=bronze_written`인 일반 재실행 | manifest가 가리키는 Bronze revision 재사용 |
| 3 | 최초 실행·`--force` | 새 Hot revision에 전체 fetch, 이전 revision 보존 |

`stage`를 `bronze_written`으로 올리는 것은 **fetch 단계를 마친 뒤**다. 라운드를 소진했든
예산이 끝났든, 더 이상 호출하지 않기로 결정한 시점이다. 그 전에 죽으면
조각이 S3에 남아 있어도 authority로 공개되지 않는다. 같은 실행의 다음 round는 이미
성공한 조각을 유지하고 manifest가 식별한 누락 조각만 다시 받는다.

재수집 전에 기존 Bronze를 지우지 않는다. 매번 새 revision을 만들고 manifest가 선택한
revision과 part만 읽으므로 1회차 5조각, 2회차 3조각이어도 이전 두 조각은 유령 데이터로
섞이지 않는다. manifest 저장 전에 중단된 orphan revision도 다음 revision 번호 계산에서
피하며, 검증된 날짜의 모든 Hot revision은 일 단위 Cold Bronze로 보존한다.

`normalize`는 bronze 재사용 여부와 무관하게 **항상 다시 수행한다.** 네트워크를 타지 않는 순수 변환이라 비용이 없고, bronze가 정규화 전 원본을 담고 있으므로 이 편이 단순하다.

실시간 API(5분 주기)는 몇 분만 지나도 그 시점 데이터를 영영 받을 수 없다. silver 저장에서 실패했을 때 fetch부터 다시 하면 **지금 시점의 다른 데이터로 덮어쓰게 되므로**, bronze 재사용이 재개의 핵심이다. `--force`는 이 분기를 무시하고 처음부터 다시 수행한다.

```
1회차:  bronze ✓ (조각 3개) → 정제 ✓ → silver ✗ (S3 오류)
        manifest: stage=validated, status=FAILED, failure_reason=storage_error
2회차:  fetch ⤳ skip (bronze 재사용) → 정제 ✓ → silver ✓
        manifest: stage=completed, status=PARTIAL
```

---

## 8. 부분 실패와 백필

> 근거는 [ADR 0004](../adr/0004-partial-fetch-and-backfill.md).

한 window를 여러 번 호출하는데 그중 일부만 실패했을 때, **성공분을 버리지 않고 실패분만 다시 받는다.** 세 단계로 나뉜다 — 실행 안에서 라운드로 재시도하고, 그래도 남으면 누락을 기록한 채 진행하며, 시간 파라미터가 있는 소스는 별도 잡이 나중에 채운다.

### 8.1 라운드 재시도

```
라운드 0 : 전체 조각 순회 → 성공분 즉시 bronze 저장, 실패분 수집
  ↓ 대기 15s
라운드 1 : TRANSIENT 실패분만 재순회
  ↓ 대기 30s
라운드 2 : 남은 TRANSIENT 실패분만 재순회
  ↓
남은 것은 누락 확정 → 완결도 게이트
```

**핵심은 재시도 횟수가 아니라 시간 간격이다.** 호출 단위 백오프는 수 초 안에서 움직이는데, 조각 실패의 주된 원인인 429는 그 시간 스케일에서 풀리지 않고 오히려 rate limit 창을 연장시킨다. 라운드 방식은 다른 조각을 받는 동안 자연히 수십 초를 벌고 그 뒤에 다시 시도한다.

부수 효과로 **실패 위치에 따른 불공평**도 사라진다. 기존에는 page 0에서 실패하면 그 window에 대해 API를 사실상 한 번 부르고 끝났고, page 19에서 실패하면 20번 부르고 끝났다.

라운드를 넣으면 총 시도가 `라운드 × 호출당 백오프`로 곱해지므로 **호출당 백오프는 2회로 줄인다.** 총 시도는 3회에서 6회로 늘지만 시간 분포가 2초에서 45초 이상으로 넓어진다. `Retry-After` 헤더가 오면 항상 존중하고, 그 값이 남은 예산을 넘으면 해당 조각을 이번 실행에서 포기한다.

### 8.2 실패 3범주

| 범주 | 해당 | 라운드 재투입 | 비고 |
| --- | --- | --- | --- |
| `TRANSIENT` | 타임아웃, 429, 5xx, 서울 `ERROR-5xx` | O | |
| `PERMANENT` | 400, 404 | X | 그 조각만 누락 확정 |
| `FATAL` | 401·403, 서울 `INFO-100`(인증키 오류) | X | **fetch 전체 즉시 중단** |

`FATAL`을 분리하는 이유는 **모든 조각이 같은 인증키를 쓰기 때문**이다. page 0이 401이면 나머지도 401이므로, 20개 조각 × 6회 = 120번을 부르고 아무것도 성공하지 못한다. 원인이 확정적인데 나머지를 다 불러볼 이유가 없다. `FATAL`은 게이트를 타지 않고 `failure_reason=fetch_error`로 즉시 끝난다 — 재시도가 무의미하고 키를 고쳐 `--force`로 재실행할 문제다.

`PERMANENT`를 분리하는 이유는 확정된 실패를 세 라운드 반복하지 않기 위해서다.

서울 API의 **`INFO-200`(해당 데이터 없음)은 빈 결과로 성공 처리한다.** 실패로 보면 정상 상황이 누락으로 집계되어 완결도가 왜곡된다.

### 8.3 안전장치 둘

| 장치 | 값 | 초과 시 |
| --- | --- | --- |
| `fetch_budget` | `min(interval × 0.5, 30m)` 기본, 소스별 오버라이드 | 새 호출 중단(진행 중인 호출은 마무리) → 게이트 |
| 라운드 간 대기 | 15s → 30s | — |

**`fetch_budget`은 window 하나의 fetch 단계 전체 예산이다.** 호출 하나의 응답 대기 상한(httpx timeout)과는 층이 다르다.

```
window (interval 5m)
└─ fetch_budget 2m30s ────────── window 하나의 fetch 전체
   ├─ 라운드 0
   │  └─ 조각 page-00001-01000
   │     ├─ 시도 1 ── httpx timeout 10s
   │     └─ 시도 2 ── httpx timeout 10s
   ├─ 대기 15s
   └─ 라운드 1 …
```

호출 단위 제한만으로는 전체 시간을 통제할 수 없다. 480페이지가 전부 타임아웃이면 호출당 10초씩 지켜도 480 × 10s × 2회 = 160분이 된다. 측정은 fetch 진입 시각부터이고 `normalize` 이후는 예산 밖이다(네트워크를 타지 않아 결정적이고, 프로세스 전체 상한은 Airflow 태스크 타임아웃이 담당한다). 판정은 **새 호출을 시작하기 직전에만** 하므로 진행 중인 호출은 끝까지 두고 거의 다 받은 응답을 버리지 않는다.

**라운드 간 대기**가 필요한 이유는 조각 3개짜리 소스에서 라운드 0이 1~2초 만에 끝나기 때문이다. 대기가 없으면 "다른 일을 하며 시간을 번다"는 이점이 사라진다.

### 8.4 게이트와 완결도

부분 성공은 **모든 소스에서 허용**하되 `max_missing_ratio`가 판정한다. 기본값 `0.0`이므로 명시적으로 열지 않은 소스는 조각 하나만 빠져도 FAILED다.

```
fetch 종료 (라운드 소진 · 예산 초과)
   ↓
성공 조각으로 missing_ratio 계산
   ↓
   ├─ max_missing_ratio 이내 → normalize → 검증 → max_drop_ratio 판정 → silver
   │                            status=PARTIAL, 종료 코드 0
   └─ 초과 → silver 쓰지 않음, status=FAILED, failure_reason=fetch_error, 종료 코드 non-zero
```

**두 게이트는 독립이고 `drop_ratio`의 분모는 `fetched`를 유지한다.** 분모를 `expected`로 바꾸면 수집이 완전한 평상시에는 두 계산이 같고 **장애 때만 폐기율이 튀는 지표**가 된다. 운영자가 "폐기율이 왜 튀었지? 정책이 잘못됐나?" 하고 엉뚱한 곳을 보게 된다.

대가로 부분 수집 시 폐기 게이트가 다소 엄격해진다. 2,765행 중 2,000행만 받고 110행을 폐기하면 `110/2000 = 5.5%`로 임계 5%를 넘지만 원본 기준으로는 4.0%다. 이 오차는 **통과시킬 것을 막는 안전한 방향**이고, 막힌 window는 `--force` 재수집으로 회복할 수 있다. 게다가 `max_missing_ratio` 기본값 `0.0`에서는 누락이 있는 순간 수집 게이트에서 이미 죽으므로 **누락을 명시적으로 연 소스에서만** 발생한다.

**종료 코드는 부분 성공에서 0이다.** 부분 성공도 `stage=completed`로 끝나므로 non-zero를 반환해 Airflow가 retry를 돌려도 재개 분기 1번이 즉시 `SKIPPED`로 빠진다. 재시도가 할 일이 없는데 태스크만 실패로 뜨는 셈이라, 다시 채우려면 `--force`로 재수집하고 가시성은 WARN 로그 · manifest · 마커로 확보한다.

### 8.5 백필 (제거됨)

계획 단계에서는 시간 파라미터가 있는 소스의 누락 조각을 `_retry_queue` 마커와 `--backfill`로 나중에 채우고 Silver를 덮어쓰려 했다. 이 경로는 제거됐다. 누락이 있는 window는 게이트 이내면 `PARTIAL`로 확정되고, 다시 받으려면 `--force`로 새 Hot revision에 전체를 수집한다.

---

## 9. 로깅

배치당 몇 줄, **행당 0줄**. 행 상세는 quarantine 파일이 담당한다 (2,765행 × 288회/일이면 로그가 터진다).

조각마다 로그를 남기지도 않는다. 아래 3줄이 한 배치의 정상 출력 전부다.

```
INFO  source_id=bike_station_realtime window=2026-08-12T14:10Z stage=bronze_written parts=3/3 rounds=1 rows=2765 bytes=482113 ms=1203
WARN  source_id=… stage=validated status=PARTIAL kept=2740 repaired=31 dropped=25 drop_ratio=0.009 completeness=0.991
INFO  source_id=… stage=completed revision=1 key=s3://…/1410.parquet
```

첫 줄이 fetch 완료와 bronze 완료를 한꺼번에 알린다 — `Stage`에 `fetched`가 없으므로 조각 저장이 끝난 시점 한 줄로 통합하고, `parts`로 **받은 조각 / 계획한 조각**을, `rounds`로 라운드 수를 남긴다. 라운드마다 로그를 남기지는 않는다.

누락이 발생하면 첫 줄이 WARN이 되고 무엇이 빠졌는지 붙는다.

```
WARN  source_id=… stage=bronze_written parts=2/3 rounds=3 missing=page-02001-02765 missing_rows=765 completeness=0.717
```

실패 시에는 `failure_reason`을 붙인다.

```
ERROR source_id=… stage=validated status=FAILED failure_reason=quality_gate dropped=412 drop_ratio=0.149
ERROR source_id=… stage=bronze_written status=FAILED failure_reason=fetch_error missing_ratio=0.638 reason=budget_exceeded
```

`logging_setup.py`가 `source_id`·`window`·`attempt`를 고정 필드로 주입해 모든 로그에 자동으로 붙인다.

---

## 10. 실행 인터페이스

```bash
cd collector
uv run python main.py --source bike_station_realtime --window-start 2026-08-12T14:10:00+09:00 [--force]
```

Airflow는 소스별 태스크에서 `data_interval_start`를 `--window-start`로 넘긴다. `window_end`는 config의 `schedule.interval`로 계산한다. **collector 자체는 스케줄을 모른다.**

`--force`는 재개 분기를 무시하고 새 Hot revision에 전체를 다시 받는다. 이전 원본은 보존한다.

**종료 코드**: `SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0, `FAILED`는 non-zero다. 누락이 있어도 게이트를 통과했으면 `PARTIAL`이므로 0이다 — 부분 성공은 `stage=completed`로 끝나 재실행해도 `SKIPPED`로 빠지므로 Airflow retry가 할 일이 없다.

**추가 의존성**: `httpx` · `pyyaml` · `pydantic` · `pyarrow` · `boto3`

**필요한 환경변수**

| 변수 | 용도 | 발급처 |
| --- | --- | --- |
| `SEOUL_OPENAPI_KEY` | 서울 열린데이터광장 소스 5종 | [열린데이터광장 인증키 신청](https://data.seoul.go.kr/together/mypage/actKeyMain.do) |
| `KMA_APIHUB_KEY` | 기상청 API 허브 소스 2종 | [기상청 API 허브](https://apihub.kma.go.kr/) |

두 변수는 `.env.example`에 자리를 만들어 뒀다. S3/MinIO 관련 변수는 이미 있다.

---

## 11. 구현 순서

각 단계는 테스트를 먼저 작성한다.

| 순서 | 대상 | 내용 |
| --- | --- | --- |
| 1 | config 스키마 + 로더 | pydantic 모델, YAML 로드, 정책 이름·`row_params` 검증, 해시 |
| 2 | 계약 타입 + 정책 레지스트리 + 공통 정책 함수 | `types.py`, 데코레이터 등록(params 모델 포함), 함수 10종 |
| 3 | 검증 엔진 | 판정 3단계, 4분면 × 정책 디스패치, 행 정책, 집계 |
| 4 | storage + manifest | 경로 규칙(조각 키), bronze 조각 쓰기·읽기·정리, 상태 어휘, 완결도 필드 |
| 5 | 어댑터 base + `seoul_openapi` | `FetchResult` 계약, 실패 3범주, 라운드·예산, `RESULT.CODE` 검사, 페이지네이션 |
| 6 | pipeline | 4단계 오케스트레이션 + 재개 분기 4가지 + 게이트 2종 |
| 7 | main.py + 로깅 | CLI(`--force`), 구조화 로그 |
| 8 | 소스 YAML 확장 | 1개로 end-to-end 검증 → 나머지 6개 추가 (+ `kma_apihub` 어댑터의 격자 반복·pivot) |

1~3단계는 네트워크와 S3 없이 순수 단위 테스트로 끝난다.

---

## 12. 검증 방법

### 단위 테스트

```bash
cd collector && uv run pytest
```

- 정책 함수 10종 각각. 교정형 3종(`clip_to_range`·`fill_zero`·`fill_default`)은 캐스팅
  실패 값을 넣어 `(None, KEEP)`이 나오는지도 확인
- config 로더: 정책 이름 오타, `row_params` 필드 오타, params를 받지 않는 정책에
  `row_params`가 온 경우가 모두 로드 시점에 죽는지
- 검증 엔진: (필수/선택) × (결측/이상치/타입오류) 6조합 × 정책별 기대 동작.
  타입오류 2조합은 컬럼 `on_outlier`가 있어도 4분면 기본값이 적용되는지 함께 확인
- 재개: manifest `stage`별 분기 4가지 (없음 / `bronze_written` / `completed` / `--force`)
- 어댑터 `fetch`: `httpx.MockTransport`로 페이지네이션·재시도·`RESULT.CODE` 에러 처리 검증
- 어댑터 `normalize`: 기상청 long → wide pivot이 관측 항목을 컬럼으로 올바르게 펴는지 검증
- bronze 조각 왕복: 저장한 조각들을 `read_bronze`로 읽은 결과가 `fetch`가 흘려보낸 원본과
  **순서까지** 같은지 검증
- immutable Bronze revision: 조각 수가 줄어든 재실행(5조각 → 3조각)에서도 이전
  revision이 보존되고 manifest가 선택한 현재 조각만 읽는지 검증

부분 실패 관련([8절](#8-부분-실패와-백필))

- 라운드: 라운드 0에서 429 → 라운드 1에서 성공하는 시나리오가 `completeness 1.0`으로
  끝나는지. 3라운드를 모두 실패하면 누락으로 확정되는지
- 실패 3범주: `TRANSIENT`는 재투입되고, `PERMANENT`는 라운드에서 제외되며, `FATAL`은
  게이트·마커 없이 즉시 종료되는지
- `fetch_budget`: 초과 시 새 호출을 시작하지 않고 진행 중 호출만 마무리하는지
- 게이트 조합: `max_missing_ratio` × `max_drop_ratio` 통과/초과 4조합. 기본값 `0.0`에서
  기존과 동일하게 조각 하나만 빠져도 FAILED인지
- 조각 키: 서울 페이지 범위·기상청 격자 키 생성, `skip`으로 호출이 실제로 생략되는지,
  제로 패딩 정렬이 호출 순서와 일치하는지
- `INFO-200`: 빈 결과로 성공 처리되어 누락에 집계되지 않는지

### end-to-end (MinIO 로컬)

```bash
make up   # 또는 ops/compose/docker-compose.yml
cd collector && uv run python main.py --source bike_station_realtime --window-start <최근 5분 경계>
```

MinIO 콘솔에서 `bronze/`의 조각 수가 API 호출 수와 일치하는지, `silver/` · `_manifest/`가 각각 하나씩 생겼는지, manifest의 `counts`가 실제 행 수와 맞는지 확인한다.

### 재개 검증

silver 쓰기 직전에 예외를 주입해 실행한다. manifest가 `stage=validated`, `status=FAILED`, `failure_reason=storage_error`로 남는지 확인한 뒤 재실행해서, 로그에 `stage=bronze_written`이 **없고** bronze 재사용으로 완료되는지 확인한다.

### 부분 실패 검증

조각 하나를 강제 실패시키고 `max_missing_ratio: 0.4`로 실행한다.

1. silver가 생기고 manifest에 `completeness < 1.0` · `missing.parts`가 남는지. **종료 코드가 0인지**
2. `max_missing_ratio: 0.0`으로 되돌려 같은 실패를 재현 → `FAILED` · `failure_reason=fetch_error` ·
   silver 미생성인지

### 목표 달성 검증

두 번째 서울 열린데이터광장 소스(예: 문화행사)를 **YAML 파일 하나만 추가**해서 동작시킨다. 공통 코드에 한 줄도 손대지 않고 성공하면 설계 목표가 충족된 것이다.

이번 변경으로 늘어난 config 키 2개(`max_missing_ratio` · `fetch`)가 **전부 생략 가능하고, 생략했을 때 기존과 같이 동작하는지**도 함께 확인한다. 그렇지 않으면 새 소스를 추가할 때마다 채워야 할 칸이 늘어난 것이다.
