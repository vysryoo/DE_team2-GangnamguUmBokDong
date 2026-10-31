# 생활인구 Nowcaster 구현

> **현재 구현:** `nowcaster/`와 Airflow `daily_population_and_events` DAG가 사용하는
> 생활인구 추정·아카이브 경로다. 코드 확인일: 2026-08-25.

## 해결하는 문제

**250m 생활인구 실측은 약 4일 늦게 공개되므로 오늘과 가까운 미래의 ML feature가 비게 된다.**

Nowcaster는 실측 archive의 동일 요일·휴일 패턴으로 KST 기준 `D-3..D+3` 생활인구를
추정한다. 나중에 해당 날짜 실측이 들어오면 이를 archive로 승격하고 임시 nowcast를
삭제한다.

## 실행 흐름

```text
Collector의 exact living_population_grid authority
                         │
                         ▼
실제 YMD별 archive 승격 ─────→ 같은 날짜 nowcast 삭제
                         │
                         ▼
D-3..D+3 중 archive 없는 날짜 추정
                         │
                         ▼
silver/.../nowcast.parquet
```

Collector의 `dt=`는 수집일이고 데이터 내부 `YMD`가 실제 발생일이다. Archive key는 반드시
`YMD`에서 얻은 business date를 사용한다. Nowcaster는 Collector와 같은 exact logical
window의 source authority가 가리키는 immutable Silver만 actual로 승격한다. PARTIAL이나
authority 게시 전 Silver는 승격하지 않지만 기존 Archive를 사용한 추정은 계속한다.
Exact authority가 단순히 없으면 실측 승격만 생략한다. 반면 authority revision chain,
manifest 또는 연결된 Silver checksum·row count가 손상됐으면 fail-closed하며 추정 task도
실패한다.

## CLI

| 명령 | 용도 | 정기 실행 여부 |
| --- | --- | --- |
| `estimate` | 실측 승격과 `D-3..D+3` 추정 | Airflow 일일 실행 |
| `bootstrap-lookback` | 현재 추정에 필요한 과거 CSV 날짜만 초기 적재 | 운영 초기 1회 |
| `backfill-archive` | 디렉터리의 공식 과거 CSV 전체 적재 | 수동 backfill |

Airflow는 Collector의 일일 `living_population_grid` 성공 뒤 다음 명령을 실행한다.

```bash
uv run --frozen python main.py estimate \
  --target-date <KST YYYY-MM-DD> \
  --source-window-start <Collector와 동일한 timezone-aware logical time>
```

`--source-window-start`를 생략한 수동 실행은 actual 승격 없이 추정만 수행한다. 날짜
prefix만으로는 어떤 Collector window가 authoritative한지 증명할 수 없기 때문이다.

## 추정 grain과 값

한 추정 행의 key는 `(H_DNG_CD, CELL_ID, TT)`다. 값은 `SPOP`과 남녀 각 14개 연령대
컬럼(`M00..M70`, `F00..F70`)이다. 실측 승격과 후보 로딩 시 같은 key의 중복은 제거한다.

출력에는 다음 provenance를 추가한다.

| 컬럼 | 값 |
| --- | --- |
| `is_estimated` | 실측 `false`, nowcast `true` |
| `estimation_method` | `actual` 또는 아래 fallback 코드 |

## 추정 우선순위

```text
1~4주 전 후보 가중평균
    ↓ 해당 CELL·시간이 모두 없음
5~8주 전 후보 중 가장 가까운 값
    ↓ 없음
동일 평일/특수일 pattern의 archive 전체 평균
    ↓ 없음
0.0
```

### 1~4주 가중치

| 거리 | 가중치 |
| ---: | ---: |
| 1주 | 0.4 |
| 2주 | 0.3 |
| 3주 | 0.2 |
| 4주 | 0.1 |

- 4개가 모두 있으면 `weighted_avg`다.
- 2~3개만 있으면 존재하는 값의 가중치를 다시 정규화하고 `reweighted_avg`다.
- 1개만 있으면 그대로 사용하고 `single_week_fallback`이다.
- 최근 후보가 모두 없고 5~8주 값이 있으면 `extended_lookback_fallback`이다.
- 확장 후보도 없고 전체 평균이 있으면 `grid_historical_avg`다.
- 끝까지 값이 없으면 수치는 `0.0`, method는 `no_data`다.

## 요일·공휴일 정책

`holiday.py`는 일요일 또는 대한민국 공휴일을 `special`, 나머지를 `weekday`로 분류한다.

- 평일 후보: 정확히 1~4주 전 같은 요일
- 특수일 후보: 과거로 탐색한 가장 가까운 special day 4개, 최대 60일
- 가중 후보는 target과 pattern이 같은 경우에만 사용
- 5~8주 fallback은 같은 요일 날짜를 사용
- 전체 평균 cache도 `weekday`, `special`로 분리

## Historical average cache

Archive 전체 평균을 매 실행마다 다시 읽지 않는다. `estimate_day.py`는 pattern별 누적
합·count와 이미 반영한 날짜 목록을 S3에 저장하고, 다음 실행에서는 새 archive 날짜만
추가한다. 따라서 archive가 증가해도 정상 일일 실행의 읽기량이 계속 선형 증가하지 않는다.

## S3 구조

```text
archive/living_population_grid/dt=YYYY-MM-DD.parquet
silver/living_population_grid/dt=YYYY-MM-DD/hh=00/nowcast.parquet
```

- Archive: 실제 발생일 기준 실측, `is_estimated=false`
- Nowcast: 추정 대상일 기준 임시 파일, `is_estimated=true`
- 같은 날짜 archive가 존재하면 estimate를 건너뛴다.
- 새 실측을 archive에 쓴 뒤 같은 날짜 nowcast 삭제는 idempotent하다.
- Collector Silver는 exact source authority의 URI·checksum·row count를 검증해 읽는다.
- PARTIAL, unpublished immutable Silver와 `nowcast.parquet`은 actual 후보가 아니다.

## 초기 적재

생활인구 API는 임의 과거 날짜를 요청할 수 없으므로 초기 lookback은 공식 과거 CSV가
필요하다.

```bash
cd nowcaster

uv run --frozen python main.py bootstrap-lookback \
  --csv-dir /path/to/csv \
  --target-date 2026-08-24
```

기본 `horizon-days=3`은 `D-3..D+3` 각각의 1~4주 후보 날짜를 계산한다. CSV와 기존
archive 어디에도 없는 필수 날짜가 하나라도 있으면 non-zero로 실패한다. `--force`를
지정하지 않으면 기존 archive는 유지한다.

전체 CSV backfill은 다음 명령을 사용한다.

```bash
uv run --frozen python main.py backfill-archive --csv-dir /path/to/csv
```

## 구현 위치

| 파일 | 책임 |
| --- | --- |
| `main.py` | CLI, 실측 승격, 7일 추정 orchestration |
| `backfill.py` | CSV schema 정규화, YMD별 분할, 실측 metadata |
| `holiday.py` | 평일·special 분류와 후보 날짜 |
| `estimate_day.py` | Vectorized weighted/fallback 추정과 cache |
| `storage.py` | Archive, nowcast, historical cache S3 I/O |

## 검증

```bash
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project nowcaster --frozen pytest nowcaster/tests -q
```

테스트는 CSV backfill, actual 승격, nowcast 삭제, 후보 재가중, 5~8주 fallback, historical
average cache, holiday pattern과 S3 key를 검증한다.
