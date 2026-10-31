# Collector 문서

Collector의 현재 동작은 `collector/` 코드와 source YAML을 최종 기준으로 한다. 이 디렉터리에는 운영 가이드와 구현 과정의 조사·계획 기록이 함께 있으므로 상태를 구분해 읽는다.

## 현재 참고 문서

| 문서 | 성격 | 최종 기준 |
| --- | --- | --- |
| [대형 ZIP 선택 준비](./archive-zip-staging.md) | bootstrap 입력 파일 준비 절차 | `collector/bootstrap/zip_stage.py` |
| [Bootstrap 설계 기록](./bootstrap-design.md) | 현재 구현의 배경과 결정 근거 | `collector/bootstrap/`과 mappings YAML |
| [Source 설정 점검](./source-config-audit.md) | 2026-08-19 API 실측 audit | 현재 상태는 `collector/sources/*.yaml`과 테스트 |

## 보관 문서

| 문서 | 보관 이유 |
| --- | --- |
| [DataSchema 초안](./DataSchema.md) | 초기 논리·Gold 스키마 초안으로 현재 source 계약과 혼재 |
| [Collector 구현 계획](./implementation-plan.md) | 구현 전 상세 계획 |
| [Bootstrap 구현 계획](./bootstrap-implementation-plan.md) | 구현 당시 task별 작업 기록 |
| [ML 연동 요청 기록](./ml-integration-requests.md) | 당시 발견 사항과 팀 간 요청 이력 |

## 현재 코드 기준 위치

- source ID, API, column, 품질 정책: `collector/sources/*.yaml`
- 설정 schema와 loader: `collector/config/`
- API adapter: `collector/adapters/`
- Bronze → Silver 실행과 품질 gate: `collector/pipeline.py`
- 검증·quarantine 정책: `collector/validation/`
- manifest와 source authority: `collector/manifest.py`
- S3/MinIO key와 I/O: `collector/storage.py`
- replay·일별 Archive와 GC: `collector/compaction.py`, `collector/maintenance.py`
- 과거 초기 적재: `collector/bootstrap/`
- 실행 계약 검증: `collector/tests/`

현재 운영 source는 다음 10개다.

```text
bike_rental_history
bike_station_master
bike_station_realtime
cultural_event
living_population_grid
performance_event
population_realtime
weather_short_term_forecast
weather_ultra_short_forecast
weather_ultra_short_live
```
