# Airflow 문서

이 디렉터리는 **현재 운영 구조**와 **구현 당시 기록**을 구분해 보관한다. 현재 동작을 확인할 때는 문서보다 `airflow/dags/`, `airflow/config/`, `airflow/orchestration/`을 최종 기준으로 삼는다.

## 현재 운영 문서

| 문서 | 내용 |
| --- | --- |
| [운영 구조와 데이터 흐름](./explain.md) | DAG별 주기, 의존성, 실패 경계, 외부 시스템 연결 |
| [태스크 자원 계측](./TASK_RESOURCE_PROFILES.md) | resource probe가 Airflow에서 빠졌다는 기록 |

## 과거 설계 기록

아래 문서는 구현 과정의 의사결정과 작업 이력을 보존하기 위한 자료다. 현재 DAG 이름, 주기 또는 인프라 구성을 설명하는 운영 문서로 사용하지 않는다.

| 문서 | 성격 |
| --- | --- |
| [Airflow–Collector 초기 구성안](./airflow_collector.md) | EC2 분리안과 SSH/SSM 비교 |
| [초기 통합 구현안](./airflow_implementation.md) | 통합 파이프라인 구축 전 계획 |
| [구현 계획](./implementation-plan.md) | 초기 상세 설계와 단계별 계획 |
| [구현 이슈](./implementation-issues.md) | 당시 이슈·브랜치 분할안 |
| [데이터 흐름 초안](./needed.md) | 현재 `explain.md`로 통합된 이전 설명 |

## 코드 기준 위치

- DAG와 의존성: `airflow/dags/`
- cron, retry, timeout: `airflow/config/schedules.py`
- source 묶음: `airflow/config/sources.py`
- 실제 CLI 실행 명령: `airflow/orchestration/`
- LocalExecutor와 동시성: `ops/compose/docker-compose*.yml`
- DAG 계약 테스트: `airflow/tests/`
