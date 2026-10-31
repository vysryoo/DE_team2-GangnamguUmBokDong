# Airflow 태스크 자원 계측

모듈 CLI 태스크를 `ops/resource_probe.py`로 감싸 process tree 메모리와 디스크 변화를 JSON manifest로 남기던 resource probe는 Airflow에서 빠졌다. 현재 `airflow/orchestration/task_builder.py`의 `build_module_task()`는 모듈 CLI를 wrapper 없이 바로 실행하며, `airflow/resource-profiles/` 아래에 manifest를 만들지 않는다.
