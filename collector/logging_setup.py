"""구조화 로그 설정. 모든 로그에 source_id·window·attempt를 고정 필드로 붙인다.

행 상세는 quarantine 파일이 담당하므로 조각·라운드 단위로는 로그를 남기지 않는다.
정상 배치는 단계 경계(`bronze_written`, `validated`, `completed`)마다 한 줄씩 남기고,
누락·폐기가 있으면 해당 단계가 WARN·ERROR로 바뀌며 `failure_reason`이 붙는다.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime

_FIXED_FIELDS = ("source_id", "window", "attempt")
_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}

# 기상청 API는 인증키를 쿼리 파라미터(?authKey=...)로, 서울 API는 경로 세그먼트로 받는다.
_SECRET_QUERY_RE = re.compile(r"(?i)([?&](?:service|auth)?key=)[^&\s'\"]+")
_SECRET_PATH_RE = re.compile(r"(?i)(openapi\.seoul\.go\.kr:8088/)[^/\s'\"]+")


def _redact(text: str) -> str:
    """URL 쿼리 파라미터와 경로에 실린 인증키 값을 가린다."""
    text = _SECRET_QUERY_RE.sub(r"\1***", text)
    text = _SECRET_PATH_RE.sub(r"\1***", text)
    return text


class _FixedFieldsFilter(logging.Filter):
    """모든 레코드에 고정 필드를 주입한다. 레코드를 걸러내지는 않는다."""

    def __init__(self, **fields):
        super().__init__()
        self._fields = fields

    def filter(self, record: logging.LogRecord) -> bool:
        for name, value in self._fields.items():
            setattr(record, name, value)
        return True


class _KeyValueFormatter(logging.Formatter):
    """`LEVEL message key=value ...` 형식으로 렌더링한다.

    고정 필드(source_id·window·attempt)를 먼저 쓰고, 호출부가 `extra=`로 넘긴
    필드를 그 뒤에 넘긴 순서대로 이어붙인다.
    """

    def format(self, record: logging.LogRecord) -> str:
        """레코드 하나를 한 줄로 렌더링한다."""
        parts = [record.levelname]
        message = record.getMessage()
        if message:
            parts.append(message)
        for field in _FIXED_FIELDS:
            if hasattr(record, field):
                parts.append(f"{field}={getattr(record, field)}")

        for key, value in vars(record).items():
            if key in _FIXED_FIELDS or key in _STANDARD_ATTRS:
                continue
            parts.append(f"{key}={value}")
        return _redact(" ".join(parts))


def configure_logging(
    source_id: str,
    window_start: datetime,
    attempt: int = 1,
    *,
    level: int = logging.INFO,
    stream=None,
) -> logging.Logger:
    """source_id·window·attempt를 모든 로그에 자동으로 붙이는 root 로거를 설정한다.

    `main.py`가 pipeline을 부르기 전에 호출해야 pipeline이 남기는 로그에도 고정 필드가 붙는다.

    args:
        source_id: 이번 실행의 소스 id.
        window_start: 이번 실행이 처리하는 window의 시작 시각.
        attempt: 이번 실행이 몇 번째 시도인지. manifest의 attempt와 같은 값을 쓴다.
        level: root 로거의 최소 레벨.
        stream: 로그를 보낼 스트림. 생략하면 컨테이너 stdout(`sys.stdout`).
    returns:
        설정이 끝난 root 로거.
    """
    fields = _FixedFieldsFilter(
        source_id=source_id, window=window_start.isoformat(), attempt=attempt
    )
    return _install_root_handler(fields, level, stream)


def configure_httpx_request_logging(adapter_name: str) -> None:
    """KMA의 elapsed 로그와 중복되는 httpx 요청 로그만 끈다.

    다른 어댑터는 기존 동작을 유지한다. 한 프로세스에서 여러 번 호출하는 테스트를 위해
    NOTSET으로 원복할 수도 있게 한다.
    """
    level = logging.WARNING if adapter_name == "kma_apihub" else logging.NOTSET
    logging.getLogger("httpx").setLevel(level)


def configure_batch_logging(
    source_id: str, *, level: int = logging.INFO, stream=None
) -> logging.Logger:
    """source_id만 고정 필드로 붙이는 root 로거를 설정한다.

    compaction처럼 날짜 범위를 처리하는 배치에는 window·attempt가 없다. 실행 시각 등으로
    억지로 채우면 실제 수집 window로 오해하므로, 같은 포매터를 쓰되 없는 필드만 뺀다.

    args:
        source_id: 이번 실행의 소스 id.
        level: root 로거의 최소 레벨.
        stream: 로그를 보낼 스트림. 생략하면 컨테이너 stdout(`sys.stdout`).
    returns:
        설정이 끝난 root 로거.
    """
    return _install_root_handler(_FixedFieldsFilter(source_id=source_id), level, stream)


def _install_root_handler(
    fields: logging.Filter, level: int, stream
) -> logging.Logger:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(_KeyValueFormatter())
    handler.addFilter(fields)
    root.addHandler(handler)
    return root
