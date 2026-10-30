"""Slack Incoming Webhook으로 메시지를 보낸다.

이 저장소는 Airflow Variable/Connection 대신 환경변수로 시크릿을 관리하므로 webhook
URL도 환경변수에서 읽는다.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

_WEBHOOK_ENV = "SLACK_WEBHOOK_URL"
_DE2_GROUP_ENV = "SLACK_DE2_GROUP_ID"
_REQUEST_TIMEOUT_SECONDS = 10


def de2_group_mention() -> str:
    """DE 2조 Slack user group 멘션 문자열을 만든다.

    Returns:
        ``SLACK_DE2_GROUP_ID``가 있으면 ``<!subteam^...>`` 멘션, 없으면 ``@de2조`` 텍스트
    """
    group_id = os.environ.get(_DE2_GROUP_ENV, "").strip()
    return f"<!subteam^{group_id}>" if group_id else "@de2조"


def send_message(text: str) -> None:
    """Slack Incoming Webhook으로 text를 보낸다.

    ``SLACK_WEBHOOK_URL``이 없으면 로그만 남기고 건너뜀. webhook 미설정으로
    모니터링 DAG가 실패하지 않게 함.

    Args:
        text: 보낼 메시지
    Raises:
        RuntimeError: Slack이 3xx 이상 상태를 반환한 경우
    """
    webhook_url = os.environ.get(_WEBHOOK_ENV, "").strip()
    if not webhook_url:
        logger.warning(
            "%s가 설정되지 않아 Slack 전송을 건너뜁니다: %s", _WEBHOOK_ENV, text
        )
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
        if response.status >= 300:
            raise RuntimeError(f"Slack 전송 실패: HTTP {response.status}")
