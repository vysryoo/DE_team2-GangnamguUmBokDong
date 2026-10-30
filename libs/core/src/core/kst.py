"""파이프라인 공통 시간대(KST) 규칙.

Airflow는 논리 시각을 KST 오프셋(+09:00)으로 넘기고, 데이터 레이크의 `dt=`/`hh=`
파티션도 KST 기준이다. 모듈마다 `ZoneInfo("Asia/Seoul")`을 따로 만들지 않고 여기서
가져다 쓴다.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def parse_kst_datetime(raw: str) -> datetime:
    """오프셋이 있는 ISO 8601 문자열을 KST datetime으로 변환한다.

    CLI의 `--window-start` 같은 인자를 `argparse`의 `type=`으로 바로 받을 수 있다.

    args:
        raw: `2026-08-12T23:10:00+09:00`처럼 오프셋을 포함한 ISO 8601 문자열
    returns:
        같은 시점을 KST로 표현한 aware datetime
    raises:
        ValueError: 형식이 잘못됐거나 오프셋이 없을 때
    """
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"시간대 오프셋이 없는 시각이다: {raw}")
    return parsed.astimezone(KST)


def today_kst() -> date:
    """현재 KST 날짜를 반환한다."""
    return datetime.now(KST).date()
