"""KST 시각 파싱 규칙을 검증한다."""

from datetime import datetime

import pytest
from core.kst import KST, parse_kst_datetime


def test_parse_keeps_kst_input() -> None:
    parsed = parse_kst_datetime("2026-08-12T23:10:00+09:00")

    assert parsed == datetime(2026, 8, 12, 23, 10, tzinfo=KST)
    assert parsed.tzinfo is KST


def test_parse_converts_other_offset_to_kst() -> None:
    parsed = parse_kst_datetime("2026-08-12T14:10:00+00:00")

    assert (parsed.year, parsed.month, parsed.day, parsed.hour) == (2026, 8, 12, 23)
    assert parsed.tzinfo is KST


def test_parse_rejects_naive_input() -> None:
    with pytest.raises(ValueError, match="오프셋"):
        parse_kst_datetime("2026-08-12T23:10:00")
