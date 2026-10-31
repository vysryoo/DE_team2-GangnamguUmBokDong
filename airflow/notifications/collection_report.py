"""collector ``--report-window-stats`` JSON을 Slack 메시지로 바꾸는 순수 함수."""

from __future__ import annotations

from typing import TypedDict

from config.alert_policy import load_thresholds
from notifications.slack import de2_group_mention


class SourceStatEvaluation(TypedDict):
    source_id: str
    stats: dict
    failure_rate: float
    drop_rate: float
    is_risky: bool


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def evaluate_source_stats(source_id: str, stats: dict) -> SourceStatEvaluation:
    """소스 하나의 실행 실패율과 행 drop 비율을 alert_policy.yaml 기준과 비교한다.

    컬럼 값 단위 결측 비율은 소스마다 평소 기준선이 크게 달라 쓰지 않음.

    Args:
        source_id: source ID
        stats: ``--report-window-stats`` JSON
    Returns:
        비율과 위험 여부를 담은 평가 결과
    """
    thresholds = load_thresholds(source_id)
    run_count = stats["run_count"]
    failed = stats["status_counts"].get("failed", 0)
    fetched = stats["fetched_count"]
    dropped = stats["dropped_count"]

    failure_rate = _ratio(failed, run_count)
    drop_rate = _ratio(dropped, fetched)

    # 실행 기록이 없으면 비율이 0이 되어 정상과 구분되지 않지만, 수집이 완전히
    # 멈춘 더 심한 장애임
    no_runs = run_count == 0
    is_risky = (
        no_runs
        or failure_rate >= thresholds["failure_rate_threshold"]
        or drop_rate >= thresholds["drop_rate_threshold"]
    )
    return {
        "source_id": source_id,
        "stats": stats,
        "failure_rate": failure_rate,
        "drop_rate": drop_rate,
        "is_risky": is_risky,
    }


# RUN/SUCC/FAIL/PART는 실행 횟수, KEEP/DROP은 행 수. 한글은 폰트마다 폭이 달라
# 고정폭 정렬이 어긋나므로 표 안은 ASCII만 씀
_COUNT_COLUMNS = (("RUN", 5), ("SUCC", 5), ("FAIL", 5), ("PART", 5), ("KEEP", 9), ("DROP", 9))
_STATUS_WIDTH = len("STATUS")


def _row(status: str, source_id: str, counts: tuple, name_width: int) -> str:
    cells = " ".join(
        f"{count:>{width}}" for count, (_, width) in zip(counts, _COUNT_COLUMNS)
    )
    return f"{status:<{_STATUS_WIDTH}}  {source_id:<{name_width}}  {cells}"


def _table(evaluations: list[SourceStatEvaluation]) -> str:
    name_width = max([len("SOURCE"), *(len(e["source_id"]) for e in evaluations)])
    header = _row("STATUS", "SOURCE", tuple(name for name, _ in _COUNT_COLUMNS), name_width)
    rows = [header, "-" * len(header)]
    for e in evaluations:
        stats = e["stats"]
        status_counts = stats["status_counts"]
        counts = (
            stats["run_count"],
            status_counts.get("succeeded", 0),
            status_counts.get("failed", 0),
            status_counts.get("partial", 0),
            stats["kept_count"],
            stats["dropped_count"],
        )
        rows.append(_row("RISK" if e["is_risky"] else "OK", e["source_id"], counts, name_width))
    return "\n".join(rows)


def build_daily_report_message(day: str, evaluations: list[SourceStatEvaluation]) -> str:
    """일별 전체 소스 리포트 메시지를 만든다. 위험 소스가 있으면 맨 아래 @de2조를 태그한다."""
    lines = [f"*데이터 수집 일일 리포트 ({day})*", "```", _table(evaluations), "```"]

    risky = [e for e in evaluations if e["is_risky"]]
    if risky:
        names = ", ".join(f"`{e['source_id']}`" for e in risky)
        lines.append(f"{de2_group_mention()} 위험 수치를 초과한 소스가 있습니다: {names}")
    return "\n".join(lines)


def build_hourly_alert_message(
    hour_label: str, risky_evaluations: list[SourceStatEvaluation]
) -> str | None:
    """시간별 이상 감지 메시지를 만든다. 위험 소스가 없으면 None(전송하지 않음)."""
    if not risky_evaluations:
        return None

    lines = [
        f"{de2_group_mention()} *데이터 수집 이상 감지 ({hour_label})*",
        "```",
        _table(risky_evaluations),
        "```",
    ]
    return "\n".join(lines)
