"""DAG run 기준 시각을 KST Jinja 템플릿으로 만드는 단일 소스.

collector 등은 ``window_start``의 시·분을 UTC로 바꾸지 않고 그대로 S3 키에 쓴다.
같은 시각이 태스크마다 다른 오프셋으로 전달되면 서로 다른 키를 계산하므로, 모든
태스크 빌더는 이 파일의 템플릿만 사용한다.

수동 trigger의 logical_date는 5분 경계가 아닐 수 있어, 모든 컴포넌트가 같은 Silver
key와 inference의 5분 그리드를 쓰도록 기준 시각을 5분 단위로 내림한다.
"""

_KST_RUN_TS = '(dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul"))'
_KST_WINDOW_TS = f"{_KST_RUN_TS}.replace(minute=({_KST_RUN_TS}.minute // 5) * 5, second=0, microsecond=0)"

KST_WINDOW_START = "{{ " + _KST_WINDOW_TS + ".isoformat() }}"
KST_DATE = "{{ " + _KST_WINDOW_TS + '.strftime("%Y-%m-%d") }}'


def _shifted(name: str, amount: int, unit: str) -> str:
    if amount <= 0:
        raise ValueError(f"{name}는 양수여야 한다: {amount}")
    return f"({_KST_WINDOW_TS} - macros.timedelta({unit}={amount}))"


def kst_date_days_ago(days: int) -> str:
    """기준 시각에서 ``days``일 전의 KST 날짜(``%Y-%m-%d``) 템플릿을 반환한다.

    Args:
        days: 며칠 전인지. 양수
    Returns:
        Jinja 템플릿 문자열
    Raises:
        ValueError: ``days``가 양수가 아닌 경우
    """
    return "{{ " + _shifted("days", days, "days") + '.strftime("%Y-%m-%d") }}'


def kst_day_hour_replay_days_ago(days: int, hour: int) -> str:
    """``days``일 전 ``hour``시의 마지막 5분 윈도우(H:55) 시작 시각 템플릿을 반환한다.

    과거 시간대는 어느 5분 윈도우로 조회해도 API 응답이 같음. H+1:00을 쓰면 23시
    결과가 다음 날짜 파티션에 쓰이므로 H:55를 사용함.

    Args:
        days: 며칠 전인지. 양수
        hour: 대상 시(0~23)
    Returns:
        ``--window-start``에 넣을 Jinja 템플릿 문자열
    Raises:
        ValueError: ``days``가 양수가 아니거나 ``hour``가 0~23 밖인 경우
    """
    if days <= 0:
        raise ValueError(f"days는 양수여야 한다: {days}")
    if not 0 <= hour <= 23:
        raise ValueError(f"hour는 0~23이어야 한다: {hour}")
    window_start = (
        f"({_KST_WINDOW_TS}.replace(hour={hour}, minute=55, second=0, microsecond=0) "
        f"- macros.timedelta(days={days}))"
    )
    return "{{ " + f"{window_start}.isoformat()" + " }}"


def kst_hours_ago_date(hours: int) -> str:
    """기준 시각에서 ``hours``시간 전의 KST 날짜(``%Y-%m-%d``) 템플릿을 반환한다.

    Args:
        hours: 몇 시간 전인지. 양수
    Returns:
        Jinja 템플릿 문자열
    Raises:
        ValueError: ``hours``가 양수가 아닌 경우
    """
    return "{{ " + _shifted("hours", hours, "hours") + '.strftime("%Y-%m-%d") }}'


def kst_hours_ago_hour(hours: int) -> str:
    """기준 시각에서 ``hours``시간 전의 KST 시(``%H``) 템플릿을 반환한다.

    Args:
        hours: 몇 시간 전인지. 양수
    Returns:
        Jinja 템플릿 문자열
    Raises:
        ValueError: ``hours``가 양수가 아닌 경우
    """
    return "{{ " + _shifted("hours", hours, "hours") + '.strftime("%H") }}'


def kst_window_start_shifted(hours: int) -> str:
    """기준 시각을 ``hours``시간 앞으로 당긴 ``--window-start`` 템플릿을 반환한다.

    collector가 쓰는 ``window_last``와 Silver ``dt``/``hh`` 파티션도 함께 당겨지므로
    데이터 시각과 파티션이 어긋나지 않음.

    Args:
        hours: 몇 시간 앞으로 당길지. 양수
    Returns:
        ``--window-start``에 넣을 Jinja 템플릿 문자열
    Raises:
        ValueError: ``hours``가 양수가 아닌 경우
    """
    return "{{ " + _shifted("hours", hours, "hours") + ".isoformat() }}"
