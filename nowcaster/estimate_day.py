"""과거 아카이브 데이터를 바탕으로 일자별 생활인구 격자 추정 테이블을 생성한다."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date

# pyrefly: ignore [missing-import]
import numpy as np
import pandas as pd

KEY_COLS = ["H_DNG_CD", "CELL_ID", "TT"]
VALUE_COLS = [
    "SPOP",
    "M00", "M10", "M15", "M20", "M25", "M30", "M35", "M40", "M45", "M50", "M55", "M60", "M65", "M70",
    "F00", "F10", "F15", "F20", "F25", "F30", "F35", "F40", "F45", "F50", "F55", "F60", "F65", "F70",
]

_WEIGHTS = {1: 0.4, 2: 0.3, 3: 0.2, 4: 0.1}


_RunningSumCount = tuple[pd.DataFrame, pd.DataFrame]


def _accumulate(running: _RunningSumCount | None, frame: pd.DataFrame | None) -> _RunningSumCount | None:
    """하루치 프레임을 누적 합/카운트에 더한다. 프레임이 비었으면 누적을 그대로 반환한다."""
    if frame is None or frame.empty:
        return running
    value_cols = [c for c in VALUE_COLS if c in frame.columns]
    keyed = frame.set_index(KEY_COLS)[value_cols]
    count = keyed.notna().astype("int64")
    values = keyed.fillna(0.0)
    if running is None:
        return values, count
    running_sum, running_count = running
    return running_sum.add(values, fill_value=0.0), running_count.add(count, fill_value=0)


def _finalize(running: _RunningSumCount | None) -> pd.DataFrame | None:
    """누적 합/카운트에서 평균 DataFrame을 만든다. 누적이 없으면 None."""
    if running is None:
        return None
    running_sum, running_count = running
    return running_sum / running_count.where(running_count > 0)


def historical_average_cached(
    pattern: str,
    dates: Iterable[date],
    read_frame: Callable[[date], pd.DataFrame | None],
    load_cache: Callable[[str], tuple[pd.DataFrame | None, pd.DataFrame | None, list[str]]],
    save_cache: Callable[[str, pd.DataFrame, pd.DataFrame, list[str]], None],
) -> pd.DataFrame | None:
    """캐시된 누적 합/카운트에 새로 추가된 날짜만 더해 과거 전체 평균을 유지한다.

    매번 전체 archive를 다시 읽으면 실행 시간이 archive 크기에 비례해 늘고, 프레임을
    한꺼번에 들고 있으면 메모리가 부족해진다. 날짜를 하나씩 누적하고 반영한 날짜를
    패턴별로 캐시해 다음 실행부터는 새 날짜만 읽는다.

    Args:
        pattern: 캐시를 구분하는 키(예: "weekday"/"special")
        dates: 이 패턴에 매칭되는 현재 archive 날짜 전체. 캐시에 없는 날짜만 읽음
        read_frame: 날짜 하나의 아카이브를 읽는 함수
        load_cache: `pattern`의 (누적 합, 누적 카운트, 반영한 날짜 문자열 목록)을 읽는
            함수. 캐시가 없으면 (None, None, [])를 반환해야 함
        save_cache: 갱신된 누적 합/카운트와 날짜 목록을 저장하는 함수. 새로 반영한
            날짜가 없으면 호출되지 않음
    Returns:
        격자·시간대별 평균 DataFrame. 누적할 데이터가 없으면 None
    """
    dates = sorted(set(dates))
    cached_sum, cached_count, included = load_cache(pattern)
    included_dates = {date.fromisoformat(d) for d in included}
    new_dates = [d for d in dates if d not in included_dates]

    running: _RunningSumCount | None = (cached_sum, cached_count) if cached_sum is not None else None
    for target in new_dates:
        running = _accumulate(running, read_frame(target))
        # 데이터가 없는 날짜도 기록해야 실행마다 다시 읽지 않음
        included_dates.add(target)

    if new_dates and running is not None:
        save_cache(pattern, running[0], running[1], sorted(d.isoformat() for d in included_dates))

    return _finalize(running)


def _prep(frames: list[pd.DataFrame | None], suffix_prefix: str) -> list[pd.DataFrame]:
    """데이터프레임의 키를 인덱스로 설정하고 컬럼명에 순번 접미사를 부여한다."""
    prepped = []
    for i, frame in enumerate(frames, start=1):
        if frame is None or frame.empty:
            continue
        present_cols = [c for c in VALUE_COLS if c in frame.columns]
        if not present_cols:
            continue
        prepped.append(frame.set_index(KEY_COLS)[present_cols].add_suffix(f"__{suffix_prefix}{i}"))
    return prepped


def build_nowcast_table(
    candidate_frames: list[pd.DataFrame | None],
    extended_frames: list[pd.DataFrame | None] = (),
    historical_avg_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """후보 주차 및 폴백 데이터들을 결합하여 일자별 생활인구 추정 테이블을 생성한다.

    격자·시간대마다 1~4주 전 값을 0.4/0.3/0.2/0.1로 가중평균하고, 빠진 주차는 남은
    가중치로 정규화한다. 4주가 모두 없으면 5~8주 전 중 가장 가까운 값, 그다음 과거
    전체 평균, 마지막으로 0을 쓴다. 판정 근거는 SPOP 기준 `estimation_method`에 남긴다.

    Args:
        candidate_frames: 1~4주 전 후보 DataFrame 목록 (결측은 None)
        extended_frames: 5~8주 전 확장 후보 DataFrame 목록
        historical_avg_frame: 과거 전체 평균 DataFrame
    Returns:
        추정치 및 추정 방식 메타데이터가 포함된 DataFrame
    """
    n_candidates = len(candidate_frames)

    candidate_prepped = _prep(candidate_frames, "c")
    extended_prepped = _prep(extended_frames, "e")
    historical_prepped = _prep([historical_avg_frame] if historical_avg_frame is not None else [], "h")

    all_prepped = candidate_prepped + extended_prepped + historical_prepped
    if not all_prepped:
        return pd.DataFrame(columns=[*KEY_COLS, *VALUE_COLS, "is_estimated", "estimation_method"])

    wide = pd.concat(all_prepped, axis=1, join="outer")

    spop_cand_cols = [f"SPOP__c{i}" for i in range(1, n_candidates + 1) if f"SPOP__c{i}" in wide.columns]
    if spop_cand_cols:
        count_valid = wide[spop_cand_cols].notna().sum(axis=1)
    else:
        count_valid = pd.Series(0, index=wide.index)

    method = pd.Series("no_data", index=wide.index, dtype=object)
    method[count_valid == n_candidates] = "weighted_avg"
    method[count_valid == 1] = "single_week_fallback"
    method[(count_valid > 1) & (count_valid < n_candidates)] = "reweighted_avg"
    need_fallback = count_valid == 0

    ext_prefix_cols = [c for c in wide.columns if "__e" in c]
    if need_fallback.any() and ext_prefix_cols:
        spop_ext_cols = [c for c in ext_prefix_cols if c.startswith("SPOP__e")]
        if spop_ext_cols:
            has_ext = wide.loc[need_fallback, spop_ext_cols].notna().any(axis=1)
            method.loc[has_ext[has_ext].index] = "extended_lookback_fallback"
            need_fallback.loc[has_ext[has_ext].index] = False

    if need_fallback.any() and "SPOP__h1" in wide.columns:
        has_hist = wide.loc[need_fallback, "SPOP__h1"].notna()
        method.loc[has_hist[has_hist].index] = "grid_historical_avg"

    result = pd.DataFrame(index=wide.index)
    for col in VALUE_COLS:
        cand_cols = [f"{col}__c{i}" for i in range(1, n_candidates + 1) if f"{col}__c{i}" in wide.columns]
        if cand_cols:
            week_weights = np.array([_WEIGHTS[int(c.rsplit("__c", 1)[1])] for c in cand_cols])
            cand_values = wide[cand_cols]
            valid = cand_values.notna()
            weight_sum = (valid * week_weights).sum(axis=1)
            weighted_sum = (cand_values.fillna(0.0) * week_weights).sum(axis=1)
            value = weighted_sum / weight_sum.where(weight_sum > 0)
        else:
            value = pd.Series(np.nan, index=wide.index)

        ext_cols = [f"{col}__e{i}" for i in range(1, len(extended_frames) + 1) if f"{col}__e{i}" in wide.columns]
        if ext_cols:
            ext_value = wide[ext_cols].bfill(axis=1).iloc[:, 0]
            value = value.where(value.notna(), ext_value)

        hist_col = f"{col}__h1"
        if hist_col in wide.columns:
            value = value.where(value.notna(), wide[hist_col])

        result[col] = value.fillna(0.0)

    result["is_estimated"] = True
    result["estimation_method"] = method
    return result.reset_index()
