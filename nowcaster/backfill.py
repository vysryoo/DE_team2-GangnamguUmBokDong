"""서울 열린데이터광장 원본 CSV를 아카이브 Parquet 규격으로 정규화한다."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

# pyrefly: ignore [missing-import]
import pandas as pd
# pyrefly: ignore [missing-import]
import pyarrow as pa
# pyrefly: ignore [missing-import]
import pyarrow.compute as pc

_COLUMN_RENAME = {
    "일자": "YMD",
    "시간": "TT",
    "행정동코드": "H_DNG_CD",
    "250M격자": "CELL_ID",
    "생활인구합계": "SPOP",
    "남자 0~9세": "M00",
    "남자 10~14세": "M10",
    "남자 15~19세": "M15",
    "남자 20~24세": "M20",
    "남자 25~29세": "M25",
    "남자 30~34세": "M30",
    "남자 35~39세": "M35",
    "남자 40~44세": "M40",
    "남자 45~49세": "M45",
    "남자 50~54세": "M50",
    "남자 55~59세": "M55",
    "남자 60~64세": "M60",
    "남자 65~69세": "M65",
    "남자 70세 이상": "M70",
    "여자 0~9세": "F00",
    "여자 10~14세": "F10",
    "여자 15~19세": "F15",
    "여자 20~24세": "F20",
    "여자 25~29세": "F25",
    "여자 30~34세": "F30",
    "여자 35~39세": "F35",
    "여자 40~44세": "F40",
    "여자 45~49세": "F45",
    "여자 50~54세": "F50",
    "여자 55~59세": "F55",
    "여자 60~64세": "F60",
    "여자 65~69세": "F65",
    "여자 70세 이상": "F70",
}

_ID_COLUMNS = ("YMD", "TT", "H_DNG_CD", "CELL_ID")


def read_source_csv(path: str | Path) -> pa.Table:
    """공공데이터 원본 CSV 파일을 읽어 표준 물리 스키마의 Arrow Table로 정규화한다."""
    frame = pd.read_csv(path, encoding="euc-kr", dtype=str, na_values=["*"])
    frame = frame.rename(columns=_COLUMN_RENAME)
    for col in frame.columns:
        if col not in _ID_COLUMNS:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return pa.Table.from_pandas(frame, preserve_index=False)


def add_estimation_columns(table: pa.Table) -> pa.Table:
    """실측 데이터 메타데이터(is_estimated=False, estimation_method='actual') 컬럼을 추가한다."""
    n = table.num_rows
    table = table.append_column("is_estimated", pa.array([False] * n, type=pa.bool_()))
    table = table.append_column("estimation_method", pa.array(["actual"] * n, type=pa.string()))
    return table


def group_rows_by_date(table: pa.Table) -> dict[date, pa.Table]:
    """테이블의 데이터를 YMD 컬럼 기준 일자별 서브 테이블로 분리한다."""
    if table.num_rows == 0:
        return {}

    ymd_values = table.column("YMD").to_pylist()
    grouped: dict[date, pa.Table] = {}
    for ymd in sorted(set(ymd_values)):
        target_date = datetime.strptime(ymd, "%Y%m%d").date()
        grouped[target_date] = table.filter(pc.equal(table.column("YMD"), ymd))
    return grouped

