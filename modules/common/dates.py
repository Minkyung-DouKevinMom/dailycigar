"""
날짜 공통 유틸 (정본).
"""
from __future__ import annotations

from typing import Tuple

import pandas as pd


def month_range(year: int, month: int) -> Tuple[str, str]:
    """(YYYY-MM-01, 해당 월 말일) 문자열 튜플."""
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def prev_month(year: int, month: int) -> Tuple[int, int]:
    dt = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(months=1)
    return dt.year, dt.month


def monthify(series: pd.Series) -> pd.Series:
    """날짜 시리즈 → 'YYYY-MM' 문자열 시리즈."""
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m")


def today_str() -> str:
    return pd.Timestamp.today().strftime("%Y-%m-%d")


def normalize_date_str(series: pd.Series) -> pd.Series:
    """여러 형식의 날짜 문자열/타임스탬프 → 'YYYY-MM-DD'. 파싱 실패는 NaN."""
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")
