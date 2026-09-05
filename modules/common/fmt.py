"""
포맷/형변환 공통 유틸 (정본).

fmt_krw / format_krw / _fmt / _currency, safe_float / _safe_float, safe_int / _safe_int, safe_str,
normalize_code 가 여러 모듈에 복사되어 있던 것을 여기로 통일합니다.
"""
from __future__ import annotations

import pandas as pd


def fmt_krw(value, default: str = "₩0") -> str:
    """원화 표시: ₩1,234,567 (소수점 없음). None/NaN/변환 실패 시 default."""
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return f"₩{float(value):,.0f}"
    except Exception:
        return default


def fmt_number(value, default: str = "0") -> str:
    """천단위 구분만: 1,234,567."""
    try:
        return f"{int(float(value or 0)):,}"
    except Exception:
        return default


def fmt_count(value, default: str = "0건") -> str:
    try:
        return f"{int(value):,}건"
    except Exception:
        return default


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        if isinstance(value, str) and value.strip() == "":
            return float(default)
        if isinstance(value, float) and pd.isna(value):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        if isinstance(value, str) and value.strip() == "":
            return int(default)
        if isinstance(value, float) and pd.isna(value):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def safe_str(value, default: str = "") -> str:
    if value is None:
        return default
    try:
        if isinstance(value, float) and pd.isna(value):
            return default
    except Exception:
        pass
    return str(value).strip()


def normalize_code(series: pd.Series) -> pd.Series:
    """상품코드 정규화: 문자열화 → 공백 제거 → 대문자. (분석 모듈들의 normalize_code 와 동일)"""
    return series.fillna("").astype(str).str.strip().str.upper()


def apply_currency_format(df: pd.DataFrame, cols) -> pd.DataFrame:
    """지정 컬럼을 ₩ 포맷 문자열로 변환한 복사본 반환 (표시 전용. 정렬이 필요한 표에는 사용하지 말 것)."""
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].apply(fmt_krw)
    return out
