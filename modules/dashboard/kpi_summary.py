"""
홈 상단 KPI 카드 집계 + 이전 기간 대비 증감(delta) 계산.

기존에는 같은 정보를 KPI 카드와 '인사이트' 문장 3개(30일 증감률/마진율/채널 비중)로
두 번 보여줬는데, 증감률·마진율은 카드의 delta·caption 으로 흡수하고 채널 비중은
채널 비중 도넛이 담당한다.

SQL 직접 실행 없음 — 정본 로더로 만든 판매 DataFrame(sales_type, sales_amount, margin_amount)만 받는다.
"""
from __future__ import annotations

import pandas as pd


def summarize_kpis(df: pd.DataFrame) -> dict:
    """
    df: sales_type('소매'/'도매'), sales_amount, margin_amount (한 기간분)
    반환: sales, margin, margin_rate(%), deal_count, avg_ticket,
          retail_sales, wholesale_sales, retail_margin, wholesale_margin, retail_count, wholesale_count
    """
    empty = df is None or df.empty
    if empty:
        d = pd.DataFrame(columns=["sales_type", "sales_amount", "margin_amount"])
    else:
        d = df.copy()
        d["sales_amount"] = pd.to_numeric(d["sales_amount"], errors="coerce").fillna(0.0)
        d["margin_amount"] = pd.to_numeric(d["margin_amount"], errors="coerce").fillna(0.0)

    def _by(t: str, col: str) -> float:
        return float(d.loc[d["sales_type"] == t, col].sum()) if not d.empty else 0.0

    sales = float(d["sales_amount"].sum()) if not d.empty else 0.0
    margin = float(d["margin_amount"].sum()) if not d.empty else 0.0
    deal_count = int(len(d))
    return {
        "sales": sales,
        "margin": margin,
        "margin_rate": (margin / sales * 100) if sales else 0.0,
        "deal_count": deal_count,
        "avg_ticket": (sales / deal_count) if deal_count else 0.0,
        "retail_sales": _by("소매", "sales_amount"),
        "wholesale_sales": _by("도매", "sales_amount"),
        "retail_margin": _by("소매", "margin_amount"),
        "wholesale_margin": _by("도매", "margin_amount"),
        "retail_count": int((d["sales_type"] == "소매").sum()) if not d.empty else 0,
        "wholesale_count": int((d["sales_type"] == "도매").sum()) if not d.empty else 0,
    }


def pct_change(recent: float, prior: float) -> float | None:
    """이전 기간 대비 증감률(%). 이전 값이 0이면 비교 불가(None)."""
    if not prior:
        return None
    return (recent - prior) / abs(prior) * 100


def format_delta_pct(recent: float, prior: float) -> str | None:
    """st.metric 의 delta 문자열. 비교 불가면 None (화살표 미표시)."""
    p = pct_change(recent, prior)
    if p is None:
        return None
    return f"{p:+.1f}%"


def format_delta_count(recent: int, prior: int) -> str | None:
    if not prior:
        return None
    return f"{recent - prior:+,}건"


def split_period(df: pd.DataFrame, today: pd.Timestamp, window_days: int = 30) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    dt 컬럼을 가진 df 를 (최근 window_days, 그 직전 window_days) 두 구간으로 나눈다.
    최근: today-(window_days-1) ~ today,  이전: today-(2*window_days-1) ~ today-window_days
    """
    today = pd.Timestamp(today).normalize()
    recent_start = today - pd.Timedelta(days=window_days - 1)
    prior_end = today - pd.Timedelta(days=window_days)
    prior_start = today - pd.Timedelta(days=2 * window_days - 1)

    if df is None or df.empty:
        return df, df
    d = df.copy()
    d["dt"] = pd.to_datetime(d["dt"])
    recent = d[(d["dt"] >= recent_start) & (d["dt"] <= today)]
    prior = d[(d["dt"] >= prior_start) & (d["dt"] <= prior_end)]
    return recent, prior
