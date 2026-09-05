"""홈 KPI 카드 집계 + 이전 기간 대비 증감 검증."""
from __future__ import annotations

import pandas as pd

from modules.dashboard.kpi_summary import (
    format_delta_count,
    format_delta_pct,
    pct_change,
    split_period,
    summarize_kpis,
)

TODAY = pd.Timestamp("2026-09-05")


def _df(rows):
    return pd.DataFrame(rows, columns=["dt", "sales_type", "sales_amount", "margin_amount"]).assign(
        dt=lambda d: pd.to_datetime(d["dt"])
    )


def test_summarize_kpis():
    k = summarize_kpis(_df([
        ("2026-09-01", "소매", 600, 300),
        ("2026-09-02", "소매", 200, 100),
        ("2026-09-03", "도매", 200, 20),
    ]))
    assert k["sales"] == 1000 and k["margin"] == 420
    assert k["margin_rate"] == 42.0
    assert k["deal_count"] == 3 and k["avg_ticket"] == 1000 / 3
    assert k["retail_sales"] == 800 and k["wholesale_sales"] == 200
    assert k["retail_margin"] == 400 and k["wholesale_margin"] == 20
    assert k["retail_count"] == 2 and k["wholesale_count"] == 1


def test_summarize_empty():
    k = summarize_kpis(pd.DataFrame(columns=["dt", "sales_type", "sales_amount", "margin_amount"]))
    assert k["sales"] == 0 and k["margin_rate"] == 0 and k["deal_count"] == 0 and k["avg_ticket"] == 0


def test_pct_change_and_delta_formats():
    assert pct_change(110, 100) == 10.0
    assert pct_change(90, 100) == -10.0
    assert pct_change(100, 0) is None            # 이전 값 0 → 비교 불가
    # 적자 개선(-100 → -50)은 '+' 로 나와야 한다 (분모를 절대값으로 두는 이유)
    assert pct_change(-50, -100) == 50.0
    assert pct_change(-150, -100) == -50.0       # 적자 확대는 '-'
    assert format_delta_pct(110, 100) == "+10.0%"
    assert format_delta_pct(90, 100) == "-10.0%"
    assert format_delta_pct(1, 0) is None
    assert format_delta_count(120, 100) == "+20건"
    assert format_delta_count(80, 100) == "-20건"
    assert format_delta_count(5, 0) is None


def test_split_period_windows_do_not_overlap():
    rows = [(TODAY - pd.Timedelta(days=i), "소매", 1, 0) for i in range(0, 70)]
    recent, prior = split_period(_df(rows), TODAY, window_days=30)
    assert len(recent) == 30 and len(prior) == 30
    assert recent["dt"].min() == TODAY - pd.Timedelta(days=29) and recent["dt"].max() == TODAY
    assert prior["dt"].max() == TODAY - pd.Timedelta(days=30)
    assert prior["dt"].min() == TODAY - pd.Timedelta(days=59)
    assert set(recent["dt"]).isdisjoint(set(prior["dt"]))


def test_home_kpi_matches_canonical_loader(conn):
    """실제 DB: 카드 값이 정본 로더의 최근 30일 매출·마진과 일치."""
    import DAILY_CIGAR as home
    from modules.common import sales_query as sq

    today = pd.Timestamp("2026-09-05")
    f = (today - pd.Timedelta(days=29)).strftime("%Y-%m-%d")
    t = today.strftime("%Y-%m-%d")
    k = summarize_kpis(home.load_period_sales(conn, f, t))

    r = sq.load_retail_sales(conn, f, t)
    w = sq.load_wholesale_sales(conn, f, t)
    assert abs(k["retail_sales"] - r["net_sales_amount"].sum()) <= 1.0
    assert abs(k["wholesale_sales"] - w["net_sales_amount"].sum()) <= 1.0
    assert abs(k["sales"] - (r["net_sales_amount"].sum() + w["net_sales_amount"].sum())) <= 1.0
    assert abs(k["margin"] - (r["retail_gross_profit_krw"].sum() + w["gross_profit_krw"].sum())) <= 1.0
