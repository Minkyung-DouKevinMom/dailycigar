"""분석 > 거래처분석: 일자별 비교 그래프의 '2번 이상 구매' 필터 검증."""
from __future__ import annotations

import pandas as pd

from modules.analytics.partner_analysis_view import (
    MIN_PURCHASES_FOR_CHART,
    count_purchase_days,
    filter_repeat_partners,
)


def _daily(rows):
    return pd.DataFrame(rows, columns=["date", "partner_name", "sales"]).assign(
        date=lambda d: pd.to_datetime(d["date"])
    )


SAMPLE = _daily([
    ("2026-01-01", "A상회", 1000),   # A: 3일
    ("2026-02-01", "A상회", 2000),
    ("2026-03-01", "A상회", 3000),
    ("2026-01-15", "B유통", 5000),   # B: 2일
    ("2026-02-15", "B유통", 5000),
    ("2026-03-20", "C상사", 9000),   # C: 1일 → 제외
])


def test_count_purchase_days():
    c = count_purchase_days(SAMPLE)
    assert c["A상회"] == 3 and c["B유통"] == 2 and c["C상사"] == 1


def test_filter_keeps_two_or_more_and_reports_excluded():
    out, excluded = filter_repeat_partners(SAMPLE)
    assert MIN_PURCHASES_FOR_CHART == 2
    assert sorted(out["partner_name"].unique()) == ["A상회", "B유통"]
    assert excluded == ["C상사"]
    assert len(out) == 5                      # C 의 1행만 빠짐
    assert out["sales"].sum() == SAMPLE["sales"].sum() - 9000


def test_zero_sales_days_do_not_count_as_purchase():
    # 같은 거래처가 2일이지만 한 날은 매출 0 → 구매일수 1 → 제외
    df = _daily([("2026-01-01", "가게", 0), ("2026-01-05", "가게", 100)])
    assert count_purchase_days(df)["가게"] == 1
    out, excluded = filter_repeat_partners(df)
    assert out.empty and excluded == ["가게"]


def test_same_day_multiple_rows_count_once():
    df = _daily([("2026-01-01", "가게", 100), ("2026-01-01", "가게", 200)])
    assert count_purchase_days(df)["가게"] == 1     # 같은 날 두 건이어도 1일


def test_custom_threshold_and_empty_input():
    out, excluded = filter_repeat_partners(SAMPLE, min_purchases=3)
    assert sorted(out["partner_name"].unique()) == ["A상회"]
    assert excluded == ["B유통", "C상사"]

    empty = pd.DataFrame(columns=["date", "partner_name", "sales"])
    o, e = filter_repeat_partners(empty)
    assert o.empty and e == []
    assert count_purchase_days(empty).empty


def test_matches_cycle_summary_purchase_count(conn):
    """실제 DB: 필터 기준(구매일수)이 구매주기 요약표의 '구매일수'와 같아야 한다."""
    from modules.analytics.partner_analysis_view import _build_partner_cycle_summary, _find_date_column

    df = pd.read_sql_query("SELECT * FROM v_wholesale_sales", conn)
    if df.empty:
        return
    df["sales"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
    df["partner_name"] = df["partner_name"].fillna("(거래처 없음)")
    date_col = _find_date_column(df)
    line_df = df.assign(date=pd.to_datetime(df[date_col], errors="coerce")).dropna(subset=["date"])
    line_df["date"] = line_df["date"].dt.normalize()

    daily = line_df.groupby(["date", "partner_name"], dropna=False)["sales"].sum().reset_index()
    counts = count_purchase_days(daily)
    summary = _build_partner_cycle_summary(line_df).set_index("거래처")["구매일수"]
    for name, n in counts.items():
        assert int(summary.loc[name]) == int(n), name

    out, excluded = filter_repeat_partners(daily)
    assert set(out["partner_name"]).isdisjoint(set(excluded))
    for name in out["partner_name"].unique():
        assert counts[name] >= MIN_PURCHASES_FOR_CHART
