"""홈 화면 '이번 달 vs 지난 달 누적 매출' 위젯 검증."""
from __future__ import annotations

import pandas as pd

from modules.common import sales_query as sq
from modules.common.dates import month_range
from modules.dashboard.month_cumulative import CUR_LABEL, PREV_LABEL, build_month_cumulative


def _df(rows):
    return pd.DataFrame(rows, columns=["dt", "sales_amount"]).assign(dt=lambda d: pd.to_datetime(d["dt"]))


def test_cumulative_math_and_same_day_comparison():
    # 지난 달(2026-08, 31일) / 이번 달(2026-09) / 오늘 = 9월 3일
    df = _df([
        ("2026-08-01", 100), ("2026-08-02", 200), ("2026-08-03", 300), ("2026-08-31", 1000),
        ("2026-09-01", 50), ("2026-09-03", 150),
        ("2026-07-31", 99999),  # 범위 밖: 무시
        ("2026-09-04", 99999),  # 오늘 이후: 무시
    ])
    long_df, info = build_month_cumulative(df, pd.Timestamp("2026-09-03"))

    prev = long_df[long_df["label"] == PREV_LABEL].set_index("day")["cumulative"]
    cur = long_df[long_df["label"] == CUR_LABEL].set_index("day")["cumulative"]

    assert len(prev) == 31 and len(cur) == 3          # 지난 달은 월말까지, 이번 달은 오늘까지
    assert prev.loc[3] == 600 and prev.loc[31] == 1600
    assert cur.loc[1] == 50 and cur.loc[2] == 50 and cur.loc[3] == 200
    assert info["cur_total"] == 200
    assert info["prev_same_day_total"] == 600
    assert info["prev_total"] == 1600
    assert info["diff_amount"] == -400
    assert round(info["diff_pct"], 2) == round(-400 / 600 * 100, 2)


def test_same_day_clamped_when_prev_month_shorter():
    # 3월 31일 기준 → 지난 달 2월(28일)에는 31일이 없으므로 28일 누적으로 비교
    df = _df([("2026-02-28", 700), ("2026-03-31", 100)])
    _, info = build_month_cumulative(df, pd.Timestamp("2026-03-31"))
    assert info["same_day"] == 28 and info["prev_same_day_total"] == 700
    assert info["cur_total"] == 100


def test_empty_input():
    long_df, info = build_month_cumulative(pd.DataFrame(columns=["dt", "sales_amount"]), pd.Timestamp("2026-09-05"))
    assert info["cur_total"] == 0 and info["prev_total"] == 0 and info["diff_pct"] is None
    assert not long_df.empty  # 0 으로 채운 선은 그려진다


def test_home_widget_matches_canonical_loader(conn, months):
    """실제 DB: 데이터가 있는 마지막 달을 '이번 달'로 두고 누적 최종값 = 정본(소매+도매) 월매출."""
    import DAILY_CIGAR as home

    y, m = months[-1]
    f, t = month_range(y, m)
    today = pd.Timestamp(t)
    # 홈 화면과 같은 방식으로 지난 달 초부터 로드
    py, pm = (y, m - 1) if m > 1 else (y - 1, 12)
    pf, _ = month_range(py, pm)
    df = home.load_period_sales(conn, pf, t)

    _, info = build_month_cumulative(df, today)

    r = sq.load_retail_sales(conn, f, t)
    w = sq.load_wholesale_sales(conn, f, t)
    expected_cur = float(r["net_sales_amount"].sum() + w["net_sales_amount"].sum())
    assert abs(info["cur_total"] - expected_cur) <= 1.0, f"{info['cur_total']:,.0f} ≠ {expected_cur:,.0f}"

    pr = sq.load_retail_sales(conn, pf, month_range(py, pm)[1])
    pw = sq.load_wholesale_sales(conn, pf, month_range(py, pm)[1])
    expected_prev = float(pr["net_sales_amount"].sum() + pw["net_sales_amount"].sum())
    assert abs(info["prev_total"] - expected_prev) <= 1.0, f"{info['prev_total']:,.0f} ≠ {expected_prev:,.0f}"
