"""홈 화면 '최근 30일 채널 비중' (매출/마진 기준) 검증."""
from __future__ import annotations

import pandas as pd

from modules.dashboard.channel_share import build_channel_share


def _df(rows):
    return pd.DataFrame(rows, columns=["sales_type", "sales_amount", "margin_amount"])


def test_ratios_and_margin_rate():
    s = build_channel_share(_df([
        ("소매", 600, 300), ("소매", 200, 100),   # 소매 매출 800, 마진 400 (마진율 50%)
        ("도매", 200, 20),                        # 도매 매출 200, 마진 20  (마진율 10%)
    ])).set_index("sales_type")
    assert s.loc["소매", "sales"] == 800 and s.loc["도매", "sales"] == 200
    assert s.loc["소매", "sales_ratio"] == 80 and s.loc["도매", "sales_ratio"] == 20
    # 마진 기준 비중은 매출 기준과 달라야 정상 (도매 마진율이 낮음)
    assert round(s.loc["소매", "margin_ratio"], 2) == round(400 / 420 * 100, 2)
    assert round(s.loc["도매", "margin_ratio"], 2) == round(20 / 420 * 100, 2)
    assert s.loc["소매", "margin_rate"] == 50 and s.loc["도매", "margin_rate"] == 10
    assert round(s["sales_ratio"].sum()) == 100 and round(s["margin_ratio"].sum()) == 100


def test_missing_channel_and_empty():
    s = build_channel_share(_df([("소매", 100, 40)])).set_index("sales_type")
    assert s.loc["도매", "sales"] == 0 and s.loc["도매", "sales_ratio"] == 0 and s.loc["도매", "margin_rate"] == 0
    assert s.loc["소매", "sales_ratio"] == 100

    e = build_channel_share(pd.DataFrame(columns=["sales_type", "sales_amount", "margin_amount"]))
    assert list(e["sales_type"]) == ["소매", "도매"] and (e["sales"] == 0).all() and (e["sales_ratio"] == 0).all()


def test_negative_total_margin_gives_zero_margin_ratio():
    s = build_channel_share(_df([("소매", 100, -50), ("도매", 100, 10)]))
    assert (s["margin_ratio"] == 0).all()          # 합계 마진 ≤ 0 → 비중 정의 안 함
    assert s.set_index("sales_type").loc["소매", "margin_rate"] == -50


def test_home_channel_share_matches_canonical_loader(conn, months):
    """실제 DB: 마지막 데이터 월 기준 채널별 매출/마진 = 정본 로더."""
    import DAILY_CIGAR as home
    from modules.common import sales_query as sq
    from modules.common.dates import month_range

    y, m = months[-1]
    f, t = month_range(y, m)
    s = build_channel_share(home.load_period_sales(conn, f, t)).set_index("sales_type")
    r = sq.load_retail_sales(conn, f, t)
    w = sq.load_wholesale_sales(conn, f, t)
    assert abs(s.loc["소매", "sales"] - r["net_sales_amount"].sum()) <= 1.0
    assert abs(s.loc["소매", "margin"] - r["retail_gross_profit_krw"].sum()) <= 1.0
    assert abs(s.loc["도매", "sales"] - w["net_sales_amount"].sum()) <= 1.0
    assert abs(s.loc["도매", "margin"] - w["gross_profit_krw"].sum()) <= 1.0
