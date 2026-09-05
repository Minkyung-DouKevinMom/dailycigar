"""홈 화면 '재고 소진 임박' 계산 검증."""
from __future__ import annotations

import pandas as pd

from modules.dashboard.stock_alert import OUT_COLUMNS, calc_stock_depletion

TODAY = pd.Timestamp("2026-09-05")


def _stock(rows):
    return pd.DataFrame(rows, columns=["product_code", "product_name", "size_name", "current_stock"])


def _sales(rows):
    return pd.DataFrame(rows, columns=["dt", "product_code", "qty"]).assign(dt=lambda d: pd.to_datetime(d["dt"]))


def test_velocity_and_days_left():
    # 최근 30일 60개 판매 → 일평균 2개, 재고 30개 → 15일 후 소진
    stock = _stock([("A", "가", "R", 30)])
    sales = _sales([(TODAY - pd.Timedelta(days=i), "A", 2) for i in range(30)])
    out = calc_stock_depletion(stock, sales, TODAY)
    assert list(out.columns) == OUT_COLUMNS
    r = out.iloc[0]
    assert r["qty_sold"] == 60 and r["daily_velocity"] == 2.0 and r["days_left"] == 15.0
    assert r["depletion_date"] == TODAY + pd.Timedelta(days=15)


def test_sorted_by_days_left_and_top_n():
    stock = _stock([(c, c, "R", s) for c, s in [("A", 10), ("B", 60), ("C", 5), ("D", 20), ("E", 40), ("F", 1)]])
    # 모두 30일간 30개 판매 = 일평균 1개 → 소진일수 = 재고
    sales = _sales([(TODAY - pd.Timedelta(days=i), c, 1) for i in range(30) for c in "ABCDEF"])
    out = calc_stock_depletion(stock, sales, TODAY, top_n=5, threshold_days=None)
    assert out["product_code"].tolist() == ["F", "C", "A", "D", "E"]   # 1,5,10,20,40 (B=60 은 top5 밖)


def test_threshold_filters_slow_movers():
    stock = _stock([("A", "가", "R", 10), ("B", "나", "R", 300)])
    sales = _sales([(TODAY - pd.Timedelta(days=i), c, 1) for i in range(30) for c in ["A", "B"]])
    out = calc_stock_depletion(stock, sales, TODAY, threshold_days=45)
    assert out["product_code"].tolist() == ["A"]      # B 는 300일치 → 제외
    # 기본 임계값(90일)에서도 300일치는 제외
    assert calc_stock_depletion(stock, sales, TODAY)["product_code"].tolist() == ["A"]


def test_excludes_zero_velocity_and_zero_or_negative_stock():
    stock = _stock([
        ("A", "가", "R", 10),    # 판매 없음 → 제외 (장기 미판매 쪽)
        ("B", "나", "R", 0),     # 재고 0 → 제외
        ("C", "다", "R", -5),    # 음수 재고 → 제외
        ("D", "라", "R", 10),    # 정상
    ])
    sales = _sales([(TODAY - pd.Timedelta(days=1), "D", 30), (TODAY - pd.Timedelta(days=1), "B", 5)])
    out = calc_stock_depletion(stock, sales, TODAY)
    assert out["product_code"].tolist() == ["D"]


def test_out_of_window_and_non_positive_qty_ignored():
    stock = _stock([("A", "가", "R", 10)])
    sales = _sales([
        (TODAY - pd.Timedelta(days=40), "A", 100),   # 30일 창 밖
        (TODAY + pd.Timedelta(days=1), "A", 100),    # 미래
        (TODAY - pd.Timedelta(days=2), "A", -3),     # 반품/음수
        (TODAY - pd.Timedelta(days=2), "A", 30),     # 유효
    ])
    out = calc_stock_depletion(stock, sales, TODAY)
    assert out.iloc[0]["qty_sold"] == 30


def test_empty_inputs():
    empty_stock = pd.DataFrame(columns=["product_code", "product_name", "size_name", "current_stock"])
    assert calc_stock_depletion(empty_stock, _sales([]), TODAY).empty
    assert calc_stock_depletion(_stock([("A", "가", "R", 10)]), None, TODAY).empty


def test_real_db_consistency(conn):
    """실제 DB: 계산 결과의 현재고·판매량이 원천 값과 맞고, 정렬/임계 조건을 지키는지."""
    import DAILY_CIGAR as home

    today = pd.Timestamp("2026-09-05")
    stock_df = home.get_current_stock_df(conn)
    card_df = home.load_period_sales(
        conn, (today - pd.Timedelta(days=29)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    )
    out = calc_stock_depletion(stock_df, card_df, today, threshold_days=45)

    assert out["days_left"].is_monotonic_increasing
    assert (out["days_left"] <= 45).all() and (out["qty_sold"] > 0).all() and (out["current_stock"] > 0).all()
    for _, r in out.iterrows():
        src_stock = float(stock_df.loc[stock_df["product_code"] == r["product_code"], "current_stock"].iloc[0])
        src_qty = float(card_df.loc[card_df["product_code"] == r["product_code"], "qty"].sum())
        assert r["current_stock"] == src_stock
        assert abs(r["qty_sold"] - src_qty) <= 1e-9
        assert abs(r["days_left"] - src_stock / (src_qty / 30)) <= 1e-6
