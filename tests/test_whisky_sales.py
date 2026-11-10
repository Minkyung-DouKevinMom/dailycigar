"""
위스키 매출/이익 정본(load_sales / summarize_*) 테스트.

기준: 매출 = 판매금액 ÷ 1.1, 원가 = 차감 ml × ml당 평균 매입가, 마진 = 매출 − 원가.
기타 출고·실사 조정은 매출·마진에 넣지 않고 손실 원가로 따로 집계.
HOME·대시보드가 쓰는 요약(전체/상품별/월별)은 같은 정본에서 나오므로 합계가 서로 같아야 한다.
"""
from __future__ import annotations

import pandas as pd
import pytest

from modules.whisky import whisky_db as wdb


@pytest.fixture
def sample(tmp_path, monkeypatch):
    monkeypatch.setenv("WHISKY_DB_PATH", str(tmp_path / "whisky_sales.db"))
    a = wdb.insert_product("A 위스키", bottle_ml=700, glass_ml=30, bottle_price=154000, glass_price=11000)
    b = wdb.insert_product("B 위스키", bottle_ml=700, glass_ml=30, bottle_price=220000, glass_price=16500)
    wdb.insert_purchase("2026-11-01", a, bottle_qty=4, unit_cost=70000)   # ml당 100원
    wdb.insert_purchase("2026-11-01", b, bottle_qty=2, unit_cost=140000)  # ml당 200원
    wdb.insert_stock_out("2026-11-20", a, wdb.OUT_GLASS_SALE, qty=10, amount=110000)
    wdb.insert_stock_out("2026-12-02", a, wdb.OUT_BOTTLE_SALE, qty=1, amount=154000)
    wdb.insert_stock_out("2026-12-03", b, wdb.OUT_GLASS_SALE, qty=2, amount=33000)
    wdb.insert_stock_out("2026-12-03", b, wdb.OUT_ETC, qty=1, notes="서비스")  # 30ml 손실
    wdb.apply_stocktake("2026-12-05", a, full_bottles=2, open_ml=350)           # 시스템 1800 → 1750, 50ml 손실
    return a, b


def test_empty_db_returns_empty_frames(tmp_path, monkeypatch):
    monkeypatch.setenv("WHISKY_DB_PATH", str(tmp_path / "empty.db"))
    df = wdb.load_sales()
    assert df.empty and list(df.columns) == wdb.SALES_COLUMNS
    s = wdb.summarize_sales(df)
    assert s["sales"] == 0 and s["margin"] == 0 and s["margin_rate"] == 0 and s["sale_count"] == 0
    assert wdb.summarize_by_product(df).empty and wdb.summarize_by_month(df).empty
    assert wdb.get_stock_outlook("2026-12-10").empty


def test_sales_cost_margin_basis(sample):
    df = wdb.load_sales()
    row = df[(df["product_name"] == "A 위스키") & (df["sales_type"] == "잔 판매")].iloc[0]
    assert row["sales_amount"] == pytest.approx(100000)      # 110,000 ÷ 1.1
    assert row["cost_amount"] == pytest.approx(300 * 100)    # 300ml × 100원
    assert row["margin_amount"] == pytest.approx(70000)

    s = wdb.summarize_sales(df)
    assert s["sales"] == pytest.approx(100000 + 140000 + 30000)
    assert s["cost"] == pytest.approx(30000 + 70000 + 60 * 200)
    assert s["margin"] == pytest.approx(s["sales"] - s["cost"])
    assert s["margin_rate"] == pytest.approx(s["margin"] / s["sales"] * 100)
    assert (s["glass_qty"], s["bottle_qty"], s["sale_count"]) == (12, 1, 3)
    assert s["glass_sales"] + s["bottle_sales"] == pytest.approx(s["sales"])
    assert s["glass_margin"] + s["bottle_margin"] == pytest.approx(s["margin"])
    # 손실: B 서비스 30ml × 200 + A 실사 50ml × 100 — 매출·마진에는 들어가지 않는다
    assert s["loss"] == pytest.approx(30 * 200 + 50 * 100)


def test_date_filter(sample):
    nov = wdb.summarize_sales(wdb.load_sales("2026-11-01", "2026-11-30"))
    dec = wdb.summarize_sales(wdb.load_sales("2026-12-01", "2026-12-31"))
    total = wdb.summarize_sales(wdb.load_sales())
    assert nov["sales"] == pytest.approx(100000)
    for key in ("sales", "cost", "margin", "loss"):
        assert nov[key] + dec[key] == pytest.approx(total[key])


def test_product_and_month_summaries_match_total(sample):
    """HOME(전체 요약)과 대시보드(상품별·월별)가 같은 숫자를 보여야 한다."""
    df = wdb.load_sales()
    total = wdb.summarize_sales(df)
    by_product = wdb.summarize_by_product(df)
    by_month = wdb.summarize_by_month(df)
    for key in ("sales", "cost", "margin", "loss"):
        assert by_product[key].sum() == pytest.approx(total[key])
        assert by_month[key].sum() == pytest.approx(total[key])
    assert by_month["month"].tolist() == ["2026-11", "2026-12"]
    assert by_product["product_name"].tolist() == ["A 위스키", "B 위스키"]  # 매출 큰 순
    assert by_product["glass_qty"].sum() == total["glass_qty"]
    assert (by_month["glass_sales"] + by_month["bottle_sales"]).sum() == pytest.approx(total["sales"])


def test_cost_matches_stock_average(sample):
    """판매 원가의 ml당 단가는 재고 화면의 평균 병 매입가와 같은 기준이어야 한다."""
    stock = wdb.get_stock().set_index("product_id")
    for pid, per_ml in wdb.get_cost_per_ml().items():
        assert per_ml * stock.loc[pid, "bottle_ml"] == pytest.approx(stock.loc[pid, "avg_bottle_cost"])


def test_stock_outlook(sample):
    a, b = sample
    out = wdb.get_stock_outlook("2026-12-10", lookback_days=30).set_index("product_name")
    # A: 최근 30일(11/11~12/10) 판매 300ml + 700ml = 1000ml, 재고 1750ml
    assert out.loc["A 위스키", "sold_ml"] == 1000
    assert out.loc["A 위스키", "days_left"] == pytest.approx(1750 / (1000 / 30))
    assert out.loc["A 위스키", "days_since_sale"] == 8
    assert out.loc["A 위스키", "last_sale_date"] == pd.Timestamp("2026-12-02")

    # 판매 이력이 없는 상품은 소진 예상·경과일수가 비어 있다
    c = wdb.insert_product("C 위스키")
    wdb.insert_purchase("2026-12-01", c, bottle_qty=1, unit_cost=50000)
    out = wdb.get_stock_outlook("2026-12-10").set_index("product_name")
    assert pd.isna(out.loc["C 위스키", "days_left"]) and pd.isna(out.loc["C 위스키", "days_since_sale"])
