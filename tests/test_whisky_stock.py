"""
위스키(개인사업자) 재고 로직 테스트.

- 위스키 데이터는 cigar.db 가 아닌 별도 whisky.db 에 저장된다.
- 재고는 용량(ml) 기준: 병 판매는 병 용량, 잔 판매는 잔 용량(30ml)만큼 차감.
- 실사 조정은 실물 수량에 맞게 차이만큼 조정 행을 넣는다.
"""
from __future__ import annotations

import sqlite3

import pytest

from modules.whisky import whisky_db as wdb


@pytest.fixture
def wpath(tmp_path, monkeypatch):
    path = tmp_path / "whisky_test.db"
    monkeypatch.setenv("WHISKY_DB_PATH", str(path))
    return path


def _stock(product_id):
    df = wdb.get_stock()
    return df[df["product_id"] == product_id].iloc[0]


def test_whisky_uses_separate_db_file(wpath, db_path):
    wdb.insert_product("테스트 위스키")
    assert wpath.exists()
    tables = {r[0] for r in sqlite3.connect(wpath).execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"whisky_product", "whisky_purchase", "whisky_stock_out"} <= tables

    cigar_tables = {r[0] for r in sqlite3.connect(db_path).execute("SELECT name FROM sqlite_master")}
    assert not any(t.startswith("whisky") for t in cigar_tables)


@pytest.mark.parametrize(
    "total_ml, expected, text",
    [
        (2500, (3, 400), "미개봉 3병 + 개봉 400ml"),
        (2100, (3, 0), "미개봉 3병"),
        (400, (0, 400), "개봉 400ml"),
        (0, (0, 0), "재고 없음"),
        (-60, (0, -60), "부족 60ml"),
    ],
)
def test_split_and_format_stock(total_ml, expected, text):
    assert wdb.split_stock_ml(total_ml, 700) == expected
    assert wdb.format_stock(total_ml, 700) == text


def test_bottle_and_glass_sales_deduct_by_ml(wpath):
    pid = wdb.insert_product("글렌 테스트 12", bottle_ml=700, glass_ml=30, bottle_price=150000, glass_price=12000)
    wdb.insert_purchase("2026-12-01", pid, bottle_qty=4, unit_cost=70000, supplier_name="도매상")
    assert _stock(pid)["stock_ml"] == 2800

    wdb.insert_stock_out("2026-12-02", pid, wdb.OUT_BOTTLE_SALE, qty=1, amount=150000)
    wdb.insert_stock_out("2026-12-02", pid, wdb.OUT_GLASS_SALE, qty=10, amount=120000)
    wdb.insert_stock_out("2026-12-03", pid, wdb.OUT_ETC, qty=1, notes="서비스 한 잔")

    row = _stock(pid)
    assert row["stock_ml"] == 2800 - 700 - 300 - 30
    assert (row["full_bottles"], row["open_ml"]) == (2, 370)
    assert row["stock_text"] == "미개봉 2병 + 개봉 370ml"
    assert row["glasses_left"] == 59
    assert row["avg_bottle_cost"] == 70000
    assert row["stock_value"] == round(70000 / 700 * 1770)


def test_changing_product_size_keeps_past_stock(wpath):
    pid = wdb.insert_product("용량 변경 테스트", bottle_ml=700, glass_ml=30)
    wdb.insert_purchase("2026-12-01", pid, bottle_qty=1)
    wdb.insert_stock_out("2026-12-01", pid, wdb.OUT_GLASS_SALE, qty=2)
    p = wdb.get_product(pid)
    wdb.update_product(pid, p["product_name"], "", "", 700, 45, 0, 0, 1, "")
    assert _stock(pid)["stock_ml"] == 640  # 과거 2잔은 30ml 기준 그대로
    wdb.insert_stock_out("2026-12-02", pid, wdb.OUT_GLASS_SALE, qty=1)
    assert _stock(pid)["stock_ml"] == 595


def test_stocktake_adjusts_to_actual(wpath):
    pid = wdb.insert_product("실사 테스트", bottle_ml=700)
    wdb.insert_purchase("2026-12-01", pid, bottle_qty=3)
    wdb.insert_stock_out("2026-12-02", pid, wdb.OUT_GLASS_SALE, qty=5)  # 시스템 1950ml

    diff = wdb.apply_stocktake("2026-12-05", pid, full_bottles=2, open_ml=500, notes="월초 실사")
    assert diff == 50  # 실물이 50ml 적음
    row = _stock(pid)
    assert (row["full_bottles"], row["open_ml"]) == (2, 500)

    assert wdb.apply_stocktake("2026-12-05", pid, full_bottles=2, open_ml=500) == 0
    outs = wdb.get_stock_outs()
    assert (outs["out_type"] == wdb.OUT_ADJUST).sum() == 1

    # 실물이 더 많으면 재고가 늘어난다
    assert wdb.apply_stocktake("2026-12-06", pid, full_bottles=2, open_ml=600) == -100
    assert _stock(pid)["stock_ml"] == 2000


def test_validation(wpath):
    pid = wdb.insert_product("검증 테스트")
    with pytest.raises(ValueError):
        wdb.insert_product("검증 테스트")
    with pytest.raises(ValueError):
        wdb.insert_product("")
    with pytest.raises(ValueError):
        wdb.insert_purchase("2026-12-01", pid, bottle_qty=0)
    with pytest.raises(ValueError):
        wdb.insert_stock_out("2026-12-01", pid, wdb.OUT_GLASS_SALE, qty=0)
    with pytest.raises(ValueError):
        wdb.apply_stocktake("2026-12-01", pid, full_bottles=0, open_ml=700)
