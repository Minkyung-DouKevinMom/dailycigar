"""
매장(망포점/영통점 등) 구분 회귀 테스트.

- 모든 소매 판매 행은 store_mst 에 있는 매장으로 기록돼 있어야 한다.
- 매장별 정본 조회의 합 = 전체 정본 조회 (매출/이익/건수).
- 매장마다 POS 주문번호가 따로 매겨지므로 중복 판정·기프트 자동차감 판정은 매장별로 해야 한다.
"""
from __future__ import annotations

import pandas as pd
import pytest

import db
from modules.common import sales_query as sq
from modules.management import retail_upload as ru

OTHER_STORE = "__다른매장__"


@pytest.fixture()
def store_conn(conn):
    db.init_store_tables()
    return conn


def test_every_retail_row_has_registered_store(store_conn):
    stores = {r[0] for r in store_conn.execute("SELECT store_code FROM store_mst")}
    used = {r[0] for r in store_conn.execute("SELECT DISTINCT store_code FROM retail_sales")}
    assert db.STORE_DEFAULT in stores
    assert used <= stores, f"store_mst 에 없는 매장 코드: {used - stores}"


def test_store_split_sums_to_total(store_conn):
    total = sq.load_retail_sales(store_conn)
    stores = [r[0] for r in store_conn.execute("SELECT store_code FROM store_mst")]
    parts = [sq.load_retail_sales(store_conn, store_code=s) for s in stores]

    assert sum(len(p) for p in parts) == len(total)
    for col in ["net_sales_amount", "retail_gross_profit_krw"]:
        assert sum(p[col].sum() for p in parts) == pytest.approx(total[col].sum(), abs=1)


def test_duplicate_check_is_per_store(store_conn):
    r = store_conn.execute(
        """
        SELECT order_no, product_code_raw, sale_datetime, qty, net_sales_amount, store_code
          FROM retail_sales
         WHERE sale_datetime IS NOT NULL AND COALESCE(order_no, '') <> ''
         ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    row = pd.Series({
        "order_no": r[0],
        "product_code_raw": r[1],
        "sale_datetime": pd.Timestamp(r[2]),
        "qty": r[3],
        "net_sales_amount": r[4],
        "store_code": r[5],
    })
    assert ru.is_duplicate_row(store_conn, row)

    row["store_code"] = OTHER_STORE
    assert not ru.is_duplicate_row(store_conn, row)


def test_gift_auto_deduction_check_is_per_store(store_conn):
    r = store_conn.execute(
        """
        SELECT source_order_no, out_date, product_code, store_code
          FROM stock_out
         WHERE source_type = 'retail_auto'
         ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    if r is None:
        pytest.skip("기프트패키지 자동차감 이력이 없습니다.")
    order_no, out_date, code, store = r
    assert ru.is_gift_auto_deducted(store_conn, order_no, out_date, code, store or db.STORE_DEFAULT)
    assert not ru.is_gift_auto_deducted(store_conn, order_no, out_date, code, OTHER_STORE)
