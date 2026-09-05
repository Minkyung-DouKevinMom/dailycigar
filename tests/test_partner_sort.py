"""도매관리 > 거래처 관리: 등록된 거래처 목록 정렬(기본 최근구매순) 검증."""
from __future__ import annotations

import pandas as pd

from modules.management.wholesale_management import (
    PARTNER_SORT_AMOUNT,
    PARTNER_SORT_NAME,
    PARTNER_SORT_RECENT,
    load_partner_last_purchase,
    sort_partners,
)


def _partners(rows):
    return pd.DataFrame(
        rows, columns=["id", "partner_name", "last_purchase_date", "purchase_supply_amount_sum"]
    )


SAMPLE = _partners([
    (1, "가나상회", "2026-08-01", 500),
    (2, "다라유통", "2026-09-02", 100),
    (3, "마바상사", None, 900),          # 구매 이력 없음
    (4, "아자스토어", "2026-09-02", 300),  # 다라유통과 같은 날 → 이름순
])


def test_default_sort_is_recent_purchase_first():
    out = sort_partners(SAMPLE)  # 기본값
    assert out["partner_name"].tolist() == ["다라유통", "아자스토어", "가나상회", "마바상사"]
    assert sort_partners(SAMPLE, PARTNER_SORT_RECENT)["id"].tolist() == out["id"].tolist()


def test_partners_without_purchase_go_last():
    out = sort_partners(SAMPLE)
    assert out["partner_name"].iloc[-1] == "마바상사"

    none_bought = _partners([(1, "나", None, 0), (2, "가", None, 0)])
    assert sort_partners(none_bought)["partner_name"].tolist() == ["가", "나"]  # 동률이면 이름순


def test_name_and_amount_sorts():
    assert sort_partners(SAMPLE, PARTNER_SORT_NAME)["partner_name"].tolist() == [
        "가나상회", "다라유통", "마바상사", "아자스토어",
    ]
    assert sort_partners(SAMPLE, PARTNER_SORT_AMOUNT)["partner_name"].tolist() == [
        "마바상사", "가나상회", "아자스토어", "다라유통",
    ]


def test_missing_column_and_empty_input():
    no_col = pd.DataFrame({"id": [2, 1], "partner_name": ["나", "가"]})
    assert sort_partners(no_col)["partner_name"].tolist() == ["가", "나"]   # 컬럼 없으면 이름순 폴백

    empty = pd.DataFrame(columns=["id", "partner_name", "last_purchase_date"])
    assert sort_partners(empty).empty


def test_load_partner_last_purchase_matches_db(conn):
    lp = load_partner_last_purchase(conn)
    raw = pd.read_sql_query(
        "SELECT partner_id, MAX(sale_date) m, COUNT(DISTINCT sale_date) c "
        "FROM wholesale_sales WHERE partner_id IS NOT NULL AND COALESCE(sale_date,'') <> '' "
        "GROUP BY partner_id",
        conn,
    )
    assert len(lp) == len(raw)
    merged = lp.merge(raw, left_on="partner_id", right_on="partner_id")
    assert (merged["last_purchase_date"] == merged["m"]).all()
    assert (merged["purchase_count"] == merged["c"]).all()


def test_real_partner_list_sorted_desc(conn):
    """실제 DB 거래처 목록을 정렬하면 최근구매일 내림차순 + 이력없음 뒤."""
    from modules.management.wholesale_management import load_partners

    partners = load_partners(conn).merge(
        load_partner_last_purchase(conn), how="left", left_on="id", right_on="partner_id"
    )
    out = sort_partners(partners)
    dates = pd.to_datetime(out["last_purchase_date"], errors="coerce")
    have = dates.dropna()
    assert have.is_monotonic_decreasing                      # 구매 이력 있는 쪽은 내림차순
    assert dates.isna().sum() == 0 or dates.tail(int(dates.isna().sum())).isna().all()  # 이력 없는 쪽이 뒤
