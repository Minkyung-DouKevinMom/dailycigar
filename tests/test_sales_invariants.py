"""
정본 로더(modules.common.sales_query)의 계산 불변식(invariant) 검증.

여기서 깨지면 "계산 기준" 자체가 바뀐 것이므로, 의도된 변경인지 반드시 확인해야 한다.
"""
from __future__ import annotations

import pandas as pd
import pytest

from modules.common import sales_query as sq
from modules.common.dates import month_range

TOL = 0.5  # 원 단위 반올림 허용 오차


def _approx(a, b, tol=TOL):
    return abs(float(a) - float(b)) <= tol


# ────────────────────────────── 소매 ──────────────────────────────

def test_retail_sales_is_vat_excluded(conn):
    """소매 매출(net_sales_amount) = 뷰의 공급가액(부가세 제외) 합계여야 한다 — 부가세 포함 금액이 섞이면 실패."""
    df = sq.load_retail_sales(conn)
    raw = pd.read_sql_query(
        "SELECT COALESCE(SUM(sales_supply_amount_krw),0) AS s, COALESCE(SUM(net_sales_amount),0) AS n "
        "FROM v_retail_sales_enriched",
        conn,
    ).iloc[0]
    assert _approx(df["net_sales_amount"].sum(), raw["s"])
    # 부가세 포함 합계와는 달라야 정상 (동일하면 VAT 제외가 깨진 것)
    assert not _approx(df["net_sales_amount"].sum(), raw["n"])


# 원가 산정 근거가 없는 과거 상품코드(마스터/수입기록 없음). 새 코드가 여기 없이 나타나면 실패 → 마스터 등록 필요.
KNOWN_LEGACY_CODES_WITHOUT_COST = {"시가커터선물용"}


def _codes_with_cost_basis(conn) -> set[str]:
    """원가를 계산할 수 있는 상품코드(대문자): 수입기록이 있는 시가 + 시가 외 마스터."""
    from db import get_non_cigar_purchase_price_map

    imp = pd.read_sql_query(
        "SELECT DISTINCT UPPER(TRIM(product_code)) AS c FROM import_item WHERE COALESCE(product_code,'')<>''", conn
    )["c"].tolist()
    nc = [str(k).strip().upper() for k in get_non_cigar_purchase_price_map(conn).keys()]
    return set(imp) | set(nc)


def test_retail_profit_equals_sales_minus_cost_rowwise(conn):
    """원가 근거가 있는 모든 소매 라인에서 이익 = 매출(부가세 제외) − 원가."""
    df = sq.load_retail_sales(conn)
    has_cost = df["product_code"].str.upper().isin(_codes_with_cost_basis(conn))
    d = df[has_cost]
    diff = (d["net_sales_amount"] - d["total_korea_cost_krw"] - d["retail_gross_profit_krw"]).abs()
    bad = d[diff > TOL]
    assert bad.empty, f"이익≠매출−원가 인 라인 {len(bad)}건:\n{bad.head().to_string()}"


def test_no_new_codes_without_cost_basis(conn):
    """원가 근거 없는 상품코드는 알려진 과거 코드에 한정되어야 한다 (새로 생기면 마스터 등록 누락)."""
    df = sq.load_retail_sales(conn)
    unknown = set(df.loc[~df["product_code"].str.upper().isin(_codes_with_cost_basis(conn)), "product_code"])
    unknown = {c for c in unknown if c}  # 빈 코드 제외
    new = unknown - KNOWN_LEGACY_CODES_WITHOUT_COST
    assert not new, f"원가 근거 없는 신규 상품코드: {sorted(new)} — 시가 외 마스터 또는 수입품목 등록 필요"


def test_non_cigar_cost_uses_purchase_price_or_gift_components(conn):
    """시가 외 상품 원가 = 매입가×수량 (기프트패키지는 구성품 원가 합계×수량)."""
    from db import get_gift_package_cost_map, get_non_cigar_purchase_price_map

    df = sq.load_retail_sales(conn)
    pp = get_non_cigar_purchase_price_map(conn)
    gift = get_gift_package_cost_map(conn)

    pp_u = {str(k).strip().upper(): v for k, v in pp.items()}
    gift_u = {str(k).strip().upper(): v for k, v in gift.items()}
    nc = df[df["product_code"].str.upper().isin(pp_u.keys())].copy()
    if nc.empty:
        pytest.skip("시가 외 판매 라인이 없습니다.")
    unit_cost = nc["product_code"].str.upper().map(lambda c: gift_u.get(c, pp_u.get(c, 0)))
    expected = unit_cost * nc["qty"]
    bad = nc[(nc["total_korea_cost_krw"] - expected).abs() > TOL]
    assert bad.empty, f"시가 외 원가 기준 불일치 {len(bad)}건:\n{bad.head().to_string()}"


def test_cigar_cost_matches_view(conn):
    """시가 상품 원가/이익은 뷰(v_retail_sales_enriched) 값이 그대로 유지되어야 한다 (보정 대상 아님)."""
    from db import get_non_cigar_purchase_price_map

    pp_upper = {str(k).strip().upper() for k in get_non_cigar_purchase_price_map(conn).keys()}
    df = sq.load_retail_sales(conn)
    view = pd.read_sql_query(
        "SELECT sale_date, order_no, product_code, qty, COALESCE(total_korea_cost_krw,0) AS cost, "
        "COALESCE(retail_gross_profit_krw,0) AS profit FROM v_retail_sales_enriched",
        conn,
    )
    key = ["sale_date", "order_no", "product_code", "qty"]
    a = df[~df["product_code"].str.upper().isin(pp_upper)].groupby(key)[["total_korea_cost_krw", "retail_gross_profit_krw"]].sum()
    b = view[~view["product_code"].str.upper().isin(pp_upper)].groupby(key)[["cost", "profit"]].sum()
    merged = a.join(b, how="inner")
    assert len(merged) > 0
    assert (merged["total_korea_cost_krw"] - merged["cost"]).abs().max() <= TOL
    assert (merged["retail_gross_profit_krw"] - merged["profit"]).abs().max() <= TOL


def test_retail_table_fallback_expr_matches_view_column(conn):
    """뷰 없이 retail_sales 를 직접 조회할 때 쓰는 부가세 제외 식이 뷰 컬럼과 동일해야 한다."""
    a = pd.read_sql_query(
        f"SELECT COALESCE(SUM({sq.RETAIL_SALES_EXPR_TABLE}),0) AS s FROM retail_sales", conn
    ).iloc[0]["s"]
    b = pd.read_sql_query(
        "SELECT COALESCE(SUM(sales_supply_amount_krw),0) AS s FROM v_retail_sales_enriched", conn
    ).iloc[0]["s"]
    assert _approx(a, b)


# ────────────────────────────── 도매 ──────────────────────────────

def test_wholesale_matches_view_join(conn):
    """정본 도매 로더의 거래처명/상품코드/상품명/매출/이익이 v_wholesale_sales 와 라인 단위로 동일."""
    df = sq.load_wholesale_sales(conn).set_index("id").sort_index()
    v = pd.read_sql_query(
        "SELECT id, partner_name, product_code, product_name, COALESCE(sales_amount,0) AS sales, "
        "COALESCE(profit_amount,0) AS profit FROM v_wholesale_sales",
        conn,
    ).set_index("id").sort_index()
    assert list(df.index) == list(v.index)
    for col in ["partner_name", "product_code", "product_name"]:
        assert (df[col].fillna("").astype(str) == v[col].fillna("").astype(str)).all(), col
    assert (df["net_sales_amount"] - v["sales"]).abs().max() <= TOL
    assert (df["gross_profit_krw"] - v["profit"]).abs().max() <= TOL


def test_wholesale_profit_consistent_with_stored_cost(conn):
    """도매 이익 = 수량 × (공급가 − 원가) 로 저장돼 있어야 한다 (등록 로직 불변식)."""
    df = sq.load_wholesale_sales(conn)
    expected = df["qty"] * (df["supply_price"] - df["unit_cost"])
    bad = df[(df["gross_profit_krw"] - expected).abs() > TOL]
    assert bad.empty, f"도매 이익 저장값 불일치 {len(bad)}건:\n{bad[['id','sale_date','product_code','qty','supply_price','unit_cost','gross_profit_krw']].head().to_string()}"


def test_wholesale_non_cigar_has_cost(conn):
    """시가 외 도매 라인의 원가가 0이면 이익이 공급가액 전체로 과대계상됨 (2026-09 감사에서 발견된 유형)."""
    df = sq.load_wholesale_sales(conn)
    nc = df[(df["item_type"] == "non_cigar") & (df["supply_price"] > 0)]
    bad = nc[nc["unit_cost"] <= 0]
    assert bad.empty, f"원가 0인 시가 외 도매 라인:\n{bad[['id','sale_date','product_name','supply_price','unit_cost']].to_string()}"


# ────────────────────────────── 월 범위 ──────────────────────────────

def test_month_range_boundaries():
    assert month_range(2026, 2) == ("2026-02-01", "2026-02-28")
    assert month_range(2028, 2) == ("2028-02-01", "2028-02-29")
    assert month_range(2026, 12) == ("2026-12-01", "2026-12-31")
