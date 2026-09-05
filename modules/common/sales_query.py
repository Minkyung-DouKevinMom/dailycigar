"""
매출/이익 조회 정본(canonical) 모듈.

대시보드 · 재무관리(매출/이익) · 분석(기간비교) · 홈 화면 · 정기 리포트(report.py)가
각자 갖고 있던 소매/도매 매출 조회 SQL 과 컬럼 매핑을 여기 하나로 통일합니다.

계산 기준 (2026-09 감사에서 확정, 모든 화면 동일):
  - 소매 매출  = 부가세 제외 공급가액 (v_retail_sales_enriched.sales_supply_amount_krw)
  - 소매 원가  = qty × 판매일 기준 배치의 korea_cost_krw (시가)
                 시가 외 상품은 db.apply_non_cigar_margin_logic 으로 재계산
                 (매입가 × qty, 기프트패키지는 구성품 원가 합계)
  - 소매 이익  = 매출(부가세 제외) − 원가
  - 도매 매출  = wholesale_sales.sales_amount (= supply_amount, 부가세 제외)
  - 도매 원가  = qty × unit_cost,  도매 이익 = profit_amount (등록 시 확정 저장)

반환 컬럼명은 모듈 간 공통이며, 화면별로 다른 이름이 필요하면 호출 측에서 rename 하세요.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from db import apply_non_cigar_margin_logic
from modules.common.dbutil import get_table_columns, table_exists, view_exists

RETAIL_VIEW = "v_retail_sales_enriched"
RETAIL_TABLE = "retail_sales"
WHOLESALE_TABLE = "wholesale_sales"

# 뷰가 없어 retail_sales 를 직접 조회할 때 쓰는 부가세 제외 매출 식 (뷰의 sales_supply_amount_krw 와 동일)
RETAIL_SALES_EXPR_TABLE = (
    "CASE WHEN COALESCE(taxable_yn, '과세') = '과세' "
    "THEN COALESCE(net_sales_amount, 0) - COALESCE(vat_amount, 0) "
    "ELSE COALESCE(net_sales_amount, 0) END"
)
RETAIL_SALES_EXPR_VIEW = "COALESCE(sales_supply_amount_krw, 0)"

RETAIL_COLUMNS = [
    "sale_date", "sale_datetime", "order_no", "order_channel", "payment_status",
    "product_code", "product_code_raw", "product_name", "size_name", "category",
    "qty", "unit_price",
    "net_sales_amount",        # 부가세 제외 매출(공급가액)
    "gross_sales_amount",      # 부가세 포함 실판매금액(참고용)
    "vat_amount",
    "total_korea_cost_krw",
    "retail_gross_profit_krw",
    "customer_name",
]

WHOLESALE_COLUMNS = [
    "id", "sale_date", "partner_id", "partner_name", "item_type",
    "product_code", "product_name",
    "qty", "unit_price", "supply_price", "unit_cost",
    "net_sales_amount",        # = sales_amount (공급가액, 부가세 제외)
    "vat_amount", "total_amount_vat",
    "total_korea_cost_krw",    # qty × unit_cost
    "gross_profit_krw",        # = profit_amount
    "grade_code_applied", "discount_rate_applied", "notes",
]


def retail_source(conn) -> Optional[str]:
    """소매 조회 소스명 (뷰 우선). 둘 다 없으면 None."""
    if view_exists(conn, RETAIL_VIEW):
        return RETAIL_VIEW
    if table_exists(conn, RETAIL_TABLE):
        return RETAIL_TABLE
    return None


def retail_sales_amount_expr(source: str) -> str:
    """소매 매출(부가세 제외) SQL 식. report.py 등 SQL 을 직접 쓰는 곳에서 사용."""
    return RETAIL_SALES_EXPR_VIEW if source == RETAIL_VIEW else RETAIL_SALES_EXPR_TABLE


def _date_filter(date_from: Optional[str], date_to: Optional[str], col: str = "sale_date"):
    sql, params = "", []
    if date_from:
        sql += f" AND {col} >= ?"
        params.append(str(date_from))
    if date_to:
        sql += f" AND {col} <= ?"
        params.append(str(date_to))
    return sql, params


def _empty(cols):
    return pd.DataFrame(columns=cols)


def load_retail_sales(conn, date_from: Optional[str] = None, date_to: Optional[str] = None) -> pd.DataFrame:
    """
    소매 판매 라인 단위 데이터 (기간 필터 포함, 시가 외 원가/이익 보정 적용 완료).
    컬럼: RETAIL_COLUMNS
    """
    source = retail_source(conn)
    if not source:
        return _empty(RETAIL_COLUMNS)

    cols = set(get_table_columns(conn, source))
    is_view = source == RETAIL_VIEW

    def c(name, default_sql):
        return f"COALESCE({name}, {default_sql})" if name in cols else default_sql

    product_name_expr = (
        "COALESCE(mst_product_name, product_code_raw, '')" if is_view else c("product_code_raw", "''")
    )
    size_expr = c("mst_size_name", "''") if is_view else "''"
    cost_expr = c("total_korea_cost_krw", "0") if is_view else "0"
    gp_expr = c("retail_gross_profit_krw", "0") if is_view else "0"

    sql = f"""
        SELECT
            sale_date,
            {c("sale_datetime", "''")} AS sale_datetime,
            {c("order_no", "''")} AS order_no,
            {c("order_channel", "''")} AS order_channel,
            {c("payment_status", "''")} AS payment_status,
            {c("product_code", "''")} AS product_code,
            {c("product_code_raw", "''")} AS product_code_raw,
            {product_name_expr} AS product_name,
            {size_expr} AS size_name,
            {c("category", "''")} AS category,
            {c("qty", "0")} AS qty,
            {c("unit_price", "0")} AS unit_price,
            {retail_sales_amount_expr(source)} AS net_sales_amount,
            {c("net_sales_amount", "0")} AS gross_sales_amount,
            {c("vat_amount", "0")} AS vat_amount,
            {cost_expr} AS total_korea_cost_krw,
            {gp_expr} AS retail_gross_profit_krw,
            '' AS customer_name
        FROM {source}
        WHERE 1=1
    """
    where, params = _date_filter(date_from, date_to)
    sql += where + " ORDER BY sale_date DESC"

    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return _empty(RETAIL_COLUMNS)

    for col in ["qty", "unit_price", "net_sales_amount", "gross_sales_amount", "vat_amount",
                "total_korea_cost_krw", "retail_gross_profit_krw"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["product_code", "product_code_raw", "product_name", "size_name", "category",
                "order_no", "order_channel", "payment_status", "sale_datetime", "customer_name"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # 시가 외 상품 원가/이익 재계산 (정본). net_sales_amount 가 부가세 제외 매출이어야 함 — 위에서 보장.
    df = apply_non_cigar_margin_logic(df, conn)
    return df[RETAIL_COLUMNS]


def load_wholesale_sales(conn, date_from: Optional[str] = None, date_to: Optional[str] = None) -> pd.DataFrame:
    """
    도매 판매 라인 단위 데이터 (기간 필터 포함). 거래처명/상품명을 조인해 반환.
    컬럼: WHOLESALE_COLUMNS
    """
    if not table_exists(conn, WHOLESALE_TABLE):
        return _empty(WHOLESALE_COLUMNS)

    wcols = set(get_table_columns(conn, WHOLESALE_TABLE))

    def wc(name, default_sql="0"):
        return f"COALESCE(w.{name}, {default_sql})" if name in wcols else default_sql

    has_pm = table_exists(conn, "partner_mst")
    has_p = table_exists(conn, "product_mst")
    has_n = table_exists(conn, "non_cigar_product_mst")

    partner_expr = "COALESCE(pm.partner_name, '')" if has_pm else "''"
    code_expr = (
        "CASE WHEN w.item_type = 'cigar' THEN p.product_code ELSE n.product_code END"
        if (has_p and has_n) else "''"
    )
    name_expr = (
        "CASE WHEN w.item_type = 'cigar' THEN p.product_name ELSE n.product_name END"
        if (has_p and has_n) else "''"
    )

    joins = ""
    if has_pm:
        joins += " LEFT JOIN partner_mst pm ON w.partner_id = pm.id"
    if has_p:
        joins += " LEFT JOIN product_mst p ON w.cigar_product_id = p.id"
    if has_n:
        joins += " LEFT JOIN non_cigar_product_mst n ON w.non_cigar_product_id = n.id"

    sql = f"""
        SELECT
            w.id,
            w.sale_date,
            w.partner_id,
            {partner_expr} AS partner_name,
            COALESCE(w.item_type, '') AS item_type,
            COALESCE({code_expr}, '') AS product_code,
            COALESCE({name_expr}, '') AS product_name,
            {wc("qty")} AS qty,
            {wc("unit_price")} AS unit_price,
            {wc("supply_price")} AS supply_price,
            {wc("unit_cost")} AS unit_cost,
            {wc("sales_amount")} AS net_sales_amount,
            {wc("vat_amount")} AS vat_amount,
            {wc("total_amount_vat")} AS total_amount_vat,
            {wc("qty")} * {wc("unit_cost")} AS total_korea_cost_krw,
            {wc("profit_amount")} AS gross_profit_krw,
            {wc("grade_code_applied", "''")} AS grade_code_applied,
            {wc("discount_rate_applied")} AS discount_rate_applied,
            {wc("notes", "''")} AS notes
        FROM {WHOLESALE_TABLE} w
        {joins}
        WHERE 1=1
    """
    where, params = _date_filter(date_from, date_to, "w.sale_date")
    sql += where + " ORDER BY w.sale_date DESC, w.id DESC"

    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return _empty(WHOLESALE_COLUMNS)

    for col in ["qty", "unit_price", "supply_price", "unit_cost", "net_sales_amount", "vat_amount",
                "total_amount_vat", "total_korea_cost_krw", "gross_profit_krw", "discount_rate_applied"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["partner_name", "item_type", "product_code", "product_name", "grade_code_applied", "notes"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df[WHOLESALE_COLUMNS]


def sum_sales_profit(df: pd.DataFrame, sales_col: str, profit_col: str) -> tuple[float, float]:
    if df is None or df.empty:
        return 0.0, 0.0
    return float(df[sales_col].sum()), float(df[profit_col].sum())
