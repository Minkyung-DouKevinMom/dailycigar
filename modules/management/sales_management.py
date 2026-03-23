from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st


# =========================
# 설정
# =========================
DB_PATH = os.environ.get("DAILYCIGAR_DB_PATH", "dailycigar.db")
RETAIL_PAYMENT_SHEET_CANDIDATES = ["결제 상세내역", "결제상세내역"]
RETAIL_ITEM_SHEET_CANDIDATES = ["상품 주문 상세내역", "상품주문상세내역"]


# =========================
# 공통 유틸
# =========================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    sql = "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?"
    row = conn.execute(sql, (table_name,)).fetchone()
    return row is not None


def normalize_colname(col: str) -> str:
    return str(col).strip().replace("\n", " ").replace("  ", " ")


def find_sheet_name(xls: pd.ExcelFile, candidates: list[str]) -> Optional[str]:
    name_map = {str(name).strip(): name for name in xls.sheet_names}
    for candidate in candidates:
        if candidate in name_map:
            return name_map[candidate]
    return None


def find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> Optional[str]:
    cols = {normalize_colname(c): c for c in df.columns}
    for candidate in candidates:
        if candidate in cols:
            return cols[candidate]
    if required:
        raise KeyError(f"필수 컬럼을 찾지 못했습니다: {candidates}")
    return None


def to_text(v) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    return s if s else None


def to_float(v, default: float = 0.0) -> float:
    if pd.isna(v):
        return default
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s == "":
        return default
    try:
        return float(s)
    except Exception:
        return default


def parse_date_text(v) -> Optional[str]:
    if pd.isna(v) or v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s:
        return None
    for fmt in ["%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def parse_time_text(v) -> Optional[str]:
    if pd.isna(v) or v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%H:%M:%S")
    s = str(v).strip()
    if not s:
        return None
    for fmt in ["%H:%M:%S", "%H:%M"]:
        try:
            return datetime.strptime(s, fmt).strftime("%H:%M:%S")
        except Exception:
            pass
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%H:%M:%S")


def combine_datetime(date_text: Optional[str], time_text: Optional[str]) -> Optional[str]:
    if not date_text and not time_text:
        return None
    if date_text and not time_text:
        return f"{date_text} 00:00:00"
    if not date_text and time_text:
        return None
    return f"{date_text} {time_text}"


def safe_div(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


# =========================
# DB 확인 / 마스터 로딩
# =========================
def validate_required_tables(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    required = [
        "product_mst",
        "non_cigar_product_mst",
        "partner_mst",
        "wholesale_sales",
        "retail_order_hdr",
        "retail_order_item",
        "retail_payment",
    ]
    missing = [name for name in required if not table_exists(conn, name)]
    return len(missing) == 0, missing


def load_cigar_products(conn: sqlite3.Connection) -> pd.DataFrame:
    queries = [
        """
        SELECT id, product_code, product_name
        FROM product_mst
        ORDER BY product_name
        """,
        """
        SELECT id, code AS product_code, product_name
        FROM product_mst
        ORDER BY product_name
        """,
        """
        SELECT id, product_code, product_nm AS product_name
        FROM product_mst
        ORDER BY product_nm
        """,
    ]
    for sql in queries:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            continue
    return pd.DataFrame(columns=["id", "product_code", "product_name"])


def load_non_cigar_products(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
        SELECT id, product_code, product_name, product_category
        FROM non_cigar_product_mst
        WHERE COALESCE(is_active, 1) = 1
        ORDER BY product_category, product_name
    """
    try:
        return pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame(columns=["id", "product_code", "product_name", "product_category"])


def load_partners(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
        SELECT id, partner_name, current_grade_code
        FROM partner_mst
        WHERE COALESCE(status, 'active') <> 'inactive'
        ORDER BY partner_name
    """
    try:
        return pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame(columns=["id", "partner_name", "current_grade_code"])


# =========================
# 소매 업로드용 정규화
# =========================
@dataclass
class ProductMatch:
    item_type: str
    cigar_product_id: Optional[int]
    non_cigar_product_id: Optional[int]
    matched_by: str


def build_product_lookup(cigar_df: pd.DataFrame, non_cigar_df: pd.DataFrame):
    lookup_code: dict[str, ProductMatch] = {}
    lookup_name: dict[str, ProductMatch] = {}

    for _, row in cigar_df.iterrows():
        pid = int(row["id"])
        pcode = to_text(row.get("product_code"))
        pname = to_text(row.get("product_name"))
        pm = ProductMatch("cigar", pid, None, "")
        if pcode:
            lookup_code[pcode.lower()] = pm
        if pname:
            lookup_name[pname.lower()] = pm

    for _, row in non_cigar_df.iterrows():
        pid = int(row["id"])
        pcode = to_text(row.get("product_code"))
        pname = to_text(row.get("product_name"))
        pm = ProductMatch("non_cigar", None, pid, "")
        if pcode:
            lookup_code[pcode.lower()] = pm
        if pname:
            lookup_name[pname.lower()] = pm

    return lookup_code, lookup_name


def auto_match_product(product_code: Optional[str], product_name: Optional[str], lookup_code, lookup_name) -> ProductMatch:
    if product_code:
        hit = lookup_code.get(product_code.lower())
        if hit:
            return ProductMatch(hit.item_type, hit.cigar_product_id, hit.non_cigar_product_id, "상품코드")
    if product_name:
        hit = lookup_name.get(product_name.lower())
        if hit:
            return ProductMatch(hit.item_type, hit.cigar_product_id, hit.non_cigar_product_id, "상품명")
    return ProductMatch("non_cigar", None, None, "미매핑")


def load_retail_excel(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    xls = pd.ExcelFile(uploaded_file)
    payment_sheet = find_sheet_name(xls, RETAIL_PAYMENT_SHEET_CANDIDATES)
    item_sheet = find_sheet_name(xls, RETAIL_ITEM_SHEET_CANDIDATES)
    if not payment_sheet or not item_sheet:
        raise ValueError(
            "업로드 파일에서 '결제 상세내역' 또는 '상품 주문 상세내역' 시트를 찾지 못했습니다."
        )

    payment_df = pd.read_excel(xls, sheet_name=payment_sheet)
    item_df = pd.read_excel(xls, sheet_name=item_sheet)
    payment_df.columns = [normalize_colname(c) for c in payment_df.columns]
    item_df.columns = [normalize_colname(c) for c in item_df.columns]

    info = {
        "payment_sheet": payment_sheet,
        "item_sheet": item_sheet,
        "payment_rows": len(payment_df),
        "item_rows": len(item_df),
    }
    return payment_df, item_df, info


def transform_retail_frames(payment_df: pd.DataFrame, item_df: pd.DataFrame, conn: sqlite3.Connection):
    cigar_df = load_cigar_products(conn)
    non_cigar_df = load_non_cigar_products(conn)
    lookup_code, lookup_name = build_product_lookup(cigar_df, non_cigar_df)

    # 결제 시트 컬럼 후보
    p_order_no = find_column(payment_df, ["주문번호"])
    p_pay_date = find_column(payment_df, ["결제일자", "결제 날짜"], required=False)
    p_pay_time = find_column(payment_df, ["결제시각", "결제 시간"], required=False)
    p_pay_method = find_column(payment_df, ["결제수단"])
    p_acquirer = find_column(payment_df, ["매입사", "카드매입사"], required=False)
    p_pay_amount = find_column(payment_df, ["결제금액", "실결제금액", "승인금액"], required=False)
    p_pay_status = find_column(payment_df, ["결제상태"])
    p_cancel_dt = find_column(payment_df, ["결제취소시각", "취소시각", "취소일시"], required=False)

    # 상품 시트 컬럼 후보
    i_order_no = find_column(item_df, ["주문번호"])
    i_order_date = find_column(item_df, ["주문일자", "결제일자"], required=False)
    i_order_time = find_column(item_df, ["주문시각", "결제시각"], required=False)
    i_product_code = find_column(item_df, ["상품코드"], required=False)
    i_product_name = find_column(item_df, ["상품명"])
    i_category = find_column(item_df, ["상품분류", "카테고리", "분류"], required=False)
    i_option_name = find_column(item_df, ["옵션"], required=False)
    i_option_price = find_column(item_df, ["옵션가격"], required=False)
    i_item_discount_name = find_column(item_df, ["상품할인"], required=False)
    i_order_discount_name = find_column(item_df, ["주문할인"], required=False)
    i_item_discount_amt = find_column(item_df, ["상품할인 금액", "상품할인금액"], required=False)
    i_order_discount_amt = find_column(item_df, ["주문할인 금액", "주문할인금액"], required=False)
    i_qty = find_column(item_df, ["수량", "주문수량"])
    i_unit_price = find_column(item_df, ["판매가", "단가", "상품판매가"], required=False)
    i_net_amount = find_column(item_df, ["실판매금액", "결제금액", "매출금액"], required=False)
    i_tax_flag = find_column(item_df, ["과세여부"] , required=False)
    i_vat_amount = find_column(item_df, ["부가세액", "VAT", "vat_amount"], required=False)

    # 헤더 생성용: 상품 시트 기준
    hdr_base = (
        item_df[[c for c in [i_order_no, i_order_date, i_order_time] if c is not None]]
        .drop_duplicates(subset=[i_order_no])
        .copy()
    )
    hdr_base.rename(columns={i_order_no: "order_no"}, inplace=True)
    hdr_base["order_date"] = hdr_base[i_order_date].apply(parse_date_text) if i_order_date else None
    hdr_base["order_time"] = hdr_base[i_order_time].apply(parse_time_text) if i_order_time else None
    hdr_base["order_datetime"] = hdr_base.apply(
        lambda r: combine_datetime(r.get("order_date"), r.get("order_time")), axis=1
    )

    # 결제집계
    pay_work = payment_df.copy()
    pay_work["order_no"] = pay_work[p_order_no].astype(str).str.strip()
    pay_work["payment_date"] = pay_work[p_pay_date].apply(parse_date_text) if p_pay_date else None
    pay_work["payment_time"] = pay_work[p_pay_time].apply(parse_time_text) if p_pay_time else None
    pay_work["payment_datetime"] = pay_work.apply(
        lambda r: combine_datetime(r.get("payment_date"), r.get("payment_time")), axis=1
    )
    pay_work["payment_amount_n"] = pay_work[p_pay_amount].apply(to_float) if p_pay_amount else 0.0

    pay_group = (
        pay_work.groupby("order_no", dropna=False)
        .agg(
            payment_status=(p_pay_status, "first"),
            payment_amount=("payment_amount_n", "sum"),
            payment_count=(p_order_no, "count"),
        )
        .reset_index()
    )

    hdr = hdr_base.merge(pay_group, on="order_no", how="left")
    hdr["order_channel"] = "매장"
    hdr["order_item_count"] = 0
    hdr["order_total_amount"] = hdr["payment_amount"].fillna(0.0)
    hdr["order_tax_amount"] = 0.0

    # 아이템 정규화
    items = item_df.copy()
    items["order_no"] = items[i_order_no].astype(str).str.strip()
    items["product_code_snapshot"] = items[i_product_code].apply(to_text) if i_product_code else None
    items["product_name_snapshot"] = items[i_product_name].apply(to_text)
    items["category_snapshot"] = items[i_category].apply(to_text) if i_category else None
    items["option_name"] = items[i_option_name].apply(to_text) if i_option_name else None
    items["option_price"] = items[i_option_price].apply(to_float) if i_option_price else 0.0
    items["item_discount_name"] = items[i_item_discount_name].apply(to_text) if i_item_discount_name else None
    items["order_discount_name"] = items[i_order_discount_name].apply(to_text) if i_order_discount_name else None
    items["item_discount_amount"] = items[i_item_discount_amt].apply(to_float) if i_item_discount_amt else 0.0
    items["order_discount_amount"] = items[i_order_discount_amt].apply(to_float) if i_order_discount_amt else 0.0
    items["qty"] = items[i_qty].apply(to_float)
    items["unit_price"] = items[i_unit_price].apply(to_float) if i_unit_price else 0.0
    items["net_sales_amount"] = items[i_net_amount].apply(to_float) if i_net_amount else 0.0
    items["tax_flag"] = items[i_tax_flag].apply(to_text) if i_tax_flag else None
    items["vat_amount"] = items[i_vat_amount].apply(to_float) if i_vat_amount else 0.0

    match_rows = []
    for _, row in items.iterrows():
        match = auto_match_product(row.get("product_code_snapshot"), row.get("product_name_snapshot"), lookup_code, lookup_name)
        match_rows.append({
            "item_type": match.item_type,
            "cigar_product_id": match.cigar_product_id,
            "non_cigar_product_id": match.non_cigar_product_id,
            "matched_by": match.matched_by,
        })
    match_df = pd.DataFrame(match_rows)
    items = pd.concat([items.reset_index(drop=True), match_df], axis=1)

    items["gross_sales_amount"] = (
        items["net_sales_amount"] + items["item_discount_amount"] + items["order_discount_amount"]
    )
    items["unit_cost"] = 0.0
    items["profit_amount"] = items["net_sales_amount"] - (items["unit_cost"] * items["qty"])

    items["line_no"] = items.groupby("order_no").cumcount() + 1

    # 헤더 보정
    item_agg = (
        items.groupby("order_no", dropna=False)
        .agg(
            order_item_count=("line_no", "count"),
            order_total_amount_items=("net_sales_amount", "sum"),
            order_tax_amount=("vat_amount", "sum"),
        )
        .reset_index()
    )
    hdr = hdr.drop(columns=["order_item_count", "order_tax_amount"], errors="ignore").merge(item_agg, on="order_no", how="left")
    hdr["order_item_count"] = hdr["order_item_count"].fillna(0).astype(int)
    hdr["order_tax_amount"] = hdr["order_tax_amount"].fillna(0.0)
    hdr["order_total_amount"] = hdr["order_total_amount"].fillna(hdr["order_total_amount_items"]).fillna(0.0)
    hdr.drop(columns=["order_total_amount_items", "payment_amount", "payment_count"], errors="ignore", inplace=True)

    # 결제 정규화
    payments = pay_work.copy()
    payments = payments[[c for c in payments.columns]]
    payments["payment_method_norm"] = payments[p_pay_method].apply(to_text)
    payments["acquirer_name_norm"] = payments[p_acquirer].apply(to_text) if p_acquirer else None
    payments["payment_status_norm"] = payments[p_pay_status].apply(to_text)
    payments["cancel_datetime_norm"] = payments[p_cancel_dt].apply(to_text) if p_cancel_dt else None

    preview = items[[
        "order_no", "line_no", "product_code_snapshot", "product_name_snapshot",
        "item_type", "matched_by", "qty", "unit_price", "net_sales_amount"
    ]].copy()
    unmatched = preview[preview["matched_by"] == "미매핑"].copy()

    return hdr, items, payments, preview, unmatched


# =========================
# 소매 업로드 저장
# =========================
def upsert_retail_data(conn: sqlite3.Connection, hdr: pd.DataFrame, items: pd.DataFrame, payments: pd.DataFrame) -> dict:
    cur = conn.cursor()
    inserted_hdr = 0
    updated_hdr = 0
    inserted_items = 0
    inserted_payments = 0

    order_id_map: dict[str, int] = {}

    for _, row in hdr.iterrows():
        order_no = to_text(row.get("order_no"))
        if not order_no:
            continue
        existing = cur.execute("SELECT id FROM retail_order_hdr WHERE order_no = ?", (order_no,)).fetchone()
        if existing:
            order_id = int(existing[0])
            cur.execute(
                """
                UPDATE retail_order_hdr
                   SET order_date = ?,
                       order_time = ?,
                       order_datetime = ?,
                       order_channel = ?,
                       payment_status = ?,
                       order_item_count = ?,
                       order_total_amount = ?,
                       order_tax_amount = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (
                    row.get("order_date"), row.get("order_time"), row.get("order_datetime"),
                    row.get("order_channel"), to_text(row.get("payment_status")),
                    int(row.get("order_item_count") or 0),
                    to_float(row.get("order_total_amount")),
                    to_float(row.get("order_tax_amount")),
                    order_id,
                )
            )
            updated_hdr += 1
        else:
            cur.execute(
                """
                INSERT INTO retail_order_hdr (
                    order_no, order_date, order_time, order_datetime,
                    order_channel, payment_status, order_item_count,
                    order_total_amount, order_tax_amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_no,
                    row.get("order_date"), row.get("order_time"), row.get("order_datetime"),
                    row.get("order_channel"), to_text(row.get("payment_status")),
                    int(row.get("order_item_count") or 0),
                    to_float(row.get("order_total_amount")),
                    to_float(row.get("order_tax_amount")),
                )
            )
            order_id = int(cur.lastrowid)
            inserted_hdr += 1
        order_id_map[order_no] = order_id

    # 기존 상세/결제 삭제 후 재삽입
    for order_no, order_id in order_id_map.items():
        cur.execute("DELETE FROM retail_order_item WHERE order_id = ?", (order_id,))
        cur.execute("DELETE FROM retail_payment WHERE order_id = ?", (order_id,))

    for _, row in items.iterrows():
        order_no = to_text(row.get("order_no"))
        order_id = order_id_map.get(order_no)
        if not order_id:
            continue
        cur.execute(
            """
            INSERT INTO retail_order_item (
                order_id, line_no, item_type, cigar_product_id, non_cigar_product_id,
                product_code_snapshot, product_name_snapshot, category_snapshot,
                option_name, option_price, item_discount_name, order_discount_name,
                item_discount_amount, order_discount_amount, qty, unit_price,
                gross_sales_amount, net_sales_amount, tax_flag, vat_amount,
                unit_cost, profit_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                int(row.get("line_no") or 1),
                to_text(row.get("item_type")) or "non_cigar",
                row.get("cigar_product_id"),
                row.get("non_cigar_product_id"),
                to_text(row.get("product_code_snapshot")),
                to_text(row.get("product_name_snapshot")) or "(상품명없음)",
                to_text(row.get("category_snapshot")),
                to_text(row.get("option_name")),
                to_float(row.get("option_price")),
                to_text(row.get("item_discount_name")),
                to_text(row.get("order_discount_name")),
                to_float(row.get("item_discount_amount")),
                to_float(row.get("order_discount_amount")),
                to_float(row.get("qty")),
                to_float(row.get("unit_price")),
                to_float(row.get("gross_sales_amount")),
                to_float(row.get("net_sales_amount")),
                to_text(row.get("tax_flag")),
                to_float(row.get("vat_amount")),
                to_float(row.get("unit_cost")),
                to_float(row.get("profit_amount")),
            )
        )
        inserted_items += 1

    # 결제 테이블: 파일 1행당 1행 저장
    p_order_no = find_column(payments, ["주문번호"])
    p_pay_date = find_column(payments, ["결제일자", "결제 날짜"], required=False)
    p_pay_time = find_column(payments, ["결제시각", "결제 시간"], required=False)
    p_pay_method = find_column(payments, ["결제수단"])
    p_acquirer = find_column(payments, ["매입사", "카드매입사"], required=False)
    p_pay_amount = find_column(payments, ["결제금액", "실결제금액", "승인금액"], required=False)
    p_pay_status = find_column(payments, ["결제상태"])
    p_cancel_dt = find_column(payments, ["결제취소시각", "취소시각", "취소일시"], required=False)

    for _, row in payments.iterrows():
        order_no = to_text(row.get(p_order_no))
        order_id = order_id_map.get(order_no)
        if not order_id:
            continue
        pdate = parse_date_text(row.get(p_pay_date)) if p_pay_date else None
        ptime = parse_time_text(row.get(p_pay_time)) if p_pay_time else None
        cur.execute(
            """
            INSERT INTO retail_payment (
                order_id, payment_date, payment_time, payment_datetime,
                payment_method, acquirer_name, payment_amount,
                payment_status, cancel_datetime, approval_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                pdate,
                ptime,
                combine_datetime(pdate, ptime),
                to_text(row.get(p_pay_method)),
                to_text(row.get(p_acquirer)) if p_acquirer else None,
                to_float(row.get(p_pay_amount)) if p_pay_amount else 0.0,
                to_text(row.get(p_pay_status)),
                to_text(row.get(p_cancel_dt)) if p_cancel_dt else None,
                1,
            )
        )
        inserted_payments += 1

    conn.commit()
    return {
        "inserted_hdr": inserted_hdr,
        "updated_hdr": updated_hdr,
        "inserted_items": inserted_items,
        "inserted_payments": inserted_payments,
    }


# =========================
# 도매 등록/조회
# =========================
def insert_wholesale_sale(
    conn: sqlite3.Connection,
    sale_date: str,
    partner_id: int,
    item_type: str,
    cigar_product_id: Optional[int],
    non_cigar_product_id: Optional[int],
    qty: float,
    unit_price: float,
    unit_cost: float,
    notes: Optional[str] = None,
):
    sales_amount = qty * unit_price
    profit_amount = sales_amount - (qty * unit_cost)

    grade_code = None
    discount_rate_applied = 0.0
    row = conn.execute("SELECT current_grade_code FROM partner_mst WHERE id = ?", (partner_id,)).fetchone()
    if row:
        grade_code = row[0]

    conn.execute(
        """
        INSERT INTO wholesale_sales (
            sale_date, partner_id, item_type, cigar_product_id, non_cigar_product_id,
            qty, unit_price, unit_cost, sales_amount, profit_amount,
            grade_code_applied, discount_rate_applied, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sale_date,
            partner_id,
            item_type,
            cigar_product_id,
            non_cigar_product_id,
            qty,
            unit_price,
            unit_cost,
            sales_amount,
            profit_amount,
            grade_code,
            discount_rate_applied,
            notes,
        )
    )
    conn.commit()


def load_wholesale_history(conn: sqlite3.Connection, limit: int = 300) -> pd.DataFrame:
    if table_exists(conn, "v_wholesale_sales"):
        sql = f"SELECT * FROM v_wholesale_sales ORDER BY sale_date DESC, id DESC LIMIT {int(limit)}"
    else:
        sql = f"SELECT * FROM wholesale_sales ORDER BY sale_date DESC, id DESC LIMIT {int(limit)}"
    try:
        return pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame()


# =========================
# Streamlit UI
# =========================
def render_retail_upload_tab(conn: sqlite3.Connection):
    st.subheader("소매 판매 이력 일괄 업로드")
    st.caption("매출리포트 엑셀의 '결제 상세내역' / '상품 주문 상세내역' 시트를 읽어 소매 이력 테이블에 저장합니다.")

    uploaded = st.file_uploader(
        "소매 매출리포트 업로드",
        type=["xlsx", "xlsm", "xls"],
        key="retail_report_upload",
    )

    if not uploaded:
        st.info("업로드할 소매 매출리포트 파일을 선택해 주세요.")
        return

    try:
        payment_df, item_df, info = load_retail_excel(uploaded)
        hdr, items, payments, preview, unmatched = transform_retail_frames(payment_df, item_df, conn)
    except Exception as e:
        st.error(f"파일 분석 중 오류가 발생했습니다: {e}")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("결제 시트 행수", f"{info['payment_rows']:,}")
    c2.metric("상품 시트 행수", f"{info['item_rows']:,}")
    c3.metric("주문 건수", f"{len(hdr):,}")
    c4.metric("미매핑 상품", f"{len(unmatched):,}")

    with st.expander("업로드 미리보기", expanded=True):
        st.markdown("**상품행 미리보기**")
        st.dataframe(preview.head(50), use_container_width=True, hide_index=True)
        if len(unmatched) > 0:
            st.warning("아래 상품들은 마스터와 자동 매핑되지 않았습니다. 일단 업로드는 가능하지만, 추후 상품 마스터 정리가 필요합니다.")
            st.dataframe(unmatched.head(100), use_container_width=True, hide_index=True)
        else:
            st.success("모든 상품이 자동 매핑되었습니다.")

    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("소매 이력 업로드 실행", type="primary", use_container_width=True):
            try:
                result = upsert_retail_data(conn, hdr, items, payments)
                st.success(
                    f"업로드 완료 | 주문 신규 {result['inserted_hdr']:,}건 / 주문 갱신 {result['updated_hdr']:,}건 / 상품행 {result['inserted_items']:,}건 / 결제행 {result['inserted_payments']:,}건"
                )
            except Exception as e:
                st.error(f"업로드 저장 중 오류가 발생했습니다: {e}")
    with col2:
        st.caption("같은 주문번호가 이미 있으면 주문 헤더는 갱신하고, 상품행/결제행은 기존 데이터 삭제 후 다시 넣습니다.")


def render_wholesale_entry_tab(conn: sqlite3.Connection):
    st.subheader("도매 판매 직접 등록")
    st.caption("도매는 화면에서 직접 입력하는 구조를 기본으로 구성했습니다.")

    partner_df = load_partners(conn)
    cigar_df = load_cigar_products(conn)
    non_cigar_df = load_non_cigar_products(conn)

    if partner_df.empty:
        st.warning("partner_mst에 등록된 파트너가 없습니다. 먼저 파트너 마스터를 등록해 주세요.")
        return

    sale_date = st.date_input("판매일자", value=datetime.today().date(), key="wh_sale_date")
    partner_name = st.selectbox("파트너", partner_df["partner_name"].tolist(), key="wh_partner")
    item_type = st.radio("상품구분", ["cigar", "non_cigar"], horizontal=True, format_func=lambda x: "시가" if x == "cigar" else "비시가", key="wh_item_type")

    cigar_product_id = None
    non_cigar_product_id = None

    if item_type == "cigar":
        if cigar_df.empty:
            st.warning("product_mst에 시가 상품이 없습니다.")
            return
        cigar_label_map = {
            f"{row['product_name']} ({to_text(row.get('product_code')) or '-'})": int(row['id'])
            for _, row in cigar_df.iterrows()
        }
        cigar_label = st.selectbox("시가 상품", list(cigar_label_map.keys()), key="wh_cigar_product")
        cigar_product_id = cigar_label_map[cigar_label]
    else:
        if non_cigar_df.empty:
            st.warning("non_cigar_product_mst에 비시가 상품이 없습니다.")
            return
        non_label_map = {
            f"[{to_text(row.get('product_category')) or '-'}] {row['product_name']} ({to_text(row.get('product_code')) or '-'})": int(row['id'])
            for _, row in non_cigar_df.iterrows()
        }
        non_label = st.selectbox("비시가 상품", list(non_label_map.keys()), key="wh_non_product")
        non_cigar_product_id = non_label_map[non_label]

    c1, c2, c3 = st.columns(3)
    qty = c1.number_input("수량", min_value=0.0, value=1.0, step=1.0, key="wh_qty")
    unit_price = c2.number_input("판매단가", min_value=0.0, value=0.0, step=100.0, key="wh_unit_price")
    unit_cost = c3.number_input("원가", min_value=0.0, value=0.0, step=100.0, key="wh_unit_cost")

    sales_amount = qty * unit_price
    profit_amount = sales_amount - (qty * unit_cost)
    c4, c5 = st.columns(2)
    c4.metric("판매금액", f"{sales_amount:,.0f}")
    c5.metric("이익금액", f"{profit_amount:,.0f}")

    notes = st.text_area("비고", key="wh_notes")

    if st.button("도매 판매 저장", type="primary", use_container_width=True):
        try:
            partner_id = int(partner_df.loc[partner_df["partner_name"] == partner_name, "id"].iloc[0])
            insert_wholesale_sale(
                conn=conn,
                sale_date=str(sale_date),
                partner_id=partner_id,
                item_type=item_type,
                cigar_product_id=cigar_product_id,
                non_cigar_product_id=non_cigar_product_id,
                qty=float(qty),
                unit_price=float(unit_price),
                unit_cost=float(unit_cost),
                notes=notes,
            )
            st.success("도매 판매 이력이 저장되었습니다.")
        except Exception as e:
            st.error(f"저장 중 오류가 발생했습니다: {e}")


def render_wholesale_history_tab(conn: sqlite3.Connection):
    st.subheader("도매 판매 이력 조회")

    limit = st.selectbox("조회건수", [50, 100, 300, 500, 1000], index=2, key="wh_limit")
    df = load_wholesale_history(conn, limit=limit)
    if df.empty:
        st.info("조회할 도매 이력이 없습니다.")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)


# =========================
# 메인 render
# =========================
def render():
    st.title("판매관리")
    st.caption("소매는 엑셀 일괄 업로드, 도매는 화면 직접 입력 중심으로 구성한 페이지입니다.")

    conn = get_conn()
    ok, missing = validate_required_tables(conn)
    if not ok:
        st.error("필수 테이블이 부족합니다: " + ", ".join(missing))
        st.stop()

    tabs = st.tabs(["소매 업로드", "도매 직접등록", "도매 이력조회"])
    with tabs[0]:
        render_retail_upload_tab(conn)
    with tabs[1]:
        render_wholesale_entry_tab(conn)
    with tabs[2]:
        render_wholesale_history_tab(conn)


if __name__ == "__main__":
    render()
