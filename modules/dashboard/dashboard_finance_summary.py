import os
import sqlite3
from typing import Tuple

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def object_exists(conn: sqlite3.Connection, name: str, obj_type: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
        (obj_type, name),
    )
    return cur.fetchone() is not None


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return object_exists(conn, table_name, "table")


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    return object_exists(conn, view_name, "view")


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [r[1] for r in rows]
    except Exception:
        return []


def pick_col(cols: list[str], candidates: list[str]):
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


def month_range(year: int, month: int) -> Tuple[str, str]:
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def prev_month(year: int, month: int) -> Tuple[int, int]:
    dt = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(months=1)
    return dt.year, dt.month


def fmt_krw(value) -> str:
    try:
        return f"₩{float(value):,.0f}"
    except Exception:
        return "₩0"


def fmt_delta_krw(curr, prev) -> str:
    try:
        diff = float(curr) - float(prev)
        sign = "+" if diff > 0 else ""
        return f"{sign}₩{diff:,.0f}"
    except Exception:
        return "₩0"


def fmt_pct(value) -> str:
    try:
        return f"{float(value):.1f}%"
    except Exception:
        return "0.0%"


def get_non_cigar_purchase_price_map(conn) -> dict:
    if not table_exists(conn, "non_cigar_product_mst"):
        return {}

    cols = get_table_columns(conn, "non_cigar_product_mst")
    if "product_code" not in cols or "purchase_price" not in cols:
        return {}

    sql = """
        SELECT
            TRIM(COALESCE(product_code, '')) AS product_code,
            COALESCE(purchase_price, 0) AS purchase_price
        FROM non_cigar_product_mst
    """
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception:
        return {}

    if df.empty:
        return {}

    df["product_code"] = df["product_code"].astype(str).str.strip()
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
    df = df[df["product_code"] != ""].copy()

    return dict(zip(df["product_code"], df["purchase_price"]))


def get_retail_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    """
    - 시가는 기존 로직 유지
    - 시가 외 상품만 purchase_price 기준으로 원가/이익 재계산
    """
    purchase_price_map = get_non_cigar_purchase_price_map(conn)

    if view_exists(conn, "v_retail_sales_enriched"):
        vcols = get_table_columns(conn, "v_retail_sales_enriched")

        sale_date_col = pick_col(vcols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(vcols, ["net_sales_amount", "sales_amount", "amount"])
        cost_col = pick_col(vcols, ["total_korea_cost_krw", "total_cost_krw"])
        gp_col = pick_col(vcols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])
        product_code_col = pick_col(vcols, ["product_code", "product_code_raw"])
        qty_col = pick_col(vcols, ["qty", "quantity"])

        if not sale_date_col or not sales_col:
            return pd.DataFrame()

        sql = f"""
            SELECT
                {sale_date_col} AS sale_date,
                COALESCE({sales_col}, 0) AS net_sales_amount,
                {"COALESCE(" + cost_col + ", 0)" if cost_col else "0"} AS total_korea_cost_krw,
                {"COALESCE(" + gp_col + ", 0)" if gp_col else "0"} AS gross_profit_krw,
                {"COALESCE(" + product_code_col + ", '')" if product_code_col else "''"} AS product_code,
                {"COALESCE(" + qty_col + ", 0)" if qty_col else "0"} AS qty
            FROM v_retail_sales_enriched
            WHERE {sale_date_col} BETWEEN ? AND ?
        """
        try:
            df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        except Exception:
            df = pd.DataFrame()

        if not df.empty:
            for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw", "qty"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

            df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()

            # 시가 외 항목만 재계산
            non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())

            df.loc[non_cigar_mask, "_purchase_price"] = (
                df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
            )
            df.loc[non_cigar_mask, "total_korea_cost_krw"] = (
                df.loc[non_cigar_mask, "_purchase_price"] * df.loc[non_cigar_mask, "qty"]
            )
            df.loc[non_cigar_mask, "gross_profit_krw"] = (
                df.loc[non_cigar_mask, "net_sales_amount"] - df.loc[non_cigar_mask, "total_korea_cost_krw"]
            )

            if "_purchase_price" in df.columns:
                df = df.drop(columns=["_purchase_price"])

            return df

    if table_exists(conn, "retail_sales"):
        cols = get_table_columns(conn, "retail_sales")
        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(cols, ["net_sales_amount", "sales_amount", "amount"])
        product_code_col = pick_col(cols, ["product_code", "product_code_raw"])
        qty_col = pick_col(cols, ["qty", "quantity"])
        gp_col = pick_col(cols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])
        cost_col = pick_col(cols, ["total_korea_cost_krw", "total_cost_krw"])

        if not sale_date_col or not sales_col:
            return pd.DataFrame()

        sql = f"""
            SELECT
                {sale_date_col} AS sale_date,
                COALESCE({sales_col}, 0) AS net_sales_amount,
                {"COALESCE(" + cost_col + ", 0)" if cost_col else "0"} AS total_korea_cost_krw,
                {"COALESCE(" + gp_col + ", 0)" if gp_col else "0"} AS gross_profit_krw,
                {"COALESCE(" + product_code_col + ", '')" if product_code_col else "''"} AS product_code,
                {"COALESCE(" + qty_col + ", 0)" if qty_col else "0"} AS qty
            FROM retail_sales
            WHERE {sale_date_col} BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

        if not df.empty:
            for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw", "qty"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

            df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()

            # 시가 외 항목만 재계산
            non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())

            df.loc[non_cigar_mask, "_purchase_price"] = (
                df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
            )
            df.loc[non_cigar_mask, "total_korea_cost_krw"] = (
                df.loc[non_cigar_mask, "_purchase_price"] * df.loc[non_cigar_mask, "qty"]
            )
            df.loc[non_cigar_mask, "gross_profit_krw"] = (
                df.loc[non_cigar_mask, "net_sales_amount"] - df.loc[non_cigar_mask, "total_korea_cost_krw"]
            )

            if "_purchase_price" in df.columns:
                df = df.drop(columns=["_purchase_price"])

        return df

    return pd.DataFrame()


def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        cols = get_table_columns(conn, "v_wholesale_sales")
        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(cols, ["sales_amount", "net_sales_amount", "amount"])
        qty_col = pick_col(cols, ["qty", "quantity"])
        unit_cost_col = pick_col(cols, ["unit_cost", "cost_per_unit"])
        gp_col = pick_col(cols, ["profit_amount", "gross_profit_krw", "margin_amount"])

        if sale_date_col and sales_col:
            qty_expr = f"COALESCE({qty_col}, 0)" if qty_col else "0"
            unit_cost_expr = f"COALESCE({unit_cost_col}, 0)" if unit_cost_col else "0"
            gp_expr = f"COALESCE({gp_col}, 0)" if gp_col else "0"

            sql = f"""
                SELECT
                    {sale_date_col} AS sale_date,
                    COALESCE({sales_col}, 0) AS net_sales_amount,
                    ({qty_expr} * {unit_cost_expr}) AS total_korea_cost_krw,
                    {gp_expr} AS gross_profit_krw
                FROM v_wholesale_sales
                WHERE {sale_date_col} BETWEEN ? AND ?
            """
            try:
                df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        if not df.empty:
            for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            return df

    if table_exists(conn, "wholesale_sales"):
        cols = get_table_columns(conn, "wholesale_sales")
        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(cols, ["sales_amount", "net_sales_amount", "amount"])
        qty_col = pick_col(cols, ["qty", "quantity"])
        unit_cost_col = pick_col(cols, ["unit_cost", "cost_per_unit"])
        gp_col = pick_col(cols, ["profit_amount", "gross_profit_krw", "margin_amount"])

        if not sale_date_col or not sales_col:
            return pd.DataFrame()

        qty_expr = f"COALESCE({qty_col}, 0)" if qty_col else "0"
        unit_cost_expr = f"COALESCE({unit_cost_col}, 0)" if unit_cost_col else "0"
        gp_expr = f"COALESCE({gp_col}, 0)" if gp_col else "0"

        sql = f"""
            SELECT
                {sale_date_col} AS sale_date,
                COALESCE({sales_col}, 0) AS net_sales_amount,
                ({qty_expr} * {unit_cost_expr}) AS total_korea_cost_krw,
                {gp_expr} AS gross_profit_krw
            FROM wholesale_sales
            WHERE {sale_date_col} BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

        for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df

    return pd.DataFrame()


def get_expense_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    cols = get_table_columns(conn, "expense_txn")
    date_col = pick_col(cols, ["expense_date", "date", "txn_date"])
    amount_col = pick_col(cols, ["amount", "expense_amount"])

    if not date_col or not amount_col:
        return pd.DataFrame()

    sql = f"""
        SELECT
            {date_col} AS expense_date,
            COALESCE({amount_col}, 0) AS amount
        FROM expense_txn
        WHERE {date_col} BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    return df


def get_month_summary(conn, year: int, month: int) -> dict:
    date_from, date_to = month_range(year, month)

    retail_df = get_retail_month_data(conn, date_from, date_to)
    wholesale_df = get_wholesale_month_data(conn, date_from, date_to)
    expense_df = get_expense_month_data(conn, date_from, date_to)

    retail_sales = float(retail_df["net_sales_amount"].sum()) if not retail_df.empty else 0
    wholesale_sales = float(wholesale_df["net_sales_amount"].sum()) if not wholesale_df.empty else 0
    total_sales = retail_sales + wholesale_sales

    retail_cost = float(retail_df["total_korea_cost_krw"].sum()) if not retail_df.empty else 0
    wholesale_cost = float(wholesale_df["total_korea_cost_krw"].sum()) if not wholesale_df.empty else 0
    total_cost = retail_cost + wholesale_cost

    retail_gp = float(retail_df["gross_profit_krw"].sum()) if not retail_df.empty else 0
    wholesale_gp = float(wholesale_df["gross_profit_krw"].sum()) if not wholesale_df.empty else 0
    gross_profit = retail_gp + wholesale_gp

    if gross_profit == 0 and total_sales != 0:
        gross_profit = total_sales - total_cost

    expense_total = float(expense_df["amount"].sum()) if not expense_df.empty else 0
    operating_profit = gross_profit - expense_total

    return {
        "year": year,
        "month": month,
        "date_from": date_from,
        "date_to": date_to,
        "retail_sales": retail_sales,
        "wholesale_sales": wholesale_sales,
        "total_sales": total_sales,
        "total_cost": total_cost,
        "gross_profit": gross_profit,
        "expense_total": expense_total,
        "operating_profit": operating_profit,
    }


def get_monthly_trend(conn, months: int = 12) -> pd.DataFrame:
    base = pd.Timestamp.today().replace(day=1)
    rows = []

    for i in range(months - 1, -1, -1):
        dt = base - pd.DateOffset(months=i)
        summary = get_month_summary(conn, dt.year, dt.month)
        rows.append(
            {
                "월": f"{dt.year}-{dt.month:02d}",
                "소매매출": summary["retail_sales"],
                "도매매출": summary["wholesale_sales"],
                "총매출": summary["total_sales"],
                "매출총이익": summary["gross_profit"],
                "지출": summary["expense_total"],
                "영업이익": summary["operating_profit"],
            }
        )

    return pd.DataFrame(rows)


def get_recent_expenses(conn, limit: int = 10) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    txn_cols = get_table_columns(conn, "expense_txn")
    cat_cols = get_table_columns(conn, "expense_category_mst") if table_exists(conn, "expense_category_mst") else []

    expense_date_col = pick_col(txn_cols, ["expense_date", "date", "txn_date"])
    amount_col = pick_col(txn_cols, ["amount", "expense_amount"])
    vendor_col = pick_col(txn_cols, ["vendor_name", "vendor", "partner_name"])
    payment_col = pick_col(txn_cols, ["payment_method", "payment_type"])
    category_id_col = pick_col(txn_cols, ["expense_category_id", "category_id"])

    if not expense_date_col or not amount_col:
        return pd.DataFrame()

    if table_exists(conn, "expense_category_mst") and category_id_col:
        expense_group_col = pick_col(cat_cols, ["expense_group", "group_name"])
        expense_name_col = pick_col(cat_cols, ["expense_name", "category_name", "name"])

        group_expr = f"c.{expense_group_col}" if expense_group_col else "''"
        name_expr = f"c.{expense_name_col}" if expense_name_col else "''"
        vendor_expr = f"COALESCE(t.{vendor_col}, '')" if vendor_col else "''"
        payment_expr = f"COALESCE(t.{payment_col}, '')" if payment_col else "''"

        sql = f"""
            SELECT
                t.{expense_date_col} AS expense_date,
                {group_expr} AS expense_group,
                {name_expr} AS expense_name,
                t.{amount_col} AS amount,
                {vendor_expr} AS vendor_name,
                {payment_expr} AS payment_method
            FROM expense_txn t
            LEFT JOIN expense_category_mst c
              ON t.{category_id_col} = c.id
            ORDER BY t.{expense_date_col} DESC, t.id DESC
            LIMIT ?
        """
    else:
        vendor_expr = f"COALESCE({vendor_col}, '')" if vendor_col else "''"
        payment_expr = f"COALESCE({payment_col}, '')" if payment_col else "''"

        sql = f"""
            SELECT
                {expense_date_col} AS expense_date,
                '' AS expense_group,
                '' AS expense_name,
                {amount_col} AS amount,
                {vendor_expr} AS vendor_name,
                {payment_expr} AS payment_method
            FROM expense_txn
            ORDER BY {expense_date_col} DESC, id DESC
            LIMIT ?
        """

    df = pd.read_sql_query(sql, conn, params=[limit])

    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    return df


def get_recent_sales_with_margin(conn, limit: int = 20) -> pd.DataFrame:
    """
    - 시가는 기존 마진 유지
    - 시가 외만 판매금액 - (매입가 × 수량)
    """
    empty = pd.DataFrame(
        columns=[
            "sale_date", "product_code", "product_name", "qty",
            "unit_price", "net_sales_amount", "매입가", "마진"
        ]
    )

    if not table_exists(conn, "retail_sales"):
        return empty

    cols = get_table_columns(conn, "retail_sales")
    sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
    product_code_col = pick_col(cols, ["product_code", "product_code_raw"])
    product_name_col = pick_col(cols, ["product_name", "mst_product_name", "item_name"])
    qty_col = pick_col(cols, ["qty", "quantity"])
    unit_price_col = pick_col(cols, ["unit_price", "price"])
    sales_col = pick_col(cols, ["net_sales_amount", "sales_amount", "amount"])
    margin_col = pick_col(cols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])

    if not sale_date_col or not sales_col:
        return empty

    sql = f"""
        SELECT
            {sale_date_col} AS sale_date,
            {"COALESCE(" + product_code_col + ", '')" if product_code_col else "''"} AS product_code,
            {"COALESCE(" + product_name_col + ", '')" if product_name_col else "''"} AS product_name,
            {"COALESCE(" + qty_col + ", 0)" if qty_col else "0"} AS qty,
            {"COALESCE(" + unit_price_col + ", 0)" if unit_price_col else "0"} AS unit_price,
            COALESCE({sales_col}, 0) AS net_sales_amount,
            {"COALESCE(" + margin_col + ", 0)" if margin_col else "0"} AS base_margin
        FROM retail_sales
        ORDER BY {sale_date_col} DESC, id DESC
        LIMIT ?
    """
    df = pd.read_sql_query(sql, conn, params=[limit])

    if df.empty:
        return empty

    for c in ["qty", "unit_price", "net_sales_amount", "base_margin"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
    df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()

    purchase_price_map = get_non_cigar_purchase_price_map(conn)

    # 기본은 기존 마진
    df["매입가"] = 0
    df["마진"] = df["base_margin"]

    # 시가 외만 재계산
    non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())

    df.loc[non_cigar_mask, "매입가"] = (
        df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
    )
    df.loc[non_cigar_mask, "마진"] = (
        df.loc[non_cigar_mask, "net_sales_amount"]
        - (df.loc[non_cigar_mask, "매입가"] * df.loc[non_cigar_mask, "qty"])
    )

    return df[
        ["sale_date", "product_code", "product_name", "qty", "unit_price", "net_sales_amount", "매입가", "마진"]
    ]


def get_top_products(conn, date_from: str, date_to: str, limit: int = 10, metric: str = "sales") -> pd.DataFrame:
    frames = []

    retail_metric_sql = "COALESCE(net_sales_amount, 0)"
    wholesale_metric_sql = "COALESCE(sales_amount, 0)"
    metric_col_name = "metric_value"

    if metric == "profit":
        retail_metric_sql = "COALESCE(retail_gross_profit_krw, 0)"
        wholesale_metric_sql = "COALESCE(profit_amount, 0)"

    if view_exists(conn, "v_retail_sales_enriched"):
        cols = get_table_columns(conn, "v_retail_sales_enriched")
        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        product_code_col = pick_col(cols, ["product_code", "product_code_raw"])
        product_name_col = pick_col(cols, ["mst_product_name", "product_name", "product_code_raw"])

        if sale_date_col:
            code_expr = f"COALESCE({product_code_col}, '')" if product_code_col else "''"
            name_expr = f"COALESCE({product_name_col}, {code_expr})" if product_name_col else code_expr

            sql = f"""
                SELECT
                    {code_expr} AS product_code,
                    {name_expr} AS product_name,
                    {retail_metric_sql} AS {metric_col_name}
                FROM v_retail_sales_enriched
                WHERE {sale_date_col} BETWEEN ? AND ?
            """
            try:
                retail_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
                if not retail_df.empty:
                    retail_df[metric_col_name] = pd.to_numeric(retail_df[metric_col_name], errors="coerce").fillna(0)
                    frames.append(retail_df)
            except Exception:
                pass

    if view_exists(conn, "v_wholesale_sales"):
        cols = get_table_columns(conn, "v_wholesale_sales")
        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        product_code_col = pick_col(cols, ["product_code"])
        product_name_col = pick_col(cols, ["product_name"])

        if sale_date_col:
            code_expr = f"COALESCE({product_code_col}, '')" if product_code_col else "''"
            name_expr = f"COALESCE({product_name_col}, {code_expr})" if product_name_col else code_expr

            sql = f"""
                SELECT
                    {code_expr} AS product_code,
                    {name_expr} AS product_name,
                    {wholesale_metric_sql} AS {metric_col_name}
                FROM v_wholesale_sales
                WHERE {sale_date_col} BETWEEN ? AND ?
            """
            try:
                wholesale_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
                if not wholesale_df.empty:
                    wholesale_df[metric_col_name] = pd.to_numeric(wholesale_df[metric_col_name], errors="coerce").fillna(0)
                    frames.append(wholesale_df)
            except Exception:
                pass

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = (
        df.groupby(["product_code", "product_name"], dropna=False)[metric_col_name]
        .sum()
        .reset_index()
        .sort_values(metric_col_name, ascending=False)
        .head(limit)
    )

    return df


def render():
    st.subheader("경영 대시보드")

    conn = get_conn()

    try:
        today = pd.Timestamp.today()
        current_year = today.year
        current_month = today.month

        y1, y2 = st.columns([1, 1])
        with y1:
            year = st.selectbox(
                "연도",
                options=list(range(current_year - 2, current_year + 1)),
                index=2,
            )
        with y2:
            month = st.selectbox(
                "월",
                options=list(range(1, 13)),
                index=current_month - 1,
            )

        current = get_month_summary(conn, year, month)
        py, pm = prev_month(year, month)
        previous = get_month_summary(conn, py, pm)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "월 총매출",
            fmt_krw(current["total_sales"]),
            fmt_delta_krw(current["total_sales"], previous["total_sales"]),
        )
        c2.metric(
            "월 지출",
            fmt_krw(current["expense_total"]),
            fmt_delta_krw(current["expense_total"], previous["expense_total"]),
        )
        c3.metric(
            "월 매출총이익",
            fmt_krw(current["gross_profit"]),
            fmt_delta_krw(current["gross_profit"], previous["gross_profit"]),
        )
        c4.metric(
            "월 영업이익",
            fmt_krw(current["operating_profit"]),
            fmt_delta_krw(current["operating_profit"], previous["operating_profit"]),
        )

        c5, c6, c7, c8 = st.columns(4)
        op_margin = (
            current["operating_profit"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )
        sales_mix_retail = (
            current["retail_sales"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )
        sales_mix_wholesale = (
            current["wholesale_sales"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )

        c5.metric("소매매출", fmt_krw(current["retail_sales"]))
        c6.metric("도매매출", fmt_krw(current["wholesale_sales"]))
        c7.metric("영업이익률", fmt_pct(op_margin))
        c8.metric("소매/도매 비중", f"{sales_mix_retail:.0f}% / {sales_mix_wholesale:.0f}%")

        st.caption(
            f"기준기간: {current['date_from']} ~ {current['date_to']} / "
            f"비교기간: {previous['date_from']} ~ {previous['date_to']}"
        )

        tab1, tab2, tab3, tab4 = st.tabs(["월별 추이", "최근 판매 내역", "최근 지출", "상위 제품"])

        with tab1:
            trend_df = get_monthly_trend(conn, months=12)

            if not trend_df.empty:
                show_trend_df = trend_df.copy()
                money_cols = ["소매매출", "도매매출", "총매출", "매출총이익", "지출", "영업이익"]
                for col in money_cols:
                    show_trend_df[col] = show_trend_df[col].apply(fmt_krw)

                st.dataframe(
                    show_trend_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                st.markdown("###### 월별 매출 추이")
                st.line_chart(
                    trend_df.set_index("월")[["소매매출", "도매매출", "총매출"]]
                )

                st.markdown("###### 월별 손익 추이")
                st.line_chart(
                    trend_df.set_index("월")[["매출총이익", "지출", "영업이익"]]
                )
            else:
                st.info("월별 추이 데이터가 없습니다.")

        with tab2:
            recent_sales_df = get_recent_sales_with_margin(conn, limit=20)

            if recent_sales_df.empty:
                st.info("최근 판매 내역이 없습니다.")
            else:
                show_sales_df = recent_sales_df.rename(
                    columns={
                        "sale_date": "판매일자",
                        "product_code": "상품코드",
                        "product_name": "상품명",
                        "qty": "수량",
                        "unit_price": "단가",
                        "net_sales_amount": "매출액",
                    }
                ).copy()

                for col in ["단가", "매출액", "매입가", "마진"]:
                    show_sales_df[col] = show_sales_df[col].apply(fmt_krw)

                st.dataframe(
                    show_sales_df[
                        ["판매일자", "상품코드", "상품명", "수량", "단가", "매출액", "매입가", "마진"]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                st.caption("※ 시가는 기존 마진 로직을 유지하고, 시가 외 상품만 판매금액 - (매입가 × 수량) 기준으로 계산합니다.")

        with tab3:
            recent_exp_df = get_recent_expenses(conn, limit=12)

            if recent_exp_df.empty:
                st.info("최근 지출 데이터가 없습니다.")
            else:
                recent_exp_df = recent_exp_df.rename(
                    columns={
                        "expense_date": "지출일자",
                        "expense_group": "지출그룹",
                        "expense_name": "지출항목",
                        "amount": "금액",
                        "vendor_name": "거래처",
                        "payment_method": "결제수단",
                    }
                )
                recent_exp_df["금액"] = recent_exp_df["금액"].apply(fmt_krw)

                st.dataframe(
                    recent_exp_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

        with tab4:
            basis = st.radio(
                "집계 기준",
                options=["매출액", "마진"],
                horizontal=True,
                key="dashboard_top_product_basis",
            )

            metric = "sales" if basis == "매출액" else "profit"
            value_col = "metric_value"
            value_label = "매출액" if basis == "매출액" else "마진"

            top_df = get_top_products(
                conn,
                current["date_from"],
                current["date_to"],
                limit=10,
                metric=metric,
            )

            if top_df.empty:
                st.info("상위 제품 데이터가 없습니다.")
            else:
                show_top_df = top_df.rename(
                    columns={
                        "product_code": "상품코드",
                        "product_name": "상품명",
                        value_col: value_label,
                    }
                ).copy()

                show_top_df[value_label] = show_top_df[value_label].apply(fmt_krw)

                st.dataframe(
                    show_top_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                chart_df = top_df.copy()
                chart_df["상품코드_표시"] = chart_df["product_code"].fillna("").astype(str).str.strip()
                chart_df.loc[chart_df["상품코드_표시"] == "", "상품코드_표시"] = (
                    chart_df.loc[chart_df["상품코드_표시"] == "", "product_name"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                )

                st.bar_chart(chart_df.set_index("상품코드_표시")[[value_col]])

    finally:
        conn.close()


if __name__ == "__main__":
    render()