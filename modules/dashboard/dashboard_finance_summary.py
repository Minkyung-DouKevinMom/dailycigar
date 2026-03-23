import os
import sqlite3
from typing import Optional, Tuple

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


def month_range(year: int, month: int) -> Tuple[str, str]:
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def prev_month(year: int, month: int) -> Tuple[int, int]:
    dt = pd.Timestamp(year=year, month=month, day=1) - pd.offsets.MonthBegin(1)
    return dt.year, dt.month


def get_retail_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_retail_sales_enriched"):
        sql = """
            SELECT
                sale_date,
                net_sales_amount,
                COALESCE(total_korea_cost_krw, 0) AS total_korea_cost_krw,
                COALESCE(retail_gross_profit_krw, 0) AS gross_profit_krw
            FROM v_retail_sales_enriched
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
    elif table_exists(conn, "retail_sales"):
        sql = """
            SELECT
                sale_date,
                net_sales_amount
            FROM retail_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        df["total_korea_cost_krw"] = 0
        df["gross_profit_krw"] = 0
    else:
        return pd.DataFrame()

    for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        sql = """
            SELECT
                sale_date,
                COALESCE(sales_amount, 0) AS net_sales_amount,
                COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS total_korea_cost_krw,
                COALESCE(profit_amount, 0) AS gross_profit_krw
            FROM v_wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
    elif table_exists(conn, "wholesale_sales"):
        sql = """
            SELECT
                sale_date,
                COALESCE(sales_amount, 0) AS net_sales_amount,
                COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS total_korea_cost_krw,
                COALESCE(profit_amount, 0) AS gross_profit_krw
            FROM wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
    else:
        return pd.DataFrame()

    for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def get_expense_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    sql = """
        SELECT
            expense_date,
            COALESCE(amount, 0) AS amount
        FROM expense_txn
        WHERE expense_date BETWEEN ? AND ?
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
        rows.append({
            "월": f"{dt.year}-{dt.month:02d}",
            "소매매출": summary["retail_sales"],
            "도매매출": summary["wholesale_sales"],
            "총매출": summary["total_sales"],
            "매출총이익": summary["gross_profit"],
            "지출": summary["expense_total"],
            "영업이익": summary["operating_profit"],
        })

    return pd.DataFrame(rows)


def get_recent_expenses(conn, limit: int = 10) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    sql = """
        SELECT
            t.expense_date,
            c.expense_group,
            c.expense_name,
            t.amount,
            t.vendor_name,
            t.payment_method
        FROM expense_txn t
        LEFT JOIN expense_category_mst c
          ON t.expense_category_id = c.id
        ORDER BY t.expense_date DESC, t.id DESC
        LIMIT ?
    """
    df = pd.read_sql_query(sql, conn, params=[limit])
    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df


def get_top_products(conn, date_from: str, date_to: str, limit: int = 10) -> pd.DataFrame:
    frames = []

    if view_exists(conn, "v_retail_sales_enriched"):
        sql = """
            SELECT
                COALESCE(product_code, product_code_raw) AS product_code,
                COALESCE(mst_product_name, product_code_raw) AS product_name,
                COALESCE(net_sales_amount, 0) AS sales_amount
            FROM v_retail_sales_enriched
            WHERE sale_date BETWEEN ? AND ?
        """
        retail_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        if not retail_df.empty:
            retail_df["sales_amount"] = pd.to_numeric(retail_df["sales_amount"], errors="coerce").fillna(0)
            frames.append(retail_df)

    if view_exists(conn, "v_wholesale_sales"):
        sql = """
            SELECT
                COALESCE(product_code, '') AS product_code,
                COALESCE(product_name, product_code) AS product_name,
                COALESCE(sales_amount, 0) AS sales_amount
            FROM v_wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        wholesale_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        if not wholesale_df.empty:
            wholesale_df["sales_amount"] = pd.to_numeric(wholesale_df["sales_amount"], errors="coerce").fillna(0)
            frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = (
        df.groupby(["product_code", "product_name"], dropna=False)["sales_amount"]
        .sum()
        .reset_index()
        .sort_values("sales_amount", ascending=False)
        .head(limit)
    )
    return df


def render():
    st.subheader("대시보드")

    conn = get_conn()
    try:
        today = pd.Timestamp.today()
        current_year = today.year
        current_month = today.month

        y1, y2 = st.columns([1, 1])
        with y1:
            year = st.selectbox("연도", options=list(range(current_year - 2, current_year + 1)), index=2)
        with y2:
            month = st.selectbox("월", options=list(range(1, 13)), index=current_month - 1)

        current = get_month_summary(conn, year, month)
        py, pm = prev_month(year, month)
        previous = get_month_summary(conn, py, pm)

        def delta(curr, prev):
            return curr - prev

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("월 총매출", f"{current['total_sales']:,.0f}", f"{delta(current['total_sales'], previous['total_sales']):,.0f}")
        c2.metric("월 지출", f"{current['expense_total']:,.0f}", f"{delta(current['expense_total'], previous['expense_total']):,.0f}")
        c3.metric("월 매출총이익", f"{current['gross_profit']:,.0f}", f"{delta(current['gross_profit'], previous['gross_profit']):,.0f}")
        c4.metric("월 영업이익", f"{current['operating_profit']:,.0f}", f"{delta(current['operating_profit'], previous['operating_profit']):,.0f}")

        c5, c6, c7, c8 = st.columns(4)
        op_margin = (current["operating_profit"] / current["total_sales"] * 100) if current["total_sales"] else 0
        sales_mix_retail = (current["retail_sales"] / current["total_sales"] * 100) if current["total_sales"] else 0
        sales_mix_wholesale = (current["wholesale_sales"] / current["total_sales"] * 100) if current["total_sales"] else 0
        c5.metric("소매매출", f"{current['retail_sales']:,.0f}")
        c6.metric("도매매출", f"{current['wholesale_sales']:,.0f}")
        c7.metric("영업이익률", f"{op_margin:.1f}%")
        c8.metric("소매/도매 비중", f"{sales_mix_retail:.0f}% / {sales_mix_wholesale:.0f}%")

        st.caption(f"기준기간: {current['date_from']} ~ {current['date_to']} / 비교기간: {previous['date_from']} ~ {previous['date_to']}")

        tab1, tab2, tab3 = st.tabs(["월별 추이", "최근 지출", "상위 제품"])

        with tab1:
            trend_df = get_monthly_trend(conn, months=12)
            st.dataframe(trend_df, use_container_width=True, hide_index=True, height=360)
            if not trend_df.empty:
                st.line_chart(trend_df.set_index("월")[["총매출", "지출", "영업이익"]])

        with tab2:
            recent_exp_df = get_recent_expenses(conn, limit=12)
            if recent_exp_df.empty:
                st.info("최근 지출 데이터가 없습니다.")
            else:
                st.dataframe(recent_exp_df, use_container_width=True, hide_index=True, height=360)

        with tab3:
            top_df = get_top_products(conn, current["date_from"], current["date_to"], limit=10)
            if top_df.empty:
                st.info("상위 제품 데이터가 없습니다.")
            else:
                st.dataframe(top_df, use_container_width=True, hide_index=True, height=360)
                chart_df = top_df.copy()
                chart_df["라벨"] = chart_df.apply(
                    lambda x: x["product_name"] if str(x["product_name"]).strip() else x["product_code"],
                    axis=1,
                )
                st.bar_chart(chart_df.set_index("라벨")[["sales_amount"]])

    finally:
        conn.close()


if __name__ == "__main__":
    render()
