import os
import sqlite3
from io import BytesIO
from typing import Dict, List, Optional, Tuple

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


def choose_source(conn: sqlite3.Connection, candidates: List[str]) -> Optional[str]:
    for name in candidates:
        if table_exists(conn, name) or view_exists(conn, name):
            return name
    return None


def to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    output.seek(0)
    return output.getvalue()


def monthify(series):
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m")


# =========================================================
# Retail
# =========================================================
def get_retail_data(conn, date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.DataFrame, str]:
    source = choose_source(conn, ["v_retail_sales_enriched", "retail_sales"])
    if not source:
        return pd.DataFrame(), ""

    if source == "v_retail_sales_enriched":
        sql = """
            SELECT
                sale_date,
                sale_datetime,
                order_no,
                order_channel,
                payment_status,
                product_code,
                product_code_raw,
                mst_product_name,
                mst_size_name,
                category,
                qty,
                net_sales_amount,
                vat_amount,
                total_korea_cost_krw,
                retail_gross_profit_krw
            FROM v_retail_sales_enriched
            WHERE 1=1
        """
    else:
        sql = """
            SELECT
                sale_date,
                sale_datetime,
                order_no,
                order_channel,
                payment_status,
                product_code,
                product_code_raw,
                category,
                qty,
                net_sales_amount,
                vat_amount
            FROM retail_sales
            WHERE 1=1
        """

    params = []
    if date_from:
        sql += " AND sale_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND sale_date <= ?"
        params.append(date_to)

    sql += " ORDER BY sale_date DESC, id DESC" if source == "retail_sales" else " ORDER BY sale_date DESC"
    df = pd.read_sql_query(sql, conn, params=params)

    for c in ["qty", "net_sales_amount", "vat_amount", "total_korea_cost_krw", "retail_gross_profit_krw"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df, source


# =========================================================
# Wholesale - v_wholesale_sales 기준
# =========================================================
def get_wholesale_data(conn, date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.DataFrame, str]:
    source = choose_source(conn, ["v_wholesale_sales", "wholesale_sales"])
    if not source:
        return pd.DataFrame(), ""

    if source == "v_wholesale_sales":
        sql = """
            SELECT
                id,
                sale_date,
                partner_name,
                item_type,
                product_name,
                product_code,
                qty,
                unit_price,
                unit_cost,
                sales_amount,
                profit_amount,
                grade_code_applied,
                discount_rate_applied,
                notes,
                created_at,
                updated_at
            FROM v_wholesale_sales
            WHERE 1=1
        """
        params = []
        if date_from:
            sql += " AND sale_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND sale_date <= ?"
            params.append(date_to)
        sql += " ORDER BY sale_date DESC, id DESC"

        df = pd.read_sql_query(sql, conn, params=params)
        if not df.empty:
            df["net_sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
            df["gross_profit_krw"] = pd.to_numeric(df["profit_amount"], errors="coerce").fillna(0)
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
            df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce").fillna(0)
            df["total_korea_cost_krw"] = df["qty"] * df["unit_cost"]
            df["vat_amount"] = 0.0
        return df, source

    # fallback: wholesale_sales 원본 테이블
    sql = """
        SELECT *
        FROM wholesale_sales
        WHERE 1=1
    """
    params = []
    if date_from:
        sql += " AND sale_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND sale_date <= ?"
        params.append(date_to)
    sql += " ORDER BY sale_date DESC, id DESC"
    df = pd.read_sql_query(sql, conn, params=params)

    rename_map = {}
    if "sales_amount" in df.columns:
        rename_map["sales_amount"] = "net_sales_amount"
    if "profit_amount" in df.columns:
        rename_map["profit_amount"] = "gross_profit_krw"
    if rename_map:
        df = df.rename(columns=rename_map)

    for c in ["qty", "net_sales_amount", "gross_profit_krw", "unit_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "total_korea_cost_krw" not in df.columns and "qty" in df.columns and "unit_cost" in df.columns:
        df["total_korea_cost_krw"] = df["qty"] * df["unit_cost"]
    if "vat_amount" not in df.columns:
        df["vat_amount"] = 0.0

    return df, source


# =========================================================
# Expense
# =========================================================
def get_expense_data(conn, date_from: Optional[str], date_to: Optional[str]) -> pd.DataFrame:
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
        WHERE 1=1
    """
    params = []
    if date_from:
        sql += " AND t.expense_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND t.expense_date <= ?"
        params.append(date_to)

    sql += " ORDER BY t.expense_date DESC, t.id DESC"
    df = pd.read_sql_query(sql, conn, params=params)
    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df


# =========================================================
# 화면 1: 매출통합조회
# =========================================================
def render_sales_combined():
    st.markdown("### 매출통합조회")

    conn = get_conn()
    try:
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("시작일", value=None, key="fin_sales_from")
        with c2:
            date_to = st.date_input("종료일", value=None, key="fin_sales_to")

        dfrom = str(date_from) if date_from else None
        dto = str(date_to) if date_to else None

        retail_df, retail_source = get_retail_data(conn, dfrom, dto)
        wholesale_df, wholesale_source = get_wholesale_data(conn, dfrom, dto)

        retail_sales = float(retail_df["net_sales_amount"].sum()) if "net_sales_amount" in retail_df.columns else 0
        retail_profit = float(retail_df["retail_gross_profit_krw"].sum()) if "retail_gross_profit_krw" in retail_df.columns else 0

        wholesale_sales = float(wholesale_df["net_sales_amount"].sum()) if "net_sales_amount" in wholesale_df.columns else 0
        wholesale_profit = float(wholesale_df["gross_profit_krw"].sum()) if "gross_profit_krw" in wholesale_df.columns else 0

        total_sales = retail_sales + wholesale_sales
        total_profit = retail_profit + wholesale_profit

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("소매매출", f"{retail_sales:,.0f}")
        m2.metric("도매매출", f"{wholesale_sales:,.0f}")
        m3.metric("통합매출", f"{total_sales:,.0f}")
        m4.metric("통합매출총이익", f"{total_profit:,.0f}")

        src_text = []
        if retail_source:
            src_text.append(f"소매: {retail_source}")
        if wholesale_source:
            src_text.append(f"도매: {wholesale_source}")
        if src_text:
            st.caption("조회 소스 - " + " / ".join(src_text))

        tab1, tab2, tab3 = st.tabs(["월별 통합", "소매 상세", "도매 상세"])

        with tab1:
            month_frames = []

            if not retail_df.empty and "sale_date" in retail_df.columns:
                x = retail_df.copy()
                x["월"] = monthify(x["sale_date"])
                g = x.groupby("월", dropna=False).agg(
                    소매매출=("net_sales_amount", "sum"),
                    소매건수=("order_no", "nunique") if "order_no" in x.columns else ("sale_date", "count"),
                ).reset_index()
                if "retail_gross_profit_krw" in x.columns:
                    gp = x.groupby("월", dropna=False)["retail_gross_profit_krw"].sum().reset_index(name="소매이익")
                    g = g.merge(gp, on="월", how="left")
                month_frames.append(g)

            if not wholesale_df.empty and "sale_date" in wholesale_df.columns:
                x = wholesale_df.copy()
                x["월"] = monthify(x["sale_date"])
                g = x.groupby("월", dropna=False).agg(
                    도매매출=("net_sales_amount", "sum"),
                    도매건수=("id", "count") if "id" in x.columns else ("sale_date", "count"),
                ).reset_index()
                if "gross_profit_krw" in x.columns:
                    gp = x.groupby("월", dropna=False)["gross_profit_krw"].sum().reset_index(name="도매이익")
                    g = g.merge(gp, on="월", how="left")
                month_frames.append(g)

            if month_frames:
                df_month = month_frames[0]
                for add in month_frames[1:]:
                    df_month = df_month.merge(add, on="월", how="outer")

                for c in [c for c in df_month.columns if c != "월"]:
                    df_month[c] = pd.to_numeric(df_month[c], errors="coerce").fillna(0)

                for c in ["소매매출", "도매매출", "소매이익", "도매이익"]:
                    if c not in df_month.columns:
                        df_month[c] = 0

                df_month["통합매출"] = df_month["소매매출"] + df_month["도매매출"]
                df_month["통합이익"] = df_month["소매이익"] + df_month["도매이익"]
                df_month = df_month.sort_values("월", ascending=False)

                st.dataframe(df_month, use_container_width=True, hide_index=True, height=420)

                chart_df = df_month.sort_values("월")
                st.line_chart(chart_df.set_index("월")[["소매매출", "도매매출", "통합매출"]])
            else:
                df_month = pd.DataFrame()
                st.info("월별 통합 데이터가 없습니다.")

        with tab2:
            if retail_df.empty:
                st.info("소매 데이터가 없습니다.")
            else:
                cols = [c for c in [
                    "sale_date", "sale_datetime", "order_no", "order_channel", "payment_status",
                    "product_code", "mst_product_name", "mst_size_name",
                    "qty", "net_sales_amount", "vat_amount", "total_korea_cost_krw", "retail_gross_profit_krw"
                ] if c in retail_df.columns]
                st.dataframe(retail_df[cols], use_container_width=True, height=460, hide_index=True)

        with tab3:
            if wholesale_df.empty:
                st.info("도매 데이터가 없습니다.")
            else:
                cols = [c for c in [
                    "sale_date", "partner_name", "item_type", "product_code", "product_name",
                    "qty", "unit_price", "unit_cost", "net_sales_amount", "gross_profit_krw",
                    "grade_code_applied", "discount_rate_applied", "notes"
                ] if c in wholesale_df.columns]
                st.dataframe(wholesale_df[cols], use_container_width=True, height=460, hide_index=True)

        excel_bytes = to_excel_bytes({
            "월별통합": df_month if 'df_month' in locals() else pd.DataFrame(),
            "소매상세": retail_df,
            "도매상세": wholesale_df,
        })
        st.download_button(
            "매출통합조회 엑셀 다운로드",
            data=excel_bytes,
            file_name="finance_sales_combined.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()


# =========================================================
# 화면 2: 손익분석(월별)
# =========================================================
def render_profit_loss():
    st.markdown("### 손익분석 (월별)")

    conn = get_conn()
    try:
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("시작일", value=None, key="fin_pl_from")
        with c2:
            date_to = st.date_input("종료일", value=None, key="fin_pl_to")

        dfrom = str(date_from) if date_from else None
        dto = str(date_to) if date_to else None

        retail_df, _ = get_retail_data(conn, dfrom, dto)
        wholesale_df, _ = get_wholesale_data(conn, dfrom, dto)
        expense_df = get_expense_data(conn, dfrom, dto)

        monthly_frames = []

        if not retail_df.empty:
            x = retail_df.copy()
            x["월"] = monthify(x["sale_date"])
            g = x.groupby("월", dropna=False).agg(소매매출=("net_sales_amount", "sum")).reset_index()
            if "total_korea_cost_krw" in x.columns:
                c = x.groupby("월", dropna=False)["total_korea_cost_krw"].sum().reset_index(name="소매원가")
                g = g.merge(c, on="월", how="left")
            if "retail_gross_profit_krw" in x.columns:
                p = x.groupby("월", dropna=False)["retail_gross_profit_krw"].sum().reset_index(name="소매이익")
                g = g.merge(p, on="월", how="left")
            monthly_frames.append(g)

        if not wholesale_df.empty:
            x = wholesale_df.copy()
            x["월"] = monthify(x["sale_date"])
            g = x.groupby("월", dropna=False).agg(도매매출=("net_sales_amount", "sum")).reset_index()
            if "total_korea_cost_krw" in x.columns:
                c = x.groupby("월", dropna=False)["total_korea_cost_krw"].sum().reset_index(name="도매원가")
                g = g.merge(c, on="월", how="left")
            if "gross_profit_krw" in x.columns:
                p = x.groupby("월", dropna=False)["gross_profit_krw"].sum().reset_index(name="도매이익")
                g = g.merge(p, on="월", how="left")
            monthly_frames.append(g)

        if not expense_df.empty:
            x = expense_df.copy()
            x["월"] = monthify(x["expense_date"])
            g = x.groupby("월", dropna=False)["amount"].sum().reset_index(name="지출")
            monthly_frames.append(g)

        if not monthly_frames:
            st.info("손익분석에 사용할 데이터가 없습니다.")
            return

        df_pl = monthly_frames[0]
        for add in monthly_frames[1:]:
            df_pl = df_pl.merge(add, on="월", how="outer")

        for c in [c for c in df_pl.columns if c != "월"]:
            df_pl[c] = pd.to_numeric(df_pl[c], errors="coerce").fillna(0)

        for c in ["소매매출", "도매매출", "소매원가", "도매원가", "소매이익", "도매이익", "지출"]:
            if c not in df_pl.columns:
                df_pl[c] = 0

        df_pl["총매출"] = df_pl["소매매출"] + df_pl["도매매출"]
        df_pl["총원가"] = df_pl["소매원가"] + df_pl["도매원가"]
        df_pl["매출총이익"] = df_pl["소매이익"] + df_pl["도매이익"]

        zero_gp_mask = (df_pl["매출총이익"] == 0) & (df_pl["총매출"] != 0)
        df_pl.loc[zero_gp_mask, "매출총이익"] = df_pl.loc[zero_gp_mask, "총매출"] - df_pl.loc[zero_gp_mask, "총원가"]

        df_pl["영업이익"] = df_pl["매출총이익"] - df_pl["지출"]
        df_pl["영업이익률(%)"] = df_pl.apply(
            lambda x: round((x["영업이익"] / x["총매출"] * 100), 1) if x["총매출"] else 0,
            axis=1,
        )

        df_pl = df_pl.sort_values("월", ascending=False)

        total_sales = float(df_pl["총매출"].sum())
        total_gp = float(df_pl["매출총이익"].sum())
        total_exp = float(df_pl["지출"].sum())
        total_op = float(df_pl["영업이익"].sum())

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총매출", f"{total_sales:,.0f}")
        m2.metric("매출총이익", f"{total_gp:,.0f}")
        m3.metric("지출", f"{total_exp:,.0f}")
        m4.metric("영업이익", f"{total_op:,.0f}")

        tab1, tab2 = st.tabs(["월별 손익", "지출 상세"])

        with tab1:
            st.dataframe(df_pl, use_container_width=True, hide_index=True, height=460)
            chart_df = df_pl.sort_values("월")
            st.line_chart(chart_df.set_index("월")[["총매출", "매출총이익", "영업이익"]])

        with tab2:
            if expense_df.empty:
                st.info("해당 기간 지출 데이터가 없습니다.")
            else:
                exp_sum = (
                    expense_df.groupby(["expense_group", "expense_name"], dropna=False)["amount"]
                    .sum()
                    .reset_index()
                    .sort_values("amount", ascending=False)
                )
                st.dataframe(exp_sum, use_container_width=True, hide_index=True, height=420)

        excel_bytes = to_excel_bytes({
            "월별손익": df_pl,
            "지출상세": expense_df,
            "소매기초": retail_df,
            "도매기초": wholesale_df,
        })
        st.download_button(
            "손익분석 엑셀 다운로드",
            data=excel_bytes,
            file_name="finance_profit_loss_monthly.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()


def render():
    tab1, tab2 = st.tabs(["매출통합조회", "손익분석"])
    with tab1:
        render_sales_combined()
    with tab2:
        render_profit_loss()


if __name__ == "__main__":
    render()
