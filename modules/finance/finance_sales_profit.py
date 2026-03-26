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


def fmt_krw(value) -> str:
    try:
        return f"₩{float(value):,.0f}"
    except Exception:
        return "₩0"


def apply_currency_format(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    result = df.copy()
    for col in cols:
        if col in result.columns:
            result[col] = result[col].apply(fmt_krw)
    return result


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [r[1] for r in rows]
    except Exception:
        return []


def get_non_cigar_purchase_price_map(conn: sqlite3.Connection) -> Dict[str, float]:
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


def apply_non_cigar_cost_logic(retail_df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    시가는 기존 원가/이익 유지
    시가 외 상품만 purchase_price 기준으로 원가/이익 재계산
    """
    if retail_df.empty:
        return retail_df

    df = retail_df.copy()

    if "product_code" not in df.columns:
        if "total_korea_cost_krw" not in df.columns:
            df["total_korea_cost_krw"] = 0.0
        if "retail_gross_profit_krw" not in df.columns:
            df["retail_gross_profit_krw"] = 0.0
        return df

    purchase_price_map = get_non_cigar_purchase_price_map(conn)

    if "total_korea_cost_krw" not in df.columns:
        df["total_korea_cost_krw"] = 0.0
    if "retail_gross_profit_krw" not in df.columns:
        df["retail_gross_profit_krw"] = 0.0
    if "qty" not in df.columns:
        df["qty"] = 0.0
    if "net_sales_amount" not in df.columns:
        df["net_sales_amount"] = 0.0

    df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["net_sales_amount"] = pd.to_numeric(df["net_sales_amount"], errors="coerce").fillna(0)
    df["total_korea_cost_krw"] = pd.to_numeric(df["total_korea_cost_krw"], errors="coerce").fillna(0)
    df["retail_gross_profit_krw"] = pd.to_numeric(df["retail_gross_profit_krw"], errors="coerce").fillna(0)

    if purchase_price_map:
        non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())

        df.loc[non_cigar_mask, "_purchase_price"] = (
            df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
        )
        df.loc[non_cigar_mask, "total_korea_cost_krw"] = (
            df.loc[non_cigar_mask, "_purchase_price"] * df.loc[non_cigar_mask, "qty"]
        )
        df.loc[non_cigar_mask, "retail_gross_profit_krw"] = (
            df.loc[non_cigar_mask, "net_sales_amount"] - df.loc[non_cigar_mask, "total_korea_cost_krw"]
        )

        if "_purchase_price" in df.columns:
            df = df.drop(columns=["_purchase_price"])

    return df


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

    if "total_korea_cost_krw" not in df.columns:
        df["total_korea_cost_krw"] = 0.0
    if "retail_gross_profit_krw" not in df.columns:
        df["retail_gross_profit_krw"] = 0.0

    # 시가 외 상품만 매입가 기준으로 원가/이익 재계산
    df = apply_non_cigar_cost_logic(df, conn)

    return df, source


# =========================================================
# Wholesale
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
                vat_amount,
                profit_amount
            FROM v_wholesale_sales
            WHERE 1=1
        """
    else:
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
                vat_amount,
                profit_amount
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

    for c in ["qty", "sales_amount", "profit_amount", "unit_cost", "vat_amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "sales_amount" in df.columns and "net_sales_amount" not in df.columns:
        df["net_sales_amount"] = df["sales_amount"]

    if "profit_amount" in df.columns and "gross_profit_krw" not in df.columns:
        df["gross_profit_krw"] = df["profit_amount"]

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
# 화면 1: 매출분석
# =========================================================
def render_sales_combined():
    st.markdown("### 매출분석")

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

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("소매매출", fmt_krw(retail_sales))
        m2.metric("소매이익", fmt_krw(retail_profit))
        m3.metric("도매매출", fmt_krw(wholesale_sales))
        m4.metric("도매이익", fmt_krw(wholesale_profit))
        m5.metric("통합매출", fmt_krw(total_sales))
        m6.metric("통합매출총이익", fmt_krw(total_profit))

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
                    소매이익=("retail_gross_profit_krw", "sum"),
                ).reset_index()
                month_frames.append(g)

            if not wholesale_df.empty and "sale_date" in wholesale_df.columns:
                x = wholesale_df.copy()
                x["월"] = monthify(x["sale_date"])
                g = x.groupby("월", dropna=False).agg(
                    도매매출=("net_sales_amount", "sum"),
                    도매건수=("id", "count") if "id" in x.columns else ("sale_date", "count"),
                    도매이익=("gross_profit_krw", "sum"),
                ).reset_index()
                month_frames.append(g)

            if not month_frames:
                st.info("해당 기간 데이터가 없습니다.")
            else:
                df_month = month_frames[0]
                for extra in month_frames[1:]:
                    df_month = df_month.merge(extra, on="월", how="outer")

                for c in ["소매매출", "도매매출", "소매이익", "도매이익"]:
                    if c not in df_month.columns:
                        df_month[c] = 0.0
                    df_month[c] = pd.to_numeric(df_month[c], errors="coerce").fillna(0)

                df_month["통합매출"] = df_month["소매매출"] + df_month["도매매출"]
                df_month["통합이익"] = df_month["소매이익"] + df_month["도매이익"]
                df_month = df_month.sort_values("월")

                show = apply_currency_format(
                    df_month,
                    ["소매매출", "도매매출", "통합매출", "소매이익", "도매이익", "통합이익"],
                )
                st.dataframe(show, use_container_width=True, hide_index=True, height=420)

            excel_bytes = to_excel_bytes({
                "월별통합": df_month if "df_month" in locals() else pd.DataFrame(),
                "소매상세": retail_df,
                "도매상세": wholesale_df,
            })
            st.download_button(
                "매출분석 엑셀 다운로드",
                data=excel_bytes,
                file_name="finance_sales_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with tab2:
            if retail_df.empty:
                st.info("해당 기간 소매 데이터가 없습니다.")
            else:
                show = retail_df.copy()

                rename_map = {
                    "sale_date": "판매일자",
                    "sale_datetime": "판매일시",
                    "order_no": "주문번호",
                    "order_channel": "채널",
                    "payment_status": "결제상태",
                    "product_code": "상품코드",
                    "product_code_raw": "원본상품코드",
                    "mst_product_name": "상품명",
                    "mst_size_name": "사이즈",
                    "category": "카테고리",
                    "qty": "수량",
                    "net_sales_amount": "매출액",
                    "vat_amount": "부가세",
                    "total_korea_cost_krw": "원가",
                    "retail_gross_profit_krw": "매출총이익",
                }
                show = show.rename(columns=rename_map)

                show = apply_currency_format(show, ["매출액", "부가세", "원가", "매출총이익"])
                st.dataframe(show, use_container_width=True, hide_index=True, height=480)
                st.caption("※ 시가는 기존 원가/이익을 유지하고, 시가 외 항목만 매입가 기준으로 원가/이익을 재계산합니다.")

        with tab3:
            if wholesale_df.empty:
                st.info("해당 기간 도매 데이터가 없습니다.")
            else:
                show = wholesale_df.copy()
                rename_map = {
                    "sale_date": "판매일자",
                    "partner_name": "거래처",
                    "item_type": "구분",
                    "product_name": "상품명",
                    "product_code": "상품코드",
                    "qty": "수량",
                    "unit_price": "단가",
                    "unit_cost": "원가단가",
                    "net_sales_amount": "매출액",
                    "vat_amount": "부가세",
                    "total_korea_cost_krw": "총원가",
                    "gross_profit_krw": "매출총이익",
                }
                show = show.rename(columns=rename_map)
                show = apply_currency_format(show, ["단가", "원가단가", "매출액", "부가세", "총원가", "매출총이익"])
                st.dataframe(show, use_container_width=True, hide_index=True, height=480)

    finally:
        conn.close()


# =========================================================
# 화면 2: 손익분석
# =========================================================
def render_profit_loss():
    st.markdown("### 손익분석")

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
        for extra in monthly_frames[1:]:
            df_pl = df_pl.merge(extra, on="월", how="outer")

        for c in ["소매매출", "소매원가", "소매이익", "도매매출", "도매원가", "도매이익", "지출"]:
            if c not in df_pl.columns:
                df_pl[c] = 0.0
            df_pl[c] = pd.to_numeric(df_pl[c], errors="coerce").fillna(0)

        df_pl["총매출"] = df_pl["소매매출"] + df_pl["도매매출"]
        df_pl["총원가"] = df_pl["소매원가"] + df_pl["도매원가"]
        df_pl["매출총이익"] = df_pl["소매이익"] + df_pl["도매이익"]
        df_pl["영업이익"] = df_pl["매출총이익"] - df_pl["지출"]
        df_pl = df_pl.sort_values("월")

        total_sales = float(df_pl["총매출"].sum())
        total_gp = float(df_pl["매출총이익"].sum())
        total_exp = float(df_pl["지출"].sum())
        total_op = float(df_pl["영업이익"].sum())
        total_retail_profit = float(df_pl["소매이익"].sum()) if "소매이익" in df_pl.columns else 0
        total_wholesale_profit = float(df_pl["도매이익"].sum()) if "도매이익" in df_pl.columns else 0

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("총매출", fmt_krw(total_sales))
        m2.metric("소매이익", fmt_krw(total_retail_profit))
        m3.metric("도매이익", fmt_krw(total_wholesale_profit))
        m4.metric("매출총이익", fmt_krw(total_gp))
        m5.metric("지출", fmt_krw(total_exp))
        m6.metric("영업이익", fmt_krw(total_op))

        tab1, tab2 = st.tabs(["월별 손익", "지출 상세"])

        with tab1:
            show = apply_currency_format(
                df_pl,
                ["소매매출", "소매원가", "소매이익", "도매매출", "도매원가", "도매이익", "총매출", "총원가", "매출총이익", "지출", "영업이익"],
            )
            st.dataframe(show, use_container_width=True, hide_index=True, height=420)

        with tab2:
            if expense_df.empty:
                st.info("해당 기간 지출 데이터가 없습니다.")
            else:
                exp_sum = (
                    expense_df.groupby(["expense_group", "expense_name"], dropna=False)["amount"]
                    .sum()
                    .reset_index()
                    .sort_values("amount", ascending=False)
                    .rename(
                        columns={
                            "expense_group": "지출그룹",
                            "expense_name": "지출항목",
                            "amount": "금액",
                        }
                    )
                )
                exp_sum = apply_currency_format(exp_sum, ["금액"])
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
    tab1, tab2 = st.tabs(["매출분석", "손익분석"])
    with tab1:
        render_sales_combined()
    with tab2:
        render_profit_loss()


if __name__ == "__main__":
    render()