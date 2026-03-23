import os
import sqlite3
from io import BytesIO

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name = ?",
        (view_name,),
    )
    return cur.fetchone() is not None


def load_filter_values(conn: sqlite3.Connection):
    channels = []
    statuses = []
    categories = []
    codes = []

    if view_exists(conn, "v_retail_sales_enriched"):
        base = "v_retail_sales_enriched"
    elif table_exists(conn, "retail_sales"):
        base = "retail_sales"
    else:
        return channels, statuses, categories, codes

    try:
        channels = pd.read_sql_query(
            f"SELECT DISTINCT order_channel FROM {base} WHERE COALESCE(order_channel,'') <> '' ORDER BY order_channel",
            conn,
        )["order_channel"].tolist()
    except Exception:
        pass

    try:
        statuses = pd.read_sql_query(
            f"SELECT DISTINCT payment_status FROM {base} WHERE COALESCE(payment_status,'') <> '' ORDER BY payment_status",
            conn,
        )["payment_status"].tolist()
    except Exception:
        pass

    try:
        categories = pd.read_sql_query(
            f"SELECT DISTINCT category FROM {base} WHERE COALESCE(category,'') <> '' ORDER BY category",
            conn,
        )["category"].tolist()
    except Exception:
        pass

    try:
        codes = pd.read_sql_query(
            f"SELECT DISTINCT product_code FROM {base} WHERE COALESCE(product_code,'') <> '' ORDER BY product_code",
            conn,
        )["product_code"].tolist()
    except Exception:
        pass

    return channels, statuses, categories, codes


def build_query(use_view: bool, filters: dict):
    base = "v_retail_sales_enriched" if use_view else "retail_sales"
    where = []
    params = []

    if filters.get("date_from"):
        where.append("sale_date >= ?")
        params.append(filters["date_from"])

    if filters.get("date_to"):
        where.append("sale_date <= ?")
        params.append(filters["date_to"])

    if filters.get("channels"):
        placeholders = ",".join(["?"] * len(filters["channels"]))
        where.append(f"order_channel IN ({placeholders})")
        params.extend(filters["channels"])

    if filters.get("statuses"):
        placeholders = ",".join(["?"] * len(filters["statuses"]))
        where.append(f"payment_status IN ({placeholders})")
        params.extend(filters["statuses"])

    if filters.get("categories"):
        placeholders = ",".join(["?"] * len(filters["categories"]))
        where.append(f"category IN ({placeholders})")
        params.extend(filters["categories"])

    if filters.get("codes"):
        placeholders = ",".join(["?"] * len(filters["codes"]))
        where.append(f"product_code IN ({placeholders})")
        params.extend(filters["codes"])

    if filters.get("keyword"):
        kw = f"%{filters['keyword'].strip()}%"
        keyword_fields = ["order_no", "product_code_raw", "product_code"]
        if use_view:
            keyword_fields.extend(["mst_product_name", "mst_size_name"])
        where.append("(" + " OR ".join([f"COALESCE({f}, '') LIKE ?" for f in keyword_fields]) + ")")
        params.extend([kw] * len(keyword_fields))

    sql = f"SELECT * FROM {base}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY sale_datetime DESC, id DESC"
    return sql, params


def calc_kpis(df: pd.DataFrame):
    result = {
        "건수": len(df),
        "주문수": df["order_no"].nunique() if "order_no" in df.columns else 0,
        "판매수량": float(df["qty"].fillna(0).sum()) if "qty" in df.columns else 0,
        "실매출": float(df["net_sales_amount"].fillna(0).sum()) if "net_sales_amount" in df.columns else 0,
        "부가세": float(df["vat_amount"].fillna(0).sum()) if "vat_amount" in df.columns else 0,
    }

    if "total_korea_cost_krw" in df.columns:
        result["원가합계"] = float(df["total_korea_cost_krw"].fillna(0).sum())

    if "total_supply_price_krw" in df.columns:
        result["공급가합계"] = float(df["total_supply_price_krw"].fillna(0).sum())

    if "retail_gross_profit_krw" in df.columns:
        gross_profit = float(df["retail_gross_profit_krw"].fillna(0).sum())
        result["매출총이익"] = gross_profit
        sales = result["실매출"]
        result["마진율"] = (gross_profit / sales * 100) if sales else 0

    return result


def make_excel_download(df_detail: pd.DataFrame, df_product: pd.DataFrame, df_daily: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_detail.to_excel(writer, index=False, sheet_name="상세내역")
        df_product.to_excel(writer, index=False, sheet_name="상품별집계")
        df_daily.to_excel(writer, index=False, sheet_name="일자별집계")
    output.seek(0)
    return output.getvalue()


def render():
    st.subheader("소매 매출 조회")

    conn = get_conn()
    try:
        has_view = view_exists(conn, "v_retail_sales_enriched")
        has_table = table_exists(conn, "retail_sales")

        if not has_view and not has_table:
            st.error("retail_sales 또는 v_retail_sales_enriched 가 없습니다.")
            return

        channels, statuses, categories, codes = load_filter_values(conn)

        st.caption("분석 뷰가 있으면 원가/공급가/마진까지 함께 조회합니다.")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_from = st.date_input("시작일", value=None)
        with col2:
            date_to = st.date_input("종료일", value=None)
        with col3:
            selected_channels = st.multiselect("주문채널", channels)
        with col4:
            selected_statuses = st.multiselect("결제상태", statuses, default=["완료"] if "완료" in statuses else [])

        col5, col6, col7 = st.columns(3)
        with col5:
            selected_categories = st.multiselect("카테고리", categories)
        with col6:
            selected_codes = st.multiselect("상품코드", codes)
        with col7:
            keyword = st.text_input("검색어", placeholder="주문번호, 상품코드, 상품명")

        filters = {
            "date_from": str(date_from) if date_from else None,
            "date_to": str(date_to) if date_to else None,
            "channels": selected_channels,
            "statuses": selected_statuses,
            "categories": selected_categories,
            "codes": selected_codes,
            "keyword": keyword,
        }

        sql, params = build_query(use_view=has_view, filters=filters)
        df = pd.read_sql_query(sql, conn, params=params)

        if df.empty:
            st.warning("조회된 데이터가 없습니다.")
            return

        kpis = calc_kpis(df)
        metric_cols = st.columns(4)

        labels = list(kpis.keys())
        values = list(kpis.values())
        for i, (label, value) in enumerate(zip(labels, values)):
            c = metric_cols[i % 4]
            if label == "마진율":
                c.metric(label, f"{value:.1f}%")
            elif isinstance(value, float):
                c.metric(label, f"{value:,.0f}")
            else:
                c.metric(label, f"{value:,}")

        tab1, tab2, tab3 = st.tabs(["상세내역", "상품별 집계", "일자별 집계"])

        with tab1:
            display_cols = [
                c for c in [
                    "sale_date",
                    "sale_datetime",
                    "order_channel",
                    "payment_status",
                    "order_no",
                    "product_code",
                    "product_code_raw",
                    "mst_product_name",
                    "mst_size_name",
                    "category",
                    "qty",
                    "unit_price",
                    "option_price",
                    "net_sales_amount",
                    "vat_amount",
                    "korea_cost_krw",
                    "supply_price_krw",
                    "retail_price_krw",
                    "retail_gross_profit_krw",
                    "source_file_name",
                    "source_row_no",
                ] if c in df.columns
            ]
            st.dataframe(df[display_cols], use_container_width=True, height=520)

        if "mst_product_name" in df.columns:
            group_name_col = "mst_product_name"
        elif "product_code" in df.columns:
            group_name_col = "product_code"
        else:
            group_name_col = "product_code_raw"

        with tab2:
            agg_map = {
                "qty": "sum",
                "net_sales_amount": "sum",
                "vat_amount": "sum",
            }
            for extra in ["total_korea_cost_krw", "total_supply_price_krw", "retail_gross_profit_krw"]:
                if extra in df.columns:
                    agg_map[extra] = "sum"

            group_cols = [c for c in ["product_code", group_name_col, "mst_size_name"] if c in df.columns]
            df_product = (
                df.groupby(group_cols, dropna=False)
                .agg(agg_map)
                .reset_index()
                .sort_values("net_sales_amount", ascending=False)
            )

            if "retail_gross_profit_krw" in df_product.columns:
                df_product["마진율(%)"] = df_product.apply(
                    lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
                    if x["net_sales_amount"] else 0,
                    axis=1
                )

            st.dataframe(df_product, use_container_width=True, height=520)

        with tab3:
            agg_map_daily = {
                "qty": "sum",
                "net_sales_amount": "sum",
                "vat_amount": "sum",
                "order_no": pd.Series.nunique,
            }
            for extra in ["total_korea_cost_krw", "retail_gross_profit_krw"]:
                if extra in df.columns:
                    agg_map_daily[extra] = "sum"

            df_daily = (
                df.groupby("sale_date", dropna=False)
                .agg(agg_map_daily)
                .reset_index()
                .rename(columns={"order_no": "주문수"})
                .sort_values("sale_date", ascending=False)
            )

            if "retail_gross_profit_krw" in df_daily.columns:
                df_daily["마진율(%)"] = df_daily.apply(
                    lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
                    if x["net_sales_amount"] else 0,
                    axis=1
                )

            st.dataframe(df_daily, use_container_width=True, height=520)

        excel_bytes = make_excel_download(
            df_detail=df,
            df_product=df_product if 'df_product' in locals() else pd.DataFrame(),
            df_daily=df_daily if 'df_daily' in locals() else pd.DataFrame(),
        )
        st.download_button(
            "조회결과 엑셀 다운로드",
            data=excel_bytes,
            file_name="retail_sales_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()
