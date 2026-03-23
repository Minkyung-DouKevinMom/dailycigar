import os
import sqlite3
from io import BytesIO
from typing import Dict, Optional, Tuple

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


def to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    output.seek(0)
    return output.getvalue()


def monthify(series):
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m")


def get_retail_product_profit(conn, date_from: Optional[str], date_to: Optional[str]) -> pd.DataFrame:
    source = "v_retail_sales_enriched" if view_exists(conn, "v_retail_sales_enriched") else None
    if not source:
        return pd.DataFrame()

    sql = """
        SELECT
            sale_date,
            COALESCE(product_code, product_code_raw) AS product_code,
            COALESCE(mst_product_name, product_code_raw) AS product_name,
            COALESCE(mst_size_name, '') AS size_name,
            category,
            qty,
            net_sales_amount,
            total_korea_cost_krw,
            retail_gross_profit_krw
        FROM v_retail_sales_enriched
        WHERE 1=1
    """
    params = []
    if date_from:
        sql += " AND sale_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND sale_date <= ?"
        params.append(date_to)

    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df

    for c in ["qty", "net_sales_amount", "total_korea_cost_krw", "retail_gross_profit_krw"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["channel_type"] = "소매"
    return df


def get_wholesale_product_profit(conn, date_from: Optional[str], date_to: Optional[str]) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        sql = """
            SELECT
                sale_date,
                product_code,
                product_name,
                item_type,
                qty,
                sales_amount AS net_sales_amount,
                (qty * unit_cost) AS total_korea_cost_krw,
                profit_amount AS gross_profit_krw,
                partner_name
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

        df = pd.read_sql_query(sql, conn, params=params)
        if df.empty:
            return df

        for c in ["qty", "net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        df["size_name"] = ""
        df["category"] = df["item_type"]
        df["retail_gross_profit_krw"] = df["gross_profit_krw"]
        df["channel_type"] = "도매"
        return df[[
            "sale_date", "product_code", "product_name", "size_name", "category",
            "qty", "net_sales_amount", "total_korea_cost_krw", "retail_gross_profit_krw",
            "channel_type"
        ]]

    return pd.DataFrame()


def build_product_profit_df(conn, date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    retail_df = get_retail_product_profit(conn, date_from, date_to)
    wholesale_df = get_wholesale_product_profit(conn, date_from, date_to)

    frames = []
    if not retail_df.empty:
        frames.append(retail_df)
    if not wholesale_df.empty:
        frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    detail_df = pd.concat(frames, ignore_index=True)
    detail_df["product_code"] = detail_df["product_code"].fillna("").astype(str).str.strip()
    detail_df["product_name"] = detail_df["product_name"].fillna("").astype(str).str.strip()
    detail_df["size_name"] = detail_df["size_name"].fillna("").astype(str).str.strip()
    detail_df["category"] = detail_df["category"].fillna("").astype(str).str.strip()

    grouped = (
        detail_df.groupby(
            ["product_code", "product_name", "size_name", "category", "channel_type"],
            dropna=False
        )[["qty", "net_sales_amount", "total_korea_cost_krw", "retail_gross_profit_krw"]]
        .sum()
        .reset_index()
    )

    grouped["마진율(%)"] = grouped.apply(
        lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
        if x["net_sales_amount"] else 0,
        axis=1,
    )

    total_grouped = (
        detail_df.groupby(
            ["product_code", "product_name", "size_name", "category"],
            dropna=False
        )[["qty", "net_sales_amount", "total_korea_cost_krw", "retail_gross_profit_krw"]]
        .sum()
        .reset_index()
        .sort_values(["net_sales_amount", "qty"], ascending=[False, False])
    )

    total_grouped["마진율(%)"] = total_grouped.apply(
        lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
        if x["net_sales_amount"] else 0,
        axis=1,
    )

    return total_grouped, grouped.sort_values(["channel_type", "net_sales_amount"], ascending=[True, False])


def render():
    st.markdown("### 제품별 수익성")

    conn = get_conn()
    try:
        c1, c2, c3 = st.columns(3)
        with c1:
            date_from = st.date_input("시작일", value=None, key="pp_from")
        with c2:
            date_to = st.date_input("종료일", value=None, key="pp_to")
        with c3:
            keyword = st.text_input("검색", placeholder="상품코드 / 상품명", key="pp_keyword")

        total_df, by_channel_df = build_product_profit_df(
            conn,
            str(date_from) if date_from else None,
            str(date_to) if date_to else None,
        )

        if total_df.empty:
            st.info("제품별 수익성 데이터가 없습니다.")
            return

        if keyword.strip():
            kw = keyword.strip().lower()
            total_df = total_df[
                total_df["product_code"].str.lower().str.contains(kw, na=False)
                | total_df["product_name"].str.lower().str.contains(kw, na=False)
            ].copy()
            by_channel_df = by_channel_df[
                by_channel_df["product_code"].str.lower().str.contains(kw, na=False)
                | by_channel_df["product_name"].str.lower().str.contains(kw, na=False)
            ].copy()

        if total_df.empty:
            st.warning("검색 조건에 맞는 제품이 없습니다.")
            return

        total_sales = float(total_df["net_sales_amount"].sum())
        total_cost = float(total_df["total_korea_cost_krw"].sum())
        total_profit = float(total_df["retail_gross_profit_krw"].sum())
        total_qty = float(total_df["qty"].sum())
        total_margin = round((total_profit / total_sales * 100), 1) if total_sales else 0

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("제품수", f"{len(total_df):,}")
        m2.metric("판매수량", f"{total_qty:,.0f}")
        m3.metric("총매출", f"{total_sales:,.0f}")
        m4.metric("총이익", f"{total_profit:,.0f}")
        m5.metric("평균마진율", f"{total_margin:.1f}%")

        tab1, tab2, tab3 = st.tabs(["제품별 통합", "채널별 상세", "월별 추이"])

        with tab1:
            st.dataframe(
                total_df,
                use_container_width=True,
                hide_index=True,
                height=520,
            )

            chart_df = total_df.head(20).copy()
            chart_df["라벨"] = chart_df.apply(
                lambda x: x["product_name"] if x["product_name"] else x["product_code"],
                axis=1,
            )
            if not chart_df.empty:
                st.markdown("#### 상위 20개 제품 매출")
                st.bar_chart(chart_df.set_index("라벨")[["net_sales_amount"]])

        with tab2:
            st.dataframe(
                by_channel_df,
                use_container_width=True,
                hide_index=True,
                height=520,
            )

        with tab3:
            retail_df = get_retail_product_profit(
                conn,
                str(date_from) if date_from else None,
                str(date_to) if date_to else None,
            )
            wholesale_df = get_wholesale_product_profit(
                conn,
                str(date_from) if date_from else None,
                str(date_to) if date_to else None,
            )

            trend_frames = []
            for df, name in [(retail_df, "소매"), (wholesale_df, "도매")]:
                if not df.empty:
                    x = df.copy()
                    x["월"] = monthify(x["sale_date"])
                    g = x.groupby("월", dropna=False)[["net_sales_amount", "retail_gross_profit_krw"]].sum().reset_index()
                    g = g.rename(columns={
                        "net_sales_amount": f"{name}매출",
                        "retail_gross_profit_krw": f"{name}이익",
                    })
                    trend_frames.append(g)

            if trend_frames:
                df_trend = trend_frames[0]
                for add in trend_frames[1:]:
                    df_trend = df_trend.merge(add, on="월", how="outer")

                for c in [c for c in df_trend.columns if c != "월"]:
                    df_trend[c] = pd.to_numeric(df_trend[c], errors="coerce").fillna(0)

                if "소매매출" not in df_trend.columns:
                    df_trend["소매매출"] = 0
                if "도매매출" not in df_trend.columns:
                    df_trend["도매매출"] = 0
                if "소매이익" not in df_trend.columns:
                    df_trend["소매이익"] = 0
                if "도매이익" not in df_trend.columns:
                    df_trend["도매이익"] = 0

                df_trend["통합매출"] = df_trend["소매매출"] + df_trend["도매매출"]
                df_trend["통합이익"] = df_trend["소매이익"] + df_trend["도매이익"]
                df_trend = df_trend.sort_values("월", ascending=False)

                st.dataframe(df_trend, use_container_width=True, hide_index=True, height=320)

                chart_df = df_trend.sort_values("월")
                st.line_chart(chart_df.set_index("월")[["소매매출", "도매매출", "통합매출"]])
            else:
                df_trend = pd.DataFrame()
                st.info("월별 추이 데이터가 없습니다.")

        excel_bytes = to_excel_bytes({
            "제품별통합": total_df,
            "채널별상세": by_channel_df,
            "월별추이": df_trend if 'df_trend' in locals() else pd.DataFrame(),
        })
        st.download_button(
            "제품별 수익성 엑셀 다운로드",
            data=excel_bytes,
            file_name="finance_product_profitability.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()


if __name__ == "__main__":
    render()
