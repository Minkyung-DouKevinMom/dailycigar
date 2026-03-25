import os
import sqlite3
from typing import List, Optional, Tuple
import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


# =========================
# 공통
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fmt_krw(x):
    try:
        return f"₩{float(x):,.0f}"
    except:
        return "₩0"


def apply_currency(df, cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_krw)
    return df


# =========================
# 데이터 로딩
# =========================
def get_retail(conn):
    try:
        df = pd.read_sql_query("""
            SELECT 
                product_code,
                mst_product_name AS product_name,
                mst_size_name AS size_name,
                category,
                qty,
                net_sales_amount,
                total_korea_cost_krw,
                retail_gross_profit_krw
            FROM v_retail_sales_enriched
        """, conn)
    except:
        return pd.DataFrame()

    return df


def get_wholesale(conn):
    try:
        df = pd.read_sql_query("""
            SELECT 
                product_code,
                product_name,
                qty,
                sales_amount AS net_sales_amount,
                (qty * unit_cost) AS total_korea_cost_krw,
                profit_amount AS retail_gross_profit_krw
            FROM v_wholesale_sales
        """, conn)
    except:
        return pd.DataFrame()

    return df


# =========================
# 메인
# =========================
def render():
    st.markdown("## 제품별 수익성")

    conn = get_conn()

    try:
        retail = get_retail(conn)
        wholesale = get_wholesale(conn)

        frames = []
        if not retail.empty:
            frames.append(retail)
        if not wholesale.empty:
            frames.append(wholesale)

        if not frames:
            st.warning("데이터가 없습니다.")
            return

        df = pd.concat(frames, ignore_index=True)

        # 숫자 처리
        for col in ["qty", "net_sales_amount", "total_korea_cost_krw", "retail_gross_profit_krw"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 집계
        grouped = df.groupby(
            ["product_code", "product_name"],
            dropna=False
        ).agg(
            판매수량=("qty", "sum"),
            매출액=("net_sales_amount", "sum"),
            원가=("total_korea_cost_krw", "sum"),
            이익=("retail_gross_profit_krw", "sum")
        ).reset_index()

        grouped["마진율(%)"] = grouped.apply(
            lambda x: round((x["이익"] / x["매출액"] * 100), 1) if x["매출액"] else 0,
            axis=1
        )

        # KPI
        total_sales = grouped["매출액"].sum()
        total_profit = grouped["이익"].sum()
        total_qty = grouped["판매수량"].sum()
        total_products = len(grouped)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("총매출", fmt_krw(total_sales))
        c2.metric("총이익", fmt_krw(total_profit))
        c3.metric("총판매수량", f"{int(total_qty):,}")
        c4.metric("상품수", f"{total_products:,}")

        st.divider()

        # 검색
        keyword = st.text_input("상품 검색")

        if keyword:
            grouped = grouped[
                grouped["product_name"].str.contains(keyword, case=False, na=False)
            ]

        # 정렬
        grouped = grouped.sort_values("매출액", ascending=False)

        # 표
        show_df = grouped.rename(columns={
            "product_code": "상품코드",
            "product_name": "상품명"
        })

        show_df = apply_currency(show_df, ["매출액", "원가", "이익"])

        st.dataframe(show_df, use_container_width=True, height=500, hide_index=True)

        # ======================
        # TOP 차트
        # ======================
        st.markdown("### 상위 제품 분석")

        basis = st.radio(
            "기준 선택",
            ["매출 기준", "이익 기준"],
            horizontal=True
        )

        metric = "매출액" if basis == "매출 기준" else "이익"

        top_df = grouped.sort_values(metric, ascending=False).head(20)

        chart_df = top_df.set_index("product_code")[[metric]]

        st.bar_chart(chart_df)

        # ======================
        # 다운로드
        # ======================
        excel = grouped.copy()
        excel_bytes = excel.to_csv(index=False).encode("utf-8-sig")

        st.download_button(
            "엑셀 다운로드",
            data=excel_bytes,
            file_name="product_profitability.csv",
            mime="text/csv"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    render()