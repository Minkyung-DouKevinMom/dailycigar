import os
import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fmt_krw(x):
    try:
        return f"₩{float(x):,.0f}"
    except:
        return "₩0"


def render():
    st.subheader("브랜드 분석")

    conn = get_conn()

    try:
        # -----------------------
        # 소매 + 도매 데이터
        # -----------------------
        retail = pd.read_sql_query("""
            SELECT
                category AS brand,
                net_sales_amount AS sales,
                retail_gross_profit_krw AS profit
            FROM v_retail_sales_enriched
        """, conn)

        wholesale = pd.read_sql_query("""
            SELECT
                product_name AS brand,
                sales_amount AS sales,
                profit_amount AS profit
            FROM v_wholesale_sales
        """, conn)

        df = pd.concat([retail, wholesale], ignore_index=True)

        if df.empty:
            st.warning("데이터가 없습니다.")
            return

        df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
        df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0)

        grouped = df.groupby("brand", dropna=False).agg(
            매출=("sales", "sum"),
            이익=("profit", "sum")
        ).reset_index()

        grouped["마진율(%)"] = grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1
        )

        grouped = grouped.sort_values("매출", ascending=False)

        # -----------------------
        # KPI
        # -----------------------
        total_sales = grouped["매출"].sum()
        total_profit = grouped["이익"].sum()
        brand_count = len(grouped)

        c1, c2, c3 = st.columns(3)
        c1.metric("총매출", fmt_krw(total_sales))
        c2.metric("총이익", fmt_krw(total_profit))
        c3.metric("브랜드 수", f"{brand_count:,}")

        st.divider()

        # -----------------------
        # 테이블
        # -----------------------
        show_df = grouped.rename(columns={"brand": "브랜드"}).copy()

        for col in ["매출", "이익"]:
            show_df[col] = show_df[col].apply(fmt_krw)

        st.dataframe(show_df, use_container_width=True, height=450, hide_index=True)

        # -----------------------
        # 차트
        # -----------------------
        st.markdown("### 브랜드 매출 비중")

        pie_df = grouped.set_index("brand")["매출"]
        st.bar_chart(pie_df)  # pie 대신 bar (streamlit 기본)

        st.markdown("### 브랜드 마진율")

        margin_df = grouped.set_index("brand")["마진율(%)"]
        st.bar_chart(margin_df)

    finally:
        conn.close()