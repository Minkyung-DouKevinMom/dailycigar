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
    st.subheader("거래처 분석")

    conn = get_conn()

    try:
        # -----------------------
        # 도매 데이터
        # -----------------------
        df = pd.read_sql_query("""
            SELECT
                partner_name,
                COALESCE(sales_amount, 0) AS sales,
                COALESCE(profit_amount, 0) AS profit
            FROM v_wholesale_sales
        """, conn)

        if df.empty:
            st.warning("도매 데이터가 없습니다.")
            return

        df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
        df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0)

        grouped = df.groupby("partner_name", dropna=False).agg(
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
        partner_count = len(grouped)

        c1, c2, c3 = st.columns(3)
        c1.metric("총매출", fmt_krw(total_sales))
        c2.metric("총이익", fmt_krw(total_profit))
        c3.metric("거래처 수", f"{partner_count:,}")

        st.divider()

        # -----------------------
        # 테이블
        # -----------------------
        show_df = grouped.rename(columns={
            "partner_name": "거래처"
        }).copy()

        for col in ["매출", "이익"]:
            show_df[col] = show_df[col].apply(fmt_krw)

        st.dataframe(show_df, use_container_width=True, height=450, hide_index=True)

        # -----------------------
        # TOP 차트
        # -----------------------
        st.markdown("### TOP 거래처")

        basis = st.radio(
            "기준 선택",
            ["매출 기준", "이익 기준"],
            horizontal=True
        )

        metric = "매출" if basis == "매출 기준" else "이익"

        top_df = grouped.sort_values(metric, ascending=False).head(10)
        chart_df = top_df.set_index("partner_name")[[metric]]

        st.bar_chart(chart_df)

    finally:
        conn.close()