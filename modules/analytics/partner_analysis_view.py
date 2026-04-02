import os
import sqlite3

import pandas as pd
import streamlit as st
import altair as alt

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fmt_krw(x):
    try:
        return f"₩{float(x):,.0f}"
    except Exception:
        return "₩0"


def _find_date_column(df: pd.DataFrame) -> str | None:
    """
    v_wholesale_sales 안의 날짜 컬럼명을 자동 탐지
    """
    candidates = [
        "sales_date",
        "sale_date",
        "order_date",
        "wholesale_date",
        "tx_date",
        "created_at",
        "created_date",
        "reg_date",
        "date",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _calc_partner_cycle_stats(partner_df: pd.DataFrame) -> dict:
    buy_dates = (
        pd.Series(partner_df["date"].dropna().dt.normalize().unique())
        .sort_values()
        .reset_index(drop=True)
    )

    buy_count = len(buy_dates)
    latest_buy_date = buy_dates.iloc[-1].date() if buy_count >= 1 else None
    prev_buy_date = buy_dates.iloc[-2].date() if buy_count >= 2 else None

    if buy_count >= 2:
        gaps = buy_dates.diff().dropna().dt.days
        recent_gap = int((buy_dates.iloc[-1] - buy_dates.iloc[-2]).days)
        avg_gap = round(float(gaps.mean()), 1) if len(gaps) else None
    else:
        recent_gap = None
        avg_gap = None

    latest_buy_amount = (
        partner_df.loc[partner_df["date"].dt.normalize() == buy_dates.iloc[-1], "sales"].sum()
        if buy_count >= 1
        else 0
    )

    return {
        "최근 구매일": latest_buy_date.strftime("%Y-%m-%d") if latest_buy_date else "-",
        "이전 구매일": prev_buy_date.strftime("%Y-%m-%d") if prev_buy_date else "-",
        "최근 구매간격(일)": recent_gap if recent_gap is not None else "-",
        "평균 구매간격(일)": avg_gap if avg_gap is not None else "-",
        "구매일수": buy_count,
        "최근 구매금액": latest_buy_amount,
    }


def render():
    st.subheader("거래처 분석")

    conn = get_conn()

    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM v_wholesale_sales
            """,
            conn,
        )

        if df.empty:
            st.warning("도매 데이터가 없습니다.")
            return

        if "partner_name" not in df.columns:
            st.error("v_wholesale_sales에 partner_name 컬럼이 없습니다.")
            return

        if "sales_amount" not in df.columns:
            st.error("v_wholesale_sales에 sales_amount 컬럼이 없습니다.")
            return

        if "profit_amount" not in df.columns:
            df["profit_amount"] = 0

        df["sales"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
        df["profit"] = pd.to_numeric(df["profit_amount"], errors="coerce").fillna(0)
        df["partner_name"] = df["partner_name"].fillna("(거래처 없음)")

        grouped = (
            df.groupby("partner_name", dropna=False)
            .agg(
                매출=("sales", "sum"),
                이익=("profit", "sum"),
            )
            .reset_index()
        )

        grouped["마진율(%)"] = grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1,
        )
        grouped = grouped.sort_values("매출", ascending=False)

        total_sales = grouped["매출"].sum()
        total_profit = grouped["이익"].sum()
        partner_count = len(grouped)

        c1, c2, c3 = st.columns(3)
        c1.metric("총매출", fmt_krw(total_sales))
        c2.metric("총이익", fmt_krw(total_profit))
        c3.metric("거래처 수", f"{partner_count:,}")

        st.divider()

        show_df = grouped.rename(columns={"partner_name": "거래처"}).copy()
        for col in ["매출", "이익"]:
            show_df[col] = show_df[col].apply(fmt_krw)

        st.dataframe(show_df, use_container_width=True, height=450, hide_index=True)

        st.markdown("### TOP 거래처")

        basis = st.radio(
            "기준 선택",
            ["매출 기준", "이익 기준"],
            horizontal=True,
        )

        metric = "매출" if basis == "매출 기준" else "이익"
        top_df = grouped.sort_values(metric, ascending=False).head(10)
        chart_df = top_df.set_index("partner_name")[[metric]]

        st.bar_chart(chart_df)

        st.markdown("### 거래처별 구매 패턴 상세")

        date_col = _find_date_column(df)
        if not date_col:
            st.info("일자 컬럼을 찾지 못했습니다. (예: sales_date, sale_date, created_at)")
            return

        line_df = df.copy()
        line_df["date"] = pd.to_datetime(line_df[date_col], errors="coerce")
        line_df = line_df.dropna(subset=["date"]).copy()

        if line_df.empty:
            st.info("일자 데이터가 없어 상세 그래프를 표시할 수 없습니다.")
            return

        partner_options = grouped["partner_name"].tolist()
        default_partner = partner_options[0] if partner_options else None

        selected_partner = st.selectbox(
            "거래처 선택",
            options=partner_options,
            index=0 if default_partner else None,
        )

        if not selected_partner:
            st.info("거래처를 선택해주세요.")
            return

        partner_df = line_df[line_df["partner_name"] == selected_partner].copy()
        if partner_df.empty:
            st.info("선택한 거래처의 데이터가 없습니다.")
            return

        partner_df["date"] = partner_df["date"].dt.normalize()

        daily_sales