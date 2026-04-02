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
    except Exception:
        return "₩0"


def _find_date_column(df: pd.DataFrame):
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


def _build_partner_cycle_summary(line_df: pd.DataFrame) -> pd.DataFrame:
    if line_df.empty:
        return pd.DataFrame()

    purchase_days = (
        line_df.groupby(["partner_name", "date"], dropna=False)["sales"]
        .sum()
        .reset_index()
    )
    purchase_days = purchase_days[purchase_days["sales"] > 0].copy()

    if purchase_days.empty:
        return pd.DataFrame()

    rows = []

    for partner_name, g in purchase_days.groupby("partner_name", dropna=False):
        g = g.sort_values("date").copy()
        dates = list(pd.to_datetime(g["date"]))
        sales_values = list(pd.to_numeric(g["sales"], errors="coerce").fillna(0))

        recent_purchase_date = dates[-1] if dates else pd.NaT
        prev_purchase_date = dates[-2] if len(dates) >= 2 else pd.NaT
        recent_purchase_amount = sales_values[-1] if sales_values else 0
        purchase_count = len(dates)
        total_sales = sum(sales_values)

        interval_days = []
        for i in range(1, len(dates)):
            interval_days.append((dates[i] - dates[i - 1]).days)

        recent_interval = interval_days[-1] if interval_days else None
        avg_interval = round(sum(interval_days) / len(interval_days), 1) if interval_days else None

        rows.append(
            {
                "거래처": partner_name,
                "최근 구매일": recent_purchase_date.date() if pd.notna(recent_purchase_date) else "",
                "이전 구매일": prev_purchase_date.date() if pd.notna(prev_purchase_date) else "",
                "최근 구매간격(일)": recent_interval if recent_interval is not None else "",
                "평균 구매간격(일)": avg_interval if avg_interval is not None else "",
                "구매일수": purchase_count,
                "최근 구매금액": recent_purchase_amount,
                "누적 매출": total_sales,
            }
        )

    summary_df = pd.DataFrame(rows)
    if summary_df.empty:
        return summary_df

    summary_df = summary_df.sort_values(
        ["누적 매출", "거래처"], ascending=[False, True]
    ).reset_index(drop=True)
    return summary_df


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

        st.dataframe(show_df, use_container_width=True, height=420, hide_index=True)

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

        st.markdown("### 일자별 거래처 구매금액 비교")

        date_col = _find_date_column(df)
        if not date_col:
            st.info("일자 컬럼을 찾지 못했습니다. 예: sales_date, sale_date, created_at")
            return

        line_df = df.copy()
        line_df["date"] = pd.to_datetime(line_df[date_col], errors="coerce")
        line_df = line_df.dropna(subset=["date"]).copy()

        if line_df.empty:
            st.info("일자 데이터가 없어 비교 그래프를 표시할 수 없습니다.")
            return

        line_df["date"] = line_df["date"].dt.normalize()

        daily_df = (
            line_df.groupby(["date", "partner_name"], dropna=False)["sales"]
            .sum()
            .reset_index()
        )

        if daily_df.empty:
            st.info("일자별 집계 데이터가 없습니다.")
            return

        min_date = pd.to_datetime(daily_df["date"]).min()
        max_date = pd.to_datetime(daily_df["date"]).max()
        all_dates = pd.date_range(start=min_date, end=max_date, freq="D")

        pivot_df = (
            daily_df.pivot(index="date", columns="partner_name", values="sales")
            .sort_index()
        )

        pivot_df.index = pd.to_datetime(pivot_df.index)
        pivot_df = pivot_df.reindex(all_dates).fillna(0)
        pivot_df.index.name = "date"

        st.caption("거래가 없는 날짜는 0으로 표시하여 거래처별 구매 주기를 비교합니다.")
        st.line_chart(pivot_df, use_container_width=True)

        st.markdown("### 거래처별 구매주기 요약")

        summary_df = _build_partner_cycle_summary(line_df)
        if summary_df.empty:
            st.info("구매주기 요약을 계산할 데이터가 없습니다.")
        else:
            for col in ["최근 구매금액", "누적 매출"]:
                if col in summary_df.columns:
                    summary_df[col] = summary_df[col].apply(fmt_krw)

            st.dataframe(summary_df, use_container_width=True, hide_index=True)

    finally:
        conn.close()