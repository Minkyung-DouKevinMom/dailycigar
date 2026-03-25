import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fmt_krw(x):
    try:
        return f"₩{float(x):,.0f}"
    except Exception:
        return "₩0"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
        (view_name,),
    )
    return cur.fetchone() is not None


def get_retail_brand_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not view_exists(conn, "v_retail_sales_enriched"):
        return pd.DataFrame(columns=["brand", "qty", "sales", "profit"])

    sql = """
        SELECT
            COALESCE(category, '미분류') AS brand,
            COALESCE(qty, 0) AS qty,
            COALESCE(net_sales_amount, 0) AS sales,
            COALESCE(retail_gross_profit_krw, 0) AS profit
        FROM v_retail_sales_enriched
        WHERE sale_date BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    for c in ["qty", "sales", "profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def get_wholesale_brand_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    source = None
    if view_exists(conn, "v_wholesale_sales"):
        source = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        source = "wholesale_sales"

    if not source:
        return pd.DataFrame(columns=["brand", "qty", "sales", "profit"])

    if source == "v_wholesale_sales":
        sql = """
            SELECT
                COALESCE(item_type, product_name, '미분류') AS brand,
                COALESCE(qty, 0) AS qty,
                COALESCE(sales_amount, 0) AS sales,
                COALESCE(profit_amount, 0) AS profit
            FROM v_wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
    else:
        sql = """
            SELECT
                COALESCE(item_type, product_name, '미분류') AS brand,
                COALESCE(qty, 0) AS qty,
                COALESCE(sales_amount, 0) AS sales,
                COALESCE(profit_amount, 0) AS profit
            FROM wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """

    df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    for c in ["qty", "sales", "profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def group_minor_as_others(df: pd.DataFrame, value_col: str, label_col: str = "brand", top_n: int = 6) -> pd.DataFrame:
    if df.empty:
        return df

    work = df[[label_col, value_col]].copy()
    work = work.groupby(label_col, as_index=False)[value_col].sum()
    work = work.sort_values(value_col, ascending=False)

    if len(work) <= top_n:
        return work

    top_df = work.head(top_n).copy()
    others_sum = work.iloc[top_n:][value_col].sum()

    if others_sum > 0:
        top_df = pd.concat(
            [
                top_df,
                pd.DataFrame([{label_col: "기타", value_col: others_sum}])
            ],
            ignore_index=True,
        )

    return top_df


def render():
    st.subheader("브랜드 분석")

    conn = get_conn()
    try:
        today = pd.Timestamp.today()
        current_year = today.year
        current_month = today.month

        c1, c2 = st.columns(2)
        with c1:
            year = st.selectbox(
                "연도",
                options=list(range(current_year - 2, current_year + 1)),
                index=2,
                key="brand_analysis_year",
            )
        with c2:
            month = st.selectbox(
                "월",
                options=list(range(1, 13)),
                index=current_month - 1,
                key="brand_analysis_month",
            )

        start_date = pd.Timestamp(year=year, month=month, day=1)
        end_date = start_date + pd.offsets.MonthEnd(1)

        retail_df = get_retail_brand_data(
            conn,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        wholesale_df = get_wholesale_brand_data(
            conn,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        frames = []
        if not retail_df.empty:
            frames.append(retail_df)
        if not wholesale_df.empty:
            frames.append(wholesale_df)

        if not frames:
            st.warning("해당 월의 브랜드 데이터가 없습니다.")
            return

        df = pd.concat(frames, ignore_index=True)

        grouped = (
            df.groupby("brand", dropna=False)
            .agg(
                판매량=("qty", "sum"),
                매출=("sales", "sum"),
                이익=("profit", "sum"),
            )
            .reset_index()
        )

        grouped["브랜드"] = grouped["brand"].fillna("미분류").astype(str).str.strip()
        grouped.loc[grouped["브랜드"] == "", "브랜드"] = "미분류"

        grouped = (
            grouped.groupby("브랜드", as_index=False)[["판매량", "매출", "이익"]]
            .sum()
        )

        grouped["마진율(%)"] = grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1,
        )

        grouped = grouped.sort_values("매출", ascending=False).reset_index(drop=True)

        total_sales = grouped["매출"].sum()
        total_profit = grouped["이익"].sum()
        brand_count = len(grouped)

        k1, k2, k3 = st.columns(3)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("브랜드 수", f"{brand_count:,}")

        st.caption(f"기준월: {year}-{month:02d}")

        st.divider()

        # 파이차트용 데이터
        qty_pie_df = group_minor_as_others(
            grouped.rename(columns={"브랜드": "brand", "판매량": "value"}),
            value_col="value",
            label_col="brand",
            top_n=6,
        )
        sales_pie_df = group_minor_as_others(
            grouped.rename(columns={"브랜드": "brand", "매출": "value"}),
            value_col="value",
            label_col="brand",
            top_n=6,
        )

        p1, p2 = st.columns(2)

        with p1:
            st.markdown("### 브랜드 판매량 비중")
            if qty_pie_df.empty or qty_pie_df["value"].sum() == 0:
                st.info("판매량 데이터가 없습니다.")
            else:
                fig_qty = px.pie(
                    qty_pie_df,
                    names="brand",
                    values="value",
                    hole=0.35,
                )
                fig_qty.update_traces(textposition="inside", textinfo="percent+label")
                fig_qty.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_qty, use_container_width=True)

        with p2:
            st.markdown("### 브랜드 판매금액 비중")
            if sales_pie_df.empty or sales_pie_df["value"].sum() == 0:
                st.info("판매금액 데이터가 없습니다.")
            else:
                fig_sales = px.pie(
                    sales_pie_df,
                    names="brand",
                    values="value",
                    hole=0.35,
                )
                fig_sales.update_traces(textposition="inside", textinfo="percent+label")
                fig_sales.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_sales, use_container_width=True)

        st.divider()

        b1, b2 = st.columns(2)

        with b1:
            st.markdown("### 브랜드별 매출")
            sales_bar_df = grouped.set_index("브랜드")[["매출"]]
            st.bar_chart(sales_bar_df, use_container_width=True)

        with b2:
            st.markdown("### 브랜드별 마진율")
            margin_bar_df = grouped.set_index("브랜드")[["마진율(%)"]]
            st.bar_chart(margin_bar_df, use_container_width=True)

        st.divider()

        st.markdown("### 브랜드 상세")

        show_df = grouped.copy()
        show_df["매출"] = show_df["매출"].apply(fmt_krw)
        show_df["이익"] = show_df["이익"].apply(fmt_krw)

        st.dataframe(
            show_df[["브랜드", "판매량", "매출", "이익", "마진율(%)"]],
            use_container_width=True,
            hide_index=True,
            height=450,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    render()