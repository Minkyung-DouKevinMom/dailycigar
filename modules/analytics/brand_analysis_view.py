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


def group_minor_as_others(df: pd.DataFrame, label_col: str, value_col: str, top_n: int = 6) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df[[label_col, value_col]].copy()
    work = work.groupby(label_col, as_index=False)[value_col].sum()
    work = work.sort_values(value_col, ascending=False)

    if len(work) <= top_n:
        return work

    top_df = work.head(top_n).copy()
    others_sum = work.iloc[top_n:][value_col].sum()

    if others_sum > 0:
        top_df = pd.concat(
            [top_df, pd.DataFrame([{label_col: "기타", value_col: others_sum}])],
            ignore_index=True,
        )

    return top_df


def render_pie_chart(df: pd.DataFrame, label_col: str, value_col: str, title: str):
    if df.empty or df[value_col].sum() == 0:
        st.info("데이터가 없습니다.")
        return

    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=40)
        .encode(
            theta=alt.Theta(field=value_col, type="quantitative"),
            color=alt.Color(field=label_col, type="nominal", legend=alt.Legend(title=None)),
            tooltip=[
                alt.Tooltip(label_col, title="구분"),
                alt.Tooltip(value_col, title="금액", format=",.0f"),
            ],
        )
        .properties(title=title, height=340)
    )
    st.altair_chart(chart, use_container_width=True)


def get_retail_brand_product_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not view_exists(conn, "v_retail_sales_enriched"):
        return pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    sql = """
        SELECT
            COALESCE(category, '미분류') AS brand,
            COALESCE(product_code, product_code_raw, '') AS product_code,
            COALESCE(mst_product_name, product_code_raw, '미분류') AS product_name,
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


def get_wholesale_brand_product_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    source = None
    if view_exists(conn, "v_wholesale_sales"):
        source = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        source = "wholesale_sales"

    if not source:
        return pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    if source == "v_wholesale_sales":
        sql = """
            SELECT
                COALESCE(item_type, '미분류') AS brand,
                COALESCE(product_code, '') AS product_code,
                COALESCE(product_name, product_code, '미분류') AS product_name,
                COALESCE(qty, 0) AS qty,
                COALESCE(sales_amount, 0) AS sales,
                COALESCE(profit_amount, 0) AS profit
            FROM v_wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
    else:
        sql = """
            SELECT
                COALESCE(item_type, '미분류') AS brand,
                COALESCE(product_code, '') AS product_code,
                COALESCE(product_name, product_code, '미분류') AS product_name,
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

        retail_df = get_retail_brand_product_data(
            conn,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        wholesale_df = get_wholesale_brand_product_data(
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

        df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
        df.loc[df["brand"] == "", "brand"] = "미분류"

        df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
        df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
        df.loc[df["product_name"] == "", "product_name"] = "미분류"

        # 브랜드 집계
        brand_grouped = (
            df.groupby("brand", dropna=False)
            .agg(
                판매량=("qty", "sum"),
                매출=("sales", "sum"),
                이익=("profit", "sum"),
            )
            .reset_index()
            .rename(columns={"brand": "브랜드"})
        )

        brand_grouped["마진율(%)"] = brand_grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1,
        )
        brand_grouped = brand_grouped.sort_values("매출", ascending=False).reset_index(drop=True)

        # 상품 집계
        product_grouped = (
            df.groupby(["product_code", "product_name"], dropna=False)
            .agg(
                판매량=("qty", "sum"),
                매출=("sales", "sum"),
                이익=("profit", "sum"),
            )
            .reset_index()
        )

        product_grouped["상품코드"] = product_grouped["product_code"].fillna("").astype(str).str.strip()
        product_grouped.loc[product_grouped["상품코드"] == "", "상품코드"] = product_grouped["product_name"]

        product_grouped["마진율(%)"] = product_grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1,
        )
        product_grouped = product_grouped.sort_values("매출", ascending=False).reset_index(drop=True)

        total_sales = brand_grouped["매출"].sum()
        total_profit = brand_grouped["이익"].sum()
        brand_count = len(brand_grouped)

        k1, k2, k3 = st.columns(3)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("브랜드 수", f"{brand_count:,}")

        st.caption(f"기준월: {year}-{month:02d}")

        st.divider()

        # 파이차트 2개
        brand_pie_df = group_minor_as_others(
            brand_grouped.rename(columns={"브랜드": "구분", "매출": "금액"}),
            label_col="구분",
            value_col="금액",
            top_n=6,
        )

        product_pie_df = group_minor_as_others(
            product_grouped.rename(columns={"상품코드": "구분", "매출": "금액"}),
            label_col="구분",
            value_col="금액",
            top_n=6,
        )

        p1, p2 = st.columns(2)

        with p1:
            render_pie_chart(
                brand_pie_df,
                label_col="구분",
                value_col="금액",
                title="브랜드별 매출금액 비중",
            )

        with p2:
            render_pie_chart(
                product_pie_df,
                label_col="구분",
                value_col="금액",
                title="시가상품별 매출금액 비중",
            )

        st.divider()

        # 하단 막대차트 2개 - 상품 기준 / X축 상품코드
        b1, b2 = st.columns(2)

        top_product_sales = product_grouped.head(20).copy()
        top_product_margin = product_grouped.sort_values("마진율(%)", ascending=False).head(20).copy()

        with b1:
            st.markdown("### 시가상품별 매출금액")
            sales_bar_df = top_product_sales.set_index("상품코드")[["매출"]]
            st.bar_chart(sales_bar_df, use_container_width=True)

        with b2:
            st.markdown("### 시가상품별 마진율")
            margin_bar_df = top_product_margin.set_index("상품코드")[["마진율(%)"]]
            st.bar_chart(margin_bar_df, use_container_width=True)

        st.divider()

        st.markdown("### 브랜드 상세")

        show_brand_df = brand_grouped.copy()
        show_brand_df["매출"] = show_brand_df["매출"].apply(fmt_krw)
        show_brand_df["이익"] = show_brand_df["이익"].apply(fmt_krw)

        st.dataframe(
            show_brand_df[["브랜드", "판매량", "매출", "이익", "마진율(%)"]],
            use_container_width=True,
            hide_index=True,
            height=420,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    render()