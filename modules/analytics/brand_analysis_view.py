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


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def group_minor_as_others(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    top_n: int = 6,
) -> pd.DataFrame:
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


def get_cigar_product_codes(conn) -> set:
    if not table_exists(conn, "product_mst"):
        return set()

    df = pd.read_sql_query(
        """
        SELECT DISTINCT UPPER(TRIM(COALESCE(product_code, ''))) AS product_code
        FROM product_mst
        WHERE TRIM(COALESCE(product_code, '')) <> ''
        """,
        conn,
    )
    return set(df["product_code"].dropna().tolist())


def get_retail_brand_product_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not view_exists(conn, "v_retail_sales_enriched"):
        return pd.DataFrame(
            columns=["brand", "category", "product_code", "product_name", "qty", "sales", "profit"]
        )

    sql = """
    SELECT
        COALESCE(category, '미분류') AS brand,
        COALESCE(category, '미분류') AS category,
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

    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    df["category"] = df["category"].fillna("미분류").astype(str).str.strip()
    df.loc[df["category"] == "", "category"] = "미분류"

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

    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

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
        df["product_code"] = normalize_code(df["product_code"])
        df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
        df.loc[df["product_name"] == "", "product_name"] = "미분류"

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

        cigar_codes = get_cigar_product_codes(conn)

        def filter_cigar(src: pd.DataFrame) -> pd.DataFrame:
            if src.empty:
                return pd.DataFrame(columns=src.columns)
            if cigar_codes:
                return src[src["product_code"].isin(cigar_codes)].copy()
            return pd.DataFrame(columns=src.columns)

        retail_cigar_df = filter_cigar(retail_df)
        wholesale_cigar_df = filter_cigar(wholesale_df)
        cigar_df = filter_cigar(df)

        def build_product_grouped(src: pd.DataFrame) -> pd.DataFrame:
            if src.empty:
                return pd.DataFrame(
                    columns=[
                        "product_code",
                        "product_name",
                        "판매량",
                        "매출",
                        "이익",
                        "상품코드",
                        "마진율(%)",
                        "개당마진금액",
                    ]
                )

            grp = (
                src.groupby(["product_code", "product_name"], dropna=False)
                .agg(판매량=("qty", "sum"), 매출=("sales", "sum"), 이익=("profit", "sum"))
                .reset_index()
            )
            grp["상품코드"] = grp["product_code"].fillna("").astype(str).str.strip()
            grp.loc[grp["상품코드"] == "", "상품코드"] = grp["product_name"]
            grp["마진율(%)"] = grp.apply(
                lambda x: round(x["이익"] / x["매출"] * 100, 1) if x["매출"] else 0,
                axis=1
            )
            grp["개당마진금액"] = grp.apply(
                lambda x: round(x["이익"] / x["판매량"], 0) if x["판매량"] else 0,
                axis=1
            )
            return grp.sort_values("매출", ascending=False).reset_index(drop=True)

        retail_product_grouped = build_product_grouped(retail_cigar_df)
        wholesale_product_grouped = build_product_grouped(wholesale_cigar_df)
        product_grouped = build_product_grouped(cigar_df)

        total_sales = brand_grouped["매출"].sum()
        total_profit = brand_grouped["이익"].sum()
        brand_count = len(brand_grouped)

        k1, k2, k3 = st.columns(3)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("브랜드 수", f"{brand_count:,}")

        st.caption(f"기준월: {year}-{month:02d}")
        st.divider()

        # 기존 파이차트 2개 유지
        def make_product_pie_df(grp: pd.DataFrame) -> pd.DataFrame:
            if grp.empty:
                return pd.DataFrame(columns=["구분", "금액"])
            return group_minor_as_others(
                grp.rename(columns={"상품코드": "구분", "매출": "금액"}),
                label_col="구분",
                value_col="금액",
                top_n=6,
            )

        p1, p2 = st.columns(2)
        with p1:
            render_pie_chart(
                make_product_pie_df(retail_product_grouped),
                label_col="구분",
                value_col="금액",
                title="시가상품별 매출금액 비중 (소매)",
            )
        with p2:
            render_pie_chart(
                make_product_pie_df(wholesale_product_grouped),
                label_col="구분",
                value_col="금액",
                title="시가상품별 매출금액 비중 (도매)",
            )

        st.divider()

        # 신규 파이차트 2개 추가
        retail_work = retail_df.copy()
        retail_work["product_code"] = normalize_code(retail_work["product_code"])
        retail_work["item_group"] = retail_work["product_code"].apply(
            lambda x: "시가" if x in cigar_codes else "사이드"
        )

        retail_cigar_side_df = (
            retail_work.groupby("item_group", as_index=False)["sales"]
            .sum()
            .rename(columns={"item_group": "구분", "sales": "금액"})
        )

        retail_side_df = retail_work[~retail_work["product_code"].isin(cigar_codes)].copy()

        if "category" not in retail_side_df.columns:
            retail_side_df["category"] = "미분류"

        retail_side_df["category"] = retail_side_df["category"].fillna("미분류").astype(str).str.strip()
        retail_side_df.loc[retail_side_df["category"] == "", "category"] = "미분류"

        retail_side_category_df = (
            retail_side_df.groupby("category", as_index=False)["sales"]
            .sum()
            .rename(columns={"category": "구분", "sales": "금액"})
        )

        retail_side_category_df = group_minor_as_others(
            retail_side_category_df,
            label_col="구분",
            value_col="금액",
            top_n=8,
        )

        p3, p4 = st.columns(2)
        with p3:
            render_pie_chart(
                retail_cigar_side_df,
                label_col="구분",
                value_col="금액",
                title="소매 매출금액 비중 (시가 / 사이드)",
            )

        with p4:
            render_pie_chart(
                retail_side_category_df,
                label_col="구분",
                value_col="금액",
                title="소매 사이드 카테고리별 매출금액",
            )

        st.divider()

        # 기존 막대차트 2개 유지
        b1, b2 = st.columns(2)

        if product_grouped.empty:
            top_product_sales = pd.DataFrame(columns=["상품코드", "매출"])
            top_product_unit_profit = pd.DataFrame(columns=["상품코드", "개당마진금액"])
        else:
            top_product_sales = product_grouped.sort_values("매출", ascending=False).head(20).copy()
            top_product_unit_profit = product_grouped.sort_values("개당마진금액", ascending=False).head(20).copy()

        with b1:
            st.markdown("### 시가상품별 매출금액 (TOP 20)")
            if top_product_sales.empty:
                st.info("시가상품 데이터가 없습니다.")
            else:
                chart_df = (
                    top_product_sales[["상품코드", "매출"]]
                    .sort_values("매출", ascending=False)
                    .copy()
                )

                chart = (
                    alt.Chart(chart_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "상품코드:N",
                            sort=chart_df["상품코드"].tolist(),
                            title="상품코드",
                        ),
                        y=alt.Y("매출:Q", title="매출금액"),
                        tooltip=[
                            alt.Tooltip("상품코드:N", title="상품코드"),
                            alt.Tooltip("매출:Q", title="매출금액", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)

        with b2:
            st.markdown("### 시가상품별 개당 마진금액 (TOP 20)")
            if top_product_unit_profit.empty:
                st.info("시가상품 데이터가 없습니다.")
            else:
                chart_df = (
                    top_product_unit_profit[["상품코드", "개당마진금액"]]
                    .sort_values("개당마진금액", ascending=False)
                    .copy()
                )

                chart = (
                    alt.Chart(chart_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "상품코드:N",
                            sort=chart_df["상품코드"].tolist(),
                            title="상품코드",
                        ),
                        y=alt.Y("개당마진금액:Q", title="개당 마진금액"),
                        tooltip=[
                            alt.Tooltip("상품코드:N", title="상품코드"),
                            alt.Tooltip("개당마진금액:Q", title="개당 마진금액", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)

        # 하단 그리드 제거

    finally:
        conn.close()


if __name__ == "__main__":
    render()