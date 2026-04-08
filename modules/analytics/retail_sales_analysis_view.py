import os
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [str(r[1]).strip() for r in rows]


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def get_non_cigar_category_map(conn) -> dict:
    if not table_exists(conn, "non_cigar_product_mst"):
        return {}

    cols = get_table_columns(conn, "non_cigar_product_mst")
    cols_lower = {c.lower(): c for c in cols}

    code_col = cols_lower.get("product_code")
    if not code_col:
        return {}

    category_col = None
    for candidate in ["category", "product_category", "item_category", "product_type"]:
        if candidate in cols_lower:
            category_col = cols_lower[candidate]
            break

    if not category_col:
        return {}

    sql = f"""
    SELECT
        UPPER(TRIM(COALESCE({code_col}, ''))) AS product_code,
        COALESCE({category_col}, '미분류') AS category
    FROM non_cigar_product_mst
    WHERE TRIM(COALESCE({code_col}, '')) <> ''
    """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return {}

    df["product_code"] = df["product_code"].fillna("").astype(str).str.strip().str.upper()
    df["category"] = df["category"].fillna("미분류").astype(str).str.strip()
    df.loc[df["category"] == "", "category"] = "미분류"
    df = df.drop_duplicates(subset=["product_code"], keep="first")

    return dict(zip(df["product_code"], df["category"]))


def get_packaging_trend_data(conn) -> pd.DataFrame:
    if not table_exists(conn, "retail_sales"):
        return pd.DataFrame(columns=["sale_month", "포장구분", "금액"])

    cols = get_table_columns(conn, "retail_sales")
    cols_lower = {c.lower(): c for c in cols}

    sale_date_col = cols_lower.get("sale_date")
    product_code_col = cols_lower.get("product_code")
    unit_price_col = cols_lower.get("unit_price")
    product_discount_name_col = cols_lower.get("product_discount_name")
    order_discount_name_col = cols_lower.get("order_discount_name")

    required = [sale_date_col, product_code_col, unit_price_col]
    if any(c is None for c in required):
        return pd.DataFrame(columns=["sale_month", "포장구분", "금액"])

    sql = f"""
    SELECT
        {sale_date_col} AS sale_date,
        COALESCE({product_code_col}, '') AS product_code,
        COALESCE({unit_price_col}, 0) AS unit_price,
        COALESCE({product_discount_name_col}, '') AS product_discount_name,
        COALESCE({order_discount_name_col}, '') AS order_discount_name
    FROM retail_sales
    WHERE COALESCE({sale_date_col}, '') <> ''
    """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return pd.DataFrame(columns=["sale_month", "포장구분", "금액"])

    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df[df["sale_date"].notna()].copy()

    df["product_code"] = normalize_code(df["product_code"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
    df["product_discount_name"] = df["product_discount_name"].fillna("").astype(str).str.strip()
    df["order_discount_name"] = df["order_discount_name"].fillna("").astype(str).str.strip()

    # 사이드 카테고리 제외
    non_cigar_category_map = get_non_cigar_category_map(conn)
    df["non_cigar_category"] = df["product_code"].map(non_cigar_category_map)
    df["non_cigar_category"] = df["non_cigar_category"].fillna("").astype(str).str.strip()
    df = df[df["non_cigar_category"] != "사이드"].copy()

    # 포장 / 비포장 구분
    df["포장구분"] = df.apply(
        lambda row: "포장"
        if row["product_discount_name"] == "포장할인" or row["order_discount_name"] == "포장할인"
        else "비포장",
        axis=1,
    )

    df["sale_month"] = df["sale_date"].dt.to_period("M").astype(str)

    trend_df = (
        df.groupby(["sale_month", "포장구분"], as_index=False)["unit_price"]
        .sum()
        .rename(columns={"unit_price": "금액"})
    )

    # 월별 합계 대비 비중(%)
    trend_df["월합계"] = trend_df.groupby("sale_month")["금액"].transform("sum")
    trend_df["비중"] = (
        (trend_df["금액"] / trend_df["월합계"]) * 100
    ).fillna(0)

    trend_df = trend_df.sort_values(["sale_month", "포장구분"])

    return trend_df


def render_packaging_trend_chart(trend_df: pd.DataFrame):
    st.markdown("### 월별 포장 / 비포장 판매금액 추이")

    if trend_df.empty:
        st.info("데이터가 없습니다.")
        return

    month_order = trend_df["sale_month"].drop_duplicates().tolist()

    chart = (
        alt.Chart(trend_df)
        .mark_bar()
        .encode(
            x=alt.X("sale_month:N", title="월", sort=month_order),
            y=alt.Y("금액:Q", title="unit_price 합계", stack="zero"),
            color=alt.Color("포장구분:N", title="구분"),
            tooltip=[
                alt.Tooltip("sale_month:N", title="월"),
                alt.Tooltip("포장구분:N", title="구분"),
                alt.Tooltip("금액:Q", title="금액", format=",.0f"),
                alt.Tooltip("비중:Q", title="비중(%)", format=".1f"),
            ],
        )
        .properties(height=380)
    )

    st.altair_chart(chart, use_container_width=True)

def get_usage_fee_trend_data(conn) -> pd.DataFrame:
    if not table_exists(conn, "retail_sales"):
        return pd.DataFrame(columns=["sale_month", "사용료금액"])

    cols = get_table_columns(conn, "retail_sales")
    cols_lower = {c.lower(): c for c in cols}

    sale_date_col = cols_lower.get("sale_date")
    product_code_col = cols_lower.get("product_code")
    unit_price_col = cols_lower.get("unit_price")

    required = [sale_date_col, product_code_col, unit_price_col]
    if any(c is None for c in required):
        return pd.DataFrame(columns=["sale_month", "사용료금액"])

    sql = f"""
    SELECT
        {sale_date_col} AS sale_date,
        COALESCE({product_code_col}, '') AS product_code,
        COALESCE({unit_price_col}, 0) AS unit_price
    FROM retail_sales
    WHERE COALESCE({sale_date_col}, '') <> ''
    """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return pd.DataFrame(columns=["sale_month", "사용료금액"])

    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df[df["sale_date"].notna()].copy()

    df["product_code"] = normalize_code(df["product_code"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)

    # 상품코드가 '사용료' 인 행만 사용
    df = df[df["product_code"] == "사용료"].copy()

    if df.empty:
        return pd.DataFrame(columns=["sale_month", "사용료금액"])

    df["sale_month"] = df["sale_date"].dt.to_period("M").astype(str)

    trend_df = (
        df.groupby("sale_month", as_index=False)["unit_price"]
        .sum()
        .rename(columns={"unit_price": "사용료금액"})
        .sort_values("sale_month")
    )

    return trend_df

def render_usage_fee_trend_chart(trend_df: pd.DataFrame):
    st.markdown("### 월별 사용료 금액 추이")

    if trend_df.empty:
        st.info("사용료 데이터가 없습니다.")
        return

    month_order = trend_df["sale_month"].drop_duplicates().tolist()

    chart = (
        alt.Chart(trend_df)
        .mark_bar()
        .encode(
            x=alt.X("sale_month:N", title="월", sort=month_order),
            y=alt.Y("사용료금액:Q", title="사용료 금액"),
            tooltip=[
                alt.Tooltip("sale_month:N", title="월"),
                alt.Tooltip("사용료금액:Q", title="사용료 금액", format=",.0f"),
            ],
        )
        .properties(height=320)
    )

    st.altair_chart(chart, use_container_width=True)

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


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


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [str(r[1]).strip() for r in rows]


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


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


def get_non_cigar_category_map(conn) -> dict:
    if not table_exists(conn, "non_cigar_product_mst"):
        return {}

    cols = get_table_columns(conn, "non_cigar_product_mst")
    cols_lower = {c.lower(): c for c in cols}

    code_col = cols_lower.get("product_code")
    if not code_col:
        return {}

    category_col = None
    for candidate in ["category", "product_category", "item_category", "product_type"]:
        if candidate in cols_lower:
            category_col = cols_lower[candidate]
            break

    if not category_col:
        return {}

    sql = f"""
    SELECT
        UPPER(TRIM(COALESCE({code_col}, ''))) AS product_code,
        COALESCE({category_col}, '미분류') AS category
    FROM non_cigar_product_mst
    WHERE TRIM(COALESCE({code_col}, '')) <> ''
    """

    df = pd.read_sql_query(sql, conn)
    if df.empty:
        return {}

    df["product_code"] = df["product_code"].fillna("").astype(str).str.strip().str.upper()
    df["category"] = df["category"].fillna("미분류").astype(str).str.strip()
    df.loc[df["category"] == "", "category"] = "미분류"
    df = df.drop_duplicates(subset=["product_code"], keep="first")

    return dict(zip(df["product_code"], df["category"]))


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


def render():
    st.subheader("소매판매 분석")

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
                key="retail_analysis_year",
            )
        with c2:
            month = st.selectbox(
                "월",
                options=list(range(1, 13)),
                index=current_month - 1,
                key="retail_analysis_month",
            )

        start_date = pd.Timestamp(year=year, month=month, day=1)
        end_date = start_date + pd.offsets.MonthEnd(1)

        retail_df = get_retail_brand_product_data(
            conn,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        if retail_df.empty:
            st.warning("해당 월의 소매판매 데이터가 없습니다.")
            return

        cigar_codes = get_cigar_product_codes(conn)
        non_cigar_category_map = get_non_cigar_category_map(conn)

        retail_work = retail_df.copy()
        retail_work["product_code"] = normalize_code(retail_work["product_code"])
        retail_work["구분"] = retail_work["product_code"].apply(
            lambda x: "시가" if x in cigar_codes else "사이드"
        )

        retail_cigar_side_df = (
            retail_work.groupby("구분", as_index=False)["sales"]
            .sum()
            .rename(columns={"sales": "금액"})
        )

        retail_non_cigar_df = retail_work[retail_work["구분"] == "사이드"].copy()
        retail_non_cigar_df["카테고리"] = retail_non_cigar_df["product_code"].map(non_cigar_category_map)
        retail_non_cigar_df["카테고리"] = (
            retail_non_cigar_df["카테고리"]
            .fillna("미분류")
            .astype(str)
            .str.strip()
        )
        retail_non_cigar_df.loc[retail_non_cigar_df["카테고리"] == "", "카테고리"] = "미분류"

        retail_non_cigar_by_category = (
            retail_non_cigar_df.groupby("카테고리", as_index=False)["sales"]
            .sum()
            .rename(columns={"sales": "금액"})
        )

        retail_non_cigar_by_category = group_minor_as_others(
            retail_non_cigar_by_category,
            label_col="카테고리",
            value_col="금액",
            top_n=8,
        )

        p1, p2 = st.columns(2)
        with p1:
            render_pie_chart(
                retail_cigar_side_df,
                label_col="구분",
                value_col="금액",
                title="소매 매출금액 비중 (시가 / 사이드)",
            )

        with p2:
            render_pie_chart(
                retail_non_cigar_by_category,
                label_col="카테고리",
                value_col="금액",
                title="소매 시가 외 상품 카테고리별 매출금액",
            )

        st.divider()

        trend_df = get_packaging_trend_data(conn)
        render_packaging_trend_chart(trend_df)

        st.divider()

        usage_fee_trend_df = get_usage_fee_trend_data(conn)
        render_usage_fee_trend_chart(usage_fee_trend_df)

    finally:
        conn.close()