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


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [str(r[1]).strip() for r in rows]


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def group_minor_as_others(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    qty_col: str | None = None,
    top_n: int = 6,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    agg_cols = [label_col, value_col]
    if qty_col and qty_col in df.columns:
        agg_cols.append(qty_col)

    work = df[agg_cols].copy()
    agg_dict = {value_col: "sum"}
    if qty_col and qty_col in df.columns:
        agg_dict[qty_col] = "sum"
    work = work.groupby(label_col, as_index=False).agg(agg_dict)
    work = work.sort_values(value_col, ascending=False)

    if len(work) <= top_n:
        return work

    top_df = work.head(top_n).copy()
    others_row = {label_col: "기타", value_col: work.iloc[top_n:][value_col].sum()}
    if qty_col and qty_col in work.columns:
        others_row[qty_col] = work.iloc[top_n:][qty_col].sum()

    if others_row[value_col] > 0:
        top_df = pd.concat(
            [top_df, pd.DataFrame([others_row])],
            ignore_index=True,
        )
    return top_df


def render_pie_chart(df: pd.DataFrame, label_col: str, value_col: str, title: str, qty_col: str | None = None):
    if df.empty or df[value_col].sum() == 0:
        st.info("데이터가 없습니다.")
        return

    tooltip = [
        alt.Tooltip(label_col, title="구분"),
        alt.Tooltip(value_col, title="금액", format=",.0f"),
    ]
    if qty_col and qty_col in df.columns:
        tooltip.append(alt.Tooltip(qty_col, title="판매수량", format=",.0f"))

    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=40)
        .encode(
            theta=alt.Theta(field=value_col, type="quantitative"),
            color=alt.Color(field=label_col, type="nominal", legend=alt.Legend(title=None)),
            tooltip=tooltip,
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


def get_retail_brand_product_data(conn, date_from: str | None, date_to: str | None) -> pd.DataFrame:
    if not view_exists(conn, "v_retail_sales_enriched"):
        return pd.DataFrame(
            columns=["brand", "product_code", "product_name", "qty", "sales", "profit"]
        )

    if date_from and date_to:
        where = "WHERE sale_date BETWEEN ? AND ?"
        params = [date_from, date_to]
    else:
        where = ""
        params = []

    sql = f"""
    SELECT
        COALESCE(category, '미분류') AS brand,
        COALESCE(product_code, product_code_raw, '') AS product_code,
        COALESCE(mst_product_name, product_code_raw, '미분류') AS product_name,
        COALESCE(qty, 0) AS qty,
        COALESCE(net_sales_amount, 0) AS sales,
        COALESCE(retail_gross_profit_krw, 0) AS profit
    FROM v_retail_sales_enriched
    {where}
    """
    df = pd.read_sql_query(sql, conn, params=params)

    for c in ["qty", "sales", "profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    return df


def get_wholesale_brand_product_data(conn, date_from: str | None, date_to: str | None) -> pd.DataFrame:
    source = None
    if view_exists(conn, "v_wholesale_sales"):
        source = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        source = "wholesale_sales"

    if not source:
        return pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    if date_from and date_to:
        where = "WHERE sale_date BETWEEN ? AND ?"
        params = [date_from, date_to]
    else:
        where = ""
        params = []

    sql = f"""
    SELECT
        COALESCE(item_type, '미분류') AS brand,
        COALESCE(product_code, '') AS product_code,
        COALESCE(product_name, product_code, '미분류') AS product_name,
        COALESCE(qty, 0) AS qty,
        COALESCE(sales_amount, 0) AS sales,
        COALESCE(profit_amount, 0) AS profit
    FROM {source}
    {where}
    """

    df = pd.read_sql_query(sql, conn, params=params)

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

        # ── 기간 선택 ──────────────────────────────────────────────
        period_mode = st.radio(
            "기간 선택",
            options=["월별", "전체 기간"],
            horizontal=True,
            key="brand_analysis_period_mode",
        )

        date_from: str | None = None
        date_to: str | None = None
        period_label = "전체 기간"

        if period_mode == "월별":
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
            date_from = start_date.strftime("%Y-%m-%d")
            date_to = end_date.strftime("%Y-%m-%d")
            period_label = f"{year}-{month:02d}"

        # ── 데이터 조회 ────────────────────────────────────────────
        retail_df = get_retail_brand_product_data(conn, date_from, date_to)
        wholesale_df = get_wholesale_brand_product_data(conn, date_from, date_to)

        frames = []
        if not retail_df.empty:
            frames.append(retail_df)
        if not wholesale_df.empty:
            frames.append(wholesale_df)

        if not frames:
            st.warning("브랜드 데이터가 없습니다.")
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
        non_cigar_category_map = get_non_cigar_category_map(conn)

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
        total_qty = brand_grouped["판매량"].sum()
        cigar_product_count = cigar_df["product_code"].nunique() if not cigar_df.empty else 0

        # ── KPI 카드 ───────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("총판매수량", f"{int(total_qty):,}개")
        k4.metric("시가 제품 수", f"{cigar_product_count:,}")

        st.caption(f"기준: {period_label}")
        st.divider()

        # ── 파이차트 ───────────────────────────────────────────────
        def make_product_pie_df(grp: pd.DataFrame) -> pd.DataFrame:
            if grp.empty:
                return pd.DataFrame(columns=["구분", "금액", "판매수량"])
            renamed = grp.rename(columns={"상품코드": "구분", "매출": "금액", "판매량": "판매수량"})
            return group_minor_as_others(
                renamed,
                label_col="구분",
                value_col="금액",
                qty_col="판매수량",
                top_n=10,
            )

        p1, p2 = st.columns(2)
        with p1:
            render_pie_chart(
                make_product_pie_df(retail_product_grouped),
                label_col="구분",
                value_col="금액",
                qty_col="판매수량",
                title="시가상품별 매출금액 비중 (소매)",
            )
        with p2:
            render_pie_chart(
                make_product_pie_df(wholesale_product_grouped),
                label_col="구분",
                value_col="금액",
                qty_col="판매수량",
                title="시가상품별 매출금액 비중 (도매)",
            )

        st.divider()

        # ── 바차트 ─────────────────────────────────────────────────
        b1, b2 = st.columns(2)

        if product_grouped.empty:
            top_product_sales = pd.DataFrame(columns=["상품코드", "매출", "판매량"])
            top_product_unit_profit = pd.DataFrame(columns=["상품코드", "개당마진금액", "판매량"])
        else:
            top_product_sales = product_grouped.sort_values("매출", ascending=False).head(20).copy()
            top_product_unit_profit = product_grouped.sort_values("개당마진금액", ascending=False).head(20).copy()

        with b1:
            st.markdown("### 시가상품별 매출금액 (TOP 20)")
            if top_product_sales.empty:
                st.info("시가상품 데이터가 없습니다.")
            else:
                chart_df = (
                    top_product_sales[["상품코드", "매출", "판매량"]]
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
                            alt.Tooltip("판매량:Q", title="판매수량", format=",.0f"),
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
                    top_product_unit_profit[["상품코드", "개당마진금액", "판매량"]]
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
                            alt.Tooltip("판매량:Q", title="판매수량", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)

    finally:
        conn.close()


if __name__ == "__main__":
    render()