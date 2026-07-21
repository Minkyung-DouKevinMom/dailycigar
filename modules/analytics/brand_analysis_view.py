import os
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

from db import get_stock_summary

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


def get_cigar_brand_map(conn, cigar_codes: set) -> dict:
    """
    product_code -> brand(category) 매핑.
    선물세트로 빠져나간 시가 수량에 브랜드를 붙이기 위해 retail_sales 기준으로
    상품코드별 대표 브랜드(category)를 조회한다. (기간 무관, 전체 이력 기준)
    """
    if not view_exists(conn, "v_retail_sales_enriched"):
        return {}

    df = pd.read_sql_query(
        """
        SELECT
            UPPER(TRIM(COALESCE(product_code, product_code_raw, ''))) AS product_code,
            COALESCE(category, '미분류') AS brand
        FROM v_retail_sales_enriched
        WHERE COALESCE(product_code, product_code_raw, '') <> ''
        """,
        conn,
    )
    if df.empty:
        return {}

    df["product_code"] = normalize_code(df["product_code"])
    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)]

    df = df.drop_duplicates(subset=["product_code"], keep="first")
    return dict(zip(df["product_code"], df["brand"]))


def get_gift_set_cigar_out(
    conn, cigar_codes: set, date_from: str | None, date_to: str | None
) -> pd.DataFrame:
    """
    선물세트(gift_set)로 차감된 시가 상품 수량.
    매출/이익은 이미 기프트패키지 상품(non_cigar) 판매에 반영되어 있으므로
    여기서는 수량만 집계하고 sales/profit은 0으로 둔다 (이중계산 방지).
    """
    empty = pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    if not table_exists(conn, "stock_out"):
        return empty

    where = "WHERE so.out_type = 'gift_set'"
    params = []
    if date_from and date_to:
        where += " AND so.out_date BETWEEN ? AND ?"
        params = [date_from, date_to]

    sql = f"""
    SELECT
        UPPER(TRIM(COALESCE(so.product_code, ''))) AS product_code,
        COALESCE(pm.product_name, so.product_code, '미분류') AS product_name,
        SUM(so.qty) AS qty
    FROM stock_out so
    LEFT JOIN product_mst pm ON so.product_code = pm.product_code
    {where}
    GROUP BY so.product_code
    """
    df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        return empty

    df["product_code"] = normalize_code(df["product_code"])
    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)].copy()
    else:
        df = df.iloc[0:0].copy()

    if df.empty:
        return empty

    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["sales"] = 0
    df["profit"] = 0
    df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
    df.loc[df["product_name"] == "", "product_name"] = "미분류"
    df["brand"] = "미분류"  # 아래에서 get_cigar_brand_map 으로 실제 브랜드 매핑

    return df[["brand", "product_code", "product_name", "qty", "sales", "profit"]]


def _get_cigar_stock_base(cigar_codes: set) -> pd.DataFrame:
    """재고관리 데이터에서 시가 제품만 추출 (내부 공통 함수)"""
    try:
        df = get_stock_summary(keyword="", include_inactive=False)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df["product_code"] = (
        df["product_code"].fillna("").astype(str).str.strip().str.upper()
    )
    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)].copy()

    if df.empty:
        return pd.DataFrame()

    for col in ["total_in", "retail_out", "wholesale_out", "other_out", "current_stock"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["total_out"] = (
        df.get("retail_out", 0)
        + df.get("wholesale_out", 0)
        + df.get("other_out", 0)
    )
    df["label"] = df.apply(
        lambda r: f"{r['product_code']}"
        + (f"\n({r['size_name']})" if pd.notna(r.get("size_name")) and str(r.get("size_name", "")).strip() else ""),
        axis=1,
    )
    return df.reset_index(drop=True)


def get_cigar_stock_chart_data(cigar_codes: set) -> pd.DataFrame:
    """재고 현황 그래프용 DataFrame"""
    df = _get_cigar_stock_base(cigar_codes)
    if df.empty:
        return df
    return df.sort_values("total_out", ascending=False).reset_index(drop=True)


def get_cigar_monthly_avg_data(
    conn,
    cigar_codes: set,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """
    지정 기간의 소매+도매 출고 합산으로 제품별 월평균 출고수를 계산하고
    재고관리의 현재고와 결합하여 잔여 개월 수를 반환.

    반환 컬럼:
      product_code, product_name, size_name, label,
      period_out      : 기간 내 총 출고수
      months          : 기간 월수
      monthly_avg     : 월평균 출고수
      current_stock   : 현재고 (재고관리 기준)
      remaining_months: 현재고 ÷ 월평균 출고수 (소진 예상 개월)
    """
    # ── 1. 기간 내 소매 출고 ──
    retail_rows = pd.DataFrame()
    if view_exists(conn, "v_retail_sales_enriched"):
        try:
            retail_rows = pd.read_sql_query(
                """
                SELECT
                    UPPER(TRIM(COALESCE(product_code, product_code_raw, ''))) AS product_code,
                    strftime('%Y-%m', sale_date) AS ym,
                    SUM(COALESCE(qty, 0)) AS qty
                FROM v_retail_sales_enriched
                WHERE sale_date BETWEEN ? AND ?
                GROUP BY product_code, ym
                """,
                conn, params=[date_from, date_to],
            )
        except Exception:
            pass

    # ── 2. 기간 내 도매 출고 ──
    wholesale_rows = pd.DataFrame()
    ws_src = None
    if view_exists(conn, "v_wholesale_sales"):
        ws_src = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        ws_src = "wholesale_sales"

    if ws_src:
        try:
            wholesale_rows = pd.read_sql_query(
                f"""
                SELECT
                    UPPER(TRIM(COALESCE(product_code, ''))) AS product_code,
                    strftime('%Y-%m', sale_date) AS ym,
                    SUM(COALESCE(qty, 0)) AS qty
                FROM {ws_src}
                WHERE sale_date BETWEEN ? AND ?
                GROUP BY product_code, ym
                """,
                conn, params=[date_from, date_to],
            )
        except Exception:
            pass

    frames = [f for f in [retail_rows, wholesale_rows] if not f.empty]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["qty"] = pd.to_numeric(combined["qty"], errors="coerce").fillna(0)
    combined["product_code"] = (
        combined["product_code"].fillna("").astype(str).str.strip().str.upper()
    )

    # 시가 제품만 필터
    if cigar_codes:
        combined = combined[combined["product_code"].isin(cigar_codes)].copy()

    if combined.empty:
        return pd.DataFrame()

    # ── 3. 월수 계산 ──
    ts_from = pd.Timestamp(date_from)
    ts_to   = pd.Timestamp(date_to)
    months  = (ts_to.year - ts_from.year) * 12 + (ts_to.month - ts_from.month) + 1
    months  = max(months, 1)

    # ── 4. 제품별 기간 총 출고 ──
    period_out = (
        combined.groupby("product_code", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "period_out"})
    )
    period_out["monthly_avg"] = (period_out["period_out"] / months).round(1)

    # ── 5. 현재고 결합 (재고관리 기준) ──
    stock_base = _get_cigar_stock_base(cigar_codes)

    if stock_base.empty:
        result = period_out.copy()
        result["product_name"] = result["product_code"]
        result["size_name"]    = ""
        result["label"]        = result["product_code"]
        result["current_stock"] = 0
    else:
        # product_code 기준 조인 (같은 코드에 사이즈 여러 개면 sum)
        stock_agg = (
            stock_base.groupby("product_code", as_index=False)
            .agg(
                product_name=("product_name", "first"),
                size_name=("size_name", "first"),
                label=("label", "first"),
                current_stock=("current_stock", "sum"),
            )
        )
        result = period_out.merge(stock_agg, on="product_code", how="left")
        result["product_name"]  = result["product_name"].fillna(result["product_code"])
        result["size_name"]     = result["size_name"].fillna("")
        result["label"]         = result["label"].fillna(result["product_code"])
        result["current_stock"] = result["current_stock"].fillna(0)

    result["months"] = months

    # ── 6. 잔여 개월 수 ──
    def calc_remaining(row):
        if row["monthly_avg"] <= 0:
            return None   # 출고 없음 → 계산 불가
        return round(row["current_stock"] / row["monthly_avg"], 1)

    result["remaining_months"] = result.apply(calc_remaining, axis=1)

    return result.sort_values("monthly_avg", ascending=False).reset_index(drop=True)


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

        # ── 시가 상품만 대상으로 필터링 (재고관리 총출고와 집계 기준 통일) ──
        cigar_codes = get_cigar_product_codes(conn)
        non_cigar_category_map = get_non_cigar_category_map(conn)

        def _filter_cigar_src(src: pd.DataFrame) -> pd.DataFrame:
            if src.empty:
                return src.copy()
            src = src.copy()
            src["product_code"] = normalize_code(src["product_code"])
            if not cigar_codes:
                return src.iloc[0:0].copy()
            return src[src["product_code"].isin(cigar_codes)].copy()

        retail_df = _filter_cigar_src(retail_df)
        wholesale_df = _filter_cigar_src(wholesale_df)

        # ── 선물세트로 차감된 시가 수량 추가 (매출·이익은 0, 수량만) ──
        # 이미 기프트패키지(non_cigar) 상품 판매로 매출/이익이 잡혀 있으므로
        # 여기서는 "판매수량"에만 반영해 재고관리 총출고 수와 기준을 맞춘다.
        gift_set_df = get_gift_set_cigar_out(conn, cigar_codes, date_from, date_to)
        if not gift_set_df.empty:
            brand_map = get_cigar_brand_map(conn, cigar_codes)
            gift_set_df["brand"] = (
                gift_set_df["product_code"].map(brand_map).fillna("미분류")
            )

        frames = []
        if not retail_df.empty:
            frames.append(retail_df)
        if not wholesale_df.empty:
            frames.append(wholesale_df)
        if not gift_set_df.empty:
            frames.append(gift_set_df)

        if not frames:
            st.warning("시가 상품 데이터가 없습니다.")
            return

        df = pd.concat(frames, ignore_index=True)
        df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
        df.loc[df["brand"] == "", "brand"] = "미분류"
        df["product_code"] = normalize_code(df["product_code"])
        df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
        df.loc[df["product_name"] == "", "product_name"] = "미분류"

        if df.empty:
            st.warning("시가 상품 데이터가 없습니다.")
            return

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

        # retail_df / wholesale_df 는 이미 시가 상품만 필터링된 상태
        # 상품별 소매/도매 차트는 선물세트분(gift_set_df)을 굳이 섞지 않음
        # (소매/도매 매출 비중 차트의 목적과 맞지 않으므로) — 전체 판매수량 KPI에는 반영됨
        retail_cigar_df = retail_df
        wholesale_cigar_df = wholesale_df
        cigar_df = df  # 선물세트분 포함된 전체 (KPI/전체상품 집계용)

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
        gift_set_qty = gift_set_df["qty"].sum() if not gift_set_df.empty else 0

        # ── KPI 카드 ───────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("총판매수량", f"{int(total_qty):,}개")
        k4.metric("시가 제품 수", f"{cigar_product_count:,}")

        st.caption(
            f"기준: {period_label} · 시가 상품만 집계"
            + (f" (선물세트 차감 {int(gift_set_qty):,}개 포함, 매출은 기프트패키지 판매에 반영됨)"
               if gift_set_qty else "")
        )
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

        st.divider()

        # ── 시가 제품별 월평균 출고 & 재고 예측 ────────────────────
        st.markdown("### 시가 제품별 월평균 출고 & 재고 예측")
        st.caption("기프트패키지 등 비시가 제품 제외 · 소매+도매 출고 합산 기준")

        # 기간 선택 (브랜드 분석 기간과 독립)
        inv_c1, inv_c2, inv_c3 = st.columns([2, 2, 3])
        with inv_c1:
            inv_date_from = st.date_input(
                "분석 시작일",
                value=pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=5),
                key="inv_avg_date_from",
            )
        with inv_c2:
            inv_date_to = st.date_input(
                "분석 종료일",
                value=pd.Timestamp.today(),
                key="inv_avg_date_to",
            )
        with inv_c3:
            st.markdown("")  # 여백

        if inv_date_from > inv_date_to:
            st.warning("시작일이 종료일보다 늦습니다.")
        else:
            inv_from_str = inv_date_from.strftime("%Y-%m-%d")
            inv_to_str   = inv_date_to.strftime("%Y-%m-%d")

            ts_from = pd.Timestamp(inv_from_str)
            ts_to   = pd.Timestamp(inv_to_str)
            n_months = (ts_to.year - ts_from.year) * 12 + (ts_to.month - ts_from.month) + 1

            st.caption(
                f"기간: {inv_from_str} ~ {inv_to_str}  |  기준 월수: **{n_months}개월**"
            )

            avg_df = get_cigar_monthly_avg_data(
                conn, cigar_codes, inv_from_str, inv_to_str
            )

            if avg_df.empty:
                st.info("해당 기간의 시가 제품 출고 데이터가 없습니다.")
            else:
                # ── KPI ──
                k1, k2, k3 = st.columns(3)
                k1.metric("분석 제품 수", f"{len(avg_df):,} 개")
                k2.metric("기간 내 총 출고", f"{int(avg_df['period_out'].sum()):,} 개비")
                k3.metric(
                    "재고 소진 임박 (2개월 이내)",
                    f"{int((avg_df['remaining_months'].dropna() <= 2).sum()):,} 개",
                )

                st.divider()

                # ── 탭: 월평균 출고 / 잔여 개월 ──
                t_avg, t_remain = st.tabs(["📊 월평균 출고수", "📅 재고 소진 예측"])

                with t_avg:
                    max_n = min(len(avg_df), 50)
                    top_n = st.slider(
                        "표시 제품 수 (월평균 출고 기준 상위)",
                        min_value=5, max_value=max_n,
                        value=min(20, max_n), step=5,
                        key="inv_avg_top_n",
                    )
                    chart_avg = avg_df.head(top_n).copy()
                    sorted_labels = chart_avg.sort_values(
                        "monthly_avg", ascending=False
                    )["label"].tolist()

                    bar_avg = (
                        alt.Chart(chart_avg)
                        .mark_bar(color="#4C9BE8")
                        .encode(
                            x=alt.X(
                                "label:N", sort=sorted_labels,
                                title="상품코드",
                                axis=alt.Axis(labelAngle=-40, labelFontSize=10),
                            ),
                            y=alt.Y("monthly_avg:Q", title="월평균 출고수 (개비)"),
                            tooltip=[
                                alt.Tooltip("product_code:N", title="상품코드"),
                                alt.Tooltip("product_name:N", title="상품명"),
                                alt.Tooltip("size_name:N",    title="사이즈"),
                                alt.Tooltip("period_out:Q",   title=f"{n_months}개월 총 출고", format=",.0f"),
                                alt.Tooltip("monthly_avg:Q",  title="월평균 출고수", format=",.1f"),
                                alt.Tooltip("current_stock:Q",title="현재고", format=",.0f"),
                            ],
                        )
                        .properties(height=400)
                    )
                    text_avg = bar_avg.mark_text(
                        align="center", baseline="bottom", dy=-3, fontSize=10
                    ).encode(text=alt.Text("monthly_avg:Q", format=".1f"))
                    st.altair_chart(bar_avg + text_avg, use_container_width=True)

                with t_remain:
                    # remaining_months 가 None(출고 없음)인 경우 별도 안내
                    no_out = avg_df[avg_df["remaining_months"].isna()].copy()
                    has_out = avg_df[avg_df["remaining_months"].notna()].copy()

                    if has_out.empty:
                        st.info("출고 이력이 있는 제품이 없습니다.")
                    else:
                        has_out["remaining_months"] = has_out["remaining_months"].clip(lower=0)
                        has_out["_status"] = has_out["remaining_months"].apply(
                            lambda v: "🔴 1개월 이내" if v <= 1
                            else ("🟠 2개월 이내" if v <= 2
                            else ("🟡 3개월 이내" if v <= 3
                            else "🟢 3개월 초과"))
                        )

                        max_n2 = min(len(has_out), 50)
                        top_n2 = st.slider(
                            "표시 제품 수 (잔여 개월 적은 순)",
                            min_value=5, max_value=max_n2,
                            value=min(20, max_n2), step=5,
                            key="inv_remain_top_n",
                        )
                        # 잔여 개월 오름차순 정렬 (소진 임박 우선)
                        chart_rem = (
                            has_out.sort_values("remaining_months", ascending=True)
                            .head(top_n2)
                            .copy()
                        )
                        sorted_rem = chart_rem["label"].tolist()

                        bar_rem = (
                            alt.Chart(chart_rem)
                            .mark_bar()
                            .encode(
                                x=alt.X(
                                    "label:N", sort=sorted_rem,
                                    title="상품코드",
                                    axis=alt.Axis(labelAngle=-40, labelFontSize=10),
                                ),
                                y=alt.Y("remaining_months:Q", title="잔여 개월 수"),
                                color=alt.Color(
                                    "_status:N",
                                    scale=alt.Scale(
                                        domain=["🔴 1개월 이내", "🟠 2개월 이내",
                                                "🟡 3개월 이내", "🟢 3개월 초과"],
                                        range=["#e53935", "#FB8C00", "#FDD835", "#43A047"],
                                    ),
                                    legend=alt.Legend(title="재고 상태"),
                                ),
                                tooltip=[
                                    alt.Tooltip("product_code:N",   title="상품코드"),
                                    alt.Tooltip("product_name:N",   title="상품명"),
                                    alt.Tooltip("size_name:N",      title="사이즈"),
                                    alt.Tooltip("current_stock:Q",  title="현재고", format=",.0f"),
                                    alt.Tooltip("monthly_avg:Q",    title="월평균 출고수", format=",.1f"),
                                    alt.Tooltip("remaining_months:Q", title="잔여 개월", format=".1f"),
                                    alt.Tooltip("_status:N",        title="상태"),
                                ],
                            )
                            .properties(height=400)
                        )
                        text_rem = bar_rem.mark_text(
                            align="center", baseline="bottom", dy=-3, fontSize=10
                        ).encode(text=alt.Text("remaining_months:Q", format=".1f"))
                        st.altair_chart(bar_rem + text_rem, use_container_width=True)

                        if not no_out.empty:
                            st.caption(
                                f"※ 기간 내 출고 이력 없음 → 잔여 개월 계산 불가 제품: "
                                + ", ".join(no_out["product_code"].tolist())
                            )

                # ── 상세 테이블 ──
                with st.expander("전체 상세 테이블"):
                    tbl = avg_df[[
                        "product_code", "product_name", "size_name",
                        "current_stock", "period_out", "monthly_avg", "remaining_months",
                    ]].copy()
                    tbl.columns = [
                        "상품코드", "상품명", "사이즈",
                        "현재고", f"{n_months}개월 총출고", "월평균 출고수", "잔여 개월",
                    ]
                    tbl["월평균 출고수"] = tbl["월평균 출고수"].map(lambda x: f"{x:.1f}")
                    tbl["잔여 개월"] = tbl["잔여 개월"].map(
                        lambda x: f"{x:.1f}" if pd.notna(x) else "-"
                    )
                    tbl["현재고"] = tbl["현재고"].map(lambda x: f"{x:,.0f}")
                    tbl[f"{n_months}개월 총출고"] = tbl[f"{n_months}개월 총출고"].map(
                        lambda x: f"{x:,.0f}"
                    )
                    st.dataframe(tbl, use_container_width=True, hide_index=True)

    finally:
        conn.close()


if __name__ == "__main__":
    render()
