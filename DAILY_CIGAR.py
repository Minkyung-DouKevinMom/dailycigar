import os
import sqlite3
import pandas as pd
import streamlit as st
from db import get_table_count, table_exists

st.set_page_config(page_title="Daily Cigar DB", layout="wide")

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


# =========================
# 공통
# =========================
def fmt_krw(value) -> str:
    try:
        return f"₩{float(value):,.0f}"
    except Exception:
        return "₩0"


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_table_columns(conn, table_name: str) -> list[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [r[1] for r in rows]
    except Exception:
        return []


def pick_col(cols: list[str], candidates: list[str]):
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


def has_table(conn, table_name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def view_exists(conn, view_name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            (view_name,),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def get_non_cigar_purchase_price_map(conn) -> dict:
    """
    non_cigar_product_mst의 현재 purchase_price를 product_code 기준으로 매핑
    """
    if not has_table(conn, "non_cigar_product_mst"):
        return {}

    cols = get_table_columns(conn, "non_cigar_product_mst")
    if "product_code" not in cols or "purchase_price" not in cols:
        return {}

    sql = """
        SELECT
            TRIM(COALESCE(product_code, '')) AS product_code,
            COALESCE(purchase_price, 0) AS purchase_price
        FROM non_cigar_product_mst
    """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return {}

    df["product_code"] = df["product_code"].astype(str).str.strip()
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)

    df = df[df["product_code"] != ""].copy()
    return dict(zip(df["product_code"], df["purchase_price"]))


# =========================
# DB 다운로드
# =========================
def render_db_download_section():
    st.subheader("DB 다운로드")

    if not os.path.exists(DB_PATH):
        st.error(f"DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return

    try:
        with open(DB_PATH, "rb") as f:
            db_bytes = f.read()

        st.download_button(
            label="현재 DB 다운로드",
            data=db_bytes,
            file_name="cigar.db",
            mime="application/octet-stream",
            use_container_width=True,
        )
        st.caption("현재 사용 중인 cigar.db 파일을 바로 다운로드합니다.")
    except Exception as e:
        st.error(f"DB 다운로드 파일 준비 중 오류: {e}")


# =========================
# 매출 로딩
# =========================
def get_retail_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    purchase_price_map = get_non_cigar_purchase_price_map(conn)

    empty_cols = [
        "dt", "sales_amount", "margin_amount", "customer_name",
        "sales_type", "product_code", "product_name", "qty", "unit_price"
    ]

    # 1) 뷰 우선 시도
    if view_exists(conn, "v_retail_sales_enriched"):
        vcols = get_table_columns(conn, "v_retail_sales_enriched")

        sale_date_col = pick_col(vcols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(vcols, ["net_sales_amount", "sales_amount", "amount"])
        margin_col = pick_col(vcols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])

        if sale_date_col and sales_col:
            retail_cols = get_table_columns(conn, "retail_sales") if has_table(conn, "retail_sales") else []

            name_col = pick_col(
                retail_cols,
                ["customer_name", "customer", "customer_nm", "buyer_name", "store_name"],
            )
            name_expr = f"COALESCE(r.{name_col}, '')" if name_col else "''"

            product_code_expr = (
                "COALESCE(r.product_code, r.product_code_raw, '')"
                if "product_code" in retail_cols and "product_code_raw" in retail_cols
                else "COALESCE(r.product_code, '')"
                if "product_code" in retail_cols
                else "COALESCE(r.product_code_raw, '')"
                if "product_code_raw" in retail_cols
                else "''"
            )
            product_name_expr = "COALESCE(r.product_name, '')" if "product_name" in retail_cols else "''"
            qty_expr = "COALESCE(r.qty, 0)" if "qty" in retail_cols else "0"
            unit_price_expr = "COALESCE(r.unit_price, 0)" if "unit_price" in retail_cols else "0"
            margin_expr = f"COALESCE(v.{margin_col}, 0)" if margin_col else "0"

            if has_table(conn, "retail_sales"):
                sql = f"""
                    SELECT
                        v.{sale_date_col} AS sale_date,
                        COALESCE(v.{sales_col}, 0) AS sales_amount,
                        {margin_expr} AS margin_amount,
                        {name_expr} AS customer_name,
                        {product_code_expr} AS product_code,
                        {product_name_expr} AS product_name,
                        {qty_expr} AS qty,
                        {unit_price_expr} AS unit_price
                    FROM v_retail_sales_enriched v
                    LEFT JOIN retail_sales r
                      ON v.{sale_date_col} = r.sale_date
                     AND COALESCE(v.{sales_col}, 0) = COALESCE(r.net_sales_amount, 0)
                    WHERE v.{sale_date_col} BETWEEN ? AND ?
                """
            else:
                sql = f"""
                    SELECT
                        v.{sale_date_col} AS sale_date,
                        COALESCE(v.{sales_col}, 0) AS sales_amount,
                        {margin_expr} AS margin_amount,
                        '' AS customer_name,
                        '' AS product_code,
                        '' AS product_name,
                        0 AS qty,
                        0 AS unit_price
                    FROM v_retail_sales_enriched v
                    WHERE v.{sale_date_col} BETWEEN ? AND ?
                """

            try:
                df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        if not df.empty:
            df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
            df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
            df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
            df["customer_name"] = df["customer_name"].fillna("")
            df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
            df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
            df["sales_type"] = "소매"

            df = df.dropna(subset=["dt"])
            df = df[df["sales_amount"] != 0].copy()

            return df[
                ["dt", "sales_amount", "margin_amount", "customer_name",
                 "sales_type", "product_code", "product_name", "qty", "unit_price"]
            ]

    # 2) retail_sales fallback
    if has_table(conn, "retail_sales"):
        cols = get_table_columns(conn, "retail_sales")

        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(cols, ["net_sales_amount", "sales_amount", "amount"])

        if not sale_date_col or not sales_col:
            return pd.DataFrame(columns=empty_cols)

        name_col = pick_col(
            cols,
            ["customer_name", "customer", "customer_nm", "buyer_name", "store_name"],
        )
        name_expr = f"COALESCE({name_col}, '')" if name_col else "''"

        product_code_expr = (
            "COALESCE(product_code, product_code_raw, '')"
            if "product_code" in cols and "product_code_raw" in cols
            else "COALESCE(product_code, '')"
            if "product_code" in cols
            else "COALESCE(product_code_raw, '')"
            if "product_code_raw" in cols
            else "''"
        )
        product_name_expr = "COALESCE(product_name, '')" if "product_name" in cols else "''"
        qty_expr = "COALESCE(qty, 0)" if "qty" in cols else "0"
        unit_price_expr = "COALESCE(unit_price, 0)" if "unit_price" in cols else "0"

        sql = f"""
            SELECT
                {sale_date_col} AS sale_date,
                COALESCE({sales_col}, 0) AS sales_amount,
                {name_expr} AS customer_name,
                {product_code_expr} AS product_code,
                {product_name_expr} AS product_name,
                {qty_expr} AS qty,
                {unit_price_expr} AS unit_price
            FROM retail_sales
            WHERE {sale_date_col} BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

        if df.empty:
            return pd.DataFrame(columns=empty_cols)

        df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
        df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)

        df["purchase_price"] = df["product_code"].map(purchase_price_map).fillna(0)
        df["margin_amount"] = df["sales_amount"] - (df["purchase_price"] * df["qty"])

        df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
        df["customer_name"] = df["customer_name"].fillna("")
        df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
        df["sales_type"] = "소매"

        df = df.dropna(subset=["dt"])
        df = df[df["sales_amount"] != 0].copy()

        return df[
            ["dt", "sales_amount", "margin_amount", "customer_name",
             "sales_type", "product_code", "product_name", "qty", "unit_price"]
        ]

    return pd.DataFrame(columns=empty_cols)


def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    empty_cols = [
        "dt", "sales_amount", "margin_amount", "customer_name",
        "sales_type", "product_code", "product_name", "qty", "unit_price"
    ]

    if view_exists(conn, "v_wholesale_sales"):
        vcols = get_table_columns(conn, "v_wholesale_sales")

        sale_date_col = pick_col(vcols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(vcols, ["sales_amount", "net_sales_amount", "amount"])
        margin_col = pick_col(vcols, ["profit_amount", "gross_profit_krw", "margin_amount"])
        customer_col = pick_col(vcols, ["partner_name", "customer_name", "customer"])
        product_code_col = pick_col(vcols, ["product_code"])
        product_name_col = pick_col(vcols, ["product_name"])
        qty_col = pick_col(vcols, ["qty", "quantity"])
        unit_price_col = pick_col(vcols, ["unit_price"])

        if sale_date_col and sales_col:
            customer_expr = f"COALESCE({customer_col}, '')" if customer_col else "''"
            product_code_expr = f"COALESCE({product_code_col}, '')" if product_code_col else "''"
            product_name_expr = f"COALESCE({product_name_col}, '')" if product_name_col else "''"
            qty_expr = f"COALESCE({qty_col}, 0)" if qty_col else "0"
            unit_price_expr = f"COALESCE({unit_price_col}, 0)" if unit_price_col else "0"
            margin_expr = f"COALESCE({margin_col}, 0)" if margin_col else "0"

            sql = f"""
                SELECT
                    {sale_date_col} AS sale_date,
                    COALESCE({sales_col}, 0) AS sales_amount,
                    {margin_expr} AS margin_amount,
                    {customer_expr} AS customer_name,
                    {product_code_expr} AS product_code,
                    {product_name_expr} AS product_name,
                    {qty_expr} AS qty,
                    {unit_price_expr} AS unit_price
                FROM v_wholesale_sales
                WHERE {sale_date_col} BETWEEN ? AND ?
            """
            try:
                df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        if not df.empty:
            df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
            df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
            df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
            df["customer_name"] = df["customer_name"].fillna("")
            df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
            df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
            df["sales_type"] = "도매"

            df = df.dropna(subset=["dt"])
            df = df[df["sales_amount"] != 0].copy()

            return df[
                ["dt", "sales_amount", "margin_amount", "customer_name",
                 "sales_type", "product_code", "product_name", "qty", "unit_price"]
            ]

    if has_table(conn, "wholesale_sales"):
        cols = get_table_columns(conn, "wholesale_sales")

        sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(cols, ["sales_amount", "net_sales_amount", "amount"])

        if not sale_date_col or not sales_col:
            return pd.DataFrame(columns=empty_cols)

        customer_col = pick_col(cols, ["partner_name", "customer_name", "customer"])
        product_code_col = pick_col(cols, ["product_code"])
        product_name_col = pick_col(cols, ["product_name"])
        qty_col = pick_col(cols, ["qty", "quantity"])
        unit_price_col = pick_col(cols, ["unit_price"])
        margin_col = pick_col(cols, ["profit_amount", "margin_amount"])

        customer_expr = f"COALESCE({customer_col}, '')" if customer_col else "''"
        product_code_expr = f"COALESCE({product_code_col}, '')" if product_code_col else "''"
        product_name_expr = f"COALESCE({product_name_col}, '')" if product_name_col else "''"
        qty_expr = f"COALESCE({qty_col}, 0)" if qty_col else "0"
        unit_price_expr = f"COALESCE({unit_price_col}, 0)" if unit_price_col else "0"
        margin_expr = f"COALESCE({margin_col}, 0)" if margin_col else "0"

        sql = f"""
            SELECT
                {sale_date_col} AS sale_date,
                COALESCE({sales_col}, 0) AS sales_amount,
                {margin_expr} AS margin_amount,
                {customer_expr} AS customer_name,
                {product_code_expr} AS product_code,
                {product_name_expr} AS product_name,
                {qty_expr} AS qty,
                {unit_price_expr} AS unit_price
            FROM wholesale_sales
            WHERE {sale_date_col} BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

        if df.empty:
            return pd.DataFrame(columns=empty_cols)

        df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
        df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
        df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
        df["customer_name"] = df["customer_name"].fillna("")
        df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
        df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
        df["sales_type"] = "도매"

        df = df.dropna(subset=["dt"])
        df = df[df["sales_amount"] != 0].copy()

        return df[
            ["dt", "sales_amount", "margin_amount", "customer_name",
             "sales_type", "product_code", "product_name", "qty", "unit_price"]
        ]

    return pd.DataFrame(columns=empty_cols)


def load_period_sales(conn, date_from: str, date_to: str) -> pd.DataFrame:
    frames = []

    retail_df = get_retail_month_data(conn, date_from, date_to)
    wholesale_df = get_wholesale_month_data(conn, date_from, date_to)

    if not retail_df.empty:
        frames.append(retail_df)
    if not wholesale_df.empty:
        frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "dt", "sales_amount", "margin_amount", "customer_name",
                "sales_type", "product_code", "product_name", "qty", "unit_price"
            ]
        )

    return pd.concat(frames, ignore_index=True)


# =========================
# 인사이트
# =========================
def calc_insights(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["매출 데이터가 아직 없습니다."]

    today = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)

    prev_month_end = month_start - pd.Timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    this_month = df[(df["dt"] >= month_start) & (df["dt"] <= today)]
    prev_month = df[(df["dt"] >= prev_month_start) & (df["dt"] <= prev_month_end)]

    messages = []

    this_sales = this_month["sales_amount"].sum()
    prev_sales = prev_month["sales_amount"].sum()

    if prev_sales > 0:
        diff_pct = (this_sales - prev_sales) / prev_sales * 100
        direction = "증가" if diff_pct >= 0 else "감소"
        messages.append(f"이번달 매출은 전월 대비 {abs(diff_pct):.1f}% {direction}했습니다.")
    else:
        messages.append("전월 비교를 위한 데이터가 아직 충분하지 않습니다.")

    if this_sales > 0:
        this_margin = this_month["margin_amount"].sum()
        margin_rate = (this_margin / this_sales * 100) if this_sales else 0
        messages.append(f"이번달 마진율은 {margin_rate:.1f}%입니다.")

    wholesale_sales = this_month.loc[this_month["sales_type"] == "도매", "sales_amount"].sum()
    wholesale_ratio = (wholesale_sales / this_sales * 100) if this_sales else 0
    retail_ratio = 100 - wholesale_ratio if this_sales else 0
    messages.append(f"이번달 매출 비중은 소매 {retail_ratio:.1f}% / 도매 {wholesale_ratio:.1f}%입니다.")

    return messages[:3]


# =========================
# 화면
# =========================
st.title("Daily Cigar 운영 관리 시스템")

conn = get_conn()

try:
    today = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    last_30_start = today - pd.Timedelta(days=29)

    month_df = load_period_sales(
        conn,
        month_start.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )

    last_30_df = load_period_sales(
        conn,
        last_30_start.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )

    sales_df = load_period_sales(
        conn,
        (today - pd.Timedelta(days=90)).strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )

    month_sales = month_df["sales_amount"].sum() if not month_df.empty else 0
    month_margin = month_df["margin_amount"].sum() if not month_df.empty else 0
    deal_count = len(month_df)
    avg_ticket = month_sales / deal_count if deal_count > 0 else 0

    wholesale_sales = month_df.loc[month_df["sales_type"] == "도매", "sales_amount"].sum() if not month_df.empty else 0
    retail_sales = month_df.loc[month_df["sales_type"] == "소매", "sales_amount"].sum() if not month_df.empty else 0

    wholesale_ratio = (wholesale_sales / month_sales * 100) if month_sales else 0
    retail_ratio = (retail_sales / month_sales * 100) if month_sales else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("이번달 매출", fmt_krw(month_sales))
    k2.metric("이번달 마진", fmt_krw(month_margin))
    k3.metric("거래건수", f"{deal_count:,}건")
    k4.metric("객단가", fmt_krw(avg_ticket))

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("최근 30일 매출 추이")

        if last_30_df.empty:
            st.info("표시할 매출 데이터가 없습니다.")
        else:
            daily_sales = (
                last_30_df.assign(date=last_30_df["dt"].dt.normalize())
                .groupby("date", as_index=True)["sales_amount"]
                .sum()
                .sort_index()
                .to_frame("매출액")
            )
            st.line_chart(daily_sales, use_container_width=True)

    with right:
        st.subheader("이번달 채널 비중")
        r1, r2 = st.columns(2)
        r1.metric("소매", f"{retail_ratio:.1f}%")
        r2.metric("도매", f"{wholesale_ratio:.1f}%")
        st.caption(f"소매 {fmt_krw(retail_sales)} / 도매 {fmt_krw(wholesale_sales)}")

    st.divider()

    st.subheader("인사이트")
    for msg in calc_insights(sales_df):
        st.write(f"- {msg}")

    st.divider()

    st.subheader("최근 판매 내역")
    if sales_df.empty:
        st.info("최근 판매 데이터가 없습니다.")
    else:
        recent_df = sales_df.sort_values("dt", ascending=False).head(15).copy()
        recent_df["dt"] = recent_df["dt"].dt.strftime("%Y-%m-%d")

        recent_df = recent_df.rename(
            columns={
                "dt": "일자",
                "sales_type": "구분",
                "customer_name": "거래처/고객",
                "product_code": "상품코드",
                "product_name": "상품명",
                "qty": "수량",
                "unit_price": "단가",
                "sales_amount": "매출액",
                "margin_amount": "마진",
            }
        )

        for col in ["단가", "매출액", "마진"]:
            recent_df[col] = recent_df[col].apply(fmt_krw)

        st.dataframe(
            recent_df[["일자", "구분", "거래처/고객", "상품코드", "상품명", "수량", "단가", "매출액", "마진"]],
            use_container_width=True,
            hide_index=True,
        )
        st.caption("※ 소매 시가 외 상품 마진은 현재 상품 마스터의 매입가(purchase_price) 기준으로 계산됩니다.")

    st.divider()

    render_db_download_section()

    st.divider()

    with st.expander("DB 상태 보기"):
        tables = [
            "product_mst",
            "import_batch",
            "import_item",
            "tax_rule",
            "export_price_item",
            "blend_profile_mst",
            "retail_sales",
            "wholesale_sales",
        ]

        cols = st.columns(4)
        for idx, table_name in enumerate(tables):
            with cols[idx % 4]:
                exists = table_exists(table_name)
                count = get_table_count(table_name) if exists else 0
                st.metric(
                    label=table_name,
                    value=count,
                    delta="OK" if exists else "없음",
                )

    with st.expander("매출 로딩 디버그"):
        try:
            if has_table(conn, "retail_sales"):
                st.write("retail_sales 컬럼:", get_table_columns(conn, "retail_sales"))
            else:
                st.write("retail_sales 테이블 없음")

            if has_table(conn, "wholesale_sales"):
                st.write("wholesale_sales 컬럼:", get_table_columns(conn, "wholesale_sales"))
            else:
                st.write("wholesale_sales 테이블 없음")

            if has_table(conn, "non_cigar_product_mst"):
                st.write("non_cigar_product_mst 컬럼:", get_table_columns(conn, "non_cigar_product_mst"))
            else:
                st.write("non_cigar_product_mst 테이블 없음")

            if view_exists(conn, "v_retail_sales_enriched"):
                st.write("v_retail_sales_enriched 컬럼:", get_table_columns(conn, "v_retail_sales_enriched"))
            else:
                st.write("v_retail_sales_enriched 뷰 없음")

            if view_exists(conn, "v_wholesale_sales"):
                st.write("v_wholesale_sales 컬럼:", get_table_columns(conn, "v_wholesale_sales"))
            else:
                st.write("v_wholesale_sales 뷰 없음")

            st.write("전체 로딩 건수:", len(sales_df))

            if not sales_df.empty:
                st.write("구분별 건수")
                st.dataframe(
                    sales_df.groupby("sales_type").size().reset_index(name="건수"),
                    use_container_width=True,
                    hide_index=True,
                )

                st.write("최근 판매 샘플")
                st.dataframe(
                    sales_df.head(20),
                    use_container_width=True,
                    hide_index=True,
                )

        except Exception as e:
            st.warning(f"디버그 정보 조회 중 오류: {e}")

    st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

finally:
    conn.close()