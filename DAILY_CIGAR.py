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


def fmt_count(value) -> str:
    try:
        return f"{int(value):,}건"
    except Exception:
        return "0건"


def metric_with_caption(column, label: str, value: str, caption: str):
    column.metric(label, value)
    column.caption(caption)


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


def get_product_name_map(conn) -> dict:
    result = {}

    if has_table(conn, "product_mst"):
        cols = get_table_columns(conn, "product_mst")
        if "product_code" in cols:
            name_col = pick_col(cols, ["product_name", "product_nm", "name"])
            if name_col:
                sql = f"""
                    SELECT
                        TRIM(COALESCE(product_code, '')) AS product_code,
                        TRIM(COALESCE({name_col}, '')) AS product_name
                    FROM product_mst
                """
                df = pd.read_sql_query(sql, conn)
                if not df.empty:
                    df["product_code"] = df["product_code"].astype(str).str.strip()
                    df["product_name"] = df["product_name"].astype(str).str.strip()
                    df = df[(df["product_code"] != "") & (df["product_name"] != "")]
                    result.update(dict(zip(df["product_code"], df["product_name"])))

    if has_table(conn, "non_cigar_product_mst"):
        cols = get_table_columns(conn, "non_cigar_product_mst")
        if "product_code" in cols:
            name_col = pick_col(cols, ["product_name", "product_nm", "name"])
            if name_col:
                sql = f"""
                    SELECT
                        TRIM(COALESCE(product_code, '')) AS product_code,
                        TRIM(COALESCE({name_col}, '')) AS product_name
                    FROM non_cigar_product_mst
                """
                df = pd.read_sql_query(sql, conn)
                if not df.empty:
                    df["product_code"] = df["product_code"].astype(str).str.strip()
                    df["product_name"] = df["product_name"].astype(str).str.strip()
                    df = df[(df["product_code"] != "") & (df["product_name"] != "")]
                    for code, name in zip(df["product_code"], df["product_name"]):
                        if code not in result:
                            result[code] = name

    return result


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
    empty_cols = [
        "dt", "sales_amount", "margin_amount", "customer_name",
        "sales_type", "product_code", "product_name", "qty", "unit_price"
    ]

    purchase_price_map = get_non_cigar_purchase_price_map(conn)
    product_name_map = get_product_name_map(conn)

    if view_exists(conn, "v_retail_sales_enriched"):
        vcols = get_table_columns(conn, "v_retail_sales_enriched")

        sale_date_col = pick_col(vcols, ["sale_date", "sales_date", "dt"])
        sales_col = pick_col(vcols, ["net_sales_amount", "sales_amount", "amount"])
        cost_col = pick_col(vcols, ["total_korea_cost_krw", "total_cost_krw"])
        gp_col = pick_col(vcols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])
        product_code_col = pick_col(vcols, ["product_code", "product_code_raw"])
        product_name_col = pick_col(vcols, ["mst_product_name", "product_name", "product_code_raw"])
        qty_col = pick_col(vcols, ["qty", "quantity"])
        unit_price_col = pick_col(vcols, ["unit_price"])
        name_col = pick_col(vcols, ["customer_name", "customer", "customer_nm", "buyer_name", "store_name"])

        if sale_date_col and sales_col:
            sql = f"""
                SELECT
                    {sale_date_col} AS sale_date,
                    COALESCE({sales_col}, 0) AS sales_amount,
                    {"COALESCE(" + cost_col + ", 0)" if cost_col else "0"} AS total_korea_cost_krw,
                    {"COALESCE(" + gp_col + ", 0)" if gp_col else "0"} AS margin_amount,
                    {"COALESCE(" + product_code_col + ", '')" if product_code_col else "''"} AS product_code,
                    {"COALESCE(" + product_name_col + ", '')" if product_name_col else "''"} AS product_name,
                    {"COALESCE(" + qty_col + ", 0)" if qty_col else "0"} AS qty,
                    {"COALESCE(" + unit_price_col + ", 0)" if unit_price_col else "0"} AS unit_price,
                    {"COALESCE(" + name_col + ", '')" if name_col else "''"} AS customer_name
                FROM v_retail_sales_enriched
                WHERE {sale_date_col} BETWEEN ? AND ?
            """
            try:
                df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
            except Exception:
                df = pd.DataFrame()

            if not df.empty:
                df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
                for c in ["sales_amount", "total_korea_cost_krw", "margin_amount", "qty", "unit_price"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                df["customer_name"] = df["customer_name"].fillna("")
                df["product_code"] = df["product_code"].fillna("").astype(str).str.strip()
                df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()

                missing_name_mask = df["product_name"].eq("") & df["product_code"].ne("")
                df.loc[missing_name_mask, "product_name"] = (
                    df.loc[missing_name_mask, "product_code"].map(product_name_map).fillna("")
                )

                non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())
                df.loc[non_cigar_mask, "_purchase_price"] = (
                    df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
                )
                df.loc[non_cigar_mask, "total_korea_cost_krw"] = (
                    df.loc[non_cigar_mask, "_purchase_price"] * df.loc[non_cigar_mask, "qty"]
                )
                df.loc[non_cigar_mask, "margin_amount"] = (
                    df.loc[non_cigar_mask, "sales_amount"] - df.loc[non_cigar_mask, "total_korea_cost_krw"]
                )

                if "_purchase_price" in df.columns:
                    df = df.drop(columns=["_purchase_price"])

                df["sales_type"] = "소매"
                df = df.dropna(subset=["dt"])
                df = df[df["sales_amount"] != 0].copy()

                return df[
                    ["dt", "sales_amount", "margin_amount", "customer_name",
                     "sales_type", "product_code", "product_name", "qty", "unit_price"]
                ]

    if not has_table(conn, "retail_sales"):
        return pd.DataFrame(columns=empty_cols)

    cols = get_table_columns(conn, "retail_sales")

    sale_date_col = pick_col(cols, ["sale_date", "sales_date", "dt"])
    sales_col = pick_col(cols, ["net_sales_amount", "sales_amount", "amount"])
    name_col = pick_col(cols, ["customer_name", "customer", "customer_nm", "buyer_name", "store_name"])
    product_code_col = pick_col(cols, ["product_code", "product_code_raw"])
    product_name_col = pick_col(cols, ["product_name"])
    qty_col = pick_col(cols, ["qty", "quantity"])
    unit_price_col = pick_col(cols, ["unit_price"])
    gp_col = pick_col(cols, ["retail_gross_profit_krw", "gross_profit_krw", "margin_amount"])

    if not sale_date_col or not sales_col:
        return pd.DataFrame(columns=empty_cols)

    sql = f"""
        SELECT
            {sale_date_col} AS sale_date,
            COALESCE({sales_col}, 0) AS sales_amount,
            {"COALESCE(" + gp_col + ", 0)" if gp_col else "0"} AS margin_amount,
            {"COALESCE(" + name_col + ", '')" if name_col else "''"} AS customer_name,
            {"COALESCE(" + product_code_col + ", '')" if product_code_col else "''"} AS product_code,
            {"COALESCE(" + product_name_col + ", '')" if product_name_col else "''"} AS product_name,
            {"COALESCE(" + qty_col + ", 0)" if qty_col else "0"} AS qty,
            {"COALESCE(" + unit_price_col + ", 0)" if unit_price_col else "0"} AS unit_price
        FROM retail_sales
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

    missing_name_mask = df["product_name"].eq("") & df["product_code"].ne("")
    df.loc[missing_name_mask, "product_name"] = (
        df.loc[missing_name_mask, "product_code"].map(product_name_map).fillna("")
    )

    non_cigar_mask = df["product_code"].isin(purchase_price_map.keys())
    df.loc[non_cigar_mask, "_purchase_price"] = (
        df.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
    )
    df.loc[non_cigar_mask, "margin_amount"] = (
        df.loc[non_cigar_mask, "sales_amount"]
        - (df.loc[non_cigar_mask, "_purchase_price"] * df.loc[non_cigar_mask, "qty"])
    )

    if "_purchase_price" in df.columns:
        df = df.drop(columns=["_purchase_price"])

    df["sales_type"] = "소매"
    df = df.dropna(subset=["dt"])
    df = df[df["sales_amount"] != 0].copy()

    return df[
        ["dt", "sales_amount", "margin_amount", "customer_name",
         "sales_type", "product_code", "product_name", "qty", "unit_price"]
    ]

def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    empty_cols = [
        "dt", "sales_amount", "margin_amount", "customer_name",
        "sales_type", "product_code", "product_name", "qty", "unit_price"
    ]
    product_name_map = get_product_name_map(conn)

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
            missing_name_mask = df["product_name"].eq("") & df["product_code"].ne("")
            df.loc[missing_name_mask, "product_name"] = df.loc[missing_name_mask, "product_code"].map(product_name_map).fillna("")
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
        missing_name_mask = df["product_name"].eq("") & df["product_code"].ne("")
        df.loc[missing_name_mask, "product_name"] = df.loc[missing_name_mask, "product_code"].map(product_name_map).fillna("")
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

with st.sidebar:
    st.markdown("## DAILY CIGAR")
    st.page_link("DAILY_CIGAR.py", label="HOME")
    st.page_link("pages/1_대시보드.py", label="대시보드⭐")
    st.page_link("pages/2_기준정보.py", label="기준정보")
    st.divider()
    st.page_link("pages/3_수입관리.py", label="수입관리")
    st.page_link("pages/4_판매관리.py", label="판매관리")
    st.page_link("pages/5_재무관리.py", label="재무관리")
    st.page_link("pages/6_분석.py", label="분석")
    st.divider()
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    st.page_link("pages/9_재고관리.py", label="재고관리📦")  # ← 추가

conn = get_conn()

try:
    today = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    last_30_start = today - pd.Timedelta(days=29)
    recent_period_start = today - pd.Timedelta(days=90)

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
        recent_period_start.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )

    card_df = last_30_df.copy()
    st.caption(f"계산기간: {last_30_start.strftime('%Y-%m-%d')}~{today.strftime('%Y-%m-%d')}")

    card_sales = card_df["sales_amount"].sum() if not card_df.empty else 0
    card_margin = card_df["margin_amount"].sum() if not card_df.empty else 0
    deal_count = len(card_df)
    avg_ticket = card_sales / deal_count if deal_count > 0 else 0

    wholesale_sales = card_df.loc[card_df["sales_type"] == "도매", "sales_amount"].sum() if not card_df.empty else 0
    retail_sales = card_df.loc[card_df["sales_type"] == "소매", "sales_amount"].sum() if not card_df.empty else 0

    wholesale_margin = card_df.loc[card_df["sales_type"] == "도매", "margin_amount"].sum() if not card_df.empty else 0
    retail_margin = card_df.loc[card_df["sales_type"] == "소매", "margin_amount"].sum() if not card_df.empty else 0

    wholesale_count = int((card_df["sales_type"] == "도매").sum()) if not card_df.empty else 0
    retail_count = int((card_df["sales_type"] == "소매").sum()) if not card_df.empty else 0

    wholesale_ratio = (wholesale_sales / card_sales * 100) if card_sales else 0
    retail_ratio = (retail_sales / card_sales * 100) if card_sales else 0

    k1, k2, k3, k4 = st.columns(4)
    metric_with_caption(
        k1,
        "최근 30일 매출",
        fmt_krw(card_sales),
        f"소매: {fmt_krw(retail_sales)}, 도매: {fmt_krw(wholesale_sales)}",
    )
    metric_with_caption(
        k2,
        "최근 30일 마진",
        fmt_krw(card_margin),
        f"소매: {fmt_krw(retail_margin)}, 도매: {fmt_krw(wholesale_margin)}",
    )
    metric_with_caption(
        k3,
        "거래건수",
        fmt_count(deal_count),
        f"소매: {retail_count:,}건, 도매: {wholesale_count:,}건",
    )
    metric_with_caption(
        k4,
        "객단가",
        fmt_krw(avg_ticket),
        f"최근 30일 매출 ÷ 거래건수",
    )

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("최근 30일 매출 추이")

        if last_30_df.empty:
            st.info("표시할 매출 데이터가 없습니다.")
        else:
            daily_sales = (
                last_30_df.assign(date=last_30_df["dt"].dt.normalize())
                .pivot_table(
                    index="date",
                    columns="sales_type",
                    values="sales_amount",
                    aggfunc="sum",
                    fill_value=0,
                )
                .sort_index()
            )

            date_index = pd.date_range(
                start=pd.Timestamp(last_30_start).normalize(),
                end=pd.Timestamp(today).normalize(),
                freq="D",
            )
            daily_sales = daily_sales.reindex(date_index, fill_value=0)
            daily_sales.index.name = "date"

            for col in ["소매", "도매"]:
                if col not in daily_sales.columns:
                    daily_sales[col] = 0
            daily_sales = daily_sales[["소매", "도매"]]

            retail_avg = daily_sales["소매"].mean()
            wholesale_avg = daily_sales["도매"].mean()
            daily_sales["소매평균선"] = retail_avg
            daily_sales["도매평균선"] = wholesale_avg

            st.line_chart(
                daily_sales[["소매", "도매", "소매평균선", "도매평균선"]],
                use_container_width=True,
            )
            st.caption(
                f"계산기간: {last_30_start.strftime('%Y-%m-%d')}~{today.strftime('%Y-%m-%d')} | "
                f"소매 일평균: {fmt_krw(retail_avg)} | 도매 일평균: {fmt_krw(wholesale_avg)}"
            )

    with right:
        st.subheader("최근 30일 채널 비중")
        r1, r2 = st.columns(2)
        r1.metric("소매", f"{retail_ratio:.1f}%")
        r2.metric("도매", f"{wholesale_ratio:.1f}%")
        st.caption(f"소매 {fmt_krw(retail_sales)} / 도매 {fmt_krw(wholesale_sales)}")

    st.divider()

    st.subheader("인사이트")
    for msg in calc_insights(sales_df):
        st.write(f"- {msg}")
    st.caption(f"계산기간: {recent_period_start.strftime('%Y-%m-%d')}~{today.strftime('%Y-%m-%d')}")

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

        recent_df["상품명"] = recent_df["상품명"].fillna("").astype(str).str.strip()
        recent_df.loc[recent_df["상품명"].eq(""), "상품명"] = "-"

        for col in ["단가", "매출액", "마진"]:
            recent_df[col] = recent_df[col].apply(fmt_krw)

        st.dataframe(
            recent_df[["일자", "구분", "거래처/고객", "상품코드", "상품명", "수량", "단가", "매출액", "마진"]],
            use_container_width=True,
            hide_index=True,
        )

        st.caption("※ 소매 시가 외 상품 마진은 판매금액 - (매입가 × 수량) 기준으로 계산합니다.")
        st.caption(f"계산기간: {recent_period_start.strftime('%Y-%m-%d')}~{today.strftime('%Y-%m-%d')}")

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

    st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

finally:
    conn.close()
