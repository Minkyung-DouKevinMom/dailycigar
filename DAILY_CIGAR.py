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

    # 최근 30일: today-29 ~ today
    recent_start = today - pd.Timedelta(days=29)
    # 이전 30일: today-59 ~ today-30
    prior_end = today - pd.Timedelta(days=30)
    prior_start = today - pd.Timedelta(days=59)

    recent = df[(df["dt"] >= recent_start) & (df["dt"] <= today)]
    prior  = df[(df["dt"] >= prior_start)  & (df["dt"] <= prior_end)]

    messages = []

    recent_sales = recent["sales_amount"].sum()
    prior_sales  = prior["sales_amount"].sum()

    if prior_sales > 0:
        diff_pct = (recent_sales - prior_sales) / prior_sales * 100
        direction = "증가" if diff_pct >= 0 else "감소"
        messages.append(
            f"최근 30일 매출({recent_start.strftime('%m/%d')}\u2013{today.strftime('%m/%d')})은 "
            f"이전 30일({prior_start.strftime('%m/%d')}\u2013{prior_end.strftime('%m/%d')}) 대비 "
            f"{abs(diff_pct):.1f}% {direction}했습니다. "
            f"({fmt_krw(prior_sales)} → {fmt_krw(recent_sales)})"
        )
    else:
        messages.append("이전 30일 비교를 위한 데이터가 아직 충분하지 않습니다.")

    if recent_sales > 0:
        recent_margin = recent["margin_amount"].sum()
        margin_rate = (recent_margin / recent_sales * 100) if recent_sales else 0
        messages.append(f"최근 30일 마진율은 {margin_rate:.1f}%입니다.")

    wholesale_sales = recent.loc[recent["sales_type"] == "도매", "sales_amount"].sum()
    wholesale_ratio = (wholesale_sales / recent_sales * 100) if recent_sales else 0
    retail_ratio = 100 - wholesale_ratio if recent_sales else 0
    messages.append(f"최근 30일 매출 비중은 소매 {retail_ratio:.1f}% / 도매 {wholesale_ratio:.1f}%입니다.")

    return messages[:3]


# =========================
# 할 일 / 공지사항 관리
# =========================
STATUS_OPTIONS = ["대기", "진행중", "완료", "보류"]
STATUS_EMOJI = {"대기": "🔵", "진행중": "🟡", "완료": "✅", "보류": "⚫"}
PRIORITY_OPTIONS = ["낮음", "보통", "높음", "긴급"]
PRIORITY_EMOJI = {"낮음": "🟢", "보통": "🔵", "높음": "🟠", "긴급": "🔴"}


def init_tasks_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT    NOT NULL,
            assignee    TEXT    DEFAULT '',
            due_date    TEXT    DEFAULT '',
            status      TEXT    DEFAULT '대기',
            priority    TEXT    DEFAULT '보통',
            note        TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT (DATE('now','localtime')),
            updated_at  TEXT    DEFAULT (DATE('now','localtime'))
        )
    """)
    conn.commit()


def get_tasks(conn) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            "SELECT * FROM tasks ORDER BY CASE status WHEN '진행중' THEN 1 WHEN '대기' THEN 2 WHEN '보류' THEN 3 ELSE 4 END, due_date ASC",
            conn,
        )
    except Exception:
        df = pd.DataFrame()
    return df


def add_task(conn, title, assignee, due_date, status, priority, note):
    conn.execute(
        """INSERT INTO tasks (title, assignee, due_date, status, priority, note, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, DATE('now','localtime'))""",
        (title, assignee, due_date, status, priority, note),
    )
    conn.commit()


def update_task(conn, task_id, title, assignee, due_date, status, priority, note):
    conn.execute(
        """UPDATE tasks
           SET title=?, assignee=?, due_date=?, status=?, priority=?, note=?,
               updated_at=DATE('now','localtime')
           WHERE id=?""",
        (title, assignee, due_date, status, priority, note, task_id),
    )
    conn.commit()


def delete_task(conn, task_id):
    conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
    conn.commit()


def render_tasks_section(conn):
    init_tasks_table(conn)

    st.subheader("📋 할 일 / 공지사항")

    tasks_df = get_tasks(conn)

    # ── 현황 요약 카드 ──
    if not tasks_df.empty:
        total = len(tasks_df)
        in_progress = int((tasks_df["status"] == "진행중").sum())
        waiting = int((tasks_df["status"] == "대기").sum())
        done = int((tasks_df["status"] == "완료").sum())
        urgent = int((tasks_df["priority"] == "긴급").sum())

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("전체", f"{total}건")
        sc2.metric("🟡 진행중", f"{in_progress}건")
        sc3.metric("🔵 대기", f"{waiting}건")
        sc4.metric("✅ 완료", f"{done}건")
        sc5.metric("🔴 긴급", f"{urgent}건")
        st.divider()

    # ── 필터 ──
    col_f1, col_f2, col_f3 = st.columns([2, 2, 2])
    with col_f1:
        filter_status = st.multiselect(
            "상태 필터",
            STATUS_OPTIONS,
            default=["대기", "진행중", "보류"],
            key="task_filter_status",
        )
    with col_f2:
        filter_priority = st.multiselect(
            "우선순위 필터",
            PRIORITY_OPTIONS,
            default=PRIORITY_OPTIONS,
            key="task_filter_priority",
        )
    with col_f3:
        filter_assignee = st.text_input("담당자 검색", key="task_filter_assignee")

    # ── 할 일 목록 ──
    if tasks_df.empty:
        st.info("등록된 할 일이 없습니다. 아래에서 새 항목을 추가해보세요.")
    else:
        view_df = tasks_df.copy()
        if filter_status:
            view_df = view_df[view_df["status"].isin(filter_status)]
        if filter_priority:
            view_df = view_df[view_df["priority"].isin(filter_priority)]
        if filter_assignee.strip():
            view_df = view_df[view_df["assignee"].str.contains(filter_assignee.strip(), na=False)]

        if view_df.empty:
            st.info("필터 조건에 해당하는 항목이 없습니다.")
        else:
            for _, row in view_df.iterrows():
                task_id = int(row["id"])
                s_emoji = STATUS_EMOJI.get(row["status"], "")
                p_emoji = PRIORITY_EMOJI.get(row["priority"], "")
                due_str = f"📅 {row['due_date']}" if row["due_date"] else ""
                assignee_str = f"👤 {row['assignee']}" if row["assignee"] else ""

                with st.expander(
                    f"{p_emoji} {s_emoji} [{row['priority']}] {row['title']}　　{assignee_str}　{due_str}",
                    expanded=False,
                ):
                    e1, e2, e3, e4, e5 = st.columns([3, 2, 2, 2, 1])
                    new_title = e1.text_input("제목", value=row["title"], key=f"t_{task_id}")
                    new_assignee = e2.text_input("담당자", value=row["assignee"] or "", key=f"a_{task_id}")
                    new_due = e3.text_input("납기 (YYYY-MM-DD)", value=row["due_date"] or "", key=f"d_{task_id}")
                    new_status = e4.selectbox(
                        "현재 상태",
                        STATUS_OPTIONS,
                        index=STATUS_OPTIONS.index(row["status"]) if row["status"] in STATUS_OPTIONS else 0,
                        key=f"s_{task_id}",
                    )
                    new_priority = e5.selectbox(
                        "우선순위",
                        PRIORITY_OPTIONS,
                        index=PRIORITY_OPTIONS.index(row["priority"]) if row["priority"] in PRIORITY_OPTIONS else 1,
                        key=f"p_{task_id}",
                    )
                    new_note = st.text_area("비고", value=row["note"] or "", key=f"n_{task_id}", height=80)

                    btn1, btn2, _ = st.columns([1, 1, 6])
                    if btn1.button("💾 저장", key=f"save_{task_id}"):
                        update_task(conn, task_id, new_title, new_assignee, new_due, new_status, new_priority, new_note)
                        st.success("저장되었습니다.")
                        st.rerun()
                    if btn2.button("🗑️ 삭제", key=f"del_{task_id}"):
                        delete_task(conn, task_id)
                        st.success("삭제되었습니다.")
                        st.rerun()

                    st.caption(f"등록일: {row.get('created_at', '')}　최종수정: {row.get('updated_at', '')}")

    st.divider()

    # ── 새 항목 추가 ──
    with st.expander("➕ 새 할 일 / 공지사항 추가", expanded=False):
        n1, n2, n3, n4, n5 = st.columns([3, 2, 2, 2, 1])
        new_title_in = n1.text_input("제목 *", key="new_task_title")
        new_assignee_in = n2.text_input("담당자", key="new_task_assignee")
        new_due_in = n3.text_input("납기 (YYYY-MM-DD)", key="new_task_due")
        new_status_in = n4.selectbox("현재 상태", STATUS_OPTIONS, key="new_task_status")
        new_priority_in = n5.selectbox("우선순위", PRIORITY_OPTIONS, index=1, key="new_task_priority")
        new_note_in = st.text_area("비고", key="new_task_note", height=80)

        if st.button("✅ 등록", use_container_width=False, key="add_task_btn"):
            if new_title_in.strip():
                add_task(conn, new_title_in.strip(), new_assignee_in, new_due_in, new_status_in, new_priority_in, new_note_in)
                st.success("등록되었습니다.")
                st.rerun()
            else:
                st.warning("제목을 입력해주세요.")


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
    st.page_link("pages/9_재고관리.py", label="재고관리📦")

conn = get_conn()

try:
    today = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    last_30_start = today - pd.Timedelta(days=29)
    insight_period_start = today - pd.Timedelta(days=59)  # 최근 30일 + 이전 30일

    last_30_df = load_period_sales(
        conn,
        last_30_start.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )

    sales_df = load_period_sales(
        conn,
        insight_period_start.strftime("%Y-%m-%d"),
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
        st.text(f"• {msg}")
    st.caption(f"비교기간: 최근 30일 vs 이전 30일  |  {insight_period_start.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')}")

    st.divider()

    # ── 할 일 / 공지사항 관리 ──────────────────────────────────────
    render_tasks_section(conn)
    
    st.divider()

    render_db_download_section()

    st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

finally:
    conn.close()