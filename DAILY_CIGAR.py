import os
import shutil
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from db import get_table_count, table_exists

st.set_page_config(page_title="Daily Cigar DB", layout="wide")

DB_PATH = "cigar.db"


# =========================
# DB 연결
# =========================
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
# 매출 로딩 (대시보드와 동일 기준)
# =========================
def view_exists(conn, view_name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            (view_name,),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def month_range(year: int, month: int) -> tuple[str, str]:
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def get_retail_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_retail_sales_enriched"):
        sql = """
            SELECT
                sale_date,
                COALESCE(net_sales_amount, 0) AS sales_amount,
                COALESCE(retail_gross_profit_krw, 0) AS margin_amount,
                '' AS customer_name
            FROM v_retail_sales_enriched
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    elif has_table(conn, "retail_sales"):
        cols = get_table_columns(conn, "retail_sales")
        name_col = pick_col(
            cols,
            ["customer_name", "customer", "customer_nm", "buyer_name", "store_name"]
        )
        name_expr = f"COALESCE({name_col}, '')" if name_col else "''"

        sql = f"""
            SELECT
                sale_date,
                COALESCE(net_sales_amount, 0) AS sales_amount,
                0 AS margin_amount,
                {name_expr} AS customer_name
            FROM retail_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    else:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    if df.empty:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
    df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
    df["customer_name"] = df["customer_name"].fillna("")
    df["sales_type"] = "소매"
    df = df.dropna(subset=["dt"])
    df = df[df["sales_amount"] != 0].copy()

    return df[["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"]]


def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        sql = """
            SELECT
                sale_date,
                COALESCE(sales_amount, 0) AS sales_amount,
                COALESCE(profit_amount, 0) AS margin_amount,
                COALESCE(partner_name, '') AS customer_name
            FROM v_wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
    elif has_table(conn, "wholesale_sales"):
        sql = """
            SELECT
                sale_date,
                COALESCE(sales_amount, 0) AS sales_amount,
                COALESCE(profit_amount, 0) AS margin_amount,
                COALESCE(partner_name, '') AS customer_name
            FROM wholesale_sales
            WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
    else:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    if df.empty:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
    df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
    df["customer_name"] = df["customer_name"].fillna("")
    df["sales_type"] = "도매"
    df = df.dropna(subset=["dt"])
    df = df[df["sales_amount"] != 0].copy()

    return df[["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"]]


def load_period_sales(conn, date_from: str, date_to: str) -> pd.DataFrame:
    frames = []

    retail_df = get_retail_month_data(conn, date_from, date_to)
    wholesale_df = get_wholesale_month_data(conn, date_from, date_to)

    if not retail_df.empty:
        frames.append(retail_df)
    if not wholesale_df.empty:
        frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

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
        messages.append(f"이번달 도매 비중은 {wholesale_ratio:.1f}%입니다.")

    return messages[:3]


# =========================
# 화면
# =========================
st.title("Daily Cigar 운영 관리 시스템")

conn = get_conn()

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

# 상단 KPI
k1, k2, k3, k4 = st.columns(4)
k1.metric("이번달 매출", f"₩{month_sales:,.0f}")
k2.metric("이번달 마진", f"₩{month_margin:,.0f}")
k3.metric("거래건수", f"{deal_count:,}건")
k4.metric("객단가", f"₩{avg_ticket:,.0f}")

st.divider()

# 차트 영역
left, right = st.columns([2, 1])

with left:
    st.subheader("최근 30일 매출 추이")
    if last_30_df.empty:
        st.info("표시할 매출 데이터가 없습니다.")
    else:
        daily = (
            last_30_df.assign(date=last_30_df["dt"].dt.normalize())
            .groupby("date", as_index=True)[["sales_amount", "margin_amount"]]
            .sum()
            .sort_index()
        )
        st.line_chart(daily, use_container_width=True)

with right:
    st.subheader("도매 / 소매 비중")
    if month_df.empty:
        st.info("표시할 데이터가 없습니다.")
    else:
        ratio_df = month_df.groupby("sales_type")["sales_amount"].sum().to_frame()
        st.bar_chart(ratio_df, use_container_width=True)

st.divider()

# 인사이트
st.subheader("인사이트")
for msg in calc_insights(sales_df):
    st.write(f"- {msg}")

st.divider()

# 최근 판매 내역
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
            "sales_amount": "매출액",
            "margin_amount": "마진",
        }
    )
    st.dataframe(
        recent_df[["일자", "구분", "거래처/고객", "매출액", "마진"]],
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# DB 다운로드
render_db_download_section()

st.divider()

# DB 상태는 접기
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
            retail_cols = get_table_columns(conn, "retail_sales")
            st.write("retail_sales 컬럼:", retail_cols)
        else:
            st.write("retail_sales 테이블 없음")

        if has_table(conn, "wholesale_sales"):
            wholesale_cols = get_table_columns(conn, "wholesale_sales")
            st.write("wholesale_sales 컬럼:", wholesale_cols)
        else:
            st.write("wholesale_sales 테이블 없음")

        st.write("전체 로딩 건수:", len(sales_df))
        if not sales_df.empty:
            st.write("구분별 건수")
            st.dataframe(
                sales_df.groupby("sales_type").size().reset_index(name="건수"),
                use_container_width=True,
                hide_index=True,
            )
    except Exception as e:
        st.warning(f"디버그 정보 조회 중 오류: {e}")

st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

conn.close()