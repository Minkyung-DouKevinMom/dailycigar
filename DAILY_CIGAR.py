import sqlite3
import pandas as pd
import streamlit as st

from db import get_table_count, table_exists, get_all_import_batch

st.set_page_config(page_title="Daily Cigar DB", layout="wide")


# =========================
# DB 연결
# =========================
DB_PATH = "cigar.db"


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
    for c in candidates:
        if c in cols:
            return c
    return None


# =========================
# 매출 로딩
# =========================
def load_sales_from_table(conn, table_name: str, sales_type_label: str) -> pd.DataFrame:
    cols = get_table_columns(conn, table_name)
    if not cols:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    date_col = pick_col(cols, ["sale_date", "sales_date", "order_date", "created_at", "date"])
    amount_col = pick_col(cols, ["total_amount", "final_amount", "sales_amount", "amount", "total_price"])
    margin_col = pick_col(cols, ["gross_profit", "margin_amount", "profit", "expected_margin"])
    name_col = pick_col(cols, ["customer_name", "partner_name", "client_name", "store_name", "partner"])

    if not date_col or not amount_col:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    margin_expr = f"{margin_col} as margin_amount" if margin_col else "0 as margin_amount"
    name_expr = f"{name_col} as customer_name" if name_col else "'' as customer_name"

    query = f"""
        SELECT
            {date_col} as dt,
            {amount_col} as sales_amount,
            {margin_expr},
            {name_expr}
        FROM {table_name}
    """

    try:
        df = pd.read_sql_query(query, conn)
        if df.empty:
            return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

        df["sales_type"] = sales_type_label
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").fillna(0)
        df["margin_amount"] = pd.to_numeric(df["margin_amount"], errors="coerce").fillna(0)
        df["customer_name"] = df["customer_name"].fillna("")
        df = df.dropna(subset=["dt"])
        return df
    except Exception:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])


def load_all_sales(conn) -> pd.DataFrame:
    retail_df = load_sales_from_table(conn, "retail_sales", "소매")
    wholesale_df = load_sales_from_table(conn, "wholesale_sales", "도매")

    if retail_df.empty and wholesale_df.empty:
        return pd.DataFrame(columns=["dt", "sales_amount", "margin_amount", "customer_name", "sales_type"])

    return pd.concat([retail_df, wholesale_df], ignore_index=True)


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
        margin_rate = this_margin / this_sales * 100
        messages.append(f"이번달 마진율은 {margin_rate:.1f}%입니다.")

        wholesale_sales = this_month.loc[this_month["sales_type"] == "도매", "sales_amount"].sum()
        wholesale_ratio = wholesale_sales / this_sales * 100
        messages.append(f"이번달 도매 비중은 {wholesale_ratio:.1f}%입니다.")

    return messages[:3]


# =========================
# 화면
# =========================
st.title("Daily Cigar 운영 관리 시스템")

conn = get_conn()
sales_df = load_all_sales(conn)

today = pd.Timestamp.today().normalize()
month_start = today.replace(day=1)
last_30_start = today - pd.Timedelta(days=29)

month_df = sales_df[(sales_df["dt"] >= month_start) & (sales_df["dt"] <= today)]
last_30_df = sales_df[(sales_df["dt"] >= last_30_start) & (sales_df["dt"] <= today)]

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
            last_30_df.groupby(last_30_df["dt"].dt.date)["sales_amount"]
            .sum()
            .reset_index()
        )
        daily.columns = ["date", "sales_amount"]
        daily = daily.set_index("date")
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
    recent_df = sales_df.sort_values("dt", ascending=False).head(10).copy()
    recent_df["dt"] = recent_df["dt"].dt.strftime("%Y-%m-%d")
    recent_df = recent_df.rename(columns={
        "dt": "일자",
        "sales_type": "구분",
        "customer_name": "거래처/고객",
        "sales_amount": "매출액",
        "margin_amount": "마진"
    })
    st.dataframe(
        recent_df[["일자", "구분", "거래처/고객", "매출액", "마진"]],
        use_container_width=True,
        hide_index=True
    )

st.divider()

# 최근 Import Batch
st.subheader("최근 Import Batch")
if table_exists("import_batch"):
    try:
        batch_df = get_all_import_batch()
        if batch_df is not None and not batch_df.empty:
            st.dataframe(batch_df.head(20), use_container_width=True, hide_index=True)
        else:
            st.info("import_batch 데이터가 없습니다.")
    except Exception as e:
        st.warning(f"import_batch 조회 중 오류: {e}")
else:
    st.info("import_batch 테이블이 없습니다.")

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
    ]

    cols = st.columns(3)
    for idx, table_name in enumerate(tables):
        with cols[idx % 3]:
            exists = table_exists(table_name)
            count = get_table_count(table_name) if exists else 0
            st.metric(
                label=table_name,
                value=count,
                delta="OK" if exists else "없음"
            )

st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

conn.close()