import os
import sqlite3
from typing import Optional, Tuple

import altair as alt
import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


# =========================
# 공통
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def object_exists(conn: sqlite3.Connection, name: str, obj_type: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
        (obj_type, name),
    )
    return cur.fetchone() is not None


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return object_exists(conn, table_name, "table")


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    return object_exists(conn, view_name, "view")


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


def calc_delta(curr: float, prev: float) -> Tuple[float, float]:
    diff = float(curr) - float(prev)
    pct = (diff / float(prev) * 100) if prev not in (0, None) else 0.0
    return diff, pct


def fmt_delta_krw(curr: float, prev: float) -> str:
    diff, pct = calc_delta(curr, prev)
    if diff > 0:
        return f"+₩{diff:,.0f} / {pct:+.1f}%"
    elif diff < 0:
        return f"-₩{abs(diff):,.0f} / {pct:+.1f}%"
    else:
        return f"₩0 / 0.0%"


def fmt_delta_count(curr: float, prev: float) -> str:
    diff, pct = calc_delta(curr, prev)
    if diff > 0:
        return f"+{int(diff):,}건 / {pct:+.1f}%"
    elif diff < 0:
        return f"-{int(abs(diff)):,}건 / {pct:+.1f}%"
    else:
        return f"0건 / 0.0%"


# =========================
# 데이터 로딩
# =========================
def get_retail_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_retail_sales_enriched"):
        sql = """
        SELECT
            sale_date,
            COALESCE(order_no, '') AS order_no,
            COALESCE(net_sales_amount, 0) AS sales_amount,
            COALESCE(total_korea_cost_krw, 0) AS cost_amount,
            COALESCE(retail_gross_profit_krw, 0) AS profit_amount
        FROM v_retail_sales_enriched
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    elif table_exists(conn, "retail_sales"):
        sql = """
        SELECT
            sale_date,
            COALESCE(order_no, '') AS order_no,
            COALESCE(net_sales_amount, 0) AS sales_amount
        FROM retail_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        df["cost_amount"] = 0
        df["profit_amount"] = 0

    else:
        return pd.DataFrame(columns=["sale_date", "order_no", "sales_amount", "cost_amount", "profit_amount", "sales_type"])

    for c in ["sales_amount", "cost_amount", "profit_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["sales_type"] = "소매"
    return df


def get_wholesale_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        sql = """
        SELECT
            sale_date,
            CAST(id AS TEXT) AS order_no,
            COALESCE(sales_amount, 0) AS sales_amount,
            COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS cost_amount,
            COALESCE(profit_amount, 0) AS profit_amount
        FROM v_wholesale_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    elif table_exists(conn, "wholesale_sales"):
        sql = """
        SELECT
            sale_date,
            CAST(id AS TEXT) AS order_no,
            COALESCE(sales_amount, 0) AS sales_amount,
            COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS cost_amount,
            COALESCE(profit_amount, 0) AS profit_amount
        FROM wholesale_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    else:
        return pd.DataFrame(columns=["sale_date", "order_no", "sales_amount", "cost_amount", "profit_amount", "sales_type"])

    for c in ["sales_amount", "cost_amount", "profit_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["sales_type"] = "도매"
    return df


def load_period_sales(conn, date_from: str, date_to: str) -> pd.DataFrame:
    frames = []

    retail_df = get_retail_data(conn, date_from, date_to)
    wholesale_df = get_wholesale_data(conn, date_from, date_to)

    if not retail_df.empty:
        frames.append(retail_df)
    if not wholesale_df.empty:
        frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame(columns=["sale_date", "order_no", "sales_amount", "cost_amount", "profit_amount", "sales_type"])

    df = pd.concat(frames, ignore_index=True)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df.dropna(subset=["sale_date"]).copy()
    return df


# =========================
# 집계
# =========================
def summarize_period(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "sales": 0.0,
            "profit": 0.0,
            "orders": 0,
            "avg_ticket": 0.0,
            "retail_sales": 0.0,
            "wholesale_sales": 0.0,
            "retail_profit": 0.0,
            "wholesale_profit": 0.0,
        }

    sales = float(df["sales_amount"].sum())
    profit = float(df["profit_amount"].sum())
    orders = int(df["order_no"].nunique()) if "order_no" in df.columns else len(df)
    avg_ticket = (sales / orders) if orders else 0.0

    retail_df = df[df["sales_type"] == "소매"]
    wholesale_df = df[df["sales_type"] == "도매"]

    return {
        "sales": sales,
        "profit": profit,
        "orders": orders,
        "avg_ticket": avg_ticket,
        "retail_sales": float(retail_df["sales_amount"].sum()) if not retail_df.empty else 0.0,
        "wholesale_sales": float(wholesale_df["sales_amount"].sum()) if not wholesale_df.empty else 0.0,
        "retail_profit": float(retail_df["profit_amount"].sum()) if not retail_df.empty else 0.0,
        "wholesale_profit": float(wholesale_df["profit_amount"].sum()) if not wholesale_df.empty else 0.0,
    }


def build_compare_table(a: dict, b: dict) -> pd.DataFrame:
    rows = [
        ["매출", a["sales"], b["sales"]],
        ["이익", a["profit"], b["profit"]],
        ["거래건수", a["orders"], b["orders"]],
        ["객단가", a["avg_ticket"], b["avg_ticket"]],
        ["소매매출", a["retail_sales"], b["retail_sales"]],
        ["도매매출", a["wholesale_sales"], b["wholesale_sales"]],
        ["소매이익", a["retail_profit"], b["retail_profit"]],
        ["도매이익", a["wholesale_profit"], b["wholesale_profit"]],
    ]

    result = []
    for metric, a_val, b_val in rows:
        diff, pct = calc_delta(a_val, b_val)
        result.append(
            {
                "항목": metric,
                "기간 A": a_val,
                "기간 B": b_val,
                "증감액": diff,
                "증감률(%)": round(pct, 1),
            }
        )

    return pd.DataFrame(result)



def format_compare_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    money_metrics = ["매출", "이익", "객단가", "소매매출", "도매매출", "소매이익", "도매이익"]
    count_metrics = ["거래건수"]

    # 먼저 표시용으로 object 타입으로 바꿔 dtype 충돌 방지
    for col in ["기간 A", "기간 B", "증감액", "증감률(%)"]:
        out[col] = out[col].astype(object)

    money_mask = out["항목"].isin(money_metrics)
    count_mask = out["항목"].isin(count_metrics)

    out.loc[money_mask, "기간 A"] = out.loc[money_mask, "기간 A"].apply(fmt_krw)
    out.loc[money_mask, "기간 B"] = out.loc[money_mask, "기간 B"].apply(fmt_krw)
    out.loc[money_mask, "증감액"] = out.loc[money_mask, "증감액"].apply(fmt_krw)

    out.loc[count_mask, "기간 A"] = out.loc[count_mask, "기간 A"].apply(fmt_count)
    out.loc[count_mask, "기간 B"] = out.loc[count_mask, "기간 B"].apply(fmt_count)
    out.loc[count_mask, "증감액"] = out.loc[count_mask, "증감액"].apply(
        lambda x: f"{int(x):,}건" if pd.notna(x) else "0건"
    )

    out["증감률(%)"] = out["증감률(%)"].apply(
        lambda x: f"{float(x):+.1f}%" if pd.notna(x) else "0.0%"
    )

    return out

# =========================
# 렌더
# =========================
def render():
    st.subheader("기간 비교")

    today = pd.Timestamp.today().normalize()
    month_start = today.replace(day=1)
    prev_month_end = month_start - pd.Timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    st.caption("기간 A와 기간 B를 비교하여 매출, 이익, 객단가, 거래건수를 확인합니다.")

    a1, a2 = st.columns(2)
    with a1:
        st.markdown("#### 기간 A")
        date_a_from = st.date_input("기간 A 시작일", value=month_start, key="period_a_from")
        date_a_to = st.date_input("기간 A 종료일", value=today, key="period_a_to")

    with a2:
        st.markdown("#### 기간 B")
        date_b_from = st.date_input("기간 B 시작일", value=prev_month_start, key="period_b_from")
        date_b_to = st.date_input("기간 B 종료일", value=prev_month_end, key="period_b_to")

    if date_a_from > date_a_to:
        st.error("기간 A의 시작일이 종료일보다 늦습니다.")
        return

    if date_b_from > date_b_to:
        st.error("기간 B의 시작일이 종료일보다 늦습니다.")
        return

    conn = get_conn()
    try:
        df_a = load_period_sales(conn, str(date_a_from), str(date_a_to))
        df_b = load_period_sales(conn, str(date_b_from), str(date_b_to))

        sum_a = summarize_period(df_a)
        sum_b = summarize_period(df_b)

        st.divider()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("매출", fmt_krw(sum_a["sales"]), fmt_delta_krw(sum_a["sales"], sum_b["sales"]))
        k2.metric("이익", fmt_krw(sum_a["profit"]), fmt_delta_krw(sum_a["profit"], sum_b["profit"]))
        k3.metric("거래건수", fmt_count(sum_a["orders"]), fmt_delta_count(sum_a["orders"], sum_b["orders"]))
        k4.metric("객단가", fmt_krw(sum_a["avg_ticket"]), fmt_delta_krw(sum_a["avg_ticket"], sum_b["avg_ticket"]))

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 비교 요약")
            compare_df = build_compare_table(sum_a, sum_b)
            show_compare_df = format_compare_table(compare_df)
            st.dataframe(show_compare_df, use_container_width=True, hide_index=True)

        with c2:
            st.markdown("#### 비교 차트")
            chart_raw = pd.DataFrame(
                {
                    "항목": ["매출", "이익", "소매매출", "도매매출"],
                    "기간 A": [
                        sum_a["sales"],
                        sum_a["profit"],
                        sum_a["retail_sales"],
                        sum_a["wholesale_sales"],
                    ],
                    "기간 B": [
                        sum_b["sales"],
                        sum_b["profit"],
                        sum_b["retail_sales"],
                        sum_b["wholesale_sales"],
                    ],
                }
            )
            chart_melt = chart_raw.melt(id_vars="항목", var_name="기간", value_name="금액")
            bar_chart = (
                alt.Chart(chart_melt)
                .mark_bar()
                .encode(
                    x=alt.X("항목:N", title=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("금액:Q", title="금액(원)", axis=alt.Axis(format=",.0f")),
                    color=alt.Color("기간:N", title=None),
                    xOffset="기간:N",
                    tooltip=[
                        alt.Tooltip("항목:N", title="항목"),
                        alt.Tooltip("기간:N", title="기간"),
                        alt.Tooltip("금액:Q", title="금액", format=",.0f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(bar_chart, use_container_width=True)

        st.divider()

        d1, d2 = st.columns(2)
        with d1:
            st.markdown("#### 기간 A 일별 추이")
            if df_a.empty:
                st.info("기간 A 데이터가 없습니다.")
            else:
                daily_a = (
                    df_a.groupby(df_a["sale_date"].dt.strftime("%Y-%m-%d"))["sales_amount"]
                    .sum()
                    .reset_index()
                    .rename(columns={"sale_date": "날짜", "sales_amount": "매출액"})
                )
                line_a = (
                    alt.Chart(daily_a)
                    .mark_line(point=True, color="#4C72B0")
                    .encode(
                        x=alt.X("날짜:N", title=None, axis=alt.Axis(labelAngle=-45, labelOverlap=True)),
                        y=alt.Y("매출액:Q", title="금액(원)", axis=alt.Axis(format=",.0f")),
                        tooltip=[
                            alt.Tooltip("날짜:N", title="날짜"),
                            alt.Tooltip("매출액:Q", title="매출액", format=",.0f"),
                        ],
                    )
                    .properties(height=280)
                )
                st.altair_chart(line_a, use_container_width=True)

        with d2:
            st.markdown("#### 기간 B 일별 추이")
            if df_b.empty:
                st.info("기간 B 데이터가 없습니다.")
            else:
                daily_b = (
                    df_b.groupby(df_b["sale_date"].dt.strftime("%Y-%m-%d"))["sales_amount"]
                    .sum()
                    .reset_index()
                    .rename(columns={"sale_date": "날짜", "sales_amount": "매출액"})
                )
                line_b = (
                    alt.Chart(daily_b)
                    .mark_line(point=True, color="#DD8452")
                    .encode(
                        x=alt.X("날짜:N", title=None, axis=alt.Axis(labelAngle=-45, labelOverlap=True)),
                        y=alt.Y("매출액:Q", title="금액(원)", axis=alt.Axis(format=",.0f")),
                        tooltip=[
                            alt.Tooltip("날짜:N", title="날짜"),
                            alt.Tooltip("매출액:Q", title="매출액", format=",.0f"),
                        ],
                    )
                    .properties(height=280)
                )
                st.altair_chart(line_b, use_container_width=True)

    finally:
        conn.close()


if __name__ == "__main__":
    render()