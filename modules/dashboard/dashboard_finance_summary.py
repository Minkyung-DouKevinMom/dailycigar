import os
import sqlite3
from typing import Tuple

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


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


def month_range(year: int, month: int) -> Tuple[str, str]:
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def prev_month(year: int, month: int) -> Tuple[int, int]:
    dt = pd.Timestamp(year=year, month=month, day=1) - pd.DateOffset(months=1)
    return dt.year, dt.month


def fmt_krw(value) -> str:
    try:
        return f"₩{float(value):,.0f}"
    except Exception:
        return "₩0"


def fmt_delta_krw(curr, prev) -> str:
    try:
        diff = float(curr) - float(prev)
        sign = "+" if diff > 0 else ""
        return f"{sign}₩{diff:,.0f}"
    except Exception:
        return "₩0"


def fmt_pct(value) -> str:
    try:
        return f"{float(value):.1f}%"
    except Exception:
        return "0.0%"


def get_retail_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_retail_sales_enriched"):
        sql = """
        SELECT
            sale_date,
            COALESCE(net_sales_amount, 0) AS net_sales_amount,
            COALESCE(total_korea_cost_krw, 0) AS total_korea_cost_krw,
            COALESCE(retail_gross_profit_krw, 0) AS gross_profit_krw
        FROM v_retail_sales_enriched
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    elif table_exists(conn, "retail_sales"):
        sql = """
        SELECT
            sale_date,
            COALESCE(net_sales_amount, 0) AS net_sales_amount
        FROM retail_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        df["total_korea_cost_krw"] = 0
        df["gross_profit_krw"] = 0

    else:
        return pd.DataFrame()

    for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if view_exists(conn, "v_wholesale_sales"):
        sql = """
        SELECT
            sale_date,
            COALESCE(sales_amount, 0) AS net_sales_amount,
            COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS total_korea_cost_krw,
            COALESCE(profit_amount, 0) AS gross_profit_krw
        FROM v_wholesale_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    elif table_exists(conn, "wholesale_sales"):
        sql = """
        SELECT
            sale_date,
            COALESCE(sales_amount, 0) AS net_sales_amount,
            COALESCE(qty, 0) * COALESCE(unit_cost, 0) AS total_korea_cost_krw,
            COALESCE(profit_amount, 0) AS gross_profit_krw
        FROM wholesale_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    else:
        return pd.DataFrame()

    for c in ["net_sales_amount", "total_korea_cost_krw", "gross_profit_krw"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def get_expense_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    sql = """
    SELECT
        expense_date,
        COALESCE(amount, 0) AS amount
    FROM expense_txn
    WHERE expense_date BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=[date_from, date_to])

    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    return df


def get_month_summary(conn, year: int, month: int) -> dict:
    date_from, date_to = month_range(year, month)

    retail_df = get_retail_month_data(conn, date_from, date_to)
    wholesale_df = get_wholesale_month_data(conn, date_from, date_to)
    expense_df = get_expense_month_data(conn, date_from, date_to)

    retail_sales = float(retail_df["net_sales_amount"].sum()) if not retail_df.empty else 0
    wholesale_sales = float(wholesale_df["net_sales_amount"].sum()) if not wholesale_df.empty else 0
    total_sales = retail_sales + wholesale_sales

    retail_cost = float(retail_df["total_korea_cost_krw"].sum()) if not retail_df.empty else 0
    wholesale_cost = float(wholesale_df["total_korea_cost_krw"].sum()) if not wholesale_df.empty else 0
    total_cost = retail_cost + wholesale_cost

    retail_gp = float(retail_df["gross_profit_krw"].sum()) if not retail_df.empty else 0
    wholesale_gp = float(wholesale_df["gross_profit_krw"].sum()) if not wholesale_df.empty else 0
    gross_profit = retail_gp + wholesale_gp

    if gross_profit == 0 and total_sales != 0:
        gross_profit = total_sales - total_cost

    expense_total = float(expense_df["amount"].sum()) if not expense_df.empty else 0
    operating_profit = gross_profit - expense_total

    return {
        "year": year,
        "month": month,
        "date_from": date_from,
        "date_to": date_to,
        "retail_sales": retail_sales,
        "wholesale_sales": wholesale_sales,
        "total_sales": total_sales,
        "total_cost": total_cost,
        "gross_profit": gross_profit,
        "expense_total": expense_total,
        "operating_profit": operating_profit,
    }


def get_monthly_trend(conn, months: int = 12) -> pd.DataFrame:
    base = pd.Timestamp.today().replace(day=1)
    rows = []

    for i in range(months - 1, -1, -1):
        dt = base - pd.DateOffset(months=i)
        summary = get_month_summary(conn, dt.year, dt.month)
        rows.append(
            {
                "월": f"{dt.year}-{dt.month:02d}",
                "소매매출": summary["retail_sales"],
                "도매매출": summary["wholesale_sales"],
                "총매출": summary["total_sales"],
                "매출총이익": summary["gross_profit"],
                "지출": summary["expense_total"],
                "영업이익": summary["operating_profit"],
            }
        )

    return pd.DataFrame(rows)


def get_recent_expenses(conn, limit: int = 10) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    sql = """
    SELECT
        t.expense_date,
        c.expense_group,
        c.expense_name,
        t.amount,
        t.vendor_name,
        t.payment_method
    FROM expense_txn t
    LEFT JOIN expense_category_mst c
        ON t.expense_category_id = c.id
    ORDER BY t.expense_date DESC, t.id DESC
    LIMIT ?
    """
    df = pd.read_sql_query(sql, conn, params=[limit])

    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    return df


def get_top_products(conn, date_from: str, date_to: str, limit: int = 10, metric: str = "sales") -> pd.DataFrame:
    frames = []

    retail_metric_sql = "COALESCE(net_sales_amount, 0)"
    wholesale_metric_sql = "COALESCE(sales_amount, 0)"
    metric_col_name = "metric_value"

    if metric == "profit":
        retail_metric_sql = "COALESCE(retail_gross_profit_krw, 0)"
        wholesale_metric_sql = "COALESCE(profit_amount, 0)"

    if view_exists(conn, "v_retail_sales_enriched"):
        sql = f"""
        SELECT
            COALESCE(product_code, product_code_raw) AS product_code,
            COALESCE(mst_product_name, product_code_raw) AS product_name,
            {retail_metric_sql} AS {metric_col_name}
        FROM v_retail_sales_enriched
        WHERE sale_date BETWEEN ? AND ?
        """
        retail_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        if not retail_df.empty:
            retail_df[metric_col_name] = pd.to_numeric(
                retail_df[metric_col_name], errors="coerce"
            ).fillna(0)
            frames.append(retail_df)

    if view_exists(conn, "v_wholesale_sales"):
        sql = f"""
        SELECT
            COALESCE(product_code, '') AS product_code,
            COALESCE(product_name, product_code) AS product_name,
            {wholesale_metric_sql} AS {metric_col_name}
        FROM v_wholesale_sales
        WHERE sale_date BETWEEN ? AND ?
        """
        wholesale_df = pd.read_sql_query(sql, conn, params=[date_from, date_to])
        if not wholesale_df.empty:
            wholesale_df[metric_col_name] = pd.to_numeric(
                wholesale_df[metric_col_name], errors="coerce"
            ).fillna(0)
            frames.append(wholesale_df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = (
        df.groupby(["product_code", "product_name"], dropna=False)[metric_col_name]
        .sum()
        .reset_index()
        .sort_values(metric_col_name, ascending=False)
        .head(limit)
    )

    return df


def render():
    st.subheader("경영 대시보드")

    conn = get_conn()
    try:
        today = pd.Timestamp.today()
        current_year = today.year
        current_month = today.month

        y1, y2 = st.columns([1, 1])
        with y1:
            year = st.selectbox(
                "연도",
                options=list(range(current_year - 2, current_year + 1)),
                index=2,
            )
        with y2:
            month = st.selectbox(
                "월",
                options=list(range(1, 13)),
                index=current_month - 1,
            )

        current = get_month_summary(conn, year, month)
        py, pm = prev_month(year, month)
        previous = get_month_summary(conn, py, pm)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "월 총매출",
            fmt_krw(current["total_sales"]),
            fmt_delta_krw(current["total_sales"], previous["total_sales"]),
        )
        c2.metric(
            "월 지출",
            fmt_krw(current["expense_total"]),
            fmt_delta_krw(current["expense_total"], previous["expense_total"]),
        )
        c3.metric(
            "월 매출총이익",
            fmt_krw(current["gross_profit"]),
            fmt_delta_krw(current["gross_profit"], previous["gross_profit"]),
        )
        c4.metric(
            "월 영업이익",
            fmt_krw(current["operating_profit"]),
            fmt_delta_krw(current["operating_profit"], previous["operating_profit"]),
        )

        c5, c6, c7, c8 = st.columns(4)
        op_margin = (
            current["operating_profit"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )
        sales_mix_retail = (
            current["retail_sales"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )
        sales_mix_wholesale = (
            current["wholesale_sales"] / current["total_sales"] * 100
            if current["total_sales"]
            else 0
        )

        c5.metric("소매매출", fmt_krw(current["retail_sales"]))
        c6.metric("도매매출", fmt_krw(current["wholesale_sales"]))
        c7.metric("영업이익률", fmt_pct(op_margin))
        c8.metric("소매/도매 비중", f"{sales_mix_retail:.0f}% / {sales_mix_wholesale:.0f}%")

        st.caption(
            f"기준기간: {current['date_from']} ~ {current['date_to']} / "
            f"비교기간: {previous['date_from']} ~ {previous['date_to']}"
        )

        tab1, tab2, tab3 = st.tabs(["월별 추이", "최근 지출", "상위 제품"])

        with tab1:
            trend_df = get_monthly_trend(conn, months=12)

            if not trend_df.empty:
                show_trend_df = trend_df.copy()
                money_cols = ["소매매출", "도매매출", "총매출", "매출총이익", "지출", "영업이익"]

                for col in money_cols:
                    show_trend_df[col] = show_trend_df[col].apply(fmt_krw)

                st.dataframe(
                    show_trend_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                st.markdown("###### 월별 매출 추이")
                st.line_chart(
                    trend_df.set_index("월")[["소매매출", "도매매출", "총매출"]]
                )

                st.markdown("###### 월별 손익 추이")
                st.line_chart(
                    trend_df.set_index("월")[["매출총이익", "지출", "영업이익"]]
                )
            else:
                st.info("월별 추이 데이터가 없습니다.")

        with tab2:
            recent_exp_df = get_recent_expenses(conn, limit=12)

            if recent_exp_df.empty:
                st.info("최근 지출 데이터가 없습니다.")
            else:
                recent_exp_df = recent_exp_df.rename(
                    columns={
                        "expense_date": "지출일자",
                        "expense_group": "지출그룹",
                        "expense_name": "지출항목",
                        "amount": "금액",
                        "vendor_name": "거래처",
                        "payment_method": "결제수단",
                    }
                )
                recent_exp_df["금액"] = recent_exp_df["금액"].apply(fmt_krw)

                st.dataframe(
                    recent_exp_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

        with tab3:
            basis = st.radio(
                "집계 기준",
                options=["매출액", "마진"],
                horizontal=True,
                key="dashboard_top_product_basis",
            )

            metric = "sales" if basis == "매출액" else "profit"
            value_col = "metric_value"
            value_label = "매출액" if basis == "매출액" else "마진"

            top_df = get_top_products(
                conn,
                current["date_from"],
                current["date_to"],
                limit=10,
                metric=metric,
            )

            if top_df.empty:
                st.info("상위 제품 데이터가 없습니다.")
            else:
                show_top_df = top_df.rename(
                    columns={
                        "product_code": "상품코드",
                        "product_name": "상품명",
                        value_col: value_label,
                    }
                ).copy()

                show_top_df[value_label] = show_top_df[value_label].apply(fmt_krw)

                st.dataframe(
                    show_top_df,
                    use_container_width=True,
                    hide_index=True,
                    height=360,
                )

                chart_df = top_df.copy()
                chart_df["상품코드_표시"] = chart_df["product_code"].fillna("").astype(str).str.strip()
                chart_df.loc[chart_df["상품코드_표시"] == "", "상품코드_표시"] = (
                    chart_df.loc[chart_df["상품코드_표시"] == "", "product_name"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                )

                st.bar_chart(chart_df.set_index("상품코드_표시")[[value_col]])

    finally:
        conn.close()


if __name__ == "__main__":
    render()