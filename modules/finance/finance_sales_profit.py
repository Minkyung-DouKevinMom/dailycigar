import sqlite3
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


from modules.common.dbutil import get_conn, table_exists, view_exists, get_table_columns
from modules.common.fmt import fmt_krw, apply_currency_format
from modules.common.dates import monthify
from modules.common.sales_query import load_retail_sales, load_wholesale_sales, retail_source


def choose_source(conn: sqlite3.Connection, candidates: List[str]) -> Optional[str]:
    for name in candidates:
        if table_exists(conn, name) or view_exists(conn, name):
            return name
    return None


def to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    output.seek(0)
    return output.getvalue()


def get_retail_data(conn, date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.DataFrame, str]:
    """소매 데이터 (정본: modules.common.sales_query.load_retail_sales). (df, source) 반환."""
    source = retail_source(conn) or ""
    if not source:
        return pd.DataFrame(), ""
    df = load_retail_sales(conn, date_from, date_to)
    if df.empty:
        return pd.DataFrame(), source
    df = df.rename(columns={"product_name": "mst_product_name", "size_name": "mst_size_name"})
    return df, source


def get_wholesale_data(conn, date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.DataFrame, str]:
    """도매 데이터 (정본: modules.common.sales_query.load_wholesale_sales). (df, source) 반환."""
    empty_cols = [
        "id", "sale_date", "partner_name", "item_type", "product_name", "product_code",
        "qty", "unit_price", "unit_cost", "sales_amount", "net_sales_amount",
        "vat_amount", "profit_amount", "gross_profit_krw", "total_korea_cost_krw"
    ]
    if not table_exists(conn, "wholesale_sales"):
        return pd.DataFrame(columns=empty_cols), ""
    df = load_wholesale_sales(conn, date_from, date_to)
    if df.empty:
        return pd.DataFrame(columns=empty_cols), "wholesale_sales"
    df = df.copy()
    df["sales_amount"] = df["net_sales_amount"]
    df["profit_amount"] = df["gross_profit_krw"]
    return df[empty_cols], "wholesale_sales"


def get_expense_data(conn, date_from: Optional[str], date_to: Optional[str]) -> pd.DataFrame:
    if not table_exists(conn, "expense_txn"):
        return pd.DataFrame()

    txn_cols = get_table_columns(conn, "expense_txn")
    cat_exists = table_exists(conn, "expense_category_mst")
    cat_cols = get_table_columns(conn, "expense_category_mst") if cat_exists else []

    date_col = "expense_date" if "expense_date" in txn_cols else None
    amount_col = "amount" if "amount" in txn_cols else None
    vendor_col = "vendor_name" if "vendor_name" in txn_cols else "vendor" if "vendor" in txn_cols else None
    payment_col = "payment_method" if "payment_method" in txn_cols else None
    category_id_col = "expense_category_id" if "expense_category_id" in txn_cols else None

    if not date_col or not amount_col:
        return pd.DataFrame()

    if cat_exists and category_id_col:
        group_col = "expense_group" if "expense_group" in cat_cols else None
        name_col = "expense_name" if "expense_name" in cat_cols else "name" if "name" in cat_cols else None

        sql = f"""
            SELECT
                t.{date_col} AS expense_date,
                {"COALESCE(c." + group_col + ", '')" if group_col else "''"} AS expense_group,
                {"COALESCE(c." + name_col + ", '')" if name_col else "''"} AS expense_name,
                COALESCE(t.{amount_col}, 0) AS amount,
                {"COALESCE(t." + vendor_col + ", '')" if vendor_col else "''"} AS vendor_name,
                {"COALESCE(t." + payment_col + ", '')" if payment_col else "''"} AS payment_method
            FROM expense_txn t
            LEFT JOIN expense_category_mst c
                ON t.{category_id_col} = c.id
            WHERE 1=1
        """
    else:
        sql = f"""
            SELECT
                {date_col} AS expense_date,
                '' AS expense_group,
                '' AS expense_name,
                COALESCE({amount_col}, 0) AS amount,
                {"COALESCE(" + vendor_col + ", '')" if vendor_col else "''"} AS vendor_name,
                {"COALESCE(" + payment_col + ", '')" if payment_col else "''"} AS payment_method
            FROM expense_txn
            WHERE 1=1
        """

    params = []
    if date_from:
        sql += " AND expense_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND expense_date <= ?"
        params.append(date_to)

    sql += " ORDER BY expense_date DESC"
    df = pd.read_sql_query(sql, conn, params=params)

    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    return df


def render_sales_combined():
    st.markdown("### 매출분석")

    conn = get_conn()
    try:
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("시작일", value=None, key="fin_sales_from")
        with c2:
            date_to = st.date_input("종료일", value=None, key="fin_sales_to")

        dfrom = str(date_from) if date_from else None
        dto = str(date_to) if date_to else None

        retail_df, retail_source = get_retail_data(conn, dfrom, dto)
        wholesale_df, wholesale_source = get_wholesale_data(conn, dfrom, dto)

        retail_sales = float(retail_df["net_sales_amount"].sum()) if "net_sales_amount" in retail_df.columns else 0
        retail_profit = float(retail_df["retail_gross_profit_krw"].sum()) if "retail_gross_profit_krw" in retail_df.columns else 0

        wholesale_sales = float(wholesale_df["net_sales_amount"].sum()) if "net_sales_amount" in wholesale_df.columns else 0
        wholesale_profit = float(wholesale_df["gross_profit_krw"].sum()) if "gross_profit_krw" in wholesale_df.columns else 0

        total_sales = retail_sales + wholesale_sales
        total_profit = retail_profit + wholesale_profit

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("소매매출", fmt_krw(retail_sales))
        m2.metric("소매이익", fmt_krw(retail_profit))
        m3.metric("도매매출", fmt_krw(wholesale_sales))
        m4.metric("도매이익", fmt_krw(wholesale_profit))
        m5.metric("통합매출", fmt_krw(total_sales))
        m6.metric("통합매출총이익", fmt_krw(total_profit))

        src_text = []
        if retail_source:
            src_text.append(f"소매: {retail_source}")
        if wholesale_source:
            src_text.append(f"도매: {wholesale_source}")
        if src_text:
            st.caption("조회 소스 - " + " / ".join(src_text))

        tab1, tab2, tab3 = st.tabs(["월별 통합", "소매 상세", "도매 상세"])

        with tab1:
            month_frames = []

            if not retail_df.empty and "sale_date" in retail_df.columns:
                x = retail_df.copy()
                x["월"] = monthify(x["sale_date"])
                # 주문번호는 일자별로 재사용되는 짧은 순번이라, order_no만으로 nunique()를
                # 구하면 같은 달 안의 다른 날짜 주문이 같은 거래로 합쳐져 과소집계된다.
                # (판매일자, 주문번호) 조합으로 유니크 키를 만들어 집계한다.
                if "order_no" in x.columns:
                    x["_order_key"] = x["sale_date"].astype(str) + "_" + x["order_no"].astype(str)
                    count_agg = ("_order_key", "nunique")
                else:
                    count_agg = ("sale_date", "count")
                g = x.groupby("월", dropna=False).agg(
                    소매매출=("net_sales_amount", "sum"),
                    소매건수=count_agg,
                    소매이익=("retail_gross_profit_krw", "sum"),
                ).reset_index()
                month_frames.append(g)

            if not wholesale_df.empty and "sale_date" in wholesale_df.columns:
                x = wholesale_df.copy()
                x["월"] = monthify(x["sale_date"])
                g = x.groupby("월", dropna=False).agg(
                    도매매출=("net_sales_amount", "sum"),
                    도매건수=("id", "count") if "id" in x.columns else ("sale_date", "count"),
                    도매이익=("gross_profit_krw", "sum"),
                ).reset_index()
                month_frames.append(g)

            if not month_frames:
                st.info("해당 기간 데이터가 없습니다.")
                df_month = pd.DataFrame()
            else:
                df_month = month_frames[0]
                for extra in month_frames[1:]:
                    df_month = df_month.merge(extra, on="월", how="outer")

                for c in ["소매매출", "도매매출", "소매이익", "도매이익"]:
                    if c not in df_month.columns:
                        df_month[c] = 0.0
                    df_month[c] = pd.to_numeric(df_month[c], errors="coerce").fillna(0)

                df_month["통합매출"] = df_month["소매매출"] + df_month["도매매출"]
                df_month["통합이익"] = df_month["소매이익"] + df_month["도매이익"]
                df_month = df_month.sort_values("월")

                show = apply_currency_format(
                    df_month,
                    ["소매매출", "도매매출", "통합매출", "소매이익", "도매이익", "통합이익"],
                )
                st.dataframe(show, use_container_width=True, hide_index=True, height=420)

            excel_bytes = to_excel_bytes({
                "월별통합": df_month,
                "소매상세": retail_df,
                "도매상세": wholesale_df,
            })
            st.download_button(
                "매출분석 엑셀 다운로드",
                data=excel_bytes,
                file_name="finance_sales_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with tab2:
            if retail_df.empty:
                st.info("해당 기간 소매 데이터가 없습니다.")
            else:
                show = retail_df.copy()

                rename_map = {
                    "sale_date": "판매일자",
                    "sale_datetime": "판매일시",
                    "order_no": "주문번호",
                    "order_channel": "채널",
                    "payment_status": "결제상태",
                    "product_code": "상품코드",
                    "product_code_raw": "원본상품코드",
                    "mst_product_name": "상품명",
                    "mst_size_name": "사이즈",
                    "category": "카테고리",
                    "qty": "수량",
                    "net_sales_amount": "매출액",
                    "vat_amount": "부가세",
                    "total_korea_cost_krw": "원가",
                    "retail_gross_profit_krw": "매출총이익",
                }
                show = show.rename(columns=rename_map)

                show = apply_currency_format(show, ["매출액", "부가세", "원가", "매출총이익"])
                st.dataframe(show, use_container_width=True, hide_index=True, height=480)
                st.caption("※ 시가는 기존 원가/이익을 유지하고, 시가 외 항목만 매입가 기준으로 원가/이익을 재계산합니다.")

        with tab3:
            if wholesale_df.empty:
                st.info("해당 기간 도매 데이터가 없습니다.")
            else:
                show = wholesale_df.copy()
                rename_map = {
                    "sale_date": "판매일자",
                    "partner_name": "거래처",
                    "item_type": "구분",
                    "product_name": "상품명",
                    "product_code": "상품코드",
                    "qty": "수량",
                    "unit_price": "단가",
                    "unit_cost": "원가단가",
                    "net_sales_amount": "매출액",
                    "vat_amount": "부가세",
                    "total_korea_cost_krw": "총원가",
                    "gross_profit_krw": "매출총이익",
                }
                show = show.rename(columns=rename_map)
                show = apply_currency_format(show, ["단가", "원가단가", "매출액", "부가세", "총원가", "매출총이익"])
                st.dataframe(show, use_container_width=True, hide_index=True, height=480)

    finally:
        conn.close()


def render_profit_loss():
    st.markdown("### 손익분석")

    conn = get_conn()
    try:
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("시작일", value=None, key="fin_pl_from")
        with c2:
            date_to = st.date_input("종료일", value=None, key="fin_pl_to")

        dfrom = str(date_from) if date_from else None
        dto = str(date_to) if date_to else None

        retail_df, _ = get_retail_data(conn, dfrom, dto)
        wholesale_df, _ = get_wholesale_data(conn, dfrom, dto)
        expense_df = get_expense_data(conn, dfrom, dto)

        monthly_frames = []

        if not retail_df.empty:
            x = retail_df.copy()
            x["월"] = monthify(x["sale_date"])
            g = x.groupby("월", dropna=False).agg(소매매출=("net_sales_amount", "sum")).reset_index()

            if "total_korea_cost_krw" in x.columns:
                c = x.groupby("월", dropna=False)["total_korea_cost_krw"].sum().reset_index(name="소매원가")
                g = g.merge(c, on="월", how="left")

            if "retail_gross_profit_krw" in x.columns:
                p = x.groupby("월", dropna=False)["retail_gross_profit_krw"].sum().reset_index(name="소매이익")
                g = g.merge(p, on="월", how="left")

            monthly_frames.append(g)

        if not wholesale_df.empty:
            x = wholesale_df.copy()
            x["월"] = monthify(x["sale_date"])
            g = x.groupby("월", dropna=False).agg(도매매출=("net_sales_amount", "sum")).reset_index()

            if "total_korea_cost_krw" in x.columns:
                c = x.groupby("월", dropna=False)["total_korea_cost_krw"].sum().reset_index(name="도매원가")
                g = g.merge(c, on="월", how="left")

            if "gross_profit_krw" in x.columns:
                p = x.groupby("월", dropna=False)["gross_profit_krw"].sum().reset_index(name="도매이익")
                g = g.merge(p, on="월", how="left")

            monthly_frames.append(g)

        NON_RECURRING_EXPENSE_GROUPS = {"투자비", "일회성비용"}

        if not expense_df.empty:
            x = expense_df.copy()
            x["월"] = monthify(x["expense_date"])
            group_norm = x["expense_group"].fillna("").astype(str).str.strip()
            x["_expense_class"] = group_norm.where(
                group_norm.isin(NON_RECURRING_EXPENSE_GROUPS), "경상지출"
            )
            g = (
                x.groupby(["월", "_expense_class"], dropna=False)["amount"]
                .sum()
                .unstack(fill_value=0.0)
                .reset_index()
            )
            for col in ["경상지출", "투자비", "일회성비용"]:
                if col not in g.columns:
                    g[col] = 0.0
            monthly_frames.append(g[["월", "경상지출", "투자비", "일회성비용"]])

        if not monthly_frames:
            st.info("손익분석에 사용할 데이터가 없습니다.")
            return

        df_pl = monthly_frames[0]
        for extra in monthly_frames[1:]:
            df_pl = df_pl.merge(extra, on="월", how="outer")

        for c in ["소매매출", "소매원가", "소매이익", "도매매출", "도매원가", "도매이익", "경상지출", "투자비", "일회성비용"]:
            if c not in df_pl.columns:
                df_pl[c] = 0.0
            df_pl[c] = pd.to_numeric(df_pl[c], errors="coerce").fillna(0)

        df_pl["총매출"] = df_pl["소매매출"] + df_pl["도매매출"]
        df_pl["총원가"] = df_pl["소매원가"] + df_pl["도매원가"]
        df_pl["매출총이익"] = df_pl["소매이익"] + df_pl["도매이익"]
        df_pl["비경상지출"] = df_pl["투자비"] + df_pl["일회성비용"]
        df_pl["총지출"] = df_pl["경상지출"] + df_pl["비경상지출"]
        df_pl["경상영업이익"] = df_pl["매출총이익"] - df_pl["경상지출"]
        df_pl["영업이익"] = df_pl["매출총이익"] - df_pl["총지출"]
        df_pl = df_pl.sort_values("월")

        total_sales = float(df_pl["총매출"].sum())
        total_gp = float(df_pl["매출총이익"].sum())
        total_recurring_exp = float(df_pl["경상지출"].sum())
        total_non_recurring_exp = float(df_pl["비경상지출"].sum())
        total_recurring_op = float(df_pl["경상영업이익"].sum())
        total_op = float(df_pl["영업이익"].sum())
        total_retail_profit = float(df_pl["소매이익"].sum()) if "소매이익" in df_pl.columns else 0
        total_wholesale_profit = float(df_pl["도매이익"].sum()) if "도매이익" in df_pl.columns else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총매출", fmt_krw(total_sales))
        m2.metric("소매이익", fmt_krw(total_retail_profit))
        m3.metric("도매이익", fmt_krw(total_wholesale_profit))
        m4.metric("매출총이익", fmt_krw(total_gp))

        n1, n2, n3, n4 = st.columns(4)
        n1.metric("경상지출", fmt_krw(total_recurring_exp))
        n2.metric("비경상지출(투자비+일회성)", fmt_krw(total_non_recurring_exp))
        n3.metric("경상영업이익", fmt_krw(total_recurring_op))
        n4.metric("영업이익(전체)", fmt_krw(total_op))
        st.caption(
            "※ 지출항목 관리에서 지출그룹을 \"투자비\"(휴미더·가구 등 자산성 구매) 또는 "
            "\"일회성비용\"(인테리어·행정사 수수료 등 매달 반복되지 않는 지출)으로 등록한 항목은 "
            "경상지출/경상영업이익에서 제외됩니다. 실제 운영 정상화 여부는 경상영업이익 기준으로 보시는 것을 권장합니다."
        )

        tab1, tab2 = st.tabs(["월별 손익", "지출 상세"])

        with tab1:
            show = apply_currency_format(
                df_pl,
                [
                    "소매매출", "소매원가", "소매이익", "도매매출", "도매원가", "도매이익",
                    "총매출", "총원가", "매출총이익",
                    "경상지출", "투자비", "일회성비용", "비경상지출", "총지출", "경상영업이익", "영업이익",
                ],
            )
            st.dataframe(show, use_container_width=True, hide_index=True, height=420)

        with tab2:
            if expense_df.empty:
                st.info("해당 기간 지출 데이터가 없습니다.")
            else:
                exp_sum = (
                    expense_df.groupby(["expense_group", "expense_name"], dropna=False)["amount"]
                    .sum()
                    .reset_index()
                    .sort_values("amount", ascending=False)
                    .rename(
                        columns={
                            "expense_group": "지출그룹",
                            "expense_name": "지출항목",
                            "amount": "금액",
                        }
                    )
                )
                exp_sum = apply_currency_format(exp_sum, ["금액"])
                st.dataframe(exp_sum, use_container_width=True, hide_index=True, height=420)

        excel_bytes = to_excel_bytes({
            "월별손익": df_pl,
            "지출상세": expense_df,
            "소매기초": retail_df,
            "도매기초": wholesale_df,
        })
        st.download_button(
            "손익분석 엑셀 다운로드",
            data=excel_bytes,
            file_name="finance_profit_loss_monthly.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()


def render():
    tab1, tab2 = st.tabs(["매출분석", "손익분석"])
    with tab1:
        render_sales_combined()
    with tab2:
        render_profit_loss()


if __name__ == "__main__":
    render()