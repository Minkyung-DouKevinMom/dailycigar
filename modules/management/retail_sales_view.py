import os
import sqlite3
from io import BytesIO

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name = ?",
        (view_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [r[1] for r in rows]
    except Exception:
        return []


def load_filter_values(conn: sqlite3.Connection):
    channels = []
    statuses = []
    categories = []
    codes = []

    if view_exists(conn, "v_retail_sales_enriched"):
        base = "v_retail_sales_enriched"
    elif table_exists(conn, "retail_sales"):
        base = "retail_sales"
    else:
        return channels, statuses, categories, codes

    try:
        channels = pd.read_sql_query(
            f"SELECT DISTINCT order_channel FROM {base} WHERE COALESCE(order_channel,'') <> '' ORDER BY order_channel",
            conn,
        )["order_channel"].tolist()
    except Exception:
        pass

    try:
        statuses = pd.read_sql_query(
            f"SELECT DISTINCT payment_status FROM {base} WHERE COALESCE(payment_status,'') <> '' ORDER BY payment_status",
            conn,
        )["payment_status"].tolist()
    except Exception:
        pass

    try:
        categories = pd.read_sql_query(
            f"SELECT DISTINCT category FROM {base} WHERE COALESCE(category,'') <> '' ORDER BY category",
            conn,
        )["category"].tolist()
    except Exception:
        pass

    try:
        codes = pd.read_sql_query(
            f"SELECT DISTINCT product_code FROM {base} WHERE COALESCE(product_code,'') <> '' ORDER BY product_code",
            conn,
        )["product_code"].tolist()
    except Exception:
        pass

    return channels, statuses, categories, codes


def build_query(use_view: bool, filters: dict):
    base = "v_retail_sales_enriched" if use_view else "retail_sales"
    where = []
    params = []

    if filters.get("date_from"):
        where.append("sale_date >= ?")
        params.append(filters["date_from"])

    if filters.get("date_to"):
        where.append("sale_date <= ?")
        params.append(filters["date_to"])

    if filters.get("channels"):
        placeholders = ",".join(["?"] * len(filters["channels"]))
        where.append(f"order_channel IN ({placeholders})")
        params.extend(filters["channels"])

    if filters.get("statuses"):
        placeholders = ",".join(["?"] * len(filters["statuses"]))
        where.append(f"payment_status IN ({placeholders})")
        params.extend(filters["statuses"])

    if filters.get("categories"):
        placeholders = ",".join(["?"] * len(filters["categories"]))
        where.append(f"category IN ({placeholders})")
        params.extend(filters["categories"])

    if filters.get("codes"):
        placeholders = ",".join(["?"] * len(filters["codes"]))
        where.append(f"product_code IN ({placeholders})")
        params.extend(filters["codes"])

    if filters.get("keyword"):
        kw = f"%{filters['keyword'].strip()}%"
        keyword_fields = ["order_no", "product_code_raw", "product_code"]
        if use_view:
            keyword_fields.extend(["mst_product_name", "mst_size_name"])
        where.append("(" + " OR ".join([f"COALESCE({f}, '') LIKE ?" for f in keyword_fields]) + ")")
        params.extend([kw] * len(keyword_fields))

    sql = f"SELECT * FROM {base}"
    if where:
        sql += " WHERE " + " AND ".join(where)

    if use_view:
        sql += " ORDER BY sale_datetime DESC, id DESC"
    else:
        sql += " ORDER BY sale_datetime DESC, id DESC"

    return sql, params


def get_non_cigar_purchase_price_map(conn: sqlite3.Connection) -> dict:
    if not table_exists(conn, "non_cigar_product_mst"):
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
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception:
        return {}

    if df.empty:
        return {}

    df["product_code"] = df["product_code"].astype(str).str.strip()
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
    df = df[df["product_code"] != ""].copy()

    return dict(zip(df["product_code"], df["purchase_price"]))


def apply_non_cigar_margin_logic(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    - 시가(product_mst): 기존 로직 유지
    - 시가 외(non_cigar_product_mst): 판매금액 - (매입가 * 수량)
    """
    if df.empty:
        return df

    out = df.copy()

    if "product_code" not in out.columns:
        return out

    purchase_price_map = get_non_cigar_purchase_price_map(conn)
    if not purchase_price_map:
        return out

    if "qty" not in out.columns:
        out["qty"] = 0.0
    if "net_sales_amount" not in out.columns:
        out["net_sales_amount"] = 0.0
    if "total_korea_cost_krw" not in out.columns:
        out["total_korea_cost_krw"] = 0.0
    if "retail_gross_profit_krw" not in out.columns:
        out["retail_gross_profit_krw"] = 0.0

    out["product_code"] = out["product_code"].fillna("").astype(str).str.strip()
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["net_sales_amount"] = pd.to_numeric(out["net_sales_amount"], errors="coerce").fillna(0)
    out["total_korea_cost_krw"] = pd.to_numeric(out["total_korea_cost_krw"], errors="coerce").fillna(0)
    out["retail_gross_profit_krw"] = pd.to_numeric(out["retail_gross_profit_krw"], errors="coerce").fillna(0)

    # 시가 외 항목만 덮어쓰기
    non_cigar_mask = out["product_code"].isin(purchase_price_map.keys())

    out.loc[non_cigar_mask, "_purchase_price"] = (
        out.loc[non_cigar_mask, "product_code"].map(purchase_price_map).fillna(0)
    )

    out.loc[non_cigar_mask, "total_korea_cost_krw"] = (
        out.loc[non_cigar_mask, "_purchase_price"] * out.loc[non_cigar_mask, "qty"]
    )

    out.loc[non_cigar_mask, "retail_gross_profit_krw"] = (
        out.loc[non_cigar_mask, "net_sales_amount"] - out.loc[non_cigar_mask, "total_korea_cost_krw"]
    )

    if "_purchase_price" in out.columns:
        out = out.drop(columns=["_purchase_price"])

    return out


def calc_kpis(df: pd.DataFrame):
    result = {
        "건수": len(df),
        "주문수": df["order_no"].nunique() if "order_no" in df.columns else 0,
        "판매수량": float(df["qty"].fillna(0).sum()) if "qty" in df.columns else 0,
        "실매출": float(df["net_sales_amount"].fillna(0).sum()) if "net_sales_amount" in df.columns else 0,
        "부가세": float(df["vat_amount"].fillna(0).sum()) if "vat_amount" in df.columns else 0,
    }

    if "total_korea_cost_krw" in df.columns:
        result["원가합계"] = float(df["total_korea_cost_krw"].fillna(0).sum())

    if "total_supply_price_krw" in df.columns:
        result["공급가합계"] = float(df["total_supply_price_krw"].fillna(0).sum())

    if "retail_gross_profit_krw" in df.columns:
        gross_profit = float(df["retail_gross_profit_krw"].fillna(0).sum())
        result["매출총이익"] = gross_profit
        sales = result["실매출"]
        result["마진율"] = (gross_profit / sales * 100) if sales else 0

    return result


def make_excel_download(df_detail: pd.DataFrame, df_product: pd.DataFrame, df_daily: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_detail.to_excel(writer, index=False, sheet_name="상세내역")
        df_product.to_excel(writer, index=False, sheet_name="상품별집계")
        df_daily.to_excel(writer, index=False, sheet_name="일자별집계")
    output.seek(0)
    return output.getvalue()


KRW_COLUMNS = [
    "unit_price",
    "option_price",
    "net_sales_amount",
    "vat_amount",
    "korea_cost_krw",
    "supply_price_krw",
    "retail_price_krw",
    "retail_gross_profit_krw",
    "total_korea_cost_krw",
    "total_supply_price_krw",
]

HEADER_MAP = {
    "sale_date": "판매일자",
    "sale_datetime": "판매일시",
    "order_channel": "주문채널",
    "payment_status": "결제상태",
    "order_no": "주문번호",
    "product_code": "상품코드",
    "product_code_raw": "원본상품코드",
    "mst_product_name": "상품명",
    "mst_size_name": "사이즈",
    "category": "카테고리",
    "qty": "수량",
    "unit_price": "판매단가",
    "option_price": "옵션금액",
    "net_sales_amount": "실매출",
    "vat_amount": "부가세",
    "korea_cost_krw": "한국원가",
    "supply_price_krw": "공급가",
    "retail_price_krw": "소매가",
    "retail_gross_profit_krw": "소매마진",
    "total_korea_cost_krw": "원가합계",
    "total_supply_price_krw": "공급가합계",
    "source_file_name": "원본파일명",
    "source_row_no": "원본행번호",
    "마진율(%)": "마진율(%)",
    "주문수": "주문수",
}


def format_krw(v) -> str:
    try:
        if pd.isna(v):
            return ""
        return f"₩{int(round(float(v))):,}"
    except Exception:
        return v


def prettify_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in KRW_COLUMNS:
        if col in out.columns:
            out[col] = out[col].apply(format_krw)
    out = out.rename(columns={k: v for k, v in HEADER_MAP.items() if k in out.columns})
    return out


def render():
    st.subheader("소매 매출 조회")

    conn = get_conn()
    try:
        has_view = view_exists(conn, "v_retail_sales_enriched")
        has_table = table_exists(conn, "retail_sales")

        if not has_view and not has_table:
            st.error("retail_sales 또는 v_retail_sales_enriched 가 없습니다.")
            return

        channels, statuses, categories, codes = load_filter_values(conn)
        st.caption("시가는 기존 로직, 시가 외 항목은 판매금액 - (매입가 × 수량) 기준으로 마진을 계산합니다.")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_from = st.date_input("시작일", value=None)
        with col2:
            date_to = st.date_input("종료일", value=None)
        with col3:
            selected_channels = st.multiselect("주문채널", channels)
        with col4:
            selected_statuses = st.multiselect("결제상태", statuses, default=["완료"] if "완료" in statuses else [])

        col5, col6, col7 = st.columns(3)
        with col5:
            selected_categories = st.multiselect("카테고리", categories)
        with col6:
            selected_codes = st.multiselect("상품코드", codes)
        with col7:
            keyword = st.text_input("검색어", placeholder="주문번호, 상품코드, 상품명")

        filters = {
            "date_from": str(date_from) if date_from else None,
            "date_to": str(date_to) if date_to else None,
            "channels": selected_channels,
            "statuses": selected_statuses,
            "categories": selected_categories,
            "codes": selected_codes,
            "keyword": keyword,
        }

        sql, params = build_query(use_view=has_view, filters=filters)
        df = pd.read_sql_query(sql, conn, params=params)

        if df.empty:
            st.warning("조회된 데이터가 없습니다.")
            return

        # 핵심 보정: 시가 외 항목만 매입가 기준 재계산
        df = apply_non_cigar_margin_logic(df, conn)

        kpis = calc_kpis(df)
        metric_cols = st.columns(4)
        labels = list(kpis.keys())
        values = list(kpis.values())

        for i, (label, value) in enumerate(zip(labels, values)):
            c = metric_cols[i % 4]
            if label == "마진율":
                c.metric(label, f"{value:.1f}%")
            elif label in ["실매출", "부가세", "원가합계", "공급가합계", "매출총이익"]:
                c.metric(label, f"₩{value:,.0f}")
            elif isinstance(value, float):
                c.metric(label, f"{value:,.0f}")
            else:
                c.metric(label, f"{value:,}")
        st.markdown("### 일별 판매 금액")

        if "sale_date" in df.columns and "net_sales_amount" in df.columns:
            chart_df = df.copy()
            chart_df["sale_date"] = pd.to_datetime(chart_df["sale_date"], errors="coerce")
            chart_df["net_sales_amount"] = pd.to_numeric(chart_df["net_sales_amount"], errors="coerce").fillna(0)

            chart_df = (
                chart_df.dropna(subset=["sale_date"])
                .groupby("sale_date", as_index=False)["net_sales_amount"]
                .sum()
                .sort_values("sale_date", ascending=True)
            )

            fig = go.Figure()

            # 판매금액 라인
            fig.add_trace(go.Scatter(
                x=chart_df["sale_date"],
                y=chart_df["net_sales_amount"],
                mode="lines+markers",
                name="판매금액",
                line=dict(color="#1f77b4", width=2),
                marker=dict(size=5),
                hovertemplate="%{x|%Y-%m-%d}<br>₩%{y:,.0f}<extra></extra>",
            ))

            # 추세선 (선형 회귀) — 데이터가 2개 이상일 때만
            if len(chart_df) >= 2:
                x_numeric = (chart_df["sale_date"] - chart_df["sale_date"].min()).dt.days.values
                coeffs = np.polyfit(x_numeric, chart_df["net_sales_amount"].values, 1)
                trend_values = np.polyval(coeffs, x_numeric)

                fig.add_trace(go.Scatter(
                    x=chart_df["sale_date"],
                    y=trend_values,
                    mode="lines",
                    name="추세선",
                    line=dict(color="red", dash="dash", width=2),
                    hovertemplate="%{x|%Y-%m-%d}<br>추세: ₩%{y:,.0f}<extra></extra>",
                ))

            fig.update_layout(
                xaxis_title="판매일자",
                yaxis_title="판매금액 (₩)",
                yaxis=dict(tickformat=",.0f"),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=40, b=40),
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("일별 판매 금액 그래프를 표시할 수 있는 컬럼이 없습니다.")

        tab1, tab2, tab3 = st.tabs(["상세내역", "상품별 집계", "일자별 집계"])

        with tab1:
            display_cols = [
                c for c in [
                    "sale_date",
                    "sale_datetime",
                    "order_channel",
                    "payment_status",
                    "order_no",
                    "product_code",
                    "product_code_raw",
                    "mst_product_name",
                    "mst_size_name",
                    "category",
                    "qty",
                    "unit_price",
                    "option_price",
                    "net_sales_amount",
                    "vat_amount",
                    "korea_cost_krw",
                    "supply_price_krw",
                    "retail_price_krw",
                    "retail_gross_profit_krw",
                    "total_korea_cost_krw",
                    "source_file_name",
                    "source_row_no",
                ] if c in df.columns
            ]
            df_detail = df[display_cols].copy()
            st.dataframe(prettify_df(df_detail), use_container_width=True, height=520)

        if "mst_product_name" in df.columns:
            group_name_col = "mst_product_name"
        elif "product_code" in df.columns:
            group_name_col = "product_code"
        else:
            group_name_col = "product_code_raw"

        with tab2:
            agg_map = {
                "qty": "sum",
                "net_sales_amount": "sum",
                "vat_amount": "sum",
            }
            for extra in ["total_korea_cost_krw", "total_supply_price_krw", "retail_gross_profit_krw"]:
                if extra in df.columns:
                    agg_map[extra] = "sum"

            group_cols = [c for c in ["product_code", group_name_col, "mst_size_name"] if c in df.columns]

            df_product = (
                df.groupby(group_cols, dropna=False)
                .agg(agg_map)
                .reset_index()
                .sort_values("net_sales_amount", ascending=False)
            )

            if "retail_gross_profit_krw" in df_product.columns:
                df_product["마진율(%)"] = df_product.apply(
                    lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
                    if x["net_sales_amount"] else 0,
                    axis=1,
                )

            st.dataframe(prettify_df(df_product), use_container_width=True, height=520)

        with tab3:
            agg_map_daily = {
                "qty": "sum",
                "net_sales_amount": "sum",
                "vat_amount": "sum",
                "order_no": pd.Series.nunique,
            }
            for extra in ["total_korea_cost_krw", "retail_gross_profit_krw"]:
                if extra in df.columns:
                    agg_map_daily[extra] = "sum"

            df_daily = (
                df.groupby("sale_date", dropna=False)
                .agg(agg_map_daily)
                .reset_index()
                .rename(columns={"order_no": "주문수"})
                .sort_values("sale_date", ascending=False)
            )

            if "retail_gross_profit_krw" in df_daily.columns:
                df_daily["마진율(%)"] = df_daily.apply(
                    lambda x: round((x["retail_gross_profit_krw"] / x["net_sales_amount"] * 100), 1)
                    if x["net_sales_amount"] else 0,
                    axis=1,
                )

            st.dataframe(prettify_df(df_daily), use_container_width=True, height=520)

        excel_bytes = make_excel_download(
            df_detail=df,
            df_product=df_product if "df_product" in locals() else pd.DataFrame(),
            df_daily=df_daily if "df_daily" in locals() else pd.DataFrame(),
        )
        st.download_button(
            "조회결과 엑셀 다운로드",
            data=excel_bytes,
            file_name="retail_sales_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        conn.close()