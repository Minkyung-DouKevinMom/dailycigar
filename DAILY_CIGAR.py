import os
import pandas as pd
import streamlit as st

from modules.common.dbutil import get_conn, get_db_path, get_table_columns, pick_col, has_table
from modules.common.fmt import fmt_krw, fmt_count
from modules.common.sales_query import load_retail_sales, load_wholesale_sales
from modules.common.upload_status import render_retail_upload_status

st.set_page_config(page_title="Daily Cigar DB", layout="wide")


# =========================
# 공통
# =========================


def metric_with_caption(column, label: str, value: str, caption: str):
    column.metric(label, value)
    column.caption(caption)


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

    if not os.path.exists(get_db_path()):
        st.error(f"DB 파일을 찾을 수 없습니다: {get_db_path()}")
        return

    try:
        with open(get_db_path(), "rb") as f:
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
    """소매 데이터 (정본: modules.common.sales_query.load_retail_sales) → 홈 화면 컬럼명."""
    cols = ["dt", "sales_amount", "margin_amount", "customer_name",
            "sales_type", "product_code", "product_name", "qty", "unit_price"]
    df = load_retail_sales(conn, date_from, date_to)
    if df.empty:
        return pd.DataFrame(columns=cols)
    df = df.rename(columns={"net_sales_amount": "sales_amount", "retail_gross_profit_krw": "margin_amount"})
    df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
    # 상품명이 비어 있으면 마스터에서 보완
    name_map = get_product_name_map(conn)
    missing = df["product_name"].eq("") & df["product_code"].ne("")
    df.loc[missing, "product_name"] = df.loc[missing, "product_code"].map(name_map).fillna("")
    df["sales_type"] = "소매"
    df = df.dropna(subset=["dt"])
    # 매출 0이지만 원가가 있는 라인(무상 제공 등)은 이익에 영향을 주므로 남긴다 — 다른 화면과 이익 합계 일치
    df = df[(df["sales_amount"] != 0) | (df["margin_amount"] != 0)].copy()
    return df[cols]

def get_wholesale_month_data(conn, date_from: str, date_to: str) -> pd.DataFrame:
    """도매 데이터 (정본: modules.common.sales_query.load_wholesale_sales) → 홈 화면 컬럼명."""
    cols = ["dt", "sales_amount", "margin_amount", "customer_name",
            "sales_type", "product_code", "product_name", "qty", "unit_price"]
    df = load_wholesale_sales(conn, date_from, date_to)
    if df.empty:
        return pd.DataFrame(columns=cols)
    df = df.rename(columns={
        "net_sales_amount": "sales_amount",
        "gross_profit_krw": "margin_amount",
        "partner_name": "customer_name",
    })
    df["dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
    name_map = get_product_name_map(conn)
    missing = df["product_name"].eq("") & df["product_code"].ne("")
    df.loc[missing, "product_name"] = df.loc[missing, "product_code"].map(name_map).fillna("")
    df["sales_type"] = "도매"
    df = df.dropna(subset=["dt"])
    # 매출 0이지만 원가가 있는 라인(무상 제공 등)은 이익에 영향을 주므로 남긴다 — 다른 화면과 이익 합계 일치
    df = df[(df["sales_amount"] != 0) | (df["margin_amount"] != 0)].copy()
    return df[cols]


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


def calc_product_sales_highlights(
    df: pd.DataFrame, today: pd.Timestamp, top_n: int = 2
) -> tuple[list[dict], list[dict]]:
    """
    최근 30일 vs 이전 30일, 상품별(소매+도매 합산) 매출 증감 Top N.
    반환: (증가 Top N, 감소 Top N) - 각 원소는
    {"product_name", "recent", "prior", "diff"}
    """
    if df.empty:
        return [], []

    recent_start = today - pd.Timedelta(days=29)
    prior_end = today - pd.Timedelta(days=30)
    prior_start = today - pd.Timedelta(days=59)

    recent = df[(df["dt"] >= recent_start) & (df["dt"] <= today)]
    prior = df[(df["dt"] >= prior_start) & (df["dt"] <= prior_end)]

    def agg(d: pd.DataFrame) -> pd.Series:
        d = d[d["product_name"].astype(str).str.strip() != ""]
        if d.empty:
            return pd.Series(dtype=float)
        return d.groupby("product_name")["sales_amount"].sum()

    recent_g = agg(recent)
    prior_g = agg(prior)
    names = set(recent_g.index) | set(prior_g.index)

    rows = []
    for name in names:
        r = float(recent_g.get(name, 0))
        p = float(prior_g.get(name, 0))
        rows.append({"product_name": name, "recent": r, "prior": p, "diff": r - p})

    gainers = sorted([r for r in rows if r["diff"] > 0], key=lambda x: x["diff"], reverse=True)[:top_n]
    decliners = sorted([r for r in rows if r["diff"] < 0], key=lambda x: x["diff"])[:top_n]
    return gainers, decliners


# =========================
# 장기 미판매 재고
# =========================
def get_current_stock_df(conn) -> pd.DataFrame:
    if not has_table(conn, "product_mst"):
        return pd.DataFrame(columns=["product_code", "product_name", "size_name", "current_stock"])

    sql = """
        SELECT
            p.product_code,
            p.product_name,
            p.size_name,
            COALESCE(si.total_in, 0)
            - COALESCE(rs.retail_out, 0)
            - COALESCE(ws.wholesale_out, 0)
            - COALESCE(so.other_out, 0) AS current_stock
        FROM product_mst p
        LEFT JOIN (
            SELECT i.product_code, SUM(i.import_unit_qty) AS total_in
            FROM import_item i JOIN import_batch b ON i.batch_id = b.id
            WHERE b.import_date <= date('now')
            GROUP BY i.product_code
        ) si ON p.product_code = si.product_code
        LEFT JOIN (
            SELECT product_code, SUM(qty) AS retail_out
            FROM retail_sales WHERE category = 'CIGAR'
            GROUP BY product_code
        ) rs ON p.product_code = rs.product_code
        LEFT JOIN (
            SELECT pm.product_code, SUM(ws.qty) AS wholesale_out
            FROM wholesale_sales ws JOIN product_mst pm ON ws.cigar_product_id = pm.id
            WHERE ws.item_type = 'cigar'
            GROUP BY pm.product_code
        ) ws ON p.product_code = ws.product_code
        LEFT JOIN (
            SELECT product_code, SUM(qty) AS other_out
            FROM stock_out GROUP BY product_code
        ) so ON p.product_code = so.product_code
        WHERE p.use_yn = 'Y'
    """
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame(columns=["product_code", "product_name", "size_name", "current_stock"])

    df["current_stock"] = pd.to_numeric(df["current_stock"], errors="coerce").fillna(0)
    return df


def get_last_sale_date_map(conn) -> pd.DataFrame:
    if not has_table(conn, "retail_sales") and not has_table(conn, "wholesale_sales"):
        return pd.DataFrame(columns=["product_code", "last_sale_date"])

    sql = """
        SELECT product_code, MAX(sale_date) AS last_sale_date FROM (
            SELECT product_code, sale_date FROM retail_sales WHERE category = 'CIGAR'
            UNION ALL
            SELECT pm.product_code, w.sale_date
            FROM wholesale_sales w JOIN product_mst pm ON w.cigar_product_id = pm.id
            WHERE w.item_type = 'cigar'
        )
        GROUP BY product_code
    """
    try:
        return pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame(columns=["product_code", "last_sale_date"])


def calc_long_unsold_stock(conn, today: pd.Timestamp, threshold_days: int = 60) -> pd.DataFrame:
    """현재고 > 0 인데 threshold_days일 이상(또는 판매 이력 자체가 없는) 상품 목록."""
    stock_df = get_current_stock_df(conn)
    if stock_df.empty:
        return pd.DataFrame()

    in_stock = stock_df[stock_df["current_stock"] > 0].copy()
    if in_stock.empty:
        return pd.DataFrame()

    last_sale_df = get_last_sale_date_map(conn)
    merged = in_stock.merge(last_sale_df, on="product_code", how="left")
    merged["last_sale_date"] = pd.to_datetime(merged["last_sale_date"], errors="coerce")
    merged["days_since_sale"] = (today - merged["last_sale_date"]).dt.days
    merged["days_since_sale"] = merged["days_since_sale"].fillna(999999).astype(int)

    flagged = merged[merged["days_since_sale"] >= threshold_days].copy()
    flagged = flagged.sort_values("days_since_sale", ascending=False)
    return flagged[
        ["product_code", "product_name", "size_name", "current_stock", "last_sale_date", "days_since_sale"]
    ]


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
    st.page_link("pages/4_소매관리.py", label="소매관리")
    st.page_link("pages/4_도매관리.py", label="도매관리")
    st.page_link("pages/5_재무관리.py", label="재무관리")
    st.page_link("pages/6_분석.py", label="분석")
    st.divider()
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    st.page_link("pages/9_재고관리.py", label="재고관리📦")

conn = get_conn()

try:
    # 소매 매출 업로드 현황 (누락 방지용 상단 배너)
    render_retail_upload_status(conn, lookback_days=30)

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

    # 전체 기간 그래프용 데이터
    all_time_df = load_period_sales(conn, "2000-01-01", today.strftime("%Y-%m-%d"))

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
        "최근 30일 매출 ÷ 거래건수",
    )

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("전체 기간 매출 추이")

        if all_time_df.empty:
            st.info("표시할 매출 데이터가 없습니다.")
        else:
            import numpy as np
            import altair as alt

            # 일별 집계
            daily_all = (
                all_time_df.assign(date=all_time_df["dt"].dt.normalize())
                .pivot_table(
                    index="date",
                    columns="sales_type",
                    values="sales_amount",
                    aggfunc="sum",
                    fill_value=0,
                )
                .sort_index()
                .reset_index()
            )
            daily_all.columns.name = None
            for col in ["소매", "도매"]:
                if col not in daily_all.columns:
                    daily_all[col] = 0

            # 추세선 계산 (선형회귀)
            def calc_trend(series: pd.Series) -> pd.Series:
                x = np.arange(len(series))
                mask = series > 0
                if mask.sum() < 2:
                    return pd.Series([float("nan")] * len(series), index=series.index)
                coeffs = np.polyfit(x[mask], series[mask], 1)
                return pd.Series(np.polyval(coeffs, x), index=series.index)

            daily_all["소매추세"] = calc_trend(daily_all["소매"])
            daily_all["도매추세"] = calc_trend(daily_all["도매"])

            # Altair 차트 (실적 바 + 추세선)
            base = alt.Chart(daily_all).encode(
                x=alt.X("date:T", title="날짜", axis=alt.Axis(format="%y-%m"))
            )

            bar_retail = base.mark_bar(opacity=0.5, color="#4C72B0").encode(
                y=alt.Y("소매:Q", title="매출액", stack=None),
                tooltip=[
                    alt.Tooltip("date:T", title="날짜", format="%Y-%m-%d"),
                    alt.Tooltip("소매:Q", title="소매", format=",.0f"),
                ],
            )
            bar_wholesale = base.mark_bar(opacity=0.5, color="#DD8452").encode(
                y=alt.Y("도매:Q", title="매출액", stack=None),
                tooltip=[
                    alt.Tooltip("date:T", title="날짜", format="%Y-%m-%d"),
                    alt.Tooltip("도매:Q", title="도매", format=",.0f"),
                ],
            )
            line_retail = base.mark_line(
                color="#1a4f9e", strokeWidth=2, strokeDash=[4, 2]
            ).encode(
                y=alt.Y("소매추세:Q"),
                tooltip=[alt.Tooltip("소매추세:Q", title="소매 추세", format=",.0f")],
            )
            line_wholesale = base.mark_line(
                color="#b05a1a", strokeWidth=2, strokeDash=[4, 2]
            ).encode(
                y=alt.Y("도매추세:Q"),
                tooltip=[alt.Tooltip("도매추세:Q", title="도매 추세", format=",.0f")],
            )

            chart = (
                (bar_retail + bar_wholesale + line_retail + line_wholesale)
                .properties(height=320)
                .configure_view(strokeWidth=0)
            )

            st.altair_chart(chart, use_container_width=True)

            chart_start = daily_all["date"].min()
            chart_end   = daily_all["date"].max()
            st.caption(
                f"전체 기간: {chart_start.strftime('%Y-%m-%d')} - {chart_end.strftime('%Y-%m-%d')} | "
                f"점선: 소매/도매 추세선"
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

    st.markdown("**📈 상품별 매출 증감 하이라이트**")
    st.caption("최근 30일 vs 이전 30일, 소매+도매 합산 상품별 매출")
    gainers, decliners = calc_product_sales_highlights(sales_df, today)
    if not gainers and not decliners:
        st.info("비교할 데이터가 아직 충분하지 않습니다.")
    else:
        hc1, hc2 = st.columns(2)
        with hc1:
            st.markdown("🔼 매출 증가 Top")
            if gainers:
                for g in gainers:
                    pct_str = f"{g['diff'] / g['prior'] * 100:+.0f}%" if g["prior"] > 0 else "신규 판매"
                    st.text(
                        f"• {g['product_name']}: {fmt_krw(g['prior'])} → {fmt_krw(g['recent'])} "
                        f"({pct_str}, +{fmt_krw(g['diff'])})"
                    )
            else:
                st.caption("증가한 상품이 없습니다.")
        with hc2:
            st.markdown("🔽 매출 감소 Top")
            if decliners:
                for d in decliners:
                    pct_str = f"{d['diff'] / d['prior'] * 100:+.0f}%" if d["prior"] > 0 else "-"
                    st.text(
                        f"• {d['product_name']}: {fmt_krw(d['prior'])} → {fmt_krw(d['recent'])} "
                        f"({pct_str}, {fmt_krw(d['diff'])})"
                    )
            else:
                st.caption("감소한 상품이 없습니다.")

    st.divider()

    st.markdown("**📦 장기 미판매 재고**")
    st.caption("현재고가 있으나 60일 이상 판매 이력이 없는 상품 (판매 이력이 아예 없는 상품 포함)")
    unsold_df = calc_long_unsold_stock(conn, today, threshold_days=60)
    if unsold_df.empty:
        st.success("60일 이상 미판매 재고가 없습니다.")
    else:
        view = unsold_df.head(10).copy()
        view["last_sale_date"] = view["last_sale_date"].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "판매이력없음"
        )
        view["days_since_sale"] = view["days_since_sale"].apply(
            lambda x: "판매이력없음" if x >= 999999 else f"{x}일"
        )
        st.dataframe(
            view.rename(columns={
                "product_code": "상품코드",
                "product_name": "상품명",
                "size_name": "사이즈",
                "current_stock": "현재고",
                "last_sale_date": "마지막 판매일",
                "days_since_sale": "경과일수",
            }),
            use_container_width=True,
            hide_index=True,
        )
        if len(unsold_df) > 10:
            st.caption(f"총 {len(unsold_df):,}건 중 상위 10건 표시")

    st.divider()

    render_db_download_section()

    st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")

finally:
    conn.close()