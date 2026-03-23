import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.ticker import FuncFormatter
from db import get_all_import_batch, get_price_analysis_view

def render():
    st.subheader("가격분석 통합조회")
    st.set_page_config(page_title="가격분석 통합조회", layout="wide")

    # 한글 폰트 설정 (Windows)
    plt.rcParams['font.family'] = 'Malgun Gothic'

    # 마이너스 깨짐 방지
    plt.rcParams['axes.unicode_minus'] = False
    # -----------------------------
    # 공통 함수
    # -----------------------------
    def format_krw(value):
        if pd.isna(value):
            return ""
        return f"₩{value:,.0f}"

    def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
        display_df = df.copy()
        krw_columns = [col for col in display_df.columns if "_krw" in col]
        for col in krw_columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").apply(
                lambda x: format_krw(x) if pd.notnull(x) else ""
            )
        return display_df

    def highlight_columns(dataframe: pd.DataFrame):
        styles = pd.DataFrame("", index=dataframe.index, columns=dataframe.columns)

        if "korea_cost_krw" in styles.columns:
            styles["korea_cost_krw"] = "background-color: #FDECEF;"   # 옅은 핑크

        if "retail_price_krw" in styles.columns:
            styles["retail_price_krw"] = "background-color: #EEF9E8;"  # 옅은 연두

        if "supply_price_krw" in styles.columns:
            styles["supply_price_krw"] = "background-color: #EAF3FF;"  # 옅은 파랑

        return styles

    # -----------------------------
    # 필터
    # -----------------------------
    batch_df = get_all_import_batch()

    batch_options = {"전체": None}
    for _, row in batch_df.iterrows():
        batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2 = st.columns([2, 3])

    with c1:
        selected = st.selectbox("버전 선택", list(batch_options.keys()))
        batch_id = batch_options[selected]

    with c2:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈")

    # -----------------------------
    # 데이터 조회
    # -----------------------------
    df = get_price_analysis_view(batch_id, keyword)

    if df.empty:
        st.info("조회된 데이터가 없습니다.")
        st.stop()

    # 숫자형 보정
    for col in [
        "korea_cost_krw", "supply_price_krw", "retail_price_krw",
        "import_total_cost_krw", "tax_total_all_krw", "margin_krw"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 파생값 계산
    df["wholesale_margin_krw"] = (df["supply_price_krw"] - df["korea_cost_krw"]).clip(lower=0)
    df["retail_margin_krw"] = (df["retail_price_krw"] - df["korea_cost_krw"]).clip(lower=0)
    df["retail_extra_margin_krw"] = (df["retail_price_krw"] - df["supply_price_krw"]).clip(lower=0)

    df["wholesale_margin_rate"] = df.apply(
        lambda r: (r["wholesale_margin_krw"] / r["supply_price_krw"] * 100) if r["supply_price_krw"] > 0 else 0,
        axis=1
    )
    df["retail_margin_rate"] = df.apply(
        lambda r: (r["retail_margin_krw"] / r["retail_price_krw"] * 100) if r["retail_price_krw"] > 0 else 0,
        axis=1
    )

    # -----------------------------
    # KPI 대시보드
    # -----------------------------
    st.subheader("핵심 지표")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("총 제품 수", f"{len(df):,}")
    with k2:
        st.metric("총 한국원가", format_krw(df["korea_cost_krw"].sum()))
    with k3:
        avg_wholesale_rate = df["wholesale_margin_rate"].mean()
        st.metric("평균 도매 마진율", f"{avg_wholesale_rate:.1f}%")
    with k4:
        avg_retail_rate = df["retail_margin_rate"].mean()
        st.metric("평균 소매 마진율", f"{avg_retail_rate:.1f}%")

    # -----------------------------
    # 테이블
    # -----------------------------
    st.subheader("가격 분석 테이블")

    table_df = df.copy()
    #table_df["도매마진"] = table_df["wholesale_margin_krw"]
    table_df["도매마진율"] = table_df["wholesale_margin_rate"].map(lambda x: f"{x:.1f}%")
    #table_df["소매마진"] = table_df["retail_margin_krw"]
    table_df["소매마진율"] = table_df["retail_margin_rate"].map(lambda x: f"{x:.1f}%")

    display_df = make_display_df(table_df)
    styled_df = display_df.style.apply(highlight_columns, axis=None)

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "version_name": "버전",
            "product_name": "상품명",
            "size_name": "사이즈",
            "product_code": "코드",
            "import_unit_qty": "수입개수",
            "unit_weight_g": "개당무게(g)",
            "total_weight_g": None,
            "individual_tax_krw": "개별소비세",
            "tobacco_tax_krw": "담배세",
            "local_education_tax_krw": "지방교육세",
            "health_charge_krw": "건강부담금",
            "import_vat_krw": "부가세",
            "tax_total_all_krw": "세금합계",
            "import_total_cost_krw": "수입금액",
            "korea_cost_krw": "한국원가",
            "retail_price_krw": "소비자가",
            "supply_price_krw": "공급가",
            "store_retail_price_krw" : "매장운영가격",
            "margin_krw": "마진",
            "wholesale_margin_krw": "도매마진",
            "retail_margin_krw" : "소매마진",
            "retail_extra_margin_krw" : None,
            "wholesale_margin_rate" : None,
            "retail_margin_rate" : None
        }
    )

    # -----------------------------
    # 그래프 옵션
    # -----------------------------
    st.subheader("제품별 가격 구조 그래프")

    o1, o2, o3 = st.columns([1.2, 1.2, 1.6])

    with o1:
        view_mode = st.radio(
            "그래프 기준",
            ["소매 기준", "도매 기준"],
            horizontal=True
        )

    with o2:
        top10_only = st.toggle("Top 10만 보기", value=False)

    with o3:
        sort_by = st.selectbox(
            "정렬 기준",
            ["소비자가", "공급가", "한국원가", "도매마진", "소매마진"]
        )

    chart_df = df.copy()
    chart_df["label"] = chart_df["product_name"].astype(str) + " / " + chart_df["size_name"].astype(str)

    sort_map = {
        "소비자가": "retail_price_krw",
        "공급가": "supply_price_krw",
        "한국원가": "korea_cost_krw",
        "도매마진": "wholesale_margin_krw",
        "소매마진": "retail_margin_krw",
    }
    chart_df = chart_df.sort_values(sort_map[sort_by], ascending=False)

    if top10_only:
        chart_df = chart_df.head(10)

    if view_mode == "소매 기준":
        base_col = "korea_cost_krw"
        mid_col = "wholesale_margin_krw"
        top_col = "retail_extra_margin_krw"
        legend1, legend2, legend3 = "한국원가", "도매 마진", "추가 소매 마진"
        chart_title = "제품별 가격 구조 (소매 기준)"
    else:
        base_col = "korea_cost_krw"
        mid_col = "wholesale_margin_krw"
        top_col = None
        legend1, legend2 = "한국원가", "도매 마진"
        chart_title = "제품별 가격 구조 (도매 기준)"

    fig_height = max(6, len(chart_df) * 0.45)
    fig, ax = plt.subplots(figsize=(14, fig_height))

    ax.barh(chart_df["label"], chart_df[base_col], label=legend1)
    ax.barh(
        chart_df["label"],
        chart_df[mid_col],
        left=chart_df[base_col],
        label=legend2
    )

    if top_col is not None:
        ax.barh(
            chart_df["label"],
            chart_df[top_col],
            left=chart_df[base_col] + chart_df[mid_col],
            label=legend3
        )

    ax.set_title(chart_title)
    ax.set_xlabel("금액(원)")
    ax.set_ylabel("제품")
    ax.legend()
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f"₩{x:,.0f}"))
    plt.tight_layout()
    st.pyplot(fig)

    # -----------------------------
    # Top 제품 요약
    # -----------------------------
    s1, s2 = st.columns(2)

    with s1:
        top_wholesale = chart_df.nlargest(min(5, len(chart_df)), "wholesale_margin_krw")[
            ["product_name", "size_name", "wholesale_margin_krw", "wholesale_margin_rate"]
        ].copy()
        top_wholesale["도매마진"] = top_wholesale["wholesale_margin_krw"].map(format_krw)
        top_wholesale["도매마진율"] = top_wholesale["wholesale_margin_rate"].map(lambda x: f"{x:.1f}%")
        top_wholesale = top_wholesale[["product_name", "size_name", "도매마진", "도매마진율"]]
        st.caption("도매 마진 상위 제품")
        st.dataframe(top_wholesale, use_container_width=True, hide_index=True)

    with s2:
        top_retail = chart_df.nlargest(min(5, len(chart_df)), "retail_margin_krw")[
            ["product_name", "size_name", "retail_margin_krw", "retail_margin_rate"]
        ].copy()
        top_retail["소매마진"] = top_retail["retail_margin_krw"].map(format_krw)
        top_retail["소매마진율"] = top_retail["retail_margin_rate"].map(lambda x: f"{x:.1f}%")
        top_retail = top_retail[["product_name", "size_name", "소매마진", "소매마진율"]]
        st.caption("소매 마진 상위 제품")
        st.dataframe(top_retail, use_container_width=True, hide_index=True)
