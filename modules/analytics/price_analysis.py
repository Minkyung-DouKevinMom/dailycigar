import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.ticker import FuncFormatter
from db import get_all_import_batch, get_price_analysis_view


def setup_korean_font():
    candidates = [
        "Malgun Gothic",
        "AppleGothic",
        "NanumGothic",
        "NanumBarunGothic",
        "DejaVu Sans",
    ]
    available = {f.name for f in fm.fontManager.ttflist}

    for name in candidates:
        if name in available:
            plt.rcParams["font.family"] = name
            break

    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["font.size"] = 9


def format_krw(value):
    if pd.isna(value):
        return ""
    return f"₩{value:,.0f}"


def format_php(value):
    if pd.isna(value):
        return ""
    return f"₱{value:,.0f}"


def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()

    krw_cols = [
        "korea_cost_krw",
        "retail_price_krw",
        "supply_price_krw",
        "store_retail_price_krw",
        "import_total_cost_krw",
        "tax_total_all_krw",
        "margin_krw",
        "wholesale_margin_krw",
        "retail_margin_krw",
        "현지가격",
    ]

    for col in krw_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").apply(
                lambda x: format_krw(x) if pd.notnull(x) else ""
            )

    php_cols = ["local_unit_price_php", "local_box_price_php"]
    for col in php_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").apply(
                lambda x: format_php(x) if pd.notnull(x) else ""
            )

    return display_df


def highlight_columns(dataframe: pd.DataFrame):
    styles = pd.DataFrame("", index=dataframe.index, columns=dataframe.columns)

    base_style = "color: #000000 !important;"
    if "korea_cost_krw" in styles.columns:
        styles["korea_cost_krw"] = base_style + "background-color: #FDECEF;"
    if "retail_price_krw" in styles.columns:
        styles["retail_price_krw"] = base_style + "background-color: #EEF9E8;"
    if "supply_price_krw" in styles.columns:
        styles["supply_price_krw"] = base_style + "background-color: #EAF3FF;"
    if "현지가격" in styles.columns:
        styles["현지가격"] = base_style + "background-color: #F4F0FF;"
    if "store_retail_price_krw" in styles.columns:
        styles["store_retail_price_krw"] = base_style + "background-color: #FFF7E8;"

    return styles


def render_kpi_card(title: str, value: str):
    st.markdown(
        f"""
        <div style="
            background:#f7f7f7;
            border:1px solid #dddddd;
            border-radius:12px;
            padding:14px 16px;
            min-height:92px;
            display:flex;
            flex-direction:column;
            justify-content:center;
        ">
            <div style="
                color:#000000 !important;
                font-size:0.95rem;
                font-weight:600;
                margin-bottom:6px;
            ">{title}</div>
            <div style="
                color:#000000 !important;
                font-size:1.5rem;
                font-weight:800;
                line-height:1.2;
            ">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render():
    st.set_page_config(page_title="가격분석 통합조회", layout="wide")
    st.subheader("가격분석 통합조회")

    st.markdown("""
    <style>
    [data-testid="stMetricLabel"] div,
    [data-testid="stMetricValue"] div,
    [data-testid="stDataFrame"] div,
    [data-testid="stTable"] div {
        color: #000000 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    setup_korean_font()

    batch_df = get_all_import_batch()

    batch_options = {"전체": None}
    for _, row in batch_df.iterrows():
        batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2 = st.columns([2, 3])

    with c1:
        selected = st.selectbox("버전 선택", list(batch_options.keys()))
        batch_id = batch_options[selected]

    with c2:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드")

    df = get_price_analysis_view(batch_id, keyword)

    if df.empty:
        st.info("조회된 데이터가 없습니다.")
        st.stop()

    numeric_cols = [
        "korea_cost_krw",
        "supply_price_krw",
        "retail_price_krw",
        "store_retail_price_krw",
        "import_total_cost_krw",
        "tax_total_all_krw",
        "margin_krw",
        "local_box_price_php",
        "local_unit_price_php",
        "local_unit_price_krw",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "local_unit_price_krw" in df.columns:
        df["현지가격"] = pd.to_numeric(df["local_unit_price_krw"], errors="coerce").fillna(0)
    else:
        df["현지가격"] = 0

    df["wholesale_margin_krw"] = (df["supply_price_krw"] - df["korea_cost_krw"]).clip(lower=0)
    df["retail_margin_krw"] = (df["retail_price_krw"] - df["korea_cost_krw"]).clip(lower=0)
    df["retail_extra_margin_krw"] = (df["retail_price_krw"] - df["supply_price_krw"]).clip(lower=0)

    df["wholesale_margin_rate"] = df.apply(
        lambda r: (r["wholesale_margin_krw"] / r["supply_price_krw"] * 100)
        if r["supply_price_krw"] > 0 else 0,
        axis=1
    )
    df["retail_margin_rate"] = df.apply(
        lambda r: (r["retail_margin_krw"] / r["retail_price_krw"] * 100)
        if r["retail_price_krw"] > 0 else 0,
        axis=1
    )

    st.subheader("핵심 지표")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("총 제품 수", f"{len(df):,}")
    with k2:
        render_kpi_card("총 한국원가", format_krw(df["korea_cost_krw"].sum()))
    with k3:
        render_kpi_card("평균 도매 마진율", f'{df["wholesale_margin_rate"].mean():.1f}%')
    with k4:
        render_kpi_card("평균 소매 마진율", f'{df["retail_margin_rate"].mean():.1f}%')

    st.subheader("가격 분석 테이블")

    table_df = df.copy()
    table_df["도매마진율"] = table_df["wholesale_margin_rate"].map(lambda x: f"{x:.1f}%")
    table_df["소매마진율"] = table_df["retail_margin_rate"].map(lambda x: f"{x:.1f}%")

    preferred_order = [
        "version_name",
        "product_code",
        "product_name",
        "size_name",
        "import_unit_qty",
        "unit_weight_g",
        "individual_tax_krw",
        "tobacco_tax_krw",
        "local_education_tax_krw",
        "health_charge_krw",
        "import_vat_krw",
        "tax_total_all_krw",
        "import_total_cost_krw",
        "korea_cost_krw",
        "retail_price_krw",
        "supply_price_krw",
        "현지가격",
        "store_retail_price_krw",
        "margin_krw",
        "wholesale_margin_krw",
        "도매마진율",
        "retail_margin_krw",
        "소매마진율",
        "local_unit_price_php",
        "local_box_price_php",
    ]

    existing_preferred = [c for c in preferred_order if c in table_df.columns]
    remaining_cols = [c for c in table_df.columns if c not in existing_preferred]
    table_df = table_df[existing_preferred + remaining_cols]

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
            "현지가격": "현지가격",
            "store_retail_price_krw": "매장운영가격",
            "local_unit_price_php": "현지가격(PHP)",
            "local_box_price_php": "박스가격(PHP)",
            "margin_krw": "마진",
            "wholesale_margin_krw": "도매마진",
            "retail_margin_krw": "소매마진",
            "retail_extra_margin_krw": None,
            "wholesale_margin_rate": None,
            "retail_margin_rate": None,
            "local_unit_price_krw": None,
        }
    )

    st.subheader("제품별 가격 구조 그래프")

    o1, o2, o3 = st.columns([1.2, 1.2, 1.6])

    with o1:
        view_mode = st.radio("그래프 기준", ["소매 기준", "도매 기준"], horizontal=True)

    with o2:
        top10_only = st.toggle("Top 10만 보기", value=False)

    with o3:
        sort_by = st.selectbox(
            "정렬 기준",
            ["소비자가", "공급가", "한국원가", "도매마진", "소매마진"]
        )

    chart_df = df.copy()
    if "product_code" in chart_df.columns:
        chart_df["label"] = chart_df["product_code"].astype(str)
    else:
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
    else:
        chart_df = chart_df.head(15)

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

    fig_height = min(max(4.5, len(chart_df) * 0.22), 7.0)
    fig, ax = plt.subplots(figsize=(10, fig_height))

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

    ax.set_title(chart_title, fontsize=11, fontweight="bold")
    ax.set_xlabel("금액(원)", fontsize=9)
    ax.set_ylabel("제품", fontsize=9)
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f"₩{x:,.0f}"))
    ax.tick_params(axis="x", labelsize=8)
    ax.tick_params(axis="y", labelsize=8)

    plt.tight_layout()
    st.pyplot(fig)

    s1, s2 = st.columns(2)

    with s1:
        top_wholesale = chart_df.nlargest(min(5, len(chart_df)), "wholesale_margin_krw")[
            ["product_name", "size_name", "wholesale_margin_krw", "wholesale_margin_rate"]
        ].copy()
        top_wholesale["도매마진"] = top_wholesale["wholesale_margin_krw"].map(format_krw)
        top_wholesale["도매마진율"] = top_wholesale["wholesale_margin_rate"].map(lambda x: f"{x:.1f}%")
        top_wholesale = top_wholesale[["product_name", "size_name", "도매마진", "도매마진율"]]

        st.caption("도매 마진 상위 제품")
        st.table(top_wholesale)

    with s2:
        top_retail = chart_df.nlargest(min(5, len(chart_df)), "retail_margin_krw")[
            ["product_name", "size_name", "retail_margin_krw", "retail_margin_rate"]
        ].copy()
        top_retail["소매마진"] = top_retail["retail_margin_krw"].map(format_krw)
        top_retail["소매마진율"] = top_retail["retail_margin_rate"].map(lambda x: f"{x:.1f}%")
        top_retail = top_retail[["product_name", "size_name", "소매마진", "소매마진율"]]

        st.caption("소매 마진 상위 제품")
        st.table(top_retail)