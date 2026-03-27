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


def render_kpi_card(title: str, value: str):
    st.markdown(
        f"""
        <div style="
            background: transparent;
            border: 1px solid rgba(128,128,128,0.35);
            border-radius: 12px;
            padding: 14px 16px;
            min-height: 92px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        ">
            <div style="
                color: inherit;
                font-size: 0.95rem;
                font-weight: 600;
                margin-bottom: 6px;
            ">{title}</div>
            <div style="
                color: inherit;
                font-size: 1.5rem;
                font-weight: 800;
                line-height: 1.2;
            ">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render():
    st.set_page_config(page_title="가격분석 통합조회", layout="wide")
    st.subheader("가격분석 통합조회")

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
        "individual_tax_krw",
        "tobacco_tax_krw",
        "local_education_tax_krw",
        "health_charge_krw",
        "import_vat_krw",
        "import_unit_qty",
        "unit_weight_g",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "supply_price_krw" in df.columns and "korea_cost_krw" in df.columns:
        df["wholesale_margin_krw"] = (
            df["supply_price_krw"].fillna(0) - df["korea_cost_krw"].fillna(0)
        ).clip(lower=0)
    else:
        df["wholesale_margin_krw"] = 0

    if "retail_price_krw" in df.columns and "korea_cost_krw" in df.columns:
        df["retail_margin_krw"] = (
            df["retail_price_krw"].fillna(0) - df["korea_cost_krw"].fillna(0)
        ).clip(lower=0)
    else:
        df["retail_margin_krw"] = 0

    if "retail_price_krw" in df.columns and "supply_price_krw" in df.columns:
        df["retail_extra_margin_krw"] = (
            df["retail_price_krw"].fillna(0) - df["supply_price_krw"].fillna(0)
        ).clip(lower=0)
    else:
        df["retail_extra_margin_krw"] = 0

    df["wholesale_margin_rate"] = df.apply(
        lambda r: (r["wholesale_margin_krw"] / r["supply_price_krw"] * 100)
        if pd.notna(r.get("supply_price_krw")) and r.get("supply_price_krw", 0) > 0
        else 0,
        axis=1,
    )
    df["retail_margin_rate"] = df.apply(
        lambda r: (r["retail_margin_krw"] / r["retail_price_krw"] * 100)
        if pd.notna(r.get("retail_price_krw")) and r.get("retail_price_krw", 0) > 0
        else 0,
        axis=1,
    )

    st.subheader("핵심 지표")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("총 제품 수", f"{len(df):,}")
    with k2:
        total_cost = df["korea_cost_krw"].fillna(0).sum() if "korea_cost_krw" in df.columns else 0
        render_kpi_card("총 한국원가", format_krw(total_cost))
    with k3:
        avg_wholesale = df["wholesale_margin_rate"].fillna(0).mean()
        render_kpi_card("평균 도매 마진율", f"{avg_wholesale:.1f}%")
    with k4:
        avg_retail = df["retail_margin_rate"].fillna(0).mean()
        render_kpi_card("평균 소매 마진율", f"{avg_retail:.1f}%")

        st.subheader("가격 분석 테이블")

    table_df = df.copy()

    # 마진율 숫자 컬럼 유지
    table_df["소매마진율"] = table_df["retail_margin_rate"].fillna(0)
    table_df["도매마진율"] = table_df["wholesale_margin_rate"].fillna(0)

    # 요청 컬럼 우선순위
    preferred_order = [
        "version_name",            # 버전
        "product_code",            # 코드
        "product_name",            # 상품명
        "size_name",               # 사이즈
        "discount_rate",           # 할인율
        "export_unit_price_usd",   # 수출가격(USD)
        "import_unit_cost_krw",    # 수입가격(KRW)
        "unit_weight_g",           # 무게(g)
        "individual_tax_krw",      # 개별소비세
        "tobacco_tax_krw",         # 담배세 (요청 철자 기준)
        "local_education_tax_krw", # 지방교육세
        "health_charge_krw",       # 건강부담금
        "import_vat_krw",          # 부가세
        "tax_total_krw",           # 세금합계
        "korea_cost_krw",          # 한국원가
        "local_box_price_php",     # 현지박스가격(PHP)
        "local_unit_price_php",    # 현지가격(PHP)
        "local_unit_price_krw",    # 현지가격(KRW)
        "retail_price_krw",        # 소매가
        "supply_price_krw",        # 공급가
        "supply_vat_krw",          # 공급가(VAT)
        "margin_krw",              # 파트너마진
        "retail_margin_rate",      # 소매마진율
        "wholesale_margin_rate",   # 도매마진율
        "store_retail_price_krw",  # 매장운영가격
    ]

    # 실제 조회 결과에 있는 컬럼만 사용
    existing_preferred = [c for c in preferred_order if c in table_df.columns]
    remaining_cols = [c for c in table_df.columns if c not in existing_preferred]
    table_df = table_df[existing_preferred + remaining_cols]

    styled_df = table_df.style.apply(highlight_columns, axis=None)

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "version_name": "버전",
            "product_code": "코드",
            "product_name": "상품명",
            "size_name": "사이즈",

            "discount_rate": st.column_config.NumberColumn("할인율", format="%.1f%%"),
            "export_unit_price_usd": st.column_config.NumberColumn("수출가격(USD)", format="$%.2f"),
            "import_unit_cost_krw": st.column_config.NumberColumn("수입가격(KRW)", format="₩%.0f"),
            "unit_weight_g": st.column_config.NumberColumn("무게(g)", format="%.2f"),

            "individual_tax_krw": st.column_config.NumberColumn("개별소비세", format="₩%.0f"),
            "tobacco_tax_krw": st.column_config.NumberColumn("담배세", format="₩%.0f"),
            "local_education_tax_krw": st.column_config.NumberColumn("지방교육세", format="₩%.0f"),
            "health_charge_krw": st.column_config.NumberColumn("건강부담금", format="₩%.0f"),
            "import_vat_krw": st.column_config.NumberColumn("부가세", format="₩%.0f"),
            "tax_total_krw": st.column_config.NumberColumn("세금합계", format="₩%.0f"),
            "korea_cost_krw": st.column_config.NumberColumn("한국원가", format="₩%.0f"),

            "local_box_price_php": st.column_config.NumberColumn("현지박스가격(PHP)", format="₱%.0f"),
            "local_unit_price_php": st.column_config.NumberColumn("현지가격(PHP)", format="₱%.0f"),
            "local_unit_price_krw": st.column_config.NumberColumn("현지가격(KRW)", format="₩%.0f"),

            "retail_price_krw": st.column_config.NumberColumn("소매가", format="₩%.0f"),
            "supply_price_krw": st.column_config.NumberColumn("공급가", format="₩%.0f"),
            "supply_vat_krw": st.column_config.NumberColumn("공급가(VAT)", format="₩%.0f"),
            "margin_krw": st.column_config.NumberColumn("파트너마진", format="₩%.0f"),

            "retail_margin_rate": st.column_config.NumberColumn("소매마진율", format="%.1f%%"),
            "wholesale_margin_rate": st.column_config.NumberColumn("도매마진율", format="%.1f%%"),

            "store_retail_price_krw": st.column_config.NumberColumn("매장운영가격", format="₩%.0f"),

            # 숨김
            "소매마진율": None,
            "도매마진율": None,
            "retail_extra_margin_krw": None,
            "wholesale_margin_krw": None,
            "retail_margin_krw": None,
            "현지가격": None,
            "import_unit_qty": None,
            "total_weight_g": None,
        },
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
            ["소비자가", "공급가", "한국원가", "도매마진", "소매마진"],
        )

    chart_df = df.copy()

    if "product_code" in chart_df.columns:
        chart_df["label"] = chart_df["product_code"].astype(str)
    else:
        product_name = chart_df["product_name"].astype(str) if "product_name" in chart_df.columns else ""
        size_name = chart_df["size_name"].astype(str) if "size_name" in chart_df.columns else ""
        chart_df["label"] = product_name + " / " + size_name

    sort_map = {
        "소비자가": "retail_price_krw",
        "공급가": "supply_price_krw",
        "한국원가": "korea_cost_krw",
        "도매마진": "wholesale_margin_krw",
        "소매마진": "retail_margin_krw",
    }

    sort_col = sort_map[sort_by]
    if sort_col in chart_df.columns:
        chart_df = chart_df.sort_values(sort_col, ascending=False)

    chart_df = chart_df.head(10 if top10_only else 12)

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

    fig_height = min(max(3.8, len(chart_df) * 0.18), 5.2)
    fig, ax = plt.subplots(figsize=(9, fig_height))

    base_vals = chart_df[base_col].fillna(0) if base_col in chart_df.columns else 0
    mid_vals = chart_df[mid_col].fillna(0) if mid_col in chart_df.columns else 0

    ax.barh(chart_df["label"], base_vals, label=legend1)
    ax.barh(chart_df["label"], mid_vals, left=base_vals, label=legend2)

    if top_col is not None and top_col in chart_df.columns:
        top_vals = chart_df[top_col].fillna(0)
        ax.barh(chart_df["label"], top_vals, left=base_vals + mid_vals, label=legend3)

    ax.set_title(chart_title, fontsize=9, fontweight="bold")
    ax.set_xlabel("금액(원)", fontsize=8)
    ax.set_ylabel("제품", fontsize=8)
    ax.legend(fontsize=7)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f"₩{x:,.0f}"))
    ax.tick_params(axis="x", labelsize=7)
    ax.tick_params(axis="y", labelsize=7)

    plt.tight_layout(pad=0.8)
    st.pyplot(fig)

    s1, s2 = st.columns(2)

    with s1:
        if "wholesale_margin_krw" in chart_df.columns:
            top_wholesale = chart_df.nlargest(
                min(5, len(chart_df)),
                "wholesale_margin_krw",
            )[
                ["product_name", "size_name", "wholesale_margin_krw", "wholesale_margin_rate"]
            ].copy()

            top_wholesale["도매마진"] = top_wholesale["wholesale_margin_krw"].apply(format_krw)
            top_wholesale["도매마진율"] = top_wholesale["wholesale_margin_rate"].fillna(0).map(lambda x: f"{x:.1f}%")
            top_wholesale = top_wholesale[["product_name", "size_name", "도매마진", "도매마진율"]]

            st.caption("도매 마진 상위 제품")
            st.table(top_wholesale)

    with s2:
        if "retail_margin_krw" in chart_df.columns:
            top_retail = chart_df.nlargest(
                min(5, len(chart_df)),
                "retail_margin_krw",
            )[
                ["product_name", "size_name", "retail_margin_krw", "retail_margin_rate"]
            ].copy()

            top_retail["소매마진"] = top_retail["retail_margin_krw"].apply(format_krw)
            top_retail["소매마진율"] = top_retail["retail_margin_rate"].fillna(0).map(lambda x: f"{x:.1f}%")
            top_retail = top_retail[["product_name", "size_name", "소매마진", "소매마진율"]]

            st.caption("소매 마진 상위 제품")
            st.table(top_retail)

def highlight_columns(dataframe: pd.DataFrame):
    styles = pd.DataFrame("", index=dataframe.index, columns=dataframe.columns)

    style_map = {
        "individual_tax_krw": "background-color: #FFF8FA; color: #111827;",
        "tobacco_tax_krw": "background-color: #FFF8FA; color: #111827;",
        "local_education_tax_krw": "background-color: #FFF8FA; color: #111827;",
        "health_charge_krw": "background-color: #FFF8FA; color: #111827;",
        "import_vat_krw": "background-color: #FFF8FA; color: #111827;",
        "tax_total_krw": "background-color: #FFF8FA; color: #111827;",
        "korea_cost_krw": "background-color: #FDECEF; color: #111827;",
        "retail_price_krw": "background-color: #EEF9E8; color: #111827;",
        "supply_price_krw": "background-color: #EAF3FF; color: #111827;",
        "store_retail_price_krw": "background-color: #FFF7E8; color: #111827;",
    }

    for col, style in style_map.items():
        if col in styles.columns:
            styles[col] = style

    return styles