import math
import pandas as pd
import streamlit as st

from db import get_all_import_batch, get_store_menu_view


def format_krw(value):
    try:
        if pd.isna(value):
            return "-"
        return f"₩{float(value):,.0f}"
    except Exception:
        return "-"


def safe_text(value, default="-"):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def strength_badge(strength: str) -> str:
    s = safe_text(strength, "").lower()

    if not s:
        return ""

    if "mild" in s or "약" in s:
        bg = "#E8F5E9"
        fg = "#1B5E20"
    elif "medium" in s or "중" in s:
        bg = "#FFF8E1"
        fg = "#8D6E63"
    elif "full" in s or "강" in s:
        bg = "#FBE9E7"
        fg = "#BF360C"
    else:
        bg = "#ECEFF1"
        fg = "#37474F"

    return f"""
        <span style="
            display:inline-block;
            padding:4px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:600;
            background:{bg};
            color:{fg};
            margin-top:6px;
        ">
            {safe_text(strength)}
        </span>
    """


def draw_menu_card(row):
    product_name = safe_text(row.get("product_name"))
    size_name = safe_text(row.get("size_name"))
    product_code = safe_text(row.get("product_code"))
    price = format_krw(row.get("store_retail_price_krw"))
    flavor = safe_text(row.get("flavor"))
    strength = safe_text(row.get("strength"), "")
    guide = safe_text(row.get("guide"))

    badge_html = strength_badge(strength)

    st.markdown(
        f"""
        <div style="
            border:1px solid #E5E7EB;
            border-radius:20px;
            padding:20px 18px;
            min-height:280px;
            background:#FFFFFF;
            box-shadow:0 2px 10px rgba(0,0,0,0.04);
            margin-bottom:14px;
        ">
            <div style="
                font-size:22px;
                font-weight:800;
                color:#2D2A26;
                line-height:1.3;
                margin-bottom:6px;
            ">
                {product_name}
            </div>

            <div style="
                font-size:14px;
                color:#6B7280;
                margin-bottom:2px;
            ">
                {size_name}
            </div>

            <div style="
                font-size:12px;
                color:#9CA3AF;
                margin-bottom:10px;
            ">
                {product_code}
            </div>

            {badge_html}

            <div style="
                font-size:28px;
                font-weight:800;
                color:#4E342E;
                margin-top:18px;
                margin-bottom:14px;
            ">
                {price}
            </div>

            <div style="
                font-size:13px;
                color:#6B7280;
                font-weight:700;
                margin-bottom:4px;
            ">
                특징
            </div>
            <div style="
                font-size:14px;
                color:#374151;
                line-height:1.6;
                margin-bottom:12px;
                min-height:48px;
            ">
                {flavor}
            </div>

            <div style="
                font-size:13px;
                color:#6B7280;
                font-weight:700;
                margin-bottom:4px;
            ">
                가이드
            </div>
            <div style="
                font-size:14px;
                color:#374151;
                line-height:1.6;
                white-space:pre-wrap;
            ">
                {guide}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render():
    st.subheader("매장 메뉴판")

    batch_df = get_all_import_batch()
    batch_options = {"전체": None}

    if not batch_df.empty:
        for _, row in batch_df.iterrows():
            batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2, c3 = st.columns([2, 3, 2])

    with c1:
        selected_batch_label = st.selectbox("버전 선택", list(batch_options.keys()))
        batch_id = batch_options[selected_batch_label]

    with c2:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드 / 특징")

    with c3:
        sort_by = st.selectbox(
            "정렬",
            ["상품명순", "가격 낮은순", "가격 높은순"]
        )

    df = get_store_menu_view(batch_id=batch_id, keyword=keyword)

    if df.empty:
        st.info("조회된 메뉴 데이터가 없습니다.")
        return

    df["store_retail_price_krw"] = pd.to_numeric(
        df["store_retail_price_krw"], errors="coerce"
    ).fillna(0)

    if sort_by == "가격 낮은순":
        df = df.sort_values(
            by=["store_retail_price_krw", "product_name", "size_name"],
            ascending=[True, True, True]
        )
    elif sort_by == "가격 높은순":
        df = df.sort_values(
            by=["store_retail_price_krw", "product_name", "size_name"],
            ascending=[False, True, True]
        )
    else:
        df = df.sort_values(
            by=["product_name", "size_name", "source_row_no"],
            ascending=[True, True, True]
        )

    total_count = len(df)
    st.caption(f"{total_count}건의 메뉴가 조회되었습니다.")

    with st.expander("목록형으로 같이 보기", expanded=False):
        list_df = df[
            [
                "product_code",
                "product_name",
                "size_name",
                "store_retail_price_krw",
                "strength",
                "flavor",
                "guide",
            ]
        ].copy()

        list_df = list_df.rename(
            columns={
                "product_code": "코드",
                "product_name": "상품명",
                "size_name": "사이즈",
                "store_retail_price_krw": "매장운영가",
                "strength": "강도",
                "flavor": "특징",
                "guide": "가이드",
            }
        )

        st.dataframe(
            list_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "매장운영가": st.column_config.NumberColumn(format="₩ %d")
            }
        )

    card_per_row = 3
    rows = math.ceil(len(df) / card_per_row)

    for r in range(rows):
        cols = st.columns(card_per_row)
        for c in range(card_per_row):
            idx = r * card_per_row + c
            if idx >= len(df):
                continue
            with cols[c]:
                draw_menu_card(df.iloc[idx].to_dict())