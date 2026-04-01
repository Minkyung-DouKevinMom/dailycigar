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


def get_strength_meta(strength: str):
    s = safe_text(strength, "").lower()

    if not s:
        return {"label": "", "bg": "#F3F4F6", "color": "#6B7280"}

    if "mild" in s or "약" in s:
        return {"label": safe_text(strength), "bg": "#E8F5E9", "color": "#1B5E20"}

    if "medium" in s or "중" in s:
        return {"label": safe_text(strength), "bg": "#FFF8E1", "color": "#8D6E63"}

    if "full" in s or "강" in s:
        return {"label": safe_text(strength), "bg": "#FBE9E7", "color": "#BF360C"}

    return {"label": safe_text(strength), "bg": "#ECEFF1", "color": "#37474F"}


def inject_css():
    st.markdown(
        """
        <style>
        .menu-card {
            border: 1px solid #E5E7EB;
            border-radius: 22px;
            padding: 18px 18px 16px 18px;
            background: linear-gradient(180deg, #FFFFFF 0%, #FCFBF8 100%);
            box-shadow: 0 2px 12px rgba(0,0,0,0.04);
            margin-bottom: 14px;
            min-height: 360px;
        }

        .menu-title {
            font-size: 1.8rem;
            font-weight: 800;
            color: #231F1A;
            line-height: 1.2;
            margin-bottom: 6px;
        }

        .menu-sub {
            font-size: 0.95rem;
            color: #6B7280;
            margin-bottom: 2px;
        }

        .menu-code {
            font-size: 0.8rem;
            color: #9CA3AF;
            margin-bottom: 10px;
        }

        .menu-price {
            font-size: 2rem;
            font-weight: 800;
            color: #4E342E;
            margin-top: 12px;
            margin-bottom: 12px;
        }

        .menu-label {
            font-size: 0.82rem;
            font-weight: 700;
            color: #8B7355;
            margin-top: 10px;
            margin-bottom: 4px;
        }

        .menu-body {
            font-size: 0.95rem;
            line-height: 1.65;
            color: #374151;
            white-space: pre-wrap;
            word-break: keep-all;
        }

        .menu-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 700;
            margin-top: 2px;
            margin-bottom: 4px;
        }

        .menu-empty {
            color: #9CA3AF;
            font-style: italic;
        }

        @media (max-width: 768px) {
            .menu-card {
                min-height: auto;
                padding: 16px;
            }
            .menu-title {
                font-size: 1.5rem;
            }
            .menu-price {
                font-size: 1.7rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_badge(strength: str):
    meta = get_strength_meta(strength)
    if not meta["label"]:
        return

    st.markdown(
        f"""
        <div class="menu-badge" style="background:{meta['bg']}; color:{meta['color']};">
            {meta['label']}
        </div>
        """,
        unsafe_allow_html=True,
    )


def draw_menu_card(row):
    product_name = safe_text(row.get("product_name"))
    size_name = safe_text(row.get("size_name"))
    product_code = safe_text(row.get("product_code"))
    price = format_krw(row.get("store_retail_price_krw"))
    flavor = safe_text(row.get("flavor"), "")
    strength = safe_text(row.get("strength"), "")
    guide = safe_text(row.get("guide"), "")

    flavor_html = flavor if flavor else '<span class="menu-empty">등록된 특징 정보가 없습니다.</span>'
    guide_html = guide if guide else '<span class="menu-empty">등록된 가이드 정보가 없습니다.</span>'

    st.markdown('<div class="menu-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="menu-title">{product_name}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="menu-sub">{size_name}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="menu-code">{product_code}</div>', unsafe_allow_html=True)

    render_badge(strength)

    st.markdown(f'<div class="menu-price">{price}</div>', unsafe_allow_html=True)

    st.markdown('<div class="menu-label">특징</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="menu-body">{flavor_html}</div>', unsafe_allow_html=True)

    st.markdown('<div class="menu-label">가이드</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="menu-body">{guide_html}</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


def render():
    inject_css()

    st.subheader("매장 메뉴판")

    batch_df = get_all_import_batch()
    batch_options = {"전체": None}

    if batch_df is not None and not batch_df.empty:
        for _, row in batch_df.iterrows():
            batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        selected_batch_label = st.selectbox("버전 선택", list(batch_options.keys()))
        batch_id = batch_options[selected_batch_label]

    with c2:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드 / 특징")

    with c3:
        sort_by = st.selectbox("정렬", ["상품명순", "가격 낮은순", "가격 높은순"])

    with c4:
        hide_zero_price = st.checkbox("가격 미등록 제외", value=True)

    df = get_store_menu_view(batch_id=batch_id, keyword=keyword)

    if df is None or df.empty:
        st.info("조회된 메뉴 데이터가 없습니다.")
        return

    df["store_retail_price_krw"] = pd.to_numeric(
        df["store_retail_price_krw"], errors="coerce"
    ).fillna(0)

    if hide_zero_price:
        df = df[df["store_retail_price_krw"] > 0].copy()

    if df.empty:
        st.info("조건에 맞는 메뉴 데이터가 없습니다.")
        return

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
        sort_cols = ["product_name", "size_name"]
        asc = [True, True]

        if "source_row_no" in df.columns:
            sort_cols.append("source_row_no")
            asc.append(True)

        df = df.sort_values(by=sort_cols, ascending=asc)

    st.caption(f"{len(df)}건의 메뉴가 조회되었습니다.")

    with st.expander("목록형으로 보기", expanded=False):
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
    total_rows = math.ceil(len(df) / card_per_row)

    for r in range(total_rows):
        cols = st.columns(card_per_row)
        for c in range(card_per_row):
            idx = r * card_per_row + c
            if idx >= len(df):
                continue
            with cols[c]:
                draw_menu_card(df.iloc[idx].to_dict())