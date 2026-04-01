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


def inject_css():
    st.markdown(
        """
        <style>
        .menu-card {
            border: 1px solid #E5E7EB;
            border-radius: 22px;
            padding: 20px 20px 18px 20px;
            background: linear-gradient(180deg, #FFFFFF 0%, #FCFBF8 100%);
            box-shadow: 0 2px 12px rgba(0,0,0,0.05);
            margin-bottom: 16px;
            min-height: 420px;
        }

        .menu-title {
            font-size: 1.9rem;
            font-weight: 800;
            color: #231F1A;
            line-height: 1.2;
            margin-bottom: 10px;
            word-break: keep-all;
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

        .menu-empty {
            color: #9CA3AF;
            font-style: italic;
        }

        .size-table-wrap {
            margin-top: 8px;
            border: 1px solid #EEE7DD;
            border-radius: 14px;
            overflow: hidden;
            background: #FFFDFC;
        }

        .size-table {
            width: 100%;
            border-collapse: collapse;
        }

        .size-table thead th {
            background: #F7F2EC;
            color: #7C6A58;
            font-size: 0.82rem;
            font-weight: 700;
            text-align: left;
            padding: 10px 12px;
            border-bottom: 1px solid #E8DED2;
        }

        .size-table thead th.col-center {
            text-align: center;
        }

        .size-table thead th.col-right {
            text-align: right;
        }

        .size-table tbody td {
            padding: 10px 12px;
            border-bottom: 1px solid #F1ECE6;
            font-size: 0.93rem;
            color: #374151;
            vertical-align: middle;
        }

        .size-table tbody tr:last-child td {
            border-bottom: none;
        }

        .size-name {
            font-weight: 700;
            color: #2D2A26;
        }

        .strength-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 700;
            text-align: center;
            min-width: 72px;
        }

        .price-text {
            font-weight: 800;
            color: #4E342E;
            text-align: right;
            white-space: nowrap;
        }

        @media (max-width: 768px) {
            .menu-card {
                min-height: auto;
                padding: 16px;
            }
            .menu-title {
                font-size: 1.45rem;
            }
            .menu-body {
                font-size: 0.92rem;
            }
            .size-table thead th,
            .size-table tbody td {
                padding: 8px 8px;
                font-size: 0.84rem;
            }
            .strength-badge {
                min-width: 60px;
                font-size: 0.72rem;
                padding: 3px 8px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_strength_badge_html(strength: str) -> str:
    s = safe_text(strength, "").lower()

    if not s:
        label = "-"
        bg = "#F3F4F6"
        color = "#6B7280"
    elif "mild" in s or "약" in s:
        label = safe_text(strength)
        bg = "#E8F5E9"
        color = "#1B5E20"
    elif "medium" in s or "중" in s:
        label = safe_text(strength)
        bg = "#FFF8E1"
        color = "#8D6E63"
    elif "full" in s or "강" in s:
        label = safe_text(strength)
        bg = "#FBE9E7"
        color = "#BF360C"
    else:
        label = safe_text(strength)
        bg = "#ECEFF1"
        color = "#37474F"

    return (
        f'<span class="strength-badge" style="background:{bg}; color:{color};">'
        f'{label}'
        f'</span>'
    )


def get_size_sort_key(size_name: str) -> int:
    name = safe_text(size_name, "").strip().lower()

    order_map = {
        "puritos": 1,
        "cigarillo": 2,
        "coronas": 10,
        "corona": 11,
        "short robusto": 20,
        "robusto": 21,
        "toro": 30,
        "torpedo": 40,
        "pyramid": 41,
        "churchill": 50,
    }
    return order_map.get(name, 999)


def build_size_table_html(group_df: pd.DataFrame) -> str:
    temp_df = group_df.copy()

    temp_df["__size_sort"] = temp_df["size_name"].apply(get_size_sort_key)
    temp_df["store_retail_price_krw"] = pd.to_numeric(
        temp_df["store_retail_price_krw"], errors="coerce"
    ).fillna(0)

    sort_cols = ["__size_sort", "size_name"]
    asc = [True, True]

    if "source_row_no" in temp_df.columns:
        sort_cols.append("source_row_no")
        asc.append(True)

    temp_df = temp_df.sort_values(by=sort_cols, ascending=asc)

    body_rows = []
    for _, row in temp_df.iterrows():
        size_name = safe_text(row.get("size_name"))
        strength = safe_text(row.get("strength"), "")
        price = format_krw(row.get("store_retail_price_krw"))
        badge_html = get_strength_badge_html(strength)

        body_rows.append(
            f"""
            <tr>
                <td class="size-name">{size_name}</td>
                <td style="text-align:center;">{badge_html}</td>
                <td class="price-text">{price}</td>
            </tr>
            """
        )

    rows_html = "".join(body_rows)

    return f"""
    <div class="size-table-wrap">
        <table class="size-table">
            <thead>
                <tr>
                    <th>사이즈</th>
                    <th class="col-center">강도</th>
                    <th class="col-right">가격</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """


def draw_grouped_menu_card(group_df: pd.DataFrame):
    first_row = group_df.iloc[0].to_dict()

    product_name = safe_text(first_row.get("product_name"))
    flavor = safe_text(first_row.get("flavor"), "")
    guide = safe_text(first_row.get("guide"), "")

    flavor_html = (
        flavor if flavor else '<span class="menu-empty">등록된 특징 정보가 없습니다.</span>'
    )
    guide_html = (
        guide if guide else '<span class="menu-empty">등록된 가이드 정보가 없습니다.</span>'
    )

    size_table_html = build_size_table_html(group_df)

    st.markdown(
        f"""
        <div class="menu-card">
            <div class="menu-title">{product_name}</div>

            <div class="menu-label">특징</div>
            <div class="menu-body">{flavor_html}</div>

            <div class="menu-label">가이드</div>
            <div class="menu-body">{guide_html}</div>

            <div class="menu-label" style="margin-top:14px;">사이즈별 정보</div>
            {size_table_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


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

    if "source_row_no" not in df.columns:
        df["source_row_no"] = 999999

    df["__min_price"] = df.groupby("product_name")["store_retail_price_krw"].transform("min")
    df["__max_price"] = df.groupby("product_name")["store_retail_price_krw"].transform("max")

    if sort_by == "가격 낮은순":
        df = df.sort_values(
            by=["__min_price", "product_name", "source_row_no", "size_name"],
            ascending=[True, True, True, True]
        )
    elif sort_by == "가격 높은순":
        df = df.sort_values(
            by=["__max_price", "product_name", "source_row_no", "size_name"],
            ascending=[False, True, True, True]
        )
    else:
        df = df.sort_values(
            by=["product_name", "source_row_no", "size_name"],
            ascending=[True, True, True]
        )

    st.caption(f"{len(df['product_name'].dropna().unique())}개 제품 / {len(df)}개 사이즈가 조회되었습니다.")

    with st.expander("목록형으로 보기", expanded=False):
        list_df = df[
            [
                "product_name",
                "size_name",
                "strength",
                "store_retail_price_krw",
                "flavor",
                "guide",
            ]
        ].copy()

        list_df = list_df.rename(
            columns={
                "product_name": "상품명",
                "size_name": "사이즈",
                "strength": "강도",
                "store_retail_price_krw": "매장운영가",
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

    grouped = [g for _, g in df.groupby("product_name", sort=False)]

    card_per_row = 2
    total_rows = math.ceil(len(grouped) / card_per_row)

    for r in range(total_rows):
        cols = st.columns(card_per_row)
        for c in range(card_per_row):
            idx = r * card_per_row + c
            if idx >= len(grouped):
                continue
            with cols[c]:
                draw_grouped_menu_card(grouped[idx])