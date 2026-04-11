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


def normalize_strength(strength: str) -> str:
    s = safe_text(strength, "")
    return s if s else "-"


def build_size_table_df(group_df: pd.DataFrame) -> pd.DataFrame:
    temp_df = group_df.copy()

    temp_df["__size_sort"] = temp_df["size_name"].apply(get_size_sort_key)
    temp_df["store_retail_price_krw"] = pd.to_numeric(
        temp_df["store_retail_price_krw"], errors="coerce"
    ).fillna(0)

    if "profile_id" not in temp_df.columns:
        temp_df["profile_id"] = 999999

    sort_cols = ["profile_id", "__size_sort", "size_name"]
    asc = [True, True, True]

    if "source_row_no" in temp_df.columns:
        sort_cols.append("source_row_no")
        asc.append(True)

    temp_df = temp_df.sort_values(by=sort_cols, ascending=asc)

    out_df = pd.DataFrame({
        "사이즈": temp_df["size_name"].apply(lambda x: safe_text(x)),
        "강도": temp_df["strength"].apply(normalize_strength),
        "길이": temp_df["length_mm"].apply(
            lambda x: f"{int(float(x))}mm" if pd.notna(x) and str(x).strip() != "" else "-"
        ) if "length_mm" in temp_df.columns else "-",
        "링게이지": temp_df["ring_gauge"].apply(
            lambda x: f"{int(float(x))}" if pd.notna(x) and str(x).strip() != "" else "-"
        ) if "ring_gauge" in temp_df.columns else "-",
        "가격": temp_df["store_retail_price_krw"],
    })

    return out_df

def draw_grouped_menu_card(group_df: pd.DataFrame, card_no: int):
    first_row = group_df.iloc[0].to_dict()

    product_name = safe_text(first_row.get("product_name"))
    flavor = safe_text(first_row.get("flavor"), "")
    guide = safe_text(first_row.get("guide"), "")

    if not flavor:
        flavor = "등록된 특징 정보가 없습니다."
    if not guide:
        guide = "등록된 가이드 정보가 없습니다."

    size_df = build_size_table_df(group_df)

    with st.container(border=True):
        st.subheader(product_name)

        st.caption("특징")
        st.write(flavor)

        st.caption("가이드")
        st.write(guide)

        st.caption("사이즈별 정보")
        st.dataframe(
            size_df,
            use_container_width=True,
            hide_index=True,
            key=f"menu_size_df_{card_no}",
            column_config={
                "가격": st.column_config.NumberColumn("가격", format="₩ %d")
            }
        )


def render():
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

    if "profile_id" not in df.columns:
        df["profile_id"] = 999999

    df["__min_price"] = df.groupby("product_name")["store_retail_price_krw"].transform("min")
    df["__max_price"] = df.groupby("product_name")["store_retail_price_krw"].transform("max")

    if sort_by == "가격 낮은순":
        df = df.sort_values(
            by=["__min_price", "profile_id", "source_row_no", "size_name"],
            ascending=[True, True, True, True]
        )
    elif sort_by == "가격 높은순":
        df = df.sort_values(
            by=["__max_price", "profile_id", "source_row_no", "size_name"],
            ascending=[False, True, True, True]
        )
    else:
        df = df.sort_values(
            by=["profile_id", "source_row_no", "size_name"],
            ascending=[True, True, True]
        )

    product_count = len(df["product_name"].dropna().unique())
    size_count = len(df)
    st.caption(f"{product_count}개 제품 / {size_count}개 사이즈가 조회되었습니다.")

    with st.expander("목록형으로 보기", expanded=False):
        list_df = df[
            [
                "product_name",
                "size_name",
                "strength",
                "length_mm",
                "ring_gauge",
                "store_retail_price_krw",
                "flavor",
                "guide",
                "profile_id",
                "source_row_no",
            ]
        ].copy()

        list_df = list_df.rename(
            columns={
                "product_name": "상품명",
                "size_name": "사이즈",
                "strength": "강도",
                "length_mm": "길이(mm)",
                "ring_gauge": "링게이지",
                "store_retail_price_krw": "매장운영가",
                "flavor": "특징",
                "guide": "가이드",
                "profile_id": "프로파일ID",
                "source_row_no": "원본행순서",
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

    card_no = 0
    for r in range(total_rows):
        cols = st.columns(card_per_row)
        for c in range(card_per_row):
            idx = r * card_per_row + c
            if idx >= len(grouped):
                continue
            with cols[c]:
                draw_grouped_menu_card(grouped[idx], card_no)
                card_no += 1