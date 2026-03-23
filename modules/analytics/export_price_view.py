import streamlit as st
from db import get_export_price_item_filtered

def render():
    st.subheader("본사 수출가격 조회")
    st.set_page_config(page_title="Export Price Item", layout="wide")
    
    c1, c2, c3 = st.columns(3)

    with c1:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈")

    with c2:
        package_type = st.selectbox("포장 유형", options=["전체", "PACK", "BOX"], index=0)
        package_type = None if package_type == "전체" else package_type

    with c3:
        qty_options = ["전체", 3, 5, 10, 20, 25, 50]
        package_qty = st.selectbox("포장 수량", options=qty_options, index=0)
        package_qty = None if package_qty == "전체" else package_qty

    df = get_export_price_item_filtered(
        keyword=keyword,
        package_type=package_type,
        package_qty=package_qty
    )

    st.subheader("조회 결과")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "id": "ID",
            "product_name": "상품명",
            "size_name": "사이즈",
            "package_type": "포장유형",
            "package_qty": "포장수량",
            "export_price_usd": st.column_config.NumberColumn("수출가격(USD)", format="%.2f"),
            "created_at": "생성일",
        }
    )

    if not df.empty:
        st.subheader("요약")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("건수", len(df))
        with c2:
            avg_val = round(df["export_price_usd"].dropna().mean(), 2) if df["export_price_usd"].notna().any() else 0
            st.metric("평균 USD", avg_val)
        with c3:
            max_val = round(df["export_price_usd"].dropna().max(), 2) if df["export_price_usd"].notna().any() else 0
            st.metric("최대 USD", max_val)
    else:
        st.info("조회 결과가 없습니다.")