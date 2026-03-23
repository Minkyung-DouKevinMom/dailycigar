import streamlit as st
from db import (
    get_all_import_batch,
    get_import_item_list_filtered,
    get_import_item_detail,
    update_import_item,
    delete_import_item,
)
def render():
    st.subheader("Import Item 관리")
    st.set_page_config(page_title="Import Item", layout="wide")

    batch_df = get_all_import_batch()

    if batch_df.empty:
        st.info("import_batch 데이터가 없습니다.")
        st.stop()

    batch_options = {"전체": None}
    for _, row in batch_df.iterrows():
        batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2 = st.columns([2, 3])

    with c1:
        selected_label = st.selectbox("Batch 선택", list(batch_options.keys()))
        selected_batch_id = batch_options[selected_label]

    with c2:
        keyword = st.text_input("검색", placeholder="제품명 / 사이즈 / 코드")

    df = get_import_item_list_filtered(selected_batch_id, keyword)

    st.subheader("조회 결과")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        st.info("조회 결과가 없습니다.")
        st.stop()

    item_options = {
        f'{row["id"]} | {row["product_name"]} | {row["size_name"]} | row:{row["source_row_no"]}': row["id"]
        for _, row in df.iterrows()
    }

    st.divider()
    selected_item_label = st.selectbox("수정할 import_item 선택", list(item_options.keys()))
    selected_item_id = item_options[selected_item_label]

    detail_df = get_import_item_detail(selected_item_id)

    if detail_df.empty:
        st.error("상세 데이터를 찾을 수 없습니다.")
        st.stop()

    row = detail_df.iloc[0]

    tab1, tab2, tab3 = st.tabs(["수정", "원본 JSON", "수식 JSON"])

    with tab1:
        st.subheader("Import Item 수정")

        with st.form("import_item_update_form"):
            col1, col2, col3 = st.columns(3)

            with col1:
                product_name = st.text_input("product_name", value=row["product_name"] or "")
                size_name = st.text_input("size_name", value=row["size_name"] or "")
                product_code = st.text_input("product_code", value=row["product_code"] or "")

            with col2:
                import_unit_qty = st.number_input(
                    "import_unit_qty",
                    min_value=0,
                    value=int(row["import_unit_qty"] or 0),
                    step=1
                )
                import_total_cost_krw = st.number_input(
                    "import_total_cost_krw",
                    min_value=0.0,
                    value=float(row["import_total_cost_krw"] or 0),
                    step=1000.0
                )
                total_weight_g = st.number_input(
                    "total_weight_g",
                    min_value=0.0,
                    value=float(row["total_weight_g"] or 0),
                    step=0.1
                )

            with col3:
                retail_price_krw = st.number_input(
                    "retail_price_krw",
                    min_value=0.0,
                    value=float(row["retail_price_krw"] or 0),
                    step=100.0
                )
                supply_price_krw = st.number_input(
                    "supply_price_krw",
                    min_value=0.0,
                    value=float(row["supply_price_krw"] or 0),
                    step=100.0
                )
                margin_krw = st.number_input(
                    "margin_krw",
                    value=float(row["margin_krw"] or 0),
                    step=100.0
                )

            c1, c2 = st.columns(2)
            with c1:
                save_btn = st.form_submit_button("수정 저장")
            with c2:
                delete_btn = st.form_submit_button("삭제")

            if save_btn:
                update_import_item(
                    item_id=selected_item_id,
                    product_name=product_name,
                    size_name=size_name,
                    product_code=product_code or None,
                    import_unit_qty=import_unit_qty if import_unit_qty != 0 else None,
                    import_total_cost_krw=import_total_cost_krw if import_total_cost_krw != 0 else None,
                    total_weight_g=total_weight_g if total_weight_g != 0 else None,
                    retail_price_krw=retail_price_krw if retail_price_krw != 0 else None,
                    supply_price_krw=supply_price_krw if supply_price_krw != 0 else None,
                    margin_krw=margin_krw if margin_krw != 0 else None,
                )
                st.success("수정되었습니다.")
                st.rerun()

            if delete_btn:
                delete_import_item(selected_item_id)
                st.success("삭제되었습니다.")
                st.rerun()

    with tab2:
        st.subheader("raw_row_json")
        st.code(row["raw_row_json"] or "", language="json")

    with tab3:
        st.subheader("raw_formula_json")
        st.code(row["raw_formula_json"] or "", language="json")