import streamlit as st

from db import (
    get_all_import_batch,
    get_import_item_list_filtered,
    get_import_item_detail,
    insert_import_item,
    update_import_item,
    delete_import_item,
)


def _none_if_blank_text(v):
    s = str(v).strip()
    return s if s else None


def _none_if_zero_int(v):
    return None if int(v) == 0 else int(v)


def _none_if_zero_float(v):
    return None if float(v) == 0 else float(v)


def render():
    st.set_page_config(page_title="Import Item", layout="wide")
    st.subheader("Import Item 관리")

    batch_df = get_all_import_batch()

    if batch_df.empty:
        st.info("import_batch 데이터가 없습니다.")
        st.stop()

    batch_options = {"선택하세요": None}
    for _, row in batch_df.iterrows():
        batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2 = st.columns([2, 3])

    with c1:
        selected_label = st.selectbox("수입버전 선택", list(batch_options.keys()))
        selected_batch_id = batch_options[selected_label]

    with c2:
        keyword = st.text_input("검색", placeholder="제품명 / 사이즈 / 코드")

    if selected_batch_id is None:
        st.info("먼저 수입버전을 선택해 주세요.")
        st.stop()

    df = get_import_item_list_filtered(selected_batch_id, keyword)

    st.subheader("조회 결과")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    tab_add, tab_edit, tab_raw, tab_formula = st.tabs(
        ["신규 추가", "수정", "원본 JSON", "수식 JSON"]
    )

    # ---------------------------
    # 신규 추가
    # ---------------------------
    with tab_add:
        st.subheader("수입제품 신규 추가")

        with st.form("import_item_insert_form", clear_on_submit=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                product_name_new = st.text_input("product_name", value="")
                size_name_new = st.text_input("size_name", value="")
                product_code_new = st.text_input("product_code", value="")

            with col2:
                import_unit_qty_new = st.number_input(
                    "import_unit_qty",
                    min_value=0,
                    value=0,
                    step=1,
                    key="insert_import_unit_qty",
                )
                import_total_cost_krw_new = st.number_input(
                    "import_total_cost_krw",
                    min_value=0.0,
                    value=0.0,
                    step=1000.0,
                    key="insert_import_total_cost_krw",
                )
                total_weight_g_new = st.number_input(
                    "total_weight_g",
                    min_value=0.0,
                    value=0.0,
                    step=0.1,
                    key="insert_total_weight_g",
                )

            with col3:
                retail_price_krw_new = st.number_input(
                    "retail_price_krw",
                    min_value=0.0,
                    value=0.0,
                    step=100.0,
                    key="insert_retail_price_krw",
                )
                supply_price_krw_new = st.number_input(
                    "supply_price_krw",
                    min_value=0.0,
                    value=0.0,
                    step=100.0,
                    key="insert_supply_price_krw",
                )
                margin_krw_new = st.number_input(
                    "margin_krw",
                    min_value=0.0,
                    value=0.0,
                    step=100.0,
                    key="insert_margin_krw",
                )

            source_row_no_new = st.number_input(
                "source_row_no",
                min_value=0,
                value=0,
                step=1,
                key="insert_source_row_no",
                help="엑셀 원본 행번호가 없으면 0으로 두세요.",
            )

            add_btn = st.form_submit_button("신규 저장")

            if add_btn:
                if not str(product_name_new).strip():
                    st.error("product_name은 필수입니다.")
                else:
                    insert_import_item(
                        batch_id=selected_batch_id,
                        product_name=str(product_name_new).strip(),
                        size_name=_none_if_blank_text(size_name_new),
                        product_code=_none_if_blank_text(product_code_new),
                        import_unit_qty=_none_if_zero_int(import_unit_qty_new),
                        import_total_cost_krw=_none_if_zero_float(import_total_cost_krw_new),
                        total_weight_g=_none_if_zero_float(total_weight_g_new),
                        retail_price_krw=_none_if_zero_float(retail_price_krw_new),
                        supply_price_krw=_none_if_zero_float(supply_price_krw_new),
                        margin_krw=_none_if_zero_float(margin_krw_new),
                        source_row_no=_none_if_zero_int(source_row_no_new),
                    )
                    st.success("신규 import_item이 추가되었습니다.")
                    st.rerun()

    # ---------------------------
    # 수정 대상이 없을 때
    # ---------------------------
    if df.empty:
        with tab_edit:
            st.info("등록된 데이터가 없습니다. 먼저 '신규 추가' 탭에서 입력해 주세요.")
        with tab_raw:
            st.info("표시할 원본 JSON이 없습니다.")
        with tab_formula:
            st.info("표시할 수식 JSON이 없습니다.")
        return

    item_options = {
        f'{row["id"]} | {row["product_name"]} | {row["size_name"]} | row:{row["source_row_no"]}': row["id"]
        for _, row in df.iterrows()
    }

    # 기본 선택
    selected_item_label = list(item_options.keys())[0]
    selected_item_id = item_options[selected_item_label]

    # ---------------------------
    # 수정
    # ---------------------------
    with tab_edit:
        selected_item_label = st.selectbox(
            "수정할 수입제품 선택",
            list(item_options.keys()),
            key="edit_item_selectbox",
        )
        selected_item_id = item_options[selected_item_label]

        detail_df = get_import_item_detail(selected_item_id)

        if detail_df.empty:
            st.error("상세 데이터를 찾을 수 없습니다.")
        else:
            row = detail_df.iloc[0]

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
                        step=1,
                        key="edit_import_unit_qty",
                    )
                    import_total_cost_krw = st.number_input(
                        "import_total_cost_krw",
                        min_value=0.0,
                        value=float(row["import_total_cost_krw"] or 0),
                        step=1000.0,
                        key="edit_import_total_cost_krw",
                    )
                    total_weight_g = st.number_input(
                        "total_weight_g",
                        min_value=0.0,
                        value=float(row["total_weight_g"] or 0),
                        step=0.1,
                        key="edit_total_weight_g",
                    )

                with col3:
                    retail_price_krw = st.number_input(
                        "retail_price_krw",
                        min_value=0.0,
                        value=float(row["retail_price_krw"] or 0),
                        step=100.0,
                        key="edit_retail_price_krw",
                    )
                    supply_price_krw = st.number_input(
                        "supply_price_krw",
                        min_value=0.0,
                        value=float(row["supply_price_krw"] or 0),
                        step=100.0,
                        key="edit_supply_price_krw",
                    )
                    margin_krw = st.number_input(
                        "margin_krw",
                        min_value=0.0,
                        value=float(row["margin_krw"] or 0),
                        step=100.0,
                        key="edit_margin_krw",
                    )

                c1, c2 = st.columns(2)
                with c1:
                    save_btn = st.form_submit_button("수정 저장")
                with c2:
                    delete_btn = st.form_submit_button("삭제")

                if save_btn:
                    if not str(product_name).strip():
                        st.error("product_name은 필수입니다.")
                    else:
                        update_import_item(
                            item_id=selected_item_id,
                            product_name=str(product_name).strip(),
                            size_name=_none_if_blank_text(size_name),
                            product_code=_none_if_blank_text(product_code),
                            import_unit_qty=_none_if_zero_int(import_unit_qty),
                            import_total_cost_krw=_none_if_zero_float(import_total_cost_krw),
                            total_weight_g=_none_if_zero_float(total_weight_g),
                            retail_price_krw=_none_if_zero_float(retail_price_krw),
                            supply_price_krw=_none_if_zero_float(supply_price_krw),
                            margin_krw=_none_if_zero_float(margin_krw),
                        )
                        st.success("수정되었습니다.")
                        st.rerun()

                if delete_btn:
                    delete_import_item(selected_item_id)
                    st.success("삭제되었습니다.")
                    st.rerun()

    # ---------------------------
    # JSON 표시
    # ---------------------------
    if selected_item_id is not None:
        detail_df = get_import_item_detail(selected_item_id)

        if not detail_df.empty:
            row = detail_df.iloc[0]

            with tab_raw:
                st.subheader("원본 JSON")
                st.code(row["raw_row_json"] or "", language="json")

            with tab_formula:
                st.subheader("수식 JSON")
                st.code(row["raw_formula_json"] or "", language="json")