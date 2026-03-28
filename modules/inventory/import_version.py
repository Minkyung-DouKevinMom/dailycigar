import streamlit as st
from db import (
    get_all_import_batch,
    get_all_tax_rule,
    get_import_items_by_batch,
    get_import_batch_detail,
    create_import_batch,
    update_import_batch,
    delete_import_batch,
)

def _to_none_str(v):
    v = (v or "").strip()
    return v if v else None

def _to_none_num(v):
    return v if v not in (None, 0, 0.0) else None


def _build_tax_rule_options():
    tax_rule_df = get_all_tax_rule()
    if tax_rule_df.empty:
        return {}, []

    option_map = {}
    option_labels = []
    for _, row in tax_rule_df.iterrows():
        label = f'{row["id"]} | {row["rule_name"]} | {row["effective_from"]}'
        option_labels.append(label)
        option_map[label] = int(row["id"])
    return option_map, option_labels

def render():
    st.subheader("수입 버전 관리")
    st.set_page_config(page_title="Import Batch", layout="wide")

    tab_list, tab_create, tab_edit, tab_delete = st.tabs(
        ["조회", "신규 등록", "수정", "삭제"]
    )

    # ---------------------------
    # 조회
    # ---------------------------
    with tab_list:
        batch_df = get_all_import_batch()

        if batch_df.empty:
            st.info("import_batch 데이터가 없습니다.")
        else:
            st.subheader("버전 목록")
            st.dataframe(
                batch_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id": "ID",
                    "version_name": "버전명",
                    "import_date": "수입일",
                    "supplier_name": "공급처",
                    "usd_to_krw_rate": st.column_config.NumberColumn("USD 환율", format="%.2f"),
                    "php_to_krw_rate": st.column_config.NumberColumn("PHP 환율", format="%.2f"),
                    "local_markup_rate": st.column_config.NumberColumn("현지 마크업", format="%.4f"),
                    "tax_rule_id": "세금규칙ID",
                    "total_item_count": "품목 수",
                    "total_unit_qty": "총 개수",
                    "total_weight_g": st.column_config.NumberColumn("총 무게(g)", format="%.2f"),
                    "total_amount_usd": st.column_config.NumberColumn("총 금액(USD)", format="%.2f"),
                    "total_amount_krw": st.column_config.NumberColumn("총 금액(KRW)", format="₩%.0f"),
                    "notes": "비고",
                    "created_at": "생성일",
                }
            )

            batch_options = {
                f'{row["id"]} | {row["version_name"]}': row["id"]
                for _, row in batch_df.iterrows()
            }

            selected_label = st.selectbox("상세 조회할 버전", list(batch_options.keys()), key="view_batch")
            selected_batch_id = batch_options[selected_label]

            st.divider()
            st.subheader("선택한 버전 상세")

            item_df = get_import_items_by_batch(selected_batch_id)
            st.dataframe(
                item_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id": "ID",
                    "product_name": "상품명",
                    "size_name": "사이즈",
                    "product_code": "코드",
                    "import_unit_qty": "수입개수",
                    "import_total_cost_krw": st.column_config.NumberColumn("수입금액", format="₩%.0f"),
                    "total_weight_g": st.column_config.NumberColumn("전체무게(g)", format="%.2f"),
                    "retail_price_krw": st.column_config.NumberColumn("소비자가", format="₩%.0f"),
                    "supply_price_krw": st.column_config.NumberColumn("공급가", format="₩%.0f"),
                    "margin_krw": st.column_config.NumberColumn("마진", format="₩%.0f"),
                    "source_row_no": "원본행",
                }
            )

    # ---------------------------
    # 신규 등록
    # ---------------------------
    with tab_create:
        st.subheader("수입 버전 신규 등록")

        tax_rule_map, tax_rule_labels = _build_tax_rule_options()

        with st.form("create_import_batch_form"):
            c1, c2, c3 = st.columns(3)

            with c1:
                version_name = st.text_input("버전명 *", placeholder="예: 2026-03 Manila Batch")
                import_date = st.date_input("수입일", value=None)
            with c2:
                supplier_name = st.text_input("공급처", placeholder="예: Tabacalera")
                usd_to_krw_rate = st.number_input("USD 환율", min_value=0.0, value=0.0, step=0.01)
            with c3:
                php_to_krw_rate = st.number_input("PHP 환율", min_value=0.0, value=0.0, step=0.01)
                local_markup_rate = st.number_input("현지 마크업", min_value=0.0, value=0.0, step=0.0001, format="%.4f")

            if tax_rule_labels:
                selected_tax_rule_label = st.selectbox("세금 규칙 *", tax_rule_labels, key="create_tax_rule")
                selected_tax_rule_id = tax_rule_map[selected_tax_rule_label]
            else:
                selected_tax_rule_id = None
                st.warning("세금 규칙 데이터가 없습니다. 먼저 세금 규칙을 등록해 주세요.")

            notes = st.text_area("비고", height=120)

            submitted = st.form_submit_button("신규 등록")

            if submitted:
                if not version_name.strip():
                    st.error("버전명은 필수입니다.")
                elif not selected_tax_rule_id:
                    st.error("세금 규칙을 선택해 주세요.")
                else:
                    create_import_batch(
                        version_name=version_name.strip(),
                        import_date=str(import_date) if import_date else None,
                        supplier_name=_to_none_str(supplier_name),
                        usd_to_krw_rate=_to_none_num(usd_to_krw_rate),
                        php_to_krw_rate=_to_none_num(php_to_krw_rate),
                        local_markup_rate=_to_none_num(local_markup_rate),
                        tax_rule_id=selected_tax_rule_id,
                        notes=_to_none_str(notes),
                    )
                    st.success("수입 버전이 등록되었습니다.")
                    st.rerun()

    # 공통 데이터
    batch_df = get_all_import_batch()

    # ---------------------------
    # 수정
    # ---------------------------
    with tab_edit:
        st.subheader("수입 버전 수정")

        tax_rule_map, tax_rule_labels = _build_tax_rule_options()

        if batch_df.empty:
            st.info("수정할 수입 버전이 없습니다.")
        else:
            batch_options = {
                f'{row["id"]} | {row["version_name"]}': row["id"]
                for _, row in batch_df.iterrows()
            }

            selected_label = st.selectbox("수정할 버전 선택", list(batch_options.keys()), key="edit_batch")
            selected_batch_id = batch_options[selected_label]

            detail_df = get_import_batch_detail(selected_batch_id)

            if detail_df.empty:
                st.error("버전 상세 데이터를 찾을 수 없습니다.")
            else:
                row = detail_df.iloc[0]

                with st.form("update_import_batch_form"):
                    c1, c2, c3 = st.columns(3)

                    with c1:
                        version_name = st.text_input("버전명 *", value=row["version_name"] or "")
                        import_date_str = row["import_date"] if row["import_date"] else None
                        import_date = st.date_input("수입일", value=import_date_str)
                    with c2:
                        supplier_name = st.text_input("공급처", value=row["supplier_name"] or "")
                        usd_to_krw_rate = st.number_input(
                            "USD 환율",
                            min_value=0.0,
                            value=float(row["usd_to_krw_rate"] or 0),
                            step=0.01,
                        )
                    with c3:
                        php_to_krw_rate = st.number_input(
                            "PHP 환율",
                            min_value=0.0,
                            value=float(row["php_to_krw_rate"] or 0),
                            step=0.01,
                        )
                        local_markup_rate = st.number_input(
                            "현지 마크업",
                            min_value=0.0,
                            value=float(row["local_markup_rate"] or 0),
                            step=0.0001,
                            format="%.4f",
                        )

                    if tax_rule_labels:
                        current_tax_rule_id = int(row["tax_rule_id"]) if row.get("tax_rule_id") not in (None, "") else None
                        default_tax_rule_label = tax_rule_labels[0]
                        for label, rid in tax_rule_map.items():
                            if rid == current_tax_rule_id:
                                default_tax_rule_label = label
                                break

                        selected_tax_rule_label = st.selectbox(
                            "세금 규칙 *",
                            tax_rule_labels,
                            index=tax_rule_labels.index(default_tax_rule_label),
                            key=f"edit_tax_rule_{selected_batch_id}",
                        )
                        selected_tax_rule_id = tax_rule_map[selected_tax_rule_label]
                    else:
                        selected_tax_rule_id = None
                        st.warning("세금 규칙 데이터가 없습니다. 먼저 세금 규칙을 등록해 주세요.")

                    notes = st.text_area("비고", value=row["notes"] or "", height=120)

                    save_btn = st.form_submit_button("수정 저장")

                    if save_btn:
                        if not version_name.strip():
                            st.error("버전명은 필수입니다.")
                        elif not selected_tax_rule_id:
                            st.error("세금 규칙을 선택해 주세요.")
                        else:
                            update_import_batch(
                                batch_id=selected_batch_id,
                                version_name=version_name.strip(),
                                import_date=str(import_date) if import_date else None,
                                supplier_name=_to_none_str(supplier_name),
                                usd_to_krw_rate=_to_none_num(usd_to_krw_rate),
                                php_to_krw_rate=_to_none_num(php_to_krw_rate),
                                local_markup_rate=_to_none_num(local_markup_rate),
                                tax_rule_id=selected_tax_rule_id,
                                notes=_to_none_str(notes),
                            )
                            st.success("수입 버전이 수정되었습니다.")
                            st.rerun()

    # ---------------------------
    # 삭제
    # ---------------------------
    with tab_delete:
        st.subheader("수입 버전 삭제")

        if batch_df.empty:
            st.info("삭제할 수입 버전이 없습니다.")
        else:
            batch_options = {
                f'{row["id"]} | {row["version_name"]}': row["id"]
                for _, row in batch_df.iterrows()
            }

            selected_label = st.selectbox("삭제할 버전 선택", list(batch_options.keys()), key="delete_batch")
            selected_batch_id = batch_options[selected_label]

            st.warning("연결된 수입 품목(import_item)이 있으면 삭제되지 않도록 처리하는 것을 권장합니다.")

            confirm = st.checkbox("위 버전을 삭제하겠습니다.")
            if st.button("삭제 실행", type="primary", disabled=not confirm):
                try:
                    delete_import_batch(selected_batch_id)
                    st.success("삭제되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 중 오류 발생: {e}")