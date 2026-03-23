import streamlit as st
from db import get_all_import_batch, get_import_items_by_batch, update_import_batch_note

def render():
    st.subheader("수입 버전")
    st.set_page_config(page_title="Import Batch", layout="wide")
    
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
                "total_item_count": "품목 수",
                "total_unit_qty": "총 개수",
                "total_weight_g": st.column_config.NumberColumn("총 무게(g)", format="%.2f"),
                "total_amount_usd": st.column_config.NumberColumn("총 금액(USD)", format="%.2f"),
                "total_amount_krw": st.column_config.NumberColumn("총 금액(KRW)", format="%.0f"),
                "notes": "비고",
                "created_at": "생성일",
            }
        )

        batch_options = {
            f'{row["id"]} | {row["version_name"]}': row["id"]
            for _, row in batch_df.iterrows()
        }

        selected_label = st.selectbox("상세 조회할 버전", list(batch_options.keys()))
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
                "import_total_cost_krw": st.column_config.NumberColumn("수입금액(KRW)", format="%.0f"),
                "total_weight_g": st.column_config.NumberColumn("전체무게(g)", format="%.2f"),
                "retail_price_krw": st.column_config.NumberColumn("소비자가격", format="%.0f"),
                "supply_price_krw": st.column_config.NumberColumn("공급가격", format="%.0f"),
                "margin_krw": st.column_config.NumberColumn("마진", format="%.0f"),
                "source_row_no": "원본행",
            }
        )

        st.divider()
        st.subheader("메모 수정")

        current_note = batch_df.loc[batch_df["id"] == selected_batch_id, "notes"].iloc[0]
        note = st.text_area("비고", value=current_note or "", height=120)

        if st.button("메모 저장"):
            update_import_batch_note(selected_batch_id, note)
            st.success("저장되었습니다.")
            st.rerun()