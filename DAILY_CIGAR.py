import streamlit as st
from db import get_table_count, table_exists, get_all_import_batch

st.set_page_config(page_title="Daily Cigar DB", layout="wide")
st.title("Daily Cigar 운영 관리 시스템")

st.subheader("DB 상태")

tables = [
    "product_mst",
    "import_batch",
    "import_item",
    "tax_rule",
    "export_price_item",
    "blend_profile_mst",
]

cols = st.columns(3)
for idx, table_name in enumerate(tables):
    with cols[idx % 3]:
        exists = table_exists(table_name)
        count = get_table_count(table_name) if exists else 0
        st.metric(
            label=table_name,
            value=count,
            delta="OK" if exists else "없음"
        )

st.divider()

st.subheader("최근 Import Batch")
if table_exists("import_batch"):
    batch_df = get_all_import_batch()
    st.dataframe(batch_df.head(20), use_container_width=True, hide_index=True)
else:
    st.info("import_batch 테이블이 없습니다.")

st.divider()
st.write("왼쪽 사이드바에서 페이지를 선택하세요.")