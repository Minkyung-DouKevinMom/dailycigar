import streamlit as st

from modules.inventory.import_detail import render as render_import_detail
from modules.inventory.import_version import render as render_import_version
from modules.inventory.export_price_view import render as render_export_price_view

st.set_page_config(page_title="수입관리", layout="wide")
st.title("수입관리")
with st.sidebar:
    st.markdown("## DAILY CIGAR")
    st.page_link("DAILY_CIGAR.py", label="HOME")
    st.page_link("pages/1_대시보드.py", label="대시보드⭐")
    st.page_link("pages/2_기준정보.py", label="기준정보")
    st.divider()
    st.page_link("pages/3_수입관리.py", label="수입관리")
    st.page_link("pages/4_판매관리.py", label="판매관리")
    st.page_link("pages/5_재무관리.py", label="재무관리")
    st.page_link("pages/6_분석.py", label="분석")
    st.divider()
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    st.page_link("pages/9_재고관리.py", label="재고관리📦")
menu = st.radio(
    "메뉴 선택",
    ["수입품목", "수입 버전", "본사 수출 배포가격"],
    horizontal=True,
)

if menu == "수입품목":
    render_import_detail()
elif menu == "수입 버전":
    render_import_version()
elif menu == "본사 수출 배포가격":
    render_export_price_view()