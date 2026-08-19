import streamlit as st

from modules.management.wholesale_management import render as render_wholesale
from modules.management.partner_grade_history_management import render as render_partner_grade_management

st.set_page_config(page_title="도매관리", layout="wide")

st.title("도매관리")
with st.sidebar:
    st.markdown("## DAILY CIGAR")
    st.page_link("DAILY_CIGAR.py", label="HOME")
    st.page_link("pages/1_대시보드.py", label="대시보드⭐")
    st.page_link("pages/2_기준정보.py", label="기준정보")
    st.divider()
    st.page_link("pages/3_수입관리.py", label="수입관리")
    st.page_link("pages/4_소매관리.py", label="소매관리")
    st.page_link("pages/4_도매관리.py", label="도매관리")
    st.page_link("pages/5_재무관리.py", label="재무관리")
    st.page_link("pages/6_분석.py", label="분석")
    st.divider()
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    st.page_link("pages/9_재고관리.py", label="재고관리📦")

menu = st.radio(
    "메뉴 선택",
    ["도매 관리", "거래처 등급관리"],
    horizontal=True
)

if menu == "도매 관리":
    render_wholesale()

elif menu == "거래처 등급관리":
    render_partner_grade_management()
