import streamlit as st

from modules.analytics.price_analysis import render as render_price
from modules.analytics.period_compare_view import render as render_period
from modules.analytics.partner_analysis_view import render as render_partner
from modules.analytics.brand_analysis_view import render as render_brand

st.set_page_config(page_title="분석", layout="wide")
st.title("분석")
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
    
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    
menu = st.radio(
    "메뉴 선택",
    ["가격 통합분석", "기간 비교", "거래처 분석", "브랜드 분석"],
    horizontal=True,
)

if menu == "가격 통합분석":
    render_price()
elif menu == "기간 비교":
    render_period()
elif menu == "거래처 분석":
    render_partner()
elif menu == "브랜드 분석":
    render_brand()