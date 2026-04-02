import streamlit as st

from modules.finance.finance_expense_management import render as render_finance_expense
from modules.finance.finance_product_profitability import render as render_product_profitability
from modules.finance.finance_sales_profit import render as render_sales_profit

st.set_page_config(page_title="재무관리", layout="wide")

st.title("재무관리")
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
    ["매출 및 손익분석", "지출관리", "제품별 수익성"],
    horizontal=True
)

if menu == "매출 및 손익분석":
    render_sales_profit()

elif menu == "지출관리":
    render_finance_expense()

elif menu == "제품별 수익성":
    render_product_profitability()