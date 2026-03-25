import streamlit as st

from modules.analytics.price_analysis_view import render as render_price
from modules.analytics.period_compare_view import render as render_period
from modules.analytics.partner_analysis_view import render as render_partner
from modules.analytics.brand_analysis_view import render as render_brand

st.set_page_config(page_title="분석", layout="wide")
st.title("분석")

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