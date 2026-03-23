import streamlit as st

from modules.analytics.export_price_view import render as render_export_price_view
from modules.analytics.price_analysis import render as render_price_analysis

st.set_page_config(page_title="분석", layout="wide")

st.title("분석")

menu = st.radio(
    "메뉴 선택",
    ["수출가격 조회", "가격분석 통합조회"],
    horizontal=True
)

if menu == "수출가격 조회":
    render_export_price_view()
elif menu == "가격분석 통합조회":
    render_price_analysis()