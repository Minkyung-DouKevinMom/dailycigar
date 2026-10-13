import streamlit as st

from modules.management.wholesale_management import render as render_wholesale
from modules.management.partner_grade_history_management import render as render_partner_grade_management
from modules.common.theme import inject_global_theme, render_sidebar

st.set_page_config(page_title="도매관리", layout="wide")
inject_global_theme()

st.title("도매관리")
render_sidebar(active="도매관리")

menu = st.radio(
    "메뉴 선택",
    ["도매 관리", "거래처 등급관리"],
    horizontal=True
)

if menu == "도매 관리":
    render_wholesale()

elif menu == "거래처 등급관리":
    render_partner_grade_management()
