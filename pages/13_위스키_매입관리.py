import streamlit as st

from modules.common.theme import inject_global_theme, render_sidebar
from modules.whisky.whisky_view import render_purchase_page as render_page


st.set_page_config(page_title="위스키 매입관리", layout="wide")
inject_global_theme()

st.title("위스키 매입관리")
render_sidebar(active="매입관리", business="whisky")

render_page()
