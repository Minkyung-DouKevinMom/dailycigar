import streamlit as st

from modules.common.theme import inject_global_theme, render_sidebar
from modules.whisky.whisky_dashboard import render_dashboard as render_page


st.set_page_config(page_title="위스키 대시보드", layout="wide")
inject_global_theme()

st.title("위스키 대시보드")
render_sidebar(active="대시보드", business="whisky")

render_page()
