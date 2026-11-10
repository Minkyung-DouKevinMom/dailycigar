import streamlit as st

from modules.common.theme import inject_global_theme, render_sidebar
from modules.whisky.whisky_dashboard import render_home as render_page


st.set_page_config(page_title="위스키 운영 관리", layout="wide")
inject_global_theme()

st.title("위스키 운영 관리")
render_sidebar(active="HOME", business="whisky")

render_page()
