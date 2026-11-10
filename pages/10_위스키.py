import streamlit as st

from modules.common.theme import inject_global_theme, render_sidebar
from modules.whisky.whisky_view import render as render_whisky


st.set_page_config(page_title="위스키", layout="wide")
inject_global_theme()

st.title("위스키")
render_sidebar(active="위스키 관리", business="whisky")

render_whisky()
