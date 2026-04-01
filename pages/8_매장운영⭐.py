import streamlit as st

from modules.store.store_menu_board import render as render_store_menu_board


st.set_page_config(page_title="매장운영", layout="wide")

st.title("매장운영")

tab1 = st.tabs(["매장 메뉴판"])[0]

with tab1:
    render_store_menu_board()