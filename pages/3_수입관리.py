import streamlit as st

from modules.inventory.import_detail import render as render_import_detail
from modules.inventory.import_version import render as render_import_version

st.set_page_config(page_title="수입관리", layout="wide")

st.title("수입관리")

menu = st.radio(
    "메뉴 선택",
    ["수입 상세","수입 버전"],
    horizontal=True
)

if menu == "수입 상세":
    render_import_detail()

elif menu == "수입 버전":
    render_import_version()