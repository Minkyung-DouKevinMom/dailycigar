import streamlit as st

from modules.inventory.import_detail import render as render_import_detail
from modules.inventory.import_version import render as render_import_version
from modules.inventory.export_price_view import render as render_export_price_view

st.set_page_config(page_title="수입관리", layout="wide")
st.title("수입관리")

menu = st.radio(
    "메뉴 선택",
    ["수입품목", "수입 버전", "본사 수출 배포가격"],
    horizontal=True,
)

if menu == "수입품목":
    render_import_detail()
elif menu == "수입 버전":
    render_import_version()
elif menu == "본사 수출 배포가격":
    render_export_price_view()