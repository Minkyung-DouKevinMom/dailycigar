import streamlit as st

from modules.management.retail_upload import render as render_retail
from modules.management.retail_customer_management import render as render_retail_customer
from modules.management.retail_sales_view import render as render_retail_view
from modules.management.gift_package_component import render as render_gift_package_component
from modules.common.theme import inject_global_theme, render_sidebar

st.set_page_config(page_title="소매관리", layout="wide")
inject_global_theme()

st.title("소매관리")
render_sidebar(active="소매관리")

menu = st.radio(
    "메뉴 선택",
    ["소매 관리", "소매 고객 관리", "소매 업로드", "기프트패키지 관리"],
    horizontal=True
)

if menu == "소매 업로드":
    render_retail()

elif menu == "소매 관리":
    render_retail_view()

elif menu == "소매 고객 관리":
    render_retail_customer()

elif menu == "기프트패키지 관리":
    render_gift_package_component()
