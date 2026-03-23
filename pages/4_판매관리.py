import streamlit as st

from modules.management.retail_upload import render as render_retail
from modules.management.wholesale_management import render as render_wholesale
from modules.management.retail_sales_view import render as render_retail_view
from modules.management.partner_grade_history import render as render_partner_grade_history
from modules.management.partner_grade_history_management import render as render_partner_grade_management

st.set_page_config(page_title="판매관리", layout="wide")

st.title("판매관리")

menu = st.radio(
    "메뉴 선택",
    ["소매 업로드", "소매 관리", "도매 관리", "거래처 등급관리"],
    horizontal=True
)

if menu == "소매 업로드":
    render_retail()

elif menu == "도매 관리":
    render_wholesale()

elif menu == "소매 관리":
    render_retail_view()

elif menu == "거래처 등급관리":
    render_partner_grade_management()