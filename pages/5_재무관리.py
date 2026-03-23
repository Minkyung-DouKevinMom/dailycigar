import streamlit as st

from modules.finance.finance_expense_management import render as render_finance_expense
from modules.finance.finance_product_profitability import render as render_product_profitability
from modules.finance.finance_sales_profit import render as render_sales_profit

st.set_page_config(page_title="재무관리", layout="wide")

st.title("재무관리")

menu = st.radio(
    "메뉴 선택",
    ["매출 및 손익분석", "지출관리", "제품별 수익성"],
    horizontal=True
)

if menu == "매출 및 손익분석":
    render_sales_profit()

elif menu == "지출관리":
    render_finance_expense()

elif menu == "제품별 수익성":
    render_product_profitability()