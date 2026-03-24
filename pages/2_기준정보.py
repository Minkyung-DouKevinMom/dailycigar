import streamlit as st

from modules.master.product_mst import render as render_product_mst
from modules.master.tax_rule import render as render_tax_rule
from modules.master.brand_profile import render as render_brand_profile
from modules.master.partner_grade_mst import render as render_partner_grade
from modules.master.non_cigar_product_mst import render as render_non_cigar_product_mst

st.set_page_config(page_title="기준정보", layout="wide")

st.title("기준정보")

menu = st.radio(
    "메뉴 선택",
    ["상품 마스터", "시가 외 상품", "세금 규칙", "브랜드 프로파일", "파트너 등급관리"],
    horizontal=True
)

if menu == "상품 마스터":
    render_product_mst()
elif menu == "시가 외 상품":
    render_non_cigar_product_mst()
elif menu == "세금 규칙":
    render_tax_rule()
elif menu == "브랜드 프로파일":
    render_brand_profile()
elif menu == "파트너 등급관리":
    render_partner_grade()