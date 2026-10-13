import streamlit as st

from modules.master.product_mst import render as render_product_mst
from modules.master.tax_rule import render as render_tax_rule
from modules.master.brand_profile import render as render_brand_profile
from modules.master.partner_grade_mst import render as render_partner_grade
from modules.master.non_cigar_product_mst import render as render_non_cigar_product_mst
from modules.master.product_price_mst import render as render_product_price_mst  # ← 추가
from modules.master.label_size_mst import render as render_label_size_mst  # ← 추가
from modules.master.data_integrity_check import render as render_integrity_check
from modules.common.theme import inject_global_theme, render_sidebar

st.set_page_config(page_title="기준정보", layout="wide")
inject_global_theme()

st.title("기준정보")
render_sidebar(active="기준정보")

menu = st.radio(
    "메뉴 선택",
    ["상품 마스터", "시가 외 상품", "세금 규칙", "브랜드 프로파일", "파트너 등급관리", "가격 마스터", "라벨 사이즈 그룹관리", "정합성 점검"],
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
elif menu == "가격 마스터":          # ← 추가
    render_product_price_mst()
elif menu == "라벨 사이즈 그룹관리":   # ← 추가
    render_label_size_mst()
elif menu == "정합성 점검":
    render_integrity_check()
