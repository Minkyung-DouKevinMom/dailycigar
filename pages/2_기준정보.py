import streamlit as st

from modules.master.product_mst import render as render_product_mst
from modules.master.tax_rule import render as render_tax_rule
from modules.master.brand_profile import render as render_brand_profile
from modules.master.partner_grade_mst import render as render_partner_grade
from modules.master.non_cigar_product_mst import render as render_non_cigar_product_mst
from modules.master.product_price_mst import render as render_product_price_mst  # ← 추가

st.set_page_config(page_title="기준정보", layout="wide")

st.title("기준정보")
with st.sidebar:
    st.markdown("## DAILY CIGAR")
    st.page_link("DAILY_CIGAR.py", label="HOME")
    st.page_link("pages/1_대시보드.py", label="대시보드⭐")
    st.page_link("pages/2_기준정보.py", label="기준정보")
    st.divider()
    st.page_link("pages/3_수입관리.py", label="수입관리")
    st.page_link("pages/4_판매관리.py", label="판매관리")
    st.page_link("pages/5_재무관리.py", label="재무관리")
    st.page_link("pages/6_분석.py", label="분석")
    st.divider()
    st.page_link("pages/7_문서출력.py", label="문서출력")
    st.page_link("pages/8_매장운영.py", label="매장운영⭐")
    st.page_link("pages/9_재고관리.py", label="재고관리📦")

menu = st.radio(
    "메뉴 선택",
    ["상품 마스터", "시가 외 상품", "세금 규칙", "브랜드 프로파일", "파트너 등급관리", "가격 마스터"],  # ← 추가
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