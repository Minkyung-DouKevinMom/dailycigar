import streamlit as st
import sys
import os

# 프로젝트 루트를 path에 추가 (db.py, db_inventory.py 접근용)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(page_title="재고관리 | Daily Cigar", layout="wide")

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

from modules.management.inventory_management import render
render()
