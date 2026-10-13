import streamlit as st
import sys
import os

# 프로젝트 루트를 path에 추가 (db.py, db_inventory.py 접근용)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(page_title="재고관리 | Daily Cigar", layout="wide")

from modules.common.theme import inject_global_theme, render_sidebar

inject_global_theme()
render_sidebar(active="재고관리")

from modules.management.inventory_management import render
render()
