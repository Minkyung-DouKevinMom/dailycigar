import sys
import os
import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from modules.dashboard.dashboard_finance_summary import render
from modules.common.theme import inject_global_theme, render_sidebar

inject_global_theme()
render_sidebar(active="대시보드")

render()
