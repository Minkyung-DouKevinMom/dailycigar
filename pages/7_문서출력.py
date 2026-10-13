import streamlit as st
from modules.export.document_export import render as render_document_export
from modules.common.theme import inject_global_theme, render_sidebar

st.set_page_config(page_title="문서출력", layout="wide")
inject_global_theme()
render_sidebar(active="문서출력")
render_document_export()