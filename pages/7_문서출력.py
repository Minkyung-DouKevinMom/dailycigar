import streamlit as st
from modules.export.document_export import render as render_document_export

st.set_page_config(page_title="문서출력", layout="wide")
render_document_export()