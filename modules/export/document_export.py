import io
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl import load_workbook

from db import get_product_intro_export_data


TEMPLATE_PATH = "templates/TAB시가소개 발송용.xlsx"

# 템플릿 헤더명 -> DataFrame 컬럼명
HEADER_MAPPING = {
    "상품명": "product_name",
    "사이즈": "size_name",
    "Flavor": "flavor",
    "Strength": "strength",
    "Length": "length_text",
    "RG": "rg",
    "Time": "time_text",
    "Guide": "guide_text",
    "소비자가(KRW)": "retail_price_krw",
}


DISPLAY_RENAME = {
    "product_name": "상품명",
    "size_name": "사이즈",
    "flavor": "Flavor",
    "strength": "Strength",
    "length_text": "Length",
    "rg": "RG",
    "time_text": "Time",
    "guide_text": "Guide",
    "retail_price_krw": "소비자가(KRW)",
}


def _safe_value(value):
    if pd.isna(value):
        return ""
    return value


def _load_template_workbook():
    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(
            f"템플릿 파일이 없습니다: {TEMPLATE_PATH}\n"
            "프로젝트 폴더의 templates 아래에 파일을 넣어주세요."
        )
    return load_workbook(TEMPLATE_PATH)


def _get_header_map(ws, header_row=1):
    """
    엑셀의 실제 헤더명을 읽어 컬럼 위치를 찾습니다.
    예: {'상품명': 1, '사이즈': 2, ...}
    """
    header_map = {}
    for col_idx in range(1, ws.max_column + 1):
        value = ws.cell(row=header_row, column=col_idx).value
        if value is None:
            continue
        header_map[str(value).strip()] = col_idx
    return header_map


def build_product_intro_from_template(df: pd.DataFrame) -> io.BytesIO:
    wb = _load_template_workbook()
    ws = wb[wb.sheetnames[0]]  # 현재 템플릿은 Sheet1

    header_row = 1
    start_row = 2

    header_map = _get_header_map(ws, header_row=header_row)

    # 기존 데이터 지우기 (2행부터 끝까지)
    if ws.max_row >= start_row:
        ws.delete_rows(start_row, ws.max_row - start_row + 1)

    # 데이터 채우기
    records = df.to_dict("records")
    for row_idx, record in enumerate(records, start=start_row):
        for excel_header, df_col in HEADER_MAPPING.items():
            col_idx = header_map.get(excel_header)
            if not col_idx:
                continue  # 템플릿에 해당 헤더가 없으면 무시

            value = _safe_value(record.get(df_col, ""))

            cell = ws.cell(row=row_idx, column=col_idx, value=value)

            # 가격 칼럼 숫자 포맷
            if excel_header == "소비자가(KRW)" and value not in ("", None):
                try:
                    cell.value = float(value)
                    cell.number_format = '₩#,##0'
                except Exception:
                    pass

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def render_product_intro_export():
    st.subheader("상품소개서 엑셀 다운로드")
    st.caption("템플릿 파일을 그대로 사용하고, 상품 데이터를 채워 넣어 다운로드합니다.")

    c1, c2, c3 = st.columns([1.2, 1, 1])

    with c1:
        keyword = st.text_input("상품명/브랜드 검색", "")

    with c2:
        use_yn = st.selectbox("사용여부", ["전체", "Y", "N"], index=0)

    with c3:
        include_price = st.checkbox("소비자가 포함", value=True)

    try:
        df = get_product_intro_export_data(
            brand_keyword=keyword.strip(),
            use_yn=use_yn,
        )
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")
        return

    if df.empty:
        st.warning("조회된 상품이 없습니다.")
        return

    export_df = df.copy()

    if not include_price and "retail_price_krw" in export_df.columns:
        export_df["retail_price_krw"] = ""

    preview_df = export_df.rename(columns=DISPLAY_RENAME).copy()

    desired_order = [
        "상품명", "사이즈", "Flavor", "Strength",
        "Length", "RG", "Time", "Guide", "소비자가(KRW)"
    ]
    preview_df = preview_df[[c for c in desired_order if c in preview_df.columns]]

    if "소비자가(KRW)" in preview_df.columns:
        preview_df["소비자가(KRW)"] = preview_df["소비자가(KRW)"].apply(
            lambda x: f"₩{int(float(x)):,}" if str(x).strip() not in ("", "nan", "None") else ""
        )

    st.markdown("### 미리보기")
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    try:
        excel_bytes = build_product_intro_from_template(export_df)
    except Exception as e:
        st.error(f"엑셀 생성 중 오류 발생: {e}")
        return

    today = datetime.now().strftime("%Y%m%d")
    file_name = f"상품소개서_발송용_{today}.xlsx"

    st.download_button(
        "상품소개서 다운로드",
        data=excel_bytes,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


def render():
    st.title("문서출력")
    tab1, tab2 = st.tabs(["상품소개서", "준비중"])

    with tab1:
        render_product_intro_export()

    with tab2:
        st.info("향후 거래명세서 / 가격표 / 파트너 제안서 등을 추가할 수 있습니다.")