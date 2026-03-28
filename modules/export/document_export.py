import io
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import Alignment

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
    "공급가(KRW)": "supply_price_krw",
    "공급가합계(KRW)": "supply_total_krw",
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
    "supply_price_krw": "공급가(KRW)",
    "supply_total_krw": "공급가합계(KRW)",
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


def _merge_cells(ws, col_idx, start_row, end_row):
    if col_idx and start_row < end_row:
        ws.merge_cells(
            start_row=start_row,
            start_column=col_idx,
            end_row=end_row,
            end_column=col_idx,
        )


def _get_product_groups(ws, header_map, start_row, end_row):
    """
    상품명 기준 연속 구간 반환
    예: [(2,4), (5,7), (8,8)]
    """
    product_col = header_map.get("상품명")
    if not product_col or end_row < start_row:
        return []

    groups = []
    group_start = start_row
    prev_value = ws.cell(row=start_row, column=product_col).value

    for row_idx in range(start_row + 1, end_row + 1):
        current_value = ws.cell(row=row_idx, column=product_col).value
        if current_value != prev_value:
            groups.append((group_start, row_idx - 1))
            group_start = row_idx
            prev_value = current_value

    groups.append((group_start, end_row))
    return groups


def _merge_within_group_same_value(ws, header_map, header_name, group_start, group_end):
    """
    지정된 상품명 그룹 내부에서만 같은 값 병합
    """
    col_idx = header_map.get(header_name)
    if not col_idx or group_end < group_start:
        return

    merge_start = group_start
    prev_value = ws.cell(row=group_start, column=col_idx).value

    for row_idx in range(group_start + 1, group_end + 1):
        current_value = ws.cell(row=row_idx, column=col_idx).value
        if current_value != prev_value:
            if prev_value not in (None, ""):
                _merge_cells(ws, col_idx, merge_start, row_idx - 1)
            merge_start = row_idx
            prev_value = current_value

    if prev_value not in (None, ""):
        _merge_cells(ws, col_idx, merge_start, group_end)


def _apply_alignment(ws, start_row, end_row):
    for row in ws.iter_rows(min_row=start_row, max_row=end_row):
        for cell in row:
            cell.alignment = Alignment(vertical="center")


def build_product_intro_from_template(df: pd.DataFrame) -> io.BytesIO:
    wb = _load_template_workbook()
    ws = wb[wb.sheetnames[0]]

    header_row = 1
    start_row = 2

    header_map = _get_header_map(ws, header_row=header_row)

    # 기존 데이터 지우기 (2행부터 끝까지)
    if ws.max_row >= start_row:
        ws.delete_rows(start_row, ws.max_row - start_row + 1)

    price_headers = {"소비자가(KRW)", "공급가(KRW)", "공급가합계(KRW)"}

    records = df.to_dict("records")
    for row_idx, record in enumerate(records, start=start_row):
        for excel_header, df_col in HEADER_MAPPING.items():
            col_idx = header_map.get(excel_header)
            if not col_idx:
                continue  # 템플릿에 해당 헤더가 없으면 무시

            value = _safe_value(record.get(df_col, ""))
            cell = ws.cell(row=row_idx, column=col_idx, value=value)

            if excel_header in price_headers and value not in ("", None):
                try:
                    cell.value = float(value)
                    cell.number_format = '₩#,##0'
                except Exception:
                    pass

    end_row = start_row + len(records) - 1
    if records:
        product_groups = _get_product_groups(ws, header_map, start_row, end_row)

        product_col = header_map.get("상품명")

        for group_start, group_end in product_groups:
            # 1) 상품명 자체 병합
            _merge_cells(ws, product_col, group_start, group_end)

            # 2) 상품명 구간 안에서만 Flavor / Guide 병합
            _merge_within_group_same_value(ws, header_map, "Flavor", group_start, group_end)
            _merge_within_group_same_value(ws, header_map, "Guide", group_start, group_end)

        _apply_alignment(ws, start_row, end_row)

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def render_product_intro_export():
    st.subheader("상품소개서 엑셀 다운로드")
    st.caption("템플릿 파일을 그대로 사용하고, 상품 데이터를 채워 넣어 다운로드합니다.")

    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])

    with c1:
        keyword = st.text_input("상품명 검색", "")

    with c2:
        use_yn = st.selectbox("사용여부", ["전체", "Y", "N"], index=0)

    with c3:
        include_retail_price = st.checkbox("소비자가 포함", value=True)

    with c4:
        include_supply_price = st.checkbox("공급가 포함", value=True)

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

    if not include_retail_price and "retail_price_krw" in export_df.columns:
        export_df["retail_price_krw"] = ""

    if not include_supply_price:
        if "supply_price_krw" in export_df.columns:
            export_df["supply_price_krw"] = ""
        if "supply_total_krw" in export_df.columns:
            export_df["supply_total_krw"] = ""

    preview_df = export_df.rename(columns=DISPLAY_RENAME).copy()

    desired_order = [
        "상품명",
        "사이즈",
        "Flavor",
        "Strength",
        "Length",
        "RG",
        "Time",
        "Guide",
        "소비자가(KRW)",
        "공급가(KRW)",
        "공급가합계(KRW)",
    ]
    preview_df = preview_df[[c for c in desired_order if c in preview_df.columns]]

    for col in ["소비자가(KRW)", "공급가(KRW)", "공급가합계(KRW)"]:
        if col in preview_df.columns:
            preview_df[col] = preview_df[col].apply(
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