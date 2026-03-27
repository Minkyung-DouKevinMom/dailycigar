import io
import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from db import get_product_intro_export_data


HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
THIN_BORDER = Border(
    left=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="CCCCCC"),
    top=Side(style="thin", color="CCCCCC"),
    bottom=Side(style="thin", color="CCCCCC"),
)


def _safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)


def _safe_int(v):
    if pd.isna(v) or v is None or v == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def build_product_intro_excel(df: pd.DataFrame) -> io.BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "상품소개"

    headers = [
        "상품명",
        "사이즈",
        "Flavor",
        "Strength",
        "Length",
        "RG",
        "Time",
        "Guide",
        "소비자가(KRW)",
    ]

    # 헤더 작성
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER

    # 데이터 작성
    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        values = [
            getattr(row, "product_name", ""),
            getattr(row, "size_name", ""),
            getattr(row, "flavor", ""),
            getattr(row, "strength", ""),
            getattr(row, "length_text", ""),
            getattr(row, "rg", ""),
            getattr(row, "time_text", ""),
            getattr(row, "guide_text", ""),
            getattr(row, "retail_price_krw", None),
        ]

        for col_idx, value in enumerate(values, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True,
                horizontal="left" if col_idx != 9 else "right"
            )

            if col_idx == 9 and value not in (None, ""):
                cell.number_format = '₩#,##0'

    # 열너비
    widths = {
        1: 28,  # 상품명
        2: 16,  # 사이즈
        3: 22,  # Flavor
        4: 12,  # Strength
        5: 12,  # Length
        6: 8,   # RG
        7: 12,  # Time
        8: 60,  # Guide
        9: 16,  # 소비자가
    }
    for col_idx, width in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    # 행 높이 조금 넉넉하게
    ws.row_dimensions[1].height = 24
    for r in range(2, ws.max_row + 1):
        ws.row_dimensions[r].height = 36

    # 틀 고정
    ws.freeze_panes = "A2"

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def render():
    st.title("문서출력")
    st.caption("상품소개서, 가격표, 거래명세서 등 각종 문서를 다운로드할 수 있습니다.")

    tab1, tab2 = st.tabs(["상품소개서", "준비중"])

    with tab1:
        render_product_intro_export()

    with tab2:
        st.info("향후 거래명세서 / 가격표 / 파트너 제안서 등을 추가할 수 있습니다.")


def render_product_intro_export():
    st.subheader("상품소개서 엑셀 다운로드")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        brand_keyword = st.text_input("브랜드/상품명 검색", value="")

    with col2:
        use_yn = st.selectbox("사용여부", ["전체", "Y", "N"], index=0)

    with col3:
        price_included = st.checkbox("소비자가 포함", value=True)

    try:
        df = get_product_intro_export_data(
            brand_keyword=brand_keyword.strip(),
            use_yn=use_yn
        )
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")
        return

    if df.empty:
        st.warning("조회된 상품이 없습니다.")
        return

    # 소비자가 제외 옵션
    preview_df = df.copy()
    if not price_included and "retail_price_krw" in preview_df.columns:
        preview_df["retail_price_krw"] = None

    # 화면 표시용 헤더
    display_df = preview_df.rename(columns={
        "product_name": "상품명",
        "size_name": "사이즈",
        "flavor": "Flavor",
        "strength": "Strength",
        "length_text": "Length",
        "rg": "RG",
        "time_text": "Time",
        "guide_text": "Guide",
        "retail_price_krw": "소비자가(KRW)",
    })

    st.markdown("### 미리보기")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    export_df = preview_df.copy()
    if not price_included and "retail_price_krw" in export_df.columns:
        export_df["retail_price_krw"] = None

    excel_data = build_product_intro_excel(export_df)

    st.download_button(
        label="상품소개서 엑셀 다운로드",
        data=excel_data,
        file_name="상품소개서_발송용.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )