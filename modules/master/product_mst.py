import streamlit as st
import pandas as pd

from db import (
    get_all_product_mst_for_edit,
    update_product_mst_by_id,
    insert_product_mst,
    delete_product_mst_by_id,
)


DISPLAY_COLUMNS = [
    "id",
    "product_name",
    "size_name",
    "product_code",
    "use_yn",
    "length_mm",
    "ring_gauge",
    "smoking_time_text",
    "unit_weight_g",
    "box_width_cm",
    "box_depth_cm",
    "box_height_cm",
]

COLUMN_RENAME = {
    "id": "ID",
    "product_name": "상품명",
    "size_name": "사이즈",
    "product_code": "상품코드",
    "use_yn": "사용여부",
    "length_mm": "길이(mm)",
    "ring_gauge": "링게이지",
    "smoking_time_text": "흡연시간",
    "unit_weight_g": "개당무게(g)",
    "box_width_cm": "박스가로(cm)",
    "box_depth_cm": "박스세로(cm)",
    "box_height_cm": "박스높이(cm)",
}


def _null(v):
    if pd.isna(v):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def _safe_float(v):
    v = _null(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _normalize_use_yn(v):
    v = str(v or "Y").strip().upper()
    return v if v in ["Y", "N"] else "Y"


def _load_df():
    df = get_all_product_mst_for_edit()
    if df is None or df.empty:
        df = pd.DataFrame(columns=DISPLAY_COLUMNS)

    for col in DISPLAY_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[DISPLAY_COLUMNS].copy()
    df["use_yn"] = df["use_yn"].fillna("Y").astype(str).str.upper()
    df.loc[~df["use_yn"].isin(["Y", "N"]), "use_yn"] = "Y"

    return df


def _render_view_table(df: pd.DataFrame):
    st.markdown("#### 조회")

    keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 상품코드")

    filtered_df = df.copy()
    if keyword:
        keyword = str(keyword).strip()
        filtered_df = filtered_df[
            filtered_df["product_name"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
            | filtered_df["size_name"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
            | filtered_df["product_code"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
        ].copy()

    st.caption("표 헤더 클릭으로 정렬해서 볼 수 있습니다.")
    st.dataframe(
        filtered_df.rename(columns=COLUMN_RENAME),
        use_container_width=True,
        height=520,
        hide_index=True,
    )
    st.caption(f"총 {len(filtered_df):,}건")

    return filtered_df


def _render_edit_form(df: pd.DataFrame):
    st.markdown("#### 기존 데이터 수정 / 삭제")

    if df.empty:
        st.info("등록된 데이터가 없습니다.")
        return

    options_df = df.copy().sort_values(by="id", ascending=True).reset_index(drop=True)
    options_df["label"] = options_df.apply(
        lambda r: f"[{int(r['id'])}] {r['product_name']} / {r['size_name']} / {r['product_code']}",
        axis=1,
    )

    selected_label = st.selectbox(
        "수정할 상품 선택",
        options=options_df["label"].tolist(),
        index=0,
        key="product_edit_selectbox",
    )

    selected_row = options_df.loc[options_df["label"] == selected_label].iloc[0]

    with st.form("product_edit_form", clear_on_submit=False):
        st.caption(f"선택 ID: {int(selected_row['id'])}")

        col1, col2, col3 = st.columns(3)
        with col1:
            product_name = st.text_input("상품명", value=selected_row.get("product_name") or "")
        with col2:
            size_name = st.text_input("사이즈", value=selected_row.get("size_name") or "")
        with col3:
            product_code = st.text_input("상품코드", value=selected_row.get("product_code") or "")

        col4, col5, col6 = st.columns(3)
        with col4:
            use_yn = st.selectbox(
                "사용여부",
                options=["Y", "N"],
                index=0 if str(selected_row.get("use_yn") or "Y").upper() == "Y" else 1,
            )
        with col5:
            length_mm = st.number_input(
                "길이(mm)",
                value=float(selected_row["length_mm"]) if pd.notna(selected_row["length_mm"]) else 0.0,
                step=1.0,
                format="%.0f",
            )
        with col6:
            ring_gauge = st.number_input(
                "링게이지",
                value=float(selected_row["ring_gauge"]) if pd.notna(selected_row["ring_gauge"]) else 0.0,
                step=1.0,
                format="%.0f",
            )

        col7, col8 = st.columns(2)
        with col7:
            smoking_time_text = st.text_input("흡연시간", value=selected_row.get("smoking_time_text") or "")
        with col8:
            unit_weight_g = st.number_input(
                "개당무게(g)",
                value=float(selected_row["unit_weight_g"]) if pd.notna(selected_row["unit_weight_g"]) else 0.0,
                step=0.01,
                format="%.2f",
            )

        col9, col10, col11 = st.columns(3)
        with col9:
            box_width_cm = st.number_input(
                "박스가로(cm)",
                value=float(selected_row["box_width_cm"]) if pd.notna(selected_row["box_width_cm"]) else 0.0,
                step=0.01,
                format="%.2f",
            )
        with col10:
            box_depth_cm = st.number_input(
                "박스세로(cm)",
                value=float(selected_row["box_depth_cm"]) if pd.notna(selected_row["box_depth_cm"]) else 0.0,
                step=0.01,
                format="%.2f",
            )
        with col11:
            box_height_cm = st.number_input(
                "박스높이(cm)",
                value=float(selected_row["box_height_cm"]) if pd.notna(selected_row["box_height_cm"]) else 0.0,
                step=0.01,
                format="%.2f",
            )

        delete_yn = st.checkbox("이 상품 삭제", value=False)

        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            submitted_update = st.form_submit_button("수정 저장", use_container_width=True, type="primary")
        with btn_col2:
            submitted_delete = st.form_submit_button("삭제 실행", use_container_width=True)

    if submitted_update:
        if not product_name.strip() or not size_name.strip() or not product_code.strip():
            st.warning("상품명, 사이즈, 상품코드는 필수입니다.")
            return

        update_product_mst_by_id(
            row_id=int(selected_row["id"]),
            product_name=product_name.strip(),
            size_name=size_name.strip(),
            product_code=product_code.strip(),
            use_yn=_normalize_use_yn(use_yn),
            length_mm=_safe_float(length_mm),
            ring_gauge=_safe_float(ring_gauge),
            smoking_time_text=_null(smoking_time_text),
            unit_weight_g=_safe_float(unit_weight_g),
            box_width_cm=_safe_float(box_width_cm),
            box_depth_cm=_safe_float(box_depth_cm),
            box_height_cm=_safe_float(box_height_cm),
        )
        st.success("수정되었습니다.")
        st.rerun()

    if submitted_delete:
        if not delete_yn:
            st.warning("삭제하려면 '이 상품 삭제'를 체크하세요.")
            return

        delete_product_mst_by_id(int(selected_row["id"]))
        st.success("삭제되었습니다.")
        st.rerun()


def _render_insert_form():
    st.markdown("#### 신규 데이터 추가")

    with st.form("product_insert_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            product_name = st.text_input("상품명")
        with col2:
            size_name = st.text_input("사이즈")
        with col3:
            product_code = st.text_input("상품코드")

        col4, col5, col6 = st.columns(3)
        with col4:
            use_yn = st.selectbox("사용여부", options=["Y", "N"], index=0)
        with col5:
            length_mm = st.number_input("길이(mm)", value=0.0, step=1.0, format="%.0f")
        with col6:
            ring_gauge = st.number_input("링게이지", value=0.0, step=1.0, format="%.0f")

        col7, col8 = st.columns(2)
        with col7:
            smoking_time_text = st.text_input("흡연시간")
        with col8:
            unit_weight_g = st.number_input("개당무게(g)", value=0.0, step=0.01, format="%.2f")

        col9, col10, col11 = st.columns(3)
        with col9:
            box_width_cm = st.number_input("박스가로(cm)", value=0.0, step=0.01, format="%.2f")
        with col10:
            box_depth_cm = st.number_input("박스세로(cm)", value=0.0, step=0.01, format="%.2f")
        with col11:
            box_height_cm = st.number_input("박스높이(cm)", value=0.0, step=0.01, format="%.2f")

        submitted_insert = st.form_submit_button("신규 등록", use_container_width=True, type="primary")

    if submitted_insert:
        if not product_name.strip() or not size_name.strip() or not product_code.strip():
            st.warning("상품명, 사이즈, 상품코드는 필수입니다.")
            return

        insert_product_mst(
            product_name=product_name.strip(),
            size_name=size_name.strip(),
            product_code=product_code.strip(),
            use_yn=_normalize_use_yn(use_yn),
            length_mm=_safe_float(length_mm),
            ring_gauge=_safe_float(ring_gauge),
            smoking_time_text=_null(smoking_time_text),
            unit_weight_g=_safe_float(unit_weight_g),
            box_width_cm=_safe_float(box_width_cm),
            box_depth_cm=_safe_float(box_depth_cm),
            box_height_cm=_safe_float(box_height_cm),
        )
        st.success("신규 등록되었습니다.")
        st.rerun()


def render():
    st.subheader("시가 상품 마스터")

    df = _load_df()

    filtered_df = _render_view_table(df)
    st.divider()

    tab1, tab2 = st.tabs(["기존 데이터 수정/삭제", "신규 데이터 추가"])

    with tab1:
        _render_edit_form(filtered_df)

    with tab2:
        _render_insert_form()