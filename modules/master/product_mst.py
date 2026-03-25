import streamlit as st
import pandas as pd
from db import (
    get_all_product_mst_for_edit,
    update_product_mst_by_id,
    insert_product_mst,
    delete_product_mst_by_id,
)

def render():
    st.subheader("시가")

    st.set_page_config(page_title="Product MST", layout="wide")
    st.caption("표에서 직접 수정 후 저장할 수 있습니다.")

    # -----------------------------
    # 데이터 조회
    # -----------------------------
    df = get_all_product_mst_for_edit()
    if "use_yn" not in df.columns:
        df["use_yn"] = "Y"
    df["use_yn"] = df["use_yn"].fillna("Y").astype(str).str.upper()
    df.loc[~df["use_yn"].isin(["Y", "N"]), "use_yn"] = "Y"

    if df.empty:
        st.info("product_mst 데이터가 없습니다.")
        df = pd.DataFrame(columns=[
    "id", "product_name", "size_name", "product_code", "use_yn",
    "length_mm", "ring_gauge", "smoking_time_text",
    "unit_weight_g", "box_width_cm", "box_depth_cm", "box_height_cm",
    "created_at", "updated_at"
    ])

    # 검색
    keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드")
    if keyword:
        mask = (
            df["product_name"].fillna("").str.contains(keyword, case=False) |
            df["size_name"].fillna("").str.contains(keyword, case=False) |
            df["product_code"].fillna("").str.contains(keyword, case=False)
        )
        df = df[mask].copy()

    # 삭제 체크용 컬럼
    if "delete_yn" not in df.columns:
        df.insert(0, "delete_yn", False)

    # 화면용 데이터
    edit_df = df.copy()

    # created_at, updated_at은 보기만 하고 수정 제외
    display_columns = [
    "delete_yn",
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
    "created_at",
    "updated_at",
]

    edit_df = edit_df[display_columns]

    st.subheader("인라인 편집")

    edited_df = st.data_editor(
        edit_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "delete_yn": st.column_config.CheckboxColumn("삭제", help="체크 후 저장하면 삭제됩니다."),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "product_name": st.column_config.TextColumn("상품명", required=True),
            "size_name": st.column_config.TextColumn("사이즈", required=True),
            "product_code": st.column_config.TextColumn("코드", required=True),
            "length_mm": st.column_config.NumberColumn("길이(mm)", format="%.1f"),
            "ring_gauge": st.column_config.NumberColumn("링게이지", format="%d"),
            "smoking_time_text": st.column_config.TextColumn("흡연시간"),
            "unit_weight_g": st.column_config.NumberColumn("개당무게(g)", format="%.2f"),
            "box_width_cm": st.column_config.NumberColumn("박스가로(cm)", format="%.2f"),
            "box_depth_cm": st.column_config.NumberColumn("박스세로(cm)", format="%.2f"),
            "box_height_cm": st.column_config.NumberColumn("박스높이(cm)", format="%.2f"),
            "created_at": st.column_config.TextColumn("생성일", disabled=True),
            "updated_at": st.column_config.TextColumn("수정일", disabled=True),
            "use_yn": st.column_config.SelectboxColumn("사용여부", options=["Y", "N"], required=True,),
        },
        disabled=["id", "created_at", "updated_at"],
        key="product_mst_editor",
    )

    c1, c2 = st.columns([1, 5])

    with c1:
        save_btn = st.button("변경사항 저장", type="primary")

    with c2:
        st.write("새 행 추가도 가능하며, ID가 비어 있으면 신규 등록됩니다.")

    # -----------------------------
    # 저장 로직
    # -----------------------------
    def null_if_blank(v):
        if pd.isna(v):
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    def to_int_or_none(v):
        v = null_if_blank(v)
        if v is None:
            return None
        try:
            return int(float(v))
        except Exception:
            return None

    def to_float_or_none(v):
        v = null_if_blank(v)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    if save_btn:
        try:
            # 기존 ID 목록
            original_ids = set(df["id"].dropna().astype(int).tolist()) if not df.empty else set()

            update_count = 0
            insert_count = 0
            delete_count = 0

            for _, row in edited_df.iterrows():
                row_id = row.get("id")
                delete_yn = bool(row.get("delete_yn", False))

                product_name = null_if_blank(row.get("product_name"))
                size_name = null_if_blank(row.get("size_name"))
                product_code = null_if_blank(row.get("product_code"))
                length_mm = to_float_or_none(row.get("length_mm"))
                ring_gauge = to_int_or_none(row.get("ring_gauge"))
                smoking_time_text = null_if_blank(row.get("smoking_time_text"))
                unit_weight_g = to_float_or_none(row.get("unit_weight_g"))
                box_width_cm = to_float_or_none(row.get("box_width_cm"))
                box_depth_cm = to_float_or_none(row.get("box_depth_cm"))
                box_height_cm = to_float_or_none(row.get("box_height_cm"))
                use_yn = str(row.get("use_yn") or "Y").strip().upper()
                if use_yn not in ["Y", "N"]:
                    use_yn = "Y"

                # 아무 값도 없는 신규 빈행은 무시
                is_blank_new_row = (
                    pd.isna(row_id)
                    and not product_name
                    and not size_name
                    and not product_code
                    and length_mm is None
                    and ring_gauge is None
                    and not smoking_time_text
                    and unit_weight_g is None
                    and box_width_cm is None
                    and box_depth_cm is None
                    and box_height_cm is None
                    and use_yn in ["Y", "", None]
                )
                if is_blank_new_row:
                    continue

                # 삭제
                if delete_yn and not pd.isna(row_id):
                    delete_product_mst_by_id(int(row_id))
                    delete_count += 1
                    continue

                # 필수값 체크
                if not product_name or not size_name or not product_code:
                    st.warning(f"필수값 누락으로 저장 제외: product_name={product_name}, size_name={size_name}, product_code={product_code}")
                    continue

                # 수정
                if not pd.isna(row_id):
                    update_product_mst_by_id(
                        row_id=int(row_id),
                        product_name=product_name,
                        size_name=size_name,
                        product_code=product_code,
                        use_yn=use_yn,
                        length_mm=length_mm,
                        ring_gauge=ring_gauge,
                        smoking_time_text=smoking_time_text,
                        unit_weight_g=unit_weight_g,
                        box_width_cm=box_width_cm,
                        box_depth_cm=box_depth_cm,
                        box_height_cm=box_height_cm,
                    )
                    update_count += 1
                else:
                    # 신규 등록
                    insert_product_mst(
                        product_name=product_name,
                        size_name=size_name,
                        product_code=product_code,
                        use_yn=use_yn,
                        length_mm=length_mm,
                        ring_gauge=ring_gauge,
                        smoking_time_text=smoking_time_text,
                        unit_weight_g=unit_weight_g,
                        box_width_cm=box_width_cm,
                        box_depth_cm=box_depth_cm,
                        box_height_cm=box_height_cm,
                    )
                    insert_count += 1

            st.success(f"저장 완료: 수정 {update_count}건 / 신규 {insert_count}건 / 삭제 {delete_count}건")
            st.rerun()

        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")