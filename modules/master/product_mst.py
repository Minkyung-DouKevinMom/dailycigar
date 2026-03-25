import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from db import (
    get_all_product_mst_for_edit,
    update_product_mst_by_id,
    insert_product_mst,
    delete_product_mst_by_id,
)

def render():
    st.subheader("시가")
    
    # -----------------------------
    # 데이터 조회
    # -----------------------------
    df = get_all_product_mst_for_edit()

    if "use_yn" not in df.columns:
        df["use_yn"] = "Y"

    df["use_yn"] = df["use_yn"].fillna("Y").astype(str).str.upper()
    df.loc[~df["use_yn"].isin(["Y", "N"]), "use_yn"] = "Y"

    if df.empty:
        df = pd.DataFrame(columns=[
            "id", "product_name", "size_name", "product_code", "use_yn",
            "length_mm", "ring_gauge", "smoking_time_text",
            "unit_weight_g", "box_width_cm", "box_depth_cm", "box_height_cm",
            "created_at", "updated_at"
        ])

    # -----------------------------
    # 검색
    # -----------------------------
    keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드")

    if keyword:
        df = df[
            df["product_name"].fillna("").str.contains(keyword, case=False) |
            df["size_name"].fillna("").str.contains(keyword, case=False) |
            df["product_code"].fillna("").str.contains(keyword, case=False)
        ].copy()

    # 삭제 컬럼
    if "delete_yn" not in df.columns:
        df.insert(0, "delete_yn", False)

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

    grid_df = df[display_columns].copy()

    # -----------------------------
    # 신규 행 추가 버튼
    # -----------------------------
    if st.button("➕ 신규 행 추가"):
        new_row = {col: None for col in grid_df.columns}
        new_row["delete_yn"] = False
        new_row["use_yn"] = "Y"
        grid_df = pd.concat([grid_df, pd.DataFrame([new_row])], ignore_index=True)

    # -----------------------------
    # AgGrid 설정
    # -----------------------------
    gb = GridOptionsBuilder.from_dataframe(grid_df)

    gb.configure_default_column(
        sortable=True,
        filter=True,
        resizable=True,
        editable=True,
    )

    gb.configure_column("delete_yn", header_name="삭제", editable=True, cellEditor="agCheckboxCellEditor")
    gb.configure_column("id", header_name="ID", editable=False)

    gb.configure_column("product_name", header_name="상품명")
    gb.configure_column("size_name", header_name="사이즈")
    gb.configure_column("product_code", header_name="코드")

    gb.configure_column(
        "use_yn",
        header_name="사용여부",
        cellEditor="agSelectCellEditor",
        cellEditorParams={"values": ["Y", "N"]},
    )

    gb.configure_column("length_mm", header_name="길이(mm)")
    gb.configure_column("ring_gauge", header_name="링게이지")
    gb.configure_column("smoking_time_text", header_name="흡연시간")

    gb.configure_column("unit_weight_g", header_name="개당무게(g)")
    gb.configure_column("box_width_cm", header_name="박스가로(cm)")
    gb.configure_column("box_depth_cm", header_name="박스세로(cm)")
    gb.configure_column("box_height_cm", header_name="박스높이(cm)")

    gb.configure_column("created_at", header_name="생성일", editable=False)
    gb.configure_column("updated_at", header_name="수정일", editable=False)

    grid_options = gb.build()

    grid_response = AgGrid(
        grid_df,
        gridOptions=grid_options,
        height=500,
        width="100%",
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        fit_columns_on_grid_load=True,
        key="product_grid",
    )

    edited_df = pd.DataFrame(grid_response["data"])

    # -----------------------------
    # 저장 버튼
    # -----------------------------
    if st.button("💾 저장", type="primary"):
        try:
            update_count = 0
            insert_count = 0
            delete_count = 0

            def null(v):
                if pd.isna(v) or str(v).strip() == "":
                    return None
                return v

            for _, row in edited_df.iterrows():
                row_id = row.get("id")
                delete_yn = bool(row.get("delete_yn", False))

                product_name = null(row.get("product_name"))
                size_name = null(row.get("size_name"))
                product_code = null(row.get("product_code"))

                length_mm = null(row.get("length_mm"))
                ring_gauge = null(row.get("ring_gauge"))
                smoking_time_text = null(row.get("smoking_time_text"))

                unit_weight_g = null(row.get("unit_weight_g"))
                box_width_cm = null(row.get("box_width_cm"))
                box_depth_cm = null(row.get("box_depth_cm"))
                box_height_cm = null(row.get("box_height_cm"))

                use_yn = str(row.get("use_yn") or "Y").upper()

                # 삭제
                if delete_yn and pd.notna(row_id):
                    delete_product_mst_by_id(int(row_id))
                    delete_count += 1
                    continue

                # 빈 행 무시
                if not product_name and not size_name and not product_code:
                    continue

                # 필수값 체크
                if not product_name or not size_name or not product_code:
                    st.warning(f"필수값 누락: {product_name}")
                    continue

                # 수정
                if pd.notna(row_id):
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

                # 신규
                else:
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

            st.success(f"완료: 수정 {update_count} / 신규 {insert_count} / 삭제 {delete_count}")
            st.rerun()

        except Exception as e:
            st.error(f"에러: {e}")