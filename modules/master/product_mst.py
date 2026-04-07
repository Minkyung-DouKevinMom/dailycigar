import streamlit as st
import pandas as pd

from db import (
    get_all_product_mst_for_edit,
    update_product_mst_by_id,
    insert_product_mst,
    delete_product_mst_by_id,
)


def _render_fallback_editor(grid_df: pd.DataFrame):
    st.warning("그리드 컴포넌트 로딩에 실패하여 기본 편집기로 표시합니다.")

    edited_df = st.data_editor(
        grid_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "delete_yn": st.column_config.CheckboxColumn("삭제"),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "product_name": st.column_config.TextColumn("상품명"),
            "size_name": st.column_config.TextColumn("사이즈"),
            "product_code": st.column_config.TextColumn("코드"),
            "use_yn": st.column_config.SelectboxColumn("사용여부", options=["Y", "N"]),
            "length_mm": st.column_config.NumberColumn("길이(mm)"),
            "ring_gauge": st.column_config.NumberColumn("링게이지"),
            "smoking_time_text": st.column_config.TextColumn("흡연시간"),
            "unit_weight_g": st.column_config.NumberColumn("개당무게(g)"),
            "box_width_cm": st.column_config.NumberColumn("박스가로(cm)"),
            "box_depth_cm": st.column_config.NumberColumn("박스세로(cm)"),
            "box_height_cm": st.column_config.NumberColumn("박스높이(cm)"),
        },
        key="product_grid_fallback",
    )
    return pd.DataFrame(edited_df)


def render():
    st.subheader("시가")
    st.caption("수정은 셀을 직접 편집 후 저장합니다. 삭제는 '삭제' 체크 후 저장합니다.")

    df = get_all_product_mst_for_edit()

    if df is None:
        df = pd.DataFrame()

    if "use_yn" not in df.columns:
        df["use_yn"] = "Y"

    if not df.empty:
        df["use_yn"] = (
            df["use_yn"]
            .fillna("Y")
            .astype(str)
            .str.upper()
        )
        df.loc[~df["use_yn"].isin(["Y", "N"]), "use_yn"] = "Y"

    if df.empty:
        df = pd.DataFrame(
            columns=[
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
        )

    keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드")

    if keyword:
        keyword = str(keyword).strip()
        df = df[
            df["product_name"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
            | df["size_name"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
            | df["product_code"].fillna("").astype(str).str.contains(keyword, case=False, na=False)
        ].copy()

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
    ]

    for col in display_columns:
        if col not in df.columns:
            df[col] = None

    grid_df = df[display_columns].copy()

    col1, col2 = st.columns([1, 4])

    with col1:
        if st.button("➕ 신규 행 추가", use_container_width=True):
            new_row = {col: None for col in grid_df.columns}
            new_row["delete_yn"] = False
            new_row["use_yn"] = "Y"
            grid_df = pd.concat([grid_df, pd.DataFrame([new_row])], ignore_index=True)

    edited_df = None
    aggrid_error = None

    # AgGrid는 클라우드에서 깨질 수 있어 런타임 import + fallback 처리
    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

        with st.container(border=True):
            gb = GridOptionsBuilder.from_dataframe(grid_df)

            gb.configure_default_column(
                sortable=True,
                filter=True,
                resizable=True,
                editable=True,
                wrapText=True,
                autoHeight=False,
            )

            gb.configure_grid_options(
                rowHeight=42,
                headerHeight=42,
                suppressHorizontalScroll=False,
                domLayout="normal",
                animateRows=False,
            )

            gb.configure_column(
                "delete_yn",
                header_name="삭제",
                editable=True,
                cellEditor="agCheckboxCellEditor",
                checkboxSelection=False,
                width=70,
                pinned="left",
            )
            gb.configure_column("id", header_name="ID", editable=False, width=80, pinned="left")
            gb.configure_column("product_name", header_name="상품명", minWidth=220, flex=2)
            gb.configure_column("size_name", header_name="사이즈", minWidth=140, flex=1)
            gb.configure_column("product_code", header_name="코드", minWidth=140, flex=1)
            gb.configure_column(
                "use_yn",
                header_name="사용여부",
                cellEditor="agSelectCellEditor",
                cellEditorParams={"values": ["Y", "N"]},
                width=100,
            )
            gb.configure_column("length_mm", header_name="길이(mm)", width=110)
            gb.configure_column("ring_gauge", header_name="링게이지", width=110)
            gb.configure_column("smoking_time_text", header_name="흡연시간", minWidth=140, flex=1)
            gb.configure_column("unit_weight_g", header_name="개당무게(g)", width=120)
            gb.configure_column("box_width_cm", header_name="박스가로(cm)", width=130)
            gb.configure_column("box_depth_cm", header_name="박스세로(cm)", width=130)
            gb.configure_column("box_height_cm", header_name="박스높이(cm)", width=130)

            grid_options = gb.build()

            grid_response = AgGrid(
                grid_df,
                gridOptions=grid_options,
                height=650,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.VALUE_CHANGED,
                fit_columns_on_grid_load=False,
                theme="streamlit",
                key="product_grid",
            )

            edited_df = pd.DataFrame(grid_response["data"])

    except Exception as e:
        aggrid_error = e

    if edited_df is None:
        if aggrid_error:
            st.error(f"AgGrid 오류: {aggrid_error}")
        with st.container(border=True):
            edited_df = _render_fallback_editor(grid_df)

    st.caption(f"총 {len(edited_df):,}건 조회되었습니다.")
    st.info("삭제할 행은 맨 왼쪽 '삭제'를 체크한 뒤 저장하세요.")

    if st.button("저장", type="primary", use_container_width=True):
        try:
            update_count = 0
            insert_count = 0
            delete_count = 0

            def null(v):
                if pd.isna(v):
                    return None
                if isinstance(v, str) and v.strip() == "":
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

                if use_yn not in ["Y", "N"]:
                    use_yn = "Y"

                # 기존 행 삭제
                if delete_yn and pd.notna(row_id):
                    delete_product_mst_by_id(int(row_id))
                    delete_count += 1
                    continue

                # 신규 빈 행 무시
                if not product_name and not size_name and not product_code:
                    continue

                # 필수값 체크
                if not product_name or not size_name or not product_code:
                    st.warning(
                        f"필수값 누락: 상품명={product_name}, 사이즈={size_name}, 코드={product_code}"
                    )
                    continue

                if pd.notna(row_id) and str(row_id).strip() != "":
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