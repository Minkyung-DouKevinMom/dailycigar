import streamlit as st
import pandas as pd
from db import (
    get_all_blend_profile,
    update_blend_profile,
    upsert_blend_profile,
    delete_blend_profile,
)

def render():
    st.subheader("블렌드 프로파일 관리")
    st.set_page_config(page_title="Blend Profile MST", layout="wide")
    st.caption("표에서 직접 수정 후 저장할 수 있습니다.")

    df = get_all_blend_profile()

    if df.empty:
        st.info("blend_profile_mst 데이터가 없습니다.")
        df = pd.DataFrame(columns=[
            "id", "product_name", "flavor", "strength", "guide", "created_at", "updated_at"
        ])

    keyword = st.text_input("검색", placeholder="상품명 / 향 / 강도 / 가이드")
    if keyword:
        mask = (
            df["product_name"].fillna("").str.contains(keyword, case=False) |
            df["flavor"].fillna("").str.contains(keyword, case=False) |
            df["strength"].fillna("").str.contains(keyword, case=False) |
            df["guide"].fillna("").str.contains(keyword, case=False)
        )
        df = df[mask].copy()

    if "delete_yn" not in df.columns:
        df.insert(0, "delete_yn", False)

    display_columns = [
        "delete_yn",
        "id",
        "product_name",
        "flavor",
        "strength",
        "guide",
        "created_at",
        "updated_at",
    ]

    edit_df = df[display_columns].copy()

    edited_df = st.data_editor(
        edit_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "delete_yn": st.column_config.CheckboxColumn("삭제", help="체크 후 저장하면 삭제됩니다."),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "product_name": st.column_config.TextColumn("상품명", required=True),
            "flavor": st.column_config.TextColumn("향/노트"),
            "strength": st.column_config.TextColumn("강도"),
            "guide": st.column_config.TextColumn("가이드"),
            "created_at": st.column_config.TextColumn("생성일", disabled=True),
            "updated_at": st.column_config.TextColumn("수정일", disabled=True),
        },
        disabled=["id", "created_at", "updated_at"],
        key="blend_profile_editor",
    )

    c1, c2 = st.columns([1, 5])
    with c1:
        save_btn = st.button("변경사항 저장", type="primary")
    with c2:
        st.write("새 행 추가 가능 / ID가 없으면 신규 등록됩니다.")

    def null_if_blank(v):
        if pd.isna(v):
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    if save_btn:
        try:
            update_count = 0
            insert_count = 0
            delete_count = 0

            for _, row in edited_df.iterrows():
                row_id = row.get("id")
                delete_yn = bool(row.get("delete_yn", False))

                product_name = null_if_blank(row.get("product_name"))
                flavor = null_if_blank(row.get("flavor"))
                strength = null_if_blank(row.get("strength"))
                guide = null_if_blank(row.get("guide"))

                is_blank_new_row = (
                    pd.isna(row_id)
                    and not product_name
                    and not flavor
                    and not strength
                    and not guide
                )
                if is_blank_new_row:
                    continue

                if delete_yn and not pd.isna(row_id):
                    delete_blend_profile(int(row_id))
                    delete_count += 1
                    continue

                if not product_name:
                    st.warning("상품명이 없는 행은 저장되지 않았습니다.")
                    continue

                if not pd.isna(row_id):
                    update_blend_profile(
                        row_id=int(row_id),
                        product_name=product_name,
                        flavor=flavor,
                        strength=strength,
                        guide=guide,
                    )
                    update_count += 1
                else:
                    upsert_blend_profile(
                        product_name=product_name,
                        flavor=flavor,
                        strength=strength,
                        guide=guide,
                    )
                    insert_count += 1

            st.success(f"저장 완료: 수정 {update_count}건 / 신규 {insert_count}건 / 삭제 {delete_count}건")
            st.rerun()

        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")