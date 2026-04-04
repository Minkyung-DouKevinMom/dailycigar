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
    st.caption("표에서는 기본 정보만 관리하고, 선택한 행의 메뉴설명/상세설명은 아래에서 길게 편집할 수 있습니다.")

    # ── 원본 전체 데이터 로드 ──────────────────────────────────────────
    df = get_all_blend_profile()

    if df.empty:
        st.info("blend_profile_mst 데이터가 없습니다.")
        df = pd.DataFrame(columns=[
            "id", "product_name", "flavor", "strength", "guide",
            "description", "detail_description", "created_at", "updated_at",
        ])

    for col in ("description", "detail_description"):
        if col not in df.columns:
            df[col] = None

    # ── 검색 필터 ──────────────────────────────────────────────────────
    keyword = st.text_input("검색", placeholder="상품명 / 향 / 강도 / 가이드 / 메뉴 설명 / 상세 설명")

    work_df = df.copy()
    if keyword:
        mask = (
            work_df["product_name"].fillna("").astype(str).str.contains(keyword, case=False)
            | work_df["flavor"].fillna("").astype(str).str.contains(keyword, case=False)
            | work_df["strength"].fillna("").astype(str).str.contains(keyword, case=False)
            | work_df["guide"].fillna("").astype(str).str.contains(keyword, case=False)
            | work_df["description"].fillna("").astype(str).str.contains(keyword, case=False)
            | work_df["detail_description"].fillna("").astype(str).str.contains(keyword, case=False)
        )
        work_df = work_df[mask].copy()

    # ── 그리드 (기본 컬럼만 표시) ──────────────────────────────────────
    if "delete_yn" not in work_df.columns:
        work_df.insert(0, "delete_yn", False)

    display_columns = ["delete_yn", "id", "product_name", "flavor", "strength", "guide"]

    edited_df = st.data_editor(
        work_df[display_columns].copy(),
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "delete_yn": st.column_config.CheckboxColumn("삭제", help="체크 후 저장하면 삭제됩니다."),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "product_name": st.column_config.TextColumn("상품명", required=True, width="medium"),
            "flavor": st.column_config.TextColumn("향/노트", width="medium"),
            "strength": st.column_config.TextColumn("강도", width="small"),
            "guide": st.column_config.TextColumn("가이드", width="large"),
        },
        disabled=["id"],
        key="blend_profile_editor",
    )

    # ── 설명 편집 섹션 ─────────────────────────────────────────────────
    st.divider()
    st.markdown("### 선택한 행 설명 편집")

    # selectbox 옵션: 원본 work_df 기준으로 만들어야 description을 가져올 수 있음
    # id와 product_name을 key로 사용
    work_df["_label"] = work_df.apply(
        lambda r: f"{int(float(r['id'])) if pd.notna(r['id']) else '신규'} | {str(r['product_name'] or '').strip()}",
        axis=1,
    )
    row_options = [lbl for lbl in work_df["_label"].tolist() if str(lbl).replace("|","").strip()]

    selected_label = st.selectbox(
        "편집 대상",
        options=[""] + row_options,
        index=0,
        help="행을 선택하면 아래에서 메뉴설명/상세설명을 편집할 수 있습니다.",
    )

    # ── 선택된 행의 description 조회 (원본 work_df에서 직접) ──────────
    selected_description = ""
    selected_detail_description = ""
    selected_work_idx = None

    if selected_label:
        match_rows = work_df[work_df["_label"] == selected_label]
        if not match_rows.empty:
            selected_work_idx = match_rows.index[0]
            row_data = match_rows.iloc[0]
            selected_description       = str(row_data.get("description") or "")
            selected_detail_description = str(row_data.get("detail_description") or "")

    # ── text_area: value로 직접 주입, key에 label 포함해 행 변경 시 리셋 ──
    col_left, col_right = st.columns(2)

    with col_left:
        menu_description = st.text_area(
            "메뉴 설명",
            value=selected_description,
            height=260,
            placeholder="메뉴판용 짧고 응축된 설명을 입력하세요.",
            key=f"desc_{selected_label}",
        )

    with col_right:
        detail_description = st.text_area(
            "상세 설명",
            value=selected_detail_description,
            height=260,
            placeholder="브랜드/제품의 상세 소개를 입력하세요.",
            key=f"detail_{selected_label}",
        )

    # 디버그: 실제로 조회된 값 확인 (개발 중에만 사용, 배포 시 제거)
    if selected_label:
        with st.expander("🔍 디버그 정보 (배포 시 제거)", expanded=False):
            st.write(f"selected_label: `{selected_label}`")
            st.write(f"selected_description: `{selected_description}`")
            st.write(f"selected_detail_description: `{selected_detail_description}`")
            st.write(f"work_df columns: {list(work_df.columns)}")

    # ── 저장 ───────────────────────────────────────────────────────────
    # edited_df에 description 병합 (선택된 행만 text_area 값, 나머지는 원본)
    edited_df["description"]       = None
    edited_df["detail_description"] = None

    # 전체 행의 description을 work_df에서 복원
    for idx, row in edited_df.iterrows():
        row_id = row.get("id")
        if pd.notna(row_id):
            try:
                rid = int(float(row_id))
                orig = work_df[work_df["id"].apply(
                    lambda x: int(float(x)) if pd.notna(x) else -1
                ) == rid]
                if not orig.empty:
                    edited_df.at[idx, "description"]       = orig.iloc[0].get("description")
                    edited_df.at[idx, "detail_description"] = orig.iloc[0].get("detail_description")
            except Exception:
                pass

    # 선택된 행만 text_area 값으로 덮어쓰기
    if selected_work_idx is not None:
        # edited_df의 인덱스와 work_df 인덱스가 같으므로 직접 사용
        if selected_work_idx in edited_df.index:
            edited_df.at[selected_work_idx, "description"]       = menu_description
            edited_df.at[selected_work_idx, "detail_description"] = detail_description

    c1, c2 = st.columns([1, 5])
    with c1:
        save_btn = st.button("변경사항 저장", type="primary")
    with c2:
        st.write("삭제 체크 기능은 유지됩니다. 새 행도 추가할 수 있습니다.")

    def null_if_blank(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    if save_btn:
        try:
            update_count = insert_count = delete_count = 0

            for _, row in edited_df.iterrows():
                row_id           = row.get("id")
                delete_yn        = bool(row.get("delete_yn", False))
                product_name     = null_if_blank(row.get("product_name"))
                flavor           = null_if_blank(row.get("flavor"))
                strength         = null_if_blank(row.get("strength"))
                guide            = null_if_blank(row.get("guide"))
                description      = null_if_blank(row.get("description"))
                detail_desc      = null_if_blank(row.get("detail_description"))

                is_blank_new_row = (
                    pd.isna(row_id)
                    and not product_name and not flavor
                    and not strength and not guide
                    and not description and not detail_desc
                )
                if is_blank_new_row:
                    continue

                if delete_yn and pd.notna(row_id):
                    delete_blend_profile(int(float(row_id)))
                    delete_count += 1
                    continue

                if not product_name:
                    st.warning("상품명이 없는 행은 저장되지 않았습니다.")
                    continue

                if pd.notna(row_id):
                    update_blend_profile(
                        row_id=int(float(row_id)),
                        product_name=product_name,
                        flavor=flavor,
                        strength=strength,
                        guide=guide,
                        description=description,
                        detail_description=detail_desc,
                    )
                    update_count += 1
                else:
                    upsert_blend_profile(
                        product_name=product_name,
                        flavor=flavor,
                        strength=strength,
                        guide=guide,
                        description=description,
                        detail_description=detail_desc,
                    )
                    insert_count += 1

            st.success(f"저장 완료: 수정 {update_count}건 / 신규 {insert_count}건 / 삭제 {delete_count}건")
            st.rerun()

        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")
