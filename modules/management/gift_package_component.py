"""
기프트패키지 구성품(BOM) 관리 모듈
modules/management/gift_package_component.py

기프트패키지(non_cigar_product_mst, 카테고리='기프트패키지') 1개를 판매했을 때
차감되어야 할 시가(product_mst) 상품과 수량을 매핑해서 저장한다.
이 매핑은 retail_upload.py 의 엑셀 업로드 시 자동 재고 차감(선물세트)에 사용된다.

사용법:
    import modules.management.gift_package_component as gpc
    gpc.render()
"""

import streamlit as st

import db


def _init_session():
    defaults = {
        "gpc_selected_gift_id": None,
        "gpc_reset_key": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _get_cigar_products() -> dict:
    """product_code -> 표시 라벨 매핑 (활성 시가만)"""
    df = db.run_query(
        "SELECT product_code, product_name, size_name FROM product_mst WHERE use_yn='Y' "
        "ORDER BY product_name, size_name"
    )
    if df.empty:
        return {}
    return {
        f"{r['product_code']} | {r['product_name']} {r['size_name']}": r["product_code"]
        for _, r in df.iterrows()
    }


def render():
    _init_session()
    db.init_gift_package_component_table()
    db.ensure_stock_out_source_columns()

    st.subheader("🎁 기프트패키지 구성품(재고 차감) 관리")
    st.caption(
        "기프트패키지 1세트 판매 시 실제로 차감되어야 할 시가 상품과 수량을 등록합니다. "
        "여기 등록된 내용은 소매 엑셀 업로드 시 자동으로 재고관리 > 기타출고관리(선물세트)에 반영됩니다."
    )

    gift_df = db.get_gift_package_products()
    if gift_df.empty:
        st.warning(
            f"'{db.GIFT_PACKAGE_CATEGORY}' 카테고리의 상품이 없습니다. "
            "시가 외 상품 마스터에서 먼저 기프트패키지 상품을 등록해주세요."
        )
        return

    gift_options = {
        f"{r['product_code']} | {r['product_name']}" + ("" if int(r['is_active'] or 0) == 1 else " (미사용)"): int(r["id"])
        for _, r in gift_df.iterrows()
    }

    selected_label = st.selectbox("기프트패키지 선택", options=list(gift_options.keys()))
    gift_product_id = gift_options[selected_label]

    if gift_product_id != st.session_state.gpc_selected_gift_id:
        st.session_state.gpc_selected_gift_id = gift_product_id
        st.session_state.gpc_reset_key += 1

    st.divider()

    left, right = st.columns([1.4, 1])

    with left:
        st.markdown("##### 등록된 구성품")
        comp_df = db.get_gift_package_components(gift_product_id)

        if comp_df.empty:
            st.info("등록된 구성품이 없습니다. 오른쪽에서 추가해주세요.")
        else:
            display = comp_df.rename(columns={
                "component_product_code": "시가 상품코드",
                "product_name": "상품명",
                "size_name": "사이즈",
                "qty_per_set": "세트당 차감수량",
                "is_active": "사용",
                "notes": "비고",
                "updated_at": "수정일시",
            }).copy()
            display["사용"] = display["사용"].apply(lambda x: "Y" if int(x or 0) == 1 else "N")

            st.dataframe(
                display[["시가 상품코드", "상품명", "사이즈", "세트당 차감수량", "사용", "비고", "수정일시"]],
                use_container_width=True,
                hide_index=True,
                height=280,
            )

            comp_options = [
                f"{int(r['id'])} | {r['component_product_code']} | {r['product_name']} | 수량 {int(r['qty_per_set'])}"
                for _, r in comp_df.iterrows()
            ]
            selected_comp_label = st.selectbox(
                "수정/삭제할 구성품 선택",
                options=[""] + comp_options,
                key=f"gpc_comp_select_{st.session_state.gpc_reset_key}",
            )

            if selected_comp_label:
                comp_id = int(selected_comp_label.split("|")[0].strip())
                comp_row = comp_df[comp_df["id"] == comp_id].iloc[0]

                with st.form(f"gpc_edit_form_{comp_id}_{st.session_state.gpc_reset_key}"):
                    e_qty = st.number_input(
                        "세트당 차감수량",
                        min_value=1,
                        value=int(comp_row["qty_per_set"]),
                        step=1,
                    )
                    e_active = st.selectbox(
                        "사용여부",
                        options=[1, 0],
                        format_func=lambda x: "사용" if x == 1 else "미사용",
                        index=0 if int(comp_row["is_active"] or 1) == 1 else 1,
                    )
                    e_notes = st.text_input("비고", value=comp_row.get("notes") or "")

                    ec1, ec2 = st.columns(2)
                    upd_clicked = ec1.form_submit_button("수정 저장", use_container_width=True)
                    del_clicked = ec2.form_submit_button("삭제", use_container_width=True)

                    if upd_clicked:
                        db.update_gift_package_component(comp_id, int(e_qty), int(e_active), e_notes.strip())
                        st.success("수정되었습니다.")
                        st.session_state.gpc_reset_key += 1
                        st.rerun()

                    if del_clicked:
                        db.delete_gift_package_component(comp_id)
                        st.success("삭제되었습니다. (과거에 이미 자동 차감된 재고 이력에는 영향 없음)")
                        st.session_state.gpc_reset_key += 1
                        st.rerun()

    with right:
        st.markdown("##### 구성품 추가")
        cigar_map = _get_cigar_products()

        if not cigar_map:
            st.warning("등록된 시가 상품이 없습니다. product_mst를 먼저 확인해주세요.")
            return

        with st.form(f"gpc_add_form_{st.session_state.gpc_reset_key}", clear_on_submit=True):
            product_label = st.selectbox("시가 상품 *", options=list(cigar_map.keys()))
            qty_per_set = st.number_input("세트당 차감수량 *", min_value=1, value=1, step=1)
            notes = st.text_input("비고", placeholder="예: 3구 세트 중 1개비")

            add_clicked = st.form_submit_button("추가", use_container_width=True)

            if add_clicked:
                comp_code = cigar_map[product_label]
                existing = db.get_gift_package_components(gift_product_id)
                if not existing.empty and comp_code in existing["component_product_code"].values:
                    st.error("이미 등록된 시가입니다. 목록에서 수정해주세요.")
                else:
                    db.insert_gift_package_component(
                        gift_product_id=gift_product_id,
                        component_product_code=comp_code,
                        qty_per_set=int(qty_per_set),
                        notes=notes.strip() or None,
                    )
                    st.success("추가되었습니다.")
                    st.session_state.gpc_reset_key += 1
                    st.rerun()


if __name__ == "__main__":
    render()
