"""
기프트패키지 구성품(BOM) 관리 모듈
modules/management/gift_package_component.py

기프트패키지(non_cigar_product_mst, 카테고리='기프트패키지') 1개를 판매했을 때
차감되어야 할 상품(시가 + 기타)과 수량, 그리고 각 구성품의 개당원가/개당판매가를 관리한다.
- 재고 차감: retail_upload.py 의 엑셀 업로드 시 자동 재고 차감(선물세트)에 사용된다.
- 손익 계산: 구성품별 개당원가/개당판매가는 기프트패키지 판매분의 원가·매출을
  구성품 단위로 정확하게 귀속시키는 데 사용된다 (개당판매가는 세트 실제 판매금액을
  나눌 때의 비중으로만 쓰이며, 그 자체가 실제 청구 금액은 아니다).

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


def _get_non_cigar_components() -> dict:
    """product_code -> 표시 라벨 매핑 (기프트패키지 카테고리 제외, 활성 상품만)"""
    df = db.run_query(
        "SELECT product_code, product_name FROM non_cigar_product_mst "
        "WHERE COALESCE(product_category, '') != ? AND COALESCE(is_active, 1) = 1 "
        "ORDER BY product_name",
        [db.GIFT_PACKAGE_CATEGORY],
    )
    if df.empty:
        return {}
    return {
        f"{r['product_code']} | {r['product_name']}": r["product_code"]
        for _, r in df.iterrows()
    }


def _default_cigar_price(product_code: str):
    """시가 상품코드 -> (개당원가, 개당판매가) 기본값. 마스터에 없으면 (0, 0)."""
    df = db.get_product_price_by_code(product_code)
    if df.empty:
        return 0.0, 0.0
    row = df.iloc[0]
    return float(row.get("korea_cost_krw") or 0), float(row.get("retail_price_krw") or 0)


def _default_non_cigar_price(product_code: str):
    """기타 상품코드 -> (개당원가, 개당판매가) 기본값. 마스터에 없으면 (0, 0)."""
    df = db.run_query(
        "SELECT purchase_price, retail_price FROM non_cigar_product_mst WHERE product_code = ?",
        [product_code],
    )
    if df.empty:
        return 0.0, 0.0
    row = df.iloc[0]
    return float(row.get("purchase_price") or 0), float(row.get("retail_price") or 0)


def render():
    _init_session()
    db.init_gift_package_component_table()
    db.ensure_stock_out_source_columns()

    st.subheader("🎁 기프트패키지 구성품(재고 차감 · 원가/판매가) 관리")
    st.caption(
        "기프트패키지 1세트 판매 시 실제로 차감되어야 할 상품(시가 + 기타)과 수량을 등록합니다. "
        "여기 등록된 내용은 소매 엑셀 업로드 시 자동으로 재고관리 > 기타출고관리(선물세트)에 반영됩니다. "
        "개당원가/개당판매가는 손익 계산에서 구성품별 원가·매출을 나누는 데 사용됩니다 "
        "(개당판매가는 실제 청구가가 아니라 세트 실판매금액을 나눌 때의 비중 역할만 합니다)."
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

    left, right = st.columns([1.6, 1])

    with left:
        st.markdown("##### 등록된 구성품")
        comp_df = db.get_gift_package_components(gift_product_id)

        if comp_df.empty:
            st.info("등록된 구성품이 없습니다. 오른쪽에서 추가해주세요.")
        else:
            total_cost = (comp_df["unit_cost_krw"].fillna(0) * comp_df["qty_per_set"].fillna(0)).sum()
            total_price = (comp_df["unit_price_krw"].fillna(0) * comp_df["qty_per_set"].fillna(0)).sum()
            m1, m2 = st.columns(2)
            m1.metric("세트 구성품 원가 합계", f"₩{total_cost:,.0f}")
            m2.metric("세트 구성품 판매가 합계(배분 비중 기준)", f"₩{total_price:,.0f}")

            display = comp_df.rename(columns={
                "component_type": "유형",
                "component_product_code": "상품코드",
                "product_name": "상품명",
                "size_name": "사이즈",
                "qty_per_set": "세트당 수량",
                "unit_cost_krw": "개당원가",
                "unit_price_krw": "개당판매가",
                "is_active": "사용",
                "notes": "비고",
                "updated_at": "수정일시",
            }).copy()
            display["유형"] = display["유형"].map({"cigar": "시가", "non_cigar": "기타"}).fillna(display["유형"])
            display["사용"] = display["사용"].apply(lambda x: "Y" if int(x or 0) == 1 else "N")

            st.dataframe(
                display[["유형", "상품코드", "상품명", "사이즈", "세트당 수량",
                         "개당원가", "개당판매가", "사용", "비고", "수정일시"]],
                use_container_width=True,
                hide_index=True,
                height=280,
            )

            comp_options = [
                f"{int(r['id'])} | {'시가' if r['component_type'] == 'cigar' else '기타'} | "
                f"{r['component_product_code']} | {r['product_name']} | 수량 {int(r['qty_per_set'])}"
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
                        "세트당 수량",
                        min_value=1,
                        value=int(comp_row["qty_per_set"]),
                        step=1,
                    )
                    e_cost = st.number_input(
                        "개당원가 (₩)",
                        min_value=0,
                        value=int(comp_row["unit_cost_krw"] or 0),
                        step=100,
                    )
                    e_price = st.number_input(
                        "개당판매가 (₩) — 세트 실판매금액 배분 비중으로 사용",
                        min_value=0,
                        value=int(comp_row["unit_price_krw"] or 0),
                        step=100,
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
                        db.update_gift_package_component(
                            comp_id, int(e_qty), float(e_cost), float(e_price),
                            int(e_active), e_notes.strip(),
                        )
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

        add_type_label = st.radio(
            "구성품 유형",
            options=["시가", "기타(악세사리 등)"],
            horizontal=True,
            key=f"gpc_add_type_{st.session_state.gpc_reset_key}",
        )
        add_type = "cigar" if add_type_label == "시가" else "non_cigar"

        if add_type == "cigar":
            product_map = _get_cigar_products()
            empty_msg = "등록된 시가 상품이 없습니다. product_mst를 먼저 확인해주세요."
        else:
            product_map = _get_non_cigar_components()
            empty_msg = "등록 가능한 기타 상품이 없습니다. 시가 외 상품 마스터를 먼저 확인해주세요."

        if not product_map:
            st.warning(empty_msg)
            return

        product_label = st.selectbox(
            "상품 선택 *",
            options=list(product_map.keys()),
            key=f"gpc_add_product_{st.session_state.gpc_reset_key}_{add_type}",
        )
        selected_code = product_map[product_label]

        default_cost, default_price = (
            _default_cigar_price(selected_code) if add_type == "cigar"
            else _default_non_cigar_price(selected_code)
        )

        with st.form(f"gpc_add_form_{st.session_state.gpc_reset_key}_{add_type}", clear_on_submit=True):
            qty_per_set = st.number_input("세트당 수량 *", min_value=1, value=1, step=1)
            unit_cost = st.number_input(
                "개당원가 (₩)", min_value=0, value=int(default_cost), step=100,
                help="상품 마스터 기준가로 자동 채워집니다. 세트 전용 매입단가가 다르면 직접 수정하세요.",
            )
            unit_price = st.number_input(
                "개당판매가 (₩)", min_value=0, value=int(default_price), step=100,
                help="세트 실제 판매금액을 구성품별로 나눌 때 비중으로만 쓰입니다 (실제 청구가가 아님).",
            )
            notes = st.text_input("비고", placeholder="예: 3구 세트 중 1개비")

            add_clicked = st.form_submit_button("추가", use_container_width=True)

            if add_clicked:
                existing = db.get_gift_package_components(gift_product_id)
                is_dup = (
                    not existing.empty
                    and ((existing["component_type"] == add_type)
                         & (existing["component_product_code"] == selected_code)).any()
                )
                if is_dup:
                    st.error("이미 등록된 상품입니다. 목록에서 수정해주세요.")
                else:
                    db.insert_gift_package_component(
                        gift_product_id=gift_product_id,
                        component_type=add_type,
                        component_product_code=selected_code,
                        qty_per_set=int(qty_per_set),
                        unit_cost_krw=float(unit_cost),
                        unit_price_krw=float(unit_price),
                        notes=notes.strip() or None,
                    )
                    st.success("추가되었습니다.")
                    st.session_state.gpc_reset_key += 1
                    st.rerun()


if __name__ == "__main__":
    render()
