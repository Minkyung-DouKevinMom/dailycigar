"""
재고관리 모듈
modules/management/inventory_management.py

사용법:
    import modules.management.inventory_management as inv
    inv.render()
"""

import pandas as pd
import streamlit as st

from db import (
    OUT_TYPE_LABELS,
    delete_stock_out,
    get_sample_summary_by_partner,
    get_stock_detail,
    get_stock_out_list,
    get_stock_out_one,
    get_stock_summary,
    init_stock_out_table,
    insert_stock_out,
    update_stock_out,
)
from db import run_query  # partner_mst, product_mst 조회용


# ──────────────────────────────────────────────
# 헬퍼
# ──────────────────────────────────────────────

def _get_partners() -> dict:
    """partner_id → partner_name 매핑"""
    df = run_query(
        "SELECT id, partner_name FROM partner_mst WHERE COALESCE(status,'active')='active' ORDER BY partner_name"
    )
    if df.empty:
        return {}
    return {f"{int(r['id'])} | {r['partner_name']}": int(r["id"]) for _, r in df.iterrows()}


def _get_active_products() -> dict:
    """product_code → 표시 라벨 매핑"""
    df = run_query(
        "SELECT product_code, product_name, size_name FROM product_mst WHERE use_yn='Y' ORDER BY product_name, size_name"
    )
    if df.empty:
        return {}
    return {
        f"{r['product_code']} | {r['product_name']} {r['size_name']}": r["product_code"]
        for _, r in df.iterrows()
    }


def _stock_color(val):
    """현재고 수치 색상"""
    if val <= 0:
        return "color: red; font-weight: bold"
    if val <= 50:
        return "color: orange; font-weight: bold"
    return "color: green"


def _init_session():
    defaults = {
        "inv_reset_key": 0,
        "inv_selected_out_id": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _reset_form():
    st.session_state.inv_selected_out_id = None
    st.session_state.inv_reset_key += 1


# ──────────────────────────────────────────────
# 탭 1 : 재고 현황
# ──────────────────────────────────────────────

def _tab_stock_summary():
    st.markdown("#### 📦 상품별 현재고")

    col1, col2 = st.columns([3, 1])
    keyword = col1.text_input("상품 검색", placeholder="상품명 / 코드", key="inv_kw")
    include_inactive = col2.checkbox("단종 포함", value=False, key="inv_inactive")

    df = get_stock_summary(keyword=keyword, include_inactive=include_inactive)

    if df.empty:
        st.info("조회된 상품이 없습니다.")
        return

    # 요약 메트릭
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("전체 상품 수", f"{len(df):,} 개")
    m2.metric("총 입고", f"{int(df['total_in'].sum()):,} 개비")
    m3.metric("총 출고", f"{int((df['retail_out']+df['wholesale_out']+df['other_out']).sum()):,} 개비")
    m4.metric("현재고 합계", f"{int(df['current_stock'].sum()):,} 개비")

    st.divider()

    # 컬럼 한글화
    display = df.rename(columns={
        "product_code":   "상품코드",
        "product_name":   "상품명",
        "size_name":      "사이즈",
        "use_yn":         "사용",
        "total_in":       "입고",
        "retail_out":     "소매출고",
        "wholesale_out":  "도매출고",
        "other_out":      "기타출고",
        "current_stock":  "현재고",
    })

    # 현재고 0 이하 강조
    styled = display.style.map(
        _stock_color, subset=["현재고"]
    ).format({
        "입고": "{:,.0f}",
        "소매출고": "{:,.0f}",
        "도매출고": "{:,.0f}",
        "기타출고": "{:,.0f}",
        "현재고": "{:,.0f}",
    })

    st.dataframe(styled, use_container_width=True, hide_index=True, height=480)

    # 상품별 상세 이력 확인
    st.divider()
    st.markdown("##### 🔍 상품별 입출고 상세")
    product_map = _get_active_products()
    if not product_map:
        st.warning("활성 상품이 없습니다.")
        return

    selected_label = st.selectbox("상품 선택", options=[""] + list(product_map.keys()), key="inv_detail_product")
    if selected_label:
        pcode = product_map[selected_label]
        detail_df = get_stock_detail(pcode)
        if detail_df.empty:
            st.info("이력이 없습니다.")
        else:
            # 이벤트 타입 한글화
            type_map = {
                "import":    "📥 입고",
                "retail":    "🏪 소매",
                "wholesale": "🏢 도매",
                "sample":    "🎁 샘플",
                "gift_set":  "🎀 선물세트",
                "disposal":  "🗑️ 폐기",
                "etc":       "📋 기타",
            }
            detail_df["event_type"] = detail_df["event_type"].map(lambda x: type_map.get(x, x))
            detail_df["누적재고"] = (detail_df["qty_in"] - detail_df["qty_out"]).cumsum().astype(int)

            st.dataframe(
                detail_df.rename(columns={
                    "event_date":   "일자",
                    "event_type":   "구분",
                    "ref_name":     "참조",
                    "qty_in":       "입고수",
                    "qty_out":      "출고수",
                    "partner_name": "거래처",
                    "note":         "비고",
                }),
                use_container_width=True,
                hide_index=True,
                height=320,
            )


# ──────────────────────────────────────────────
# 탭 2 : 기타 출고 관리 (샘플/선물세트/폐기)
# ──────────────────────────────────────────────

def _tab_stock_out():
    st.markdown("#### 📤 기타 출고 관리")
    st.caption("도매·소매 이외의 재고 차감 (샘플 제공, 선물세트, 폐기, 기타)을 기록합니다.")

    product_map = _get_active_products()
    partner_map = _get_partners()

    left, right = st.columns([1.3, 1])

    # ── 이력 목록 ──
    with left:
        st.markdown("##### 출고 이력")

        f1, f2 = st.columns(2)
        filter_type = f1.selectbox(
            "출고 유형",
            options=["전체"] + list(OUT_TYPE_LABELS.values()),
            key="inv_filter_type",
        )
        filter_partner_label = f2.selectbox(
            "거래처",
            options=["전체"] + list(partner_map.keys()),
            key="inv_filter_partner",
        )

        out_type_filter = ""
        if filter_type != "전체":
            out_type_filter = {v: k for k, v in OUT_TYPE_LABELS.items()}.get(filter_type, "")

        partner_id_filter = None
        if filter_partner_label != "전체":
            partner_id_filter = partner_map.get(filter_partner_label)

        list_df = get_stock_out_list(
            out_type=out_type_filter,
            partner_id=partner_id_filter,
        )

        if list_df.empty:
            st.info("출고 이력이 없습니다.")
        else:
            list_df["out_type_kr"] = list_df["out_type"].map(OUT_TYPE_LABELS)
            st.dataframe(
                list_df[[
                    "id", "out_date", "product_name", "size_name",
                    "qty", "out_type_kr", "partner_name", "note",
                ]].rename(columns={
                    "id":           "ID",
                    "out_date":     "출고일",
                    "product_name": "상품명",
                    "size_name":    "사이즈",
                    "qty":          "수량",
                    "out_type_kr":  "유형",
                    "partner_name": "거래처",
                    "note":         "비고",
                }),
                use_container_width=True,
                hide_index=True,
                height=380,
            )

            out_options = [
                f"{int(r['id'])} | {r['out_date']} | {r['product_name']} | {OUT_TYPE_LABELS.get(r['out_type'], r['out_type'])} | {int(r['qty'])}개"
                for _, r in list_df.iterrows()
            ]
            selected_out_label = st.selectbox(
                "수정/삭제할 항목 선택",
                options=[""] + out_options,
                key="inv_out_select",
            )
            if st.button("불러오기", use_container_width=True):
                if selected_out_label:
                    st.session_state.inv_selected_out_id = int(
                        selected_out_label.split("|")[0].strip()
                    )
                    st.rerun()

    # ── 등록/수정 폼 ──
    selected = None
    if st.session_state.inv_selected_out_id:
        selected = get_stock_out_one(st.session_state.inv_selected_out_id)

    with right:
        st.markdown(f"##### {'✏️ 출고 수정' if selected else '➕ 출고 등록'}")
        form_key = f"inv_out_form_{st.session_state.inv_reset_key}"

        if not product_map:
            st.warning("활성 상품이 없습니다. product_mst를 먼저 확인해주세요.")
            return

        # 폼 기본값 설정
        default_product_label = list(product_map.keys())[0]
        if selected:
            for label, code in product_map.items():
                if code == selected.get("product_code"):
                    default_product_label = label
                    break

        default_partner_label = list(partner_map.keys())[0] if partner_map else None
        if selected and selected.get("partner_id"):
            for label, pid in partner_map.items():
                if pid == int(selected["partner_id"]):
                    default_partner_label = label
                    break

        default_out_type_label = list(OUT_TYPE_LABELS.values())[0]
        if selected and selected.get("out_type"):
            default_out_type_label = OUT_TYPE_LABELS.get(selected["out_type"], default_out_type_label)

        with st.form(form_key):
            product_label = st.selectbox(
                "상품 *",
                options=list(product_map.keys()),
                index=list(product_map.keys()).index(default_product_label),
            )

            out_date = st.date_input(
                "출고일 *",
                value=(
                    pd.to_datetime(selected["out_date"]).date()
                    if selected and selected.get("out_date")
                    else pd.Timestamp.today().date()
                ),
            )

            out_type_label = st.selectbox(
                "출고 유형 *",
                options=list(OUT_TYPE_LABELS.values()),
                index=list(OUT_TYPE_LABELS.values()).index(default_out_type_label),
            )

            qty = st.number_input(
                "수량 (개비) *",
                min_value=1,
                value=int(selected["qty"]) if selected and selected.get("qty") else 1,
                step=1,
            )

            # 거래처 (샘플일 때만 필수 안내)
            out_type_code = {v: k for k, v in OUT_TYPE_LABELS.items()}.get(out_type_label, "")
            partner_required = out_type_code == "sample"
            partner_label_text = "거래처 *" if partner_required else "거래처 (선택)"

            if partner_map:
                partner_options = ["(없음)"] + list(partner_map.keys())
                default_partner_idx = 0
                if default_partner_label and default_partner_label in partner_map:
                    default_partner_idx = partner_options.index(default_partner_label)

                partner_selected = st.selectbox(
                    partner_label_text,
                    options=partner_options,
                    index=default_partner_idx,
                )
                partner_id_val = partner_map.get(partner_selected)
            else:
                st.info("등록된 거래처가 없습니다.")
                partner_id_val = None

            note = st.text_area(
                "비고",
                value=selected.get("note", "") if selected else "",
                height=80,
                placeholder="예: 신규 거래처 시음용, 행사 증정 등",
            )

            save_col, reset_col, del_col = st.columns(3)
            save_clicked   = save_col.form_submit_button("저장", use_container_width=True)
            reset_clicked  = reset_col.form_submit_button("초기화", use_container_width=True)
            delete_clicked = del_col.form_submit_button(
                "삭제", use_container_width=True, disabled=(selected is None)
            )

        # ── 저장 처리 ──
        if save_clicked:
            if partner_required and not partner_id_val:
                st.error("샘플 유형은 거래처를 반드시 선택해야 합니다.")
            else:
                try:
                    payload = dict(
                        out_date=str(out_date),
                        product_code=product_map[product_label],
                        qty=int(qty),
                        out_type=out_type_code,
                        partner_id=partner_id_val,
                        note=note.strip() or None,
                    )
                    if selected:
                        update_stock_out(row_id=st.session_state.inv_selected_out_id, **payload)
                        st.success("출고 이력이 수정되었습니다.")
                    else:
                        insert_stock_out(**payload)
                        st.success("출고 이력이 등록되었습니다.")
                    _reset_form()
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 중 오류: {e}")

        if reset_clicked:
            _reset_form()
            st.rerun()

        if delete_clicked and selected:
            try:
                delete_stock_out(st.session_state.inv_selected_out_id)
                st.success("출고 이력이 삭제되었습니다.")
                _reset_form()
                st.rerun()
            except Exception as e:
                st.error(f"삭제 중 오류: {e}")


# ──────────────────────────────────────────────
# 탭 3 : 거래처별 샘플 현황
# ──────────────────────────────────────────────

def _tab_sample_by_partner():
    st.markdown("#### 🎁 거래처별 샘플 제공 현황")

    df = get_sample_summary_by_partner()
    if df.empty:
        st.info("샘플 제공 이력이 없습니다.")
        return

    # 거래처별 총계 요약
    partner_total = (
        df.groupby("partner_name")["total_sample_qty"]
        .sum()
        .reset_index()
        .rename(columns={"partner_name": "거래처", "total_sample_qty": "총 샘플 수량"})
        .sort_values("총 샘플 수량", ascending=False)
    )

    col1, col2 = st.columns([1, 1.6])

    with col1:
        st.markdown("##### 거래처별 합계")
        st.dataframe(partner_total, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("##### 거래처 × 상품별 상세")
        st.dataframe(
            df.rename(columns={
                "partner_name":       "거래처",
                "product_code":       "상품코드",
                "product_name":       "상품명",
                "size_name":          "사이즈",
                "total_sample_qty":   "수량합계",
                "first_date":         "최초제공일",
                "last_date":          "최근제공일",
            }),
            use_container_width=True,
            hide_index=True,
            height=400,
        )


# ──────────────────────────────────────────────
# 메인 render
# ──────────────────────────────────────────────

def render():
    _init_session()
    init_stock_out_table()  # stock_out 테이블 없으면 자동 생성

    st.subheader("📦 재고관리")

    tab1, tab2, tab3 = st.tabs(["재고 현황", "기타 출고 관리", "거래처별 샘플 현황"])

    with tab1:
        _tab_stock_summary()

    with tab2:
        _tab_stock_out()

    with tab3:
        _tab_sample_by_partner()


if __name__ == "__main__":
    render()
