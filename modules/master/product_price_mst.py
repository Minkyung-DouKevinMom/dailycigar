import streamlit as st
import pandas as pd
from db import (
    get_all_product_price_mst,
    update_product_price_mst,
    delete_product_price_mst,
)


def _fmt(v) -> str:
    try:
        return f"₩{float(v):,.0f}"
    except Exception:
        return "₩0"


def render():
    st.subheader("💰 가격 마스터")
    st.caption("수입제품 저장 시 자동 갱신됩니다. 직접 수정도 가능합니다.")

    df = get_all_product_price_mst()

    # ── 검색 필터 ──────────────────────────────────
    col_s1, col_s2, col_s3 = st.columns([2, 2, 1])
    with col_s1:
        kw = st.text_input("검색 (상품명 / 사이즈 / 코드)", key="ppm_search")
    with col_s2:
        active_filter = st.selectbox("활성 여부", ["전체", "활성만", "비활성만"], key="ppm_active")
    with col_s3:
        st.write("")
        st.write("")
        refresh = st.button("🔄 새로고침", use_container_width=True, key="ppm_refresh")

    if not df.empty:
        if kw.strip():
            mask = (
                df["product_name"].str.contains(kw.strip(), case=False, na=False)
                | df["size_name"].str.contains(kw.strip(), case=False, na=False)
                | df["product_code"].fillna("").str.contains(kw.strip(), case=False, na=False)
            )
            df = df[mask]
        if active_filter == "활성만":
            df = df[df["is_active"] == 1]
        elif active_filter == "비활성만":
            df = df[df["is_active"] == 0]

    # ── 요약 카드 ──────────────────────────────────
    if not df.empty:
        total = len(df)
        active_cnt = int((df["is_active"] == 1).sum())

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("전체 품목", f"{total:,}개")
        m2.metric("활성", f"{active_cnt:,}개")
        m3.metric("평균 공급가", _fmt(df["supply_price_krw"].mean()))
        m4.metric("평균 소비자가", _fmt(df["retail_price_krw"].mean()))
        m5.metric("평균 매장운영가", _fmt(df["store_retail_price_krw"].mean()))

    st.divider()

    # ── 목록 테이블 ────────────────────────────────
    if df.empty:
        st.info("등록된 가격 마스터가 없습니다. 수입관리에서 제품을 저장하면 자동으로 등록됩니다.")
        return

    show_df = df.rename(columns={
        "id": "ID",
        "product_name": "상품명",
        "size_name": "사이즈",
        "product_code": "상품코드",
        "supply_price_krw": "공급가",
        "retail_price_krw": "소비자가",
        "store_retail_price_krw": "매장운영가",
        "proposal_retail_price_krw": "소비자제안가",
        "korea_cost_krw": "한국원가",
        "is_active": "활성",
        "ref_batch_id": "기준배치",
        "updated_at": "최종수정",
    }).copy()

    for col in ["공급가", "소비자가", "매장운영가", "소비자제안가", "한국원가"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(
                lambda x: f"₩{x:,.0f}" if pd.notna(x) else ""
            )
    show_df["활성"] = show_df["활성"].apply(lambda x: "✅" if x == 1 else "❌")

    disp_cols = ["ID", "상품코드", "상품명", "사이즈",
                 "공급가", "소비자가", "매장운영가", "소비자제안가",
                 "한국원가", "활성", "기준배치", "최종수정"]
    show_df = show_df[[c for c in disp_cols if c in show_df.columns]]

    st.dataframe(show_df, use_container_width=True, hide_index=True, height=340)
    st.caption(f"{len(show_df):,}건 조회")

    st.divider()

    # ── 수정 / 삭제 ────────────────────────────────
    st.markdown("### ✏️ 수정 / 삭제")

    item_options = {
        f'{int(row["ID"])} | {row["상품명"]} | {row["사이즈"]}': int(row["ID"])
        for _, row in show_df.iterrows()
    }
    selected_label = st.selectbox("수정할 항목 선택", list(item_options.keys()), key="ppm_select")
    selected_id = item_options[selected_label]

    raw_row = df[df["id"] == selected_id].iloc[0].to_dict()

    with st.form("ppm_edit_form"):
        e1, e2, e3, e4 = st.columns(4)
        new_supply = e1.number_input(
            "공급가", min_value=0,
            value=int(round(float(raw_row.get("supply_price_krw") or 0))),
            step=100, format="%d"
        )
        new_retail = e2.number_input(
            "소비자가", min_value=0,
            value=int(round(float(raw_row.get("retail_price_krw") or 0))),
            step=100, format="%d"
        )
        new_store = e3.number_input(
            "매장운영가", min_value=0,
            value=int(round(float(raw_row.get("store_retail_price_krw") or 0))),
            step=100, format="%d"
        )
        new_proposal = e4.number_input(
            "소비자제안가", min_value=0,
            value=int(round(float(raw_row.get("proposal_retail_price_krw") or 0))),
            step=100, format="%d"
        )

        e5, e6 = st.columns([1, 3])
        new_active = e5.selectbox(
            "활성 여부", ["활성", "비활성"],
            index=0 if raw_row.get("is_active", 1) == 1 else 1,
            key="ppm_active_sel"
        )
        new_notes = e6.text_input(
            "비고", value=str(raw_row.get("notes") or ""), key="ppm_notes"
        )

        st.caption(
            f"상품명: **{raw_row['product_name']}** | 사이즈: **{raw_row['size_name']}** "
            f"| 한국원가(참고): {_fmt(raw_row.get('korea_cost_krw'))} "
            f"| 기준배치 ID: {raw_row.get('ref_batch_id', '-')}"
        )

        btn_save, btn_del, _ = st.columns([1, 1, 5])
        save_btn = btn_save.form_submit_button("💾 저장", use_container_width=True, type="primary")
        del_btn  = btn_del.form_submit_button("🗑️ 삭제", use_container_width=True)

        if save_btn:
            update_product_price_mst(
                price_id=selected_id,
                supply_price_krw=float(new_supply),
                retail_price_krw=float(new_retail),
                store_retail_price_krw=float(new_store),
                proposal_retail_price_krw=float(new_proposal),
                is_active=1 if new_active == "활성" else 0,
                notes=new_notes.strip() or None,
            )
            st.success("저장되었습니다.")
            st.rerun()

        if del_btn:
            delete_product_price_mst(selected_id)
            st.success("삭제되었습니다.")
            st.rerun()