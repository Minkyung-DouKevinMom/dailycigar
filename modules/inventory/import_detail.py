import streamlit as st
import pandas as pd

from db import (
    get_all_import_batch,
    get_import_item_list_filtered,
    get_import_item_detail,
    delete_import_item,
    get_export_price_product_names,
    get_export_price_sizes_by_product,
    get_export_price_package_options,
    get_export_price_item_one,
    get_product_mst_one,
    get_latest_tax_rule_for_import_calc,
    upsert_import_item_full,
)


USD_TO_KRW_DEFAULT = 1500.0


def _n(v, default=0.0):
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _i(v, default=0):
    try:
        if v is None or v == "":
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _calc_values(
    export_price_usd: float,
    discount_rate: float,
    import_unit_qty: int,
    unit_weight_g: float,
    usd_to_krw_rate: float,
    tax_rule: dict,
):
    discounted_box_price_usd = export_price_usd * (1 - (discount_rate / 100.0))
    export_unit_price_usd = discounted_box_price_usd / import_unit_qty if import_unit_qty > 0 else 0.0
    import_unit_cost_krw = export_unit_price_usd * usd_to_krw_rate
    import_total_cost_krw = discounted_box_price_usd * usd_to_krw_rate

    total_weight_g = unit_weight_g * import_unit_qty

    individual_tax_krw = total_weight_g * _n(tax_rule.get("individual_tax_per_g"))
    tobacco_tax_krw = total_weight_g * _n(tax_rule.get("tobacco_tax_per_g"))
    local_education_tax_krw = (individual_tax_krw + import_total_cost_krw) * _n(tax_rule.get("local_education_rate"))
    health_charge_krw = total_weight_g * _n(tax_rule.get("health_charge_per_g"))
    import_vat_krw = import_total_cost_krw * _n(tax_rule.get("import_vat_rate"))

    tax_total_krw = (
        individual_tax_krw
        + tobacco_tax_krw
        + local_education_tax_krw
        + health_charge_krw
        + import_vat_krw
    )

    korea_cost_krw = import_total_cost_krw + tax_total_krw

    return {
        "discounted_box_price_usd": discounted_box_price_usd,
        "export_unit_price_usd": export_unit_price_usd,
        "import_unit_cost_krw": import_unit_cost_krw,
        "import_total_cost_krw": import_total_cost_krw,
        "total_weight_g": total_weight_g,
        "individual_tax_krw": individual_tax_krw,
        "tobacco_tax_krw": tobacco_tax_krw,
        "local_education_tax_krw": local_education_tax_krw,
        "health_charge_krw": health_charge_krw,
        "import_vat_krw": import_vat_krw,
        "tax_total_krw": tax_total_krw,
        "korea_cost_krw": korea_cost_krw,
    }


def render():
    st.set_page_config(page_title="수입제품 관리", layout="wide")
    st.subheader("수입제품 관리")

    batch_df = get_all_import_batch()
    if batch_df.empty:
        st.info("수입 버전 데이터가 없습니다.")
        st.stop()

    batch_options = {"전체": None}
    for _, row in batch_df.iterrows():
        batch_options[f'{row["id"]} | {row["version_name"]}'] = row["id"]

    c1, c2 = st.columns([2, 3])
    with c1:
        selected_label = st.selectbox("수입 버전 선택", list(batch_options.keys()))
        selected_batch_id = batch_options[selected_label]
    with c2:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈 / 코드")

    df = get_import_item_list_filtered(selected_batch_id, keyword)

    st.markdown("### 조회 결과")
    if not df.empty:
        show_df = df.rename(columns={
            "id": "ID",
            "batch_id": "버전ID",
            "product_name": "상품명",
            "size_name": "사이즈",
            "product_code": "상품코드",
            "import_unit_qty": "수입개수",
            "import_total_cost_krw": "총수입금액",
            "total_weight_g": "총무게(g)",
            "retail_price_krw": "소비자가",
            "supply_price_krw": "공급가",
            "margin_krw": "마진",
            "source_row_no": "원본행번호",
        }).copy()

        money_cols = ["총수입금액", "소비자가", "공급가", "마진"]
        for col in money_cols:
            if col in show_df.columns:
                show_df[col] = show_df[col].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "")

        st.dataframe(show_df, use_container_width=True, hide_index=True, height=320)
    else:
        st.info("조회 결과가 없습니다.")

    st.divider()

    tab1, tab2 = st.tabs(["신규 추가/수정", "원본 JSON"])

    with tab1:
        render_editor(df, selected_batch_id)

    with tab2:
        if not df.empty:
            item_options = {
                f'{row["id"]} | {row["product_name"]} | {row["size_name"]}': row["id"]
                for _, row in df.iterrows()
            }
            selected_item_label = st.selectbox("원본 JSON 확인할 항목", list(item_options.keys()))
            selected_item_id = item_options[selected_item_label]
            detail_df = get_import_item_detail(selected_item_id)
            if not detail_df.empty:
                row = detail_df.iloc[0]
                st.code(row.get("raw_row_json") or "", language="json")
                st.code(row.get("raw_formula_json") or "", language="json")


def render_editor(df: pd.DataFrame, selected_batch_id):
    edit_mode = st.radio("작업 선택", ["신규 추가", "기존 수정"], horizontal=True)

    detail_row = {}
    selected_item_id = None

    if edit_mode == "기존 수정":
        if df.empty:
            st.warning("수정할 수입제품이 없습니다.")
            return

        item_options = {
            f'{row["id"]} | {row["product_name"]} | {row["size_name"]} | row:{row["source_row_no"]}': row["id"]
            for _, row in df.iterrows()
        }
        selected_item_label = st.selectbox("수정할 수입제품 선택", list(item_options.keys()))
        selected_item_id = item_options[selected_item_label]
        detail_df = get_import_item_detail(selected_item_id)
        if detail_df.empty:
            st.error("상세 데이터를 찾을 수 없습니다.")
            return
        detail_row = detail_df.iloc[0].to_dict()

    product_names = get_export_price_product_names()
    default_product = detail_row.get("product_name") if detail_row else None
    if default_product not in product_names and product_names:
        default_product = product_names[0]

    with st.form("import_item_form"):
        st.markdown("### 수입제품 입력")

        col1, col2, col3 = st.columns(3)

        with col1:
            product_name = st.selectbox(
                "상품명",
                options=product_names,
                index=product_names.index(default_product) if default_product in product_names else 0,
            )

            size_options = get_export_price_sizes_by_product(product_name)
            default_size = detail_row.get("size_name") if detail_row else None
            if default_size not in size_options and size_options:
                default_size = size_options[0]

            size_name = st.selectbox(
                "사이즈",
                options=size_options,
                index=size_options.index(default_size) if default_size in size_options else 0,
            )

            package_options_df = get_export_price_package_options(product_name, size_name)
            package_labels = []
            package_map = {}
            for _, r in package_options_df.iterrows():
                label = f'{r["package_type"]} / {int(r["package_qty"])}개 / USD {float(r["export_price_usd"]):,.2f}'
                package_labels.append(label)
                package_map[label] = r.to_dict()

            default_pkg_label = package_labels[0] if package_labels else None
            package_label = st.selectbox("수출 포장유형/개수", package_labels, index=0 if package_labels else None)

            pkg = package_map.get(package_label, {}) if package_label else {}
            export_price_usd = _n(pkg.get("export_price_usd"), _n(detail_row.get("export_box_price_usd")))
            package_type = pkg.get("package_type", "")
            package_qty = _i(pkg.get("package_qty"), _i(detail_row.get("import_unit_qty"), 1))

        with col2:
            product_info = get_product_mst_one(product_name, size_name)
            product_code = st.text_input(
                "상품코드",
                value=product_info.get("product_code") or detail_row.get("product_code") or "",
                disabled=True,
            )

            import_unit_qty = st.number_input(
                "총 수입 개수",
                min_value=1,
                value=_i(detail_row.get("import_unit_qty"), package_qty if package_qty > 0 else 1),
                step=1,
            )

            discount_rate = st.number_input(
                "할인율(%)",
                min_value=0.0,
                max_value=100.0,
                value=_n(detail_row.get("discount_rate"), 0.0),
                step=0.1,
            )

            discounted_box_price_usd_manual = st.number_input(
                "최종 수출가격(USD, 박스기준)",
                min_value=0.0,
                value=_n(
                    detail_row.get("discounted_box_price_usd"),
                    export_price_usd * (1 - discount_rate / 100.0)
                ),
                step=0.01,
            )

        with col3:
            usd_to_krw_rate = st.number_input(
                "달러환율",
                min_value=0.0,
                value=USD_TO_KRW_DEFAULT,
                step=1.0,
            )

            unit_weight_g = st.number_input(
                "개당 무게(g)",
                min_value=0.0,
                value=_n(detail_row.get("unit_weight_g"), _n(product_info.get("unit_weight_g"), 0.0)),
                step=0.1,
            )

            retail_price_krw = st.number_input(
                "소비자가",
                min_value=0.0,
                value=_n(detail_row.get("retail_price_krw"), 0.0),
                step=100.0,
            )

            supply_price_krw = st.number_input(
                "공급가",
                min_value=0.0,
                value=_n(detail_row.get("supply_price_krw"), 0.0),
                step=100.0,
            )

        tax_rule = get_latest_tax_rule_for_import_calc()

        # 할인율 기준값
        discounted_box_price_usd_calc = export_price_usd * (1 - discount_rate / 100.0)

        # 최종 수출가격 수동 수정 허용 -> 역산 할인율도 같이 반영
        final_discounted_box_price_usd = discounted_box_price_usd_manual
        if export_price_usd > 0:
            effective_discount_rate = (1 - (final_discounted_box_price_usd / export_price_usd)) * 100
        else:
            effective_discount_rate = 0.0

        calc = _calc_values(
            export_price_usd=export_price_usd,
            discount_rate=effective_discount_rate,
            import_unit_qty=import_unit_qty,
            unit_weight_g=unit_weight_g,
            usd_to_krw_rate=usd_to_krw_rate,
            tax_rule=tax_rule,
        )

        margin_krw = retail_price_krw - supply_price_krw if retail_price_krw or supply_price_krw else 0.0

        st.markdown("### 자동 계산 결과")
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("본사 수출 가격(USD)", f"{export_price_usd:,.2f}")
        r2.metric("최종 수출가격(USD)", f"{calc['discounted_box_price_usd']:,.2f}")
        r3.metric("최종 수입가격(원)", f"₩{calc['import_total_cost_krw']:,.0f}")
        r4.metric("한국원가", f"₩{calc['korea_cost_krw']:,.0f}")

        r5, r6, r7, r8, r9 = st.columns(5)
        r5.metric("총 무게(g)", f"{calc['total_weight_g']:,.1f}")
        r6.metric("개별소비세", f"₩{calc['individual_tax_krw']:,.0f}")
        r7.metric("담배소비세", f"₩{calc['tobacco_tax_krw']:,.0f}")
        r8.metric("지방교육세", f"₩{calc['local_education_tax_krw']:,.0f}")
        r9.metric("국민건강증진금", f"₩{calc['health_charge_krw']:,.0f}")

        r10, r11, r12 = st.columns(3)
        r10.metric("수입부가세", f"₩{calc['import_vat_krw']:,.0f}")
        r11.metric("세금합계", f"₩{calc['tax_total_krw']:,.0f}")
        r12.metric("마진", f"₩{margin_krw:,.0f}")

        source_row_no = st.number_input(
            "원본 행번호",
            min_value=0,
            value=_i(detail_row.get("source_row_no"), 0),
            step=1,
        )

        raw_row_json = detail_row.get("raw_row_json") or ""
        raw_formula_json = detail_row.get("raw_formula_json") or ""

        c1, c2 = st.columns(2)
        save_btn = c1.form_submit_button("저장", use_container_width=True, type="primary")
        delete_btn = c2.form_submit_button("삭제", use_container_width=True)

        if save_btn:
            upsert_import_item_full(
                item_id=selected_item_id,
                batch_id=selected_batch_id,
                product_name=product_name,
                size_name=size_name,
                product_code=product_code or None,
                export_box_price_usd=export_price_usd,
                discounted_box_price_usd=calc["discounted_box_price_usd"],
                discount_rate=effective_discount_rate,
                import_unit_qty=import_unit_qty,
                export_unit_price_usd=calc["export_unit_price_usd"],
                import_unit_cost_krw=calc["import_unit_cost_krw"],
                import_total_cost_krw=calc["import_total_cost_krw"],
                unit_weight_g=unit_weight_g,
                total_weight_g=calc["total_weight_g"],
                individual_tax_krw=calc["individual_tax_krw"],
                tobacco_tax_krw=calc["tobacco_tax_krw"],
                local_education_tax_krw=calc["local_education_tax_krw"],
                health_charge_krw=calc["health_charge_krw"],
                import_vat_krw=calc["import_vat_krw"],
                tax_total_krw=calc["tax_total_krw"],
                tax_total_all_krw=calc["tax_total_krw"],
                korea_cost_krw=calc["korea_cost_krw"],
                retail_price_krw=retail_price_krw if retail_price_krw else None,
                supply_price_krw=supply_price_krw if supply_price_krw else None,
                margin_krw=margin_krw if margin_krw else None,
                source_row_no=source_row_no if source_row_no else None,
                raw_row_json=raw_row_json,
                raw_formula_json=raw_formula_json,
            )
            st.success("저장되었습니다.")
            st.rerun()

        if delete_btn and selected_item_id:
            delete_import_item(selected_item_id)
            st.success("삭제되었습니다.")
            st.rerun()