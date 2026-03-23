import streamlit as st
import pandas as pd
from db import get_all_tax_rule, update_tax_rule, upsert_tax_rule, delete_tax_rule
def render():
    st.subheader("세금 규칙 관리")
    st.set_page_config(page_title="Tax Rule", layout="wide")
    st.caption("표에서 직접 수정 후 저장할 수 있습니다.")

    df = get_all_tax_rule()

    if df.empty:
        st.info("tax_rule 데이터가 없습니다.")
        df = pd.DataFrame(columns=[
            "id", "rule_name", "effective_from", "effective_to",
            "individual_tax_per_g", "tobacco_tax_per_g", "local_education_rate",
            "health_charge_per_g", "import_vat_rate", "notes",
            "created_at"
        ])

    if "delete_yn" not in df.columns:
        df.insert(0, "delete_yn", False)

    display_columns = [
        "delete_yn", "id", "rule_name", "effective_from", "effective_to",
        "individual_tax_per_g", "tobacco_tax_per_g", "local_education_rate",
        "health_charge_per_g", "import_vat_rate", "notes", "created_at"
    ]

    edit_df = df[display_columns].copy()

    edited_df = st.data_editor(
        edit_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "delete_yn": st.column_config.CheckboxColumn("삭제"),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "rule_name": st.column_config.TextColumn("규칙명", required=True),
            "effective_from": st.column_config.TextColumn("적용시작일", required=True),
            "effective_to": st.column_config.TextColumn("적용종료일"),
            "individual_tax_per_g": st.column_config.NumberColumn("개별소비세/g", format="%.4f"),
            "tobacco_tax_per_g": st.column_config.NumberColumn("담배소비세/g", format="%.4f"),
            "local_education_rate": st.column_config.NumberColumn("지방교육세율", format="%.4f"),
            "health_charge_per_g": st.column_config.NumberColumn("국민건강/g", format="%.4f"),
            "import_vat_rate": st.column_config.NumberColumn("수입부가세율", format="%.4f"),
            "notes": st.column_config.TextColumn("비고"),
            "created_at": st.column_config.TextColumn("생성일", disabled=True),
        },
        disabled=["id", "created_at"],
        key="tax_rule_editor",
    )

    def null_if_blank(v):
        if pd.isna(v):
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    def to_float_or_none(v):
        v = null_if_blank(v)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    if st.button("변경사항 저장", type="primary"):
        try:
            update_count = 0
            insert_count = 0
            delete_count = 0

            for _, row in edited_df.iterrows():
                row_id = row.get("id")
                delete_yn = bool(row.get("delete_yn", False))

                rule_name = null_if_blank(row.get("rule_name"))
                effective_from = null_if_blank(row.get("effective_from"))
                effective_to = null_if_blank(row.get("effective_to"))
                individual_tax_per_g = to_float_or_none(row.get("individual_tax_per_g"))
                tobacco_tax_per_g = to_float_or_none(row.get("tobacco_tax_per_g"))
                local_education_rate = to_float_or_none(row.get("local_education_rate"))
                health_charge_per_g = to_float_or_none(row.get("health_charge_per_g"))
                import_vat_rate = to_float_or_none(row.get("import_vat_rate"))
                notes = null_if_blank(row.get("notes"))

                is_blank_new_row = (
                    pd.isna(row_id)
                    and not rule_name
                    and not effective_from
                    and individual_tax_per_g is None
                    and tobacco_tax_per_g is None
                    and local_education_rate is None
                    and health_charge_per_g is None
                    and import_vat_rate is None
                )
                if is_blank_new_row:
                    continue

                if delete_yn and not pd.isna(row_id):
                    delete_tax_rule(int(row_id))
                    delete_count += 1
                    continue

                if not rule_name or not effective_from:
                    st.warning("규칙명과 적용시작일이 없는 행은 저장되지 않았습니다.")
                    continue

                if not pd.isna(row_id):
                    update_tax_rule(
                        row_id=int(row_id),
                        rule_name=rule_name,
                        effective_from=effective_from,
                        effective_to=effective_to,
                        individual_tax_per_g=individual_tax_per_g,
                        tobacco_tax_per_g=tobacco_tax_per_g,
                        local_education_rate=local_education_rate,
                        health_charge_per_g=health_charge_per_g,
                        import_vat_rate=import_vat_rate,
                        notes=notes,
                    )
                    update_count += 1
                else:
                    upsert_tax_rule(
                        rule_name=rule_name,
                        effective_from=effective_from,
                        effective_to=effective_to,
                        individual_tax_per_g=individual_tax_per_g,
                        tobacco_tax_per_g=tobacco_tax_per_g,
                        local_education_rate=local_education_rate,
                        health_charge_per_g=health_charge_per_g,
                        import_vat_rate=import_vat_rate,
                        notes=notes,
                    )
                    insert_count += 1

            st.success(f"저장 완료: 수정 {update_count}건 / 신규 {insert_count}건 / 삭제 {delete_count}건")
            st.rerun()

        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")