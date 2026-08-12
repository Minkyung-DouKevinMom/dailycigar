# modules/master/label_size_mst.py
"""
흡연경고 라벨 사이즈 그룹(label_size_mst) 조회/등록/수정 화면.

상품마스터의 각 제품은 이 그룹(A, B, C ...) 중 하나를 지정해서
전후면(Front & Back)/측면(Side) 라벨 사이즈를 공유한다.
"""

import sqlite3
from contextlib import closing

import pandas as pd
import streamlit as st

import db

DB_PATH = "cigar.db"
TABLE_NAME = "label_size_mst"


# -----------------------------
# DB 공통
# -----------------------------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def load_data():
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} ORDER BY group_code", conn)
    return df


def run_query(sql, params=None):
    params = params or []
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()


def insert_row(data: dict):
    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO {TABLE_NAME} ({', '.join(cols)}) VALUES ({placeholders})"
    run_query(sql, [data[c] for c in cols])


def update_row(row_id, data: dict):
    set_clause = ", ".join([f"{k}=?" for k in data.keys()])
    sql = f"UPDATE {TABLE_NAME} SET {set_clause}, updated_at=CURRENT_TIMESTAMP WHERE id=?"
    run_query(sql, list(data.values()) + [row_id])


def delete_row(row_id):
    run_query(f"DELETE FROM {TABLE_NAME} WHERE id=?", [row_id])


def group_in_use_count(group_code: str) -> int:
    """product_mst에 label_group_code 컬럼이 있을 경우, 해당 그룹을 쓰는 제품 수."""
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cols = {r[1] for r in cur.execute("PRAGMA table_info(product_mst)").fetchall()}
        if "label_group_code" not in cols:
            return 0
        cur.execute(
            "SELECT COUNT(*) FROM product_mst WHERE label_group_code = ?",
            (group_code,),
        )
        return cur.fetchone()[0]


# -----------------------------
# 표시용
# -----------------------------
DISPLAY_COLS = {
    "id": "ID",
    "group_code": "그룹 코드",
    "group_name": "그룹 설명",
    "fb_image_l_cm": "전후면 L(cm)",
    "fb_image_w_cm": "전후면 W(cm)",
    "fb_picture_l_cm": "- 그림 영역(cm)",
    "fb_message_l_cm": "- 문구 영역(cm)",
    "side_image_l_cm": "측면 L(cm)",
    "side_message_l_cm": "측면 문구 영역(cm)",
    "border_mm": "적용 테두리(mm)",
    "margin_mm": "적용 여유마진(mm)",
    "rule1_pct": "Rule1 충족률(%, 라벨 전체/박스 면적)",
    "rule1_1_pct": "Rule1-1 충족률(%, 문구/박스 면적)",
    "rule2_pct": "Rule2 충족률(%)",
    "is_active": "사용여부",
    "notes": "비고",
    "created_at": "등록일시",
    "updated_at": "수정일시",
}


def render_guide():
    with st.expander("흡연경고 라벨 규정 가이드 (참고)", expanded=False):
        st.markdown(
            """
- **Rule 1**: 흡연경고 라벨은 원목 시가박스 **전면·후면** 면적의 **50% 이상**을 덮어야 합니다.
- **Rule 1-1** (정부 규정 3-2 기준, 2026-08-12 정정): ~~Rule 1의 라벨 영역 중 문구가 30% 이상~~ 이 아니라,
  경고 **그림**은 박스 전후면 면적의 **30% 이상**, 경고 **문구**는 박스 전후면 면적의 **20% 이상**을
  각각 **독립적으로** 차지해야 합니다 (그림 30% + 문구 20% = 라벨 50%로 Rule 1과 정확히 맞물립니다).
  실제 A~G 확정 그룹 데이터로 검증해보면 그림 30.0~32.7%, 문구 20.0~21.9%로 이 공식과 정확히 일치합니다.
- **Rule 2**: 흡연경고 라벨은 원목 시가박스 각 **측면** 면적의 **30% 이상**을 덮어야 합니다.

그룹별 라벨 사이즈는 해당 그룹에 속한 박스들 중 가장 불리한(면적이 큰) 박스를 기준으로,
위 규정을 충족하도록 산정되어 있습니다.

여기 표시되는 L/W 값은 **인쇄 이미지 자체의 사이즈**입니다. 실제 부착물은 여기에
**검정 테두리 2mm**가 추가되며, 라벨(테두리 포함) 바깥쪽과 박스 가장자리 사이에
**최소 2mm 이상의 여유**가 확보되도록 계산되었습니다. Rule1/Rule2 충족률(%)은
테두리 포함 실제 부착 면적 기준이며, Rule1-1 충족률(%)은 문구 영역(테두리 미포함)이
박스 전후면 면적에서 차지하는 비율입니다.

**그림 비율(이미지 비율) 참고**
- 정부 규정(경고그림·경고문구 표기 지침 3-2)상 경고 **그림(픽토그램)** 은 가로:세로 = **1 : 0.8** 고정 비율이며,
  가로 x = 0.8 × √(박스 L × W), 세로 y = 0.8 × x 로 산출됩니다.
- 전후면 라벨(W 기준) 내에서 **그림 영역 : 문구 영역 비율은 약 60% : 40%** 로 구성되어 있습니다
  (위 표의 "- 그림 영역(cm)" / "- 문구 영역(cm)" 값이 이 비율로 계산된 값입니다).
- **A~G 그룹은 이미 본사에 전송되어 라벨이 제작된 확정 사이즈이므로, 이 비율/사이즈는 임의로 변경하지 않습니다.**
  신규 제품은 기존 그룹에 변경 없이 맞는 경우에만 배정하고, 조금이라도 안 맞으면 새 그룹을 만듭니다.
            """
        )


def build_form_values(selected_row: dict | None = None):
    selected_row = selected_row or {}
    values = {}

    def f(col, default=0.0):
        v = selected_row.get(col, default)
        try:
            return float(v) if v not in (None, "") else default
        except Exception:
            return default

    col1, col2 = st.columns(2)

    with col1:
        values["group_code"] = st.text_input(
            "그룹 코드", value=str(selected_row.get("group_code", "") or ""), max_chars=10
        )
        values["fb_image_l_cm"] = st.number_input(
            "전후면 라벨 L(cm)", value=f("fb_image_l_cm"), step=0.01, format="%.2f"
        )
        values["fb_picture_l_cm"] = st.number_input(
            "- 그림 영역(cm)", value=f("fb_picture_l_cm"), step=0.01, format="%.2f"
        )
        values["side_image_l_cm"] = st.number_input(
            "측면 라벨 L(cm)", value=f("side_image_l_cm"), step=0.01, format="%.2f"
        )
        values["border_mm"] = st.number_input(
            "적용 테두리(mm)", value=f("border_mm", 2.0), step=0.5, format="%.1f"
        )
        values["rule1_pct"] = st.number_input(
            "Rule1 충족률(%, 참고용/worst-case)", value=f("rule1_pct"), step=0.1, format="%.1f"
        )
        values["rule2_pct"] = st.number_input(
            "Rule2 충족률(%, 참고용/worst-case)", value=f("rule2_pct"), step=0.1, format="%.1f"
        )

    with col2:
        values["group_name"] = st.text_input(
            "그룹 설명", value=str(selected_row.get("group_name", "") or "")
        )
        values["fb_image_w_cm"] = st.number_input(
            "전후면 라벨 W(cm)", value=f("fb_image_w_cm"), step=0.01, format="%.2f"
        )
        values["fb_message_l_cm"] = st.number_input(
            "- 문구 영역(cm)", value=f("fb_message_l_cm"), step=0.01, format="%.2f"
        )
        values["side_message_l_cm"] = st.number_input(
            "측면 문구 영역(cm)", value=f("side_message_l_cm"), step=0.01, format="%.2f"
        )
        values["margin_mm"] = st.number_input(
            "적용 여유마진(mm)", value=f("margin_mm", 2.0), step=0.5, format="%.1f"
        )
        values["rule1_1_pct"] = st.number_input(
            "Rule1-1 충족률(%, 참고용)", value=f("rule1_1_pct"), step=0.1, format="%.1f"
        )
        current_active = selected_row.get("is_active", 1)
        try:
            active_default = bool(int(current_active)) if current_active not in (None, "") else True
        except Exception:
            active_default = True
        values["is_active"] = 1 if st.checkbox("사용", value=active_default) else 0

    values["notes"] = st.text_area(
        "비고", value=str(selected_row.get("notes", "") or ""), height=80
    )

    values["group_code"] = values["group_code"].strip().upper()
    values["group_name"] = values["group_name"].strip()
    values["notes"] = values["notes"].strip()

    return values


def validate_form(data: dict, df: pd.DataFrame, editing_id=None):
    errors = []
    if not data.get("group_code"):
        errors.append("그룹 코드는 필수입니다.")
    else:
        dup = df[df["group_code"] == data["group_code"]]
        if editing_id is not None:
            dup = dup[dup["id"] != editing_id]
        if not dup.empty:
            errors.append(f"그룹 코드 '{data['group_code']}'는 이미 사용 중입니다.")

    if not data.get("fb_image_l_cm") or not data.get("fb_image_w_cm"):
        errors.append("전후면 라벨 L/W는 0보다 커야 합니다.")
    if not data.get("side_image_l_cm"):
        errors.append("측면 라벨 L은 0보다 커야 합니다.")
    return errors


# -----------------------------
# 메인 화면
# -----------------------------
def render():
    db.init_label_size_mst_table()

    st.title("라벨 사이즈 그룹 관리")
    st.caption(
        "흡연경고 라벨(전후면/측면) 사이즈를 그룹 단위로 관리합니다. "
        "상품마스터에서 각 제품에 그룹을 지정해 사용합니다."
    )

    render_guide()

    tab1, tab2 = st.tabs(["조회/수정", "신규 등록"])

    # -------------------------
    # 조회 / 수정
    # -------------------------
    with tab1:
        df = load_data()

        if df.empty:
            st.info("등록된 라벨 그룹이 없습니다.")
        else:
            options = df.to_dict("records")
            option_labels = [
                f"{r['group_code']} - {r.get('group_name') or ''}" for r in options
            ]

            selected_idx = st.selectbox(
                "수정할 그룹 선택",
                options=list(range(len(options))),
                format_func=lambda x: option_labels[x],
            )
            selected_row = options[selected_idx]

            st.markdown("#### 선택된 그룹 수정")
            with st.form("edit_label_size_form", clear_on_submit=False):
                form_data = build_form_values(selected_row)
                submitted = st.form_submit_button("수정 저장", use_container_width=True)

                if submitted:
                    errors = validate_form(form_data, df, editing_id=selected_row["id"])
                    if errors:
                        for e in errors:
                            st.error(e)
                    else:
                        try:
                            update_row(selected_row["id"], form_data)
                            st.success("수정되었습니다.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"수정 중 오류 발생: {e}")

            st.markdown("---")
            st.markdown("#### 삭제")
            in_use = group_in_use_count(selected_row["group_code"])
            if in_use > 0:
                st.warning(
                    f"이 그룹은 상품마스터에서 {in_use}개 제품이 사용 중입니다. "
                    "삭제하면 해당 제품들의 라벨 그룹 지정이 끊어질 수 있습니다."
                )
            if st.button("선택 그룹 삭제", type="secondary", use_container_width=True):
                try:
                    delete_row(selected_row["id"])
                    st.success("삭제되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 중 오류 발생: {e}")

            st.markdown("---")
            st.markdown("#### 전체 그룹 목록")
            view_df = df.rename(columns=DISPLAY_COLS)
            st.dataframe(view_df, use_container_width=True, hide_index=True)

    # -------------------------
    # 신규 등록
    # -------------------------
    with tab2:
        st.markdown("#### 신규 그룹 등록")
        df = load_data()

        with st.form("new_label_size_form", clear_on_submit=True):
            form_data = build_form_values(selected_row=None)
            submitted = st.form_submit_button("신규 저장", use_container_width=True)

            if submitted:
                errors = validate_form(form_data, df, editing_id=None)
                if errors:
                    for e in errors:
                        st.error(e)
                else:
                    try:
                        insert_row(form_data)
                        st.success("등록되었습니다.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"등록 중 오류 발생: {e}")
