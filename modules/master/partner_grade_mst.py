# modules/master/partner_grade_mst.py

import sqlite3
from contextlib import closing

import pandas as pd
import streamlit as st


DB_PATH = "cigar.db"
TABLE_NAME = "partner_grade_mst"


# -----------------------------
# DB 공통
# -----------------------------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_table_columns():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE_NAME})")
        rows = cur.fetchall()

    # PRAGMA table_info 결과:
    # cid, name, type, notnull, dflt_value, pk
    cols = []
    for r in rows:
        cols.append(
            {
                "cid": r[0],
                "name": r[1],
                "type": (r[2] or "").upper(),
                "notnull": r[3],
                "default": r[4],
                "pk": r[5],
            }
        )
    return cols


def table_exists():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?
            """,
            (TABLE_NAME,),
        )
        return cur.fetchone() is not None


def load_data():
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
    return df


def run_query(sql, params=None):
    params = params or []
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()


# -----------------------------
# 컬럼 유틸
# -----------------------------
def get_col_names():
    return [c["name"] for c in get_table_columns()]


def has_col(col_name: str) -> bool:
    return col_name in get_col_names()


def find_first_existing(candidates):
    for c in candidates:
        if has_col(c):
            return c
    return None


def get_pk_column():
    cols = get_table_columns()
    for c in cols:
        if c["pk"] == 1:
            return c["name"]
    # 일반적으로 id를 우선 사용
    if has_col("id"):
        return "id"
    return None


def get_code_col():
    return find_first_existing(["grade_code", "partner_grade_code", "code"])


def get_name_col():
    return find_first_existing(["grade_name", "partner_grade_name", "name"])


def get_rate_col():
    return find_first_existing(
        ["discount_rate", "margin_rate", "wholesale_rate", "rate"]
    )


def get_sort_col():
    return find_first_existing(["sort_order", "display_order", "order_no", "sort_no"])


def get_use_col():
    return find_first_existing(["is_active", "use_yn", "active_yn", "use_flag"])


def get_remark_col():
    return find_first_existing(["remark", "remarks", "memo", "note", "benefit_desc"])


def get_created_col():
    return find_first_existing(["created_at", "created_dt", "reg_date", "created_date"])


def get_updated_col():
    return find_first_existing(["updated_at", "updated_dt", "mod_date", "updated_date"])


def is_numeric_type(type_str: str) -> bool:
    t = (type_str or "").upper()
    return any(x in t for x in ["INT", "REAL", "NUM", "DEC", "FLOAT", "DOUBLE"])


def map_display_headers(df: pd.DataFrame):
    rename_map = {
        "id": "ID",
        "grade_code": "등급코드",
        "partner_grade_code": "등급코드",
        "code": "코드",
        "grade_name": "등급명",
        "partner_grade_name": "등급명",
        "name": "등급명",
        "discount_rate": "할인율(%)",
        "margin_rate": "마진율(%)",
        "wholesale_rate": "도매율(%)",
        "rate": "비율(%)",
        "sort_order": "정렬순서",
        "display_order": "정렬순서",
        "order_no": "정렬순서",
        "sort_no": "정렬순서",
        "is_active": "사용여부",
        "use_yn": "사용여부",
        "active_yn": "사용여부",
        "use_flag": "사용여부",
        "benefit_desc": "비고",
        "description": "설명",
        "created_at": "등록일시",
        "created_dt": "등록일시",
        "reg_date": "등록일시",
        "created_date": "등록일시",
        "updated_at": "수정일시",
        "updated_dt": "수정일시",
        "mod_date": "수정일시",
        "updated_date": "수정일시",
    }
    return df.rename(columns=rename_map)


# -----------------------------
# CRUD
# -----------------------------
def insert_row(data: dict):
    if not data:
        return

    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"""
        INSERT INTO {TABLE_NAME} ({", ".join(cols)})
        VALUES ({placeholders})
    """
    params = [data[c] for c in cols]
    run_query(sql, params)


def update_row(pk_col: str, pk_val, data: dict):
    if not data:
        return

    set_clause = ", ".join([f"{k}=?" for k in data.keys()])
    sql = f"""
        UPDATE {TABLE_NAME}
        SET {set_clause}
        WHERE {pk_col}=?
    """
    params = list(data.values()) + [pk_val]
    run_query(sql, params)


def delete_row(pk_col: str, pk_val):
    sql = f"DELETE FROM {TABLE_NAME} WHERE {pk_col}=?"
    run_query(sql, [pk_val])


# -----------------------------
# 입력폼 생성
# -----------------------------
def build_form_values(selected_row: dict | None = None):
    cols = get_table_columns()
    values = {}
    selected_row = selected_row or {}

    st.markdown("### 등급 정보 입력")

    code_col = get_code_col()
    name_col = get_name_col()
    rate_col = get_rate_col()
    sort_col = get_sort_col()
    use_col = get_use_col()
    remark_col = get_remark_col()

    rendered_cols = set()

    col1, col2 = st.columns(2)

    # 1) 핵심 컬럼은 명시적으로 먼저 표시
    with col1:
        if code_col:
            values[code_col] = st.text_input(
                "등급코드",
                value=str(selected_row.get(code_col, "") or "")
            )
            rendered_cols.add(code_col)

        if rate_col:
            default_val = selected_row.get(rate_col, 0)
            try:
                num_default = float(default_val) if default_val not in [None, ""] else 0.0
            except Exception:
                num_default = 0.0

            values[rate_col] = st.number_input(
                "할인율(%)",
                value=float(num_default),
                step=0.1,
                format="%.2f",
            )
            rendered_cols.add(rate_col)

        if use_col:
            current = str(selected_row.get(use_col, "Y")).upper()
            checked = current in ["Y", "1", "TRUE", "T"]
            values[use_col] = st.checkbox("사용여부", value=checked)
            rendered_cols.add(use_col)

    with col2:
        if name_col:
            values[name_col] = st.text_input(
                "등급명",
                value=str(selected_row.get(name_col, "") or "")
            )
            rendered_cols.add(name_col)

        if sort_col:
            default_val = selected_row.get(sort_col, 0)
            try:
                num_default = int(float(default_val)) if default_val not in [None, ""] else 0
            except Exception:
                num_default = 0

            values[sort_col] = st.number_input(
                "정렬순서",
                value=int(num_default),
                step=1,
                format="%d",
            )
            rendered_cols.add(sort_col)

    # 혜택 컬럼 명시 처리
    if "benefit_desc" in [c["name"] for c in cols]:
        values["benefit_desc"] = st.text_area(
            "혜택",
            value=str(selected_row.get("benefit_desc", "") or ""),
            height=100,
        )
        rendered_cols.add("benefit_desc")

    # 2) 나머지 컬럼 자동 렌더링
    extra_cols = [c for c in cols if c["name"] not in rendered_cols]

    col3, col4 = st.columns(2)

    for idx, c in enumerate(extra_cols):
        col_name = c["name"]
        col_type = c["type"]
        default_val = selected_row.get(col_name, None)

        # PK 제외
        if c["pk"] == 1:
            continue

        # created/updated 제외
        if col_name in [get_created_col(), get_updated_col()]:
            continue

        target = col3 if idx % 2 == 0 else col4

        with target:
            label_map = {
                remark_col: "비고",
                "benefit_desc": "혜택",
            }
            label = label_map.get(col_name, col_name)

            if col_name == use_col:
                current = str(default_val).upper() if default_val is not None else "Y"
                checked = current in ["Y", "1", "TRUE", "T"]
                values[col_name] = st.checkbox(label, value=checked)

            elif is_numeric_type(col_type):
                if "INT" in col_type:
                    if default_val not in [None, ""]:
                        try:
                            num_default = int(float(default_val))
                        except Exception:
                            num_default = 0
                    else:
                        num_default = 0

                    values[col_name] = st.number_input(
                        label,
                        value=int(num_default),
                        step=1,
                        format="%d",
                    )
                else:
                    if default_val not in [None, ""]:
                        try:
                            num_default = float(default_val)
                        except Exception:
                            num_default = 0.0
                    else:
                        num_default = 0.0

                    values[col_name] = st.number_input(
                        label,
                        value=float(num_default),
                        step=0.1,
                        format="%.2f",
                    )

            else:
                text_default = "" if default_val is None else str(default_val)

                if col_name in [remark_col]:
                    values[col_name] = st.text_area(label, value=text_default, height=100)
                else:
                    values[col_name] = st.text_input(label, value=text_default)

    # checkbox 값 변환
    if use_col and use_col in values:
        col_type = next((c["type"] for c in cols if c["name"] == use_col), "")
        if is_numeric_type(col_type):
            values[use_col] = 1 if values[use_col] else 0
        else:
            values[use_col] = "Y" if values[use_col] else "N"

    clean_values = {}
    for k, v in values.items():
        if isinstance(v, str):
            clean_values[k] = v.strip()
        else:
            clean_values[k] = v

    return clean_values


def validate_form(data: dict):
    errors = []

    code_col = get_code_col()
    name_col = get_name_col()

    if code_col and not str(data.get(code_col, "")).strip():
        errors.append("등급코드는 필수입니다.")

    if name_col and not str(data.get(name_col, "")).strip():
        errors.append("등급명은 필수입니다.")

    return errors


def sort_df_for_view(df: pd.DataFrame):
    sort_candidates = [c for c in [get_sort_col(), get_code_col(), get_name_col(), get_pk_column()] if c and c in df.columns]
    if sort_candidates:
        return df.sort_values(by=sort_candidates, ascending=True)
    return df


# -----------------------------
# 메인 화면
# -----------------------------
def render():
    st.title("파트너 등급 관리")

    if not table_exists():
        st.error(f"테이블이 없습니다: {TABLE_NAME}")
        st.info("먼저 cigar.db 안에 partner_grade_mst 테이블이 생성되어 있어야 합니다.")
        return

    cols = get_table_columns()
    if not cols:
        st.error("테이블 컬럼 정보를 읽을 수 없습니다.")
        return

    pk_col = get_pk_column()
    if not pk_col:
        st.error("기본키(PK) 컬럼을 찾을 수 없습니다. 일반적으로 id 컬럼이 필요합니다.")
        return

    # 상단 설명
    st.caption("파트너 등급(예: Bronze, Silver, Gold, Platinum, Diamond)을 등록/수정하는 화면입니다.")

    tab1, tab2 = st.tabs(["조회/수정", "신규 등록"])

    # -------------------------
    # 조회 / 수정
    # -------------------------
    with tab1:
        df = load_data()

        if df.empty:
            st.info("등록된 파트너 등급이 없습니다.")
        else:
            df = sort_df_for_view(df)

            code_col = get_code_col()
            name_col = get_name_col()

            def make_label(row):
                parts = []
                if code_col and code_col in row and pd.notna(row[code_col]):
                    parts.append(str(row[code_col]))
                if name_col and name_col in row and pd.notna(row[name_col]):
                    parts.append(str(row[name_col]))
                if not parts:
                    parts.append(f"{pk_col}={row[pk_col]}")
                return " / ".join(parts)

            options = df.to_dict("records")
            option_labels = [make_label(r) for r in options]

            selected_idx = st.selectbox(
                "수정할 등급 선택",
                options=list(range(len(options))),
                format_func=lambda x: option_labels[x],
            )

            selected_row = options[selected_idx]

            st.markdown("#### 선택된 등급 수정")
            with st.form("edit_partner_grade_form", clear_on_submit=False):
                form_data = build_form_values(selected_row)
                submitted = st.form_submit_button("수정 저장", use_container_width=True)

                if submitted:
                    errors = validate_form(form_data)
                    if errors:
                        for e in errors:
                            st.error(e)
                    else:
                        try:
                            update_row(pk_col, selected_row[pk_col], form_data)
                            st.success("수정되었습니다.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"수정 중 오류 발생: {e}")

            st.markdown("---")

            st.markdown("#### 삭제")
            delete_label = make_label(selected_row)
            st.warning(f"선택된 등급 삭제: {delete_label}")

            if st.button("선택 등급 삭제", type="secondary", use_container_width=True):
                try:
                    delete_row(pk_col, selected_row[pk_col])
                    st.success("삭제되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(f"삭제 중 오류 발생: {e}")

            st.markdown("---")
            st.markdown("#### 전체 등급 목록")
            view_df = map_display_headers(df.copy())
            st.dataframe(view_df, use_container_width=True, hide_index=True)

    # -------------------------
    # 신규 등록
    # -------------------------
    with tab2:
        st.markdown("#### 신규 등급 등록")

        with st.form("new_partner_grade_form", clear_on_submit=True):
            form_data = build_form_values(selected_row=None)
            submitted = st.form_submit_button("신규 저장", use_container_width=True)

            if submitted:
                errors = validate_form(form_data)
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