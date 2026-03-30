import os
import sqlite3
from typing import Optional

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")
TABLE_NAME = "non_cigar_product_mst"


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return cur.fetchall()


def init_session_state():
    if "ncp_selected_id" not in st.session_state:
        st.session_state.ncp_selected_id = None
    if "ncp_form_reset_key" not in st.session_state:
        st.session_state.ncp_form_reset_key = 0


def load_data(conn: sqlite3.Connection, keyword: str = "", active_filter: str = "전체") -> pd.DataFrame:
    sql = f"""
        SELECT
            id,
            product_code,
            product_name,
            product_category,
            brand_name,
            unit_type,
            spec,
            purchase_price,
            wholesale_price,
            retail_price,
            COALESCE(store_retail_price, 0) AS store_retail_price,
            is_active,
            notes,
            created_at,
            updated_at
        FROM {TABLE_NAME}
        WHERE 1=1
    """
    params = []

    if keyword.strip():
        sql += """
            AND (
                COALESCE(product_code, '') LIKE ?
                OR COALESCE(product_name, '') LIKE ?
                OR COALESCE(product_category, '') LIKE ?
                OR COALESCE(brand_name, '') LIKE ?
                OR COALESCE(spec, '') LIKE ?
            )
        """
        kw = f"%{keyword.strip()}%"
        params.extend([kw, kw, kw, kw, kw])

    if active_filter == "사용":
        sql += " AND COALESCE(is_active, 1) = 1 "
    elif active_filter == "미사용":
        sql += " AND COALESCE(is_active, 1) = 0 "

    sql += " ORDER BY product_category, product_name, id DESC "
    return pd.read_sql_query(sql, conn, params=params)


def load_one(conn: sqlite3.Connection, row_id: int) -> Optional[dict]:
    df = pd.read_sql_query(
        f"SELECT * FROM {TABLE_NAME} WHERE id = ?",
        conn,
        params=[row_id],
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def safe_int(v) -> int:
    if v in (None, ""):
        return 0
    try:
        return int(float(v))
    except Exception:
        return 0


def format_krw(v) -> str:
    try:
        return f"{int(float(v or 0)):,}"
    except Exception:
        return "0"


def reset_form():
    st.session_state.ncp_selected_id = None
    st.session_state.ncp_form_reset_key += 1


def delete_row(conn: sqlite3.Connection, row_id: int):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {TABLE_NAME} WHERE id = ?", (row_id,))
    conn.commit()


def upsert_row(conn: sqlite3.Connection, row_id: Optional[int], payload: dict):
    cur = conn.cursor()

    if row_id is None:
        sql = f"""
            INSERT INTO {TABLE_NAME} (
                product_code,
                product_name,
                product_category,
                brand_name,
                unit_type,
                spec,
                purchase_price,
                wholesale_price,
                retail_price,
                store_retail_price,
                is_active,
                notes,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cur.execute(
            sql,
            (
                payload["product_code"],
                payload["product_name"],
                payload["product_category"],
                payload["brand_name"],
                payload["unit_type"],
                payload["spec"],
                payload["purchase_price"],
                payload["wholesale_price"],
                payload["retail_price"],
                payload["store_retail_price"],
                payload["is_active"],
                payload["notes"],
            ),
        )
    else:
        sql = f"""
            UPDATE {TABLE_NAME}
            SET
                product_code = ?,
                product_name = ?,
                product_category = ?,
                brand_name = ?,
                unit_type = ?,
                spec = ?,
                purchase_price = ?,
                wholesale_price = ?,
                retail_price = ?,
                store_retail_price = ?,
                is_active = ?,
                notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        cur.execute(
            sql,
            (
                payload["product_code"],
                payload["product_name"],
                payload["product_category"],
                payload["brand_name"],
                payload["unit_type"],
                payload["spec"],
                payload["purchase_price"],
                payload["wholesale_price"],
                payload["retail_price"],
                payload["store_retail_price"],
                payload["is_active"],
                payload["notes"],
                row_id,
            ),
        )

    conn.commit()


def render():
    init_session_state()
    st.subheader("시가 외 상품 마스터 관리")

    conn = get_conn()
    try:
        if not table_exists(conn, TABLE_NAME):
            st.error(f"{TABLE_NAME} 테이블이 없습니다.")
            return

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            keyword = st.text_input("검색", placeholder="상품코드, 상품명, 카테고리, 브랜드, 규격")
        with c2:
            active_filter = st.selectbox("사용여부", ["전체", "사용", "미사용"], index=0)
        with c3:
            st.write("")
            st.write("")
            if st.button("신규 등록", use_container_width=True):
                reset_form()
                st.rerun()

        df = load_data(conn, keyword=keyword, active_filter=active_filter)

        m1, m2, m3 = st.columns(3)
        m1.metric("조회건수", f"{len(df):,}")
        m2.metric("사용", f"{int((df['is_active'].fillna(1) == 1).sum()):,}" if not df.empty else "0")
        m3.metric("미사용", f"{int((df['is_active'].fillna(1) == 0).sum()):,}" if not df.empty else "0")

        left, right = st.columns([1.2, 1])

        with left:
            st.markdown("#### 상품 목록")

            if df.empty:
                st.info("등록된 논시가 상품이 없습니다.")
            else:
                display_df = df.copy()
                display_df["사용"] = display_df["is_active"].apply(lambda x: "Y" if int(x or 0) == 1 else "N")

                # 금액 컬럼 정수/콤마 처리
                for col in ["purchase_price", "wholesale_price", "retail_price", "store_retail_price"]:
                    display_df[col] = display_df[col].apply(format_krw)

                # 한글 헤더명 변경
                display_df = display_df.rename(
                    columns={
                        "id": "ID",
                        "product_code": "상품코드",
                        "product_name": "상품명",
                        "product_category": "카테고리",
                        "brand_name": "브랜드명",
                        "unit_type": "단위",
                        "purchase_price": "매입가(₩)",
                        "wholesale_price": "도매가(₩)",
                        "retail_price": "소매가(₩)",
                        "store_retail_price": "매장운영가(₩)",
                        "updated_at": "수정일시",
                    }
                )

                display_cols = [
                    "ID",
                    "상품코드",
                    "상품명",
                    "카테고리",
                    "브랜드명",
                    "단위",
                    "매입가(₩)",
                    "도매가(₩)",
                    "소매가(₩)",
                    "매장운영가(₩)",
                    "사용",
                    "수정일시",
                ]
                
                styled_df = display_df[display_cols].style.set_properties(
                    subset=["매입가(₩)", "도매가(₩)", "소매가(₩)", "매장운영가(₩)"],
                    **{"text-align": "right"}
                )
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=520,
                    hide_index=True,
                )

                row_options = [
                    f"{int(r['id'])} | {r['product_code'] or '-'} | {r['product_name']}"
                    for _, r in df.iterrows()
                ]

                selected_label = st.selectbox(
                    "수정/삭제할 상품 선택",
                    options=[""] + row_options,
                    index=0,
                )

                if selected_label:
                    selected_id = int(selected_label.split("|")[0].strip())
                    if st.button("선택 상품 불러오기", use_container_width=True):
                        st.session_state.ncp_selected_id = selected_id
                        st.rerun()

        selected = None
        if st.session_state.ncp_selected_id:
            selected = load_one(conn, st.session_state.ncp_selected_id)
            if selected is None:
                st.warning("선택한 상품을 찾을 수 없습니다.")
                reset_form()
                st.rerun()

        form_key = f"non_cigar_form_{st.session_state.ncp_form_reset_key}"

        with right:
            mode_text = "상품 수정" if selected else "신규 상품 등록"
            st.markdown(f"#### {mode_text}")

            with st.form(form_key, clear_on_submit=False):
                product_code = st.text_input(
                    "상품코드 *",
                    value=selected.get("product_code", "") if selected else "",
                )
                product_name = st.text_input(
                    "상품명 *",
                    value=selected.get("product_name", "") if selected else "",
                )
                product_category = st.text_input(
                    "상품카테고리 *",
                    value=selected.get("product_category", "") if selected else "",
                    placeholder="예: 악세사리 / 기프트패키지 / 사이드 / 금액결제",
                )
                brand_name = st.text_input(
                    "브랜드명",
                    value=selected.get("brand_name", "") if selected else "",
                )

                col_a, col_b = st.columns(2)
                with col_a:
                    unit_type = st.selectbox(
                        "단위",
                        options=["EA", "SET", "BOX", "PACK", "기타"],
                        index=(
                            ["EA", "SET", "BOX", "PACK", "기타"].index(selected.get("unit_type", "EA"))
                            if selected and selected.get("unit_type", "EA") in ["EA", "SET", "BOX", "PACK", "기타"]
                            else 0
                        ),
                    )
                with col_b:
                    is_active = st.selectbox(
                        "사용여부",
                        options=[1, 0],
                        format_func=lambda x: "사용" if x == 1 else "미사용",
                        index=0 if not selected else (0 if int(selected.get("is_active", 1)) == 1 else 1),
                    )

                spec = st.text_input(
                    "규격/사양",
                    value=selected.get("spec", "") if selected else "",
                    placeholder="예: 블랙 / 3구 / 메탈 / 22x15x8cm",
                )

                p1, p2, p3, p4 = st.columns(4)

                with p1:
                    purchase_price = st.number_input(
                        "₩ 매입가",
                        min_value=0,
                        value=int(selected.get("purchase_price", 0) or 0) if selected else 0,
                        step=100,
                        format="%d",
                    )

                with p2:
                    wholesale_price = st.number_input(
                        "₩ 도매가",
                        min_value=0,
                        value=int(selected.get("wholesale_price", 0) or 0) if selected else 0,
                        step=100,
                        format="%d",
                    )

                with p3:
                    retail_price = st.number_input(
                        "₩ 소매가",
                        min_value=0,
                        value=int(selected.get("retail_price", 0) or 0) if selected else 0,
                        step=100,
                        format="%d",
                    )

                with p4:
                    store_retail_price = st.number_input(
                        "₩ 매장운영가",
                        min_value=0,
                        value=int(selected.get("store_retail_price", 0) or 0) if selected else 0,
                        step=100,
                        format="%d",
                    )

                notes = st.text_area(
                    "비고",
                    value=selected.get("notes", "") if selected else "",
                    height=120,
                )

                payload = {
                    "product_code": product_code.strip(),
                    "product_name": product_name.strip(),
                    "product_category": product_category.strip(),
                    "brand_name": brand_name.strip(),
                    "unit_type": unit_type.strip(),
                    "spec": spec.strip(),
                    "purchase_price": safe_int(purchase_price),
                    "wholesale_price": safe_int(wholesale_price),
                    "retail_price": safe_int(retail_price),
                    "store_retail_price": safe_int(store_retail_price),
                    "is_active": int(is_active),
                    "notes": notes.strip(),
                }

                save_col, reset_col, delete_col = st.columns(3)
                save_clicked = save_col.form_submit_button("저장", use_container_width=True)
                reset_clicked = reset_col.form_submit_button("초기화", use_container_width=True)
                delete_clicked = delete_col.form_submit_button(
                    "삭제",
                    use_container_width=True,
                    disabled=(selected is None),
                )

                if save_clicked:
                    if not payload["product_code"]:
                        st.error("상품코드는 필수입니다.")
                    elif not payload["product_name"]:
                        st.error("상품명은 필수입니다.")
                    elif not payload["product_category"]:
                        st.error("상품카테고리는 필수입니다.")
                    else:
                        try:
                            upsert_row(conn, st.session_state.ncp_selected_id, payload)
                            st.success("저장되었습니다.")
                            reset_form()
                            st.rerun()
                        except sqlite3.IntegrityError as e:
                            st.error(f"저장 중 제약조건 오류: {e}")
                        except Exception as e:
                            st.error(f"저장 중 오류 발생: {e}")

                if reset_clicked:
                    reset_form()
                    st.rerun()

                if delete_clicked and selected is not None:
                    try:
                        delete_row(conn, st.session_state.ncp_selected_id)
                        st.success("삭제되었습니다.")
                        reset_form()
                        st.rerun()
                    except Exception as e:
                        st.error(f"삭제 중 오류 발생: {e}")

    finally:
        conn.close()