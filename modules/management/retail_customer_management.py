import os
import re
import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def normalize_phone(value: str) -> str:
    """전화번호에서 공백/특수문자만 정리. 형식 자동 변환은 하지 않음."""
    if not value:
        return ""
    return re.sub(r"\s+", "", str(value)).strip()


def is_duplicate_customer(
    conn: sqlite3.Connection,
    customer_name: str,
    phone: str,
    exclude_id: Optional[int] = None,
) -> bool:
    """이름 + 연락처 조합으로 중복 확인."""
    name_n = (customer_name or "").strip()
    phone_n = normalize_phone(phone)

    if not name_n:
        return False

    if phone_n:
        sql = """
            SELECT 1 FROM retail_customer_mst
             WHERE TRIM(COALESCE(customer_name, '')) = ?
               AND REPLACE(REPLACE(COALESCE(phone, ''), ' ', ''), '-', '')
                 = REPLACE(REPLACE(?, ' ', ''), '-', '')
        """
        params = [name_n, phone_n]
    else:
        sql = """
            SELECT 1 FROM retail_customer_mst
             WHERE TRIM(COALESCE(customer_name, '')) = ?
               AND COALESCE(phone, '') = ''
        """
        params = [name_n]

    if exclude_id is not None:
        sql += " AND id <> ?"
        params.append(exclude_id)

    sql += " LIMIT 1"

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchone() is not None


# ─────────────────────────────────────────────────────────────────
# 신규 등록
# ─────────────────────────────────────────────────────────────────
def render_register_form(conn: sqlite3.Connection):
    st.markdown("### 신규 고객 등록")

    today = pd.Timestamp.today().date()

    with st.form("retail_customer_register_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        customer_name = c1.text_input("고객명 *", value="")
        phone = c2.text_input("연락처", value="", placeholder="010-1234-5678")
        email = c3.text_input("이메일", value="")

        c4, c5, c6 = st.columns(3)
        join_date = c4.date_input("가입일", value=today)
        status = c5.selectbox("상태", ["active", "inactive"], index=0)
        delivery_customer = c6.checkbox(
            "배달주문 고객",
            value=False,
            help="택배/배달로 주문한 적이 있는 고객인 경우 체크",
        )

        address = st.text_input("주소", value="")
        notes = st.text_area("특이사항 / 기호 메모", value="", height=80)

        submitted = st.form_submit_button("등록", type="primary")

    if not submitted:
        return

    if not customer_name.strip():
        st.error("고객명은 필수입니다.")
        return

    phone_clean = normalize_phone(phone)

    if is_duplicate_customer(conn, customer_name, phone_clean):
        st.warning(
            f"동일 고객(이름 + 연락처)이 이미 등록되어 있습니다: "
            f"{customer_name.strip()} / {phone_clean or '(연락처 없음)'}"
        )
        return

    payload = {
        "customer_name": customer_name.strip(),
        "phone": phone_clean,
        "email": email.strip(),
        "address": address.strip(),
        "join_date": str(join_date) if join_date else None,
        "status": status,
        "delivery_customer_yn": "Y" if delivery_customer else "N",
        "notes": notes.strip(),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    columns_sql = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    sql = f"INSERT INTO retail_customer_mst ({columns_sql}) VALUES ({placeholders})"

    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(payload.values()))
        conn.commit()
        new_id = cur.lastrowid
        st.success(f"고객 등록 완료. ID = {new_id}")
    except Exception as e:
        conn.rollback()
        st.error(f"등록 오류: {e}")


# ─────────────────────────────────────────────────────────────────
# 조회 / 수정 / 삭제
# ─────────────────────────────────────────────────────────────────
def render_list_and_edit(conn: sqlite3.Connection):
    st.markdown("### 고객 조회 / 수정 / 삭제")

    f1, f2, f3, f4 = st.columns(4)
    keyword = f1.text_input("검색어 (이름 / 연락처 / 이메일)", value="", key="rc_keyword")
    status_filter = f2.selectbox(
        "상태",
        ["전체", "active", "inactive"],
        index=0,
        key="rc_status_filter",
    )
    delivery_filter = f3.selectbox(
        "배달주문 고객",
        ["전체", "배달 고객만", "비배달 고객만"],
        index=0,
        key="rc_delivery_filter",
    )
    f4.write("")  # 정렬 맞춤
    f4.write("")
    if f4.button("새로고침", key="rc_refresh"):
        st.rerun()

    # 쿼리 구성
    where = []
    params = []

    if keyword.strip():
        kw = f"%{keyword.strip()}%"
        where.append(
            "(COALESCE(customer_name, '') LIKE ? "
            " OR COALESCE(phone, '') LIKE ? "
            " OR COALESCE(email, '') LIKE ?)"
        )
        params.extend([kw, kw, kw])

    if status_filter != "전체":
        where.append("COALESCE(status, 'active') = ?")
        params.append(status_filter)

    if delivery_filter == "배달 고객만":
        where.append("COALESCE(delivery_customer_yn, 'N') = 'Y'")
    elif delivery_filter == "비배달 고객만":
        where.append("COALESCE(delivery_customer_yn, 'N') = 'N'")

    sql = """
        SELECT id, customer_name, phone, email, address, join_date,
               status, delivery_customer_yn, notes, created_at, updated_at
          FROM retail_customer_mst
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT 500"

    try:
        df = pd.read_sql_query(sql, conn, params=params)
    except Exception as e:
        st.error(f"조회 오류: {e}")
        return

    st.caption(f"조회 결과: {len(df):,}건 (최대 500건)")

    if df.empty:
        st.info("조회된 고객이 없습니다.")
        return

    # 보기 좋게 한글 라벨로 변환한 표시용 사본
    display_df = df.rename(
        columns={
            "id": "ID",
            "customer_name": "고객명",
            "phone": "연락처",
            "email": "이메일",
            "address": "주소",
            "join_date": "가입일",
            "status": "상태",
            "delivery_customer_yn": "배달",
            "notes": "메모",
            "created_at": "등록일시",
            "updated_at": "수정일시",
        }
    )
    st.dataframe(
        display_df[["ID", "고객명", "연락처", "이메일", "상태", "배달", "가입일", "메모"]],
        use_container_width=True,
        hide_index=True,
        height=320,
    )

    # ── 선택 / 작업 ─────────────────────────────────────────────
    customer_ids = df["id"].tolist()
    selected_id = st.selectbox(
        "수정 또는 삭제할 고객 선택",
        options=customer_ids,
        format_func=lambda x: (
            f"ID {x}  |  {df.loc[df['id']==x, 'customer_name'].values[0]}"
            f"  |  {df.loc[df['id']==x, 'phone'].values[0] or '-'}"
        ),
        key="rc_select_id",
    )

    if selected_id is None:
        return

    sel = df[df["id"] == selected_id].iloc[0]

    action = st.radio("작업", ["수정", "삭제"], horizontal=True, key="rc_action")

    if action == "삭제":
        _render_delete(conn, int(selected_id), sel)
    else:
        _render_edit_form(conn, int(selected_id), sel)


def _render_delete(conn: sqlite3.Connection, customer_id: int, sel: pd.Series):
    st.warning(
        f"**ID {customer_id} ({sel['customer_name']})** 고객을 삭제합니다. "
        "이 작업은 되돌릴 수 없습니다."
    )

    # 사용 중인지 사전 확인
    using_count = 0
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM retail_sales WHERE retail_customer_id = ?",
            (customer_id,),
        )
        using_count = int(cur.fetchone()[0])
    except Exception:
        # retail_sales에 컬럼이 아직 없는 경우 무시
        pass

    if using_count > 0:
        st.error(
            f"이 고객이 소매 판매내역에 {using_count:,}건 연결되어 있어 삭제할 수 없습니다. "
            "먼저 해당 판매내역의 고객 연결을 해제하거나, '상태'를 inactive로 변경하세요."
        )
        return

    if st.button("삭제 확인", type="primary", key="rc_delete_btn"):
        try:
            conn.execute("DELETE FROM retail_customer_mst WHERE id = ?", (customer_id,))
            conn.commit()
            st.success(f"ID {customer_id} 삭제 완료.")
            st.rerun()
        except Exception as e:
            conn.rollback()
            st.error(f"삭제 오류: {e}")


def _render_edit_form(conn: sqlite3.Connection, customer_id: int, sel: pd.Series):
    def _v(col, default=""):
        v = sel.get(col, default)
        return default if pd.isna(v) else v

    with st.form("rc_edit_form", clear_on_submit=False):
        st.markdown(f"**ID {customer_id} 수정**")

        c1, c2, c3 = st.columns(3)
        ed_customer_name = c1.text_input("고객명 *", value=str(_v("customer_name", "")))
        ed_phone = c2.text_input("연락처", value=str(_v("phone", "")))
        ed_email = c3.text_input("이메일", value=str(_v("email", "")))

        c4, c5, c6 = st.columns(3)
        join_default = _v("join_date", "")
        try:
            join_date_value = (
                pd.to_datetime(join_default).date()
                if join_default
                else pd.Timestamp.today().date()
            )
        except Exception:
            join_date_value = pd.Timestamp.today().date()
        ed_join_date = c4.date_input("가입일", value=join_date_value)

        status_options = ["active", "inactive"]
        status_val = str(_v("status", "active"))
        if status_val not in status_options:
            status_val = "active"
        ed_status = c5.selectbox(
            "상태",
            status_options,
            index=status_options.index(status_val),
        )

        ed_delivery = c6.checkbox(
            "배달주문 고객",
            value=(str(_v("delivery_customer_yn", "N")).upper() == "Y"),
        )

        ed_address = st.text_input("주소", value=str(_v("address", "")))
        ed_notes = st.text_area("특이사항 / 기호 메모", value=str(_v("notes", "")), height=80)

        submitted = st.form_submit_button("수정 저장", type="primary")

    if not submitted:
        return

    if not ed_customer_name.strip():
        st.error("고객명은 필수입니다.")
        return

    phone_clean = normalize_phone(ed_phone)

    if is_duplicate_customer(conn, ed_customer_name, phone_clean, exclude_id=customer_id):
        st.warning(
            f"동일 고객(이름 + 연락처)이 이미 등록되어 있습니다: "
            f"{ed_customer_name.strip()} / {phone_clean or '(연락처 없음)'}"
        )
        return

    update_payload = {
        "customer_name": ed_customer_name.strip(),
        "phone": phone_clean,
        "email": ed_email.strip(),
        "address": ed_address.strip(),
        "join_date": str(ed_join_date) if ed_join_date else None,
        "status": ed_status,
        "delivery_customer_yn": "Y" if ed_delivery else "N",
        "notes": ed_notes.strip(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    set_clause = ", ".join([f"{k} = ?" for k in update_payload.keys()])
    params = list(update_payload.values()) + [customer_id]

    try:
        conn.execute(
            f"UPDATE retail_customer_mst SET {set_clause} WHERE id = ?",
            params,
        )
        conn.commit()
        st.success(f"ID {customer_id} 수정 완료.")
        st.rerun()
    except Exception as e:
        conn.rollback()
        st.error(f"수정 오류: {e}")


# ─────────────────────────────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────────────────────────────
def render():
    st.subheader("소매 고객 관리")

    conn = get_conn()
    try:
        if not table_exists(conn, "retail_customer_mst"):
            st.error(
                "retail_customer_mst 테이블이 존재하지 않습니다. "
                "create_retail_customer_mst.sql 을 먼저 실행해 주세요."
            )
            return

        tab1, tab2 = st.tabs(["고객 조회 / 수정", "신규 등록"])

        with tab1:
            render_list_and_edit(conn)

        with tab2:
            render_register_form(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    render()
