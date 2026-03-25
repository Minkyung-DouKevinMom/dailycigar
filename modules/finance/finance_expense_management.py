import os
import sqlite3
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


def init_session_state():
    defaults = {
        "exp_cat_selected_id": None,
        "exp_txn_selected_id": None,
        "exp_cat_form_reset_key": 0,
        "exp_txn_form_reset_key": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def safe_float(v) -> float:
    if v in (None, ""):
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


# =========================================================
# Category
# =========================================================
def load_categories(conn, keyword: str = "", active_filter: str = "전체") -> pd.DataFrame:
    sql = """
        SELECT
            id, expense_name, expense_group, is_active, notes, created_at, updated_at
        FROM expense_category_mst
        WHERE 1=1
    """
    params = []

    if keyword.strip():
        sql += """
            AND (
                COALESCE(expense_name, '') LIKE ?
                OR COALESCE(expense_group, '') LIKE ?
                OR COALESCE(notes, '') LIKE ?
            )
        """
        kw = f"%{keyword.strip()}%"
        params.extend([kw, kw, kw])

    if active_filter == "사용":
        sql += " AND COALESCE(is_active, 1) = 1 "
    elif active_filter == "미사용":
        sql += " AND COALESCE(is_active, 1) = 0 "

    sql += " ORDER BY COALESCE(expense_group, ''), expense_name, id DESC "
    return pd.read_sql_query(sql, conn, params=params)


def load_one_category(conn, row_id: int) -> Optional[dict]:
    df = pd.read_sql_query(
        "SELECT * FROM expense_category_mst WHERE id = ?",
        conn,
        params=[row_id],
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def upsert_category(conn, row_id: Optional[int], payload: dict):
    cur = conn.cursor()
    if row_id is None:
        cur.execute(
            """
            INSERT INTO expense_category_mst (
                expense_name, expense_group, is_active, notes, updated_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payload["expense_name"],
                payload["expense_group"],
                payload["is_active"],
                payload["notes"],
            ),
        )
    else:
        cur.execute(
            """
            UPDATE expense_category_mst
               SET expense_name = ?,
                   expense_group = ?,
                   is_active = ?,
                   notes = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (
                payload["expense_name"],
                payload["expense_group"],
                payload["is_active"],
                payload["notes"],
                row_id,
            ),
        )
    conn.commit()


def delete_category(conn, row_id: int):
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM expense_txn WHERE expense_category_id = ?",
        (row_id,),
    )
    txn_count = cur.fetchone()[0]
    if txn_count > 0:
        raise ValueError(f"해당 지출항목을 사용한 지출내역이 {txn_count}건 있어 삭제할 수 없습니다.")
    cur.execute("DELETE FROM expense_category_mst WHERE id = ?", (row_id,))
    conn.commit()


def reset_category_form():
    st.session_state.exp_cat_selected_id = None
    st.session_state.exp_cat_form_reset_key += 1


def render_category_tab(conn):
    st.markdown("### 지출항목 관리")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        keyword = st.text_input("지출항목 검색", placeholder="항목명, 그룹, 비고", key="cat_keyword")
    with c2:
        active_filter = st.selectbox("사용여부", ["전체", "사용", "미사용"], key="cat_active")
    with c3:
        st.write("")
        st.write("")
        if st.button("지출항목 신규등록", use_container_width=True):
            reset_category_form()
            st.rerun()

    df = load_categories(conn, keyword=keyword, active_filter=active_filter)

    m1, m2, m3 = st.columns(3)
    m1.metric("조회건수", f"{len(df):,}")
    m2.metric("사용", f"{int((df['is_active'].fillna(1) == 1).sum()) if not df.empty else 0:,}")
    m3.metric("미사용", f"{int((df['is_active'].fillna(1) == 0).sum()) if not df.empty else 0:,}")

    left, right = st.columns([1.15, 1])

    with left:
        if df.empty:
            st.info("등록된 지출항목이 없습니다.")
        else:
            display_df = df.copy()
            display_df["사용"] = display_df["is_active"].apply(lambda x: "Y" if int(x or 0) == 1 else "N")
            st.dataframe(
                display_df[["id", "expense_group", "expense_name", "사용", "updated_at"]],
                use_container_width=True,
                height=480,
                hide_index=True,
            )

            options = [
                f"{int(r['id'])} | {r['expense_group'] or '-'} | {r['expense_name']}"
                for _, r in df.iterrows()
            ]
            selected_label = st.selectbox("수정/삭제할 지출항목 선택", [""] + options, key="cat_selector")
            if selected_label and st.button("선택 항목 불러오기", use_container_width=True, key="cat_load_btn"):
                st.session_state.exp_cat_selected_id = int(selected_label.split("|")[0].strip())
                st.rerun()

    selected = None
    if st.session_state.exp_cat_selected_id:
        selected = load_one_category(conn, st.session_state.exp_cat_selected_id)
        if selected is None:
            reset_category_form()
            st.rerun()

    with right:
        st.markdown(f"#### {'지출항목 수정' if selected else '지출항목 신규등록'}")
        with st.form(f"cat_form_{st.session_state.exp_cat_form_reset_key}"):
            expense_group = st.text_input(
                "지출그룹",
                value=selected.get("expense_group", "") if selected else "",
                placeholder="예: 고정비 / 변동비 / 마케팅 / 운영비",
            )
            expense_name = st.text_input(
                "지출항목명 *",
                value=selected.get("expense_name", "") if selected else "",
                placeholder="예: 임대료 / 택배비 / 광고비 / 소모품비",
            )
            is_active = st.selectbox(
                "사용여부",
                options=[1, 0],
                format_func=lambda x: "사용" if x == 1 else "미사용",
                index=0 if not selected else (0 if int(selected.get("is_active", 1)) == 1 else 1),
            )
            notes = st.text_area(
                "비고",
                value=selected.get("notes", "") if selected else "",
                height=120,
            )

            payload = {
                "expense_group": expense_group.strip(),
                "expense_name": expense_name.strip(),
                "is_active": int(is_active),
                "notes": notes.strip(),
            }

            col_a, col_b, col_c = st.columns(3)
            save_clicked = col_a.form_submit_button("저장", use_container_width=True)
            reset_clicked = col_b.form_submit_button("초기화", use_container_width=True)
            delete_clicked = col_c.form_submit_button(
                "삭제",
                use_container_width=True,
                disabled=(selected is None),
            )

        if save_clicked:
            if not payload["expense_name"]:
                st.error("지출항목명은 필수입니다.")
            else:
                try:
                    upsert_category(conn, st.session_state.exp_cat_selected_id, payload)
                    st.success("지출항목이 저장되었습니다.")
                    reset_category_form()
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 중 오류 발생: {e}")

        if reset_clicked:
            reset_category_form()
            st.rerun()

        if delete_clicked and selected is not None:
            try:
                delete_category(conn, st.session_state.exp_cat_selected_id)
                st.success("지출항목이 삭제되었습니다.")
                reset_category_form()
                st.rerun()
            except Exception as e:
                st.error(str(e))


# =========================================================
# Expense Txn
# =========================================================
def load_category_options(conn):
    return pd.read_sql_query(
        """
        SELECT id, expense_group, expense_name
          FROM expense_category_mst
         WHERE COALESCE(is_active, 1) = 1
         ORDER BY COALESCE(expense_group, ''), expense_name
        """,
        conn,
    )


def load_expenses(
    conn,
    date_from: str = "",
    date_to: str = "",
    category_ids=None,
    keyword: str = "",
):
    sql = """
        SELECT
            t.id,
            t.expense_date,
            t.expense_category_id,
            c.expense_group,
            c.expense_name,
            t.amount,
            t.vendor_name,
            t.payment_method,
            t.notes,
            t.created_at,
            t.updated_at
        FROM expense_txn t
        JOIN expense_category_mst c
          ON t.expense_category_id = c.id
        WHERE 1=1
    """
    params = []

    if date_from:
        sql += " AND t.expense_date >= ? "
        params.append(date_from)
    if date_to:
        sql += " AND t.expense_date <= ? "
        params.append(date_to)

    if category_ids:
        placeholders = ",".join(["?"] * len(category_ids))
        sql += f" AND t.expense_category_id IN ({placeholders}) "
        params.extend(category_ids)

    if keyword.strip():
        kw = f"%{keyword.strip()}%"
        sql += """
            AND (
                COALESCE(c.expense_group, '') LIKE ?
                OR COALESCE(c.expense_name, '') LIKE ?
                OR COALESCE(t.vendor_name, '') LIKE ?
                OR COALESCE(t.payment_method, '') LIKE ?
                OR COALESCE(t.notes, '') LIKE ?
            )
        """
        params.extend([kw, kw, kw, kw, kw])

    sql += " ORDER BY t.expense_date DESC, t.id DESC "
    return pd.read_sql_query(sql, conn, params=params)


def load_one_expense(conn, row_id: int) -> Optional[dict]:
    df = pd.read_sql_query(
        "SELECT * FROM expense_txn WHERE id = ?",
        conn,
        params=[row_id],
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def upsert_expense(conn, row_id: Optional[int], payload: dict):
    cur = conn.cursor()
    if row_id is None:
        cur.execute(
            """
            INSERT INTO expense_txn (
                expense_date, expense_category_id, amount, vendor_name,
                payment_method, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payload["expense_date"],
                payload["expense_category_id"],
                payload["amount"],
                payload["vendor_name"],
                payload["payment_method"],
                payload["notes"],
            ),
        )
    else:
        cur.execute(
            """
            UPDATE expense_txn
               SET expense_date = ?,
                   expense_category_id = ?,
                   amount = ?,
                   vendor_name = ?,
                   payment_method = ?,
                   notes = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (
                payload["expense_date"],
                payload["expense_category_id"],
                payload["amount"],
                payload["vendor_name"],
                payload["payment_method"],
                payload["notes"],
                row_id,
            ),
        )
    conn.commit()


def delete_expense(conn, row_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM expense_txn WHERE id = ?", (row_id,))
    conn.commit()


def reset_expense_form():
    st.session_state.exp_txn_selected_id = None
    st.session_state.exp_txn_form_reset_key += 1


def render_expense_tab(conn):
    st.markdown("### 지출내역 관리")

    cat_df = load_category_options(conn)
    cat_map = {
        f"{int(r['id'])} | {r['expense_group'] or '-'} | {r['expense_name']}": int(r["id"])
        for _, r in cat_df.iterrows()
    }

    f1, f2, f3, f4 = st.columns([1, 1, 1.2, 1.2])
    with f1:
        date_from = st.date_input("시작일", value=None, key="exp_from")
    with f2:
        date_to = st.date_input("종료일", value=None, key="exp_to")
    with f3:
        selected_cat_labels = st.multiselect("지출항목", list(cat_map.keys()), key="exp_cats")
    with f4:
        keyword = st.text_input("검색", placeholder="거래처, 지출항목, 결제수단, 비고", key="exp_kw")

    category_ids = [cat_map[x] for x in selected_cat_labels]
    df = load_expenses(
        conn,
        date_from=str(date_from) if date_from else "",
        date_to=str(date_to) if date_to else "",
        category_ids=category_ids,
        keyword=keyword,
    )

    m1, m2, m3 = st.columns(3)
    m1.metric("조회건수", f"{len(df):,}")
    m2.metric("지출합계", f"{df['amount'].sum():,.0f}" if not df.empty else "0")
    m3.metric("평균지출", f"{df['amount'].mean():,.0f}" if not df.empty else "0")

    left, right = st.columns([1.2, 1])

    with left:
        if df.empty:
            st.info("조회된 지출내역이 없습니다.")
        else:
            st.dataframe(
                df[[
                    "id", "expense_date", "expense_group", "expense_name",
                    "amount", "vendor_name", "payment_method", "updated_at"
                ]],
                use_container_width=True,
                height=500,
                hide_index=True,
            )

            options = [
                f"{int(r['id'])} | {r['expense_date']} | {r['expense_name']} | {r['amount']:,.0f}"
                for _, r in df.iterrows()
            ]
            selected_label = st.selectbox("수정/삭제할 지출내역 선택", [""] + options, key="exp_selector")

            col_x, col_y = st.columns(2)
            if col_x.button("선택 내역 불러오기", use_container_width=True):
                if selected_label:
                    st.session_state.exp_txn_selected_id = int(selected_label.split("|")[0].strip())
                    st.rerun()
            if col_y.button("지출내역 신규등록", use_container_width=True):
                reset_expense_form()
                st.rerun()

        if not df.empty:
            st.markdown("#### 집계")
            group_sum = (
                df.groupby(["expense_group", "expense_name"], dropna=False)["amount"]
                .sum()
                .reset_index()
                .sort_values("amount", ascending=False)
            )
            st.dataframe(group_sum, use_container_width=True, height=220, hide_index=True)

    selected = None
    if st.session_state.exp_txn_selected_id:
        selected = load_one_expense(conn, st.session_state.exp_txn_selected_id)
        if selected is None:
            reset_expense_form()
            st.rerun()

    with right:
        st.markdown(f"#### {'지출내역 수정' if selected else '지출내역 신규등록'}")

        if cat_df.empty:
            st.warning("먼저 지출항목을 등록해 주세요.")
            return

        category_option_labels = list(cat_map.keys())
        selected_category_label = None
        if selected and selected.get("expense_category_id"):
            for label, cid in cat_map.items():
                if cid == int(selected["expense_category_id"]):
                    selected_category_label = label
                    break

        default_idx = 0
        if selected_category_label in category_option_labels:
            default_idx = category_option_labels.index(selected_category_label)

        with st.form(f"exp_form_{st.session_state.exp_txn_form_reset_key}"):
            expense_date = st.date_input(
                "지출일자 *",
                value=pd.to_datetime(selected.get("expense_date")).date() if selected and selected.get("expense_date") else pd.Timestamp.today().date(),
            )
            category_label = st.selectbox(
                "지출항목 *",
                options=category_option_labels,
                index=default_idx,
            )
            amount = st.number_input(
                "금액 *",
                min_value=0.0,
                value=float(selected.get("amount", 0) or 0) if selected else 0.0,
                step=1000.0,
            )
            vendor_name = st.text_input(
                "거래처/사용처",
                value=selected.get("vendor_name", "") if selected else "",
                placeholder="예: 네이버, 쿠팡, 택배사, 건물주",
            )
            payment_method = st.text_input(
                "결제수단",
                value=selected.get("payment_method", "") if selected else "",
                placeholder="예: 카드, 계좌이체, 현금",
            )
            notes = st.text_area(
                "비고",
                value=selected.get("notes", "") if selected else "",
                height=120,
            )

            payload = {
                "expense_date": str(expense_date),
                "expense_category_id": cat_map[category_label],
                "amount": safe_float(amount),
                "vendor_name": vendor_name.strip(),
                "payment_method": payment_method.strip(),
                "notes": notes.strip(),
            }

            col_a, col_b, col_c = st.columns(3)
            save_clicked = col_a.form_submit_button("저장", use_container_width=True)
            reset_clicked = col_b.form_submit_button("초기화", use_container_width=True)
            delete_clicked = col_c.form_submit_button(
                "삭제", use_container_width=True, disabled=(selected is None)
            )

        if save_clicked:
            if not payload["expense_date"]:
                st.error("지출일자는 필수입니다.")
            elif not payload["expense_category_id"]:
                st.error("지출항목은 필수입니다.")
            elif payload["amount"] <= 0:
                st.error("금액은 0보다 커야 합니다.")
            else:
                try:
                    upsert_expense(conn, st.session_state.exp_txn_selected_id, payload)
                    st.success("지출내역이 저장되었습니다.")
                    reset_expense_form()
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 중 오류 발생: {e}")

        if reset_clicked:
            reset_expense_form()
            st.rerun()

        if delete_clicked and selected is not None:
            try:
                delete_expense(conn, st.session_state.exp_txn_selected_id)
                st.success("지출내역이 삭제되었습니다.")
                reset_expense_form()
                st.rerun()
            except Exception as e:
                st.error(f"삭제 중 오류 발생: {e}")


# =========================================================
# Summary
# =========================================================
def render_summary_tab(conn):
    st.markdown("### 지출 요약")

    f1, f2 = st.columns(2)
    with f1:
        date_from = st.date_input("요약 시작일", value=None, key="sum_from")
    with f2:
        date_to = st.date_input("요약 종료일", value=None, key="sum_to")

    sql = """
        SELECT
            t.expense_date,
            c.expense_group,
            c.expense_name,
            t.amount
        FROM expense_txn t
        JOIN expense_category_mst c
          ON t.expense_category_id = c.id
        WHERE 1=1
    """
    params = []
    if date_from:
        sql += " AND t.expense_date >= ? "
        params.append(str(date_from))
    if date_to:
        sql += " AND t.expense_date <= ? "
        params.append(str(date_to))

    df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        st.info("조회된 지출내역이 없습니다.")
        return

    k1, k2, k3 = st.columns(3)
    k1.metric("총 지출", f"{df['amount'].sum():,.0f}")
    k2.metric("건수", f"{len(df):,}")
    k3.metric("평균", f"{df['amount'].mean():,.0f}")

    col1, col2 = st.columns(2)

    with col1:
        monthly = (
            df.assign(월=pd.to_datetime(df["expense_date"]).dt.strftime("%Y-%m"))
            .groupby("월", dropna=False)["amount"]
            .sum()
            .reset_index()
            .sort_values("월")
        )
        st.markdown("#### 월별 지출")
        st.dataframe(monthly, use_container_width=True, hide_index=True)

    with col2:
        by_group = (
            df.groupby(["expense_group", "expense_name"], dropna=False)["amount"]
            .sum()
            .reset_index()
            .sort_values("amount", ascending=False)
        )
        st.markdown("#### 항목별 지출")
        st.dataframe(by_group, use_container_width=True, hide_index=True, height=420)


def render():
    init_session_state()
    st.subheader("재무관리")

    conn = get_conn()
    try:
        missing_tables = [
            t for t in ["expense_category_mst", "expense_txn"]
            if not table_exists(conn, t)
        ]
        if missing_tables:
            st.error("필수 테이블이 없습니다: " + ", ".join(missing_tables))
            return

        tab1, tab2, tab3 = st.tabs(["지출항목 관리", "지출내역 관리", "지출 요약"])
        with tab1:
            render_category_tab(conn)
        with tab2:
            render_expense_tab(conn)
        with tab3:
            render_summary_tab(conn)

    finally:
        conn.close()