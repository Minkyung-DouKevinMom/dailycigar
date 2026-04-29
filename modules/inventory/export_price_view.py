import sqlite3
import streamlit as st
from db import get_export_price_item_filtered, get_conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _get_table_columns(conn: sqlite3.Connection, table_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}


def _ensure_note_column():
    """export_price_item 테이블에 note 컬럼이 없으면 자동으로 추가한다."""
    conn = get_conn()
    try:
        if not _table_exists(conn, "export_price_item"):
            return
        cols = _get_table_columns(conn, "export_price_item")
        if "note" not in cols:
            conn.execute("ALTER TABLE export_price_item ADD COLUMN note TEXT")
            conn.commit()
    finally:
        conn.close()


def _save_item(row_id: int, price_usd: float, note_value: str):
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE export_price_item SET export_price_usd = ?, note = ? WHERE id = ?",
            (price_usd, note_value.strip(), row_id),
        )
        conn.commit()
    finally:
        conn.close()


def render():
    _ensure_note_column()

    st.subheader("본사 수출 배포가격")

    c1, c2, c3 = st.columns(3)

    with c1:
        keyword = st.text_input("검색", placeholder="상품명 / 사이즈")

    with c2:
        package_type = st.selectbox(
            "포장 유형",
            options=["전체", "PACK", "BOX"],
            index=0,
        )
        package_type = None if package_type == "전체" else package_type

    with c3:
        qty_options = ["전체", 3, 5, 10, 20, 25, 50]
        package_qty = st.selectbox(
            "포장 수량",
            options=qty_options,
            index=0,
        )
        package_qty = None if package_qty == "전체" else package_qty

    df = get_export_price_item_filtered(
        keyword=keyword,
        package_type=package_type,
        package_qty=package_qty,
    )

    # note 컬럼이 DB에 없는 구버전 대비 보정
    if "note" not in df.columns:
        df["note"] = ""
    else:
        df["note"] = df["note"].fillna("")

    st.subheader("조회 결과")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "id": "ID",
            "product_name": "상품명",
            "size_name": "사이즈",
            "package_type": "포장유형",
            "package_qty": "포장수량",
            "export_price_usd": st.column_config.NumberColumn(
                "수출가격(USD)",
                format="%.2f",
            ),
            "note": st.column_config.TextColumn("비고(Note)"),
            "created_at": "생성일",
        },
    )

    if not df.empty:
        st.subheader("요약")
        s1, s2, s3 = st.columns(3)

        with s1:
            st.metric("건수", len(df))

        with s2:
            avg_val = round(df["export_price_usd"].dropna().mean(), 2) if df["export_price_usd"].notna().any() else 0
            st.metric("평균 USD", f"{avg_val:.2f}")

        with s3:
            max_val = round(df["export_price_usd"].dropna().max(), 2) if df["export_price_usd"].notna().any() else 0
            st.metric("최대 USD", f"{max_val:.2f}")

        # ── 가격 / 비고 수정 ───────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("수출가격 / 비고 수정")

        id_options = df["id"].tolist()
        selected_id = st.selectbox(
            "수정할 항목 선택",
            options=id_options,
            format_func=lambda x: (
                f"ID {x}  |  "
                f"{df.loc[df['id'] == x, 'product_name'].values[0]}  |  "
                f"{df.loc[df['id'] == x, 'size_name'].values[0]}  |  "
                f"${df.loc[df['id'] == x, 'export_price_usd'].values[0]:.2f}"
            ),
            key="edit_select_id",
        )

        sel = df.loc[df["id"] == selected_id].iloc[0]

        with st.form("edit_item_form", clear_on_submit=False):
            e1, e2 = st.columns(2)

            new_price = e1.number_input(
                "수출가격 (USD)",
                value=float(sel["export_price_usd"]) if sel["export_price_usd"] else 0.0,
                min_value=0.0,
                step=0.01,
                format="%.2f",
                key="edit_price",
            )

            new_note = e2.text_input(
                "비고 (Note)",
                value=str(sel["note"]),
                placeholder="예) 2025년 4월 인상 적용, 전월 대비 +0.50 USD",
                key="edit_note",
            )

            submitted = st.form_submit_button("저장", type="primary")

        if submitted:
            _save_item(int(selected_id), new_price, new_note)
            st.success(
                f"ID {selected_id} 저장 완료 — "
                f"수출가격: ${new_price:.2f} / 비고: {new_note or '(없음)'}"
            )
            st.rerun()

    else:
        st.info("조회 결과가 없습니다.")