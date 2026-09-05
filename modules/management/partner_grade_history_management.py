from typing import Optional

import pandas as pd
import streamlit as st

import modules.management.wholesale_management as wm

from modules.common.dbutil import get_conn, table_exists


def init_session_state():
    defaults = {
        "pgh_selected_partner_id": None,
        "pgh_selected_history_id": None,
        "pgh_reset_key": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def add_12_months_minus_1day(start_date):
    dt = pd.Timestamp(start_date)
    end_dt = dt + pd.DateOffset(months=12) - pd.Timedelta(days=1)
    return end_dt.date()


def get_partners(conn, keyword: str = "") -> pd.DataFrame:
    sql = """
        SELECT id, partner_name
        FROM partner_mst
        WHERE 1=1
    """
    params = []
    if keyword.strip():
        sql += " AND COALESCE(partner_name, '') LIKE ? "
        params.append(f"%{keyword.strip()}%")
    sql += " ORDER BY partner_name"
    return pd.read_sql_query(sql, conn, params=params)


def get_grade_mst(conn) -> pd.DataFrame:
    if not table_exists(conn, "partner_grade_mst"):
        return pd.DataFrame(columns=["grade_code", "grade_name"])
    cols = pd.read_sql_query("PRAGMA table_info(partner_grade_mst)", conn)["name"].tolist()
    order_by = "sort_order, grade_code" if "sort_order" in cols else "grade_code"
    df = pd.read_sql_query(
        f"SELECT * FROM partner_grade_mst ORDER BY {order_by}",
        conn,
    )
    return df


def get_partner_current_grade(conn, partner_id: int) -> pd.DataFrame:
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    sql = """
        SELECT h.*, m.grade_name
        FROM partner_grade_history h
        LEFT JOIN partner_grade_mst m
          ON h.grade_code = m.grade_code
        WHERE h.partner_id = ?
          AND h.start_date <= ?
          AND (h.end_date IS NULL OR h.end_date >= ?)
        ORDER BY h.start_date DESC, h.id DESC
    """
    return pd.read_sql_query(sql, conn, params=[partner_id, today, today])


def get_partner_grade_history(conn, partner_id: int) -> pd.DataFrame:
    sql = """
        SELECT
            h.id,
            h.partner_id,
            h.grade_code,
            m.grade_name,
            h.start_date,
            h.end_date,
            h.reason,
            h.created_at,
            h.updated_at
        FROM partner_grade_history h
        LEFT JOIN partner_grade_mst m
          ON h.grade_code = m.grade_code
        WHERE h.partner_id = ?
        ORDER BY h.start_date DESC, h.id DESC
    """
    return pd.read_sql_query(sql, conn, params=[partner_id])


def get_one_history(conn, row_id: int) -> Optional[dict]:
    df = pd.read_sql_query(
        "SELECT * FROM partner_grade_history WHERE id = ?",
        conn,
        params=[row_id],
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_grade_label_map(grade_df: pd.DataFrame):
    labels = []
    mapping = {}
    if grade_df.empty:
        return labels, mapping

    name_col = "grade_name" if "grade_name" in grade_df.columns else None
    for _, r in grade_df.iterrows():
        code = str(r["grade_code"])
        name = str(r[name_col]) if name_col and pd.notna(r[name_col]) else code
        label = f"{code} | {name}"
        labels.append(label)
        mapping[label] = code
    return labels, mapping


def reset_form():
    st.session_state.pgh_selected_history_id = None
    st.session_state.pgh_reset_key += 1


def validate_overlap(conn, partner_id: int, start_date: str, end_date: str, exclude_id: Optional[int] = None) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM partner_grade_history
        WHERE partner_id = ?
          AND NOT (COALESCE(end_date, '9999-12-31') < ? OR start_date > ?)
    """
    params = [partner_id, start_date, end_date]
    if exclude_id is not None:
        sql += " AND id <> ?"
        params.append(exclude_id)

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchone()[0] > 0


def apply_grade_upgrade(conn, partner_id: int, grade_code: str, achieved_date: str, reason: str):
    start_date = pd.Timestamp(achieved_date).strftime("%Y-%m-%d")
    end_date = add_12_months_minus_1day(start_date).strftime("%Y-%m-%d")
    prev_day = (pd.Timestamp(start_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    cur = conn.cursor()

    # 기존 활성 이력 종료
    cur.execute(
        """
        UPDATE partner_grade_history
           SET end_date = ?,
               updated_at = CURRENT_TIMESTAMP
         WHERE partner_id = ?
           AND start_date <= ?
           AND (end_date IS NULL OR end_date >= ?)
        """,
        (prev_day, partner_id, start_date, start_date),
    )

    # 새 등급 시작
    cur.execute(
        """
        INSERT INTO partner_grade_history (
            partner_id, grade_code, start_date, end_date, reason, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (partner_id, grade_code, start_date, end_date, reason),
    )
    conn.commit()


def upsert_history_manual(conn, row_id: Optional[int], payload: dict):
    if validate_overlap(
        conn=conn,
        partner_id=payload["partner_id"],
        start_date=payload["start_date"],
        end_date=payload["end_date"],
        exclude_id=row_id,
    ):
        raise ValueError("같은 거래처의 등급 기간이 겹칩니다.")

    cur = conn.cursor()
    if row_id is None:
        cur.execute(
            """
            INSERT INTO partner_grade_history (
                partner_id, grade_code, start_date, end_date, reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                payload["partner_id"],
                payload["grade_code"],
                payload["start_date"],
                payload["end_date"],
                payload["reason"],
            ),
        )
    else:
        cur.execute(
            """
            UPDATE partner_grade_history
               SET grade_code = ?,
                   start_date = ?,
                   end_date = ?,
                   reason = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (
                payload["grade_code"],
                payload["start_date"],
                payload["end_date"],
                payload["reason"],
                row_id,
            ),
        )
    conn.commit()


def delete_history(conn, row_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM partner_grade_history WHERE id = ?", (row_id,))
    conn.commit()


def render():
    init_session_state()
    st.subheader("거래처 등급관리")

    conn = get_conn()
    try:
        required = ["partner_mst", "partner_grade_history", "partner_grade_mst"]
        missing = [t for t in required if not table_exists(conn, t)]
        if missing:
            st.error("필수 테이블이 없습니다: " + ", ".join(missing))
            return

        partner_search = st.text_input("거래처 검색", placeholder="거래처명", key="pgh_partner_search")
        partners = get_partners(conn, partner_search)
        if partners.empty:
            st.info("조회된 거래처가 없습니다.")
            return

        partner_options = {
            f"{int(r['id'])} | {r['partner_name']}": int(r["id"])
            for _, r in partners.iterrows()
        }

        selected_partner_label = st.selectbox(
            "거래처 선택",
            options=list(partner_options.keys()),
            index=0,
        )
        partner_id = partner_options[selected_partner_label]
        st.session_state.pgh_selected_partner_id = partner_id

        current_df = get_partner_current_grade(conn, partner_id)
        history_df = get_partner_grade_history(conn, partner_id)
        grade_df = get_grade_mst(conn)
        grade_labels, grade_map = get_grade_label_map(grade_df)

        top1, top2, top3 = st.columns(3)
        if current_df.empty:
            top1.metric("현재 등급", "-")
            top2.metric("시작일", "-")
            top3.metric("종료일", "-")
        else:
            cur_row = current_df.iloc[0]
            current_grade_name = cur_row["grade_name"] if "grade_name" in cur_row and pd.notna(cur_row["grade_name"]) else cur_row["grade_code"]
            top1.metric("현재 등급", str(current_grade_name))
            top2.metric("시작일", str(cur_row["start_date"]))
            top3.metric("종료일", str(cur_row["end_date"]))

        st.divider()
        st.markdown("#### 등급업 이후 누적 구매액 (실시간 계산)")
        st.caption(
            "실제 도매 가격 계산(등급 할인)에 쓰이는 것과 동일한 로직으로, "
            "현재 등급을 달성한 시점부터 오늘까지의 누적 공급가 합계를 계산합니다. "
            "위의 '시작일'(수동 등록 이력)과 하루 이틀 정도 차이가 날 수 있습니다."
        )

        partner_row_df = pd.read_sql_query(
            "SELECT join_date FROM partner_mst WHERE id = ?", conn, params=[partner_id]
        )
        today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
        join_date = (
            str(partner_row_df.iloc[0]["join_date"])
            if not partner_row_df.empty and pd.notna(partner_row_df.iloc[0]["join_date"])
            else today_str
        )

        thresholds = wm.load_partner_grade_thresholds(conn)
        if thresholds.empty:
            st.info("등급 기준 정보(partner_grade_mst)가 없어 계산할 수 없습니다.")
        else:
            progress = wm.compute_grade_reset_cumulative(conn, partner_id, today_str, join_date, thresholds)

            tier_rows = thresholds.sort_values("min_purchase_amount").reset_index(drop=True)
            grade_codes_list = tier_rows["grade_code"].tolist()
            grade_thresholds_list = tier_rows["min_purchase_amount"].tolist()
            cur_idx = (
                grade_codes_list.index(progress["grade_code"])
                if progress["grade_code"] in grade_codes_list
                else -1
            )

            g1, g2, g3 = st.columns(3)
            g1.metric("등급 시작일(실시간 계산)", str(progress["grade_start_date"]))
            g2.metric("시작일 이후 누적 구매액(공급가)", f"{progress['cumulative_since_start']:,.0f}원")

            if cur_idx >= 0 and cur_idx + 1 < len(grade_codes_list):
                next_grade = grade_codes_list[cur_idx + 1]
                next_threshold = grade_thresholds_list[cur_idx + 1]
                remaining = max(next_threshold - progress["cumulative_since_start"], 0)
                g3.metric(f"{next_grade} 등급까지 남은 금액", f"{remaining:,.0f}원")
                ratio = (
                    min(progress["cumulative_since_start"] / next_threshold, 1.0)
                    if next_threshold
                    else 0.0
                )
                st.progress(
                    ratio,
                    text=f"{progress['cumulative_since_start']:,.0f}원 / {next_threshold:,.0f}원 ({ratio * 100:.1f}%)",
                )
            else:
                g3.metric("다음 등급", "최고 등급 달성" if cur_idx >= 0 else "-")

        tab1, tab2 = st.tabs(["승급 처리", "이력 관리"])

        with tab1:
            st.markdown("#### 등급 승급 처리")
            st.caption("달성일자를 기준으로 즉시 새 등급이 시작되고, 12개월 유지됩니다. 기존 활성 등급은 전일자로 자동 종료됩니다.")

            if grade_labels:
                default_grade_idx = 0
            else:
                st.warning("partner_grade_mst에 등급이 없습니다.")
                return

            achieved_key = f"pgh_achieved_date_{partner_id}"
            if achieved_key not in st.session_state:
                st.session_state[achieved_key] = pd.Timestamp.today().date()

            achieved_date_preview = st.date_input(
                "달성일자",
                key=achieved_key,
            )

            auto_end_preview = add_12_months_minus_1day(achieved_date_preview)
            st.text_input(
                "자동 종료일",
                value=str(auto_end_preview),
                disabled=True,
                key=f"pgh_auto_end_preview_upgrade_{partner_id}",
            )

            with st.form("grade_upgrade_form"):
                selected_grade_label = st.selectbox("새 등급", grade_labels, index=default_grade_idx)
                reason = st.text_area("사유", height=100, placeholder="예: 최근 12개월 매출 목표 달성")
                submit_upgrade = st.form_submit_button("승급 적용", use_container_width=True)

            if submit_upgrade:
                try:
                    apply_grade_upgrade(
                        conn=conn,
                        partner_id=partner_id,
                        grade_code=grade_map[selected_grade_label],
                        achieved_date=str(achieved_date_preview),
                        reason=reason.strip(),
                    )
                    st.success("승급이 적용되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(f"승급 적용 중 오류 발생: {e}")

        with tab2:
            left, right = st.columns([1.2, 1])

            with left:
                st.markdown("#### 등급 이력")
                if history_df.empty:
                    st.info("등록된 등급 이력이 없습니다.")
                else:
                    display_df = history_df.copy()
                    if "grade_name" in display_df.columns:
                        display_df["등급"] = display_df["grade_name"].fillna(display_df["grade_code"])
                    else:
                        display_df["등급"] = display_df["grade_code"]

                    st.dataframe(
                        display_df[["id", "grade_code", "등급", "start_date", "end_date", "reason", "updated_at"]],
                        use_container_width=True,
                        hide_index=True,
                        height=440,
                    )

                    history_options = [
                        f"{int(r['id'])} | {r['grade_code']} | {r['start_date']} ~ {r['end_date']}"
                        for _, r in history_df.iterrows()
                    ]
                    selected_history_label = st.selectbox(
                        "수정/삭제할 이력 선택",
                        options=[""] + history_options,
                        index=0,
                    )
                    if st.button("이력 불러오기", use_container_width=True):
                        if selected_history_label:
                            st.session_state.pgh_selected_history_id = int(selected_history_label.split("|")[0].strip())
                            st.rerun()

            selected = None
            if st.session_state.pgh_selected_history_id:
                selected = get_one_history(conn, st.session_state.pgh_selected_history_id)

            with right:
                st.markdown(f"#### {'이력 수정' if selected else '이력 신규 등록'}")
                form_key = f"pgh_form_{st.session_state.pgh_reset_key}"

                if not grade_labels:
                    st.warning("partner_grade_mst에 등급을 먼저 등록해 주세요.")
                    return

                selected_label = grade_labels[0]
                if selected and selected.get("grade_code"):
                    for gl in grade_labels:
                        if grade_map[gl] == selected["grade_code"]:
                            selected_label = gl
                            break

                # 시작일은 form 밖에서 즉시 반영되도록 처리 (form 안에 있으면 저장 전까지
                # 값이 바뀌어도 아래 "자동 종료일" 미리보기가 갱신되지 않는 문제가 있었음)
                start_date = st.date_input(
                    "시작일",
                    value=pd.to_datetime(selected["start_date"]).date() if selected and selected.get("start_date") else pd.Timestamp.today().date(),
                    key=f"pgh_start_date_{st.session_state.pgh_selected_history_id or 'new'}",
                )
                auto_end_date = add_12_months_minus_1day(start_date)
                st.text_input(
                    "자동 종료일",
                    value=str(auto_end_date),
                    disabled=True,
                    key=f"pgh_auto_end_date_history_{st.session_state.pgh_selected_history_id or 'new'}",
                )

                with st.form(form_key):
                    grade_label = st.selectbox(
                        "등급",
                        options=grade_labels,
                        index=grade_labels.index(selected_label) if selected_label in grade_labels else 0,
                    )

                    reason = st.text_area(
                        "사유",
                        value=(selected.get("reason") or "") if selected else "",
                        height=100,
                    )

                    save_col, reset_col, del_col = st.columns(3)
                    save_clicked = save_col.form_submit_button("저장", use_container_width=True)
                    reset_clicked = reset_col.form_submit_button("초기화", use_container_width=True)
                    delete_clicked = del_col.form_submit_button(
                        "삭제",
                        use_container_width=True,
                        disabled=(selected is None),
                    )

                if save_clicked:
                    # ✅ 수정: end_date → auto_end_date 로 변수명 통일
                    if pd.Timestamp(auto_end_date) < pd.Timestamp(start_date):
                        st.error("종료일은 시작일보다 빠를 수 없습니다.")
                    else:
                        try:
                            upsert_history_manual(
                                conn=conn,
                                row_id=st.session_state.pgh_selected_history_id,
                                payload={
                                    "partner_id": partner_id,
                                    "grade_code": grade_map[grade_label],
                                    "start_date": pd.Timestamp(start_date).strftime("%Y-%m-%d"),
                                    "end_date": pd.Timestamp(auto_end_date).strftime("%Y-%m-%d"),  # ✅ 핵심 수정
                                    "reason": reason.strip(),
                                },
                            )
                            st.success("이력이 저장되었습니다.")
                            reset_form()
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

                if reset_clicked:
                    reset_form()
                    st.rerun()

                if delete_clicked and selected is not None:
                    try:
                        delete_history(conn, st.session_state.pgh_selected_history_id)
                        st.success("이력이 삭제되었습니다.")
                        reset_form()
                        st.rerun()
                    except Exception as e:
                        st.error(f"삭제 중 오류 발생: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    render()