"""
월 목표 대비 진행률 위젯 (대시보드).

- monthly_target 테이블에 연/월별 목표(매출총액·영업이익)를 저장 (없으면 최근 목표를 이어서 사용)
- 선택한 월의 현재 실적(정본 get_month_summary), 경과일 기준 일평균, 월말 예상치, 목표까지 남은 금액을 표시
- 지난 달(완료된 달)은 예상치 대신 최종 달성률만 표시
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from modules.common.dbutil import table_exists
from modules.common.fmt import fmt_krw

TABLE = "monthly_target"


def ensure_target_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            target_sales REAL NOT NULL DEFAULT 0,             -- 목표 총매출(소매+도매, 부가세 제외)
            target_operating_profit REAL NOT NULL DEFAULT 0,  -- 목표 영업이익
            notes TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, month)
        )
        """
    )
    conn.commit()


def get_target(conn, year: int, month: int) -> tuple[dict | None, bool]:
    """(목표 dict, 해당 월에 직접 설정된 것인지). 없으면 가장 최근 설정을 이어서 사용(inherited=False)."""
    ensure_target_table(conn)
    row = conn.execute(
        f"SELECT target_sales, target_operating_profit, notes FROM {TABLE} WHERE year=? AND month=?", (year, month)
    ).fetchone()
    if row:
        return {"target_sales": float(row[0] or 0), "target_operating_profit": float(row[1] or 0), "notes": row[2] or ""}, True
    row = conn.execute(
        f"SELECT target_sales, target_operating_profit, notes FROM {TABLE} "
        f"WHERE (year*100+month) < ? ORDER BY year DESC, month DESC LIMIT 1", (year * 100 + month,)
    ).fetchone()
    if row:
        return {"target_sales": float(row[0] or 0), "target_operating_profit": float(row[1] or 0), "notes": row[2] or ""}, False
    return None, False


def save_target(conn, year: int, month: int, target_sales: float, target_operating_profit: float, notes: str = "") -> None:
    ensure_target_table(conn)
    conn.execute(
        f"""
        INSERT INTO {TABLE} (year, month, target_sales, target_operating_profit, notes, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(year, month) DO UPDATE SET
            target_sales = excluded.target_sales,
            target_operating_profit = excluded.target_operating_profit,
            notes = excluded.notes,
            updated_at = CURRENT_TIMESTAMP
        """,
        (int(year), int(month), float(target_sales), float(target_operating_profit), notes.strip() or None),
    )
    conn.commit()


def compute_progress(actual: float, target: float, elapsed_days: int, total_days: int, month_closed: bool) -> dict:
    """진행률 계산 (순수 함수, 테스트 대상)."""
    rate = (actual / target * 100) if target else None
    if month_closed or elapsed_days <= 0:
        projected = actual
    else:
        projected = actual / elapsed_days * total_days
    remaining = max(target - actual, 0.0) if target else None
    remaining_days = max(total_days - elapsed_days, 0)
    # 남은 기간 동안 하루 얼마씩 더 필요한가
    needed_per_day = (remaining / remaining_days) if (remaining and remaining_days > 0) else 0.0
    return {
        "rate": rate,
        "projected": projected,
        "projected_rate": (projected / target * 100) if target else None,
        "remaining": remaining,
        "remaining_days": remaining_days,
        "needed_per_day": needed_per_day,
    }


def progress_bar_ratio(rate: float) -> float:
    """달성률(%)을 st.progress()가 요구하는 0.0~1.0 범위로 변환.

    적자(rate < 0)나 목표 초과 달성(rate > 100)에서도 항상 유효 범위를 반환해야
    StreamlitValueOutOfRangeError가 나지 않는다. 실제 달성률 표시(텍스트)는
    클램프하지 않은 rate를 그대로 쓴다.
    """
    return max(0.0, min(rate / 100, 1.0))


def month_elapsed(year: int, month: int, today: pd.Timestamp | None = None) -> tuple[int, int, bool]:
    """(경과일수, 그 달의 총일수, 마감여부). 미래 달이면 0일 경과, 지난 달이면 마감."""
    today = pd.Timestamp(today).normalize() if today is not None else pd.Timestamp.today().normalize()
    month_start = pd.Timestamp(year=year, month=month, day=1)
    month_end = month_start + pd.offsets.MonthEnd(1)
    total_days = int(month_end.day)
    if today < month_start:
        return 0, total_days, False
    if today > month_end:
        return total_days, total_days, True
    return int(today.day), total_days, False


def status_icon(projected_rate: float | None) -> str:
    """월말 예상 달성률 기준 신호등."""
    if projected_rate is None:
        return "⚪"
    if projected_rate >= 100:
        return "🟢"
    if projected_rate >= 80:
        return "🟡"
    return "🔴"


def build_target_summary(
    target: dict | None, summary: dict, elapsed: int, total_days: int, closed: bool
) -> dict:
    """
    홈 상단 한 줄 요약용 계산 (순수 함수).
    반환: {has_target, elapsed, total_days, closed,
           metrics: {"매출": {...}, "영업이익": {...}}}
      각 metric: actual, target, rate(None 가능), projected, projected_rate(None 가능), icon
    """
    out = {
        "has_target": target is not None,
        "elapsed": elapsed,
        "total_days": total_days,
        "closed": closed,
        "metrics": {},
    }
    if target is None:
        return out

    pairs = (
        ("매출", float(summary.get("total_sales", 0) or 0), float(target.get("target_sales", 0) or 0)),
        ("영업이익", float(summary.get("operating_profit", 0) or 0), float(target.get("target_operating_profit", 0) or 0)),
    )
    for label, actual, target_v in pairs:
        p = compute_progress(actual, target_v, elapsed, total_days, closed)
        out["metrics"][label] = {
            "actual": actual,
            "target": target_v,
            "rate": p["rate"],
            "projected": p["projected"],
            "projected_rate": p["projected_rate"],
            "icon": status_icon(p["projected_rate"]),
        }
    return out


def render_target_summary_line(conn, year: int, month: int, summary: dict) -> None:
    """홈 상단용 한 줄 요약 (달성률 + 월말 예상). 상세 위젯은 대시보드에 있음."""
    import streamlit as st_

    ensure_target_table(conn)
    elapsed, total_days, closed = month_elapsed(year, month)
    target, is_own = get_target(conn, year, month)
    s = build_target_summary(target, summary, elapsed, total_days, closed)

    if not s["has_target"]:
        st_.caption("🎯 이번 달 목표가 설정되지 않았습니다 — 대시보드 > 월 목표에서 설정하면 여기에 진행률이 표시됩니다.")
        return

    head = f"🎯 **{year}년 {month}월 목표**"
    head += " (마감)" if closed else f" ({elapsed}/{total_days}일 경과)"
    if not is_own:
        head += " *이월*"

    parts = [head]
    for label, m in s["metrics"].items():
        if m["rate"] is None:
            parts.append(f"⚪ {label} 목표 미설정")
            continue
        seg = f"{m['icon']} {label} **{m['rate']:.0f}%**"
        if not closed:
            seg += f" → 월말 예상 **{m['projected_rate']:.0f}%**"
        seg += f" ({fmt_krw(m['actual'])} / {fmt_krw(m['target'])})"
        parts.append(seg)

    st_.markdown("　·　".join(parts))


def render_target_widget(conn, year: int, month: int, summary: dict) -> None:
    """
    summary: get_month_summary() 결과 (total_sales, operating_profit 사용)
    """
    ensure_target_table(conn)
    elapsed, total_days, closed = month_elapsed(year, month)

    target, is_own = get_target(conn, year, month)

    st.markdown("###### 월 목표 대비 진행률")
    with st.expander("목표 설정", expanded=(target is None)):
        with st.form(f"target_form_{year}_{month}"):
            c1, c2 = st.columns(2)
            t_sales = c1.number_input(
                "목표 총매출(원, 부가세 제외)", min_value=0, step=100_000,
                value=int(target["target_sales"]) if target else 0, format="%d",
            )
            t_profit = c2.number_input(
                "목표 영업이익(원)", min_value=0, step=100_000,
                value=int(target["target_operating_profit"]) if target else 0, format="%d",
            )
            notes = st.text_input("메모 (선택)", value=target["notes"] if target else "")
            if st.form_submit_button(f"{year}년 {month}월 목표 저장", use_container_width=True):
                save_target(conn, year, month, t_sales, t_profit, notes)
                st.success("저장했습니다.")
                st.rerun()

    if not target:
        st.info("목표가 설정되지 않았습니다. 위에서 이번 달 목표를 입력하면 진행률이 표시됩니다.")
        return
    if not is_own:
        st.caption("※ 이 달에 설정된 목표가 없어 가장 최근 목표를 그대로 적용했습니다. 바꾸려면 위에서 저장하세요.")

    actual_sales = float(summary.get("total_sales", 0) or 0)
    actual_profit = float(summary.get("operating_profit", 0) or 0)
    ps = compute_progress(actual_sales, target["target_sales"], elapsed, total_days, closed)
    pp = compute_progress(actual_profit, target["target_operating_profit"], elapsed, total_days, closed)

    period_txt = f"{elapsed}/{total_days}일 경과" if not closed else "마감"
    st.caption(f"{year}년 {month}월 · {period_txt}")

    def _block(col, label, actual, target_v, p):
        col.metric(f"{label} 실적", fmt_krw(actual), f"목표 {fmt_krw(target_v)}", delta_color="off")
        if p["rate"] is not None:
            col.progress(progress_bar_ratio(p["rate"]), text=f"달성률 {p['rate']:.0f}%")
            if closed:
                col.caption("최종 달성률" + (" — 목표 달성 ✅" if p["rate"] >= 100 else f" — 미달 {fmt_krw(p['remaining'])}"))
            else:
                line = f"월말 예상 {fmt_krw(p['projected'])} ({p['projected_rate']:.0f}%)"
                if p["remaining"] and p["remaining_days"] > 0:
                    line += f" · 남은 {p['remaining_days']}일 동안 하루 {fmt_krw(p['needed_per_day'])} 필요"
                elif p["remaining"] == 0:
                    line += " · 목표 달성 ✅"
                col.caption(line)
        else:
            col.caption("목표가 0원이라 달성률을 계산하지 않습니다.")

    a, b = st.columns(2)
    _block(a, "총매출", actual_sales, target["target_sales"], ps)
    _block(b, "영업이익", actual_profit, target["target_operating_profit"], pp)
