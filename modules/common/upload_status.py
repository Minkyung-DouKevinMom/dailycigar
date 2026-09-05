"""
소매 매출 업로드 현황 (홈 화면 표시용).

- 마지막 업로드 일시 / 업로드로 커버된 마지막 판매일 / 오늘 기준 경과일수
- 최근 N일 중 판매 기록도 없고 업로드 기간에도 포함되지 않은 날짜 목록
  (휴무일일 수도 있으므로 "확인 필요" 수준으로만 안내)
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from modules.common.dbutil import table_exists

WARN_DAYS = 2   # 이 일수 이상 업로드가 없으면 주의
ALERT_DAYS = 4  # 이 일수 이상이면 경고


@dataclass
class UploadStatus:
    last_uploaded_at: str | None = None      # 마지막 업로드 실행 시각
    last_covered_date: str | None = None     # 업로드로 커버된 마지막 판매일
    last_sale_date: str | None = None        # retail_sales 상 마지막 판매일
    days_since_covered: int | None = None    # 오늘 - last_covered_date
    uncovered_dates: list[str] = field(default_factory=list)  # 최근 N일 중 기록 없는 날
    level: str = "ok"                        # ok / warn / alert / none


def get_retail_upload_status(conn, lookback_days: int = 30, today: pd.Timestamp | None = None) -> UploadStatus:
    st = UploadStatus()
    today = (today or pd.Timestamp.today()).normalize()

    if not table_exists(conn, "retail_sales"):
        st.level = "none"
        return st

    sold = pd.read_sql_query("SELECT DISTINCT sale_date FROM retail_sales WHERE sale_date IS NOT NULL", conn)
    sold_dates = set(pd.to_datetime(sold["sale_date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d"))
    st.last_sale_date = max(sold_dates) if sold_dates else None

    covered: set[str] = set()
    if table_exists(conn, "retail_sales_upload"):
        up = pd.read_sql_query(
            "SELECT upload_period_from AS f, upload_period_to AS t, uploaded_at "
            "FROM retail_sales_upload WHERE upload_period_from IS NOT NULL AND upload_period_to IS NOT NULL",
            conn,
        )
        if not up.empty:
            st.last_uploaded_at = str(up["uploaded_at"].max())
            for _, r in up.iterrows():
                try:
                    for d in pd.date_range(r["f"], r["t"]):
                        covered.add(d.strftime("%Y-%m-%d"))
                except Exception:
                    continue
            st.last_covered_date = max(covered) if covered else None

    ref = st.last_covered_date or st.last_sale_date
    if ref:
        st.days_since_covered = int((today - pd.Timestamp(ref)).days)

    start = today - pd.Timedelta(days=lookback_days)
    if sold_dates:
        start = max(start, pd.Timestamp(min(sold_dates)))
    rng = pd.date_range(start, today - pd.Timedelta(days=1))
    st.uncovered_dates = [
        d.strftime("%Y-%m-%d") for d in rng
        if d.strftime("%Y-%m-%d") not in covered and d.strftime("%Y-%m-%d") not in sold_dates
    ]

    if st.days_since_covered is None:
        st.level = "none"
    elif st.days_since_covered >= ALERT_DAYS:
        st.level = "alert"
    elif st.days_since_covered >= WARN_DAYS:
        st.level = "warn"
    else:
        st.level = "ok"
    return st


def render_retail_upload_status(conn, lookback_days: int = 30) -> None:
    """홈 화면용 위젯. streamlit 컨텍스트 안에서 호출."""
    import streamlit as st_

    s = get_retail_upload_status(conn, lookback_days=lookback_days)
    if s.level == "none":
        st_.info("소매 매출 업로드 이력이 없습니다.")
        return

    weekday_kr = ["월", "화", "수", "목", "금", "토", "일"]

    def _with_dow(d: str) -> str:
        return f"{d[5:]}({weekday_kr[pd.Timestamp(d).dayofweek]})"

    headline = (
        f"소매 매출 업로드: 마지막 반영일 **{s.last_covered_date or s.last_sale_date}** "
        f"(오늘 기준 {s.days_since_covered}일 경과)"
    )
    if s.last_uploaded_at:
        headline += f" · 마지막 업로드 실행 {s.last_uploaded_at[:16]}"

    if s.level == "alert":
        st_.error(headline + f" — {ALERT_DAYS}일 이상 업로드가 없습니다. 매출 엑셀을 올려주세요.")
    elif s.level == "warn":
        st_.warning(headline + " — 업로드가 밀리고 있습니다.")
    else:
        st_.success(headline)

    if s.uncovered_dates:
        st_.caption(
            f"최근 {lookback_days}일 중 판매 기록이 없는 날 {len(s.uncovered_dates)}일: "
            + ", ".join(_with_dow(d) for d in s.uncovered_dates)
            + "  (휴무일이면 정상, 영업일이면 업로드 누락 확인)"
        )
