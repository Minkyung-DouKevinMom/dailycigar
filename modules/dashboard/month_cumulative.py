"""
이번 달 vs 지난 달 일자별 누적 매출 비교 (홈 화면 위젯).

- 입력은 홈 화면이 이미 정본 로더(sales_query)로 만든 판매 DataFrame(dt, sales_amount)이며,
  여기서는 SQL 을 직접 실행하지 않는다 (CLAUDE.md 규칙 2).
- x축 = 일(1~31), y축 = 월초부터의 누적 매출(소매+도매). 지난 달은 월말까지, 이번 달은 오늘까지.
- "지난달 같은 시점" = 지난 달의 오늘 일자(오늘이 5일이면 지난달 5일)까지 누적.
  지난 달이 더 짧아 해당 일자가 없으면 지난 달 마지막 날 기준.
"""
from __future__ import annotations

import calendar

import pandas as pd

from modules.common.fmt import fmt_krw

CUR_LABEL = "이번 달"
PREV_LABEL = "지난 달"


def _month_bounds(ts: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp, int]:
    start = ts.normalize().replace(day=1)
    ndays = calendar.monthrange(start.year, start.month)[1]
    end = start + pd.Timedelta(days=ndays - 1)
    return start, end, ndays


def build_month_cumulative(df: pd.DataFrame, today: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    """
    df: 컬럼 dt(datetime), sales_amount(float) 를 가진 판매 라인 (소매+도매 합산, 필터 전 상태여도 됨)
    반환:
      long_df: [day, label, cumulative, date]  — 차트용
      info:    {cur_total, prev_same_day_total, prev_total, today_day, prev_days, cur_days,
                diff_amount, diff_pct(None 가능), cur_month, prev_month}
    """
    today = pd.Timestamp(today).normalize()
    cur_start, cur_end, cur_days = _month_bounds(today)
    prev_start, prev_end, prev_days = _month_bounds(cur_start - pd.Timedelta(days=1))
    today_day = int(today.day)

    if df is None or df.empty:
        daily = pd.Series(dtype=float)
    else:
        d = df[["dt", "sales_amount"]].dropna(subset=["dt"]).copy()
        d["dt"] = pd.to_datetime(d["dt"]).dt.normalize()
        d = d[(d["dt"] >= prev_start) & (d["dt"] <= today)]
        daily = d.groupby("dt")["sales_amount"].sum()

    def _cum(start: pd.Timestamp, last: pd.Timestamp, label: str) -> pd.DataFrame:
        days = pd.date_range(start, last, freq="D")
        s = daily.reindex(days, fill_value=0.0).cumsum()
        return pd.DataFrame({
            "date": days,
            "day": days.day,
            "label": label,
            "cumulative": s.values.astype(float),
        })

    prev_df = _cum(prev_start, prev_end, PREV_LABEL)
    cur_df = _cum(cur_start, today, CUR_LABEL)
    long_df = pd.concat([prev_df, cur_df], ignore_index=True)

    cur_total = float(cur_df["cumulative"].iloc[-1]) if not cur_df.empty else 0.0
    prev_total = float(prev_df["cumulative"].iloc[-1]) if not prev_df.empty else 0.0
    same_day = min(today_day, prev_days)
    prev_same_day_total = float(prev_df.loc[prev_df["day"] == same_day, "cumulative"].iloc[0]) if not prev_df.empty else 0.0

    diff_amount = cur_total - prev_same_day_total
    diff_pct = (diff_amount / prev_same_day_total * 100) if prev_same_day_total else None

    info = {
        "cur_total": cur_total,
        "prev_same_day_total": prev_same_day_total,
        "prev_total": prev_total,
        "today_day": today_day,
        "same_day": same_day,
        "prev_days": prev_days,
        "cur_days": cur_days,
        "diff_amount": diff_amount,
        "diff_pct": diff_pct,
        "cur_month": f"{cur_start.year}년 {cur_start.month}월",
        "prev_month": f"{prev_start.year}년 {prev_start.month}월",
    }
    return long_df, info


def render_month_cumulative(df: pd.DataFrame, today: pd.Timestamp, height: int = 240) -> None:
    """streamlit 컨텍스트 안에서 호출. df 는 홈 화면의 판매 DataFrame(dt, sales_amount)."""
    import altair as alt
    import streamlit as st

    long_df, info = build_month_cumulative(df, today)

    st.markdown("###### 이번 달 vs 지난 달 누적 매출")
    if long_df.empty or (info["cur_total"] == 0 and info["prev_total"] == 0):
        st.info("비교할 매출 데이터가 없습니다.")
        return

    color = alt.Color(
        "label:N",
        title=None,
        scale=alt.Scale(domain=[PREV_LABEL, CUR_LABEL], range=["#B0B0B0", "#1a4f9e"]),
        legend=alt.Legend(orient="top-left"),
    )
    base = alt.Chart(long_df).encode(
        x=alt.X("day:O", title="일", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("cumulative:Q", title="누적 매출", axis=alt.Axis(format="~s")),
        color=color,
    )
    lines = base.mark_line(strokeWidth=2.5, interpolate="monotone").encode(
        tooltip=[
            alt.Tooltip("label:N", title="구분"),
            alt.Tooltip("date:T", title="날짜", format="%Y-%m-%d"),
            alt.Tooltip("cumulative:Q", title="누적 매출", format=",.0f"),
        ],
    )
    # 오늘 시점 포인트 (이번 달 마지막 점 + 지난 달 같은 일자 점)
    pts = long_df[
        ((long_df["label"] == CUR_LABEL) & (long_df["day"] == info["today_day"]))
        | ((long_df["label"] == PREV_LABEL) & (long_df["day"] == info["same_day"]))
    ]
    points = alt.Chart(pts).mark_point(size=70, filled=True).encode(
        x="day:O", y="cumulative:Q", color=color,
        tooltip=[
            alt.Tooltip("label:N", title="구분"),
            alt.Tooltip("cumulative:Q", title="누적 매출", format=",.0f"),
        ],
    )
    st.altair_chart((lines + points).properties(height=height).configure_view(strokeWidth=0), use_container_width=True)

    if info["diff_pct"] is None:
        cmp_txt = f"지난달 같은 시점({info['same_day']}일) 매출이 없어 비교 불가"
    else:
        sign = "+" if info["diff_amount"] >= 0 else "−"
        cmp_txt = (
            f"지난달 같은 시점({info['same_day']}일) 대비 **{sign}{abs(info['diff_pct']):.1f}%** "
            f"({fmt_krw(info['prev_same_day_total'])} → {fmt_krw(info['cur_total'])})"
        )
    st.caption(
        f"{info['cur_month']} {info['today_day']}일까지 누적 {fmt_krw(info['cur_total'])} · {cmp_txt}  \n"
        f"{info['prev_month']} 최종 {fmt_krw(info['prev_total'])}"
    )
