"""
전체 기간 매출 추이 차트 (홈 화면).

- 일/주/월 집계 단위 전환 (기본: 월). 14개월치 일별 막대는 겹쳐서 추세가 안 읽히므로 월 단위를 기본으로.
- 소매/도매 누적 막대 + 소매/도매 선형 추세선(점선).
- 아직 끝나지 않은 마지막 구간(이번 달/이번 주/오늘)은 '진행 중'으로 흐리게 표시하고 추세선 계산에서 제외.
- SQL 직접 실행 없음 — 홈이 정본 로더로 만든 판매 DataFrame(dt, sales_type, sales_amount)을 그대로 받는다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

UNITS = ("월", "주", "일")
TYPES = ("소매", "도매")

_X_FORMAT = {"월": "%y-%m", "주": "%y-%m", "일": "%y-%m"}  # 주/일도 눈금은 월 단위로 찍히므로 연-월 표기
_TOOLTIP_FORMAT = {"월": "%Y-%m", "주": "%Y-%m-%d 주", "일": "%Y-%m-%d"}


def period_start(ts: pd.Series | pd.Timestamp, unit: str):
    """구간 시작일. 월=1일, 주=월요일, 일=그날."""
    t = pd.to_datetime(ts)
    if unit == "월":
        return t.to_period("M").to_timestamp() if isinstance(t, pd.Timestamp) else t.dt.to_period("M").dt.to_timestamp()
    if unit == "주":
        return t.to_period("W-SUN").start_time.normalize() if isinstance(t, pd.Timestamp) else t.dt.to_period("W-SUN").dt.start_time.dt.normalize()
    return t.normalize() if isinstance(t, pd.Timestamp) else t.dt.normalize()


def aggregate_trend(df: pd.DataFrame, unit: str, today: pd.Timestamp) -> pd.DataFrame:
    """
    df: dt, sales_type('소매'/'도매'), sales_amount
    반환(wide): period, 소매, 도매, 합계, in_progress(bool), 소매추세, 도매추세
      - 데이터가 없는 중간 구간은 0 으로 채움 (막대 간격 균일)
      - 추세선: 첫 매출 발생 이후의 마감 구간(0 포함)으로 선형회귀, 진행 중 구간은 제외·미표시
    """
    if unit not in UNITS:
        raise ValueError(f"unit must be one of {UNITS}")
    cols = ["period", *TYPES, "합계", "in_progress", "소매추세", "도매추세"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    today = pd.Timestamp(today).normalize()
    d = df[["dt", "sales_type", "sales_amount"]].dropna(subset=["dt"]).copy()
    d["dt"] = pd.to_datetime(d["dt"]).dt.normalize()
    d["sales_amount"] = pd.to_numeric(d["sales_amount"], errors="coerce").fillna(0.0).astype(float)
    d = d[d["dt"] <= today]
    if d.empty:
        return pd.DataFrame(columns=cols)
    d["period"] = period_start(d["dt"], unit)

    wide = d.pivot_table(index="period", columns="sales_type", values="sales_amount", aggfunc="sum", fill_value=0.0)
    for t in TYPES:
        if t not in wide.columns:
            wide[t] = 0.0
    wide = wide[list(TYPES)]

    # 빈 구간 채우기
    freq = {"월": "MS", "주": "W-MON", "일": "D"}[unit]
    full_idx = pd.date_range(wide.index.min(), period_start(today, unit), freq=freq)
    wide = wide.reindex(full_idx, fill_value=0.0)
    wide.index.name = "period"
    wide = wide.reset_index()
    wide["합계"] = wide["소매"] + wide["도매"]

    current_period = period_start(today, unit)
    wide["in_progress"] = wide["period"] == current_period
    if unit == "일":
        # 하루 단위는 '오늘'만 진행 중
        wide["in_progress"] = wide["period"] == today

    def _trend(series: pd.Series) -> pd.Series:
        x = np.arange(len(series))
        out = pd.Series(np.nan, index=series.index, dtype=float)
        nonzero = np.flatnonzero(series.values > 0)
        if len(nonzero) == 0:
            return out
        # 첫 매출 발생 이후의 마감 구간 전부(0 포함)로 회귀 — 시작 전 구간은 제외, 이후의 0(휴무일 등)은 포함
        mask = (np.arange(len(series)) >= nonzero[0]) & (~wide["in_progress"].values)
        if mask.sum() < 2:
            return out
        coeffs = np.polyfit(x[mask], series[mask], 1)
        out[mask] = np.polyval(coeffs, x[mask])  # 첫 매출 이후 ~ 마지막 마감 구간에만 선을 그림
        return out.clip(lower=0)  # 회귀선이 0 아래로 내려가는 구간은 0 으로

    wide["소매추세"] = _trend(wide["소매"])
    wide["도매추세"] = _trend(wide["도매"])
    return wide[cols]


def render_sales_trend(df: pd.DataFrame, today: pd.Timestamp, height: int = 320) -> None:
    """streamlit 컨텍스트 안에서 호출."""
    import altair as alt
    import streamlit as st

    unit = st.radio("집계 단위", list(UNITS), index=0, horizontal=True, key="home_trend_unit", label_visibility="collapsed")
    wide = aggregate_trend(df, unit, today)
    if wide.empty:
        st.info("표시할 매출 데이터가 없습니다.")
        return

    long_df = wide.melt(
        id_vars=["period", "in_progress", "합계"], value_vars=list(TYPES),
        var_name="구분", value_name="매출",
    )
    long_df["상태"] = np.where(long_df["in_progress"], "진행 중", "마감")

    x = alt.X("period:T", title=None, axis=alt.Axis(format=_X_FORMAT[unit], labelAngle=0))
    # 시간축 막대는 폭을 직접 지정해야 함 (구간 수에 따라 2~36px)
    bar_size = float(np.clip(700 / max(len(wide), 1) * 0.65, 2, 36))
    bars = alt.Chart(long_df).mark_bar(size=bar_size).encode(
        x=x,
        y=alt.Y("매출:Q", title="매출액", stack="zero", axis=alt.Axis(format="~s")),
        color=alt.Color(
            "구분:N", title=None,
            scale=alt.Scale(domain=list(TYPES), range=["#4C72B0", "#DD8452"]),
            legend=alt.Legend(orient="top-left"),
        ),
        opacity=alt.condition(alt.datum.in_progress, alt.value(0.45), alt.value(0.9)),
        tooltip=[
            alt.Tooltip("period:T", title="구간", format=_TOOLTIP_FORMAT[unit]),
            alt.Tooltip("구분:N", title="구분"),
            alt.Tooltip("매출:Q", title="매출", format=",.0f"),
            alt.Tooltip("합계:Q", title="합계", format=",.0f"),
            alt.Tooltip("상태:N", title="상태"),
        ],
    )
    trend_base = alt.Chart(wide).encode(x=x)
    line_retail = trend_base.mark_line(color="#1a4f9e", strokeWidth=2, strokeDash=[4, 2]).encode(
        y=alt.Y("소매추세:Q", title=None),
        tooltip=[alt.Tooltip("소매추세:Q", title="소매 추세", format=",.0f")],
    )
    line_wholesale = trend_base.mark_line(color="#b05a1a", strokeWidth=2, strokeDash=[4, 2]).encode(
        y=alt.Y("도매추세:Q", title=None),
        tooltip=[alt.Tooltip("도매추세:Q", title="도매 추세", format=",.0f")],
    )
    chart = (bars + line_retail + line_wholesale).properties(height=height).configure_view(strokeWidth=0)
    st.altair_chart(chart, use_container_width=True)

    first, last = wide["period"].min(), wide["period"].max()
    n = len(wide)
    unit_word = {"월": "개월", "주": "주", "일": "일"}[unit]
    st.caption(
        f"전체 기간: {first.strftime('%Y-%m-%d')} ~ {last.strftime('%Y-%m-%d')} ({n:,}{unit_word}) | "
        f"점선: 소매/도매 추세선(마감 구간 기준) | 흐린 막대: 진행 중인 {unit}"
    )
