"""
요일별 평균 매출 패턴 (홈 화면).

- 최근 13주(91일, 각 요일이 정확히 13번씩) 소매 매출을 요일별로 평균.
- 평균의 분모는 '판매 기록이 있는 날(영업일)' — 정기휴무(월요일 등)를 0원으로 섞으면
  "월요일에 열면 얼마 파는지"를 알 수 없어 발주 판단에 쓸 수 없기 때문.
  휴무로 추정되는 날 수는 툴팁·캡션에 따로 보여준다.
- 도매는 B2B 출고라 날짜가 몰려 매장 요일 패턴을 왜곡하므로 제외(소매 기준).
- SQL 직접 실행 없음 — 홈이 정본 로더로 만든 DataFrame(dt, sales_type, sales_amount)을 받는다.
"""
from __future__ import annotations

import pandas as pd

from modules.common.fmt import fmt_krw

DOW_KR = ["월", "화", "수", "목", "금", "토", "일"]
DEFAULT_LOOKBACK_DAYS = 91  # 13주 — 각 요일이 정확히 13번
BAR_COLOR = "#4C72B0"


def weekday_pattern(
    df: pd.DataFrame,
    today: pd.Timestamp,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    sales_type: str | None = "소매",
) -> tuple[pd.DataFrame, dict]:
    """
    df: dt, sales_type, sales_amount
    기간: today 직전 lookback_days 일 (오늘은 집계 중이므로 제외)

    반환:
      table: dow(0=월), 요일, days_total, days_open, days_closed, sales_sum, avg_open, avg_all
      info:  {date_from, date_to, lookback_days, total_open, total_closed, has_data,
              best(dict|None), worst(dict|None), overall_avg}
    """
    today = pd.Timestamp(today).normalize()
    date_to = today - pd.Timedelta(days=1)                 # 어제까지
    date_from = date_to - pd.Timedelta(days=lookback_days - 1)
    days = pd.date_range(date_from, date_to)

    table = pd.DataFrame({"dow": range(7), "요일": DOW_KR})
    table["days_total"] = [int((days.dayofweek == i).sum()) for i in range(7)]

    daily = pd.Series(dtype=float)
    if df is not None and not df.empty:
        d = df[["dt", "sales_type", "sales_amount"]].dropna(subset=["dt"]).copy()
        if sales_type is not None:
            d = d[d["sales_type"] == sales_type]
        d["dt"] = pd.to_datetime(d["dt"]).dt.normalize()
        d["sales_amount"] = pd.to_numeric(d["sales_amount"], errors="coerce").fillna(0.0)
        d = d[(d["dt"] >= date_from) & (d["dt"] <= date_to)]
        if not d.empty:
            daily = d.groupby("dt")["sales_amount"].sum()

    open_days = pd.DatetimeIndex(daily.index)              # 판매 기록이 있는 날 = 영업일
    table["days_open"] = [int((open_days.dayofweek == i).sum()) if len(open_days) else 0 for i in range(7)]
    table["days_closed"] = table["days_total"] - table["days_open"]
    table["sales_sum"] = [
        float(daily[open_days.dayofweek == i].sum()) if len(open_days) else 0.0 for i in range(7)
    ]
    table["avg_open"] = [
        (s / o if o else 0.0) for s, o in zip(table["sales_sum"], table["days_open"])
    ]
    table["avg_all"] = [
        (s / t if t else 0.0) for s, t in zip(table["sales_sum"], table["days_total"])
    ]

    played = table[table["days_open"] > 0]
    total_open = int(table["days_open"].sum())
    overall_avg = float(table["sales_sum"].sum() / total_open) if total_open else 0.0

    def _pick(row) -> dict:
        return {"요일": row["요일"], "avg_open": float(row["avg_open"]), "days_open": int(row["days_open"])}

    best = _pick(played.loc[played["avg_open"].idxmax()]) if not played.empty else None
    worst = _pick(played.loc[played["avg_open"].idxmin()]) if not played.empty else None

    info = {
        "date_from": date_from,
        "date_to": date_to,
        "lookback_days": lookback_days,
        "total_open": total_open,
        "total_closed": int(table["days_closed"].sum()),
        "has_data": total_open > 0,
        "best": best,
        "worst": worst,
        "overall_avg": overall_avg,
    }
    return table, info


def render_weekday_pattern(
    df: pd.DataFrame,
    today: pd.Timestamp,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    height: int = 190,
) -> None:
    """streamlit 컨텍스트 안에서 호출."""
    import altair as alt
    import streamlit as st

    table, info = weekday_pattern(df, today, lookback_days=lookback_days)

    st.subheader("요일별 평균 매출")
    if not info["has_data"]:
        st.info("최근 기간에 소매 매출 데이터가 없습니다.")
        return

    plot = table.assign(
        만원=(table["avg_open"] / 10_000).round(0),
        라벨=[f"{v/10_000:,.0f}만" if v else "-" for v in table["avg_open"]],
    )
    x = alt.X("요일:N", sort=DOW_KR, title=None, axis=alt.Axis(labelAngle=0))
    bars = alt.Chart(plot).mark_bar(size=26, cornerRadiusEnd=3, color=BAR_COLOR).encode(
        x=x,
        y=alt.Y("avg_open:Q", title=None, axis=alt.Axis(format="~s")),
        opacity=alt.condition(alt.datum.days_open == 0, alt.value(0.25), alt.value(0.9)),
        tooltip=[
            alt.Tooltip("요일:N", title="요일"),
            alt.Tooltip("avg_open:Q", title="영업일 평균", format=",.0f"),
            alt.Tooltip("days_open:Q", title="영업일 수", format="d"),
            alt.Tooltip("days_closed:Q", title="기록 없는 날", format="d"),
            alt.Tooltip("sales_sum:Q", title="기간 합계", format=",.0f"),
        ],
    )
    labels = alt.Chart(plot).mark_text(dy=-7, fontSize=11, color="#555").encode(x=x, y="avg_open:Q", text="라벨:N")
    avg_rule = alt.Chart(pd.DataFrame({"v": [info["overall_avg"]]})).mark_rule(
        color="#999", strokeDash=[4, 3]
    ).encode(y="v:Q", tooltip=[alt.Tooltip("v:Q", title="전체 평균", format=",.0f")])

    st.altair_chart(
        (bars + labels + avg_rule).properties(height=height).configure_view(strokeWidth=0),
        use_container_width=True,
    )

    b, w = info["best"], info["worst"]
    line = (
        f"{info['date_from'].strftime('%Y-%m-%d')} ~ {info['date_to'].strftime('%Y-%m-%d')} "
        f"({info['lookback_days'] // 7}주) 소매 기준 · 영업일 {info['total_open']}일 평균 {fmt_krw(info['overall_avg'])}"
    )
    if b and w and b["요일"] != w["요일"]:
        line += f"  \n최고 **{b['요일']}요일** {fmt_krw(b['avg_open'])} · 최저 **{w['요일']}요일** {fmt_krw(w['avg_open'])}"
    closed = table[table["days_closed"] > 0]
    if not closed.empty:
        txt = ", ".join(f"{r['요일']} {int(r['days_closed'])}일" for _, r in closed.iterrows())
        line += f"  \n판매 기록이 없던 날(휴무 추정): {txt} — 평균 계산에서 제외"
    st.caption(line)
