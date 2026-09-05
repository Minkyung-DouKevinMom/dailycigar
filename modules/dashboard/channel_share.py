"""
최근 30일 채널(소매/도매) 비중 — 매출 기준과 마진 기준을 도넛 두 개로 병기 (홈 화면).

- 도매는 매출 비중보다 마진 비중이 낮은 게 보통이므로 둘을 같이 봐야 채널 판단이 된다.
- SQL 직접 실행 없음 — 홈이 정본 로더로 만든 최근 30일 DataFrame(sales_type, sales_amount, margin_amount)을 받는다.
"""
from __future__ import annotations

import pandas as pd

from modules.common.fmt import fmt_krw

TYPES = ("소매", "도매")
COLORS = {"소매": "#4C72B0", "도매": "#DD8452"}


def build_channel_share(df: pd.DataFrame) -> pd.DataFrame:
    """
    반환: sales_type, sales, margin, sales_ratio(%), margin_ratio(%), margin_rate(%)
    항상 소매/도매 두 행 (데이터 없으면 0).
    """
    base = pd.DataFrame({"sales_type": list(TYPES)})
    if df is None or df.empty:
        g = pd.DataFrame({"sales_type": list(TYPES), "sales": 0.0, "margin": 0.0})
    else:
        d = df[["sales_type", "sales_amount", "margin_amount"]].copy()
        d["sales_amount"] = pd.to_numeric(d["sales_amount"], errors="coerce").fillna(0.0)
        d["margin_amount"] = pd.to_numeric(d["margin_amount"], errors="coerce").fillna(0.0)
        g = d.groupby("sales_type", as_index=False).agg(sales=("sales_amount", "sum"), margin=("margin_amount", "sum"))
    out = base.merge(g, on="sales_type", how="left").fillna({"sales": 0.0, "margin": 0.0})
    total_sales = float(out["sales"].sum())
    total_margin = float(out["margin"].sum())
    out["sales_ratio"] = out["sales"] / total_sales * 100 if total_sales else 0.0
    # 마진 합계가 0 이하(적자)면 비중 정의가 안 되므로 0 처리
    out["margin_ratio"] = out["margin"] / total_margin * 100 if total_margin > 0 else 0.0
    out["margin_rate"] = [(m / s * 100) if s else 0.0 for m, s in zip(out["margin"], out["sales"])]
    return out


def render_channel_share(df: pd.DataFrame, height: int = 150) -> None:
    """streamlit 컨텍스트 안에서 호출."""
    import altair as alt
    import streamlit as st

    share = build_channel_share(df)
    total_sales = float(share["sales"].sum())
    total_margin = float(share["margin"].sum())

    if total_sales <= 0:
        st.info("최근 30일 매출 데이터가 없습니다.")
        return

    def _donut(value_col: str, ratio_col: str, title: str, center_label: str) -> alt.LayerChart:
        data = share.assign(비중=share[ratio_col].round(1), 금액=share[value_col])
        base = alt.Chart(data).encode(
            theta=alt.Theta(f"{value_col}:Q", stack=True),
            color=alt.Color(
                "sales_type:N", title=None,
                scale=alt.Scale(domain=list(TYPES), range=[COLORS[t] for t in TYPES]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("sales_type:N", title="채널"),
                alt.Tooltip("금액:Q", title=title, format=",.0f"),
                alt.Tooltip("비중:Q", title="비중(%)", format=".1f"),
                alt.Tooltip("margin_rate:Q", title="마진율(%)", format=".1f"),
            ],
        )
        ring = base.mark_arc(innerRadius=int(height * 0.29), outerRadius=int(height * 0.45), cornerRadius=2)
        center = alt.Chart(pd.DataFrame({"t": [center_label]})).mark_text(
            fontSize=15, fontWeight="bold", color="#333"
        ).encode(text="t:N")
        sub = alt.Chart(pd.DataFrame({"t": [title]})).mark_text(fontSize=10, dy=15, color="#777").encode(text="t:N")
        return (ring + center + sub).properties(height=height)

    retail = share.set_index("sales_type").loc["소매"]
    wholesale = share.set_index("sales_type").loc["도매"]

    c1, c2 = st.columns(2)
    with c1:
        st.altair_chart(
            _donut("sales", "sales_ratio", "매출 기준", f"소매 {retail['sales_ratio']:.0f}%"),
            use_container_width=True,
        )
    with c2:
        if total_margin > 0:
            st.altair_chart(
                _donut("margin", "margin_ratio", "마진 기준", f"소매 {retail['margin_ratio']:.0f}%"),
                use_container_width=True,
            )
        else:
            st.caption("마진 합계가 0 이하라 마진 기준 비중을 표시하지 않습니다.")

    st.caption(
        f"🟦 소매 매출 {fmt_krw(retail['sales'])} ({retail['sales_ratio']:.1f}%) · 마진 {fmt_krw(retail['margin'])} "
        f"({retail['margin_ratio']:.1f}%) · 마진율 {retail['margin_rate']:.1f}%  \n"
        f"🟧 도매 매출 {fmt_krw(wholesale['sales'])} ({wholesale['sales_ratio']:.1f}%) · 마진 {fmt_krw(wholesale['margin'])} "
        f"({wholesale['margin_ratio']:.1f}%) · 마진율 {wholesale['margin_rate']:.1f}%"
    )
