"""
위스키(개인사업자) HOME / 경영 대시보드 화면.

- 데일리시가 HOME·대시보드와 같은 구성(최근 30일 KPI → 매출 추이·비중 → 재고 알림)을 따른다.
- 매출/원가/마진은 modules/whisky/whisky_db.py 의 정본(load_sales, summarize_*)만 사용한다.
  이 파일에는 SQL 도, 매출·이익 계산식도 쓰지 않는다 (집계 단위 변환과 표시만 한다).
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

from modules.common.fmt import fmt_krw
from modules.dashboard.kpi_summary import format_delta_pct
from modules.dashboard.sales_trend import period_start
from modules.whisky import whisky_db as wdb

UNITS = ("월", "주", "일")
TYPES = wdb.SALE_TYPE_LABELS  # ("잔 판매", "병 판매")
COLORS = {"잔 판매": "#4C72B0", "병 판매": "#DD8452"}  # 데일리시가 차트(소매/도매)와 같은 두 색
_TOOLTIP_FORMAT = {"월": "%Y-%m", "주": "%Y-%m-%d 주", "일": "%Y-%m-%d"}

DEPLETION_THRESHOLD_DAYS = 30
UNSOLD_THRESHOLD_DAYS = 60


# =========================================================
# 집계 (표시용 형태 변환만 — 금액 기준은 whisky_db 정본)
# =========================================================
def aggregate_trend(df: pd.DataFrame, unit: str, today) -> pd.DataFrame:
    """
    load_sales 결과 → 구간별 잔/병 매출(wide).
    반환: period, 잔 판매, 병 판매, 합계, in_progress(bool). 중간에 비는 구간은 0 으로 채운다.
    """
    if unit not in UNITS:
        raise ValueError(f"unit must be one of {UNITS}")
    cols = ["period", *TYPES, "합계", "in_progress"]
    today = pd.Timestamp(today).normalize()
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    d = df[(df["sales_type"] != "") & (df["dt"] <= today)]
    if d.empty:
        return pd.DataFrame(columns=cols)
    d = d.assign(period=period_start(d["dt"].dt.normalize(), unit))

    wide = d.pivot_table(index="period", columns="sales_type", values="sales_amount", aggfunc="sum", fill_value=0.0)
    for t in TYPES:
        if t not in wide.columns:
            wide[t] = 0.0
    freq = {"월": "MS", "주": "W-MON", "일": "D"}[unit]
    full_idx = pd.date_range(wide.index.min(), period_start(today, unit), freq=freq)
    wide = wide[list(TYPES)].reindex(full_idx, fill_value=0.0)
    wide.index.name = "period"
    wide = wide.reset_index()
    wide["합계"] = wide[list(TYPES)].sum(axis=1)
    wide["in_progress"] = wide["period"] == period_start(today, unit)
    return wide[cols]


def build_type_share(summary: dict) -> pd.DataFrame:
    """summarize_sales 결과 → 잔/병 두 행: sales_type, sales, margin, sales_ratio(%), margin_ratio(%), margin_rate(%)."""
    out = pd.DataFrame({
        "sales_type": list(TYPES),
        "sales": [summary["glass_sales"], summary["bottle_sales"]],
        "margin": [summary["glass_margin"], summary["bottle_margin"]],
    })
    total_sales, total_margin = float(out["sales"].sum()), float(out["margin"].sum())
    out["sales_ratio"] = out["sales"] / total_sales * 100 if total_sales else 0.0
    out["margin_ratio"] = out["margin"] / total_margin * 100 if total_margin > 0 else 0.0
    out["margin_rate"] = [(m / s * 100) if s else 0.0 for m, s in zip(out["margin"], out["sales"])]
    return out


# =========================================================
# 차트
# =========================================================
def render_sales_trend(df: pd.DataFrame, today, key: str, units=UNITS, height: int = 320) -> None:
    """잔/병 누적 막대 매출 추이. 진행 중인 마지막 구간은 흐리게."""
    import altair as alt
    import streamlit as st

    unit = units[0]
    if len(units) > 1:
        unit = st.radio("집계 단위", list(units), index=0, horizontal=True, key=key, label_visibility="collapsed")
    wide = aggregate_trend(df, unit, today)
    if wide.empty:
        st.info("표시할 매출 데이터가 없습니다. 판매가 등록되면 여기에 추이가 표시됩니다.")
        return

    long_df = wide.melt(
        id_vars=["period", "in_progress", "합계"], value_vars=list(TYPES), var_name="구분", value_name="매출",
    )
    long_df["상태"] = np.where(long_df["in_progress"], "진행 중", "마감")
    bar_size = float(np.clip(700 / max(len(wide), 1) * 0.65, 2, 36))
    months = max((wide["period"].max().to_period("M") - wide["period"].min().to_period("M")).n + 1, 1)
    tick_step = int(np.ceil(months / 12))  # 눈금이 12개를 넘지 않게
    chart = alt.Chart(long_df).mark_bar(size=bar_size).encode(
        x=alt.X(
            "period:T", title=None,
            # 눈금은 월 단위로 하나씩, 양 끝 막대가 잘리지 않게 여백을 둔다
            axis=alt.Axis(format="%y-%m", labelAngle=0, tickCount={"interval": "month", "step": tick_step}),
            scale=alt.Scale(padding=bar_size / 2 + 6),
        ),
        y=alt.Y("매출:Q", title="매출액", stack="zero", axis=alt.Axis(format="~s")),
        color=alt.Color(
            "구분:N", title=None,
            scale=alt.Scale(domain=list(TYPES), range=[COLORS[t] for t in TYPES]),
            legend=alt.Legend(orient="top", direction="horizontal"),  # 막대와 겹치지 않게 차트 위에
        ),
        order=alt.Order("구분:N", sort="descending"),
        opacity=alt.condition(alt.datum.in_progress, alt.value(0.45), alt.value(0.9)),
        tooltip=[
            alt.Tooltip("period:T", title="구간", format=_TOOLTIP_FORMAT[unit]),
            alt.Tooltip("구분:N", title="구분"),
            alt.Tooltip("매출:Q", title="매출", format=",.0f"),
            alt.Tooltip("합계:Q", title="합계", format=",.0f"),
            alt.Tooltip("상태:N", title="상태"),
        ],
    ).properties(height=height).configure_view(strokeWidth=0)
    st.altair_chart(chart, use_container_width=True)

    unit_word = {"월": "개월", "주": "주", "일": "일"}[unit]
    st.caption(
        f"기간: {wide['period'].min():%Y-%m-%d} ~ {wide['period'].max():%Y-%m-%d} ({len(wide):,}{unit_word}) | "
        f"매출은 부가세 제외 | 흐린 막대: 진행 중인 {unit}"
    )


def render_type_share(summary: dict, height: int = 150) -> None:
    """잔/병 비중 — 매출 기준과 마진 기준 도넛 병기."""
    import altair as alt
    import streamlit as st

    share = build_type_share(summary)
    total_sales, total_margin = float(share["sales"].sum()), float(share["margin"].sum())
    if total_sales <= 0:
        st.info("최근 30일 매출 데이터가 없습니다.")
        return

    def _donut(value_col: str, ratio_col: str, title: str, center_label: str):
        data = share.assign(비중=share[ratio_col].round(1), 금액=share[value_col])
        ring = alt.Chart(data).mark_arc(
            innerRadius=int(height * 0.29), outerRadius=int(height * 0.45), cornerRadius=2,
        ).encode(
            theta=alt.Theta(f"{value_col}:Q", stack=True),
            color=alt.Color(
                "sales_type:N", title=None,
                scale=alt.Scale(domain=list(TYPES), range=[COLORS[t] for t in TYPES]), legend=None,
            ),
            tooltip=[
                alt.Tooltip("sales_type:N", title="구분"),
                alt.Tooltip("금액:Q", title=title, format=",.0f"),
                alt.Tooltip("비중:Q", title="비중(%)", format=".1f"),
                alt.Tooltip("margin_rate:Q", title="마진율(%)", format=".1f"),
            ],
        )
        center = alt.Chart(pd.DataFrame({"t": [center_label]})).mark_text(
            fontSize=15, fontWeight="bold", color="#333"
        ).encode(text="t:N")
        sub = alt.Chart(pd.DataFrame({"t": [title]})).mark_text(fontSize=10, dy=15, color="#777").encode(text="t:N")
        return (ring + center + sub).properties(height=height)

    glass, bottle = share.iloc[0], share.iloc[1]
    c1, c2 = st.columns(2)
    with c1:
        st.altair_chart(
            _donut("sales", "sales_ratio", "매출 기준", f"잔 {glass['sales_ratio']:.0f}%"), use_container_width=True,
        )
    with c2:
        if total_margin > 0:
            st.altair_chart(
                _donut("margin", "margin_ratio", "마진 기준", f"잔 {glass['margin_ratio']:.0f}%"),
                use_container_width=True,
            )
        else:
            st.caption("마진 합계가 0 이하라 마진 기준 비중을 표시하지 않습니다.")
    st.caption(
        f"🟦 잔 판매 매출 {fmt_krw(glass['sales'])} ({glass['sales_ratio']:.1f}%) · 마진 {fmt_krw(glass['margin'])} "
        f"({glass['margin_ratio']:.1f}%) · 마진율 {glass['margin_rate']:.1f}%  \n"
        f"🟧 병 판매 매출 {fmt_krw(bottle['sales'])} ({bottle['sales_ratio']:.1f}%) · 마진 {fmt_krw(bottle['margin'])} "
        f"({bottle['margin_ratio']:.1f}%) · 마진율 {bottle['margin_rate']:.1f}%"
    )


def render_product_ranking(by_product: pd.DataFrame, top_n: int = 10) -> None:
    """매출 상위 상품 가로 막대 (값 직접 표기)."""
    import altair as alt
    import streamlit as st

    data = by_product[by_product["sales"] > 0].head(top_n)
    if data.empty:
        st.info("표시할 매출 데이터가 없습니다.")
        return
    base = alt.Chart(data).encode(
        y=alt.Y("product_name:N", title=None, sort="-x", axis=alt.Axis(labelLimit=220)),
        x=alt.X("sales:Q", title="매출액", axis=alt.Axis(format="~s", tickCount=5)),
        tooltip=[
            alt.Tooltip("product_name:N", title="상품"),
            alt.Tooltip("sales:Q", title="매출", format=",.0f"),
            alt.Tooltip("margin:Q", title="마진", format=",.0f"),
            alt.Tooltip("margin_rate:Q", title="마진율(%)", format=".1f"),
        ],
    )
    bars = base.mark_bar(color=COLORS["잔 판매"], cornerRadiusEnd=4, height=18)
    labels = base.mark_text(align="left", dx=4, color="#333").encode(text=alt.Text("sales:Q", format=",.0f"))
    # 상품 한 줄당 높이를 고정 (상품 수가 적어도 막대가 겹치지 않게)
    chart = (bars + labels).properties(height=alt.Step(32)).configure_view(strokeWidth=0)
    st.altair_chart(chart, use_container_width=True)


# =========================================================
# 공통 조각
# =========================================================
def _metric(column, label: str, value: str, caption: str, delta: str | None = None) -> None:
    column.metric(label, value, delta=delta)
    column.caption(caption)


def _render_empty_notice() -> None:
    import streamlit as st

    st.info(
        "아직 등록된 판매 내역이 없습니다. 상품·매입·판매가 등록되면 아래 지표와 차트가 채워집니다. "
        "(지금은 화면 틀만 보입니다)"
    )


def render_stock_alerts(today) -> None:
    """재고 소진 임박 + 장기 미판매 재고 (데일리시가 HOME 과 같은 구성)."""
    import streamlit as st

    outlook = wdb.get_stock_outlook(today, lookback_days=30)

    st.markdown("**⏳ 재고 소진 임박**")
    st.caption(
        f"최근 30일 판매 속도(병·잔 판매 용량) 기준으로 {DEPLETION_THRESHOLD_DAYS}일 이내 소진이 예상되는 위스키 "
        "(발주 시점 판단용)"
    )
    soon = outlook[outlook["days_left"].notna() & (outlook["days_left"] <= DEPLETION_THRESHOLD_DAYS)]
    if soon.empty:
        st.success(f"{DEPLETION_THRESHOLD_DAYS}일 이내 소진이 예상되는 상품이 없습니다.")
    else:
        view = soon.sort_values("days_left").assign(
            sold=lambda d: d["sold_ml"].map(lambda v: f"{v:,.0f}ml"),
            daily=lambda d: d["daily_ml"].map(lambda v: f"{v:,.0f}ml"),
            left=lambda d: d["days_left"].map(lambda v: f"{v:,.0f}일 후"),
        )[["product_name", "stock_text", "sold", "daily", "left"]]
        st.dataframe(
            view.rename(columns={
                "product_name": "상품", "stock_text": "현재고", "sold": "최근 30일 판매",
                "daily": "일평균", "left": "소진 예상",
            }),
            use_container_width=True, hide_index=True,
        )

    st.markdown("")
    st.markdown("**📦 장기 미판매 재고**")
    st.caption(
        f"재고가 있으나 {UNSOLD_THRESHOLD_DAYS}일 이상 판매 이력이 없는 위스키 (판매 이력이 아예 없는 상품 포함)"
    )
    has_stock = outlook[outlook["stock_ml"] > 0]
    unsold = has_stock[has_stock["days_since_sale"].isna() | (has_stock["days_since_sale"] >= UNSOLD_THRESHOLD_DAYS)]
    if unsold.empty:
        st.success(f"{UNSOLD_THRESHOLD_DAYS}일 이상 미판매 재고가 없습니다.")
    else:
        view = unsold.assign(
            last=lambda d: d["last_sale_date"].map(lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "판매이력없음"),
            since=lambda d: d["days_since_sale"].map(lambda v: f"{v:,.0f}일" if pd.notna(v) else "판매이력없음"),
        )[["product_name", "stock_text", "last", "since"]]
        st.dataframe(
            view.rename(columns={
                "product_name": "상품", "stock_text": "현재고", "last": "마지막 판매일", "since": "경과일수",
            }),
            use_container_width=True, hide_index=True,
        )


def render_db_download() -> None:
    import streamlit as st

    st.subheader("DB 다운로드")
    path = wdb.get_db_path()
    if not os.path.exists(path):
        st.error(f"위스키 DB 파일을 찾을 수 없습니다: {path}")
        return
    with open(path, "rb") as f:
        st.download_button(
            "현재 위스키 DB 다운로드", data=f.read(), file_name="whisky.db",
            mime="application/octet-stream", use_container_width=True, key="whisky_db_download",
        )
    st.caption("데일리시가(cigar.db)와 분리된 위스키 전용 파일(whisky.db)입니다.")


# =========================================================
# HOME
# =========================================================
def render_home() -> None:
    import streamlit as st

    st.caption("위스키는 개인사업자 매출이라 데일리시가(법인) 장부와 분리해 whisky.db 에 따로 저장합니다.")

    today = pd.Timestamp.today().normalize()
    last_30_start = today - pd.Timedelta(days=29)
    prior_start, prior_end = today - pd.Timedelta(days=59), today - pd.Timedelta(days=30)

    all_df = wdb.load_sales(date_to=today.strftime("%Y-%m-%d"))
    cur_df = all_df[all_df["dt"] >= last_30_start]
    prev_df = all_df[(all_df["dt"] >= prior_start) & (all_df["dt"] <= prior_end)]
    cur, prev = wdb.summarize_sales(cur_df), wdb.summarize_sales(prev_df)
    stock = wdb.get_stock(active_only=True)

    if all_df.empty:
        _render_empty_notice()

    # 물결표는 마크다운에서 취소선으로 해석되므로 이스케이프
    st.caption(
        f"계산기간: {last_30_start:%Y-%m-%d}\\~{today:%Y-%m-%d}  |  "
        f"증감 비교 대상: 이전 30일 {prior_start:%Y-%m-%d}\\~{prior_end:%Y-%m-%d}"
    )
    k1, k2, k3, k4 = st.columns(4)
    _metric(
        k1, "최근 30일 매출", fmt_krw(cur["sales"]),
        f"잔: {fmt_krw(cur['glass_sales'])}, 병: {fmt_krw(cur['bottle_sales'])}",
        delta=format_delta_pct(cur["sales"], prev["sales"]),
    )
    _metric(
        k2, "최근 30일 마진", fmt_krw(cur["margin"]),
        f"마진율 {cur['margin_rate']:.1f}%  ·  잔: {fmt_krw(cur['glass_margin'])}, 병: {fmt_krw(cur['bottle_margin'])}",
        delta=format_delta_pct(cur["margin"], prev["margin"]),
    )
    _metric(
        k3, "최근 30일 판매량", f"{cur['glass_qty']:,.0f}잔 · {cur['bottle_qty']:,.0f}병",
        f"이전 30일: {prev['glass_qty']:,.0f}잔 · {prev['bottle_qty']:,.0f}병",
    )
    _metric(
        k4, "재고 금액", fmt_krw(stock["stock_value"].sum() if not stock.empty else 0),
        f"상품 {len(stock)}개 · 미개봉 {int(stock['full_bottles'].sum()) if not stock.empty else 0}병 (평균 매입가 기준)",
    )

    st.divider()
    left, right = st.columns([2, 1])
    with left:
        st.subheader("전체 기간 매출 추이")
        render_sales_trend(all_df, today, key="whisky_home_trend_unit")
    with right:
        st.subheader("최근 30일 병·잔 비중")
        render_type_share(cur)

    st.divider()
    render_stock_alerts(today)

    st.divider()
    render_db_download()
    st.caption("왼쪽 사이드바에서 상세 페이지를 선택하세요.")


# =========================================================
# 경영 대시보드
# =========================================================
def render_dashboard() -> None:
    import streamlit as st

    st.subheader("경영 대시보드")
    today = pd.Timestamp.today().normalize()
    default_from = (today - pd.DateOffset(months=11)).replace(day=1)
    c1, c2, _ = st.columns([1, 1, 2])
    date_from = c1.date_input("시작일", value=default_from.date(), key="whisky_dash_from")
    date_to = c2.date_input("종료일", value=today.date(), key="whisky_dash_to")
    if date_from > date_to:
        st.error("시작일이 종료일보다 늦습니다.")
        return

    df = wdb.load_sales(str(date_from), str(date_to))
    total = wdb.summarize_sales(df)
    if df.empty:
        _render_empty_notice()

    k1, k2, k3, k4 = st.columns(4)
    _metric(k1, "매출", fmt_krw(total["sales"]), f"잔: {fmt_krw(total['glass_sales'])}, 병: {fmt_krw(total['bottle_sales'])}")
    _metric(k2, "매출원가", fmt_krw(total["cost"]), "차감 용량 × ml당 평균 매입가")
    _metric(k3, "매출총이익", fmt_krw(total["margin"]), f"마진율 {total['margin_rate']:.1f}%")
    _metric(k4, "손실·서비스 원가", fmt_krw(total["loss"]), "기타 출고·실사 조정 (마진에는 미포함)")
    st.caption("매출은 판매금액(결제금액)에서 부가세를 뺀 금액입니다.")

    tab_month, tab_product, tab_loss = st.tabs(["월별 추이", "상품별", "손실·조정"])

    with tab_month:
        st.markdown("###### 월별 매출 추이")
        render_sales_trend(df, min(today, pd.Timestamp(date_to)), key="whisky_dash_trend_unit", units=("월",))
        by_month = wdb.summarize_by_month(df)
        if not by_month.empty:
            st.markdown("###### 월별 손익")
            st.dataframe(
                by_month.rename(columns={
                    "month": "월", "glass_sales": "잔 매출", "bottle_sales": "병 매출", "sales": "매출",
                    "cost": "매출원가", "margin": "매출총이익", "margin_rate": "마진율(%)", "loss": "손실·서비스 원가",
                }),
                use_container_width=True, hide_index=True,
                column_config={
                    **{c: st.column_config.NumberColumn(format="₩ %,d")
                       for c in ["잔 매출", "병 매출", "매출", "매출원가", "매출총이익", "손실·서비스 원가"]},
                    "마진율(%)": st.column_config.NumberColumn(format="%.1f"),
                },
            )

    with tab_product:
        by_product = wdb.summarize_by_product(df)
        st.markdown("###### 매출액 상위 상품")
        render_product_ranking(by_product)
        if not by_product.empty:
            st.markdown("###### 상품별 손익")
            st.dataframe(
                by_product.rename(columns={
                    "product_name": "상품", "glass_qty": "잔", "bottle_qty": "병", "sales": "매출",
                    "cost": "매출원가", "margin": "매출총이익", "margin_rate": "마진율(%)", "loss": "손실·서비스 원가",
                }),
                use_container_width=True, hide_index=True,
                column_config={
                    "잔": st.column_config.NumberColumn(format="%d"),
                    "병": st.column_config.NumberColumn(format="%d"),
                    **{c: st.column_config.NumberColumn(format="₩ %,d")
                       for c in ["매출", "매출원가", "매출총이익", "손실·서비스 원가"]},
                    "마진율(%)": st.column_config.NumberColumn(format="%.1f"),
                },
            )

    with tab_loss:
        st.caption("서비스·시음·파손 같은 기타 출고와 재고 실사 조정입니다. 매출이 없어 마진 계산에서는 빼고 따로 보여줍니다.")
        loss_df = df[df["sales_type"] == ""]
        if loss_df.empty:
            st.info("해당 기간에 기타 출고·실사 조정이 없습니다.")
        else:
            st.dataframe(
                loss_df[["out_date", "product_name", "out_type_label", "total_ml", "loss_amount"]].rename(columns={
                    "out_date": "날짜", "product_name": "상품", "out_type_label": "유형",
                    "total_ml": "용량(ml)", "loss_amount": "원가",
                }),
                use_container_width=True, hide_index=True,
                column_config={
                    "용량(ml)": st.column_config.NumberColumn(format="%,d"),
                    "원가": st.column_config.NumberColumn(format="₩ %,d"),
                },
            )
