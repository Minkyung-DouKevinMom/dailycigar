"""
재고 소진 임박 알림 (홈 화면). 장기 미판매 재고의 반대 개념 — 발주 시점 판단용.

- 최근 N일(기본 30일) 판매 수량으로 일평균 판매 속도를 구하고, 현재고 ÷ 속도 = 소진 예상일수.
- 판매 속도가 0인 상품(=장기 미판매 쪽)과 재고가 없는 상품은 제외.
- SQL 직접 실행 없음 — 현재고 df 와 판매 df(정본 로더 기반, dt/product_code/qty)를 받아 계산하는 순수 함수.
"""
from __future__ import annotations

import pandas as pd

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_THRESHOLD_DAYS = 90   # 이 일수 안에 소진 예상되는 상품만 알림 (수입 리드타임 감안)
DEFAULT_TOP_N = 5
URGENT_DAYS = 14              # 이 일수 미만이면 긴급 표시

OUT_COLUMNS = [
    "product_code", "product_name", "size_name",
    "current_stock", "qty_sold", "daily_velocity", "days_left", "depletion_date",
]


def calc_stock_depletion(
    stock_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    today: pd.Timestamp,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    threshold_days: int | None = DEFAULT_THRESHOLD_DAYS,
    top_n: int | None = DEFAULT_TOP_N,
) -> pd.DataFrame:
    """
    stock_df: product_code, product_name, size_name, current_stock  (시가 기준 현재고)
    sales_df: dt, product_code, qty  (소매+도매, 기간은 넓어도 됨 — 여기서 최근 N일로 자름)
    반환: OUT_COLUMNS, 소진 예상일수 오름차순 (threshold_days 이내만, top_n 개)
    """
    today = pd.Timestamp(today).normalize()
    if stock_df is None or stock_df.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)

    stock = stock_df.copy()
    stock["current_stock"] = pd.to_numeric(stock["current_stock"], errors="coerce").fillna(0.0)
    stock = stock[stock["current_stock"] > 0]
    if stock.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)

    date_from = today - pd.Timedelta(days=lookback_days - 1)
    sold = pd.Series(dtype=float)
    if sales_df is not None and not sales_df.empty:
        s = sales_df[["dt", "product_code", "qty"]].dropna(subset=["dt"]).copy()
        s["dt"] = pd.to_datetime(s["dt"]).dt.normalize()
        s["qty"] = pd.to_numeric(s["qty"], errors="coerce").fillna(0.0)
        s = s[(s["dt"] >= date_from) & (s["dt"] <= today) & (s["qty"] > 0)]
        if not s.empty:
            sold = s.groupby("product_code")["qty"].sum()

    out = stock.copy()
    out["qty_sold"] = out["product_code"].map(sold).fillna(0.0).astype(float)
    out = out[out["qty_sold"] > 0].copy()
    if out.empty:
        return pd.DataFrame(columns=OUT_COLUMNS)

    out["daily_velocity"] = out["qty_sold"] / lookback_days
    out["days_left"] = out["current_stock"] / out["daily_velocity"]
    out["depletion_date"] = [today + pd.Timedelta(days=float(d)) for d in out["days_left"]]

    if threshold_days is not None:
        out = out[out["days_left"] <= threshold_days]
    out = out.sort_values(["days_left", "current_stock"]).reset_index(drop=True)
    if top_n is not None:
        out = out.head(top_n)

    for c in ["product_name", "size_name"]:
        if c not in out.columns:
            out[c] = ""
    return out[OUT_COLUMNS]


def render_stock_depletion(
    stock_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    today: pd.Timestamp,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    threshold_days: int = DEFAULT_THRESHOLD_DAYS,
    top_n: int = DEFAULT_TOP_N,
) -> None:
    """streamlit 컨텍스트 안에서 호출."""
    import streamlit as st

    st.markdown("**⏳ 재고 소진 임박**")
    st.caption(
        f"최근 {lookback_days}일 판매 속도 기준으로 {threshold_days}일 이내 소진이 예상되는 시가 상위 {top_n}건 "
        f"(발주 시점 판단용 · 판매 이력이 없는 재고는 아래 장기 미판매 재고 참고)"
    )

    df = calc_stock_depletion(
        stock_df, sales_df, today,
        lookback_days=lookback_days, threshold_days=threshold_days, top_n=top_n,
    )
    if df.empty:
        # 임계값 밖이라도 '다음에 소진될 상품'은 알려줘야 섹션이 살아있다
        nxt = calc_stock_depletion(
            stock_df, sales_df, today, lookback_days=lookback_days, threshold_days=None, top_n=1
        )
        if nxt.empty:
            st.success(f"{threshold_days}일 이내 소진이 예상되는 상품이 없습니다.")
        else:
            r = nxt.iloc[0]
            st.success(
                f"{threshold_days}일 이내 소진이 예상되는 상품이 없습니다. "
                f"(가장 이른 소진 예상: {r['product_name']} {r['size_name']} — "
                f"{r['days_left']:,.0f}일 후, {r['depletion_date'].strftime('%Y-%m-%d')})"
            )
        return

    view = pd.DataFrame({
        "": ["🔴" if d < URGENT_DAYS else "🟡" for d in df["days_left"]],
        "상품코드": df["product_code"],
        "상품명": df["product_name"],
        "사이즈": df["size_name"],
        "현재고": df["current_stock"].map(lambda v: f"{v:,.0f}"),
        f"최근 {lookback_days}일 판매": df["qty_sold"].map(lambda v: f"{v:,.0f}"),
        "일평균": df["daily_velocity"].map(lambda v: f"{v:,.2f}"),
        "소진 예상": df["days_left"].map(lambda v: f"{v:,.0f}일 후"),
        "예상 소진일": df["depletion_date"].dt.strftime("%Y-%m-%d"),
    })
    st.dataframe(view, use_container_width=True, hide_index=True)

    urgent = int((df["days_left"] < URGENT_DAYS).sum())
    if urgent:
        st.caption(f"🔴 {URGENT_DAYS}일 이내 소진 예상 {urgent}건 — 발주 검토가 필요합니다.")
