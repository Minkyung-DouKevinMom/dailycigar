"""홈 화면 '전체 기간 매출 추이' 일/주/월 집계 검증."""
from __future__ import annotations

import pandas as pd
import pytest

from modules.common import sales_query as sq
from modules.dashboard.sales_trend import aggregate_trend, period_start


def _df(rows):
    return pd.DataFrame(rows, columns=["dt", "sales_type", "sales_amount"]).assign(dt=lambda d: pd.to_datetime(d["dt"]))


SAMPLE = _df([
    ("2026-07-03", "소매", 100), ("2026-07-20", "도매", 50),
    ("2026-08-10", "소매", 200),
    ("2026-09-01", "소매", 30), ("2026-09-04", "도매", 70),
    ("2026-09-06", "소매", 99999),  # 오늘(9/5) 이후 → 무시
])
TODAY = pd.Timestamp("2026-09-05")


def test_month_aggregation_totals_and_in_progress():
    w = aggregate_trend(SAMPLE, "월", TODAY)
    assert list(w["period"].dt.strftime("%Y-%m")) == ["2026-07", "2026-08", "2026-09"]
    assert w.set_index(w["period"].dt.month).loc[7, ["소매", "도매"]].tolist() == [100, 50]
    assert w.set_index(w["period"].dt.month).loc[8, ["소매", "도매"]].tolist() == [200, 0]
    assert w.set_index(w["period"].dt.month).loc[9, ["소매", "도매"]].tolist() == [30, 70]
    assert w["in_progress"].tolist() == [False, False, True]
    # 합계 보존 (미래 행 제외)
    assert w["합계"].sum() == 450


def test_week_starts_on_monday_and_fills_gaps():
    w = aggregate_trend(SAMPLE, "주", TODAY)
    assert (w["period"].dt.dayofweek == 0).all()
    # 7/3(금) 이 속한 주의 월요일 = 6/29, 오늘(9/5 토) 이 속한 주 = 8/31
    assert w["period"].iloc[0] == pd.Timestamp("2026-06-29")
    assert w["period"].iloc[-1] == pd.Timestamp("2026-08-31")
    assert w["in_progress"].sum() == 1 and w["in_progress"].iloc[-1]
    assert w["합계"].sum() == 450
    # 연속 주 (빈 주 0 으로 채움)
    assert (w["period"].diff().dropna() == pd.Timedelta(days=7)).all()


def test_day_aggregation_only_today_in_progress():
    w = aggregate_trend(SAMPLE, "일", TODAY)
    assert w["period"].iloc[0] == pd.Timestamp("2026-07-03") and w["period"].iloc[-1] == TODAY
    assert w["in_progress"].sum() == 1 and w.loc[w["in_progress"], "period"].iloc[0] == TODAY
    assert w["합계"].sum() == 450


def test_trend_excludes_in_progress_and_needs_two_points():
    w = aggregate_trend(SAMPLE, "월", TODAY)
    # 소매: 7,8월 두 마감 구간으로 추세 → 마감 구간에만 값, 진행 중(9월) 은 NaN
    assert w["소매추세"].notna().tolist() == [True, True, False]
    # 도매: 첫 매출(7월) 이후 마감 구간 7,8월(8월은 0) 로 회귀 → 9월(진행 중) 제외
    assert w["도매추세"].notna().tolist() == [True, True, False]
    assert w["도매추세"].min() >= 0  # 0 아래로 내려가지 않음

    # 매출이 나중에 시작된 채널: 시작 전 구간에는 추세선을 그리지 않는다
    late = _df([("2026-05-10", "소매", 10), ("2026-06-10", "소매", 10),
                ("2026-07-10", "도매", 100), ("2026-08-10", "도매", 200)])
    w2 = aggregate_trend(late, "월", TODAY)
    assert w2["도매추세"].notna().tolist() == [False, False, True, True, False]


def test_invalid_unit():
    with pytest.raises(ValueError):
        aggregate_trend(SAMPLE, "년", TODAY)


def test_period_start_scalar():
    assert period_start(pd.Timestamp("2026-09-05"), "월") == pd.Timestamp("2026-09-01")
    assert period_start(pd.Timestamp("2026-09-05"), "주") == pd.Timestamp("2026-08-31")
    assert period_start(pd.Timestamp("2026-09-05 13:00"), "일") == pd.Timestamp("2026-09-05")


def test_home_trend_matches_canonical_loader(conn, months):
    """실제 DB: 월 집계의 각 월 소매/도매 = 정본 로더 월매출."""
    import DAILY_CIGAR as home
    from modules.common.dates import month_range

    last_y, last_m = months[-1]
    today = pd.Timestamp(month_range(last_y, last_m)[1])
    df = home.load_period_sales(conn, "2000-01-01", today.strftime("%Y-%m-%d"))
    w = aggregate_trend(df, "월", today).set_index("period")

    for y, m in months:
        f, t = month_range(y, m)
        r = sq.load_retail_sales(conn, f, t)["net_sales_amount"].sum()
        wh = sq.load_wholesale_sales(conn, f, t)["net_sales_amount"].sum()
        row = w.loc[pd.Timestamp(f)]
        assert abs(row["소매"] - r) <= 1.0, f"{y}-{m} 소매 {row['소매']:,.0f} ≠ {r:,.0f}"
        assert abs(row["도매"] - wh) <= 1.0, f"{y}-{m} 도매 {row['도매']:,.0f} ≠ {wh:,.0f}"
