"""홈 화면 '요일별 평균 매출' 검증."""
from __future__ import annotations

import pandas as pd

from modules.dashboard.weekday_pattern import DEFAULT_LOOKBACK_DAYS, DOW_KR, weekday_pattern


def _df(rows):
    return pd.DataFrame(rows, columns=["dt", "sales_type", "sales_amount"]).assign(
        dt=lambda d: pd.to_datetime(d["dt"])
    )


TODAY = pd.Timestamp("2026-09-05")  # 토요일


def test_window_is_13_weeks_ending_yesterday():
    _, info = weekday_pattern(_df([]), TODAY)
    assert info["date_to"] == pd.Timestamp("2026-09-04")            # 오늘(집계 중)은 제외
    assert info["date_from"] == pd.Timestamp("2026-06-06")
    assert info["lookback_days"] == DEFAULT_LOOKBACK_DAYS
    t, _ = weekday_pattern(_df([]), TODAY)
    assert t["days_total"].tolist() == [13] * 7                     # 91일 = 각 요일 13번
    assert list(t["요일"]) == DOW_KR


def test_average_uses_open_days_not_calendar_days():
    # 월요일 3번 중 1번만 영업(60만) → 영업일 평균 60만, 전체일 평균은 그보다 낮다
    rows = [("2026-06-08", "소매", 600_000)]           # 월
    rows += [("2026-06-09", "소매", 100_000), ("2026-06-16", "소매", 300_000)]  # 화 2회
    t, info = weekday_pattern(_df(rows), TODAY)
    mon = t.set_index("요일").loc["월"]
    tue = t.set_index("요일").loc["화"]
    assert mon["days_open"] == 1 and mon["days_closed"] == 12
    assert mon["avg_open"] == 600_000
    assert mon["avg_all"] == 600_000 / 13
    assert tue["days_open"] == 2 and tue["avg_open"] == 200_000
    # 전체 평균 = 총매출 / 총영업일 (3일)
    assert info["total_open"] == 3
    assert info["overall_avg"] == 1_000_000 / 3
    assert info["best"]["요일"] == "월" and info["worst"]["요일"] == "화"


def test_same_day_lines_are_summed_and_wholesale_excluded():
    rows = [
        ("2026-08-01", "소매", 100), ("2026-08-01", "소매", 50),   # 같은 토요일 두 라인 → 150
        ("2026-08-01", "도매", 9_999_999),                          # 도매는 제외
    ]
    t, _ = weekday_pattern(_df(rows), TODAY)
    sat = t.set_index("요일").loc["토"]
    assert sat["days_open"] == 1 and sat["sales_sum"] == 150 and sat["avg_open"] == 150


def test_out_of_window_rows_ignored():
    rows = [
        ("2026-06-05", "소매", 111),   # 기간 시작 하루 전
        ("2026-09-05", "소매", 222),   # 오늘 (집계 중)
        ("2026-07-01", "소매", 333),   # 기간 내 (수)
    ]
    t, info = weekday_pattern(_df(rows), TODAY)
    assert t["sales_sum"].sum() == 333 and info["total_open"] == 1


def test_empty_input():
    t, info = weekday_pattern(pd.DataFrame(columns=["dt", "sales_type", "sales_amount"]), TODAY)
    assert not info["has_data"] and info["best"] is None and info["worst"] is None
    assert t["avg_open"].sum() == 0 and t["days_open"].sum() == 0


def test_matches_real_db_retail_totals(conn):
    """실제 DB: 요일별 합계의 총합 = 같은 기간 정본 소매 매출 합계."""
    import DAILY_CIGAR as home
    from modules.common import sales_query as sq

    today = pd.Timestamp("2026-09-05")
    df = home.load_period_sales(conn, "2000-01-01", today.strftime("%Y-%m-%d"))
    t, info = weekday_pattern(df, today)

    r = sq.load_retail_sales(conn, info["date_from"].strftime("%Y-%m-%d"), info["date_to"].strftime("%Y-%m-%d"))
    assert abs(t["sales_sum"].sum() - float(r["net_sales_amount"].sum())) <= 1.0
    # 영업일 수 = 판매 기록이 있는 날짜 수
    assert info["total_open"] == r["sale_date"].nunique()
