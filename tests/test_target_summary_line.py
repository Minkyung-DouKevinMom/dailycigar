"""홈 상단 '이번 달 목표 진행률 한 줄 요약' 검증."""
from __future__ import annotations

import pandas as pd

from modules.dashboard.monthly_target import build_target_summary, month_elapsed, status_icon

TARGET = {"target_sales": 15_000_000, "target_operating_profit": 3_000_000, "notes": ""}


def test_month_elapsed():
    assert month_elapsed(2026, 9, pd.Timestamp("2026-09-05")) == (5, 30, False)   # 진행 중
    assert month_elapsed(2026, 8, pd.Timestamp("2026-09-05")) == (31, 31, True)   # 지난 달 = 마감
    assert month_elapsed(2026, 10, pd.Timestamp("2026-09-05")) == (0, 31, False)  # 미래 달
    assert month_elapsed(2026, 2, pd.Timestamp("2026-02-28")) == (28, 28, False)  # 윤년 아닌 2월 말일


def test_status_icon_thresholds():
    assert status_icon(120) == "🟢" and status_icon(100) == "🟢"
    assert status_icon(99.9) == "🟡" and status_icon(80) == "🟡"
    assert status_icon(79.9) == "🔴" and status_icon(-253) == "🔴"
    assert status_icon(None) == "⚪"


def test_summary_rates_and_projection():
    # 30일 중 5일 경과, 매출 250만 → 월말 예상 1,500만 = 목표 100%
    summary = {"total_sales": 2_500_000, "operating_profit": 250_000, "expense_total": 0}
    s = build_target_summary(TARGET, summary, elapsed=5, total_days=30, closed=False)
    assert s["has_target"] and s["elapsed"] == 5 and not s["closed"]

    sales = s["metrics"]["매출"]
    assert round(sales["rate"], 2) == round(2_500_000 / 15_000_000 * 100, 2)
    assert sales["projected"] == 15_000_000 and round(sales["projected_rate"]) == 100
    assert sales["icon"] == "🟢"

    profit = s["metrics"]["영업이익"]
    assert profit["projected"] == 1_500_000 and round(profit["projected_rate"]) == 50
    assert profit["icon"] == "🔴"


def test_deficit_month_does_not_break_summary():
    """임시 대형 지출 등으로 영업이익이 크게 적자여도 요약이 계산돼야 한다."""
    # 지출이 매출총이익보다 훨씬 커서 월말까지 벌어도 못 메우는 경우 → 예상도 적자
    summary = {"total_sales": 2_223_235, "operating_profit": -7_593_225, "expense_total": 9_000_000}
    s = build_target_summary(TARGET, summary, elapsed=5, total_days=30, closed=False)
    p = s["metrics"]["영업이익"]
    assert p["rate"] < 0
    # 매출총이익 140.7만 → 월말 844.6만, 지출 900만 고정 → 예상 약 -55만 (적자지만 -1369% 같은 값은 아님)
    assert -1_000_000 < p["projected"] < 0 and p["icon"] == "🔴"


def test_expenses_are_not_scaled_in_projection():
    """지출은 현재 수준 고정 — 경과일수로 비례 확대하지 않는다."""
    summary = {"total_sales": 3_533_235, "operating_profit": -6_844_858, "expense_total": 9_000_000}
    s = build_target_summary(TARGET, summary, elapsed=5, total_days=30, closed=False)
    p = s["metrics"]["영업이익"]
    gross = summary["operating_profit"] + summary["expense_total"]      # 매출총이익 215.5만
    assert p["projected"] == gross / 5 * 30 - summary["expense_total"]  # ≈ +393만
    assert p["projected"] > 0 and p["icon"] == "🟢"
    assert p["flat_component"] == -summary["expense_total"]
    # 매출 지표에는 고정 성분이 없다
    assert s["metrics"]["매출"]["flat_component"] == 0.0


def test_no_target_and_zero_target():
    assert build_target_summary(None, {"total_sales": 1}, 5, 30, False)["has_target"] is False

    zero = build_target_summary(
        {"target_sales": 0, "target_operating_profit": 0}, {"total_sales": 100, "operating_profit": 10}, 5, 30, False
    )
    assert zero["has_target"]
    assert zero["metrics"]["매출"]["rate"] is None and zero["metrics"]["매출"]["icon"] == "⚪"


def test_closed_month_uses_actual_not_projection():
    summary = {"total_sales": 30_086_744, "operating_profit": 5_000_000, "expense_total": 2_000_000}
    s = build_target_summary(TARGET, summary, elapsed=31, total_days=31, closed=True)
    assert s["metrics"]["매출"]["projected"] == 30_086_744   # 마감 달은 예상 = 실적
    assert s["metrics"]["영업이익"]["projected"] == 5_000_000


def test_summary_matches_dashboard_widget_source(conn, months):
    """홈 요약과 대시보드 위젯이 같은 정본(get_month_summary + compute_progress)을 쓰는지."""
    from modules.dashboard.dashboard_finance_summary import get_month_summary
    from modules.dashboard.monthly_target import compute_progress

    y, m = months[-1]
    summary = get_month_summary(conn, y, m)
    elapsed, total_days, closed = month_elapsed(y, m, pd.Timestamp(f"{y}-{m:02d}-15"))
    s = build_target_summary(TARGET, summary, elapsed, total_days, closed)

    expected = compute_progress(summary["total_sales"], TARGET["target_sales"], elapsed, total_days, closed)
    assert s["metrics"]["매출"]["rate"] == expected["rate"]
    assert s["metrics"]["매출"]["projected"] == expected["projected"]
