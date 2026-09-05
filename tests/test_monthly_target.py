"""월 목표 진행률 계산 로직 단위 테스트."""
from modules.dashboard.monthly_target import compute_progress


def test_projection_mid_month():
    p = compute_progress(actual=10_000_000, target=30_000_000, elapsed_days=10, total_days=30, month_closed=False)
    assert round(p["rate"]) == 33
    assert p["projected"] == 30_000_000 and round(p["projected_rate"]) == 100
    assert p["remaining"] == 20_000_000 and p["remaining_days"] == 20
    assert p["needed_per_day"] == 1_000_000


def test_closed_month_no_projection():
    p = compute_progress(actual=25_000_000, target=30_000_000, elapsed_days=31, total_days=31, month_closed=True)
    assert p["projected"] == 25_000_000 and p["remaining"] == 5_000_000 and p["remaining_days"] == 0
    assert p["needed_per_day"] == 0


def test_target_zero_or_achieved():
    p = compute_progress(actual=5, target=0, elapsed_days=3, total_days=30, month_closed=False)
    assert p["rate"] is None and p["remaining"] is None
    p = compute_progress(actual=40, target=30, elapsed_days=15, total_days=30, month_closed=False)
    assert p["remaining"] == 0 and p["needed_per_day"] == 0 and p["rate"] > 100
