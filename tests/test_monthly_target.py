"""월 목표 진행률 계산 로직 단위 테스트."""
from modules.dashboard.monthly_target import compute_progress, progress_bar_ratio


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


def test_progress_bar_ratio_clamped_to_valid_range():
    """실적이 큰 적자(예: 임시로 큰 지출을 등록한 달)라 rate가 크게 음수여도,
    st.progress()에 넘길 값은 항상 [0.0, 1.0] 안에 있어야 한다.
    (실제로 rate=-253% 상황에서 min()만 걸려 있어 StreamlitValueOutOfRangeError가 났던 버그의 회귀 테스트)
    """
    p = compute_progress(actual=-7_593_225, target=3_000_000, elapsed_days=5, total_days=30, month_closed=False)
    assert p["rate"] < 0
    ratio = progress_bar_ratio(p["rate"])
    assert 0.0 <= ratio <= 1.0
    assert ratio == 0.0

    # 목표 초과 달성(300%)에서도 상한 1.0을 넘지 않아야 한다.
    assert progress_bar_ratio(300) == 1.0
    # 정상 범위는 그대로 비율로 변환된다.
    assert progress_bar_ratio(50) == 0.5


def test_flat_component_keeps_expenses_at_current_level():
    """지출은 남은 기간에 늘지 않는다고 보고, 매출총이익만 환산해야 한다."""
    # 5/30일 경과, 매출총이익 215만, 지출 900만 → 영업이익 -685만
    gross, expense = 2_150_000, 9_000_000
    actual = gross - expense
    p = compute_progress(actual, 3_000_000, elapsed_days=5, total_days=30,
                         month_closed=False, flat_component=-expense)
    assert p["projected"] == gross / 5 * 30 - expense          # = +390만
    assert p["projected"] > 0 and p["rate"] < 0                # 현재는 적자지만 월말 예상은 흑자
    # flat_component 를 주지 않으면 예전처럼 적자가 그대로 6배로 커진다
    old = compute_progress(actual, 3_000_000, 5, 30, False)
    assert old["projected"] == actual / 5 * 30 and old["projected"] < p["projected"]


def test_flat_component_ignored_when_month_closed():
    p = compute_progress(-1000, 3000, elapsed_days=30, total_days=30,
                         month_closed=True, flat_component=-5000)
    assert p["projected"] == -1000            # 마감 달은 실적 그대로
