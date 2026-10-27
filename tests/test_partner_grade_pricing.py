"""
도매 등급 할인 계산(compute_tiered_order_pricing)이 거래처등급관리 화면의
'다음 등급까지 남은 금액'과 같은 기준(현재 등급 달성 이후 누적액 vs 다음 등급 문턱)으로
동작하는지 확인.

과거 버그: 첫 승급 이후 '현재 등급 문턱 + 리셋 이후 누적액'을 절대 누적값으로 보고
다음 등급 문턱과 비교해, Silver 달성 후 600만원만 더 사도 Gold 할인이 들어갔다.
"""
from __future__ import annotations

import pandas as pd
import pytest

from modules.management import wholesale_management as wm

ORDER_DATE = pd.Timestamp.today().strftime("%Y-%m-%d")


def _progress_by_partner(conn):
    thresholds = wm.load_partner_grade_thresholds(conn).sort_values("min_purchase_amount").reset_index(drop=True)
    partners = wm.load_partners(conn)
    exempt = {
        int(r[0]) for r in conn.execute("SELECT id FROM partner_mst WHERE COALESCE(program_exempt_yn, 0) = 1")
    }
    out = []
    for _, p in partners.iterrows():
        pid = int(p["id"])
        if pid in exempt or not p.get("join_date"):
            continue
        prog = wm.compute_grade_reset_cumulative(conn, pid, ORDER_DATE, str(p["join_date"]), thresholds)
        codes = thresholds["grade_code"].tolist()
        idx = codes.index(prog["grade_code"])
        if idx + 1 >= len(codes):
            continue
        remaining = thresholds["min_purchase_amount"].iloc[idx + 1] - prog["cumulative_since_start"]
        out.append((pid, codes[idx], codes[idx + 1], remaining))
    return out


def _pick(conn, above_lowest: bool):
    for pid, cur, nxt, remaining in _progress_by_partner(conn):
        if (cur != "BRONZE") == above_lowest and remaining > 20_000:
            return pid, cur, nxt, remaining
    pytest.skip("조건에 맞는 거래처가 없습니다.")


@pytest.mark.parametrize("above_lowest", [True, False])
def test_order_below_remaining_stays_in_current_grade(conn, above_lowest):
    pid, cur, _, remaining = _pick(conn, above_lowest)
    amount = remaining - 10_000
    r = wm.compute_tiered_order_pricing(conn, pid, 1, amount, amount, ORDER_DATE)
    assert [s["grade_code"] for s in r["segments"]] == [cur]


@pytest.mark.parametrize("above_lowest", [True, False])
def test_order_crossing_next_threshold_splits_at_remaining(conn, above_lowest):
    pid, cur, nxt, remaining = _pick(conn, above_lowest)
    excess = 100_000
    amount = remaining + excess
    r = wm.compute_tiered_order_pricing(conn, pid, 1, amount, amount, ORDER_DATE)
    segs = [(s["grade_code"], s["amount"]) for s in r["segments"]]
    assert [c for c, _ in segs] == [cur, nxt]
    assert segs[0][1] == pytest.approx(remaining)
    assert segs[1][1] == pytest.approx(excess)
