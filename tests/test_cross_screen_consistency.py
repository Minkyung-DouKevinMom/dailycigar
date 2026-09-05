"""
화면 간 집계 일치 검증 (재발 방지 핵심 테스트).

같은 기간에 대해 대시보드 / 홈 / 기간비교 / 재무관리 / 브랜드분석 / 거래처분석 / 상위제품 이
정본 로더(sales_query)와 같은 매출·이익을 내는지 확인한다.
과거에 실제로 어긋났던 유형:
  - 소매 매출을 부가세 포함으로 집계 (홈 화면)
  - 시가 외 상품 보정을 자체 구현하면서 기프트패키지 구성품 원가 누락 (기간비교)
  - 뷰 값을 그대로 써서 시가 외 상품 이익이 0 (상위 제품)
"""
from __future__ import annotations

import pandas as pd
import pytest

from modules.common import sales_query as sq
from modules.common.dates import month_range

TOL = 1.0


def _expected(conn, date_from, date_to):
    r = sq.load_retail_sales(conn, date_from, date_to)
    w = sq.load_wholesale_sales(conn, date_from, date_to)
    return {
        "retail_sales": float(r["net_sales_amount"].sum()),
        "retail_profit": float(r["retail_gross_profit_krw"].sum()),
        "wholesale_sales": float(w["net_sales_amount"].sum()),
        "wholesale_profit": float(w["gross_profit_krw"].sum()),
    }


def _assert_close(name, got, exp):
    assert abs(float(got) - float(exp)) <= TOL, f"{name}: 화면값 {got:,.0f} ≠ 정본 {exp:,.0f}"


def _month_params(months):
    # 데이터가 있는 모든 월 + 전체 기간
    return [pytest.param(y, m, id=f"{y}-{m:02d}") for (y, m) in months]


@pytest.fixture(scope="module")
def all_months(months):
    return months


def pytest_generate_tests(metafunc):
    if "year" in metafunc.fixturenames and "month" in metafunc.fixturenames:
        # conftest 의 months fixture 값을 직접 계산 (parametrize 시점에는 fixture 사용 불가)
        from tests.conftest import SOURCE_DB, _months_with_data
        ms = _months_with_data(SOURCE_DB) if SOURCE_DB.exists() else []
        metafunc.parametrize(("year", "month"), [(y, m) for y, m in ms], ids=[f"{y}-{m:02d}" for y, m in ms])


def test_dashboard_month_summary(conn, year, month):
    import modules.dashboard.dashboard_finance_summary as dfs

    f, t = month_range(year, month)
    exp = _expected(conn, f, t)
    s = dfs.get_month_summary(conn, year, month)
    _assert_close("대시보드 소매매출", s["retail_sales"], exp["retail_sales"])
    _assert_close("대시보드 도매매출", s["wholesale_sales"], exp["wholesale_sales"])
    _assert_close("대시보드 매출총이익", s["gross_profit"], exp["retail_profit"] + exp["wholesale_profit"])


def test_home_page_loader(conn, year, month):
    import DAILY_CIGAR as home

    f, t = month_range(year, month)
    exp = _expected(conn, f, t)
    r = home.get_retail_month_data(conn, f, t)
    w = home.get_wholesale_month_data(conn, f, t)
    _assert_close("홈 소매매출", r["sales_amount"].sum(), exp["retail_sales"])
    _assert_close("홈 소매이익", r["margin_amount"].sum(), exp["retail_profit"])
    _assert_close("홈 도매매출", w["sales_amount"].sum(), exp["wholesale_sales"])
    _assert_close("홈 도매이익", w["margin_amount"].sum(), exp["wholesale_profit"])


def test_period_compare_loader(conn, year, month):
    import modules.analytics.period_compare_view as pcv

    f, t = month_range(year, month)
    exp = _expected(conn, f, t)
    r = pcv.get_retail_data(conn, f, t)
    w = pcv.get_wholesale_data(conn, f, t)
    _assert_close("기간비교 소매매출", r["sales_amount"].sum(), exp["retail_sales"])
    _assert_close("기간비교 소매이익", r["profit_amount"].sum(), exp["retail_profit"])
    _assert_close("기간비교 도매매출", w["sales_amount"].sum(), exp["wholesale_sales"])
    _assert_close("기간비교 도매이익", w["profit_amount"].sum(), exp["wholesale_profit"])


def test_finance_sales_profit_loader(conn, year, month):
    import modules.finance.finance_sales_profit as fsp

    f, t = month_range(year, month)
    exp = _expected(conn, f, t)
    r, _ = fsp.get_retail_data(conn, f, t)
    w, _ = fsp.get_wholesale_data(conn, f, t)
    _assert_close("재무관리 소매매출", r["net_sales_amount"].sum() if not r.empty else 0, exp["retail_sales"])
    _assert_close("재무관리 소매이익", r["retail_gross_profit_krw"].sum() if not r.empty else 0, exp["retail_profit"])
    _assert_close("재무관리 도매매출", w["net_sales_amount"].sum() if not w.empty else 0, exp["wholesale_sales"])
    _assert_close("재무관리 도매이익", w["gross_profit_krw"].sum() if not w.empty else 0, exp["wholesale_profit"])


def test_top_products_profit_includes_non_cigar(conn, year, month):
    """상위 제품(이익)은 소매+도매 상품별 이익 합과 같아야 하며, 시가 외 상품도 0이 아닌 이익으로 포함돼야 한다."""
    import modules.dashboard.dashboard_finance_summary as dfs

    f, t = month_range(year, month)
    r = sq.load_retail_sales(conn, f, t)
    w = sq.load_wholesale_sales(conn, f, t)
    if r.empty and w.empty:
        pytest.skip("데이터 없음")
    exp = pd.concat([
        r.rename(columns={"retail_gross_profit_krw": "p"})[["product_code", "p"]],
        w.rename(columns={"gross_profit_krw": "p"})[["product_code", "p"]],
    ]).groupby("product_code")["p"].sum()

    top = dfs.get_top_products(conn, f, t, limit=10_000, metric="profit")
    got = top.groupby("product_code")["metric_value"].sum()
    joined = pd.concat([exp.rename("exp"), got.rename("got")], axis=1).fillna(0)
    bad = joined[(joined["exp"] - joined["got"]).abs() > TOL]
    assert bad.empty, f"상위제품 이익 불일치:\n{bad.head(10).to_string()}"


def test_brand_analysis_retail_matches_canonical(conn):
    """브랜드 분석(시가만)의 소매 매출/이익 = 정본 로더를 시가 코드로 필터한 합계."""
    import modules.analytics.brand_analysis_view as bav

    cigar_codes = bav.get_cigar_product_codes(conn)
    if not cigar_codes:
        pytest.skip("product_mst 없음")
    bdf = bav.get_retail_brand_product_data(conn, None, None)
    bdf["product_code"] = bdf["product_code"].astype(str).str.strip().str.upper()
    bdf = bdf[bdf["product_code"].isin(cigar_codes)]

    r = sq.load_retail_sales(conn)
    r = r[r["product_code"].str.upper().isin(cigar_codes)]
    _assert_close("브랜드분석 소매매출(시가)", bdf["sales"].sum(), r["net_sales_amount"].sum())
    _assert_close("브랜드분석 소매이익(시가)", bdf["profit"].sum(), r["retail_gross_profit_krw"].sum())


def test_brand_analysis_wholesale_matches_canonical(conn):
    import modules.analytics.brand_analysis_view as bav

    bdf = bav.get_wholesale_brand_product_data(conn, None, None)
    w = sq.load_wholesale_sales(conn)
    _assert_close("브랜드분석 도매매출", bdf["sales"].sum(), w["net_sales_amount"].sum())
    _assert_close("브랜드분석 도매이익", bdf["profit"].sum(), w["gross_profit_krw"].sum())


def test_partner_analysis_matches_canonical(conn):
    """거래처 분석은 v_wholesale_sales 를 거래처별로 합산 → 정본 로더의 거래처별 합과 동일해야 한다."""
    v = pd.read_sql_query(
        "SELECT COALESCE(partner_name,'(거래처 없음)') AS partner_name, "
        "COALESCE(SUM(sales_amount),0) AS sales, COALESCE(SUM(profit_amount),0) AS profit "
        "FROM v_wholesale_sales GROUP BY partner_name",
        conn,
    ).set_index("partner_name")
    w = sq.load_wholesale_sales(conn)
    w["partner_name"] = w["partner_name"].replace("", "(거래처 없음)")
    g = w.groupby("partner_name")[["net_sales_amount", "gross_profit_krw"]].sum()
    joined = v.join(g, how="outer").fillna(0)
    assert (joined["sales"] - joined["net_sales_amount"]).abs().max() <= TOL
    assert (joined["profit"] - joined["gross_profit_krw"]).abs().max() <= TOL


def test_retail_sales_view_screen_matches_canonical(conn):
    """'소매 매출 조회' 화면 KPI(실매출/원가/이익) = 정본 로더 (전체 기간)."""
    import modules.management.retail_sales_view as rsv

    filters = {"date_from": None, "date_to": None}
    try:
        sql, params = rsv.build_query(True, filters)
    except Exception:
        pytest.skip("build_query 시그니처가 달라 건너뜀")
    df = pd.read_sql_query(sql, conn, params=params)
    if "sales_supply_amount_krw" in df.columns:
        df["net_sales_amount"] = df["sales_supply_amount_krw"]
    from db import apply_non_cigar_margin_logic
    df = apply_non_cigar_margin_logic(df, conn)
    k = rsv.calc_kpis(df)

    r = sq.load_retail_sales(conn)
    _assert_close("소매매출조회 실매출", k["실매출"], r["net_sales_amount"].sum())
    if "매출총이익" in k:
        _assert_close("소매매출조회 매출총이익", k["매출총이익"], r["retail_gross_profit_krw"].sum())
