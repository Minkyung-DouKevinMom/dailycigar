"""
기준정보 > 데이터 정합성 점검.

tests/ 의 검증 로직을 화면으로 옮긴 것. 코드가 아니라 "데이터"가 어긋난 지점을 버튼 한 번에 찾는다.
각 점검은 (설명, 조치 안내, 문제 행 DataFrame) 을 반환하고, 0건이면 정상으로 표시한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd
import streamlit as st

from db import get_gift_package_cost_map, get_non_cigar_purchase_price_map
from modules.common.dbutil import table_exists, view_exists
from modules.common.sales_query import load_retail_sales, load_wholesale_sales

TOL = 0.5


@dataclass
class CheckResult:
    key: str
    title: str
    description: str
    action: str
    issues: pd.DataFrame
    severity: str = "error"  # error(반드시 조치) / warn(확인 권장) / info(참고)


# ─────────────────────────── 개별 점검 ───────────────────────────

def check_price_master_vs_import(conn) -> CheckResult:
    """가격마스터(product_price_mst) 와 최신 수입품목(v_import_item_latest_price) 가격 불일치."""
    cols = ["상품코드", "상품명", "항목", "가격마스터", "최신 수입품목", "차이"]
    if not (table_exists(conn, "product_price_mst") and view_exists(conn, "v_import_item_latest_price")):
        return CheckResult("price", "가격마스터 vs 수입품목", "", "", pd.DataFrame(columns=cols))
    df = pd.read_sql_query(
        """
        SELECT m.product_code, m.product_name, m.size_name,
               m.supply_price_krw AS m_supply, l.supply_price_krw AS l_supply,
               m.retail_price_krw AS m_retail, l.retail_price_krw AS l_retail,
               m.store_retail_price_krw AS m_store, l.store_retail_price_krw AS l_store
        FROM product_price_mst m
        JOIN v_import_item_latest_price l ON l.product_code = m.product_code
        WHERE COALESCE(m.is_active,1)=1
        """,
        conn,
    )
    rows = []
    for _, r in df.iterrows():
        for label, a, b in [("공급가", r.m_supply, r.l_supply), ("소비자가", r.m_retail, r.l_retail), ("매장운영가", r.m_store, r.l_store)]:
            if pd.notna(a) and pd.notna(b) and abs(float(a) - float(b)) > TOL:
                rows.append([r.product_code, f"{r.product_name} {r.size_name}", label, float(a), float(b), float(a) - float(b)])
    return CheckResult(
        "price", "가격마스터 vs 최신 수입품목 가격 불일치",
        "가격마스터에 저장된 공급가/소비자가/매장운영가가 가장 최근 수입 배치의 값과 다릅니다. "
        "수입품목에서 가격을 고친 뒤 마스터가 갱신되지 않았거나, 마스터를 직접 수정한 경우입니다.",
        "어느 쪽이 맞는지 확인 후 수입관리 > 수입품목 저장(마스터 자동 갱신) 또는 기준정보 > 가격 마스터에서 정정.",
        pd.DataFrame(rows, columns=cols), "warn",
    )


def check_codes_without_cost_basis(conn) -> CheckResult:
    """판매된 상품코드 중 원가를 계산할 근거(수입기록 또는 시가 외 마스터)가 없는 코드."""
    cols = ["상품코드", "유형", "판매건수", "판매수량", "매출(부가세 제외)", "최근 판매일"]
    r = load_retail_sales(conn)
    if r.empty:
        return CheckResult("cost", "원가 근거 없는 판매 상품코드", "", "", pd.DataFrame(columns=cols))
    imp = set(pd.read_sql_query(
        "SELECT DISTINCT UPPER(TRIM(product_code)) c FROM import_item WHERE COALESCE(product_code,'')<>''", conn)["c"])
    pm = set(pd.read_sql_query("SELECT UPPER(TRIM(product_code)) c FROM product_mst WHERE COALESCE(product_code,'')<>''", conn)["c"])
    nc = {str(k).strip().upper() for k in get_non_cigar_purchase_price_map(conn)}
    r["code_u"] = r["product_code"].str.upper()
    bad = r[(r["code_u"] != "") & ~r["code_u"].isin(imp | nc)]
    if bad.empty:
        return CheckResult("cost", "원가 근거 없는 판매 상품코드", "", "", pd.DataFrame(columns=cols))
    g = bad.groupby("product_code").agg(판매건수=("qty", "size"), 판매수량=("qty", "sum"),
                                       매출=("net_sales_amount", "sum"), 최근=("sale_date", "max")).reset_index()
    g["유형"] = g["product_code"].str.upper().map(lambda c: "시가(마스터 있음, 수입기록 없음)" if c in pm else "마스터 미등록")
    g = g.rename(columns={"product_code": "상품코드", "매출": "매출(부가세 제외)", "최근": "최근 판매일"})
    return CheckResult(
        "cost", "원가 근거 없는 판매 상품코드",
        "이 코드들의 판매는 매출에는 잡히지만 원가가 0으로 계산되어 이익이 과대 표시됩니다.",
        "시가 외 상품이면 기준정보 > 시가 외 상품에 매입가와 함께 등록, 시가면 수입관리에 수입품목 등록.",
        g[cols], "error",
    )


def check_wholesale_non_cigar_zero_cost(conn) -> CheckResult:
    cols = ["ID", "판매일", "거래처", "상품명", "수량", "공급가", "원가", "이익", "마스터 매입가"]
    w = load_wholesale_sales(conn)
    pp = {str(k).strip().upper(): v for k, v in get_non_cigar_purchase_price_map(conn).items()}
    bad = w[(w["item_type"] == "non_cigar") & (w["supply_price"] > 0) & (w["unit_cost"] <= 0)].copy()
    bad["마스터 매입가"] = bad["product_code"].str.upper().map(pp).fillna(0)
    out = bad.rename(columns={"id": "ID", "sale_date": "판매일", "partner_name": "거래처", "product_name": "상품명",
                              "qty": "수량", "supply_price": "공급가", "unit_cost": "원가", "gross_profit_krw": "이익"})
    return CheckResult(
        "ws_cost", "원가 0으로 등록된 시가 외 도매 판매",
        "원가가 0이면 공급가액 전체가 이익으로 잡힙니다.",
        "도매관리 > 도매판매관리에서 해당 건의 원가를 매입가로 수정.",
        out[cols] if not out.empty else pd.DataFrame(columns=cols), "error",
    )


def check_wholesale_profit_formula(conn) -> CheckResult:
    cols = ["ID", "판매일", "거래처", "상품코드", "수량", "공급가", "원가", "저장된 이익", "계산 이익"]
    w = load_wholesale_sales(conn)
    w["계산 이익"] = w["qty"] * (w["supply_price"] - w["unit_cost"])
    bad = w[(w["gross_profit_krw"] - w["계산 이익"]).abs() > TOL]
    out = bad.rename(columns={"id": "ID", "sale_date": "판매일", "partner_name": "거래처", "product_code": "상품코드",
                              "qty": "수량", "supply_price": "공급가", "unit_cost": "원가", "gross_profit_krw": "저장된 이익"})
    return CheckResult(
        "ws_formula", "도매 이익 저장값 ≠ 수량×(공급가−원가)",
        "등록 후 공급가나 원가만 수정되어 이익이 재계산되지 않은 경우입니다.",
        "도매판매관리에서 해당 건을 다시 저장하면 이익이 재계산됩니다.",
        out[cols] if not out.empty else pd.DataFrame(columns=cols), "error",
    )


SERVICE_CATEGORIES = {"사이드", "금액결제", "서비스"}  # 매입 원가가 없는 것이 정상인 카테고리


def check_non_cigar_master_gaps(conn) -> CheckResult:
    """시가 외 마스터 중 매입가가 0인 상품 (서비스성 카테고리·구성품 있는 기프트패키지 제외)."""
    cols = ["상품코드", "상품명", "카테고리", "매입가", "도매가", "소매가", "판매수량(전체)"]
    if not table_exists(conn, "non_cigar_product_mst"):
        return CheckResult("nc_master", "시가 외 마스터 매입가 누락", "", "", pd.DataFrame(columns=cols))
    df = pd.read_sql_query(
        "SELECT TRIM(product_code) product_code, product_name, COALESCE(product_category,'') product_category, "
        "COALESCE(purchase_price,0) purchase_price, COALESCE(wholesale_price,0) wholesale_price, COALESCE(retail_price,0) retail_price "
        "FROM non_cigar_product_mst WHERE COALESCE(is_active,1)=1 AND COALESCE(purchase_price,0) <= 0", conn)
    gift_codes = {str(k).strip().upper() for k in get_gift_package_cost_map(conn)}
    df = df[~df["product_category"].isin(SERVICE_CATEGORIES) & ~df["product_code"].str.upper().isin(gift_codes)]
    if df.empty:
        return CheckResult("nc_master", "시가 외 마스터 매입가 누락", "", "", pd.DataFrame(columns=cols))
    r = load_retail_sales(conn)
    sold = r.assign(code_u=r["product_code"].str.upper()).groupby("code_u")["qty"].sum() if not r.empty else pd.Series(dtype=float)
    df["판매수량(전체)"] = df["product_code"].str.upper().map(sold).fillna(0)
    out = df.rename(columns={"product_code": "상품코드", "product_name": "상품명", "product_category": "카테고리",
                             "purchase_price": "매입가", "wholesale_price": "도매가", "retail_price": "소매가"})
    return CheckResult(
        "nc_master", "시가 외 마스터 매입가 누락 (이익 과대 위험)",
        "매입가가 0이면 이 상품의 이익이 매출 전체로 계산됩니다. (서비스성 카테고리와 구성품이 등록된 기프트패키지는 제외)",
        "기준정보 > 시가 외 상품에서 매입가 입력. 기프트패키지는 구성품을 등록하면 구성품 원가로 계산됩니다. 안 쓰는 상품은 사용안함 처리.",
        out[cols], "warn",
    )


def check_code_case_mismatch(conn) -> CheckResult:
    cols = ["마스터 코드", "판매 데이터 코드", "구분"]
    rows = []
    if table_exists(conn, "retail_sales"):
        sold = set(pd.read_sql_query("SELECT DISTINCT TRIM(product_code) c FROM retail_sales WHERE COALESCE(product_code,'')<>''", conn)["c"])
        sold_u = {c.upper(): c for c in sold}
        for tbl, label in [("non_cigar_product_mst", "시가 외"), ("product_mst", "시가")]:
            if not table_exists(conn, tbl):
                continue
            for c in pd.read_sql_query(f"SELECT TRIM(product_code) c FROM {tbl} WHERE COALESCE(product_code,'')<>''", conn)["c"]:
                if c.upper() in sold_u and sold_u[c.upper()] != c:
                    rows.append([c, sold_u[c.upper()], label])
    return CheckResult(
        "case", "상품코드 대소문자 불일치 (마스터 vs 판매 데이터)",
        "판매 데이터는 업로드 시 대문자로 저장되는데 마스터 코드는 원문이라 표기가 다릅니다. "
        "계산 로직은 대소문자를 무시하도록 보정되어 있어 현재 금액 오류는 없지만, 검색·필터에서 헷갈릴 수 있습니다.",
        "여유가 있을 때 마스터 코드를 대문자로 통일하면 근본적으로 해소됩니다.",
        pd.DataFrame(rows, columns=cols), "info",
    )


def check_gift_without_components(conn) -> CheckResult:
    cols = ["상품코드", "상품명", "매입가", "최근 판매일", "판매수량(전체)"]
    if not table_exists(conn, "non_cigar_product_mst"):
        return CheckResult("gift", "구성품 없는 기프트패키지", "", "", pd.DataFrame(columns=cols))
    df = pd.read_sql_query(
        "SELECT TRIM(product_code) code, product_name, COALESCE(purchase_price,0) purchase_price "
        "FROM non_cigar_product_mst WHERE COALESCE(is_active,1)=1 AND product_category LIKE '%기프트%'", conn)
    gift_codes = {str(k).strip().upper() for k in get_gift_package_cost_map(conn)}
    df = df[~df["code"].str.upper().isin(gift_codes)]
    if df.empty:
        return CheckResult("gift", "구성품 없는 기프트패키지", "", "", pd.DataFrame(columns=cols))
    r = load_retail_sales(conn)
    r["code_u"] = r["product_code"].str.upper()
    sold = r.groupby("code_u").agg(last=("sale_date", "max"), qty=("qty", "sum"))
    df["최근 판매일"] = df["code"].str.upper().map(sold["last"]).fillna("-")
    df["판매수량(전체)"] = df["code"].str.upper().map(sold["qty"]).fillna(0)
    out = df.rename(columns={"code": "상품코드", "product_name": "상품명", "purchase_price": "매입가"})
    return CheckResult(
        "gift", "구성품이 등록되지 않은 기프트패키지",
        "구성품이 없으면 원가를 매입가로 계산하고, 판매 시 구성 시가의 재고 자동 차감도 되지 않습니다.",
        "소매관리 > 기프트패키지 구성 관리에서 구성품 등록.",
        out[cols], "warn",
    )


CHECKS: list[Callable] = [
    check_codes_without_cost_basis,
    check_wholesale_non_cigar_zero_cost,
    check_wholesale_profit_formula,
    check_price_master_vs_import,
    check_non_cigar_master_gaps,
    check_gift_without_components,
    check_code_case_mismatch,
]


def run_all_checks(conn) -> list[CheckResult]:
    results = []
    for fn in CHECKS:
        try:
            results.append(fn(conn))
        except Exception as e:  # 한 점검이 실패해도 나머지는 계속
            results.append(CheckResult(fn.__name__, fn.__name__, f"점검 실행 오류: {e}", "", pd.DataFrame(), "error"))
    return results


# ─────────────────────────── 화면 ───────────────────────────

_ICON = {"error": "🔴", "warn": "🟠", "info": "🔵"}


def render():
    from modules.common.dbutil import get_conn

    st.subheader("데이터 정합성 점검")
    st.caption("매출/이익 계산에 영향을 주는 데이터 불일치를 한 번에 찾습니다. 코드 변경 시 자동 실행되는 테스트(tests/)와 같은 기준입니다.")

    conn = get_conn()
    try:
        results = run_all_checks(conn)
    finally:
        conn.close()

    n_err = sum(len(r.issues) for r in results if r.severity == "error")
    n_warn = sum(len(r.issues) for r in results if r.severity == "warn")
    n_info = sum(len(r.issues) for r in results if r.severity == "info")
    c1, c2, c3 = st.columns(3)
    c1.metric("🔴 반드시 조치", f"{n_err:,}건")
    c2.metric("🟠 확인 권장", f"{n_warn:,}건")
    c3.metric("🔵 참고", f"{n_info:,}건")
    if n_err == 0 and n_warn == 0:
        st.success("금액 계산에 영향을 주는 불일치가 없습니다.")

    for r in results:
        n = len(r.issues)
        label = f"{_ICON.get(r.severity, '')} {r.title} — {n:,}건" if n else f"✅ {r.title} — 정상"
        with st.expander(label, expanded=(n > 0 and r.severity == "error")):
            if r.description:
                st.write(r.description)
            if n:
                st.dataframe(r.issues, use_container_width=True, hide_index=True)
                if r.action:
                    st.info("조치: " + r.action)
            else:
                st.caption("문제 없음")
