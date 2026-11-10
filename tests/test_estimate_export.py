"""
견적서 엑셀 생성 옵션 테스트: 부가세 포함/제외, 빈 줄 숨기기.

- 부가세 제외: 부가세 칸은 비우고 합계 = 수량 × 공급단가 (Total 의 부가세 칸도 비움)
- 빈 줄 숨기기: 품목 아래 남는 템플릿 행만 숨기고 합계 행은 보인다
"""
from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import load_workbook

from modules.export import document_export as de

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def sample(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)  # 템플릿 경로가 저장소 기준 상대경로
    return de._build_sample_estimate_data()


def _build(sample, template_type="A4", **kw):
    cigar_df, non_cigar_df, partner_info = sample
    buf = de.build_estimate_workbook(cigar_df, non_cigar_df, partner_info, template_type=template_type, **kw)
    layout = de.ESTIMATE_LAYOUTS[template_type]
    return load_workbook(buf)[layout["sheet_cigar"]], layout, len(cigar_df)


@pytest.mark.parametrize("template_type", ["A4", "A3"])
def test_vat_included_by_default(sample, template_type):
    ws, L, n = _build(sample, template_type)
    r = L["data_start_row"]
    assert str(ws[f"{L['col_vat']}{r}"].value).startswith("=ROUND(")
    assert L["col_vat"] in str(ws[f"{L['col_total']}{r}"].value)
    assert ws[f"{L['col_vat']}{L['total_row']}"].value


@pytest.mark.parametrize("template_type", ["A4", "A3"])
def test_vat_excluded(sample, template_type):
    ws, L, n = _build(sample, template_type, include_vat=False)
    q, sp, v, t = L["col_qty"], L["col_supply"], L["col_vat"], L["col_total"]
    for r in range(L["data_start_row"], L["data_start_row"] + n):
        assert ws[f"{v}{r}"].value is None
        assert ws[f"{t}{r}"].value == f"={q}{r}*{sp}{r}"
    assert ws[f"{v}{L['total_row']}"].value is None
    assert str(ws[f"{t}{L['total_row']}"].value).startswith("=SUM(")


@pytest.mark.parametrize("template_type", ["A4", "A3"])
def test_hide_empty_rows(sample, template_type):
    ws, L, n = _build(sample, template_type, hide_empty_rows=True)
    start, end = L["data_start_row"], L["data_end_row"]
    assert not any(ws.row_dimensions[r].hidden for r in range(start, start + n))
    assert all(ws.row_dimensions[r].hidden for r in range(start + n, end + 1))
    assert not ws.row_dimensions[L["total_row"]].hidden

    ws2, _, _ = _build(sample, template_type)
    assert not any(ws2.row_dimensions[r].hidden for r in range(start, end + 1))


def _amount_cell(sample, template_type="A4", **kw):
    ws, L, _ = _build(sample, template_type, **kw)
    return ws[L["amount_text_cell"]]


@pytest.mark.parametrize("template_type", ["A4", "A3"])
def test_amount_text_red_without_discount(sample, template_type):
    cigar_df = sample[0]
    total = int((cigar_df["qty"] * cigar_df["supply_price_krw"]).sum())
    cell = _amount_cell(sample, template_type)
    assert f"{total:,} 원 (V.A.T. 제외)" in cell.value
    assert "할인" not in cell.value
    assert cell.font.color.rgb == de.ESTIMATE_AMOUNT_FONT_COLOR


@pytest.mark.parametrize("template_type", ["A4", "A3"])
def test_amount_text_shows_grade_discounted_amount(sample, template_type):
    cigar_df, non_cigar_df, partner_info = sample
    info = {**partner_info, "grade_code": "SILVER", "estimate_discount_rate": 0.05}
    total = int((cigar_df["qty"] * cigar_df["supply_price_krw"]).sum())
    discounted = int(total * 0.95 + 0.5)

    cell = _amount_cell((cigar_df, non_cigar_df, info), template_type, show_grade_discount=True)
    assert f"{discounted:,} 원 (V.A.T. 제외)" in cell.value
    assert cell.font.color.rgb == de.ESTIMATE_AMOUNT_FONT_COLOR

    # 등급 표기를 끄면 할인 전 금액
    cell_off = _amount_cell((cigar_df, non_cigar_df, info), template_type)
    assert f"{total:,} 원 (V.A.T. 제외)" in cell_off.value

    # 시가 외 시트는 할인 대상이 아님
    buf = de.build_estimate_workbook(cigar_df, non_cigar_df, info, template_type=template_type,
                                     show_grade_discount=True)
    L = de.ESTIMATE_LAYOUTS[template_type]
    nc_df = non_cigar_df
    nc_total = int((nc_df["qty"] * nc_df["supply_price_krw"]).sum())
    nc_cell = load_workbook(buf)[L["sheet_non_cigar"]][L["amount_text_cell"]]
    assert f"{nc_total:,} 원 (V.A.T. 제외)" in nc_cell.value
