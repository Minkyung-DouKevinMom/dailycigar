/**
 * build_pie_report.js
 * compute_pie_data.py가 만든 pie_data.json을 읽어 월간 브랜드 분석 PPT를 생성한다.
 *
 * 사용법:
 *   node build_pie_report.js [pie_data.json경로] [출력pptx경로]
 *
 * 기본값:
 *   pie_data.json경로 = ./pie_data.json
 *   출력pptx경로       = ./daily_cigar_brand_report_<오늘날짜>.pptx
 */
const pptxgen = require("pptxgenjs");
const fs = require("fs");

const JSON_PATH = process.argv[2] || "pie_data.json";
const today = new Date().toISOString().slice(0, 10).replace(/-/g, "");
const OUT_PATH = process.argv[3] || `daily_cigar_brand_report_${today}.pptx`;

const data = JSON.parse(fs.readFileSync(JSON_PATH, "utf-8"));
const ins = data.insights;

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";

const NAVY = "1F2A44";
const GRAY_TEXT = "5B6472";
const LIGHT_BG = "F7F8FA";

function fmtKrwShort(v) {
  const sign = v < 0 ? "-" : "";
  v = Math.abs(v);
  if (v >= 1e8) return sign + (v / 1e8).toFixed(2) + "억";
  if (v >= 1e4) return sign + Math.round(v / 1e4).toLocaleString() + "만";
  return sign + Math.round(v).toLocaleString();
}
function fmtQty(v) {
  return Math.round(v).toLocaleString() + "개";
}

// ── 표지 슬라이드 ──
const cover = pres.addSlide();
cover.background = { color: "FFFFFF" };
cover.addText("데일리시가 브랜드 분석 리포트", {
  x: 0.9, y: 1.55, w: 11.5, h: 0.9,
  fontFace: "Malgun Gothic", fontSize: 34, bold: true, color: NAVY,
});
cover.addText("시가 상품 매출 · 이익 · 판매수량 비중 (전체 기간, 소매+도매 통합)", {
  x: 0.9, y: 2.45, w: 11.5, h: 0.5,
  fontFace: "Malgun Gothic", fontSize: 16, color: GRAY_TEXT,
});
cover.addText(`집계 기간: ${ins.period_from} ~ ${ins.period_to} 누적`, {
  x: 0.9, y: 3.0, w: 11.5, h: 0.4,
  fontFace: "Malgun Gothic", fontSize: 13, color: GRAY_TEXT,
});
cover.addShape(pres.ShapeType.rect, {
  x: 0.9, y: 0.5, w: 0.55, h: 0.55, fill: { color: NAVY }, line: { type: "none" },
});

// ── 한줄평 (매달 자동 계산된 인사이트) ──
cover.addShape(pres.ShapeType.rect, {
  x: 0.9, y: 3.85, w: 11.5, h: 1.15,
  fill: { color: LIGHT_BG }, line: { type: "none" },
});
cover.addText(
  [
    { text: `매출 1위는 ${ins.sales_top1_code}(${ins.sales_top1_pct}%), 이익 1위는 ${ins.profit_top1_code}(${ins.profit_top1_pct}%), `, options: { color: NAVY } },
    { text: `판매수량 1위는 ${ins.qty_top1_code}(${ins.qty_top1_pct}%)`, options: { bold: true, color: NAVY } },
    { text: `입니다. 상위 5개 상품이 전체 매출의 `, options: { color: NAVY } },
    { text: `${ins.sales_top5_share}%`, options: { bold: true, color: NAVY } },
    { text: `를 차지하며, 전체 평균 마진율은 `, options: { color: NAVY } },
    { text: `${ins.overall_margin_pct}%`, options: { bold: true, color: NAVY } },
    { text: `입니다.`, options: { color: NAVY } },
  ],
  {
    x: 1.2, y: 4.02, w: 10.9, h: 0.85,
    fontFace: "Malgun Gothic", fontSize: 13, valign: "middle", lineSpacingMultiple: 1.25,
  }
);

// ── 제안 (인사이트 지표 기반 자동 제안) ──
if (Array.isArray(ins.recommendations) && ins.recommendations.length) {
  cover.addText("제안", {
    x: 0.9, y: 5.15, w: 11.5, h: 0.32,
    fontFace: "Malgun Gothic", fontSize: 14, bold: true, color: NAVY,
  });
  cover.addText(
    ins.recommendations.map((r) => ({
      text: r,
      options: { bullet: { code: "2022" }, breakLine: true, color: GRAY_TEXT },
    })),
    {
      x: 1.1, y: 5.5, w: 11.1, h: 0.95,
      fontFace: "Malgun Gothic", fontSize: 11, valign: "top", lineSpacingMultiple: 1.25,
    }
  );
}

// ── 차트 슬라이드 (세로 막대, 기존 파이차트 색상 그대로 유지) ──
function addBarSlide(title, subtitle, records, valueLabelFmt) {
  const slide = pres.addSlide();
  slide.background = { color: "FFFFFF" };

  slide.addText(title, {
    x: 0.6, y: 0.35, w: 12.0, h: 0.55,
    fontFace: "Malgun Gothic", fontSize: 24, bold: true, color: NAVY,
  });
  slide.addText(subtitle, {
    x: 0.6, y: 0.88, w: 12.0, h: 0.4,
    fontFace: "Malgun Gothic", fontSize: 12, color: GRAY_TEXT,
  });

  const labels = records.map(r => r.label);
  const values = records.map(r => r.pct);
  const colors = records.map(r => r.color);

  slide.addChart(pres.ChartType.bar, [
    { name: valueLabelFmt.name, labels, values },
  ], {
    x: 0.5, y: 1.35, w: 12.3, h: 5.75,
    barDir: "col",
    barGapWidthPct: 22,
    chartColors: colors,
    showTitle: false,
    showLegend: false,
    valAxisHidden: true,
    valAxisMinVal: 0,
    valAxisMaxVal: Math.max(...values) * 1.18,
    catAxisLabelColor: NAVY,
    catAxisLabelFontFace: "Malgun Gothic",
    catAxisLabelFontSize: records.length > 11 ? 9 : 10.5,
    catAxisLabelRotate: 30,
    catAxisLineShow: true,
    showValue: true,
    dataLabelPosition: "outEnd",
    dataLabelFormatCode: '0.0"%"',
    dataLabelColor: NAVY,
    dataLabelFontFace: "Malgun Gothic",
    dataLabelFontSize: records.length > 11 ? 9 : 10.5,
    dataLabelFontBold: true,
  });
}

addBarSlide(
  "시가상품별 매출금액 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 추정치 포함 · 상위 14개 상품 + 기타",
  data.sales, { name: "매출금액", fmt: fmtKrwShort }
);
addBarSlide(
  "시가상품별 이익 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 추정치 포함 · 상위 14개 상품 + 기타",
  data.profit, { name: "이익금액", fmt: fmtKrwShort }
);
addBarSlide(
  "시가상품별 판매수량 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 포함 · 상위 14개 상품 + 기타",
  data.qty, { name: "판매수량", fmt: fmtQty }
);

pres.writeFile({ fileName: OUT_PATH }).then(() => {
  console.log(`저장 완료: ${OUT_PATH}`);
});