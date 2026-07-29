/**
 * build_partner_report.js
 * compute_partner_data.py가 만든 partner_data.json을 읽어 월간 거래처 분석 PPT를 생성한다.
 *
 * 사용법:
 *   node build_partner_report.js [partner_data.json경로] [출력pptx경로]
 *
 * 기본값:
 *   partner_data.json경로 = ./partner_data.json
 *   출력pptx경로           = ./daily_cigar_partner_report_<오늘날짜>.pptx
 */
const pptxgen = require("pptxgenjs");
const fs = require("fs");

const JSON_PATH = process.argv[2] || "partner_data.json";
const today = new Date().toISOString().slice(0, 10).replace(/-/g, "");
const OUT_PATH = process.argv[3] || `daily_cigar_partner_report_${today}.pptx`;

const data = JSON.parse(fs.readFileSync(JSON_PATH, "utf-8"));
const ins = data.insights;

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";

const NAVY = "1F2A44";
const GRAY_TEXT = "5B6472";
const LIGHT_BG = "F7F8FA";
const SERIES_COLORS = ["4C72B0", "DD8452", "55A868", "C44E52", "8172B2", "937860"];

function fmtKrwShort(v) {
  const sign = v < 0 ? "-" : "";
  v = Math.abs(v);
  if (v >= 1e8) return sign + (v / 1e8).toFixed(2) + "억";
  if (v >= 1e4) return sign + Math.round(v / 1e4).toLocaleString() + "만";
  return sign + Math.round(v).toLocaleString();
}

// ── 표지 슬라이드 ──
const cover = pres.addSlide();
cover.background = { color: "FFFFFF" };
cover.addText("데일리시가 거래처 분석 리포트", {
  x: 0.9, y: 1.55, w: 11.5, h: 0.9,
  fontFace: "Malgun Gothic", fontSize: 34, bold: true, color: NAVY,
});
cover.addText("도매 거래처별 구매매출 · 월별 매출 추이", {
  x: 0.9, y: 2.45, w: 11.5, h: 0.5,
  fontFace: "Malgun Gothic", fontSize: 16, color: GRAY_TEXT,
});
cover.addText(`집계 기간: ${ins.period_from} ~ ${ins.period_to} 누적 · 거래 거래처 ${ins.partner_count}곳`, {
  x: 0.9, y: 3.0, w: 11.5, h: 0.4,
  fontFace: "Malgun Gothic", fontSize: 13, color: GRAY_TEXT,
});
cover.addShape(pres.ShapeType.rect, {
  x: 0.9, y: 0.5, w: 0.55, h: 0.55, fill: { color: NAVY }, line: { type: "none" },
});

// ── 한줄평 (매달 자동 계산된 요약문구) ──
cover.addShape(pres.ShapeType.rect, {
  x: 0.9, y: 3.85, w: 11.5, h: 1.15,
  fill: { color: LIGHT_BG }, line: { type: "none" },
});
const momText = ins.mom_pct === null
  ? ""
  : ins.mom_pct >= 0
    ? `전월 대비 ${ins.mom_pct}% 증가`
    : `전월 대비 ${Math.abs(ins.mom_pct)}% 감소`;
cover.addText(
  [
    { text: `TOP1 거래처는 `, options: { color: NAVY } },
    { text: `${ins.top1_name}(${ins.top1_pct}%)`, options: { bold: true, color: NAVY } },
    { text: `이며, 상위 5개 거래처가 전체 도매 매출의 `, options: { color: NAVY } },
    { text: `${ins.top5_share}%`, options: { bold: true, color: NAVY } },
    { text: `를 차지합니다. 최근월(${ins.latest_month}${ins.is_latest_month_partial ? ", 집계중" : ""}) 매출은 `, options: { color: NAVY } },
    { text: `${fmtKrwShort(ins.latest_month_sales)}원(${momText})`, options: { bold: true, color: NAVY } },
    { text: `이고, 전체 도매 평균 마진율은 `, options: { color: NAVY } },
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

// ── TOP5 거래처 바차트 (매출 · 이익) ──
const barSlide = pres.addSlide();
barSlide.background = { color: "FFFFFF" };
barSlide.addText("TOP 5 거래처 구매매출 · 이익", {
  x: 0.6, y: 0.35, w: 12.0, h: 0.55,
  fontFace: "Malgun Gothic", fontSize: 24, bold: true, color: NAVY,
});
barSlide.addText(`${ins.period_from} ~ ${ins.period_to} 누적 도매 매출 기준 상위 5개 거래처`, {
  x: 0.6, y: 0.88, w: 12.0, h: 0.4,
  fontFace: "Malgun Gothic", fontSize: 12, color: GRAY_TEXT,
});

const topNames = data.top_partners.map((p) => p.name);
barSlide.addChart(
  pres.ChartType.bar,
  [
    { name: "구매매출", labels: topNames, values: data.top_partners.map((p) => p.sales) },
    { name: "이익", labels: topNames, values: data.top_partners.map((p) => p.profit) },
  ],
  {
    x: 0.5, y: 1.4, w: 12.3, h: 5.7,
    barDir: "col",
    barGrouping: "clustered",
    chartColors: ["4C72B0", "55A868"],
    showTitle: false,
    showLegend: true,
    legendPos: "b",
    showValue: true,
    dataLabelFontSize: 9,
    dataLabelFormatCode: "#,##0",
    catAxisLabelFontSize: 11,
    valAxisLabelFormatCode: "#,##0",
    catAxisLabelColor: NAVY,
    valAxisLabelColor: GRAY_TEXT,
  }
);

// ── 월별 거래처 매출 추이 (전체 + TOP5) ──
const lineSlide = pres.addSlide();
lineSlide.background = { color: "FFFFFF" };
lineSlide.addText("월별 도매 매출 추이", {
  x: 0.6, y: 0.35, w: 12.0, h: 0.55,
  fontFace: "Malgun Gothic", fontSize: 24, bold: true, color: NAVY,
});
lineSlide.addText("전체 합계 + TOP5 거래처 개별 추이", {
  x: 0.6, y: 0.88, w: 12.0, h: 0.4,
  fontFace: "Malgun Gothic", fontSize: 12, color: GRAY_TEXT,
});

const months = data.monthly_trend.months;
const lineSeries = [
  { name: "전체 합계", labels: months, values: data.monthly_trend.total },
  ...data.monthly_trend.series.map((s) => ({ name: s.name, labels: months, values: s.values })),
];
const lineColors = [NAVY, ...SERIES_COLORS];

lineSlide.addChart(pres.ChartType.line, lineSeries, {
  x: 0.5, y: 1.4, w: 12.3, h: 5.7,
  chartColors: lineColors,
  showTitle: false,
  showLegend: true,
  legendPos: "b",
  lineDataSymbol: "circle",
  lineDataSymbolSize: 5,
  lineSize: 2.25,
  catAxisLabelFontSize: 11,
  valAxisLabelFormatCode: "#,##0",
  catAxisLabelColor: NAVY,
  valAxisLabelColor: GRAY_TEXT,
});

pres.writeFile({ fileName: OUT_PATH }).then(() => {
  console.log(`저장 완료: ${OUT_PATH}`);
});
