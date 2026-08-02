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

// ── 차트 슬라이드 (직접 그리는 세로 막대: 항목 전체 표시, 값+비율 라벨, 색상은 기존 매핑 유지) ──
function addBarSlide(title, subtitle, records, valueLabelFmt) {
  const slide = pres.addSlide();
  slide.background = { color: "FFFFFF" };

  slide.addText(title, {
    x: 0.6, y: 0.3, w: 12.0, h: 0.5,
    fontFace: "Malgun Gothic", fontSize: 22, bold: true, color: NAVY,
  });
  slide.addText(`${subtitle} · 전체 ${records.length}개 상품`, {
    x: 0.6, y: 0.78, w: 12.0, h: 0.35,
    fontFace: "Malgun Gothic", fontSize: 11.5, color: GRAY_TEXT,
  });

  const n = records.length;
  const chartX0 = 0.5, chartX1 = 12.9;
  const chartBottom = 6.35;
  const chartTop = 1.35;
  const labelH = n > 28 ? 0.4 : 0.46;
  const maxBarAreaH = chartBottom - chartTop - labelH;
  const usableW = chartX1 - chartX0;
  const slotW = usableW / n;
  const barW = slotW * 0.66;
  const vMax = Math.max(...records.map(r => r.pct));

  const valueFontSize = n > 28 ? 6.5 : n > 20 ? 7.5 : n > 12 ? 9 : 10.5;
  const catFontSize = n > 28 ? 6.5 : n > 20 ? 7.5 : n > 12 ? 9 : 10.5;

  records.forEach((r, i) => {
    const barH = Math.max((r.pct / vMax) * maxBarAreaH, 0.04);
    const barX = chartX0 + i * slotW + (slotW - barW) / 2;
    const barY = chartBottom - barH;
    const barCenterX = barX + barW / 2;

    slide.addShape(pres.ShapeType.rect, {
      x: barX, y: barY, w: barW, h: barH,
      fill: { color: r.color }, line: { type: "none" },
    });

    const labelW = Math.max(slotW * 2.1, 0.75);
    slide.addText(
      [
        { text: valueLabelFmt.fmt(r.value), options: { breakLine: true, bold: false, color: GRAY_TEXT } },
        { text: `${r.pct}%`, options: { bold: true, color: NAVY } },
      ],
      {
        x: barCenterX - labelW / 2, y: barY - labelH - 0.02, w: labelW, h: labelH,
        fontFace: "Malgun Gothic", fontSize: valueFontSize, align: "center", valign: "bottom",
        lineSpacingMultiple: 0.98, wrap: false,
      }
    );

    const catW = n > 28 ? 0.62 : n > 20 ? 0.8 : n > 12 ? 1.0 : 1.15;
    slide.addText(r.label, {
      x: barCenterX - catW, y: chartBottom + 0.05, w: catW, h: 0.2,
      fontFace: "Malgun Gothic", fontSize: catFontSize, color: NAVY,
      align: "right", valign: "middle", rotate: -30, wrap: false,
    });
  });

  slide.addShape(pres.ShapeType.line, {
    x: chartX0, y: chartBottom, w: chartX1 - chartX0, h: 0,
    line: { color: "D9DCE3", width: 1 },
  });
}

addBarSlide(
  "시가상품별 매출금액 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 추정치 포함",
  data.sales, { name: "매출금액", fmt: fmtKrwShort }
);
addBarSlide(
  "시가상품별 이익 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 추정치 포함",
  data.profit, { name: "이익금액", fmt: fmtKrwShort }
);
addBarSlide(
  "시가상품별 판매수량 비중 (전체)",
  "소매 + 도매 직접판매 + 선물세트 수량 포함",
  data.qty, { name: "판매수량", fmt: fmtQty }
);

pres.writeFile({ fileName: OUT_PATH }).then(() => {
  console.log(`저장 완료: ${OUT_PATH}`);
});