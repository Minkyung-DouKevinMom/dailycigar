#!/usr/bin/env python3
"""
compute_partner_data.py
cigar.db의 wholesale_sales를 거래처별로 집계해 partner_data.json으로 저장한다.
build_partner_report.js가 이 JSON을 읽어 거래처 분석 PPT를 만든다.

사용법:
    python3 compute_partner_data.py [DB경로] [출력JSON경로]

기본값:
    DB경로       = ./cigar.db
    출력JSON경로 = ./partner_data.json
"""
import sys
import json
import sqlite3
import pandas as pd

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "cigar.db"
OUT_JSON = sys.argv[2] if len(sys.argv) > 2 else "partner_data.json"

TOP_N = 5


def main():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query(
        """
        SELECT
            w.sale_date,
            pm.partner_name,
            COALESCE(w.sales_amount, 0)  AS sales,
            COALESCE(w.profit_amount, 0) AS profit
        FROM wholesale_sales w
        JOIN partner_mst pm ON w.partner_id = pm.id
        """,
        conn,
    )
    conn.close()

    if df.empty:
        raise SystemExit("wholesale_sales 데이터가 없습니다.")

    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0)
    df["ym"] = df["sale_date"].astype(str).str.slice(0, 7)

    # ── 거래처별 합계 ──
    by_partner = (
        df.groupby("partner_name", dropna=False)
        .agg(sales=("sales", "sum"), profit=("profit", "sum"))
        .reset_index()
        .sort_values("sales", ascending=False)
        .reset_index(drop=True)
    )

    total_sales = by_partner["sales"].sum()
    total_profit = by_partner["profit"].sum()

    top5 = by_partner.head(TOP_N).copy()
    top_partners = [
        {
            "name": r["partner_name"],
            "sales": round(float(r["sales"])),
            "profit": round(float(r["profit"])),
            "sales_pct": round(float(r["sales"]) / total_sales * 100, 1) if total_sales else 0,
        }
        for _, r in top5.iterrows()
    ]

    # ── 월별 추이 (전체 + TOP5 거래처) ──
    months = sorted(df["ym"].unique().tolist())
    top5_names = top5["partner_name"].tolist()

    monthly_total = df.groupby("ym")["sales"].sum().reindex(months).fillna(0)

    series = []
    for name in top5_names:
        sub = df[df["partner_name"] == name].groupby("ym")["sales"].sum().reindex(months).fillna(0)
        series.append({"name": name, "values": [round(float(v)) for v in sub.tolist()]})

    monthly_trend = {
        "months": months,
        "total": [round(float(v)) for v in monthly_total.tolist()],
        "series": series,
    }

    # ── 인사이트 (표지 요약문구용) ──
    partner_count = int((by_partner["sales"] > 0).sum())
    top1 = top_partners[0]
    top5_share = round(top5["sales"].sum() / total_sales * 100, 1) if total_sales else 0
    overall_margin_pct = round(total_profit / total_sales * 100, 1) if total_sales else 0

    latest_month = months[-1]
    prev_month = months[-2] if len(months) >= 2 else None
    latest_month_sales = float(monthly_total.iloc[-1])
    prev_month_sales = float(monthly_total.iloc[-2]) if prev_month else None
    mom_pct = (
        round((latest_month_sales - prev_month_sales) / prev_month_sales * 100, 1)
        if prev_month_sales
        else None
    )

    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
    is_latest_month_partial = latest_month == today_str[:7]

    period_from = months[0].replace("-", ".")
    period_to = months[-1].replace("-", ".")

    insights = {
        "period_from": period_from,
        "period_to": period_to,
        "total_sales": round(float(total_sales)),
        "total_profit": round(float(total_profit)),
        "partner_count": partner_count,
        "overall_margin_pct": overall_margin_pct,
        "top1_name": top1["name"],
        "top1_pct": top1["sales_pct"],
        "top5_share": top5_share,
        "latest_month": latest_month,
        "latest_month_sales": round(latest_month_sales),
        "prev_month": prev_month,
        "mom_pct": mom_pct,
        "is_latest_month_partial": bool(is_latest_month_partial),
    }

    # ── 제안 (규칙 기반 자동 제안) ──
    recs = []

    if top5_share >= 70:
        recs.append(
            f"상위 {TOP_N}개 거래처가 전체 도매 매출의 {top5_share}%를 차지해 편중도가 매우 높습니다. "
            f"소수 거래처 이탈 시 매출 타격이 크므로 중위권 거래처 발굴·관리를 강화하는 것을 권장합니다."
        )
    elif top5_share >= 50:
        recs.append(
            f"상위 {TOP_N}개 거래처가 매출의 {top5_share}%를 차지해 다소 편중되어 있습니다. "
            f"신규 거래처 확보로 매출 기반을 넓히는 것을 고려해 보세요."
        )
    else:
        recs.append(
            f"상위 {TOP_N}개 거래처 비중이 {top5_share}%로 비교적 고르게 분산되어 있습니다. "
            f"현재의 다각화된 거래처 구성을 유지하세요."
        )

    if mom_pct is not None:
        partial_note = " (집계 진행 중인 달)" if is_latest_month_partial else ""
        if mom_pct >= 15:
            recs.append(
                f"{latest_month} 도매 매출이 전월 대비 {mom_pct}% 증가했습니다{partial_note}. "
                f"성장 거래처 대상 재고·프로모션을 우선 지원하세요."
            )
        elif mom_pct <= -15:
            recs.append(
                f"{latest_month} 도매 매출이 전월 대비 {abs(mom_pct)}% 감소했습니다{partial_note}. "
                f"주요 거래처의 구매 중단 여부를 점검할 필요가 있습니다."
            )
        else:
            recs.append(
                f"{latest_month} 도매 매출은 전월 대비 {mom_pct:+.1f}%로 큰 변동이 없습니다{partial_note}."
            )

    if overall_margin_pct < 45:
        recs.append(f"도매 평균 마진율이 {overall_margin_pct}%로 낮은 편입니다. 등급별 할인율을 점검해 보세요.")
    elif overall_margin_pct >= 55:
        recs.append(f"도매 평균 마진율이 {overall_margin_pct}%로 양호합니다. 현재 등급별 가격 정책을 유지하세요.")

    insights["recommendations"] = recs[:3]

    data = {
        "top_partners": top_partners,
        "monthly_trend": monthly_trend,
        "insights": insights,
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"저장 완료: {OUT_JSON}")
    print(f"TOP1: {top1['name']} ({top1['sales_pct']}%) / TOP5 비중 {top5_share}% / 전체 마진율 {overall_margin_pct}%")


if __name__ == "__main__":
    main()
