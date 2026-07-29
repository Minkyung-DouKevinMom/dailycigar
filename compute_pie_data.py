#!/usr/bin/env python3
"""
compute_pie_data.py
cigar.db에서 시가 상품 매출/이익/판매수량(전체기간, 소매+도매 통합)을 계산해
pie_data.json으로 저장한다. build_pie_report.js가 이 JSON을 읽어 PPT를 만든다.

사용법:
    python3 compute_pie_data.py [DB경로] [출력JSON경로]

기본값:
    DB경로       = ./cigar.db
    출력JSON경로 = ./pie_data.json

하드코딩 규칙 (필요 시 아래 값만 수정):
    DISCOUNT_CODE_RATES: 특정 상품코드의 매출/이익에 곱할 할인율
"""
import sys
import sqlite3
import pandas as pd

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "cigar.db"
OUT_JSON = sys.argv[2] if len(sys.argv) > 2 else "pie_data.json"

# ── 하드코딩: 파이차트에만 반영되는 상품코드별 할인율 ──
DISCOUNT_CODE_RATES = {"TAB(HC)": 0.70}


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def view_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (name,))
    return cur.fetchone() is not None


def group_minor_as_others(df: pd.DataFrame, value_col: str, top_n: int = 10) -> pd.DataFrame:
    work = df[["상품코드", value_col]].groupby("상품코드", as_index=False).sum()
    work = work.sort_values(value_col, ascending=False)
    if len(work) <= top_n:
        return work
    top = work.head(top_n).copy()
    others_sum = work.iloc[top_n:][value_col].sum()
    if others_sum > 0:
        top = pd.concat(
            [top, pd.DataFrame([{"상품코드": "기타", value_col: others_sum}])],
            ignore_index=True,
        )
    return top


def hsl_to_hex(h: float, s: float, l: float) -> str:
    h = h % 360
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = l - c / 2
    if h < 60:
        r, g, b = c, x, 0
    elif h < 120:
        r, g, b = x, c, 0
    elif h < 180:
        r, g, b = 0, c, x
    elif h < 240:
        r, g, b = 0, x, c
    elif h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x
    r, g, b = [round((v + m) * 255) for v in (r, g, b)]
    return f"{r:02X}{g:02X}{b:02X}"


def main():
    conn = sqlite3.connect(DB_PATH)

    cigar_codes = set(
        pd.read_sql_query(
            "SELECT DISTINCT UPPER(TRIM(COALESCE(product_code,''))) AS c FROM product_mst "
            "WHERE TRIM(COALESCE(product_code,''))<>''",
            conn,
        )["c"].tolist()
    )

    # ── 소매 (전체기간, 시가만) ──
    retail_df = pd.read_sql_query(
        """
        SELECT
            COALESCE(product_code, product_code_raw,'') AS product_code,
            COALESCE(mst_product_name, product_code_raw,'미분류') AS product_name,
            COALESCE(qty,0) AS qty,
            COALESCE(net_sales_amount,0) AS sales,
            COALESCE(retail_gross_profit_krw,0) AS profit
        FROM v_retail_sales_enriched
        """,
        conn,
    )
    for c in ["qty", "sales", "profit"]:
        retail_df[c] = pd.to_numeric(retail_df[c], errors="coerce").fillna(0)
    retail_df["product_code"] = normalize_code(retail_df["product_code"])
    retail_df = retail_df[retail_df["product_code"].isin(cigar_codes)].copy()

    # ── 도매 (전체기간, 시가만) ──
    ws_src = "v_wholesale_sales" if view_exists(conn, "v_wholesale_sales") else "wholesale_sales"
    wholesale_df = pd.read_sql_query(
        f"""
        SELECT
            COALESCE(product_code,'') AS product_code,
            COALESCE(product_name, product_code,'미분류') AS product_name,
            COALESCE(qty,0) AS qty,
            COALESCE(sales_amount,0) AS sales,
            COALESCE(profit_amount,0) AS profit
        FROM {ws_src}
        """,
        conn,
    )
    for c in ["qty", "sales", "profit"]:
        wholesale_df[c] = pd.to_numeric(wholesale_df[c], errors="coerce").fillna(0)
    wholesale_df["product_code"] = normalize_code(wholesale_df["product_code"])
    wholesale_df = wholesale_df[wholesale_df["product_code"].isin(cigar_codes)].copy()

    # ── 선물세트 수량 (매출/이익은 0, 추후 평균단가로 추정) ──
    gift_set_df = pd.DataFrame(columns=["product_code", "product_name", "qty"])
    if table_exists(conn, "stock_out"):
        gdf = pd.read_sql_query(
            """
            SELECT
                UPPER(TRIM(COALESCE(so.product_code,''))) AS product_code,
                COALESCE(pm.product_name, so.product_code,'미분류') AS product_name,
                SUM(so.qty) AS qty
            FROM stock_out so
            LEFT JOIN product_mst pm ON so.product_code = pm.product_code
            WHERE so.out_type = 'gift_set'
            GROUP BY so.product_code
            """,
            conn,
        )
        gdf["product_code"] = normalize_code(gdf["product_code"])
        gift_set_df = gdf[gdf["product_code"].isin(cigar_codes)].copy()

    # ── 직접판매(소매+도매) 통합 집계 ──
    direct = pd.concat([retail_df, wholesale_df], ignore_index=True)
    direct["product_code"] = normalize_code(direct["product_code"])
    grp = direct.groupby(["product_code", "product_name"], dropna=False).agg(
        판매량=("qty", "sum"), 매출=("sales", "sum"), 이익=("profit", "sum")
    ).reset_index()
    grp["상품코드"] = grp["product_code"]

    # ── 하드코딩 할인 적용 (파이차트 전용) ──
    for code, rate in DISCOUNT_CODE_RATES.items():
        mask = grp["상품코드"].astype(str).str.strip().str.upper() == code
        if mask.any():
            grp.loc[mask, "매출"] *= rate
            grp.loc[mask, "이익"] *= rate

    # ── 선물세트 반영: 평균단가/단위이익 × 선물세트 수량 추정치 더하기 ──
    gift_qty_by_code = gift_set_df.set_index("product_code")["qty"].to_dict() if not gift_set_df.empty else {}
    gift_name_by_code = gift_set_df.set_index("product_code")["product_name"].to_dict() if not gift_set_df.empty else {}

    base = {}
    for _, r in grp.iterrows():
        code = str(r["상품코드"])
        base[code] = {
            "product_name": r["product_name"],
            "판매량": float(r["판매량"]),
            "매출": float(r["매출"]),
            "이익": float(r["이익"]),
        }

    all_codes = set(base.keys()) | set(gift_qty_by_code.keys())
    rows = []
    for code in all_codes:
        b = base.get(code, {"product_name": gift_name_by_code.get(code, code), "판매량": 0.0, "매출": 0.0, "이익": 0.0})
        direct_qty, direct_sales, direct_profit = b["판매량"], b["매출"], b["이익"]
        avg_price = (direct_sales / direct_qty) if direct_qty else 0.0
        avg_profit = (direct_profit / direct_qty) if direct_qty else 0.0
        gift_qty = float(gift_qty_by_code.get(code, 0) or 0)
        rows.append({
            "상품코드": code,
            "product_name": b["product_name"],
            "판매량": direct_qty + gift_qty,
            "매출": direct_sales + avg_price * gift_qty,
            "이익": direct_profit + avg_profit * gift_qty,
        })

    combined = pd.DataFrame(rows).sort_values("매출", ascending=False).reset_index(drop=True)

    sales_pie = group_minor_as_others(combined, "매출", top_n=14)
    profit_pie = group_minor_as_others(combined, "이익", top_n=14)
    qty_pie = group_minor_as_others(combined, "판매량", top_n=14)

    # ── 색상: 세 차트에 등장하는 라벨 합집합 기준으로 고유 배정 (동일 상품 = 동일 색) ──
    all_labels = set(sales_pie["상품코드"]) | set(profit_pie["상품코드"]) | set(qty_pie["상품코드"])
    all_labels.discard("기타")
    sorted_labels = sorted(all_labels)
    n = len(sorted_labels)
    color_map = {}
    for i, lbl in enumerate(sorted_labels):
        hue = (i * 360.0 / n) % 360 if n else 0
        sat = 0.62 if i % 2 == 0 else 0.75
        light = 0.52 if i % 2 == 0 else 0.42
        color_map[lbl] = hsl_to_hex(hue, sat, light)
    color_map["기타"] = "B0B0B0"

    def to_records(df, value_col):
        total = df[value_col].sum()
        return [
            {
                "label": r["상품코드"],
                "value": round(float(r[value_col])),
                "pct": round(float(r[value_col]) / total * 100, 1) if total else 0,
                "color": color_map[r["상품코드"]],
            }
            for _, r in df.iterrows()
        ]

    # ── 인사이트 계산 (표지 한줄평용) ──
    def top_n_share(df, value_col, n=5):
        non_other = df[df["상품코드"] != "기타"].sort_values(value_col, ascending=False)
        total = df[value_col].sum()
        return round(non_other.head(n)[value_col].sum() / total * 100, 1) if total else 0

    # ── 데이터 집계 기간 (표지 자동 표기용) ──
    r_range = pd.read_sql_query("SELECT MIN(sale_date) mn, MAX(sale_date) mx FROM v_retail_sales_enriched", conn)
    w_range = pd.read_sql_query(f"SELECT MIN(sale_date) mn, MAX(sale_date) mx FROM {ws_src}", conn)
    dates = [d for d in [r_range.iloc[0]["mn"], r_range.iloc[0]["mx"], w_range.iloc[0]["mn"], w_range.iloc[0]["mx"]] if d]
    period_from = min(dates)[:7].replace("-", ".") if dates else ""
    period_to = max(dates)[:7].replace("-", ".") if dates else ""

    insights = {
        "period_from": period_from,
        "period_to": period_to,
        "sales_top1_code": str(sales_pie.iloc[0]["상품코드"]),
        "profit_top1_code": str(profit_pie.iloc[0]["상품코드"]),
        "sales_top1_pct": round(float(sales_pie.iloc[0]["매출"]) / sales_pie["매출"].sum() * 100, 1),
        "profit_top1_pct": round(float(profit_pie.iloc[0]["이익"]) / profit_pie["이익"].sum() * 100, 1),
        "sales_top5_share": top_n_share(sales_pie, "매출"),
        "qty_top1_code": str(qty_pie.iloc[0]["상품코드"]),
        "qty_top1_pct": round(float(qty_pie.iloc[0]["판매량"]) / qty_pie["판매량"].sum() * 100, 1),
        "overall_margin_pct": round(combined["이익"].sum() / combined["매출"].sum() * 100, 1) if combined["매출"].sum() else 0,
    }

    # ── 제안 (인사이트 지표 기반 규칙형 자동 제안, 표지에 함께 표기) ──
    def generate_recommendations(ins: dict) -> list:
        recs = []

        top5 = ins["sales_top5_share"]
        if top5 >= 50:
            recs.append(
                f"상위 5개 상품이 전체 매출의 {top5}%를 차지해 편중도가 높습니다. "
                f"6위 이하 상품의 노출·프로모션을 늘려 판매 채널을 다각화하는 것을 권장합니다."
            )
        elif top5 >= 35:
            recs.append(
                f"상위 5개 상품이 매출의 {top5}%를 차지해 다소 편중되어 있습니다. "
                f"중위권 상품 프로모션으로 매출 기반을 넓히는 것을 고려해 보세요."
            )
        else:
            recs.append(
                f"상위 5개 상품 비중이 {top5}%로 비교적 고르게 분산되어 있습니다. "
                f"현재의 다각화된 상품 구성을 유지하세요."
            )

        sales_top1, qty_top1 = ins["sales_top1_code"], ins["qty_top1_code"]
        if sales_top1 != qty_top1:
            recs.append(
                f"매출 1위는 {sales_top1}, 판매수량 1위는 {qty_top1}로 서로 다릅니다. "
                f"{qty_top1} 구매 고객에게 {sales_top1} 등 고단가 상품을 함께 제안하는 업셀링 전략을 고려해 보세요."
            )
        else:
            recs.append(
                f"{sales_top1}이(가) 매출과 판매수량 모두 1위로 핵심 주력 상품입니다. "
                f"재고·매입 우선순위를 해당 상품에 집중하세요."
            )

        if ins["sales_top1_code"] == ins["profit_top1_code"]:
            diff = ins["profit_top1_pct"] - ins["sales_top1_pct"]
            if diff >= 1.5:
                recs.append(
                    f"{sales_top1}은(는) 매출 비중({ins['sales_top1_pct']}%)보다 이익 비중({ins['profit_top1_pct']}%)이 "
                    f"더 높아 마진 효율이 우수합니다. 관련 재고 확보와 마케팅을 강화하세요."
                )
            elif diff <= -1.5:
                recs.append(
                    f"{sales_top1}은(는) 매출 비중({ins['sales_top1_pct']}%) 대비 이익 비중({ins['profit_top1_pct']}%)이 "
                    f"낮습니다. 매입 단가나 할인 정책을 점검해 마진을 개선할 필요가 있습니다."
                )

        margin = ins["overall_margin_pct"]
        if margin < 40:
            recs.append(f"전체 평균 마진율이 {margin}%로 낮은 편입니다. 저마진 상품 비중을 줄이거나 매입가 협상을 검토하세요.")
        elif margin >= 55:
            recs.append(f"전체 평균 마진율이 {margin}%로 양호합니다. 현재의 상품 믹스와 가격 정책을 유지하세요.")

        return recs[:3]

    insights["recommendations"] = generate_recommendations(insights)

    import json
    data = {
        "sales": to_records(sales_pie, "매출"),
        "profit": to_records(profit_pie, "이익"),
        "qty": to_records(qty_pie, "판매량"),
        "insights": insights,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"저장 완료: {OUT_JSON}")
    print(f"매출 1위: {insights['sales_top1_code']} ({insights['sales_top1_pct']}%) / 전체 마진율 {insights['overall_margin_pct']}%")


if __name__ == "__main__":
    main()