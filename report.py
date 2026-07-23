"""
데일리시가 정기 리포트 스크립트
================================
Claude 예약 작업(클라우드)에서 매 실행마다 저장소를 clone한 뒤 이 스크립트를 실행합니다.
streamlit 없이 sqlite3 + matplotlib만 사용하며, 산출물은 ./report_output/ 에 PNG로 저장하고
콘솔에 요약 텍스트를 출력합니다. Claude가 이 콘솔 출력 + 이미지를 읽어 채팅으로 보고합니다.

⚠️ 실제 DB 파일 없이 작성되었습니다. 아래 컬럼/뷰 이름이 실제 스키마와 다르면
   "설정값" 섹션과 각 함수 상단의 SQL을 실제 스키마에 맞게 조정해 주세요.
   실행 시 스키마 문제가 있으면 해당 섹션은 에러 메시지를 출력하고 건너뜁니다
   (한 섹션이 실패해도 나머지 섹션은 계속 실행됩니다).

사용법:
    python report.py
    python report.py --db-path /path/to/cigar.db --out-dir ./report_output
"""

import argparse
import os
import sqlite3
import sys
import traceback
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

# ── 한글 폰트 설정 (클라우드 실행 환경엔 기본적으로 한글 폰트가 없을 수 있음) ──
def setup_korean_font():
    candidates = [
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "NanumGothic", "Malgun Gothic", "AppleGothic",
    ]
    for c in candidates:
        try:
            if os.path.exists(c):
                fm.fontManager.addfont(c)
                plt.rcParams["font.family"] = fm.FontProperties(fname=c).get_name()
                return
        except Exception:
            continue
    # 설치된 폰트 이름으로 재시도 (예: apt install fonts-nanum 이후)
    for name in ["NanumGothic", "Noto Sans CJK KR", "Malgun Gothic"]:
        if any(name.lower() in f.name.lower() for f in fm.fontManager.ttflist):
            plt.rcParams["font.family"] = name
            return
    print("⚠️ 한글 폰트를 찾지 못했습니다. 차트의 한글이 깨질 수 있습니다. "
          "(클라우드 환경이라면 'apt-get install -y fonts-nanum' 필요)")

plt.rcParams["axes.unicode_minus"] = False

# ── 브랜드 매핑 (product_code 접두어 기준, 실제 라인업에 맞게 조정) ──
def brand_of(code: str) -> str:
    c = (code or "").upper()
    if c.startswith("1881"):
        return "1881"
    if c.startswith("ALH"):
        return "Alhambra"
    if c.startswith("DJUA") or c.startswith("DJU"):
        return "Don Juan Urquijo"
    if c.startswith("TABFF"):
        return "Flor Fina"
    if c.startswith("TABE") or c.startswith("TABR") or c.startswith("TAB"):
        return "Tabacalera"
    return "기타"


def get_conn(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
    return sqlite3.connect(db_path)


def table_or_view_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    )
    return cur.fetchone() is not None


def fmt_krw(n) -> str:
    try:
        return f"₩{float(n):,.0f}"
    except Exception:
        return "₩0"


# ══════════════════════════════════════════════════════════════
# 1. 브랜드별 판매 현황 (도소매 합산, 전체 기간, 매출/이익/수량 값+비율)
# ══════════════════════════════════════════════════════════════
def section_brand_overview(conn, out_dir: str):
    print("\n" + "=" * 60)
    print("[1] 브랜드별 판매 현황 (전체 기간, 도소매 합산)")
    print("=" * 60)

    retail_src = "v_retail_sales_enriched" if table_or_view_exists(conn, "v_retail_sales_enriched") else "retail_sales"
    wholesale_src = "v_wholesale_sales" if table_or_view_exists(conn, "v_wholesale_sales") else "wholesale_sales"

    try:
        cigar_codes = set(
            pd.read_sql_query(
                "SELECT DISTINCT UPPER(TRIM(product_code)) AS c FROM product_mst "
                "WHERE TRIM(COALESCE(product_code,'')) <> ''",
                conn,
            )["c"].tolist()
        )
    except Exception as e:
        print(f"  product_mst 조회 실패: {e}")
        cigar_codes = set()

    retail_sql = f"""
        SELECT
            UPPER(TRIM(COALESCE(product_code, product_code_raw, ''))) AS product_code,
            COALESCE(qty, 0) AS qty,
            COALESCE(net_sales_amount, 0) AS sales,
            COALESCE(retail_gross_profit_krw, 0) AS profit
        FROM {retail_src}
    """
    wholesale_sql = f"""
        SELECT
            UPPER(TRIM(COALESCE(product_code, ''))) AS product_code,
            COALESCE(qty, 0) AS qty,
            COALESCE(sales_amount, 0) AS sales,
            COALESCE(profit_amount, 0) AS profit
        FROM {wholesale_src}
    """

    try:
        retail_df = pd.read_sql_query(retail_sql, conn)
    except Exception as e:
        print(f"  소매 데이터 조회 실패 ({retail_src}): {e}")
        retail_df = pd.DataFrame(columns=["product_code", "qty", "sales", "profit"])

    try:
        wholesale_df = pd.read_sql_query(wholesale_sql, conn)
    except Exception as e:
        print(f"  도매 데이터 조회 실패 ({wholesale_src}): {e}")
        wholesale_df = pd.DataFrame(columns=["product_code", "qty", "sales", "profit"])

    df = pd.concat([retail_df, wholesale_df], ignore_index=True)
    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)].copy()
    if df.empty:
        print("  데이터가 없습니다.")
        return

    df["brand"] = df["product_code"].map(brand_of)
    grp = df.groupby("brand", as_index=False).agg(
        판매수량=("qty", "sum"), 매출=("sales", "sum"), 이익=("profit", "sum")
    )
    grp["마진율(%)"] = (grp["이익"] / grp["매출"].replace(0, pd.NA) * 100).round(1).fillna(0)
    grp = grp.sort_values("매출", ascending=False).reset_index(drop=True)

    total_sales = grp["매출"].sum()
    total_profit = grp["이익"].sum()
    total_qty = grp["판매수량"].sum()

    print(f"  총매출 {fmt_krw(total_sales)} · 총이익 {fmt_krw(total_profit)} "
          f"· 총판매 {int(total_qty):,}개 · 전체마진율 {total_profit/total_sales*100:.1f}%")
    for _, r in grp.iterrows():
        share = r["매출"] / total_sales * 100 if total_sales else 0
        print(f"  - {r['brand']:<20} 매출 {fmt_krw(r['매출']):>15} ({share:4.1f}%)  "
              f"이익 {fmt_krw(r['이익']):>15}  수량 {int(r['판매수량']):>6,}개  마진율 {r['마진율(%)']:.1f}%")

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, col, title in zip(axes, ["매출", "이익", "판매수량"], ["매출", "이익", "판매수량"]):
        vals = grp[col]
        total = vals.sum()
        labels = [f"{n}\n{v/total*100:.0f}%" for n, v in zip(grp["brand"], vals)]
        ax.pie(vals, labels=labels, autopct=lambda p: f"{p:.0f}%" if False else None, startangle=90)
        ax.set_title(f"브랜드별 {title} 비중")
    plt.tight_layout()
    path = os.path.join(out_dir, "1_brand_overview.png")
    plt.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  차트 저장: {path}")


# ══════════════════════════════════════════════════════════════
# 2. 도매 업체별 구매현황
# ══════════════════════════════════════════════════════════════
def section_wholesale_by_partner(conn, out_dir: str):
    print("\n" + "=" * 60)
    print("[2] 도매 업체별 구매현황")
    print("=" * 60)

    sql = """
        SELECT
            COALESCE(p.partner_name, '미지정') AS 업체명,
            COALESCE(w.qty, 0) AS qty,
            COALESCE(w.sales_amount, 0) AS sales,
            COALESCE(w.profit_amount, 0) AS profit
        FROM wholesale_sales w
        LEFT JOIN partner_mst p ON w.partner_id = p.id
    """
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"  조회 실패 (partner_id 컬럼명이 다를 수 있음): {e}")
        return

    if df.empty:
        print("  데이터가 없습니다.")
        return

    grp = df.groupby("업체명", as_index=False).agg(
        구매수량=("qty", "sum"), 구매금액=("sales", "sum"), 이익=("profit", "sum")
    ).sort_values("구매금액", ascending=False)

    for _, r in grp.iterrows():
        print(f"  - {r['업체명']:<20} 구매금액 {fmt_krw(r['구매금액']):>15}  "
              f"수량 {int(r['구매수량']):>6,}개  이익 {fmt_krw(r['이익']):>15}")

    top = grp.head(15)
    fig, ax = plt.subplots(figsize=(10, max(4, 0.4 * len(top))))
    ax.barh(top["업체명"][::-1], top["구매금액"][::-1], color="#1B2A4A")
    ax.set_title("도매 업체별 구매금액 (상위 15개)")
    ax.set_xlabel("구매금액 (원)")
    plt.tight_layout()
    path = os.path.join(out_dir, "2_wholesale_by_partner.png")
    plt.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  차트 저장: {path}")


# ══════════════════════════════════════════════════════════════
# 3. 이번달 30일 vs 지난달 30일 매출/이익 증감 (rolling 30-day window)
# ══════════════════════════════════════════════════════════════
def section_month_over_month(conn, out_dir: str):
    print("\n" + "=" * 60)
    print("[3] 최근 30일 vs 이전 30일 매출/이익 증감")
    print("=" * 60)

    today = date.today()
    cur_start = today - timedelta(days=29)
    cur_end = today
    prev_start = today - timedelta(days=59)
    prev_end = today - timedelta(days=30)

    retail_src = "v_retail_sales_enriched" if table_or_view_exists(conn, "v_retail_sales_enriched") else "retail_sales"
    wholesale_src = "v_wholesale_sales" if table_or_view_exists(conn, "v_wholesale_sales") else "wholesale_sales"

    def period_totals(d_from, d_to):
        r_sql = f"""
            SELECT COALESCE(SUM(net_sales_amount),0) AS sales, COALESCE(SUM(retail_gross_profit_krw),0) AS profit
            FROM {retail_src} WHERE sale_date BETWEEN ? AND ?
        """
        w_sql = f"""
            SELECT COALESCE(SUM(sales_amount),0) AS sales, COALESCE(SUM(profit_amount),0) AS profit
            FROM {wholesale_src} WHERE sale_date BETWEEN ? AND ?
        """
        try:
            r = pd.read_sql_query(r_sql, conn, params=[str(d_from), str(d_to)]).iloc[0]
        except Exception as e:
            print(f"  소매 조회 실패: {e}")
            r = pd.Series({"sales": 0, "profit": 0})
        try:
            w = pd.read_sql_query(w_sql, conn, params=[str(d_from), str(d_to)]).iloc[0]
        except Exception as e:
            print(f"  도매 조회 실패: {e}")
            w = pd.Series({"sales": 0, "profit": 0})
        return {"sales": r["sales"] + w["sales"], "profit": r["profit"] + w["profit"]}

    cur = period_totals(cur_start, cur_end)
    prev = period_totals(prev_start, prev_end)

    def delta(a, b):
        diff = a - b
        pct = (diff / b * 100) if b else 0
        return diff, pct

    sales_diff, sales_pct = delta(cur["sales"], prev["sales"])
    profit_diff, profit_pct = delta(cur["profit"], prev["profit"])

    print(f"  최근 30일 ({cur_start} ~ {cur_end}): 매출 {fmt_krw(cur['sales'])} / 이익 {fmt_krw(cur['profit'])}")
    print(f"  이전 30일 ({prev_start} ~ {prev_end}): 매출 {fmt_krw(prev['sales'])} / 이익 {fmt_krw(prev['profit'])}")
    print(f"  매출 증감: {sales_diff:+,.0f}원 ({sales_pct:+.1f}%)")
    print(f"  이익 증감: {profit_diff:+,.0f}원 ({profit_pct:+.1f}%)")

    fig, ax = plt.subplots(figsize=(6, 5))
    labels = ["이전 30일", "최근 30일"]
    x = range(2)
    width = 0.35
    ax.bar([i - width/2 for i in x], [prev["sales"], cur["sales"]], width, label="매출", color="#1B2A4A")
    ax.bar([i + width/2 for i in x], [prev["profit"], cur["profit"]], width, label="이익", color="#B08D57")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title("최근 30일 vs 이전 30일 매출/이익")
    ax.legend()
    plt.tight_layout()
    path = os.path.join(out_dir, "3_month_over_month.png")
    plt.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  차트 저장: {path}")


# ══════════════════════════════════════════════════════════════
# 4. 소매 월별 매출 추이 + 채널별(택배/매장) 막대그래프
#    ※ "월별사용료"는 소매 월별 매출 추이로 해석했습니다. 다른 의미라면 알려주세요.
# ══════════════════════════════════════════════════════════════
def section_retail_monthly_trend(conn, out_dir: str):
    print("\n" + "=" * 60)
    print("[4] 소매 월별 매출 추이 (채널별: 택배/매장)")
    print("=" * 60)

    sql = """
        SELECT
            strftime('%Y-%m', sale_date) AS ym,
            CASE WHEN delivery_yn = 'Y' THEN '택배' ELSE '매장' END AS channel,
            COALESCE(net_sales_amount, 0) AS sales
        FROM retail_sales
    """
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"  조회 실패 (order_channel/net_sales_amount 컬럼명이 다를 수 있음): {e}")
        return

    if df.empty:
        print("  데이터가 없습니다.")
        return

    print(f"  channel 값 종류: {sorted(df['channel'].unique().tolist())}")
    pivot = df.groupby(["ym", "channel"], as_index=False)["sales"].sum()
    monthly_total = df.groupby("ym", as_index=False)["sales"].sum().sort_values("ym")

    for _, r in monthly_total.iterrows():
        print(f"  - {r['ym']}: 매출 {fmt_krw(r['sales'])}")

    wide = pivot.pivot(index="ym", columns="channel", values="sales").fillna(0).sort_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    wide.plot(kind="bar", stacked=True, ax=ax, colormap="tab10")
    ax.set_title("소매 월별 매출 추이 (채널별)")
    ax.set_xlabel("월")
    ax.set_ylabel("매출 (원)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    path = os.path.join(out_dir, "4_retail_monthly_trend.png")
    plt.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  차트 저장: {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=os.getenv("DAILYCIGAR_DB_PATH", "cigar.db"))
    parser.add_argument("--out-dir", default="report_output")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    setup_korean_font()

    conn = get_conn(args.db_path)
    try:
        for section in [
            section_brand_overview,
            section_wholesale_by_partner,
            section_month_over_month,
            section_retail_monthly_trend,
        ]:
            try:
                section(conn, args.out_dir)
            except Exception:
                print(f"\n⚠️ {section.__name__} 섹션 실행 중 오류 발생, 건너뜁니다:")
                traceback.print_exc()
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print(f"리포트 생성 완료. 이미지: {args.out_dir}/*.png")
    print("=" * 60)


if __name__ == "__main__":
    sys.exit(main())