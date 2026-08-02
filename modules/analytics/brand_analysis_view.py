import os
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

from db import get_stock_summary

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fmt_krw(x):
    try:
        return f"₩{float(x):,.0f}"
    except Exception:
        return "₩0"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
        (view_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [str(r[1]).strip() for r in rows]


def normalize_code(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


# ── 하드코딩: 특정 상품코드에 대한 계산 예외 처리 ──
# TAB(HC): 파이차트에 표시되는 매출/이익만 30% 할인된 금액으로 계산한다.
# (상단 KPI 카드나 바차트 등 다른 곳의 실제 매출/이익에는 영향을 주지 않는다)
DISCOUNT_CODE_RATES = {"TAB(HC)": 0.70}


def apply_discount_to_grouped(
    grp: pd.DataFrame,
    code_col: str = "상품코드",
    sales_col: str = "매출",
    profit_col: str = "이익",
    qty_col: str = "판매량",
) -> pd.DataFrame:
    """
    파이차트용으로 이미 상품별 집계된 DataFrame(build_product_grouped 등의 결과)에
    DISCOUNT_CODE_RATES 의 할인율을 적용한다. 원본 retail_df/wholesale_df나
    KPI 계산에 쓰이는 brand_grouped와는 완전히 분리되어, 이 함수를 거친 사본만
    할인이 반영된다 — 즉 파이차트에만 영향을 준다.
    """
    if grp.empty or not DISCOUNT_CODE_RATES or code_col not in grp.columns:
        return grp
    grp = grp.copy()
    for code, rate in DISCOUNT_CODE_RATES.items():
        mask = grp[code_col].astype(str).str.strip().str.upper() == code
        if mask.any():
            if sales_col in grp.columns:
                grp.loc[mask, sales_col] = grp.loc[mask, sales_col] * rate
            if profit_col in grp.columns:
                grp.loc[mask, profit_col] = grp.loc[mask, profit_col] * rate
    if "마진율(%)" in grp.columns and sales_col in grp.columns and profit_col in grp.columns:
        grp["마진율(%)"] = grp.apply(
            lambda x: round(x[profit_col] / x[sales_col] * 100, 1) if x[sales_col] else 0, axis=1
        )
    if "개당마진금액" in grp.columns and qty_col in grp.columns and profit_col in grp.columns:
        grp["개당마진금액"] = grp.apply(
            lambda x: round(x[profit_col] / x[qty_col], 0) if x[qty_col] else 0, axis=1
        )
    return grp


def group_minor_as_others(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    qty_col: str | None = None,
    extra_cols: list[str] | None = None,
    top_n: int = 6,
) -> pd.DataFrame:
    """
    extra_cols: value_col/qty_col 외에 함께 합산해서 유지하고 싶은 컬럼들
    (예: 직접판매량, 선물세트수량 같은 브레이크다운용 컬럼). "기타" 행에도 합산되어 반영된다.
    """
    if df.empty:
        return df.copy()

    extra_cols = [c for c in (extra_cols or []) if c in df.columns and c not in (value_col, qty_col)]

    agg_cols = [label_col, value_col]
    if qty_col and qty_col in df.columns:
        agg_cols.append(qty_col)
    agg_cols += extra_cols

    work = df[agg_cols].copy()
    agg_dict = {value_col: "sum"}
    if qty_col and qty_col in df.columns:
        agg_dict[qty_col] = "sum"
    for c in extra_cols:
        agg_dict[c] = "sum"
    work = work.groupby(label_col, as_index=False).agg(agg_dict)
    work = work.sort_values(value_col, ascending=False)

    if len(work) <= top_n:
        return work

    top_df = work.head(top_n).copy()
    others_row = {label_col: "기타", value_col: work.iloc[top_n:][value_col].sum()}
    if qty_col and qty_col in work.columns:
        others_row[qty_col] = work.iloc[top_n:][qty_col].sum()
    for c in extra_cols:
        others_row[c] = work.iloc[top_n:][c].sum()

    if others_row[value_col] > 0:
        top_df = pd.concat(
            [top_df, pd.DataFrame([others_row])],
            ignore_index=True,
        )
    return top_df


PIE_OTHER_COLOR = "#B0B0B0"


def _hsl_to_hex(h: float, s: float, l: float) -> str:
    """h: 0~360, s/l: 0~1. HSL -> #RRGGBB 변환."""
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
    return f"#{r:02X}{g:02X}{b:02X}"


def build_product_color_map(codes) -> dict:
    """
    상품코드 -> 고정 색상 매핑을 한 번만 생성한다.
    상품 개수만큼 색상환(Hue 0~360도)을 균등하게 나눠서 배정하므로,
    상품이 몇 개든 서로 다른 색이 나오고(고정 팔레트처럼 순환되며 겹치지 않는다),
    코드를 정렬한 뒤 배정하므로 어떤 파이차트(Top N)에 등장하든 같은 상품은 항상 같은 색이다.
    채도/명도를 살짝씩 교차시켜(짝수/홀수 인덱스) 인접한 색상환 각도끼리도 더 잘 구분되게 한다.
    "기타"는 별도로 항상 회색 처리한다.
    """
    sorted_codes = sorted(str(c) for c in codes if str(c).strip())
    n = len(sorted_codes)
    if n == 0:
        return {}

    color_map = {}
    for i, code in enumerate(sorted_codes):
        hue = (i * 360.0 / n) % 360
        # 인접 색상 간 대비를 높이기 위해 채도/명도를 살짝 교차
        sat = 0.62 if i % 2 == 0 else 0.75
        light = 0.52 if i % 2 == 0 else 0.42
        color_map[code] = _hsl_to_hex(hue, sat, light)
    return color_map


def fmt_krw_short(x: float) -> str:
    """
    금액을 한국식 만/억 단위로 축약. 예: 12,340,000 -> '1,234만', 150,000,000 -> '1.5억'
    (1000k 처럼 K/M 영문 단위를 쓰면 1000k=100만원이 되어 1000만원과 헷갈리므로 만/억 단위를 사용)
    """
    try:
        x = float(x)
    except Exception:
        return str(x)
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1e8:
        v = x / 1e8
        return f"{sign}{v:.2f}억" if v < 10 else f"{sign}{v:,.1f}억"
    if x >= 1e4:
        return f"{sign}{x/1e4:,.0f}만"
    return f"{sign}{x:,.0f}"


def fmt_qty_short(x: float) -> str:
    """수량은 단순 정수 콤마 표기"""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def render_pie_chart(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    qty_col: str | None = None,
    value_label: str = "금액",
    value_format: str = ",.0f",
    color_map: dict | None = None,
    height: int = 340,
    show_top_labels: bool = False,
    top_label_n: int | None = None,
    label_value_formatter=None,
):
    """
    파이차트 렌더링. 범례는 표시하지 않고, 대신 조각 안에 상품코드/값/비율 라벨을 직접 표시한다.
    툴팁에도 항목명 / 값(금액 또는 수량) / 전체 대비 비율(%)을 함께 담는다.
    value_label, value_format 으로 매출/이익/수량 등 지표에 맞는 라벨·포맷을 지정할 수 있다.
    color_map 이 주어지면 상품코드 기준 고정 색상을 사용해, 여러 파이차트에 걸쳐
    동일 상품이 항상 같은 색으로 보이도록 한다. "기타"는 항상 회색으로 고정한다.
    color_map 이 없거나 해당 라벨이 맵에 없으면 순서대로 팔레트를 배정하는 기존 방식으로 대체한다.

    height: 차트 높이(px). 크게 보고 싶을 때 키운다.
    show_top_labels: True면 조각 위에 "상품코드 / 값 / 비율%" 라벨을 직접 표시한다.
    top_label_n: 라벨을 표시할 상위 개수 제한. None이면 전체 조각에 라벨을 표시한다.
    label_value_formatter: 라벨에 쓸 값 축약 포맷 함수 (예: fmt_krw_short). 없으면 value_format으로 표시.
    """
    if df.empty or df[value_col].sum() == 0:
        st.info("데이터가 없습니다.")
        return

    total = df[value_col].sum()
    df = df.copy()
    df["_pct"] = (df[value_col] / total * 100) if total else 0

    tooltip = [
        alt.Tooltip(label_col, title="구분"),
        alt.Tooltip(value_col, title=value_label, format=value_format),
        alt.Tooltip("_pct:Q", title="비율(%)", format=".1f"),
    ]
    if qty_col and qty_col in df.columns:
        tooltip.append(alt.Tooltip(qty_col, title="판매수량", format=",.0f"))

    # ── 색상 배정: color_map이 있으면 상품코드 고정 색상, 없으면 이 차트 안에서
    #    동적으로 색상환을 균등 분할해 배정 (겹치지 않도록) ──
    domain = df[label_col].astype(str).tolist()
    fallback_labels = [d for d in domain if d != "기타" and not (color_map and d in color_map)]
    fallback_map = {}
    if fallback_labels:
        n_fb = len(fallback_labels)
        for i, lbl in enumerate(fallback_labels):
            hue = (i * 360.0 / n_fb) % 360
            fallback_map[lbl] = _hsl_to_hex(hue, 0.65, 0.5)

    color_range = []
    for d in domain:
        if d == "기타":
            color_range.append(PIE_OTHER_COLOR)
        elif color_map and d in color_map:
            color_range.append(color_map[d])
        else:
            color_range.append(fallback_map[d])

    outer_radius = max(90, height // 2 - 70)
    inner_radius = min(50, outer_radius - 30) if outer_radius > 30 else 0

    base = alt.Chart(df).encode(
        theta=alt.Theta(field=value_col, type="quantitative", stack=True),
        color=alt.Color(
            field=label_col,
            type="nominal",
            scale=alt.Scale(domain=domain, range=color_range),
            legend=None,  # 범례 제거 — 조각 위 라벨로 항목을 식별한다.
        ),
        order=alt.Order(value_col, sort="descending"),
    )

    arc = base.mark_arc(innerRadius=inner_radius, outerRadius=outer_radius).encode(tooltip=tooltip)

    layers = [arc]

    if show_top_labels:
        fmt_fn = label_value_formatter or (lambda v: format(v, value_format))
        n_limit = top_label_n if top_label_n is not None else len(df)
        rank = df[value_col].rank(method="first", ascending=False)
        df["_label"] = [
            f"{lbl}\n{fmt_fn(v)}\n({p:.1f}%)" if r <= n_limit else ""
            for lbl, v, p, r in zip(df[label_col].astype(str), df[value_col], df["_pct"], rank)
        ]
        # 라벨을 도넛 정중앙보다는 바깥쪽(70% 지점)에 둬서 조각 색 영역 안에서
        # 최대한 넓은 공간을 확보한다 (85%까지 보내면 3줄 텍스트 높이 때문에
        # 조각 밖 흰 배경과 겹쳐 안 보이는 문제가 있어 70%로 당김).
        # 검은 외곽선(stroke)은 fill과 겹쳐 글자가 두 겹으로 보이는 부작용이 있어 제거하고
        # 단색 흰 글씨로 표시한다.
        label_radius = inner_radius + (outer_radius - inner_radius) * 0.70
        text = alt.Chart(df).encode(
            theta=alt.Theta(field=value_col, type="quantitative", stack=True),
            order=alt.Order(value_col, sort="descending"),
            text=alt.Text("_label:N"),
        ).mark_text(
            radius=label_radius,
            size=11,
            fontWeight="bold",
            color="white",
            lineBreak="\n",
        )
        layers.append(text)

    chart = alt.layer(*layers).properties(title=title, height=height)
    st.altair_chart(chart, use_container_width=True)


def build_full_grouped_with_giftset(
    direct_grp: pd.DataFrame,
    gift_qty_by_code: dict,
    gift_name_by_code: dict,
) -> pd.DataFrame:
    """
    build_product_grouped() 결과(직접판매: 소매+도매 실적)에 선물세트 수량을 더한다.
    실제 회계상 매출·이익은 선물세트 자체에는 없지만(=기프트패키지 상품 매출로 이미 잡혀 있음),
    "이 상품이 판매수량 대비 얼마나 매출·이익에 기여했는지"를 보여주기 위해
    해당 상품의 직접판매 평균 단가(매출/판매량)·평균 단위이익(이익/판매량)에
    선물세트 수량을 곱한 값을 "추정 매출/이익"으로 직접판매 실적에 더한다.
    직접판매 이력이 전혀 없는 상품은 단가를 알 수 없어 추정 매출/이익을 0으로 처리한다.
    """
    base = {}
    if not direct_grp.empty:
        for _, r in direct_grp.iterrows():
            code = str(r["상품코드"])
            base[code] = {
                "product_name": r.get("product_name", code),
                "판매량": float(r.get("판매량", 0) or 0),
                "매출": float(r.get("매출", 0) or 0),
                "이익": float(r.get("이익", 0) or 0),
            }

    all_codes = set(base.keys()) | set(gift_qty_by_code.keys())
    rows = []
    for code in all_codes:
        b = base.get(
            code,
            {"product_name": gift_name_by_code.get(code, code), "판매량": 0.0, "매출": 0.0, "이익": 0.0},
        )
        direct_qty = b["판매량"]
        direct_sales = b["매출"]
        direct_profit = b["이익"]
        avg_price = (direct_sales / direct_qty) if direct_qty else 0.0
        avg_profit = (direct_profit / direct_qty) if direct_qty else 0.0
        gift_qty = float(gift_qty_by_code.get(code, 0) or 0)

        total_qty = direct_qty + gift_qty
        est_sales = direct_sales + avg_price * gift_qty
        est_profit = direct_profit + avg_profit * gift_qty

        rows.append({
            "상품코드": code,
            "product_name": b["product_name"],
            "판매량": total_qty,
            "매출": est_sales,
            "이익": est_profit,
        })

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["상품코드", "product_name", "판매량", "매출", "이익", "마진율(%)", "개당마진금액"])

    result["마진율(%)"] = result.apply(
        lambda x: round(x["이익"] / x["매출"] * 100, 1) if x["매출"] else 0, axis=1
    )
    result["개당마진금액"] = result.apply(
        lambda x: round(x["이익"] / x["판매량"], 0) if x["판매량"] else 0, axis=1
    )
    return result.sort_values("매출", ascending=False).reset_index(drop=True)


def make_full_pie_df(
    grp: pd.DataFrame,
    value_col_name: str,
    qty_col_name: str | None = "판매량",
    top_n: int = 10,
) -> pd.DataFrame:
    """
    상품별 집계(product_grouped: 소매+도매+선물세트 통합) 기준으로
    특정 지표(매출/이익/판매량) 파이차트용 DataFrame 을 만든다.
    qty_col_name 이 value_col_name 과 같으면(예: 판매량 자체를 값으로 쓰는 경우)
    중복 표시를 피하기 위해 수량 툴팁은 생략한다.
    """
    if grp.empty or "상품코드" not in grp.columns or value_col_name not in grp.columns:
        cols = ["구분", "값"]
        if qty_col_name and qty_col_name != value_col_name:
            cols.append("판매수량")
        return pd.DataFrame(columns=cols)

    include_qty = bool(qty_col_name) and qty_col_name != value_col_name and qty_col_name in grp.columns

    cols_needed = ["상품코드", value_col_name]
    if include_qty:
        cols_needed.append(qty_col_name)

    work = grp[cols_needed].copy()
    rename_map = {"상품코드": "구분", value_col_name: "값"}
    if include_qty:
        rename_map[qty_col_name] = "판매수량"
    work = work.rename(columns=rename_map)

    return group_minor_as_others(
        work,
        label_col="구분",
        value_col="값",
        qty_col="판매수량" if include_qty else None,
        top_n=top_n,
    )


def get_cigar_product_codes(conn) -> set:
    if not table_exists(conn, "product_mst"):
        return set()

    df = pd.read_sql_query(
        """
        SELECT DISTINCT UPPER(TRIM(COALESCE(product_code, ''))) AS product_code
        FROM product_mst
        WHERE TRIM(COALESCE(product_code, '')) <> ''
        """,
        conn,
    )
    return set(df["product_code"].dropna().tolist())


def get_retail_brand_product_data(conn, date_from: str | None, date_to: str | None) -> pd.DataFrame:
    if not view_exists(conn, "v_retail_sales_enriched"):
        return pd.DataFrame(
            columns=["brand", "product_code", "product_name", "qty", "sales", "profit"]
        )

    if date_from and date_to:
        where = "WHERE sale_date BETWEEN ? AND ?"
        params = [date_from, date_to]
    else:
        where = ""
        params = []

    sql = f"""
    SELECT
        COALESCE(category, '미분류') AS brand,
        COALESCE(product_code, product_code_raw, '') AS product_code,
        COALESCE(mst_product_name, product_code_raw, '미분류') AS product_name,
        COALESCE(qty, 0) AS qty,
        COALESCE(net_sales_amount, 0) AS sales,
        COALESCE(retail_gross_profit_krw, 0) AS profit
    FROM v_retail_sales_enriched
    {where}
    """
    df = pd.read_sql_query(sql, conn, params=params)

    for c in ["qty", "sales", "profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    return df


def get_wholesale_brand_product_data(conn, date_from: str | None, date_to: str | None) -> pd.DataFrame:
    source = None
    if view_exists(conn, "v_wholesale_sales"):
        source = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        source = "wholesale_sales"

    if not source:
        return pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    if date_from and date_to:
        where = "WHERE sale_date BETWEEN ? AND ?"
        params = [date_from, date_to]
    else:
        where = ""
        params = []

    sql = f"""
    SELECT
        COALESCE(item_type, '미분류') AS brand,
        COALESCE(product_code, '') AS product_code,
        COALESCE(product_name, product_code, '미분류') AS product_name,
        COALESCE(qty, 0) AS qty,
        COALESCE(sales_amount, 0) AS sales,
        COALESCE(profit_amount, 0) AS profit
    FROM {source}
    {where}
    """

    df = pd.read_sql_query(sql, conn, params=params)

    for c in ["qty", "sales", "profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    return df


def get_cigar_brand_map(conn, cigar_codes: set) -> dict:
    """
    product_code -> brand(category) 매핑.
    선물세트로 빠져나간 시가 수량에 브랜드를 붙이기 위해 retail_sales 기준으로
    상품코드별 대표 브랜드(category)를 조회한다. (기간 무관, 전체 이력 기준)
    """
    if not view_exists(conn, "v_retail_sales_enriched"):
        return {}

    df = pd.read_sql_query(
        """
        SELECT
            UPPER(TRIM(COALESCE(product_code, product_code_raw, ''))) AS product_code,
            COALESCE(category, '미분류') AS brand
        FROM v_retail_sales_enriched
        WHERE COALESCE(product_code, product_code_raw, '') <> ''
        """,
        conn,
    )
    if df.empty:
        return {}

    df["product_code"] = normalize_code(df["product_code"])
    df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
    df.loc[df["brand"] == "", "brand"] = "미분류"

    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)]

    df = df.drop_duplicates(subset=["product_code"], keep="first")
    return dict(zip(df["product_code"], df["brand"]))


def get_gift_set_cigar_out(
    conn, cigar_codes: set, date_from: str | None, date_to: str | None
) -> pd.DataFrame:
    """
    선물세트(gift_set)로 차감된 시가 상품 수량.
    매출/이익은 이미 기프트패키지 상품(non_cigar) 판매에 반영되어 있으므로
    여기서는 수량만 집계하고 sales/profit은 0으로 둔다 (이중계산 방지).
    """
    empty = pd.DataFrame(columns=["brand", "product_code", "product_name", "qty", "sales", "profit"])

    if not table_exists(conn, "stock_out"):
        return empty

    where = "WHERE so.out_type = 'gift_set'"
    params = []
    if date_from and date_to:
        where += " AND so.out_date BETWEEN ? AND ?"
        params = [date_from, date_to]

    sql = f"""
    SELECT
        UPPER(TRIM(COALESCE(so.product_code, ''))) AS product_code,
        COALESCE(pm.product_name, so.product_code, '미분류') AS product_name,
        SUM(so.qty) AS qty
    FROM stock_out so
    LEFT JOIN product_mst pm ON so.product_code = pm.product_code
    {where}
    GROUP BY so.product_code
    """
    df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        return empty

    df["product_code"] = normalize_code(df["product_code"])
    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)].copy()
    else:
        df = df.iloc[0:0].copy()

    if df.empty:
        return empty

    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["sales"] = 0
    df["profit"] = 0
    df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
    df.loc[df["product_name"] == "", "product_name"] = "미분류"
    df["brand"] = "미분류"  # 아래에서 get_cigar_brand_map 으로 실제 브랜드 매핑

    return df[["brand", "product_code", "product_name", "qty", "sales", "profit"]]


def _get_cigar_stock_base(cigar_codes: set) -> pd.DataFrame:
    """재고관리 데이터에서 시가 제품만 추출 (내부 공통 함수)"""
    try:
        df = get_stock_summary(keyword="", include_inactive=False)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df["product_code"] = (
        df["product_code"].fillna("").astype(str).str.strip().str.upper()
    )
    if cigar_codes:
        df = df[df["product_code"].isin(cigar_codes)].copy()

    if df.empty:
        return pd.DataFrame()

    for col in ["total_in", "retail_out", "wholesale_out", "other_out", "current_stock"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["total_out"] = (
        df.get("retail_out", 0)
        + df.get("wholesale_out", 0)
        + df.get("other_out", 0)
    )
    df["label"] = df.apply(
        lambda r: f"{r['product_code']}"
        + (f"\n({r['size_name']})" if pd.notna(r.get("size_name")) and str(r.get("size_name", "")).strip() else ""),
        axis=1,
    )
    return df.reset_index(drop=True)


def get_cigar_stock_chart_data(cigar_codes: set) -> pd.DataFrame:
    """재고 현황 그래프용 DataFrame"""
    df = _get_cigar_stock_base(cigar_codes)
    if df.empty:
        return df
    return df.sort_values("total_out", ascending=False).reset_index(drop=True)


def get_cigar_monthly_avg_data(
    conn,
    cigar_codes: set,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """
    지정 기간의 소매+도매 출고 합산으로 제품별 월평균 출고수를 계산하고
    재고관리의 현재고와 결합하여 잔여 개월 수를 반환.

    반환 컬럼:
      product_code, product_name, size_name, label,
      period_out      : 기간 내 총 출고수
      months          : 기간 월수
      monthly_avg     : 월평균 출고수
      current_stock   : 현재고 (재고관리 기준)
      remaining_months: 현재고 ÷ 월평균 출고수 (소진 예상 개월)
    """
    # ── 1. 기간 내 소매 출고 ──
    retail_rows = pd.DataFrame()
    if view_exists(conn, "v_retail_sales_enriched"):
        try:
            retail_rows = pd.read_sql_query(
                """
                SELECT
                    UPPER(TRIM(COALESCE(product_code, product_code_raw, ''))) AS product_code,
                    strftime('%Y-%m', sale_date) AS ym,
                    SUM(COALESCE(qty, 0)) AS qty
                FROM v_retail_sales_enriched
                WHERE sale_date BETWEEN ? AND ?
                GROUP BY product_code, ym
                """,
                conn, params=[date_from, date_to],
            )
        except Exception:
            pass

    # ── 2. 기간 내 도매 출고 ──
    wholesale_rows = pd.DataFrame()
    ws_src = None
    if view_exists(conn, "v_wholesale_sales"):
        ws_src = "v_wholesale_sales"
    elif table_exists(conn, "wholesale_sales"):
        ws_src = "wholesale_sales"

    if ws_src:
        try:
            wholesale_rows = pd.read_sql_query(
                f"""
                SELECT
                    UPPER(TRIM(COALESCE(product_code, ''))) AS product_code,
                    strftime('%Y-%m', sale_date) AS ym,
                    SUM(COALESCE(qty, 0)) AS qty
                FROM {ws_src}
                WHERE sale_date BETWEEN ? AND ?
                GROUP BY product_code, ym
                """,
                conn, params=[date_from, date_to],
            )
        except Exception:
            pass

    frames = [f for f in [retail_rows, wholesale_rows] if not f.empty]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["qty"] = pd.to_numeric(combined["qty"], errors="coerce").fillna(0)
    combined["product_code"] = (
        combined["product_code"].fillna("").astype(str).str.strip().str.upper()
    )

    # 시가 제품만 필터
    if cigar_codes:
        combined = combined[combined["product_code"].isin(cigar_codes)].copy()

    if combined.empty:
        return pd.DataFrame()

    # ── 3. 월수 계산 ──
    ts_from = pd.Timestamp(date_from)
    ts_to   = pd.Timestamp(date_to)
    months  = (ts_to.year - ts_from.year) * 12 + (ts_to.month - ts_from.month) + 1
    months  = max(months, 1)

    # ── 4. 제품별 기간 총 출고 ──
    period_out = (
        combined.groupby("product_code", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "period_out"})
    )
    period_out["monthly_avg"] = (period_out["period_out"] / months).round(1)

    # ── 5. 현재고 결합 (재고관리 기준) ──
    stock_base = _get_cigar_stock_base(cigar_codes)

    if stock_base.empty:
        result = period_out.copy()
        result["product_name"] = result["product_code"]
        result["size_name"]    = ""
        result["label"]        = result["product_code"]
        result["current_stock"] = 0
    else:
        # product_code 기준 조인 (같은 코드에 사이즈 여러 개면 sum)
        stock_agg = (
            stock_base.groupby("product_code", as_index=False)
            .agg(
                product_name=("product_name", "first"),
                size_name=("size_name", "first"),
                label=("label", "first"),
                current_stock=("current_stock", "sum"),
            )
        )
        result = period_out.merge(stock_agg, on="product_code", how="left")
        result["product_name"]  = result["product_name"].fillna(result["product_code"])
        result["size_name"]     = result["size_name"].fillna("")
        result["label"]         = result["label"].fillna(result["product_code"])
        result["current_stock"] = result["current_stock"].fillna(0)

    result["months"] = months

    # ── 6. 잔여 개월 수 ──
    def calc_remaining(row):
        if row["monthly_avg"] <= 0:
            return None   # 출고 없음 → 계산 불가
        return round(row["current_stock"] / row["monthly_avg"], 1)

    result["remaining_months"] = result.apply(calc_remaining, axis=1)

    return result.sort_values("monthly_avg", ascending=False).reset_index(drop=True)


def render():
    st.subheader("브랜드 분석")

    conn = get_conn()
    try:
        today = pd.Timestamp.today()
        current_year = today.year
        current_month = today.month

        # ── 기간 선택 ──────────────────────────────────────────────
        period_mode = st.radio(
            "기간 선택",
            options=["월별", "전체 기간"],
            horizontal=True,
            key="brand_analysis_period_mode",
        )

        date_from: str | None = None
        date_to: str | None = None
        period_label = "전체 기간"

        if period_mode == "월별":
            c1, c2 = st.columns(2)
            with c1:
                year = st.selectbox(
                    "연도",
                    options=list(range(current_year - 2, current_year + 1)),
                    index=2,
                    key="brand_analysis_year",
                )
            with c2:
                month = st.selectbox(
                    "월",
                    options=list(range(1, 13)),
                    index=current_month - 1,
                    key="brand_analysis_month",
                )

            start_date = pd.Timestamp(year=year, month=month, day=1)
            end_date = start_date + pd.offsets.MonthEnd(1)
            date_from = start_date.strftime("%Y-%m-%d")
            date_to = end_date.strftime("%Y-%m-%d")
            period_label = f"{year}-{month:02d}"

        # ── 데이터 조회 ────────────────────────────────────────────
        retail_df = get_retail_brand_product_data(conn, date_from, date_to)
        wholesale_df = get_wholesale_brand_product_data(conn, date_from, date_to)

        # ── 시가 상품만 대상으로 필터링 (재고관리 총출고와 집계 기준 통일) ──
        cigar_codes = get_cigar_product_codes(conn)

        # 상품코드 -> 고정 색상 매핑 (모든 파이차트에서 동일 상품 = 동일 색)
        pie_color_map = build_product_color_map(cigar_codes)

        def _filter_cigar_src(src: pd.DataFrame) -> pd.DataFrame:
            if src.empty:
                return src.copy()
            src = src.copy()
            src["product_code"] = normalize_code(src["product_code"])
            if not cigar_codes:
                return src.iloc[0:0].copy()
            return src[src["product_code"].isin(cigar_codes)].copy()

        retail_df = _filter_cigar_src(retail_df)
        wholesale_df = _filter_cigar_src(wholesale_df)

        # ── 선물세트로 차감된 시가 수량 추가 (매출·이익은 0, 수량만) ──
        # 이미 기프트패키지(non_cigar) 상품 판매로 매출/이익이 잡혀 있으므로
        # 여기서는 "판매수량"에만 반영해 재고관리 총출고 수와 기준을 맞춘다.
        gift_set_df = get_gift_set_cigar_out(conn, cigar_codes, date_from, date_to)
        if not gift_set_df.empty:
            brand_map = get_cigar_brand_map(conn, cigar_codes)
            gift_set_df["brand"] = (
                gift_set_df["product_code"].map(brand_map).fillna("미분류")
            )

        frames = []
        if not retail_df.empty:
            frames.append(retail_df)
        if not wholesale_df.empty:
            frames.append(wholesale_df)
        if not gift_set_df.empty:
            frames.append(gift_set_df)

        if not frames:
            st.warning("시가 상품 데이터가 없습니다.")
            return

        df = pd.concat(frames, ignore_index=True)
        df["brand"] = df["brand"].fillna("미분류").astype(str).str.strip()
        df.loc[df["brand"] == "", "brand"] = "미분류"
        df["product_code"] = normalize_code(df["product_code"])
        df["product_name"] = df["product_name"].fillna("미분류").astype(str).str.strip()
        df.loc[df["product_name"] == "", "product_name"] = "미분류"

        if df.empty:
            st.warning("시가 상품 데이터가 없습니다.")
            return

        brand_grouped = (
            df.groupby("brand", dropna=False)
            .agg(
                판매량=("qty", "sum"),
                매출=("sales", "sum"),
                이익=("profit", "sum"),
            )
            .reset_index()
            .rename(columns={"brand": "브랜드"})
        )
        brand_grouped["마진율(%)"] = brand_grouped.apply(
            lambda x: round((x["이익"] / x["매출"] * 100), 1) if x["매출"] else 0,
            axis=1,
        )
        brand_grouped = brand_grouped.sort_values("매출", ascending=False).reset_index(drop=True)

        # retail_df / wholesale_df 는 이미 시가 상품만 필터링된 상태
        # 상품별 소매/도매 차트는 선물세트분(gift_set_df)을 굳이 섞지 않음
        # (소매/도매 매출 비중 차트의 목적과 맞지 않으므로) — 전체 판매수량 KPI에는 반영됨
        retail_cigar_df = retail_df
        wholesale_cigar_df = wholesale_df
        cigar_df = df  # 선물세트분 포함된 전체 (KPI/전체상품 집계용)

        def build_product_grouped(src: pd.DataFrame) -> pd.DataFrame:
            if src.empty:
                return pd.DataFrame(
                    columns=[
                        "product_code",
                        "product_name",
                        "판매량",
                        "매출",
                        "이익",
                        "상품코드",
                        "마진율(%)",
                        "개당마진금액",
                    ]
                )

            grp = (
                src.groupby(["product_code", "product_name"], dropna=False)
                .agg(판매량=("qty", "sum"), 매출=("sales", "sum"), 이익=("profit", "sum"))
                .reset_index()
            )
            grp["상품코드"] = grp["product_code"].fillna("").astype(str).str.strip()
            grp.loc[grp["상품코드"] == "", "상품코드"] = grp["product_name"]
            grp["마진율(%)"] = grp.apply(
                lambda x: round(x["이익"] / x["매출"] * 100, 1) if x["매출"] else 0,
                axis=1
            )
            grp["개당마진금액"] = grp.apply(
                lambda x: round(x["이익"] / x["판매량"], 0) if x["판매량"] else 0,
                axis=1
            )
            return grp.sort_values("매출", ascending=False).reset_index(drop=True)

        retail_product_grouped = build_product_grouped(retail_cigar_df)
        wholesale_product_grouped = build_product_grouped(wholesale_cigar_df)

        # ── 상품별 "직접 판매" 집계 (소매+도매만, 선물세트 제외) ──
        # 파이차트/바차트처럼 상품 간 매출·이익·판매량을 비교하는 화면에서는
        # 선물세트 차감분(매출·이익 0)을 섞으면 "많이 팔렸는데 이익이 없다"는
        # 왜곡이 생기므로, 상품별 비교용 집계는 반드시 direct 판매분만 사용한다.
        direct_frames = []
        if not retail_cigar_df.empty:
            direct_frames.append(retail_cigar_df)
        if not wholesale_cigar_df.empty:
            direct_frames.append(wholesale_cigar_df)
        direct_df = (
            pd.concat(direct_frames, ignore_index=True) if direct_frames else pd.DataFrame(columns=df.columns)
        )
        product_grouped = build_product_grouped(direct_df)

        total_sales = brand_grouped["매출"].sum()
        total_profit = brand_grouped["이익"].sum()
        total_qty = brand_grouped["판매량"].sum()
        cigar_product_count = cigar_df["product_code"].nunique() if not cigar_df.empty else 0
        gift_set_qty = gift_set_df["qty"].sum() if not gift_set_df.empty else 0

        # ── 파이차트 전용: 선물세트 수량을 포함하고, 매출·이익은
        # 해당 상품의 직접판매 평균 단가/단위이익 × 선물세트 수량으로 추정해 더한다.
        # (KPI 카드와 위의 product_grouped(바차트용)는 실제 판매분만 사용해 그대로 둔다)
        # 선물세트는 채널 구분 정보가 없어 소매 채널로 간주해 소매 파이에 포함한다.
        gift_qty_by_code = (
            gift_set_df.groupby("product_code")["qty"].sum().to_dict()
            if not gift_set_df.empty else {}
        )
        gift_name_by_code = (
            gift_set_df.drop_duplicates("product_code")
            .set_index("product_code")["product_name"].to_dict()
            if not gift_set_df.empty else {}
        )
        retail_full_grp = build_full_grouped_with_giftset(
            apply_discount_to_grouped(retail_product_grouped), gift_qty_by_code, gift_name_by_code
        )
        combined_full_grp = build_full_grouped_with_giftset(
            apply_discount_to_grouped(product_grouped), gift_qty_by_code, gift_name_by_code
        )
        # 도매 파이는 선물세트 병합이 없어 할인만 적용한 별도 사본을 사용한다.
        wholesale_pie_grp = apply_discount_to_grouped(wholesale_product_grouped)

        # ── KPI 카드 ───────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("총매출", fmt_krw(total_sales))
        k2.metric("총이익", fmt_krw(total_profit))
        k3.metric("총판매수량", f"{int(total_qty):,}개")
        k4.metric("시가 제품 수", f"{cigar_product_count:,}")

        st.caption(
            f"기준: {period_label} · 시가 상품만 집계"
            + (f" (KPI 총판매수량은 선물세트 차감 {int(gift_set_qty):,}개 포함 — 매출은 기프트패키지 판매에 반영됨)"
               if gift_set_qty else "")
        )
        st.caption(
            "※ 아래 상품별 파이차트·바차트는 소매+도매 직접 판매분만 집계합니다 "
            "(선물세트는 매출·이익이 0으로 잡혀 상품별 비교를 왜곡하므로 제외)."
        )
        st.divider()

        # ── 파이차트 ───────────────────────────────────────────────
        def make_product_pie_df(grp: pd.DataFrame) -> pd.DataFrame:
            if grp.empty:
                return pd.DataFrame(columns=["구분", "금액", "판매수량"])
            renamed = grp.rename(columns={"상품코드": "구분", "매출": "금액", "판매량": "판매수량"})
            return group_minor_as_others(
                renamed,
                label_col="구분",
                value_col="금액",
                qty_col="판매수량",
                top_n=10,
            )

        st.caption(
            "소매 파이는 선물세트 수량을 포함합니다(선물세트는 채널 구분이 없어 소매로 간주, "
            "매출·이익은 상품별 직접판매 평균 단가·단위이익 기반 추정치). "
            "도매는 선물세트 개념이 없어 도매 직접판매만 집계합니다."
        )
        render_pie_chart(
            make_product_pie_df(retail_full_grp),
            label_col="구분",
            value_col="금액",
            qty_col="판매수량",
            title="시가상품별 매출금액 비중 (소매, 선물세트 포함 추정)",
            value_label="매출금액",
            color_map=pie_color_map,
            height=560,
            show_top_labels=True,
            label_value_formatter=fmt_krw_short,
        )
        render_pie_chart(
            make_product_pie_df(wholesale_pie_grp),
            label_col="구분",
            value_col="금액",
            qty_col="판매수량",
            title="시가상품별 매출금액 비중 (도매)",
            value_label="매출금액",
            color_map=pie_color_map,
            height=560,
            show_top_labels=True,
            label_value_formatter=fmt_krw_short,
        )

        st.divider()

        # ── 통합(소매+도매+선물세트) 지표별 파이차트 ─────────────────
        st.markdown("### 소매·도매 통합 지표별 비중 (선물세트 수량 포함, 매출·이익은 추정치)")
        st.caption(
            "판매수량은 선물세트로 나간 수량까지 포함합니다. 매출·이익은 선물세트분에 대해 "
            "실제 회계상 매출이 없는 대신, 해당 상품의 직접판매 평균 단가·단위이익 × 선물세트 수량으로 "
            "추정해 더한 값입니다(직접판매 이력이 없는 상품은 추정 불가로 0 처리). "
            "모든 조각 위에 상품코드·값·비율을 함께 표시합니다."
        )

        sales_pie_all = make_full_pie_df(combined_full_grp, "매출", "판매량")
        profit_pie_all = make_full_pie_df(combined_full_grp, "이익", "판매량")
        qty_pie_all = make_full_pie_df(combined_full_grp, "판매량", None)

        render_pie_chart(
            sales_pie_all,
            label_col="구분",
            value_col="값",
            qty_col="판매수량",
            title="시가상품별 매출금액 비중 (전체)",
            value_label="매출금액",
            color_map=pie_color_map,
            height=560,
            show_top_labels=True,
            label_value_formatter=fmt_krw_short,
        )
        render_pie_chart(
            profit_pie_all,
            label_col="구분",
            value_col="값",
            qty_col="판매수량",
            title="시가상품별 이익 비중 (전체)",
            value_label="이익금액",
            color_map=pie_color_map,
            height=560,
            show_top_labels=True,
            label_value_formatter=fmt_krw_short,
        )
        render_pie_chart(
            qty_pie_all,
            label_col="구분",
            value_col="값",
            qty_col=None,
            title="시가상품별 판매수량 비중 (전체)",
            value_label="판매수량",
            value_format=",.0f",
            color_map=pie_color_map,
            height=560,
            show_top_labels=True,
            label_value_formatter=fmt_qty_short,
        )

        st.divider()

        # ── 바차트 ─────────────────────────────────────────────────
        b1, b2 = st.columns(2)

        if product_grouped.empty:
            top_product_sales = pd.DataFrame(columns=["상품코드", "매출", "판매량"])
            top_product_unit_profit = pd.DataFrame(columns=["상품코드", "개당마진금액", "판매량"])
        else:
            top_product_sales = product_grouped.sort_values("매출", ascending=False).head(20).copy()
            top_product_unit_profit = product_grouped.sort_values("개당마진금액", ascending=False).head(20).copy()

        with b1:
            st.markdown("### 시가상품별 매출금액 (TOP 20)")
            if top_product_sales.empty:
                st.info("시가상품 데이터가 없습니다.")
            else:
                chart_df = (
                    top_product_sales[["상품코드", "매출", "판매량"]]
                    .sort_values("매출", ascending=False)
                    .copy()
                )

                chart = (
                    alt.Chart(chart_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "상품코드:N",
                            sort=chart_df["상품코드"].tolist(),
                            title="상품코드",
                        ),
                        y=alt.Y("매출:Q", title="매출금액"),
                        tooltip=[
                            alt.Tooltip("상품코드:N", title="상품코드"),
                            alt.Tooltip("매출:Q", title="매출금액", format=",.0f"),
                            alt.Tooltip("판매량:Q", title="판매수량", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)

        with b2:
            st.markdown("### 시가상품별 개당 마진금액 (TOP 20)")
            if top_product_unit_profit.empty:
                st.info("시가상품 데이터가 없습니다.")
            else:
                chart_df = (
                    top_product_unit_profit[["상품코드", "개당마진금액", "판매량"]]
                    .sort_values("개당마진금액", ascending=False)
                    .copy()
                )

                chart = (
                    alt.Chart(chart_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "상품코드:N",
                            sort=chart_df["상품코드"].tolist(),
                            title="상품코드",
                        ),
                        y=alt.Y("개당마진금액:Q", title="개당 마진금액"),
                        tooltip=[
                            alt.Tooltip("상품코드:N", title="상품코드"),
                            alt.Tooltip("개당마진금액:Q", title="개당 마진금액", format=",.0f"),
                            alt.Tooltip("판매량:Q", title="판매수량", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)

        st.divider()

        # ── 시가 제품별 월평균 출고 & 재고 예측 ────────────────────
        st.markdown("### 시가 제품별 월평균 출고 & 재고 예측")
        st.caption("기프트패키지 등 비시가 제품 제외 · 소매+도매 출고 합산 기준")

        # 기간 선택 (브랜드 분석 기간과 독립)
        inv_c1, inv_c2, inv_c3 = st.columns([2, 2, 3])
        with inv_c1:
            inv_date_from = st.date_input(
                "분석 시작일",
                value=pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=5),
                key="inv_avg_date_from",
            )
        with inv_c2:
            inv_date_to = st.date_input(
                "분석 종료일",
                value=pd.Timestamp.today(),
                key="inv_avg_date_to",
            )
        with inv_c3:
            st.markdown("")  # 여백

        if inv_date_from > inv_date_to:
            st.warning("시작일이 종료일보다 늦습니다.")
        else:
            inv_from_str = inv_date_from.strftime("%Y-%m-%d")
            inv_to_str   = inv_date_to.strftime("%Y-%m-%d")

            ts_from = pd.Timestamp(inv_from_str)
            ts_to   = pd.Timestamp(inv_to_str)
            n_months = (ts_to.year - ts_from.year) * 12 + (ts_to.month - ts_from.month) + 1

            st.caption(
                f"기간: {inv_from_str} ~ {inv_to_str}  |  기준 월수: **{n_months}개월**"
            )

            avg_df = get_cigar_monthly_avg_data(
                conn, cigar_codes, inv_from_str, inv_to_str
            )

            if avg_df.empty:
                st.info("해당 기간의 시가 제품 출고 데이터가 없습니다.")
            else:
                # ── KPI ──
                k1, k2, k3 = st.columns(3)
                k1.metric("분석 제품 수", f"{len(avg_df):,} 개")
                k2.metric("기간 내 총 출고", f"{int(avg_df['period_out'].sum()):,} 개비")
                k3.metric(
                    "재고 소진 임박 (2개월 이내)",
                    f"{int((avg_df['remaining_months'].dropna() <= 2).sum()):,} 개",
                )

                st.divider()

                # ── 탭: 월평균 출고 / 잔여 개월 ──
                t_avg, t_remain = st.tabs(["📊 월평균 출고수", "📅 재고 소진 예측"])

                with t_avg:
                    max_n = min(len(avg_df), 50)
                    top_n = st.slider(
                        "표시 제품 수 (월평균 출고 기준 상위)",
                        min_value=5, max_value=max_n,
                        value=min(20, max_n), step=5,
                        key="inv_avg_top_n",
                    )
                    chart_avg = avg_df.head(top_n).copy()
                    sorted_labels = chart_avg.sort_values(
                        "monthly_avg", ascending=False
                    )["label"].tolist()

                    bar_avg = (
                        alt.Chart(chart_avg)
                        .mark_bar(color="#4C9BE8")
                        .encode(
                            x=alt.X(
                                "label:N", sort=sorted_labels,
                                title="상품코드",
                                axis=alt.Axis(labelAngle=-40, labelFontSize=10),
                            ),
                            y=alt.Y("monthly_avg:Q", title="월평균 출고수 (개비)"),
                            tooltip=[
                                alt.Tooltip("product_code:N", title="상품코드"),
                                alt.Tooltip("product_name:N", title="상품명"),
                                alt.Tooltip("size_name:N",    title="사이즈"),
                                alt.Tooltip("period_out:Q",   title=f"{n_months}개월 총 출고", format=",.0f"),
                                alt.Tooltip("monthly_avg:Q",  title="월평균 출고수", format=",.1f"),
                                alt.Tooltip("current_stock:Q",title="현재고", format=",.0f"),
                            ],
                        )
                        .properties(height=400)
                    )
                    text_avg = bar_avg.mark_text(
                        align="center", baseline="bottom", dy=-3, fontSize=10
                    ).encode(text=alt.Text("monthly_avg:Q", format=".1f"))
                    st.altair_chart(bar_avg + text_avg, use_container_width=True)

                with t_remain:
                    # remaining_months 가 None(출고 없음)인 경우 별도 안내
                    no_out = avg_df[avg_df["remaining_months"].isna()].copy()
                    has_out = avg_df[avg_df["remaining_months"].notna()].copy()

                    if has_out.empty:
                        st.info("출고 이력이 있는 제품이 없습니다.")
                    else:
                        has_out["remaining_months"] = has_out["remaining_months"].clip(lower=0)
                        has_out["_status"] = has_out["remaining_months"].apply(
                            lambda v: "🔴 1개월 이내" if v <= 1
                            else ("🟠 2개월 이내" if v <= 2
                            else ("🟡 3개월 이내" if v <= 3
                            else "🟢 3개월 초과"))
                        )

                        max_n2 = min(len(has_out), 50)
                        top_n2 = st.slider(
                            "표시 제품 수 (잔여 개월 적은 순)",
                            min_value=5, max_value=max_n2,
                            value=min(20, max_n2), step=5,
                            key="inv_remain_top_n",
                        )
                        # 잔여 개월 오름차순 정렬 (소진 임박 우선)
                        chart_rem = (
                            has_out.sort_values("remaining_months", ascending=True)
                            .head(top_n2)
                            .copy()
                        )
                        sorted_rem = chart_rem["label"].tolist()

                        bar_rem = (
                            alt.Chart(chart_rem)
                            .mark_bar()
                            .encode(
                                x=alt.X(
                                    "label:N", sort=sorted_rem,
                                    title="상품코드",
                                    axis=alt.Axis(labelAngle=-40, labelFontSize=10),
                                ),
                                y=alt.Y("remaining_months:Q", title="잔여 개월 수"),
                                color=alt.Color(
                                    "_status:N",
                                    scale=alt.Scale(
                                        domain=["🔴 1개월 이내", "🟠 2개월 이내",
                                                "🟡 3개월 이내", "🟢 3개월 초과"],
                                        range=["#e53935", "#FB8C00", "#FDD835", "#43A047"],
                                    ),
                                    legend=alt.Legend(title="재고 상태"),
                                ),
                                tooltip=[
                                    alt.Tooltip("product_code:N",   title="상품코드"),
                                    alt.Tooltip("product_name:N",   title="상품명"),
                                    alt.Tooltip("size_name:N",      title="사이즈"),
                                    alt.Tooltip("current_stock:Q",  title="현재고", format=",.0f"),
                                    alt.Tooltip("monthly_avg:Q",    title="월평균 출고수", format=",.1f"),
                                    alt.Tooltip("remaining_months:Q", title="잔여 개월", format=".1f"),
                                    alt.Tooltip("_status:N",        title="상태"),
                                ],
                            )
                            .properties(height=400)
                        )
                        text_rem = bar_rem.mark_text(
                            align="center", baseline="bottom", dy=-3, fontSize=10
                        ).encode(text=alt.Text("remaining_months:Q", format=".1f"))
                        st.altair_chart(bar_rem + text_rem, use_container_width=True)

                        if not no_out.empty:
                            st.caption(
                                "※ 기간 내 출고 이력 없음 → 잔여 개월 계산 불가 제품: "
                                + ", ".join(no_out["product_code"].tolist())
                            )

                # ── 상세 테이블 ──
                with st.expander("전체 상세 테이블"):
                    tbl = avg_df[[
                        "product_code", "product_name", "size_name",
                        "current_stock", "period_out", "monthly_avg", "remaining_months",
                    ]].copy()
                    tbl.columns = [
                        "상품코드", "상품명", "사이즈",
                        "현재고", f"{n_months}개월 총출고", "월평균 출고수", "잔여 개월",
                    ]
                    tbl["월평균 출고수"] = tbl["월평균 출고수"].map(lambda x: f"{x:.1f}")
                    tbl["잔여 개월"] = tbl["잔여 개월"].map(
                        lambda x: f"{x:.1f}" if pd.notna(x) else "-"
                    )
                    tbl["현재고"] = tbl["현재고"].map(lambda x: f"{x:,.0f}")
                    tbl[f"{n_months}개월 총출고"] = tbl[f"{n_months}개월 총출고"].map(
                        lambda x: f"{x:,.0f}"
                    )
                    st.dataframe(tbl, use_container_width=True, hide_index=True)

    finally:
        conn.close()


if __name__ == "__main__":
    render()