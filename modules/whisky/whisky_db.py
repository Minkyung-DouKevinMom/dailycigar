"""
위스키(개인사업자) 데이터 접근 — 정본.

위스키는 데일리시가(법인)와 사업자가 달라 장부를 섞지 않는다.
- 데이터는 cigar.db 가 아니라 별도 파일 whisky.db 에만 저장한다.
- 위스키 화면/스크립트는 반드시 이 모듈의 함수만 사용한다 (화면 파일에 SQL 을 새로 쓰지 않는다).

DB 경로 우선순위:
  1) 환경변수 WHISKY_DB_PATH
  2) 기본값 "whisky.db" (앱 실행 위치 기준 상대경로 — cigar.db 와 같은 방식)

매출/이익 기준(파일 아래쪽 '매출 / 이익 (정본)' 참고):
- 매출 = 판매금액(결제금액) ÷ 1.1, 원가 = 차감 ml × ml당 평균 매입가, 마진 = 매출 − 원가.

재고 기준:
- 재고는 용량(ml)으로 관리한다. 매입 = 병 수 × 병 용량, 병 판매 = 병 수 × 병 용량,
  잔 판매 = 잔 수 × 잔 용량(기본 30ml).
- 매입/출고 행에 등록 시점의 ml 를 함께 저장하므로, 나중에 상품의 병·잔 용량을 고쳐도
  과거 재고가 바뀌지 않는다.
- 화면의 '미개봉 N병 + 개봉 X ml' 은 총 잔량을 병 용량으로 나눈 값이다
  (개봉한 병은 한 번에 한 병이라고 본다).
"""
from __future__ import annotations

import os
import sqlite3
from typing import Optional

import pandas as pd

DEFAULT_DB_PATH = "whisky.db"
DEFAULT_BOTTLE_ML = 700.0
DEFAULT_GLASS_ML = 30.0

OUT_BOTTLE_SALE = "bottle_sale"
OUT_GLASS_SALE = "glass_sale"
OUT_ETC = "etc"
OUT_ADJUST = "adjust"

OUT_TYPE_LABELS = {
    OUT_BOTTLE_SALE: "병 판매",
    OUT_GLASS_SALE: "잔 판매",
    OUT_ETC: "기타 출고",
    OUT_ADJUST: "실사 조정",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS whisky_product (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name  TEXT NOT NULL UNIQUE,
    brand_name    TEXT,
    category      TEXT,
    bottle_ml     REAL NOT NULL DEFAULT 700,
    glass_ml      REAL NOT NULL DEFAULT 30,
    bottle_price  REAL NOT NULL DEFAULT 0,
    glass_price   REAL NOT NULL DEFAULT 0,
    is_active     INTEGER NOT NULL DEFAULT 1,
    notes         TEXT,
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at    TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS whisky_purchase (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_date  TEXT NOT NULL,
    product_id     INTEGER NOT NULL REFERENCES whisky_product(id),
    supplier_name  TEXT,
    bottle_qty     REAL NOT NULL,
    bottle_ml      REAL NOT NULL,
    total_ml       REAL NOT NULL,
    unit_cost      REAL NOT NULL DEFAULT 0,
    notes          TEXT,
    created_at     TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS whisky_stock_out (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    out_date         TEXT NOT NULL,
    product_id       INTEGER NOT NULL REFERENCES whisky_product(id),
    out_type         TEXT NOT NULL,
    qty              REAL NOT NULL DEFAULT 0,
    total_ml         REAL NOT NULL,
    amount           REAL NOT NULL DEFAULT 0,
    source_type      TEXT NOT NULL DEFAULT 'manual',
    source_order_no  TEXT,
    notes            TEXT,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_whisky_purchase_product ON whisky_purchase(product_id);
CREATE INDEX IF NOT EXISTS idx_whisky_stock_out_product ON whisky_stock_out(product_id, out_date);
"""


def get_db_path() -> str:
    """현재 시점의 위스키 DB 경로. 환경변수를 매번 다시 읽어 테스트에서 경로를 바꿀 수 있게 한다."""
    return os.getenv("WHISKY_DB_PATH", DEFAULT_DB_PATH)


def get_conn() -> sqlite3.Connection:
    """위스키 DB 커넥션. 파일/테이블이 없으면 만든다."""
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn


def _query(sql: str, params=()) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query(sql, conn, params=list(params))
    finally:
        conn.close()


def _execute(sql: str, params=()) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(sql, list(params))
        conn.commit()
        return int(cur.lastrowid or 0)
    finally:
        conn.close()


def _clean(text) -> Optional[str]:
    text = "" if text is None else str(text).strip()
    return text or None


# =========================================================
# 재고 표현
# =========================================================
def split_stock_ml(total_ml: float, bottle_ml: float) -> tuple[int, float]:
    """총 잔량(ml) → (미개봉 병 수, 개봉 병 잔량 ml). 음수 재고는 (0, 음수 ml) 로 돌려준다."""
    total_ml = round(float(total_ml or 0), 2)
    bottle_ml = float(bottle_ml or 0)
    if total_ml <= 0 or bottle_ml <= 0:
        return 0, total_ml
    full = int(total_ml // bottle_ml)
    rest = round(total_ml - full * bottle_ml, 2)
    return full, rest


def format_stock(total_ml: float, bottle_ml: float) -> str:
    """'미개봉 3병 + 개봉 400ml' 형태의 재고 문구."""
    full, rest = split_stock_ml(total_ml, bottle_ml)
    if rest < 0:
        return f"부족 {abs(rest):,.0f}ml"
    if full == 0 and rest == 0:
        return "재고 없음"
    parts = []
    if full:
        parts.append(f"미개봉 {full}병")
    if rest:
        parts.append(f"개봉 {rest:,.0f}ml")
    return " + ".join(parts)


# =========================================================
# 상품
# =========================================================
def get_products(active_only: bool = False) -> pd.DataFrame:
    sql = "SELECT * FROM whisky_product"
    if active_only:
        sql += " WHERE is_active = 1"
    return _query(sql + " ORDER BY product_name")


def get_product(product_id: int) -> dict:
    df = _query("SELECT * FROM whisky_product WHERE id = ?", [int(product_id)])
    if df.empty:
        raise ValueError(f"위스키 상품을 찾을 수 없습니다: id={product_id}")
    return df.iloc[0].to_dict()


def _validate_product(product_name, bottle_ml, glass_ml):
    if not _clean(product_name):
        raise ValueError("상품명을 입력해 주세요.")
    if float(bottle_ml or 0) <= 0:
        raise ValueError("병 용량(ml)은 0보다 커야 합니다.")
    if float(glass_ml or 0) <= 0:
        raise ValueError("잔 용량(ml)은 0보다 커야 합니다.")


def insert_product(
    product_name: str,
    brand_name: str = "",
    category: str = "",
    bottle_ml: float = DEFAULT_BOTTLE_ML,
    glass_ml: float = DEFAULT_GLASS_ML,
    bottle_price: float = 0,
    glass_price: float = 0,
    notes: str = "",
) -> int:
    _validate_product(product_name, bottle_ml, glass_ml)
    try:
        return _execute(
            """
            INSERT INTO whisky_product
                (product_name, brand_name, category, bottle_ml, glass_ml, bottle_price, glass_price, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [_clean(product_name), _clean(brand_name), _clean(category), float(bottle_ml), float(glass_ml),
             float(bottle_price or 0), float(glass_price or 0), _clean(notes)],
        )
    except sqlite3.IntegrityError:
        raise ValueError(f"이미 등록된 상품명입니다: {product_name}")


def update_product(
    product_id: int,
    product_name: str,
    brand_name: str,
    category: str,
    bottle_ml: float,
    glass_ml: float,
    bottle_price: float,
    glass_price: float,
    is_active: int,
    notes: str,
) -> None:
    _validate_product(product_name, bottle_ml, glass_ml)
    try:
        _execute(
            """
            UPDATE whisky_product
            SET product_name = ?, brand_name = ?, category = ?, bottle_ml = ?, glass_ml = ?,
                bottle_price = ?, glass_price = ?, is_active = ?, notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [_clean(product_name), _clean(brand_name), _clean(category), float(bottle_ml), float(glass_ml),
             float(bottle_price or 0), float(glass_price or 0), int(is_active), _clean(notes), int(product_id)],
        )
    except sqlite3.IntegrityError:
        raise ValueError(f"이미 등록된 상품명입니다: {product_name}")


# =========================================================
# 매입
# =========================================================
def insert_purchase(
    purchase_date: str,
    product_id: int,
    bottle_qty: float,
    unit_cost: float = 0,
    supplier_name: str = "",
    notes: str = "",
) -> int:
    """매입 등록. unit_cost 는 병당 매입가."""
    if float(bottle_qty or 0) <= 0:
        raise ValueError("매입 병 수는 0보다 커야 합니다.")
    product = get_product(product_id)
    bottle_ml = float(product["bottle_ml"])
    return _execute(
        """
        INSERT INTO whisky_purchase
            (purchase_date, product_id, supplier_name, bottle_qty, bottle_ml, total_ml, unit_cost, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [str(purchase_date), int(product_id), _clean(supplier_name), float(bottle_qty), bottle_ml,
         float(bottle_qty) * bottle_ml, float(unit_cost or 0), _clean(notes)],
    )


def get_purchases(limit: Optional[int] = None) -> pd.DataFrame:
    sql = """
        SELECT pu.id, pu.purchase_date, p.product_name, pu.supplier_name, pu.bottle_qty, pu.bottle_ml,
               pu.total_ml, pu.unit_cost, pu.bottle_qty * pu.unit_cost AS total_cost, pu.notes
        FROM whisky_purchase pu
        JOIN whisky_product p ON p.id = pu.product_id
        ORDER BY pu.purchase_date DESC, pu.id DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return _query(sql)


def delete_purchase(purchase_id: int) -> None:
    _execute("DELETE FROM whisky_purchase WHERE id = ?", [int(purchase_id)])


# =========================================================
# 판매 / 출고
# =========================================================
def insert_stock_out(
    out_date: str,
    product_id: int,
    out_type: str,
    qty: float,
    amount: float = 0,
    notes: str = "",
    unit: Optional[str] = None,
    source_type: str = "manual",
    source_order_no: str = "",
) -> int:
    """
    판매/출고 등록. 차감 용량은 등록 시점의 상품 병·잔 용량으로 계산한다.
    - bottle_sale: qty = 병 수 / glass_sale: qty = 잔 수
    - etc(서비스·시음·파손 등): unit 이 'bottle' 이면 병, 그 외는 잔 단위
    """
    if out_type not in (OUT_BOTTLE_SALE, OUT_GLASS_SALE, OUT_ETC):
        raise ValueError(f"지원하지 않는 출고 유형입니다: {out_type}")
    if float(qty or 0) <= 0:
        raise ValueError("수량은 0보다 커야 합니다.")
    product = get_product(product_id)
    by_bottle = out_type == OUT_BOTTLE_SALE or (out_type == OUT_ETC and unit == "bottle")
    per_ml = float(product["bottle_ml"] if by_bottle else product["glass_ml"])
    return _execute(
        """
        INSERT INTO whisky_stock_out
            (out_date, product_id, out_type, qty, total_ml, amount, source_type, source_order_no, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [str(out_date), int(product_id), out_type, float(qty), float(qty) * per_ml, float(amount or 0),
         source_type, _clean(source_order_no), _clean(notes)],
    )


def get_stock_outs(limit: Optional[int] = None) -> pd.DataFrame:
    sql = """
        SELECT o.id, o.out_date, p.product_name, o.out_type, o.qty, o.total_ml, o.amount,
               o.source_type, o.notes
        FROM whisky_stock_out o
        JOIN whisky_product p ON p.id = o.product_id
        ORDER BY o.out_date DESC, o.id DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    df = _query(sql)
    if not df.empty:
        df["out_type_label"] = df["out_type"].map(OUT_TYPE_LABELS).fillna(df["out_type"])
    else:
        df["out_type_label"] = []
    return df


def delete_stock_out(out_id: int) -> None:
    _execute("DELETE FROM whisky_stock_out WHERE id = ?", [int(out_id)])


# =========================================================
# 재고
# =========================================================
def get_stock(active_only: bool = False) -> pd.DataFrame:
    """
    상품별 재고. 컬럼:
      product_id, product_name, brand_name, bottle_ml, glass_ml, is_active,
      purchased_ml, out_ml, stock_ml, full_bottles, open_ml, stock_text, glasses_left,
      avg_bottle_cost(매입 가중평균 병당 원가), stock_value(평균 원가 기준 재고 금액)
    """
    df = _query(
        """
        SELECT
            p.id AS product_id, p.product_name, p.brand_name, p.bottle_ml, p.glass_ml, p.is_active,
            COALESCE(pu.purchased_ml, 0) AS purchased_ml,
            COALESCE(pu.purchased_cost, 0) AS purchased_cost,
            COALESCE(o.out_ml, 0) AS out_ml
        FROM whisky_product p
        LEFT JOIN (
            SELECT product_id, SUM(total_ml) AS purchased_ml, SUM(bottle_qty * unit_cost) AS purchased_cost
            FROM whisky_purchase GROUP BY product_id
        ) pu ON pu.product_id = p.id
        LEFT JOIN (
            SELECT product_id, SUM(total_ml) AS out_ml FROM whisky_stock_out GROUP BY product_id
        ) o ON o.product_id = p.id
        ORDER BY p.product_name
        """
    )
    if active_only and not df.empty:
        df = df[df["is_active"] == 1].copy()
    if df.empty:
        for col in ["stock_ml", "full_bottles", "open_ml", "stock_text", "glasses_left",
                    "avg_bottle_cost", "stock_value"]:
            df[col] = []
        return df.drop(columns=["purchased_cost"], errors="ignore")

    df["stock_ml"] = (df["purchased_ml"] - df["out_ml"]).round(2)
    split = [split_stock_ml(ml, b) for ml, b in zip(df["stock_ml"], df["bottle_ml"])]
    df["full_bottles"] = [s[0] for s in split]
    df["open_ml"] = [s[1] for s in split]
    df["stock_text"] = [format_stock(ml, b) for ml, b in zip(df["stock_ml"], df["bottle_ml"])]
    df["glasses_left"] = (df["stock_ml"].clip(lower=0) / df["glass_ml"]).astype(int)

    cost_per_ml = (df["purchased_cost"] / df["purchased_ml"]).where(df["purchased_ml"] > 0, 0.0)
    df["avg_bottle_cost"] = (cost_per_ml * df["bottle_ml"]).round(0)
    df["stock_value"] = (cost_per_ml * df["stock_ml"].clip(lower=0)).round(0)
    return df.drop(columns=["purchased_cost"]).reset_index(drop=True)


def get_stock_ml(product_id: int) -> float:
    df = get_stock()
    row = df[df["product_id"] == int(product_id)]
    return float(row["stock_ml"].iloc[0]) if not row.empty else 0.0


def apply_stocktake(
    out_date: str,
    product_id: int,
    full_bottles: float,
    open_ml: float,
    notes: str = "",
) -> float:
    """
    재고 실사 반영. 실제 수량(미개봉 병 수 + 개봉 병 잔량 ml)에 맞도록 '실사 조정' 행을 넣는다.
    돌려주는 값은 조정한 용량(ml) — 양수면 시스템보다 실물이 적어 차감, 음수면 실물이 많아 증가.
    차이가 없으면 아무것도 넣지 않고 0 을 돌려준다.
    """
    if float(full_bottles or 0) < 0 or float(open_ml or 0) < 0:
        raise ValueError("실사 수량은 0 이상이어야 합니다.")
    product = get_product(product_id)
    bottle_ml = float(product["bottle_ml"])
    if float(open_ml or 0) >= bottle_ml:
        raise ValueError(f"개봉 병 잔량은 병 용량({bottle_ml:,.0f}ml)보다 작아야 합니다.")
    actual_ml = float(full_bottles or 0) * bottle_ml + float(open_ml or 0)
    system_ml = get_stock_ml(product_id)
    diff = round(system_ml - actual_ml, 2)
    if diff == 0:
        return 0.0
    auto_note = (
        f"재고 실사: 실물 {format_stock(actual_ml, bottle_ml)} / "
        f"시스템 {format_stock(system_ml, bottle_ml)}"
    )
    _execute(
        """
        INSERT INTO whisky_stock_out
            (out_date, product_id, out_type, qty, total_ml, amount, source_type, notes)
        VALUES (?, ?, ?, 0, ?, 0, 'manual', ?)
        """,
        [str(out_date), int(product_id), OUT_ADJUST, diff,
         f"{auto_note} / {notes.strip()}" if _clean(notes) else auto_note],
    )
    return diff


# =========================================================
# 매출 / 이익 (정본) — 위스키 HOME·대시보드는 반드시 이 함수들만 사용한다
# =========================================================
# 기준(데일리시가와 같은 방식):
#   - 판매금액(amount)은 결제금액(부가세 포함). 매출 = 판매금액 ÷ 1.1 (부가세 제외 공급가액)
#   - 병당 매입가(unit_cost)는 부가세 제외 금액으로 입력한다.
#   - 원가 = 차감 용량(ml) × 상품의 ml당 평균 매입가(전체 매입 가중평균)
#   - 마진 = 매출 − 원가 (병·잔 판매 행만). 기타 출고·실사 조정은 매출이 없으므로
#     마진에 넣지 않고 '손실·서비스 원가'(loss_amount)로 따로 집계한다.
VAT_RATE = 0.1
SALE_TYPES = (OUT_GLASS_SALE, OUT_BOTTLE_SALE)
SALE_TYPE_LABELS = ("잔 판매", "병 판매")

SALES_COLUMNS = [
    "id", "out_date", "dt", "product_id", "product_name", "brand_name", "out_type", "out_type_label",
    "sales_type", "qty", "total_ml", "amount", "sales_amount", "cost_amount", "margin_amount", "loss_amount",
]


def get_cost_per_ml() -> dict:
    """상품별 ml당 평균 매입가 (전체 매입 가중평균). 매입이 없으면 0."""
    df = _query(
        """
        SELECT product_id, SUM(total_ml) AS ml, SUM(bottle_qty * unit_cost) AS cost
        FROM whisky_purchase GROUP BY product_id
        """
    )
    return {
        int(r["product_id"]): (float(r["cost"]) / float(r["ml"]) if float(r["ml"] or 0) > 0 else 0.0)
        for _, r in df.iterrows()
    }


def load_sales(date_from: Optional[str] = None, date_to: Optional[str] = None) -> pd.DataFrame:
    """
    판매·출고 행에 매출/원가/마진을 붙인 정본 데이터.

    - sales_type: '잔 판매' / '병 판매' (기타 출고·실사 조정은 빈 문자열)
    - sales_amount: 부가세 제외 매출 (판매 행만)
    - cost_amount / margin_amount: 판매 행의 원가 / 마진
    - loss_amount: 기타 출고·실사 조정 행의 원가 (실물이 더 많았던 조정은 음수)
    """
    sql = """
        SELECT o.id, o.out_date, o.product_id, p.product_name, p.brand_name, o.out_type,
               o.qty, o.total_ml, o.amount
        FROM whisky_stock_out o
        JOIN whisky_product p ON p.id = o.product_id
        WHERE 1 = 1
    """
    params = []
    if date_from:
        sql += " AND o.out_date >= ?"
        params.append(str(date_from))
    if date_to:
        sql += " AND o.out_date <= ?"
        params.append(str(date_to))
    df = _query(sql + " ORDER BY o.out_date, o.id", params)
    if df.empty:
        return pd.DataFrame(columns=SALES_COLUMNS)

    for col in ("qty", "total_ml", "amount"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
    df["brand_name"] = df["brand_name"].fillna("").astype(str)
    df["dt"] = pd.to_datetime(df["out_date"], errors="coerce")
    df["out_type_label"] = df["out_type"].map(OUT_TYPE_LABELS).fillna(df["out_type"])

    is_sale = df["out_type"].isin(SALE_TYPES)
    df["sales_type"] = df["out_type_label"].where(is_sale, "")

    cost_per_ml = get_cost_per_ml()
    unit = pd.to_numeric(df["product_id"].map(cost_per_ml), errors="coerce").fillna(0.0).astype(float)
    row_cost = df["total_ml"] * unit

    df["sales_amount"] = (df["amount"] / (1 + VAT_RATE)).where(is_sale, 0.0)
    df["cost_amount"] = row_cost.where(is_sale, 0.0)
    df["margin_amount"] = df["sales_amount"] - df["cost_amount"]
    df["loss_amount"] = row_cost.where(~is_sale, 0.0)
    return df[SALES_COLUMNS].reset_index(drop=True)


def summarize_sales(df: pd.DataFrame) -> dict:
    """load_sales 결과 요약. 매출/원가/마진/마진율, 잔·병별 매출·마진·수량, 손실 원가."""
    def _sum(mask, col):
        return float(df.loc[mask, col].sum()) if not df.empty else 0.0

    everything = pd.Series(True, index=df.index)
    glass = df["sales_type"] == SALE_TYPE_LABELS[0] if not df.empty else everything
    bottle = df["sales_type"] == SALE_TYPE_LABELS[1] if not df.empty else everything
    sales = _sum(everything, "sales_amount")
    margin = _sum(everything, "margin_amount")
    return {
        "sales": sales,
        "cost": _sum(everything, "cost_amount"),
        "margin": margin,
        "margin_rate": (margin / sales * 100) if sales else 0.0,
        "glass_sales": _sum(glass, "sales_amount"),
        "bottle_sales": _sum(bottle, "sales_amount"),
        "glass_margin": _sum(glass, "margin_amount"),
        "bottle_margin": _sum(bottle, "margin_amount"),
        "glass_qty": _sum(glass, "qty"),
        "bottle_qty": _sum(bottle, "qty"),
        "sale_count": int((df["sales_type"] != "").sum()) if not df.empty else 0,
        "loss": _sum(everything, "loss_amount"),
    }


def summarize_by_product(df: pd.DataFrame) -> pd.DataFrame:
    """상품별 요약: 잔/병 수량, 매출, 원가, 마진, 마진율, 손실 원가. 매출 큰 순."""
    cols = ["product_name", "glass_qty", "bottle_qty", "sales", "cost", "margin", "margin_rate", "loss"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.assign(
        glass_qty=df["qty"].where(df["sales_type"] == SALE_TYPE_LABELS[0], 0.0),
        bottle_qty=df["qty"].where(df["sales_type"] == SALE_TYPE_LABELS[1], 0.0),
    )
    g = d.groupby("product_name", as_index=False).agg(
        glass_qty=("glass_qty", "sum"), bottle_qty=("bottle_qty", "sum"),
        sales=("sales_amount", "sum"), cost=("cost_amount", "sum"),
        margin=("margin_amount", "sum"), loss=("loss_amount", "sum"),
    )
    g["margin_rate"] = [(m / s * 100) if s else 0.0 for m, s in zip(g["margin"], g["sales"])]
    return g.sort_values(["sales", "product_name"], ascending=[False, True])[cols].reset_index(drop=True)


def summarize_by_month(df: pd.DataFrame) -> pd.DataFrame:
    """월별 요약: month(YYYY-MM), 잔 매출, 병 매출, 매출, 원가, 마진, 마진율, 손실 원가."""
    cols = ["month", "glass_sales", "bottle_sales", "sales", "cost", "margin", "margin_rate", "loss"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.assign(
        month=df["dt"].dt.strftime("%Y-%m"),
        glass_sales=df["sales_amount"].where(df["sales_type"] == SALE_TYPE_LABELS[0], 0.0),
        bottle_sales=df["sales_amount"].where(df["sales_type"] == SALE_TYPE_LABELS[1], 0.0),
    )
    g = d.groupby("month", as_index=False).agg(
        glass_sales=("glass_sales", "sum"), bottle_sales=("bottle_sales", "sum"),
        sales=("sales_amount", "sum"), cost=("cost_amount", "sum"),
        margin=("margin_amount", "sum"), loss=("loss_amount", "sum"),
    )
    g["margin_rate"] = [(m / s * 100) if s else 0.0 for m, s in zip(g["margin"], g["sales"])]
    return g.sort_values("month")[cols].reset_index(drop=True)


def get_stock_outlook(today, lookback_days: int = 30) -> pd.DataFrame:
    """
    판매 중 상품의 재고 전망. 컬럼:
      product_name, stock_ml, stock_text, sold_ml(최근 lookback_days 일 병·잔 판매 용량),
      daily_ml(일평균), days_left(소진 예상 일수 — 판매가 없으면 NaN),
      last_sale_date(마지막 판매일 — 없으면 NaT), days_since_sale(경과일수 — 판매 이력 없으면 NaN)
    """
    today = pd.Timestamp(today).normalize()
    stock = get_stock(active_only=True)
    cols = ["product_name", "stock_ml", "stock_text", "sold_ml", "daily_ml", "days_left",
            "last_sale_date", "days_since_sale"]
    if stock.empty:
        return pd.DataFrame(columns=cols)

    sales = load_sales(date_to=today.strftime("%Y-%m-%d"))
    sales = sales[sales["sales_type"] != ""]
    start = today - pd.Timedelta(days=lookback_days - 1)
    recent = sales[sales["dt"] >= start].groupby("product_id")["total_ml"].sum()
    last = sales.groupby("product_id")["dt"].max()

    out = stock[["product_id", "product_name", "stock_ml", "stock_text"]].copy()
    out["sold_ml"] = pd.to_numeric(out["product_id"].map(recent), errors="coerce").fillna(0.0).astype(float)
    out["daily_ml"] = out["sold_ml"] / lookback_days
    out["days_left"] = (out["stock_ml"].clip(lower=0) / out["daily_ml"]).where(out["daily_ml"] > 0)
    out["last_sale_date"] = pd.to_datetime(out["product_id"].map(last), errors="coerce")
    out["days_since_sale"] = (today - out["last_sale_date"]).dt.days
    return out[cols].reset_index(drop=True)
