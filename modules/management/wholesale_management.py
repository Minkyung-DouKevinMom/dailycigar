import os
from pathlib import Path
import sqlite3
from typing import Optional
from io import BytesIO

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", str(BASE_DIR / "cigar.db"))


STATEMENT_COMPANY_NAME = "㈜ 데일리시가"
STATEMENT_BANK_NAME = "신한은행"
STATEMENT_BANK_ACCOUNT = "140-015-512046"
STATEMENT_ACCOUNT_HOLDER = "㈜데일리시가"
STATEMENT_DOC_PREFIX = "WS"
STATEMENT_PAYMENT_DUE_DAYS = 3


# -----------------------------
# DB helpers
# -----------------------------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn, table_name: str) -> list[str]:
    if not table_exists(conn, table_name):
        return []
    df = pd.read_sql(f"PRAGMA table_info({table_name})", conn)
    if "name" not in df.columns:
        return []
    return df["name"].tolist()


def find_existing_column(columns: list[str], candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def ensure_required_tables(conn):
    required_tables = ["partner_mst", "wholesale_sales", "product_mst"]
    return [t for t in required_tables if not table_exists(conn, t)]


def ensure_wholesale_sales_columns(conn):
    existing = set(get_table_columns(conn, "wholesale_sales"))
    required = {
        "supply_price": "REAL",
        "supply_amount": "REAL",
        "vat_amount": "REAL",
        "total_amount_vat": "REAL",
        "updated_at": "TEXT",
    }
    cur = conn.cursor()
    changed = False
    for col, col_type in required.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE wholesale_sales ADD COLUMN {col} {col_type}")
            changed = True
    if changed:
        conn.commit()


# -----------------------------
# Loaders
# -----------------------------
def load_partners(conn) -> pd.DataFrame:
    cols = get_table_columns(conn, "partner_mst")
    select_cols = [
        "id",
        "partner_name",
        "partner_type",
        "business_no",
        "owner_name",
        "contact_name",
        "phone",
        "email",
        "address",
        "current_grade_code",
        "status",
        "join_date",
        "notes",
    ]
    final_select = []
    for col in select_cols:
        if col in cols:
            final_select.append(col)
        else:
            final_select.append(f"NULL AS {col}")

    sql = f"""
        SELECT {', '.join(final_select)}
        FROM partner_mst
        ORDER BY partner_name
    """
    return pd.read_sql(sql, conn)


def load_grade_codes(conn) -> list[str]:
    if not table_exists(conn, "partner_grade_mst"):
        return []

    cols = get_table_columns(conn, "partner_grade_mst")
    code_col = find_existing_column(cols, ["grade_code", "partner_grade_code", "code"])
    use_col = find_existing_column(cols, ["is_active", "use_yn", "active_yn", "use_flag"])
    sort_col = find_existing_column(cols, ["sort_order", "display_order", "order_no", "sort_no"])
    if not code_col:
        return []

    sql = f"SELECT {code_col} AS grade_code FROM partner_grade_mst"
    if use_col:
        if use_col == "is_active":
            sql += f" WHERE COALESCE({use_col}, 1) IN (1, '1', 'Y', 'y', 'TRUE', 'true')"
        else:
            sql += f" WHERE COALESCE({use_col}, 'Y') IN ('Y', 'y', '1', 1, 'TRUE', 'true')"
    if sort_col:
        sql += f" ORDER BY {sort_col}, grade_code"
    else:
        sql += " ORDER BY grade_code"

    df = pd.read_sql(sql, conn)
    if df.empty:
        return []
    return [str(x) for x in df["grade_code"].dropna().tolist()]


def _safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return float(default)
        if isinstance(value, str) and value.strip() == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0) -> int:
    try:
        if value is None:
            return int(default)
        if isinstance(value, str) and value.strip() == "":
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _currency(value) -> str:
    return f"₩{_safe_float(value):,.0f}"


def load_cigar_products_for_wholesale(conn) -> pd.DataFrame:
    product_cols = get_table_columns(conn, "product_mst")
    code_col = find_existing_column(product_cols, ["product_code", "code", "item_code"])
    name_col = find_existing_column(product_cols, ["product_name", "name"])
    use_col = find_existing_column(product_cols, ["use_yn", "is_active", "active_yn", "use_flag"])

    if not name_col:
        return pd.DataFrame(columns=["id", "product_code", "product_name", "retail_price_krw", "supply_price_krw", "korea_cost_krw"])

    select_parts = [
        "id",
        f"{code_col} AS product_code" if code_col else "'' AS product_code",
        f"{name_col} AS product_name",
    ]

    sql = f"SELECT {', '.join(select_parts)} FROM product_mst"
    if use_col:
        if use_col == "use_yn":
            sql += f" WHERE COALESCE({use_col}, 'Y') = 'Y'"
        else:
            sql += f" WHERE COALESCE({use_col}, 'Y') IN ('Y', '1', 1, 'TRUE', 'true')"
    sql += " ORDER BY id"

    product_df = pd.read_sql(sql, conn)
    product_df["retail_price_krw"] = 0.0
    product_df["supply_price_krw"] = 0.0
    product_df["korea_cost_krw"] = 0.0

    if not table_exists(conn, "import_item"):
        return product_df

    import_cols = get_table_columns(conn, "import_item")
    retail_col = find_existing_column(import_cols, ["retail_price_krw", "retail_price", "retail_krw"])
    supply_col = find_existing_column(import_cols, ["supply_price_krw", "supply_price", "supply_krw"])
    korea_cost_col = find_existing_column(import_cols, ["korea_cost_krw", "korean_cost_krw", "korea_cost"])
    if not retail_col and not supply_col and not korea_cost_col:
        return product_df

    import_ref_candidates = [
        "product_id",
        "cigar_product_id",
        "product_mst_id",
        "product_code",
        "item_code",
        "code",
        "product_name",
        "item_name",
        "name",
    ]
    import_ref_col = find_existing_column(import_cols, import_ref_candidates)
    if not import_ref_col:
        return product_df

    import_select = [f"{import_ref_col} AS import_ref"]
    import_select.append(f"{retail_col} AS retail_price_krw" if retail_col else "0 AS retail_price_krw")
    import_select.append(f"{supply_col} AS supply_price_krw" if supply_col else "0 AS supply_price_krw")
    import_select.append(f"{korea_cost_col} AS korea_cost_krw" if korea_cost_col else "0 AS korea_cost_krw")

    order_candidates = [c for c in ["updated_at", "created_at", "import_date", "id"] if c in import_cols]
    for c in order_candidates:
        import_select.append(c)

    import_df = pd.read_sql(f"SELECT {', '.join(import_select)} FROM import_item", conn)
    if import_df.empty:
        return product_df

    sort_cols = [c for c in ["updated_at", "created_at", "import_date", "id"] if c in import_df.columns]
    if sort_cols:
        import_df = import_df.sort_values(sort_cols, ascending=False)

    if import_ref_col in ["product_id", "cigar_product_id", "product_mst_id"]:
        product_df["join_ref"] = product_df["id"].astype(str)
        import_df["join_ref"] = import_df["import_ref"].astype(str)
    elif import_ref_col in ["product_code", "item_code", "code"] and "product_code" in product_df.columns:
        product_df["join_ref"] = product_df["product_code"].fillna("").astype(str)
        import_df["join_ref"] = import_df["import_ref"].fillna("").astype(str)
    else:
        product_df["join_ref"] = product_df["product_name"].fillna("").astype(str)
        import_df["join_ref"] = import_df["import_ref"].fillna("").astype(str)

    import_df = import_df.drop_duplicates(subset=["join_ref"], keep="first")

    merged = product_df.merge(
        import_df[["join_ref", "retail_price_krw", "supply_price_krw", "korea_cost_krw"]],
        how="left",
        on="join_ref",
        suffixes=("", "_imp"),
    )

    if "retail_price_krw_imp" in merged.columns:
        merged["retail_price_krw"] = merged["retail_price_krw_imp"].combine_first(merged["retail_price_krw"])
    if "supply_price_krw_imp" in merged.columns:
        merged["supply_price_krw"] = merged["supply_price_krw_imp"].combine_first(merged["supply_price_krw"])
    if "korea_cost_krw_imp" in merged.columns:
        merged["korea_cost_krw"] = merged["korea_cost_krw_imp"].combine_first(merged["korea_cost_krw"])

    keep_cols = ["id", "product_code", "product_name", "retail_price_krw", "supply_price_krw", "korea_cost_krw"]
    result = merged[[c for c in keep_cols if c in merged.columns]].copy()
    result["retail_price_krw"] = result["retail_price_krw"].apply(_safe_float)
    result["supply_price_krw"] = result["supply_price_krw"].apply(_safe_float)
    result["korea_cost_krw"] = result["korea_cost_krw"].apply(_safe_float)
    return result.sort_values("id").reset_index(drop=True)


def load_non_cigar_products(conn) -> pd.DataFrame:
    if not table_exists(conn, "non_cigar_product_mst"):
        return pd.DataFrame(columns=["id", "product_name"])

    cols = get_table_columns(conn, "non_cigar_product_mst")
    name_col = find_existing_column(cols, ["product_name", "name"])
    if not name_col:
        return pd.DataFrame(columns=["id", "product_name"])

    sql = f"""
        SELECT id, {name_col} AS product_name
        FROM non_cigar_product_mst
        ORDER BY id
    """
    return pd.read_sql(sql, conn)


def load_wholesale_sales(conn) -> pd.DataFrame:
    ensure_wholesale_sales_columns(conn)
    return pd.read_sql(
        """
        SELECT *
        FROM wholesale_sales
        ORDER BY sale_date DESC, id DESC
        """,
        conn,
    )


def load_wholesale_sales_for_grid(conn) -> pd.DataFrame:
    ensure_wholesale_sales_columns(conn)
    sales = load_wholesale_sales(conn)
    if sales.empty:
        return sales

    df = sales.copy()

    for col in ["partner_id", "cigar_product_id", "non_cigar_product_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    partner_base = load_partners(conn).copy()
    partner_keep_cols = [
        c for c in ["id", "partner_name", "business_no", "owner_name", "contact_name", "phone", "email", "address"]
        if c in partner_base.columns
    ]
    partners = partner_base[partner_keep_cols].copy()
    partners["id"] = pd.to_numeric(partners["id"], errors="coerce")
    partners = partners.rename(columns={"id": "partner_id_lookup"})
    df = df.merge(partners, how="left", left_on="partner_id", right_on="partner_id_lookup")

    cigar_products = load_cigar_products_for_wholesale(conn)
    if not cigar_products.empty:
        cigar_products = cigar_products.copy()
        cigar_products["id"] = pd.to_numeric(cigar_products["id"], errors="coerce")
        cigar_products = cigar_products.rename(columns={"id": "cigar_product_id"})
        df = df.merge(
            cigar_products,
            how="left",
            on="cigar_product_id",
            suffixes=("", "_cigar"),
        )

    if table_exists(conn, "non_cigar_product_mst"):
        nc = load_non_cigar_products(conn).copy()
        if not nc.empty:
            nc["id"] = pd.to_numeric(nc["id"], errors="coerce")
            nc = nc.rename(
                columns={
                    "id": "non_cigar_product_id",
                    "product_name": "non_cigar_product_name",
                }
            )

            df = df.merge(
                nc,
                how="left",
                on="non_cigar_product_id",
                suffixes=("", "_non_cigar"),
            )

            if "product_name" in df.columns:
                df["product_name"] = df["product_name"].fillna(df["non_cigar_product_name"])
            else:
                df["product_name"] = df["non_cigar_product_name"]

    if "product_code" not in df.columns:
        df["product_code"] = ""

    if "sales_amount" not in df.columns and "supply_amount" in df.columns:
        df["sales_amount"] = df["supply_amount"]

    df["item_type_label"] = df["item_type"].map({
        "cigar": "시가",
        "non_cigar": "시가 외",
    }).fillna(df["item_type"])

    return df


def _validate_statement_rows(selected_df: pd.DataFrame) -> tuple[bool, str]:
    if selected_df.empty:
        return False, "거래명세서로 만들 항목을 하나 이상 선택해 주세요."

    partner_cnt = selected_df["partner_name"].fillna("").astype(str).nunique()
    sale_date_cnt = selected_df["sale_date"].fillna("").astype(str).nunique()

    if partner_cnt != 1:
        return False, "같은 거래처의 항목만 하나의 거래명세서로 출력할 수 있습니다."
    if sale_date_cnt != 1:
        return False, "같은 판매일자의 항목만 하나의 거래명세서로 출력할 수 있습니다."

    return True, ""


def _build_statement_from_rows(selected_df: pd.DataFrame) -> tuple[dict, pd.DataFrame, dict]:
    sale_date_raw = str(selected_df.iloc[0].get("sale_date", "") or "")
    try:
        sale_dt = pd.to_datetime(sale_date_raw).to_pydatetime()
        sale_date_display = sale_dt.strftime("%Y-%m-%d")
        due_date_display = (sale_dt + pd.Timedelta(days=STATEMENT_PAYMENT_DUE_DAYS)).strftime("%Y-%m-%d")
        doc_no = f"{sale_dt.strftime('%Y%m%d')}{str(selected_df.iloc[0].get('partner_id', '') or '').zfill(3) if str(selected_df.iloc[0].get('partner_id', '') or '').strip() else '001'}"
    except Exception:
        sale_date_display = sale_date_raw
        due_date_display = sale_date_raw
        doc_no = f"{pd.Timestamp.now().strftime('%Y%m%d')}001"

    contact_name = str(selected_df.iloc[0].get("contact_name", "") or "").strip()
    owner_name = str(selected_df.iloc[0].get("owner_name", "") or "").strip()
    partner_contact = contact_name or owner_name or "대표님"

    header = {
        "partner_name": str(selected_df.iloc[0].get("partner_name", "") or ""),
        "partner_contact": partner_contact,
        "partner_phone": str(selected_df.iloc[0].get("phone", "") or ""),
        "partner_address": str(selected_df.iloc[0].get("address", "") or ""),
        "business_no": str(selected_df.iloc[0].get("business_no", "") or ""),
        "sale_date": sale_date_display,
        "due_date": due_date_display,
        "document_no": doc_no,
        "notes": " / ".join(
            sorted(
                {
                    str(x).strip()
                    for x in selected_df.get("notes", pd.Series(dtype=str)).fillna("").tolist()
                    if str(x).strip()
                }
            )
        ),
    }

    statement_df = selected_df[
        [
            "product_name",
            "unit_price",
            "qty",
            "supply_amount",
            "vat_amount",
            "total_amount_vat",
        ]
    ].copy()

    statement_df = statement_df.rename(
        columns={
            "product_name": "품목",
            "unit_price": "단가",
            "qty": "수량",
            "supply_amount": "공급가액",
            "vat_amount": "세액",
            "total_amount_vat": "합계",
        }
    )

    totals = {
        "qty": int(pd.to_numeric(statement_df["수량"], errors="coerce").fillna(0).sum()),
        "supply_amount": float(pd.to_numeric(statement_df["공급가액"], errors="coerce").fillna(0).sum()),
        "vat_amount": float(pd.to_numeric(statement_df["세액"], errors="coerce").fillna(0).sum()),
        "total_amount": float(pd.to_numeric(statement_df["합계"], errors="coerce").fillna(0).sum()),
    }
    return header, statement_df, totals


def _render_statement_preview(header: dict, statement_df: pd.DataFrame, totals: dict):
    st.divider()
    st.markdown("#### 거래명세서 미리보기")

    c1, c2 = st.columns(2)
    with c1:
        st.write(f"**거래처**: {header['partner_name']}")
    with c2:
        st.write(f"**판매일자**: {header['sale_date']}")

    if header.get("notes"):
        st.write(f"**비고**: {header['notes']}")

    st.dataframe(
        statement_df.style.format(
            {
                "단가": "₩{:,.0f}",
                "공급가액": "₩{:,.0f}",
                "세액": "₩{:,.0f}",
                "합계": "₩{:,.0f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("공급가액 합계", f"₩{totals['supply_amount']:,.0f}")
    with m2:
        st.metric("부가세 합계", f"₩{totals['vat_amount']:,.0f}")
    with m3:
        st.metric("총 합계", f"₩{totals['total_amount']:,.0f}")


def _make_statement_excel_bytes(header: dict, statement_df: pd.DataFrame, totals: dict) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        sheet_name = "거래명세서"
        statement_df.to_excel(writer, sheet_name=sheet_name, startrow=0, index=False)

        ws = writer.sheets[sheet_name]

        # 기존 데이터 헤더/본문 제거 후 템플릿 형태로 다시 구성
        if ws.max_row > 0:
            ws.delete_rows(1, ws.max_row)

        # 시트 기본 설정
        ws.sheet_view.showGridLines = False
        ws.freeze_panes = "B18"

        # 컬럼 폭
        widths = {
            "A": 1.55, "B": 16.89, "C": 14.00, "D": 8.00, "E": 10.11,
            "F": 8.00, "G": 4.66, "H": 9.33, "I": 9.00, "J": 11.22,
        }
        for col, width in widths.items():
            ws.column_dimensions[col].width = width

        # 행 높이
        row_heights = {
            2: 36.0, 3: 18.0, 4: 13.5, 5: 13.5, 10: 18.0, 12: 18.0, 13: 18.0,
            14: 18.0, 17: 18.0, 43: 13.5, 44: 45.0, 47: 49.5,
        }
        for r, h in row_heights.items():
            ws.row_dimensions[r].height = h

        # 공통 스타일
        dark_fill = PatternFill("solid", fgColor="1A3A5C")
        light_fill = PatternFill("solid", fgColor="F7F9FC")
        thin_side = Side(style="thin", color="000000")
        medium_side = Side(style="medium", color="000000")

        border_thin = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
        border_bottom_thin = Border(bottom=thin_side)
        border_bottom_medium = Border(bottom=medium_side)
        border_header_left = Border(left=medium_side, top=medium_side, bottom=medium_side)
        border_header_mid = Border(top=medium_side, bottom=medium_side)
        border_header_right = Border(right=medium_side, top=medium_side, bottom=medium_side)

        def set_cell(cell_ref, value=None, *, font=None, fill=None, align=None, border=None, numfmt=None):
            cell = ws[cell_ref]
            if value is not None:
                cell.value = value
            if font:
                cell.font = font
            if fill:
                cell.fill = fill
            if align:
                cell.alignment = align
            if border:
                cell.border = border
            if numfmt:
                cell.number_format = numfmt
            return cell

        # 병합
        merge_ranges = [
            "B2:D2", "I2:J2", "H3:J3", "I4:J4", "I5:J5",
            "B10:D10", "E10:J10",
            "C12:D12", "H12:J12",
            "C13:D13", "H13:J13",
            "C14:D14", "H14:J14",
        ]
        for rng in merge_ranges:
            ws.merge_cells(rng)

        # 헤더
        set_cell("B2", "거 래 명 세 서", font=Font(size=20, bold=True), align=Alignment(horizontal="left", vertical="center"))
        set_cell("I2", header.get("partner_name", ""), font=Font(size=12, bold=True), align=Alignment(horizontal="right", vertical="center"))
        set_cell("H3", header.get("partner_address", ""), font=Font(size=9), align=Alignment(horizontal="right", vertical="center"))
        set_cell("I4", f"{header.get('partner_contact', '')}", font=Font(size=9), align=Alignment(horizontal="right", vertical="center"))
        set_cell("I5", header.get("partner_phone", ""), font=Font(size=9), align=Alignment(horizontal="right", vertical="center"))

        # 정보 영역 라벨
        label_font = Font(size=9, bold=True)
        value_font = Font(size=9)

        set_cell("B10", "고객명 (담당자명)", font=label_font, align=Alignment(horizontal="left", vertical="center"), border=border_bottom_thin)
        set_cell("E10", "입금 계좌 정보", font=label_font, align=Alignment(horizontal="right", vertical="center"), border=border_bottom_thin)

        for rng in ["B10:D10", "E10:J10", "B12:E12", "B13:E13", "B14:E14", "F12:J12", "F13:J13", "F14:J14", "B43:J43", "B44:J44"]:
            start = ws[rng.split(":")[0]]
            end = ws[rng.split(":")[1]]
            # bottom border across merged range
            for row in ws[rng]:
                for cell in row:
                    cell.border = border_bottom_thin

        set_cell("B12", "문서번호", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("C12", header.get("document_no", ""), font=value_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("F12", "은행명", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("H12", STATEMENT_BANK_NAME, font=value_font, align=Alignment(horizontal="left", vertical="center"))

        set_cell("B13", "청구일", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("C13", header.get("sale_date", ""), font=value_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("F13", "계좌번호", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("H13", STATEMENT_BANK_ACCOUNT, font=value_font, align=Alignment(horizontal="left", vertical="center"))

        set_cell("B14", "납부기한", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("C14", header.get("due_date", ""), font=value_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("F14", "예금주", font=label_font, align=Alignment(horizontal="left", vertical="center"))
        set_cell("H14", STATEMENT_ACCOUNT_HOLDER, font=value_font, align=Alignment(horizontal="left", vertical="center"))

        # 표 헤더
        ws.merge_cells("B17:E17")
        header_specs = {
            "B17": ("품목", border_header_left),
            "F17": ("단가", border_header_mid),
            "G17": ("수량", border_header_mid),
            "H17": ("공급가액", border_header_mid),
            "I17": ("세액", border_header_mid),
            "J17": ("합계", border_header_right),
        }
        for ref, (val, bdr) in header_specs.items():
            set_cell(ref, val, font=Font(size=9, bold=True, color="FFFFFF"), fill=dark_fill,
                     align=Alignment(horizontal="center", vertical="center"), border=bdr)

        # 본문
        start_row = 18
        item_count = len(statement_df)
        display_rows = max(20, item_count)
        end_row = start_row + display_rows - 1

        for idx in range(display_rows):
            r = start_row + idx
            actual = idx < item_count
            item = statement_df.iloc[idx] if actual else None

            ws.merge_cells(f"B{r}:E{r}")

            # 값 입력
            set_cell(f"B{r}", "" if item is None else item.get("품목", ""), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="left", vertical="center"), border=border_thin)
            set_cell(f"F{r}", 0 if item is None else _safe_float(item.get("단가", 0)), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="right", vertical="center"), border=border_thin, numfmt="#,##0")
            set_cell(f"G{r}", 0 if item is None else _safe_int(item.get("수량", 0)), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="center", vertical="center"), border=border_thin, numfmt="0")
            set_cell(f"H{r}", 0 if item is None else _safe_float(item.get("공급가액", 0)), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="right", vertical="center"), border=border_thin, numfmt="#,##0")
            set_cell(f"I{r}", 0 if item is None else _safe_float(item.get("세액", 0)), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="right", vertical="center"), border=border_thin, numfmt="#,##0")
            set_cell(f"J{r}", 0 if item is None else _safe_float(item.get("합계", 0)), font=Font(size=9),
                     fill=light_fill, align=Alignment(horizontal="right", vertical="center"), border=border_thin, numfmt="#,##0")

        # 합계 영역
        total_supply_row = end_row + 2
        total_vat_row = end_row + 3
        total_amount_row = end_row + 4
        note_title_row = end_row + 6
        note_body_row = end_row + 7
        sign_row = end_row + 10

        for rng in [f"E{total_supply_row}:H{total_supply_row}", f"E{total_vat_row}:H{total_vat_row}", f"E{total_amount_row}:H{total_amount_row}"]:
            for row in ws[rng]:
                for cell in row:
                    cell.border = border_bottom_thin

        ws.merge_cells(f"E{total_supply_row}:H{total_supply_row}")
        ws.merge_cells(f"E{total_vat_row}:H{total_vat_row}")
        ws.merge_cells(f"E{total_amount_row}:H{total_amount_row}")
        ws.merge_cells(f"B{note_title_row}:J{note_title_row}")
        ws.merge_cells(f"B{note_body_row}:J{note_body_row}")
        ws.merge_cells(f"B{sign_row}:J{sign_row}")

        set_cell(f"E{total_supply_row}", "총 공급가액", font=Font(size=9), align=Alignment(horizontal="right", vertical="center"))
        set_cell(f"I{total_supply_row}", totals["supply_amount"], font=Font(size=9), align=Alignment(horizontal="right", vertical="center"), border=border_bottom_thin, numfmt="#,##0")

        set_cell(f"E{total_vat_row}", "총 세액", font=Font(size=9), align=Alignment(horizontal="right", vertical="center"))
        set_cell(f"I{total_vat_row}", totals["vat_amount"], font=Font(size=9), align=Alignment(horizontal="right", vertical="center"), border=border_bottom_thin, numfmt="#,##0")

        set_cell(f"E{total_amount_row}", "총 합계", font=Font(size=10, bold=True), align=Alignment(horizontal="right", vertical="center"))
        set_cell(f"I{total_amount_row}", totals["total_amount"], font=Font(size=10, bold=True), align=Alignment(horizontal="right", vertical="center"), border=border_bottom_medium, numfmt="#,##0")

        # 비고
        note_text = header.get("notes", "") or "비고를 입력해주세요."
        set_cell(f"B{note_title_row}", "비고", font=label_font, align=Alignment(horizontal="left", vertical="center"), border=border_bottom_thin)
        set_cell(f"B{note_body_row}", note_text, font=Font(size=9), align=Alignment(horizontal="left", vertical="center", wrap_text=True), border=border_bottom_thin)

        # 서명
        set_cell(f"B{sign_row}", STATEMENT_COMPANY_NAME, font=Font(size=13, bold=True), align=Alignment(horizontal="center", vertical="center"))

        # 행 높이 추가 설정
        ws.row_dimensions[note_title_row].height = 13.5
        ws.row_dimensions[note_body_row].height = 45.0
        ws.row_dimensions[sign_row].height = 49.5

    output.seek(0)
    return output.getvalue()


# -----------------------------
# Partner CRUD
# -----------------------------
def insert_partner(
    conn,
    partner_name: str,
    partner_type: str,
    business_no: str,
    owner_name: str,
    contact_name: str,
    phone: str,
    email: str,
    address: str,
    join_date: str,
    current_grade_code: str,
    notes: str,
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO partner_mst (
            partner_name,
            partner_type,
            business_no,
            owner_name,
            contact_name,
            phone,
            email,
            address,
            join_date,
            status,
            current_grade_code,
            grade_acquired_date,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            partner_name,
            partner_type,
            business_no,
            owner_name,
            contact_name,
            phone,
            email,
            address,
            join_date,
            "active",
            current_grade_code if current_grade_code else None,
            join_date if current_grade_code else None,
            notes,
        ),
    )
    conn.commit()


def update_partner(
    conn,
    partner_id: int,
    partner_name: str,
    partner_type: str,
    business_no: str,
    owner_name: str,
    contact_name: str,
    phone: str,
    email: str,
    address: str,
    join_date: str,
    current_grade_code: str,
    notes: str,
    status: str,
):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE partner_mst
        SET
            partner_name = ?,
            partner_type = ?,
            business_no = ?,
            owner_name = ?,
            contact_name = ?,
            phone = ?,
            email = ?,
            address = ?,
            join_date = ?,
            current_grade_code = ?,
            notes = ?,
            status = ?,
            grade_acquired_date = CASE
                WHEN COALESCE(?, '') <> '' AND COALESCE(current_grade_code, '') <> COALESCE(?, '') THEN ?
                ELSE grade_acquired_date
            END
        WHERE id = ?
        """,
        (
            partner_name,
            partner_type,
            business_no,
            owner_name,
            contact_name,
            phone,
            email,
            address,
            join_date,
            current_grade_code if current_grade_code else None,
            notes,
            status,
            current_grade_code if current_grade_code else None,
            current_grade_code if current_grade_code else None,
            join_date,
            partner_id,
        ),
    )
    conn.commit()


# -----------------------------
# Wholesale CRUD
# -----------------------------
def insert_wholesale_sale(
    conn,
    sale_date: str,
    partner_id: int,
    item_type: str,
    cigar_product_id,
    non_cigar_product_id,
    qty: int,
    unit_price: float,
    supply_price: float,
    unit_cost: float,
    supply_amount: float,
    vat_amount: float,
    total_amount_vat: float,
    profit_amount: float,
    notes: str,
):
    ensure_wholesale_sales_columns(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO wholesale_sales (
            sale_date,
            partner_id,
            item_type,
            cigar_product_id,
            non_cigar_product_id,
            qty,
            unit_price,
            supply_price,
            unit_cost,
            sales_amount,
            supply_amount,
            vat_amount,
            total_amount_vat,
            profit_amount,
            notes,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            sale_date,
            partner_id,
            item_type,
            cigar_product_id,
            non_cigar_product_id,
            qty,
            unit_price,
            supply_price,
            unit_cost,
            supply_amount,
            supply_amount,
            vat_amount,
            total_amount_vat,
            profit_amount,
            notes,
        ),
    )
    conn.commit()


def update_wholesale_sale(
    conn,
    sale_id: int,
    sale_date: str,
    partner_id: int,
    item_type: str,
    cigar_product_id,
    non_cigar_product_id,
    qty: int,
    unit_price: float,
    supply_price: float,
    unit_cost: float,
    supply_amount: float,
    vat_amount: float,
    total_amount_vat: float,
    profit_amount: float,
    notes: str,
):
    ensure_wholesale_sales_columns(conn)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE wholesale_sales
        SET
            sale_date = ?,
            partner_id = ?,
            item_type = ?,
            cigar_product_id = ?,
            non_cigar_product_id = ?,
            qty = ?,
            unit_price = ?,
            supply_price = ?,
            unit_cost = ?,
            sales_amount = ?,
            supply_amount = ?,
            vat_amount = ?,
            total_amount_vat = ?,
            profit_amount = ?,
            notes = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            sale_date,
            partner_id,
            item_type,
            cigar_product_id,
            non_cigar_product_id,
            qty,
            unit_price,
            supply_price,
            unit_cost,
            supply_amount,
            supply_amount,
            vat_amount,
            total_amount_vat,
            profit_amount,
            notes,
            sale_id,
        ),
    )
    conn.commit()


def delete_wholesale_sale(conn, sale_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM wholesale_sales WHERE id = ?", (sale_id,))
    conn.commit()


# -----------------------------
# UI helpers
# -----------------------------
def _render_amount_summary(qty: int, unit_price: float, supply_price: float, unit_cost: float):
    supply_amount = float(qty) * float(supply_price)
    vat_amount = round(supply_amount * 0.1, 2)
    total_amount_vat = supply_amount + vat_amount
    profit_amount = float(qty) * (float(supply_price) - float(unit_cost))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("공급가액", f"₩{supply_amount:,.0f}")
    c2.metric("부가세", f"₩{vat_amount:,.0f}")
    c3.metric("부가세 포함 금액", f"₩{total_amount_vat:,.0f}")
    c4.metric("예상마진", f"₩{profit_amount:,.0f}")
    return supply_amount, vat_amount, total_amount_vat, profit_amount


def _bind_price_cost_by_selection(state_prefix: str, selected_product_key: str, auto_unit_price: float, auto_supply_price: float, auto_unit_cost: float):
    prev_key = f"{state_prefix}_selected_product_prev"
    price_key = f"{state_prefix}_unit_price"
    supply_key = f"{state_prefix}_supply_price"
    cost_key = f"{state_prefix}_unit_cost"

    prev_selected = st.session_state.get(prev_key)
    if prev_selected != selected_product_key:
        st.session_state[price_key] = int(auto_unit_price)
        st.session_state[supply_key] = int(auto_supply_price)
        st.session_state[cost_key] = int(auto_unit_cost)
        st.session_state[prev_key] = selected_product_key
    else:
        st.session_state.setdefault(price_key, int(auto_unit_price))
        st.session_state.setdefault(supply_key, int(auto_supply_price))
        st.session_state.setdefault(cost_key, int(auto_unit_cost))


# -----------------------------
# Render sections
# -----------------------------
def render_partner_registration(conn):
    st.markdown("### 거래처 관리")

    partners = load_partners(conn)
    grade_codes = load_grade_codes(conn)

    mode = st.radio("작업", ["신규 등록", "기존 업체 수정"], horizontal=True)

    selected_partner = None
    selected_row = None
    if mode == "기존 업체 수정":
        if partners.empty:
            st.info("수정할 거래처가 없습니다.")
            return

        option_df = partners[["id", "partner_name"]].copy()
        option_df["label"] = option_df["partner_name"].fillna("") + " (ID:" + option_df["id"].astype(str) + ")"
        selected_label = st.selectbox("수정할 업체 선택", option_df["label"].tolist())
        selected_partner = option_df.loc[option_df["label"] == selected_label].iloc[0]
        selected_row = partners.loc[partners["id"] == selected_partner["id"]].iloc[0]

    with st.form("partner_registration_form", clear_on_submit=(mode == "신규 등록")):
        c1, c2 = st.columns(2)
        partner_type_list = ["wholesale", "retail_partner", "bar", "cafe", "shop", "etc"]
        status_list = ["active", "inactive"]

        with c1:
            partner_name = st.text_input("거래처명 *", value="" if selected_row is None else str(selected_row["partner_name"] or ""))
            default_partner_type = "wholesale" if selected_row is None else str(selected_row["partner_type"] or "wholesale")
            partner_type = st.selectbox(
                "거래처 유형",
                partner_type_list,
                index=partner_type_list.index(default_partner_type) if default_partner_type in partner_type_list else 0,
            )
            business_no = st.text_input("사업자번호", value="" if selected_row is None else str(selected_row.get("business_no", "") or ""))
            owner_name = st.text_input("대표자명", value="" if selected_row is None else str(selected_row["owner_name"] or ""))
            contact_name = st.text_input("담당자명", value="" if selected_row is None else str(selected_row["contact_name"] or ""))

        with c2:
            phone = st.text_input("연락처", value="" if selected_row is None else str(selected_row["phone"] or ""))
            email = st.text_input("이메일", value="" if selected_row is None else str(selected_row["email"] or ""))
            address = st.text_input("주소", value="" if selected_row is None else str(selected_row.get("address", "") or ""))

            default_join_date = pd.to_datetime("today").date()
            if selected_row is not None and pd.notna(selected_row["join_date"]):
                default_join_date = pd.to_datetime(selected_row["join_date"]).date()
            join_date = str(st.date_input("가입일", value=default_join_date))

            if grade_codes:
                grade_list = [""] + grade_codes
                default_grade = "" if selected_row is None else str(selected_row["current_grade_code"] or "")
                current_grade_code = st.selectbox(
                    "현재 등급",
                    grade_list,
                    index=grade_list.index(default_grade) if default_grade in grade_list else 0,
                )
            else:
                current_grade_code = st.text_input("현재 등급", value="" if selected_row is None else str(selected_row["current_grade_code"] or ""))

            default_status = "active" if selected_row is None else str(selected_row["status"] or "active")
            status = st.selectbox(
                "상태",
                status_list,
                index=status_list.index(default_status) if default_status in status_list else 0,
            )
            notes = st.text_area("비고", value="" if selected_row is None else str(selected_row.get("notes", "") or ""))

        submitted = st.form_submit_button(
            "거래처 등록" if mode == "신규 등록" else "거래처 수정 저장",
            use_container_width=True,
        )

        if submitted:
            if not partner_name.strip():
                st.error("거래처명은 필수입니다.")
                return

            if mode == "신규 등록":
                dup = pd.read_sql(
                    "SELECT COUNT(*) AS cnt FROM partner_mst WHERE partner_name = ?",
                    conn,
                    params=(partner_name.strip(),),
                )["cnt"].iloc[0]
                if dup > 0:
                    st.error("이미 같은 거래처명이 등록되어 있습니다.")
                    return

                insert_partner(
                    conn=conn,
                    partner_name=partner_name.strip(),
                    partner_type=partner_type,
                    business_no=business_no.strip(),
                    owner_name=owner_name.strip(),
                    contact_name=contact_name.strip(),
                    phone=phone.strip(),
                    email=email.strip(),
                    address=address.strip(),
                    join_date=join_date,
                    current_grade_code=current_grade_code.strip() if current_grade_code else "",
                    notes=notes.strip(),
                )
                st.success(f"거래처가 등록되었습니다: {partner_name}")
            else:
                dup = pd.read_sql(
                    "SELECT COUNT(*) AS cnt FROM partner_mst WHERE partner_name = ? AND id <> ?",
                    conn,
                    params=(partner_name.strip(), int(selected_partner["id"])),
                )["cnt"].iloc[0]
                if dup > 0:
                    st.error("이미 같은 거래처명이 등록되어 있습니다.")
                    return

                update_partner(
                    conn=conn,
                    partner_id=int(selected_partner["id"]),
                    partner_name=partner_name.strip(),
                    partner_type=partner_type,
                    business_no=business_no.strip(),
                    owner_name=owner_name.strip(),
                    contact_name=contact_name.strip(),
                    phone=phone.strip(),
                    email=email.strip(),
                    address=address.strip(),
                    join_date=join_date,
                    current_grade_code=current_grade_code.strip() if current_grade_code else "",
                    notes=notes.strip(),
                    status=status,
                )
                st.success(f"거래처 정보가 수정되었습니다: {partner_name}")

            st.rerun()

    if not partners.empty:
        st.divider()
        st.markdown("#### 등록된 거래처")
        view_df = partners.rename(columns={
            "partner_name": "거래처명",
            "partner_type": "유형",
            "business_no": "사업자번호",
            "owner_name": "대표자명",
            "contact_name": "담당자명",
            "phone": "연락처",
            "email": "이메일",
            "address": "주소",
            "current_grade_code": "등급",
            "status": "상태",
            "join_date": "가입일",
            "notes": "비고",
        })
        st.dataframe(view_df, use_container_width=True, hide_index=True)
    else:
        st.info("등록된 거래처가 없습니다.")


def render_wholesale_management(conn):
    st.markdown("### 도매 판매 관리")
    st.caption("상단에서 등록하고, 하단 AgGrid에서 검색/정렬/수정/삭제할 수 있습니다. 여러 줄 선택 시 거래명세서 미리보기/엑셀 다운로드도 가능합니다.")

    partners = load_partners(conn)
    cigar_products = load_cigar_products_for_wholesale(conn)
    non_cigar_products = load_non_cigar_products(conn)

    if partners.empty:
        st.warning("먼저 거래처를 등록해 주세요.")
        return

    # -----------------------------
    # 등록 영역
    # -----------------------------
    with st.expander("신규 도매 판매 등록", expanded=True):
        item_type_label = st.radio("상품 구분", ["시가", "시가 외"], horizontal=True, key="wh_item_type_label")
        item_type = "cigar" if item_type_label == "시가" else "non_cigar"

        c1, c2 = st.columns(2)
        with c1:
            partner_name = st.selectbox("거래처", partners["partner_name"].tolist(), key="wh_partner_name")
            sale_date = str(st.date_input("판매일자", key="wh_sale_date"))
            qty = st.number_input("수량", min_value=1, value=1, step=1, format="%d", key="wh_qty")

        cigar_product_id = None
        non_cigar_product_id = None
        product_name = ""
        product_code = ""
        auto_unit_price = 0.0
        auto_supply_price = 0.0
        auto_unit_cost = 0.0

        if item_type == "cigar":
            if cigar_products.empty:
                st.warning("시가 상품 기준정보가 없습니다.")
                return

            display_options = cigar_products.copy()
            display_options["display_name"] = display_options.apply(
                lambda r: f"{str(r['product_code']).strip()} | {str(r['product_name']).strip()}"
                if str(r["product_code"]).strip() else str(r["product_name"]).strip(),
                axis=1,
            )

            selected_display = st.selectbox("시가 상품", display_options["display_name"].tolist(), key="wh_cigar_product")
            selected_product = display_options.loc[display_options["display_name"] == selected_display].iloc[0]
            cigar_product_id = _safe_int(selected_product["id"])
            product_name = str(selected_product["product_name"] or "")
            product_code = str(selected_product["product_code"] or "")
            auto_unit_price = _safe_float(selected_product.get("retail_price_krw", 0))
            auto_supply_price = _safe_float(selected_product.get("supply_price_krw", 0))
            auto_unit_cost = _safe_float(selected_product.get("korea_cost_krw", 0))

            _bind_price_cost_by_selection(
                state_prefix="wh",
                selected_product_key=f"cigar::{cigar_product_id}",
                auto_unit_price=auto_unit_price,
                auto_supply_price=auto_supply_price,
                auto_unit_cost=auto_unit_cost,
            )
        else:
            if non_cigar_products.empty:
                st.warning("시가 외 상품 기준정보가 없습니다.")
                return

            selected_name = st.selectbox("시가 외 상품", non_cigar_products["product_name"].tolist(), key="wh_non_cigar_product")
            selected_product = non_cigar_products.loc[non_cigar_products["product_name"] == selected_name].iloc[0]
            non_cigar_product_id = _safe_int(selected_product["id"])
            product_name = str(selected_name)

            _bind_price_cost_by_selection(
                state_prefix="wh",
                selected_product_key=f"non_cigar::{non_cigar_product_id}",
                auto_unit_price=0,
                auto_supply_price=0,
                auto_unit_cost=0,
            )

        with c2:
            unit_price = st.number_input("단가 (₩)", min_value=0, step=100, format="%d", key="wh_unit_price")
            supply_price = st.number_input("공급가 (₩)", min_value=0, step=100, format="%d", key="wh_supply_price")
            unit_cost = st.number_input("원가 (₩)", min_value=0, step=100, format="%d", key="wh_unit_cost")
            notes = st.text_area("비고", key="wh_notes")

        # 👇 추가 (중요)
        st.caption(
            f"단가: ₩{unit_price:,.0f} / "
            f"공급가: ₩{supply_price:,.0f} / "
            f"원가: ₩{unit_cost:,.0f}"
        )

        if product_code:
            st.caption(f"상품코드: {product_code}")

        if item_type == "cigar":
            st.caption("단가 = import_item.retail_price_krw / 공급가 = import_item.supply_price_krw / 원가 = import_item.korea_cost_krw 기준 자동 바인딩")

        supply_amount, vat_amount, total_amount_vat, profit_amount = _render_amount_summary(qty, unit_price, supply_price, unit_cost)

        if st.button("도매 판매 저장", use_container_width=True, key="wh_insert_btn"):
            partner_id = int(partners.loc[partners["partner_name"] == partner_name, "id"].iloc[0])

            insert_wholesale_sale(
                conn=conn,
                sale_date=sale_date,
                partner_id=partner_id,
                item_type=item_type,
                cigar_product_id=cigar_product_id,
                non_cigar_product_id=non_cigar_product_id,
                qty=int(qty),
                unit_price=float(unit_price),
                supply_price=float(supply_price),
                unit_cost=float(unit_cost),
                supply_amount=float(supply_amount),
                vat_amount=float(vat_amount),
                total_amount_vat=float(total_amount_vat),
                profit_amount=float(profit_amount),
                notes=notes.strip(),
            )
            st.success("도매 판매 이력이 저장되었습니다.")
            st.rerun()

    # -----------------------------
    # 조회/관리 영역
    # -----------------------------
    st.divider()
    st.markdown("#### 도매 판매 내역")

    df = load_wholesale_sales_for_grid(conn)
    if df.empty:
        st.info("등록된 도매 판매 이력이 없습니다.")
        return

    with st.expander("검색 / 필터", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            keyword = st.text_input("거래처 / 상품 / 코드 검색", key="manage_keyword")
        with f2:
            item_type_filter = st.selectbox("상품구분", ["전체", "시가", "시가 외"], key="manage_item_type")
        with f3:
            partner_options = ["전체"] + sorted([str(x) for x in df["partner_name"].dropna().unique().tolist()])
            partner_filter = st.selectbox("거래처", partner_options, key="manage_partner")

    filtered = df.copy()
    if keyword.strip():
        kw = keyword.strip().lower()
        mask = (
            filtered.get("partner_name", pd.Series("", index=filtered.index)).fillna("").astype(str).str.lower().str.contains(kw)
            | filtered.get("product_name", pd.Series("", index=filtered.index)).fillna("").astype(str).str.lower().str.contains(kw)
            | filtered.get("product_code", pd.Series("", index=filtered.index)).fillna("").astype(str).str.lower().str.contains(kw)
        )
        filtered = filtered[mask]

    if item_type_filter != "전체":
        filtered = filtered[filtered["item_type_label"] == item_type_filter]

    if partner_filter != "전체":
        filtered = filtered[filtered["partner_name"] == partner_filter]

    if filtered.empty:
        st.warning("검색 결과가 없습니다.")
        return

    grid_df = filtered[[
        "id", "sale_date", "partner_name", "item_type_label", "product_code", "product_name",
        "qty", "unit_price", "supply_price", "unit_cost", "supply_amount",
        "vat_amount", "total_amount_vat", "profit_amount", "notes", "updated_at"
    ]].copy()

    gb = GridOptionsBuilder.from_dataframe(grid_df)
    gb.configure_selection("multiple", use_checkbox=True)
    gb.configure_default_column(sortable=True, filter=True, resizable=True)
    gb.configure_column("id", header_name="ID", width=80)
    gb.configure_column("sale_date", header_name="판매일자", width=110)
    gb.configure_column("partner_name", header_name="거래처명", width=160)
    gb.configure_column("item_type_label", header_name="상품구분", width=100)
    gb.configure_column("product_code", header_name="상품코드", width=130)
    gb.configure_column("product_name", header_name="상품명", width=180)
    gb.configure_column("qty", header_name="수량", width=90, type=["numericColumn"])
    gb.configure_column("unit_price", header_name="단가", width=120, type=["numericColumn"], valueFormatter="data.unit_price != null ? '₩' + Number(data.unit_price).toLocaleString() : ''")
    gb.configure_column("supply_price", header_name="공급가", width=120, type=["numericColumn"], valueFormatter="data.supply_price != null ? '₩' + Number(data.supply_price).toLocaleString() : ''")
    gb.configure_column("unit_cost", header_name="원가", width=120, type=["numericColumn"], valueFormatter="data.unit_cost != null ? '₩' + Number(data.unit_cost).toLocaleString() : ''")
    gb.configure_column("supply_amount", header_name="공급가액", width=130, type=["numericColumn"], valueFormatter="data.supply_amount != null ? '₩' + Number(data.supply_amount).toLocaleString() : ''")
    gb.configure_column("vat_amount", header_name="부가세", width=110, type=["numericColumn"], valueFormatter="data.vat_amount != null ? '₩' + Number(data.vat_amount).toLocaleString() : ''")
    gb.configure_column("total_amount_vat", header_name="부가세포함금액", width=150, type=["numericColumn"], valueFormatter="data.total_amount_vat != null ? '₩' + Number(data.total_amount_vat).toLocaleString() : ''")
    gb.configure_column("profit_amount", header_name="예상마진", width=120, type=["numericColumn"], valueFormatter="data.profit_amount != null ? '₩' + Number(data.profit_amount).toLocaleString() : ''")
    gb.configure_column("notes", header_name="비고", width=200)
    gb.configure_column("updated_at", header_name="수정일시", width=150)

    grid_response = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=False,
        height=420,
        use_container_width=True,
        key="wholesale_aggrid",
    )

    selected_rows = grid_response.get("selected_rows", [])
    if isinstance(selected_rows, pd.DataFrame):
        selected_rows = selected_rows.to_dict("records")

    selected_grid_df = pd.DataFrame(selected_rows) if selected_rows else pd.DataFrame()

    if not selected_grid_df.empty:
        ok, msg = _validate_statement_rows(selected_grid_df)
        if ok:
            header, statement_df, totals = _build_statement_from_rows(selected_grid_df)
            _render_statement_preview(header, statement_df, totals)

            excel_bytes = _make_statement_excel_bytes(header, statement_df, totals)
            safe_partner = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in header["partner_name"]).strip("_") or "거래처"
            safe_date = str(header["sale_date"]).replace("-", "")
            st.download_button(
                "거래명세서 엑셀 다운로드",
                data=excel_bytes,
                file_name=f"거래명세서_{safe_partner}_{safe_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_statement_excel",
            )
        else:
            st.warning(msg)

    if not selected_rows:
        st.info("수정/삭제 또는 거래명세서 출력을 위해 행을 하나 이상 선택해 주세요.")
        return

    selected_id = _safe_int(selected_rows[0].get("id"))
    base_row = load_wholesale_sales(conn)
    base_row = base_row.loc[base_row["id"] == selected_id]
    if base_row.empty:
        st.error("선택한 도매 판매 건을 찾을 수 없습니다.")
        return
    row = base_row.iloc[0]

    partner_names = partners["partner_name"].tolist()
    partner_name_map = dict(zip(partners["id"], partners["partner_name"]))
    current_partner_name = partner_name_map.get(row["partner_id"], partner_names[0] if partner_names else "")

    st.divider()
    st.markdown("#### 선택 건 수정 / 삭제")

    item_type = str(row["item_type"])
    item_type_label = "시가" if item_type == "cigar" else "시가 외"
    cigar_product_id = row["cigar_product_id"] if "cigar_product_id" in row.index else None
    non_cigar_product_id = row["non_cigar_product_id"] if "non_cigar_product_id" in row.index else None

    c1, c2 = st.columns(2)
    with c1:
        sale_date = str(st.date_input("판매일자", value=pd.to_datetime(row["sale_date"]).date(), key=f"edit_sale_date_{selected_id}"))
        partner_name = st.selectbox(
            "거래처",
            partner_names,
            index=partner_names.index(current_partner_name) if current_partner_name in partner_names else 0,
            key=f"edit_partner_{selected_id}",
        )
        qty = st.number_input("수량", min_value=1, value=_safe_int(row["qty"], 1), step=1, format="%d", key=f"edit_qty_{selected_id}")

    selected_product_code = ""
    auto_unit_price = _safe_float(row.get("unit_price", 0))
    auto_supply_price = _safe_float(row.get("supply_price", 0))
    auto_unit_cost = _safe_float(row.get("unit_cost", 0))

    if item_type == "cigar":
        if cigar_products.empty:
            st.warning("시가 상품 기준정보가 없습니다.")
            return

        product_options = cigar_products.copy()
        product_options["display_name"] = product_options.apply(
            lambda r: f"{str(r['product_code']).strip()} | {str(r['product_name']).strip()}"
            if str(r["product_code"]).strip() else str(r["product_name"]).strip(),
            axis=1,
        )

        default_idx = 0
        if pd.notna(cigar_product_id):
            matched_idx = product_options.index[product_options["id"] == _safe_int(cigar_product_id)].tolist()
            if matched_idx:
                default_idx = product_options.index.get_loc(matched_idx[0])

        selected_display = st.selectbox(
            "시가 상품",
            product_options["display_name"].tolist(),
            index=default_idx,
            key=f"edit_cigar_product_{selected_id}",
        )
        selected_product = product_options.loc[product_options["display_name"] == selected_display].iloc[0]
        cigar_product_id = _safe_int(selected_product["id"])
        non_cigar_product_id = None
        selected_product_code = str(selected_product["product_code"] or "")
        auto_unit_price = _safe_float(selected_product.get("retail_price_krw", auto_unit_price))
        auto_supply_price = _safe_float(selected_product.get("supply_price_krw", auto_supply_price))
        auto_unit_cost = _safe_float(selected_product.get("korea_cost_krw", auto_unit_cost))

        _bind_price_cost_by_selection(
            state_prefix=f"edit_{selected_id}",
            selected_product_key=f"cigar::{cigar_product_id}",
            auto_unit_price=auto_unit_price,
            auto_supply_price=auto_supply_price,
            auto_unit_cost=auto_unit_cost,
        )
    else:
        if non_cigar_products.empty:
            st.warning("시가 외 상품 기준정보가 없습니다.")
            return

        default_nc_idx = 0
        if pd.notna(non_cigar_product_id):
            matched_idx = non_cigar_products.index[non_cigar_products["id"] == _safe_int(non_cigar_product_id)].tolist()
            if matched_idx:
                default_nc_idx = non_cigar_products.index.get_loc(matched_idx[0])

        selected_product_name = st.selectbox(
            "시가 외 상품",
            non_cigar_products["product_name"].tolist(),
            index=default_nc_idx,
            key=f"edit_non_cigar_product_{selected_id}",
        )
        selected_product = non_cigar_products.loc[non_cigar_products["product_name"] == selected_product_name].iloc[0]
        non_cigar_product_id = _safe_int(selected_product["id"])
        cigar_product_id = None

        _bind_price_cost_by_selection(
            state_prefix=f"edit_{selected_id}",
            selected_product_key=f"non_cigar::{non_cigar_product_id}",
            auto_unit_price=_safe_float(row.get("unit_price", 0)),
            auto_supply_price=_safe_float(row.get("supply_price", 0)),
            auto_unit_cost=_safe_float(row.get("unit_cost", 0)),
        )

    with c2:
        unit_price = st.number_input("단가 (₩)", min_value=0, step=100, format="%d", key=f"edit_{selected_id}_unit_price")
        supply_price = st.number_input("공급가 (₩)", min_value=0, step=100, format="%d", key=f"edit_{selected_id}_supply_price")
        unit_cost = st.number_input("원가 (₩)", min_value=0, step=100, format="%d", key=f"edit_{selected_id}_unit_cost")

    st.caption(
    f"단가: ₩{unit_price:,.0f} / "
    f"공급가: ₩{supply_price:,.0f} / "
    f"원가: ₩{unit_cost:,.0f}"
    )

    notes = st.text_area(
        "비고",
        value=str(row.get("notes", "") or ""),
        key=f"edit_notes_{selected_id}"
    )

    st.caption(f"상품구분: {item_type_label}")
    if selected_product_code:
        st.caption(f"상품코드: {selected_product_code}")

    supply_amount, vat_amount, total_amount_vat, profit_amount = _render_amount_summary(qty, unit_price, supply_price, unit_cost)

    b1, b2 = st.columns(2)
    with b1:
        if st.button("선택 건 수정 저장", use_container_width=True, key=f"save_sale_{selected_id}"):
            partner_id = int(partners.loc[partners["partner_name"] == partner_name, "id"].iloc[0])
            update_wholesale_sale(
                conn=conn,
                sale_id=selected_id,
                sale_date=sale_date,
                partner_id=partner_id,
                item_type=item_type,
                cigar_product_id=int(cigar_product_id) if cigar_product_id is not None else None,
                non_cigar_product_id=int(non_cigar_product_id) if non_cigar_product_id is not None else None,
                qty=int(qty),
                unit_price=float(unit_price),
                supply_price=float(supply_price),
                unit_cost=float(unit_cost),
                supply_amount=float(supply_amount),
                vat_amount=float(vat_amount),
                total_amount_vat=float(total_amount_vat),
                profit_amount=float(profit_amount),
                notes=notes.strip(),
            )
            st.success("도매 판매 건이 수정되었습니다.")
            st.rerun()

    with b2:
        confirm_delete = st.checkbox("삭제 확인", key=f"confirm_delete_{selected_id}")
        if st.button("선택 건 삭제", use_container_width=True, type="secondary", key=f"delete_sale_{selected_id}"):
            if not confirm_delete:
                st.error("삭제 확인을 체크해 주세요.")
            else:
                delete_wholesale_sale(conn, selected_id)
                st.success("도매 판매 건이 삭제되었습니다.")
                st.rerun()


def render():
    st.subheader("도매 관리")

    conn = get_conn()
    try:
        missing_tables = ensure_required_tables(conn)
        if missing_tables:
            st.error("필수 테이블이 없습니다: " + ", ".join(missing_tables))
            st.info("먼저 DB 초기화 SQL을 실행해 주세요.")
            return

        ensure_wholesale_sales_columns(conn)

        tab1, tab2 = st.tabs([
            "거래처 관리",
            "도매 판매 관리",
        ])

        with tab1:
            render_partner_registration(conn)

        with tab2:
            render_wholesale_management(conn)

    finally:
        conn.close()