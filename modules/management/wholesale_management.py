
import os
from pathlib import Path
import sqlite3
from typing import Optional

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", str(BASE_DIR / "cigar.db"))


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


def load_cigar_products_for_wholesale(conn) -> pd.DataFrame:
    product_cols = get_table_columns(conn, "product_mst")
    code_col = find_existing_column(product_cols, ["product_code", "code", "item_code"])
    name_col = find_existing_column(product_cols, ["product_name", "name"])
    if not name_col:
        return pd.DataFrame(columns=["id", "product_code", "product_name", "retail_price_krw", "supply_price_krw", "korea_cost_krw"])

    select_parts = [
        "id",
        f"{code_col} AS product_code" if code_col else "'' AS product_code",
        f"{name_col} AS product_name",
    ]
    product_df = pd.read_sql(
        f"SELECT {', '.join(select_parts)} FROM product_mst ORDER BY {name_col}",
        conn,
    )
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

    merged["retail_price_krw"] = merged["retail_price_krw_imp"].combine_first(merged["retail_price_krw"]) if "retail_price_krw_imp" in merged.columns else merged["retail_price_krw"]
    merged["supply_price_krw"] = merged["supply_price_krw_imp"].combine_first(merged["supply_price_krw"]) if "supply_price_krw_imp" in merged.columns else merged["supply_price_krw"]
    merged["korea_cost_krw"] = merged["korea_cost_krw_imp"].combine_first(merged["korea_cost_krw"]) if "korea_cost_krw_imp" in merged.columns else merged["korea_cost_krw"]

    keep_cols = ["id", "product_code", "product_name", "retail_price_krw", "supply_price_krw", "korea_cost_krw"]
    result = merged[[c for c in keep_cols if c in merged.columns]].copy()
    result["retail_price_krw"] = result["retail_price_krw"].apply(_safe_float)
    result["supply_price_krw"] = result["supply_price_krw"].apply(_safe_float)
    result["korea_cost_krw"] = result["korea_cost_krw"].apply(_safe_float)
    return result.sort_values(["product_name", "product_code"]).reset_index(drop=True)


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
        ORDER BY {name_col}
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

    # 키 컬럼 타입 정리
    for col in ["partner_id", "cigar_product_id", "non_cigar_product_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 거래처
    partners = load_partners(conn)[["id", "partner_name"]].copy()
    partners["id"] = pd.to_numeric(partners["id"], errors="coerce")
    partners = partners.rename(columns={"id": "partner_id_lookup"})
    df = df.merge(partners, how="left", left_on="partner_id", right_on="partner_id_lookup")

    # 시가 상품
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

    # 논시가 상품
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

    return df


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
def display_partner_dataframe(df: pd.DataFrame):
    rename_map = {
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
    }
    st.dataframe(df.rename(columns=rename_map), use_container_width=True, hide_index=True)


def display_wholesale_dataframe(df: pd.DataFrame):
    rename_map = {
        "sale_date": "판매일자",
        "partner_name": "거래처명",
        "item_type": "상품구분",
        "product_code": "상품코드",
        "product_name": "상품명",
        "qty": "수량",
        "unit_price": "단가",
        "supply_price": "공급가",
        "unit_cost": "원가",
        "sales_amount": "공급가액",
        "supply_amount": "공급가액",
        "vat_amount": "부가세",
        "total_amount_vat": "부가세포함금액",
        "profit_amount": "예상마진",
        "notes": "비고",
        "updated_at": "수정일시",
    }
    st.dataframe(df.rename(columns=rename_map), use_container_width=True, hide_index=True)


def _get_selected_grid_row(editor_df: pd.DataFrame) -> Optional[pd.Series]:
    if editor_df.empty or "_선택" not in editor_df.columns:
        return None
    selected = editor_df[editor_df["_선택"] == True]
    if selected.empty:
        return None
    return selected.iloc[0]


def _render_amount_summary(qty: int, unit_price: float, supply_price: float, unit_cost: float):
    supply_amount = float(qty) * float(supply_price)
    vat_amount = round(supply_amount * 0.1, 2)
    total_amount_vat = supply_amount + vat_amount
    profit_amount = float(qty) * (float(supply_price) - float(unit_cost))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("공급가액", f"{supply_amount:,.0f}원")
    c2.metric("부가세", f"{vat_amount:,.0f}원")
    c3.metric("부가세 포함 금액", f"{total_amount_vat:,.0f}원")
    c4.metric("예상마진", f"{profit_amount:,.0f}원")
    return supply_amount, vat_amount, total_amount_vat, profit_amount


# -----------------------------
# Render sections
# -----------------------------


def _bind_price_cost_by_selection(state_prefix: str, selected_product_key: str, auto_unit_price: float, auto_supply_price: float, auto_unit_cost: float):
    prev_key = f"{state_prefix}_selected_product_prev"
    price_key = f"{state_prefix}_unit_price"
    supply_key = f"{state_prefix}_supply_price"
    cost_key = f"{state_prefix}_unit_cost"

    prev_selected = st.session_state.get(prev_key)
    if prev_selected != selected_product_key:
        st.session_state[price_key] = float(auto_unit_price)
        st.session_state[supply_key] = float(auto_supply_price)
        st.session_state[cost_key] = float(auto_unit_cost)
        st.session_state[prev_key] = selected_product_key
    else:
        st.session_state.setdefault(price_key, float(auto_unit_price))
        st.session_state.setdefault(supply_key, float(auto_supply_price))
        st.session_state.setdefault(cost_key, float(auto_unit_cost))

def render_partner_registration(conn):
    st.markdown("### 도매업체 등록 / 수정")

    partners = load_partners(conn)
    grade_codes = load_grade_codes(conn)

    mode = st.radio("작업", ["신규 등록", "기존 업체 수정"], horizontal=True)

    selected_partner = None
    selected_row = None
    if mode == "기존 업체 수정":
        if partners.empty:
            st.info("수정할 도매업체가 없습니다.")
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
            "도매업체 등록" if mode == "신규 등록" else "도매업체 수정 저장",
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


def render_partner_list(conn):
    st.markdown("### 등록된 도매업체")
    partners = load_partners(conn)
    if partners.empty:
        st.info("등록된 도매업체가 없습니다.")
        return
    display_partner_dataframe(partners)


def render_wholesale_entry(conn):
    st.markdown("### 도매 등록")

    partners = load_partners(conn)
    cigar_products = load_cigar_products_for_wholesale(conn)
    non_cigar_products = load_non_cigar_products(conn)

    if partners.empty:
        st.warning("먼저 위에서 도매업체를 등록해 주세요.")
        return

    item_type = st.radio("상품 구분", ["cigar", "non_cigar"], horizontal=True, key="wh_item_type")

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
            st.warning("product_mst 또는 import_item에 등록된 시가 상품 기준정보가 없습니다.")
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
            st.warning("non_cigar_product_mst에 등록된 비시가 상품이 없습니다.")
            return

        selected_name = st.selectbox("비시가 상품", non_cigar_products["product_name"].tolist(), key="wh_non_cigar_product")
        selected_product = non_cigar_products.loc[non_cigar_products["product_name"] == selected_name].iloc[0]
        non_cigar_product_id = _safe_int(selected_product["id"])
        product_name = str(selected_name)

    with c2:
        if item_type != "cigar":
            _bind_price_cost_by_selection(
                state_prefix="wh",
                selected_product_key=f"{item_type}::{non_cigar_product_id if non_cigar_product_id is not None else 'none'}",
                auto_unit_price=auto_unit_price,
                auto_supply_price=auto_supply_price,
                auto_unit_cost=auto_unit_cost,
            )
        unit_price = st.number_input("단가", min_value=0.0, step=100.0, key="wh_unit_price")
        supply_price = st.number_input("공급가", min_value=0.0, step=100.0, key="wh_supply_price")
        unit_cost = st.number_input("원가", min_value=0.0, step=100.0, key="wh_unit_cost")
        notes = st.text_area("비고", key="wh_notes")

    if product_code:
        st.caption(f"상품코드: {product_code}")
    if item_type == "cigar":
        st.caption("단가 = import_item.retail_price_krw / 공급가 = import_item.supply_price_krw / 원가 = import_item.korea_cost_krw 기준 자동 바인딩")

    supply_amount, vat_amount, total_amount_vat, profit_amount = _render_amount_summary(qty, unit_price, supply_price, unit_cost)

    if st.button("도매 저장", use_container_width=True, key="wh_insert_btn"):
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


def render_wholesale_history(conn):
    st.markdown("### 도매 이력 조회")
    df = load_wholesale_sales_for_grid(conn)
    if df.empty:
        st.info("등록된 도매 이력이 없습니다.")
        return

    view_cols = [c for c in [
        "sale_date", "partner_name", "item_type", "product_code", "product_name",
        "qty", "unit_price", "supply_price", "unit_cost", "supply_amount", "vat_amount",
        "total_amount_vat", "profit_amount", "notes", "updated_at"
    ] if c in df.columns]
    display_wholesale_dataframe(df[view_cols].copy())


def render_wholesale_manage(conn):
    st.markdown("### 도매 등록 관리")
    st.caption("검색 후 그리드에서 1건을 선택해서 수정 또는 삭제할 수 있습니다.")

    df = load_wholesale_sales_for_grid(conn)
    partners = load_partners(conn)
    cigar_products = load_cigar_products_for_wholesale(conn)
    non_cigar_products = load_non_cigar_products(conn)

    if df.empty:
        st.info("관리할 도매 등록 건이 없습니다.")
        return

    with st.expander("검색", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            keyword = st.text_input("거래처/상품/코드 검색", key="manage_keyword")
        with c2:
            item_type_filter = st.selectbox("상품구분", ["전체", "cigar", "non_cigar"], key="manage_item_type")
        with c3:
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

    if item_type_filter != "전체" and "item_type" in filtered.columns:
        filtered = filtered[filtered["item_type"] == item_type_filter]

    if partner_filter != "전체" and "partner_name" in filtered.columns:
        filtered = filtered[filtered["partner_name"] == partner_filter]

    if filtered.empty:
        st.warning("검색 결과가 없습니다.")
        return

    grid_cols = [c for c in [
        "id", "sale_date", "partner_name", "item_type", "product_code", "product_name",
        "qty", "unit_price", "supply_price", "unit_cost", "supply_amount", "vat_amount", "total_amount_vat",
        "profit_amount", "notes", "updated_at"
    ] if c in filtered.columns]
    grid_df = filtered[grid_cols].copy()
    grid_df.insert(0, "_선택", False)

    rename_map = {
        "_선택": "선택",
        "id": "ID",
        "sale_date": "판매일자",
        "partner_name": "거래처명",
        "item_type": "상품구분",
        "product_code": "상품코드",
        "product_name": "상품명",
        "qty": "수량",
        "unit_price": "단가",
        "supply_price": "공급가",
        "unit_cost": "원가",
        "supply_amount": "공급가액",
        "vat_amount": "부가세",
        "total_amount_vat": "부가세포함금액",
        "profit_amount": "예상마진",
        "notes": "비고",
        "updated_at": "수정일시",
    }

    editor_df = st.data_editor(
        grid_df.rename(columns=rename_map),
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in rename_map.values() if c != "선택"],
        column_config={
            "선택": st.column_config.CheckboxColumn(required=False),
        },
        key="wholesale_manage_grid",
    )

    reverse_map = {v: k for k, v in rename_map.items()}
    editor_df = editor_df.rename(columns=reverse_map)

    selected_row = _get_selected_grid_row(editor_df)
    if selected_row is None:
        st.info("수정 또는 삭제할 행을 1건 선택해 주세요.")
        return

    sale_id = _safe_int(selected_row["id"])
    base_row = load_wholesale_sales(conn)
    base_row = base_row.loc[base_row["id"] == sale_id]
    if base_row.empty:
        st.error("선택한 도매 등록 건을 찾을 수 없습니다.")
        return
    row = base_row.iloc[0]

    partner_names = partners["partner_name"].tolist()
    partner_name_map = dict(zip(partners["id"], partners["partner_name"]))
    current_partner_name = partner_name_map.get(row["partner_id"], partner_names[0] if partner_names else "")

    st.markdown("#### 선택 건 수정")
    item_type = str(row["item_type"])
    cigar_product_id = row["cigar_product_id"] if "cigar_product_id" in row.index else None
    non_cigar_product_id = row["non_cigar_product_id"] if "non_cigar_product_id" in row.index else None

    c1, c2 = st.columns(2)
    with c1:
        sale_date = str(st.date_input("판매일자", value=pd.to_datetime(row["sale_date"]).date(), key=f"edit_sale_date_{sale_id}"))
        partner_name = st.selectbox(
            "거래처",
            partner_names,
            index=partner_names.index(current_partner_name) if current_partner_name in partner_names else 0,
            key=f"edit_partner_{sale_id}",
        )
        qty = st.number_input("수량", min_value=1, value=_safe_int(row["qty"], 1), step=1, format="%d", key=f"edit_qty_{sale_id}")

    selected_product_name = ""
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
            key=f"edit_cigar_product_{sale_id}",
        )
        selected_product = product_options.loc[product_options["display_name"] == selected_display].iloc[0]
        cigar_product_id = _safe_int(selected_product["id"])
        non_cigar_product_id = None
        selected_product_name = str(selected_product["product_name"] or "")
        selected_product_code = str(selected_product["product_code"] or "")
        auto_unit_price = _safe_float(selected_product.get("retail_price_krw", auto_unit_price))
        auto_supply_price = _safe_float(selected_product.get("supply_price_krw", auto_supply_price))
        auto_unit_cost = _safe_float(selected_product.get("korea_cost_krw", auto_unit_cost))
        _bind_price_cost_by_selection(
            state_prefix=f"edit_{sale_id}",
            selected_product_key=f"cigar::{cigar_product_id}",
            auto_unit_price=auto_unit_price,
            auto_supply_price=auto_supply_price,
            auto_unit_cost=auto_unit_cost,
        )
    else:
        if non_cigar_products.empty:
            st.warning("비시가 상품 기준정보가 없습니다.")
            return

        default_nc_idx = 0
        if pd.notna(non_cigar_product_id):
            matched_idx = non_cigar_products.index[non_cigar_products["id"] == _safe_int(non_cigar_product_id)].tolist()
            if matched_idx:
                default_nc_idx = non_cigar_products.index.get_loc(matched_idx[0])

        selected_product_name = st.selectbox(
            "비시가 상품",
            non_cigar_products["product_name"].tolist(),
            index=default_nc_idx,
            key=f"edit_non_cigar_product_{sale_id}",
        )
        selected_product = non_cigar_products.loc[non_cigar_products["product_name"] == selected_product_name].iloc[0]
        non_cigar_product_id = _safe_int(selected_product["id"])
        cigar_product_id = None

    with c2:
        if item_type != "cigar":
            _bind_price_cost_by_selection(
                state_prefix=f"edit_{sale_id}",
                selected_product_key=f"{item_type}::{non_cigar_product_id if non_cigar_product_id is not None else 'none'}",
                auto_unit_price=_safe_float(row.get("unit_price", 0)),
                auto_supply_price=_safe_float(row.get("supply_price", 0)),
                auto_unit_cost=_safe_float(row.get("unit_cost", 0)),
            )
        else:
            st.session_state.setdefault(f"edit_{sale_id}_unit_price", float(auto_unit_price))
            st.session_state.setdefault(f"edit_{sale_id}_supply_price", float(auto_supply_price))
            st.session_state.setdefault(f"edit_{sale_id}_unit_cost", float(auto_unit_cost))
        unit_price = st.number_input("단가", min_value=0.0, step=100.0, key=f"edit_{sale_id}_unit_price")
        supply_price = st.number_input("공급가", min_value=0.0, step=100.0, key=f"edit_{sale_id}_supply_price")
        unit_cost = st.number_input("원가", min_value=0.0, step=100.0, key=f"edit_{sale_id}_unit_cost")
        notes = st.text_area("비고", value=str(row.get("notes", "") or ""), key=f"edit_notes_{sale_id}")

    if selected_product_code:
        st.caption(f"상품코드: {selected_product_code}")
    supply_amount, vat_amount, total_amount_vat, profit_amount = _render_amount_summary(qty, unit_price, supply_price, unit_cost)

    b1, b2 = st.columns(2)
    with b1:
        if st.button("선택 건 수정 저장", use_container_width=True, key=f"save_sale_{sale_id}"):
            partner_id = int(partners.loc[partners["partner_name"] == partner_name, "id"].iloc[0])
            update_wholesale_sale(
                conn=conn,
                sale_id=sale_id,
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
            st.success("도매 등록 건이 수정되었습니다.")
            st.rerun()

    with b2:
        confirm_delete = st.checkbox("삭제 확인", key=f"confirm_delete_{sale_id}")
        if st.button("선택 건 삭제", use_container_width=True, type="secondary", key=f"delete_sale_{sale_id}"):
            if not confirm_delete:
                st.error("삭제 확인을 체크해 주세요.")
            else:
                delete_wholesale_sale(conn, sale_id)
                st.success("도매 등록 건이 삭제되었습니다.")
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

        tab1, tab2, tab3, tab4 = st.tabs([
            "도매업체 등록/수정",
            "도매 등록",
            "도매 이력 조회",
            "도매 등록 관리",
        ])

        with tab1:
            render_partner_registration(conn)
            st.divider()
            render_partner_list(conn)

        with tab2:
            render_wholesale_entry(conn)

        with tab3:
            render_wholesale_history(conn)

        with tab4:
            render_wholesale_manage(conn)
    finally:
        conn.close()
