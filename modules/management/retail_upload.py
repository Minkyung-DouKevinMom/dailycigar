import os
import re
import sqlite3
from typing import Dict, Optional, Set

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("DAILYCIGAR_DB_PATH", "cigar.db")

ITEM_SHEET_NAME = "상품 주문 상세내역"

REQUIRED_COLUMNS = [
    "주문기준일자",
    "결제상태",
    "주문시작시각",
    "주문채널",
    "주문번호",
    "상품코드",
    "카테고리",
    "옵션",
    "수량",
    "상품가격",
    "옵션가격",
    "상품할인 금액",
    "주문할인 금액",
    "실판매금액 \n (할인, 옵션 포함)",
    "과세여부",
    "부가세액",
]

MIN_REQUIRED_DB_COLUMNS = {
    "sale_date",
    "sale_datetime",
    "order_no",
    "product_code_raw",
    "qty",
    "net_sales_amount",
}


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return {row[1] for row in rows}


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def safe_float(value) -> float:
    if pd.isna(value) or value == "":
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def normalize_product_code(value) -> str:
    code = safe_str(value)
    code = code.replace("/", "")
    code = re.sub(r"\s+", " ", code).strip()
    return code.upper()


def load_excel(uploaded_file):
    return pd.read_excel(uploaded_file, sheet_name=ITEM_SHEET_NAME)


def clean_item_df(df_item: pd.DataFrame) -> pd.DataFrame:
    df = df_item.copy()

    df = df[df["주문번호"].notna()].copy()
    df = df[df["주문기준일자"].notna()].copy()
    df = df[~df["주문번호"].astype(str).str.contains("주문번호", na=False)].copy()

    df["source_row_no"] = range(2, len(df) + 2)

    df["sale_date"] = pd.to_datetime(df["주문기준일자"], errors="coerce").dt.date
    df["sale_datetime"] = pd.to_datetime(df["주문시작시각"], errors="coerce")

    numeric_cols = [
        "수량",
        "상품가격",
        "옵션가격",
        "상품할인 금액",
        "주문할인 금액",
        "실판매금액 \n (할인, 옵션 포함)",
        "부가세액",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["order_no"] = df["주문번호"].astype(str).str.strip()
    df["payment_status"] = df["결제상태"].apply(safe_str)
    df["order_channel"] = df["주문채널"].apply(safe_str)
    df["product_code_raw"] = df["상품코드"].apply(safe_str)
    df["product_code"] = df["상품코드"].apply(normalize_product_code)
    df["category"] = df["카테고리"].apply(safe_str)
    df["option_name"] = df["옵션"].apply(safe_str)
    df["product_discount_name"] = df["상품할인"].apply(safe_str) if "상품할인" in df.columns else ""
    df["order_discount_name"] = df["주문할인"].apply(safe_str) if "주문할인" in df.columns else ""
    df["taxable_yn"] = df["과세여부"].apply(safe_str)

    df["qty"] = df["수량"]
    df["unit_price"] = df["상품가격"]
    df["option_price"] = df["옵션가격"]
    df["product_discount_amt"] = df["상품할인 금액"]
    df["order_discount_amt"] = df["주문할인 금액"]
    df["net_sales_amount"] = df["실판매금액 \n (할인, 옵션 포함)"]
    df["vat_amount"] = df["부가세액"]

    return df


def validate_item_df(df: pd.DataFrame) -> Dict[str, int]:
    return {
        "전체 행수": int(len(df)),
        "주문번호 누락": int(df["order_no"].eq("").sum()),
        "판매일시 누락": int(df["sale_datetime"].isna().sum()),
        "상품코드 누락": int(df["product_code_raw"].eq("").sum()),
        "수량 오류": int(df["qty"].isna().sum()),
        "판매금액 오류": int(df["net_sales_amount"].isna().sum()),
    }


def fetch_product_codes(conn: sqlite3.Connection) -> Set[str]:
    product_codes = set()

    for table_name in ["product_mst", "non_cigar_product_mst"]:
        if not table_exists(conn, table_name):
            continue

        cols = get_table_columns(conn, table_name)
        if "product_code" not in cols:
            continue

        try:
            df = pd.read_sql_query(
                f"SELECT product_code FROM {table_name} WHERE product_code IS NOT NULL",
                conn,
            )
            product_codes.update(
                {normalize_product_code(x) for x in df["product_code"].dropna().tolist()}
            )
        except Exception:
            pass

    return product_codes


def is_duplicate_row(conn: sqlite3.Connection, row: pd.Series) -> bool:
    sql = """
        SELECT 1
          FROM retail_sales
         WHERE order_no = ?
           AND COALESCE(product_code_raw, '') = ?
           AND COALESCE(sale_datetime, '') = ?
           AND COALESCE(qty, 0) = ?
           AND COALESCE(net_sales_amount, 0) = ?
         LIMIT 1
    """
    sale_datetime = (
        row["sale_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        if pd.notna(row["sale_datetime"])
        else ""
    )

    cur = conn.cursor()
    cur.execute(
        sql,
        (
            safe_str(row["order_no"]),
            safe_str(row["product_code_raw"]),
            sale_datetime,
            safe_float(row["qty"]),
            safe_float(row["net_sales_amount"]),
        ),
    )
    return cur.fetchone() is not None


def delete_existing_period_data(conn: sqlite3.Connection, start_date: str, end_date: str) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
          FROM retail_sales
         WHERE sale_date BETWEEN ? AND ?
        """,
        (start_date, end_date),
    )
    count_before = cur.fetchone()[0]

    cur.execute(
        """
        DELETE FROM retail_sales
         WHERE sale_date BETWEEN ? AND ?
        """,
        (start_date, end_date),
    )
    return int(count_before)


def insert_upload_history(
    conn: sqlite3.Connection,
    file_name: str,
    source_sheet_name: str,
    total_rows: int,
    success_rows: int = 0,
    fail_rows: int = 0,
    upload_period_from: Optional[str] = None,
    upload_period_to: Optional[str] = None,
    note: str = "",
) -> Optional[int]:
    if not table_exists(conn, "retail_sales_upload"):
        return None

    cols = get_table_columns(conn, "retail_sales_upload")
    payload = {
        "file_name": file_name,
        "upload_period_from": upload_period_from,
        "upload_period_to": upload_period_to,
        "source_sheet_name": source_sheet_name,
        "total_rows": total_rows,
        "success_rows": success_rows,
        "fail_rows": fail_rows,
        "note": note,
    }
    payload = {k: v for k, v in payload.items() if k in cols}
    if not payload:
        return None

    cur = conn.cursor()
    columns_sql = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    cur.execute(
        f"INSERT INTO retail_sales_upload ({columns_sql}) VALUES ({placeholders})",
        tuple(payload.values()),
    )
    return cur.lastrowid


def update_upload_history(
    conn: sqlite3.Connection,
    upload_id: Optional[int],
    success_rows: int,
    fail_rows: int,
    note: str = "",
):
    if upload_id is None or not table_exists(conn, "retail_sales_upload"):
        return

    cols = get_table_columns(conn, "retail_sales_upload")
    update_fields = []
    params = []

    if "success_rows" in cols:
        update_fields.append("success_rows = ?")
        params.append(success_rows)
    if "fail_rows" in cols:
        update_fields.append("fail_rows = ?")
        params.append(fail_rows)
    if "note" in cols:
        update_fields.append("note = ?")
        params.append(note)

    if not update_fields:
        return

    params.append(upload_id)
    sql = f"UPDATE retail_sales_upload SET {', '.join(update_fields)} WHERE id = ?"
    cur = conn.cursor()
    cur.execute(sql, tuple(params))


def build_insert_payload(
    row: pd.Series,
    db_columns: Set[str],
    upload_id: Optional[int],
    source_file_name: str,
) -> Dict:
    sale_date = row["sale_date"].strftime("%Y-%m-%d") if pd.notna(row["sale_date"]) else None
    sale_datetime = (
        row["sale_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        if pd.notna(row["sale_datetime"])
        else None
    )

    payload = {
        "sale_date": sale_date,
        "sale_datetime": sale_datetime,
        "order_no": safe_str(row["order_no"]),
        "order_channel": safe_str(row["order_channel"]),
        "payment_status": safe_str(row["payment_status"]),
        "product_code_raw": safe_str(row["product_code_raw"]),
        "product_code": safe_str(row["product_code"]),
        "category": safe_str(row["category"]),
        "option_name": safe_str(row["option_name"]),
        "qty": safe_float(row["qty"]),
        "unit_price": safe_float(row["unit_price"]),
        "option_price": safe_float(row["option_price"]),
        "product_discount_name": safe_str(row["product_discount_name"]),
        "order_discount_name": safe_str(row["order_discount_name"]),
        "product_discount_amt": safe_float(row["product_discount_amt"]),
        "order_discount_amt": safe_float(row["order_discount_amt"]),
        "net_sales_amount": safe_float(row["net_sales_amount"]),
        "vat_amount": safe_float(row["vat_amount"]),
        "taxable_yn": safe_str(row["taxable_yn"]),
        "upload_id": upload_id,
        "source_file_name": source_file_name,
        "source_row_no": int(row["source_row_no"]) if pd.notna(row["source_row_no"]) else None,
    }

    return {k: v for k, v in payload.items() if k in db_columns}

def build_manual_input_raw_df(form: Dict) -> pd.DataFrame:
    """
    엑셀 '상품 주문 상세내역' 1행과 동일한 구조의 DataFrame 생성
    -> 이후 clean_item_df()로 동일 처리
    """
    row = {
        "주문기준일자": form.get("주문기준일자"),
        "결제상태": form.get("결제상태", ""),
        "주문시작시각": form.get("주문시작시각"),
        "주문채널": form.get("주문채널", ""),
        "주문번호": form.get("주문번호", ""),
        "상품코드": form.get("상품코드", ""),
        "카테고리": form.get("카테고리", ""),
        "옵션": form.get("옵션", ""),
        "수량": form.get("수량", 0),
        "상품가격": form.get("상품가격", 0),
        "옵션가격": form.get("옵션가격", 0),
        "상품할인 금액": form.get("상품할인 금액", 0),
        "주문할인 금액": form.get("주문할인 금액", 0),
        "실판매금액 \n (할인, 옵션 포함)": form.get("실판매금액 \n (할인, 옵션 포함)", 0),
        "과세여부": form.get("과세여부", ""),
        "부가세액": form.get("부가세액", 0),
    }

    # 업로드 파일에 있을 수도 있는 선택 컬럼도 맞춰줌
    row["상품할인"] = form.get("상품할인", "")
    row["주문할인"] = form.get("주문할인", "")

    return pd.DataFrame([row])


def render_manual_input(conn: sqlite3.Connection, retail_sales_cols: Set[str]):
    st.markdown("---")
    st.subheader("소매 매출 1건 직접 입력")

    now_ts = pd.Timestamp.now()

    with st.form("retail_manual_input_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        sale_date = c1.date_input("주문기준일자", value=now_ts.date())
        sale_datetime = c2.datetime_input("주문시작시각", value=now_ts.to_pydatetime())
        payment_status = c3.selectbox("결제상태", ["완료", "취소", "대기", "환불", "기타"], index=0)

        c4, c5, c6 = st.columns(3)
        order_channel = c4.text_input("주문채널", value="포스")
        order_no = c5.text_input("주문번호")
        category = c6.text_input("카테고리", value= "CIGAR")

        c7, c8, c9 = st.columns(3)
        product_code_raw = c7.text_input("상품코드")
        option_name = c8.text_input("옵션", value="")
        taxable_yn = c9.selectbox("과세여부", ["과세", "면세", "비과세", ""], index=0)

        c10, c11, c12 = st.columns(3)
        qty = c10.number_input("수량", min_value=0, value=1, step=1)
        unit_price = c11.number_input("상품가격", min_value=0, value=0, step=1000)
        option_price = c12.number_input("옵션가격", min_value=0, value=0, step=1000)

        c13, c14, c15 = st.columns(3)
        product_discount_amt = c13.number_input("상품할인 금액", min_value=0, value=0, step=1000)
        order_discount_amt = c14.number_input("주문할인 금액", min_value=0, value=0, step=1000)
        vat_amount = c15.number_input("부가세액", min_value=0, value=0, step=100)

        c16, c17 = st.columns(2)
        product_discount_name = c16.text_input("상품할인", value="")
        order_discount_name = c17.text_input("주문할인", value="")

        auto_calc = st.checkbox("실판매금액 자동계산", value=True)

        if auto_calc:
            net_sales_amount = max(
                0.0,
                float(qty) * (float(unit_price) + float(option_price))
                - float(product_discount_amt)
                - float(order_discount_amt),
            )
            st.info(f"실판매금액: {net_sales_amount:,.0f}원")
        else:
            net_sales_amount = st.number_input(
                "실판매금액 (할인, 옵션 포함)",
                min_value=0.0,
                value=0.0,
                step=1000.0,
            )

        submitted = st.form_submit_button("직접 입력 저장", type="primary")

    if not submitted:
        return

    raw_df = build_manual_input_raw_df(
        {
            "주문기준일자": sale_date,
            "결제상태": payment_status,
            "주문시작시각": sale_datetime,
            "주문채널": order_channel,
            "주문번호": order_no,
            "상품코드": product_code_raw,
            "카테고리": category,
            "옵션": option_name,
            "수량": qty,
            "상품가격": unit_price,
            "옵션가격": option_price,
            "상품할인": product_discount_name,
            "주문할인": order_discount_name,
            "상품할인 금액": product_discount_amt,
            "주문할인 금액": order_discount_amt,
            "실판매금액 \n (할인, 옵션 포함)": net_sales_amount,
            "과세여부": taxable_yn,
            "부가세액": vat_amount,
        }
    )

    df = clean_item_df(raw_df)
    row = df.iloc[0]

    validation = validate_item_df(df)

    error_messages = []
    if safe_str(row["order_no"]) == "":
        error_messages.append("주문번호를 입력해주세요.")
    if pd.isna(row["sale_datetime"]):
        error_messages.append("주문시작시각이 올바르지 않습니다.")
    if safe_str(row["product_code_raw"]) == "":
        error_messages.append("상품코드를 입력해주세요.")
    if pd.isna(row["qty"]) or safe_float(row["qty"]) <= 0:
        error_messages.append("수량은 0보다 커야 합니다.")
    if pd.isna(row["net_sales_amount"]):
        error_messages.append("실판매금액이 올바르지 않습니다.")

    product_codes = fetch_product_codes(conn)
    is_mapped = True
    if product_codes:
        is_mapped = row["product_code"] in product_codes

    if not is_mapped:
        st.warning(f"상품코드 미매핑: {row['product_code_raw']} → {row['product_code']}")

    if is_duplicate_row(conn, row):
        st.warning("동일 데이터가 이미 존재합니다. 저장을 중단합니다.")
        return

    if error_messages:
        for msg in error_messages:
            st.error(msg)
        return

    try:
        cur = conn.cursor()
        conn.execute("BEGIN")

        upload_id = insert_upload_history(
            conn=conn,
            file_name="MANUAL_INPUT",
            source_sheet_name=ITEM_SHEET_NAME,
            total_rows=1,
            success_rows=0,
            fail_rows=0,
            upload_period_from=str(row["sale_date"]) if pd.notna(row["sale_date"]) else None,
            upload_period_to=str(row["sale_date"]) if pd.notna(row["sale_date"]) else None,
            note="직접 입력 시작",
        )

        payload = build_insert_payload(
            row=row,
            db_columns=retail_sales_cols,
            upload_id=upload_id,
            source_file_name="MANUAL_INPUT",
        )

        columns_sql = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        cur.execute(
            f"INSERT INTO retail_sales ({columns_sql}) VALUES ({placeholders})",
            tuple(payload.values()),
        )

        update_upload_history(
            conn=conn,
            upload_id=upload_id,
            success_rows=1,
            fail_rows=0,
            note="직접 입력 완료",
        )

        conn.commit()
        st.success("직접 입력 저장이 완료되었습니다.")

    except Exception as e:
        conn.rollback()
        st.error(f"직접 입력 저장 중 오류 발생: {e}")

def render():
    st.subheader("소매 매출 업로드")

    conn = get_conn()

    try:
        if not table_exists(conn, "retail_sales"):
            st.error("retail_sales 테이블이 없습니다.")
            return

        retail_sales_cols = get_table_columns(conn, "retail_sales")
        missing_db_cols = sorted(MIN_REQUIRED_DB_COLUMNS - retail_sales_cols)
        if missing_db_cols:
            st.error(
                "retail_sales 테이블의 필수 컬럼이 부족합니다: "
                + ", ".join(missing_db_cols)
            )
            return

        uploaded_file = st.file_uploader("엑셀 파일 업로드", type=["xlsx"])

        only_completed = st.checkbox("결제상태가 '완료'인 데이터만 업로드", value=True)
        skip_duplicates = st.checkbox("중복 데이터는 건너뛰기", value=True)
        replace_period = st.checkbox("같은 기간 기존 데이터를 삭제 후 재업로드", value=False)

        if uploaded_file is None:
            st.info(f"'{ITEM_SHEET_NAME}' 시트를 업로드 대상으로 사용합니다.")
        else:
            try:
                df_item_raw = load_excel(uploaded_file)
            except Exception as e:
                st.error(f"엑셀 읽기 오류: {e}")
                return

            missing_cols = [col for col in REQUIRED_COLUMNS if col not in df_item_raw.columns]
            if missing_cols:
                st.error("필수 컬럼이 없습니다: " + ", ".join(missing_cols))
                st.write("현재 컬럼 목록", list(df_item_raw.columns))
                return

            df = clean_item_df(df_item_raw)

            if only_completed:
                df = df[df["payment_status"] == "완료"].copy()

            if df.empty:
                st.warning("업로드 가능한 데이터가 없습니다.")
                return

            validation = validate_item_df(df)

            product_codes = fetch_product_codes(conn)
            st.caption("상품코드 매핑 기준: product_mst + non_cigar_product_mst")

            if product_codes:
                df["product_mapped"] = df["product_code"].isin(product_codes)
                unmapped_df = df[~df["product_mapped"]][["product_code_raw", "product_code"]].drop_duplicates()
                validation["상품코드 미매핑"] = int(len(unmapped_df))
            else:
                unmapped_df = pd.DataFrame(columns=["product_code_raw", "product_code"])
                validation["상품코드 미매핑"] = 0

            duplicate_count = 0
            if skip_duplicates:
                for _, row in df.iterrows():
                    if is_duplicate_row(conn, row):
                        duplicate_count += 1
            validation["중복 예상"] = duplicate_count

            c1, c2, c3, c4 = st.columns(4)
            metrics = list(validation.items())
            for idx, (label, value) in enumerate(metrics):
                [c1, c2, c3, c4][idx % 4].metric(label, f"{value:,}")

            sale_from = df["sale_date"].min()
            sale_to = df["sale_date"].max()
            st.caption(f"업로드 대상 기간: {sale_from} ~ {sale_to}")

            with st.expander("미리보기", expanded=True):
                preview_cols = [
                    "sale_date",
                    "sale_datetime",
                    "payment_status",
                    "order_no",
                    "product_code_raw",
                    "product_code",
                    "category",
                    "qty",
                    "unit_price",
                    "net_sales_amount",
                    "vat_amount",
                ]
                st.dataframe(df[preview_cols].head(30), use_container_width=True)

            if not unmapped_df.empty:
                with st.expander("상품코드 미매핑 목록", expanded=False):
                    st.dataframe(unmapped_df, use_container_width=True)

            if st.button("업로드 실행", type="primary"):
                cur = conn.cursor()
                inserted = 0
                skipped = 0
                failed = 0

                try:
                    conn.execute("BEGIN")

                    if replace_period and sale_from and sale_to:
                        deleted_count = delete_existing_period_data(
                            conn,
                            str(sale_from),
                            str(sale_to),
                        )
                        st.info(f"기존 기간 데이터 삭제: {deleted_count:,}건")

                    upload_id = insert_upload_history(
                        conn=conn,
                        file_name=uploaded_file.name,
                        source_sheet_name=ITEM_SHEET_NAME,
                        total_rows=len(df),
                        success_rows=0,
                        fail_rows=0,
                        upload_period_from=str(sale_from) if sale_from else None,
                        upload_period_to=str(sale_to) if sale_to else None,
                        note="업로드 시작",
                    )

                    for _, row in df.iterrows():
                        if (
                            safe_str(row["order_no"]) == ""
                            or pd.isna(row["sale_datetime"])
                            or safe_str(row["product_code_raw"]) == ""
                            or pd.isna(row["qty"])
                            or pd.isna(row["net_sales_amount"])
                        ):
                            failed += 1
                            continue

                        if skip_duplicates and is_duplicate_row(conn, row):
                            skipped += 1
                            continue

                        payload = build_insert_payload(
                            row=row,
                            db_columns=retail_sales_cols,
                            upload_id=upload_id,
                            source_file_name=uploaded_file.name,
                        )

                        if not payload:
                            failed += 1
                            continue

                        columns_sql = ", ".join(payload.keys())
                        placeholders = ", ".join(["?"] * len(payload))
                        cur.execute(
                            f"INSERT INTO retail_sales ({columns_sql}) VALUES ({placeholders})",
                            tuple(payload.values()),
                        )
                        inserted += 1

                    update_upload_history(
                        conn=conn,
                        upload_id=upload_id,
                        success_rows=inserted,
                        fail_rows=failed,
                        note=f"완료 / 중복스킵 {skipped}건",
                    )

                    conn.commit()
                    st.success(
                        f"업로드 완료: 저장 {inserted:,}건 / 중복 스킵 {skipped:,}건 / 실패 {failed:,}건"
                    )

                except Exception as e:
                    conn.rollback()
                    st.error(f"업로드 중 오류 발생: {e}")

        render_manual_input(conn, retail_sales_cols)

    finally:
        conn.close()
