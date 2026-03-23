import sqlite3
import pandas as pd
import os
import re

DB_PATH = r"C:\DAILYCIGAR_DB\cigar.db"
EXCEL_PATH = r"C:\DAILYCIGAR_DB\product_mst.xlsx"
SHEET_NAME = "시가특징"

print("현재 작업폴더:", os.getcwd())
print("DB 파일 존재:", os.path.exists(DB_PATH))
print("엑셀 파일 존재:", os.path.exists(EXCEL_PATH))

# -------------------------
# 유틸 함수
# -------------------------
def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None

def to_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None

def to_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None

def parse_length_mm(value):
    """
    예:
    '128 mm' -> 128.0
    '95 mm'  -> 95.0
    """
    if pd.isna(value):
        return None

    text = str(value).strip().lower()
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if not m:
        return None
    return float(m.group(1))


# -------------------------
# 엑셀 읽기
# -------------------------
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
df.columns = df.columns.map(lambda x: str(x).strip())

print("엑셀 컬럼:", df.columns.tolist())

required_columns = [
    "상품명",
    "사이즈",
    "코드",
    "Length",
    "RG",
    "흡연시간",
    "무게",
    "박스사이즈 가로(CM)",
    "박스사이즈 세로(CM)",
    "박스사이즈 높이(CM)"
]

for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"필수 컬럼이 없습니다: {col}")

# 상품명/사이즈/코드 없는 행 제거
df = df[
    df["상품명"].notna() &
    df["사이즈"].notna() &
    df["코드"].notna()
].copy()

# -------------------------
# DB 연결
# -------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

upsert_sql = """
INSERT INTO product_mst (
    product_name,
    size_name,
    product_code,
    length_text,
    length_mm,
    ring_gauge,
    smoking_time_text,
    unit_weight_g,
    box_width_cm,
    box_depth_cm,
    box_height_cm
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(product_code) DO UPDATE SET
    product_name = excluded.product_name,
    size_name = excluded.size_name,
    length_text = excluded.length_text,
    length_mm = excluded.length_mm,
    ring_gauge = excluded.ring_gauge,
    smoking_time_text = excluded.smoking_time_text,
    unit_weight_g = excluded.unit_weight_g,
    box_width_cm = excluded.box_width_cm,
    box_depth_cm = excluded.box_depth_cm,
    box_height_cm = excluded.box_height_cm,
    updated_at = CURRENT_TIMESTAMP
"""

count = 0

for _, row in df.iterrows():
    product_name = clean_text(row["상품명"])
    size_name = clean_text(row["사이즈"])
    product_code = clean_text(row["코드"])

    length_text = clean_text(row["Length"])
    length_mm = parse_length_mm(row["Length"])
    ring_gauge = to_int(row["RG"])
    smoking_time_text = clean_text(row["흡연시간"])
    unit_weight_g = to_float(row["무게"])

    box_width_cm = to_float(row["박스사이즈 가로(CM)"])
    box_depth_cm = to_float(row["박스사이즈 세로(CM)"])
    box_height_cm = to_float(row["박스사이즈 높이(CM)"])

    cur.execute(upsert_sql, (
        product_name,
        size_name,
        product_code,
        length_text,
        length_mm,
        ring_gauge,
        smoking_time_text,
        unit_weight_g,
        box_width_cm,
        box_depth_cm,
        box_height_cm
    ))
    count += 1

conn.commit()
conn.close()

print(f"PRODUCT_MST 입력 완료: {count}건")