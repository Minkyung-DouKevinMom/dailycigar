import sqlite3
import pandas as pd
import os

DB_PATH = r"C:\DAILYCIGAR_DB\cigar.db"
EXCEL_PATH = r"C:\DAILYCIGAR_DB\brand_mst.xlsx"
SHEET_NAME = "Sheet1"

print("현재 작업폴더:", os.getcwd())
print("DB 파일 존재:", os.path.exists(DB_PATH))
print("엑셀 파일 존재:", os.path.exists(EXCEL_PATH))

# 1. 엑셀 읽기
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
df.columns = df.columns.map(lambda x: str(x).strip())

print("엑셀 컬럼:", df.columns.tolist())

# 필수 컬럼 체크
required_cols = ["상품명", "Flavor", "Strength", "Guide"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"엑셀에 '{col}' 컬럼이 없습니다.")

# 상품명 없는 행 제거
df = df[df["상품명"].notna()].copy()

# 문자열 정리
for col in required_cols:
    df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)

# 2. DB 연결
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 3. INSERT / UPDATE
# 상품명이 이미 있으면 갱신, 없으면 신규 입력
upsert_sql = """
INSERT INTO blend_profile_mst (
    product_name,
    flavor,
    strength,
    guide,
    source_file_name,
    source_sheet_name
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(product_name) DO UPDATE SET
    flavor = excluded.flavor,
    strength = excluded.strength,
    guide = excluded.guide,
    source_file_name = excluded.source_file_name,
    source_sheet_name = excluded.source_sheet_name,
    updated_at = CURRENT_TIMESTAMP
"""

count = 0

for _, row in df.iterrows():
    product_name = row["상품명"]
    flavor = row["Flavor"]
    strength = row["Strength"]
    guide = row["Guide"]

    cur.execute(upsert_sql, (
        product_name,
        flavor,
        strength,
        guide,
        os.path.basename(EXCEL_PATH),
        SHEET_NAME
    ))
    count += 1

conn.commit()
conn.close()

print(f"blend_profile_mst 입력 완료: {count}건")