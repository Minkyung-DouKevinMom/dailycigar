import sqlite3
import pandas as pd
import os

DB_PATH = r"C:\DAILYCIGAR_DB\cigar.db"
EXCEL_PATH = r"C:\DAILYCIGAR_DB\본사수출가격.xlsx"
SHEET_NAME = "(참고)본사정식수출가격"

print("현재 작업폴더:", os.getcwd())
print("DB 파일 존재:", os.path.exists(DB_PATH))
print("엑셀 파일 존재:", os.path.exists(EXCEL_PATH))

# 1. 엑셀 읽기
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
df.columns = df.columns.map(lambda x: str(x).strip())

# 상품명 빈칸은 위 값으로 채우기
if "상품명" in df.columns:
    df["상품명"] = df["상품명"].ffill()

# 옵션 빈칸 제거
df = df[df["옵션"].notna()].copy()

# 숫자 컬럼 매핑
package_columns = {
    "PACK OF 3": ("PACK", 3),
    "PACK OF 5": ("PACK", 5),
    "PACK OF 10": ("PACK", 10),
    "PACK OF 20": ("PACK", 20),
    "BOX OF 5": ("BOX", 5),
    "BOX OF 10": ("BOX", 10),
    "BOX OF 25": ("BOX", 25),
    "BOX OF 50": ("BOX", 50),
}

# 2. DB 연결
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

insert_sql = """
INSERT OR IGNORE INTO export_price_item (
    product_name,
    option_name,
    package_type,
    package_qty,
    export_price_usd,
    source_file_name,
    source_sheet_name,
    row_no
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_count = 0

# 3. 행 단위로 읽어서 세로형으로 저장
for idx, row in df.iterrows():
    product_name = str(row["상품명"]).strip()
    option_name = str(row["옵션"]).strip()
    excel_row_no = idx + 2  # 헤더 포함 기준 대략적인 원본 행번호

    for col_name, (package_type, package_qty) in package_columns.items():
        if col_name not in df.columns:
            continue

        value = row.get(col_name)

        if pd.isna(value):
            continue

        try:
            export_price_usd = float(value)
        except Exception:
            continue

        cur.execute(insert_sql, (
            product_name,
            option_name,
            package_type,
            package_qty,
            export_price_usd,
            os.path.basename(EXCEL_PATH),
            SHEET_NAME,
            excel_row_no
        ))
        insert_count += 1

conn.commit()
conn.close()

print(f"입력 완료: {insert_count}건 처리")