import sqlite3
import os
import pandas as pd
from pathlib import Path
from datetime import date

# =========================
# 0. 파일/기준값 설정
# =========================
DB_PATH = r"C:\DAILYCIGAR_DB\dailycigar.db"

print("현재 작업폴더:", os.getcwd())
print("사용할 DB_PATH:", DB_PATH)
print("DB 파일 존재:", os.path.exists(DB_PATH))

EXCEL_PATH = "Tabacalera 가격분석.xlsx"
SHEET_NAME = "가격분석"

BATCH_NAME = "2026-03 가격분석"
SOURCE_FILE_NAME = "Tabacalera 가격분석.xlsx"
SOURCE_SHEET_NAME = "가격분석"

# 세금/환율 기준값
# 필요하면 여기만 수정하세요.
USD_TO_KRW = 1493.0
PHP_TO_KRW = 28.0

TAX_RULE_NAME = "기본 세율"
TAX_EFFECTIVE_FROM = "2025-09-01"
INDIVIDUAL_TAX_PER_G = 61.0
TOBACCO_TAX_PER_G = 103.0
LOCAL_EDU_RATE = 0.4399
HEALTH_CHARGE_PER_G = 85.8
IMPORT_VAT_RATE = 0.10


# =========================
# 1. 유틸
# =========================
def to_none(v):
    if pd.isna(v):
        return None
    return v


def to_float(v):
    if pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def to_int(v):
    if pd.isna(v):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


# =========================
# 2. DB 연결
# =========================
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

# =========================
# 3. 엑셀 읽기
#    - header=1 : 두 번째 줄을 실제 헤더로 사용
# =========================
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=1)

# 브랜드명(제품명) 빈칸은 위 값으로 채움
#df["제품명"] = df["제품명"].ffill()

# 현재 파일 헤더를 코드에서 표준 이름으로 바꿔 사용
rename_map = {
    "제품명": "brand_name",
    "사이즈": "size_name",
    "박스수출원가($)": "export_box_price_usd",
    "할인가($)": "discounted_box_price_usd",
    "수입개수": "import_qty_boxes",
    "수출가격($/unit)": "export_unit_price_usd",
    "수입원가(KRW)": "import_unit_cost_krw",
    "수입가격합계": "import_total_cost_krw",
    "무게": "unit_weight_g",
    "전체무게": "total_weight_g",
    "개별소비세": "individual_consumption_tax_krw",
    "담배소비세": "tobacco_tax_krw",
    "지방교육세": "local_education_tax_krw",
    "국민건강": "health_charge_krw",
    "부가세": "import_vat_krw",
    "세금합계": "tax_total_per_unit_krw",
    "세금합계(총합계)": "tax_total_all_krw",
    "한국원가(KRW)": "korea_cost_per_unit_krw",
    "현지Box(페소)": "local_box_price_php",
    "현지Unit(페소)": "local_unit_price_php",
    "현지Unit(KRW)": "local_unit_price_krw",
    "소비자가격": "retail_price_krw",
    "공급 가격": "supply_price_krw",
    "공급부가세": "supply_vat_krw",
    "공급금액": "supply_total_krw",
    "업체마진": "margin_krw",
    "소매이익률": "retail_margin_rate",
    "도매이익률": "wholesale_margin_rate",
    "(매장)소비자가격": "store_retail_price_krw",
}
df = df.rename(columns=rename_map)

# 빈 행 제거
df = df[df["brand_name"].notna() & df["size_name"].notna()].copy()


# =========================
# 4. 1번 테이블: import_batch
# =========================
cur.execute("""
    INSERT INTO import_batch (
        batch_name,
        source_file_name,
        source_sheet_name
    ) VALUES (?, ?, ?)
""", (
    BATCH_NAME,
    SOURCE_FILE_NAME,
    SOURCE_SHEET_NAME
))
batch_id = cur.lastrowid


# =========================
# 5. 3번 테이블: tax_rule
#    같은 rule이 이미 있으면 재사용
# =========================
cur.execute("""
    SELECT id
    FROM tax_rule
    WHERE rule_name = ?
      AND effective_from = ?
      AND individual_consumption_tax_per_g = ?
      AND tobacco_tax_per_g = ?
      AND local_education_rate = ?
      AND health_charge_per_g = ?
      AND import_vat_rate = ?
""", (
    TAX_RULE_NAME,
    TAX_EFFECTIVE_FROM,
    INDIVIDUAL_TAX_PER_G,
    TOBACCO_TAX_PER_G,
    LOCAL_EDU_RATE,
    HEALTH_CHARGE_PER_G,
    IMPORT_VAT_RATE
))
row = cur.fetchone()

if row:
    tax_rule_id = row[0]
else:
    cur.execute("""
        INSERT INTO tax_rule (
            rule_name,
            effective_from,
            individual_consumption_tax_per_g,
            tobacco_tax_per_g,
            local_education_rate,
            health_charge_per_g,
            import_vat_rate,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        TAX_RULE_NAME,
        TAX_EFFECTIVE_FROM,
        INDIVIDUAL_TAX_PER_G,
        TOBACCO_TAX_PER_G,
        LOCAL_EDU_RATE,
        HEALTH_CHARGE_PER_G,
        IMPORT_VAT_RATE,
        "가격분석 시트 기준"
    ))
    tax_rule_id = cur.lastrowid


# =========================
# 6. 4번 테이블: fx_rate
#    USD / PHP 각각 등록 또는 재사용
# =========================
fx_effective_date = date.today().isoformat()

def get_or_create_fx(currency_code, rate_to_krw):
    cur.execute("""
        SELECT id
        FROM fx_rate
        WHERE currency_code = ?
          AND effective_date = ?
          AND rate_to_krw = ?
    """, (currency_code, fx_effective_date, rate_to_krw))
    r = cur.fetchone()
    if r:
        return r[0]

    cur.execute("""
        INSERT INTO fx_rate (
            currency_code,
            rate_to_krw,
            effective_date,
            source,
            notes
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        currency_code,
        rate_to_krw,
        fx_effective_date,
        "manual",
        "가격분석 import 시 사용"
    ))
    return cur.lastrowid

usd_fx_rate_id = get_or_create_fx("USD", USD_TO_KRW)
php_fx_rate_id = get_or_create_fx("PHP", PHP_TO_KRW)


# =========================
# 7. 2번 테이블: product_master
#    중복 없이 먼저 등록
# =========================
product_key_to_id = {}

unique_products = (
    df[["brand_name", "size_name", "unit_weight_g"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

for _, r in unique_products.iterrows():
    brand_name = str(r["brand_name"]).strip()
    size_name = str(r["size_name"]).strip()
    product_display_name = f"{brand_name} {size_name}"
    unit_weight_g = to_float(r["unit_weight_g"])

    # line_name은 현재 파일에 별도 컬럼이 없어서 NULL 처리
    line_name = None

    # 이미 있으면 조회
    cur.execute("""
        SELECT id
        FROM product_master
        WHERE brand_name = ?
          AND size_name = ?
          AND line_name IS ?
    """, (brand_name, size_name, line_name))
    existing = cur.fetchone()

    if existing:
        product_id = existing[0]
    else:
        cur.execute("""
            INSERT INTO product_master (
                brand_name,
                line_name,
                size_name,
                product_display_name,
                units_per_box,
                unit_weight_g,
                active_yn
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            brand_name,
            line_name,
            size_name,
            product_display_name,
            None,              # 제품 마스터에는 현재 고정값 없음
            unit_weight_g,
            "Y"
        ))
        product_id = cur.lastrowid

    product_key_to_id[(brand_name, size_name)] = product_id


# =========================
# 8. 5번 테이블: price_analysis_item
#    시트의 각 행을 입력
# =========================
for excel_row_no, r in df.reset_index().iterrows():
    brand_name = str(r["brand_name"]).strip()
    size_name = str(r["size_name"]).strip()
    product_id = product_key_to_id[(brand_name, size_name)]

    export_box_price_usd = to_float(r.get("export_box_price_usd"))
    discounted_box_price_usd = to_float(r.get("discounted_box_price_usd"))
    import_qty_boxes = to_int(r.get("import_qty_boxes"))
    export_unit_price_usd = to_float(r.get("export_unit_price_usd"))
    import_unit_cost_krw = to_float(r.get("import_unit_cost_krw"))
    import_total_cost_krw = to_float(r.get("import_total_cost_krw"))
    unit_weight_g = to_float(r.get("unit_weight_g"))
    total_weight_g = to_float(r.get("total_weight_g"))

    individual_consumption_tax_krw = to_float(r.get("individual_consumption_tax_krw"))
    tobacco_tax_krw = to_float(r.get("tobacco_tax_krw"))
    local_education_tax_krw = to_float(r.get("local_education_tax_krw"))
    health_charge_krw = to_float(r.get("health_charge_krw"))
    import_vat_krw = to_float(r.get("import_vat_krw"))
    tax_total_per_unit_krw = to_float(r.get("tax_total_per_unit_krw"))
    tax_total_all_krw = to_float(r.get("tax_total_all_krw"))
    korea_cost_per_unit_krw = to_float(r.get("korea_cost_per_unit_krw"))

    local_box_price_php = to_float(r.get("local_box_price_php"))
    local_unit_price_php = to_float(r.get("local_unit_price_php"))
    local_unit_price_krw = to_float(r.get("local_unit_price_krw"))

    retail_price_krw = to_float(r.get("retail_price_krw"))
    supply_price_krw = to_float(r.get("supply_price_krw"))
    supply_vat_krw = to_float(r.get("supply_vat_krw"))
    supply_total_krw = to_float(r.get("supply_total_krw"))
    margin_krw = to_float(r.get("margin_krw"))
    retail_margin_rate = to_float(r.get("retail_margin_rate"))
    wholesale_margin_rate = to_float(r.get("wholesale_margin_rate"))
    store_retail_price_krw = to_float(r.get("store_retail_price_krw"))

    # discount_rate 계산
    discount_rate = None
    if export_box_price_usd and discounted_box_price_usd:
        if export_box_price_usd != 0:
            discount_rate = discounted_box_price_usd / export_box_price_usd

    # units_per_box 계산
    # 수입가격합계 / 수입원가(개당) = 총 개수, 이를 박스수로 나눠 박스당 수량 추정
    units_per_box = None
    if import_qty_boxes and import_unit_cost_krw and import_total_cost_krw:
        total_units = import_total_cost_krw / import_unit_cost_krw if import_unit_cost_krw else None
        if total_units and import_qty_boxes != 0:
            units_per_box = int(round(total_units / import_qty_boxes))

    cur.execute("""
        INSERT INTO price_analysis_item (
            batch_id,
            product_id,
            tax_rule_id,
            usd_fx_rate_id,
            php_fx_rate_id,
            row_no,
            remarks,

            export_box_price_usd,
            discount_rate,
            discounted_box_price_usd,
            import_qty_boxes,
            units_per_box,
            export_unit_price_usd,
            usd_to_krw_rate,
            import_unit_cost_krw,
            import_total_cost_krw,
            unit_weight_g,
            total_weight_g,

            individual_consumption_tax_krw,
            tobacco_tax_krw,
            local_education_tax_krw,
            health_charge_krw,
            import_vat_krw,
            tax_total_per_unit_krw,
            tax_total_all_krw,
            korea_cost_per_unit_krw,

            local_box_price_php,
            local_unit_price_php,
            php_to_krw_rate,
            local_unit_price_krw,

            retail_price_krw,
            supply_price_krw,
            supply_vat_krw,
            supply_total_krw,
            margin_krw,
            retail_margin_rate,
            wholesale_margin_rate,
            store_retail_price_krw,

            raw_brand_name,
            raw_line_name,
            raw_size_name,
            raw_product_display_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id,
        product_id,
        tax_rule_id,
        usd_fx_rate_id,
        php_fx_rate_id,
        int(r["index"]) + 2,    # 엑셀 실제 행 느낌으로 보정
        None,

        export_box_price_usd,
        discount_rate,
        discounted_box_price_usd,
        import_qty_boxes,
        units_per_box,
        export_unit_price_usd,
        USD_TO_KRW,
        import_unit_cost_krw,
        import_total_cost_krw,
        unit_weight_g,
        total_weight_g,

        individual_consumption_tax_krw,
        tobacco_tax_krw,
        local_education_tax_krw,
        health_charge_krw,
        import_vat_krw,
        tax_total_per_unit_krw,
        tax_total_all_krw,
        korea_cost_per_unit_krw,

        local_box_price_php,
        local_unit_price_php,
        PHP_TO_KRW,
        local_unit_price_krw,

        retail_price_krw,
        supply_price_krw,
        supply_vat_krw,
        supply_total_krw,
        margin_krw,
        retail_margin_rate,
        wholesale_margin_rate,
        store_retail_price_krw,

        brand_name,
        None,
        size_name,
        f"{brand_name} {size_name}"
    ))


# =========================
# 9. 저장/종료
# =========================
conn.commit()
conn.close()

print("완료: 5개 테이블 입력이 끝났습니다.")