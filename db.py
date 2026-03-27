import sqlite3
import pandas as pd

DB_PATH = r"cigar.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def run_query(sql, params=None):
    conn = get_conn()
    try:
        df = pd.read_sql_query(sql, conn, params=params or [])
        return df
    finally:
        conn.close()


def execute(sql, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def execute_many(sql, data):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
    finally:
        conn.close()


def table_exists(table_name: str) -> bool:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM sqlite_master
    WHERE type='table' AND name=?
    """
    df = run_query(sql, [table_name])
    return int(df.iloc[0]["cnt"]) > 0


def get_table_count(table_name: str) -> int:
    if not table_exists(table_name):
        return 0
    sql = f"SELECT COUNT(*) AS cnt FROM {table_name}"
    df = run_query(sql)
    return int(df.iloc[0]["cnt"])


def get_all_product_mst():
    sql = """
    SELECT
        id,
        product_name,
        size_name,
        product_code,
        length_mm,
        ring_gauge,
        smoking_time_text,
        unit_weight_g,
        box_width_cm,
        box_depth_cm,
        box_height_cm,
        created_at,
        updated_at
    FROM product_mst
    ORDER BY product_name, size_name
    """
    return run_query(sql)


def upsert_product_mst(
    product_name,
    size_name,
    product_code,
    length_mm,
    ring_gauge,
    smoking_time_text,
    unit_weight_g,
    box_width_cm,
    box_depth_cm,
    box_height_cm,
):
    sql = """
    INSERT INTO product_mst (
        product_name,
        size_name,
        product_code,
        length_mm,
        ring_gauge,
        smoking_time_text,
        unit_weight_g,
        box_width_cm,
        box_depth_cm,
        box_height_cm
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(product_code) DO UPDATE SET
        product_name = excluded.product_name,
        size_name = excluded.size_name,
        length_mm = excluded.length_mm,
        ring_gauge = excluded.ring_gauge,
        smoking_time_text = excluded.smoking_time_text,
        unit_weight_g = excluded.unit_weight_g,
        box_width_cm = excluded.box_width_cm,
        box_depth_cm = excluded.box_depth_cm,
        box_height_cm = excluded.box_height_cm,
        updated_at = CURRENT_TIMESTAMP
    """
    execute(sql, [
        product_name,
        size_name,
        product_code,
        length_mm,
        ring_gauge,
        smoking_time_text,
        unit_weight_g,
        box_width_cm,
        box_depth_cm,
        box_height_cm,
    ])


def delete_product_mst(product_code):
    sql = "DELETE FROM product_mst WHERE product_code = ?"
    execute(sql, [product_code])


def get_all_import_batch():
    sql = """
    SELECT
        id,
        version_name,
        import_date,
        supplier_name,
        usd_to_krw_rate,
        php_to_krw_rate,
        local_markup_rate,
        total_item_count,
        total_unit_qty,
        total_weight_g,
        total_amount_usd,
        total_amount_krw,
        notes,
        created_at
    FROM import_batch
    ORDER BY id DESC
    """
    return run_query(sql)


def get_import_items_by_batch(batch_id):
    sql = """
    SELECT
        id,
        product_name,
        size_name,
        product_code,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no
    FROM import_item
    WHERE batch_id = ?
    ORDER BY source_row_no
    """
    return run_query(sql, [batch_id])


def update_import_batch_note(batch_id, notes):
    sql = """
    UPDATE import_batch
    SET notes = ?
    WHERE id = ?
    """
    execute(sql, [notes, batch_id])


def get_all_tax_rule():
    sql = """
    SELECT
        id,
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes,
        created_at
    FROM tax_rule
    ORDER BY effective_from DESC, id DESC
    """
    return run_query(sql)


def upsert_tax_rule(
    rule_name,
    effective_from,
    effective_to,
    individual_tax_per_g,
    tobacco_tax_per_g,
    local_education_rate,
    health_charge_per_g,
    import_vat_rate,
    notes,
):
    sql = """
    INSERT INTO tax_rule (
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute(sql, [
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes,
    ])


def update_tax_rule(
    row_id,
    rule_name,
    effective_from,
    effective_to,
    individual_tax_per_g,
    tobacco_tax_per_g,
    local_education_rate,
    health_charge_per_g,
    import_vat_rate,
    notes,
):
    sql = """
    UPDATE tax_rule
    SET
        rule_name = ?,
        effective_from = ?,
        effective_to = ?,
        individual_tax_per_g = ?,
        tobacco_tax_per_g = ?,
        local_education_rate = ?,
        health_charge_per_g = ?,
        import_vat_rate = ?,
        notes = ?
    WHERE id = ?
    """
    execute(sql, [
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes,
        row_id,
    ])


def delete_tax_rule(row_id):
    sql = "DELETE FROM tax_rule WHERE id = ?"
    execute(sql, [row_id])

def get_import_item_detail(item_id):
    sql = """
    SELECT
        id,
        batch_id,
        product_name,
        size_name,
        product_code,
        export_box_price_usd,
        discounted_box_price_usd,
        discount_rate,
        import_unit_qty,
        export_unit_price_usd,
        import_unit_cost_krw,
        import_total_cost_krw,
        unit_weight_g,
        total_weight_g,
        individual_tax_krw,
        tobacco_tax_krw,
        local_education_tax_krw,
        health_charge_krw,
        import_vat_krw,
        tax_total_krw,
        tax_total_all_krw,
        korea_cost_krw,
        local_box_price_php,
        local_unit_price_php,
        local_unit_price_krw,
        retail_price_krw,
        supply_price_krw,
        supply_vat_krw,
        supply_total_krw,
        margin_krw,
        retail_margin_rate,
        wholesale_margin_rate,
        store_retail_price_krw,
        source_row_no,
        raw_row_json,
        raw_formula_json,
        created_at
    FROM import_item
    WHERE id = ?
    """
    df = run_query(sql, [item_id])
    return df


def update_import_item(
    item_id,
    product_name,
    size_name,
    product_code,
    import_unit_qty,
    import_total_cost_krw,
    total_weight_g,
    retail_price_krw,
    supply_price_krw,
    margin_krw,
):
    sql = """
    UPDATE import_item
    SET
        product_name = ?,
        size_name = ?,
        product_code = ?,
        import_unit_qty = ?,
        import_total_cost_krw = ?,
        total_weight_g = ?,
        retail_price_krw = ?,
        supply_price_krw = ?,
        margin_krw = ?
    WHERE id = ?
    """
    execute(sql, [
        product_name,
        size_name,
        product_code,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        supply_price_krw,
        margin_krw,
        item_id,
    ])


def delete_import_item(item_id):
    sql = "DELETE FROM import_item WHERE id = ?"
    execute(sql, [item_id])


def get_import_item_list_filtered(batch_id=None, keyword=None):
    conditions = []
    params = []

    if batch_id is not None:
        conditions.append("batch_id = ?")
        params.append(batch_id)

    if keyword:
        conditions.append("""
        (
            COALESCE(product_name, '') LIKE ?
            OR COALESCE(size_name, '') LIKE ?
            OR COALESCE(product_code, '') LIKE ?
        )
        """)
        like_kw = f"%{keyword}%"
        params.extend([like_kw, like_kw, like_kw])

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT
        id,
        batch_id,
        product_name,
        size_name,
        product_code,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no
    FROM import_item
    {where_sql}
    ORDER BY batch_id DESC, source_row_no ASC, id ASC
    """
    return run_query(sql, params)

def get_export_price_item_filtered(keyword=None, package_type=None, package_qty=None):
    conditions = []
    params = []

    if keyword:
        conditions.append("""
        (
            COALESCE(product_name, '') LIKE ?
            OR COALESCE(size_name, '') LIKE ?
        )
        """)
        like_kw = f"%{keyword}%"
        params.extend([like_kw, like_kw])

    if package_type:
        conditions.append("package_type = ?")
        params.append(package_type)

    if package_qty is not None:
        conditions.append("package_qty = ?")
        params.append(package_qty)

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT
        id,
        product_name,
        size_name,
        package_type,
        package_qty,
        export_price_usd,
        created_at
    FROM export_price_item
    {where_sql}
    ORDER BY product_name, size_name, package_type, package_qty
    """
    return run_query(sql, params)


def get_all_blend_profile():
    sql = """
    SELECT
        id,
        product_name,
        flavor,
        strength,
        guide,
        created_at,
        updated_at
    FROM blend_profile_mst
    ORDER BY product_name
    """
    return run_query(sql)


def get_blend_profile_detail(row_id):
    sql = """
    SELECT
        id,
        product_name,
        flavor,
        strength,
        guide,
        created_at,
        updated_at
    FROM blend_profile_mst
    WHERE id = ?
    """
    return run_query(sql, [row_id])


def upsert_blend_profile(product_name, flavor, strength, guide):
    sql = """
    INSERT INTO blend_profile_mst (
        product_name,
        flavor,
        strength,
        guide
    ) VALUES (?, ?, ?, ?)
    ON CONFLICT(product_name) DO UPDATE SET
        flavor = excluded.flavor,
        strength = excluded.strength,
        guide = excluded.guide,
        updated_at = CURRENT_TIMESTAMP
    """
    execute(sql, [product_name, flavor, strength, guide])


def update_blend_profile(row_id, product_name, flavor, strength, guide):
    sql = """
    UPDATE blend_profile_mst
    SET
        product_name = ?,
        flavor = ?,
        strength = ?,
        guide = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    """
    execute(sql, [product_name, flavor, strength, guide, row_id])


def delete_blend_profile(row_id):
    sql = "DELETE FROM blend_profile_mst WHERE id = ?"
    execute(sql, [row_id])

def get_all_product_mst_for_edit():
    sql = """
        SELECT
            id,
            product_name,
            size_name,
            product_code,
            COALESCE(use_yn, 'Y') AS use_yn,
            length_mm,
            ring_gauge,
            smoking_time_text,
            unit_weight_g,
            box_width_cm,
            box_depth_cm,
            box_height_cm,
            created_at,
            updated_at
        FROM product_mst
        ORDER BY product_name, size_name, product_code
    """
    return run_query(sql)


def update_product_mst_by_id(
    row_id,
    product_name,
    size_name,
    product_code,
    use_yn,
    length_mm,
    ring_gauge,
    smoking_time_text,
    unit_weight_g,
    box_width_cm,
    box_depth_cm,
    box_height_cm,
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE product_mst
        SET
            product_name = ?,
            size_name = ?,
            product_code = ?,
            use_yn = ?,
            length_mm = ?,
            ring_gauge = ?,
            smoking_time_text = ?,
            unit_weight_g = ?,
            box_width_cm = ?,
            box_depth_cm = ?,
            box_height_cm = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            product_name,
            size_name,
            product_code,
            use_yn,
            length_mm,
            ring_gauge,
            smoking_time_text,
            unit_weight_g,
            box_width_cm,
            box_depth_cm,
            box_height_cm,
            row_id,
        ),
    )
    conn.commit()
    conn.close()


def insert_product_mst(
    product_name,
    size_name,
    product_code,
    use_yn,
    length_mm,
    ring_gauge,
    smoking_time_text,
    unit_weight_g,
    box_width_cm,
    box_depth_cm,
    box_height_cm,
):
    conn = get_conn()  
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO product_mst (
            product_name,
            size_name,
            product_code,
            use_yn,
            length_mm,
            ring_gauge,
            smoking_time_text,
            unit_weight_g,
            box_width_cm,
            box_depth_cm,
            box_height_cm
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            product_name,
            size_name,
            product_code,
            use_yn,
            length_mm,
            ring_gauge,
            smoking_time_text,
            unit_weight_g,
            box_width_cm,
            box_depth_cm,
            box_height_cm,
        ),
    )
    conn.commit()
    conn.close()


def delete_product_mst_by_id(row_id):
    sql = "DELETE FROM product_mst WHERE id = ?"
    execute(sql, [row_id])

def get_price_analysis_view(batch_id=None, keyword=None):
    conditions = []
    params = []

    if batch_id:
        conditions.append("i.batch_id = ?")
        params.append(batch_id)

    if keyword:
        conditions.append("""
        (
            COALESCE(i.product_name, '') LIKE ?
            OR COALESCE(i.size_name, '') LIKE ?
        )
        """)
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT
        b.version_name,
        i.product_name,
        i.size_name,
        i.product_code,

        i.import_unit_qty,
        i.unit_weight_g,
        i.total_weight_g,

        i.individual_tax_krw,
        i.tobacco_tax_krw,
        i.local_education_tax_krw,
        i.health_charge_krw,
        i.import_vat_krw,
        i.tax_total_all_krw,

        i.import_total_cost_krw,
        i.korea_cost_krw,

        i.retail_price_krw,
        i.supply_price_krw,
        i.store_retail_price_krw,
        i.margin_krw

    FROM import_item i
    JOIN import_batch b ON i.batch_id = b.id
    {where_sql}
    ORDER BY b.id DESC, i.product_name, i.size_name
    """

    return run_query(sql, params)

def get_import_batch_detail(batch_id: int) -> pd.DataFrame:
    sql = """
        SELECT
            id,
            version_name,
            import_date,
            supplier_name,
            usd_to_krw_rate,
            php_to_krw_rate,
            local_markup_rate,
            notes,
            created_at
        FROM import_batch
        WHERE id = ?
    """
    return run_query(sql, [batch_id])


def create_import_batch(
    version_name,
    import_date=None,
    supplier_name=None,
    usd_to_krw_rate=None,
    php_to_krw_rate=None,
    local_markup_rate=None,
    notes=None,
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO import_batch (
            version_name,
            import_date,
            supplier_name,
            usd_to_krw_rate,
            php_to_krw_rate,
            local_markup_rate,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            version_name,
            import_date,
            supplier_name,
            usd_to_krw_rate,
            php_to_krw_rate,
            local_markup_rate,
            notes,
        ),
    )
    conn.commit()
    conn.close()


def update_import_batch(
    batch_id,
    version_name,
    import_date=None,
    supplier_name=None,
    usd_to_krw_rate=None,
    php_to_krw_rate=None,
    local_markup_rate=None,
    notes=None,
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE import_batch
        SET
            version_name = ?,
            import_date = ?,
            supplier_name = ?,
            usd_to_krw_rate = ?,
            php_to_krw_rate = ?,
            local_markup_rate = ?,
            notes = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            version_name,
            import_date,
            supplier_name,
            usd_to_krw_rate,
            php_to_krw_rate,
            local_markup_rate,
            notes,
            batch_id,
        ),
    )
    conn.commit()
    conn.close()


def delete_import_batch(batch_id: int):
    conn = get_conn()
    cur = conn.cursor()

    # import_item이 연결되어 있으면 먼저 확인
    cnt = cur.execute(
        "SELECT COUNT(*) FROM import_item WHERE batch_id = ?",
        (batch_id,),
    ).fetchone()[0]

    if cnt > 0:
        conn.close()
        raise ValueError("해당 수입 버전에 연결된 import_item이 있어 삭제할 수 없습니다.")

    cur.execute("DELETE FROM import_batch WHERE id = ?", (batch_id,))
    conn.commit()
    conn.close()

def insert_import_item(
    batch_id,
    product_name,
    size_name,
    product_code,
    import_unit_qty,
    import_total_cost_krw,
    total_weight_g,
    retail_price_krw,
    supply_price_krw,
    margin_krw,
    source_row_no=None,
):
    sql = """
    INSERT INTO import_item (
        batch_id,
        product_name,
        size_name,
        product_code,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute(sql, [
        batch_id,
        product_name,
        size_name,
        product_code,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no,
    ])


def _get_table_columns(conn, table_name: str) -> set:
    try:
        df = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
        if df.empty or "name" not in df.columns:
            return set()
        return set(df["name"].astype(str).tolist())
    except Exception:
        return set()


def _pick_expr(alias: str, columns: set, candidates: list, default_sql: str = "''") -> str:
    for col in candidates:
        if col in columns:
            return f"{alias}.{col}"
    return default_sql

def get_product_intro_export_data(brand_keyword: str = "", use_yn: str = "전체") -> pd.DataFrame:
    """
    상품소개서 엑셀 출력용 데이터 조회

    기준:
    - 상품 기본: product_mst
    - 프로파일: blend_profile_mst (product_name 기준 조인)
    - 소비자가: import_item.retail_price_krw 우선
    """

    sql = """
    SELECT
        p.product_name AS product_name,
        COALESCE(i.size_name, p.size_name, '') AS size_name,
        COALESCE(bp.flavor, '') AS flavor,
        COALESCE(bp.strength, '') AS strength,

        CASE
            WHEN p.length_mm IS NOT NULL AND p.length_mm <> ''
                THEN CAST(p.length_mm AS TEXT) || ' mm'
            ELSE ''
        END AS length_text,

        COALESCE(p.ring_gauge, '') AS rg,
        COALESCE(p.smoking_time_text, '') AS time_text,
        COALESCE(bp.guide, '') AS guide_text,
        COALESCE(i.retail_price_krw, '') AS retail_price_krw

    FROM product_mst p
    LEFT JOIN blend_profile_mst bp
        ON TRIM(p.product_name) = TRIM(bp.product_name)
    LEFT JOIN (
        SELECT
            product_name,
            size_name,
            retail_price_krw,
            ROW_NUMBER() OVER (
                PARTITION BY product_name, size_name
                ORDER BY id DESC
            ) AS rn
        FROM import_item
    ) i
        ON TRIM(p.product_name) = TRIM(i.product_name)
       AND TRIM(COALESCE(p.size_name, '')) = TRIM(COALESCE(i.size_name, ''))
       AND i.rn = 1
    WHERE 1=1
    """

    params = []

    if brand_keyword:
        sql += """
        AND (
            COALESCE(p.product_name, '') LIKE ?
            OR COALESCE(p.size_name, '') LIKE ?
            OR COALESCE(p.product_code, '') LIKE ?
        )
        """
        kw = f"%{brand_keyword}%"
        params.extend([kw, kw, kw])

    if use_yn in ("Y", "N"):
        sql += " AND COALESCE(p.use_yn, 'Y') = ? "
        params.append(use_yn)

    sql += """
    ORDER BY
        COALESCE(p.product_name, ''),
        COALESCE(p.size_name, '')
    """

    df = run_query(sql, params)

    wanted_cols = [
        "product_name", "size_name", "flavor", "strength",
        "length_text", "rg", "time_text", "guide_text", "retail_price_krw"
    ]

    for col in wanted_cols:
        if col not in df.columns:
            df[col] = ""

    return df[wanted_cols]

def get_export_price_product_names():
    sql = """
    SELECT DISTINCT product_name
    FROM export_price_item
    WHERE COALESCE(product_name, '') <> ''
    ORDER BY product_name
    """
    df = run_query(sql)
    return df["product_name"].tolist() if not df.empty else []


def get_export_price_sizes_by_product(product_name):
    sql = """
    SELECT DISTINCT size_name
    FROM export_price_item
    WHERE product_name = ?
      AND COALESCE(size_name, '') <> ''
    ORDER BY size_name
    """
    df = run_query(sql, [product_name])
    return df["size_name"].tolist() if not df.empty else []


def get_export_price_package_options(product_name, size_name):
    sql = """
    SELECT
        id,
        product_name,
        size_name,
        package_type,
        package_qty,
        export_price_usd,
        created_at
    FROM export_price_item
    WHERE product_name = ?
      AND size_name = ?
    ORDER BY package_type, package_qty
    """
    return run_query(sql, [product_name, size_name])


def get_product_mst_one(product_name, size_name):
    sql = """
    SELECT
        id,
        product_name,
        size_name,
        product_code,
        use_yn,
        length_mm,
        ring_gauge,
        smoking_time_text,
        unit_weight_g
    FROM product_mst
    WHERE product_name = ?
      AND size_name = ?
    ORDER BY id DESC
    LIMIT 1
    """
    df = run_query(sql, [product_name, size_name])
    return df.iloc[0].to_dict() if not df.empty else {}


def get_tax_rule_by_id(rule_id):
    sql = """
    SELECT
        id,
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes
    FROM tax_rule
    WHERE id = ?
    """
    df = run_query(sql, [rule_id])
    return df.iloc[0].to_dict() if not df.empty else {}


def get_latest_tax_rule_for_import_calc():
    sql = """
    SELECT
        id,
        rule_name,
        effective_from,
        effective_to,
        individual_tax_per_g,
        tobacco_tax_per_g,
        local_education_rate,
        health_charge_per_g,
        import_vat_rate,
        notes
    FROM tax_rule
    ORDER BY effective_from DESC, id DESC
    LIMIT 1
    """
    df = run_query(sql)
    return df.iloc[0].to_dict() if not df.empty else {
        "id": None,
        "rule_name": "",
        "individual_tax_per_g": 0,
        "tobacco_tax_per_g": 0,
        "local_education_rate": 0,
        "health_charge_per_g": 0,
        "import_vat_rate": 0,
    }


def get_import_batch_one(batch_id):
    sql = """
    SELECT
        id,
        version_name,
        usd_to_krw_rate,
        php_to_krw_rate,
        tax_rule_id,
        raw_json,
        created_at
    FROM import_batch
    WHERE id = ?
    """
    df = run_query(sql, [batch_id])
    return df.iloc[0].to_dict() if not df.empty else {}


def update_import_batch_rates_and_tax_rule(batch_id, usd_to_krw_rate, php_to_krw_rate, tax_rule_id):
    sql = """
    UPDATE import_batch
    SET
        usd_to_krw_rate = ?,
        php_to_krw_rate = ?,
        tax_rule_id = ?
    WHERE id = ?
    """
    execute(sql, [usd_to_krw_rate, php_to_krw_rate, tax_rule_id, batch_id])


def get_tax_rule_options():
    sql = """
    SELECT
        id,
        rule_name,
        effective_from,
        effective_to
    FROM tax_rule
    ORDER BY effective_from DESC, id DESC
    """
    return run_query(sql)


def upsert_import_item_full(
    item_id,
    batch_id,
    product_name,
    size_name,
    product_code,
    export_box_price_usd,
    discounted_box_price_usd,
    discount_rate,
    import_unit_qty,
    export_unit_price_usd,
    import_unit_cost_krw,
    import_total_cost_krw,
    unit_weight_g,
    total_weight_g,
    individual_tax_krw,
    tobacco_tax_krw,
    local_education_tax_krw,
    health_charge_krw,
    import_vat_krw,
    tax_total_krw,
    tax_total_all_krw,
    korea_cost_krw,
    local_box_price_php,
    local_unit_price_php,
    local_unit_price_krw,
    php_to_krw_rate,
    use_php_price,
    usd_to_krw_rate,
    tax_rule_id,
    retail_price_krw,
    supply_price_krw,
    margin_krw,
    source_row_no,
    raw_row_json,
    raw_formula_json,
):
    if item_id:
        sql = """
        UPDATE import_item
        SET
            batch_id = ?,
            product_name = ?,
            size_name = ?,
            product_code = ?,
            export_box_price_usd = ?,
            discounted_box_price_usd = ?,
            discount_rate = ?,
            import_unit_qty = ?,
            export_unit_price_usd = ?,
            import_unit_cost_krw = ?,
            import_total_cost_krw = ?,
            unit_weight_g = ?,
            total_weight_g = ?,
            individual_tax_krw = ?,
            tobacco_tax_krw = ?,
            local_education_tax_krw = ?,
            health_charge_krw = ?,
            import_vat_krw = ?,
            tax_total_krw = ?,
            tax_total_all_krw = ?,
            korea_cost_krw = ?,
            local_box_price_php = ?,
            local_unit_price_php = ?,
            local_unit_price_krw = ?,
            php_to_krw_rate = ?,
            use_php_price = ?,
            usd_to_krw_rate = ?,
            tax_rule_id = ?,
            retail_price_krw = ?,
            supply_price_krw = ?,
            margin_krw = ?,
            source_row_no = ?,
            raw_row_json = ?,
            raw_formula_json = ?
        WHERE id = ?
        """
        execute(sql, [
            batch_id,
            product_name,
            size_name,
            product_code,
            export_box_price_usd,
            discounted_box_price_usd,
            discount_rate,
            import_unit_qty,
            export_unit_price_usd,
            import_unit_cost_krw,
            import_total_cost_krw,
            unit_weight_g,
            total_weight_g,
            individual_tax_krw,
            tobacco_tax_krw,
            local_education_tax_krw,
            health_charge_krw,
            import_vat_krw,
            tax_total_krw,
            tax_total_all_krw,
            korea_cost_krw,
            local_box_price_php,
            local_unit_price_php,
            local_unit_price_krw,
            php_to_krw_rate,
            use_php_price,
            usd_to_krw_rate,
            tax_rule_id,
            retail_price_krw,
            supply_price_krw,
            margin_krw,
            source_row_no,
            raw_row_json,
            raw_formula_json,
            item_id,
        ])
    else:
        sql = """
        INSERT INTO import_item (
            batch_id,
            product_name,
            size_name,
            product_code,
            export_box_price_usd,
            discounted_box_price_usd,
            discount_rate,
            import_unit_qty,
            export_unit_price_usd,
            import_unit_cost_krw,
            import_total_cost_krw,
            unit_weight_g,
            total_weight_g,
            individual_tax_krw,
            tobacco_tax_krw,
            local_education_tax_krw,
            health_charge_krw,
            import_vat_krw,
            tax_total_krw,
            tax_total_all_krw,
            korea_cost_krw,
            local_box_price_php,
            local_unit_price_php,
            local_unit_price_krw,
            php_to_krw_rate,
            use_php_price,
            usd_to_krw_rate,
            tax_rule_id,
            retail_price_krw,
            supply_price_krw,
            margin_krw,
            source_row_no,
            raw_row_json,
            raw_formula_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        execute(sql, [
            batch_id,
            product_name,
            size_name,
            product_code,
            export_box_price_usd,
            discounted_box_price_usd,
            discount_rate,
            import_unit_qty,
            export_unit_price_usd,
            import_unit_cost_krw,
            import_total_cost_krw,
            unit_weight_g,
            total_weight_g,
            individual_tax_krw,
            tobacco_tax_krw,
            local_education_tax_krw,
            health_charge_krw,
            import_vat_krw,
            tax_total_krw,
            tax_total_all_krw,
            korea_cost_krw,
            local_box_price_php,
            local_unit_price_php,
            local_unit_price_krw,
            php_to_krw_rate,
            use_php_price,
            usd_to_krw_rate,
            tax_rule_id,
            retail_price_krw,
            supply_price_krw,
            margin_krw,
            source_row_no,
            raw_row_json,
            raw_formula_json,
        ])