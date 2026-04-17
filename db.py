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
        total_tax_krw,
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
        product_code,
        product_name,
        size_name,
        export_package_type,
        export_package_qty,
        discount_rate,
        export_unit_price_usd,
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
        proposal_retail_price_krw,
        store_retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no,
        notes,
        raw_row_json,
        raw_formula_json
    FROM import_item
    WHERE id = ?
    """
    return run_query(sql, [item_id])

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


def get_import_item_list_filtered(batch_id, keyword=""):
    sql = """
    SELECT
        id,
        batch_id,
        product_code,
        product_name,
        size_name,
        import_unit_qty,
        import_total_cost_krw,
        total_weight_g,
        retail_price_krw,
        proposal_retail_price_krw,
        store_retail_price_krw,
        supply_price_krw,
        margin_krw,
        source_row_no,
        notes
    FROM import_item
    WHERE batch_id = ?
    """
    params = [batch_id]

    if keyword and str(keyword).strip():
        sql += """
        AND (
            product_name LIKE ?
            OR size_name LIKE ?
            OR product_code LIKE ?
            OR notes LIKE ?
        )
        """
        like_kw = f"%{keyword.strip()}%"
        params.extend([like_kw, like_kw, like_kw, like_kw])

    sql += " ORDER BY source_row_no, id DESC"
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
        description,
        detail_description,
        created_at,
        updated_at
    FROM blend_profile_mst
    ORDER BY id,product_name
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
        description,
        created_at,
        updated_at
    FROM blend_profile_mst
    WHERE id = ?
    """
    return run_query(sql, [row_id])


def upsert_blend_profile(product_name, flavor, strength, guide, description=None):
    sql = """
    INSERT INTO blend_profile_mst (
        product_name,
        flavor,
        strength,
        guide,
        description
    ) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(product_name) DO UPDATE SET
        flavor = excluded.flavor,
        strength = excluded.strength,
        guide = excluded.guide,
        description = excluded.description,
        updated_at = CURRENT_TIMESTAMP
    """
    execute(sql, [product_name, flavor, strength, guide, description])


def update_blend_profile(row_id, product_name, flavor, strength, guide,
                          description=None, detail_description=None):
    sql = """
    UPDATE blend_profile_mst
    SET product_name=?, flavor=?, strength=?, guide=?,
        description=?, detail_description=?,
        updated_at=CURRENT_TIMESTAMP
    WHERE id=?
    """
    execute(sql, [product_name, flavor, strength, guide,
                  description, detail_description, row_id])


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
            OR COALESCE(i.product_code, '') LIKE ?
        )
        """)
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw])

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT
        b.version_name,
        i.product_code,
        i.product_name,
        i.size_name,
        (1-i.discount_rate)*100 discount_rate,
        i.export_unit_price_usd,
        i.import_unit_cost_krw,
        i.unit_weight_g,
        i.individual_tax_krw,
        i.tobacco_tax_krw,
        i.local_education_tax_krw,
        i.health_charge_krw,
        i.import_vat_krw,
        i.tax_total_krw,
        i.korea_cost_krw,

        i.local_box_price_php,
        i.local_unit_price_php,
        i.local_unit_price_krw,

        i.retail_price_krw,
        i.supply_price_krw,
        i.supply_vat_krw,
        i.margin_krw,
        i.retail_margin_rate,
        i.wholesale_margin_rate,
        i.proposal_retail_price_krw,
        i.store_retail_price_krw

    FROM import_item i
    JOIN import_batch b ON i.batch_id = b.id
    {where_sql}
    ORDER BY i.source_row_no, b.id DESC, i.product_name, i.size_name
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
    tax_rule_id=None,
    notes=None,
):
    cols_df = run_query("PRAGMA table_info(import_batch)")
    existing_cols = set(cols_df["name"].tolist()) if not cols_df.empty else set()

    insert_cols = []
    placeholders = []
    params = []

    def add_col(col_name, value):
        if col_name in existing_cols:
            insert_cols.append(col_name)
            placeholders.append("?")
            params.append(value)

    add_col("version_name", version_name)
    add_col("import_date", import_date)
    add_col("supplier_name", supplier_name)
    add_col("usd_to_krw_rate", usd_to_krw_rate)
    add_col("php_to_krw_rate", php_to_krw_rate)
    add_col("local_markup_rate", local_markup_rate)
    add_col("tax_rule_id", tax_rule_id)
    add_col("notes", notes)

    if "created_at" in existing_cols:
        insert_cols.append("created_at")
        placeholders.append("CURRENT_TIMESTAMP")

    if not insert_cols:
        raise Exception("import_batch 테이블에 INSERT 가능한 컬럼이 없습니다.")

    sql = f"""
    INSERT INTO import_batch (
        {", ".join(insert_cols)}
    )
    VALUES (
        {", ".join(placeholders)}
    )
    """
    execute(sql, params)

def update_import_batch(
    batch_id,
    version_name,
    import_date=None,
    supplier_name=None,
    usd_to_krw_rate=None,
    php_to_krw_rate=None,
    local_markup_rate=None,
    tax_rule_id=None,
    notes=None,
):
    cols_df = run_query("PRAGMA table_info(import_batch)")
    existing_cols = set(cols_df["name"].tolist()) if not cols_df.empty else set()

    update_parts = []
    params = []

    if "version_name" in existing_cols:
        update_parts.append("version_name = ?")
        params.append(version_name)

    if "import_date" in existing_cols:
        update_parts.append("import_date = ?")
        params.append(import_date)

    if "supplier_name" in existing_cols:
        update_parts.append("supplier_name = ?")
        params.append(supplier_name)

    if "usd_to_krw_rate" in existing_cols:
        update_parts.append("usd_to_krw_rate = ?")
        params.append(usd_to_krw_rate)

    if "php_to_krw_rate" in existing_cols:
        update_parts.append("php_to_krw_rate = ?")
        params.append(php_to_krw_rate)

    if "local_markup_rate" in existing_cols:
        update_parts.append("local_markup_rate = ?")
        params.append(local_markup_rate)

    if "tax_rule_id" in existing_cols:
        update_parts.append("tax_rule_id = ?")
        params.append(tax_rule_id)

    if "notes" in existing_cols:
        update_parts.append("notes = ?")
        params.append(notes)

    if "updated_at" in existing_cols:
        update_parts.append("updated_at = CURRENT_TIMESTAMP")

    if not update_parts:
        raise Exception("import_batch 테이블에 업데이트 가능한 컬럼이 없습니다.")

    sql = f"""
    UPDATE import_batch
    SET
        {", ".join(update_parts)}
    WHERE id = ?
    """
    params.append(batch_id)

    execute(sql, params)

def delete_import_batch(batch_id: int):
    conn = get_conn()
    cur = conn.cursor()

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

def get_product_intro_export_data(brand_keyword="", use_yn="전체"):
    sql = """
    SELECT
        A.product_name,
        A.size_name,
        B.flavor,
        B.strength,
        CAST(C.length_mm AS TEXT) || ' mm' AS length_text,
        CAST(C.ring_gauge AS TEXT) AS rg,
        C.smoking_time_text AS time_text,
        B.guide AS guide_text,
        A.retail_price_krw,
        A.supply_price_krw,
        A.supply_total_krw
    FROM import_item A
    LEFT JOIN blend_profile_mst B
        ON A.product_name = B.product_name
    LEFT JOIN product_mst C
        ON A.product_name = C.product_name
        AND A.SIZE_NAME = C.SIZE_NAME
    WHERE 1=1
    """
    params = []

    if brand_keyword:
        sql += """
        AND (
            A.product_name LIKE ?
            OR C.brand_name LIKE ?
        )
        """
        keyword = f"%{brand_keyword}%"
        params.extend([keyword, keyword])

    if use_yn in ("Y", "N"):
        sql += " AND COALESCE(C.use_yn, 'Y') = ? "
        params.append(use_yn)

    sql += " ORDER BY A.source_row_no, C.id "

    return run_query(sql, params)

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


def get_import_batch_tax_rule(batch_id):
    cols_df = run_query("PRAGMA table_info(import_batch)")
    existing_cols = set(cols_df["name"].tolist()) if not cols_df.empty else set()

    if "tax_rule_id" not in existing_cols:
        return get_latest_tax_rule_for_import_calc()

    sql = """
    SELECT tax_rule_id
    FROM import_batch
    WHERE id = ?
    """
    df = run_query(sql, [batch_id])

    if df.empty or pd.isna(df.iloc[0]["tax_rule_id"]):
        return get_latest_tax_rule_for_import_calc()

    return get_tax_rule_by_id(int(df.iloc[0]["tax_rule_id"])) or get_latest_tax_rule_for_import_calc()


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
    df = run_query(sql, [batch_id])
    return df.iloc[0].to_dict() if not df.empty else {}


def get_import_item_defaults_by_product_code(product_code: str):
    sql = """
    SELECT
        source_row_no,
        local_box_price_php,
        local_unit_price_php
    FROM import_item
    WHERE product_code = ?
    ORDER BY id DESC
    LIMIT 1
    """
    return run_query(sql, [product_code])


def get_import_item_defaults_by_name_size(product_name: str, size_name: str):
    sql = """
    SELECT
        source_row_no,
        local_box_price_php,
        local_unit_price_php
    FROM import_item
    WHERE product_name = ?
      AND size_name = ?
    ORDER BY id DESC
    LIMIT 1
    """
    return run_query(sql, [product_name, size_name])

def upsert_import_item_full(
    item_id=None,
    batch_id=None,
    product_name=None,
    size_name=None,
    product_code=None,
    export_package_type=None,
    export_package_qty=None,
    export_box_price_usd=None,
    discounted_box_price_usd=None,
    discount_rate=None,
    import_unit_qty=None,
    export_unit_price_usd=None,
    import_unit_cost_krw=None,
    import_total_cost_krw=None,
    unit_weight_g=None,
    total_weight_g=None,
    individual_tax_krw=None,
    tobacco_tax_krw=None,
    local_education_tax_krw=None,
    health_charge_krw=None,
    import_vat_krw=None,
    tax_total_krw=None,
    tax_total_all_krw=None,
    korea_cost_krw=None,
    local_box_price_php=None,
    local_unit_price_php=None,
    local_unit_price_krw=None,
    retail_price_krw=None,
    proposal_retail_price_krw=None,
    supply_price_krw=None,
    supply_vat_krw=None,
    supply_total_krw=None,
    retail_margin_rate=None,
    wholesale_margin_rate=None,
    store_retail_price_krw=None,
    margin_krw=None,
    source_row_no=None,
    notes=None,
    raw_row_json=None,
    raw_formula_json=None,
):
    import_item_cols_df = run_query("PRAGMA table_info(import_item)")
    import_item_cols = set(import_item_cols_df["name"].tolist()) if not import_item_cols_df.empty else set()

    field_values = {
        "batch_id": batch_id,
        "product_name": product_name,
        "size_name": size_name,
        "product_code": product_code,
        "export_package_type": export_package_type,
        "export_package_qty": export_package_qty,
        "export_box_price_usd": export_box_price_usd,
        "discounted_box_price_usd": discounted_box_price_usd,
        "discount_rate": discount_rate,
        "import_unit_qty": import_unit_qty,
        "export_unit_price_usd": export_unit_price_usd,
        "import_unit_cost_krw": import_unit_cost_krw,
        "import_total_cost_krw": import_total_cost_krw,
        "unit_weight_g": unit_weight_g,
        "total_weight_g": total_weight_g,
        "individual_tax_krw": individual_tax_krw,
        "tobacco_tax_krw": tobacco_tax_krw,
        "local_education_tax_krw": local_education_tax_krw,
        "health_charge_krw": health_charge_krw,
        "import_vat_krw": import_vat_krw,
        "tax_total_krw": tax_total_krw,
        "tax_total_all_krw": tax_total_all_krw,
        "korea_cost_krw": korea_cost_krw,
        "local_box_price_php": local_box_price_php,
        "local_unit_price_php": local_unit_price_php,
        "local_unit_price_krw": local_unit_price_krw,
        "retail_price_krw": retail_price_krw,
        "supply_price_krw": supply_price_krw,
        "supply_vat_krw": supply_vat_krw,
        "supply_total_krw": supply_total_krw,
        "retail_margin_rate": retail_margin_rate,
        "wholesale_margin_rate": wholesale_margin_rate,
        "proposal_retail_price_krw": proposal_retail_price_krw,
        "store_retail_price_krw": store_retail_price_krw,
        "margin_krw": margin_krw,
        "source_row_no": source_row_no,
        "notes": notes,
        "raw_row_json": raw_row_json,
        "raw_formula_json": raw_formula_json,
    }

    valid_items = [(k, v) for k, v in field_values.items() if k in import_item_cols]

    if item_id:
        prev_df = run_query("SELECT batch_id FROM import_item WHERE id = ?", [item_id])
        old_batch_id = int(prev_df.iloc[0]["batch_id"]) if not prev_df.empty and pd.notna(prev_df.iloc[0]["batch_id"]) else None

        update_parts = [f"{k} = ?" for k, _ in valid_items]
        params = [v for _, v in valid_items]

        if "updated_at" in import_item_cols:
            update_parts.append("updated_at = CURRENT_TIMESTAMP")

        sql = f"""
        UPDATE import_item
           SET {", ".join(update_parts)}
         WHERE id = ?
        """
        params.append(item_id)
        execute(sql, params)

        if "refresh_import_batch_totals" in globals():
            if old_batch_id:
                refresh_import_batch_totals(old_batch_id)
            if batch_id and batch_id != old_batch_id:
                refresh_import_batch_totals(batch_id)
            elif batch_id:
                refresh_import_batch_totals(batch_id)

    else:
        insert_cols = [k for k, _ in valid_items]
        placeholders = ", ".join(["?"] * len(insert_cols))
        sql = f"""
        INSERT INTO import_item (
            {", ".join(insert_cols)}
        ) VALUES ({placeholders})
        """
        execute(sql, [v for _, v in valid_items])

        if "refresh_import_batch_totals" in globals() and batch_id:
            refresh_import_batch_totals(batch_id)

def refresh_import_batch_totals(batch_id):
    item_df = run_query(
        """
        SELECT
            COUNT(*) AS total_item_count,
            COALESCE(SUM(import_unit_qty), 0) AS total_unit_qty,
            COALESCE(SUM(total_weight_g), 0) AS total_weight_g,
            COALESCE(SUM(COALESCE(export_unit_price_usd, 0) * COALESCE(import_unit_qty, 0)), 0) AS total_amount_usd,
            COALESCE(SUM(import_total_cost_krw), 0) AS total_amount_krw,
            COALESCE(SUM(tax_total_all_krw), 0) AS total_tax_krw
        FROM import_item
        WHERE batch_id = ?
        """,
        [batch_id],
    )

    if item_df.empty:
        total_item_count = 0
        total_unit_qty = 0
        total_weight_g = 0
        total_amount_usd = 0
        total_amount_krw = 0
        total_tax_krw = 0
    else:
        row = item_df.iloc[0]
        total_item_count = int(row["total_item_count"] or 0)
        total_unit_qty = float(row["total_unit_qty"] or 0)
        total_weight_g = float(row["total_weight_g"] or 0)
        total_amount_usd = float(row["total_amount_usd"] or 0)
        total_amount_krw = float(row["total_amount_krw"] or 0)
        total_tax_krw = float(row["total_tax_krw"] or 0)

    cols_df = run_query("PRAGMA table_info(import_batch)")
    existing_cols = set(cols_df["name"].tolist()) if not cols_df.empty else set()

    update_parts = []
    params = []

    if "total_item_count" in existing_cols:
        update_parts.append("total_item_count = ?")
        params.append(total_item_count)

    if "total_unit_qty" in existing_cols:
        update_parts.append("total_unit_qty = ?")
        params.append(total_unit_qty)

    if "total_weight_g" in existing_cols:
        update_parts.append("total_weight_g = ?")
        params.append(total_weight_g)

    if "total_amount_usd" in existing_cols:
        update_parts.append("total_amount_usd = ?")
        params.append(total_amount_usd)

    if "total_amount_krw" in existing_cols:
        update_parts.append("total_amount_krw = ?")
        params.append(total_amount_krw)

    if "total_tax_krw" in existing_cols:
        update_parts.append("total_tax_krw = ?")
        params.append(total_tax_krw)

    if "updated_at" in existing_cols:
        update_parts.append("updated_at = CURRENT_TIMESTAMP")

    if not update_parts:
        return

    sql = f"""
    UPDATE import_batch
    SET
        {", ".join(update_parts)}
    WHERE id = ?
    """
    params.append(batch_id)

    execute(sql, params)

def get_all_partner_for_select():
    sql = """
    SELECT
        id AS partner_id,
        partner_name
    FROM partner_mst
    WHERE COALESCE(status, 'active') = 'active'
    ORDER BY partner_name
    """
    return run_query(sql)


def get_partner_detail_by_id(partner_id):
    sql = """
        SELECT
        p.id AS partner_id,
        p.partner_name,
        p.address,
        p.owner_name,
        p.contact_name,
        p.phone,
        h.grade_code,
        COALESCE(g.estimate_discount_rate, 0) AS estimate_discount_rate
    FROM partner_mst p
    LEFT JOIN partner_grade_history h
        ON h.partner_id = p.id
    AND date('now') BETWEEN h.start_date AND COALESCE(h.end_date, '9999-12-31')
    LEFT JOIN partner_grade_mst g
        ON h.grade_code = g.grade_code
    WHERE p.id = ?
    LIMIT 1
    """
    df = run_query(sql, [partner_id])
    return df.iloc[0].to_dict() if not df.empty else {}


def get_estimate_cigar_items(only_in_stock: bool = False):
    """
    견적서용 시가 목록.
    - product_mst.use_yn = 'Y' 인 상품만 포함
    - 가격은 import_date <= 오늘인 배치 중 가장 최신 import_item의 값 사용
    - current_stock: 오늘 이전 입고 - 소매출고 - 도매출고 - 기타출고
    - source_row_no: 최신 배치 기준 값을 가져와 정렬에 사용
    - only_in_stock=True 이면 current_stock > 0 인 상품만 반환
    """
    stock_filter = (
        """  AND (
            COALESCE(si.total_in,      0)
          - COALESCE(rs.retail_out,    0)
          - COALESCE(ws.wholesale_out, 0)
          - COALESCE(so.other_out,     0)
      ) > 0"""
        if only_in_stock
        else ""
    )

    sql = f"""
    SELECT
        p.product_code,
        p.product_name,
        p.size_name,
        COALESCE(i.retail_price_krw, 0)                                    AS retail_price_krw,
        COALESCE(i.proposal_retail_price_krw, i.retail_price_krw, 0)       AS proposal_retail_price_krw,
        COALESCE(i.supply_price_krw, 0)                                    AS supply_price_krw,
        COALESCE(i.source_row_no, 999999)                                  AS source_row_no,
        COALESCE(si.total_in,      0)
        - COALESCE(rs.retail_out,    0)
        - COALESCE(ws.wholesale_out, 0)
        - COALESCE(so.other_out,     0)                                    AS current_stock

    FROM product_mst p

    LEFT JOIN import_item i
        ON i.id = (
            SELECT ii.id
            FROM import_item ii
            JOIN import_batch ib ON ii.batch_id = ib.id
            WHERE ii.product_name = p.product_name
              AND COALESCE(ii.size_name, '') = COALESCE(p.size_name, '')
              AND ib.import_date <= date('now')
            ORDER BY ib.import_date DESC, ii.id DESC
            LIMIT 1
        )

    LEFT JOIN (
        SELECT ii.product_code, SUM(ii.import_unit_qty) AS total_in
        FROM import_item ii
        JOIN import_batch ib ON ii.batch_id = ib.id
        WHERE ib.import_date <= date('now')
        GROUP BY ii.product_code
    ) si ON p.product_code = si.product_code

    LEFT JOIN (
        SELECT product_code, SUM(qty) AS retail_out
        FROM retail_sales
        WHERE category = 'CIGAR'
        GROUP BY product_code
    ) rs ON p.product_code = rs.product_code

    LEFT JOIN (
        SELECT pm2.product_code, SUM(ws.qty) AS wholesale_out
        FROM wholesale_sales ws
        JOIN product_mst pm2 ON ws.cigar_product_id = pm2.id
        WHERE ws.item_type = 'cigar'
        GROUP BY pm2.product_code
    ) ws ON p.product_code = ws.product_code

    LEFT JOIN (
        SELECT product_code, SUM(qty) AS other_out
        FROM stock_out
        GROUP BY product_code
    ) so ON p.product_code = so.product_code

    WHERE COALESCE(p.use_yn, 'Y') = 'Y'
    {stock_filter}

    ORDER BY COALESCE(i.source_row_no, 999999), p.product_name, p.size_name
    """
    return run_query(sql)


def get_estimate_non_cigar_items():
    sql = """
    SELECT
        product_code,
        product_name,
        '' AS size_name,
        COALESCE(retail_price, 0) AS retail_price_krw,
        COALESCE(wholesale_price, 0) AS supply_price_krw
    FROM non_cigar_product_mst
    WHERE COALESCE(is_active, 1) = 1
    ORDER BY COALESCE(source_row_no, 999999), product_name, id
    """
    return run_query(sql)

def get_store_menu_view(batch_id=None, keyword=""):
    sql = """
    SELECT
        ib.version_name,
        ii.batch_id,
        ii.product_code,
        ii.product_name,
        ii.size_name,
        pm.length_mm,
        pm.ring_gauge,
        COALESCE(ii.store_retail_price_krw, 0) AS store_retail_price_krw,
        COALESCE(bp.flavor, '') AS flavor,
        COALESCE(bp.strength, '') AS strength,
        COALESCE(bp.guide, '') AS guide,
        COALESCE(bp.id, 999999) AS profile_id,
        COALESCE(ii.source_row_no, 999999) AS source_row_no,
        ib.import_date
    FROM import_item ii
    LEFT JOIN import_batch ib
        ON ii.batch_id = ib.id
    LEFT JOIN blend_profile_mst bp
        ON TRIM(ii.product_name) = TRIM(bp.product_name)
    LEFT JOIN product_mst pm
        ON ii.product_code = pm.product_code
    -- ★ 핵심: 동일 product_name+size_name 중 import_date 최신 배치만
    INNER JOIN (
        SELECT
            i2.product_name,
            i2.size_name,
            MAX(ib2.import_date) AS max_import_date
        FROM import_item i2
        JOIN import_batch ib2 ON i2.batch_id = ib2.id
        GROUP BY i2.product_name, i2.size_name
    ) latest
        ON  ii.product_name = latest.product_name
        AND ii.size_name     = latest.size_name
        AND ib.import_date   = latest.max_import_date
    WHERE pm.use_yn = 'Y'
    """

    params = []

    if batch_id:
        sql += " AND ii.batch_id = ? "
        params.append(batch_id)

    if keyword:
        sql += """
        AND (
            ii.product_code LIKE ?
            OR ii.product_name LIKE ?
            OR ii.size_name LIKE ?
            OR bp.flavor LIKE ?
            OR bp.strength LIKE ?
            OR bp.guide LIKE ?
        )
        """
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw, kw, kw])

    sql += """
    ORDER BY
        profile_id,
        source_row_no,
        ii.size_name,
        ii.id
    """

    return run_query(sql, params)


# ──────────────────────────────────────────────
# 1. 테이블 초기화 (최초 1회 실행)
# ──────────────────────────────────────────────

def init_stock_out_table():
    """stock_out 테이블이 없으면 생성"""
    execute("""
        CREATE TABLE IF NOT EXISTS stock_out (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            out_date     TEXT    NOT NULL,
            product_code TEXT    NOT NULL,
            qty          INTEGER NOT NULL CHECK(qty > 0),
            out_type     TEXT    NOT NULL
                             CHECK(out_type IN ('sample','gift_set','disposal','etc')),
            partner_id   INTEGER,
            note         TEXT,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)


# ──────────────────────────────────────────────
# 2. 재고 현황 조회
# ──────────────────────────────────────────────

def get_stock_summary(keyword: str = "", include_inactive: bool = False):
    """
    상품별 현재고 요약
      total_in       : import_item 입고 합계 (import_date <= 오늘인 배치만)
      retail_out     : retail_sales (CIGAR) 차감
      wholesale_out  : wholesale_sales (cigar) 차감
      other_out      : stock_out (샘플/선물세트/폐기/기타) 차감
      current_stock  : 현재고 = total_in - retail_out - wholesale_out - other_out
    """
    use_filter = "" if include_inactive else "AND p.use_yn = 'Y'"
    keyword_filter = ""
    params = []

    if keyword.strip():
        keyword_filter = """
            AND (
                p.product_code LIKE ?
                OR p.product_name LIKE ?
                OR p.size_name   LIKE ?
            )
        """
        kw = f"%{keyword.strip()}%"
        params.extend([kw, kw, kw])

    sql = f"""
        SELECT
            p.product_code,
            p.product_name,
            p.size_name,
            p.use_yn,
            COALESCE(si.total_in,      0) AS total_in,
            COALESCE(rs.retail_out,    0) AS retail_out,
            COALESCE(ws.wholesale_out, 0) AS wholesale_out,
            COALESCE(so.other_out,     0) AS other_out,
            COALESCE(si.total_in,      0)
            - COALESCE(rs.retail_out,    0)
            - COALESCE(ws.wholesale_out, 0)
            - COALESCE(so.other_out,     0) AS current_stock
        FROM product_mst p

        -- 입고 (오늘 이전 수입날짜만)
        LEFT JOIN (
            SELECT i.product_code, SUM(i.import_unit_qty) AS total_in
            FROM import_item i
            JOIN import_batch b ON i.batch_id = b.id
            WHERE b.import_date <= date('now')
            GROUP BY i.product_code
        ) si ON p.product_code = si.product_code

        -- 소매 판매
        LEFT JOIN (
            SELECT product_code, SUM(qty) AS retail_out
            FROM retail_sales
            WHERE category = 'CIGAR'
            GROUP BY product_code
        ) rs ON p.product_code = rs.product_code

        -- 도매 판매
        LEFT JOIN (
            SELECT pm.product_code, SUM(ws.qty) AS wholesale_out
            FROM wholesale_sales ws
            JOIN product_mst pm ON ws.cigar_product_id = pm.id
            WHERE ws.item_type = 'cigar'
            GROUP BY pm.product_code
        ) ws ON p.product_code = ws.product_code

        -- 기타 출고 (샘플/선물세트/폐기)
        LEFT JOIN (
            SELECT product_code, SUM(qty) AS other_out
            FROM stock_out
            GROUP BY product_code
        ) so ON p.product_code = so.product_code

        WHERE 1=1
        {use_filter}
        {keyword_filter}
        ORDER BY p.product_name, p.size_name
    """
    return run_query(sql, params)


def get_stock_detail(product_code: str):
    """
    특정 상품의 입출고 이벤트 전체 이력 (날짜 오름차순)
    입고는 import_date <= 오늘인 배치만 포함
    type 컬럼: 'import' | 'retail' | 'wholesale' | 'sample' | 'gift_set' | 'disposal' | 'etc'
    """
    sql = """
    SELECT * FROM (
        -- 입고 (오늘 이전 수입날짜만)
        SELECT
            i.created_at AS event_date,
            'import'     AS event_type,
            b.version_name AS ref_name,
            i.import_unit_qty AS qty_in,
            0                 AS qty_out,
            NULL              AS partner_name,
            NULL              AS note
        FROM import_item i
        JOIN import_batch b ON i.batch_id = b.id
        WHERE i.product_code = ?
          AND b.import_date <= date('now')

        UNION ALL

        -- 소매 판매
        SELECT
            r.sale_date,
            'retail',
            r.order_no,
            0,
            r.qty,
            NULL,
            r.order_channel
        FROM retail_sales r
        WHERE r.product_code = ?
          AND r.category = 'CIGAR'

        UNION ALL

        -- 도매 판매
        SELECT
            w.sale_date,
            'wholesale',
            p.partner_name,
            0,
            w.qty,
            p.partner_name,
            w.notes
        FROM wholesale_sales w
        JOIN product_mst pm ON w.cigar_product_id = pm.id
        LEFT JOIN partner_mst p ON w.partner_id = p.id
        WHERE pm.product_code = ?
          AND w.item_type = 'cigar'

        UNION ALL

        -- 기타 출고 (샘플/선물세트/폐기/기타)
        SELECT
            so.out_date,
            so.out_type,
            COALESCE(p2.partner_name, '-'),
            0,
            so.qty,
            p2.partner_name,
            so.note
        FROM stock_out so
        LEFT JOIN partner_mst p2 ON so.partner_id = p2.id
        WHERE so.product_code = ?
    )
    ORDER BY event_date
    """
    return run_query(sql, [product_code, product_code, product_code, product_code])


# ──────────────────────────────────────────────
# 3. stock_out CRUD
# ──────────────────────────────────────────────

OUT_TYPE_LABELS = {
    "sample":    "샘플/서비스",
    "gift_set":  "선물세트",
    "disposal":  "폐기/손실",
    "etc":       "기타",
}

def get_stock_out_list(product_code: str = "", out_type: str = "", partner_id: int = None):
    """stock_out 이력 조회"""
    conditions = ["1=1"]
    params = []

    if product_code.strip():
        conditions.append("so.product_code = ?")
        params.append(product_code.strip())

    if out_type.strip():
        conditions.append("so.out_type = ?")
        params.append(out_type.strip())

    if partner_id:
        conditions.append("so.partner_id = ?")
        params.append(partner_id)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            so.id,
            so.out_date,
            so.product_code,
            p_mst.product_name,
            p_mst.size_name,
            so.qty,
            so.out_type,
            so.partner_id,
            pm.partner_name,
            so.note,
            so.created_at
        FROM stock_out so
        LEFT JOIN product_mst p_mst ON so.product_code = p_mst.product_code
        LEFT JOIN partner_mst pm    ON so.partner_id   = pm.id
        WHERE {where}
        ORDER BY so.out_date DESC, so.id DESC
    """
    return run_query(sql, params)


def insert_stock_out(
    out_date: str,
    product_code: str,
    qty: int,
    out_type: str,
    partner_id: int = None,
    note: str = None,
):
    """기타 출고 1건 등록"""
    execute(
        """
        INSERT INTO stock_out (out_date, product_code, qty, out_type, partner_id, note)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [out_date, product_code, qty, out_type, partner_id, note],
    )


def update_stock_out(
    row_id: int,
    out_date: str,
    product_code: str,
    qty: int,
    out_type: str,
    partner_id: int = None,
    note: str = None,
):
    """기타 출고 1건 수정"""
    execute(
        """
        UPDATE stock_out
           SET out_date     = ?,
               product_code = ?,
               qty          = ?,
               out_type     = ?,
               partner_id   = ?,
               note         = ?,
               updated_at   = CURRENT_TIMESTAMP
         WHERE id = ?
        """,
        [out_date, product_code, qty, out_type, partner_id, note, row_id],
    )


def delete_stock_out(row_id: int):
    """기타 출고 1건 삭제"""
    execute("DELETE FROM stock_out WHERE id = ?", [row_id])


def get_stock_out_one(row_id: int) -> dict:
    """stock_out 단건 조회"""
    df = run_query("SELECT * FROM stock_out WHERE id = ?", [row_id])
    return df.iloc[0].to_dict() if not df.empty else {}


# ──────────────────────────────────────────────
# 4. 거래처별 샘플 수령 현황
# ──────────────────────────────────────────────

def get_sample_summary_by_partner():
    """거래처별 샘플 제공 합계"""
    sql = """
        SELECT
            pm.partner_name,
            so.product_code,
            p.product_name,
            p.size_name,
            SUM(so.qty) AS total_sample_qty,
            MIN(so.out_date) AS first_date,
            MAX(so.out_date) AS last_date
        FROM stock_out so
        JOIN partner_mst pm ON so.partner_id = pm.id
        LEFT JOIN product_mst p ON so.product_code = p.product_code
        WHERE so.out_type = 'sample'
        GROUP BY pm.partner_name, so.product_code
        ORDER BY pm.partner_name, so.product_code
    """
    return run_query(sql)