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
        product_code,
        product_name,
        size_name,
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
        retail_margin_rate,
        wholesale_margin_rate,
        proposal_retail_price_krw,
        store_retail_price_krw,
        margin_krw,
        source_row_no,
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
        supply_price_krw,
        margin_krw,
        source_row_no
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
        )
        """
        like_kw = f"%{keyword.strip()}%"
        params.extend([like_kw, like_kw, like_kw])

    sql += " ORDER BY id DESC"
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
    notes=None,
):
    """
    import_version.py 신규 등록 화면과 맞춘 안전한 INSERT
    실제 import_batch 테이블에 존재하는 컬럼만 INSERT 한다.
    """

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
    notes=None,
):
    """
    import_version.py 수정 화면과 맞춘 안전한 UPDATE
    실제 import_batch 테이블에 존재하는 컬럼만 업데이트한다.
    """

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
    """
    import_version.py에서 실제 사용하는 import_batch 컬럼 기준 조회
    """
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


def get_import_item_detail(item_id):
    sql = """
    SELECT
        id,
        batch_id,
        product_code,
        product_name,
        size_name,
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
        supply_price_krw,
        margin_krw,
        source_row_no,
        raw_row_json,
        raw_formula_json
    FROM import_item
    WHERE id = ?
    """
    return run_query(sql, [item_id])


def upsert_import_item_full(
    item_id=None,
    batch_id=None,
    product_name=None,
    size_name=None,
    product_code=None,
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
    supply_price_krw=None,
    supply_vat_krw=None,
    supply_total_krw=None,
    retail_margin_rate=None,
    wholesale_margin_rate=None,
    proposal_retail_price_krw=None,
    store_retail_price_krw=None,
    margin_krw=None,
    source_row_no=None,
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
    """
    import_item 기준으로 import_batch 집계 컬럼 갱신
    import_batch 테이블에 실제 존재하는 컬럼만 UPDATE
    """

    item_df = run_query(
        """
        SELECT
            COUNT(*) AS total_item_count,
            COALESCE(SUM(import_unit_qty), 0) AS total_unit_qty,
            COALESCE(SUM(total_weight_g), 0) AS total_weight_g,
            COALESCE(SUM(discounted_box_price_usd), 0) AS total_amount_usd,
            COALESCE(SUM(import_total_cost_krw), 0) AS total_amount_krw
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
    else:
        row = item_df.iloc[0]
        total_item_count = int(row["total_item_count"] or 0)
        total_unit_qty = float(row["total_unit_qty"] or 0)
        total_weight_g = float(row["total_weight_g"] or 0)
        total_amount_usd = float(row["total_amount_usd"] or 0)
        total_amount_krw = float(row["total_amount_krw"] or 0)

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
        id AS partner_id,
        partner_name,
        owner_name,
        contact_name,
        phone,
        email,
        address,
        status,
        notes
    FROM partner_mst
    WHERE id = ?
    """
    df = run_query(sql, [partner_id])
    return df.iloc[0].to_dict() if not df.empty else {}


def get_estimate_cigar_items():
    sql = """
    SELECT
        i.product_code,
        i.product_name,
        i.size_name,
        COALESCE(i.retail_price_krw, 0) AS retail_price_krw,
        COALESCE(i.proposal_retail_price_krw, retail_price_krw) AS proposal_retail_price_krw,
        COALESCE(i.supply_price_krw, 0) AS supply_price_krw
    FROM import_item i
    INNER JOIN (
        SELECT
            product_name,
            size_name,
            MAX(id) AS max_id
        FROM import_item
        GROUP BY product_name, size_name
    ) x
        ON i.id = x.max_id
    LEFT JOIN product_mst p
        ON i.product_name = p.product_name
       AND COALESCE(i.size_name, '') = COALESCE(p.size_name, '')
    WHERE COALESCE(p.use_yn, 'Y') = 'Y'
    ORDER BY i.source_row_no, i.product_name, i.size_name
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
    ORDER BY product_name
    """
    return run_query(sql)