"""
가격 마스터 포장가(takeout_price_krw) 회귀 테스트.

포장가는 매장에서 직접 관리하는 값이므로, 수입제품 저장 시 호출되는
upsert_product_price_mst 가 포장가를 덮어쓰면 안 된다.
"""
from __future__ import annotations

import db


def test_takeout_price_survives_import_upsert(db_path):
    df = db.get_all_product_price_mst()
    assert "takeout_price_krw" in df.columns
    row = df.iloc[0].to_dict()

    db.update_product_price_mst(
        price_id=int(row["id"]),
        supply_price_krw=row["supply_price_krw"],
        retail_price_krw=row["retail_price_krw"],
        store_retail_price_krw=row["store_retail_price_krw"],
        proposal_retail_price_krw=row["proposal_retail_price_krw"],
        is_active=int(row["is_active"]),
        notes=row["notes"],
        takeout_price_krw=12345.0,
    )

    db.upsert_product_price_mst(
        product_name=row["product_name"],
        size_name=row["size_name"],
        product_code=row["product_code"],
        supply_price_krw=row["supply_price_krw"],
        retail_price_krw=row["retail_price_krw"],
        store_retail_price_krw=row["store_retail_price_krw"],
        proposal_retail_price_krw=row["proposal_retail_price_krw"],
        korea_cost_krw=row["korea_cost_krw"],
        batch_id=row["ref_batch_id"],
        notes=row["notes"],
    )

    after = db.get_all_product_price_mst().set_index("id").loc[int(row["id"])]
    assert after["takeout_price_krw"] == 12345.0
