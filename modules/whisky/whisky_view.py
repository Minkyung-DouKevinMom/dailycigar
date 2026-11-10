"""
위스키(개인사업자) 관리 화면.

데이터는 모두 modules/whisky/whisky_db.py(정본)를 통해 whisky.db 에서 읽고 쓴다.
이 파일에는 SQL 을 쓰지 않는다.
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
import streamlit as st

from modules.common.fmt import fmt_krw
from modules.whisky import whisky_db as wdb

FLASH_KEY = "whisky_flash"


def _flash(message: str) -> None:
    st.session_state[FLASH_KEY] = message


def _show_flash() -> None:
    # 메시지가 있든 없든 항상 같은 자리를 차지하는 컨테이너에 표시한다.
    # (메시지 유무에 따라 아래 탭의 위치가 바뀌면 Streamlit 이 탭을 새로 그려 첫 탭으로 돌아간다)
    box = st.container()
    message = st.session_state.pop(FLASH_KEY, None)
    if message:
        box.success(message)


def _blank(df: pd.DataFrame, cols) -> pd.DataFrame:
    """글자 컬럼의 빈 값(None/NaN)을 빈칸으로 바꾼 사본."""
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    return out


def _product_options(products: pd.DataFrame) -> dict:
    return {row["product_name"]: int(row["id"]) for _, row in products.iterrows()}


# =========================================================
# 재고 현황
# =========================================================
def render_stock_tab() -> None:
    include_inactive = st.checkbox("판매 중지 상품 포함", value=False, key="whisky_stock_inactive")
    stock = wdb.get_stock(active_only=not include_inactive)

    if stock.empty:
        st.info("등록된 위스키가 없습니다. '상품' 탭에서 먼저 상품을 등록해 주세요.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("상품 수", f"{len(stock)}개")
    c2.metric("미개봉 병", f"{int(stock['full_bottles'].sum())}병")
    c3.metric("재고 금액 (평균 매입가 기준)", fmt_krw(stock["stock_value"].sum()))

    short = stock[stock["stock_ml"] < 0]
    if not short.empty:
        st.warning(
            "재고가 부족(음수)한 상품이 있습니다: "
            + ", ".join(short["product_name"].tolist())
            + " — 매입이 빠졌는지 확인하거나 '재고 실사' 탭에서 맞춰 주세요."
        )

    view = _blank(stock, ["brand_name"])[[
        "product_name", "brand_name", "stock_text", "glasses_left", "stock_ml",
        "bottle_ml", "avg_bottle_cost", "stock_value",
    ]].rename(columns={
        "product_name": "상품",
        "brand_name": "브랜드",
        "stock_text": "재고",
        "glasses_left": "남은 잔 수",
        "stock_ml": "총 잔량(ml)",
        "bottle_ml": "병 용량(ml)",
        "avg_bottle_cost": "평균 병 매입가",
        "stock_value": "재고 금액",
    })
    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "남은 잔 수": st.column_config.NumberColumn(format="%d잔", help="총 잔량 ÷ 잔 용량"),
            "총 잔량(ml)": st.column_config.NumberColumn(format="%,d"),
            "병 용량(ml)": st.column_config.NumberColumn(format="%,d"),
            "평균 병 매입가": st.column_config.NumberColumn(format="₩ %,d"),
            "재고 금액": st.column_config.NumberColumn(format="₩ %,d"),
        },
    )
    st.caption(
        "재고는 용량(ml)으로 계산합니다. '미개봉 N병 + 개봉 Xml'은 총 잔량을 병 용량으로 나눈 값이며, "
        "개봉한 병은 한 번에 한 병이라고 봅니다."
    )

    with st.expander("위스키 DB 다운로드"):
        path = wdb.get_db_path()
        if os.path.exists(path):
            with open(path, "rb") as f:
                st.download_button(
                    "현재 위스키 DB 다운로드",
                    data=f.read(),
                    file_name="whisky.db",
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="whisky_db_download",
                )
            st.caption("데일리시가(cigar.db)와 분리된 위스키 전용 파일(whisky.db)입니다.")


# =========================================================
# 상품
# =========================================================
def _product_form(key: str, product: dict | None = None):
    """상품 입력 폼. 저장 버튼을 누르면 입력값 dict, 아니면 None."""
    product = product or {}
    with st.form(key, clear_on_submit=product == {}):
        c1, c2, c3 = st.columns(3)
        name = c1.text_input("상품명 *", value=product.get("product_name") or "", key=f"{key}_name")
        brand = c2.text_input("브랜드", value=product.get("brand_name") or "", key=f"{key}_brand")
        category = c3.text_input(
            "종류", value=product.get("category") or "", placeholder="예: 싱글몰트, 버번", key=f"{key}_category",
        )

        c4, c5, c6, c7 = st.columns(4)
        bottle_ml = c4.number_input(
            "병 용량(ml) *", min_value=1.0, step=50.0,
            value=float(product.get("bottle_ml") or wdb.DEFAULT_BOTTLE_ML), key=f"{key}_bottle_ml",
        )
        glass_ml = c5.number_input(
            "잔 용량(ml) *", min_value=1.0, step=5.0,
            value=float(product.get("glass_ml") or wdb.DEFAULT_GLASS_ML), key=f"{key}_glass_ml",
        )
        bottle_price = c6.number_input(
            "병 판매가", min_value=0.0, step=1000.0, value=float(product.get("bottle_price") or 0),
            key=f"{key}_bottle_price",
        )
        glass_price = c7.number_input(
            "잔 판매가", min_value=0.0, step=500.0, value=float(product.get("glass_price") or 0),
            key=f"{key}_glass_price",
        )
        notes = st.text_input("비고", value=product.get("notes") or "", key=f"{key}_notes")
        is_active = True
        if product:
            is_active = st.checkbox("판매 중", value=bool(product.get("is_active", 1)), key=f"{key}_active")

        if st.form_submit_button("저장", type="primary"):
            return dict(
                product_name=name, brand_name=brand, category=category, bottle_ml=bottle_ml,
                glass_ml=glass_ml, bottle_price=bottle_price, glass_price=glass_price,
                notes=notes, is_active=int(is_active),
            )
    return None


def render_product_tab() -> None:
    products = wdb.get_products()

    if products.empty:
        st.info("등록된 위스키가 없습니다. 아래에서 첫 상품을 등록해 주세요.")
    else:
        view = _blank(products, ["brand_name", "category", "notes"]).assign(
            is_active=products["is_active"].map({1: "판매 중", 0: "중지"})
        )[[
            "product_name", "brand_name", "category", "bottle_ml", "glass_ml",
            "bottle_price", "glass_price", "is_active", "notes",
        ]].rename(columns={
            "product_name": "상품명", "brand_name": "브랜드", "category": "종류",
            "bottle_ml": "병 용량(ml)", "glass_ml": "잔 용량(ml)",
            "bottle_price": "병 판매가", "glass_price": "잔 판매가",
            "is_active": "상태", "notes": "비고",
        })
        st.dataframe(
            view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "병 용량(ml)": st.column_config.NumberColumn(format="%,d"),
                "잔 용량(ml)": st.column_config.NumberColumn(format="%,d"),
                "병 판매가": st.column_config.NumberColumn(format="₩ %,d"),
                "잔 판매가": st.column_config.NumberColumn(format="₩ %,d"),
            },
        )

    with st.expander("새 상품 등록", expanded=products.empty):
        values = _product_form("whisky_product_new")
        if values:
            try:
                values.pop("is_active")
                wdb.insert_product(**values)
                _flash(f"상품을 등록했습니다: {values['product_name']}")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

    if not products.empty:
        with st.expander("상품 수정"):
            options = _product_options(products)
            selected = st.selectbox("수정할 상품", list(options.keys()), key="whisky_product_edit_select")
            product = wdb.get_product(options[selected])
            values = _product_form(f"whisky_product_edit_{product['id']}", product)
            if values:
                try:
                    wdb.update_product(int(product["id"]), **values)
                    _flash(f"상품을 수정했습니다: {values['product_name']}")
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))
            st.caption("병·잔 용량을 바꿔도 이미 등록한 매입·판매의 재고 계산은 바뀌지 않습니다.")


# =========================================================
# 매입
# =========================================================
def render_purchase_tab() -> None:
    products = wdb.get_products(active_only=True)
    if products.empty:
        st.info("먼저 '상품' 탭에서 위스키를 등록해 주세요.")
        return
    options = _product_options(products)

    st.markdown("#### 매입 등록")
    with st.form("whisky_purchase_new", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([1, 2, 1, 1])
        purchase_date = c1.date_input("매입일", value=date.today())
        product_name = c2.selectbox("상품", list(options.keys()))
        bottle_qty = c3.number_input("병 수", min_value=1, step=1, value=1)
        unit_cost = c4.number_input("병당 매입가", min_value=0.0, step=1000.0, value=0.0)
        c5, c6 = st.columns(2)
        supplier = c5.text_input("거래처")
        notes = c6.text_input("비고")
        if st.form_submit_button("매입 등록", type="primary"):
            try:
                wdb.insert_purchase(
                    str(purchase_date), options[product_name], bottle_qty, unit_cost, supplier, notes,
                )
                _flash(f"매입을 등록했습니다: {product_name} {bottle_qty}병")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

    st.markdown("#### 매입 내역")
    purchases = wdb.get_purchases()
    if purchases.empty:
        st.caption("등록된 매입이 없습니다.")
        return
    view = _blank(purchases, ["supplier_name", "notes"])[[
        "id", "purchase_date", "product_name", "bottle_qty", "unit_cost", "total_cost",
        "supplier_name", "notes",
    ]].rename(columns={
        "id": "번호", "purchase_date": "매입일", "product_name": "상품", "bottle_qty": "병 수",
        "unit_cost": "병당 매입가", "total_cost": "매입 금액", "supplier_name": "거래처", "notes": "비고",
    })
    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "병 수": st.column_config.NumberColumn(format="%d"),
            "병당 매입가": st.column_config.NumberColumn(format="₩ %,d"),
            "매입 금액": st.column_config.NumberColumn(format="₩ %,d"),
        },
    )
    with st.expander("매입 삭제"):
        labels = {
            f"{r['id']}번 · {r['purchase_date']} · {r['product_name']} {r['bottle_qty']:g}병": int(r["id"])
            for _, r in purchases.iterrows()
        }
        selected = st.selectbox("삭제할 매입", list(labels.keys()), key="whisky_purchase_delete_select")
        if st.button("선택한 매입 삭제", key="whisky_purchase_delete"):
            wdb.delete_purchase(labels[selected])
            _flash(f"매입을 삭제했습니다: {selected}")
            st.rerun()


# =========================================================
# 판매 / 출고
# =========================================================
def render_stock_out_tab() -> None:
    products = wdb.get_products(active_only=True)
    if products.empty:
        st.info("먼저 '상품' 탭에서 위스키를 등록해 주세요.")
        return
    options = _product_options(products)
    type_labels = {
        wdb.OUT_TYPE_LABELS[t]: t for t in (wdb.OUT_GLASS_SALE, wdb.OUT_BOTTLE_SALE, wdb.OUT_ETC)
    }

    st.markdown("#### 판매·출고 등록")
    # 등록할 때마다 번호를 올려 수량·금액·비고 입력칸을 새로 만든다 (같은 내용을 실수로 두 번 등록하지 않도록).
    nonce = st.session_state.get("whisky_out_nonce", 0)
    c1, c2, c3, c4 = st.columns([1, 2, 1, 1])
    out_date = c1.date_input("날짜", value=date.today(), key="whisky_out_date")
    product_name = c2.selectbox("상품", list(options.keys()), key="whisky_out_product")
    type_label = c3.selectbox("유형", list(type_labels.keys()), key="whisky_out_type")
    out_type = type_labels[type_label]
    product = wdb.get_product(options[product_name])

    unit = None
    if out_type == wdb.OUT_ETC:
        unit = "bottle" if c4.selectbox("단위", ["잔", "병"], key="whisky_out_unit") == "병" else "glass"
    by_bottle = out_type == wdb.OUT_BOTTLE_SALE or unit == "bottle"
    unit_text = "병" if by_bottle else "잔"

    c5, c6, c7 = st.columns([1, 1, 2])
    qty = c5.number_input(f"수량({unit_text})", min_value=1, step=1, value=1, key=f"whisky_out_qty_{nonce}")
    is_sale = out_type != wdb.OUT_ETC
    list_price = float(product["bottle_price"] if by_bottle else product["glass_price"])
    amount = None
    if is_sale:
        amount = c6.number_input(
            "판매금액",
            min_value=0.0, step=1000.0, value=None,
            placeholder=f"비우면 {fmt_krw(list_price * qty)}",
            help="비워 두면 상품의 판매가 × 수량으로 저장합니다.",
            key=f"whisky_out_amount_{nonce}",
        )
    notes = c7.text_input(
        "비고", key=f"whisky_out_notes_{nonce}",
        placeholder="예: 서비스, 시음, 파손" if not is_sale else "",
    )

    per_ml = float(product["bottle_ml"] if by_bottle else product["glass_ml"])
    st.caption(
        f"재고에서 {per_ml * qty:,.0f}ml 차감 "
        f"(현재 재고: {wdb.format_stock(wdb.get_stock_ml(int(product['id'])), product['bottle_ml'])})"
    )
    if st.button("등록", type="primary", key="whisky_out_submit"):
        try:
            final_amount = 0.0 if not is_sale else (list_price * qty if amount is None else amount)
            wdb.insert_stock_out(
                str(out_date), int(product["id"]), out_type, qty, final_amount, notes, unit=unit,
            )
            _flash(f"등록했습니다: {product_name} {type_label} {qty}{unit_text}")
            st.session_state["whisky_out_nonce"] = nonce + 1
            st.rerun()
        except ValueError as e:
            st.error(str(e))

    st.markdown("#### 판매·출고 내역")
    outs = wdb.get_stock_outs()
    if outs.empty:
        st.caption("등록된 판매·출고가 없습니다.")
        return
    view = _blank(outs, ["notes"])[[
        "id", "out_date", "product_name", "out_type_label", "qty", "total_ml", "amount", "notes",
    ]].rename(columns={
        "id": "번호", "out_date": "날짜", "product_name": "상품", "out_type_label": "유형",
        "qty": "수량", "total_ml": "차감(ml)", "amount": "판매금액", "notes": "비고",
    })
    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "수량": st.column_config.NumberColumn(format="%d"),
            "차감(ml)": st.column_config.NumberColumn(format="%,d"),
            "판매금액": st.column_config.NumberColumn(format="₩ %,d"),
        },
    )
    with st.expander("판매·출고 삭제"):
        labels = {
            f"{r['id']}번 · {r['out_date']} · {r['product_name']} · {r['out_type_label']} "
            f"({r['total_ml']:,.0f}ml)": int(r["id"])
            for _, r in outs.iterrows()
        }
        selected = st.selectbox("삭제할 내역", list(labels.keys()), key="whisky_out_delete_select")
        if st.button("선택한 내역 삭제", key="whisky_out_delete"):
            wdb.delete_stock_out(labels[selected])
            _flash(f"내역을 삭제했습니다: {selected}")
            st.rerun()


# =========================================================
# 재고 실사
# =========================================================
def render_stocktake_tab() -> None:
    stock = wdb.get_stock(active_only=True)
    if stock.empty:
        st.info("먼저 '상품' 탭에서 위스키를 등록해 주세요.")
        return

    st.caption(
        "잔으로 팔면 따르는 양이 조금씩 달라 재고가 서서히 어긋납니다. "
        "실제로 센 수량을 넣으면 그 차이만큼 재고를 맞춥니다."
    )
    names = stock["product_name"].tolist()
    selected = st.selectbox("상품", names, key="whisky_take_product")
    row = stock[stock["product_name"] == selected].iloc[0]
    bottle_ml = float(row["bottle_ml"])
    product_id = int(row["product_id"])

    st.markdown(f"**시스템 재고:** {row['stock_text']} (총 {row['stock_ml']:,.0f}ml)")

    # 입력칸 기본값은 현재 시스템 재고. 재고가 바뀌면(매입·판매·실사 후) 키가 달라져 새 기본값으로 다시 채워진다.
    # (키가 그대로면 Streamlit 이 예전 입력값을 유지해, 재고 0 이던 때의 0 이 남는다)
    state_key = f"{product_id}_{row['stock_ml']:.0f}"
    c1, c2, c3 = st.columns(3)
    take_date = c1.date_input("실사일", value=date.today(), key="whisky_take_date")
    full_bottles = c2.number_input(
        "미개봉 병 수", min_value=0, step=1, value=int(row["full_bottles"]),
        key=f"whisky_take_full_{state_key}",
    )
    open_ml = c3.number_input(
        "개봉 병 잔량(ml)", min_value=0.0, max_value=max(bottle_ml - 1, 0.0), step=10.0,
        value=float(min(max(row["open_ml"], 0), max(bottle_ml - 1, 0))),
        key=f"whisky_take_open_{state_key}",
    )
    notes = st.text_input("비고", key="whisky_take_notes")

    actual_ml = full_bottles * bottle_ml + open_ml
    diff = round(float(row["stock_ml"]) - actual_ml, 2)
    if diff == 0:
        st.info("시스템 재고와 같습니다. 조정할 내용이 없습니다.")
    elif diff > 0:
        st.warning(f"실물이 {diff:,.0f}ml 적습니다 (약 {diff / float(row['glass_ml']):.1f}잔). 그만큼 차감합니다.")
    else:
        st.warning(f"실물이 {abs(diff):,.0f}ml 많습니다. 그만큼 재고를 늘립니다.")

    if st.button("실사 반영", type="primary", disabled=diff == 0, key="whisky_take_submit"):
        try:
            wdb.apply_stocktake(str(take_date), product_id, full_bottles, open_ml, notes)
            _flash(f"실사를 반영했습니다: {selected} → {wdb.format_stock(actual_ml, bottle_ml)}")
            st.rerun()
        except ValueError as e:
            st.error(str(e))


def render() -> None:
    st.caption("위스키는 개인사업자 매출이라 데일리시가(법인) 장부와 분리해 whisky.db 에 따로 저장합니다.")
    _show_flash()
    tab_stock, tab_product, tab_purchase, tab_out, tab_take = st.tabs(
        ["재고 현황", "상품", "매입", "판매·출고", "재고 실사"]
    )
    with tab_stock:
        render_stock_tab()
    with tab_product:
        render_product_tab()
    with tab_purchase:
        render_purchase_tab()
    with tab_out:
        render_stock_out_tab()
    with tab_take:
        render_stocktake_tab()
