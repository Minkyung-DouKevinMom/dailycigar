"""
지출 엑셀(은행 거래내역) 업로드 + 자동분류 규칙.

흐름
  1) 은행 거래내역 엑셀 업로드 → 출금액 > 0 인 행만 추출 (기간 필터 가능)
  2) expense_rule_mst 규칙으로 지출항목 자동 배정 (내용/적요에 키워드 포함 여부, 우선순위 순)
     - action='exclude' 규칙에 걸리면 기본적으로 저장 대상에서 제외 (예: 해외송금 = 상품 매입, 지출 아님)
  3) 이미 등록된 지출(같은 날짜·금액·내용)은 중복으로 표시하고 기본 제외
  4) 미리보기 표에서 항목/포함 여부를 직접 고친 뒤 저장 → expense_txn 에 INSERT

규칙 관리 탭에서 키워드 → 지출항목 규칙을 추가/삭제한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st

from modules.common.dbutil import table_exists
from modules.common.fmt import safe_float, safe_str

RULE_TABLE = "expense_rule_mst"
BANK_REQUIRED_COLUMNS = ["거래일시", "출금액"]
BANK_OPTIONAL_COLUMNS = ["적요", "내용", "거래점명", "입금액", "메모"]

EXCLUDE_LABEL = "(제외: 지출 아님)"
UNASSIGNED_LABEL = "(미분류 — 직접 선택)"


# ─────────────────────────── 규칙 테이블 ───────────────────────────

def ensure_expense_rule_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RULE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,                 -- 내용/적요에 포함되면 매칭 (대소문자 무시)
            action TEXT NOT NULL DEFAULT 'assign', -- assign: 항목 배정 / exclude: 지출 아님(저장 제외)
            expense_category_id INTEGER,           -- action='assign' 일 때 배정할 지출항목
            priority INTEGER NOT NULL DEFAULT 100, -- 작을수록 먼저 검사
            is_active INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def load_rules(conn, active_only: bool = True) -> pd.DataFrame:
    ensure_expense_rule_table(conn)
    sql = f"""
        SELECT r.id, r.keyword, r.action, r.expense_category_id, r.priority, r.is_active, r.notes,
               c.expense_group, c.expense_name
        FROM {RULE_TABLE} r
        LEFT JOIN expense_category_mst c ON r.expense_category_id = c.id
        {"WHERE COALESCE(r.is_active,1)=1" if active_only else ""}
        ORDER BY r.priority ASC, LENGTH(r.keyword) DESC, r.id ASC
    """
    return pd.read_sql_query(sql, conn)


def insert_rule(conn, keyword: str, action: str, expense_category_id: Optional[int], priority: int, notes: str = "") -> None:
    ensure_expense_rule_table(conn)
    conn.execute(
        f"INSERT INTO {RULE_TABLE} (keyword, action, expense_category_id, priority, is_active, notes) VALUES (?,?,?,?,1,?)",
        (keyword.strip(), action, expense_category_id if action == "assign" else None, int(priority), notes.strip() or None),
    )
    conn.commit()


def delete_rule(conn, rule_id: int) -> None:
    conn.execute(f"DELETE FROM {RULE_TABLE} WHERE id = ?", (int(rule_id),))
    conn.commit()


# ─────────────────────────── 매칭 로직 (순수 함수) ───────────────────────────

@dataclass
class RuleMatch:
    rule_id: Optional[int]
    action: str                       # assign / exclude / none
    expense_category_id: Optional[int]
    keyword: Optional[str]


def match_rule(text: str, rules: pd.DataFrame) -> RuleMatch:
    """
    text(내용+적요 결합)에 대해 우선순위 순으로 첫 매칭 규칙을 반환.
    rules 는 load_rules() 결과(정렬 완료) 또는 같은 컬럼을 가진 DataFrame.
    """
    hay = safe_str(text).upper()
    if not hay or rules is None or rules.empty:
        return RuleMatch(None, "none", None, None)
    for _, r in rules.iterrows():
        kw = safe_str(r.get("keyword")).upper()
        if kw and kw in hay:
            action = safe_str(r.get("action")) or "assign"
            cat = r.get("expense_category_id")
            cat = int(cat) if pd.notna(cat) and action == "assign" else None
            return RuleMatch(int(r["id"]) if pd.notna(r.get("id")) else None, action, cat, safe_str(r.get("keyword")))
    return RuleMatch(None, "none", None, None)


# ─────────────────────────── 엑셀 파싱 ───────────────────────────

def parse_bank_excel(file) -> pd.DataFrame:
    """
    은행 거래내역 엑셀 → 표준 컬럼:
      expense_date(YYYY-MM-DD), txn_datetime(원문), amount(출금액), vendor_name(내용), payment_method(적요), branch(거래점명)
    출금액이 0 이거나 비어 있는 행은 제외.
    """
    raw = pd.read_excel(file, sheet_name=0)
    raw.columns = [str(c).strip() for c in raw.columns]
    missing = [c for c in BANK_REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError("필수 컬럼이 없습니다: " + ", ".join(missing) + f" (현재 컬럼: {list(raw.columns)})")

    df = pd.DataFrame()
    dt = pd.to_datetime(raw["거래일시"], errors="coerce")
    df["expense_date"] = dt.dt.strftime("%Y-%m-%d")
    df["txn_datetime"] = raw["거래일시"].astype(str)
    df["amount"] = raw["출금액"].apply(safe_float)
    df["vendor_name"] = raw["내용"].apply(safe_str) if "내용" in raw.columns else ""
    df["payment_method"] = raw["적요"].apply(safe_str) if "적요" in raw.columns else ""
    df["branch"] = raw["거래점명"].apply(safe_str) if "거래점명" in raw.columns else ""

    df = df[dt.notna() & (df["amount"] > 0)].copy()
    df = df.sort_values(["expense_date", "txn_datetime"]).reset_index(drop=True)
    return df


def find_duplicates(conn, df: pd.DataFrame) -> pd.Series:
    """같은 날짜·금액·내용의 지출이 이미 있으면 True."""
    if df.empty or not table_exists(conn, "expense_txn"):
        return pd.Series([False] * len(df), index=df.index)
    existing = pd.read_sql_query(
        "SELECT expense_date, amount, COALESCE(vendor_name,'') AS vendor_name FROM expense_txn", conn
    )
    if existing.empty:
        return pd.Series([False] * len(df), index=df.index)
    key_exist = set(zip(existing["expense_date"], existing["amount"].round(0), existing["vendor_name"].astype(str).str.strip()))
    return pd.Series(
        [(d, round(a), v.strip()) in key_exist for d, a, v in zip(df["expense_date"], df["amount"], df["vendor_name"])],
        index=df.index,
    )


def classify(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    """각 행에 규칙을 적용해 matched_action / matched_category_id / matched_keyword 컬럼 추가."""
    out = df.copy()
    actions, cats, kws = [], [], []
    for _, r in out.iterrows():
        m = match_rule(f"{r['vendor_name']} {r['payment_method']}", rules)
        actions.append(m.action)
        cats.append(m.expense_category_id)
        kws.append(m.keyword or "")
    out["matched_action"] = actions
    out["matched_category_id"] = cats
    out["matched_keyword"] = kws
    return out


# ─────────────────────────── 화면: 엑셀 업로드 ───────────────────────────

def _category_options(conn) -> tuple[list[str], dict[str, Optional[int]], dict[int, str]]:
    cats = pd.read_sql_query(
        "SELECT id, expense_group, expense_name FROM expense_category_mst WHERE COALESCE(is_active,1)=1 "
        "ORDER BY COALESCE(expense_group,''), expense_name",
        conn,
    )
    labels = [UNASSIGNED_LABEL, EXCLUDE_LABEL]
    label_to_id: dict[str, Optional[int]] = {UNASSIGNED_LABEL: None, EXCLUDE_LABEL: None}
    id_to_label: dict[int, str] = {}
    for _, c in cats.iterrows():
        label = f"{c['expense_group'] or ''} | {c['expense_name']}".strip(" |")
        labels.append(label)
        label_to_id[label] = int(c["id"])
        id_to_label[int(c["id"])] = label
    return labels, label_to_id, id_to_label


def render_upload_tab(conn) -> None:
    st.markdown("#### 은행 거래내역 엑셀 업로드")
    st.caption("거래일시 / 적요 / 출금액 / 내용 / 거래점명 컬럼이 있는 은행 거래내역 엑셀을 올리면 출금 건만 지출로 가져옵니다.")

    ensure_expense_rule_table(conn)
    labels, label_to_id, id_to_label = _category_options(conn)
    if len(labels) <= 2:
        st.warning("먼저 '지출항목 관리' 탭에서 지출항목을 등록해 주세요.")
        return

    uploaded = st.file_uploader("은행 거래내역 엑셀 (.xlsx)", type=["xlsx"], key="exp_upload_file")
    if uploaded is None:
        return

    try:
        df = parse_bank_excel(uploaded)
    except Exception as e:
        st.error(f"엑셀 읽기 오류: {e}")
        return
    if df.empty:
        st.info("출금 건이 없습니다.")
        return

    # 기간 필터 (기본: 마지막 등록 지출일 다음날부터)
    last = pd.read_sql_query("SELECT MAX(expense_date) AS d FROM expense_txn", conn).iloc[0]["d"]
    default_from = pd.to_datetime(df["expense_date"].min())
    if last:
        default_from = max(default_from, pd.to_datetime(last) + pd.Timedelta(days=1))
    default_from = min(default_from, pd.to_datetime(df["expense_date"].max()))
    c1, c2 = st.columns(2)
    d_from = c1.date_input("가져올 시작일", value=default_from.date(), key="exp_upload_from")
    d_to = c2.date_input("가져올 종료일", value=pd.to_datetime(df["expense_date"].max()).date(), key="exp_upload_to")
    df = df[(df["expense_date"] >= str(d_from)) & (df["expense_date"] <= str(d_to))].copy()
    if df.empty:
        st.info("선택한 기간에 출금 건이 없습니다.")
        return

    rules = load_rules(conn)
    df = classify(df, rules)
    df["is_duplicate"] = find_duplicates(conn, df)

    def _initial_label(row) -> str:
        if row["matched_action"] == "exclude":
            return EXCLUDE_LABEL
        if row["matched_action"] == "assign" and pd.notna(row["matched_category_id"]):
            return id_to_label.get(int(row["matched_category_id"]), UNASSIGNED_LABEL)
        return UNASSIGNED_LABEL

    df["지출항목"] = df.apply(_initial_label, axis=1)
    df["저장"] = (~df["is_duplicate"]) & (df["지출항목"] != EXCLUDE_LABEL)
    df["상태"] = df.apply(
        lambda r: "중복(이미 등록됨)" if r["is_duplicate"]
        else ("규칙: " + r["matched_keyword"] if r["matched_keyword"] else "미분류"),
        axis=1,
    )

    n_rule = int((df["matched_action"] != "none").sum())
    n_dup = int(df["is_duplicate"].sum())
    n_un = int(((df["matched_action"] == "none") & ~df["is_duplicate"]).sum())
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("출금 건수", f"{len(df):,}")
    m2.metric("규칙 매칭", f"{n_rule:,}")
    m3.metric("미분류(직접 선택)", f"{n_un:,}")
    m4.metric("중복", f"{n_dup:,}")

    show = df[["저장", "expense_date", "vendor_name", "payment_method", "amount", "지출항목", "상태", "branch"]].rename(
        columns={"expense_date": "날짜", "vendor_name": "내용", "payment_method": "적요", "amount": "출금액", "branch": "거래점명"}
    )
    edited = st.data_editor(
        show,
        use_container_width=True,
        hide_index=True,
        height=min(600, 60 + 35 * len(show)),
        column_config={
            "저장": st.column_config.CheckboxColumn("저장"),
            "지출항목": st.column_config.SelectboxColumn("지출항목", options=labels, required=True),
            "출금액": st.column_config.NumberColumn("출금액(원)", format="%,.0f", disabled=True),
            "날짜": st.column_config.TextColumn("날짜", disabled=True),
            "내용": st.column_config.TextColumn("내용", disabled=True),
            "적요": st.column_config.TextColumn("적요", disabled=True),
            "상태": st.column_config.TextColumn("상태", disabled=True),
            "거래점명": st.column_config.TextColumn("거래점명", disabled=True),
        },
        key="exp_upload_editor",
    )

    to_save = edited[edited["저장"]]
    unassigned = to_save[to_save["지출항목"].isin([UNASSIGNED_LABEL, EXCLUDE_LABEL])]
    st.caption(f"저장 대상 {len(to_save):,}건 · 합계 {to_save['출금액'].sum():,.0f}원")
    if not unassigned.empty:
        st.warning(f"저장 체크된 행 중 지출항목이 정해지지 않은 행이 {len(unassigned)}건 있습니다. 항목을 선택하거나 저장 체크를 해제해 주세요.")

    if st.button("선택한 지출 저장", type="primary", disabled=(to_save.empty or not unassigned.empty), key="exp_upload_save"):
        inserted = 0
        try:
            for idx, r in to_save.iterrows():
                src = df.loc[idx]
                cat_id = label_to_id.get(r["지출항목"])
                if cat_id is None:
                    continue
                notes = (
                    f"은행거래내역 업로드({uploaded.name}) 거래일시 {src['txn_datetime']}"
                    + (f" / 거래점명 {src['branch']}" if src["branch"] else "")
                    + (f" / 자동분류: {src['matched_keyword']}" if src["matched_keyword"] else "")
                )
                conn.execute(
                    """
                    INSERT INTO expense_txn (expense_date, expense_category_id, amount, vendor_name, payment_method, notes, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (src["expense_date"], int(cat_id), float(src["amount"]), src["vendor_name"], src["payment_method"], notes),
                )
                inserted += 1
            conn.commit()
            st.success(f"지출 {inserted:,}건 저장했습니다.")
            st.rerun()
        except Exception as e:
            conn.rollback()
            st.error(f"저장 중 오류: {e}")


# ─────────────────────────── 화면: 규칙 관리 ───────────────────────────

def render_rules_tab(conn) -> None:
    st.markdown("#### 자동분류 규칙")
    st.caption("업로드한 거래의 내용·적요에 키워드가 포함되면 지정한 지출항목으로 자동 배정합니다. 우선순위 숫자가 작을수록 먼저 검사하고, 같은 우선순위면 긴 키워드가 먼저입니다.")

    ensure_expense_rule_table(conn)
    labels, label_to_id, _ = _category_options(conn)
    cat_labels = [l for l in labels if l not in (UNASSIGNED_LABEL, EXCLUDE_LABEL)]

    rules = load_rules(conn, active_only=False)
    if rules.empty:
        st.info("등록된 규칙이 없습니다.")
    else:
        view = rules.copy()
        view["지출항목"] = view.apply(
            lambda r: EXCLUDE_LABEL if r["action"] == "exclude"
            else f"{r['expense_group'] or ''} | {r['expense_name'] or ''}".strip(" |"), axis=1
        )
        view = view.rename(columns={"id": "ID", "keyword": "키워드", "priority": "우선순위", "notes": "비고"})
        st.dataframe(view[["ID", "키워드", "지출항목", "우선순위", "비고"]], use_container_width=True, hide_index=True)

    st.markdown("##### 규칙 추가")
    with st.form("exp_rule_add", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 2, 1])
        keyword = c1.text_input("키워드 (내용 또는 적요에 포함되는 문자열)")
        target = c2.selectbox("지출항목", [EXCLUDE_LABEL] + cat_labels)
        priority = c3.number_input("우선순위", min_value=1, max_value=999, value=100, step=10)
        notes = st.text_input("비고 (선택)")
        if st.form_submit_button("추가", use_container_width=True):
            if not keyword.strip():
                st.error("키워드를 입력하세요.")
            else:
                action = "exclude" if target == EXCLUDE_LABEL else "assign"
                insert_rule(conn, keyword, action, label_to_id.get(target), int(priority), notes)
                st.success("규칙을 추가했습니다.")
                st.rerun()

    if not rules.empty:
        st.markdown("##### 규칙 삭제")
        opts = {f"{int(r['id'])} | {r['keyword']}": int(r["id"]) for _, r in rules.iterrows()}
        sel = st.selectbox("삭제할 규칙", [""] + list(opts.keys()), key="exp_rule_del_sel")
        if sel and st.button("삭제", key="exp_rule_del_btn"):
            delete_rule(conn, opts[sel])
            st.success("삭제했습니다.")
            st.rerun()
