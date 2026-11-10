# DAILY_CIGAR 작업 규칙 (Claude 세션용)

이 파일은 이 저장소에서 작업하는 Claude 세션이 자동으로 읽는 규칙입니다.

## 반드시 지킬 것

1. **코드/DB 를 수정한 뒤 커밋하기 전에 테스트를 실행한다.**
   ```bash
   cd <repo 루트> && python -m pytest tests -q
   ```
   특히 아래를 건드렸다면 필수: `db.py`, `modules/common/**`, `modules/dashboard/**`, `modules/finance/**`,
   `modules/analytics/**`, `DAILY_CIGAR.py`, `report.py`, 소매/도매 판매 데이터(`cigar.db`).
   실패하면 원인을 고치기 전에는 푸시하지 않는다. (GitHub Actions 도 푸시마다 같은 테스트를 돌린다.)

2. **매출/이익 계산은 정본만 사용한다.**
   - 소매/도매 판매 데이터 조회: `modules/common/sales_query.py` 의 `load_retail_sales` / `load_wholesale_sales`
   - 시가 외 상품 원가/이익 보정: `db.apply_non_cigar_margin_logic`
   - 화면 모듈에 SQL 로 매출/이익을 직접 계산하는 코드를 새로 만들지 않는다. 컬럼명만 rename 해서 쓴다.
   - 기준: 소매 매출 = 부가세 제외 공급가액, 시가 외 원가 = 매입가(기프트패키지는 구성품 원가)×수량,
     도매 이익 = 수량×(공급가−원가). 이 기준을 바꿀 때는 `tests/test_sales_invariants.py` 도 함께 갱신.

3. **매출/이익을 보여주는 화면을 새로 추가하면** `tests/test_cross_screen_consistency.py` 에
   그 화면 로더를 정본과 비교하는 테스트 함수를 하나 추가한다 (기존 함수 복사, 5~10줄).

4. **공통 헬퍼는 `modules/common/` 에서 가져다 쓴다** (`dbutil.get_conn/table_exists/...`, `fmt.fmt_krw/safe_*`,
   `dates.month_range`). 화면 파일에 같은 함수를 다시 정의하지 않는다.

## DB 변경 작업 절차 (기존 관행)

- 변경 전 `git fetch origin` 으로 원격과 분기 여부 확인.
- 먼저 `cigar.db` 사본(`/tmp/test_cigar.db` 등)에서 스크립트를 실행해 결과를 확인한 뒤 실제 DB 에 적용.
- 앱의 기존 함수(`wm.insert_wholesale_sale`, `ru.clean_item_df` 등)를 재사용하고 로직을 재구현하지 않는다.
- `db.py` 의 `DB_PATH` 는 상대경로 `cigar.db` 이므로 스크립트는 저장소 루트에서 실행한다.
- 커밋/푸시 시 토큰이 출력에 노출되지 않도록 한다.

## 위스키(개인사업자)는 장부를 분리한다

- 위스키는 데일리시가(법인)와 사업자가 다르다. 위스키 데이터는 `cigar.db` 에 넣지 않고 **`whisky.db`** 에만 저장한다
  (환경변수 `WHISKY_DB_PATH` 로 경로 변경 가능 — 스크립트/테스트는 사본을 가리키게 해서 먼저 확인).
- 위스키 데이터 접근은 `modules/whisky/whisky_db.py`(정본)의 함수만 사용한다. 화면(`modules/whisky/whisky_view.py`,
  `modules/whisky/whisky_dashboard.py`, `pages/10~15_위스키*.py`)이나 스크립트에 SQL 을 새로 쓰지 않는다.
- 위스키 매출/이익도 정본만 사용: `whisky_db.load_sales` / `summarize_sales` / `summarize_by_product` / `summarize_by_month`.
  기준: 매출 = 판매금액(결제금액) ÷ 1.1, 원가 = 차감 ml × ml당 평균 매입가(매입가는 부가세 제외), 마진 = 매출 − 원가.
  기타 출고·실사 조정은 마진에 넣지 않고 손실 원가로 따로 집계. 기준을 바꾸면 `tests/test_whisky_sales.py` 도 갱신.
- 재고는 용량(ml) 기준: 매입 = 병 수 × 병 용량, 병 판매 = 병 용량, 잔 판매 = 잔 용량(기본 30ml) 차감.
  실사는 `whisky_db.apply_stocktake` 로 차이만큼 조정한다. 이 기준을 바꾸면 `tests/test_whisky_stock.py` 도 갱신.
- 시가 쪽 정본(`sales_query`, `db.apply_non_cigar_margin_logic`)과 대시보드/재무 화면에 위스키 매출을 섞지 않는다.
