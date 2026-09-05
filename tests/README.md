# 테스트 스위트

계산 로직(매출/이익/원가)이 화면 간에 어긋나는 일을 자동으로 잡기 위한 회귀 테스트입니다.
실제 `cigar.db` 를 임시 폴더에 **복사해서** 검사하므로 원본 DB 는 변경되지 않습니다.

## 실행

```bash
cd <repo 루트>
python -m pytest tests -q
```

## 구성

| 파일 | 검증 내용 |
|---|---|
| `test_sales_invariants.py` | 정본 로더(`modules/common/sales_query.py`)의 불변식: 소매 매출 = 부가세 제외 공급가액, 이익 = 매출 − 원가, 시가 외 원가 = 매입가(기프트는 구성품 원가)×수량, 도매 이익 = 수량×(공급가−원가), 시가 외 도매 원가 0 금지, 원가 근거 없는 신규 상품코드 감지 |
| `test_cross_screen_consistency.py` | 데이터가 있는 **모든 월**에 대해 대시보드 / 홈 / 기간비교 / 재무관리 / 상위제품(이익) 이 정본과 일치하는지, 브랜드분석·거래처분석·소매매출조회 집계가 정본과 일치하는지 |

## 실패했을 때

- `test_sales_invariants` 실패 → 계산 기준 자체가 바뀐 것. 의도한 변경이면 테스트의 기대값/설명을 함께 갱신.
- `test_cross_screen_consistency` 실패 → 특정 화면이 정본 로더를 우회해 자체 계산을 시작한 것. 해당 화면을 `sales_query` 로더로 되돌리는 것이 원칙.
- `test_no_new_codes_without_cost_basis` 실패 → 마스터(시가 외) 또는 수입품목에 등록되지 않은 상품코드가 판매됨. 마스터 등록 후 재실행.
  (과거 코드 3개는 `KNOWN_LEGACY_CODES_WITHOUT_COST` 에 예외로 등록됨)

## 새 화면을 만들 때

매출/이익을 보여주는 화면을 추가하면 `test_cross_screen_consistency.py` 에 그 화면의 로더를 정본과 비교하는
테스트 함수를 하나 추가해 주세요 (기존 함수 5~10줄짜리를 복사하면 됩니다).
