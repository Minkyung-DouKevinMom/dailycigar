---
description: cigar.db 기반 시가 브랜드 매출/이익/판매수량 파이차트 월간 PPT 생성
---

다음 순서로 월간 브랜드 분석 PPT를 생성한다.

1. `python3 compute_pie_data.py "C:\DAILYCIGAR_DB\cigar.db" pie_data.json` 실행
   - cigar.db에서 시가 상품(product_mst 등록 코드)만 필터링
   - 소매+도매 직접판매 + 선물세트 수량(평균단가·단위이익 기반 추정 매출/이익) 반영
   - TAB(HC) 매출/이익 30% 할인 적용 (스크립트 상단 DISCOUNT_CODE_RATES)
   - 결과를 pie_data.json으로 저장

2. `node build_pie_report.js pie_data.json "C:\Users\lovec\Documents\데일리시가\4.매장운영\브랜드분석리포트\daily_cigar_brand_report_<오늘날짜YYYYMMDD>.pptx"` 실행
   - 표지 1장(집계기간·핵심 인사이트 한줄평 자동 계산) + 매출/이익/판매수량 파이차트 3장 생성
   - 파일명에 오늘 날짜를 YYYYMMDD 형식으로 포함
   - 출력 파일은 `C:\Users\lovec\Documents\데일리시가\4.매장운영\브랜드분석리포트` 폴더에 저장

3. 두 명령 모두 종료 코드 0으로 성공했는지 확인하고, 에러가 있으면 원인(파일 경로, 누락된 테이블/뷰 등)을 설명한다.

4. 생성된 pptx 파일 경로를 알려주고, 요청 시 열어서 표지의 한줄평 수치(매출 1위, 이익 1위, 판매수량 1위, 상위 5개 매출 비중, 전체 마진율)를 간단히 요약해 보고한다.

스크립트(`compute_pie_data.py`, `build_pie_report.js`)는 이 커맨드 파일과 같은 프로젝트 루트에 있다고 가정한다. 위치가 다르면 실행 전 먼저 찾는다.
