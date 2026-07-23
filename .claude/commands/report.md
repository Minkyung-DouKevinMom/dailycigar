---
description: 데일리시가 정기 리포트(브랜드별 판매현황/도매 업체별/30일 증감/소매 월별 추이) 실행
---

저장소를 최신으로 pull한 뒤 `python report.py` 를 실행해줘.

실행이 끝나면:
1. `report_output/` 폴더의 PNG 이미지 4개(1_brand_overview.png, 2_wholesale_by_partner.png, 3_month_over_month.png, 4_retail_monthly_trend.png)를 모두 채팅에 첨부해서 보여줘.
2. 콘솔에 출력된 핵심 수치(총매출/총이익/전체마진율, 도매 상위 업체, 최근 30일 vs 이전 30일 매출·이익 증감, 소매 월별 매출 최근 3개월)를 짧게 요약해서 같이 보여줘.
3. 스크립트 실행 중 에러가 난 섹션이 있으면 어떤 섹션이 실패했는지도 알려줘.

리포트 내용을 추가로 분석하거나 해석하지 말고, 위 4개 항목을 있는 그대로 보여주는 데 집중해줘.
