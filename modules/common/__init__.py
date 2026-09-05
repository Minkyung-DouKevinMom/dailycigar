"""
modules.common — 화면 모듈들이 공통으로 쓰는 유틸리티 모음.

- dbutil : DB 경로/커넥션, 테이블·뷰 존재 확인, 컬럼 조회, 컬럼 선택
- fmt    : 금액/숫자 포맷, 안전한 형변환, 상품코드 정규화
- dates  : 월 범위 등 날짜 헬퍼

⚠️ 여기 있는 함수들이 정본(canonical)입니다. 각 화면 모듈에 같은 함수를 다시 만들지 말고
   `from modules.common.dbutil import get_conn` 처럼 가져다 쓰세요.
"""
