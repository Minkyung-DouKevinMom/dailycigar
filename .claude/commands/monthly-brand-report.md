---
description: cigar.db 기반 시가 브랜드 매출/이익/판매수량 파이차트 월간 PDF 리포트 생성
---

다음 순서로 월간 브랜드 분석 PDF를 생성한다. 최종 산출물은 PPTX가 아닌 PDF 한 개만 남긴다.

1. `python3 compute_pie_data.py "C:\DAILYCIGAR_DB\cigar.db" pie_data.json` 실행
   - cigar.db에서 시가 상품(product_mst 등록 코드)만 필터링
   - 소매+도매 직접판매 + 선물세트 수량(평균단가·단위이익 기반 추정 매출/이익) 반영
   - TAB(HC) 매출/이익 30% 할인 적용 (스크립트 상단 DISCOUNT_CODE_RATES)
   - 결과를 pie_data.json으로 저장

2. `node build_pie_report.js pie_data.json "<스크래치패드경로>\daily_cigar_brand_report_<오늘날짜YYYYMMDD>.pptx"` 실행
   - PPTX는 최종 산출물이 아니므로 스크래치패드(임시) 경로에 생성
   - 표지 1장(집계기간·핵심 인사이트 한줄평 자동 계산) + 매출/이익/판매수량 파이차트 3장 생성
   - 파일명에 오늘 날짜를 YYYYMMDD 형식으로 포함

3. PowerShell + PowerPoint COM 자동화로 PPTX를 PDF로 변환하여 최종 폴더에 저장하고, 임시 PPTX는 삭제한다.
   ```powershell
   Get-Process POWERPNT -ErrorAction SilentlyContinue | Stop-Process -Force
   Start-Sleep -Seconds 1
   $src = "<스크래치패드경로>\daily_cigar_brand_report_<오늘날짜YYYYMMDD>.pptx"
   $dst = "C:\Users\lovec\Documents\데일리시가\4.매장운영\브랜드분석리포트\daily_cigar_brand_report_<오늘날짜YYYYMMDD>.pdf"
   $ppt = New-Object -ComObject PowerPoint.Application
   $pres = $ppt.Presentations.Open($src, $true, $true, $false)
   Start-Sleep -Milliseconds 500
   $pres.SaveAs($dst, 32)  # ppSaveAsPDF = 32
   Start-Sleep -Milliseconds 500
   $pres.Close()
   $ppt.Quit()
   Remove-Item $src -Force
   ```
   - 시작 전에 남아있는 POWERPNT 프로세스를 정리하고, Open/SaveAs 사이에 짧은 대기를 둬야 COMException("파일을 저장하는 중 오류가 발생했습니다")이 줄어든다.
   - SaveAs가 COMException으로 실패하면 대개 대상 `$dst` 파일이 다른 프로그램(Acrobat 등 PDF 뷰어)에서 열려 잠겨 있는 경우다. 사용자에게 해당 파일을 열어둔 뷰어를 닫아달라고 요청한 뒤 재시도한다.
   - 재시도 후에도 실패하면(예: PowerPoint 미설치) 원인을 설명하고, 필요 시 PPTX를 최종 폴더에 대신 저장한다.
   - PDF 변환 후에는 `pypdf` 등으로 표지 텍스트를 추출해 "제안" 섹션이 실제로 포함됐는지 확인한다(과거에 COM 오류 없이도 변환이 이전 캐시본으로 남는 경우가 있었음).

4. 1~3단계 모두 성공(종료 코드 0 / Test-Path $dst = True)했는지 확인하고, 에러가 있으면 원인(파일 경로, 누락된 테이블/뷰, COM 자동화 실패 등)을 설명한다.

5. 생성된 pdf 파일 경로를 알려주고, 요청 시 열어서 표지의 한줄평 수치(매출 1위, 이익 1위, 판매수량 1위, 상위 5개 매출 비중, 전체 마진율)와 인사이트 기반 자동 제안(pie_data.json의 insights.recommendations)을 간단히 요약해 보고한다.

스크립트(`compute_pie_data.py`, `build_pie_report.js`)는 이 커맨드 파일과 같은 프로젝트 루트에 있다고 가정한다. 위치가 다르면 실행 전 먼저 찾는다.
