# Tableau Refresh Setup (Self-hosted Airflow)

이 문서는 Self-hosted Airflow 환경에서 Tableau Extract Refresh를 호출하기 위한
환경변수/시크릿 설정 가이드입니다.

## 사전 준비
1. Tableau Cloud에서 Personal Access Token(PAT) 생성
2. 리프레시 대상 Workbook 또는 Data Source ID 확인

## 필요 환경변수
- `TABLEAU_SERVER_URL` 예: `https://prod-apn-a.online.tableau.com`
- `TABLEAU_SITE_CONTENT_URL` 예: `koin` (사이트 URL 경로)
- `TABLEAU_PAT_NAME` PAT 이름
- `TABLEAU_PAT_SECRET` PAT 시크릿
- `TABLEAU_REFRESH_TARGET` `workbook` 또는 `datasource`
- `TABLEAU_WORKBOOK_ID` (target이 workbook일 때)
- `TABLEAU_DATASOURCE_ID` (target이 datasource일 때)
- `TABLEAU_API_VERSION` 선택 사항 (기본 `3.27`)

## 설정 방법
1. VM의 환경변수로 주입하거나 `.env`로 관리
2. Docker Compose에서 `env_file` 또는 `environment`로 연결
3. 민감 정보는 Secret Manager를 사용하거나 VM 내 보안 파일로 관리

## 확인 방법
1. Airflow UI에서 `dataform_tableau_pipeline` 수동 실행
2. `tableau_refresh` 태스크 로그에서 `refresh job id` 확인
