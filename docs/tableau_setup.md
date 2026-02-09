# Tableau Refresh Setup (Composer)

이 문서는 Cloud Composer에서 Tableau Extract Refresh를 호출하기 위한 환경변수/시크릿 설정 가이드입니다.

## 사전 준비

1. Tableau Cloud에서 Personal Access Token(PAT) 생성
1. 리프레시 대상의 Workbook 또는 Data Source ID 확인

## 필요한 환경변수

- `TABLEAU_SERVER_URL` 예: `https://prod-apn-a.online.tableau.com`
- `TABLEAU_SITE_CONTENT_URL` 예: `koin` (사이트 URL의 뒤쪽 경로)
- `TABLEAU_PAT_NAME` PAT 이름
- `TABLEAU_PAT_SECRET` PAT 시크릿
- `TABLEAU_REFRESH_TARGET` `workbook` 또는 `datasource`
- `TABLEAU_WORKBOOK_ID` (target이 workbook일 때)
- `TABLEAU_DATASOURCE_ID` (target이 datasource일 때)
- `TABLEAU_API_VERSION` 선택 사항 (기본 `3.27`)

## Secret Manager에 저장 (권장)

1. 각 값을 Secret으로 생성
1. Composer 환경변수에 Secret을 매핑

### 예시 (gcloud)

```bash
gcloud secrets create tableau-pat-name --data-file=-
gcloud secrets create tableau-pat-secret --data-file=-
gcloud secrets create tableau-server-url --data-file=-
gcloud secrets create tableau-site-content-url --data-file=-
gcloud secrets create tableau-workbook-id --data-file=-
```

## Composer 환경변수 등록

Composer UI 또는 CLI에서 아래 환경변수를 추가합니다.

```text
TABLEAU_SERVER_URL
TABLEAU_SITE_CONTENT_URL
TABLEAU_PAT_NAME
TABLEAU_PAT_SECRET
TABLEAU_REFRESH_TARGET
TABLEAU_WORKBOOK_ID
TABLEAU_DATASOURCE_ID
TABLEAU_API_VERSION
```

## 점검

1. Airflow UI에서 `dataform_tableau_pipeline` 수동 실행
1. `tableau_refresh` 태스크 로그에서 `refresh job id` 확인
