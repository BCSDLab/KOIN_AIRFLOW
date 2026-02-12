# Airflow 구축 히스토리

## 2026-02-06
- GCP 프로젝트 `kap-chat` 설정
- API 활성화: Dataform, Secret Manager
- 서비스 계정 `airflow-prod-sa` 생성
- 권한 부여: `dataform.admin`, `bigquery.jobUser`, `bigquery.dataEditor`, `secretmanager.secretAccessor`
- README.md 전체 재작성 (Airflow 기반 설계/구조/운영 원칙 반영)
- DAG 스켈레톤 생성: dags/pipelines/dataform_tableau_pipeline.py
- 설정 파일 추가: dags/config/settings.py
- Dataform API 호출 설계 문서 추가: docs/dataform_api_design.md
- 비용 알림 파이프라인 계획서 추가: docs/cost_alert_plan.md
- Dataform API 호출/폴링 로직 추가: dags/tasks/dataform_api.py
- Dataform DAG에 PythonOperator/PythonSensor 적용
- DAG 패키지 인식용 __init__.py 추가 (dags/, dags/config, dags/tasks, dags/pipelines)
- DAG import 경로 수정 (dags.* -> config/tasks 직접 참조)
- Dataform repository/workflow config 수정: koin-repository / daily_stg_ga4_production
- Dataform API 로그 추가 (create/get URL 및 상태 출력)
- Dataform 파이프라인 정상 실행 확인 (DAG 성공)
- 문제 원인 정리: Dataform 리소스 경로 오타 수정 후 정상 동작
- 수동 실행 중이던 DAG Run(2026-02-06T11:22:25+00:00) 사용자에 의해 중단됨
- 내일 진행 예정: Tableau 리프레시 구현, Slack 알림, 스케줄 설정, 비용 알림 DAG

## 2026-02-09
- Tableau REST API 기반 리프레시 태스크 추가 (tableau_api.py)
- Tableau 설정 환경변수/디버그 플래그 추가 (settings.py, TABLEAU_DEBUG_AUTH_ONLY)
- DAG에서 tableau_refresh를 PythonOperator로 변경
- Tableau 설정 가이드 문서 추가: docs/tableau_setup.md
- 진행 중단: Tableau Cloud Flow 자동화는 내일 진행

### 내일 할 일
- Tableau Cloud에서 contentUrl, pod 확인 및 PAT 생성
- Flow ID 확인 (UI URL 또는 REST API로 조회)
- Secret Manager에 Tableau 시크릿 생성/버전 등록
- auth-only 디버깅 후 Flow Run Now API 태스크 추가 검토

## 2026-02-10
- Composer 비용 과다 이슈로 자체 호스팅 전환 결정
- Composer 환경 설정 백업: docs/composer_env_backup.json
- Composer 환경 삭제 요청 (airflow-prod)
- 비용 알림 DAG 스케줄을 22시로 변경 (cost_alert_pipeline)
- Cloud Composer 관련 문서/스크립트/체크리스트 정리
- Self-hosted Airflow 구성 파일 추가: docker-compose.yml, .env.example
- Self-hosted Airflow 가이드 문서 추가: docs/self_hosted_airflow.md
- VM `airflowvm`(asia-northeast3-a, e2-medium) 재사용 결정
- Cloud Scheduler 작업 생성: `airflowvm-start-2100`, `airflowvm-stop-2200`
- VM에 Docker/Compose 설치 및 Airflow 초기화 완료
- Airflow webserver/scheduler 컨테이너 실행
- Cloud Scheduler 작업 비활성화: `airflowvm-start-2100`, `airflowvm-stop-2200`

### 내일 할 일 (2026-02-11)
- Airflow UI 접근 방식 확정 (8080 방화벽 열기 vs IAP)
- `.env` 업데이트 (admin 비밀번호, Slack webhook, Dataform/비용/Billing Export, Tableau PAT 등)
- Airflow 재시작 후 DAG 정상 로딩 확인
- 비용 확인용으로 VM 런타임/과금 내역 체크
- 필요 시 외부 IP 제거(비용 절감)

## 2026-02-12
- 비용 알림 DAG(`cost_alert_pipeline`) 집계 쿼리 수정: labels `UNNEST` 중복으로 인한 비용 과대합산 이슈 제거
- 비용 알림 메시지 포맷 정리 및 인코딩 깨짐 문자열 교체
- Billing Export 미연결/미생성 테이블 상황에서 실패 대신 안내 메시지 전송하도록 예외 처리 추가
- 시크릿 안전조치: `.gitignore`에 서비스 계정 키 패턴(`*sa-key*.json`) 추가
- GCP Billing 상태 점검: 프로젝트 `kap-chat` billing account 연결 상태 확인
- BigQuery 데이터셋 `kap-chat:gcp_billing` 생성 (US)
- API 활성화: `cloudbilling.googleapis.com`
- Billing Export 테이블 존재 재검증: `gcp_billing_export_resource_v1_*` 미생성(0건) 확인
- VM(`airflowvm`)에서 `cost_alert_pipeline` unpause 및 수동 트리거 실행
- 최신 수동 실행(`manual__2026-02-12T10:47:08+00:00`) 기준 태스크 성공 확인
  - `bq_cost_query`: success
  - `slack_notify`: success
- Slack 전송 동작 확인: Billing Export 테이블 미생성 안내 메시지 전송됨

### 내일 시작할 때 (2026-02-13)
- 현재 비용 절감을 위해 VM `airflowvm` 상태는 `TERMINATED`
- Cloud Scheduler 작업 상태
  - `airflowvm-start-2100`: `PAUSED`
  - `airflowvm-stop-2200`: `PAUSED`
- 먼저 확인할 것
  - Billing Export 상세 데이터가 `kap-chat:gcp_billing`로 연결/저장되어 있는지 콘솔에서 확인
  - `gcp_billing_export_resource_v1_*` 테이블 생성 여부 확인
- 재개 순서
  1. VM 시작: `gcloud compute instances start airflowvm --zone asia-northeast3-a`
  2. VM 접속 후 Airflow 컨테이너 상태 확인: `cd ~/airflow && docker compose ps`
  3. `cost_alert_pipeline` 수동 실행 후 Slack 수신 확인
  4. 비용 테이블 생성 전이면 안내 메시지, 생성 후면 실제 비용 집계 메시지 전송 확인
