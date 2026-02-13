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

## 2026-02-13
- 요청: Airflow 운영 재개 전, Billing Export 비용 테이블 생성 선행
- 현재 계정/프로젝트 점검 완료: `marchingg42@gmail.com`, 프로젝트 `kap-chat`
- 기존 연결 결제계정 `018460-5136FC-9053A4`는 접근 권한 부족으로 조회/설정 불가 확인
  - `gcloud billing accounts describe 018460-5136FC-9053A4` 권한 오류
- 접근 가능한 열린 결제계정 확인: `0111A5-C6DB11-C0603F`
- 프로젝트 결제계정 변경 완료
  - `gcloud billing projects link kap-chat --billing-account=0111A5-C6DB11-C0603F`
- 결제계정 IAM 확인: 사용자 `marchingg42@gmail.com`에 `roles/billing.admin` 존재
- BigQuery 데이터셋 상태 재확인: `kap-chat:gcp_billing` 존재(US), 테이블 0개
- Transfer 설정 확인 결과
  - 생성/확인된 항목: `Pricing BigQuery Transfer` 1건
  - `dataSourceId`: `5e7e25d3-0000-2a63-baa9-089e0825fcf8`
  - destination dataset: `gcp_billing`
- 수동 transfer run 생성 후 상태 확인
  - run 생성됨, 상태 `PENDING` 지속
- 최종 확인 결과: 비용 집계용 상세 테이블 `gcp_billing_export_resource_v1_*` 미생성

### 현재 막힌 지점
- 활성화된 내보내기는 `Pricing`으로 보이며, 비용 알림 DAG에 필요한 `Detailed usage cost` 내보내기 반영이 확인되지 않음
- 따라서 `cost_alert_pipeline`의 실제 비용 집계 검증을 진행할 수 없음

### 재개 시 해야 할 일
1. Billing 콘솔에서 `Detailed usage cost` 내보내기가 켜져 있는지 재확인
2. 대상이 `project=kap-chat`, `dataset=gcp_billing (US)`인지 재확인
3. 반영 대기 후 `gcp_billing_export_resource_v1_*` 생성 여부 재검증
4. 테이블 생성 확인 후 `cost_alert_pipeline` 수동 실행 및 Slack 메시지(안내/실집계) 검증

### Tableau 우선 진행 (2026-02-13 추가)
- 비용 테이블 이슈 보류 후 Tableau 자동화 우선 진행으로 전환
- `dags/tasks/tableau_api.py` 확장
  - `TABLEAU_REFRESH_TARGET=flow` 지원 추가
  - Flow 실행 API(`/flows/{flow_id}/run`) 호출 추가
  - `TABLEAU_WAIT_FOR_JOB=true`일 때 Tableau job 폴링/완료 대기 기능 추가
  - job 실패/타임아웃 시 예외 처리 강화
- `dags/config/settings.py`에 Tableau 확장 환경변수 추가
  - `TABLEAU_FLOW_ID`, `TABLEAU_WAIT_FOR_JOB`, `TABLEAU_JOB_POLL_SECONDS`, `TABLEAU_JOB_TIMEOUT_SECONDS`
- `.env.example`에 신규 Tableau 환경변수 반영
- `docs/tableau_setup.md` 문서 정리 및 Flow 타깃/검증 절차 반영
- 문법 검증 완료
  - `python -m py_compile dags/tasks/tableau_api.py dags/config/settings.py dags/pipelines/dataform_tableau_pipeline.py`

### 중단 시점 상세 기록 (2026-02-13, 다음 작업자용)
- 로컬 환경에서는 `docker` 명령이 없어 직접 실행 불가 확인
  - 오류: `docker : The term 'docker' is not recognized`
- VM `airflowvm` 기동 후 원격으로 Airflow 컨테이너 실행 확인
  - webserver/scheduler/postgres 기동 상태 확인 완료
- 로컬 `.env`를 VM `/home/march/airflow/.env`로 복사 완료
- Airflow 태스크 단건 테스트 실행
  - 명령: `airflow tasks test dataform_tableau_pipeline tableau_refresh 2026-02-13`
  - 결과: 실패
  - 실패 위치: Tableau `signin` API
  - HTTP: `401 Unauthorized`
- 원인 검증(직접 REST 호출) 결과
  - 응답 코드: `401001`
  - 응답 상세: `The personal access token you provided is invalid.`
  - 결론: 현재 PAT(`TABLEAU_PAT_NAME`/`TABLEAU_PAT_SECRET`) 자체가 유효하지 않음

#### 현재 확정된 막힘
1. Tableau PAT가 만료/오입력/권한불일치 상태라 인증 불가
2. 인증이 풀려도 실제 실행 타겟 ID가 비어 있음
   - `TABLEAU_REFRESH_TARGET=workbook`인 경우 `TABLEAU_WORKBOOK_ID` 필요
   - `TABLEAU_REFRESH_TARGET=flow`인 경우 `TABLEAU_FLOW_ID` 필요

#### 재개 시 우선 순서 (커맨드 포함)
1. Tableau Cloud에서 PAT 재발급
   - 새 `PAT name` / `PAT secret` 확보
2. VM `.env` 값 갱신
   - 파일: `/home/march/airflow/.env`
   - 필수: `TABLEAU_SERVER_URL`, `TABLEAU_SITE_CONTENT_URL`, `TABLEAU_PAT_NAME`, `TABLEAU_PAT_SECRET`
   - 실행 대상에 따라 `TABLEAU_WORKBOOK_ID` 또는 `TABLEAU_FLOW_ID` 입력
3. 컨테이너 재적용
   - `cd ~/airflow && docker compose up -d`
4. 인증 단건 검증
   - `cd ~/airflow && docker compose exec -T airflow-scheduler airflow tasks test dataform_tableau_pipeline tableau_refresh 2026-02-13`
5. ID 미확정이면 REST API로 목록 조회 후 ID 확정
   - 로그인 성공 후 `workbooks`/`flows` 목록 API 호출하여 대상 ID 채움
6. 최종 검증
   - `dataform_tableau_pipeline` 수동 실행
   - `tableau_refresh` 로그에서 `refresh job id` 및(옵션) job 완료 로그 확인

#### 보안 메모
- 실제 PAT/시크릿 값은 Git 추적 파일에 저장 금지
- `.env.example`은 placeholder만 유지
