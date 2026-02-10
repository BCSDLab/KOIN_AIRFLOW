# KOIN_AIRFLOW

KOIN 서비스의 데이터 파이프라인을 관리하는 Apache Airflow 프로젝트입니다.  
Dataform(SQLX) 기반 BigQuery 전처리 결과를 Tableau로 제공하고, Slack으로 운영 상태를 알림합니다.

---

## 목적

- Dataform → BigQuery → Tableau 파이프라인 오케스트레이션
- 실행 상태 모니터링 및 재시도 표준화
- 실패 감지 및 Slack 알림
- 운영 자동화와 확장성 확보

---

## 아키텍처 (현재 계획)

- **Airflow**: Self-hosted (Compute Engine + Docker)
- **ETL**: Dataform (SQLX) → BigQuery
- **BI**: Tableau Cloud (Extract Refresh)
- **Notification**: Slack (Webhook 또는 Bot)
- **Secrets**: Secret Manager

---

## 배포 흐름 (Self-hosted)

1. 로컬에서 DAG/코드 개발
2. GitHub에 푸시
3. VM에서 리포지토리 pull 또는 배포 스크립트 실행
4. Docker Compose로 Airflow 재시작/적용

## 로컬/VM 실행 (Docker)

1. `.env.example` → `.env` 복사 후 값 설정
2. 초기화: `docker compose up airflow-init`
3. 실행: `docker compose up -d`
4. UI: `http://<VM_IP>:8080`

---

## 디렉터리 구조

- `dags/` 실제 DAG 엔트리
- `dags/pipelines/` 파이프라인 DAG
- `dags/tasks/` 공통 태스크/헬퍼
- `dags/config/` 설정/상수
- `plugins/` 커스텀 플러그인
- `docs/` 운영/설계 문서
- `history.md` 작업 히스토리 기록
- `docs/tableau_setup.md` Tableau 리프레시 설정 가이드
- `docs/self_hosted_airflow.md` Self-hosted Airflow 구축 가이드

---

## 운영 원칙

- 모든 파이프라인은 Airflow로 단일 관리
- 실패 즉시 알림 + 일일 요약 알림
- 신규 SaaS 연동 시 Secrets Manager 기반으로 확장
