# 🛠 KOIN_AIRFLOW

KOIN 서비스의 데이터 파이프라인을 관리하기 위한 **Apache Airflow 환경**입니다.  
GA4 → BigQuery → Dataform으로 구성된 ETL 파이프라인을 **스케줄링, 모니터링, 재시도**하기 위해 Airflow를 사용합니다.

---

## 📌 Purpose

- 일 단위 데이터 적재 및 변환 작업 자동화
- 데이터 파이프라인 실행 상태 가시화
- 실패 시 재시도 및 안정적인 운영
- DAG 성공/실패 상태에 대한 Slack 알림 제공

---

## ⚙️ Architecture

- **Apache Airflow** (Docker Compose 기반)
- **Executor**: CeleryExecutor
- **Metadata DB**: PostgreSQL
- **Message Broker**: Redis
- **ETL**: Dataform 기반 BigQuery 파이프라인
- **Notification**: Slack Webhook을 통한 작업 상태 알림

---

## 🧪 Usage

- 로컬 개발 및 테스트 환경
- 서버 환경으로 확장 가능한 구조
- 운영 환경에서는 별도 서버(VM)에서 상시 실행을 전제

---

## 🔔 Notification

- DAG 실행 결과(성공/실패)를 Slack으로 전송
- 운영 중 파이프라인 이상 징후를 즉시 인지할 수 있도록 구성

