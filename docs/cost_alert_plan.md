# 비용 알림 파이프라인 계획서

## 목표

- GCP 사용 비용을 매일 집계하여 Slack으로 알림한다.
- 비용을 `유저/팀/워크플로우` 기준으로 구분해 보여준다.
- Airflow 하나에서 기존 파이프라인과 함께 통합 운영한다.

---

## 범위

- 대상 비용: BigQuery 및 관련 GCP 리소스 비용
- 집계 기준: 전일(UTC 기준 또는 KST 기준 중 선택)
- 데이터 소스: Cloud Billing Export → BigQuery
- 알림 채널: Slack (Webhook 또는 Bot)

---

## 핵심 설계

### 1) 식별 전략 (권장 방식)

- Airflow/개인/팀 작업은 **BigQuery Job 라벨**로 구분
- 라벨 예시
  - `cost_owner_type`: `individual` | `team` | `workflow`
  - `cost_owner`: 개인/팀 이름
  - `workflow`: 워크플로우 이름
- Dataform 비용은 정밀 분리하지 않고 전체 비용에 포함

### 2) 데이터 소스

- Cloud Billing Export를 BigQuery로 활성화
- 상세 테이블: `gcp_billing_export_resource_v1_*`
- 집계 쿼리는 전일 기준으로 실행

### 3) Airflow DAG 구조

- DAG 이름: `cost_alert_pipeline`
- 태스크 흐름:
  1. `bq_cost_query` (BigQuery 집계)
  2. `slack_notify` (결과 요약 전송)

---

## 구현 방법

### 1) 사전 준비

- Billing Export 활성화
- Billing Export 데이터셋/테이블 확인
- Composer 서비스 계정에 BigQuery 조회 권한 부여
- Slack Webhook 또는 Bot Token 준비

### 2) BigQuery 집계 쿼리

- 전일 비용 집계
- 라벨 기반 그룹핑 (`cost_owner_type`, `cost_owner`, `workflow`)
- 총합/상위 항목 정리

### 3) Airflow DAG 구현

- BigQueryOperator 또는 PythonOperator로 쿼리 실행
- 결과 포맷을 Slack 메시지로 구성
- 실패 시 재시도 및 에러 알림

---

## 작업 단계

1. Billing Export 활성화 및 데이터셋 확인
2. 라벨 정책 확정 및 BigQuery 작업 라벨 적용
3. 비용 집계 SQL 초안 작성 및 검증
4. `cost_alert_pipeline` DAG 스켈레톤 추가
5. Slack 전송 방식 확정(Webhook/Bot) 및 연동
6. 알림 포맷 확정
7. 테스트 실행 및 운영 반영

---

## 산출물

- `docs/cost_alert_plan.md`
- `dags/pipelines/cost_alert_pipeline.py`
- `dags/tasks/slack_notify.py` (공통 모듈)
- `dags/config/settings.py` (알림 설정/상수)
