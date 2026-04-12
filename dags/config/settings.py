# =====================================================================
# 중앙 설정 모듈 (settings.py)
# ---------------------------------------------------------------------
# * 목적: 모든 DAG와 태스크에서 사용하는 설정값을 한 곳에서 관리합니다.
# * 방식: 환경변수(.env)에서 읽되, 없으면 기본값(default)을 사용합니다.
# * 변경: 설정 변경 시 .env 파일만 수정하면 됩니다. 코드 수정 불필요.
# =====================================================================
from __future__ import annotations

import os

# =====================================================================
# Dataform 설정
# - Dataform REST API를 통해 파이프라인을 트리거할 때 사용합니다.
# - GCP 프로젝트 내 Dataform 저장소(repository)와 워크플로우 설정을 지정합니다.
# =====================================================================
DATAFORM_PROJECT_ID = os.getenv("DATAFORM_PROJECT_ID", "kap-chat")                          # GCP 프로젝트 ID
DATAFORM_LOCATION = os.getenv("DATAFORM_LOCATION", "asia-northeast3")                       # Dataform 저장소 리전 (서울)
DATAFORM_REPOSITORY_ID = os.getenv("DATAFORM_REPOSITORY_ID", "koin-repository")              # Dataform 저장소 이름
DATAFORM_WORKFLOW_CONFIG = os.getenv("DATAFORM_WORKFLOW_CONFIG", "daily_stg_ga4_production")  # 실행할 워크플로우 설정 이름

# =====================================================================
# GA4 소스 설정
# - GA4 데이터 도착 여부를 확인하는 센서(ga4_sensor.py)에서 사용합니다.
# - events_* 와일드카드 테이블에서 어제 데이터 존재 여부를 쿼리합니다.
# =====================================================================
GA4_PROJECT_ID = os.getenv("GA4_PROJECT_ID", "kap-chat")                # GA4 데이터가 있는 GCP 프로젝트
GA4_DATASET_ID = os.getenv("GA4_DATASET_ID", "analytics_432041405")     # GA4 BigQuery 데이터셋 ID

# =====================================================================
# 일반 설정
# =====================================================================
DEFAULT_TIMEZONE = "Asia/Seoul"              # KST 기준 (스케줄, 날짜 계산에 사용)
DEFAULT_OWNER = "airflow"                    # DAG 소유자 (Airflow UI 표시용)
GCP_CONN_ID = "google_cloud_default"         # Airflow GCP Connection ID (BigQuery Hook에서 사용)

# =====================================================================
# Tableau 설정
# - Tableau Cloud REST API를 통해 데이터소스 새로고침을 트리거합니다.
# - PAT(Personal Access Token)으로 인증합니다 (OTP 불필요).
# =====================================================================
TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL", "")                 # Tableau Cloud 서버 URL
TABLEAU_SITE_CONTENT_URL = os.getenv("TABLEAU_SITE_CONTENT_URL", "")     # 사이트 이름 (URL의 /#/site/여기 부분)
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME", "")                     # PAT 토큰 이름
TABLEAU_PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET", "")                 # PAT 토큰 시크릿
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.24")           # Tableau REST API 버전

# 새로고침 대상 데이터소스 ID 목록 (콤마로 구분)
# 예: "ds-id-1,ds-id-2,ds-id-3" → ["ds-id-1", "ds-id-2", "ds-id-3"]
TABLEAU_DATASOURCE_IDS = [
    ds.strip()
    for ds in os.getenv("TABLEAU_DATASOURCE_IDS", "").split(",")
    if ds.strip()
]

# 새로고침 완료까지 대기할지 여부 (true면 polling으로 완료 확인)
TABLEAU_WAIT_FOR_JOB = os.getenv("TABLEAU_WAIT_FOR_JOB", "true").lower() in {"1", "true", "yes"}
TABLEAU_JOB_POLL_SECONDS = int(os.getenv("TABLEAU_JOB_POLL_SECONDS", "20"))       # 폴링 간격 (초)
TABLEAU_JOB_TIMEOUT_SECONDS = int(os.getenv("TABLEAU_JOB_TIMEOUT_SECONDS", "3600"))  # 최대 대기 시간 (초, 기본 1시간)

# =====================================================================
# Slack 설정
# - 파이프라인 성공/실패 시 Slack 채널로 알림을 보냅니다.
# - Slack Incoming Webhook URL을 사용합니다.
# =====================================================================
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")       # Slack Webhook URL (비어있으면 알림 생략)
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow-koin")  # Slack 메시지 발신자 이름

# =====================================================================
# GCP Billing 설정
# - cost_alert_pipeline DAG에서 사용합니다.
# - Cloud Billing Export 테이블에서 전일 비용을 조회하여 Slack으로 알림합니다.
# =====================================================================
# =====================================================================
# 로깅 모니터링 설정
# - 신규 로깅 감지 및 로깅 감소 알림에 사용합니다.
# =====================================================================
LOGGING_LOOKBACK_DAYS = int(os.getenv("LOGGING_LOOKBACK_DAYS", "30"))  # 신규 로깅 판별 기준 기간 (일)
MART_DATASET = os.getenv("MART_DATASET", "mart")                       # Mart 데이터셋 이름
MART_INTEGRATED_VIEW = os.getenv("MART_INTEGRATED_VIEW", "vw_mart_integrated_logs")  # 통합 마트 뷰 이름

# =====================================================================
# Tableau Flow 설정
# - Tableau Cloud Flow 실행을 트리거할 때 사용합니다.
# =====================================================================
TABLEAU_FLOW_IDS = [
    f.strip()
    for f in os.getenv("TABLEAU_FLOW_IDS", "").split(",")
    if f.strip()
]

# =====================================================================
# GCP Billing 설정
# - cost_alert_pipeline DAG에서 사용합니다.
# - Cloud Billing Export 테이블에서 전일 비용을 조회하여 Slack으로 알림합니다.
# =====================================================================
BILLING_EXPORT_PROJECT_ID = os.getenv("BILLING_EXPORT_PROJECT_ID", DATAFORM_PROJECT_ID)                  # 빌링 데이터가 있는 프로젝트
BILLING_EXPORT_DATASET = os.getenv("BILLING_EXPORT_DATASET", "gcp_billing")                              # 빌링 Export 데이터셋
BILLING_EXPORT_TABLE_PREFIX = os.getenv("BILLING_EXPORT_TABLE_PREFIX", "gcp_billing_export_resource_v1_")  # 빌링 테이블 접두사
BILLING_TIMEZONE = os.getenv("BILLING_TIMEZONE", DEFAULT_TIMEZONE)                                        # 비용 집계 기준 타임존
