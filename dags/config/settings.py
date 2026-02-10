from __future__ import annotations

import os


DATAFORM_PROJECT_ID = "kap-chat"
DATAFORM_LOCATION = "asia-northeast3"
DATAFORM_REPOSITORY_ID = "koin-repository"
DATAFORM_WORKFLOW_CONFIG = "daily_stg_ga4_production"

DEFAULT_TIMEZONE = "Asia/Seoul"
DEFAULT_OWNER = "airflow"
GCP_CONN_ID = "google_cloud_default"

# Tableau (REST API)
TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL", "")
TABLEAU_SITE_CONTENT_URL = os.getenv("TABLEAU_SITE_CONTENT_URL", "")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME", "")
TABLEAU_PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET", "")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.27")
TABLEAU_REFRESH_TARGET = os.getenv("TABLEAU_REFRESH_TARGET", "workbook")
TABLEAU_WORKBOOK_ID = os.getenv("TABLEAU_WORKBOOK_ID", "")
TABLEAU_DATASOURCE_ID = os.getenv("TABLEAU_DATASOURCE_ID", "")

# Cost alert (Billing Export + Slack)
BILLING_EXPORT_PROJECT_ID = os.getenv("BILLING_EXPORT_PROJECT_ID", DATAFORM_PROJECT_ID)
BILLING_EXPORT_DATASET = os.getenv("BILLING_EXPORT_DATASET", "billing_export")
BILLING_EXPORT_TABLE_PREFIX = os.getenv(
    "BILLING_EXPORT_TABLE_PREFIX", "gcp_billing_export_resource_v1_"
)
BILLING_TIMEZONE = os.getenv("BILLING_TIMEZONE", DEFAULT_TIMEZONE)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow-cost-alert")
