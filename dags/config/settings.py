from __future__ import annotations

import os

# Dataform
DATAFORM_PROJECT_ID = os.getenv("DATAFORM_PROJECT_ID", "kap-chat")
DATAFORM_LOCATION = os.getenv("DATAFORM_LOCATION", "asia-northeast3")
DATAFORM_REPOSITORY_ID = os.getenv("DATAFORM_REPOSITORY_ID", "koin-repository")
DATAFORM_WORKFLOW_CONFIG = os.getenv("DATAFORM_WORKFLOW_CONFIG", "daily_stg_ga4_production")

# GA4 Source
GA4_PROJECT_ID = os.getenv("GA4_PROJECT_ID", "kap-chat")
GA4_DATASET_ID = os.getenv("GA4_DATASET_ID", "analytics_432041405")

# General
DEFAULT_TIMEZONE = "Asia/Seoul"
DEFAULT_OWNER = "airflow"
GCP_CONN_ID = "google_cloud_default"

# Tableau
TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL", "")
TABLEAU_SITE_CONTENT_URL = os.getenv("TABLEAU_SITE_CONTENT_URL", "")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME", "")
TABLEAU_PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET", "")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.24")
TABLEAU_DATASOURCE_IDS = [
    ds.strip()
    for ds in os.getenv("TABLEAU_DATASOURCE_IDS", "").split(",")
    if ds.strip()
]
TABLEAU_WAIT_FOR_JOB = os.getenv("TABLEAU_WAIT_FOR_JOB", "true").lower() in {"1", "true", "yes"}
TABLEAU_JOB_POLL_SECONDS = int(os.getenv("TABLEAU_JOB_POLL_SECONDS", "20"))
TABLEAU_JOB_TIMEOUT_SECONDS = int(os.getenv("TABLEAU_JOB_TIMEOUT_SECONDS", "3600"))

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow-koin")

# Billing
BILLING_EXPORT_PROJECT_ID = os.getenv("BILLING_EXPORT_PROJECT_ID", DATAFORM_PROJECT_ID)
BILLING_EXPORT_DATASET = os.getenv("BILLING_EXPORT_DATASET", "gcp_billing")
BILLING_EXPORT_TABLE_PREFIX = os.getenv("BILLING_EXPORT_TABLE_PREFIX", "gcp_billing_export_resource_v1_")
BILLING_TIMEZONE = os.getenv("BILLING_TIMEZONE", DEFAULT_TIMEZONE)
