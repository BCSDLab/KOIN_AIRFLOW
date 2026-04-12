# =====================================================================
# 일일 지표 요약 DAG (daily_metrics_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: 전일 주요 KPI를 집계하여 Slack으로 알림합니다.
# * 스케줄: 매일 KST 10:00 (Daily Pipeline 완료 이후)
# * 파이프라인 흐름:
#
#   build_metrics → slack_notify
#
# * 지표:
#   - DAU (전날 대비 증감)
#   - 서비스별 고유 유저수 Top 5 (user_pseudo_id 기준)
#   - 서비스별 이벤트 수 Top 5
# =====================================================================
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.daily_metrics import build_daily_metrics_summary
from tasks.slack_notify import on_failure_slack, send_slack_message

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="daily_metrics_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 7, tz=local_tz),
    schedule="0 10 * * *",  # 매일 KST 10:00
    catchup=False,
    tags=["koin", "metrics", "daily", "slack"],
) as dag:

    # Step 1: 지표 집계 (DAU, 서비스별 Top 5 유저/이벤트)
    build_metrics = PythonOperator(
        task_id="build_metrics",
        python_callable=build_daily_metrics_summary,
    )

    # Step 2: Slack으로 요약 전송
    slack_notify = PythonOperator(
        task_id="slack_notify",
        python_callable=send_slack_message,
        op_kwargs={"message": "{{ ti.xcom_pull(task_ids='build_metrics') }}"},
    )

    build_metrics >> slack_notify
