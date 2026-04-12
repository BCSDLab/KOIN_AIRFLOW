# =====================================================================
# 로깅 모니터링 DAG (logging_alert_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: GA4 이벤트 로깅의 변화를 감지하여 Slack으로 알림합니다.
# * 스케줄: 매일 KST 10:00 (Daily Pipeline 완료 이후)
# * 파이프라인 흐름:
#
#   [detect_new_logging] ──┐
#                           ├──→ [send_alert]
#   [detect_decreased_logging] ─┘
#
#   1. 신규 로깅 감지: 과거 30일간 없었던 새로운 event_label 탐지
#   2. 로깅 감소 감지: 전날 대비 50% 이상 감소한 event_label 탐지
#   3. 결과를 합쳐서 Slack 알림 전송
# =====================================================================
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.logging_monitor import (
    detect_decreased_logging,
    detect_new_event_labels,
    send_combined_logging_alert,
)
from tasks.slack_notify import on_failure_slack

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="logging_alert_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 7, tz=local_tz),
    schedule="0 10 * * *",  # 매일 KST 10:00 (Daily Pipeline 이후)
    catchup=False,
    tags=["koin", "monitoring", "logging", "slack"],
) as dag:

    # Step 1a: 신규 로깅 감지 (과거 N일간 없었던 event_label)
    detect_new = PythonOperator(
        task_id="detect_new_logging",
        python_callable=detect_new_event_labels,
    )

    # Step 1b: 로깅 감소 감지 (전날 대비 50% 이상 감소)
    detect_decreased = PythonOperator(
        task_id="detect_decreased_logging",
        python_callable=detect_decreased_logging,
    )

    # Step 2: 결과 합산 후 Slack 전송
    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_combined_logging_alert,
    )

    # 1a, 1b 병렬 실행 → 2 순차 실행
    [detect_new, detect_decreased] >> send_alert
