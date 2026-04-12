# =====================================================================
# Tableau Flow 실행 DAG (tableau_flow_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: Tableau Cloud의 Flow(흐름)를 트리거합니다.
# * 스케줄: 없음 (수동 트리거 전용)
# * 인증: PAT(Personal Access Token) 방식
# * 파이프라인 흐름:
#
#   trigger_flows → notify_success
#
# * 설정: TABLEAU_FLOW_IDS 환경변수에 Flow LUID를 콤마로 구분하여 등록
#   예: TABLEAU_FLOW_IDS=flow-id-1,flow-id-2
# * API: POST /api/{version}/sites/{site_id}/flows/{flow_id}/run
# =====================================================================
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.slack_notify import on_failure_slack, send_slack_message
from tasks.tableau_api import run_all_flows

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="tableau_flow_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 7, tz=local_tz),
    schedule=None,  # 수동 트리거 전용
    catchup=False,
    tags=["koin", "tableau", "flow", "manual"],
) as dag:

    # Step 1: 등록된 모든 Tableau Flow 실행
    trigger_flows = PythonOperator(
        task_id="trigger_flows",
        python_callable=run_all_flows,
    )

    # Step 2: 성공 알림
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_slack_message,
        op_kwargs={
            "message": (
                ":gear: *[KOIN] Tableau Flow 실행 완료*\n"
                "- 등록된 모든 Flow가 정상 실행되었습니다."
            )
        },
    )

    trigger_flows >> notify_success
