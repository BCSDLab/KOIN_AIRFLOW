# =====================================================================
# 백필 DAG (backfill_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: Dataform incremental 테이블을 전체 재빌드합니다.
# * 스케줄: 없음 (수동 트리거 전용)
# * 사용 시나리오:
#   1. GA4에 신규 서비스 로깅이 추가됨
#   2. Dataform SQLX를 수정하여 신규 로깅을 반영
#   3. 이 DAG를 수동 실행하여 과거 데이터를 재처리
#
# * 파이프라인 흐름:
#   dataform_run → dataform_wait → assertion_check → tableau_refresh → notify_success
#
# * 주의:
#   - fullyRefreshIncrementalTablesEnabled=True로 실행되어
#     시작일(2024-07-01)부터 모든 데이터를 재처리합니다.
#   - 전체 재빌드는 BigQuery 비용이 높으므로 필요한 경우에만 실행하세요.
# =====================================================================
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.assertion_check import check_assertions
from tasks.backfill import create_backfill_invocation
from tasks.dataform_api import wait_for_workflow_invocation
from tasks.slack_notify import on_failure_slack, send_slack_message
from tasks.tableau_api import refresh_all_datasources

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 1,                           # 백필은 재시도 1회로 제한
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="backfill_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 4, 7, tz=local_tz),
    schedule=None,  # 수동 트리거 전용
    catchup=False,
    tags=["koin", "backfill", "dataform", "manual"],
) as dag:

    # Step 1: Dataform fullRefresh 실행 트리거
    # - 모든 incremental 테이블을 처음부터 재빌드합니다.
    # - task_id를 "dataform_run"으로 유지하여
    #   wait_for_workflow_invocation의 XCom 참조와 호환합니다.
    dataform_run = PythonOperator(
        task_id="dataform_run",
        python_callable=create_backfill_invocation,
    )

    # Step 2: Dataform 실행 완료 대기
    # - fullRefresh는 전체 재빌드이므로 최대 6시간 대기합니다.
    dataform_wait = PythonSensor(
        task_id="dataform_wait",
        python_callable=wait_for_workflow_invocation,
        poke_interval=120,        # 2분마다 상태 확인
        timeout=60 * 60 * 6,      # 최대 6시간 대기
        mode="reschedule",
    )

    # Step 3: 데이터 품질 검증
    assertion_check = PythonOperator(
        task_id="assertion_check",
        python_callable=check_assertions,
    )

    # Step 4: Tableau 데이터소스 새로고침
    tableau_refresh = PythonOperator(
        task_id="tableau_refresh",
        python_callable=refresh_all_datasources,
    )

    # Step 5: 성공 알림
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_slack_message,
        op_kwargs={
            "message": (
                ":arrows_counterclockwise: *[KOIN] Backfill 완료*\n"
                "- Dataform: fullRefresh SUCCEEDED\n"
                "- Assertions: PASSED\n"
                "- Tableau: Refreshed"
            )
        },
    )

    dataform_run >> dataform_wait >> assertion_check >> tableau_refresh >> notify_success
