from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.dataform_api import create_workflow_invocation, wait_for_workflow_invocation
from tasks.ga4_sensor import check_ga4_freshness
from tasks.assertion_check import check_assertions
from tasks.tableau_api import refresh_all_datasources
from tasks.slack_notify import on_failure_slack, send_slack_message

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="koin_daily_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 22, tz=local_tz),
    schedule="0 6 * * *",
    catchup=False,
    tags=["koin", "dataform", "tableau", "daily"],
) as dag:

    ga4_check = PythonSensor(
        task_id="ga4_freshness_check",
        python_callable=check_ga4_freshness,
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    dataform_run = PythonOperator(
        task_id="dataform_run",
        python_callable=create_workflow_invocation,
    )

    dataform_wait = PythonSensor(
        task_id="dataform_wait",
        python_callable=wait_for_workflow_invocation,
        poke_interval=60,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    assertion_check = PythonOperator(
        task_id="assertion_check",
        python_callable=check_assertions,
    )

    tableau_refresh = PythonOperator(
        task_id="tableau_refresh",
        python_callable=refresh_all_datasources,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_slack_message,
        op_kwargs={
            "message": (
                ":white_check_mark: *[KOIN] Daily Pipeline 완료*\n"
                "- GA4 Sensor: OK\n"
                "- Dataform: SUCCEEDED\n"
                "- Assertions: PASSED\n"
                "- Tableau: Refreshed"
            )
        },
    )

    ga4_check >> dataform_run >> dataform_wait >> assertion_check >> tableau_refresh >> notify_success
