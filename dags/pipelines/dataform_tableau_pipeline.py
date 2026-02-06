from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.dataform_api import create_workflow_invocation, wait_for_workflow_invocation


local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dataform_tableau_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 6, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["dataform", "tableau", "pipeline"],
) as dag:
    start = EmptyOperator(task_id="start")
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
    tableau_refresh = EmptyOperator(task_id="tableau_refresh")
    end = EmptyOperator(task_id="end")

    start >> dataform_run >> dataform_wait >> tableau_refresh >> end
