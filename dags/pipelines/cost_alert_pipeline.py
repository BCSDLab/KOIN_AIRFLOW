from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from config.settings import (
    BILLING_EXPORT_DATASET,
    BILLING_EXPORT_PROJECT_ID,
    BILLING_EXPORT_TABLE_PREFIX,
    BILLING_TIMEZONE,
    DEFAULT_OWNER,
    GCP_CONN_ID,
)
from tasks.slack_notify import send_slack_message


def _build_cost_query() -> str:
    table = f"{BILLING_EXPORT_PROJECT_ID}.{BILLING_EXPORT_DATASET}.{BILLING_EXPORT_TABLE_PREFIX}*"
    return f"""
DECLARE target_date DATE;
SET target_date = DATE_SUB(CURRENT_DATE('{BILLING_TIMEZONE}'), INTERVAL 1 DAY);

SELECT
  target_date AS cost_date,
  IFNULL(MAX(IF(l.key = 'cost_owner_type', l.value, NULL)), 'unknown') AS cost_owner_type,
  IFNULL(MAX(IF(l.key = 'cost_owner', l.value, NULL)), 'unknown') AS cost_owner,
  IFNULL(MAX(IF(l.key = 'workflow', l.value, NULL)), 'unknown') AS workflow,
  SUM(cost) AS total_cost
FROM `{table}` AS b
LEFT JOIN UNNEST(b.labels) AS l
WHERE DATE(b.usage_start_time, '{BILLING_TIMEZONE}') = target_date
GROUP BY cost_date, cost_owner_type, cost_owner, workflow
ORDER BY total_cost DESC
LIMIT 50
""".strip()


def query_daily_costs(**_context: Any) -> str:
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client()
    query = _build_cost_query()
    rows = list(client.query(query).result())

    target_date = (
        pendulum.now(BILLING_TIMEZONE).subtract(days=1).format("YYYY-MM-DD")
    )
    total_cost = sum(float(row.total_cost or 0) for row in rows)

    lines = [
        f"*GCP 비용 알림* ({target_date} {BILLING_TIMEZONE})",
        f"총 비용: ${total_cost:,.2f}",
        "상위 항목:",
    ]

    if not rows:
        lines.append("- 집계된 비용이 없습니다.")
    else:
        for row in rows[:10]:
            lines.append(
                f"- {row.cost_owner_type}/{row.cost_owner}/{row.workflow}: "
                f"${float(row.total_cost or 0):,.2f}"
            )
    return "\n".join(lines)


with DAG(
    dag_id="cost_alert_pipeline",
    schedule_interval="0 22 * * *",
    start_date=pendulum.datetime(2026, 2, 10, tz=BILLING_TIMEZONE),
    catchup=False,
    default_args={
        "owner": DEFAULT_OWNER,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cost", "billing", "slack"],
) as dag:
    bq_cost_query = PythonOperator(
        task_id="bq_cost_query",
        python_callable=query_daily_costs,
    )

    slack_notify = PythonOperator(
        task_id="slack_notify",
        python_callable=send_slack_message,
        op_kwargs={"message": "{{ ti.xcom_pull(task_ids='bq_cost_query') }}"},
    )

    bq_cost_query >> slack_notify
