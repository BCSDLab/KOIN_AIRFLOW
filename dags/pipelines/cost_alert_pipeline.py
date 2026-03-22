# =====================================================================
# GCP 비용 알림 DAG (cost_alert_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: 전일 GCP 사용 비용을 조회하여 Slack으로 알림합니다.
# * 스케줄: 매일 KST 22:00 (cron: "0 22 * * *")
# * 데이터 소스: Cloud Billing Export (BigQuery)
#   - GCP 콘솔에서 Billing Export를 BigQuery로 설정해야 합니다.
#   - 테이블: {project}.{dataset}.gcp_billing_export_resource_v1_*
# * 파이프라인 흐름:
#   bq_cost_query → slack_notify
#   1. BigQuery에서 전일 비용을 cost_owner/workflow별로 집계
#   2. 결과를 Slack 메시지로 포맷팅하여 전송
# * 출처: test_yun 브랜치에서 계승 (임포트 경로만 조정)
# =====================================================================
from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # Airflow GCP Provider의 BigQuery 연동
from google.api_core.exceptions import NotFound                          # 테이블 미존재 예외 처리용

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
    """
    전일 GCP 비용을 조회하는 SQL 쿼리를 생성합니다.

    쿼리 로직:
        - target_date = 어제 (KST 기준)
        - labels에서 cost_owner_type, cost_owner, workflow를 추출
        - cost 합산 → 상위 50건 반환

    Labels 설명:
        - cost_owner_type: 비용 발생 주체 유형 (예: service, user)
        - cost_owner: 비용 발생 주체 이름 (예: bigquery, dataform)
        - workflow: 관련 워크플로우 이름
    """
    table = f"{BILLING_EXPORT_PROJECT_ID}.{BILLING_EXPORT_DATASET}.{BILLING_EXPORT_TABLE_PREFIX}*"
    return f"""
DECLARE target_date DATE;
SET target_date = DATE_SUB(CURRENT_DATE('{BILLING_TIMEZONE}'), INTERVAL 1 DAY);

SELECT
  target_date AS cost_date,
  IFNULL(
    (SELECT l.value FROM UNNEST(b.labels) AS l WHERE l.key = 'cost_owner_type' LIMIT 1),
    'unknown'
  ) AS cost_owner_type,
  IFNULL(
    (SELECT l.value FROM UNNEST(b.labels) AS l WHERE l.key = 'cost_owner' LIMIT 1),
    'unknown'
  ) AS cost_owner,
  IFNULL(
    (SELECT l.value FROM UNNEST(b.labels) AS l WHERE l.key = 'workflow' LIMIT 1),
    'unknown'
  ) AS workflow,
  SUM(cost) AS total_cost
FROM `{table}` AS b
WHERE DATE(b.usage_start_time, '{BILLING_TIMEZONE}') = target_date
GROUP BY cost_date, cost_owner_type, cost_owner, workflow
ORDER BY total_cost DESC
LIMIT 50
""".strip()


def _build_export_table_exists_query() -> str:
    """
    Billing Export 테이블이 존재하는지 확인하는 쿼리를 생성합니다.

    INFORMATION_SCHEMA.TABLES에서 테이블 접두사로 검색합니다.
    테이블이 없으면 Cloud Billing Export 설정이 필요합니다.
    """
    return f"""
SELECT COUNT(1) AS table_count
FROM `{BILLING_EXPORT_PROJECT_ID}.{BILLING_EXPORT_DATASET}.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '{BILLING_EXPORT_TABLE_PREFIX}%'
""".strip()


def _build_missing_table_message() -> str:
    """Billing Export 테이블이 없을 때 표시할 안내 메시지를 생성합니다."""
    return (
        f"*GCP Cost Alert* ({pendulum.now(BILLING_TIMEZONE).format('YYYY-MM-DD')} {BILLING_TIMEZONE})\n"
        f"- Billing export table not found: "
        f"`{BILLING_EXPORT_PROJECT_ID}.{BILLING_EXPORT_DATASET}.{BILLING_EXPORT_TABLE_PREFIX}*`\n"
        "- Configure Cloud Billing export (Detailed usage cost data) to BigQuery and retry."
    )


def query_daily_costs(**_context: Any) -> str:
    """
    BigQuery에서 전일 GCP 비용을 조회하고 Slack 메시지 형태로 반환합니다.

    처리 흐름:
        1. Billing Export 테이블 존재 여부 확인
        2. 테이블이 없으면 안내 메시지 반환
        3. 테이블이 있으면 비용 쿼리 실행
        4. 결과를 Slack 마크다운 형태로 포맷팅하여 반환

    Returns:
        Slack으로 전송할 비용 요약 메시지 (문자열)
        → XCom에 자동 저장되어 다음 태스크(slack_notify)에서 사용됩니다.
    """
    # BigQuery Hook으로 클라이언트 생성 (Airflow Connection 사용)
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client()
    exists_query = _build_export_table_exists_query()
    query = _build_cost_query()

    # 1) Billing Export 테이블 존재 확인
    try:
        table_count_row = next(iter(client.query(exists_query).result()), None)
    except NotFound:
        return _build_missing_table_message()

    if not table_count_row or int(table_count_row.table_count or 0) == 0:
        return _build_missing_table_message()

    # 2) 비용 쿼리 실행
    rows = list(client.query(query).result())

    # 3) 결과 포맷팅
    target_date = pendulum.now(BILLING_TIMEZONE).subtract(days=1).format("YYYY-MM-DD")
    total_cost = sum(float(row.total_cost or 0) for row in rows)

    lines = [
        f"*GCP Cost Alert* ({target_date} {BILLING_TIMEZONE})",
        f"Total cost: ${total_cost:,.2f}",
        "Top items:",
    ]

    if not rows:
        lines.append("- No billed usage found for the target date.")
    else:
        # 상위 10건만 표시
        for row in rows[:10]:
            lines.append(
                f"- {row.cost_owner_type}/{row.cost_owner}/{row.workflow}: "
                f"${float(row.total_cost or 0):,.2f}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DAG 정의
# - 매일 KST 22:00 실행
# - catchup=False: 과거 미실행 스케줄 소급 실행하지 않음
# ---------------------------------------------------------------------------
with DAG(
    dag_id="cost_alert_pipeline",
    schedule_interval="0 22 * * *",  # 매일 KST 22:00
    start_date=pendulum.datetime(2026, 2, 10, tz=BILLING_TIMEZONE),
    catchup=False,
    default_args={
        "owner": DEFAULT_OWNER,
        "retries": 2,                           # 실패 시 2회 재시도
        "retry_delay": timedelta(minutes=5),     # 재시도 간격 5분
    },
    tags=["cost", "billing", "slack"],
) as dag:

    # Step 1: BigQuery에서 전일 비용 조회
    # - 반환값(문자열)이 XCom에 자동 저장됨
    bq_cost_query = PythonOperator(
        task_id="bq_cost_query",
        python_callable=query_daily_costs,
    )

    # Step 2: 조회 결과를 Slack으로 전송
    # - Jinja 템플릿으로 이전 태스크의 XCom 값을 가져옴
    # - {{ ti.xcom_pull(...) }}: Airflow가 실행 시점에 실제 값으로 치환
    slack_notify = PythonOperator(
        task_id="slack_notify",
        python_callable=send_slack_message,
        op_kwargs={"message": "{{ ti.xcom_pull(task_ids='bq_cost_query') }}"},
    )

    # 태스크 의존성: 비용 조회 → Slack 알림
    bq_cost_query >> slack_notify
