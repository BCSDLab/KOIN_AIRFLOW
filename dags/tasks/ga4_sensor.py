from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from config.settings import GA4_DATASET_ID, GA4_PROJECT_ID


def check_ga4_freshness(**_context: Any) -> bool:
    """어제 날짜의 GA4 events 테이블이 존재하고 데이터가 있는지 확인."""
    client = bigquery.Client(project=GA4_PROJECT_ID)
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM `{GA4_PROJECT_ID}.{GA4_DATASET_ID}.events_*`
    WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',
        DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    """
    result = list(client.query(query).result())
    cnt = result[0].cnt if result else 0
    print(f"[ga4_sensor] yesterday event count: {cnt}")
    return cnt > 0
