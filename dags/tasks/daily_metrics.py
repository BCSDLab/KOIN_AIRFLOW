# =====================================================================
# 일일 지표 요약 모듈 (daily_metrics.py)
# ---------------------------------------------------------------------
# * 목적: 전일 주요 KPI를 집계하여 Slack 메시지로 포맷팅합니다.
# * 지표:
#   1. DAU (전날 대비 증감)
#   2. 서비스별 고유 유저수 Top 5
#   3. 서비스별 이벤트 수 Top 5
# * 데이터 소스: kap-chat.mart.vw_mart_integrated_logs
# * 유저 카운트: user_pseudo_id 기준 (분석 기법 규칙)
# =====================================================================
from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from config.settings import (
    DATAFORM_PROJECT_ID,
    GCP_CONN_ID,
    MART_DATASET,
    MART_INTEGRATED_VIEW,
)

MART_TABLE = f"{DATAFORM_PROJECT_ID}.{MART_DATASET}.{MART_INTEGRATED_VIEW}"


def build_daily_metrics_summary(**context: Any) -> str:
    """
    전일 주요 지표를 집계하고 Slack 메시지 형태로 반환합니다.

    집계 항목:
        1. DAU: 어제 vs 그제 고유 유저수 비교
        2. 서비스별 고유 유저수 Top 5 (unmapped 제외)
        3. 서비스별 이벤트 수 Top 5 (unmapped 제외)

    Returns:
        Slack으로 전송할 지표 요약 메시지 (XCom 자동 저장)
    """
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client()

    yesterday = pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
    day_before = pendulum.now("Asia/Seoul").subtract(days=2).format("YYYY-MM-DD")

    # 1) 서비스별 고유 유저수 Top 5
    top_users_query = f"""
    SELECT service_name, COUNT(DISTINCT user_pseudo_id) AS unique_users
    FROM `{MART_TABLE}`
    WHERE event_dt = '{yesterday}'
      AND service_name != 'unmapped'
    GROUP BY service_name
    ORDER BY unique_users DESC
    LIMIT 5
    """

    # 2) 서비스별 이벤트 수 Top 5
    top_events_query = f"""
    SELECT service_name, COUNT(*) AS event_cnt
    FROM `{MART_TABLE}`
    WHERE event_dt = '{yesterday}'
      AND service_name != 'unmapped'
    GROUP BY service_name
    ORDER BY event_cnt DESC
    LIMIT 5
    """

    # 3) DAU 비교 (어제 vs 그제)
    dau_query = f"""
    SELECT
      event_dt,
      COUNT(DISTINCT user_pseudo_id) AS dau
    FROM `{MART_TABLE}`
    WHERE event_dt IN ('{yesterday}', '{day_before}')
    GROUP BY event_dt
    ORDER BY event_dt
    """

    top_users = list(client.query(top_users_query).result())
    top_events = list(client.query(top_events_query).result())
    dau_rows = list(client.query(dau_query).result())

    # DAU 비교 계산
    dau_map = {str(row.event_dt): row.dau for row in dau_rows}
    dau_yesterday = dau_map.get(yesterday, 0)
    dau_day_before = dau_map.get(day_before, 0)
    dau_change = dau_yesterday - dau_day_before
    dau_pct = round(dau_change / dau_day_before * 100, 1) if dau_day_before else 0
    dau_emoji = ":arrow_up:" if dau_change >= 0 else ":arrow_down:"

    # 메시지 포맷팅
    lines = [
        f":bar_chart: *[KOIN] Daily Metrics Summary* ({yesterday})",
        "",
        f"*DAU: {dau_yesterday:,}명* {dau_emoji} {dau_change:+,} ({dau_pct:+.1f}%) vs 전일({dau_day_before:,})",
        "",
        "*서비스별 고유 유저수 Top 5:*",
    ]
    for i, row in enumerate(top_users, 1):
        lines.append(f"  {i}. {row.service_name}: {row.unique_users:,}명")

    lines.append("")
    lines.append("*서비스별 이벤트 수 Top 5:*")
    for i, row in enumerate(top_events, 1):
        lines.append(f"  {i}. {row.service_name}: {row.event_cnt:,}건")

    return "\n".join(lines)
