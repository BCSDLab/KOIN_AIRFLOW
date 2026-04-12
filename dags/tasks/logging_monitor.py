# =====================================================================
# 로깅 모니터링 모듈 (logging_monitor.py)
# ---------------------------------------------------------------------
# * 목적: GA4 이벤트 로깅의 변화를 감지하여 Slack으로 알림합니다.
# * 기능:
#   1. detect_new_event_labels(): 과거 N일간 없었던 신규 event_label 감지
#   2. detect_decreased_logging(): 전날 대비 50% 이상 감소한 event_label 감지
#   3. send_combined_logging_alert(): 위 두 결과를 합쳐서 Slack 전송
# =====================================================================
from __future__ import annotations

from typing import Any

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from config.settings import (
    DATAFORM_PROJECT_ID,
    GCP_CONN_ID,
    LOGGING_LOOKBACK_DAYS,
    MART_DATASET,
    MART_INTEGRATED_VIEW,
)
from tasks.slack_notify import send_slack_message

# 마트 뷰 전체 경로
MART_TABLE = f"{DATAFORM_PROJECT_ID}.{MART_DATASET}.{MART_INTEGRATED_VIEW}"


def detect_new_event_labels(**context: Any) -> str:
    """
    과거 N일간 존재하지 않았던 신규 event_label을 감지합니다.

    - 어제 발생한 event_label 중 직전 N일간 한 번도 없었던 것을 찾습니다.
    - 결과를 Slack 메시지 형태로 반환 (XCom 자동 저장).
    """
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client()

    query = f"""
    WITH yesterday AS (
      SELECT DISTINCT event_label
      FROM `{MART_TABLE}`
      WHERE event_dt = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
        AND event_label IS NOT NULL
    ),
    historical AS (
      SELECT DISTINCT event_label
      FROM `{MART_TABLE}`
      WHERE event_dt BETWEEN
        DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL {LOGGING_LOOKBACK_DAYS + 1} DAY)
        AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 2 DAY)
        AND event_label IS NOT NULL
    )
    SELECT y.event_label
    FROM yesterday y
    LEFT JOIN historical h ON y.event_label = h.event_label
    WHERE h.event_label IS NULL
    ORDER BY y.event_label
    """

    rows = list(client.query(query).result())

    if not rows:
        return ":mag: *[KOIN] 신규 로깅 감지*\n- 새로 발견된 event_label 없음"

    labels = [row.event_label for row in rows]
    return (
        f":sparkles: *[KOIN] 신규 로깅 {len(labels)}건 감지!*\n"
        + "\n".join(f"- `{label}`" for label in labels)
    )


def detect_decreased_logging(**context: Any) -> str:
    """
    전날 대비 50% 이상 감소한 event_label을 감지합니다.

    - 그제(D-2) 대비 어제(D-1) 이벤트 수가 50% 이상 줄어든 항목을 찾습니다.
    - 완전히 사라진 경우(0건)도 포함합니다.
    """
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client()

    query = f"""
    WITH yesterday AS (
      SELECT event_label, COUNT(*) AS cnt
      FROM `{MART_TABLE}`
      WHERE event_dt = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
        AND event_label IS NOT NULL
      GROUP BY event_label
    ),
    day_before AS (
      SELECT event_label, COUNT(*) AS cnt
      FROM `{MART_TABLE}`
      WHERE event_dt = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 2 DAY)
        AND event_label IS NOT NULL
      GROUP BY event_label
    )
    SELECT
      d.event_label,
      d.cnt AS prev_cnt,
      IFNULL(y.cnt, 0) AS curr_cnt,
      ROUND(SAFE_DIVIDE(IFNULL(y.cnt, 0) - d.cnt, d.cnt) * 100, 1) AS change_pct
    FROM day_before d
    LEFT JOIN yesterday y ON d.event_label = y.event_label
    WHERE IFNULL(y.cnt, 0) < d.cnt * 0.5
    ORDER BY change_pct ASC
    LIMIT 20
    """

    rows = list(client.query(query).result())

    if not rows:
        return ":chart_with_downwards_trend: *[KOIN] 로깅 감소 알림*\n- 50% 이상 감소한 event_label 없음"

    lines = [f":warning: *[KOIN] 로깅 50%+ 감소 {len(rows)}건 감지!*"]
    for row in rows:
        lines.append(
            f"- `{row.event_label}`: {row.prev_cnt:,} → {row.curr_cnt:,} ({row.change_pct:+.1f}%)"
        )
    return "\n".join(lines)


def send_combined_logging_alert(**context: Any) -> None:
    """
    신규 로깅 감지 + 감소 알림 결과를 합쳐서 Slack으로 전송합니다.

    - detect_new_logging, detect_decreased_logging 태스크의 XCom에서
      각각의 메시지를 가져와 합칩니다.
    """
    ti = context["ti"]
    new_msg = ti.xcom_pull(task_ids="detect_new_logging") or ""
    dec_msg = ti.xcom_pull(task_ids="detect_decreased_logging") or ""
    combined = f"{new_msg}\n\n{dec_msg}"
    send_slack_message(combined)
