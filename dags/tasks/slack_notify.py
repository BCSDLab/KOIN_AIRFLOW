# =====================================================================
# Slack 알림 모듈 (slack_notify.py)
# ---------------------------------------------------------------------
# * 목적: 파이프라인 성공/실패 시 Slack 채널로 알림을 전송합니다.
# * 사용처:
#   - send_slack_message(): 성공 알림 등 범용 메시지 전송
#   - on_failure_slack(): DAG default_args의 on_failure_callback으로 등록하여
#                         태스크 실패 시 자동으로 Slack 알림 발송
# =====================================================================
from __future__ import annotations

from typing import Any

import requests

from config.settings import SLACK_USERNAME, SLACK_WEBHOOK_URL


def send_slack_message(message: str, **_context: Any) -> None:
    """
    Slack Webhook으로 메시지를 전송합니다.

    Args:
        message: 전송할 텍스트 메시지 (Slack 마크다운 지원)
        **_context: Airflow PythonOperator가 전달하는 컨텍스트 (여기서는 미사용)

    Note:
        SLACK_WEBHOOK_URL이 설정되지 않으면 로그만 남기고 건너뜁니다.
        (로컬 개발 환경에서 Slack 없이도 DAG가 정상 실행되도록)
    """
    if not SLACK_WEBHOOK_URL:
        print("[slack] SLACK_WEBHOOK_URL not set, skipping.")
        return
    payload = {"text": message, "username": SLACK_USERNAME}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()


def on_failure_slack(context: dict) -> None:
    """
    Airflow 태스크 실패 시 자동 호출되는 콜백 함수.

    DAG의 default_args에 아래와 같이 등록합니다:
        default_args = {
            "on_failure_callback": on_failure_slack,
        }

    Args:
        context: Airflow가 전달하는 실행 컨텍스트 딕셔너리.
                 dag, task_instance, execution_date, exception 등을 포함합니다.

    전송 예시:
        :x: *[KOIN] Pipeline FAILED*
        - DAG: `koin_daily_pipeline`
        - Task: `dataform_run`
        - Date: 2026-03-22T06:00:00+09:00
        - Error: Dataform workflow FAILED
    """
    dag_id = context.get("dag", {}).dag_id             # 실패한 DAG 이름
    task_id = context.get("task_instance", {}).task_id  # 실패한 태스크 이름
    exec_date = context.get("execution_date", "")       # 실행 예정 시각
    exception = context.get("exception", "")             # 발생한 예외 메시지
    message = (
        f":x: *[KOIN] Pipeline FAILED*\n"
        f"- DAG: `{dag_id}`\n"
        f"- Task: `{task_id}`\n"
        f"- Date: {exec_date}\n"
        f"- Error: {exception}"
    )
    send_slack_message(message)
