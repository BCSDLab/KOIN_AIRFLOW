from __future__ import annotations

from typing import Any

import requests

from config.settings import SLACK_USERNAME, SLACK_WEBHOOK_URL


def send_slack_message(message: str, **_context: Any) -> None:
    if not SLACK_WEBHOOK_URL:
        print("[slack] SLACK_WEBHOOK_URL not set, skipping.")
        return
    payload = {"text": message, "username": SLACK_USERNAME}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()


def on_failure_slack(context: dict) -> None:
    dag_id = context.get("dag", {}).dag_id
    task_id = context.get("task_instance", {}).task_id
    exec_date = context.get("execution_date", "")
    exception = context.get("exception", "")
    message = (
        f":x: *[KOIN] Pipeline FAILED*\n"
        f"- DAG: `{dag_id}`\n"
        f"- Task: `{task_id}`\n"
        f"- Date: {exec_date}\n"
        f"- Error: {exception}"
    )
    send_slack_message(message)
