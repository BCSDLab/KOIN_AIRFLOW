from __future__ import annotations

from typing import Any

import requests

from config.settings import SLACK_USERNAME, SLACK_WEBHOOK_URL


def _require(value: str, name: str) -> str:
    if not value:
        raise RuntimeError(f"Missing required Slack setting: {name}")
    return value


def send_slack_message(message: str, **_context: Any) -> None:
    webhook_url = _require(SLACK_WEBHOOK_URL, "SLACK_WEBHOOK_URL")
    payload = {"text": message, "username": SLACK_USERNAME}
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
