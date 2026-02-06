from __future__ import annotations

import time
from typing import Any

import google.auth
from google.auth.transport.requests import Request
import requests

from config.settings import (
    DATAFORM_LOCATION,
    DATAFORM_PROJECT_ID,
    DATAFORM_REPOSITORY_ID,
    DATAFORM_WORKFLOW_CONFIG,
)


DATAFORM_BASE_URL = "https://dataform.googleapis.com/v1beta1"


def _get_access_token() -> str:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    return credentials.token


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_access_token()}",
        "Content-Type": "application/json",
    }


def _workflow_config_path() -> str:
    return (
        f"projects/{DATAFORM_PROJECT_ID}/locations/{DATAFORM_LOCATION}"
        f"/repositories/{DATAFORM_REPOSITORY_ID}/workflowConfigs/{DATAFORM_WORKFLOW_CONFIG}"
    )


def _invocations_parent() -> str:
    return (
        f"projects/{DATAFORM_PROJECT_ID}/locations/{DATAFORM_LOCATION}"
        f"/repositories/{DATAFORM_REPOSITORY_ID}/workflowInvocations"
    )


def create_workflow_invocation(**context: Any) -> str:
    url = f"{DATAFORM_BASE_URL}/{_invocations_parent()}"
    payload = {"workflowConfig": _workflow_config_path()}
    print(f"[dataform] create url: {url}")
    print(f"[dataform] workflow config: {_workflow_config_path()}")
    response = requests.post(url, headers=_headers(), json=payload, timeout=30)
    response.raise_for_status()
    invocation = response.json()
    invocation_name = invocation.get("name")
    if not invocation_name:
        raise RuntimeError("Dataform workflow invocation name not returned.")
    print(f"[dataform] invocation name: {invocation_name}")
    context["ti"].xcom_push(key="dataform_invocation_name", value=invocation_name)
    return invocation_name


def get_workflow_invocation_state(invocation_name: str) -> str:
    url = f"{DATAFORM_BASE_URL}/{invocation_name}"
    print(f"[dataform] get url: {url}")
    response = requests.get(url, headers=_headers(), timeout=30)
    response.raise_for_status()
    state = response.json().get("state", "STATE_UNSPECIFIED")
    print(f"[dataform] invocation state: {state}")
    return state


def wait_for_workflow_invocation(**context: Any) -> bool:
    invocation_name = context["ti"].xcom_pull(
        key="dataform_invocation_name", task_ids="dataform_run"
    )
    if not invocation_name:
        raise RuntimeError("No Dataform invocation name found in XCom.")

    state = get_workflow_invocation_state(invocation_name)
    if state == "SUCCEEDED":
        return True
    if state in {"FAILED", "CANCELLED"}:
        raise RuntimeError(f"Dataform workflow failed with state: {state}")
    return False
