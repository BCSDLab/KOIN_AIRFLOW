from __future__ import annotations

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
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_access_token()}",
        "Content-Type": "application/json",
    }


def _repo_path() -> str:
    return (
        f"projects/{DATAFORM_PROJECT_ID}/locations/{DATAFORM_LOCATION}"
        f"/repositories/{DATAFORM_REPOSITORY_ID}"
    )


def create_workflow_invocation(**context: Any) -> str:
    url = f"{DATAFORM_BASE_URL}/{_repo_path()}/workflowInvocations"
    payload = {
        "workflowConfig": f"{_repo_path()}/workflowConfigs/{DATAFORM_WORKFLOW_CONFIG}"
    }
    print(f"[dataform] POST {url}")
    response = requests.post(url, headers=_headers(), json=payload, timeout=30)
    response.raise_for_status()
    invocation_name = response.json().get("name")
    if not invocation_name:
        raise RuntimeError("Dataform invocation name not returned.")
    print(f"[dataform] invocation: {invocation_name}")
    context["ti"].xcom_push(key="dataform_invocation_name", value=invocation_name)
    return invocation_name


def wait_for_workflow_invocation(**context: Any) -> bool:
    invocation_name = context["ti"].xcom_pull(
        key="dataform_invocation_name", task_ids="dataform_run"
    )
    if not invocation_name:
        raise RuntimeError("No Dataform invocation name in XCom.")

    url = f"{DATAFORM_BASE_URL}/{invocation_name}"
    response = requests.get(url, headers=_headers(), timeout=30)
    response.raise_for_status()
    state = response.json().get("state", "STATE_UNSPECIFIED")
    print(f"[dataform] state: {state}")

    if state == "SUCCEEDED":
        return True
    if state in {"FAILED", "CANCELLED"}:
        raise RuntimeError(f"Dataform workflow {state}")
    return False
