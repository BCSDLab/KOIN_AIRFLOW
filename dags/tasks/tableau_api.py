from __future__ import annotations

import os
import time
from typing import Any

import requests

from config.settings import (
    TABLEAU_API_VERSION,
    TABLEAU_DATASOURCE_ID,
    TABLEAU_FLOW_ID,
    TABLEAU_JOB_POLL_SECONDS,
    TABLEAU_JOB_TIMEOUT_SECONDS,
    TABLEAU_PAT_NAME,
    TABLEAU_PAT_SECRET,
    TABLEAU_REFRESH_TARGET,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_CONTENT_URL,
    TABLEAU_WAIT_FOR_JOB,
    TABLEAU_WORKBOOK_ID,
)


def _require(value: str, name: str) -> str:
    if not value:
        raise RuntimeError(f"Missing required Tableau setting: {name}")
    return value


def _is_true(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _sign_in() -> tuple[str, str]:
    server = _require(TABLEAU_SERVER_URL, "TABLEAU_SERVER_URL").rstrip("/")
    version = _require(TABLEAU_API_VERSION, "TABLEAU_API_VERSION")
    pat_name = _require(TABLEAU_PAT_NAME, "TABLEAU_PAT_NAME")
    pat_secret = _require(TABLEAU_PAT_SECRET, "TABLEAU_PAT_SECRET")

    url = f"{server}/api/{version}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": TABLEAU_SITE_CONTENT_URL},
        }
    }
    print(f"[tableau] sign-in url: {url}")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    credentials = response.json().get("credentials", {})
    token = credentials.get("token")
    site_id = credentials.get("site", {}).get("id")
    if not token or not site_id:
        raise RuntimeError("Tableau sign-in failed: token or site id missing.")
    return token, site_id


def _sign_out(token: str) -> None:
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/auth/signout"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    try:
        requests.post(url, headers=headers, timeout=30)
    except requests.RequestException:
        print("[tableau] sign-out failed (ignored).")


def _extract_job_id(response: requests.Response) -> str | None:
    payload = response.json()
    job = payload.get("job")
    if isinstance(job, dict):
        return job.get("id")
    return None


def _refresh_workbook(token: str, site_id: str) -> str | None:
    workbook_id = _require(TABLEAU_WORKBOOK_ID, "TABLEAU_WORKBOOK_ID")
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/workbooks/{workbook_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh workbook url: {url}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return _extract_job_id(response)


def _refresh_datasource(token: str, site_id: str) -> str | None:
    datasource_id = _require(TABLEAU_DATASOURCE_ID, "TABLEAU_DATASOURCE_ID")
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/datasources/{datasource_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh datasource url: {url}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return _extract_job_id(response)


def _run_flow(token: str, site_id: str) -> str | None:
    flow_id = _require(TABLEAU_FLOW_ID, "TABLEAU_FLOW_ID")
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/flows/{flow_id}/run"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] run flow url: {url}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return _extract_job_id(response)


def _get_job(token: str, site_id: str, job_id: str) -> dict[str, Any]:
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/jobs/{job_id}"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("job", {})


def _wait_for_job(token: str, site_id: str, job_id: str) -> None:
    poll_seconds = max(5, TABLEAU_JOB_POLL_SECONDS)
    timeout_seconds = max(60, TABLEAU_JOB_TIMEOUT_SECONDS)
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        job = _get_job(token, site_id, job_id)
        finish_code = str(job.get("finishCode", ""))
        progress = job.get("progress")
        print(f"[tableau] job {job_id} progress={progress} finishCode={finish_code}")

        if finish_code == "0":
            print(f"[tableau] job {job_id} completed.")
            return
        if finish_code not in {"", "-1"}:
            raise RuntimeError(f"Tableau job failed. job_id={job_id}, finishCode={finish_code}, payload={job}")

        time.sleep(poll_seconds)

    raise TimeoutError(f"Tableau job wait timeout. job_id={job_id}, timeout={timeout_seconds}s")


def refresh_tableau_extract(**_context: Any) -> str | None:
    target = TABLEAU_REFRESH_TARGET.lower()
    if target not in {"workbook", "datasource", "flow"}:
        raise RuntimeError("TABLEAU_REFRESH_TARGET must be 'workbook', 'datasource', or 'flow'.")

    token, site_id = _sign_in()
    try:
        if _is_true(os.getenv("TABLEAU_DEBUG_AUTH_ONLY", "false")):
            print("[tableau] auth-only debug enabled: skipping refresh call.")
            return None

        if target == "workbook":
            job_id = _refresh_workbook(token, site_id)
        elif target == "datasource":
            job_id = _refresh_datasource(token, site_id)
        else:
            job_id = _run_flow(token, site_id)

        if job_id:
            print(f"[tableau] refresh job id: {job_id}")
            if _is_true(TABLEAU_WAIT_FOR_JOB):
                _wait_for_job(token, site_id, job_id)

        return job_id
    finally:
        _sign_out(token)
