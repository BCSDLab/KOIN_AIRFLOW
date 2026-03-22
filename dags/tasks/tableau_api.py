from __future__ import annotations

import time
from typing import Any

import requests

from config.settings import (
    TABLEAU_API_VERSION,
    TABLEAU_DATASOURCE_IDS,
    TABLEAU_JOB_POLL_SECONDS,
    TABLEAU_JOB_TIMEOUT_SECONDS,
    TABLEAU_PAT_NAME,
    TABLEAU_PAT_SECRET,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_CONTENT_URL,
    TABLEAU_WAIT_FOR_JOB,
)


def _base_url() -> str:
    return f"{TABLEAU_SERVER_URL.rstrip('/')}/api/{TABLEAU_API_VERSION}"


def _sign_in() -> tuple[str, str]:
    url = f"{_base_url()}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": TABLEAU_PAT_NAME,
            "personalAccessTokenSecret": TABLEAU_PAT_SECRET,
            "site": {"contentUrl": TABLEAU_SITE_CONTENT_URL},
        }
    }
    response = requests.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    creds = response.json().get("credentials", {})
    token = creds.get("token")
    site_id = creds.get("site", {}).get("id")
    if not token or not site_id:
        raise RuntimeError("Tableau sign-in failed.")
    print(f"[tableau] signed in. site_id={site_id}")
    return token, site_id


def _sign_out(token: str) -> None:
    try:
        requests.post(
            f"{_base_url()}/auth/signout",
            headers={"X-Tableau-Auth": token},
            timeout=10,
        )
    except requests.RequestException:
        pass


def _refresh_datasource(token: str, site_id: str, ds_id: str) -> str | None:
    url = f"{_base_url()}/sites/{site_id}/datasources/{ds_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh datasource: {ds_id}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    job = response.json().get("job")
    return job.get("id") if isinstance(job, dict) else None


def _wait_for_job(token: str, site_id: str, job_id: str) -> None:
    deadline = time.time() + TABLEAU_JOB_TIMEOUT_SECONDS

    while time.time() < deadline:
        url = f"{_base_url()}/sites/{site_id}/jobs/{job_id}"
        response = requests.get(
            url,
            headers={"X-Tableau-Auth": token, "Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        job = response.json().get("job", {})
        finish_code = str(job.get("finishCode", ""))
        print(f"[tableau] job {job_id} finishCode={finish_code}")

        if finish_code == "0":
            return
        if finish_code not in {"", "-1"}:
            raise RuntimeError(f"Tableau job failed: {job_id}, code={finish_code}")
        time.sleep(TABLEAU_JOB_POLL_SECONDS)

    raise TimeoutError(f"Tableau job timeout: {job_id}")


def refresh_all_datasources(**_context: Any) -> None:
    """TABLEAU_DATASOURCE_IDS에 등록된 모든 datasource를 순차 새로고침."""
    if not TABLEAU_DATASOURCE_IDS:
        print("[tableau] No datasource IDs configured, skipping.")
        return

    token, site_id = _sign_in()
    try:
        for ds_id in TABLEAU_DATASOURCE_IDS:
            job_id = _refresh_datasource(token, site_id, ds_id)
            if job_id and TABLEAU_WAIT_FOR_JOB:
                _wait_for_job(token, site_id, job_id)
        print(f"[tableau] All {len(TABLEAU_DATASOURCE_IDS)} datasources refreshed.")
    finally:
        _sign_out(token)
