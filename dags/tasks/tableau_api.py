from __future__ import annotations

from typing import Any

import requests

from config.settings import (
    TABLEAU_API_VERSION,
    TABLEAU_DATASOURCE_ID,
    TABLEAU_PAT_NAME,
    TABLEAU_PAT_SECRET,
    TABLEAU_REFRESH_TARGET,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_CONTENT_URL,
    TABLEAU_WORKBOOK_ID,
)
import os


def _require(value: str, name: str) -> str:
    if not value:
        raise RuntimeError(f"Missing required Tableau setting: {name}")
    return value


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


def _refresh_workbook(token: str, site_id: str) -> str | None:
    workbook_id = _require(TABLEAU_WORKBOOK_ID, "TABLEAU_WORKBOOK_ID")
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/workbooks/{workbook_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh workbook url: {url}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("job", {}).get("id")


def _refresh_datasource(token: str, site_id: str) -> str | None:
    datasource_id = _require(TABLEAU_DATASOURCE_ID, "TABLEAU_DATASOURCE_ID")
    server = TABLEAU_SERVER_URL.rstrip("/")
    version = TABLEAU_API_VERSION
    url = f"{server}/api/{version}/sites/{site_id}/datasources/{datasource_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh datasource url: {url}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("job", {}).get("id")


def refresh_tableau_extract(**_context: Any) -> str | None:
    target = TABLEAU_REFRESH_TARGET.lower()
    if target not in {"workbook", "datasource"}:
        raise RuntimeError("TABLEAU_REFRESH_TARGET must be 'workbook' or 'datasource'.")

    token, site_id = _sign_in()
    try:
        if os.getenv("TABLEAU_DEBUG_AUTH_ONLY", "false").lower() in {"1", "true", "yes"}:
            print("[tableau] auth-only debug enabled: skipping refresh call.")
            return None

        if target == "workbook":
            job_id = _refresh_workbook(token, site_id)
        else:
            job_id = _refresh_datasource(token, site_id)
        if job_id:
            print(f"[tableau] refresh job id: {job_id}")
        return job_id
    finally:
        _sign_out(token)
