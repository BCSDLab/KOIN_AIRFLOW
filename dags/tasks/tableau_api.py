# =====================================================================
# Tableau REST API 모듈 (tableau_api.py)
# ---------------------------------------------------------------------
# * 목적: Tableau Cloud의 데이터소스를 새로고침(Extract Refresh)합니다.
# * 배경: Dataform이 BigQuery에 데이터를 적재한 후,
#         Tableau 대시보드가 최신 데이터를 반영하려면 데이터소스 새로고침이 필요합니다.
# * 인증: PAT(Personal Access Token) 방식 → OTP/MFA 불필요
# * 흐름:
#   1. PAT으로 sign-in → API 토큰 + site_id 획득
#   2. 각 데이터소스에 대해 refresh 요청 → job_id 반환
#   3. (선택) job 완료까지 polling으로 대기
#   4. sign-out으로 토큰 해제
# * API 문서: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm
# =====================================================================
from __future__ import annotations

import time
from typing import Any

import requests

from config.settings import (
    TABLEAU_API_VERSION,
    TABLEAU_DATASOURCE_IDS,
    TABLEAU_FLOW_IDS,
    TABLEAU_JOB_POLL_SECONDS,
    TABLEAU_JOB_TIMEOUT_SECONDS,
    TABLEAU_PAT_NAME,
    TABLEAU_PAT_SECRET,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_CONTENT_URL,
    TABLEAU_WAIT_FOR_JOB,
)


def _base_url() -> str:
    """Tableau REST API의 베이스 URL을 반환합니다."""
    return f"{TABLEAU_SERVER_URL.rstrip('/')}/api/{TABLEAU_API_VERSION}"


def _sign_in() -> tuple[str, str]:
    """
    Tableau Cloud에 PAT으로 로그인합니다.

    Returns:
        (token, site_id) 튜플:
        - token: 이후 API 호출 시 X-Tableau-Auth 헤더에 사용
        - site_id: 사이트 고유 ID (API 경로에 사용)

    Raises:
        RuntimeError: 인증 실패 시 (잘못된 PAT이나 만료된 토큰)
    """
    url = f"{_base_url()}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": TABLEAU_PAT_NAME,        # PAT 이름
            "personalAccessTokenSecret": TABLEAU_PAT_SECRET,    # PAT 시크릿
            "site": {"contentUrl": TABLEAU_SITE_CONTENT_URL},   # 사이트 이름 (예: "bcsd")
        }
    }
    response = requests.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    # 응답에서 인증 토큰과 사이트 ID 추출
    creds = response.json().get("credentials", {})
    token = creds.get("token")
    site_id = creds.get("site", {}).get("id")
    if not token or not site_id:
        raise RuntimeError("Tableau sign-in failed.")

    print(f"[tableau] signed in. site_id={site_id}")
    return token, site_id


def _sign_out(token: str) -> None:
    """
    Tableau Cloud에서 로그아웃합니다.

    - 토큰을 무효화하여 보안을 유지합니다.
    - 실패해도 예외를 발생시키지 않습니다 (finally 블록에서 호출되므로).
    """
    try:
        requests.post(
            f"{_base_url()}/auth/signout",
            headers={"X-Tableau-Auth": token},
            timeout=10,
        )
    except requests.RequestException:
        pass  # 로그아웃 실패는 무시 (토큰은 자연 만료됨)


def _refresh_datasource(token: str, site_id: str, ds_id: str) -> str | None:
    """
    특정 데이터소스의 Extract 새로고침을 트리거합니다.

    Args:
        token: Tableau 인증 토큰
        site_id: 사이트 고유 ID
        ds_id: 새로고침할 데이터소스의 고유 ID

    Returns:
        job_id: 새로고침 작업의 ID (polling용). 없으면 None.
    """
    url = f"{_base_url()}/sites/{site_id}/datasources/{ds_id}/refresh"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] refresh datasource: {ds_id}")
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()

    # 응답에서 job ID 추출 (비동기 작업이므로 즉시 반환됨)
    job = response.json().get("job")
    return job.get("id") if isinstance(job, dict) else None


def _wait_for_job(token: str, site_id: str, job_id: str) -> None:
    """
    Tableau 작업(job)이 완료될 때까지 polling으로 대기합니다.

    - finishCode "0": 성공 → 리턴
    - finishCode ""이나 "-1": 아직 진행 중 → 대기 후 재시도
    - 그 외 finishCode: 실패 → RuntimeError 발생
    - timeout 초과: TimeoutError 발생

    Args:
        token: Tableau 인증 토큰
        site_id: 사이트 고유 ID
        job_id: 모니터링할 작업 ID
    """
    deadline = time.time() + TABLEAU_JOB_TIMEOUT_SECONDS  # 최대 대기 시한

    while time.time() < deadline:
        # 작업 상태 조회
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
            return  # 성공 완료
        if finish_code not in {"", "-1"}:
            raise RuntimeError(f"Tableau job failed: {job_id}, code={finish_code}")

        # 아직 진행 중 → 대기 후 재시도
        time.sleep(TABLEAU_JOB_POLL_SECONDS)

    # timeout 초과
    raise TimeoutError(f"Tableau job timeout: {job_id}")


def _run_flow(token: str, site_id: str, flow_id: str) -> str | None:
    """
    특정 Tableau Flow 실행을 트리거합니다.

    Args:
        token: Tableau 인증 토큰
        site_id: 사이트 고유 ID
        flow_id: 실행할 Flow의 LUID

    Returns:
        job_id: Flow 실행 작업의 ID (polling용). 없으면 None.
    """
    url = f"{_base_url()}/sites/{site_id}/flows/{flow_id}/run"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    print(f"[tableau] run flow: {flow_id}")
    response = requests.post(url, headers=headers, json={}, timeout=30)
    response.raise_for_status()

    job = response.json().get("job")
    return job.get("id") if isinstance(job, dict) else None


def run_all_flows(**_context: Any) -> None:
    """
    TABLEAU_FLOW_IDS에 등록된 모든 Flow를 순차적으로 실행합니다.

    흐름:
        1. PAT으로 로그인
        2. 각 Flow에 대해 run 요청 → job_id 획득
        3. TABLEAU_WAIT_FOR_JOB=true면 완료까지 polling 대기
        4. 로그아웃 (finally로 보장)
    """
    if not TABLEAU_FLOW_IDS:
        print("[tableau] No flow IDs configured, skipping.")
        return

    token, site_id = _sign_in()
    try:
        for flow_id in TABLEAU_FLOW_IDS:
            job_id = _run_flow(token, site_id, flow_id)
            if job_id and TABLEAU_WAIT_FOR_JOB:
                _wait_for_job(token, site_id, job_id)
        print(f"[tableau] All {len(TABLEAU_FLOW_IDS)} flows executed.")
    finally:
        _sign_out(token)


def refresh_all_datasources(**_context: Any) -> None:
    """
    TABLEAU_DATASOURCE_IDS에 등록된 모든 데이터소스를 순차적으로 새로고침합니다.

    흐름:
        1. PAT으로 로그인
        2. 각 데이터소스에 대해:
           a. refresh 요청 → job_id 획득
           b. TABLEAU_WAIT_FOR_JOB=true면 완료까지 polling 대기
        3. 로그아웃 (finally로 보장)

    Note:
        - TABLEAU_DATASOURCE_IDS가 비어있으면 로그만 남기고 건너뜁니다.
        - 순차 실행이므로 하나가 실패하면 이후 데이터소스는 새로고침되지 않습니다.
    """
    if not TABLEAU_DATASOURCE_IDS:
        print("[tableau] No datasource IDs configured, skipping.")
        return

    token, site_id = _sign_in()
    try:
        for ds_id in TABLEAU_DATASOURCE_IDS:
            job_id = _refresh_datasource(token, site_id, ds_id)
            if job_id and TABLEAU_WAIT_FOR_JOB:
                _wait_for_job(token, site_id, job_id)  # 완료 대기
        print(f"[tableau] All {len(TABLEAU_DATASOURCE_IDS)} datasources refreshed.")
    finally:
        _sign_out(token)  # 성공/실패 관계없이 반드시 로그아웃
