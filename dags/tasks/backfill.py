# =====================================================================
# 백필 모듈 (backfill.py)
# ---------------------------------------------------------------------
# * 목적: Dataform incremental 테이블을 전체 재빌드(fullRefresh)합니다.
# * 배경: GA4에 신규 로깅이 추가되었으나 ETL에 바로 반영하지 못한 경우,
#         SQLX를 수정한 뒤 이 모듈로 전체 재빌드하여 과거 데이터를 백필합니다.
# * 동작:
#   - Dataform API의 fullyRefreshIncrementalTablesEnabled=True 옵션으로
#     모든 incremental 테이블을 처음부터 재빌드합니다.
#   - 기존 2일 lookback 대신 시작일(2024-07-01)부터 전체 재처리됩니다.
# * 주의: 전체 재빌드는 비용이 높으므로 필요할 때만 수동 실행하세요.
# =====================================================================
from __future__ import annotations

from typing import Any

import requests

from config.settings import DATAFORM_WORKFLOW_CONFIG
from tasks.dataform_api import DATAFORM_BASE_URL, _headers, _repo_path


def create_backfill_invocation(**context: Any) -> str:
    """
    Dataform fullRefresh 워크플로우 실행을 생성합니다.

    - 기존 workflowConfig를 기반으로 하되,
      fullyRefreshIncrementalTablesEnabled=True로 오버라이드합니다.
    - 모든 incremental 테이블이 처음부터 재빌드됩니다.
    - 생성된 invocation 이름을 XCom에 저장합니다.

    Returns:
        invocation_name: 생성된 워크플로우 실행의 리소스 이름
    """
    url = f"{DATAFORM_BASE_URL}/{_repo_path()}/workflowInvocations"
    payload = {
        "workflowConfig": f"{_repo_path()}/workflowConfigs/{DATAFORM_WORKFLOW_CONFIG}",
        "invocationConfig": {
            "fullyRefreshIncrementalTablesEnabled": True,
        },
    }

    print(f"[backfill] POST {url} (fullRefresh=True)")
    response = requests.post(url, headers=_headers(), json=payload, timeout=30)
    response.raise_for_status()

    invocation_name = response.json().get("name")
    if not invocation_name:
        raise RuntimeError("Dataform invocation name not returned.")

    print(f"[backfill] invocation: {invocation_name}")
    context["ti"].xcom_push(key="dataform_invocation_name", value=invocation_name)
    return invocation_name
