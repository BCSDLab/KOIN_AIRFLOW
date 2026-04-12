# =====================================================================
# Dataform API 모듈 (dataform_api.py)
# ---------------------------------------------------------------------
# * 목적: Dataform REST API를 호출하여 데이터 파이프라인을 트리거합니다.
# * 흐름:
#   1. create_workflow_invocation() → Dataform 워크플로우 실행을 생성(트리거)
#   2. wait_for_workflow_invocation() → 실행 완료될 때까지 상태를 폴링
# * 인증: GCP 서비스 계정 키(GOOGLE_APPLICATION_CREDENTIALS)를 사용하여
#         OAuth2 토큰을 자동으로 발급받습니다.
# * API: https://cloud.google.com/dataform/docs/reference/rest
# =====================================================================
from __future__ import annotations

from typing import Any

import google.auth                          # GCP 인증 라이브러리
from google.auth.transport.requests import Request  # 토큰 갱신용
import requests                              # HTTP 클라이언트

from config.settings import (
    DATAFORM_LOCATION,
    DATAFORM_PROJECT_ID,
    DATAFORM_REPOSITORY_ID,
    DATAFORM_WORKFLOW_CONFIG,
)

# Dataform REST API 베이스 URL (v1beta1)
DATAFORM_BASE_URL = "https://dataform.googleapis.com/v1beta1"


def _get_access_token() -> str:
    """
    GCP 서비스 계정에서 OAuth2 액세스 토큰을 발급받습니다.

    - GOOGLE_APPLICATION_CREDENTIALS 환경변수에 지정된 키 파일을 사용합니다.
    - 토큰은 호출 시마다 갱신되어 만료 걱정이 없습니다.
    """
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())  # 토큰 갱신
    return credentials.token


def _headers() -> dict[str, str]:
    """Dataform API 호출에 필요한 HTTP 헤더를 생성합니다."""
    return {
        "Authorization": f"Bearer {_get_access_token()}",
        "Content-Type": "application/json",
    }


def _repo_path() -> str:
    """
    Dataform 저장소의 리소스 경로를 반환합니다.

    형식: projects/{project}/locations/{location}/repositories/{repo}
    예시: projects/kap-chat/locations/asia-northeast3/repositories/koin-repository
    """
    return (
        f"projects/{DATAFORM_PROJECT_ID}/locations/{DATAFORM_LOCATION}"
        f"/repositories/{DATAFORM_REPOSITORY_ID}"
    )


def create_workflow_invocation(**context: Any) -> str:
    """
    Dataform 워크플로우 실행(Invocation)을 생성합니다.

    - POST /workflowInvocations API를 호출하여 파이프라인 실행을 트리거합니다.
    - 생성된 invocation의 이름(리소스 경로)을 XCom에 저장합니다.
      → 후속 태스크(wait_for_workflow_invocation)에서 XCom으로 꺼내 상태를 확인합니다.

    Args:
        **context: Airflow PythonOperator가 전달하는 컨텍스트.
                   context["ti"]로 XCom 읽기/쓰기가 가능합니다.

    Returns:
        invocation_name: 생성된 워크플로우 실행의 리소스 이름
    """
    # API 엔드포인트: {repo_path}/workflowInvocations
    url = f"{DATAFORM_BASE_URL}/{_repo_path()}/workflowInvocations"

    # 요청 본문: 어떤 워크플로우 설정(workflowConfig)을 실행할지 지정
    payload = {
        "workflowConfig": f"{_repo_path()}/workflowConfigs/{DATAFORM_WORKFLOW_CONFIG}"
    }

    print(f"[dataform] POST {url}")
    response = requests.post(url, headers=_headers(), json=payload, timeout=30)
    response.raise_for_status()  # 4xx/5xx 응답 시 예외 발생

    # 응답에서 invocation 이름 추출
    invocation_name = response.json().get("name")
    if not invocation_name:
        raise RuntimeError("Dataform invocation name not returned.")

    print(f"[dataform] invocation: {invocation_name}")

    # XCom에 invocation 이름 저장 (다음 태스크에서 참조)
    context["ti"].xcom_push(key="dataform_invocation_name", value=invocation_name)
    return invocation_name


def wait_for_workflow_invocation(**context: Any) -> bool:
    """
    Dataform 워크플로우 실행 상태를 확인합니다.

    - PythonSensor의 python_callable로 사용됩니다.
    - True 반환 시 센서 통과 (다음 태스크로 진행)
    - False 반환 시 poke_interval 후 재시도
    - 실패/취소 시 RuntimeError 발생 → 태스크 실패 처리

    상태값:
        - RUNNING: 아직 실행 중 → False (재시도)
        - SUCCEEDED: 성공 → True (통과)
        - FAILED/CANCELLED: 실패 → 예외 발생

    Args:
        **context: Airflow 컨텍스트. XCom에서 invocation_name을 가져옵니다.
    """
    # XCom에서 이전 태스크(dataform_run)가 저장한 invocation 이름을 가져옴
    invocation_name = context["ti"].xcom_pull(
        key="dataform_invocation_name", task_ids="dataform_run"
    )
    if not invocation_name:
        raise RuntimeError("No Dataform invocation name in XCom.")

    # GET API로 실행 상태 조회
    url = f"{DATAFORM_BASE_URL}/{invocation_name}"
    response = requests.get(url, headers=_headers(), timeout=30)
    response.raise_for_status()
    state = response.json().get("state", "STATE_UNSPECIFIED")
    print(f"[dataform] state: {state}")

    if state == "SUCCEEDED":
        return True  # 센서 통과 → 다음 태스크로 진행
    if state in {"FAILED", "CANCELLED"}:
        raise RuntimeError(f"Dataform workflow {state}")  # 태스크 실패 처리
    return False  # 아직 실행 중 → poke_interval 후 재시도
