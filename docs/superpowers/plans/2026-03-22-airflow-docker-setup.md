# Airflow Docker Setup + Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** test_yun 기반으로 개선된 Airflow 환경(Docker Compose + LocalExecutor)과 Dataform→Assertion→Tableau 통합 파이프라인을 구축한다.

**Architecture:** LocalExecutor + Postgres 기반 Airflow. 메인 DAG(koin_daily_pipeline)이 GA4 freshness 확인 → Dataform API 트리거 → Assertion 검증 → Tableau 다중 datasource 새로고침 → Slack 알림을 순차 실행. 기존 cost_alert_pipeline은 그대로 계승.

**Tech Stack:** Apache Airflow 2.10.5, Docker Compose, PostgreSQL 15, Python 3.11, google-auth, requests

---

## File Structure

```
KOIN_AIRFLOW/
├── docker-compose.yml              # Airflow 환경 (LocalExecutor + Postgres)
├── Dockerfile                      # 커스텀 이미지 (추가 의존성)
├── requirements.txt                # Python 의존성
├── .env.example                    # 환경변수 템플릿
├── .gitignore                      # logs, postgres-data, .env 등
├── dags/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py             # 중앙 설정 (Dataform/Tableau/Slack/Billing)
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── koin_daily_pipeline.py  # 메인 DAG (sensor→dataform→assertion→tableau→slack)
│   │   └── cost_alert_pipeline.py  # GCP 비용 알림 DAG (test_yun 계승)
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── dataform_api.py         # Dataform REST API 호출
│   │   ├── tableau_api.py          # Tableau REST API 호출
│   │   ├── assertion_check.py      # Dataform assertion 검증 (신규)
│   │   ├── ga4_sensor.py           # GA4 데이터 도착 확인 (신규)
│   │   └── slack_notify.py         # Slack webhook 알림
│   └── __init__.py
├── plugins/
│   └── (비움 - 커스텀 플러그인 필요 시 추가)
└── README.md
```

---

### Task 1: Docker 환경 구성 (Dockerfile + docker-compose.yml + requirements.txt)

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `requirements.txt`
- Create: `.env.example`
- Modify: `.gitignore`

- [ ] **Step 1: requirements.txt 작성**

```txt
google-auth>=2.28.0
google-auth-httplib2>=0.2.0
requests>=2.31.0
apache-airflow-providers-google>=10.15.0
```

- [ ] **Step 2: Dockerfile 작성**

```dockerfile
FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

- [ ] **Step 3: docker-compose.yml 작성**

```yaml
version: "3.8"

x-airflow-common: &airflow-common
  build: .
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/gcp-key.json
  env_file:
    - .env
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./keys:/opt/airflow/keys:ro
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - ./postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      retries: 5

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: >
      -c "airflow db migrate &&
          airflow users create
            --username $${AIRFLOW_ADMIN_USER}
            --password $${AIRFLOW_ADMIN_PASSWORD}
            --firstname Admin
            --lastname User
            --role Admin
            --email $${AIRFLOW_ADMIN_EMAIL}"
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always
```

- [ ] **Step 4: .env.example 작성**

```env
# Airflow Admin
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=change_me
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Dataform
DATAFORM_PROJECT_ID=kap-chat
DATAFORM_LOCATION=asia-northeast3
DATAFORM_REPOSITORY_ID=koin-repository
DATAFORM_WORKFLOW_CONFIG=daily_stg_ga4_production

# Slack
SLACK_WEBHOOK_URL=
SLACK_USERNAME=airflow-koin

# Billing
BILLING_EXPORT_PROJECT_ID=kap-chat
BILLING_EXPORT_DATASET=gcp_billing
BILLING_EXPORT_TABLE_PREFIX=gcp_billing_export_resource_v1_

# Tableau
TABLEAU_SERVER_URL=https://prod-apnortheast-a.online.tableau.com
TABLEAU_SITE_CONTENT_URL=bcsd
TABLEAU_PAT_NAME=
TABLEAU_PAT_SECRET=
TABLEAU_API_VERSION=3.24
TABLEAU_DATASOURCE_IDS=
TABLEAU_WAIT_FOR_JOB=true
TABLEAU_JOB_POLL_SECONDS=20
TABLEAU_JOB_TIMEOUT_SECONDS=3600
```

- [ ] **Step 5: .gitignore 업데이트**

```gitignore
venv
.env
logs/
postgres-data/
keys/
__pycache__/
*.pyc
airflow-src.tgz
```

- [ ] **Step 6: Commit**

```bash
git add Dockerfile docker-compose.yml requirements.txt .env.example .gitignore
git commit -m "feat: Docker Compose 환경 구성 (LocalExecutor + Postgres)"
```

---

### Task 2: 설정 및 공통 모듈 (config + slack_notify)

**Files:**
- Create: `dags/__init__.py`
- Create: `dags/config/__init__.py`
- Create: `dags/config/settings.py`
- Create: `dags/tasks/__init__.py`
- Create: `dags/tasks/slack_notify.py`
- Create: `dags/pipelines/__init__.py`

- [ ] **Step 1: __init__.py 파일들 생성**

`dags/__init__.py`, `dags/config/__init__.py`, `dags/tasks/__init__.py`, `dags/pipelines/__init__.py` — 모두 빈 파일

- [ ] **Step 2: dags/config/settings.py 작성**

test_yun 기반 + 개선:
- TABLEAU_DATASOURCE_IDS (콤마 구분 다중 지원)
- GA4 sensor 관련 설정 추가

```python
from __future__ import annotations

import os

# Dataform
DATAFORM_PROJECT_ID = os.getenv("DATAFORM_PROJECT_ID", "kap-chat")
DATAFORM_LOCATION = os.getenv("DATAFORM_LOCATION", "asia-northeast3")
DATAFORM_REPOSITORY_ID = os.getenv("DATAFORM_REPOSITORY_ID", "koin-repository")
DATAFORM_WORKFLOW_CONFIG = os.getenv("DATAFORM_WORKFLOW_CONFIG", "daily_stg_ga4_production")

# GA4 Source
GA4_PROJECT_ID = os.getenv("GA4_PROJECT_ID", "kap-chat")
GA4_DATASET_ID = os.getenv("GA4_DATASET_ID", "analytics_432041405")

# General
DEFAULT_TIMEZONE = "Asia/Seoul"
DEFAULT_OWNER = "airflow"
GCP_CONN_ID = "google_cloud_default"

# Tableau
TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL", "")
TABLEAU_SITE_CONTENT_URL = os.getenv("TABLEAU_SITE_CONTENT_URL", "")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME", "")
TABLEAU_PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET", "")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.24")
TABLEAU_DATASOURCE_IDS = [
    ds.strip()
    for ds in os.getenv("TABLEAU_DATASOURCE_IDS", "").split(",")
    if ds.strip()
]
TABLEAU_WAIT_FOR_JOB = os.getenv("TABLEAU_WAIT_FOR_JOB", "true").lower() in {"1", "true", "yes"}
TABLEAU_JOB_POLL_SECONDS = int(os.getenv("TABLEAU_JOB_POLL_SECONDS", "20"))
TABLEAU_JOB_TIMEOUT_SECONDS = int(os.getenv("TABLEAU_JOB_TIMEOUT_SECONDS", "3600"))

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow-koin")

# Billing
BILLING_EXPORT_PROJECT_ID = os.getenv("BILLING_EXPORT_PROJECT_ID", DATAFORM_PROJECT_ID)
BILLING_EXPORT_DATASET = os.getenv("BILLING_EXPORT_DATASET", "gcp_billing")
BILLING_EXPORT_TABLE_PREFIX = os.getenv("BILLING_EXPORT_TABLE_PREFIX", "gcp_billing_export_resource_v1_")
BILLING_TIMEZONE = os.getenv("BILLING_TIMEZONE", DEFAULT_TIMEZONE)
```

- [ ] **Step 3: dags/tasks/slack_notify.py 작성**

test_yun 계승 + on_failure_callback 헬퍼 추가

```python
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
```

- [ ] **Step 4: Commit**

```bash
git add dags/
git commit -m "feat: config/settings.py + slack_notify 모듈 구성"
```

---

### Task 3: Dataform API 모듈

**Files:**
- Create: `dags/tasks/dataform_api.py`

- [ ] **Step 1: dataform_api.py 작성**

test_yun 코드 계승 (이미 잘 구현됨)

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add dags/tasks/dataform_api.py
git commit -m "feat: Dataform API 모듈 (create + wait invocation)"
```

---

### Task 4: GA4 Freshness Sensor (신규)

**Files:**
- Create: `dags/tasks/ga4_sensor.py`

- [ ] **Step 1: ga4_sensor.py 작성**

```python
from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from config.settings import GA4_DATASET_ID, GA4_PROJECT_ID


def check_ga4_freshness(**_context: Any) -> bool:
    """어제 날짜의 GA4 events 테이블이 존재하고 데이터가 있는지 확인."""
    client = bigquery.Client(project=GA4_PROJECT_ID)
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM `{GA4_PROJECT_ID}.{GA4_DATASET_ID}.events_*`
    WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',
        DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    """
    result = list(client.query(query).result())
    cnt = result[0].cnt if result else 0
    print(f"[ga4_sensor] yesterday event count: {cnt}")
    return cnt > 0
```

- [ ] **Step 2: Commit**

```bash
git add dags/tasks/ga4_sensor.py
git commit -m "feat: GA4 freshness sensor 추가"
```

---

### Task 5: Assertion 검증 모듈 (신규)

**Files:**
- Create: `dags/tasks/assertion_check.py`

- [ ] **Step 1: assertion_check.py 작성**

```python
from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from config.settings import DATAFORM_PROJECT_ID

ASSERTIONS = [
    "dataform_assertions.chk_incremental_stg_events_final",
    "dataform_assertions.chk_incremental_core_events_final",
    "dataform_assertions.chk_vw_mart_integrated_logs",
]


def check_assertions(**_context: Any) -> None:
    """Dataform assertion 테이블을 쿼리하여 실패 행이 있으면 예외 발생."""
    client = bigquery.Client(project=DATAFORM_PROJECT_ID)
    failures = []

    for assertion in ASSERTIONS:
        table_ref = f"{DATAFORM_PROJECT_ID}.{assertion}"
        query = f"SELECT COUNT(*) AS fail_cnt FROM `{table_ref}`"
        result = list(client.query(query).result())
        fail_cnt = result[0].fail_cnt if result else 0
        print(f"[assertion] {assertion}: {fail_cnt} failures")
        if fail_cnt > 0:
            failures.append(f"{assertion} ({fail_cnt} rows)")

    if failures:
        raise RuntimeError(
            f"Assertion failures detected: {', '.join(failures)}"
        )
    print("[assertion] All assertions passed.")
```

- [ ] **Step 2: Commit**

```bash
git add dags/tasks/assertion_check.py
git commit -m "feat: Dataform assertion 검증 모듈 추가"
```

---

### Task 6: Tableau API 모듈 (개선)

**Files:**
- Create: `dags/tasks/tableau_api.py`

- [ ] **Step 1: tableau_api.py 작성**

test_yun 기반 + 개선: 다중 datasource 순차 새로고침 지원

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add dags/tasks/tableau_api.py
git commit -m "feat: Tableau API 모듈 (다중 datasource 순차 새로고침)"
```

---

### Task 7: 메인 DAG (koin_daily_pipeline)

**Files:**
- Create: `dags/pipelines/koin_daily_pipeline.py`

- [ ] **Step 1: koin_daily_pipeline.py 작성**

```python
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE
from tasks.dataform_api import create_workflow_invocation, wait_for_workflow_invocation
from tasks.ga4_sensor import check_ga4_freshness
from tasks.assertion_check import check_assertions
from tasks.tableau_api import refresh_all_datasources
from tasks.slack_notify import on_failure_slack, send_slack_message

local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,
}

with DAG(
    dag_id="koin_daily_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 22, tz=local_tz),
    schedule="0 6 * * *",  # KST 06:00
    catchup=False,
    tags=["koin", "dataform", "tableau", "daily"],
) as dag:

    ga4_check = PythonSensor(
        task_id="ga4_freshness_check",
        python_callable=check_ga4_freshness,
        poke_interval=300,       # 5분마다 체크
        timeout=3600,            # 최대 1시간 대기
        mode="reschedule",
    )

    dataform_run = PythonOperator(
        task_id="dataform_run",
        python_callable=create_workflow_invocation,
    )

    dataform_wait = PythonSensor(
        task_id="dataform_wait",
        python_callable=wait_for_workflow_invocation,
        poke_interval=60,
        timeout=60 * 60 * 3,    # 최대 3시간
        mode="reschedule",
    )

    assertion_check = PythonOperator(
        task_id="assertion_check",
        python_callable=check_assertions,
    )

    tableau_refresh = PythonOperator(
        task_id="tableau_refresh",
        python_callable=refresh_all_datasources,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_slack_message,
        op_kwargs={
            "message": (
                ":white_check_mark: *[KOIN] Daily Pipeline 완료*\n"
                "- GA4 Sensor: OK\n"
                "- Dataform: SUCCEEDED\n"
                "- Assertions: PASSED\n"
                "- Tableau: Refreshed"
            )
        },
    )

    ga4_check >> dataform_run >> dataform_wait >> assertion_check >> tableau_refresh >> notify_success
```

- [ ] **Step 2: Commit**

```bash
git add dags/pipelines/koin_daily_pipeline.py
git commit -m "feat: koin_daily_pipeline DAG (sensor→dataform→assertion→tableau→slack)"
```

---

### Task 8: Cost Alert Pipeline (test_yun 계승)

**Files:**
- Create: `dags/pipelines/cost_alert_pipeline.py`

- [ ] **Step 1: cost_alert_pipeline.py 작성**

test_yun 코드 그대로 계승 (이미 완성도 높음). settings.py 임포트 경로만 맞춤.

- [ ] **Step 2: Commit**

```bash
git add dags/pipelines/cost_alert_pipeline.py
git commit -m "feat: cost_alert_pipeline DAG 계승 (BQ billing → Slack)"
```

---

### Task 9: 기존 연습용 파일 정리 + README 업데이트

**Files:**
- Delete: `dags/dags_bash_select_fruit.py` 외 연습용 DAG 전체
- Delete: `plugins/common/common_func.py`, `plugins/shell/selelct_fruit.sh`
- Delete: `KOIN_AIRFLOW` (서브모듈 또는 더미 파일)
- Modify: `README.md`

- [ ] **Step 1: 연습용 파일 삭제**

```bash
rm -f dags/dags_*.py
rm -rf plugins/common plugins/shell
rm -f KOIN_AIRFLOW
```

- [ ] **Step 2: README.md 업데이트**

프로젝트 구조, 실행 방법, DAG 설명 반영

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: 연습용 파일 제거 + README 업데이트"
```

---

### Task 10: Docker 실행 테스트

- [ ] **Step 1: .env 파일 생성 (.env.example 복사 후 값 채우기)**

```bash
cp .env.example .env
# .env에 실제 값 입력
```

- [ ] **Step 2: keys 디렉토리에 GCP 서비스 계정 키 배치**

```bash
mkdir -p keys
# gcp-key.json 배치
```

- [ ] **Step 3: Docker Compose 빌드 및 실행**

```bash
docker-compose up --build -d
```

Expected: postgres, airflow-init, airflow-webserver, airflow-scheduler 컨테이너 정상 기동

- [ ] **Step 4: Airflow UI 접속 확인**

http://localhost:8080 접속 → admin/change_me 로그인 → koin_daily_pipeline DAG 확인

- [ ] **Step 5: DAG 수동 트리거 테스트**

Airflow UI에서 koin_daily_pipeline 수동 실행 → 각 태스크 로그 확인
