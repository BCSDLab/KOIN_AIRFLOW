# =====================================================================
# 메인 DAG: KOIN Daily Pipeline (koin_daily_pipeline.py)
# ---------------------------------------------------------------------
# * 목적: KOIN 서비스의 전체 데이터 파이프라인을 일 단위로 자동 실행합니다.
# * 스케줄: 매일 KST 06:00 (cron: "0 6 * * *")
# * 파이프라인 흐름:
#
#   ga4_freshness_check → dataform_run → dataform_wait → assertion_check → tableau_refresh → notify_success
#
#   1. GA4 Freshness Sensor  : BigQuery에 어제 GA4 데이터가 도착했는지 확인
#   2. Dataform Run          : Dataform API로 ETL 파이프라인 실행 트리거
#   3. Dataform Wait         : Dataform 실행 완료까지 폴링 대기
#   4. Assertion Check       : Dataform assertion으로 데이터 품질 검증
#   5. Tableau Refresh       : Tableau Cloud 데이터소스 새로고침
#   6. Notify Success        : Slack으로 성공 알림 전송
#
# * 실패 처리: 어떤 태스크든 실패하면 on_failure_slack 콜백이
#              자동으로 Slack에 실패 알림을 보냅니다.
# =====================================================================
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

# --- 설정 ---
from config.settings import DEFAULT_OWNER, DEFAULT_TIMEZONE

# --- 태스크 함수 임포트 ---
from tasks.dataform_api import create_workflow_invocation, wait_for_workflow_invocation
from tasks.ga4_sensor import check_ga4_freshness
from tasks.assertion_check import check_assertions
from tasks.tableau_api import refresh_all_datasources
from tasks.slack_notify import on_failure_slack, send_slack_message

# KST 타임존 객체 생성
local_tz = pendulum.timezone(DEFAULT_TIMEZONE)

# ---------------------------------------------------------------------------
# DAG 기본 인자
# - 모든 태스크에 공통 적용됩니다.
# - retries: 실패 시 2회까지 자동 재시도 (5분 간격)
# - on_failure_callback: 태스크 실패 시 Slack 알림 자동 발송
# ---------------------------------------------------------------------------
default_args = {
    "owner": DEFAULT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_slack,  # 실패 시 Slack 알림
}

# ---------------------------------------------------------------------------
# DAG 정의
# - catchup=False: 과거 미실행 스케줄을 소급 실행하지 않음
# ---------------------------------------------------------------------------
with DAG(
    dag_id="koin_daily_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 22, tz=local_tz),  # DAG 시작 날짜
    schedule="0 6 * * *",  # 매일 KST 06:00 실행
    catchup=False,
    tags=["koin", "dataform", "tableau", "daily"],
) as dag:

    # =========================================================================
    # Step 1: GA4 데이터 도착 확인 (Sensor)
    # - 어제 날짜의 GA4 events 테이블에 데이터가 있는지 확인합니다.
    # - 5분(300초)마다 체크하고, 최대 1시간(3600초) 대기합니다.
    # - mode="reschedule": 대기 중 워커 슬롯을 점유하지 않음 (리소스 효율적)
    # =========================================================================
    ga4_check = PythonSensor(
        task_id="ga4_freshness_check",
        python_callable=check_ga4_freshness,
        poke_interval=300,       # 5분마다 체크
        timeout=3600,            # 최대 1시간 대기
        mode="reschedule",       # 대기 중 워커 슬롯 반환
    )

    # =========================================================================
    # Step 2: Dataform 파이프라인 실행 트리거
    # - Dataform REST API를 호출하여 워크플로우 실행을 생성합니다.
    # - 생성된 invocation 이름을 XCom에 저장합니다.
    # =========================================================================
    dataform_run = PythonOperator(
        task_id="dataform_run",
        python_callable=create_workflow_invocation,
    )

    # =========================================================================
    # Step 3: Dataform 실행 완료 대기 (Sensor)
    # - 1분(60초)마다 실행 상태를 폴링합니다.
    # - SUCCEEDED → 통과, FAILED/CANCELLED → 예외 발생
    # - 최대 3시간 대기 (대규모 전체 로드 시 시간이 오래 걸릴 수 있음)
    # =========================================================================
    dataform_wait = PythonSensor(
        task_id="dataform_wait",
        python_callable=wait_for_workflow_invocation,
        poke_interval=60,         # 1분마다 상태 확인
        timeout=60 * 60 * 3,      # 최대 3시간 대기
        mode="reschedule",
    )

    # =========================================================================
    # Step 4: 데이터 품질 검증 (Assertion)
    # - Dataform assertion 테이블(3개)을 쿼리하여 실패 행이 있는지 확인합니다.
    # - 실패 시 Tableau 새로고침을 차단하여 잘못된 데이터 반영을 방지합니다.
    # =========================================================================
    assertion_check = PythonOperator(
        task_id="assertion_check",
        python_callable=check_assertions,
    )

    # =========================================================================
    # Step 5: Tableau 데이터소스 새로고침
    # - TABLEAU_DATASOURCE_IDS에 등록된 모든 데이터소스를 순차 새로고침합니다.
    # - PAT 인증 → refresh 트리거 → 완료 대기 → 로그아웃
    # =========================================================================
    tableau_refresh = PythonOperator(
        task_id="tableau_refresh",
        python_callable=refresh_all_datasources,
    )

    # =========================================================================
    # Step 6: 성공 알림
    # - 모든 단계가 성공적으로 완료되었음을 Slack으로 알립니다.
    # =========================================================================
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

    # =========================================================================
    # 태스크 의존성 (실행 순서)
    # GA4 확인 → Dataform 트리거 → 완료 대기 → 품질 검증 → Tableau → 알림
    # =========================================================================
    ga4_check >> dataform_run >> dataform_wait >> assertion_check >> tableau_refresh >> notify_success
