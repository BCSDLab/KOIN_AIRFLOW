# =====================================================================
# Dataform Assertion 검증 모듈 (assertion_check.py)
# ---------------------------------------------------------------------
# * 목적: Dataform 파이프라인 실행 후 데이터 품질을 검증합니다.
# * 배경: Dataform은 assertion(어설션)이라는 데이터 품질 테스트를 지원합니다.
#         assertion 쿼리가 0행을 반환하면 [성공], 1행 이상이면 [실패]입니다.
#         실패한 행은 dataform_assertions 데이터셋의 테이블에 저장됩니다.
# * 동작:
#   - 3개의 assertion 테이블을 순회하며 실패 행 수를 확인합니다.
#   - 하나라도 실패 행이 있으면 RuntimeError 발생 → DAG 중단
#   - 모든 assertion 통과 시 다음 태스크(Tableau 새로고침)로 진행합니다.
# * 중요: assertion 실패 시 Tableau 새로고침을 차단하여
#         잘못된 데이터가 대시보드에 반영되는 것을 방지합니다.
# =====================================================================
from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from config.settings import DATAFORM_PROJECT_ID

# 검증 대상 assertion 테이블 목록
# - 각 테이블은 Dataform의 assertion 쿼리 결과(실패 행)를 저장합니다.
# - 테이블이 비어있으면(0행) 해당 검증이 통과된 것입니다.
ASSERTIONS = [
    # STG 최종 테이블 검증: PK(event_dt, user_pseudo_id) 누락 확인
    "dataform_assertions.chk_incremental_stg_events_final",

    # CORE 최종 테이블 검증: PK + service_name 매핑 + Android 세션 보정 확인
    "dataform_assertions.chk_incremental_core_events_final",

    # MART 통합 뷰 검증: PK + 유저 프로필 중복 + 학번 추출 정합성 확인
    "dataform_assertions.chk_vw_mart_integrated_logs",
]


def check_assertions(**_context: Any) -> None:
    """
    모든 Dataform assertion 테이블을 쿼리하여 데이터 품질을 검증합니다.

    각 assertion 테이블에 대해:
        SELECT COUNT(*) AS fail_cnt FROM `{테이블}`
    을 실행하고, fail_cnt > 0이면 해당 assertion이 실패한 것입니다.

    Raises:
        RuntimeError: 하나 이상의 assertion에서 실패 행이 발견된 경우.
                      실패한 assertion 이름과 행 수가 메시지에 포함됩니다.
    """
    client = bigquery.Client(project=DATAFORM_PROJECT_ID)
    failures = []  # 실패한 assertion 목록

    for assertion in ASSERTIONS:
        # assertion 테이블의 실패 행 수를 카운트
        table_ref = f"{DATAFORM_PROJECT_ID}.{assertion}"
        query = f"SELECT COUNT(*) AS fail_cnt FROM `{table_ref}`"
        result = list(client.query(query).result())
        fail_cnt = result[0].fail_cnt if result else 0

        print(f"[assertion] {assertion}: {fail_cnt} failures")

        if fail_cnt > 0:
            failures.append(f"{assertion} ({fail_cnt} rows)")

    # 실패한 assertion이 하나라도 있으면 DAG 중단
    if failures:
        raise RuntimeError(
            f"Assertion failures detected: {', '.join(failures)}"
        )

    print("[assertion] All assertions passed.")
