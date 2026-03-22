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
