# =====================================================================
# GA4 Freshness Sensor (ga4_sensor.py)
# ---------------------------------------------------------------------
# * 목적: GA4 이벤트 데이터가 BigQuery에 도착했는지 확인합니다.
# * 배경: GA4 데이터는 보통 당일 새벽에 전날 데이터가 BigQuery에 적재됩니다.
#         하지만 지연(late arriving)이 발생할 수 있어, Dataform 실행 전에
#         어제 데이터가 실제로 존재하는지 확인이 필요합니다.
# * 동작:
#   - PythonSensor로 사용되어 True 반환 시 다음 태스크로 진행합니다.
#   - False 반환 시 poke_interval(5분) 후 재시도합니다.
#   - timeout(1시간) 초과 시 태스크가 실패합니다.
# =====================================================================
from __future__ import annotations

from typing import Any

from google.cloud import bigquery  # BigQuery Python 클라이언트

from config.settings import GA4_DATASET_ID, GA4_PROJECT_ID


def check_ga4_freshness(**_context: Any) -> bool:
    """
    어제 날짜의 GA4 events 테이블에 데이터가 존재하는지 확인합니다.

    쿼리 로직:
        - events_* 와일드카드 테이블에서 _TABLE_SUFFIX로 어제 날짜 필터링
        - KST(Asia/Seoul) 기준으로 날짜 계산
        - COUNT(*) > 0 이면 True (데이터 도착 확인)

    Returns:
        True: 어제 데이터 존재 → 센서 통과
        False: 어제 데이터 미도착 → poke_interval 후 재시도
    """
    client = bigquery.Client(project=GA4_PROJECT_ID)

    # _TABLE_SUFFIX: GA4 events 테이블은 날짜별로 events_YYYYMMDD 형태로 저장됨
    # 예: events_20260321 → _TABLE_SUFFIX = '20260321'
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM `{GA4_PROJECT_ID}.{GA4_DATASET_ID}.events_*`
    WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',
        DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    """

    result = list(client.query(query).result())
    cnt = result[0].cnt if result else 0
    print(f"[ga4_sensor] yesterday event count: {cnt}")
    return cnt > 0  # 0보다 크면 데이터 도착 확인
