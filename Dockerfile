# =====================================================================
# KOIN Airflow 커스텀 Docker 이미지
# ---------------------------------------------------------------------
# * 베이스: Apache Airflow 2.10.5 (Python 3.11)
# * 목적: 기본 Airflow 이미지에 GCP/Tableau 연동에 필요한
#          추가 Python 패키지를 설치한 커스텀 이미지를 생성합니다.
# * 빌드: docker-compose up --build 시 자동으로 빌드됩니다.
# =====================================================================

# Airflow 공식 이미지를 베이스로 사용 (Python 3.11 버전)
FROM apache/airflow:2.10.5-python3.11

# requirements.txt를 컨테이너 내부 임시 경로로 복사
COPY requirements.txt /tmp/requirements.txt

# 추가 패키지 설치 (--no-cache-dir: 캐시 미사용으로 이미지 크기 최소화)
# - google-auth: GCP 서비스 계정 인증 (Dataform API 호출용)
# - requests: HTTP 클라이언트 (Dataform/Tableau REST API 호출용)
# - apache-airflow-providers-google: BigQuery Hook 등 GCP 연동 오퍼레이터
RUN pip install --no-cache-dir -r /tmp/requirements.txt
