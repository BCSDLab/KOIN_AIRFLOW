# KOIN_AIRFLOW

KOIN 서비스의 데이터 파이프라인을 관리하기 위한 Apache Airflow 환경.

## Architecture

- **Apache Airflow 2.10.5** (Docker Compose, LocalExecutor)
- **Metadata DB**: PostgreSQL 15
- **ETL**: Dataform API → BigQuery
- **Visualization**: Tableau Cloud (REST API, PAT 인증)
- **Notification**: Slack Webhook

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `koin_daily_pipeline` | 매일 KST 06:00 | GA4 Sensor → Dataform → Assertion → Tableau Refresh → Slack |
| `cost_alert_pipeline` | 매일 KST 22:00 | GCP Billing 비용 조회 → Slack 알림 |

## Project Structure

```
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── dags/
│   ├── config/settings.py
│   ├── pipelines/
│   │   ├── koin_daily_pipeline.py
│   │   └── cost_alert_pipeline.py
│   └── tasks/
│       ├── dataform_api.py
│       ├── ga4_sensor.py
│       ├── assertion_check.py
│       ├── tableau_api.py
│       └── slack_notify.py
├── plugins/
├── keys/           (gitignored)
└── logs/           (gitignored)
```

## Quick Start

```bash
# 1. 환경변수 설정
cp .env.example .env
# .env 파일에 실제 값 입력

# 2. GCP 서비스 계정 키 배치
mkdir -p keys
# keys/gcp-key.json 배치

# 3. 실행
docker-compose up --build -d

# 4. 접속
# http://localhost:8080 (admin / .env에 설정한 비밀번호)
```

## Environment Variables

See `.env.example` for full list. Key variables:

- `DATAFORM_*` — Dataform API 설정
- `TABLEAU_*` — Tableau Cloud PAT 인증 및 datasource IDs
- `SLACK_WEBHOOK_URL` — Slack 알림
- `BILLING_*` — GCP 비용 알림
