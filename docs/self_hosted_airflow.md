# Self-hosted Airflow (VM + Docker)

이 문서는 GCP VM 위에 Docker로 Airflow를 구동하는 기준 절차를 정리합니다.
목표는 "하루 1회 실행"을 위해 VM을 필요한 시간에만 켜는 운영 모델입니다.

## 1) VM 생성 (예시)
- 리전: `asia-northeast3`
- 머신 타입: `e2-small` 또는 `e2-medium`
- 디스크: 30GB (표준)
- 방화벽: TCP `8080` (Airflow UI)

## 2) VM에서 Docker 설치
1. Docker 설치
2. Docker Compose 설치
3. 리포지토리 클론 후 `docker-compose.yml` 확인

## 3) 환경변수 설정
1. `.env.example`를 복사해 `.env`로 생성
2. `AIRFLOW_ADMIN_*` 계정 정보 입력
3. Slack/Tableau/Dataform/비용 알림 관련 환경변수 입력

## 4) Airflow 초기화 및 실행
```bash
docker compose up airflow-init
docker compose up -d
```

## 5) VM 자동 시작/종료 (하루 1회)
1. Cloud Scheduler로 인스턴스 시작/종료 예약
2. 실행 시간은 DAG 소요 시간에 맞춰 설정
3. 종료 전에 DAG가 완료되는지 확인

## 6) 기본 확인
- `http://<VM_EXTERNAL_IP>:8080` 접속
- `dataform_tableau_pipeline`와 `cost_alert_pipeline` DAG 확인

## 7) 운영 팁
- 로그/DB는 VM 디스크에 저장됨
- 주기적으로 디스크 용량 확인
- 버전 업그레이드는 도커 이미지 태그 변경으로 관리
