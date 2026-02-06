# Airflow 구축 히스토리

## 2026-02-06
- GCP 프로젝트 `kap-chat` 설정
- API 활성화: Composer, Dataform, Secret Manager
- 서비스 계정 `airflow-prod-sa` 생성
- 권한 부여: `composer.worker`, `dataform.admin`, `bigquery.jobUser`, `bigquery.dataEditor`, `secretmanager.secretAccessor`
- Cloud Composer 환경 `airflow-prod` 생성 시작 (asia-northeast3, small)

- README에 Composer 배포 흐름 섹션 추가
- README.md 전체 재작성 (Composer 기반 설계/구조/운영 원칙 반영)
- DAG 스켈레톤 생성: dags/pipelines/dataform_tableau_pipeline.py
- 설정 파일 추가: dags/config/settings.py
- Dataform API 호출 설계 문서 추가: docs/dataform_api_design.md
- 비용 알림 파이프라인 계획서 추가: docs/cost_alert_plan.md
- Composer 환경 RUNNING 확인
- Airflow UI: https://6df27c6b06a2417989cd681140847a9f-dot-asia-northeast3.composer.googleusercontent.com
- DAGs 버킷: gs://asia-northeast3-airflow-pro-a44456c2-bucket/dags
- Dataform API 호출/폴링 로직 추가: dags/tasks/dataform_api.py
- Dataform DAG에 PythonOperator/PythonSensor 적용
- DAG 패키지 인식용 __init__.py 추가 (dags/, dags/config, dags/tasks, dags/pipelines)
- DAG import 경로 수정 (dags.* -> config/tasks 직접 참조)
- Dataform repository/workflow config 수정: koin-repository / daily_stg_ga4_production
- Dataform API 로그 추가 (create/get URL 및 상태 출력)
- Dataform 파이프라인 정상 실행 확인 (DAG 성공)
- 문제 원인 정리: Dataform 리소스 경로 오타 수정 후 정상 동작
- 수동 실행 중이던 DAG Run(2026-02-06T11:22:25+00:00) 사용자에 의해 중단됨
- 내일 진행 예정: Tableau 리프레시 구현, Slack 알림, 스케줄 설정, 비용 알림 DAG
