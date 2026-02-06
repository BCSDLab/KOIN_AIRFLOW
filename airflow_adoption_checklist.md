# Airflow 도입 체크리스트 (GCP + 확장성 고려)

## 1. 목표/범위 고정
- [ ] 파이프라인 범위 확정: Dataform → BigQuery → Tableau → Slack
- [ ] 실행 정책 확정: 일 1회, Dataform 완료 후 Tableau 갱신

## 2. IAM/보안 기반 정리
- [ ] Composer 서비스 계정 생성
- [ ] Dataform 권한 부여
- [ ] BigQuery 권한 부여
- [ ] Tableau/Slack 토큰은 Secret Manager에 저장

## 3. 네트워크 설계 (확장성 핵심)
- [ ] Composer 환경 리전/네트워크 결정
- [ ] VPC/사설망 접근 필요 여부 확인
- [ ] 외부 SaaS 연동 대비 egress 정책 확인

## 4. Cloud Composer 환경 구축
- [ ] Composer 2 환경 생성
- [ ] Airflow 기본 설정(Variables/Connections/Plugins) 확정
- [ ] Secret Manager 연동 설정

## 5. Dataform 연동 DAG 구현
- [ ] workflow_config 트리거 태스크 구현
- [ ] 상태 폴링 센서 구현
- [ ] 실패 처리/재시도 정책 적용

## 6. Tableau 연동 PoC
- [ ] Tableau Cloud API 인증(PAT) 준비
- [ ] Extract Refresh Task 실행 테스트
- [ ] DAG에 연결

## 7. Slack 요약 알림
- [ ] 성공/실패 집계 DAG 설계
- [ ] 일일 요약 메시지 포맷 확정
- [ ] Webhook 또는 Bot 방식 확정

## 8. 운영 기준 확정
- [ ] SLA/재시도 기준 정의
- [ ] 알림 수신자/에스컬레이션 규칙 정의
- [ ] 로그/모니터링 정책 정의

## 9. 확장성 패턴 정리
- [ ] DAG 모듈화 구조 확립
- [ ] Connection/Secret 관리 규칙 문서화
- [ ] 신규 SaaS 추가 가이드 작성
