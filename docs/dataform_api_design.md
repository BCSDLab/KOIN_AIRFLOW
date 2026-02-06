# Dataform API 호출 설계 (Airflow)

## 대상 정보

- Project: `kap-chat`
- Location: `asia-northeast3`
- Repository: `koin_repository`
- Workflow Config: `develop`

---

## 실행 흐름

1. `workflowInvocations.create` 호출로 실행 시작
2. 반환된 `workflowInvocation` 이름 저장
3. `workflowInvocations.get` 또는 `workflowInvocations.query`로 상태 폴링
4. `SUCCEEDED`면 성공, `FAILED/CANCELLED`면 실패 처리

---

## REST 엔드포인트

Base:

- `https://dataform.googleapis.com/v1beta1/`

Create:

- `projects/{project}/locations/{location}/repositories/{repo}/workflowInvocations`

Get:

- `projects/{project}/locations/{location}/repositories/{repo}/workflowInvocations/{invocation_id}`

---

## Create 요청 (workflow_config 사용)

요청 바디 예시:

```json
{
  "workflowConfig": "projects/kap-chat/locations/asia-northeast3/repositories/koin_repository/workflowConfigs/develop"
}
```

응답에서 `name` 필드를 저장 (예: `projects/.../workflowInvocations/{id}`)

---

## 상태 폴링

응답 필드:

- `state`: `STATE_UNSPECIFIED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`

종료 조건:

- 성공: `SUCCEEDED`
- 실패: `FAILED`, `CANCELLED`

권장 폴링:

- interval: 30~60초
- timeout: 2~3시간 (워크플로 길이 기준)

---

## 실패 처리 전략

- `FAILED/CANCELLED` 상태면 DAG 실패 처리
- 필요 시 `invocationId`를 로그에 기록
- Slack 알림에서 invocation 링크/ID 포함

