# v2 아키텍처: 역할과 책임의 경계

이 문서는 v2 **공개 아키텍처 후보**가 의도하는 책임 분리와 검증해야 할 안전 조건을 설명합니다. 기존 Python Harness의 보장이 Paseo에 자동 이전되었다고 주장하지 않습니다.

## 1. Ownership과 Source of Truth

| 주체 | 책임 | 책임이 아닌 것 |
| --- | --- | --- |
| **사용자** | 작업 승인, Business/API/DB Contract, QA 의사결정 | 누락된 계약을 모델이 임의로 채우도록 위임하지 않음 |
| **Project Profile** | 정본 프로젝트 규칙, 코드·DB·UI 맵 및 영향 범위에 대한 검증된 근거 | Credential 저장소 또는 현재 운영 상태의 대체재 |
| **Orchestrator** | 요구 분석, Job 분리, dependency·shared contract 판단, 검증 Gate 확인 | Worker의 성공 메시지를 무조건 성공으로 확정 |
| **Worker** | 승인된 Scope 안의 최소 수정과 자체 단위 점검 | 변경 범위 바깥의 임의 수정이나 승인을 생략한 Commit/Push |
| **Advisor / Reviewer** | 분석·설계 자문, 변경 Diff 및 Evidence 검토 | 구현 변경, 독립 Build/Test 실행 완료 주장 |
| **Reporter** | 변경 전후와 실제 검증 기록을 구분하여 보고 | 실행하지 않은 테스트·성과 창작 |
| **Paseo** | Workspace·Provider·Agent 연결 및 작업 실행 환경 | v1의 Queue authority, 원자적 Batch publisher, scoped rollback, journal, proof-binding의 자동 대체 |

## 2. Workflows와 Acceptance

```text
요구 / Contract / 허용 Scope
  → Orchestrator 조사·Job 분리
  → (필요 시) Advisor
  → Worker 구현 및 Diff
  → Build/Test (프로젝트 실제 명령)
  → Cross-family Reviewer
  → Orchestrator Verification
  → (필요 시) Human QA
  → 승인된 Commit/Push 및 Reporter
```

`PROPOSED`, `IMPLEMENTING`, `VALIDATING`, `REVIEWING`, `AWAITING_QA`, `ACCEPTED`, `BLOCKED`는 **설계상의 흐름 표현**이며, 이 공개 후보에 영속적으로 구현된 상태 기계라는 뜻은 아닙니다.

`SKIPPED`, `CANCELLED`, `FAILED`, `FAILED_FINAL`, `BLOCKED`, `INTERRUPTED`, `AWAITING_QA`는 완료로 승격하지 않습니다. Commit/Push/Deploy는 사용자 승인 및 필요한 검증을 별도로 만족해야 합니다.

## 3. Dependency와 충돌 처리

- **Semantic Dependency:** 선행 Job의 결과가 후속 Job의 비즈니스 의미를 결정
- **Execution Dependency:** 선행 결과·승인이 후속 Job의 실제 실행 전제
- **Write Conflict:** 동일 파일·경로·Hunk 등에 대한 수정 충돌 위험
- **Shared Contract:** 서로 독립 실행 가능하지만 DB/API/DTO/Business 의미를 공유

`sequence != dependency`입니다. Write Conflict만으로 존재하지 않는 Semantic Dependency를 만들지 않습니다.

Human QA가 의미 결정을 좌우할 경우 해당 dependent Job만 대기시킵니다. 독립 Job의 분석과 구현을 일괄 중단할 이유는 아닙니다. 후보 승격에는 최신 Machine Verification과 필요한 Human Acceptance가 모두 필요합니다.

## 4. Risk Tier 및 모델 배치

- **LOW:** 기계적 수정, 국소적이고 계약이 명확한 변경
- **MID:** 정의된 계약하의 다중 파일 기능·결함 수정
- **HIGH:** 아키텍처·Transaction·권한·데이터 의미·교차 모듈 호환성
- **CRITICAL:** 파괴적 변경·보안·개인정보·계약 미확정 등 추가 승인과 강제 Gate가 필요한 작업

Tier는 모델·리뷰 비용과 범위를 정하는 보조 기준이지, 사용자 승인이나 검증 결과의 대체재가 아닙니다. 실제 모델/Provider 버전은 환경 설정에 따라 달라지며 샘플 설정을 운영 지원 매트릭스로 해석하지 않습니다.

## 5. 안전 경계와 미구현 보장

1. `paseoTools.disabledTools`는 Paseo가 제공하는 도구를 비노출할 뿐, Shell·파일시스템 권한을 제거하지 않습니다.
2. Codex `sandbox_mode: read-only` 같은 Provider-native 설정과 실제 차단 테스트를 구분해야 합니다. Claude의 `disallowedTools`만으로 OS Sandbox를 보증하지 않습니다.
3. Policy에 기록한 write scope는 실제 Workspace/OS 격리, 승인 Gate, 최종 Diff 검사로 확인해야 합니다.
4. Runtime/Provider 장애와 Product 구현 결함은 실패 원인과 재시도 정책을 분리해야 합니다.
5. v1의 Git snapshot·rollback·integrity·journal·proof receipt·Batch publication 등의 기능은 이 문서만으로 실행되지 않습니다.
6. 불명확한 DB/API/권한·데이터 의미, 불안전한 Rollback, 외부 Side Effect가 있으면 추정 구현하지 않고 검증·승인을 요구합니다.

## 6. 공개 자료의 범위

제공받은 로컬 `config.json`은 설계 검토에만 사용했으며, 이 저장소에는 실사용 값이 없는 `config.sample.json`만 포함합니다. 실제 고객사 Profile, Credential, Queue, 작업 Evidence, DB 연결 및 Product 소스는 공개 범위가 아닙니다.

실사용 전 검증 항목과 공개 전환 상태는 [migration-checklist.md](migration-checklist.md)에서 관리합니다.
