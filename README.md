# Autonomous Coding Harness

**레거시 프로젝트에 AI 코딩 에이전트를 안전하고 반복 가능하게 적용하기 위해 설계·발전시켜 온 Harness Engineering 프로젝트입니다.**

초기에는 Python·SQLite 기반의 자체 실행 관리 엔진을 만들었습니다. 실제 개발과 검증 과정에서 얻은 통제 원칙을 바탕으로, 현재는 `AGENTS.md`, 재사용 가능한 Skills, 프로젝트별 Profile과 [Paseo](https://paseo.sh/)를 조합한 **경량 멀티 에이전트 워크플로(v2)**로 구조를 전환하고 있습니다.

> **현재 공개 상태: `2.0.0-candidate`**
>
> 이 저장소는 공개 가능한 정책·Skill·샘플 구성 중심의 **v2 아키텍처 후보**입니다. 실제 운영 환경에서 전체 검증 게이트, 격리, 복구, 권한 차단이 검증된 완성형 실행 엔진이라는 뜻은 아닙니다.

## 1. 해결하려는 문제

AI 에이전트가 코드를 작성하는 것과, 실제 프로젝트에서 **그 변경을 안전하게 수용할 수 있는 것**은 다른 문제입니다.

- 에이전트가 기존 호출 흐름과 Business/API/DB Contract를 충분히 확인하지 않고 수정할 수 있습니다.
- 작업 범위 밖의 파일 변경이나 기존 변경사항 훼손 가능성이 있습니다.
- Build/Test 실패, 모델·도구 장애, 구현 결함이 동일한 실패로 처리될 수 있습니다.
- Worker의 “완료” 응답이나 Reviewer의 의견만으로 실제 동작을 검증할 수 없습니다.
- 여러 에이전트가 동시에 작업할 때 dependency, write conflict, QA 대기 및 최종 통합 검증이 필요합니다.

이 프로젝트의 초점은 **코드 생성 프롬프트를 잘 작성하는 것**보다, 요구사항 분석부터 검증 근거까지 추적 가능한 실행 절차를 만드는 데 있습니다.

## 2. 아키텍처의 변화: v1 → v2

| 단계 | 접근 | 얻은 것과 남은 과제 |
| --- | --- | --- |
| **v1 / 0.99.x** | Python + SQLite 자체 실행 관리 엔진 | Queue/Job/Task, scope, retry/recovery, rollback, Build/Test/Review/Verification 등 통제 개념을 코드로 구현. 반면 런타임·오케스트레이션 유지보수 부담 증가 |
| **v1.x 실험 이력** | 후속 실행 계층 개선 실험 기록 | 현재 공개 적합성 점검 대상이며 포트폴리오에서 직접 링크하지 않음 |
| **v2 후보 (현재)** | `AGENTS.md` + Skills + Profiles + Paseo | 프로젝트별 지식과 행동 규칙을 분리하고, 에이전트 역할에 따라 분석·구현·리뷰·보고를 조합. 기존 v1 보장사항의 자동 승계는 **없음** |

**전환 이유:** 자체 Python 커널에서 책임·상태·실패·증거를 다뤄본 경험은 유지하되, 모델 실행·세션 관리·에이전트 연결처럼 외부 도구가 제공하는 기능까지 중복 구현하지 않기 위해서입니다. 목표는 핵심 통제 원칙을 버리는 것이 아니라, **유지해야 할 책임과 외부에 위임할 책임을 명확히 구분**하는 것입니다.

과거 소스는 제거된 것이 아니라 Git 이력과 별도 브랜치에 남겨 두었습니다.

- [v1 공개 마지막 기준점 — 0.99.2.1](https://github.com/cdmk5454/autonomous-coding-harness/tree/archive/0.99.2.1-python-kernel)

## 3. 현재 v2 워크플로

```text
[사용자] 요구사항 / 허용 범위 / Business·API·DB Contract / Acceptance
   │
   ▼
[Orchestrator] 소스 조사 · Job 분리 · dependency 및 shared contract 분석
   │
   ├──► [Advisor] 고위험·미확정 계약에 대한 읽기 전용 자문
   │
   ▼
[Worker] LOW / MID / HIGH 위험도에 따른 구현 및 단위 검증
   │
   ▼
[Build / Test] 프로젝트의 실제 명령으로 검증 근거 수집
   │
   ▼
[Cross-family Reviewer] 변경 diff · 영향 범위 · 검증 누락 점검
   │
   ▼
[Orchestrator] Acceptance 확인 / 추가 수정 / Human QA 대기 / 차단
   │
   ▼
[Reporter] 변경 전후와 근거, 남은 위험을 사용자에게 보고
```

**역할과 책임:** Paseo는 Agent·Provider·Workspace를 구성하는 오케스트레이션 환경입니다. 위 흐름과 위험도 분류는 이 저장소의 **설계·운영 규칙**이며, Paseo 자체가 v1의 Queue authority, 영속 상태 기계, 원자적 Batch publication, scoped rollback을 제공한다는 의미는 아닙니다.

### 설계 원칙

1. **Evidence-first:** 요구사항 → 기존 호출 흐름 → 실제 Source/Contract → 최소 변경 → Build/Test/Review/Verification 순서로 판단합니다.
2. **최소 변경과 Scope 통제:** 프로젝트 Profile을 명시적으로 선택하고 승인된 write scope와 실제 diff를 비교합니다. 정책 문구만으로 파일 접근이 격리되지는 않습니다.
3. **Semantic risk 기반 라우팅:** LOW/MID/HIGH는 파일 개수보다 권한·데이터 의미·상태 전이·호환성·영향 범위를 기준으로 구분합니다.
4. **역할 분리 및 교차 리뷰:** Worker 결과와 Reviewer 의견을 별개의 근거로 다룹니다. 이기종 모델 검토도 Build/Test를 대체하지 않습니다.
5. **명시적 QA/승인:** 미확정 Business/API/DB Contract나 Human QA에 따라 영향을 받는 후속 작업을 구분합니다. `AWAITING_QA`나 `BLOCKED`는 성공이 아닙니다.
6. **정확한 보고:** 실제 실행하지 않은 검증은 `NOT_RUN`으로 기록합니다. 예상 효과와 관찰된 결과를 구분합니다.

## 4. 권한 및 보안 경계

- Paseo의 `paseoTools.disabledTools`는 **Paseo 도구 노출 제한**입니다. Shell·파일시스템 접근까지 차단하는 OS 수준 Sandbox는 아닙니다.
- Codex의 `sandbox_mode: read-only`는 Provider별 원시 설정입니다. Claude 등 다른 Provider는 별도의 권한·Sandbox 설정 및 실제 차단 테스트가 필요합니다.
- Worker의 write scope는 문서상의 지시만으로 강제되지 않습니다. 실제 작업 공간 격리, 경로·Diff 검증, 승인 Gate가 별도로 필요합니다.
- DB/API 접근은 프로젝트별로 승인된 Contract를 따릅니다. 공개 샘플에 실사용 DB 접속정보와 실행 기능을 포함하지 않습니다.
- 기존 Python 엔진이 구현한 crash consistency, rollback, integrity, proof-binding 기능은 **v2에 자동으로 이전되지 않습니다.** 실제 보장이 필요하면 별도 구현·검증해야 합니다.

## 5. 저장소 구성

```text
.
├── README.md
├── AGENTS.md                 # 공통 Agent 행동·검증 규칙
├── VERSION                   # 2.0.0-candidate
├── config.sample.json        # 비실행형 Paseo 설정 예시
├── skills/                   # planning, project-context, backend, frontend,
│                             # sql, testing, debugging, review, reporter
├── hooks/
│   └── validation.md         # 검증 체크리스트 (독립 보안 제어 아님)
├── profiles/
│   └── sample-project/       # 실제 고객 정보가 없는 합성 예시
└── docs/
    ├── architecture.md       # 역할·권한 경계·상태 및 의존성
    └── migration-checklist.md # 공개/운영 전환 검증 항목
```

`config.sample.json`은 로컬 `config.json`을 그대로 복사한 실행 설정이 아닙니다. 모델 Alias, 경로, Credentials, 프로젝트별 값을 직접 확인하고 환경에 맞게 구성해야 합니다. 이 저장소만 Clone하면 실제 Agent 파이프라인이 즉시 기동되는 패키지는 아닙니다.

## 6. 검증 상태 및 향후 과제

| 검증 | 현재 공개 상태 |
| --- | --- |
| v1 기준점/1.x 아카이브 보존 | 확인 |
| v2 공개 후보 JSON·Markdown 정적 구조 검사 및 선별 제외 점검 | 수행 |
| Paseo/Provider별 Tool denial 및 실질 Read-only 검증 | `NOT_RUN` |
| 실제 LOW Job, Build/Test/Review/QA 흐름 | `NOT_RUN` |
| 실패 복구, 작업 격리, 최종 Batch publication | `NOT_RUN` |
| 실사용 시스템에서의 end-to-end 운용 승인 | `NOT_RUN` |

> **공개 후보와 실무 사용 환경의 구분:** 위 표의 `NOT_RUN`은 이 저장소에 공개된 **v2 후보 패키지 기준으로 재현·확인한 검증 기록이 없다는 의미**입니다. 실제 업무에서는 고객사 정보가 포함된 비공개 설정 및 프로젝트 환경에서 Harness와 AI 코딩 에이전트 작업 흐름을 사용해 왔습니다. 해당 환경의 실행 기록과 설정은 공개하지 않으며, **실무 사용 이력이 공개 v2 후보의 전체 검증 완료를 의미하지는 않습니다.**

현재 후보의 목적은 **설계와 공개 자료를 정확하게 보여주는 것**입니다. 완료되지 않은 검증을 PASS로 표시하지 않으며, 운영 기능을 단정하지 않습니다. 상세 내용은 [아키텍처](docs/architecture.md)와 [전환 체크리스트](docs/migration-checklist.md)를 참조해 주세요.

## 참고

- [Paseo 공식 사이트](https://paseo.sh/)
- [Paseo MCP / 도구 경계](https://paseo.sh/docs/mcp)
- [Paseo Provider 설정](https://paseo.sh/docs/sdk/provider-options)
- [Paseo Security](https://paseo.sh/docs/hub/security)
