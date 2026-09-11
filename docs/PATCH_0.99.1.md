# 0.99.1 Lean Execution Kernel Patch

0.99.1은 새 플랫폼이 아니라 decomplexification release다. 하나의 실행 kernel과
기존 gate/recovery 구조를 유지하면서, normal Product Job의 blocking complexity를
줄이고 runtime 신뢰 경계의 실제 결함만 수정한다.

## Release acceptance completion patch (`0.99.1+20260910.3`)

verification bundle에서 확인된 남은 contract gap을 최소 수정했다. 이미 정상으로
확인된 영역(review evidence reuse, Job undo/inherited batch delta 보존, flat
Stage Ticket, Worker Pool, and Reviewer Pool remain deferred to 1.0.

Knowledge/RAG and the generic sandbox DSL remain deferred to 1.1+.
회귀만 유지하고 재설계하지 않았다.

- **P0-1 Persistent OpenCode old-writer quiescence.** persistent server
  (`KKM_OPENCODE_SERVER_URL`)에서 in-flight turn이 있던 중 technical failure
  (progress timeout, transport/HTTP error 등)가 발생하면, error path는 이제
  `adapter.interrupt(session_id)`(native abort)를 호출하고 bounded
  terminal/idle/lost 확인을 수행한다. 확인 결과와 무관하게 해당 command는
  `aborted`로 기록되어 늦게 도착하는 old assistant response가 현재 결과로
  publish되지 않는다. abort/query가 UNKNOWN/transport failure/timeout이면
  quiescence를 `unconfirmed`로 기록하고, 다음 canonical writer는 bounded
  read-only 재확인이 terminal/idle/lost를 증명하기 전까지
  `OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED`로 거부된다(candidate/delta 보존,
  technical failure, product retry delta 0). `writer_authority=SUSPENDED`,
  lease expired, harness runtime ENDED만으로는 대체 writer를 허용하지 않는다.
  harness-owned local server는 기존 process-tree cleanup이 deterministic
  isolation을 유지한다. 새 Recovery Execution/재귀 recovery graph는 없다.
- **P0-2 Normal snapshot path의 historical scan 제거.**
  `ControlRepository.candidate_ownership(current_only=True)`가 current-custody
  질문(active/unresolved ownership, exact hash integrity, accepted current
  baseline input)만 답한다. normal `prepare_job_snapshot()` 경로는 이 mode를
  사용하므로 `_verified_candidate_history()` 재구성과 SUCCEEDED/SKELETON_READY
  logical-Job 전체 scan(과거 successful genealogy/`successful_hashes` 재구성)을
  수행하지 않는다. historical verified-Job scan은 incident/operator forensic
  (`_completed_qa_corrective_successor_id` 등 명시적 provenance 검증 경로)에만
  남는다. active unresolved candidate의 scope conflict/scope 밖 보존은 그대로
  유지된다.
- **P1-1 Explicit LOW deterministic oracle.** LOW이기 때문에 Review를 생략하지
  않는다. immutable Job `test_plan`에 typed `review_oracle:
  DETERMINISTIC_ORACLE`(`mandatory` test contract 필요)이 명시적으로 선언되고,
  effective review risk가 LOW이며 hard escalation flag가 없고, pending QA
  hold가 없고, official tester 실행이 현재 frozen `test_scope_hash`와 현재
  materialized `execution_surface_hash`에 binding되고, BUILD/TEST gate가 현재
  candidate/acceptance identity에 fresh-PASS일 때만 Manager가 별도 LLM Reviewer
  호출을 0회로 생략하고 `review_status=REVIEW_PASS`,
  `review_mode=DETERMINISTIC_ORACLE`, `reviewer_required=false`,
  `reviewer_invocations=0`, `oracle_evidence_sha256`(current) REVIEW gate
  receipt를 남긴다. 조건이 하나라도 없으면 기존 LLM Review를 그대로 수행한다.
  Worker self-review는 oracle이 아니다. keyword inference는 없다.
- **P1-2 Meaningful progress.** polling fallback의 progress watchdog reset 근거가
  submitted turn에 상관된 semantic projection(correlated assistant
  text/part 실질 변화, correlated tool state transition, correlated message
  추가/finish 전이, relevant session state 전이)만으로 바뀌었다. heartbeat,
  timestamp/usage/token metadata, 상관 없는 session message, 동일 semantic
  state의 metadata mutation은 reset하지 않는다.
- **P1-3 MANAGED local ephemeral auth 정합화.** `start_local_server()`는
  base_url이 있는 MANAGED remote + 무password만
  `MANAGED_REMOTE_AUTH_REQUIRED`로 거부한다. harness-owned local server는
  무password 시 ephemeral per-instance 자격증명을 발급한다(문서/릴리스
  claim대로). person 환경 canary가 아닌 MANAGED local/no-password 직접 회귀로
  고정했다.
- **P1-4 Direct adoption boundary 정직화.** 확정 0.99.1 contract(Interactive
  Direct → adoption → 동일 validation/publication)를 현재 구현이 만족하지
  못한다. 기존 Worker-free validation 경로(`run_control_recovery`)는
  preserved-delta recovery용이며 external adoption에 그대로 연결하려면 새
  lifecycle/schema가 필요하다. 따라서 이번 patch는
  `BASELINE_ADOPTION_ONLY`/`DEFERRED_1_0`로 경계를 명시한다: adoption
  manifest와 queue 기록은 `validation_boundary=BASELINE_ADOPTION_ONLY`,
  `generation_provenance=UNKNOWN`(origin=EXTERNAL 보존, 없는 evidence를
  만들지 않음)을 기록하고, store는 이 명시 없는 adoption을 거부한다. Direct
  delta의 Build/Test/Review/Verification 포함 validation/publication은 1.0
  roadmap 항목이다. "Direct adoption COMPLETE"가 아니다.
- **P1-5 Capability guarantee의 실제 보장 범위.** typed
  `operation_capabilities` admission floor는 유지하며, release invariant를
  실제 증명 범위로 분리해 명시했다(아래 "Release invariants"). undeclared
  arbitrary shell/API side effect의 deterministic interception은 현재 runtime이
  제공하지 않으므로 보장으로 표현하지 않고, undeclared capability를 keyword
  scan으로 추정하지도 않는다.

### Release invariants (truthful scope)

```text
# Proven by implementation + focused regressions (0.99.1+20260910.3)
PERSISTENT_OPENCODE_UNQUIESCED_REPLACEMENT = 0
NORMAL_JOB_HISTORICAL_SUCCESS_SCAN = 0
LOW_EXPLICIT_ORACLE_LLM_REVIEW_SKIP = supported (typed contract only)
METADATA_ONLY_PROGRESS_RESET = 0
REVIEW_EVIDENCE_FALSE_REUSE = 0
PRIOR_BATCH_DELTA_LOSS = 0
RUNTIME_FAILURE_TO_PRODUCT_RETRY_LEAK = 0
NESTED_RECOVERY = 0
DECLARED_DANGEROUS_CAPABILITY_AUTONOMOUS_ADMISSION = 0

# NOT PROVEN by the current runtime (do not claim as guarantees)
# - deterministic interception of undeclared arbitrary shell/API side effects:
#   the typed floor denies declared dangerous capabilities at admission; the
#   managed runtimes provide no runtime effect interception, and undeclared
#   capabilities are never guessed by keyword scanning.
# - Direct-adoption validation/publication (BASELINE_ADOPTION_ONLY today;
#   DEFERRED_1_0).
```

`UNAUTHORIZED_EXTERNAL_EFFECT=0`은 runtime enforcement의 절대 보장으로
표현하지 않는다. 실제 보장은 위의
`DECLARED_DANGEROUS_CAPABILITY_AUTONOMOUS_ADMISSION = 0`(typed admission
floor, fail-closed)이다.

## Wave 0 — 조사 및 hotfix reconciliation

현재 source(`VERSION=0.99.0`, control schema v5) 대비 확정 결과:

| 과거 지적 | 현재 상태 | 근거 |
|---|---|---|
| OpenCode pending approval/question이 SUPPORTED로 선언 | `CURRENT_DEFECT` → 수정 | `query_session`이 `/session/status`를 ACTIVE/IDLE로만 매핑해 `WAITING_INPUT`을 만든 적 없음. `interaction.pending_approval/question`을 `PARTIAL`로 정정 (G1) |
| OpenCode 기술 실패 코드가 `OPENCODE_TRANSPORT_ERROR`로 붕괴 | `CURRENT_DEFECT` → 수정 | worker except-branch가 `exc.code` 대신 고정 코드 반환. 원 코드 보존으로 변경 |
| local server stdout/stderr PIPE 무드레인 | `CURRENT_DEFECT` → 수정 | OS pipe buffer가 차면 server가 hang할 수 있음. `DEVNULL`로 변경 |
| OpenCode 1.18.30 loopback 무인증 거부 (401) | `CURRENT_DEFECT`(신규 환경 drift) → 수정 | 1.18.29 live 검증 이후 CLI upgrade. harness-owned local server에 ephemeral per-instance 자격증명 발급으로 해결, live canary PASS |
| per-stage timing breakdown 부재 | `CURRENT_DEFECT` → 추가 | 전체 `duration_seconds`만 존재. 아래 "Timing" 참고 |
| Review PASS evidence의 scope별 재사용 부재 | `CURRENT_DEFECT` → 추가 | candidate 변경 시 REVIEW gate 전체 STALE, 전 batch 재리뷰. 아래 "Review evidence reuse" 참고 |
| production DB/실발송/파괴적 외부 효과의 typed 차단 부재 | `CURRENT_DEFECT` → 추가 | 아래 "Operation capability floor" 참고 |
| `test_0916_*`/`test_skeleton_policy`/`test_09004_hotfix` stub이 현재 계약과 불일치 | `CURRENT_DEFECT`(test) → 수정 | `accepted_baseline_hashes` Mock, `attempt_reservation` 누락. fixture만 계약에 맞게 수리 |
| writer overlap / late response / rollback delta 보존 | `ALREADY_FIXED` | `OLD_RUNTIME_WRITER_ACTIVE`, command delivery FSM/`QUERY_SESSION_DO_NOT_RESEND`, parentID 상관, scoped rollback + `inherited_delta_preservation`이 이미 존재하며 회귀 테스트 유지 |
| nested recovery | `NO_CHANGE_REQUIRED` | recovery는 새 Job을 만들지 않음(구조적으로 불가). `runtime_recoveries.product_retry_delta=0` 제약 유지 |
| report/Telegram 실패가 Product 재실행을 유발 | `NO_CHANGE_REQUIRED` | notify/report는 fail-soft diagnostic이며 `failure_disposition`에 영향 없음 |
| session reuse 실패 = Product 실패 | `NO_CHANGE_REQUIRED` | durable session resume 실패 시 fresh fenced attempt + `[기술 복구 인계]` handoff 경로 존재 |
| batch-final commit/staging 도구 부재 | `DEFERRED_1_0` | `_refresh_batch_lifecycle`은 eligibility만 계산. 이번 release 범위 외 |

Roadmap 문서(`HARNESS_ROADMAP_CURRENT_0.99_TO_1.1.md`)와 과거
review/evidence 문서는 workspace에 존재하지 않아, 현재 source와 실행 evidence를
최우선 근거로 사용했다.

## Wave A — Runtime trust boundary

1. **Capability declaration truthfulness (G1).** OpenCode descriptor의
   `interaction.pending_approval`/`interaction.pending_question`을
   `SUPPORTED` → `PARTIAL`로 정정했다. native server는 pending 상태를 SSE로만
   노출하고 본 adapter는 SSE client를 binding하지 않는다. permission 대기는
   따라서 "제품 결함"이 아니라 무진행 기술 timeout(`OPENCODE_PROGRESS_TIMEOUT`,
   `product_retry_delta=0`)으로 관측된다. 구현되지 않은 capability를 낙관적으로
   선언하지 않는다. 계약 hash가 바뀌므로 구 execution의 frozen contract와
   비교되면 기존대로 `RUNTIME_CONTRACT_DRIFT`로 fail-closed 된다.
2. **Original failure code 보존.** OpenCode worker의 except-branch가 adapter의
   원래 코드(`OPENCODE_PROGRESS_TIMEOUT`, `OPENCODE_ASSISTANT_ERROR`,
   `OPENCODE_HTTP_ERROR`, server 시작/정리 실패 등)를 그대로
   `WorkerResult.execution_failure_code`로 반환한다. `CODEX_IPC_FAILURES`에
   runtime 단계의 기술 실패 코드 전체를 추가해 Manager 분류(HARNESS_CAUSED
   infrastructure, `RUNTIME_RECOVERY`)가 유지된다. 제품 semantic retry는
   여전히 소비하지 않는다.
3. **Managed server pipe backpressure 제거.** `opencode serve` 자식 프로세스의
   stdout/stderr를 `DEVNULL`로 연다. 실행 중 drain하지 않는 PIPE handle는 OS
   pipe buffer가 차는 순간 server를 dead-lock시킨다. 진단은 기존처럼
   health/status API로 한다.
4. **OpenCode ≥1.18.30 loopback 인증.** 신규 CLI는 loopback에서도 무인증
   요청을 401로 거부한다(2026-09-08 canary는 1.18.29 기준). password가 없는
   harness-owned local server는 기동 시 `secrets.token_urlsafe` ephemeral
   자격증명을 발급해 env로 전달하고 동일 값을 요청 header에 사용한다. 무인증
   listener를 없애는 방향의 최소 변경이며 live canary(서버 기동/health/session
   create/query/delete/자식 정리)로 검증했다.

이외 Wave A 항목(writer fencing, 상관 없는 완료 미채택, abort 후 process tree
종결, lease/heartbeat/progress 분리)은 0.99.0에서 이미 구현·테스트되어 있어
`ALREADY_FIXED`로 유지하고 회귀를 그대로 둔다.

## Wave B — Custody / snapshot / Job undo

`CURRENT_DEFECT` 없음. batch baseline(`BASELINE-G<n>-…`), Job-start
before-image(`SNAP-…`, claim 시 검증), rollback checkpoint(`CHK-…`,
before/after blob·mode·untracked·EOL/BOM), task-owned scoped rollback과
`inherited_delta_preservation=PRESERVED`, 외부 baseline adoption(CAS +
confirmation)이 모두 존재한다. B0→J01/J02 성공→J03 rollback 시
`B0+Δ1+Δ2`와 pre-existing dirty 보존 시나리오는 기존 회귀
(`test_batch_final_commit`, `test_0913_baseline`, gate scenarios)로 증명된다.
checkpoint 복원 시 review evidence가 새 epoch에서 재사용되지 않도록
`_prepare_restored_evidence_epoch`에서 `review_evidence` 초기화만 추가했다.

## Wave C — Lean LOW path / capability floor

- **LOW normal flow는 이미 직행이다.** WORKER→GIT→BUILD→(TEST 기본
  OFF)→REVIEW→VERIFICATION 외的历史 reconciliation, session resurrection,
  recursive recovery, mandatory checkpoint chain, deep E2E는 normal path에
  없다. deterministic oracle(build/official tester)이 이미 gate이며, 별도 LLM
  Reviewer는 모든 tier의 독립 oracle로 유지한다(Worker self-review로 대체하지
  않는다).
- **Operation capability floor (G2).** `JobRequest`에 typed
  `operation_capabilities` 필드를 추가했다. 요청 text는 절대 keyword
  scan하지 않는다. 등록된 capability(`PRODUCTION_DB_APPLY`,
  `PRODUCTION_DB_WRITE`, `REAL_EXTERNAL_SEND`, `REAL_PAYMENT`,
  `DESTRUCTIVE_EXTERNAL_OPERATION`, `PII_EXTERNAL_EGRESS`)는 현재 runtime에서
  결정적 grant/revoke가 불가능하므로 전부 autonomous 실행 거부
  (`OPERATION_CAPABILITY_UNSUPPORTED_AUTONOMOUS`, `OPERATION_CAPABILITY_UNKNOWN`
  fail-closed). migration 파일 "작성"은 capability 없이 정상 coding job이며,
  금지 문구(security/migration/email/운영 drop 등)가 포함된 requirement는
  review depth만 올리고 실행 capability를 바꾸지 않는다(회귀 테스트로 고정).
- Review risk는 기존대로 reviewer model/effort/context/depth와 worker model
  하한만 결정하고, runtime permission/sandbox와는 무관하다
  (`review_risk_axis: SEPARATE` 유지).

## Wave D — Evidence-proportional review (G3)

새 lifecycle/schema 없이 기존 `DiffBatch`/changed-atom ledger 위에
`review_evidence.py`(schema `review-evidence/1`)를 얹었다.

- 매 review round 종료 시 PASS batch의 chunk 단위 증명을
  `TaskState.review_evidence`에 기록한다. unit key는
  `module|path|hunk_id|content_sha256`(byte 단위 정확성)이며 proof inputs는
  `acceptance_hash`, `contract_revision`, `execution_surface_hash`다.
- 다음 round(같은 Job의 fix 재시도)에서 proof inputs이 정확히 일치하는 경우에만:
  - 내용이 byte 동일한 이전 PASS chunk → `REUSED_VALID_EVIDENCE` (재호출 없음)
  - 변경/신규/이전 FAIL/불명확 → STALE → 재리뷰
- 보수적 제약: LOW/MID(`DIFF_ONLY`/`LOCAL`) depth에서만 재사용. HIGH/CRITICAL은
  cross-module semantic closure를 결정적으로 증명할 수 없어 전체 재리뷰.
  checkpoint 복원 epoch, infra 실패 scope, acceptance/contract 변경은 재사용
  금지. 재사용 carrier batch는 atom coverage에 포함되므로 coverage 불완전은
  여전히 `REVIEW_ERROR`(false success 불가).
- coverage에 `review_evidence_reuse`(재사용/재리뷰 파일 목록)가 기록된다.
  Worker self-review는 기존대로 독립 Review를 대체하지 않는다.

## Wave 0 — Timing breakdown (G5)

추정 없이 이미 durable한 timestamp에서만 도출한다.

- `execution_evidence.stage_timing`: worker/git/build+test/review/verification
  초(`progress_events`의 stage marker에서 파생), review는
  `reviewer_invocations` 실측과의 max. `queue_wait_seconds`/
  `admission_control_seconds`는 job history(`ENQUEUED→CLAIMED→TASK_STARTED`)에서
  주입. `technical_recovery_seconds`/`human_wait_seconds`는 현재 원천이 없어
  `NOT_CAPTURED`(회복 횟수는 기존대로 기록).
- `job_queue.job_lead_time(job)`: queue wait/admission/human
  wait(`TASK_FINISHED(SUCCEEDED)→SUCCESS_ACKNOWLEDGED`)/total lead time.
  acknowledge 시 `acknowledgement_wait_seconds`를 job에 기록한다.
- token/cost는 여전히 `NOT_AVAILABLE`/`UNRELIABLE`로만 보고한다.

## Non-goals (DEFERRED_1_0)

Stage Ticket scheduler, Worker Pool, Reviewer Pool, work stealing, parallel
candidate writers, multi-writer canonical workspace, generic policy DSL,
recursive/nested recovery framework, Knowledge/RAG, Incident Responder,
LangGraph top-level orchestration, Serena production dependency, batch-final
staging/commit 도구.

## 검증

- 신규 focused 회귀 `test_099_lean_kernel`(19): capability 정직성, 원 코드
  보존, DEVNULL spawn, typed capability floor, 금지문구 false-positive,
  evidence reuse(정확 입력 일치/불일치, HIGH 무재사용, checkpoint epoch 초기화,
  coverage 무결성), timing breakdown.
- 기존 suite 전체(1487)와 deterministic Eval corpus는 release manifest에
  기록된 대로 통과해야 release로 간주한다(`docs/index.md` 규칙 유지).
- Live managed runtime: OpenCode 1.18.30 local server canary PASS(ephemeral
  auth). Windows 실기반 나머지 adapter(Codex/Droid) live 다중 턴은 여전히
  `DROID_NATIVE_SESSION=FAIL` 등 기록된 한계를 유지한다(RUNTIME_CAPABILITY_MATRIX
  참조).

## 호환성

- control schema v5 그대로(additive 변경 없음). Queue/Job JSON schema도 동일.
- `TaskState`에 `review_evidence`, `queue_wait_seconds`,
  `admission_control_seconds` 필드가 추가되었다(구버전 JSON은 기본값으로
  로드).
- OpenCode runtime contract hash가 바뀌었다(PARTIAL 정정). 구 materialized
  execution이 새 adapter로 재개되면 기존 규칙대로 `RUNTIME_CONTRACT_DRIFT`로
  차단된다. 신규 실행은 새 계약으로 freeze된다.
- failure code semantics: `OPENCODE_*` 기술 코드가 WorkerResult에 그대로
  노출된다. 신규 코드 `OPERATION_CAPABILITY_UNKNOWN`,
  `OPERATION_CAPABILITY_UNSUPPORTED_AUTONOMOUS`.
