# PATCH 0.99.2 — Operational Convergence & Verification Stabilization

0.99.2는 새로운 orchestration architecture를 도입하지 않는다. 0.99.1 Lean
Execution Kernel이 실제 Product workload에서 반복적으로 노출한
runtime/recovery/review/convergence 관측 문제를 최소 수정으로 닫고, 1.0
Bounded Parallel Execution 진입 전제인 reliable single-worker baseline을
확보하는 릴리스다.

## 실사용 증거가 지시한 수정

0.99.1 실운영에서 반복 관측된 대표 패턴:

```text
OPENCODE_PROGRESS_TIMEOUT       multiple occurrences
OPENCODE_SERVER_START_TIMEOUT   multiple occurrences
REVIEW_COVERAGE_PLAN_INVALID    multiple occurrences
```

- recoverable runtime failure가 곧바로 `AWAITING_QA`(사람 개입)로 종결됐다.
- 실제 Product semantic rework가 operator의 new-Job 재등록으로 흩어져 `product_semantic_retry_count=0`으로
  관측되고, cross-rework review evidence 재사용 근거도 끊겼다.
- `REVIEW_COVERAGE_PLAN_INVALID`가 단일 code로 뭉쳐 실제 고장 위치를
  구분할 수 없었다.
- finalization-only storage failure가 Worker 재실행 압력으로 새겨졌다.

## P0-1 Bounded runtime technical recovery (same semantic work)

- `record_result`에서 recoverable runtime failure
  (`OPENCODE_PROGRESS_TIMEOUT`, `OPENCODE_SERVER_START_TIMEOUT`,
  `OPENCODE_SERVER_START_FAILED`, `OPENCODE_TRANSPORT_ERROR`,
  `OPENCODE_HTTP_ERROR`, `OPENCODE_SESSION_CREATE_INVALID`)가
  `RUNTIME_RECOVERY`/`HARNESS_CAUSED`/`infrastructure`/`PRESERVED`로
  종결될 때, 기존 `TECHNICAL_RECOVERY` attempt budget이 남아 있으면
  **동일 Job**을 `QUEUED`로 재예약한다
  (`RUNTIME_FAILURE_TECHNICAL_RECOVERY_QUEUED`).
- quiescence contract는 강화되지 않고 유지된다: control repository의
  `runtime_recoveries` 최신 증거가 quiesced가 아니면
  (`OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED`) 재예약 거부(fail-closed).
  서버가 turn을 제출한 적 없는 start 장애는 부재로 확인된다.
- 재예약은 concise handoff(`runtime_recovery_context`)를 남기고 다음
  실행에서 새 execution/binding(=fresh process/session)으로 동일
  요구사항을 계속한다. 기존 writer admission gate/`WRITER_AUTHORITY_SUSPENDED`
  방어는 그대로다.
- counter 분리: `job.technical_replacement_count`(기술 대체)만 증가하고
  `product_semantic_retry_count`/`semantic_rework_*`는 불변
  (`RUNTIME_FAILURE_TO_PRODUCT_RETRY_LEAK = 0` 유지,
  `attempt_outcomes.product_retry_delta = 0`).
- budget 소진 시 기존처럼 `AWAITING_QA` human gate
  (`RUNTIME_TECHNICAL_RECOVERY_HELD` 이벤트로 사유 기록).
- absolute wall-clock timeout은 재도입하지 않는다. 기존 heartbeat
  lease/meaningful-progress watchdog/RPC timeout 방향 유지.

## P0-2 Semantic rework lineage (cross-Job)

- `JobRequest`에 명시적 `rework_of_job_id` 추가. canonical lineage는
  오직 이 선언적 참조에서만 나온다. client Job ID의 `R1`/`FIX` 등
  suffix parsing은 telemetry hint로도 사용하지 않는다.
- 각 canonical Job identity는 보존되고, enqueue 시 queue mutex 안에서
  `semantic_root_job_id`(root walkthrough로 확정), `semantic_rework_ordinal`
  (family 내 max+1, durable), 참조 root의 제자리 정규화
  (`SEMANTIC_LINEAGE_NORMALIZED`)가 기록된다.
- 검증: 미존재 대상 `REWORK_TARGET_NOT_FOUND`, cycle/과depth
  `REWORK_LINEAGE_CYCLIC`/`REWORK_LINEAGE_TOO_DEEP`로 거부.
- metric 분리 유지: `technical_replacement_count`(runtime 기술 복구),
  `product_semantic_retry_count`(same-Job semantic retry, 의미 불변),
  `semantic_rework_job_count`/`semantic_rework_family_count`(cross-Job
  semantic rework) — 기존 metric 의미를 몰래 바꾸지 않는다.
- **SQLite schema는 5를 유지한다.** lineage는 canonical job payload
  (`logical_jobs.payload`)에 durable 저장되며, family 조회는 queue
  order 순회로 충분하다(단일 worker 규모). schema 6이 필요한 최소
  정당성이 없으므로 확장하지 않았다.

## P0/P1-3 Review / finalization stabilization

- **결정적 diff transport parsing**: planner가 `str.splitlines` 대신
  LF 전용 split(`_split_diff_lines`)을 사용한다. Unicode line boundary
  (`\r`, `\v`, `\f`, `\x1c`-`\x1e`, `\x85`, `\u2028`, `\u2029`)가 파일
  *내용*에 있을 때 hunk가 오분할되어 발생하던
  `hunk range count mismatch`/`malformed hunk body` 교통 오염이 제거된다.
  well-formed diff에 대해 byte-identical하며 atom ledger는 그대로 무손실.
- **typed failure stage**: `DiffBatchPlan.failure_stage`
  (`PLAN_CONFIG`/`DIFF_SECTION`/`FILE_HEADER`/`BINARY_ARTIFACT`/
  `HUNK_HEADER`/`HUNK_BODY`/`HUNK_RANGE`/`CHUNK_LIMIT`/`OWNERSHIP`/
  `ORCHESTRATION`/`AGGREGATE_COVERAGE`)가 plan 거부 영수증(coverage)에
  기록된다. 동일 입력은 동일 stage로 분류된다.
- **plan parameter binding**: 유효 plan의 `max_chars`/`max_batches`/
  batch/chunk/atom 수가 `review_coverage["review_plan"]`에 기록되어
  동일 diff/policy에 대한 재현 비교가 가능하다.
- **binary/manual review 분류**: `BINARY_DIFF_REQUIRES_MANUAL_REVIEW`는
  `review_disposition=MANUAL_REVIEW_REQUIRED`,
  `manual_review_reason=BINARY_OR_NON_TEXT_ARTIFACT`를 남기고 generic
  reviewer infrastructure failure와 구분된다(예: `.xlsx`).
- **finalization-only retry**: `HarnessService._finalization_retry`가
  attempt-outcome persist, validation disposition persist, troubleshooting
  bundle, developer report, task state save, markdown report에 bounded
  2회 in-place 재시도를 적용한다. Worker/Build/Test/Review 결과는 이미
  bound된 상태이므로 **어떤 경로도 Worker를 재실행하지 않는다.**
  재시도 성공은 `FINALIZATION_STEP_RETRIED` 진단, 최종 실패는 기존
  typed code 유지.
- **typed no-delta classification (A/B/C)**:
  `WORKER_NO_CHANGE`(A, 기존 fail-closed 유지),
  `ALREADY_SATISFIED`(B, 기존 current-state 검증 경로),
  `VERIFICATION_ONLY`(C, contract가 명시적으로 `verification_only`를
  선언하고 요구사항이 명시 경로를 지목할 때 current-state 검증으로
  진행). source mutation을 요구하는 Job의 fail-closed는 불변.

## P1-4 Cross-rework review evidence reuse

- rework Job enqueue 시 선행 Job의 terminal task state에서
  `review-evidence/1` PASS units를 load해
  `semantic_prior_review_evidence`로 seed(1,000 units 상한)하고, Task
  생성 시 `inherited_review_evidence`로 전달한다.
- 재사용 판정은 기존 `fresh_units`/`partition` exact proof-input
  contract 그대로: same semantic root AND `acceptance_hash` 일치 AND
  `contract_revision` 일치 AND `execution_surface_hash` 일치 AND
  byte-identical chunk AND HIGH/CRITICAL depth 제외. 하나라도 어긋나면
  STALE/전량 재리뷰. 증가시키는 것은 재사용 횟수가 아니라 정확성이다.
- `review_coverage["review_evidence_reuse"]["prior_source"]`가
  `SAME_TASK_RETRY`/`SEMANTIC_FAMILY_JOB:<id>`로 provenance를 기록하고,
  기록 시 inherited units이 현재 round 아래로 chain된다.

## P1-5 Convergence telemetry (non-blocking)

- `convergence_signals.py`: durable job records에서만 계산하는 typed
  signals — `CONVERGENCE_REWORK_HIGH`(family rework ≥2),
  `CONVERGENCE_FINDING_RECURS`(동일 violation code ≥2회),
  `CONVERGENCE_SOURCE_CHURN`(동일 file ≥3회),
  `CONVERGENCE_VALIDATION_LOOP`(동일 candidate 재검증),
  `CONVERGENCE_DIFF_EXPANSION_NO_PROOF`(diff 증가 + 무성공).
- 새로운 LLM observer/정책 gate 없음. `record_result` 시
  `job["convergence_signals"]`로 기록·보고만 하고 canonical status를
  절대 바꾸지 않는다. root+1 rework 같은 정상 다단계 진행은 signal을
  내지 않는다(false drift 금지).
- canonical authority invariant: 모든 상태 mutation은 기존
  service/repository 권한 경유. report/projection이 SUCCESS를 주장해도
  canonical Job/execution state를 바꾸지 않는다(17.7 검증).

## P1-6 Rollback rehearsal

- `test_0992_rollback_rehearsal.py`: 실제 임시 git 저장소에서
  `B0 → Δ1 → Δ2 → (pre-existing dirty) → Δ3(modify/delete/create/stage)
  → rollback → tree-hash로 정확히 B0+Δ1+Δ2` 증명. HEAD 이동 시 롤백
  거부 포함. 실제 Product source는 전혀 건드리지 않는다.

## Release invariants (truthful scope)

```text
FALSE_SUCCESS = 0
UNSAFE_WRITER_OVERLAP = 0
PRIOR_BATCH_DELTA_LOSS = 0
RUNTIME_FAILURE_TO_PRODUCT_RETRY_LEAK = 0
NESTED_RECOVERY_DEPTH = 0
OLD_WRITER_QUIESCENCE_UNCONFIRMED_REPLACEMENT = 0   (0.99.1 유지)
ABORTED_LATE_RESPONSE_PUBLICATION = 0               (0.99.1 유지)
CONVERGENCE_SIGNAL_JOB_OUTCOME_MUTATION = 0         (0.99.2 신규)
SCHEMA_STAYS = 5                                    (lineage는 job payload)
```

NOT LIVE PROVEN: 영구 persistent session에 대한 실환경 장애 주입,
reviewer 모델 장애 시 최종 단계 복구 — fake/test adapter와 회귀 suite로만
증명(실환경 위험 주입 금지 원칙).

## Non-goals (DEFERRED_1_0)

```text
OMP / Pi / SoL-Pi worker bake-off
Multi-Worker / Worker Pool / Reviewer Pool / Stage Ticket scheduler
parallel candidate writer
Advisor / Advisor lifecycle / escalation runtime
Chrome DevTools MCP / Browser QA subsystem
DAILY → WEB_REVIEW, SLEEP → AUTO_REVIEWER, Review Package, Web Review Receipt
Direct candidate full validation/publication
Knowledge/RAG/Librarian, Claude-Mem, Serena production dependency
LangGraph top-level orchestration, generic Policy DSL, sandbox/network interception
agent society / recursive delegation / candidate tournament / complex ResourcePool
```

0.99.2의 convergence/quality signal은 review attention/telemetry 용도이며
새로운 통합 risk score나 destructive capability 판정이 아니다(Review Risk ≠
Execution Safety Risk 분리 유지).

## 검증

- Focused 0.99.2 acceptance: `test_0992_operational_convergence.py`(37) +
  `test_0992_rollback_rehearsal.py`(2) — runtime recovery/lineage/review
  plan determinism/evidence reuse/finalization retry/convergence
  non-blocking/authority/rollback rehearsal.
- Full integrated suite, deterministic Harness Eval gate
  (`test_099_harness_eval`), profile/control checks, release integrity
  (`releases/0.99.2/SHA256SUMS.json` missing=0 mismatch=0)는 release
  manifest에 기록된 대로 통과해야 한다.
- Live canary: OpenCode 1.18.30 MANAGED-local ephemeral-auth 스크립트
  (`tmp/canary_managed_local_ephemeral.py`) 재실행.

## 호환성

- SQLite schema 5 유지, 기존 control DB 마이그레이션 불필요.
- Job payload는 additive 필드만 추가(구 Job 그대로 load).
- `JobRequest.to_dict`는 신규 필드 사용 시에만 포함하므로 기존 요청
  fingerprint 불변.
- 기존 0.99.1 invariant 전부 보존(Section 1 대비 회귀 0).
