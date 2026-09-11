# Changelog

## 0.99.1

- Release-acceptance completion (`0.99.1+20260910.3`): a persistent OpenCode
  server technical failure now aborts the native session and confirms
  terminal/idle/lost within a bound before any replacement canonical writer is
  allowed; an unconfirmed abort denies the replacement writer
  (`OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED`) with the candidate preserved and
  zero product retry, and an aborted command's late assistant response is
  never published as the current result.
- The normal Job snapshot path now answers current custody only
  (`candidate_ownership(current_only=True)`): no verified-candidate-history
  reconstruction and no SUCCEEDED/SKELETON_READY successful-Job genealogy scan
  during normal admission. Historical verification remains available for
  incident/operator forensic paths.
- An explicitly approved LOW deterministic review oracle (typed immutable
  `test_plan.review_oracle=DETERMINISTIC_ORACLE` with mandatory tests, bound
  oracle execution identity, fresh PASS BUILD/TEST gates, no hard flags or QA
  hold) may skip the separate LLM Reviewer with 0 invocations while leaving a
  REVIEW_PASS receipt bound to the oracle evidence hash. LOW risk alone, stale
  oracle evidence, or any missing condition keeps the LLM review.
- The OpenCode progress watchdog now resets only on turn-correlated semantic
  change (assistant text/part deltas, tool state transitions, correlated
  message/finish transitions, session state transitions); heartbeat,
  timestamp/usage metadata, unrelated messages, and metadata-only mutations no
  longer reset it.
- Fixed the MANAGED local auth contract: only a configured (remote) managed
  endpoint requires an explicit password; a harness-owned local server started
  without one generates an ephemeral per-instance credential as documented.
- External baseline adoption now records and requires its truthful boundary
  (`validation_boundary=BASELINE_ADOPTION_ONLY`,
  `generation_provenance=UNKNOWN`); Direct validated publication is explicitly
  DEFERRED_1_0, and the release invariants distinguish the proven typed
  capability admission floor
  (`DECLARED_DANGEROUS_CAPABILITY_AUTONOMOUS_ADMISSION=0`) from the unproven
  interception of undeclared arbitrary side effects.
- Corrected the OpenCode capability contract to be truthful (G1): pending
  approval/question observation is PARTIAL (no bound SSE observer); a
  permission wait surfaces as a technical no-progress timeout with zero
  product retry, never as a product defect.
- Preserved the adapter's original OpenCode failure code end-to-end instead
  of collapsing every runtime-phase failure to `OPENCODE_TRANSPORT_ERROR`;
  extended the IPC failure set so classification stays harness-caused
  infrastructure/RUNTIME_RECOVERY.
- Removed the managed-server pipe dead-lock risk (server stdout/stderr now
  DEVNULL) and added ephemeral per-instance authentication for harness-owned
  local OpenCode servers (OpenCode >=1.18.30 rejects unauthenticated loopback
  requests); live canary PASS on 1.18.30.
- Added a typed `operation_capabilities` contract floor: production DB
  apply/write, real external send, real payment, destructive/irreversible
  external operations, and PII egress are denied for autonomous execution by
  explicit typed request only. Requirement text is never keyword-scanned for
  capability decisions.
- Added evidence-proportional review reuse over the existing DiffBatch atom
  ledger: exact-input (acceptance/contract/execution-surface + byte-identical
  chunk) prior PASS units are REUSED_VALID_EVIDENCE; changed, failed, or
  ambiguous scopes re-review. Reuse is disabled for HIGH/CRITICAL depth and
  cleared on checkpoint-restored epochs; atom coverage stays lossless so a
  coverage hole can never pass.
- Added the derived timing breakdown (queue wait, admission, worker, git,
  build/test, review, verification, harness maintenance) from existing
  durable timestamps only; human wait is recorded at acknowledgement
  (`acknowledgement_wait_seconds`, `job_lead_time`). Nothing is estimated.
- Repaired drifted test stubs (`test_0916_*`, `test_skeleton_policy`,
  `test_09004_hotfix`) to the current candidate-ownership/attempt-reservation
  contracts.

## 0.9.0.6

- Aligned the Droid model registry with the current Droid custom model IDs:
  Thinking, Flash Thinking, and Flash Fast, with the primary Thinking model as
  the default coding and fail-closed execution target.
- Added a `MODEL_ALIAS_COMPATIBILITY` alias resolving the retired
  `custom:GLM-5.3-[Z.AI-Coding]-0` ID to the canonical Thinking model at
  dispatch time without rewriting immutable requested-model evidence.
- Kept unknown Droid models fail-closed across contract validation, routing,
  dispatch, and manifest normalization; retired non-aliased IDs are rejected.
- Centralized Droid invocation defaults (worker, planner, tester) on the
  canonical registry instead of hard-coded retired IDs.
- Verified no Harness-side reasoning override exists on the Droid invocation
  path; per-model thinking settings remain owned by the Droid configuration.

## 0.9.0.5

- Added a revision-safe anchored replacement primitive for `FAILED_FINAL`
  corrective Jobs and pristine pre-execution `QUEUED` contract replacement.
- Preserved append-only history and sibling lineage while separating persisted
  enqueue order from effective replacement scheduling anchors.
- Added verified cross-Job corrective checkpoint reuse, including safe EOL-only
  prestate recognition and durable PREPARING recovery.

## 0.9.0.4

- Added hash-guarded QA candidate isolation for structured decision holds,
  including a Worker-free recovery path for preserved `AWAITING_QA` deltas.
- Preserved business QA and coincident Reviewer infrastructure provenance as
  separate failure dimensions.
- Preserved trailing blank context prefixes in collected unified diffs so
  strict Reviewer hunk coverage remains lossless.
- Apply resolved candidates only after pre-Job baseline verification, permit
  bounded autonomous technical recovery for the resulting Harness-only
  checkpoint failure, and avoid re-quarantining a resolved strict success.
- Accept exact no-QA sentinels and resume an exit-zero Worker execution stopped
  only by QA parsing at Git, Build, and Review without rerunning Worker.
- Validate post-Worker recovery against the complete snapshot delta while
  retaining the canonical exclusion of already-owned Batch paths.
- Record post-Worker recovery Reviewer verdicts on the same Job lineage,
  including source findings, without inventing a Worker retry.
- Rehydrate a completed post-Worker verdict after a Queue-record-only failure
  without rerunning Build, Review, or Worker.

## 0.9.0.2 - 2026-09-01

- 동일 TaskState concurrent writer를 path-scoped serialization하고 Windows WinError 5/32 replace를 bounded retry한다.
- control failure의 stable code/field와 HARNESS_CAUSED/BATCH safety-hold origin을 보존한다.
- Worker/Git regeneration 없는 BUILD→REVIEW canonical control recovery를 기존 Reviewer-only operator에 추가한다.
- explicit structured conditional QA와 existing candidate/dependency quarantine를 연결한다.
- durable terminal/gate transition 뒤 Telegram attempt와 semantic revision duplicate suppression을 추가한다.
- `Authoritative scope`를 authorization으로 오인한 `auth` substring risk defect를 수정한다.

## 0.9.0.1 - 2026-09-01

- 기존 `supersede-external-handoff`를 AWAITING_QA/CANCELLED member까지 revision-safe하게 일반화했다.
- AWAITING_QA의 historical Task result와 candidate/snapshot evidence를 변경하지 않고 Job만 non-success terminal로 종결한다.
- 이미 CANCELLED인 member는 다시 쓰지 않으며, old gate를 해제한 뒤 기존 operator pause를 `NEW_BATCH_READY` dispatch hold로 재사용한다.
- unexpected runtime dirty가 있으면 supersede와 ready 전환을 함께 거부한다.

## 0.9.0 - 2026-09-01

- immutable Job contract를 scope/build authority로 고정해 requirement 경로 기반 module contamination을 제거했다.
- 반복 prompt 제약을 policy reference로 줄이고 deterministic LOW/MID/HIGH/CRITICAL Reviewer model/context policy를 추가했다.
- failure origin을 분류해 Harness/infra 장애가 Worker source remediation으로 전달되지 않게 했다.
- Queue revision과 독립된 ProgressSnapshot, silence/30분 UX heartbeat 및 Telegram delivery evidence를 추가했다.
- 기존 Worker/Reviewer `Popen` lifecycle owner에 execution identity, 15초 runtime heartbeat/45초 lease, stdout/stderr progress와 stale/stalled detection을 연결했다. Restart 시 lease-expired + exact PID dead + snapshot/baseline integrity가 확인돼도 immutable crash salvage와 focused-recovery handoff를 먼저 남긴 뒤에만 기존 INTERRUPTED/PRESERVED reconciliation으로 넘긴다.
- Technical/profile recovery를 포함한 모든 outer execution을 `max_outer_attempts` global hard cap 안에 두고, 과거 overflow는 rewrite 없이 invalid lineage로 투영한다.
- Computed/effective Reviewer risk, non-overridable policy defaults와 stricter overlays, compact raw execution lineage, 1.0 bootstrap 전 evidence retention을 추가했다.
- `qa_type`/`hold_scope`, candidate quarantine, dependency-aware scheduling, focused rebase와 integration-gated promotion을 추가했다.
- Reviewer infrastructure 전용 `retry_review_only` operator를 Telegram과 MCP에 revision-safe하게 노출했다.
- 기존 Task/event/artifact에서 additive execution summary를 생성하고 legacy records를 default-safe하게 유지했다.
- Historical note (superseded): an earlier roadmap placed Knowledge/RAG in the next major milestone. The current roadmap places it in 1.1+.

## 0.8.5.2 - 2026-08-30

- 승인된 Agent Harness context/policy 변경만 허용하는 revision-safe `PROFILE_DRIFT` revalidation operator를 추가했다.
- current execution context를 서버에서 재계산하고 request, project, baseline, predecessor, successor 및 release provenance 불변성을 검증한다.
- context rebind와 `BLOCKED → QUEUED` 전이를 하나의 Job revision에 기록하고 Queue는 ATTENDED/paused로 유지한다.
- preview, idempotent mutation, stale revision 및 semantic/unknown drift fail-closed 회귀 테스트를 추가했다.

## 0.8.5.1 - 2026-08-30

- ATTENDED cumulative success의 자동 acknowledge와 다음 Job 자동 claim을 차단했다.
- BATCH_FINAL_COMMIT의 delta 보존·commit defer와 execution release 결정을 분리했다.
- 기존 `continue_after_success`에 Job revision과 Batch ID 검증을 추가했다.
- 동일 request replay는 단일 event/dispatch로 유지하고 stale revision은 fail-closed한다.
- AUTONOMOUS cumulative auto-continue와 checkpoint/rollback 계약은 유지한다.

## 0.8.5 - 2026-08-30

- 시작 시 고정되는 runtime identity와 release manifest 검증을 추가했다.
- 승인 요구사항에서 결정적 TaskSpec과 content-addressed ContextManifest를 생성한다.
- 공통·프로필·역할·영역 규칙과 검증된 사실을 출처·우선순위·예산에 따라 선택한다.
- Worker, Planner, Tester, Reviewer가 동일한 권위 순서와 역할별 context pack을 사용한다.
- 변경 파일 기반 context refresh와 TaskState pack lineage를 추가했다.
- Golden Context Eval 8개 시나리오와 0.8.5 전용 32개 회귀 테스트를 추가했다.
- 기존 Queue schema, 누적 Batch, rollback, deferred final commit 계약을 유지한다.

## 0.8.4 - 2026-08-30

- 누적 Batch checkpoint/rollback, restore epoch, structured Build evidence를 안정화했다.
