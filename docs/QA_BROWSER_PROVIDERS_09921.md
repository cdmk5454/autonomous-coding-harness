# Browser QA Providers (0.99.2.1)

Ownership (fixed):

```
Momentic / Stagehand -> QA execution/evidence provider
QAAdapter            -> provider result normalization
Harness Verification -> canonical PASS / FAIL / AWAITING_QA authority
```

A provider never sets `task.status = SUCCESS`, never decides Job success,
and its raw verdict is never canonical Product completion.

## Execution identity & correlation (build +20260915.2)

One canonical `qa_execution_id` correlates the whole execution:

```
QAAdapter (generates / accepts the id)
  -> provider invocation (qa_execution_id kwarg)
  -> per-execution artifact boundary <scratch>/<qa_execution_id>/{momentic,stagehand}
  -> harness execution manifest (manifest.json: qa_execution_id, scenario_id,
     scenario_hash, started_epoch)
  -> normalized QAExecutionResult
  -> Harness Verification identity comparison
```

- The Stagehand fallback attempt reuses the same parent `qa_execution_id`.
- Every independent execution gets a brand-new artifact boundary; stale
  reports/results are deleted before the run (`qa-*-adhoc` static-path reuse
  removed).
- Stale artifact rejection: a Momentic report without the requested scenario
  `testId`, or predating the execution manifest, and any Stagehand result
  whose `scenario_id`/`qa_execution_id` does not exactly match, is rejected
  with `QA_RESULT_CORRELATION_MISMATCH`. Another run's PASS never satisfies
  the current execution (`runs[0]` fallback forbidden).
- Generated provider inputs (Momentic YAML under `integrations/momentic/qa/`,
  Stagehand `input.json`) are temporary execution artifacts and are cleaned
  up after the run; sanitized results stay as evidence.

## Typed action contract (build +20260915.2)

Steps normalize deterministically: `goto/navigate→navigate`,
`assert/assert_contains/assert_visible→assert`, `click/doubleclick→click`,
`type/fill/input→type`, `download→download`, `upload→upload`. A step whose
normalized action is not declared in `allowed_actions` is denied before any
provider runs (`QA_SCENARIO_ACTION_NOT_ALLOWED`); `forbidden_actions` always
wins over `allowed_actions`. Secret-shaped literals are rejected at scenario
admission (`QA_SCENARIO_SECRET_LITERAL_FORBIDDEN`, reusing the existing
`runtime_safety` scrub patterns); env references stay valid and credentials
belong in process-local env / auth fixtures.

## Topology

```
Worker -> Build/Test -> Reviewer -> candidate frozen
  -> QA required?
       NO  -> (0.99.2 path, unchanged)
       YES -> QAAdapter
              |- MomenticProvider (primary)   -> local Google Chrome
              `- StagehandProvider (fallback) -> Browserbase
                    -> OpenCode Go -> DeepSeek V4.1 Flash
       -> QAExecutionResult (candidate-bound evidence)
  -> Harness Verification -> SUCCESS / FIX / AWAITING_QA
```

## Current status

- Momentic 3.53.0 — `MOMENTIC_RUNTIME_READY`; read-only canary PASS
  (`integrations/momentic/canary/harness-canary-readonly.test.yaml`,
  example.com, `--browser chrome`); primary provider enabled.
- Stagehand 4.1.0 — `STAGEHAND_BROWSERBASE_READY` / `STAGEHAND_LLM_READY` /
  `STAGEHAND_STRUCTURED_OUTPUT_READY`; Browserbase fallback live PASS.
- Stagehand Local Chrome — optional P2, `NOT_YET_VERIFIED`. If it later
  passes a canary, preference may become Local-first, Browserbase as
  remote/CI/final fallback. Not a 0.99.2.1 blocker.
- Playwright Test — deterministic regression / exact assertions (unchanged).

## Runtime boundaries

- Momentic runs only through `integrations/momentic/run_momentic.bat`
  (Node 24.15.0 wrapper). Never `nvm use 24`, never global Node mutation,
  never changes the Product Node 18 frontend runtime.
- Stagehand runs only through `integrations/stagehand/run_stagehand.bat`
  (Node 24 + tsx) executing `qa-runner.ts` with the DeepSeek ClientLLM
  (`opencode-go-deepseek-client.ts`).
- `OPENCODE_GO_SESSION_ID = harness-qa-<qa_execution_id>` per QA execution;
  stable within one execution, new for each new execution/attempt.

## Scenario contract

Canonical requirement = vendor-neutral `QAScenario`
(`qa_browser_contract.py`). Provider files are materializations:

- Momentic: `materialize_momentic_test` writes
  `integrations/momentic/qa/<scenario>.test.yaml` (native separators;
  project-scoped config/include/cache) and runs it with
  `--disable-cache --browser chrome` + JSON reporter.
- Stagehand: input JSON (scenario + session) → `qa-runner.ts` →
  structured result JSON (assertions, failure_code, session metadata).

## Credentials

Process-local environment / provider-local dotenv only. Never in Git,
`.tasks`, Control DB payloads, scenarios, reports, test results, or release
evidence. QA evidence passes `redact_credentials` + `scrub_secrets`;
provider argv passes `assert_secret_free_argv`. Current Momentic login-based
auth is for dev/canary; the structure stays API-key ready
(`MOMENTIC_API_KEY` / `--api-key`).

## Side-effect safety

Enforced pre-execution by `SideEffectPolicy`: forbidden classes
(EXTERNAL_SEND, DB_MUTATION, DESTRUCTIVE, PAYMENT, APPROVAL,
PERMISSION_CHANGE, IRREVERSIBLE) and default forbidden action tokens
(save/delete/send/approval/payment/submit and configured localized equivalents)
deny with `QA_SCENARIO_FORBIDDEN` before any provider subprocess starts.
Explicit operator authorization is the only override path.

## Normalization and failover

- `PASS` / `RECOVERED_PASS` (Momentic recovery recorded distinctly) /
  `FAIL` / `INFRA_FAILURE`.
- Application failure (`QA_ASSERTION_FAILED`, `QA_FLOW_FAILED`,
  `QA_EXPECTED_ELEMENT_MISSING`, `QA_UNEXPECTED_BEHAVIOR`) → terminal QA
  FAIL; fallback cannot overwrite it.
- Provider/infra failure (quota/auth/service/browser/timeout/compatibility/
  schema) → Stagehand fallback; both failing → `QA_INFRA_UNAVAILABLE` →
  existing `AWAITING_QA` human gate; never a Worker retry.

## Verification binding

`QAExecutionResult` carries `candidate_hash`, `acceptance_hash`,
`scenario_hash`, `execution_surface_hash` (from `gate_evidence.identity`).
Verification compares against current state; mismatch →
`STALE_QA_EVIDENCE` and SUCCESS stays forbidden. QA required + no valid
current evidence → `verification_status != VERIFIED` → `AWAITING_QA`.
