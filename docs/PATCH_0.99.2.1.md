# Patch 0.99.2.1 — QA Backend Hotfix

Version: `0.99.2.1` · Build: `0.99.2.1+20260915.2` · SQLite schema: `5` (unchanged)

## Completion Hotfix (build `+20260915.2`)

Closes the QA false-success acceptance gaps found in independent verification
of `+20260915.1`. Version and schema unchanged; no feature work.

- **QA execution correlation (P0):** one canonical `qa_execution_id` flows
  adapter → provider invocation → execution artifacts → normalized
  `QAExecutionResult`; the fallback attempt reuses the parent id (identity
  lineage). Harness Verification compares it against the current identity.
- **Unique execution artifact boundary (P0):** per-execution directory
  `<scratch>/<qa_execution_id>/{momentic,stagehand}` plus a harness-written
  execution manifest; the `qa-*-adhoc` static result-path reuse is removed
  and stale reports/results are deleted before every run.
- **Stale artifact rejection (P0):** Momentic reports must contain the
  requested scenario `testId` and must not predate the execution manifest
  (`runs[0]` fallback forbidden); Stagehand results must exactly match
  `scenario_id` AND `qa_execution_id`. Violations →
  `QA_RESULT_CORRELATION_MISMATCH` (invalid infra result; another run's PASS
  is never reusable).
- **allowed_actions enforcement (P0):** deterministic step→action
  normalization (`goto/navigate→navigate`, `assert*→assert`,
  `click/doubleclick→click`, `type/fill/input→type`, `download→download`,
  `upload→upload`); a step whose normalized action is undeclared →
  `QA_SCENARIO_ACTION_NOT_ALLOWED` denied before any provider runs;
  `forbidden_actions` always wins over `allowed_actions`.
- **Secret-bearing scenario rejection (P1):** admission rejects
  secret-shaped literals (`QA_SCENARIO_SECRET_LITERAL_FORBIDDEN`) by reusing
  the existing `runtime_safety` scrub patterns (API keys, password/token
  literals, Bearer headers, JWTs, private keys, credential URLs); env
  references (`{{ env.X }}`) remain valid. Credentials belong in env /
  auth fixtures, never in canonical scenarios.
- **Materialization lifecycle:** generated Momentic YAML and Stagehand input
  are temporary execution artifacts, deleted after the run; sanitized
  results/evidence are preserved.
- **Release SHA coverage:** QA runtime integration files
  (`qa-runner.ts`, `opencode-go-deepseek-client.ts`, `run_stagehand.bat`,
  `run_npm24.bat`, `run_momentic.bat`, `momentic.config.yaml`, canary YAML)
  are bound in `releases/0.99.2.1/SHA256SUMS.json` (missing=0, mismatch=0).

Focused regression: `test_09921_qa_hotfix` 73/73 (stale Momentic/Stagehand
result, execution identity lineage, scenario mismatch, action policy, secret
safety, cleanup, manifest binding + all original coverage); Momentic
read-only short canary re-PASS; Stagehand short Browserbase re-run PASS
(invocation path changed). Full regression NOT_RUN (impact-scoped).

## Purpose

Browser QA became an immediate operational capability for the current
operator workflow. This bounded hotfix removes the generic Chrome MCP
(Chrome DevTools MCP) QA path and connects replaceable QA execution
providers into the existing Harness Verification:

```
Provider (Momentic / Stagehand)  -> evidence
QAAdapter                        -> normalize
Harness Verification             -> authority (unchanged)
```

No new QA lifecycle, queue, scheduler, or pool was introduced. The
synchronous QAAdapter call sits inside the existing `_run_pipeline` VERIFY
stage, after the candidate is frozen (Build + Test + Review already PASS).

## Provider Status

| Component | State |
|---|---|
| Momentic 3.53.0 (Node 24 wrapper, local Google Chrome, Asia/Seoul) | `MOMENTIC_RUNTIME_READY`, read-only canary PASS (example.com), primary QA execution PASS |
| Stagehand 4.1.0 (Browserbase) | `STAGEHAND_BROWSERBASE_READY`, fallback execution PASS (live) |
| Stagehand Local Chrome | optional P2, NOT_YET_VERIFIED, not a release blocker |
| OpenCode Go / DeepSeek V4.1 Flash (Stagehand ClientLLM) | `STAGEHAND_LLM_READY`, structured output ready, live PASS |
| DeepSeek V4.1 Flash as Harness Worker | roadmap candidate ONLY — default Worker routing unchanged |
| Playwright Test / Playwright MCP | preserved (deterministic regression / exact assertions) |
| generic Chrome DevTools MCP | REMOVED (capability + tester prompt + current docs) |

## Contracts

- `qa_browser_contract.py` — vendor-neutral `QAScenario` (canonical
  requirement; provider YAML/scripts are materializations only) and
  `QAExecutionResult` (provider/browser/model identity, status, binding
  hashes, assertions, artifacts, recovery/fallback flags, failure
  class/code, timing, redacted provider metadata).
- Binding: `candidate_hash`, `acceptance_hash`, `scenario_hash`,
  `execution_surface_hash` (reuses `gate_evidence.identity`). Mismatch →
  `STALE_QA_EVIDENCE`; QA PASS is never reusable across candidates.
- Statuses: `PASS`, `RECOVERED_PASS`, `FAIL`, `INFRA_FAILURE`. Provider raw
  statuses are never canonical.

## Failover (exact policy)

```
Momentic PASS             -> QA PASS
Momentic RECOVERED_PASS   -> QA RECOVERED PASS
Momentic assertion FAIL   -> QA FAIL (Stagehand fallback FORBIDDEN)
Momentic provider/infra   -> Stagehand fallback
Stagehand infra too       -> QA_INFRA_UNAVAILABLE -> AWAITING_QA (existing semantics)
```

## Failure classification

- Provider/infra: `QA_PROVIDER_QUOTA_EXHAUSTED`, `QA_PROVIDER_AUTH_FAILURE`,
  `QA_PROVIDER_SERVICE_UNAVAILABLE`, `QA_BROWSER_INFRA_FAILURE`,
  `QA_PROVIDER_TIMEOUT`, `QA_PROVIDER_COMPATIBILITY_ERROR`,
  `QA_SCHEMA_VALIDATION_ERROR` → `failure_type=infrastructure`,
  `failure_origin=TOOL_INFRA`, Product semantic retry delta = 0.
- Application: `QA_ASSERTION_FAILED`, `QA_FLOW_FAILED`,
  `QA_EXPECTED_ELEMENT_MISSING`, `QA_UNEXPECTED_BEHAVIOR` →
  `failure_type=qa`, `failure_origin=QA_FINDING` (semantic rework candidate;
  `_decide_retry_scope` keeps the reviewed tree).

## Activation

Jobs declare browser QA in the frozen test plan:

```json
"test_plan": {
  "browser_qa": {
    "required": true,
    "scenario_id": "SampleListEntry",
    "title": "SampleList entry",
    "acceptance": ["expected filter absent", "query action works", "result grid renders"],
    "allowed_actions": ["navigate", "click", "assert"],
    "forbidden_actions": ["save", "send", "delete"],
    "side_effect_class": "READ_ONLY",
    "target_profile": "sample-profile",
    "start_url": "...",
    "steps": []
  }
}
```

Absent declaration → `qa_required = false` → byte-identical 0.99.2 path
(no adapter construction, no provider process, no gate change).

## Side-effect safety

`allowed_actions`/`forbidden_actions`/`side_effect_class` are checked before
execution. `EXTERNAL_SEND`, `DB_MUTATION`, `DESTRUCTIVE`, `PAYMENT`,
`APPROVAL`, `PERMISSION_CHANGE`, `IRREVERSIBLE` deny execution with
`QA_SCENARIO_FORBIDDEN` before any provider runs. An AI browser provider
never acquires side-effect authority implicitly.

## Credentials

`MOMENTIC_API_KEY`, `BROWSERBASE_API_KEY`, and `OPENCODE_GO_API_KEY` are
process-local/provider-local only. QA evidence is structurally redacted
(`redact_credentials` + `scrub_secrets`); provider argv is
`assert_secret_free_argv`-checked. `OPENCODE_GO_SESSION_ID` is generated per
QA execution (`harness-qa-<qa_execution_id>`), never static.

## Files

- New reviewable public surface: `qa_browser_contract.py`,
  `qa_browser_adapter.py`, `qa_browser_providers.py`, provider integration
  source/config, focused public QA tests, this doc, and
  `docs/QA_BROWSER_PROVIDERS_09921.md`.
- Canonical/private build additionally wires the QA gate into the existing
  Manager/Verification path. This public export intentionally omits private
  runtime state, provider auth, raw execution evidence, and Product-specific
  materialization.

## Validation (impact-scoped)

- Expanded canonical QA-focused suite: 73 tests.
- Independent Linux verification: 72/73, with the single failure caused by a
  Windows-path-only release-manifest test; normalized SHA recheck: 97/97.
- Directly impacted 0.99.2 regression surface: 143/143 PASS.
- Live canonical evidence: Momentic read-only/primary PASS and Stagehand
  Browserbase fallback PASS.
- `FULL_REGRESSION_NOT_RUN` — reason = impact-scoped QA hotfix.

## Non-goals (unchanged)

Multi-Worker, Worker/Reviewer pools, Stage Ticket, async QA/Review queues,
OMP/Pi/SoL-Pi, default DeepSeek Worker switch, Advisor, Discord,
Direct Agent publication, Knowledge/RAG, LangGraph control-plane, new
generic ResourcePool, new QA lifecycle hierarchy, generic sandbox/network
policy system.
