# Roadmap

This is the current roadmap for the public source export. Historical release notes may describe superseded plans.

## Released

### 0.99.2.1 — QA Backend Hotfix

Implementation acceptance is complete. Browser QA is provider-backed but remains subordinate to Harness Verification. Momentic is primary; Stagehand v4 is the provider/infrastructure fallback. Stale provider results, undeclared actions, secret literals, and missing/stale QA evidence fail closed. Playwright remains the deterministic regression path.

### 0.99.2 — Operational Convergence & Verification

Completed bounded same-Job technical recovery, explicit cross-Job semantic rework lineage, review/finalization stabilization, strict evidence reuse, convergence telemetry, and rollback rehearsal.

### 0.99.1 — Lean Execution Kernel

Completed the single-writer correctness kernel, current-custody snapshot behavior, bounded runtime recovery, evidence-proportional review reuse, and false-success protections.

## Operational acceptance before 1.0

Use real Jobs to measure accepted throughput, QA latency, provider fallback/infra failures, semantic rework, review failures, operator interventions, and false-success rate. New blocking defects may receive a narrow 0.99.2.x hotfix; feature expansion moves to 1.0.

## 1.0 — Bounded Parallel Execution

Planned core:

- Stage Ticket / minimal executable work record
- frozen candidate
- Worker 1 + Reviewer 1 asynchronous first
- `WORK <-> REVIEW <-> FIX`
- isolated candidate writers with one canonical publication authority
- WIP limits and backpressure
- integration revalidation against current canonical state
- job-scoped Review Package and candidate-bound Review Receipt
- DAILY -> Web Review / SLEEP -> Auto Reviewer backend routing
- Worker runtime bake-off under identical Job/snapshot/Acceptance/Review/Verification conditions

Worker bake-off candidates include the current OpenCode baseline, Pi/OMP, Pi + SoL-Pi, and OpenCode Go / DeepSeek V4.1 Flash. DeepSeek is a candidate, not the 0.99.2.1 default Worker.

Optional 1.0 work after the core stabilizes may include Advisor escalation, additional Worker/Reviewer capacity, async QA Stage Tickets, and Direct candidate validation/publication.

## 1.0.x

Operator UX and notification transport improvements, including a possible Telegram-to-Discord adapter migration without changing canonical execution authority.

## 1.1+

Evidence-driven trajectory evaluation, retrieval, context compression, richer evaluation, and optional Knowledge/RAG/Librarian capabilities only when operational data proves they are useful.
