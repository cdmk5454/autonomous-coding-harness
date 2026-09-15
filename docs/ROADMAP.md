# Roadmap

This is the public roadmap for the sanitized Harness source export. Historical release notes may describe earlier plans; this file is the current public direction for the 0.99.2 milestone.

## 0.99.2 — Operational Convergence & Verification

Implementation is complete. This release stabilizes the 0.99.1 Lean Execution Kernel under real Product workload evidence rather than expanding the orchestration topology.

Shipped scope:

- bounded same-Job technical recovery for recoverable runtime failures
- explicit cross-Job semantic rework lineage without client-ID heuristics
- deterministic Review coverage-plan diagnostics and manual/binary review disposition
- finalization-only retry without rerunning Worker work
- exact-proof-input cross-rework Review evidence reuse
- non-blocking convergence telemetry derived from durable evidence
- real-git rollback rehearsal for prior accepted delta preservation
- SQLite control schema remains version 5

## 1.0 — Bounded Parallel Execution

Planned core:

- Stage Ticket / atomic claim
- frozen candidate
- Worker 1 + Reviewer 1 asynchronous execution first
- WORK / REVIEW / FIX handoff
- candidate isolation
- integration revalidation against the current canonical input
- WIP/backpressure
- one canonical publication path
- Job-scoped Review Package and candidate-bound Review Receipt
- DAILY/SLEEP review backend routing

Planned evaluation/optional scope:

- Worker runtime bake-off rather than hard-coded migration
- Advisor as an optional WORK/FIX optimization
- Browser QA capabilities only when operational evidence justifies activation

## 1.0.x — Operator UX

- operator projection/notification transport improvements
- Discord migration is an operator-UX concern, not a correctness-kernel dependency

## 1.1+ — Evidence-driven optimization

Conditional on measured repetition/cost:

- trajectory evaluation
- evidence/failure retrieval
- successful-pattern reuse
- context compression
- richer evaluation
- optional Knowledge/RAG/Librarian capabilities

Knowledge remains outside the control plane and must not block normal coding execution.
