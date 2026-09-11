# Autonomous Coding Harness

Production-oriented execution layer for AI coding agents.

Current stable milestone: **0.99.1 — Lean Execution Kernel**

## Problem

AI coding agents can produce useful changes while leaving execution state, ownership, recovery, and verification ambiguous. The Harness gives those actions a durable control boundary.

## What this Harness does

It materializes an immutable Job contract, selects a validated project profile, runs a fenced Worker, records Git and execution evidence, applies Build/Test/Review/Verification gates, and preserves candidates when recovery or human action is required.

## Architecture overview

The Python source tree contains the control repository, Job and Task state models, queue and supervisor, runtime adapters, Worker, Planner, Tester, Reviewer, Git collection and commit handling, risk controls, rollback and snapshot custody, and evidence projections. `docs/` explains the contracts and lifecycle.

## Core invariants

- One canonical writer owns a materialized execution surface.
- Job scope, profile, runtime contract, and relevant policy are frozen before execution.
- Unknown ownership, path escape, stale revision, missing evidence, and unsafe capability requests fail closed.
- A failed or interrupted attempt preserves its candidate and prior delta.
- Review evidence is bound to the exact contract, execution surface, and changed atoms.
- Technical recovery does not consume product retry budget.

## Current 0.99.1 status

`0.99.1` implementation is complete. The current phase is **Product workload stabilization / evidence collection**. The release metadata identifies build `0.99.1+20260910.3` and records the release acceptance result.

The release covers the Lean Execution Kernel: correctness, recovery, ownership, rollback, review evidence, truthful runtime capability reporting, typed autonomous capability admission, and derived timing from durable timestamps.

## Validation evidence

The canonical/private Harness release records `0.99.1` implementation complete, an integrated suite result of `1524/1524 PASS`, release acceptance evidence, and a managed local runtime canary. That evidence belongs to build `0.99.1+20260910.3` and is retained separately under `release-evidence/`; it is not presented as a result reproduced from this sanitized tree.

The public sanitized export has its own reproducible gates. On the exported tree, the focused command below runs 155 tests with import and collection errors at zero; provider-dependent checks are explicit skips when their CLIs are unavailable. The deterministic corpus runs 50 cases with `release_gate=PASS`. Exact results and the validation environment are recorded in `PUBLIC_EXPORT_REPORT_FINAL.md` distributed beside the archive.

## Quick Start

```text
python -m unittest discover -s tests -p "test_099*.py"
python scripts/run_deterministic_eval.py
```

These commands make no provider or LLM call. The runtime itself expects a project profile and common rule surface supplied through explicit configuration such as `AGENTS_DIR` and `AGENT_PROFILE_DIR`. A fully synthetic contract example is included in `examples/sample-profile`; no real project profile, credentials, control database, or execution history is included here.

## Repository structure

- Root Python modules: Harness runtime and control-plane implementation.
- `tests/`: focused 0.99.1 regressions and synthetic control fixtures.
- `evals/`: deterministic evaluation corpus contract.
- `scripts/`: public deterministic validation entry point.
- `examples/`: synthetic profile and Job contracts.
- `docs/`: current architecture, runtime, operations, release, and roadmap material.
- `releases/0.99.1/`: public export metadata and current-tree SHA-256 manifest.
- `release-evidence/0.99.1/`: clearly labeled canonical/private release evidence.

## Safety and scope limitations

This export is a public source review package. It does not include `.git`, profiles, raw prompts, raw Jobs or Tasks, operator sessions, snapshots, checkpoints, rollback backups, incident bundles, customer or institutional data, or provider credentials. Runtime integrations are adapters; provider capability is reported as supported, partial, or unsupported according to the installed surface.

`1.0` Bounded Parallel Execution remains a roadmap item; bounded parallel Worker and Reviewer execution, candidate isolation, and single canonical publication are not claimed as implemented.

Knowledge/RAG belongs to the optional `1.1+` evidence-driven optimization roadmap and is also not implemented by this release.

## Roadmap

- **0.99.1:** Lean Execution Kernel stabilization.
- **1.0:** Bounded Parallel Execution with Stage Ticket, frozen candidates, asynchronous Worker and Reviewer, candidate isolation, Review Package and candidate-bound Review Receipt, WIP/backpressure, integration revalidation, and one canonical publication.
- **1.1+:** Evidence Retrieval, Knowledge/RAG/Librarian capabilities, richer evaluation, context compression, and evidence-driven optimization.
