# Autonomous Coding Harness

Production-oriented execution and verification layer for AI coding agents.

Current stable public milestone: **0.99.2 — Operational Convergence & Verification**

## Problem

AI coding agents can produce useful changes while leaving execution state, writer ownership, recovery, semantic retry, and verification ambiguous. The Harness gives those actions a durable control boundary and keeps Product outcome separate from runtime/tool failure.

## What this Harness does

It materializes an immutable Job contract, selects a validated project profile, runs a fenced Worker, records Git and execution evidence, applies Build/Test/Review/Verification gates, preserves candidates across recovery, and keeps technical replacement separate from Product semantic rework.

## Core invariants

- One canonical writer owns a materialized execution surface.
- Job scope, profile, runtime contract, and relevant policy are frozen before execution.
- Unknown ownership, path escape, stale revision, missing evidence, and unsafe capability requests fail closed.
- A failed or interrupted attempt preserves its candidate and prior accepted delta.
- Review evidence is reusable only while its exact proof inputs remain current.
- Technical recovery does not consume Product semantic retry/rework budget.
- Activity is not treated as meaningful progress; meaningful progress is not treated as proof of goal convergence.
- Reports and operator projections are not a second execution authority.

## Current 0.99.2 status

`0.99.2` implementation acceptance is complete. It keeps the 0.99.1 Lean Execution Kernel and adds operational stabilization from real workload evidence:

- bounded same-Job runtime technical recovery
- explicit cross-Job semantic rework lineage
- deterministic Review coverage-plan diagnostics
- manual/binary review disposition
- finalization-only retry without Worker rerun
- conservative cross-rework Review evidence reuse
- typed, non-blocking convergence telemetry
- real-git rollback rehearsal

SQLite control schema remains `5`. The release does **not** claim Multi-Worker, Stage Ticket, Advisor, Web Review, or Knowledge/RAG as implemented.

## Validation evidence

The canonical/private 0.99.2 release records build `0.99.2+20260914.1`, a `1563/1563` integrated Windows suite, deterministic evaluation release-gate PASS, and an OpenCode 1.18.30 managed-local canary. Those canonical records are not presented as if reproduced from this sanitized repository.

An independent verification pass over the 0.99.2 review bundle re-ran the focused 0.99.2 acceptance suite (`39/39 PASS`), rechecked release hashes (`152/152 MATCH`), and re-ran the deterministic evaluation gate. The full Windows/provider-dependent suite was not reproduced in that Linux verification environment. See `docs/PUBLIC_VALIDATION_REPORT_0.99.2.md`.

## Quick Start

```text
python -m unittest discover -s . -p "test_099*.py"
python scripts/run_deterministic_eval.py
```

These public gates use synthetic fixtures and make no claim that private provider/live-runtime checks are reproducible without provider-specific configuration.

## Repository structure

- Root Python modules: runtime/control implementation.
- `test_*.py`: focused regressions and synthetic control fixtures.
- `evals/`: deterministic evaluation corpus.
- `scripts/`: public deterministic validation entry point.
- `examples/`: synthetic profile and Job contracts.
- `docs/`: architecture, operations, release notes, roadmap, and anonymized case study.
- `releases/`: sanitized public release metadata and hash manifests.
- `release-evidence/`: sanitized summaries of canonical/private acceptance evidence.

## Safety and scope limitations

This repository is a public source review package. It excludes real project profiles, raw Jobs/Tasks, `.control`, `.tasks`, control databases, snapshots/checkpoints, operator sessions, customer/institutional data, credentials, auth files, and Product source/evidence. Provider integrations are adapters; capability claims are limited to the surfaces actually validated by the corresponding release.

## Roadmap

- **0.99.2:** Operational Convergence & Verification — complete.
- **1.0:** Bounded Parallel Execution — Stage Ticket, frozen candidate, asynchronous Worker/Reviewer handoff, candidate isolation, integration revalidation, WIP/backpressure, and single canonical publication.
- **1.0.x:** operator UX/notification transport improvements.
- **1.1+:** evidence-driven evaluation/retrieval/knowledge optimizations when real usage proves the need.
