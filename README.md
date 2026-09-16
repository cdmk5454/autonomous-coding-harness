# Autonomous Coding Harness

Production-oriented execution and verification layer for AI coding agents.

Current stable public milestone: **0.99.2.1 — QA Backend Hotfix**

## Problem

AI coding agents can produce useful changes while leaving execution state, writer ownership, recovery, semantic retry, browser QA, and verification ambiguous. The Harness gives those actions a durable control boundary and keeps Product outcome separate from runtime/tool failure.

## What this Harness does

It materializes an immutable Job contract, selects a validated project profile, runs a fenced Worker, records Git and execution evidence, applies Build/Test/Review/QA/Verification gates, preserves candidates across recovery, and keeps technical replacement separate from Product semantic rework.

## Core invariants

- One canonical writer owns a materialized execution surface.
- Job scope, profile, runtime contract, and relevant policy are frozen before execution.
- Unknown ownership, path escape, stale revision, missing evidence, and unsafe capability requests fail closed.
- A failed or interrupted attempt preserves its candidate and prior accepted delta.
- Review and QA evidence are reusable only while their exact proof inputs remain current.
- Technical/provider recovery does not consume Product semantic retry/rework budget.
- Provider success never directly sets canonical Job success; Verification remains the authority.
- Activity is not meaningful progress; meaningful progress is not proof of goal convergence.
- Reports and operator projections are not a second execution authority.

## Current 0.99.2.1 status

`0.99.2.1` implementation acceptance is complete. It keeps the 0.99.2 Operational Convergence & Verification release and adds a replaceable browser-QA backend:

- vendor-neutral `QAScenario` and candidate-bound `QAExecutionResult`
- Momentic as the primary browser QA provider
- Stagehand v4 + Browserbase fallback for provider/infrastructure failure
- exact `qa_execution_id` / scenario correlation to reject stale provider results
- assertion/application failure remains terminal and cannot be overwritten by fallback success
- typed allowed/forbidden action admission and pre-execution side-effect denial
- secret-literal rejection before scenario persistence/materialization
- missing/stale QA evidence blocks false success
- Playwright deterministic regression path remains separate and preserved

SQLite control schema remains `5`. Multi-Worker, Stage Ticket, Advisor, async QA scheduling, Discord migration, and Knowledge/RAG remain outside this release.

## Validation evidence

The canonical/private release records build `0.99.2.1+20260915.2`. Independent verification re-ran the expanded QA-focused suite (72/73 in a Linux environment, with the single failure caused by a Windows-path-only manifest test), manually rechecked release hashes as `97/97 MATCH`, reproduced the previously reported stale-result/action/secret guards, and re-ran the directly impacted 0.99.2 regression surface as `143/143 PASS`. The public repository CI remains Windows-based. See `docs/PUBLIC_VALIDATION_REPORT_0.99.2.1.md`.

## Quick Start

```text
python -m unittest discover -s . -p "test_099*.py"
python scripts/run_deterministic_eval.py
```

Provider-specific live QA requires external credentials and browser/provider configuration; no credentials or private execution evidence are included here.

## Repository structure

- Root Python modules: runtime/control implementation.
- `test_*.py`: focused regressions and synthetic control fixtures.
- `integrations/momentic/`: Momentic adapter runtime/configuration surface.
- `integrations/stagehand/`: Stagehand fallback runtime and OpenAI-compatible ClientLLM adapter.
- `evals/`: deterministic evaluation corpus.
- `scripts/`: public deterministic validation entry point.
- `examples/`: synthetic profile and Job contracts.
- `docs/`: architecture, operations, release notes, roadmap, and anonymized case study.
- `releases/`: sanitized public release metadata and hash manifests.
- `release-evidence/`: sanitized summaries of canonical/private acceptance evidence.

## Safety and scope limitations

This repository is a public source review package. It excludes real project profiles, raw Jobs/Tasks, `.control`, `.tasks`, control databases, snapshots/checkpoints, operator sessions, customer/institutional data, credentials, auth files, private browser reports, and Product source/evidence.

## Roadmap

- **0.99.2.1:** QA Backend Hotfix — complete.
- **Operational acceptance:** collect real Job/QA data before 1.0.
- **1.0:** Bounded Parallel Execution — Stage Ticket, frozen candidate, Worker 1 + Reviewer 1 async first, candidate isolation, integration revalidation, WIP/backpressure, Review Package/Receipt, DAILY/SLEEP review routing, and Worker runtime bake-off.
- **1.0.x:** operator UX/notification transport improvements.
- **1.1+:** evidence-driven evaluation/retrieval/knowledge optimization when real usage proves the need.
