# Public Validation Report — 0.99.2.1

## Release

- Canonical version: `0.99.2.1`
- Canonical private build: `0.99.2.1+20260915.2`
- Control schema: `5` (unchanged)
- Public export: sanitized source and reproducible non-secret integration configuration only

## Independent verification

The verification bundle was inspected independently after the completion hotfix.

- Previously reproduced stale Momentic-result false success: **closed**
- Previously reproduced stale Stagehand-result false success: **closed**
- QA execution/scenario correlation: **PASS**
- `allowed_actions` / forbidden-action admission: **PASS**
- secret-literal scenario rejection: **PASS**
- release SHA coverage: **97/97 MATCH, 0 missing, 0 mismatch** after Windows path normalization
- directly impacted 0.99.2 regression surface: **143/143 PASS**

The expanded QA-focused suite contains 73 tests. In the independent Linux verification environment 72 passed and one release-manifest test failed only because it treated a Windows-style `..\..\` manifest path as a literal Linux filename. Manual normalization and hash recomputation confirmed the release contents. The public CI runs on `windows-latest`, matching the canonical path convention.

## Live provider evidence

Canonical release evidence reports Momentic read-only/primary PASS and Stagehand Browserbase fallback PASS. Raw credentials, auth files, browser reports, and private execution evidence are intentionally excluded from this public export.

## Scope

No Multi-Worker, Stage Ticket, Advisor, async QA queue, Discord integration, Knowledge/RAG, or default DeepSeek Worker switch is claimed by this release.
