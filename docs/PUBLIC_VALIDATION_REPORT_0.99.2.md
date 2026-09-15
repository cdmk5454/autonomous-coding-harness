# Public Validation Report — 0.99.2

This report describes the evidence used to publish the sanitized `0.99.2` source export. It deliberately separates canonical/private release evidence from checks independently reproduced during external review.

## Release identity

- Public milestone: `0.99.2 — Operational Convergence & Verification`
- Canonical release build: `0.99.2+20260914.1`
- SQLite control schema: `5`
- Public export scope: sanitized source/tests/docs only

## Canonical/private release evidence

The canonical release records:

- integrated Windows suite: `1563/1563 PASS`
- new focused 0.99.2 acceptance coverage: 39 tests
- deterministic evaluation release gate: PASS
- OpenCode 1.18.30 managed-local canary: PASS
- canonical release SHA-256 manifest: `152/152 MATCH`, no missing/mismatch

These are release records from the private/canonical Harness environment. They are not claimed as results reproduced from the public sanitized tree.

## Independent review-bundle verification

The 0.99.2 verification bundle was independently inspected and the following were reproduced:

- focused 0.99.2 acceptance: `39/39 PASS`
- deterministic Harness evaluation gate: PASS
- canonical release SHA-256 recheck: `152/152 MATCH`
- source-level verification of bounded runtime recovery, semantic rework lineage, Review/finalization stabilization, non-blocking convergence telemetry, and rollback rehearsal

The full Windows/provider-dependent suite was not reproduced in the Linux review environment. Environment-dependent failures caused by unavailable Windows commands/provider CLIs were not counted as Product/Harness regressions.

## Scope not proven by this release

- unrestricted autonomous execution with arbitrary undeclared external side effects
- Multi-Worker / Stage Ticket / Reviewer Pool
- Advisor
- Browser/Web QA backend
- Direct candidate full validation/publication
- Knowledge/RAG/Librarian control-plane behavior

Those items are either explicitly deferred or outside the declared 0.99.2 correctness surface.

## Privacy

No customer source, project profile, raw execution history, control database, credential, or real Product artifact is included in the public export.
