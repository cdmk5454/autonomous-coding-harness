# Harness 0.99 operator runbook

## Stabilization validation policy

The first integrated 0.99 full-suite PASS is the validation baseline. After that baseline, compute the changed file/symbol/schema/contract surface and its direct plus semantic dependency closure for every patch. Run only the focused, integration, and Eval cases in that closure. Evidence outside the closure is recorded as `REUSED_VALID_EVIDENCE / UNAFFECTED` when its production/dependency hashes, contract/policy identity, schema compatibility, execution surface, and evidence binding are unchanged.

Run the full Harness suite again only when a shared execution/control/schema invariant changes broadly, the selective closure cannot be bounded safely, release packaging changes executable behavior, or manifest/hash consistency fails. VERSION, manifest, report, and runbook-only changes do not invalidate the integrated suite baseline. The approved integrated baseline is 1,438/1,438 PASS; the ProductReadiness compatibility delta is 6/6 PASS, with eight affected Eval cases rerun and the other 42 reused.

## Upgrade boundary

0.99 remains `PRE_STABILIZATION` until the provider-independent release gate passes. The live 0.9.1.6 runtime and schema-v1 database stay untouched during source implementation. Before a controlled upgrade, pause the Queue, verify there is no running Execution or owned runtime process, record product-workspace integrity, and keep all pre-existing dirty files unchanged.

## Backup and migration

Use the SQLite backup API in `control_backup.py`; do not copy a live WAL database with a plain filesystem copy. Create the backup at a new explicit path and validate its fingerprint, `PRAGMA integrity_check`, foreign keys, and table projection hashes. Rehearse migration on a copy first by opening it through `ControlRepository`; open it a second time and require an identical fingerprint to prove idempotency.

For the real upgrade, retain the pre-migration backup and its manifest outside the active DB path. Migrate only after the final schema version, integrated Harness baseline plus current selective closure, affected Eval cases plus reusable unaffected Eval evidence, product read-only integrity preflight, and operator approval are all current. A schema mismatch, missing table, foreign-key error, or changed legacy projection stops the cutover.

## Health check

After migration, require:

- SQLite integrity `PASS` and zero foreign-key errors;
- Queue revision/order and every legacy Job/Execution projection unchanged;
- no unexpected product dirty files;
- no stale managed runtime listener or old writer process;
- runtime capability and role-specific frozen contract preflight `PASS`;
- Harness Eval critical corpus all pass with every forbidden counter zero.

## Restore and fallback

Stop every Harness writer before restore. Restore the verified backup to a new path, validate its fingerprint and artifact-reference hashes, then atomically select it only under a separate controlled cutover. Never overwrite an active database merely to test rollback. If post-migration health fails, keep 0.99 stopped and return to the verified 0.9.1.6 runtime/database pair; do not downgrade an already-mutated database in place.

Droid is the OpenCode fallback. Prefer native resume only when the frozen logical job, contract revision, role, workspace/SourceView, runtime surface, and candidate/context are compatible. Otherwise stop/fence the old writer, preserve the candidate/checkpoint, create a fresh Attempt/session, and deliver explicit technical recovery context. Runtime recovery never consumes product retry budget.

## OpenCode deferred finalization

Provider/auth/live work remains `DEFERRED_USER_SETUP`. Do not set `KKM_OPENCODE_LIVE_VALIDATED=1` until all criteria pass:

1. isolated fixture suite;
2. live model/session smoke;
3. at least five LOW-risk product Jobs;
4. at least three distinct normal Batches;
5. fault scenarios;
6. phone/PC reconnect and continuity;
7. candidate and prior batch-delta preservation;
8. Droid fallback.

The counts are rollout heuristics, not a statistical safety guarantee. One unsafe writer overlap, ownership violation, evidence false success, unauthorized mutation, or candidate/prior-delta loss stops default migration promotion.

## Feature-freeze and cutover criteria

Feature freeze requires closed execution/session/runtime, SourceView, verification, QA/readiness, tooling/MCP, telemetry, Eval, migration/backup/restore, portability, and runbook foundations. Unknown or not-captured telemetry remains visible and is never converted to PASS. Multi-agent scheduling, concurrent SourceViews, worktree orchestration, ResourcePool, Serena production integration, Claude-Mem, and automatic human-trace-to-E2E generation remain outside 0.99.
