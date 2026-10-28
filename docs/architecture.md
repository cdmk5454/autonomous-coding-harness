# v2 Architecture: responsibilities, not inherited guarantees

## Owners and state

- **User**: source of authorization, business/API/data contract and QA decisions.
- **Project profile**: standing project rules, architecture evidence, protected paths and verification hints; not a credential vault.
- **Orchestrator**: semantic decomposition, candidate routing, dependency and shared-contract coordination. It does not turn a delegated Worker report into canonical success.
- **Worker**: changes the allowed workspace with explicit constraints. Policy alone cannot prevent writes outside declared paths; inspect final diff against scope.
- **Advisor / Reviewer**: analysis-only roles. For Codex, native `read-only` sandbox can restrict writes; Claude needs provider-native sandbox/permissions and a separate enforcement test, not `disallowedTools` alone.
- **Reporter**: summarizes only collected diff, Build/Test, review and QA evidence.
- **Paseo**: agent/workspace orchestration layer; not automatically a replacement for the v1 queue authority, crash-consistency journal, scoped rollback, evidence binding, integrity admission or transactional batch publisher.

## State and acceptance

A task can be `PROPOSED`, `IMPLEMENTING`, `VALIDATING`, `REVIEWING`, `AWAITING_QA`, `ACCEPTED` or `BLOCKED` in human process terms. **These are conceptual labels, not a demonstrated persisted v2 state machine.** `SKIPPED`, `CANCELLED`, `FAILED`, `FAILED_FINAL`, `BLOCKED`, `INTERRUPTED` and `AWAITING_QA` are never automatically treated as success. Commit/push/deploy requires explicit approval and successful required gates.

## Dependency correctness

Keep semantic dependency, execution dependency, write conflict and shared contract distinct. `sequence != dependency`; a file collision requires conflict handling, not an invented semantic dependency. If a QA-sensitive decision can change the dependent implementation, hold only that dependent work. Do not publish or promote an isolated candidate before human acceptance and current machine verification.

## Risk routing

LOW: mechanical edits or localized safe changes. MID: defined multi-file behavior. HIGH: architecture, transaction, authorization, data meaning, cross-module or critical compatibility. CRITICAL: destructive/security/privacy-sensitive operations and unknown contracts. Routing preferences should be evidence-driven, not a substitute for deterministic permission checks.

## Public artifact note

The attached runtime `config.json` was used for a **local configuration review only**. It is not published, and `config.sample.json` deliberately is not the user's executable configuration.
