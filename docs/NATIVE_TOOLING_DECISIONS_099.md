# Native tooling and measured diet decisions — 0.99

## Skills and context

Mandatory Profile/rule/task context remains `MANDATORY_CONTEXT` and is delivered by the Harness regardless of runtime discovery. Codex and OpenCode native skills are `OPTIONAL_DISCOVERABLE_SKILL`; OpenCode advertises metadata and loads bodies on demand. Current decision: `NATIVE_SKILLS=PASS`; a live Harness-managed OpenCode turn proved the mandatory Profile mapping and the 24-entry optional native skill catalog without duplicate injection.

The context manifest records `AVAILABLE`, `SELECTED`, and `DELIVERED` separately. `ACCESSED` remains `NOT_CAPTURED` unless a runtime provides direct evidence; delivery is never treated as access. Stable/dynamic hashes and selective context remain authoritative.

Deletion candidate after native validation: remove any Harness optional-skill scanner/injector that duplicates the runtime catalog. The mandatory Profile loader/parser is not a deletion candidate because native optional discovery cannot replace it.

## MCP

Codex 0.153.4 exposes configured MCP discovery and the current host reports seven configured entries. OpenCode 1.18.29 exposes MCP discovery/status and currently reports zero configured entries. The canonical catalog records availability, version, capability, requiredness, health, loader owner, technical retry domain, and product retry delta zero. One MCP identity may have only one loader (`NATIVE_ONLY` when adopted); duplicate Harness/native injection is rejected.

Current decision: `NATIVE_MCP=PASS_EMPTY_OPTIONAL_CATALOG`. Live discovery reports zero configured and zero required OpenCode MCP servers, which matches the accepted Profile mapping. This does not claim an MCP tool execution. Future MCP unavailable/schema/runtime failures remain technical and cannot be graded as product semantic failures.

Deletion candidate after adoption: remove the duplicate Harness MCP context injector or native declaration, keeping exactly one. The SQLite ControlRepository and native runtime/session databases remain separate.

## LSP measured PoC

The installed OpenCode exposes diagnostic/symbol/document-symbol debug commands. Standalone JDTLS, Java language server, Vue language server, TypeScript language server, and ESLint language server were not available on PATH. Provider-independent diagnostic probes still completed successfully through OpenCode for representative `sample-service` Java and `sample-web` Vue files. Both returned an empty diagnostic object; first-run latency was 1,563 ms and 1,220 ms respectively. Process-tree peak memory could not be captured because the available sampler dependency was absent, so memory remains `NOT_CAPTURED` rather than zero. No incremental diagnostic-quality gain over the existing Maven/frontend build/lint/typecheck path was established.

| Module | Existing verifier | Diagnostic gain | Latency | Memory | Synchronization | Decision |
|---|---|---|---|---|---|---|
| sample-core/sample-shared/sample-analytics/sample-portal/sample-service | Maven/JDK | 0 observed diagnostics on representative `sample-service` file | 1,563 ms | NOT_CAPTURED | PASS | OFF |
| sample-analytics-web/sample-portal-web/sample-web | npm build/lint/typecheck | 0 observed diagnostics on representative `sample-web` file | 1,220 ms | NOT_CAPTURED | PASS | OFF |

`NATIVE_LSP=OFF_NO_MEASURED_INCREMENTAL_VALUE`. Missing values are not PASS. A later measured benchmark may turn on only modules with positive diagnostic gain and stable synchronization plus captured latency/memory.

## Optional tools

- `SERENA=DEFERRED`: no repeated investigation bottleneck beyond current search/grep/CLI evidence justified a benchmark; it is not installed or a production dependency.
- `REPOMIX=ADOPTED_OPTIONAL`: onboarding/export/context-bundle use only; per-Attempt full-repository injection is forbidden.
- `UI_UX_CAPABILITY=ADOPTED_OPTIONAL`: existing profile frontend patterns/references may be selected for actual UI Jobs; external UI skills are not runtime dependencies.
- `CLAUDE_MEM=DEFERRED_TO_1_1`: no installation or integration.

## Wrapper and retry-loop diet

The old Droid `new exec + handoff_context` wrapper remains necessary because installed 0.211.0 native resume and JSON-RPC live probes failed despite advertised CLI/protocol surfaces. It becomes a deletion candidate only after native resume passes fault/reconnect tests. Runtime/schema/MCP recovery stays in technical domains with product retry delta zero; no parallel native retry loop may compete with the Harness command/event ledgers.
