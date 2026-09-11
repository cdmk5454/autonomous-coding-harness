# OpenCode runtime — 0.99.1

## Current status

```text
OPENCODE_ADAPTER_IMPLEMENTED = YES
OPENCODE_PROVIDER_SETUP = PASS
OPENCODE_LIVE_MODEL_CALL = PASS (GLM 5.3 Flash + GLM 5.3)
OPENCODE_LIVE_CANARY = PASS
OPENCODE_LIVE_VALIDATED = YES
```

The adapter targets the installed OpenCode 1.18.30 CLI/server contract (0.99.1 re-verified; 0.99.0 finalized against 1.18.29). Final acceptance covers CLI/schema preflight, authenticated local server health, session create/query/delete, correlated turn completion, reconnect/resume, normalized events, process cleanup, and live calls through both approved GLM models. Pending permission/question observation is PARTIAL: the native server publishes pending state on SSE only and the adapter binds no SSE client, so a permission wait surfaces as a no-progress technical timeout with zero product retry (see the capability matrix).

## Runtime roles

- `PERSONAL`: user-controlled Web/TUI investigation and continuity. It may use native UI operations but is never the managed product writer.
- `MANAGED`: Harness is the only dispatch/stop/QA/recovery controller and product writer. Direct native-UI mutation of its session is forbidden.
- Harness Worklist/Queue remains authoritative. OpenCode sessions are execution resources, not a replacement queue.

OpenCode is available only for an explicit OpenCode Job and records `requested_runtime=opencode`. Existing Codex/Droid defaults remain unchanged, and Droid remains the configured OpenCode recovery fallback. Missing or revoked provider/model finalization still fails closed as `DEFERRED_USER_SETUP` rather than silently executing through a different writer.

## Managed configuration

Set these outside the repository; never store credentials in a Job, config file, log, or report.

```text
KKM_OPENCODE_MODEL=<provider>/<model>
OPENCODE_SERVER_USERNAME=opencode
OPENCODE_SERVER_PASSWORD=<user-managed secret>
KKM_OPENCODE_SERVER_URL=https://<managed-host>   # omit for local loopback server
KKM_OPENCODE_FALLBACK_RUNTIME=droid
```

Non-loopback managed endpoints require HTTPS and authentication. A local managed server started without an explicit password generates one ephemeral per-instance credential (OpenCode >=1.18.30 rejects unauthenticated loopback requests); it lives only in the child process environment and request headers, is never persisted or logged, and is not a provider credential. The adapter never enables OpenCode `--auto`, selects a provider, or guesses a model.

The accepted alternative for a remote managed host is a private network boundary or an authenticated TLS gateway. CORS is not authentication. API secrets, provider credentials, DB credentials, and classified PII must be redacted before prompt/event/report replication. The managed mutation endpoint is not exposed directly. Credential, process, and configuration boundaries remain separate. OpenCode permission allow/ask/deny is not an OS sandbox and does not prove filesystem, network, process, VM, or container isolation.

The deterministic Profile mapping records mandatory Profile/rule context separately from optional native skill discovery, uses argv arrays for Windows drive/path quoting, keeps Windows-native JDK/Maven/Node/npm and encoding/process semantics, and selects a single native MCP loader per server identity. OpenCode adoption does not simultaneously migrate SAMPLE_PROFILE to WSL; runtime migration and OS/toolchain migration remain separate experiment axes.

The completed finalization checklist enables:

```text
KKM_OPENCODE_LIVE_VALIDATED=1
```

This is an operator assertion backed by the 0.99 provider-finalization evidence; it is not an automatic bypass.

## Finalization checklist

1. Select an OpenCode provider.
2. Authenticate using OpenCode's own provider flow.
3. Set an explicit `<provider>/<model>` identifier.
4. Run one read-only live model call and verify normalized events/results.
5. Run the explicitly approved LOW-risk product canary without product mutation.
6. Run fault scenarios and phone/PC reconnect/continuity.
7. Prove candidate/prior batch-delta preservation and Droid fallback.
8. Record PASS/FAIL evidence and only then enable `KKM_OPENCODE_LIVE_VALIDATED=1`.

The earlier five-Job/three-Batch quantities remain future rollout heuristics, not a 0.99 release requirement; the user explicitly approved one correlated LOW-risk canary for this finalization. Any unsafe writer overlap, ownership violation, evidence false success, unauthorized mutation, or candidate/prior-delta loss blocks promotion after one occurrence.

Failures in these steps stay in `RUNTIME_RECOVERY` or `TECHNICAL_EXECUTION_RETRY`; they do not consume `PRODUCT_SEMANTIC_RETRY` unless a real build/test/review product defect is independently proven.

## Recovery

For a disconnect, query the durable command ledger first. `SENT` or `UNKNOWN` delivery is reconciled through the bound session and must not be resent blindly. If the native session exists, reconnect and continue with the next turn. A fresh Attempt/session is allowed only after the adapter proves the bound session is unavailable. Runtime recovery exhaustion leaves product readiness pending and cannot produce `FAILED_FINAL` by itself.

On a persistent server, a technical failure during an in-flight turn aborts the native session and confirms terminal/idle/lost within a bounded window before the failure is returned (`0.99.1+20260910.3`). The aborted command's late assistant response is never published as the current result. If the abort or the confirmation query ends UNKNOWN (transport failure/timeout), the next canonical writer is denied with `OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED` until a bounded read-only query proves the session is idle/terminal/lost; the preserved candidate and prior batch delta are untouched and the failure stays technical with zero product retry. The polling progress watchdog resets only on turn-correlated semantic change (assistant content deltas, tool state transitions, correlated message/finish or session state transitions) — heartbeat, token/usage metadata, unrelated messages, and metadata-only mutations do not reset it.
