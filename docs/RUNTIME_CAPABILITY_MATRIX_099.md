# Runtime capability contract — 0.99.1

Observed locally on 2026-09-08 (0.99.0) and re-observed 2026-09-10 (0.99.1: OpenCode 1.18.30). The durable contract is `runtime-adapter/2`; every materialized role freezes its runtime version, adapter/protocol/schema revision, capability snapshot, permission/tool profile, process-lifetime semantics, and execution environment.

| Capability | Codex 0.153.4 | Droid 0.211.0 | OpenCode 1.18.30 |
|---|---|---|---|
| Session create | SUPPORTED | SUPPORTED | SUPPORTED |
| Explicit resume | SUPPORTED | SUPPORTED | SUPPORTED |
| Session read/query | PARTIAL | UNSUPPORTED | SUPPORTED |
| Session fork | UNSUPPORTED | SUPPORTED | UNSUPPORTED |
| Turn submit | SUPPORTED | SUPPORTED | SUPPORTED |
| Active steer | UNSUPPORTED | PARTIAL | UNSUPPORTED |
| Interrupt | UNSUPPORTED | PARTIAL | SUPPORTED |
| Event subscribe | PARTIAL | PARTIAL | PARTIAL |
| Event reconnect | PARTIAL | PARTIAL | PARTIAL |
| Replay/cursor | UNSUPPORTED | UNSUPPORTED | UNSUPPORTED |
| Terminal query | PARTIAL | UNSUPPORTED | SUPPORTED |
| Pending tool approval | PARTIAL | PARTIAL | PARTIAL |
| Pending question | PARTIAL | PARTIAL | PARTIAL |
| Permission response | UNSUPPORTED | PARTIAL | UNSUPPORTED |
| Response delivery | SUPPORTED | SUPPORTED | SUPPORTED |
| Structured output | SUPPORTED | SUPPORTED | PARTIAL |
| Structured schema revision | SUPPORTED | PARTIAL | UNSUPPORTED |
| Permission/tool semantics | SUPPORTED | SUPPORTED | SUPPORTED |
| Usage metadata | PARTIAL | SUPPORTED | PARTIAL |
| Native queue | UNSUPPORTED | UNSUPPORTED | UNSUPPORTED |
| Protocol revision | SUPPORTED | SUPPORTED | SUPPORTED |

`PARTIAL` means the installed runtime exposes some relevant native surface but the current adapter cannot claim the complete canonical operation. It must not be silently substituted by another feature.

0.99.1 corrections: OpenCode pending approval/question observation is PARTIAL, not SUPPORTED. The native server publishes pending permission/question state on its SSE stream, which this adapter does not bind; `/session/status` exposes no pending state, so a permission wait is observed as a no-progress technical timeout (`OPENCODE_PROGRESS_TIMEOUT`, zero product retry) rather than a product defect. Blanket permission auto-approval is not used (`interaction.permission_response` remains UNSUPPORTED). OpenCode >=1.18.30 also rejects unauthenticated loopback requests; harness-owned local servers therefore start with an ephemeral per-instance credential (live canary PASS on 1.18.30: serve/health/session create-query-delete/child cleanup).

Codex and Droid sessions are persisted beyond a submitting process. OpenCode uses persistent server storage. Process residency is therefore not required for session durability, and no hot Worker/Reviewer pool is introduced. Droid native continuation uses `exec --session-id`; multi-turn control is exposed by `stream-jsonrpc`. Its authoritative session query, terminal query, and event cursor/replay remain unsupported. The legacy fresh `droid exec + handoff_context` path remains only for proven continuation failure and starts a fresh Attempt after the old writer is stopped.

Persistent-server writer quiescence (`0.99.1+20260910.3`): because an OpenCode session outlives the submitting harness process, a technical failure during an in-flight turn now performs the native session abort (`POST /session/{id}/abort`) plus a bounded terminal/idle/lost confirmation before the failure is returned. The aborted command is marked non-publishable, so its late assistant response is never adopted as the current result. When the abort/query outcome is UNKNOWN, a transport failure, or a timeout, the next canonical writer is denied (`OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED`) until a bounded read-only query proves the session is no longer active; the candidate stays preserved and the failure stays technical with zero product retry. Logical runtime END, DB fencing, or writer-authority suspension alone never proves native write quiescence. Harness-owned local servers keep deterministic process-tree isolation instead.

The matrix above describes the installed CLI's advertised native surface. Live validation is a separate gate: on this Windows host, four clean first turns returned a session ID but every follow-up `exec --session-id` ended with silent exit code 1; two raw `droid.initialize_session` JSON-RPC probes produced no response before timeout. Therefore the current evidence result is `DROID_NATIVE_SESSION=FAIL` and `DROID_MULTITURN=FAIL`, and production fallback must use a fresh fenced Attempt rather than claiming native continuation.

Native resume, rewind, and fork are history operations only. They are not filesystem rollback evidence, do not resurrect a terminal Execution, and do not clone writer authority. Canonical rollback remains snapshot/candidate/scoped rollback with preserved batch delta. Native runtime databases remain external identity/reference stores and are never merged into the Harness SQLite ControlRepository. Native queueing cannot preload a Batch or bypass Worklist/JIT/gates.

OpenCode skill discovery is optional and on-demand; mandatory Profile context remains Harness-delivered. Native MCP is catalogued by availability/version/capability/requiredness/health and injected through one loader only. OpenCode permission rules are approval policy, not OS filesystem/process/network isolation.

References: [OpenCode skills](https://opencode.ai/docs/skills), [OpenCode MCP servers](https://opencode.ai/v2/docs/mcp-servers), [OpenCode permissions](https://opencode.ai/docs/permissions/), [OpenCode Windows/WSL](https://opencode.ai/docs/es/windows-wsl/).
