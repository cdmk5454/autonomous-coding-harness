# Autonomous Coding Harness — v2 Public Architecture Candidate

**Public, sanitized reference** for a profile-driven AI coding workflow using `AGENTS.md`, task-specific Skills, and [Paseo](https://paseo.sh/) for multi-agent orchestration. This repository describes a migration candidate, **not** a claim that all v2 runtime controls have passed production validation.

## Evolution

| Version | Responsibility | Historical source |
| --- | --- | --- |
| v1 / 0.99.x | Custom Python/SQLite execution control: queue, owned write scope, retries, rollback and verification | [`main` history before migration](https://github.com/cdmk5454/autonomous-coding-harness/commit/4e9393fcf56ca23f5da177dcfd63719a135f5523) |
| v1.x reference | Frozen 1.0/1.1 source-level archive, separate from the 0.99.2.1 public baseline | [`archive/harness-1.x-reference`](https://github.com/cdmk5454/autonomous-coding-harness/tree/archive/harness-1.x-reference) |
| v2 candidate | Declarative policy and project Profiles, explicit agent-role routing, read-only review and evidence-driven verification | This branch |

The goal is to reduce duplicated orchestration code, while retaining the engineering lessons from v1. **The declarative v2 approach does not automatically reproduce all v1 transaction, rollback, publication, integrity or proof gates.** Missing runtime enforcement must remain an explicit limitation until implemented and tested.

## Workflow (role convention, not a built-in transaction manager)

```text
User / job contract (goal, authorized scope, contracts, acceptance)
           |
           v
Orchestrator (Claude or Codex; analysis and delegation)
           |
           +--> Advisor (on ambiguous or high-risk contracts; no implementation)
           |
           v
Worker (LOW / MID / HIGH by semantic impact; scoped implementation)
           |
           v
Build / Test and evidence capture (tooling and commands from the project)
           |
           v
Cross-family Reviewer (read-only role; diff and evidence assessment)
           |
           v
Orchestrator: acceptance, block or retry; human QA when required
           |
           v
Reporter: evidence-bound change summary
```

Paseo provides workspace, provider and orchestration capabilities. The role progression, risk tiers and gates above are **workflow policy** unless separate testable enforcement is configured. A successful Worker message or a Reviewer opinion is not proof of Build/Test/QA success.

## Security and isolation boundaries

- Paseo's `paseoTools.disabledTools` removes specific Paseo tools from the provider catalog. It **does not restrict shell or filesystem operations** by itself.
- Codex's native `sandbox_mode: read-only` is an explicit read-only profile configuration. Other providers use their own permission/sandbox controls; a `disallowedTools` list alone is not an OS sandbox.
- A `write scope` in policy is an instruction until supported by actual workspace/container/OS access boundaries, explicit output-diff checks and admission gates.
- No real customer/organization profile, tokens, credentials, project mapping, operational queues or production evidence is distributed here.
- DB access is determined by the active project's **approved** data access contract. This public package includes no DB connection credentials or live DB query runner.

See [Architecture and limitations](docs/architecture.md) and [Public migration checklist](docs/migration-checklist.md).

## Files

- `AGENTS.md` — common policy and contract rules (from supplied reference snapshot)
- `skills/{planning,project-context,backend,frontend,sql,testing,debugging,review,reporter}/SKILL.md` — custom reusable task procedures selected from the supplied snapshot
- `hooks/validation.md` — pre-completion checklist; documentation, not a standalone security mechanism
- `config.sample.json` — intentionally **non-executable illustration**, stripped of machine paths, profiles IDs, live model aliases and credentials
- `profiles/sample-project/` — synthetic profile layout for understanding expected references; adapt before use

## Validation status

Static JSON / Markdown structure checks and a curated package exclusion audit are performed when building this public candidate. No Paseo daemon, model-provider, end-to-end job, live DB, rollback, multi-worker or production QA validation is implied by those checks. Consult the migration checklist for the required runtime gates before calling v2 operational.

## References

- [Paseo MCP/tool-catalog boundaries](https://paseo.sh/docs/mcp)
- [Paseo provider options](https://paseo.sh/docs/sdk/provider-options)
- [Paseo security](https://paseo.sh/docs/hub/security)
