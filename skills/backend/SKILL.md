---
name: backend
description: Implement backend changes using the active project profile and the existing backend structure, conventions, and safety rules.
---

# backend

## Purpose

Make minimal backend changes without drifting from the target project's established architecture and contracts.

## Prerequisites

1. Run `project-context`.
2. Read the active profile's always-on rules and backend hooks/references.
3. Determine whether the task is interactive or an external harness `MODIFICATION` task.

## Execution Modes

- **Interactive work**: If scope is unclear or the change is risky, explain the plan and wait for user approval.
- **External harness `MODIFICATION`**: Treat the Manager-provided requirements as the approved scope. Make an internal plan and execute within that scope without requesting reapproval.
- In either mode, stop for scope expansion, destructive work, security-sensitive changes, protected-path changes, or ambiguous business decisions.
- Do not add refactoring or improvements outside the approved requirements.

## Workflow

1. Trace the existing request path and read the closest working implementation before editing.
2. Confirm local naming, layering, dependency, response, session, validation, error, and transaction patterns from code and profile references.
3. Change only the required handlers, services, models, persistence code, and configuration.
4. Preserve public contracts and shared boundaries unless the approved requirement explicitly changes them.
5. For data changes, verify state transitions, idempotency, history/audit semantics, concurrency, and failure behavior where relevant.
6. Use the project's existing validation commands and review protected or high-risk paths explicitly.

## Checklist

- Existing layering and naming preserved
- Request, response, session, error, and transaction contracts preserved
- Data-changing paths validate input and state
- No protected or shared infrastructure change without approval
- No unrelated files, dependencies, or abstractions added
- Relevant verification evidence recorded

Stop when project rules or business behavior remain ambiguous, a protected change is required, or validation cannot cover a high-risk change.
