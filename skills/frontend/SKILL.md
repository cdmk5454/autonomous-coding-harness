---
name: frontend
description: Implement frontend changes using the active project profile and the target application's actual framework and established UI patterns.
---

# frontend

## Purpose

Make minimal UI changes while preserving the target application's framework, component, state, routing, styling, and API conventions.

## Prerequisites

1. Run `project-context`.
2. Read the active profile's always-on rules and frontend references.
3. Identify the exact frontend module and confirm its framework from project files and nearby code.
4. Determine whether the task is interactive or an external harness `MODIFICATION` task.

## Execution Modes

- **Interactive work**: If scope is unclear or the change is risky, explain the plan and wait for user approval.
- **External harness `MODIFICATION`**: Treat the Manager-provided requirements as the approved scope. Make an internal plan and execute within that scope without requesting reapproval.
- In either mode, stop for scope expansion, destructive work, security-sensitive changes, protected-path changes, or ambiguous business decisions.
- Do not add redesign, refactoring, framework changes, or improvements outside the approved requirements.

## Workflow

1. Read the closest working page or component in the same application.
2. Confirm the existing API client, authentication, state, router, component, form, validation, grid, editor, and styling patterns that apply.
3. Reuse existing components and dependencies before creating anything new.
4. Preserve request/response keys, loading/error behavior, accessibility, and responsive behavior relevant to the change.
5. Keep edits inside the target application unless a cross-boundary change is explicitly approved.
6. Run the validation commands declared or used by the target project and perform focused UI checks.

## Checklist

- Correct application and actual framework confirmed
- Existing API, state, router, and component patterns followed
- No new dependency or cross-application import without approval
- Contract, error, accessibility, and responsive behavior checked
- No unrelated redesign or refactoring
- Relevant build, test, lint, or UI evidence recorded

Stop when the application cannot be identified, the framework or API contract is unclear, or the requested behavior requires an unapproved broad redesign.
