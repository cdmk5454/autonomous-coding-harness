---
name: review
description: Review a change once against approved requirements, the active project profile, the actual diff, and current verification evidence.
---

# review

## Purpose

Perform one read-only review and return one verdict based on concrete correctness, safety, scope, and verification evidence.

## Inputs

- User-approved requirements, including valid authorization relayed by the task caller, are the primary baseline. The assignment does not expand approval or the read-only review role.
- Common rules and the active project's rules, hooks, references, protected paths, and risk areas apply.
- Use a Planner brief only when provided; it is supplemental and never a required input.
- Review the actual local change set and current validation output. Do not infer success from summaries alone.

## Review Focus

1. **Constraints**: secrets, destructive operations, external effects, protected paths, and authorization boundaries.
2. **Correctness**: caller/callee contracts, validation, errors, state changes, null/empty/boundary inputs, concurrency, and data integrity.
3. **Pattern consistency**: established structure, naming, dependencies, configuration, and active-profile conventions.
4. **Scope**: unrelated refactoring, modernization, dependency changes, or improvement work outside the requirement.
5. **Regression risk**: shared paths, downstream consumers, environment differences, compatibility, and rollback behavior.
6. **Verification**: whether the evidence directly exercises the changed behavior and required risk areas.

## Output Contract

One Reviewer invocation performs one review and returns exactly one verdict in this format:

```text
RESULT: PASS | FAIL
REASON: one-line summary
DETAIL: concrete evidence
RISK: NONE | LOW | MID | HIGH
```

- Use `RISK: NONE` for `PASS`; here NONE means no blocking finding within the reviewed scope, not zero residual risk or proof that unrun checks passed. State the review scope, check results, evidence, and unverified limits in `DETAIL`.
- Do not emit `PASS WITH NOTES`; put non-blocking observations in `DETAIL` with `RESULT: PASS`.
- For `FAIL`, use `LOW` for a localized patch, `MID` for a substantial in-tree approach correction, and `HIGH` for a hard-constraint violation, broken build, empty modification, or fundamentally unsafe change.

## Invocation Boundary

- Perform the review checklist once and emit one `PASS` or `FAIL` verdict.
- Do not modify code or run an internal fix/re-review loop.
- Return the verdict, unresolved findings, and unverified checks to the requesting task. Do not initiate follow-up review assignments.

## Previous Feedback

When previous Reviewer feedback is provided, list each prior issue and verify it against the current change. Any unresolved prior issue produces `FAIL`; do not silently drop it.

## Read-Only Review Rules

- Prefer the local diff or change collector as the source of truth.
- Remote change context may be read when available, but the review step must not push, comment, approve, or mutate remote state.
- Do not request idealized redesigns or unrelated cleanup.
- When a high-risk area is touched, require directly relevant evidence.
- If no meaningful change exists for a modification task, return `FAIL` with `RISK: HIGH`.
- A review pass cannot override failed build, test, change-collection, or safety gates.

Stop and return `FAIL` when a hard constraint is violated, an unapproved protected change exists, the scope is materially wrong, critical business behavior is ambiguous, verification is insufficient for the risk, or previous feedback remains unresolved.
