---
name: testing
description: Add or run focused verification using the active project profile and existing project test practices without forcing a new test framework.
---

# testing

## Purpose

Provide practical evidence for changed behavior with the smallest stable verification that fits the project.

## When to Use

- Regression checks for bug fixes
- Business or data logic changes
- Existing nearby automated tests
- Build, API, UI, query, lint, or validation checks required by the active profile

## Strategy

1. Run `project-context` and identify validation commands and high-risk rules from the active profile.
2. Prefer the established test style and commands in the affected project area.
3. If no automated test pattern exists, use the smallest reproducible practical check.
4. Add automated tests only when stable, meaningful, and consistent with existing practice.
5. Record exactly what was run, its result, and what remains unverified.

## Rules

- Test behavior that can regress; skip trivial getters and duplicate coverage.
- Do not add a testing framework or broadly restructure production code without approval.
- Do not claim a path was tested when only syntax, lint, or a neighboring path was checked.
- High-risk security, data, permission, financial, workflow, or infrastructure behavior needs stronger explicit evidence.
- Use safe fixtures or read-only checks; never alter live data merely to prove a test.

## Output Format

Use `PASS`, `FAIL`, and `NOT_RUN` for each check's execution status:

- `PASS`: the check ran and passed.
- `FAIL`: the check ran and failed, regardless of cause.
- `NOT_RUN`: the check did not run.

Record whether the check is required separately from its execution status. For failures or blocked checks, identify the cause as `product`, `tool`, `encoding`, `permission`, `environment`, or `unknown`, with the command or method, target, and evidence. Do not infer a product defect from another failure cause.

Retain these existing labels as reasons, not additional execution statuses:

- `NOT_REQUIRED`: the approved contract does not require this verification.
- `UNAVAILABLE`: the required tool or environment was unavailable. Use `NOT_RUN` if the check could not start; use `FAIL` if it ran and failed.
- `NOT_RUN_DUE_TO_PRIOR_GATE`: an earlier gate prevented this check from starting; its status is `NOT_RUN`.

Report a failed prerequisite separately as `FAIL` and an unexecuted dependent check as `NOT_RUN`. Required checks with `FAIL` or `NOT_RUN` leave the relevant requirement unverified; state the remaining verification condition instead of claiming it passed.

```text
### Verification Plan
- Build / lint: ...
- Functional / API / UI / query: ...
- Automated tests: Yes / No

### What Was Verified
- ...

### What Was Not Verified
- ...

### Residual Risk
- None
or
- ...
```

Stop when verification would require an unapproved framework, destructive data change, production access, or material redesign, or when high-risk behavior cannot be verified with available evidence.
