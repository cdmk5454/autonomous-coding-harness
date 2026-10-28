---
name: sql
description: Write and review SQL using the active project profile, safe parameter binding, existing persistence conventions, and focused regression checks.
---

# sql

## Purpose

Make safe, minimal SQL changes that follow the target project's database and persistence conventions.

## Prerequisites

1. Run `project-context`.
2. Read the active profile's always-on and SQL rules/references.
3. Locate the call site, parameters, result consumers, closest existing query, and target database behavior.
4. Determine whether the task is interactive or an external harness `MODIFICATION` task.

## Execution Modes

- **Interactive work**: If scope is unclear or the change is risky, explain the plan and wait for user approval.
- **External harness `MODIFICATION`**: Treat the Manager-provided requirements as the approved scope. Make an internal plan and execute within that scope without requesting reapproval.
- In either mode, stop for scope expansion, destructive work, security-sensitive changes, protected-path changes, or ambiguous business decisions.
- Do not add unrelated query rewrites, schema changes, indexes, or cleanup.

## Workflow

1. Copy the closest established query shape and naming style in the same data domain.
2. Use the project's parameter-binding mechanism; do not concatenate untrusted values into SQL.
3. Select only required columns and preserve result mapping and caller contracts.
4. Keep dynamic SQL as small as possible and validate empty, null, boundary, and collection inputs.
5. Preserve the domain's history, audit, locking, transaction, and data-change semantics.
6. For performance-sensitive changes, inspect the execution plan or equivalent evidence using safe read-only tooling when available.
7. Verify representative results and regression paths without mutating production data.

## Checklist

- Query and mapping conventions match the active profile
- Values use safe binding; identifiers are controlled and explicitly justified
- No unapproved DDL, index, broad update/delete, or migration
- Caller and result contracts remain aligned
- Data history, transaction, and concurrency behavior considered
- Performance and regression evidence recorded where relevant

Stop when schema facts are unclear, dangerous operations are required, the query could have broad impact, or the data business rule is ambiguous.
