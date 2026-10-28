---
name: project-context
description: Load task-relevant rules and evidence from the explicitly selected active project profile before non-trivial work.
---

# project-context

## Purpose

Resolve the active project profile and load only the context needed for the current task.

## Profile Resolution

1. Use an active profile directory explicitly supplied by the external executor or current prompt.
2. If none is supplied, read `AGENT_PROFILE_DIR` from the environment.
3. If neither source provides a profile, use common rules only. Do not select a default profile or infer one from the workspace. For project-specific work that requires profile facts, fail clearly instead of guessing them.
4. Read `project.json` from the selected directory and require a supported `schema_version`, all required fields, and valid profile-relative referenced paths.
5. If the selected profile directory or manifest is invalid, do not start project changes; report the validation failure clearly.
6. Do not write a global current-profile file, mutate profile selection, or persist profile state.

## Selective Loading

1. Apply the profile's standing project rules declared by `context.project_rules`.
2. Search `context.analysis` by headings, target path, component, screen ID, class name, and task keywords.
3. For a single component or screen, read only matching analysis sections. Read the full analysis only for broad architecture, multi-component, or shared-infrastructure work.
4. For screen work, search only the relevant files under `context.ui_map`.
5. For DB-impacting or screen work, use `context.db_catalog` when configured. Load only tables named by the requirement or selected UI-map section, plus their columns, direct relationships, and quality warnings. Do not load an entire schema by default.
6. Use `area_patterns` to identify task areas, then read `rules.always` and only the matching area hooks and references.
7. Resolve every profile-relative path inside the active profile directory; reject missing paths and path traversal.
8. Record any compatibility fallback as an explicit context warning; never silently promote a legacy global analysis or UI-map path to canonical profile evidence.

## Evidence Rules

- Prefer current code and verified profile facts over assumptions.
- Apply DB evidence in this order: current DB metadata, current persistence SQL, supplied specification snapshot, then inferred relationships.
- Treat supplied DB catalogs as dated snapshots. Never infer current common-code rows or production data values from table structure metadata.
- Treat analysis recommendations as unapproved unless the user requirement includes them.
- Never turn an unverified statement into fact.
- If a snapshot or mapping may be stale, follow its revalidation instructions.
- Mark a fact whose evidence hash changed as `PROFILE_FACT_STALE`. Mark a meaningful conflict between a declared fact and current source as `PROFILE_SOURCE_MISMATCH`; neither state may be injected as a hard rule.

## Automation Behavior

For external harness `MODIFICATION` work, treat the Manager-provided requirements as approved within their stated scope. Use the context summary internally and continue without requesting reapproval unless important information is genuinely missing or a stop condition applies.

## Output

Summarize the active profile path, affected areas, applicable rules/references, protected paths, high-risk concerns, and any missing critical evidence.
