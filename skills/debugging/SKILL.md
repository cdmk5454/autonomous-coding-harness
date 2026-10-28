---
name: debugging
description: Diagnose failures with hypothesis-driven investigation using the active project profile before making the smallest verified fix.
---

# debugging

## Purpose

Find and verify the root cause before changing code.

## Core Loop

1. **Observe**: State the exact symptom, environment, inputs, and evidence.
2. **Reproduce**: Find the smallest reliable reproduction or document why it is intermittent.
3. **Locate the boundary**: Narrow the failure to the responsible component, contract, data path, configuration, or environment.
4. **Hypothesize**: Rank concrete causes by likelihood and cost to test.
5. **Test**: Run the smallest non-destructive check that can disprove the leading hypothesis.
6. **Conclude**: Confirm the root cause with evidence.
7. **Fix and verify**: Apply the smallest in-scope fix and rerun the reproduction plus relevant regressions.

## Project Context

- Run `project-context` when architecture, protected paths, environment behavior, or project conventions matter.
- Read the active profile's rules and only the hooks/references for the suspected area.
- Compare with a nearby working path before inventing a new pattern.

## Rules

- Do not patch symptoms without locating the responsible boundary.
- Do not make broad changes while investigating.
- Do not refactor unrelated code or add speculative logging and dependencies.
- Do not mutate live data, weaken security, or bypass validation to make the symptom disappear.
- If multiple hypotheses remain, test the cheapest discriminating one first.

## Output Before Fixing

```text
### Symptom and Reproduction
- ...

### Suspected Boundary
- ...

### Hypotheses and Checks
1. ...

### Root Cause Evidence
- ...

### Minimal Fix and Verification
- ...
```

Stop when the issue cannot be reproduced or bounded, the root cause requires an unapproved protected/infrastructure change, irreversible data action is involved, or critical environment access is unavailable.
