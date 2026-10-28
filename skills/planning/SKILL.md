---
name: planning
description: Plan non-trivial changes using the active project profile to define scope, risks, completion criteria, and validation.
---

# planning

## Purpose

Define the goal, approved scope, impact, risks, completion criteria, and implementation order before non-trivial work.

## When to Use

- Multi-file or multi-component changes
- Business logic, configuration, infrastructure, or data changes
- Work whose impact or implementation path is unclear

Simple, well-defined changes may use a short internal plan.

## Action

1. Run `project-context` and use the active profile's relevant rules, hooks, references, and analysis.
2. Restate the required outcome and explicit exclusions.
3. Define observable completion criteria and required validation.
4. Identify affected areas, shared boundaries, protected paths, and downstream consumers.
5. Identify security, data-loss, compatibility, deployment, and business-rule risks.
6. Break the work into the smallest ordered changes that preserve existing patterns.
7. Estimate file scope and stop if it expands materially beyond the approved requirement.

## Authorization Modes

- **Interactive work**: When scope is unclear or the change is risky, present the plan and wait for user approval.
- **External harness `MODIFICATION`**: Treat the Manager-provided requirements as the approved scope. Build an internal plan and execute without requesting reapproval.
- In either mode, stop for scope expansion, destructive work, security-sensitive changes, protected-path changes, or ambiguous business decisions.
- Never add unrequested refactoring or improvements to the plan.

## Output

Keep the plan proportional to the task and include:

- goal and completion criteria
- affected areas and protected paths
- risks and stop conditions
- ordered implementation steps
- validation commands or checks
- expected file scope

Do not invent missing project facts or build commands. If critical evidence is absent from the active profile and code, stop and ask.
