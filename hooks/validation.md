# validation

## Absolute Constraints

- Never expose or add secrets, credentials, tokens, private keys, or private configuration values.
- Never run destructive DDL/DML, broad data mutations, or irreversible operations without explicit approval.
- Confirm scope before calling or changing external APIs, shared services, deployment infrastructure, or production configuration.
- Never modify files outside the approved task or active workspace scope.
- Never expand a small task into an unnecessary large rewrite.
- Never claim completion without current, relevant verification evidence.

## Pre-Completion Checks

Before finishing, verify:

- the change matches the approved requirements
- common rules and active-profile rules were followed
- protected paths were not modified without approval
- no unrelated refactoring or dependency changes were introduced
- relevant build, test, lint, validation, or manual checks were run
- unverified behavior and residual risk are stated explicitly

## Stop Conditions

Stop and ask before continuing when a secret appears, a destructive or irreversible action is required, scope expands materially, a protected change lacks approval, or critical high-risk behavior cannot be verified.
