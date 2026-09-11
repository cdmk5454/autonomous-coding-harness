# Anonymized engineering case study

The Harness has been exercised in a real-world legacy multi-module web development workflow. This account intentionally omits the organization, system, modules, schemas, endpoints, screens, and requirements.

A coding-agent runtime failure can leave writer ownership ambiguous even after the client process stops. The 0.99.1 execution path treats provider session state and filesystem write authority separately. A persistent runtime is aborted and queried within a bound before replacement writer authority is granted. An unconfirmed quiescence result preserves the candidate and blocks replacement.

When a Job fails after changing files, rollback is scoped to that Job's accepted prestate. Previously accepted batch changes remain preserved. Snapshot, candidate, and changed-file evidence provide the custody boundary used by recovery.

Runtime and provider failures enter technical recovery with zero product retry delta. A verified product defect uses the separate semantic retry budget. This prevents infrastructure instability from being reported as repeated product failure.

Worker completion alone does not establish success. Build, Test, Review, and Verification produce separate evidence bound to the frozen contract and candidate. Missing, stale, or mismatched evidence blocks publication and prevents false success.
