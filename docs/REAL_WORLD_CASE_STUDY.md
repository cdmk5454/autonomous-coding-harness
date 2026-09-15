# Anonymized engineering case study

The Harness has been exercised in a real-world legacy multi-module web development workflow. This account intentionally omits the organization, system, modules, schemas, endpoints, screens, credentials, and business requirements.

## 0.99.1 — correctness first

A coding-agent runtime failure can leave writer ownership ambiguous even after the client process stops. The Lean Execution Kernel separates provider/session state from filesystem write authority: replacement writer authority is withheld until the prior writer is quiesced or otherwise safely fenced. Scoped rollback preserves previously accepted batch changes, and Build/Test/Review/Verification evidence remains bound to the frozen candidate and contract.

## What real workload evidence exposed

After the correctness baseline was established, Product usage showed a different class of cost: recoverable runtime failures could still return to a human too early, semantic rework could be fragmented across separately enqueued Jobs, Review finalization failures could obscure where the failure actually occurred, and repeated activity did not necessarily mean the Job was converging toward Acceptance.

## 0.99.2 — operational convergence

The next release kept the single-worker topology and addressed those measured costs instead of adding more agents. It introduced bounded same-Job technical recovery, explicit cross-Job semantic rework lineage, typed Review-plan diagnostics, finalization-only retries, conservative cross-rework Review evidence reuse, non-blocking convergence signals derived from durable records, and an actual Git rollback rehearsal proving restoration to the exact prior accepted state.

The design rule remained unchanged: technical/runtime failure is not Product semantic failure, activity is not meaningful progress, and meaningful progress is not automatically goal convergence.

The next major milestone is bounded parallel execution, not an unrestricted multi-agent society.
