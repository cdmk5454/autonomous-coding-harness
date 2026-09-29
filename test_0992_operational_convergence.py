"""0.99.2 Operational Convergence & Verification Stabilization focused tests.

Covers the 0.99.2 release contract:
- P0 runtime recovery: bounded same-Job technical recovery requeue with the
  writer-quiescence invariant preserved and product semantic counters intact.
- P0 semantic rework lineage: explicit rework_of linkage, root derivation,
  durable ordinals, invalid/cyclic rejection, and no client-id parsing.
- Review/finalization: deterministic coverage-plan parsing (Unicode line
  boundaries), typed failure stages, binary manual-review disposition,
  finalization-only retry without Worker rerun, verification-only no-delta
  classification.
- Cross-rework review evidence reuse under the exact proof-input contract.
- Convergence telemetry: typed signals, no false positives on normal
  progress, and strictly non-blocking behavior.
- Canonical authority: projections/reports never mutate control state.
"""

from __future__ import annotations

import hashlib
import json
import os
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from convergence_signals import (
    CONVERGENCE_DIFF_EXPANSION_NO_PROOF,
    CONVERGENCE_FINDING_RECURS,
    CONVERGENCE_REWORK_HIGH,
    CONVERGENCE_SOURCE_CHURN,
    CONVERGENCE_VALIDATION_LOOP,
    signals_for_family,
)
from job_contract import JobContractError, JobRequest
from job_queue import (
    AWAITING_QA,
    BLOCKED,
    QUEUED,
    JobStore,
    QueueError,
    RUNTIME_RECOVERY_ELIGIBLE_FAILURE_CODES,
    TECHNICAL_RECOVERY,
)
from manager import Manager
from reviewer import (
    BINARY_DIFF_REQUIRES_MANUAL_REVIEW,
    DiffBatchPlan,
    ReviewResult,
    Reviewer,
    plan_diff_batches,
)
from review_evidence import EVIDENCE_SCHEMA
import review_evidence
from task_state import TaskState
from test_job_control import execution_context, request, valid_enqueue


MODULE_HEADER = "--- [module: suc] ---\n"


def _file_diff(name: str, added: str) -> str:
    return (
        f"{MODULE_HEADER}"
        f"diff --git a/{name} b/{name}\n"
        f"index 1111111..2222222 100644\n"
        f"--- a/{name}\n"
        f"+++ b/{name}\n"
        f"@@ -1,3 +1,4 @@\n"
        f" context line\n"
        f"+{added}\n"
        f" another context\n"
        f" final context\n"
    )


DIFF_V1 = _file_diff("A.java", "added A") + _file_diff("B.java", "added B")
DIFF_V2 = _file_diff("A.java", "added A") + _file_diff("B.java", "added B fixed")


def _result_for(batch, *, passed):
    return ReviewResult(
        passed=passed,
        status=REVIEW_PASS if passed else REVIEW_FAIL,
        reviewed_files=list(batch.changed_files),
        reason="" if passed else "defect in " + ", ".join(batch.changed_files),
    )


REVIEW_PASS = "REVIEW_PASS"
REVIEW_FAIL = "REVIEW_FAIL"


def _runtime_failure_result(
    *,
    code="OPENCODE_PROGRESS_TIMEOUT",
    execution_id="EXE-0992-RUNTIME",
    task_id="TASK-20260914-120000",
    history=None,
):
    # 0.99.2.2 projection: a Worker transport failure with no QA subject is a
    # technical FAILED task (previously emulated as AWAITING_QA).
    return {
        "task_id": task_id,
        "status": "FAILED",
        "stage": "FAILED",
        "success": False,
        "verification_status": "PARTIAL",
        "failure_stage": "WORKER",
        "failure_code": code,
        "failure_origin": "HARNESS_CAUSED",
        "failure_type": "infrastructure",
        "fix_scope": "NON_RETRYABLE",
        "retry_domain": "RUNTIME_RECOVERY",
        "worktree_disposition": "PRESERVED",
        "execution_id": execution_id,
        "runtime_recovery_count": 1,
        "technical_execution_retry_count": 0,
        "product_semantic_retry_count": 0,
        "runtime_recovery_history": list(history or []),
        "changed_files": ["suc/A.java"],
    }


class RuntimeTechnicalRecoveryTests(unittest.TestCase):
    """18.1: recoverable runtime failures requeue the SAME Job, bounded."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "control"
        self.store = JobStore(self.root)
        jobs, _ = valid_enqueue(
            self.store, "batch-0992-recovery", [request(max_outer_attempts=2)]
        )
        self.job_id = jobs[0]["job_id"]
        self.assertTrue(
            self.store.claim_next("owner")["job_id"] == self.job_id
        )
        self.store.mark_task_started(self.job_id, "owner", "TASK-0992-1")

    def _record(self, result):
        return self.store.record_result(self.job_id, "owner", result)

    def test_progress_timeout_requeues_same_job_with_technical_recovery(self):
        recorded = self._record(_runtime_failure_result())
        self.assertEqual(QUEUED, recorded["status"])
        self.assertEqual(TECHNICAL_RECOVERY, recorded["pending_attempt_kind"])
        provenance = recorded["pending_attempt_provenance"]
        self.assertEqual(
            "RUNTIME_FAILURE_AUTO_TECHNICAL_RECOVERY", provenance["source"]
        )
        self.assertEqual(
            "OPENCODE_PROGRESS_TIMEOUT", provenance["failure_code"]
        )
        self.assertEqual(1, recorded["technical_replacement_count"])
        self.assertIn("RUNTIME_TECHNICAL_RECOVERY", recorded["runtime_recovery_context"])
        events = [
            entry.get("event") for entry in recorded.get("history", [])
        ]
        self.assertIn("RUNTIME_FAILURE_TECHNICAL_RECOVERY_QUEUED", events)
        decision = recorded["last_result"].get("queue_decision_code", "")
        self.assertEqual(
            "RUNTIME_FAILURE_TECHNICAL_RECOVERY_QUEUED", decision
        )
        # The gate must not hold the requeued Job back.
        snapshot = self.store.queue_snapshot()
        self.assertNotEqual(self.job_id, snapshot.get("blocked_by_job_id"))
        # Product semantic retry accounting is untouched by technical recovery.
        self.assertEqual(
            0, recorded["last_result"].get("product_semantic_retry_count", -1)
        )

    def test_requeued_job_claims_technical_recovery_reservation(self):
        self._record(_runtime_failure_result())
        claimed = self.store.claim_next("owner")
        self.assertEqual(self.job_id, claimed["job_id"])
        reservation = claimed["active_attempt_reservation"]
        self.assertEqual(TECHNICAL_RECOVERY, reservation["attempt_kind"])
        self.assertEqual(1, reservation["technical_recovery_attempt_no"])
        self.assertEqual(1, reservation["normal_attempt_no"])

    def test_server_start_failure_requeues_without_quiescence_history(self):
        # No turn was ever submitted, so no writer-quiescence evidence is
        # required: confirmation by absence.
        recorded = self._record(
            _runtime_failure_result(
                code="OPENCODE_SERVER_START_TIMEOUT",
                history=[],
            )
        )
        self.assertEqual(QUEUED, recorded["status"])
        self.assertEqual(TECHNICAL_RECOVERY, recorded["pending_attempt_kind"])

    def test_second_runtime_failure_after_budget_spent_stays_blocked(self):
        self._record(_runtime_failure_result())
        self.store.claim_next("owner")
        self.store.mark_task_started(self.job_id, "owner", "TASK-0992-2")
        held = self._record(
            _runtime_failure_result(
                task_id="TASK-20260914-120001",
            )
        )
        self.assertEqual(BLOCKED, held["status"])
        self.assertEqual(
            "PRESERVED_DELTA_TECHNICAL_RECOVERY_REQUIRED",
            held["last_result"].get("queue_decision_code", ""),
        )
        events = [
            entry.get("event") for entry in held.get("history", [])
        ]
        self.assertIn("RUNTIME_TECHNICAL_RECOVERY_HELD", events)

    def test_quiescence_unconfirmed_denies_requeue(self):
        unconfirmed_history = [
            {
                "failure_code": "OPENCODE_PROGRESS_TIMEOUT",
                "state": "FAILED",
                "evidence": {
                    "writer_quiescence": {"quiesced": False, "abort_delivered": True},
                },
            }
        ]
        recorded = self._record(
            _runtime_failure_result(history=unconfirmed_history)
        )
        self.assertEqual(BLOCKED, recorded["status"])
        self.assertEqual("", recorded.get("pending_attempt_kind", ""))
        events = [
            entry.get("event") for entry in recorded.get("history", [])
        ]
        self.assertIn("RUNTIME_TECHNICAL_RECOVERY_HELD", events)

    def test_repository_quiescence_authority_is_fail_closed(self):
        store = JobStore(self.root)
        job = {
            "job_id": self.job_id,
            "outer_attempt": 1,
            "max_outer_attempts": 2,
            "normal_attempt_budget": 2,
            "technical_recovery_budget": 1,
            "technical_recovery_attempt_count": 0,
            "history": [],
        }
        store.repository = SimpleNamespace(
            unconfirmed_writer_quiescence=lambda execution_id: {
                "quiesced": False,
                "confirmed_state": "ACTIVE",
            }
        )
        self.assertIsNone(
            store._plan_runtime_technical_recovery(
                job, _runtime_failure_result()
            )
        )

    def test_repository_confirmed_quiescence_allows_requeue(self):
        store = JobStore(self.root)
        job = {
            "job_id": self.job_id,
            "outer_attempt": 1,
            "max_outer_attempts": 2,
            "normal_attempt_budget": 2,
            "technical_recovery_budget": 1,
            "technical_recovery_attempt_count": 0,
            "history": [],
        }
        store.repository = SimpleNamespace(
            unconfirmed_writer_quiescence=lambda execution_id: None
        )
        plan = store._plan_runtime_technical_recovery(
            job, _runtime_failure_result()
        )
        self.assertIsNotNone(plan)
        self.assertEqual(
            "OPENCODE_PROGRESS_TIMEOUT", plan["failure_code"]
        )

    def test_non_eligible_runtime_code_stays_blocked_without_requeue(self):
        self.assertNotIn(
            "OPENCODE_MODEL_ID_INVALID", RUNTIME_RECOVERY_ELIGIBLE_FAILURE_CODES
        )
        recorded = self._record(
            _runtime_failure_result(code="OPENCODE_MODEL_ID_INVALID")
        )
        self.assertEqual(BLOCKED, recorded["status"])

    def test_semantic_failure_domain_never_auto_requeues(self):
        semantic = _runtime_failure_result(code="BUILD_FAIL")
        semantic.update(
            {
                "failure_origin": "WORKER_CAUSED",
                "failure_type": "build",
                "retry_domain": "PRODUCT_SEMANTIC_RETRY",
                "status": "FAILED",
            }
        )
        recorded = self._record(semantic)
        self.assertNotEqual(QUEUED, recorded["status"])
        self.assertEqual(
            0, int(recorded.get("technical_replacement_count", 0))
        )


class SemanticReworkLineageTests(unittest.TestCase):
    """18.2: explicit cross-Job semantic rework lineage."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "control"

    def _terminal_root(self, store, client_id="root-1"):
        jobs, _ = valid_enqueue(
            store, f"batch-{client_id}", [request(client_job_id=client_id)]
        )
        job_id = jobs[0]["job_id"]
        self.assertTrue(store.claim_next("owner"))
        store.mark_task_started(job_id, "owner", "TASK-ROOT")
        store.record_result(
            job_id,
            "owner",
            {
                "task_id": "TASK-ROOT",
                "status": "SUCCESS",
                "stage": "DONE",
                "success": True,
                "verification_status": "VERIFIED",
                "failure_code": "",
                "changed_files": ["suc/A.java"],
            },
        )
        return store.get_job(job_id)

    def test_root_and_ordinal_derivation_with_durable_reload(self):
        store = JobStore(self.root)
        root = self._terminal_root(store)
        rework_jobs, _ = valid_enqueue(
            store,
            "batch-rework-1",
            [request(client_job_id="root-1-R1", rework_of_job_id="root-1")],
        )
        rework1 = rework_jobs[0]
        self.assertEqual(root["job_id"], rework1["rework_of_job_id"])
        self.assertEqual(root["job_id"], rework1["semantic_root_job_id"])
        self.assertEqual(1, rework1["semantic_rework_ordinal"])
        # Root normalized in place as its own family root.
        reloaded_root = store.get_job(root["job_id"])
        self.assertEqual(root["job_id"], reloaded_root["semantic_root_job_id"])
        self.assertEqual(0, int(reloaded_root["semantic_rework_ordinal"]))
        # Second rework references the first rework: same root, ordinal 2.
        rework_jobs_2, _ = valid_enqueue(
            store,
            "batch-rework-2",
            [request(client_job_id="root-1-R2", rework_of_job_id=rework1["job_id"])],
        )
        rework2 = rework_jobs_2[0]
        self.assertEqual(root["job_id"], rework2["semantic_root_job_id"])
        self.assertEqual(2, rework2["semantic_rework_ordinal"])
        # Durable reload: a fresh store reads identical lineage truth.
        fresh = JobStore(self.root)
        fresh_rework2 = fresh.get_job(rework2["job_id"])
        self.assertEqual(
            rework2["semantic_root_job_id"], fresh_rework2["semantic_root_job_id"]
        )
        self.assertEqual(
            rework2["semantic_rework_ordinal"], fresh_rework2["semantic_rework_ordinal"]
        )
        snapshot = fresh.queue_snapshot()
        self.assertEqual(2, snapshot["semantic_rework_job_count"])
        self.assertEqual(1, snapshot["semantic_rework_family_count"])

    def test_unknown_rework_target_rejected(self):
        store = JobStore(self.root)
        with self.assertRaisesRegex(QueueError, "REWORK_TARGET_NOT_FOUND"):
            valid_enqueue(
                store,
                "batch-rework-missing",
                [request(client_job_id="bad-1", rework_of_job_id="no-such-job")],
            )

    def test_cyclic_lineage_rejected(self):
        store = JobStore(self.root)
        root = self._terminal_root(store)
        jobs, _ = valid_enqueue(
            store,
            "batch-cycle",
            [request(client_job_id="cyc-a", rework_of_job_id=root["job_id"])],
        )
        cyc_a = jobs[0]
        # Forcing an A -> A self-cycle by direct state edit (defensive guard
        # must not trust stored roots to short-circuit the chain walk).
        cyc_a_record = store._read_job(cyc_a["job_id"])
        cyc_a_record["rework_of_job_id"] = cyc_a["job_id"]
        cyc_a_record.pop("semantic_root_job_id", None)
        store._write_job(cyc_a_record)
        with self.assertRaisesRegex(QueueError, "REWORK_LINEAGE_CYCLIC"):
            valid_enqueue(
                store,
                "batch-cycle-2",
                [request(client_job_id="cyc-b", rework_of_job_id=cyc_a["job_id"])],
            )

    def test_invalid_rework_reference_format_rejected(self):
        with self.assertRaisesRegex(JobContractError, "INVALID_REWORK_TARGET"):
            request(client_job_id="fmt-1", rework_of_job_id="bad reference!")

    def test_client_id_suffixes_are_never_parsed_into_lineage(self):
        store = JobStore(self.root)
        self._terminal_root(store)
        jobs, _ = valid_enqueue(
            store,
            "batch-suffix",
            [request(client_job_id="root-1-R7")],
        )
        job = jobs[0]
        self.assertEqual("", job["rework_of_job_id"])
        self.assertEqual("", job["semantic_root_job_id"])
        self.assertEqual(0, int(job["semantic_rework_ordinal"]))

    def test_verification_only_flag_flows_into_job(self):
        store = JobStore(self.root)
        jobs, _ = valid_enqueue(
            store,
            "batch-verify-only",
            [request(client_job_id="vo-1", verification_only=True)],
        )
        self.assertTrue(jobs[0]["verification_only"])
        with self.assertRaisesRegex(JobContractError, "INVALID_TYPE"):
            request(client_job_id="vo-2", verification_only="yes")


class SemanticEvidenceInheritanceTests(unittest.TestCase):
    """18.3 (queue side): rework enqueue seeds prior evidence candidates."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "control"

    def test_rework_enqueue_seeds_prior_review_evidence(self):
        store = JobStore(self.root)
        # Terminal root with a saved task state carrying review evidence.
        jobs, _ = valid_enqueue(
            store, "batch-seed", [request(client_job_id="seed-root")]
        )
        root_id = jobs[0]["job_id"]
        task_path = Path(self.temp.name) / "20260914" / "TASK-SEED.json"
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_text(
            json.dumps(
                {
                    "task_id": "TASK-SEED",
                    "review_evidence": {
                        "schema": EVIDENCE_SCHEMA,
                        "proof_inputs": {
                            "acceptance_hash": "acc",
                            "contract_revision": 1,
                            "execution_surface_hash": "surface",
                        },
                        "units": {
                            "suc|A.java|h1|c1": {
                                "chunk_id": "x",
                                "round": 1,
                            }
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        store.claim_next("owner")
        store.mark_task_started(root_id, "owner", "TASK-SEED")
        store.record_result(
            root_id,
            "owner",
            {
                "task_id": "TASK-SEED",
                "status": "SUCCESS",
                "stage": "DONE",
                "success": True,
                "verification_status": "VERIFIED",
                "failure_code": "",
                "task_state_path": task_path.as_posix(),
            },
        )
        rework_jobs, _ = valid_enqueue(
            store,
            "batch-seed-rework",
            [request(client_job_id="seed-r1", rework_of_job_id="seed-root")],
        )
        rework = rework_jobs[0]
        self.assertEqual(root_id, rework["semantic_prior_evidence_source_job_id"])
        seeded = rework["semantic_prior_review_evidence"]
        self.assertEqual(EVIDENCE_SCHEMA, seeded.get("schema"))
        self.assertIn("suc|A.java|h1|c1", seeded.get("units", {}))


class ReviewPlanDeterminismTests(unittest.TestCase):
    """18.4: same diff -> same valid plan; typed failure classification."""

    def test_unicode_line_boundary_content_no_longer_corrupts_parsing(self):
        # A form feed and a Unicode line separator inside diff body content
        # are legal bytes of one git line; splitting on them previously
        # produced 'hunk range count mismatch' transport corruption.
        diff = (
            f"{MODULE_HEADER}"
            "diff --git a/F.vue b/F.vue\n"
            "index 1111111..2222222 100644\n"
            "--- a/F.vue\n"
            "+++ b/F.vue\n"
            "@@ -1,3 +1,4 @@\n"
            " context \x0c formfeed\n"
            "+added line \u2028 separator\n"
            " another context\n"
            " final context\n"
        )
        plan = plan_diff_batches(diff, max_chars=400, max_batches=4)
        self.assertTrue(plan.valid, plan.reason)
        self.assertEqual(1, len(plan.batches))
        self.assertEqual(tuple(plan.expected_atoms), tuple(plan.assigned_atoms))

    def test_identical_inputs_produce_identical_plans(self):
        first = plan_diff_batches(DIFF_V1, max_chars=300, max_batches=12)
        second = plan_diff_batches(DIFF_V1, max_chars=300, max_batches=12)
        self.assertEqual(
            [batch.batch_id for batch in first.batches],
            [batch.batch_id for batch in second.batches],
        )
        self.assertEqual(
            [
                chunk.chunk_id
                for batch in first.batches
                for chunk in batch.chunks
            ],
            [
                chunk.chunk_id
                for batch in second.batches
                for chunk in batch.chunks
            ],
        )

    def test_failure_stages_are_typed_and_deterministic(self):
        bad_header = (
            f"{MODULE_HEADER}"
            "diff --git a/A.java b/A.java\n"
            "index 1111111..2222222 100644\n"
            "--- a/A.java\n"
            "+++ b/A.java\n"
            "@@ -1,x +1,4 @@\n"
            " context\n"
        )
        plan = plan_diff_batches(bad_header, max_chars=400, max_batches=4)
        self.assertFalse(plan.valid)
        self.assertEqual("HUNK_HEADER", plan.failure_stage)
        range_mismatch = (
            f"{MODULE_HEADER}"
            "diff --git a/A.java b/A.java\n"
            "index 1111111..2222222 100644\n"
            "--- a/A.java\n"
            "+++ b/A.java\n"
            "@@ -1,3 +1,4 @@\n"
            " context\n"
        )
        plan = plan_diff_batches(range_mismatch, max_chars=400, max_batches=4)
        self.assertFalse(plan.valid)
        self.assertEqual("HUNK_RANGE", plan.failure_stage)
        stray = "content before any diff\n" + DIFF_V1
        plan = plan_diff_batches(stray, max_chars=400, max_batches=4)
        self.assertFalse(plan.valid)
        self.assertEqual("DIFF_SECTION", plan.failure_stage)
        # The typed stage flows into the pre-review rejection receipt.
        result = Reviewer.plan_failure_result(
            plan, ["suc/A.java", "suc/B.java"]
        )
        self.assertEqual("DIFF_SECTION", result.coverage["failure_stage"])

    def test_binary_diff_gets_manual_review_disposition(self):
        diff = (
            f"{MODULE_HEADER}"
            "diff --git a/report.xlsx b/report.xlsx\n"
            "index 1111111..2222222 100644\n"
            "Binary files a/report.xlsx and b/report.xlsx differ\n"
        )
        plan = plan_diff_batches(diff, max_chars=400, max_batches=4)
        self.assertFalse(plan.valid)
        self.assertEqual(BINARY_DIFF_REQUIRES_MANUAL_REVIEW, plan.failure_code)
        self.assertEqual("BINARY_ARTIFACT", plan.failure_stage)
        result = Reviewer.plan_failure_result(plan, ["suc/report.xlsx"])
        self.assertEqual(
            "MANUAL_REVIEW_REQUIRED", result.coverage["review_disposition"]
        )
        self.assertTrue(result.coverage["manual_review_required"])
        self.assertEqual(
            "BINARY_OR_NON_TEXT_ARTIFACT", result.coverage["manual_review_reason"]
        )


class CrossReworkEvidenceReuseTests(unittest.TestCase):
    """18.3: family predecessor PASS units reuse under exact proof inputs."""

    def setUp(self):
        self.plan_v1 = plan_diff_batches(DIFF_V1, max_chars=300, max_batches=12)
        self.plan_v2 = plan_diff_batches(DIFF_V2, max_chars=300, max_batches=12)
        self.assertTrue(self.plan_v1.valid)
        self.proof = {
            "acceptance_hash": "acc-hash-0992",
            "contract_revision": 1,
            "execution_surface_hash": "surface-hash-0992",
        }
        self.manager = object.__new__(Manager)
        self.identity = {
            "acceptance_hash": self.proof["acceptance_hash"],
            "execution_surface_hash": self.proof["execution_surface_hash"],
        }

    def _prior_evidence(self):
        pairs = [
            (batch, _result_for(batch, passed=True))
            for batch in self.plan_v1.batches
        ]
        return review_evidence.record(pairs, round_no=1, **self.proof)

    def test_same_semantic_root_same_proof_inputs_reuse_eligible(self):
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1},
            review_evidence={},
            inherited_review_evidence=self._prior_evidence(),
            inherited_review_evidence_source="JOB-PRIOR",
        )
        merged, partition, carrier = self.manager._plan_review_reuse(
            task, self.plan_v2, self.identity
        )
        self.assertIsNotNone(carrier)
        self.assertIsNotNone(partition)
        self.assertEqual(
            "SEMANTIC_FAMILY_JOB:JOB-PRIOR", partition.prior_source
        )
        residual_files = {
            name for batch in merged.batches[1:] for name in batch.changed_files
        }
        self.assertEqual({"suc/B.java"}, residual_files)

    def test_changed_acceptance_is_stale(self):
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1},
            review_evidence={},
            inherited_review_evidence=self._prior_evidence(),
            inherited_review_evidence_source="JOB-PRIOR",
        )
        stale_identity = dict(self.identity)
        stale_identity["acceptance_hash"] = "changed-acceptance"
        plan, partition, carrier = self.manager._plan_review_reuse(
            task, self.plan_v2, stale_identity
        )
        self.assertIsNone(carrier)
        self.assertIsNone(partition)
        self.assertIs(self.plan_v2, plan)

    def test_changed_contract_revision_is_stale(self):
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 2},
            review_evidence={},
            inherited_review_evidence=self._prior_evidence(),
            inherited_review_evidence_source="JOB-PRIOR",
        )
        _, _, carrier = self.manager._plan_review_reuse(
            task, self.plan_v2, self.identity
        )
        self.assertIsNone(carrier)

    def test_high_depth_policy_requires_full_review(self):
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DOMAIN"},
            materialized_execution={"contract_revision": 1},
            review_evidence={},
            inherited_review_evidence=self._prior_evidence(),
            inherited_review_evidence_source="JOB-PRIOR",
        )
        plan, partition, carrier = self.manager._plan_review_reuse(
            task, self.plan_v2, self.identity
        )
        self.assertIsNone(carrier)
        self.assertIsNone(partition)
        self.assertIs(self.plan_v2, plan)

    def test_changed_content_units_are_stale(self):
        # Prior record only proves A.java; every B.java chunk is STALE.
        pairs = [
            (batch, _result_for(batch, passed=("suc/B.java" not in batch.changed_files)))
            for batch in self.plan_v1.batches
        ]
        partial = review_evidence.record(pairs, round_no=1, **self.proof)
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1},
            review_evidence={},
            inherited_review_evidence=partial,
            inherited_review_evidence_source="JOB-PRIOR",
        )
        merged, partition, carrier = self.manager._plan_review_reuse(
            task, self.plan_v2, self.identity
        )
        self.assertIsNotNone(carrier)
        self.assertIn("suc/B.java", partition.stale_files)
        self.assertNotIn("suc/B.java", partition.reused_files)

    def test_record_chains_inherited_units_under_new_proof(self):
        task = SimpleNamespace(
            retry_count=0,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1},
            review_evidence={},
            report_diagnostics=[],
            inherited_review_evidence=self._prior_evidence(),
            inherited_review_evidence_source="JOB-PRIOR",
        )
        pairs = [
            (batch, _result_for(batch, passed=True))
            for batch in self.plan_v2.batches
        ]
        self.manager._record_review_evidence(task, pairs, self.identity, None)
        units = task.review_evidence["units"]
        self.assertEqual(
            self.proof["acceptance_hash"],
            task.review_evidence["proof_inputs"]["acceptance_hash"],
        )
        self.assertTrue(units)


class FinalizationRetryTests(unittest.TestCase):
    """18.4: finalization-only storage failures never rerun the Worker."""

    def setUp(self):
        from harness_service import HarnessService

        self.service = object.__new__(HarnessService)
        self.task = SimpleNamespace(report_diagnostics=[])

    def test_transient_failure_is_retried_in_place(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("transient lock")
            return "done"

        value, exc = self.service._finalization_retry(self.task, "step", flaky)
        self.assertEqual("done", value)
        self.assertIsNone(exc)
        self.assertEqual(2, calls["n"])
        self.assertEqual(
            "FINALIZATION_STEP_RETRIED",
            self.task.report_diagnostics[0]["code"],
        )

    def test_persistent_failure_keeps_typed_semantics(self):
        def broken():
            raise OSError("persistent")

        value, exc = self.service._finalization_retry(self.task, "step", broken)
        self.assertIsNone(value)
        self.assertIsInstance(exc, OSError)
        self.assertEqual([], self.task.report_diagnostics)


class ConvergenceSignalTests(unittest.TestCase):
    """18.5: typed convergence signals from durable evidence."""

    def _job(self, job_id, *, rework_of="", ordinal=0, result=None):
        return {
            "job_id": job_id,
            "rework_of_job_id": rework_of,
            "semantic_root_job_id": "JOB-ROOT",
            "semantic_rework_ordinal": ordinal,
            "sequence": ordinal,
            "last_result": dict(result or {}),
        }

    def test_rework_high_detected(self):
        family = [
            self._job("JOB-ROOT"),
            self._job("JOB-R1", rework_of="JOB-ROOT", ordinal=1),
            self._job("JOB-R2", rework_of="JOB-R1", ordinal=2, result={"success": False}),
        ]
        signals = signals_for_family(family)
        codes = {item["signal"] for item in signals}
        self.assertIn(CONVERGENCE_REWORK_HIGH, codes)

    def test_finding_recurrence_and_source_churn_detected(self):
        family = [
            self._job("JOB-ROOT", result={
                "review_violations": ["SEC_SECRET_IN_SOURCE"],
                "changed_files": ["suc/A.java"],
            }),
            self._job(
                "JOB-R1",
                rework_of="JOB-ROOT",
                ordinal=1,
                result={
                    "review_violations": ["SEC_SECRET_IN_SOURCE"],
                    "changed_files": ["suc/A.java"],
                },
            ),
            self._job(
                "JOB-R2",
                rework_of="JOB-R1",
                ordinal=2,
                result={
                    "review_violations": ["SEC_SECRET_IN_SOURCE"],
                    "changed_files": ["suc/A.java"],
                },
            ),
        ]
        signals = signals_for_family(family)
        codes = {item["signal"] for item in signals}
        self.assertIn(CONVERGENCE_FINDING_RECURS, codes)
        self.assertIn(CONVERGENCE_SOURCE_CHURN, codes)

    def test_validation_loop_detected(self):
        family = [
            self._job("JOB-ROOT", result={"candidate_id": "CAND-1"}),
            self._job(
                "JOB-R1",
                rework_of="JOB-ROOT",
                ordinal=1,
                result={"candidate_id": "CAND-1"},
            ),
        ]
        signals = signals_for_family(family)
        codes = {item["signal"] for item in signals}
        self.assertIn(CONVERGENCE_VALIDATION_LOOP, codes)

    def test_diff_expansion_without_proof_progress(self):
        family = [
            self._job("JOB-ROOT", result={"success": False, "changed_files": ["a"]}),
            self._job("JOB-R1", rework_of="JOB-ROOT", ordinal=1,
                      result={"success": False, "changed_files": ["a", "b"]}),
            self._job("JOB-R2", rework_of="JOB-R1", ordinal=2,
                      result={"success": False, "changed_files": ["a", "b", "c"]}),
        ]
        codes = {item["signal"] for item in signals_for_family(family)}
        self.assertIn(CONVERGENCE_DIFF_EXPANSION_NO_PROOF, codes)

    def test_normal_multi_step_progress_has_no_false_signal(self):
        family = [
            self._job("JOB-ROOT", result={
                "success": False,
                "review_violations": ["FIRST_FINDING"],
                "changed_files": ["suc/A.java"],
            }),
            self._job("JOB-R1", rework_of="JOB-ROOT", ordinal=1, result={
                "success": True,
                "review_violations": [],
                "changed_files": ["suc/B.java"],
            }),
        ]
        self.assertEqual([], signals_for_family(family))

    def test_signals_are_recorded_without_blocking_success(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / "control")
            jobs, _ = valid_enqueue(
                store, "batch-conv", [request(client_job_id="conv-root")]
            )
            root_id = jobs[0]["job_id"]
            store.claim_next("owner")
            store.mark_task_started(root_id, "owner", "TASK-CONV-1")
            store.record_result(
                root_id,
                "owner",
                {
                    "task_id": "TASK-CONV-1",
                    "status": "SUCCESS",
                    "stage": "DONE",
                    "success": True,
                    "verification_status": "VERIFIED",
                    "failure_code": "",
                    "changed_files": ["suc/A.java"],
                },
            )
            # ATTENDED human gate: acknowledge the strict success explicitly.
            store.acknowledge_success(root_id, "ack-conv-root")
            rework_jobs, _ = valid_enqueue(
                store,
                "batch-conv-r1",
                [request(client_job_id="conv-r1", rework_of_job_id="conv-root")],
            )
            rework = rework_jobs[0]
            store.claim_next("owner")
            store.mark_task_started(rework["job_id"], "owner", "TASK-CONV-2")
            recorded = store.record_result(
                rework["job_id"],
                "owner",
                {
                    "task_id": "TASK-CONV-2",
                    "status": "SUCCESS",
                    "stage": "DONE",
                    "success": True,
                    "verification_status": "VERIFIED",
                    "failure_code": "",
                    "changed_files": ["suc/B.java"],
                },
            )
            # Convergence telemetry exists on the durable record while the
            # canonical status stays SUCCEEDED — never a gate.
            self.assertEqual("SUCCEEDED", recorded["status"])
            self.assertIn("convergence_signals", recorded)


class VerificationOnlyNoDeltaTests(unittest.TestCase):
    """18.4/8.6: typed no-delta classification A/B/C at the source delta gate."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._old_cwd)
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir,
            model="",
            max_retry=1,
            timeout=5,
            worker_type="droid",
            max_workers=1,
        )
        self.manager.profile = SimpleNamespace(
            profile_dir=Path(self._tmp.name), module_names=["suc", "sucui"]
        )
        self.manager.shutdown = self.manager.shutdown
        self.addCleanup(self.manager.shutdown)

    def _run_no_delta(self, *, verification_only, requirement):
        from worker import WorkerResult
        from git_collector import GitDiffResult
        from build_test_runner import BuildTestResult

        wr = WorkerResult(success=True, exit_code=0, stdout="", stderr="", command=[])
        git = GitDiffResult(success=True, changed_files=[], diff="")
        build = BuildTestResult(success=True, build_status="PASS", test_status="SKIPPED")
        task = TaskState(
            task_id="TASK-0992-NODELTA",
            requirement=requirement,
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
        )
        task.verification_only = verification_only

        class _Collector:
            def capture_baseline(self, task=None):
                return object(), ""

            def collect(self, task=None, baseline=None):
                task.changed_files = []
                task.git_status = ""
                task.git_diff = ""
                return git

            def repair_eol_to_index(self, task=None, baseline=None):
                from git_collector import EolRepairResult

                return EolRepairResult(True, [], [], "")

            def assess_commit_safety(self, task=None, baseline=None):
                return True, [], {}

        class _Worker:
            calls = 0

            def execute(self, requirement, working_dir, task=None):
                type(self).calls += 1
                return wr

        class _Runner:
            def run(self, changed_files=None, target_modules=None):
                return build

        class _Reviewer:
            def review(self, task=None, **kwargs):
                task.review_status = "REVIEW_PASS"
                return ReviewResult(passed=True, status="REVIEW_PASS")

        self.manager.collector = _Collector()
        self.manager._get_worker = lambda _task: (_Worker(), "droid")
        self.manager.build_test_runner = _Runner()
        self.manager.reviewer = _Reviewer()
        result = self.manager._run_pipeline(task)
        return result, task

    def test_worker_no_change_stays_fail_closed(self):
        result, task = self._run_no_delta(
            verification_only=False,
            requirement="suc/src/main/java/App.java 로직을 수정한다",
        )
        self.assertFalse(result.success)
        self.assertEqual("NO_MEANINGFUL_SOURCE_CHANGE", task.failure_code)
        self.assertEqual("WORKER_NO_CHANGE", task.no_delta_classification)
        self.assertEqual("SOURCE_DELTA_GATE", task.failure_stage)

    def test_verification_only_contract_validates_current_state(self):
        result, task = self._run_no_delta(
            verification_only=True,
            requirement="suc/src/main/java/App.java 현재 구현을 검증한다",
        )
        self.assertEqual("", task.failure_code)
        self.assertEqual("VERIFICATION_ONLY", task.no_delta_classification)
        self.assertNotEqual(
            "NO_MEANINGFUL_SOURCE_CHANGE", task.failure_code
        )


class CanonicalAuthorityProjectionTests(unittest.TestCase):
    """18.7: projections and reports never mutate canonical control state."""

    def test_report_and_projection_do_not_touch_control_repository(self):
        import sqlite3

        from control_repository import ControlRepository
        from progress_projection import queue_progress_snapshot
        from reporter import Reporter

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "control.sqlite3"
            repo = ControlRepository(path, create=True)
            job_record = {
                "job_id": "JOB-1",
                "logical_job_id": "LOGICAL-1",
                "client_job_id": "CLIENT-1",
                "status": "QUEUED",
                "revision": 0,
                "contract_revision": 1,
                "owner_id": "",
                "request": {"worker": "codex", "model": "gpt-5.6-sol"},
                "depends_on": [],
            }
            repo.bootstrap(
                {"revision": 0, "generation": 1, "order": ["JOB-1"]},
                {"JOB-1": job_record},
                {"JOB-1": {"path": "jobs/JOB-1.json", "sha256": "fixture"}},
            )

            def dump() -> str:
                connection = sqlite3.connect(path)
                try:
                    parts = []
                    for (name,) in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "ORDER BY name"
                    ):
                        rows = connection.execute(
                            f"SELECT * FROM '{name}'"
                        ).fetchall()
                        parts.append(f"{name}:{rows!r}")
                    return hashlib.sha256("|".join(parts).encode()).hexdigest()
                finally:
                    connection.close()

            before = dump()
            task = TaskState(
                task_id="TASK-20260914-130000",
                requirement="projection authority probe",
                working_dir=temp,
                task_mode="MODIFICATION",
            )
            report_dir = Path(temp) / "reports"
            report_dir.mkdir()
            Reporter().save_markdown(task, model="m", out_dir=report_dir)
            queue = {
                "schema_version": 1,
                "revision": 0,
                "running_job_id": "",
                "operator_paused": False,
                "paused": False,
            }
            jobs = [
                {
                    "job_id": "JOB-1",
                    "status": "SUCCEEDED",
                    "task_ids": ["TASK-20260914-130000"],
                    "history": [
                        {"at": "2026-09-14T13:00:00", "event": "CLAIMED"},
                        {"at": "2026-09-14T13:05:00", "event": "TASK_FINISHED",
                         "status": "SUCCEEDED"},
                    ],
                }
            ]
            snapshot = queue_progress_snapshot(queue, jobs, None)
            self.assertIsNotNone(snapshot)
            # A report claiming SUCCESS must not canonize the QUEUED job.
            reloaded = repo.read_job("JOB-1")
            self.assertEqual("QUEUED", reloaded["status"])
            self.assertEqual(before, dump())


if __name__ == "__main__":
    unittest.main()
