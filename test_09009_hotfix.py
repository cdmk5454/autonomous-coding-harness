"""Focused 0.9.0.9 hotfix regressions: job-scoped QA gate ownership.

Covers the historical AWAITING_QA reconciliation defect (H01), the explicit
corrective successor gate handoff (H02), and AUTONOMOUS independent
continuation under a Job-scoped QA hold (H03). The historical QA Job is
immutable evidence throughout; no status, Review, Verification, or
failure_code is ever rewritten by the handoff.
"""

from __future__ import annotations

import copy
import harness_temp as tempfile
import unittest
from pathlib import Path

from job_contract import JobRequest
from job_queue import (
    AWAITING_QA,
    FAILED_FINAL,
    QUEUED,
    SUCCEEDED,
    SUCCESS_ACK_REQUIRED,
    EXECUTION_MODE_AUTONOMOUS,
    AUTONOMOUS_CONFIRMATION,
    QA_CORRECTIVE_CONFIRMATION,
    JobStore,
    QueueError,
)


def context() -> dict[str, object]:
    return {
        "profile_id": "test",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }


def request(
    client: str,
    resources: list[str] | None = None,
    depends_on: list[str] | None = None,
) -> JobRequest:
    return JobRequest.from_mapping({
        "client_job_id": client,
        "requirement": f"{client} acceptance",
        "worker": "codex", "model": "gpt-5.6-sol",
        "reasoning_effort": "medium", "target_modules": ["app"],
        "target_resources": resources or [f"app/{client}.txt"],
        "max_outer_attempts": 1,
        "depends_on": [] if depends_on is None else depends_on,
    }, allowed_modules=("app",))


def success(task_id: str = "TASK-X") -> dict[str, object]:
    return {
        "task_id": task_id, "status": "SUCCESS", "stage": "DONE",
        "success": True, "build_status": "PASS", "test_status": "PASS",
        "review_status": "REVIEW_PASS", "verification_status": "VERIFIED",
        "failure_code": "", "secondary_failures": [],
        "changed_files": ["app/a.txt"], "worktree_disposition": "PRESERVED",
    }


def qa_result(task_id: str, changed: list[str]) -> dict[str, object]:
    return {
        "task_id": task_id, "status": "AWAITING_QA", "stage": "AWAITING_QA",
        "success": False, "verification_status": "FAILED",
        "review_status": "REVIEW_FAIL",
        "failure_code": "USER_QA_REQUIRED",
        "failure_type": "user_qa", "fix_scope": "NON_RETRYABLE",
        "worktree_disposition": "CLEAN_ROLLBACK",
        "qa_type": "HUMAN_ACCEPTANCE", "hold_scope": "JOB",
        "machine_verified": False,
        "candidate_id": "CAND-" + "A" * 24,
        "candidate_manifest": "candidate.json",
        "changed_files": changed,
    }


class QaGateHandoffBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "control"

    def make_store(self) -> JobStore:
        return JobStore(self.root)

    def land_qa(self, store: JobStore, client: str, changed: list[str]) -> dict:
        """Drive one Job to a job-scoped historical AWAITING_QA terminal state."""
        claimed = store.claim_next("owner-qa")
        assert claimed is not None and claimed["client_job_id"] == client
        store.mark_task_started(claimed["job_id"], "owner-qa", f"TASK-{client}")
        job = store.record_result(
            claimed["job_id"], "owner-qa", qa_result(f"TASK-{client}", changed)
        )
        self.assertEqual(AWAITING_QA, job["status"])
        return job

    def handoff(
        self,
        store: JobStore,
        qa_job: dict,
        corrective: dict,
        *,
        request_id: str = "qa-handoff-1",
        queue_revision: int | None = None,
        job_revision: int | None = None,
        confirmation: str = QA_CORRECTIVE_CONFIRMATION,
    ):
        queue = store.queue_snapshot()
        return store.record_qa_corrective_successor(
            qa_job["job_id"],
            corrective["job_id"],
            expected_queue_revision=(
                queue["revision"] if queue_revision is None else queue_revision
            ),
            expected_job_revision=(
                corrective["revision"] if job_revision is None else job_revision
            ),
            request_id=request_id,
            reason="USER_QA_CORRECTIVE",
            confirmation=confirmation,
        )


class HistoricalQaReconciliationTest(QaGateHandoffBase):
    """H01: start reconciliation must not resurrect a Job-scoped QA gate."""

    def test_restart_reconciliation_keeps_job_scoped_qa_non_global(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-restart", [request("J1"), request("J2")], execution_context=context()
        )
        self.land_qa(store, "J1", ["app/J1.txt"])
        historical = store.get_job(jobs[0]["job_id"])
        historical_before = copy.deepcopy(historical)

        # A production start always reconciles durable state first. The
        # historical job-scoped QA must not regain the live global gate.
        restarted = self.make_store()
        self.assertEqual([], restarted.reconcile_interrupted())
        snapshot = restarted.queue_snapshot()
        self.assertEqual("", snapshot["blocked_by_job_id"])
        self.assertEqual("", snapshot["gate_reason"])
        self.assertFalse(snapshot["paused"])
        self.assertEqual(
            AWAITING_QA, restarted.get_job(jobs[0]["job_id"])["status"]
        )
        self.assertEqual(
            historical_before["revision"],
            restarted.get_job(jobs[0]["job_id"])["revision"],
        )
        claimed = restarted.claim_next("owner-next")
        self.assertEqual(jobs[1]["job_id"], claimed["job_id"])

    def test_batch_scoped_qa_still_gates_globally(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-batch", [request("J1"), request("J2")], execution_context=context()
        )
        claimed = store.claim_next("owner-qa")
        store.mark_task_started(claimed["job_id"], "owner-qa", "TASK-J1")
        result = {
            **qa_result("TASK-J1", ["app/J1.txt"]),
            "hold_scope": "BATCH",
            "qa_type": "SAFETY_INTEGRITY",
        }
        store.record_result(claimed["job_id"], "owner-qa", result)
        restarted = self.make_store()
        restarted.reconcile_interrupted()
        snapshot = restarted.queue_snapshot()
        self.assertEqual(jobs[0]["job_id"], snapshot["blocked_by_job_id"])
        self.assertTrue(snapshot["paused"])
        self.assertIsNone(restarted.claim_next("owner-next"))


class CorrectiveSuccessorHandoffTest(QaGateHandoffBase):
    """H02: explicit corrective successor owns the QA hold's active path."""

    def setUp(self):
        super().setUp()
        self.store = self.make_store()
        jobs, _ = self.store.enqueue_batch(
            "qa-corrective",
            [request("J1", ["app/J1.txt", "app/J1-extra.txt"]), request("J1-C1", ["app/J1.txt"])],
            execution_context=context(),
        )
        self.qa_job = self.land_qa(self.store, "J1", ["app/J1.txt", "app/J1-extra.txt"])
        self.corrective = self.store.get_job(jobs[1]["job_id"])

    def test_declared_successor_is_runnable_and_history_stays_immutable(self):
        self.qa_job = self.store.get_job(self.qa_job["job_id"])
        qa_before = copy.deepcopy(self.qa_job)
        linked, replayed = self.handoff(self.store, self.qa_job, self.corrective)
        self.assertFalse(replayed)
        self.assertEqual(self.qa_job["job_id"], linked["corrects_job_id"])

        # The historical QA Job is untouched evidence.
        after = self.store.get_job(self.qa_job["job_id"])
        self.assertEqual(AWAITING_QA, after["status"])
        self.assertEqual(qa_before["revision"], after["revision"])
        self.assertEqual(qa_before["last_result"], after["last_result"])
        self.assertEqual(qa_before["history"], after["history"])
        self.assertEqual(
            [], list(dict(after.get("qa_resolution_actions") or {}).values())
        )

        # Gate ownership handed over: empty global gate, active successor path.
        snapshot = self.store.queue_snapshot()
        self.assertEqual("", snapshot["blocked_by_job_id"])
        record = snapshot["qa_corrective_successors"][self.qa_job["job_id"]]
        self.assertEqual(self.corrective["job_id"], record["corrective_job_id"])
        self.assertEqual("ACTIVE", record["state"])

        # The overlapping corrective dispatches; without the handoff the
        # quarantined scope would hold it.
        claimed = self.store.claim_next("owner-c")
        self.assertEqual(self.corrective["job_id"], claimed["job_id"])
        final_after_claim = self.store.get_job(self.qa_job["job_id"])
        self.assertEqual(qa_before["revision"], final_after_claim["revision"])

    def test_overlapping_scope_stays_held_without_declared_successor(self):
        self.assertIsNone(self.store.claim_next("owner-c"))
        snapshot = self.store.queue_snapshot()
        self.assertNotIn(
            self.qa_job["job_id"], snapshot.get("qa_corrective_successors", {})
        )

    def test_duplicate_handoff_is_idempotent_and_stale_revision_mutates_nothing(self):
        queue_before = self.store.queue_snapshot()
        corrective_before = self.store.get_job(self.corrective["job_id"])
        with self.assertRaisesRegex(QueueError, "QUEUE_REVISION_CONFLICT"):
            self.handoff(
                self.store, self.qa_job, self.corrective,
                queue_revision=queue_before["revision"] - 1,
            )
        with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
            self.handoff(
                self.store, self.qa_job, self.corrective,
                job_revision=corrective_before["revision"] - 1,
            )
        self.assertEqual(
            queue_before["revision"], self.store.queue_snapshot()["revision"]
        )
        self.assertEqual(
            corrective_before["revision"],
            self.store.get_job(self.corrective["job_id"])["revision"],
        )

        # A duplicate replays the original request payload verbatim, exactly
        # like every other revision-safe control action in this architecture.
        original = {
            "queue_revision": queue_before["revision"],
            "job_revision": corrective_before["revision"],
        }
        linked, replayed = self.handoff(
            self.store, self.qa_job, self.corrective,
            request_id="qa-handoff-dup", **original,
        )
        self.assertFalse(replayed)
        replay, replayed_again = self.handoff(
            self.store, self.qa_job, self.corrective,
            request_id="qa-handoff-dup", **original,
        )
        self.assertTrue(replayed_again)
        self.assertEqual(linked["job_id"], replay["job_id"])
        # A genuinely different payload under the same request id conflicts.
        with self.assertRaisesRegex(QueueError, "IDEMPOTENCY_CONFLICT"):
            self.handoff(
                self.store, self.qa_job, self.corrective,
                request_id="qa-handoff-dup",
                queue_revision=self.store.queue_snapshot()["revision"],
            )
        self.assertEqual(1, len(self.store.queue_snapshot()["qa_corrective_successors"]))

class InvalidCorrectiveLineageTest(QaGateHandoffBase):
    """Section 19: no explicit lineage evidence, no gate handoff."""

    def setUp(self):
        super().setUp()
        self.store = self.make_store()
        jobs, _ = self.store.enqueue_batch(
            "qa-invalid",
            [
                request("J1", ["app/J1.txt"]),
                request("J1-UNRELATED", ["app/unrelated.txt"]),
            ],
            execution_context=context(),
        )
        self.qa_job = self.land_qa(self.store, "J1", ["app/J1.txt"])
        # A shared client name alone is not logical-identity evidence.
        edited = self.store._read_job(jobs[1]["job_id"])
        edited["client_job_id"] = "J1"
        self.store._write_job(edited)
        self.same_name = self.store.get_job(jobs[1]["job_id"])

    def test_same_name_without_lineage_evidence_is_rejected(self):
        candidate = self.same_name
        queue_before = self.store.queue_snapshot()
        with self.assertRaisesRegex(QueueError, "QA_CORRECTIVE_LINEAGE_EVIDENCE_MISSING"):
            self.handoff(self.store, self.qa_job, candidate, request_id="qa-handoff-bad")
        self.assertEqual(queue_before["revision"], self.store.queue_snapshot()["revision"])
        self.assertEqual(
            candidate["revision"], self.store.get_job(candidate["job_id"])["revision"]
        )
        snapshot = self.store.queue_snapshot()
        self.assertNotIn(
            self.qa_job["job_id"], snapshot.get("qa_corrective_successors", {})
        )
        self.assertEqual(QUEUED, self.store.get_job(candidate["job_id"])["status"])
        self.assertEqual(
            AWAITING_QA, self.store.get_job(self.qa_job["job_id"])["status"]
        )

    def test_overlapping_scope_without_declared_successor_stays_held(self):
        jobs, _ = self.store.enqueue_batch(
            "qa-overlap", [request("J1-C1", ["app/J1.txt"])],
            execution_context=context(),
        )
        overlapping = self.store.get_job(jobs[0]["job_id"])
        conflict = self.store._quarantined_scope_conflict(
            self.store._read_queue(), overlapping
        )
        self.assertEqual(self.qa_job["job_id"], conflict)
        # The later unrelated Job dispatches while the overlapping one waits.
        claimed = self.store.claim_next("owner-c")
        self.assertEqual(self.same_name["job_id"], claimed["job_id"])

    def test_handoff_requires_confirmation_and_queued_corrective(self):
        with self.assertRaisesRegex(QueueError, "QA_CORRECTIVE_CONFIRMATION_REQUIRED"):
            self.handoff(
                self.store, self.qa_job, self.same_name,
                request_id="qa-handoff-noconf", confirmation="",
            )
        claimed = self.store.claim_next("owner-c3")
        self.assertEqual(self.same_name["job_id"], claimed["job_id"])
        self.store.mark_task_started(claimed["job_id"], "owner-c3", "TASK-J3")
        failed = self.store.record_result(
            claimed["job_id"], "owner-c3",
            {"status": "FAILED", "success": False, "stage": "DONE",
             "failure_type": "review", "fix_scope": "NON_RETRYABLE",
             "worktree_disposition": "CLEAN_ROLLBACK", "failure_code": "REVIEW_FAIL"},
        )
        self.assertEqual(FAILED_FINAL, failed["status"])
        with self.assertRaisesRegex(QueueError, "QA_CORRECTIVE_NOT_QUEUED"):
            self.handoff(self.store, self.qa_job, failed, request_id="qa-handoff-run")


class IndependentContinuationTest(QaGateHandoffBase):
    """H03: a Job-scoped QA hold never blocks unrelated runnable Jobs."""

    def test_independent_job_continues_under_autonomous(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-independent",
            [request("J1", ["app/J1.txt"]), request("J2", ["app/J2.txt"])],
            execution_context=context(),
        )
        store.set_execution_mode(
            EXECUTION_MODE_AUTONOMOUS, "mode-auto-1",
            confirmation=AUTONOMOUS_CONFIRMATION,
        )
        qa_job = self.land_qa(store, "J1", ["app/J1.txt"])
        restarted = self.make_store()
        restarted.reconcile_interrupted()
        claimed = restarted.claim_next("owner-next")
        self.assertEqual(jobs[1]["job_id"], claimed["job_id"])
        self.assertEqual(
            AWAITING_QA, restarted.get_job(qa_job["job_id"])["status"]
        )

    def test_real_dependency_holds_while_unrelated_job_continues(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-dependency",
            [
                request("J1", ["app/J1.txt"]),
                request("J2", ["app/J2.txt"], depends_on=["J1"]),
                request("J3", ["app/J3.txt"]),
            ],
            execution_context=context(),
        )
        store.set_execution_mode(
            EXECUTION_MODE_AUTONOMOUS, "mode-auto-2",
            confirmation=AUTONOMOUS_CONFIRMATION,
        )
        qa_job = self.land_qa(store, "J1", ["app/J1.txt"])
        self.make_store().reconcile_interrupted()
        claimed = store.claim_next("owner-next")
        self.assertEqual(jobs[2]["job_id"], claimed["job_id"])
        dependent = store.get_job(jobs[1]["job_id"])
        self.assertEqual(QUEUED, dependent["status"])
        self.assertEqual([], dependent.get("speculative_candidate_ids") or [])

    def test_attended_mode_keeps_operator_stop_semantics(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-attended",
            [request("J1", ["app/J1.txt"]), request("J2", ["app/J2.txt"])],
            execution_context=context(),
        )
        self.land_qa(store, "J1", ["app/J1.txt"])
        restarted = self.make_store()
        restarted.reconcile_interrupted()
        # Independent work still dispatches, unchanged from the pre-hotfix
        # runtime semantics...
        claimed = restarted.claim_next("owner-next")
        self.assertEqual(jobs[1]["job_id"], claimed["job_id"])
        restarted.mark_task_started(claimed["job_id"], "owner-next", "TASK-J2")
        finished = restarted.record_result(
            claimed["job_id"], "owner-next", success("TASK-J2")
        )
        self.assertEqual(SUCCEEDED, finished["status"])
        # ...but DAILY never becomes an automatic continuation mode: success
        # still requires the operator acknowledgement.
        snapshot = restarted.queue_snapshot()
        self.assertEqual(jobs[1]["job_id"], snapshot["blocked_by_job_id"])
        self.assertEqual(SUCCESS_ACK_REQUIRED, snapshot["gate_reason"])
        self.assertTrue(snapshot["paused"])

    def test_terminal_corrective_returns_quarantine_hold_fail_closed(self):
        store = self.make_store()
        jobs, _ = store.enqueue_batch(
            "qa-terminal-successor",
            [request("J1", ["app/J1.txt"]), request("J1-C1", ["app/J1.txt"])],
            execution_context=context(),
        )
        qa_job = self.land_qa(store, "J1", ["app/J1.txt"])
        corrective = store.get_job(jobs[1]["job_id"])
        linked, _ = self.handoff(store, qa_job, corrective, request_id="qa-handoff-t")
        claimed = store.claim_next("owner-c")
        self.assertEqual(corrective["job_id"], claimed["job_id"])
        store.mark_task_started(claimed["job_id"], "owner-c", "TASK-J1C1")
        failed = store.record_result(
            claimed["job_id"], "owner-c",
            {"status": "FAILED", "success": False, "stage": "DONE",
             "failure_type": "review", "fix_scope": "NON_RETRYABLE",
             "worktree_disposition": "CLEAN_ROLLBACK",
             "failure_code": "REVIEW_FAIL"},
        )
        self.assertEqual(FAILED_FINAL, failed["status"])
        snapshot = store.queue_snapshot()
        self.assertEqual(
            "FAILED",
            snapshot["qa_corrective_successors"][qa_job["job_id"]]["state"],
        )
        # The successor is terminal, so the historical QA scope is held again
        # for every overlapping Job until a fresh corrective is declared.
        self.assertIsNone(store.claim_next("owner-after"))
        self.assertEqual(
            AWAITING_QA, store.get_job(qa_job["job_id"])["status"]
        )


if __name__ == "__main__":
    unittest.main()
