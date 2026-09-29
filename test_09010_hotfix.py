"""Focused 0.9.0.10 hotfix regressions: official Tester verdict projection.

H1: when the official Tester ran and passed, the strict batch contract must
read that verdict, not the approved skip-by-default build-embedded test phase.
Includes the revision-safe STRICT_RESULT_REPROJECTION recovery for one fully
verified execution that was misrecorded before the fix.
"""

from __future__ import annotations

import harness_temp as tempfile
import unittest
from pathlib import Path

from harness_service import HarnessService
from job_contract import EXECUTION_CONTEXT_KEYS, JobRequest
from job_queue import (
    AWAITING_QA,
    BATCH_FINAL_COMMIT,
    QUEUED,
    SUCCEEDED,
    JobStore,
    QueueError,
)
from manager import ManagerResult
from task_state import TaskState


def _context() -> dict[str, object]:
    result: dict[str, object] = {
        "profile_id": "test",
        "profile_schema_version": 1,
    }
    for key in EXECUTION_CONTEXT_KEYS - {"profile_id", "profile_schema_version"}:
        result[key] = "a" * 64
    return result


def _request(client_id: str, resource: str = "app/same.txt") -> JobRequest:
    return JobRequest(
        client_job_id=client_id,
        requirement="modify the declared test file",
        worker="codex",
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        target_modules=("app",),
        target_resources=(resource,),
        commit_summary="batch final test",
        max_outer_attempts=2,
    )


def _active_fields() -> dict[str, object]:
    return {
        "commit_policy": BATCH_FINAL_COMMIT,
        "cumulative_worktree": True,
        "auto_continue_without_commit": True,
        "batch_owned_delta_supported": True,
        "pre_job_snapshot_supported": True,
        "scoped_rollback_supported": True,
        "final_commit_required": True,
        "active_baseline_id": "BASELINE-G1-0123456789ABCDEF",
        "task_owned_baseline_files": ["app/same.txt"],
        "external_frozen_baseline_files": ["app/frozen.txt"],
        "unexpected_runtime_dirty_files": [],
    }


def _verified_task(**overrides) -> TaskState:
    task = TaskState(task_id="TASK-09010", requirement="verified work")
    task.status = "SUCCESS"
    task.stage = "DONE"
    task.verification_status = "VERIFIED"
    task.build = {"status": "PASS", "success": True}
    task.test = {"status": "SKIPPED", "output": "approved pom default"}
    task.test_required = True
    task.test_status = "PASS"
    task.review_status = "REVIEW_PASS"
    task.commit_eligible = True
    task.external_frozen_integrity = True
    task.baseline_declaration_integrity = True
    task.unexpected_external_dirty_files = []
    task.changed_files = ["app/same.txt"]
    for key, value in overrides.items():
        setattr(task, key, value)
    return task


class TesterVerdictProjectionTest(unittest.TestCase):
    def test_official_tester_verdict_wins_over_build_phase_skip(self):
        task = _verified_task()
        outcome = HarnessService._safe_outcome(
            task, ManagerResult(success=True), Path("task.json"), None
        )
        self.assertEqual("PASS", outcome["test_status"])
        self.assertEqual("PASS", outcome["tester_status"])
        self.assertTrue(outcome["success"])

    def test_not_required_and_failure_projections(self):
        not_required = _verified_task(test_required=False, test_status="SKIPPED")
        outcome = HarnessService._safe_outcome(
            not_required, ManagerResult(success=True), Path("task.json"), None
        )
        self.assertEqual("NOT_REQUIRED", outcome["test_status"])

        failed = _verified_task(test_status="FAIL")
        outcome = HarnessService._safe_outcome(
            failed, ManagerResult(success=False), Path("task.json"), None
        )
        self.assertEqual("FAIL", outcome["test_status"])
        self.assertFalse(outcome["success"])

    def test_legacy_missing_verdict_falls_back_to_build_phase(self):
        legacy = _verified_task(test_status="")
        outcome = HarnessService._safe_outcome(
            legacy, ManagerResult(success=True), Path("task.json"), None
        )
        self.assertEqual("SKIPPED", outcome["test_status"])


class StrictResultReprojectionTest(unittest.TestCase):
    def _landing_result(self, task_id: str) -> dict[str, object]:
        # The pre-0.9.0.10 misprojection: every gate PASS, tester PASS, but the
        # result's test_status carried the build-embedded SKIPPED phase.
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "stage": "DONE",
            "success": True,
            "verification_status": "VERIFIED",
            "build_status": "PASS",
            "review_status": "REVIEW_PASS",
            "test_status": "SKIPPED",
            "tester_status": "PASS",
            "changed_files": ["app/same.txt"],
            "worktree_disposition": "PRESERVED",
        }

    def _reprojection_result(self, task_id: str) -> dict[str, object]:
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "stage": "DONE",
            "success": True,
            "verification_status": "VERIFIED",
            "build_status": "PASS",
            "review_status": "REVIEW_PASS",
            "test_status": "PASS",
            "tester_status": "PASS",
            "changed_files": ["app/same.txt"],
            "worktree_disposition": "PRESERVED",
            "recovery_kind": "STRICT_RESULT_REPROJECTION",
            "worker_invoked": False,
            "worker_result_reused": True,
        }

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / ".control"
        self.root.mkdir()
        self.store = JobStore(self.root)
        self.store.activate_cumulative_policy(1, _active_fields())
        jobs, _ = self.store.enqueue_batch(
            "reprojection-09010", [_request("J1")], execution_context=_context()
        )
        self.job_id = jobs[0]["job_id"]
        self.task_id = "TASK-20260903-160000"
        claimed = self.store.claim_next("owner")
        self.assertEqual(self.job_id, claimed["job_id"])

    def test_misprojected_landing_and_revision_safe_reprojection(self):
        landed = self.store.record_result(
            self.job_id, "owner", self._landing_result(self.task_id)
        )
        self.assertEqual(AWAITING_QA, landed["status"])
        self.assertEqual(
            "CUMULATIVE_VERIFICATION_CONTRACT_INVALID",
            (landed.get("last_result") or {}).get("queue_decision_code"),
        )
        self.assertEqual(self.job_id, self.store.queue_snapshot()["blocked_by_job_id"])

        # A duplicate replays the original request payload verbatim.
        reprojection_revision = self.store.get_job(self.job_id)["revision"]
        updated, replayed = self.store.record_review_only_result(
            self.job_id,
            expected_revision=reprojection_revision,
            request_id="reproject-1",
            result=self._reprojection_result(self.task_id),
        )
        self.assertFalse(replayed)
        self.assertEqual(SUCCEEDED, updated["status"])
        result = updated.get("last_result") or {}
        self.assertEqual("PASS", result["test_status"])
        self.assertEqual(
            "DEFERRED_TO_BATCH_FINAL", result.get("job_commit_status")
        )
        self.assertIn("app/same.txt", self.store.queue_snapshot()["batch_delta_files"])

        replay, replayed_again = self.store.record_review_only_result(
            self.job_id,
            expected_revision=reprojection_revision,
            request_id="reproject-1",
            result=self._reprojection_result(self.task_id),
        )
        self.assertTrue(replayed_again)
        self.assertEqual(SUCCEEDED, replay["status"])

    def test_stale_revision_and_ineligible_prior_are_rejected(self):
        self.store.record_result(
            self.job_id, "owner", self._landing_result(self.task_id)
        )
        revision = self.store.get_job(self.job_id)["revision"]
        with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
            self.store.record_review_only_result(
                self.job_id,
                expected_revision=revision - 1,
                request_id="reproject-stale",
                result=self._reprojection_result(self.task_id),
            )
        with self.assertRaisesRegex(QueueError, "REVIEW_ONLY_RETRY_NOT_ELIGIBLE"):
            self.store.record_review_only_result(
                self.job_id,
                expected_revision=revision,
                request_id="reproject-wrongkind",
                result={
                    **self._reprojection_result(self.task_id),
                    "recovery_kind": "SOMETHING_ELSE",
                },
            )
        self.assertEqual(
            AWAITING_QA, self.store.get_job(self.job_id)["status"]
        )

    def test_corrected_projection_lands_succeeded_directly(self):
        landed = self.store.record_result(
            self.job_id,
            "owner",
            {
                **self._landing_result(self.task_id),
                "test_status": "PASS",
            },
        )
        self.assertEqual(SUCCEEDED, landed["status"])
        self.assertEqual(
            "", (landed.get("last_result") or {}).get("queue_decision_code", "")
        )


if __name__ == "__main__":
    unittest.main()
