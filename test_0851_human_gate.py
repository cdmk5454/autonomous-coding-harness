from __future__ import annotations

import hashlib
import harness_temp as tempfile
import unittest
from pathlib import Path

from job_contract import EXECUTION_CONTEXT_KEYS, JobRequest
from job_queue import (
    AUTONOMOUS_CONFIRMATION,
    AWAITING_QA,
    BATCH_FINAL_COMMIT,
    EXECUTION_MODE_ATTENDED,
    EXECUTION_MODE_AUTONOMOUS,
    FAILED_FINAL,
    QUEUED,
    SUCCEEDED,
    SUCCESS_ACK_REQUIRED,
    JobStore,
    QueueError,
)
from queue_supervisor import QueueSupervisor


def _context() -> dict[str, object]:
    context: dict[str, object] = {
        "profile_id": "test",
        "profile_schema_version": 1,
    }
    for key in EXECUTION_CONTEXT_KEYS - {"profile_id", "profile_schema_version"}:
        context[key] = "a" * 64
    return context


def _request(client_job_id: str) -> JobRequest:
    return JobRequest(
        client_job_id=client_job_id,
        requirement="exercise the attended human release boundary",
        worker="codex",
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        target_modules=("app",),
        target_resources=("app/shared.txt",),
        commit_summary="human gate test",
        max_outer_attempts=1,
    )


def _policy() -> dict[str, object]:
    return {
        "commit_policy": BATCH_FINAL_COMMIT,
        "cumulative_worktree": True,
        "auto_continue_without_commit": True,
        "batch_owned_delta_supported": True,
        "pre_job_snapshot_supported": True,
        "scoped_rollback_supported": True,
        "final_commit_required": True,
        "active_baseline_id": "BASELINE-G1-0123456789ABCDEF",
        "task_owned_baseline_files": ["app/predecessor.txt"],
        "external_frozen_baseline_files": [],
        "unexpected_runtime_dirty_files": [],
    }


def _success(task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "verification_status": "VERIFIED",
        "build_status": "PASS",
        "test_status": "NOT_REQUIRED",
        "review_status": "REVIEW_PASS",
        "failure_code": "",
        "secondary_failures": [],
        "changed_files": ["app/shared.txt"],
        "worktree_disposition": "PRESERVED",
    }


class _Harness:
    def __init__(self, results: list[dict[str, object]] | None = None):
        self.results = list(results or [])
        self.executed: list[str] = []
        self.lease = False

    def acquire_execution_lease(self) -> bool:
        if self.lease:
            return False
        self.lease = True
        return True

    def release_execution_lease(self) -> None:
        self.lease = False

    def validate_configuration(self) -> dict[str, object]:
        return _context()

    def prepare_job_snapshot(self, job, queue):
        return {
            "snapshot_id": f"SNAP-{job['job_id']}",
            "manifest_path": f"D:/snapshots/{job['job_id']}.json",
            "inherited_batch_delta_files": list(queue.get("batch_delta_files") or []),
            "active_baseline_id": queue.get("active_baseline_id", ""),
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
        }

    def execute(self, job, *, on_task_started):
        self.executed.append(job["job_id"])
        task_id = f"TASK-{len(self.executed)}"
        on_task_started(task_id)
        if self.results:
            result = dict(self.results.pop(0))
            result["task_id"] = task_id
            return result
        return _success(task_id)

    def commit_verified_job(self, job):
        raise AssertionError("BATCH_FINAL_COMMIT must defer per-Job commit")


class HumanGatedBoundaryTest(unittest.TestCase):
    def _store(self, base: str, count: int, *, autonomous: bool = False):
        root = Path(base) / ".control"
        root.mkdir()
        store = JobStore(root)
        store.activate_cumulative_policy(1, _policy())
        jobs, _ = store.enqueue_batch(
            "human-gate-boundary-v1",
            [_request(name) for name in ("U03", "U04", "A07")[:count]],
            execution_context=_context(),
        )
        mode = EXECUTION_MODE_AUTONOMOUS if autonomous else EXECUTION_MODE_ATTENDED
        confirmation = AUTONOMOUS_CONFIRMATION if autonomous else ""
        queue = store.queue_snapshot()
        store.set_execution_mode(
            mode,
            "human-gate-mode-v1",
            confirmation,
            expected_revision=queue["revision"],
            batch_id=jobs[0]["batch_id"],
            reason="test execution boundary",
        )
        return store, jobs

    def _run(self, store: JobStore, harness: _Harness) -> QueueSupervisor:
        supervisor = QueueSupervisor(store, harness)
        self.assertTrue(supervisor.start()["started"])
        self.assertTrue(supervisor.wait(10))
        return supervisor

    def test_attended_two_job_success_stops_without_auto_ack_or_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 2)
            harness = _Harness()
            self._run(store, harness)
            first = store.get_job(jobs[0]["job_id"])
            queue = store.queue_snapshot()
            self.assertEqual([jobs[0]["job_id"]], harness.executed)
            self.assertEqual(SUCCEEDED, first["status"])
            self.assertEqual({}, first["success_acknowledgements"])
            self.assertEqual(QUEUED, store.get_job(jobs[1]["job_id"])["status"])
            self.assertEqual(SUCCESS_ACK_REQUIRED, queue["gate_reason"])
            self.assertEqual(jobs[0]["job_id"], queue["blocked_by_job_id"])
            self.assertEqual("DEFERRED_TO_BATCH_FINAL", first["last_result"]["commit_status"])
            self.assertEqual("PRESERVED", first["last_result"]["job_delta_preservation_status"])

    def test_attended_three_job_one_release_stops_again_before_a07(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 3)
            harness = _Harness()
            self._run(store, harness)
            first = store.get_job(jobs[0]["job_id"])
            acknowledged, replayed = store.acknowledge_success(
                first["job_id"],
                "continue-u03-v1",
                expected_revision=first["revision"],
                batch_id=first["batch_id"],
            )
            self.assertFalse(replayed)
            self.assertEqual(first["job_id"], acknowledged["job_id"])
            self._run(store, harness)
            second = store.get_job(jobs[1]["job_id"])
            queue = store.queue_snapshot()
            self.assertEqual([jobs[0]["job_id"], jobs[1]["job_id"]], harness.executed)
            self.assertEqual(SUCCEEDED, second["status"])
            self.assertEqual({}, second["success_acknowledgements"])
            self.assertEqual(QUEUED, store.get_job(jobs[2]["job_id"])["status"])
            self.assertEqual(jobs[1]["job_id"], queue["blocked_by_job_id"])
            self.assertEqual(SUCCESS_ACK_REQUIRED, queue["gate_reason"])

    def test_duplicate_continue_replays_without_event_or_dispatch_duplication(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 3)
            harness = _Harness()
            self._run(store, harness)
            first = store.get_job(jobs[0]["job_id"])
            args = (first["job_id"], "continue-u03-dedupe-v1")
            store.acknowledge_success(
                *args,
                expected_revision=first["revision"],
                batch_id=first["batch_id"],
            )
            self._run(store, harness)
            replayed_job, replayed = store.acknowledge_success(
                *args,
                expected_revision=first["revision"],
                batch_id=first["batch_id"],
            )
            self.assertTrue(replayed)
            self.assertEqual(1, sum(
                event.get("event") == "SUCCESS_ACKNOWLEDGED"
                for event in replayed_job["history"]
            ))
            self.assertEqual(2, len(harness.executed))
            self.assertEqual(QUEUED, store.get_job(jobs[2]["job_id"])["status"])

    def test_stale_revision_rejects_release_without_dispatch(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 2)
            harness = _Harness()
            self._run(store, harness)
            first = store.get_job(jobs[0]["job_id"])
            with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
                store.acknowledge_success(
                    first["job_id"],
                    "continue-u03-stale-v1",
                    expected_revision=first["revision"] - 1,
                    batch_id=first["batch_id"],
                )
            self.assertEqual([jobs[0]["job_id"]], harness.executed)
            self.assertEqual(QUEUED, store.get_job(jobs[1]["job_id"])["status"])

    def test_failed_qa_and_review_unavailable_cannot_release_success_gate(self):
        cases = (
            ({**_success("TASK"), "status": "FAILED", "success": False,
              "failure_code": "BUILD_FAILED", "failure_type": "build",
              "fix_scope": "NON_RETRYABLE", "worktree_disposition": "NO_DELTA"}, FAILED_FINAL),
            ({**_success("TASK"), "status": "AWAITING_QA", "success": False,
              "failure_code": "USER_QA_REQUIRED"}, AWAITING_QA),
            ({**_success("TASK"), "review_status": "REVIEW_UNAVAILABLE"}, AWAITING_QA),
        )
        for index, (result, expected_status) in enumerate(cases):
            with self.subTest(case=index), tempfile.TemporaryDirectory() as temp:
                store, jobs = self._store(temp, 2)
                harness = _Harness([result])
                self._run(store, harness)
                first = store.get_job(jobs[0]["job_id"])
                self.assertEqual(expected_status, first["status"])
                with self.assertRaisesRegex(QueueError, "JOB_NOT_SUCCEEDED"):
                    store.acknowledge_success(
                        first["job_id"],
                        f"continue-invalid-{index}-v1",
                        expected_revision=first["revision"],
                        batch_id=first["batch_id"],
                    )
                self.assertEqual(QUEUED, store.get_job(jobs[1]["job_id"])["status"])

    def test_autonomous_three_job_regression_auto_continues(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 3, autonomous=True)
            harness = _Harness()
            self._run(store, harness)
            self.assertEqual([job["job_id"] for job in jobs], harness.executed)
            self.assertTrue(all(
                store.get_job(job["job_id"])["status"] == SUCCEEDED for job in jobs
            ))
            self.assertFalse(store.queue_snapshot()["paused"])

    def test_attended_wait_and_predecessor_delta_survive_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            predecessor = Path(temp) / "predecessor.txt"
            predecessor.write_bytes(b"approved-u02-delta\r\n")
            expected_hash = hashlib.sha256(predecessor.read_bytes()).hexdigest()
            store, jobs = self._store(temp, 2)
            queue = store._read_queue()
            queue["batch_delta_files"] = ["app/predecessor.txt"]
            store._write_queue(queue)
            harness = _Harness()
            self._run(store, harness)

            restarted = JobStore(store.root)
            self.assertEqual([], restarted.reconcile_interrupted())
            snapshot = restarted.queue_snapshot()
            first = restarted.get_job(jobs[0]["job_id"])
            self.assertEqual(SUCCEEDED, first["status"])
            self.assertEqual({}, first["success_acknowledgements"])
            self.assertEqual(SUCCESS_ACK_REQUIRED, snapshot["gate_reason"])
            self.assertEqual(["app/predecessor.txt", "app/shared.txt"], snapshot["batch_delta_files"])
            self.assertIsNone(restarted.claim_next("owner-after-restart"))
            self.assertEqual(expected_hash, hashlib.sha256(predecessor.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
