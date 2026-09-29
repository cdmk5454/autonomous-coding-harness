from __future__ import annotations

import harness_temp as tempfile
import unittest
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from chatgpt_control import ChatGPTControlService
from job_queue import AWAITING_QA, JobStore, QueueError
from test_job_control import request, valid_enqueue


class CanonicalIntegritySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = JobStore(Path(self.temp.name) / ".control")
        jobs, _ = valid_enqueue(self.store, "integrity", [request()])
        self.job_id = jobs[0]["job_id"]
        self.store.claim_next("controller-test")
        self.store.mark_task_started(self.job_id, "controller-test", "TASK-20260830-180000")
        self.store.record_result(
            self.job_id,
            "controller-test",
            {
                "task_id": "TASK-20260830-180000",
                "status": AWAITING_QA,
                "stage": AWAITING_QA,
                "success": False,
                "verification_status": "PARTIAL",
                "failure_stage": "WORKER",
                "failure_code": "WORKER_PATCH_CONTEXT_STALE",
                "failure_type": "infrastructure",
                "worktree_disposition": "UNKNOWN",
                "changed_files": [],
            },
        )
        queue = self.store._read_queue()
        queue["active_baseline_id"] = "BASELINE-TEST"
        queue["batch_delta_files"] = ["suc/predecessor.txt"]
        # Deliberately omit derived integrity fields from durable state.
        queue.pop("external_frozen_integrity", None)
        queue.pop("baseline_declaration_integrity", None)
        self.store._write_queue(queue)
        job = self.store._read_job(self.job_id)
        job["profile_revalidation"] = {
            "predecessor_hashes": {"suc/predecessor.txt": "a" * 64}
        }
        self.store._write_job(job)

    @staticmethod
    def snapshot(**changes):
        value = {
            "generation": 1,
            "active_baseline_id": "BASELINE-TEST",
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [],
            "predecessor_delta_hashes": {"suc/predecessor.txt": "a" * 64},
            "source": "CANONICAL_COMPUTED",
            "computed_at": "2026-08-30T18:00:00+09:00",
        }
        value.update(changes)
        value["snapshot_sha256"] = hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return value

    def preview(self, **changes):
        return self.store.preview_awaiting_qa_technical(
            self.job_id,
            expected_revision=self.store.get_job(self.job_id)["revision"],
            integrity_snapshot_provider=lambda queue: self.snapshot(**changes),
        )

    def assert_integrity_rejected(self, **changes):
        with self.assertRaisesRegex(QueueError, "TECHNICAL_RETRY_WORKTREE_INTEGRITY_INVALID"):
            self.preview(**changes)

    def test_missing_persisted_fields_recompute_and_pass(self):
        result = self.preview()
        self.assertTrue(result["eligible"])
        self.assertEqual("CANONICAL_COMPUTED", result["integrity_snapshot"]["source"])

    def test_external_frozen_mismatch_fails(self):
        self.assert_integrity_rejected(external_frozen_integrity=False)

    def test_baseline_declaration_mismatch_fails(self):
        self.assert_integrity_rejected(baseline_declaration_integrity=False)

    def test_unexpected_dirty_fails(self):
        self.assert_integrity_rejected(unexpected_runtime_dirty_files=["suc/x.txt"])

    def test_persisted_true_cannot_override_canonical_false(self):
        queue = self.store._read_queue()
        queue["external_frozen_integrity"] = True
        queue["baseline_declaration_integrity"] = True
        self.store._write_queue(queue)
        self.assert_integrity_rejected(external_frozen_integrity=False)

    def test_persisted_false_cannot_override_canonical_true(self):
        queue = self.store._read_queue()
        queue["external_frozen_integrity"] = False
        queue["baseline_declaration_integrity"] = False
        self.store._write_queue(queue)
        self.assertTrue(self.preview()["eligible"])

    def test_active_baseline_mismatch_fails(self):
        self.assert_integrity_rejected(active_baseline_id="BASELINE-OTHER")

    def test_predecessor_hash_mismatch_fails(self):
        self.assert_integrity_rejected(
            predecessor_delta_hashes={"suc/predecessor.txt": "f" * 64}
        )

    def test_stale_revision_fails(self):
        with self.assertRaisesRegex(QueueError, "STALE_JOB_REVISION"):
            self.store.preview_awaiting_qa_technical(
                self.job_id,
                expected_revision=self.store.get_job(self.job_id)["revision"] - 1,
                integrity_snapshot_provider=lambda queue: self.snapshot(),
            )

    def test_duplicate_request_id_mutates_once(self):
        revision = self.store.get_job(self.job_id)["revision"]
        kwargs = dict(
            expected_revision=revision,
            stability_evidence_sha256="d" * 64,
            recovery_context="USER_APPROVED_SCHEMA_DECISION=A",
            request_id="integrity-idempotent",
            integrity_snapshot_provider=lambda queue: self.snapshot(),
        )
        first, replayed1 = self.store.retry_awaiting_qa_technical(self.job_id, **kwargs)
        second, replayed2 = self.store.retry_awaiting_qa_technical(self.job_id, **kwargs)
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first["revision"], second["revision"])
        events = [item for item in second["history"] if item["event"] == "AWAITING_QA_TECHNICAL_RETRY_QUEUED"]
        self.assertEqual(1, len(events))

    def test_control_runtime_view_and_retry_preview_share_snapshot(self):
        workspace = Path(self.temp.name) / "workspace"
        target = workspace / "suc" / "predecessor.txt"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"predecessor")
        digest = hashlib.sha256(b"predecessor").hexdigest()
        job = self.store._read_job(self.job_id)
        job["profile_revalidation"] = {"predecessor_hashes": {"suc/predecessor.txt": digest}}
        self.store._write_job(job)

        class FakeHarness:
            def cumulative_policy_status(self, **kwargs):
                return {
                    "external_frozen_integrity": True,
                    "baseline_declaration_integrity": True,
                    "unexpected_runtime_dirty_files": [],
                }

            @staticmethod
            def probe_execution_idle():
                return True

        control = object.__new__(ChatGPTControlService)
        control.store = self.store
        control.harness = FakeHarness()
        control.configuration = {"working_dir": str(workspace)}
        control.read_only = False
        control.supervisor = SimpleNamespace(is_running=lambda: False)
        runtime = control._queue_snapshot()["integrity_snapshot"]
        preview = control.preview_awaiting_qa_technical(
            self.job_id, self.store.get_job(self.job_id)["revision"]
        )["integrity_snapshot"]
        for key in (
            "external_frozen_integrity",
            "baseline_declaration_integrity",
            "unexpected_runtime_dirty_files",
            "predecessor_delta_hashes",
        ):
            self.assertEqual(runtime[key], preview[key])


if __name__ == "__main__":
    unittest.main()
