from __future__ import annotations

import hashlib
import json
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from job_contract import (
    DROID_GLM_FAIL_CLOSED_MODEL,
    DROID_GLM_FAIL_CLOSED_POLICY,
)
from job_queue import AWAITING_QA, JobStore, QueueError
from manager import Manager
from task_state import TaskState
from test_job_control import request, valid_enqueue
from worker import DroidWorker


class GlmRoutingHotfixTests(unittest.TestCase):
    @staticmethod
    def _integrity(queue):
        value = {
            "generation": int(queue["generation"]),
            "active_baseline_id": queue["active_baseline_id"],
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [],
            "predecessor_delta_hashes": {},
            "source": "CANONICAL_COMPUTED",
            "computed_at": "2026-08-30T00:00:00+09:00",
        }
        value["snapshot_sha256"] = hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return value

    def _awaiting_qa_store(self):
        temp = tempfile.TemporaryDirectory()
        store = JobStore(Path(temp.name) / ".control")
        jobs, _ = valid_enqueue(store, "glm-retry", [request()])
        job_id = jobs[0]["job_id"]
        claim = store.claim_next("controller-test")
        store.mark_task_started(job_id, "controller-test", "TASK-20260830-170000")
        store.record_result(job_id, "controller-test", {
            "task_id": "TASK-20260830-170000",
            "status": "AWAITING_QA",
            "stage": "AWAITING_QA",
            "success": False,
            "verification_status": "PARTIAL",
            "failure_stage": "WORKER",
            "failure_code": "WORKER_PATCH_CONTEXT_STALE",
            "failure_type": "infrastructure",
            "fix_scope": "RETRYABLE",
            "worktree_disposition": "UNKNOWN",
            "changed_files": [],
        })
        queue = store._read_queue()
        queue["active_baseline_id"] = "BASELINE-TEST"
        queue["unexpected_runtime_dirty_files"] = []
        queue["external_frozen_integrity"] = True
        queue["baseline_declaration_integrity"] = True
        store._write_queue(queue)
        return temp, store, job_id

    def test_same_job_retry_preserves_request_and_records_exact_route(self):
        temp, store, job_id = self._awaiting_qa_store()
        self.addCleanup(temp.cleanup)
        before = store.get_job(job_id)
        after, replayed = store.retry_awaiting_qa_technical(
            job_id,
            expected_revision=before["revision"],
            stability_evidence_sha256="a" * 64,
            recovery_context="USER_APPROVED_BUSINESS_CONTRACT: NULL maps to N",
            request_id="retry-u03-glm-v1",
            integrity_snapshot_provider=self._integrity,
        )
        self.assertFalse(replayed)
        self.assertEqual(before["request"], after["request"])
        self.assertEqual("QUEUED", after["status"])
        self.assertEqual(DROID_GLM_FAIL_CLOSED_POLICY, after["technical_retry_route"]["policy"])
        self.assertEqual("droid", after["technical_retry_route"]["worker"])
        self.assertEqual(DROID_GLM_FAIL_CLOSED_MODEL, after["technical_retry_route"]["model"])
        self.assertEqual(before["request"]["requirement"], after["current_requirement"])
        self.assertEqual(
            "USER_APPROVED_BUSINESS_CONTRACT: NULL maps to N",
            after["technical_retry_context"],
        )
        queue = store.queue_snapshot()
        self.assertTrue(queue["paused"])
        self.assertEqual("AWAITING_QA_TECHNICAL_RETRY_READY", queue["operator_pause_reason"])

    def test_retry_is_revision_safe_and_bounded(self):
        temp, store, job_id = self._awaiting_qa_store()
        self.addCleanup(temp.cleanup)
        revision = store.get_job(job_id)["revision"]
        with self.assertRaisesRegex(QueueError, "STALE_JOB_REVISION"):
            store.retry_awaiting_qa_technical(
                job_id,
                expected_revision=revision - 1,
                stability_evidence_sha256="b" * 64,
                recovery_context="approved technical recovery",
                request_id="stale",
                integrity_snapshot_provider=self._integrity,
            )

    def test_fail_closed_policy_accepts_only_droid_glm_and_has_no_fallback(self):
        allowed = TaskState(
            task_id="TASK-GLM",
            requirement="수정",
            worker="droid",
            droid_model=DROID_GLM_FAIL_CLOSED_MODEL,
            worker_routing_policy=DROID_GLM_FAIL_CLOSED_POLICY,
        )
        self.assertIsNone(Manager._worker_routing_failure(allowed))
        manager = object.__new__(Manager)
        manager.worker_type = "codex"
        manager.working_dir = "D:/inha_dev"
        self.assertEqual(("", "", "fail-closed Droid GLM policy forbids worker/model fallback"), manager._fallback_route(allowed))

        for worker, model in (("codex", "gpt-5.6-sol"), ("droid", "custom:GLM-5.2-[Z.AI-Coding]-0")):
            task = TaskState(
                task_id="TASK-BLOCK",
                requirement="수정",
                worker=worker,
                droid_model=model if worker == "droid" else "",
                codex_model=model if worker == "codex" else None,
                worker_routing_policy=DROID_GLM_FAIL_CLOSED_POLICY,
            )
            result = Manager._worker_routing_failure(task)
            self.assertIsNotNone(result)
            self.assertEqual("DROID_MODEL_ROUTING_POLICY_VIOLATION", task.failure_code)
            self.assertEqual("NO_DELTA", task.worktree_disposition)

    def test_droid_subprocess_argv_contains_exact_model(self):
        with tempfile.TemporaryDirectory() as td:
            completed = SimpleNamespace(returncode=0, stdout="ok", stderr="")
            worker = DroidWorker(model=DROID_GLM_FAIL_CLOSED_MODEL, timeout_config=None)
            with patch("worker.build_prompt", return_value="prompt"), patch(
                "worker.trusted_executable", return_value="D:/trusted/droid.exe"
            ), patch("worker.subprocess.run", return_value=completed) as run_mock:
                result = worker.execute("probe", td)
            cmd = run_mock.call_args.args[0]
            self.assertTrue(result.success)
            self.assertEqual(DROID_GLM_FAIL_CLOSED_MODEL, cmd[cmd.index("-m") + 1])
            self.assertNotIn("claude-opus-5", cmd)


if __name__ == "__main__":
    unittest.main()
