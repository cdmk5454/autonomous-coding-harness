from __future__ import annotations

import hashlib
import json
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from git_collector import GitCollector
from job_contract import DROID_GLM_FAIL_CLOSED_MODEL, JobRequest
from job_queue import AWAITING_QA, JobStore, QueueError, _canonical_sha256
from manager import Manager
from qa_quarantine import CandidateError, QACandidateStore
from queue_supervisor import QueueSupervisor
from reviewer import plan_diff_batches
from task_state import TaskState
from worker import parse_qa_request
from chatgpt_control import _post_worker_recovery_integrity_valid


def _context() -> dict[str, object]:
    return {
        "profile_id": "test",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }


class CandidateIntegrity09004Tests(unittest.TestCase):
    def _candidate(self, root: Path):
        workspace = root / "workspace"
        snapshots = root / "state" / "snapshots" / "SNAP"
        workspace.mkdir()
        (snapshots / "files").mkdir(parents=True)
        (workspace / "a.txt").write_bytes(b"candidate\n")
        (snapshots / "files" / "a.txt").write_bytes(b"base\n")
        manifest = snapshots / "manifest.json"
        manifest.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT",
            "files": [{
                "path": "a.txt", "state": "PRESENT",
                "sha256": hashlib.sha256(b"base\n").hexdigest(),
                "snapshot": "files/a.txt",
            }],
        }), encoding="utf-8")
        task = SimpleNamespace(
            task_owned_changed_files=["a.txt"], changed_files=["a.txt"],
            pre_job_snapshot_manifest=str(manifest), pre_job_snapshot_id="SNAP",
            git_diff="-base\n+candidate", job_id="JOB-QA", task_id="TASK-QA",
            qa_type="DB_CONTRACT", hold_scope="JOB", machine_verified=False,
            qa_request={"required": True, "qa_type": "DB_CONTRACT", "hold_scope": "JOB",
                        "reason": "choose persistence key", "evidence": "schema evidence"},
            active_baseline_id="BASE", build={"status": "PASS"}, test_required=False,
            test_status="SKIPPED", review_status="REVIEW_ERROR",
            review_coverage={}, verification_status="PARTIAL",
            failure_code="REVIEW_COVERAGE_PLAN_INVALID", failure_origin="TOOL_INFRA",
            failure_stage="REVIEW", failure_reason="malformed hunk body",
        )
        store = QACandidateStore(root / "state" / "candidates", workspace, None)
        return store, store.create(task)

    def test_manifest_patch_and_coverage_are_hash_guarded(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, candidate = self._candidate(root)
            loaded = store.load(candidate["candidate_id"])
            self.assertEqual("REVIEW_COVERAGE_PLAN_INVALID", loaded["evidence"]["failure"]["code"])
            self.assertEqual("choose persistence key", loaded["qa"]["reason"])
            manifest = Path(candidate["manifest_path"])
            original = manifest.read_bytes()
            manifest.write_bytes(original + b" ")
            with self.assertRaisesRegex(CandidateError, "CANDIDATE_MANIFEST_HASH_MISMATCH"):
                store.load(candidate["candidate_id"])

    def test_patch_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            store, candidate = self._candidate(Path(temp))
            Path(candidate["manifest_path"]).with_name("candidate.patch").write_text("tampered")
            with self.assertRaisesRegex(CandidateError, "CANDIDATE_PATCH_HASH_MISMATCH"):
                store.load(candidate["candidate_id"])


class QueueIsolation09004Tests(unittest.TestCase):
    def test_post_worker_recovery_records_reviewer_source_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            request = JobRequest.from_mapping({
                "client_job_id": "J05", "requirement": "db contract", "worker": "droid",
                "model": DROID_GLM_FAIL_CLOSED_MODEL, "reasoning_effort": "medium",
                "target_modules": ["suc"], "target_resources": ["suc/x.xml"],
                "commit_summary": "x", "max_outer_attempts": 2, "depends_on": [],
            }, allowed_modules=["suc"])
            jobs, _ = store.enqueue_batch(
                "post-worker-recovery", [request], execution_context=_context()
            )
            running = store.claim_next("owner")
            store.mark_task_started(running["job_id"], "owner", "TASK-J05")
            awaiting = store.record_result(running["job_id"], "owner", {
                "task_id": "TASK-J05", "status": AWAITING_QA, "stage": AWAITING_QA,
                "success": False, "failure_stage": "CONTROL",
                "failure_code": "CONTROL_EXECUTION_ERROR",
                "failure_origin": "HARNESS_CAUSED",
            })
            result = {
                "task_id": "TASK-J05", "status": AWAITING_QA, "stage": "REVIEW",
                "success": False, "failure_stage": "REVIEW",
                "failure_code": "REVIEW_FAILED",
                "failure_origin": "REVIEWER_CODE_FINDING",
                "review_status": "REVIEW_FAIL", "verification_status": "FAILED",
                "recovery_kind": "POST_WORKER_QA_PARSE_RECOVERY",
                "worker_invoked": False, "worker_result_reused": True,
            }
            updated, replayed = store.record_review_only_result(
                awaiting["job_id"], expected_revision=awaiting["revision"],
                request_id="post-worker-review-1", result=result,
            )
            self.assertFalse(replayed)
            self.assertEqual(AWAITING_QA, updated["status"])
            self.assertEqual("REVIEW_FAILED", updated["last_result"]["failure_code"])

    def test_candidate_recovery_is_non_global_and_adds_no_attempt(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            request = JobRequest.from_mapping({
                "client_job_id": "J05", "requirement": "db contract", "worker": "droid",
                "model": DROID_GLM_FAIL_CLOSED_MODEL, "reasoning_effort": "medium", "target_modules": ["suc"],
                "target_resources": ["suc/src/main/java/suc/sp"], "commit_summary": "x",
                "max_outer_attempts": 2, "qa_type": "DB_CONTRACT", "hold_scope": "JOB",
                "machine_verified": False, "depends_on": [],
            }, allowed_modules=["suc"])
            jobs, _ = store.enqueue_batch("candidate-isolation", [request], execution_context=_context())
            running = store.claim_next("owner")
            store.mark_task_started(running["job_id"], "owner", "TASK-J05")
            awaiting = store.record_result(running["job_id"], "owner", {
                "task_id": "TASK-J05", "status": AWAITING_QA, "stage": AWAITING_QA,
                "success": False, "failure_stage": "REVIEW",
                "failure_code": "REVIEW_COVERAGE_PLAN_INVALID", "failure_origin": "TOOL_INFRA",
                "worktree_disposition": "PRESERVED", "changed_files": ["suc/x.xml"],
                "qa_request": {"required": True, "qa_type": "DB_CONTRACT", "hold_scope": "JOB"},
            })
            attempts = list(awaiting["attempts"])
            result = dict(awaiting["last_result"])
            result.update({
                "recovery_kind": "QA_CANDIDATE_ISOLATION", "worker_invoked": False,
                "candidate_id": "CAND-" + "A" * 24, "candidate_manifest": "manifest.json",
                "qa_type": "DB_CONTRACT", "hold_scope": "JOB", "machine_verified": False,
                "failure_origin": "USER_QA", "failure_code": "USER_QA_REQUIRED",
                "worktree_disposition": "CLEAN_ROLLBACK",
            })
            updated, replayed = store.record_review_only_result(
                awaiting["job_id"], expected_revision=awaiting["revision"],
                request_id="isolate-1", result=result,
            )
            self.assertFalse(replayed)
            self.assertEqual(AWAITING_QA, updated["status"])
            self.assertEqual(attempts, updated["attempts"])
            self.assertEqual("", store.queue_snapshot()["blocked_by_job_id"])

            resolved, replayed = store.resolve_qa_candidate_for_retry(
                updated["job_id"], expected_revision=updated["revision"],
                resolution_text=(
                    "Explicit user DB contract: persist V_ENTRP_INFO.IDSTR_CD; "
                    "manager fields are current View data; show every agreement "
                    "ordered by AGREM_DT DESC; retain SSUC070 legacy fallback."
                ),
                candidate_id="CAND-" + "A" * 24,
                request_id="resolve-candidate-1",
            )
            self.assertFalse(replayed)
            self.assertEqual("QUEUED", resolved["status"])
            self.assertEqual("CAND-" + "A" * 24, resolved["qa_resolution_candidate_id"])
            self.assertIn("Authoritative QA resolution", resolved["current_requirement"])
            self.assertEqual(attempts, resolved["attempts"])

    def test_resolved_candidate_technical_recovery_is_allowed_in_autonomous_mode(self):
        queue = {
            "generation": 5, "execution_mode": "AUTONOMOUS", "paused": True,
            "running_job_id": "", "blocked_by_job_id": "JOB-J05",
            "gate_reason": AWAITING_QA, "active_baseline_id": "BASE-5",
        }
        job = {
            "job_id": "JOB-J05", "status": AWAITING_QA, "revision": 17,
            "qa_resolution_candidate_id": "CAND-" + "A" * 24,
            "last_result": {"failure_code": "CHECKPOINT_CAPTURE_FAILED"},
        }
        integrity = {
            "source": "CANONICAL_COMPUTED", "generation": 5,
            "active_baseline_id": "BASE-5", "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [], "predecessor_delta_hashes": {},
        }
        integrity["snapshot_sha256"] = _canonical_sha256(integrity)
        result = JobStore.__new__(JobStore)._validate_technical_retry_locked(
            queue, job, expected_revision=17,
            integrity_snapshot_provider=lambda _queue: integrity,
        )
        self.assertEqual(integrity["snapshot_sha256"], result["snapshot_sha256"])
        job["qa_resolution_candidate_id"] = ""
        with self.assertRaisesRegex(QueueError, "TECHNICAL_RETRY_CONTROL_STATE_INVALID"):
            JobStore.__new__(JobStore)._validate_technical_retry_locked(
                queue, job, expected_revision=17,
                integrity_snapshot_provider=lambda _queue: integrity,
            )


class CandidateExecution09004Tests(unittest.TestCase):
    def test_candidate_applier_runs_after_snapshot_verification(self):
        events = []
        manager = Manager.__new__(Manager)
        manager.worker_type = "droid"
        manager.model = DROID_GLM_FAIL_CLOSED_MODEL
        manager.codex_model = None
        manager.profile = None
        manager._task_baselines = {}
        manager._initial_baseline = object()
        manager.initial_delta_applier = lambda: (
            events.append("apply") or (_ for _ in ()).throw(RuntimeError("stop"))
        )
        manager.configuration_guard = None
        manager.restore_checkpoint_manifest = ""
        manager._configuration_failure = lambda _task: None
        manager._worker_routing_failure = lambda _task: None
        manager.collector = SimpleNamespace(
            verify_baseline_current=lambda _baseline: (events.append("verify") or (True, ""))
        )
        task = SimpleNamespace(
            task_id="TASK-J05", requirement="modify", task_mode="MODIFICATION",
            original_worker="droid", original_model=DROID_GLM_FAIL_CLOSED_MODEL,
            worker="droid", codex_model="", droid_model=DROID_GLM_FAIL_CLOSED_MODEL,
            target_module=["suc"], inherited_batch_delta_files=[],
            preserved_predecessor_delta_files=[], failure_stage="", failure_code="",
            failure_reason="", failure_origin="", failure_type="", fix_scope="",
            verification_status="", status="", stage="", worktree_disposition="",
            attempt_reservation={},
        )
        result = manager._run_pipeline(task)
        self.assertFalse(result.success)
        self.assertEqual(["verify", "apply"], events)
        self.assertEqual("QA_CANDIDATE_APPLY_FAILED", task.failure_code)

    def test_resolved_strict_result_is_not_requarantined_without_new_qa(self):
        supervisor = QueueSupervisor(SimpleNamespace(), SimpleNamespace(task_root="missing"))
        result = {
            "success": True, "status": "SUCCESS", "stage": "DONE",
            "build_status": "PASS", "test_status": "NOT_REQUIRED",
            "review_status": "REVIEW_PASS", "verification_status": "VERIFIED",
            "failure_code": "", "qa_request": {},
        }
        returned = supervisor._quarantine_machine_verified_result(
            {"qa_resolution_candidate_id": "CAND-" + "A" * 24, "qa_type": "DB_CONTRACT"},
            result,
            SimpleNamespace(quarantine_and_rollback=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError())),
        )
        self.assertIs(result, returned)


class DiffTransport09004Tests(unittest.TestCase):
    def test_final_blank_context_line_survives_collection_and_review_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
            target = root / "sample.txt"
            target.write_text("one\ntwo\nthree\nfour\n\n", encoding="utf-8")
            subprocess.run(["git", "add", "sample.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=root, check=True)
            task = TaskState(task_id="TASK-DIFF", requirement="x", working_dir=str(root))
            baseline, error = GitCollector(str(root)).capture_baseline(task)
            self.assertEqual("", error)
            target.write_text("one\nchanged\nthree\nfour\n\n", encoding="utf-8")
            result = GitCollector(str(root)).collect(task, baseline)
            self.assertTrue(result.success)
            self.assertTrue(result.diff.endswith(" "))
            self.assertEqual("", plan_diff_batches(result.diff).failure_code)

    def test_explicit_no_qa_sentinel_is_not_parsed_as_json(self):
        self.assertEqual({}, parse_qa_request("HARNESS_QA_REQUEST_JSON: 없음"))
        self.assertEqual({}, parse_qa_request("HARNESS_QA_REQUEST_JSON: NONE"))
        self.assertEqual(
            {},
            parse_qa_request(
                "HARNESS_QA_REQUEST_JSON: 없음 — authoritative decision resolved"
            ),
        )

    def test_post_worker_integrity_allows_batch_owned_changed_subset(self):
        integrity = {
            "unexpected_runtime_dirty_files": ["new.vue"],
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
        }
        self.assertTrue(
            _post_worker_recovery_integrity_valid(
                ["batch-owned.xml", "new.vue"], integrity
            )
        )
        integrity["unexpected_runtime_dirty_files"] = ["unrelated.txt"]
        self.assertFalse(
            _post_worker_recovery_integrity_valid(
                ["batch-owned.xml", "new.vue"], integrity
            )
        )


if __name__ == "__main__":
    unittest.main()
