from __future__ import annotations

import hashlib
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from cumulative_policy import CumulativePolicyError, CumulativePolicyManager
from git_collector import GitCollector
from harness_service import HarnessServiceError
from job_contract import EXECUTION_CONTEXT_KEYS, JobRequest
from job_queue import (
    AUTONOMOUS_CONFIRMATION,
    AWAITING_QA,
    BATCH_FINAL_COMMIT,
    EXECUTION_MODE_AUTONOMOUS,
    JobStore,
    PREDECESSOR_SUCCESS_GATE_FAILED,
    QUEUED,
    SKIPPED,
    SUCCEEDED,
)
from project_profile import (
    BaselineFileProfile,
    ControlPolicyProfile,
    ModuleProfile,
    ProjectProfile,
)
from queue_supervisor import QueueSupervisor


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _context() -> dict[str, object]:
    result: dict[str, object] = {
        "profile_id": "test",
        "profile_schema_version": 1,
    }
    for key in EXECUTION_CONTEXT_KEYS - {
        "profile_id",
        "profile_schema_version",
    }:
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


def _success(task_id: str, changed: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "verification_status": "VERIFIED",
        "build_status": "PASS",
        "review_status": "REVIEW_PASS",
        "changed_files": [changed],
        "worktree_disposition": "PRESERVED",
    }


class CumulativeJobStoreTest(unittest.TestCase):
    def test_success_auto_acknowledges_without_commit_and_advances(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"
            root.mkdir()
            store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            jobs, _ = store.enqueue_batch(
                "batch-final-store-1",
                [_request("one"), _request("two")],
                execution_context=_context(),
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS,
                "batch-final-store-mode-1",
                AUTONOMOUS_CONFIRMATION,
            )
            first = store.claim_next("owner")
            self.assertEqual(jobs[0]["job_id"], first["job_id"])
            store.mark_task_started(first["job_id"], "owner", "TASK-ONE")
            completed = store.record_result(
                first["job_id"], "owner", _success("TASK-ONE", "app/same.txt")
            )
            snapshot = store.queue_snapshot()
            self.assertEqual(SUCCEEDED, completed["status"])
            self.assertTrue(completed["success_acknowledgements"])
            self.assertEqual({}, completed["commit_actions"])
            self.assertFalse(snapshot["paused"])
            self.assertEqual("", snapshot["gate_reason"])
            self.assertEqual(["app/same.txt"], snapshot["batch_delta_files"])
            second = store.claim_next("owner")
            self.assertEqual(jobs[1]["job_id"], second["job_id"])

    def test_restart_recovers_policy_and_legacy_policy_still_gates(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"
            root.mkdir()
            store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            jobs, _ = store.enqueue_batch(
                "batch-final-restart-1",
                [_request("one")],
                execution_context=_context(),
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS,
                "batch-final-restart-mode-1",
                AUTONOMOUS_CONFIRMATION,
            )
            claimed = store.claim_next("owner")
            store.mark_task_started(claimed["job_id"], "owner", "TASK-ONE")
            store.record_result(
                claimed["job_id"], "owner", _success("TASK-ONE", "app/same.txt")
            )
            restarted = JobStore(root)
            state = restarted.queue_snapshot()
            self.assertEqual(BATCH_FINAL_COMMIT, state["commit_policy"])
            self.assertEqual(_active_fields()["active_baseline_id"], state["active_baseline_id"])
            self.assertFalse(state["paused"])

            legacy_root = Path(temp) / "legacy-control"
            legacy_root.mkdir()
            legacy = JobStore(legacy_root)
            legacy_jobs, _ = legacy.enqueue_batch(
                "legacy-gate-1", [_request("legacy")], execution_context=_context()
            )
            claimed = legacy.claim_next("legacy-owner")
            legacy.mark_task_started(claimed["job_id"], "legacy-owner", "TASK-LEGACY")
            legacy.record_result(
                legacy_jobs[0]["job_id"],
                "legacy-owner",
                _success("TASK-LEGACY", "app/same.txt"),
            )
            self.assertEqual("SUCCESS_ACK_REQUIRED", legacy.queue_snapshot()["gate_reason"])

    def test_verified_result_without_build_and_review_pass_stops_for_qa(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"
            root.mkdir()
            store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            jobs, _ = store.enqueue_batch(
                "batch-final-contract-1", [_request("one")], execution_context=_context()
            )
            claimed = store.claim_next("owner")
            store.mark_task_started(claimed["job_id"], "owner", "TASK-ONE")
            invalid = _success("TASK-ONE", "app/same.txt")
            invalid["review_status"] = "REVIEW_ERROR"
            result = store.record_result(jobs[0]["job_id"], "owner", invalid)
            self.assertEqual(AWAITING_QA, result["status"])
            self.assertEqual(
                "CUMULATIVE_VERIFICATION_CONTRACT_INVALID",
                result["last_result"]["failure_code"],
            )


class _BatchHarness:
    def __init__(self, *, snapshot_error: str = ""):
        self.snapshot_error = snapshot_error
        self.commits = 0
        self.executions = 0
        self.lease = False

    def acquire_execution_lease(self):
        self.lease = True
        return True

    def release_execution_lease(self):
        self.lease = False

    def prepare_job_snapshot(self, job, queue):
        if self.snapshot_error:
            raise HarnessServiceError(self.snapshot_error)
        return {
            "snapshot_id": f"SNAP-{job['sequence']}",
            "manifest_path": f"D:/snapshots/{job['job_id']}.json",
        }

    def execute(self, job, *, on_task_started):
        self.executions += 1
        task_id = f"TASK-{job['sequence']}"
        on_task_started(task_id)
        return _success(task_id, "app/same.txt")

    def commit_verified_job(self, job):
        self.commits += 1
        raise AssertionError("Supervisor must not commit under BATCH_FINAL_COMMIT")


class CumulativeSupervisorTest(unittest.TestCase):
    def _store(self, temp: str, count: int) -> tuple[JobStore, list[dict]]:
        root = Path(temp) / ".control"
        root.mkdir()
        store = JobStore(root)
        store.activate_cumulative_policy(1, _active_fields())
        jobs, _ = store.enqueue_batch(
            "batch-final-supervisor-1",
            [_request(f"job-{index}") for index in range(count)],
            execution_context=_context(),
        )
        store.set_execution_mode(
            EXECUTION_MODE_AUTONOMOUS,
            "batch-final-mode-1",
            AUTONOMOUS_CONFIRMATION,
        )
        return store, jobs

    def test_autonomous_success_runs_next_without_commit_or_success_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 2)
            harness = _BatchHarness()
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(10))
            self.assertEqual(2, harness.executions)
            self.assertEqual(0, harness.commits)
            self.assertTrue(all(store.get_job(job["job_id"])["status"] == SUCCEEDED for job in jobs))
            self.assertFalse(store.queue_snapshot()["paused"])

    def test_snapshot_hash_failure_stops_awaiting_qa_before_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 1)
            harness = _BatchHarness(snapshot_error="SNAPSHOT_HASH_MISMATCH")
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(10))
            job = store.get_job(jobs[0]["job_id"])
            self.assertEqual(0, harness.executions)
            self.assertEqual(AWAITING_QA, job["status"])
            self.assertEqual("SNAPSHOT_HASH_MISMATCH", job["last_result"]["failure_code"])

    def test_skipped_u03_never_claims_or_executes_a07(self):
        with tempfile.TemporaryDirectory() as temp:
            store, jobs = self._store(temp, 2)
            first = store.claim_next("owner")
            store.mark_task_started(first["job_id"], "owner", "TASK-U03")
            store.record_result(
                first["job_id"],
                "owner",
                {
                    "task_id": "TASK-U03",
                    "status": "FAILED",
                    "stage": "DONE",
                    "success": False,
                    "verification_status": "PARTIAL",
                    "build_status": "SKIPPED",
                    "review_status": "REVIEW_ERROR",
                    "failure_type": "build",
                    "fix_scope": "RETRYABLE",
                    "worktree_disposition": "CLEAN_ROLLBACK",
                },
            )
            skipped, _ = store.skip_failed_job(
                first["job_id"], "U03 build was not verified", "skip-u03-regression-1"
            )
            self.assertEqual(SKIPPED, skipped["status"])

            harness = _BatchHarness()
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(10))
            self.assertEqual(0, harness.executions)
            self.assertEqual(QUEUED, store.get_job(jobs[1]["job_id"])["status"])
            snapshot = store.queue_snapshot()
            self.assertEqual(first["job_id"], snapshot["blocked_by_job_id"])
            self.assertEqual(
                PREDECESSOR_SUCCESS_GATE_FAILED, snapshot["gate_reason"]
            )


class CumulativeSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.workspace = self.base / "workspace"
        self.repo = self.workspace / "app"
        self.repo.mkdir(parents=True)
        subprocess.run(["git", "init", "-q"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.name", "Harness Test"], cwd=self.repo, check=True)
        (self.repo / "same.txt").write_text("head\n", encoding="utf-8")
        (self.repo / "frozen.txt").write_text("head frozen\n", encoding="utf-8")
        subprocess.run(["git", "add", "same.txt", "frozen.txt"], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=self.repo, check=True)
        (self.repo / "same.txt").write_text("approved task baseline\n", encoding="utf-8")
        (self.repo / "frozen.txt").write_text("external frozen baseline\n", encoding="utf-8")
        subprocess.run(["git", "add", "frozen.txt"], cwd=self.repo, check=True)
        self.control = self.base / "harness" / ".control"
        self.control.mkdir(parents=True)
        placeholder = self.base / "placeholder"
        placeholder.write_text("rules", encoding="utf-8")
        policy = ControlPolicyProfile(
            commit_policy=BATCH_FINAL_COMMIT,
            cumulative_worktree=True,
            auto_continue_without_commit=True,
            pre_job_snapshot_supported=True,
            scoped_rollback_supported=True,
            final_commit_required=True,
            task_owned_baseline_files=(
                BaselineFileProfile("app/same.txt", _sha(self.repo / "same.txt"), "task evidence"),
            ),
            external_frozen_baseline_files=(
                BaselineFileProfile("app/frozen.txt", _sha(self.repo / "frozen.txt"), "external"),
            ),
        )
        self.profile = ProjectProfile(
            schema_version=1,
            id="test",
            display_name="test",
            profile_dir=self.base,
            workspace_roots=(self.workspace.resolve(),),
            project_rules=placeholder,
            analysis=placeholder,
            ui_map=None,
            db_catalog=None,
            modules=(ModuleProfile("app", "app", "backend", False),),
            rules={"always": (placeholder,)},
            area_patterns={},
            protected_paths=(),
            local_dev_config_excludes=(),
            control_policy=policy,
        )
        self.manager = CumulativePolicyManager(
            self.workspace, self.control, self.profile
        )

    def tearDown(self):
        self.temp.cleanup()

    def _job(self, job_id: str) -> dict[str, object]:
        return {
            "job_id": job_id,
            "outer_attempt": 0,
            "request": {"target_resources": ["app/same.txt"]},
        }

    def test_same_file_second_job_failure_restores_job1_and_removes_new_file(self):
        baseline = self.manager.prepare_batch_baseline(5)
        baseline_id = baseline["active_baseline_id"]
        first = self.manager.prepare_job_snapshot(self._job("JOB-ONE"), baseline_id, [])
        self.assertTrue(Path(first["manifest_path"]).is_file())
        (self.repo / "same.txt").write_text("job one success\n", encoding="utf-8")

        second = self.manager.prepare_job_snapshot(
            self._job("JOB-TWO"), baseline_id, ["app/same.txt"]
        )
        pre_job_two = self.manager.load_job_baseline(second["manifest_path"])
        (self.repo / "same.txt").write_text("job two failed\n", encoding="utf-8")
        (self.repo / "new.txt").write_bytes(b"new job file\x00binary")
        collector = GitCollector(str(self.workspace), profile=self.profile)
        ok, message = collector.rollback(baseline=pre_job_two)
        self.assertTrue(ok, message)
        self.assertEqual("job one success\n", (self.repo / "same.txt").read_text(encoding="utf-8"))
        self.assertFalse((self.repo / "new.txt").exists())
        self.assertEqual(
            "external frozen baseline\n",
            (self.repo / "frozen.txt").read_text(encoding="utf-8"),
        )

        restarted = CumulativePolicyManager(self.workspace, self.control, self.profile)
        loaded = restarted.load_job_baseline(second["manifest_path"])
        self.assertEqual(pre_job_two.repos[0].worktree_tree, loaded.repos[0].worktree_tree)
        status = restarted.public_status(baseline_id, ["app/same.txt"])
        self.assertEqual(["app/same.txt"], status["batch_delta_files"])
        self.assertEqual([], status["unexpected_runtime_dirty_files"])
        self.assertTrue(status["external_frozen_integrity"])
        self.assertIn("app/same.txt", status["final_staging_files"])
        self.assertNotIn("app/frozen.txt", status["final_staging_files"])
        self.assertIn("app/frozen.txt", status["final_staging_excluded_files"])
        self.assertEqual(
            "frozen.txt",
            subprocess.run(
                ["git", "diff", "--cached", "--name-only"],
                cwd=self.repo,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
        )

    def test_snapshot_copy_hash_mismatch_fails_closed(self):
        baseline = self.manager.prepare_batch_baseline(5)
        snapshot = self.manager.prepare_job_snapshot(
            self._job("JOB-HASH"), baseline["active_baseline_id"], []
        )
        manifest = Path(snapshot["manifest_path"])
        copied = next((manifest.parent / "files").rglob("*"))
        while copied.is_dir():
            copied = next(copied.rglob("*"))
        copied.write_bytes(b"corrupt")
        with self.assertRaises(CumulativePolicyError) as raised:
            self.manager.load_job_baseline(manifest)
        self.assertEqual("SNAPSHOT_HASH_MISMATCH", raised.exception.code)

    def test_task_owned_and_frozen_dirty_are_classified_and_job_delta_is_separate(self):
        baseline = self.manager.prepare_batch_baseline(5)
        baseline_id = baseline["active_baseline_id"]
        manifest = self.manager.load_batch_manifest(baseline_id)
        self.assertEqual(
            ["app/same.txt"],
            [item["path"] for item in manifest["task_owned_baseline_files"]],
        )
        self.assertEqual(
            ["app/frozen.txt"],
            [item["path"] for item in manifest["external_frozen_baseline_files"]],
        )
        self.assertEqual(
            ["app/frozen.txt", "app/same.txt"], manifest["initial_dirty_files"]
        )
        (self.repo / "same.txt").write_text("batch delta\n", encoding="utf-8")
        status = self.manager.public_status(baseline_id, ["app/same.txt"])
        self.assertEqual(["app/same.txt"], status["batch_delta_files"])
        self.assertEqual([], status["unexpected_runtime_dirty_files"])
        self.assertEqual(["app/same.txt"], status["final_staging_files"])
        self.assertEqual(["app/frozen.txt"], status["final_staging_excluded_files"])

    def test_baseline_after_external_change_is_blocked_and_frozen_overlap_stays_excluded(self):
        baseline = self.manager.prepare_batch_baseline(5)
        baseline_id = baseline["active_baseline_id"]
        (self.repo / "runtime.txt").write_text("external runtime\n", encoding="utf-8")
        status = self.manager.public_status(baseline_id, [])
        self.assertEqual(["app/runtime.txt"], status["unexpected_runtime_dirty_files"])
        (self.repo / "runtime.txt").unlink()

        (self.repo / "frozen.txt").write_text("job touched frozen\n", encoding="utf-8")
        overlap = self.manager.public_status(baseline_id, ["app/frozen.txt"])
        self.assertTrue(overlap["external_frozen_integrity"])
        self.assertNotIn("app/frozen.txt", overlap["final_staging_files"])
        self.assertIn("app/frozen.txt", overlap["final_staging_excluded_files"])

    def test_missing_snapshot_manifest_fails_closed_after_restart(self):
        restarted = CumulativePolicyManager(self.workspace, self.control, self.profile)
        with self.assertRaises(CumulativePolicyError) as raised:
            restarted.load_job_baseline(self.control / "cumulative" / "missing.json")
        self.assertEqual("PRE_JOB_SNAPSHOT_MISSING", raised.exception.code)

    def test_porcelain_modified_with_equal_normalized_blobs_is_clean(self):
        clean = self.repo / "normalized.txt"
        clean.write_text("normalized\n", encoding="utf-8")
        subprocess.run(["git", "add", "normalized.txt"], cwd=self.repo, check=True)
        subprocess.run(
            ["git", "commit", "--only", "-qm", "normalized fixture", "--", "normalized.txt"],
            cwd=self.repo,
            check=True,
        )
        original = self.manager._run_git

        def porcelain_m(repo, args, **kwargs):
            if args and args[0] == "status":
                return 0, " M normalized.txt\0", ""
            return original(repo, args, **kwargs)

        with mock.patch.object(self.manager, "_run_git", side_effect=porcelain_m):
            inventory = self.manager._repo_inventory(self.repo, "app")
        self.assertNotIn(
            "app/normalized.txt", self.manager._dirty_paths([inventory])
        )

    def test_semantic_worktree_change_is_dirty(self):
        semantic = self.repo / "semantic.txt"
        semantic.write_text("before\n", encoding="utf-8")
        subprocess.run(["git", "add", "semantic.txt"], cwd=self.repo, check=True)
        subprocess.run(
            ["git", "commit", "--only", "-qm", "semantic fixture", "--", "semantic.txt"],
            cwd=self.repo,
            check=True,
        )
        semantic.write_text("after with a real change\n", encoding="utf-8")
        inventory = self.manager._repo_inventory(self.repo, "app")
        self.assertIn("app/semantic.txt", self.manager._dirty_paths([inventory]))

    def test_cumulative_baseline_check_error_does_not_block_final_staging(self):
        baseline = self.manager.prepare_batch_baseline(5)
        snapshot = self.manager.prepare_job_snapshot(
            self._job("JOB-U02"), baseline["active_baseline_id"], []
        )
        pre_job = self.manager.load_job_baseline(snapshot["manifest_path"])
        collector = GitCollector(str(self.workspace), profile=self.profile)
        task = SimpleNamespace(changed_files=["app/same.txt"])
        with mock.patch.object(collector, "_run_in", return_value=(2, "", "error")):
            eligible, blockers, hashes = collector.assess_commit_safety(task, pre_job)
        self.assertTrue(eligible)
        self.assertEqual([], blockers)
        self.assertIn("app/same.txt", hashes)
        status = self.manager.public_status(
            baseline["active_baseline_id"], ["app/same.txt"]
        )
        self.assertEqual(["app/same.txt"], status["final_staging_files"])
        self.assertNotIn("app/frozen.txt", status["final_staging_files"])


if __name__ == "__main__":
    unittest.main()
