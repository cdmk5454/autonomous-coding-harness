from __future__ import annotations

import hashlib
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path

from git_collector import GitCollector
from git_committer import JobCommitter
from job_contract import JobContractError, JobRequest
from job_queue import (
    AUTONOMOUS_CONFIRMATION,
    EXECUTION_MODE_ATTENDED,
    EXECUTION_MODE_AUTONOMOUS,
    RESET_CONFIRMATION,
    JobStore,
    QueueError,
)
from manager import TaskScopeCoordinator
from project_profile import ModuleProfile, ProjectProfile
from queue_supervisor import QueueSupervisor
from reviewer import deterministic_violations, plan_diff_batches
from task_state import TaskState


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def execution_context() -> dict[str, object]:
    digest = "a" * 64
    return {
        "profile_id": "e4u",
        "profile_schema_version": 1,
        "profile_manifest_sha256": digest,
        "profile_snapshot_sha256": digest,
        "policy_snapshot_sha256": digest,
        "workspace_identity_sha256": digest,
    }


def request(client: str = "T-1") -> JobRequest:
    return JobRequest.from_mapping(
        {
            "client_job_id": client,
            "requirement": "SP0022 검색 조건을 수정한다.",
            "worker": "codex",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "medium",
            "target_modules": ["suc", "sucui"],
            "target_resources": [
                "suc/src/main/java/suc/sp/query",
                "sucui/src/pages/admin/sp",
            ],
            "commit_summary": "SP0022 검색 조건을 개선했습니다.",
        },
        allowed_modules=["suc", "sucui"],
    )


class DiffParserRegressionTests(unittest.TestCase):
    def test_blank_transport_separator_is_not_a_malformed_hunk(self):
        diff = (
            "diff --git a/a.txt b/a.txt\n"
            "--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-old\n+new\n\n"
            "diff --git a/b.txt b/b.txt\n"
            "--- a/b.txt\n+++ b/b.txt\n@@ -1 +1 @@\n-x\n+y\n"
        )
        plan = plan_diff_batches(diff)
        self.assertTrue(plan.valid, plan.reason)
        self.assertEqual(4, len(plan.expected_atoms))


class ResourceScopeTests(unittest.TestCase):
    def test_job_contract_preserves_granular_resources(self):
        parsed = request()
        self.assertEqual(
            ("suc/src/main/java/suc/sp/query", "sucui/src/pages/admin/sp"),
            parsed.target_resources,
        )
        self.assertIn("검색 조건", parsed.commit_summary)

    def test_resource_must_be_below_declared_module(self):
        payload = request().to_dict()
        payload["target_resources"] = ["core/src"]
        with self.assertRaises(JobContractError) as raised:
            JobRequest.from_mapping(payload, allowed_modules=["suc", "sucui", "core"])
        self.assertEqual("RESOURCE_OUTSIDE_MODULES", raised.exception.code)

    def test_prefix_overlap_blocks_but_sibling_feature_does_not(self):
        self.assertTrue(
            TaskScopeCoordinator._overlaps(
                TaskScopeCoordinator.keys(["suc/src/main/java/suc/sp"]),
                TaskScopeCoordinator.keys(["suc/src/main/java/suc/sp/query/A.xml"]),
            )
        )
        self.assertFalse(
            TaskScopeCoordinator._overlaps(
                TaskScopeCoordinator.keys(["suc/src/main/java/suc/de"]),
                TaskScopeCoordinator.keys(["suc/src/main/java/suc/sc"]),
            )
        )

    def test_changed_file_outside_resource_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = GitAutomationTests()._profile(root)
            codes, details = deterministic_violations(
                ["suc/src/main/java/suc/sc/Other.java"],
                "",
                profile,
                ["suc"],
                ["suc/src/main/java/suc/de"],
            )
            self.assertIn("OUT_OF_RESOURCE_SCOPE", codes)
            self.assertTrue(any("Other.java" in item for item in details))


class QueuePolicyTests(unittest.TestCase):
    def test_mode_batch_cancel_and_logical_reset_are_safe_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "batch-071", [request("A"), request("B")],
                execution_context=execution_context(),
            )
            batch_id = jobs[0]["batch_id"]
            with self.assertRaises(QueueError):
                store.set_execution_mode("AUTONOMOUS", "mode-1", "wrong")
            queue, replayed = store.set_execution_mode(
                "AUTONOMOUS", "mode-1", AUTONOMOUS_CONFIRMATION
            )
            self.assertFalse(replayed)
            self.assertEqual(EXECUTION_MODE_AUTONOMOUS, queue["execution_mode"])

            cancelled, replayed = store.cancel_queued_batch(
                batch_id, "superseded", "cancel-1"
            )
            self.assertFalse(replayed)
            self.assertEqual(2, len(cancelled))
            replay, replayed = store.cancel_queued_batch(
                batch_id, "superseded", "cancel-1"
            )
            self.assertTrue(replayed)
            self.assertEqual(2, len(replay))

            queue, replayed = store.reset_terminal_queue(
                "reset-1", RESET_CONFIRMATION
            )
            self.assertFalse(replayed)
            snapshot = store.queue_snapshot()
            self.assertEqual(0, snapshot["total"])
            self.assertEqual(2, snapshot["history_total"])
            self.assertEqual(EXECUTION_MODE_ATTENDED, snapshot["execution_mode"])
            new_jobs, replayed = store.enqueue_batch(
                "batch-071", [request("C")], execution_context=execution_context()
            )
            self.assertFalse(replayed)
            self.assertNotEqual(batch_id, new_jobs[0]["batch_id"])
            self.assertEqual(1, store.queue_snapshot()["total"])

    def test_autonomous_mode_commits_acknowledges_and_runs_next_job(self):
        class Harness:
            def __init__(self):
                self.lease = False
                self.executed: list[str] = []
                self.committed: list[str] = []

            def validate_configuration(self):
                return {
                    "working_dir": "D:\\project",
                    "profile_dir": "D:\\agents\\profiles\\e4u",
                    "profile_id": "e4u",
                    "profile_schema_version": 1,
                    "available_modules": ["suc", "sucui"],
                    "task_root": "D:\\kkm_test\\.tasks",
                    **execution_context(),
                }

            def acquire_execution_lease(self):
                self.lease = True
                return True

            def release_execution_lease(self):
                self.lease = False

            def probe_execution_idle(self):
                return not self.lease

            def execute(self, job, *, on_task_started):
                self.executed.append(job["job_id"])
                on_task_started(f"TASK-071-{len(self.executed)}")
                return {
                    "task_id": f"TASK-071-{len(self.executed)}",
                    "status": "SUCCESS",
                    "stage": "DONE",
                    "success": True,
                    "verification_status": "VERIFIED",
                    "build_status": "PASS",
                    "test_status": "SKIPPED",
                    "review_status": "REVIEW_PASS",
                    "changed_files": ["suc/src/A.java"],
                    "commit_eligible": True,
                    "changed_file_sha256": {"suc/src/A.java": "a" * 64},
                    "commit_blockers": [],
                    "failure_code": "",
                }

            def commit_verified_job(self, job):
                self.committed.append(job["job_id"])
                return {
                    "status": "COMMITTED",
                    "commits": [{"repository": "suc", "commit": "f" * 40}],
                }

        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "autonomous-071", [request("A"), request("B")],
                execution_context=execution_context(),
            )
            store.set_execution_mode(
                "AUTONOMOUS", "mode-autonomous-071", AUTONOMOUS_CONFIRMATION
            )
            harness = Harness()
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            self.assertEqual([job["job_id"] for job in jobs], harness.executed)
            self.assertEqual([job["job_id"] for job in jobs], harness.committed)
            for job in jobs:
                persisted = store.get_job(job["job_id"])
                self.assertEqual("SUCCEEDED", persisted["status"])
                self.assertTrue(persisted["success_acknowledgements"])
                self.assertEqual("COMMITTED", persisted["last_result"]["commit_status"])
            self.assertFalse(store.queue_snapshot()["paused"])


class GitAutomationTests(unittest.TestCase):
    def _profile(self, root: Path) -> ProjectProfile:
        marker = root / "marker.md"
        marker.write_text("x", encoding="utf-8")
        return ProjectProfile(
            schema_version=1,
            id="test",
            display_name="test",
            profile_dir=root,
            workspace_roots=(root,),
            project_rules=marker,
            analysis=marker,
            ui_map=None,
            db_catalog=None,
            modules=(ModuleProfile("suc", "suc", "backend", False),),
            rules={},
            area_patterns={},
            protected_paths=(),
            local_dev_config_excludes=(),
        )

    def test_eol_repair_and_verified_path_scoped_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo = root / "suc"
            repo.mkdir()
            git(repo, "init")
            git(repo, "config", "user.name", "Harness Test")
            git(repo, "config", "user.email", "harness@example.invalid")
            git(repo, "config", "core.autocrlf", "false")
            path = repo / "src/main/java/A.java"
            unrelated = repo / "src/main/java/Unrelated.java"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"one\ntwo\n")
            unrelated.write_bytes(b"original\n")
            git(repo, "add", ".")
            git(repo, "commit", "-m", "init")

            collector = GitCollector(str(root), self._profile(root))
            task = TaskState(
                task_id="TASK-071",
                requirement="change",
                working_dir=str(root),
                target_module=["suc"],
            )
            baseline, error = collector.capture_baseline(task)
            self.assertIsNotNone(baseline, error)
            path.write_bytes(b"one\r\nchanged\r\n")
            result = collector.collect(task, baseline)
            self.assertTrue(result.success)
            repaired = collector.repair_eol_to_index(task, baseline)
            self.assertTrue(repaired.success)
            self.assertEqual(["suc/src/main/java/A.java"], repaired.repaired_files)
            self.assertEqual(b"one\nchanged\n", path.read_bytes())
            result = collector.collect(task, baseline)
            eligible, blockers, hashes = collector.assess_commit_safety(task, baseline)
            self.assertTrue(eligible, blockers)

            # An unrelated staged change must remain outside this Job commit.
            unrelated.write_bytes(b"user change\n")
            git(repo, "add", "--", "src/main/java/Unrelated.java")

            job = {
                "status": "SUCCEEDED",
                "client_job_id": "TEST-071",
                "current_requirement": "Java 파일 내용을 개선한다.",
                "request": {
                    "requirement": "Java 파일 내용을 개선한다.",
                    "commit_summary": "Java 파일 내용을 개선했습니다.",
                },
                "last_result": {
                    "success": True,
                    "verification_status": "VERIFIED",
                    "build_status": "PASS",
                    "review_status": "REVIEW_PASS",
                    "changed_files": result.changed_files,
                    "commit_eligible": eligible,
                    "commit_blockers": blockers,
                    "changed_file_sha256": hashes,
                },
            }
            receipt = JobCommitter(root, self._profile(root)).commit(job)
            self.assertEqual("COMMITTED", receipt["status"])
            self.assertEqual(1, len(receipt["commits"]))
            self.assertFalse(git(repo, "status", "--porcelain", "--", "src/main/java/A.java"))
            self.assertTrue(
                git(repo, "status", "--porcelain", "--", "src/main/java/Unrelated.java").startswith("M ")
            )
            recovered = JobCommitter(root, self._profile(root)).commit(job)
            self.assertEqual(receipt["commits"][0]["commit"], recovered["commits"][0]["commit"])
            self.assertTrue(recovered["commits"][0]["recovered"])


if __name__ == "__main__":
    unittest.main()
