from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import attempt_checkpoint
import worker
from attempt_checkpoint import AttemptCheckpointError, AttemptCheckpointStore
from build_test_runner import BuildTestResult, BuildTestRunner
from git_collector import GitCollector
from harness_service import HarnessService
from job_queue import (
    AUTONOMOUS_CONFIRMATION,
    BATCH_FINAL_COMMIT,
    EXECUTION_MODE_AUTONOMOUS,
    JobStore,
)
from manager import (
    CHECKPOINT_CAPTURE_FAILED,
    Manager,
    WORKER_PATCH_CONFLICT,
    WORKER_PATCH_CONTEXT_STALE,
    WORKER_PATCH_CONTEXT_MISMATCH,
    _worker_patch_failure,
)
from reviewer import REVIEW_PASS, ReviewResult
from task_state import TaskState
from test_081_patch import PROFILE, _Collector, _Runner, _Reviewer, _Worker
from test_batch_final_commit import _active_fields, _context, _request
from worker import CodexWorker, DroidWorker


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class _CheckpointFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.base = Path(self.temp.name)
        self.root = self.base / "workspace"
        self.root.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        self.before = {
            "tracked.txt": b"before\n",
            "delete.txt": b"delete-me\r\n",
            "rename.txt": b"rename-body\n",
            "binary.bin": b"\x00before\xff",
            "bom.txt": b"\xef\xbb\xbfbefore\r\n",
            "mode.sh": b"#!/bin/sh\necho before\n",
            "inherited.txt": b"base\n",
            "frozen.txt": b"external\n",
        }
        for name, payload in self.before.items():
            (self.root / name).write_bytes(payload)
        if os.name != "nt":
            os.chmod(self.root / "mode.sh", 0o755)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=self.root, check=True)

        # These two changes exist before the Job and must survive rollback/restore.
        (self.root / "inherited.txt").write_bytes(b"u02-preserved\n")
        (self.root / "frozen.txt").write_bytes(b"external-frozen\n")
        self.inherited_hash = _sha((self.root / "inherited.txt").read_bytes())
        self.frozen_hash = _sha((self.root / "frozen.txt").read_bytes())
        self.collector = GitCollector(str(self.root))
        self.baseline, error = self.collector.capture_baseline()
        self.assertIsNotNone(self.baseline, error)

        (self.root / "tracked.txt").write_bytes(b"after\n")
        (self.root / "delete.txt").unlink()
        (self.root / "rename.txt").rename(self.root / "renamed.txt")
        (self.root / "binary.bin").write_bytes(b"\x00after\xfe")
        (self.root / "bom.txt").write_bytes(b"\xef\xbb\xbfafter\r\n")
        (self.root / "new.txt").write_bytes(b"new\n")
        (self.root / "mode.sh").write_bytes(b"#!/bin/sh\necho after\n")
        self.changed = [
            "tracked.txt", "delete.txt", "rename.txt", "renamed.txt",
            "binary.bin", "bom.txt", "new.txt", "mode.sh",
        ]
        self.store = AttemptCheckpointStore(self.root, self.base / "control")

    def tearDown(self):
        self.temp.cleanup()

    def task(self, task_id: str = "TASK-084") -> TaskState:
        return TaskState(
            task_id=task_id,
            requirement="checkpoint stabilization",
            task_mode="MODIFICATION",
            working_dir=str(self.root),
            worker="codex",
            codex_model="gpt-5.6-sol",
            task_owned_changed_files=list(self.changed),
            scoped_rollback_targets=list(self.changed),
            changed_files=list(self.changed),
            inherited_batch_delta_files=["inherited.txt"],
            queue_generation=5,
            active_baseline_id="BASELINE-G5-TEST",
            batch_id="BATCH-084",
            job_id="JOB-084",
            client_job_id="CLIENT-084",
            outer_attempt=1,
            pre_job_snapshot_id="SNAP-084",
            external_frozen_integrity=True,
            baseline_declaration_integrity=True,
        )


class CheckpointContract084Test(_CheckpointFixture):
    def test_all_file_shapes_round_trip_and_predecessors_remain_byte_exact(self):
        task = self.task()
        record = self.store.capture(task, self.baseline, "review failure")
        manifest = self.store.load(record["manifest_path"])
        self.assertEqual(set(self.changed), set(manifest["task_owned_changed_files"]))
        self.assertEqual({"from": "rename.txt", "to": "renamed.txt"}, manifest["renames"][0])
        rows = {item["path"]: item for item in manifest["files"]}
        self.assertEqual("DELETED", rows["delete.txt"]["after_state"])
        self.assertTrue(rows["new.txt"]["untracked"])
        self.assertTrue(rows["binary.bin"]["binary"])
        self.assertEqual("CRLF", rows["bom.txt"]["after_eol"])
        self.assertEqual("UTF8", rows["bom.txt"]["after_bom"])
        self.assertEqual(rows["rename.txt"]["before_sha256"], rows["renamed.txt"]["after_sha256"])

        blob_files = [item for item in self.store.blob_root.rglob("*") if item.is_file()]
        referenced = {
            item[key] for item in manifest["files"]
            for key in ("before_blob", "after_blob") if item.get(key)
        }
        self.assertEqual(len(referenced), len(blob_files))

        ok, detail = self.collector.rollback(task=task, baseline=self.baseline)
        self.assertTrue(ok, detail)
        self.assertEqual(self.inherited_hash, _sha((self.root / "inherited.txt").read_bytes()))
        self.assertEqual(self.frozen_hash, _sha((self.root / "frozen.txt").read_bytes()))

        restored = self.store.restore_for_retry(self.task(), record["manifest_path"])
        self.assertEqual(record["checkpoint_id"], restored["checkpoint_id"])
        self.assertEqual(b"after\n", (self.root / "tracked.txt").read_bytes())
        self.assertFalse((self.root / "delete.txt").exists())
        self.assertFalse((self.root / "rename.txt").exists())
        self.assertEqual(b"rename-body\n", (self.root / "renamed.txt").read_bytes())
        self.assertEqual(b"\x00after\xfe", (self.root / "binary.bin").read_bytes())
        self.assertEqual(b"\xef\xbb\xbfafter\r\n", (self.root / "bom.txt").read_bytes())
        self.assertEqual(b"new\n", (self.root / "new.txt").read_bytes())
        self.assertEqual(self.inherited_hash, _sha((self.root / "inherited.txt").read_bytes()))
        self.assertEqual(self.frozen_hash, _sha((self.root / "frozen.txt").read_bytes()))
        if os.name != "nt":
            self.assertEqual(rows["mode.sh"]["after_mode"], stat.S_IMODE((self.root / "mode.sh").stat().st_mode))

    def test_manifest_incomplete_corrupt_blob_and_atomic_publish_fail_closed(self):
        task = self.task()
        record = self.store.capture(task, self.baseline, "failure")
        manifest_path = Path(record["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0].pop("after_sha256")
        payload = json.dumps(manifest, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
        manifest_path.write_bytes(payload)
        (manifest_path.parent / "manifest.sha256").write_text(_sha(payload), encoding="ascii")
        with self.assertRaises(AttemptCheckpointError) as raised:
            self.store.load(manifest_path)
        self.assertEqual("CHECKPOINT_MANIFEST_INCOMPLETE", raised.exception.code)

        # A fresh store proves corrupted content-addressed blobs are rejected.
        other = self.base / "other"
        fresh = AttemptCheckpointStore(self.root, other)
        record2 = fresh.capture(self.task("TASK-084-BLOB"), self.baseline, "failure")
        loaded = json.loads(Path(record2["manifest_path"]).read_text(encoding="utf-8"))
        digest = next(item["after_blob"] for item in loaded["files"] if item.get("after_blob"))
        (fresh.blob_root / digest[:2] / digest).write_bytes(b"corrupt")
        with self.assertRaises(AttemptCheckpointError) as raised:
            fresh.load(record2["manifest_path"])
        self.assertEqual("CHECKPOINT_BLOB_HASH_MISMATCH", raised.exception.code)

        atomic = AttemptCheckpointStore(self.root, self.base / "atomic")
        real_replace = attempt_checkpoint.os.replace
        def fail_publish(source, destination):
            if Path(source).is_dir():
                raise OSError("fault before atomic directory publish")
            return real_replace(source, destination)
        with mock.patch("attempt_checkpoint.os.replace", side_effect=fail_publish):
            with self.assertRaises(OSError):
                atomic.capture(self.task("TASK-084-ATOMIC"), self.baseline, "failure")
        self.assertFalse(any(atomic.root.rglob("manifest.json")))
        self.assertFalse(any(path.name.endswith(".tmp") for path in atomic.root.rglob("*")))

    def test_restore_rejects_lineage_integrity_and_preimage_drift(self):
        record = self.store.capture(self.task(), self.baseline, "failure")
        ok, detail = self.collector.rollback(task=self.task(), baseline=self.baseline)
        self.assertTrue(ok, detail)
        mutations = [
            ("queue_generation", 6, "CHECKPOINT_GENERATION_MISMATCH"),
            ("active_baseline_id", "DRIFT", "CHECKPOINT_BASELINE_MISMATCH"),
            ("job_id", "OTHER", "CHECKPOINT_JOB_MISMATCH"),
            ("external_frozen_integrity", False, "CHECKPOINT_EXTERNAL_INTEGRITY_FAILED"),
            ("baseline_declaration_integrity", False, "CHECKPOINT_BASELINE_INTEGRITY_FAILED"),
            ("unexpected_external_dirty_files", ["x"], "CHECKPOINT_UNEXPECTED_DIRTY"),
        ]
        for field, value, code in mutations:
            candidate = self.task()
            setattr(candidate, field, value)
            with self.subTest(field=field), self.assertRaises(AttemptCheckpointError) as raised:
                self.store.restore_for_retry(candidate, record["manifest_path"])
            self.assertEqual(code, raised.exception.code)
        (self.root / "tracked.txt").write_bytes(b"conflict\n")
        with self.assertRaises(AttemptCheckpointError) as raised:
            self.store.restore_for_retry(self.task(), record["manifest_path"])
        self.assertEqual("CHECKPOINT_RESTORE_CONFLICT", raised.exception.code)

    def test_restore_apply_failure_compensates_already_applied_files(self):
        record = self.store.capture(self.task(), self.baseline, "failure")
        ok, detail = self.collector.rollback(task=self.task(), baseline=self.baseline)
        self.assertTrue(ok, detail)
        before = {path: ((self.root / path).read_bytes() if (self.root / path).is_file() else None) for path in self.changed}
        real_apply = self.store._apply_state
        calls = {"count": 0}
        def fail_second(target, payload, mode):
            calls["count"] += 1
            if calls["count"] == 2:
                raise OSError("fault during restore")
            return real_apply(target, payload, mode)
        with mock.patch.object(self.store, "_apply_state", side_effect=fail_second):
            with self.assertRaises(AttemptCheckpointError) as raised:
                self.store.restore_for_retry(self.task(), record["manifest_path"])
        self.assertEqual("CHECKPOINT_RESTORE_APPLY_FAILED", raised.exception.code)
        after = {path: ((self.root / path).read_bytes() if (self.root / path).is_file() else None) for path in self.changed}
        self.assertEqual(before, after)


class RollbackCoordinator084Test(_CheckpointFixture):
    def test_checkpoint_first_and_verified_no_delta_are_the_only_paths(self):
        task = self.task()
        manager = Manager(
            str(self.root), "", profile=PROFILE, task_root=self.base / "tasks",
            checkpoint_store=self.store,
        )
        manager.collector = self.collector
        manager._task_baselines[task.task_id] = self.baseline
        self.assertTrue(manager._rollback_for_outer_retry(task, "fault"))
        self.assertEqual("CAPTURED_AND_VERIFIED", task.checkpoint_status)
        self.assertEqual("CHECKPOINTED_CLEAN_ROLLBACK", task.worktree_disposition)
        self.assertTrue(task.checkpoint_id)
        self.assertEqual(self.inherited_hash, _sha((self.root / "inherited.txt").read_bytes()))
        manager.shutdown()

        clean_baseline, error = self.collector.capture_baseline()
        self.assertIsNotNone(clean_baseline, error)
        clean = self.task("TASK-084-NO-DELTA")
        clean.changed_files = []
        clean.task_owned_changed_files = []
        clean.scoped_rollback_targets = []
        manager = Manager(
            str(self.root), "", profile=PROFILE, task_root=self.base / "tasks-clean",
            checkpoint_store=self.store,
        )
        manager.collector = self.collector
        manager._task_baselines[clean.task_id] = clean_baseline
        self.assertTrue(manager._rollback_for_outer_retry(clean, "no delta"))
        self.assertTrue(clean.no_task_delta_evidence["passed"])
        self.assertEqual("VERIFIED_NO_TASK_DELTA", clean.checkpoint_status)
        self.assertEqual("CLEAN_ROLLBACK", clean.worktree_disposition)
        manager.shutdown()

    def test_checkpoint_failure_preserves_current_and_predecessor_delta(self):
        class BrokenStore:
            def capture(self, *args, **kwargs):
                raise AttemptCheckpointError("CHECKPOINT_TEST_FAULT")
        task = self.task()
        manager = Manager(
            str(self.root), "", profile=PROFILE, task_root=self.base / "tasks",
            checkpoint_store=BrokenStore(),
        )
        manager.collector = self.collector
        manager._task_baselines[task.task_id] = self.baseline
        tracked_hash = _sha((self.root / "tracked.txt").read_bytes())
        self.assertFalse(manager._rollback_for_outer_retry(task, "fault"))
        self.assertEqual(CHECKPOINT_CAPTURE_FAILED, task.failure_code)
        self.assertEqual("AWAITING_QA", task.status)
        self.assertEqual("PRESERVED", task.worktree_disposition)
        self.assertFalse(task.checkpoint_failure_evidence["rollback_performed"])
        self.assertEqual(tracked_hash, _sha((self.root / "tracked.txt").read_bytes()))
        self.assertEqual(self.inherited_hash, _sha((self.root / "inherited.txt").read_bytes()))
        manager.shutdown()


class RestoreAndEvidence084Test(unittest.TestCase):
    def test_restore_clears_stale_success_evidence_and_cannot_auto_promote(self):
        task = TaskState(
            task_id="TASK-084-RESTORE", requirement="x", status="SUCCESS", stage="DONE",
            test_status="PASS", verification_status="VERIFIED",
            review_status="REVIEW_PASS", commit_eligible=True, outer_attempt=2,
        )
        task.build = {"status": "PASS"}
        task.test = {"status": "PASS"}
        task.build_evidence = {"status": "PASS"}
        task.review_result = "old"
        prior_epoch = task.restored_evidence_epoch
        Manager._prepare_restored_evidence_epoch(task, "CHK-084", ["src/A.java"])
        self.assertEqual("RUNNING", task.status)
        self.assertEqual("RESTORED_FOR_RETRY", task.checkpoint_status)
        self.assertEqual("WAITING", task.build["status"])
        self.assertFalse(task.test_required)
        self.assertEqual("SKIPPED", task.test_status)
        self.assertEqual("SKIPPED", task.verification_status)
        self.assertNotEqual("REVIEW_PASS", task.review_status)
        self.assertFalse(task.commit_eligible)
        self.assertNotEqual(prior_epoch, task.restored_evidence_epoch)

    def test_high_risk_and_non_retryable_checkpoint_never_auto_restore(self):
        base = {
            "checkpoint_manifest": "manifest.json", "failure_type": "review",
            "failure_code": "REVIEW_FAIL", "fix_scope": "RETRYABLE",
        }
        self.assertEqual("manifest.json", HarnessService._restore_checkpoint_candidate(base, 1))
        self.assertEqual("", HarnessService._restore_checkpoint_candidate({**base, "severity": "HIGH"}, 1))
        self.assertEqual("", HarnessService._restore_checkpoint_candidate({**base, "fix_scope": "NON_RETRYABLE"}, 1))
        self.assertEqual("", HarnessService._restore_checkpoint_candidate({**base, "failure_code": "SECURITY_POLICY"}, 1))

    def test_primary_patch_failure_survives_no_diff_gate(self):
        for stderr, expected in (
            ("apply_patch verification failed: multiple operations target A.java", WORKER_PATCH_CONFLICT),
            ("apply_patch verification failed: target context not found", WORKER_PATCH_CONTEXT_MISMATCH),
        ):
            task = TaskState(task_id="TASK-084-PATCH", requirement="x", worker_stderr=stderr)
            self.assertEqual(expected, _worker_patch_failure(task)[0])

    def test_actual_worker_executable_and_invocation_match_real_command(self):
        completed = SimpleNamespace(returncode=0, stdout='{"type":"result","result":"ok"}\n', stderr="")
        with tempfile.TemporaryDirectory() as temp, \
             mock.patch("worker.trusted_executable", side_effect=lambda value, **kwargs: f"C:/verified/{value}.exe"), \
             mock.patch("worker.codex_host_pair_matches", return_value=True), \
             mock.patch("worker.subprocess.run", return_value=completed) as run:
            task = TaskState(task_id="TASK-084-CODEX", requirement="read", worker="codex", codex_model="gpt-x")
            result = CodexWorker(model="gpt-x").execute("read", temp, task)
            self.assertEqual("C:/verified/codex.exe", result.command[0])
            self.assertEqual(result.command[0], task.actual_executable)
            self.assertTrue(task.actual_invocation_id.startswith("INV-"))
            self.assertEqual(result.command, run.call_args.args[0])

        with tempfile.TemporaryDirectory() as temp, \
             mock.patch("worker.trusted_executable", side_effect=lambda value, **kwargs: f"C:/verified/{value}.exe"), \
             mock.patch("worker.subprocess.run", return_value=completed) as run:
            task = TaskState(task_id="TASK-084-DROID", requirement="read", worker="droid", droid_model="alternate")
            result = DroidWorker(model="alternate").execute("read", temp, task)
            self.assertEqual("C:/verified/droid.exe", result.command[0])
            self.assertIn("alternate", result.command)
            self.assertEqual(result.command, run.call_args.args[0])


class StructuredBuild084Test(unittest.TestCase):
    def _evidence(self, root: Path, *, module: str = "suc", task_id: str = "TASK-BUILD", exit_code: int = 0):
        log = root / f"{module}.log"
        payload = b"build ok\n"
        log.write_bytes(payload)
        return {
            "module": module, "cwd": str(root), "argv": ["mvn.cmd", "test"],
            "command_source": "profile", "started_at": "2026-01-01T00:00:00+09:00",
            "finished_at": "2026-01-01T00:00:01+09:00", "exit_code": exit_code,
            "status": "PASS" if exit_code == 0 else "FAIL",
            "log_sha256": _sha(payload), "log_path": str(log),
            "task_id": task_id, "baseline_id": "BASELINE-X", "checkpoint_id": "",
            "restored_checkpoint_id": "", "restored_evidence_epoch": "",
        }

    def test_valid_evidence_passes_stale_wrong_module_partial_and_planned_only_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager = Manager(str(root), "", profile=PROFILE, task_root=root / "tasks")
            manager.build_test_runner = BuildTestRunner(str(root), evidence_root=root / "evidence")
            task = TaskState(task_id="TASK-BUILD", requirement="x", target_module=["suc", "sucui"], active_baseline_id="BASELINE-X")
            good = [self._evidence(root, module="suc"), self._evidence(root, module="sucui")]
            result = BuildTestResult(True, "PASS", "PASS", details={"executions": good})
            self.assertEqual((True, ""), manager._validate_structured_build_evidence(task, result))

            stale = json.loads(json.dumps(good)); stale[0]["task_id"] = "OLD"
            self.assertEqual("STALE_BUILD_EVIDENCE", manager._validate_structured_build_evidence(task, BuildTestResult(True, "PASS", "PASS", details={"executions": stale}))[1])
            partial = BuildTestResult(True, "PASS", "PASS", details={"executions": [good[0]]})
            self.assertEqual("BUILD_MODULE_COVERAGE_PARTIAL", manager._validate_structured_build_evidence(task, partial)[1])
            self.assertEqual("BUILD_EXECUTION_EVIDENCE_MISSING", manager._validate_structured_build_evidence(task, BuildTestResult(True, "PASS", "PASS", details={"planned_commands": [["mvn"]]}))[1])
            manager.shutdown()

    def test_structured_pass_wins_only_over_build_evidence_prose(self):
        task = TaskState(task_id="TASK", requirement="x")
        task.build_evidence = {"status": "PASS", "success": True, "details": {"structured_evidence_valid": True}}
        evidence_only = ReviewResult(False, "missing", status="REVIEW_FAIL", violation_codes=["BUILD_EVIDENCE_MISSING"])
        semantic = ReviewResult(False, "bug", status="REVIEW_FAIL", violation_codes=["BUSINESS_RULE"])
        self.assertTrue(Manager._review_only_disputes_valid_build(task, evidence_only))
        self.assertFalse(Manager._review_only_disputes_valid_build(task, semantic))


class BatchAndCompatibility084Test(unittest.TestCase):
    def test_three_job_batch_separates_validation_delta_commit_and_baseline_states(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            fields = _active_fields()
            self.assertEqual(BATCH_FINAL_COMMIT, fields["commit_policy"])
            store.activate_cumulative_policy(1, fields)
            jobs, _ = store.enqueue_batch(
                "batch-084", [_request("a"), _request("b"), _request("c")],
                execution_context=_context(),
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS,
                "batch-084-autonomous-v1",
                AUTONOMOUS_CONFIRMATION,
            )
            for number, queued in enumerate(jobs, 1):
                claimed = store.claim_next("owner")
                self.assertEqual(queued["job_id"], claimed["job_id"])
                store.mark_task_started(claimed["job_id"], "owner", f"TASK-{number}")
                stored = store.record_result(claimed["job_id"], "owner", {
                    "success": True, "status": "SUCCESS", "stage": "DONE",
                    "verification_status": "VERIFIED", "build_status": "PASS",
                    "review_status": "REVIEW_PASS", "changed_files": [f"src/{number}.txt"],
                    "task_id": f"TASK-{number}", "worktree_disposition": "PRESERVED",
                })
                self.assertEqual("SUCCEEDED", stored["status"])
                self.assertEqual("DEFERRED_TO_BATCH_FINAL", stored["last_result"]["job_commit_status"])
            snapshot = store.queue_snapshot()
            self.assertEqual(3, snapshot["job_validation_success_count"])
            self.assertEqual(3, snapshot["job_delta_preserved_count"])
            self.assertEqual(3, snapshot["job_commit_deferred_count"])
            self.assertTrue(snapshot["batch_final_staging_eligible"])
            self.assertFalse(snapshot["batch_final_commit_eligible"])
            self.assertFalse(snapshot["baseline_advance_eligible"])

    def test_task_state_loads_legacy_and_preserves_unknown_optional_fields(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy = {"task_id": "TASK-082", "requirement": "legacy", "status": "FAILED", "future_optional": {"x": 1}}
            tasks = root / ".tasks"
            tasks.mkdir()
            path = tasks / "TASK-082.json"
            path.write_text(json.dumps(legacy), encoding="utf-8")
            loaded = TaskState.load("TASK-082", tasks)
            self.assertEqual("FAILED", loaded.status)
            self.assertEqual({"x": 1}, loaded.unknown_optional_fields["future_optional"])
            loaded.save(tasks)
            roundtrip = TaskState.load("TASK-082", tasks)
            self.assertEqual({"x": 1}, roundtrip.unknown_optional_fields["future_optional"])


if __name__ == "__main__":
    unittest.main()
