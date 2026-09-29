from __future__ import annotations

import os
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path

from attempt_checkpoint import AttemptCheckpointError, AttemptCheckpointStore
from git_collector import GitCollector
from handoff import build_retry_handoff
from manager import (
    CHECKPOINT_CAPTURE_FAILED,
    Manager,
    WORKER_PATCH_CONFLICT,
    WORKER_PATCH_CONTEXT_STALE,
    WORKER_PATCH_CONTEXT_MISMATCH,
    _worker_patch_failure,
)
from reviewer import build_tester_context
from task_state import TaskState
from test_081_patch import PROFILE, U02, _Collector, _Runner, _Reviewer, _Worker


class PatchFailureClassificationTest(unittest.TestCase):
    def test_multiple_target_is_conflict_not_empty_delta(self):
        task = TaskState(task_id="TASK-083-C", requirement="test", worker_stderr="apply_patch verification failed: invalid patch: multiple operations target A.java")
        self.assertEqual(WORKER_PATCH_CONFLICT, _worker_patch_failure(task)[0])

    def test_context_failure_without_hash_evidence_is_mismatch(self):
        task = TaskState(task_id="TASK-083-S", requirement="test", worker_stderr="apply_patch verification failed: target context not found")
        self.assertEqual(WORKER_PATCH_CONTEXT_MISMATCH, _worker_patch_failure(task)[0])

    def test_corrected_prompt_is_exact_and_bounded(self):
        task = TaskState(task_id="TASK-083-H", requirement="test", retry_strategy="PATCH_CORRECTION_SAME_WORKER")
        text = build_retry_handoff(task)
        for phrase in (
            "실패한 대상 파일을 다시 읽는다",
            "동일 파일을 두 번 대상으로 지정하지 않는다",
            "하나의 patch operation으로 합치거나",
            "파일과 Git diff를 다시 읽는다",
            "CSS-only 정리나 범위 외 재포맷을 하지 않는다",
        ):
            self.assertIn(phrase, text)

    def test_repeated_conflict_executes_alternate_before_budget_end(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            primary = _Worker("apply_patch verification failed: invalid patch: multiple operations target A.java")
            alternate = _Worker("apply_patch verification failed: invalid patch: multiple operations target A.java")
            manager = Manager(str(root), "", max_retry=2, worker_type="codex", codex_model="gpt-5.6-sol", profile=PROFILE, task_root=root / "tasks")
            manager.collector = _Collector([])
            manager.build_test_runner = _Runner()
            manager.reviewer = _Reviewer()
            manager._fallback_route = lambda task: ("droid", "fallback-model", "verified")
            manager._get_worker = lambda task: ((alternate, "droid") if task.retry_worker else (primary, "codex"))
            task = TaskState(task_id="TASK-083-ALT", requirement="application source 수정", task_mode="MODIFICATION", worker="codex", codex_model="gpt-5.6-sol", target_module=["suc"], working_dir=str(root))
            manager.run_with_retry(task)
            manager.shutdown()
        self.assertGreaterEqual(primary.calls, 2)
        self.assertGreaterEqual(alternate.calls, 1)
        self.assertTrue(any(item.get("actual_worker") == "droid" for item in task.attempt_history))
        self.assertLessEqual(primary.calls + alternate.calls, 4)

    def test_build_command_and_exit_code_are_reviewer_context(self):
        task = TaskState(task_id="TASK-083-B", requirement="test")
        task.build_evidence = {"status": "PASS", "details": {"argv": ["mvn.cmd", "clean", "install"], "exit_code": 0}}
        context = build_tester_context(task)
        self.assertIn("mvn.cmd", context)
        self.assertIn('"exit_code": 0', context)


class AttemptCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.base = Path(self.temp.name)
        self.root = self.base / "workspace"
        self.root.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        (self.root / "text.txt").write_bytes(b"\xef\xbb\xbfbefore\r\n")
        (self.root / "inherited.txt").write_bytes(b"base\n")
        subprocess.run(["git", "add", "text.txt", "inherited.txt"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=self.root, check=True)
        (self.root / "inherited.txt").write_bytes(b"u02\n")
        self.collector = GitCollector(str(self.root))
        self.baseline, error = self.collector.capture_baseline()
        self.assertIsNotNone(self.baseline, error)
        (self.root / "text.txt").write_bytes(b"\xef\xbb\xbfafter\r\n")
        (self.root / "new.bin").write_bytes(b"\x00\x01\xff")
        self.store = AttemptCheckpointStore(self.root, self.base / "control")

    def tearDown(self):
        self.temp.cleanup()

    def _task(self):
        return TaskState(
            task_id="TASK-083-CP", requirement="checkpoint test", working_dir=str(self.root),
            task_owned_changed_files=["text.txt", "new.bin"],
            changed_files=["text.txt", "new.bin"],
            inherited_batch_delta_files=["inherited.txt"],
            queue_generation=5, active_baseline_id="BASELINE-G5-X",
            batch_id="BATCH-X", job_id="JOB-X", client_job_id="CLIENT-X",
            outer_attempt=1, pre_job_snapshot_id="SNAP-X",
            worker="codex", codex_model="gpt", retry_strategy="ORIGINAL",
            external_frozen_integrity=True, baseline_declaration_integrity=True,
        )

    def test_checkpoint_first_roundtrip_text_untracked_binary_bom_eol_and_dedupe(self):
        task = self._task()
        record = self.store.capture(task, self.baseline, "review fail")
        manifest = self.store.load(record["manifest_path"])
        self.assertEqual({"text.txt", "new.bin"}, set(record["files"]))
        text = next(item for item in manifest["files"] if item["path"] == "text.txt")
        binary = next(item for item in manifest["files"] if item["path"] == "new.bin")
        self.assertEqual("CRLF", text["eol"])
        self.assertEqual("UTF8", text["bom"])
        self.assertTrue(binary["binary"])
        self.assertTrue(binary["untracked"])
        second = self.store.capture(task, self.baseline, "same content")
        self.assertEqual(record["checkpoint_id"], second["checkpoint_id"])
        ok, _ = self.collector.rollback(task=task, baseline=self.baseline)
        self.assertTrue(ok)
        retry = self._task()
        restarted_store = AttemptCheckpointStore(self.root, self.base / "control")
        restored = restarted_store.restore_for_retry(retry, record["manifest_path"])
        self.assertEqual(b"\xef\xbb\xbfafter\r\n", (self.root / "text.txt").read_bytes())
        self.assertEqual(b"\x00\x01\xff", (self.root / "new.bin").read_bytes())
        self.assertEqual(b"u02\n", (self.root / "inherited.txt").read_bytes())
        self.assertEqual(record["checkpoint_id"], restored["checkpoint_id"])

    def test_inherited_hash_drift_and_unexpected_dirty_reject_restore(self):
        task = self._task()
        record = self.store.capture(task, self.baseline, "review fail")
        self.collector.rollback(task=task, baseline=self.baseline)
        (self.root / "inherited.txt").write_bytes(b"drift\n")
        with self.assertRaises(AttemptCheckpointError) as raised:
            self.store.restore_for_retry(self._task(), record["manifest_path"])
        self.assertEqual("CHECKPOINT_INHERITED_DELTA_MISMATCH", raised.exception.code)
        (self.root / "inherited.txt").write_bytes(b"u02\n")
        retry = self._task(); retry.unexpected_external_dirty_files = ["external.txt"]
        with self.assertRaises(AttemptCheckpointError) as raised:
            self.store.restore_for_retry(retry, record["manifest_path"])
        self.assertEqual("CHECKPOINT_UNEXPECTED_DIRTY", raised.exception.code)

    def test_capture_failure_blocks_rollback_and_preserves_delta(self):
        class Broken:
            def capture(self, *args, **kwargs):
                raise AttemptCheckpointError("BROKEN")
        collector = _Collector(["text.txt"])
        manager = Manager(str(self.root), "", profile=PROFILE, task_root=self.root / "tasks", checkpoint_store=Broken())
        manager.collector = collector
        task = self._task(); manager._task_baselines[task.task_id] = self.baseline
        self.assertFalse(manager._rollback_for_outer_retry(task, "test"))
        manager.shutdown()
        self.assertEqual(0, collector.rollbacks)
        self.assertEqual(CHECKPOINT_CAPTURE_FAILED, task.failure_code)
        self.assertEqual("AWAITING_QA", task.status)
        self.assertEqual(b"\xef\xbb\xbfafter\r\n", (self.root / "text.txt").read_bytes())


if __name__ == "__main__":
    unittest.main()
