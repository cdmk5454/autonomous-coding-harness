from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import harness_temp as tempfile

from build_test_runner import BuildTestResult, BuildTestRunner
from manager import Manager
from reviewer import ReviewResult
from task_state import TaskState
from tester import TesterResult, _apply_to_state, _parse_tester_output, run_tester
from tester_lifecycle import run_recovery_attempt


class BuildEvidenceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runner = BuildTestRunner(self.root, evidence_root=self.root / "evidence")
        self.runner._evidence_context = {
            "task_id": "TASK-1",
            "job_id": "JOB-1",
            "execution_id": "EXEC-PRODUCT-1",
            "candidate_id": "OCAND-1",
            "baseline_id": "BASE-1",
            "checkpoint_id": "",
            "restored_checkpoint_id": "",
            "restored_evidence_epoch": "",
        }

    @staticmethod
    def completed(stdout="ok\n", stderr="", returncode=0):
        return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)

    def execute(self, module, output, returncode=0):
        with patch("build_test_runner.trusted_executable", return_value="tool.exe"), patch(
            "build_test_runner.subprocess.run",
            return_value=self.completed(output, returncode=returncode),
        ):
            code, out, err = self.runner._run(["tool"], cwd=self.root)
        return self.runner._execution_details(module, "profile", (out or "") + ("\n" + err if err else "")), code

    def validate(self, executions, *, status="PASS", success=True):
        manager = object.__new__(Manager)
        manager.build_test_runner = self.runner
        task = TaskState(task_id="TASK-1", requirement="x", worker="opencode")
        task.target_module = [item["module"] for item in executions]
        task.active_baseline_id = "BASE-1"
        result = BuildTestResult(
            success=success,
            build_status=status,
            details={"executions": executions},
        )
        return manager._validate_structured_build_evidence(task, result)

    def test_module_receipts_use_distinct_immutable_logs(self):
        backend, _ = self.execute("suc", "backend\r\n")
        frontend, _ = self.execute("sucui", "frontend\n")
        self.assertNotEqual(backend["execution_id"], frontend["execution_id"])
        self.assertNotEqual(backend["log_path"], frontend["log_path"])
        self.assertEqual("EXEC-PRODUCT-1", backend["product_execution_id"])
        self.assertEqual("EXEC-PRODUCT-1", frontend["product_execution_id"])
        self.assertEqual((True, ""), self.validate([backend, frontend]))

    def test_finalized_log_mutation_is_rejected(self):
        receipt, _ = self.execute("suc", "immutable\n")
        Path(receipt["log_path"]).write_text("modified", encoding="utf-8")
        self.assertEqual(
            (False, "BUILD_LOG_HASH_MISMATCH"), self.validate([receipt])
        )

    def test_utf8_and_line_endings_hash_exact_final_bytes(self):
        receipt, _ = self.execute("suc", "가\r\n나\n")
        payload = Path(receipt["log_path"]).read_bytes()
        self.assertEqual("가\r\n나\n".encode("utf-8"), payload)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), receipt["log_sha256"])

    def test_nonzero_exit_remains_failure(self):
        receipt, code = self.execute("suc", "failed\n", returncode=2)
        self.assertEqual(2, code)
        receipt["status"] = "FAIL"
        self.assertEqual(
            (False, "BUILD_EXECUTION_NOT_PASS"),
            self.validate([receipt], status="FAIL", success=False),
        )


class TesterRecoveryTests(unittest.TestCase):
    def test_missing_result_is_protocol_error(self):
        parsed = _parse_tester_output("SUMMARY: missing marker")
        self.assertEqual("ERROR", parsed.status)
        self.assertIn("TEST_RESULT", parsed.summary)

    def test_malformed_output_is_preserved_with_hash(self):
        task = TaskState(task_id="TASK-T", requirement="x", worker="opencode")
        task.profile_dir = "D:/profile"
        task.effective_policy = {
            "resolved_tester": {"runtime": "droid", "model": "tester-model", "timeout": 10}
        }
        malformed = "SUMMARY: malformed output\nSTEPS:\n- no marker\n"
        with patch("tester.trusted_executable", return_value="droid.exe"), patch(
            "tester.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout=malformed, stderr=""),
        ):
            result = run_tester(task, ".")
        self.assertEqual("ERROR", result.status)
        self.assertEqual(malformed, task.official_tester_execution["stdout"])
        self.assertEqual(
            hashlib.sha256(malformed.encode("utf-8")).hexdigest(),
            task.official_tester_execution["stdout_sha256"],
        )

    def test_tester_only_recovery_is_bounded_and_preserves_candidate(self):
        task = TaskState(task_id="TASK-T", requirement="x", worker="opencode")
        task.candidate_id = "OCAND-SAME"
        task.tester_attempt_history = [{
            "attempt_id": "TESTER-OLD", "test_status": "ERROR", "status": "FAILED"
        }]

        def succeed():
            task.official_tester_invoked = True
            task.official_tester_execution = {"stdout": "TEST_RESULT: PASS"}
            _apply_to_state(task, TesterResult("PASS", "ok", ["verified"]))

        self.assertEqual("PASS", run_recovery_attempt(task, succeed))
        self.assertEqual("OCAND-SAME", task.candidate_id)
        self.assertEqual(1, task.technical_execution_retry_count)
        self.assertEqual("TESTER_TECHNICAL_RECOVERY", task.tester_attempt_history[-1]["kind"])
        task.test_status = "ERROR"
        with self.assertRaisesRegex(ValueError, "LIMIT_EXHAUSTED"):
            run_recovery_attempt(task, succeed)

    def test_recovery_failure_remains_technical_not_semantic(self):
        task = TaskState(task_id="TASK-T", requirement="x", worker="opencode")
        task.candidate_id = "OCAND-SAME"
        task.product_semantic_retry_count = 0
        task.tester_attempt_history = [{
            "attempt_id": "TESTER-OLD", "test_status": "ERROR", "status": "FAILED"
        }]

        def malformed():
            task.official_tester_execution = {"stdout": "bad"}
            _apply_to_state(task, TesterResult("ERROR", "TEST_RESULT missing"))

        self.assertEqual("ERROR", run_recovery_attempt(task, malformed))
        self.assertEqual(0, task.product_semantic_retry_count)
        self.assertEqual("OCAND-SAME", task.candidate_id)

    def test_successful_recovery_reaches_review_with_same_candidate(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        changed = "suc/A.java"
        target = root / changed
        target.parent.mkdir(parents=True)
        target.write_text("candidate", encoding="utf-8")
        runner = BuildTestRunner(root, evidence_root=root / "evidence")

        def build(**_kwargs):
            runner._evidence_context = {
                "task_id": "TASK-R", "job_id": "JOB-R",
                "execution_id": "EXEC-R", "candidate_id": "OCAND-SAME",
                "baseline_id": "BASE-R", "checkpoint_id": "",
                "restored_checkpoint_id": "", "restored_evidence_epoch": "",
            }
            runner._last_execution = {
                **runner._evidence_context,
                "execution_id": "BUILD-R",
                "product_execution_id": "EXEC-R",
                "cwd": str(root), "argv": ["tool"], "command_source": "profile",
                "started_at": "start", "finished_at": "end",
                "exit_code": 0, "status": "PASS",
            }
            receipt = runner._execution_details("suc", "profile", "build passed\n")
            return BuildTestResult(
                success=True, build_status="PASS",
                details={"tool": "profile", "executions": [receipt]},
            )

        runner.run = build
        reviewed = []
        manager = object.__new__(Manager)
        manager.working_dir = str(root)
        manager.build_test_runner = runner
        manager.configuration_guard = None
        manager.profile = None
        manager.policy_root = None
        manager.task_root = root / ".tasks"
        manager.max_retry = 1
        manager._initial_baseline = object()
        manager._log_stage = lambda *_args, **_kwargs: None
        manager._notify_progress = lambda *_args, **_kwargs: {}
        manager._prepare_reviewer_effort = lambda _task: None
        manager.reviewer = SimpleNamespace(review=lambda **kwargs: (
            reviewed.append(kwargs["task"].candidate_id)
            or ReviewResult(True, status="REVIEW_PASS", reviewed_files=[changed])
        ))
        manager.collector = SimpleNamespace(assess_commit_safety=lambda *_args: (
            True, [], {changed: hashlib.sha256(target.read_bytes()).hexdigest()}
        ))
        task = TaskState(task_id="TASK-R", requirement="x", worker="opencode")
        task.job_id = "JOB-R"
        task.execution_id = "EXEC-R"
        task.candidate_id = "OCAND-SAME"
        task.active_baseline_id = "BASE-R"
        task.changed_files = [changed]
        task.task_owned_changed_files = [changed]
        task.target_module = ["suc"]
        task.scope_authority = "JOB_CONTRACT"
        task.git_diff = "diff --git a/suc/A.java b/suc/A.java\n"
        task.test_required = True
        task.test_plan = {"mandatory": True}
        task.tester_attempt_history = [{
            "attempt_id": "TESTER-OLD", "test_status": "ERROR", "status": "FAILED"
        }]
        task.effective_policy = {
            "resolved_tester": {"runtime": "droid", "model": "tester", "timeout": 10},
            "resolved_reviewer": {"model": "gpt-5.6-sol", "reasoning_effort": "high"},
        }

        def tester_pass(current, *_args, **_kwargs):
            current.official_tester_invoked = True
            current.official_tester_execution = {"stdout": "TEST_RESULT: PASS"}
            return _apply_to_state(current, TesterResult("PASS", "ok", ["verified"]))

        with patch("manager.run_tester", side_effect=tester_pass):
            result = manager.run_control_recovery(task, tester_recovery=True)
        self.assertTrue(result.success)
        self.assertEqual(["OCAND-SAME"], reviewed)
        self.assertEqual("VERIFIED", task.verification_status)
        self.assertEqual("SUCCESS", task.status)
        self.assertEqual(0, task.product_semantic_retry_count)
        self.assertEqual(1, task.technical_execution_retry_count)


if __name__ == "__main__":
    unittest.main()
