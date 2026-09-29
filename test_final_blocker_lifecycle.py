"""Disposable Manager lifecycle with real test subprocess receipts and SQLite."""
import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_final_testplan_amendment as fixtures
import gate_evidence
from manager import Manager
from reviewer import ReviewResult
from task_state import TaskState
from test_plan import derive_test_plan
from job_contract import JobRequest
from tester import execute_test_receipts, arbitrate_tester_result, TesterResult, _apply_to_state
from testplan_amendment import apply_to_task
from test_checkpoint import accepted
from control_repository import RepositoryError


class FinalBlockerLifecycleTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.TestPlanAmendmentTests(methodName="test_exact_replay")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        task = fixture.task
        task.control_repository_path = str(fixture.repo.path)
        task.materialized_execution = copy.deepcopy(fixture.execution)
        task.target_resources = ["app/a.py"]
        task.git_diff = "diff --git a/app/a.py b/app/a.py\n--- a/app/a.py\n+++ b/app/a.py\n@@ -1 +1 @@\n-value = 0\n+value = 1\n"
        task.task_owned_changed_files = ["app/a.py"]
        task.build = copy.deepcopy(task.build_evidence)
        task.effective_policy = dict(resolved_tester=dict(runtime="droid", model="fixture"),
            resolved_reviewer=dict(model="gpt-5.6-sol", reasoning_effort="high"))
        task.test_plan = derive_test_plan(JobRequest.from_mapping(
            fixture.repo.read_job(task.job_id)["request"], allowed_modules=["app"]), fixture.workspace)
        gate_evidence.bind(task, "BUILD", gate_evidence.identity(task, fixture.workspace), "PASS")
        self.build_proof = copy.deepcopy(task.gate_evidence["BUILD"])
        plan = {**fixture.kwargs["test_plan"],
            "command": '"' + Path(sys.executable).as_posix() + '" -B app/test_a.py'}
        job = fixture.amend(test_plan=plan)["job"]
        apply_to_task(task, job)
        self.task = task
        self.root = fixture.root / "tasks"
        task.save(self.root)
        manager = object.__new__(Manager)
        manager.working_dir = str(fixture.workspace)
        manager.task_root = self.root
        manager.profile = fixture.profile
        manager.policy_root = None
        manager.max_retry = 1
        manager._initial_baseline = object()
        manager._prepare_reviewer_effort = lambda _task: None
        manager.collector = SimpleNamespace(assess_commit_safety=lambda *_args: (True, [], fixture.hashes))
        manager.reviewer = SimpleNamespace(review=lambda **_kwargs:
            ReviewResult(True, status="REVIEW_PASS", reviewed_files=["app/a.py"]))
        self.manager = manager
        self.tester_calls = 0

    def invoke_tester(self, task, workspace, **_kwargs):
        self.tester_calls += 1
        receipts = execute_test_receipts(task, workspace)
        task.official_tester_invoked = True
        task.official_tester_execution = dict(runtime="fixture", status="SUCCEEDED", stdout="TEST_RESULT: PASS")
        return _apply_to_state(task, arbitrate_tester_result(task,
            TesterResult("PASS", "real disposable assertion", evidence=["app/test_a.py executed"]), receipts))

    def run_manager(self, task=None):
        with patch("manager.run_tester", side_effect=self.invoke_tester):
            return self.manager.run_amended_test_plan(task or self.task)

    def test_amend_test_checkpoint_review_verify_complete(self):
        outcome = self.run_manager()
        self.assertTrue(outcome.success, (self.task.failure_code, self.task.failure_reason))
        self.assertEqual("VERIFIED", self.task.verification_status)
        self.assertEqual(1, self.tester_calls)
        self.assertEqual(self.build_proof, self.task.gate_evidence["BUILD"])
        self.assertEqual(self.fixture.execution, self.fixture.repo.execution(self.task.execution_id))
        self.assertNotEqual(self.task.execution_id, self.task.reviewer_execution["execution_id"])
        self.assertEqual("REVIEWER", self.task.reviewer_execution["role"])
        self.assertEqual("PASS", accepted(self.task, self.fixture.workspace)["disposition"])
        self.assertEqual(0, self.task.product_semantic_retry_count)
        self.assertTrue(self.task.test_receipts)
        self.assertEqual(0, self.task.test_receipts[0]["exit_code"])

    def test_registration_failure_retains_pass_and_resumes_without_test(self):
        with patch("reviewer_successor.prepare", side_effect=RuntimeError("registration failed")):
            with self.assertRaisesRegex(RuntimeError, "registration failed"):
                self.run_manager()
        proof = accepted(self.task, self.fixture.workspace)
        self.assertIsNotNone(proof)
        self.assertEqual("PASS", self.task.test_status)
        result = self.run_manager()
        self.assertTrue(result.success, self.task.failure_reason)
        self.assertEqual(1, self.tester_calls)
        self.assertEqual(proof, self.task.test_checkpoint)

    def test_crash_after_canonical_checkpoint_restores_without_test(self):
        original_save = self.task.save
        crashed = False

        def fail_once(root):
            nonlocal crashed
            if self.task.test_checkpoint and not crashed:
                crashed = True
                raise RuntimeError("projection crash")
            return original_save(root)

        with patch.object(self.task, "save", side_effect=fail_once):
            failed = self.run_manager()
        self.assertFalse(failed.success)
        restored = TaskState.load(self.task.task_id, self.root)
        result = self.run_manager(restored)
        self.assertTrue(result.success, restored.failure_reason)
        self.assertEqual(1, self.tester_calls)
        self.assertEqual("PASS", restored.test_status)

    def test_test_side_mutation_cannot_checkpoint(self):
        original = self.invoke_tester

        def mutate(task, workspace, **kwargs):
            result = original(task, workspace, **kwargs)
            (self.fixture.workspace / "app/test_a.py").write_text("assert False\n")
            return result

        with patch("manager.run_tester", side_effect=mutate):
            result = self.manager.run_amended_test_plan(self.task)
        self.assertFalse(result.success)
        self.assertEqual("ERROR", self.task.test_status)
        self.assertEqual({}, self.task.test_checkpoint)
        self.assertEqual({}, self.task.reviewer_execution)

    def test_unscoped_test_side_mutation_cannot_checkpoint(self):
        def mutate(task, workspace, **kwargs):
            result = self.invoke_tester(task, workspace, **kwargs)
            (self.fixture.workspace / "app/unscoped.py").write_text("unexpected = True\n")
            return result

        with patch("manager.run_tester", side_effect=mutate):
            result = self.manager.run_amended_test_plan(self.task)
        self.assertFalse(result.success)
        self.assertEqual("TEST_CHECKPOINT_SOURCE_CHANGED", self.task.test_summary)
        self.assertEqual({}, self.task.test_checkpoint)
        self.assertEqual({}, self.task.reviewer_execution)

    def test_semantic_review_failure_preserves_test_and_requests_product_fix(self):
        build = copy.deepcopy(self.task.build_evidence)
        source = (self.fixture.workspace / "app/a.py").read_bytes()
        candidate_id = self.task.candidate_id
        self.manager.reviewer.review = lambda **_kwargs: ReviewResult(
            False, status="REVIEW_FAIL", reason="fixture semantic violation",
            reviewed_files=["app/a.py"])
        result = self.run_manager()
        self.assertFalse(result.success)
        self.assertEqual("AWAITING_QA", self.task.status)
        self.assertEqual("RETRYABLE", self.task.fix_scope)
        self.assertEqual("PRODUCT_SEMANTIC_RETRY", self.task.retry_domain)
        self.assertEqual("PASS", self.task.test_status)
        self.assertEqual(self.task.test_checkpoint, accepted(self.task, self.fixture.workspace))
        self.assertEqual(1, self.tester_calls)
        self.assertEqual(candidate_id, self.task.candidate_id)
        self.assertEqual(source, (self.fixture.workspace / "app/a.py").read_bytes())
        self.assertEqual(build, self.task.build_evidence)
        self.assertEqual(self.fixture.execution, self.fixture.repo.execution(self.task.execution_id))
        with self.fixture.repo.hold():
            roles = self.fixture.repo._connection.execute("SELECT role FROM attempts").fetchall()
        self.assertEqual(["REVIEWER"], [row["role"] for row in roles])

    def test_explicit_infrastructure_retry_uses_fresh_role_execution_and_session(self):
        build = copy.deepcopy(self.task.build_evidence)
        candidate_id = self.task.candidate_id
        self.manager.reviewer.review = lambda **_kwargs: ReviewResult(
            False, status="REVIEW_ERROR", reason="fixture provider unavailable",
            reviewed_files=["app/a.py"])
        first = self.run_manager()
        self.assertFalse(first.success)
        first_role = self.task.reviewer_execution["execution_id"]
        proof = accepted(self.task, self.fixture.workspace)
        self.assertEqual("PASS", self.task.test_status)
        self.task.reviewer_recovery_request_id = "operator-retry-1"
        bindings = []

        def review(**kwargs):
            task = kwargs["task"]
            role_id = task.reviewer_execution["execution_id"]
            bindings.append(self.fixture.repo.bind_native_session(role_id,
                runtime="fixture", native_session_id="reviewer-successor-native",
                adapter_revision="fixture-v1", runtime_version="fixture-v1", role="REVIEWER"))
            with self.assertRaisesRegex(RepositoryError, "TERMINAL_EXECUTION_IMMUTABLE"):
                self.fixture.repo.bind_native_session(task.execution_id,
                    runtime="fixture", native_session_id="product-must-stay-terminal",
                    adapter_revision="fixture-v1", runtime_version="fixture-v1", role="REVIEWER")
            return ReviewResult(True, status="REVIEW_PASS", reviewed_files=["app/a.py"])

        self.manager.reviewer.review = review
        second = self.run_manager()
        self.assertTrue(second.success, self.task.failure_reason)
        self.assertNotEqual(first_role, self.task.reviewer_execution["execution_id"])
        self.assertEqual(self.task.reviewer_execution["execution_id"], bindings[0]["execution_id"])
        self.assertEqual("REVIEWER", bindings[0]["role"])
        self.assertEqual(proof, self.task.test_checkpoint)
        self.assertEqual(1, self.tester_calls)
        self.assertEqual(0, self.task.product_semantic_retry_count)
        self.assertEqual(candidate_id, self.task.candidate_id)
        self.assertEqual(build, self.task.build_evidence)
        self.assertEqual(self.fixture.execution, self.fixture.repo.execution(self.task.execution_id))

if __name__ == "__main__":
    unittest.main()
