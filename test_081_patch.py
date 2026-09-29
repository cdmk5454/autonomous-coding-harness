from __future__ import annotations

import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from build_test_runner import BuildTestResult
from git_collector import GitDiffResult
from harness_service import HarnessService
from job_queue import (
    BATCH_FINAL_COMMIT,
    CANCELLED,
    FAILED_FINAL,
    JobStore,
    PREDECESSOR_SUCCESS_GATE_FAILED,
    QUEUED,
    SKIPPED,
)
from manager import (
    Manager,
    ManagerResult,
    NO_MEANINGFUL_SOURCE_CHANGE,
    WORKER_PATCH_CONTEXT_MISMATCH,
    TECHNICAL_RECOVERY_MAX_ATTEMPTS,
    failure_fingerprint,
)
from project_profile import load_project_profile
from reviewer import ReviewResult, REVIEW_PASS
from task_state import TaskState
from worker import WorkerResult, build_prompt

from test_batch_final_commit import _active_fields, _context, _request, _success


PROFILE = load_project_profile(Path(r"D:\agents\profiles\e4u"), Path(r"D:\inha_dev"))
SP0021 = "suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml"
U02 = "suc/src/main/java/suc/sp/query/SsucSp0022Qry.xml"


class _Worker:
    def __init__(self, stderr: str = ""):
        self.calls = 0
        self.handoffs: list[str] = []
        self.stderr = stderr

    def execute(self, requirement, working_dir, task=None):
        self.calls += 1
        self.handoffs.append(task.handoff_context if task else "")
        return WorkerResult(True, 0, "implemented", self.stderr, [])


class _Collector:
    def __init__(self, files: list[str], diff: str = ""):
        self.files = list(files)
        self.diff = diff
        self.rollbacks = 0
        self.selected: list[list[str]] = []

    def capture_baseline(self, task=None):
        return object(), ""

    def verify_baseline_current(self, baseline):
        return True, ""

    def collect(self, task=None, baseline=None):
        result = GitDiffResult(True, list(self.files), diff=self.diff)
        if task is not None:
            task.changed_files = list(self.files)
            task.git_diff = self.diff
        return result

    def collect_selected(self, task, baseline, selected_files):
        selected = list(selected_files)
        self.selected.append(selected)
        task.changed_files = selected
        task.task_owned_changed_files = selected
        task.git_diff = self.diff if selected else ""
        return GitDiffResult(True, selected, diff=task.git_diff)

    def repair_eol_to_index(self, task, baseline):
        return SimpleNamespace(success=True, repaired_files=[], error="")

    def assess_commit_safety(self, task, baseline):
        return True, [], {path: "a" * 64 for path in task.changed_files}

    def rollback(self, task=None, baseline=None):
        self.rollbacks += 1
        if task is not None:
            task.is_rolled_back = True
        return True, "scoped rollback"


class _Runner:
    def __init__(self):
        self.calls = 0

    def run(self, changed_files=None, target_modules=None):
        self.calls += 1
        return BuildTestResult(True, "PASS", "PASS")


class _Reviewer:
    def __init__(self):
        self.calls = 0
        self.kwargs: list[dict] = []

    def review(self, task=None, **kwargs):
        self.calls += 1
        self.kwargs.append(kwargs)
        files = list(kwargs.get("changed_files") or [])
        result = ReviewResult(
            True,
            "ok",
            status=REVIEW_PASS,
            reviewed_files=files,
            coverage={"total_files": files, "reviewed_files": files, "missing_files": []},
        )
        task.review_status = REVIEW_PASS
        task.review_result = "ok"
        task.review_coverage = dict(result.coverage)
        return result


def _diff(path: str = "src/main/java/suc/sp/query/SsucSp0021Qry.xml") -> str:
    return (
        "--- [module: suc] ---\n"
        f"diff --git a/{path} b/{path}\n"
        f"--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n-old\n+new"
    )


class Patch081Test(unittest.TestCase):
    def _manager(self, root: Path, collector: _Collector, worker: _Worker):
        manager = Manager(
            str(root), "", max_retry=2, worker_type="codex",
            codex_model="gpt-5.6-sol", profile=PROFILE,
            task_root=root / "tasks",
        )
        manager.collector = collector
        manager.build_test_runner = _Runner()
        manager.reviewer = _Reviewer()
        manager._get_worker = lambda task: (worker, task.retry_worker or "codex")
        return manager

    @staticmethod
    def _task(root: Path):
        return TaskState(
            task_id="TASK-081-TEST", requirement="SP0021 application source 수정",
            task_mode="MODIFICATION", worker="codex", codex_model="gpt-5.6-sol",
            working_dir=str(root), target_module=["suc"],
            target_resources=["suc/src/main/java/suc/sp"],
            inherited_batch_delta_files=[U02], pre_job_snapshot_id="SNAP-081",
        )

    def test_01_task_plan_failure_does_not_authorize_source_abort(self):
        prompt = build_prompt(
            "SP0021 구현", ["suc"], "MODIFICATION", profile=PROFILE, role="codex"
        )
        self.assertIn("계획/검증 문서 패치가 실패해도", prompt)
        self.assertIn("애플리케이션 소스 구현을 계속", prompt)

    def test_02_planning_artifact_only_is_no_meaningful_source_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); worker = _Worker("apply_patch verification failed: task_plan.md")
            manager = self._manager(root, _Collector(["suc/task_plan.md"], _diff("task_plan.md")), worker)
            task = self._task(root)
            result = manager._run_pipeline(task)
            self.assertFalse(result.success)
            self.assertEqual(WORKER_PATCH_CONTEXT_MISMATCH, task.failure_code)
            self.assertEqual(NO_MEANINGFUL_SOURCE_CHANGE, task.no_diff_gate["secondary_gate"])
            self.assertTrue(task.planning_artifact_errors)
            manager.shutdown()

    def test_03_zero_source_diff_skips_build_and_review(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            task = self._task(root); manager._run_pipeline(task)
            self.assertEqual(0, manager.build_test_runner.calls)
            self.assertEqual(0, manager.reviewer.calls)
            manager.shutdown()

    def test_04_inherited_u02_is_not_u03_changed_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            task = self._task(root); manager._run_pipeline(task)
            self.assertEqual([], task.changed_files)
            self.assertEqual([U02], task.inherited_batch_delta_files)
            manager.shutdown()

    def test_05_inherited_u02_is_not_missing_reviewed_file(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([SP0021], _diff()), _Worker())
            task = self._task(root); result = manager._run_pipeline(task)
            self.assertTrue(result.success)
            self.assertNotIn(U02, task.review_coverage.get("missing_files", []))
            manager.shutdown()

    def test_06_only_actual_sp0021_change_is_reviewed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([SP0021], _diff()), _Worker())
            task = self._task(root); manager._run_pipeline(task)
            self.assertEqual([SP0021], manager.reviewer.kwargs[0]["changed_files"])
            manager.shutdown()

    def test_07_scoped_rollback_preserves_inherited_delta(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); collector = _Collector([]); manager = self._manager(root, collector, _Worker())
            task = self._task(root); manager.run_with_retry(task)
            self.assertGreaterEqual(collector.rollbacks, 1)
            self.assertEqual([U02], task.preserved_predecessor_delta_files)
            manager.shutdown()

    def test_08_failure_fingerprint_is_stable(self):
        task = self._task(Path(".")); task.failure_stage = "SOURCE_DELTA_GATE"
        task.failure_code = NO_MEANINGFUL_SOURCE_CHANGE; task.failure_reason = "  EMPTY   source "
        one = failure_fingerprint(task)[0]; two = failure_fingerprint(task)[0]
        self.assertEqual(one, two)

    def test_09_repeated_fingerprint_changes_prompt_strategy(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); worker = _Worker(); manager = self._manager(root, _Collector([]), worker)
            manager._fallback_route = lambda task: ("droid", "custom:GLM-5.2-[Z.AI-Coding]-0", "available")
            task = self._task(root); manager.run_with_retry(task)
            strategies = [item["retry_strategy"] for item in task.attempt_history]
            self.assertIn("DIRECT_IMPLEMENTATION", strategies)
            self.assertTrue(any("target application source" in item for item in worker.handoffs))
            manager.shutdown()

    def test_10_repeated_fingerprint_reroutes_worker_and_model(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            manager._fallback_route = lambda task: ("droid", "fallback-model", "available")
            task = self._task(root); manager.run_with_retry(task)
            self.assertEqual("droid", task.retry_worker)
            self.assertEqual("fallback-model", task.retry_model)
            self.assertEqual("REPEATED_FAILURE_FINGERPRINT", task.reroute_reason)
            manager.shutdown()

    def test_11_missing_fallback_stops_safely(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            manager._fallback_route = lambda task: ("", "", "unavailable")
            task = self._task(root); manager.run_with_retry(task)
            self.assertEqual("SAFE_FALLBACK_UNAVAILABLE", task.user_intervention_reason)
            self.assertEqual("CLEAN_ROLLBACK", task.worktree_disposition)
            manager.shutdown()

    def test_12_remediation_cycle_is_bounded_to_one(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            manager._fallback_route = lambda task: ("droid", "fallback-model", "available")
            task = self._task(root); manager.run_with_retry(task)
            self.assertEqual(1, task.remediation_cycle)
            manager.shutdown()

    def test_13_technical_recovery_has_no_infinite_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); worker = _Worker(); manager = self._manager(root, _Collector([]), worker)
            manager._fallback_route = lambda task: ("droid", "fallback-model", "available")
            task = self._task(root); manager.run_with_retry(task)
            self.assertLessEqual(worker.calls, TECHNICAL_RECOVERY_MAX_ATTEMPTS)
            self.assertEqual("TECHNICAL_RECOVERY_EXHAUSTED", task.user_intervention_reason)
            manager.shutdown()

    def test_14_business_policy_failure_never_enters_remediation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); manager = self._manager(root, _Collector([]), _Worker())
            task = self._task(root)
            def policy_failure(_task):
                _task.failure_stage = "REVIEW"; _task.failure_code = "PII_POLICY_DECISION_REQUIRED"
                _task.failure_reason = "human policy decision"; _task.failure_type = "policy"
                _task.fix_scope = "NON_RETRYABLE"; _task.verification_status = "PARTIAL"
                return ManagerResult(False, message="policy")
            manager._run_pipeline = policy_failure
            manager.run_with_retry(task)
            self.assertEqual(0, task.remediation_cycle)
            self.assertEqual("", task.retry_worker)
            manager.shutdown()

    def test_15_new_batch_sequence_one_can_clear_safe_stale_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"; root.mkdir(); store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            old, _ = store.enqueue_batch("old-081", [_request("old")], execution_context=_context())
            new, _ = store.enqueue_batch("new-081", [_request("new")], execution_context=_context())
            with store._mutex.hold():
                old_job = store._read_job(old[0]["job_id"]); old_job["status"] = SKIPPED; store._write_job(old_job)
                queue = store._read_queue(); store._set_gate(queue, old_job["job_id"], PREDECESSOR_SUCCESS_GATE_FAILED); store._write_queue(queue)
            self.assertEqual(1, new[0]["sequence"])
            self.assertTrue(store.clear_stale_terminal_predecessor_gate_for_batch_head())
            self.assertEqual("", store.queue_snapshot()["blocked_by_job_id"])

    def test_16_a07_cannot_claim_before_u03_verified_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"; root.mkdir(); store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            jobs, _ = store.enqueue_batch("gate-081", [_request("u03"), _request("a07")], execution_context=_context())
            first = store.claim_next("owner"); store.mark_task_started(first["job_id"], "owner", "TASK-U03")
            failed = _success("TASK-U03", "app/same.txt"); failed.update({"success": False, "status": "FAILED", "verification_status": "FAILED", "failure_type": "technical", "fix_scope": "NON_RETRYABLE", "worktree_disposition": "CLEAN_ROLLBACK"})
            store.record_result(jobs[0]["job_id"], "owner", failed)
            self.assertIsNone(store.claim_next("owner"))
            self.assertEqual(FAILED_FINAL, store.queue_snapshot()["gate_reason"])

    def test_17_get_job_outcome_keeps_old_and_new_fields(self):
        task = self._task(Path(".")); task.task_owned_changed_files = [SP0021]; task.changed_files = [SP0021]
        task.failure_fingerprint = "f" * 64; task.retry_strategy = "DIRECT_IMPLEMENTATION"
        outcome = HarnessService._safe_outcome(task, ManagerResult(False), Path("task.json"), None)
        self.assertIn("changed_files", outcome)
        self.assertEqual([SP0021], outcome["task_owned_changed_files"])
        self.assertEqual([U02], outcome["inherited_batch_delta_files"])

    def test_18_active_batch_inherited_delta_remains_control_valid_context(self):
        fields = _active_fields()
        self.assertEqual(BATCH_FINAL_COMMIT, fields["commit_policy"])
        self.assertEqual([], fields["unexpected_runtime_dirty_files"])
        self.assertTrue(fields["scoped_rollback_supported"])

    def test_19_eol_semantic_clean_contract_remains_available(self):
        from cumulative_policy import CumulativePolicyManager
        self.assertTrue(hasattr(CumulativePolicyManager, "_tracked_path_semantically_dirty"))
        self.assertIn("hash-object", CumulativePolicyManager._tracked_path_semantically_dirty.__doc__ or "Classify")

    def test_20_autonomous_success_contract_stays_strict(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"; root.mkdir(); store = JobStore(root)
            store.activate_cumulative_policy(1, _active_fields())
            jobs, _ = store.enqueue_batch("strict-081", [_request("u03")], execution_context=_context())
            claimed = store.claim_next("owner"); store.mark_task_started(claimed["job_id"], "owner", "TASK-U03")
            invalid = _success("TASK-U03", "app/same.txt"); invalid["review_status"] = "REVIEW_ERROR"
            result = store.record_result(jobs[0]["job_id"], "owner", invalid)
            self.assertNotEqual("SUCCEEDED", result["status"])


if __name__ == "__main__":
    unittest.main()
