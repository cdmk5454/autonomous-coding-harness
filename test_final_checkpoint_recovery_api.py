"""Durable TEST recovery through real service dispatch and canonical JobStore."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import test_final_blocker_lifecycle as lifecycle
from harness_service import HarnessService
from job_queue import JobStore, QueueError
from task_state import TaskState
from test_checkpoint import accepted
from job_contract import JobRequest, EXECUTION_CONTEXT_KEYS
from testplan_amendment import CONFIRMATION, apply_to_task
import gate_evidence
from project_profile import PROFILE_SCHEMA_FILE


class CheckpointRecoveryAPITests(unittest.TestCase):
    def setUp(self):
        fixture = lifecycle.FinalBlockerLifecycleTests(
            methodName="test_registration_failure_retains_pass_and_resumes_without_test")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.repo = fixture.fixture.repo
        self.task = fixture.task
        self.store = JobStore(fixture.fixture.root / "queue-store", repository=self.repo)
        # The lower-level fixture uses a short repository-only Job ID. Create a
        # normal JobStore contract before producing this test's canonical proof.
        request = JobRequest.from_mapping(self.repo.read_job(self.task.job_id)["request"], allowed_modules=["app"])
        jobs, _ = self.store.enqueue_batch("recovery-api", [request],
            execution_context=fixture.fixture.execution["execution_context"])
        job = jobs[0]
        execution = self.repo.materialize(job, context=fixture.fixture.execution["execution_context"],
            policy={"effective_policy_sha256": "policy"}, baseline_hash="base", execution_surface_hash="surface")
        self.task.job_id = job["job_id"]
        self.task.execution_id = execution["execution_id"]
        self.task.materialized_execution = execution
        self.task.test_plan_amendment = {}
        receipt = self.task.build_evidence["details"]["executions"][0]
        receipt.update(job_id=self.task.job_id, product_execution_id=self.task.execution_id)
        self.task.build = copy.deepcopy(self.task.build_evidence)
        gate_evidence.bind(self.task, "BUILD", gate_evidence.identity(self.task, fixture.fixture.workspace), "PASS")
        job.update(status="AWAITING_QA", revision=job["revision"]+1, candidate_id=self.task.candidate_id,
            candidate_manifest=str(fixture.fixture.root / "allocation.json"), last_result=dict(
                task_id=self.task.task_id, execution_id=self.task.execution_id,
                failure_stage="TEST", worktree_disposition="PRESERVED"))
        self.repo.write_job(job, expected_revision=job["revision"]-1)
        allocation = Path(job["candidate_manifest"])
        record = json.loads(allocation.read_text())
        record.update(job_id=self.task.job_id, execution_id=self.task.execution_id)
        allocation.write_text(json.dumps(record))
        amended = self.repo.amend_preserved_test_plan(self.task.job_id,
            expected_revision=job["revision"], request_id="api-plan", confirmation=CONFIRMATION,
            candidate_id=self.task.candidate_id, source_hashes=fixture.fixture.hashes,
            test_plan=request.test_plan, task=self.task, workspace=fixture.fixture.workspace)
        apply_to_task(self.task, amended["job"])
        with patch("reviewer_successor.prepare", side_effect=RuntimeError("crash before role admission")):
            with self.assertRaises(RuntimeError):
                fixture.run_manager()
        self.proof = accepted(self.task, fixture.fixture.workspace)
        self.task.test_status = "ERROR"
        self.task.test_checkpoint = {}
        self.task.test_receipts = []
        self.task.official_tester_invoked = False
        self.task.failure_stage = "CONTROL"
        self.task.failure_code = "CONTROL_EXECUTION_ERROR"
        self.task.worker_stderr = "ControlPathError: CONTROL_STATE_WRITE_FAILED: task_state"
        self.task.worktree_disposition = "PRESERVED"
        self.task.status = self.task.stage = "AWAITING_QA"
        self.task.external_frozen_integrity = True
        self.task.baseline_declaration_integrity = True
        self.task.save(fixture.root)
        queue = self.repo.read_queue()
        revision = queue["revision"]
        queue.update(blocked_by_job_id=self.task.job_id, running_job_id="", revision=revision+1)
        self.repo.write_queue(queue, expected_revision=revision)

    def result(self, proof=None):
        return dict(task_id=self.task.task_id, execution_id=self.task.execution_id,
            status="AWAITING_QA", stage="AWAITING_QA", success=False,
            review_status="REVIEW_UNAVAILABLE", failure_stage="REVIEW",
            failure_origin="TOOL_INFRA", recovery_kind="DURABLE_TEST_REVIEW_RECOVERY",
            worker_invoked=False, build_rebuilt=False, tester_retried=False,
            test_checkpoint=copy.deepcopy(self.proof if proof is None else proof))

    def test_queue_accepts_exact_canonical_checkpoint_without_worker_attempt(self):
        before = self.repo.read_job(self.task.job_id)
        updated, replayed = self.store.record_review_only_result(self.task.job_id,
            expected_revision=before["revision"], request_id="resume-durable-1", result=self.result())
        self.assertFalse(replayed)
        self.assertEqual(self.proof, updated["last_result"]["test_checkpoint"])
        self.assertEqual(before.get("attempts"), updated.get("attempts"))
        self.assertEqual(before.get("candidate_id"), updated.get("candidate_id"))
        self.assertEqual(before.get("task_ids"), updated.get("task_ids"))
        self.assertEqual(1, self.fixture.tester_calls)

    def test_queue_rejects_fabricated_checkpoint(self):
        before = self.repo.read_job(self.task.job_id)
        fabricated = copy.deepcopy(self.proof)
        fabricated["detail"]["test_summary"] = "fabricated pass"
        with self.assertRaisesRegex(QueueError, "REVIEW_ONLY_RETRY_NOT_ELIGIBLE"):
            self.store.record_review_only_result(self.task.job_id,
                expected_revision=before["revision"], request_id="fabricated", result=self.result(fabricated))
        self.assertEqual(before, self.repo.read_job(self.task.job_id))

    def test_fabricated_checkpoint_cannot_fall_back_to_legacy_review_admission(self):
        before = self.repo.read_job(self.task.job_id)
        revision = before["revision"]
        before["last_result"].update(failure_origin="TOOL_INFRA", failure_stage="REVIEW")
        before["revision"] += 1
        self.repo.write_job(before, expected_revision=revision)
        fabricated = copy.deepcopy(self.proof)
        fabricated["detail"]["test_summary"] = "fabricated pass"
        with self.assertRaises(QueueError):
            self.store.record_review_only_result(self.task.job_id,
                expected_revision=before["revision"], request_id="fabricated-legacy",
                result=self.result(fabricated))
        self.assertEqual(before, self.repo.read_job(self.task.job_id))

    def test_queue_rejects_exact_checkpoint_from_prior_contract_revision(self):
        before = self.repo.read_job(self.task.job_id)
        amended = self.repo.amend_preserved_test_plan(self.task.job_id,
            expected_revision=before["revision"], request_id="second-plan", confirmation=CONFIRMATION,
            candidate_id=self.task.candidate_id, source_hashes=self.fixture.fixture.hashes,
            test_plan=before["request"]["test_plan"], task=self.task,
            workspace=self.fixture.fixture.workspace)["job"]
        self.assertNotEqual(self.proof["detail"]["contract_revision"], amended["contract_revision"])
        with self.assertRaises(QueueError):
            self.store.record_review_only_result(self.task.job_id,
                expected_revision=amended["revision"], request_id="stale-contract",
                result=self.result())
        self.assertEqual(amended, self.repo.read_job(self.task.job_id))

    def test_service_restores_stale_projection_and_dispatches_review_only(self):
        fixture = self.fixture
        service = HarnessService(fixture.fixture.workspace, self.task.profile_dir,
            task_root=fixture.root, snapshot_root=fixture.fixture.root / "service-control" / "snapshots",
            manager_factory=lambda **_kwargs: fixture.manager)
        service.control_root.mkdir(parents=True)
        (Path(self.task.profile_dir) / "ui_map" / "index.md").write_text("# Fixture UI map\n")
        policy = fixture.fixture.root / "policy"
        policy.mkdir()
        (policy / "AGENTS.md").write_text("# Disposable recovery policy\n")
        (policy / "hooks").mkdir()
        (policy / "skills" / "review").mkdir(parents=True)
        (policy / "hooks" / "validation.md").write_text("# Fixture validation\n")
        (policy / "skills" / "review" / "SKILL.md").write_text("# Fixture review\n")
        service._policy_files = ((Path(PROFILE_SCHEMA_FILE), "profile_schema"),
                                (policy / "AGENTS.md", "common_agents"))
        # Real Git trees and baseline loader; all paths belong to this fixture.
        workspace = fixture.fixture.workspace
        for args in (("init", "-q"), ("config", "user.name", "Fixture"),
                     ("config", "user.email", "fixture@example.invalid"),
                     ("add", "app"), ("commit", "-qm", "fixture")):
            subprocess.run(["git", *args], cwd=workspace, capture_output=True, check=True)
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=workspace,
            capture_output=True, text=True, check=True).stdout.strip()
        tree = subprocess.run(["git", "rev-parse", "HEAD^{tree}"], cwd=workspace,
            capture_output=True, text=True, check=True).stdout.strip()
        baseline_root = service._cumulative_manager().root / "recovery-fixture"
        baseline_root.mkdir(parents=True)
        (baseline_root / "snapshot.patch").write_bytes(b"")
        manifest = baseline_root / "manifest.json"
        manifest.write_text(json.dumps(dict(schema_version=1, kind="PRE_JOB_SNAPSHOT", repos=[dict(
            repo=str(workspace), module="app", head=head, worktree_tree=tree, index_tree=tree,
            binary_patch=dict(path="snapshot.patch", sha256=hashlib.sha256(b"").hexdigest()))])))
        self.task.pre_job_snapshot_manifest = str(manifest)
        self.task.save(fixture.root)
        profile, modules = service._load_profile()
        job = self.repo.read_job(self.task.job_id)
        configuration = service._configuration(profile, modules)
        job["execution_context"] = {key: configuration[key] for key in EXECUTION_CONTEXT_KEYS}
        job["reviewer_recovery_request_id"] = "explicit-recovery"
        fixture.manager.shutdown = lambda: None
        service._lease_held = True
        before = copy.deepcopy(self.task.build_evidence)
        with patch.object(fixture.manager, "run_control_recovery", side_effect=AssertionError("BUILD forbidden")), \
             patch.object(fixture.manager, "run_post_worker_recovery", side_effect=AssertionError("WORK forbidden")), \
             patch("manager.run_tester", side_effect=AssertionError("TEST forbidden")):
            outcome = service.retry_review_only(job)
        restored = TaskState.load(self.task.task_id, fixture.root)
        self.assertEqual("DURABLE_TEST_REVIEW_RECOVERY", outcome["recovery_kind"])
        self.assertFalse(outcome["worker_invoked"])
        self.assertFalse(outcome["build_rebuilt"])
        self.assertFalse(outcome["tester_retried"])
        self.assertEqual("PASS", restored.test_status)
        self.assertEqual("REVIEW_PASS", restored.review_status)
        self.assertEqual(self.proof, restored.test_checkpoint)
        self.assertEqual(before, restored.build_evidence)
        self.assertEqual("explicit-recovery", restored.reviewer_recovery_request_id)
        self.assertEqual(1, fixture.tester_calls)


if __name__ == "__main__":
    unittest.main()
