"""Disposable candidate/SQLite tests for the guarded TestPlan-only operation."""
import copy
import hashlib
import json
from pathlib import Path
import unittest

import harness_temp as tempfile
from control_repository import ControlRepository, RepositoryError
from job_contract import JobRequest
from job_queue import JobStore
from production_isolation import inventory
from project_profile import load_project_profile
from task_state import TaskState
from test_job_control import execution_context
from testplan_amendment import CONFIRMATION, apply_to_task


class TestPlanAmendmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "candidate"
        (self.workspace / "app").mkdir(parents=True)
        (self.workspace / "app/a.py").write_text("value = 1\n")
        (self.workspace / "app/test_a.py").write_text("assert True\n")
        profile = self.root / "fixture"
        (profile / "ui_map").mkdir(parents=True)
        for name in ("rules.md", "analysis.md"):
            (profile / name).write_text("# Disposable fixture\n")
        manifest = dict(schema_version=1, id="fixture", display_name="Fixture",
            workspace_roots=[str(self.workspace)],
            context=dict(project_rules="rules.md", analysis="analysis.md", ui_map="ui_map"),
            modules=[dict(name="app", path="app", kind="backend", protected=False)],
            rules=dict(always=[], backend=[], frontend=[], sql=[], architecture=[], egov=[]),
            area_patterns=dict(backend=[], frontend=[], sql=[], architecture=[]),
            protected_paths=[], local_dev_config_excludes=[])
        (profile / "project.json").write_text(json.dumps(manifest))
        self.profile = load_project_profile(profile, self.workspace)
        request = JobRequest.from_mapping(dict(client_job_id="A", requirement="Fix value",
            target_modules=["app"], target_resources=["app/a.py"],
            test_plan=dict(required_tests=["app/test_a.py"], command="python app/test_a.py", mandatory=True)),
            allowed_modules=["app"]).to_dict()
        self.repo = ControlRepository(self.root / "control.sqlite3", create=True)
        self.repo.bootstrap(JobStore._empty_queue(), {}, {})
        job = dict(job_id="JOB-A", status="QUEUED", revision=1, contract_revision=1,
                   request=request, owner_id="", depends_on=[], current_requirement="Fix value",
                   candidate_id="CAND-A", candidate_manifest=str(self.root / "allocation.json"))
        self.repo.write_job(job, expected_revision=0)
        self.execution = self.repo.materialize(job, context=execution_context(),
            policy={"effective_policy_sha256": "policy"}, baseline_hash="base", execution_surface_hash="surface")
        task = TaskState(task_id="TASK-A", requirement="Fix value", worker="codex")
        task.job_id = "JOB-A"
        task.execution_id = self.execution["execution_id"]
        task.candidate_id = "CAND-A"
        task.working_dir = str(self.workspace)
        task.profile_dir = str(profile)
        task.target_module = ["app"]
        task.changed_files = ["app/a.py"]
        task.active_baseline_id = "base"
        self.hashes = {"app/a.py": hashlib.sha256((self.workspace / "app/a.py").read_bytes()).hexdigest()}
        log = self.root / "build.log"
        log.write_bytes(b"accepted build\n")
        receipt = dict(execution_id="BUILD-A", product_execution_id=task.execution_id,
            candidate_id=task.candidate_id, job_id=task.job_id, task_id=task.task_id,
            module="app", cwd=str(self.workspace), argv=["build"], command_source="profile",
            started_at="start", finished_at="finish", exit_code=0, status="PASS",
            log_path=str(log), log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
            baseline_id="base", checkpoint_id="", restored_checkpoint_id="", restored_evidence_epoch="")
        task.build_evidence = dict(success=True, status="PASS", details=dict(
            executions=[receipt], source_sha256=self.hashes))
        self.task = task
        job.update(status="AWAITING_QA", revision=2, last_result=dict(task_id=task.task_id,
            execution_id=task.execution_id, worktree_disposition="PRESERVED", failure_stage="TEST"))
        self.repo.write_job(job, expected_revision=1)
        (self.root / "allocation.json").write_text(json.dumps(dict(state="CLOSED",
            job_id=job["job_id"], execution_id=task.execution_id, candidate_id=task.candidate_id,
            candidate=str(self.workspace))))
        self.write_cleanup()
        self.kwargs = dict(expected_revision=2, request_id="amend-1", confirmation=CONFIRMATION,
            candidate_id="CAND-A", source_hashes=self.hashes,
            test_plan=dict(required_tests=["app/test_a.py"], command="python -B app/test_a.py", mandatory=True),
            task=task, workspace=self.workspace)

    def write_cleanup(self, **updates):
        cleanup = dict(state="QUIESCED", quiesced=True, imported=True,
            host_input=inventory(self.workspace, profile=self.profile, preserve=["app/a.py", "app/test_a.py"]))
        cleanup.update(updates)
        (self.root / "cleanup.json").write_text(json.dumps(cleanup))

    def amend(self, **overrides):
        return self.repo.amend_preserved_test_plan("JOB-A", **{**self.kwargs, **overrides})

    def test_amend_preserves_candidate_build_execution_and_qa(self):
        old = self.repo.read_job("JOB-A")
        build = copy.deepcopy(self.task.build_evidence)
        result = self.amend()
        job = result["job"]
        self.assertFalse(result["dispatched"])
        self.assertEqual("AWAITING_QA", job["status"])
        self.assertEqual(self.execution, self.repo.execution(self.task.execution_id))
        self.assertEqual(old["request"], job["test_plan_amendment"]["prior_request"])
        self.assertEqual(2, job["contract_revision"])
        self.assertEqual(3, job["revision"])
        for key in old["request"]:
            if key != "test_plan":
                self.assertEqual(old["request"][key], job["request"][key])
        self.assertTrue(apply_to_task(self.task, job))
        self.assertEqual(build, self.task.build_evidence)
        self.assertEqual(self.execution["execution_id"], self.task.execution_id)

    def test_exact_replay(self):
        first = self.amend()
        self.assertTrue(self.amend()["replayed"])
        self.assertEqual(first["job"], self.repo.read_job("JOB-A"))

    def test_conflicting_replay(self):
        self.amend()
        with self.assertRaisesRegex(RepositoryError, "IDEMPOTENCY_CONFLICT"):
            self.amend(source_hashes={"app/a.py": "wrong"})

    def test_stale_revision(self):
        with self.assertRaisesRegex(RepositoryError, "REVISION_CONFLICT"):
            self.amend(expected_revision=1)

    def test_confirmation(self):
        with self.assertRaisesRegex(RepositoryError, "CONFIRMATION_REQUIRED"):
            self.amend(confirmation="yes")

    def test_candidate_mismatch(self):
        with self.assertRaisesRegex(RepositoryError, "CANDIDATE_MISMATCH"):
            self.amend(candidate_id="other")

    def test_source_hash_mismatch(self):
        with self.assertRaisesRegex(RepositoryError, "SOURCE_CHANGED"):
            self.amend(source_hashes={"app/a.py": "wrong"})

    def test_source_mutation(self):
        (self.workspace / "app/a.py").write_text("changed")
        with self.assertRaisesRegex(RepositoryError, "CONTENT_CHANGED"):
            self.amend()

    def test_custody_not_closed(self):
        path = self.root / "allocation.json"
        record = json.loads(path.read_text())
        record["state"] = "RUNNING"
        path.write_text(json.dumps(record))
        with self.assertRaisesRegex(RepositoryError, "CUSTODY_INVALID"):
            self.amend()

    def test_custody_not_quiesced(self):
        self.write_cleanup(quiesced=False)
        with self.assertRaisesRegex(RepositoryError, "CUSTODY_INVALID"):
            self.amend()

    def test_build_log_mutation(self):
        (self.root / "build.log").write_text("changed")
        with self.assertRaisesRegex(RepositoryError, "BUILD_LOG_HASH_MISMATCH"):
            self.amend()

    def test_build_receipt_bindings(self):
        receipt = self.task.build_evidence["details"]["executions"][0]
        for key in ("candidate_id", "product_execution_id", "job_id", "task_id", "baseline_id", "restored_evidence_epoch"):
            with self.subTest(key=key):
                old = receipt[key]
                receipt[key] = "other"
                with self.assertRaises(RepositoryError):
                    self.amend()
                receipt[key] = old

    def test_duplicate_build_receipts(self):
        receipts = self.task.build_evidence["details"]["executions"]
        receipts.append(copy.deepcopy(receipts[0]))
        with self.assertRaisesRegex(RepositoryError, "BUILD_BINDING_INVALID"):
            self.amend()

    def test_forbidden_contract_fields(self):
        for key in ("requirement", "target_modules", "target_resources", "depends_on", "worker", "model", "risk", "policy_overlays", "build"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.amend(test_plan={**self.kwargs["test_plan"], key: "forbidden"})

    def test_command_must_bind_required_tests(self):
        with self.assertRaisesRegex(RepositoryError, "COMMAND_UNBOUND"):
            self.amend(test_plan={**self.kwargs["test_plan"], "command": "python --version"})

    def test_overlay_rejects_later_candidate_or_build_change(self):
        job = self.amend()["job"]
        self.task.build_evidence["details"]["extra"] = "changed"
        with self.assertRaisesRegex(RepositoryError, "OVERLAY_BINDING_INVALID"):
            apply_to_task(self.task, job)

    def test_required_test_byte_mutation_blocks_overlay(self):
        job = self.amend()["job"]
        (self.workspace / "app/test_a.py").write_text("assert False\n")
        with self.assertRaisesRegex(RepositoryError, "OVERLAY_BINDING_INVALID"):
            apply_to_task(self.task, job)

    def test_replay_after_mutation_is_historical_only(self):
        first = self.amend()
        (self.workspace / "app/a.py").write_text("value = 2\n")
        replay = self.amend()
        self.assertTrue(replay["replayed"])
        self.assertFalse(replay["dispatched"])
        self.assertEqual(first["job"], replay["job"])
        self.assertEqual({}, self.task.test_plan_amendment)
        with self.assertRaisesRegex(RepositoryError, "OVERLAY_BINDING_INVALID"):
            apply_to_task(self.task, replay["job"])

    def test_overlay_rejects_identical_source_in_different_workspace(self):
        import shutil
        job = self.amend()["job"]
        other = self.root / "other"
        shutil.copytree(self.workspace, other)
        self.task.working_dir = str(other)
        with self.assertRaisesRegex(RepositoryError, "OVERLAY_BINDING_INVALID"):
            apply_to_task(self.task, job)

    def test_wrong_workspace_rejected_at_amendment(self):
        with self.assertRaisesRegex(RepositoryError, "CANDIDATE_NOT_ELIGIBLE"):
            self.amend(workspace=self.root)

    def test_overlay_rechecks_build_log_bytes(self):
        job = self.amend()["job"]
        (self.root / "build.log").write_bytes(b"mutated receipt log")
        with self.assertRaisesRegex(RepositoryError, "BUILD_LOG_HASH_MISMATCH"):
            apply_to_task(self.task, job)


if __name__ == "__main__":
    unittest.main()
