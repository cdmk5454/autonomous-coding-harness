"""0.99.2.2 Real-Work Stabilization Completion Hotfix focused tests.

Covers the incident regressions of JOB-20260917-152753-58771352
(DEFECT-J039-CNSL-INFO-EXPOSURE, BLOCKED before any candidate, stale
2026-09-16 Worker contract delivered to a new OpenCode session):

- HOTFIX 1 (O1-O4): exact target_resources stay exact files; an optional
  minimal-test allowance is not broad ownership; unrelated dirty state does
  not false-block admission; a real ownership overlap still fail-closes.
- HOTFIX 2 (D1-D4): BLOCKED/terminal Jobs cannot launch Worker runtime; a
  durable Job without materialized execution lineage fail-closes before
  launch; the canonical QUEUED->materialization->execution->attempt path is
  preserved; an admission block creates no runtime authority and consumes no
  worker invocation.
- HOTFIX 3 (P1-P4): a durable task without current execution/attempt context
  fail-closes instead of falling back to any previous prompt/session; a
  finished or foreign attempt is a stale replay; the canonical live attempt
  passes and the command ledger still accepts it.
- HOTFIX 4 (N1-N3): a pre-candidate technical BLOCKED notification projects
  Candidate/Product delta NONE with a recovery action (no candidate review
  demand); notification and product_readiness agree on user_action_required;
  the genuine AWAITING_QA candidate flow is unchanged.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import harness_temp
from control_repository import ControlRepository
from harness_service import HarnessService, HarnessServiceError
from job_queue import JobStore
from operator_notifications import action_for, terminal_projection
from product_readiness import derive_product_readiness
from test_job_control import request, valid_enqueue
from worker import durable_worker_lineage_error, worker_attempt_lineage_error


J039_QUERY = "suc/src/main/java/suc/cs/query/SsucCs0020Qry.xml"
J039_VUE = "sucui/src/pages/student/cs/SsucCs0020List.vue"
J039_EXACT_TARGETS = [J039_QUERY, J039_VUE]
UNRELATED_TEST = "suc/src/test/java/suc/other/ExistingUnrelatedTest.java"
J039_REQUIREMENT = (
    "Goal: 학생 상담현황 조회에서 관리자/상담자 내부용 상담메모가 학생에게 노출되는 결함을 수정한다.\n"
    "Scope:\n"
    "- Primary write scope: " + J039_QUERY + ", " + J039_VUE + "\n"
    "- 관련 최소 regression test 수정/추가 허용 (suc/src/test/java/suc/cs/...)\n"
    "- 다른 파일은 조사만 할 수 있고 write scope를 자동 확대하지 않는다.\n"
)


def bootstrap_store(root: Path, contracts) -> tuple[ControlRepository, JobStore, list[dict]]:
    control_root = root / ".control"
    control_root.mkdir(parents=True, exist_ok=True)
    legacy = JobStore(control_root)
    jobs, _ = valid_enqueue(legacy, "fixture-enqueue", contracts)
    queue = legacy._read_queue()
    repository = ControlRepository(control_root / "control.sqlite3", create=True)
    repository.bootstrap(
        queue,
        {job["job_id"]: job for job in jobs},
        {job["job_id"]: {"path": "jobs/" + job["job_id"] + ".json"} for job in jobs},
    )
    (control_root / "sqlite-authority.json").write_text("{}", encoding="utf-8")
    return repository, JobStore(control_root, repository=repository), jobs


def conflict_resources(repository: ControlRepository, job_id: str) -> list[str]:
    with repository.hold():
        return [
            row[0]
            for row in repository._connection.execute(
                "SELECT resource FROM conflicts WHERE job_id=? ORDER BY resource",
                (job_id,),
            )
        ]


def canonical_execution(repository: ControlRepository, job: dict, *, attempt_id="A-WORKER"):
    execution = repository.materialize(
        job,
        context={"profile_snapshot_sha256": "p", "policy_snapshot_sha256": "p"},
        policy={"effective_policy_sha256": "p", "resolved_worker": {}},
        baseline_hash="b",
        execution_surface_hash="s",
    )
    attempt = repository.start_attempt(
        execution, attempt_id=attempt_id, role="WORKER", model="m"
    )
    return execution, attempt


# ---------------------------------------------------------------------------
# HOTFIX 1 — exact ownership scope preservation
# ---------------------------------------------------------------------------


class OwnershipScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        for name in (J039_QUERY, J039_VUE, UNRELATED_TEST):
            path = self.workspace / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture " + name, encoding="utf-8")
        self.digest = hashlib.sha256(
            (self.workspace / UNRELATED_TEST).read_bytes()
        ).hexdigest()
        legacy = JobStore(self.root / ".control")
        old = request(
            client_job_id="OLD-QUARANTINED",
            target_modules=["suc", "sucui"],
            target_resources=[UNRELATED_TEST],
        )
        jobs, _ = valid_enqueue(legacy, "ownership-fixture", [old])
        queue = legacy._read_queue()
        jobs[0].update(status="AWAITING_QA", revision=1)
        self.repo = ControlRepository(
            self.root / ".control" / "control.sqlite3", create=True
        )
        self.repo.bootstrap(
            queue,
            {jobs[0]["job_id"]: jobs[0]},
            {jobs[0]["job_id"]: {"path": "jobs/" + jobs[0]["job_id"] + ".json"}},
        )
        (self.root / ".control" / "sqlite-authority.json").write_text("{}", encoding="utf-8")
        self.repo.preserve_candidate_ownership(
            jobs[0]["job_id"],
            {UNRELATED_TEST: self.digest},
            workspace=self.workspace,
            evidence_ref="fixture",
        )
        self.service = object.__new__(HarnessService)
        self.service.working_dir = self.workspace
        self.service.control_root = self.root / ".control"
        self.service.task_root = self.root / ".tasks"
        self.service._cumulative_manager = lambda: SimpleNamespace(
            adopted_external_hashes=lambda _baseline_id, repository_paths: {},
            prepare_job_snapshot=lambda _job, _baseline, _inherited: {
                "snapshot_id": "SNAP-OK",
            },
        )
        # Simulate unrelated external modification of the quarantined file.
        (self.workspace / UNRELATED_TEST).write_text("externally changed", encoding="utf-8")

    def _job(self, targets):
        return {
            "job_id": "JOB-NEW",
            "corrects_job_id": "",
            "request": {"target_modules": ["suc", "sucui"], "target_resources": targets},
        }

    def _snapshot(self, targets):
        return self.service.prepare_job_snapshot(
            self._job(targets),
            {"active_baseline_id": "BASE-1", "batch_delta_files": []},
        )

    def test_o1_exact_files_preserved_as_ownership_resources(self):
        repository, _store, jobs = bootstrap_store(
            self.root / "o1",
            [
                request(
                    client_job_id="DEFECT-J039-EXACT",
                    requirement=J039_REQUIREMENT,
                    target_modules=["suc", "sucui"],
                    target_resources=J039_EXACT_TARGETS,
                )
            ],
        )
        job_id = jobs[0]["job_id"]
        stored = repository.read_job(job_id)["request"]["target_resources"]
        self.assertEqual(sorted(J039_EXACT_TARGETS), sorted(stored))
        resources = conflict_resources(repository, job_id)
        self.assertEqual(sorted(J039_EXACT_TARGETS), sorted(resources))
        ancestors = {
            "suc/src/main/java/suc",
            "suc/src/main/java/suc/cs",
            "suc/src/main/java/suc/cs/query",
            "sucui/src/pages/student",
            "sucui/src/pages/student/cs",
            "suc/src/test/java/suc",
            "suc",
            "sucui",
        }
        self.assertEqual([], [r for r in resources if r in ancestors])

    def test_o2_optional_test_allowance_is_not_broad_ownership(self):
        repository, _store, jobs = bootstrap_store(
            self.root / "o2",
            [
                request(
                    client_job_id="DEFECT-J039-ALLOWANCE",
                    requirement=J039_REQUIREMENT,
                    target_modules=["suc", "sucui"],
                    target_resources=J039_EXACT_TARGETS,
                )
            ],
        )
        resources = conflict_resources(repository, jobs[0]["job_id"])
        self.assertNotIn("suc/src/test/java/suc", resources)
        self.assertNotIn("suc/src/test/java/suc/cs", resources)
        self.assertEqual(sorted(J039_EXACT_TARGETS), sorted(resources))

    def test_o3_unrelated_dirty_state_does_not_false_block(self):
        ownership = self.repo.candidate_ownership(self.workspace, current_only=True)
        self.assertFalse(ownership["valid"])
        self.assertEqual([UNRELATED_TEST], ownership["invalid_files"])
        result = self._snapshot(J039_EXACT_TARGETS)
        self.assertEqual("SNAP-OK", result["snapshot_id"])

    def test_o4_real_overlap_still_blocks(self):
        with self.assertRaisesRegex(
            HarnessServiceError, "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED"
        ):
            self._snapshot([UNRELATED_TEST])
        with self.assertRaisesRegex(
            HarnessServiceError, "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED"
        ):
            self._snapshot(["suc/src/test/java/suc"])
        # The exact target job itself stays clean when scope does not overlap.
        self.assertEqual("SNAP-OK", self._snapshot(J039_EXACT_TARGETS)["snapshot_id"])


# ---------------------------------------------------------------------------
# HOTFIX 2 — BLOCKED Job runtime launch hard stop
# ---------------------------------------------------------------------------


class DispatchGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control_root = self.root / ".control"
        self.control_root.mkdir(parents=True)
        (self.control_root / "sqlite-authority.json").write_text("{}", encoding="utf-8")
        self.service = object.__new__(HarnessService)
        self.service.control_root = self.control_root
        self.service._lease_held = True

    def _execute(self, job):
        return self.service._execute(
            job, on_task_started=lambda _task_id: None
        )

    def test_d1_blocked_means_no_runtime_launch(self):
        for status in ("BLOCKED", "CANCELLED", "FAILED_FINAL", "SKIPPED", "SUCCEEDED", "AWAITING_QA"):
            with self.assertRaisesRegex(
                HarnessServiceError, "JOB_NOT_DISPATCHABLE"
            ):
                self._execute({"job_id": "JOB-1", "status": status})

    def test_d1b_blocked_job_launches_no_runtime_authority(self):
        repository, store, jobs = bootstrap_store(
            self.root / "d1b",
            [request(client_job_id="D1-JOB")],
        )
        job = dict(jobs[0])
        job.update(status="BLOCKED", phase="TERMINAL")
        with self.assertRaisesRegex(HarnessServiceError, "JOB_NOT_DISPATCHABLE"):
            self._execute(job)
        with repository.hold():
            for table in ("executions", "materializations", "attempts", "native_sessions", "commands"):
                count = repository._connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                self.assertEqual(0, count, table)

    def test_d2_missing_execution_lineage_fail_closed(self):
        with self.assertRaisesRegex(
            HarnessServiceError, "WORKER_DISPATCH_CONTEXT_MISSING"
        ):
            self._execute({"job_id": "JOB-2", "status": "RUNNING"})
        with self.assertRaisesRegex(
            HarnessServiceError, "WORKER_DISPATCH_CONTEXT_MISSING"
        ):
            self._execute({
                "job_id": "JOB-2", "status": "RUNNING",
                "materialized_execution": {"execution_id": "EXEC-1"},
            })
        with self.assertRaisesRegex(
            HarnessServiceError, "WORKER_DISPATCH_CONTEXT_MISSING"
        ):
            self._execute({
                "job_id": "JOB-2", "status": "RUNNING",
                "materialized_execution": {
                    "execution_id": "EXEC-1",
                    "materialization_id": "MAT-1",
                    "job_id": "JOB-OTHER",
                },
            })

    def test_d2b_contract_revision_mismatch_fail_closed(self):
        with self.assertRaisesRegex(
            HarnessServiceError, "MATERIALIZED_CONTRACT_MISMATCH"
        ):
            self._execute({
                "job_id": "JOB-2", "status": "RUNNING", "contract_revision": 2,
                "materialized_execution": {
                    "execution_id": "EXEC-1",
                    "materialization_id": "MAT-1",
                    "job_id": "JOB-2",
                    "contract_revision": 1,
                },
            })

    def test_d3_canonical_lineage_passes_the_gate(self):
        frozen = {
            "execution_id": "EXEC-1",
            "materialization_id": "MAT-1",
            "job_id": "JOB-3",
            "contract_revision": 1,
        }
        self.assertIsNone(
            self.service._assert_dispatchable(
                {"job_id": "JOB-3", "status": "RUNNING", "contract_revision": 1,
                 "materialized_execution": frozen}
            )
        )
        self.assertIsNone(
            self.service._assert_dispatchable(
                {"job_id": "JOB-3", "status": "QUEUED",
                 "materialized_execution": frozen}
            )
        )
        # A QUEUED job that was never claimed/materialized still fail-closes
        # on the missing canonical lineage, never on a silent legacy launch.
        with self.assertRaisesRegex(
            HarnessServiceError, "WORKER_DISPATCH_CONTEXT_MISSING"
        ):
            self.service._assert_dispatchable({"job_id": "JOB-3", "status": "QUEUED"})
        # Legacy non-durable programmatic adapter contract is unchanged.
        self.assertIsNone(self.service._assert_dispatchable({"status": "BLOCKED"}))
        repository, _store, jobs = bootstrap_store(
            self.root / "d3",
            [request(client_job_id="D3-JOB")],
        )
        execution, attempt = canonical_execution(repository, jobs[0])
        self.assertEqual(
            {"execution_id": execution["execution_id"], "role": "WORKER", "status": "RUNNING"},
            repository.attempt_identity(attempt["attempt_id"]),
        )

    def test_d4_infrastructure_block_consumes_no_runtime_authority(self):
        repository, store, jobs = bootstrap_store(
            self.root / "d4",
            [request(client_job_id="D4-JOB")],
        )
        claimed = store.claim_next("controller-test")
        self.assertEqual(jobs[0]["job_id"], claimed["job_id"])
        blocked = store.record_blocked(
            claimed["job_id"], "controller-test", "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED"
        )
        self.assertEqual("BLOCKED", blocked["status"])
        self.assertEqual(
            "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED",
            blocked["last_result"]["failure_code"],
        )
        self.assertEqual({}, blocked.get("active_attempt_reservation"))
        cancelled = [
            reservation
            for reservation in blocked.get("attempt_reservations") or []
            if reservation.get("cancellation_reason")
        ]
        self.assertEqual(1, len(cancelled))
        self.assertEqual(
            "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED",
            cancelled[0]["cancellation_reason"],
        )
        with repository.hold():
            for table in ("executions", "materializations", "attempts", "native_sessions", "commands"):
                count = repository._connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                self.assertEqual(0, count, table)


# ---------------------------------------------------------------------------
# HOTFIX 3 — stale Worker prompt fallback elimination
# ---------------------------------------------------------------------------


class StalePromptGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.task_root = self.root / ".tasks"
        legacy = JobStore(self.root / ".control")
        contracts = [
            request(client_job_id="OLD-20260916"),
            request(client_job_id="NEW-J039"),
        ]
        jobs, _ = valid_enqueue(legacy, "stale-fixture", contracts)
        queue = legacy._read_queue()
        self.repo = ControlRepository(
            self.root / ".control" / "control.sqlite3", create=True
        )
        self.repo.bootstrap(
            queue,
            {job["job_id"]: job for job in jobs},
            {job["job_id"]: {"path": "jobs/" + job["job_id"] + ".json"} for job in jobs},
        )
        (self.root / ".control" / "sqlite-authority.json").write_text("{}", encoding="utf-8")
        self.old_job, self.new_job = jobs
        self.old_execution, self.old_attempt = canonical_execution(
            self.repo, self.old_job, attempt_id="TASK-20260916-141759-WORKER"
        )
        # The previous job finished: its attempt is terminal evidence and its
        # execution is terminal, exactly like a closed 2026-09-16 Job.
        self.repo.finish_attempt(
            self.old_attempt["attempt_id"], status="FAILED", failure_code="OLD_FAILURE"
        )
        self.old_job.update(status="FAILED_FINAL", revision=self.old_job["revision"] + 1)
        self.repo.write_job(self.old_job, expected_revision=self.old_job["revision"] - 1)
        stale_dir = self.task_root / "context-manifests"
        stale_dir.mkdir(parents=True, exist_ok=True)
        (stale_dir / "TASK-20260916-141759-worker-CTX-STALE.json").write_text(
            json.dumps({"task_id": "TASK-20260916-141759", "prompt": "stale prompt"}),
            encoding="utf-8",
        )

    def _task(self, **fields):
        task = SimpleNamespace(
            control_repository_path=str(self.repo.path),
            execution_id="",
            attempt_id="",
        )
        for key, value in fields.items():
            setattr(task, key, value)
        return task

    def test_p1_no_current_context_fail_closed(self):
        task = self._task()
        self.assertEqual(
            "WORKER_DISPATCH_CONTEXT_MISSING", durable_worker_lineage_error(task)
        )
        task = self._task(execution_id=self.old_execution["execution_id"])
        self.assertEqual(
            "WORKER_DISPATCH_CONTEXT_MISSING", durable_worker_lineage_error(task)
        )

    def test_p2_previous_prompt_artifacts_are_never_a_fallback(self):
        task = self._task(
            execution_id=self.old_execution["execution_id"],
            attempt_id=self.old_attempt["attempt_id"],
        )
        # The stale disk manifest exists, but the finished attempt is a stale
        # replay: the runtime launch must fail-closed, not resume old context.
        self.assertTrue(
            (self.task_root / "context-manifests").is_dir()
            and list((self.task_root / "context-manifests").iterdir())
        )
        self.assertEqual(
            "WORKER_ATTEMPT_NOT_ACTIVE",
            worker_attempt_lineage_error(self.repo, task),
        )

    def test_p3_mismatched_identity_fail_closed(self):
        execution, _attempt = canonical_execution(
            self.repo, self.new_job, attempt_id="TASK-CURRENT-WORKER"
        )
        task = self._task(
            execution_id=execution["execution_id"],
            attempt_id=self.old_attempt["attempt_id"],
        )
        self.assertEqual(
            "WORKER_ATTEMPT_NOT_ACTIVE", worker_attempt_lineage_error(self.repo, task)
        )
        task = self._task(
            execution_id=execution["execution_id"], attempt_id="MISSING-ATTEMPT"
        )
        self.assertEqual(
            "WORKER_ATTEMPT_NOT_ACTIVE", worker_attempt_lineage_error(self.repo, task)
        )

    def test_p4_current_canonical_attempt_passes_and_command_ledger_accepts(self):
        execution, attempt = canonical_execution(
            self.repo, self.new_job, attempt_id="TASK-CURRENT-WORKER"
        )
        task = self._task(
            execution_id=execution["execution_id"],
            attempt_id=attempt["attempt_id"],
        )
        self.assertEqual("", durable_worker_lineage_error(task))
        self.assertEqual("", worker_attempt_lineage_error(self.repo, task))
        from runtime_adapter import payload_sha256

        command = self.repo.prepare_command(
            execution["execution_id"],
            attempt["attempt_id"],
            idempotency_key="p4-initial",
            payload_sha256=payload_sha256({"prompt": "current contract"}),
        )
        self.assertEqual("PREPARED", command["delivery_status"])


# ---------------------------------------------------------------------------
# HOTFIX 4 — BLOCKED notification / product readiness projection
# ---------------------------------------------------------------------------


def incident_job():
    return {
        "job_id": "JOB-20260917-152753-58771352",
        "client_job_id": "DEFECT-J039-CNSL-INFO-EXPOSURE",
        "logical_job_id": "DEFECT-J039-CNSL-INFO-EXPOSURE",
        "status": "BLOCKED",
        "batch_id": "BATCH-J039",
        "sequence": 1,
        "revision": 3,
        "candidate_id": "",
        "hold_scope": "BATCH",
        "request": {
            "requirement": J039_REQUIREMENT,
            "qa_type": "SAFETY_INTEGRITY",
            "operator_label": "J039 학생 상담메모 노출 제거",
        },
        "last_result": {
            "status": "BLOCKED",
            "failure_code": "CANDIDATE_OWNERSHIP_INTEGRITY_FAILED",
        },
    }


class BlockedProjectionTests(unittest.TestCase):
    def queue(self):
        return {"batches": {}, "gate_reason": "BLOCKED", "blocked_by_job_id": ""}

    def test_n1_pre_candidate_block_has_no_candidate_review_wording(self):
        projection = terminal_projection(incident_job(), self.queue())
        message = projection["message"]
        self.assertIn("Status: BLOCKED", message)
        self.assertIn("Failure: CANDIDATE_OWNERSHIP_INTEGRITY_FAILED", message)
        self.assertIn("Candidate: NONE", message)
        self.assertIn("Product delta: NONE", message)
        self.assertIn("Recovery: Harness recovery required", message)
        self.assertNotIn("Review the candidate", message)
        self.assertNotIn("QA decision", message)
        self.assertNotIn("User action: Review", message)
        self.assertEqual("NONE", projection["terminal_disposition"])

    def test_n2_notification_agrees_with_product_readiness(self):
        projection = terminal_projection(incident_job(), self.queue())
        readiness = derive_product_readiness("BLOCKED", ())
        self.assertEqual(
            readiness["user_action_required"], projection["user_action_required"]
        )
        self.assertFalse(projection["user_action_required"])
        self.assertEqual(
            readiness["open_decision_count"], projection["open_decision_count"]
        )
        self.assertEqual(
            "Harness recovery required",
            action_for(incident_job(), self.queue()),
        )

    def test_n3_real_qa_flow_is_preserved(self):
        job = incident_job()
        job.update(status="AWAITING_QA")
        job["last_result"] = {
            "status": "AWAITING_QA",
            "stage": "AWAITING_QA",
            "failure_code": "USER_QA_REQUIRED",
            "failure_origin": "USER_QA",
            "candidate_id": "CAND-1",
            "candidate_manifest": "manifest.json",
            "user_qa_required": True,
            "worktree_disposition": "PRESERVED",
        }
        projection = terminal_projection(job, self.queue())
        self.assertTrue(projection["user_action_required"])
        self.assertEqual(
            "Review the candidate and record the required QA decision",
            action_for(job, self.queue()),
        )
        self.assertIn("Review the candidate and record the required QA decision", projection["message"])
        self.assertEqual("PRESERVED_UNVERIFIED", projection["terminal_disposition"])
        self.assertIn("Product: FINALIZATION_PENDING", projection["message"])

    def test_n3b_preserved_technical_hold_keeps_user_action_required(self):
        job = incident_job()
        job.update(status="AWAITING_QA")
        job["last_result"] = {
            "status": "AWAITING_QA",
            "failure_code": "PROFILE_DRIFT",
            "failure_origin": "HARNESS_CAUSED",
            "failure_stage": "CONTROL",
            "worktree_disposition": "PRESERVED",
        }
        projection = terminal_projection(job, self.queue())
        self.assertEqual("Harness recovery required", action_for(job, self.queue()))
        self.assertTrue(projection["user_action_required"])
        self.assertIn("User action: Harness recovery required", projection["message"])


if __name__ == "__main__":
    unittest.main()
