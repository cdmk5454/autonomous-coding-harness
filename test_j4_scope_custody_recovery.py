from __future__ import annotations

import hashlib
import json
from pathlib import Path
from concurrent.futures import Future
from types import SimpleNamespace
import unittest

import harness_temp as tempfile

from harness_service import HarnessService, HarnessServiceError
from control_repository import ControlRepository
from job_contract import FROZEN_JOB_ROUTE_ONLY, JobRequest
from job_queue import JobStore, QueueError
from manager import ManagerResult
from test_job_control import MODULES, execution_context, valid_enqueue


CONFIRMATION = "REJECT_CANDIDATE_SCOPE_MISMATCH_AND_RETRY_WITH_CORRECTED_SCOPE"
OLD_CANDIDATE = "OCAND-OLD"
OLD_SCOPE = [
    "suc/src/main/java/suc/sp/service/SsucSp0020Svc.java",
    "sucui/src/pages/admin/sp/SsucSp0020ExcelUploadPop.vue",
]
TEST_PATH = "sucui/test/repair-batch-contracts.test.mjs"


class ScopeCustodyRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidate = self.root / "isolation" / "candidate"
        self.candidate.mkdir(parents=True)
        self.changed = OLD_SCOPE + [TEST_PATH]
        self.hashes = {}
        for index, relative in enumerate(self.changed):
            target = self.candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(f"candidate-{index}", encoding="utf-8")
            self.hashes[relative] = hashlib.sha256(target.read_bytes()).hexdigest()

        self.store = JobStore(self.root / ".control")
        original = self.contract(OLD_SCOPE)
        jobs, _ = valid_enqueue(self.store, "scope-custody", [original])
        self.job_id = jobs[0]["job_id"]
        self.store.claim_next("owner")
        self.store.mark_task_started(self.job_id, "owner", "TASK-SCOPE-1")
        self.store.record_result(self.job_id, "owner", {
            "task_id": "TASK-SCOPE-1",
            "execution_id": "EXEC-SCOPE-1",
            "attempt_id": "ATTEMPT-SCOPE-1",
            "candidate_id": OLD_CANDIDATE,
            "status": "AWAITING_QA",
            "stage": "AWAITING_QA",
            "success": False,
            "verification_status": "PARTIAL",
            "failure_stage": "CONTROL",
            "failure_code": "CONTROL_EXECUTION_ERROR",
            "failure_type": "infrastructure",
            "worktree_disposition": "UNKNOWN",
            "changed_files": [],
        })
        queue = self.store._read_queue()
        queue["active_baseline_id"] = "BASELINE-SCOPE-TEST"
        self.store._write_queue(queue)
        job = self.store._read_job(self.job_id)
        job["candidate_id"] = OLD_CANDIDATE
        job["candidate_manifest"] = str(self.candidate / "allocation.json")
        self.store._write_job(job)

    @staticmethod
    def contract(scope):
        return JobRequest.from_mapping({
            "client_job_id": "J4",
            "requirement": "Remove the SP0020 S01-only assumption.",
            "worker": "opencode",
            "model": "opencode-go/deepseek-v4.1-flash",
            "reasoning_effort": "medium",
            "target_modules": ["suc", "sucui"],
            "target_resources": list(scope),
            "max_outer_attempts": 2,
        }, allowed_modules=("suc", "sucui"))

    def integrity(self, queue):
        value = {
            "generation": int(queue["generation"]),
            "active_baseline_id": queue["active_baseline_id"],
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [],
            "predecessor_delta_hashes": {},
            "source": "CANONICAL_COMPUTED",
            "computed_at": "2026-09-21T23:00:00+09:00",
        }
        value["snapshot_sha256"] = hashlib.sha256(json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")).hexdigest()
        return value

    def receipt(self):
        return {
            "candidate_id": OLD_CANDIDATE,
            "candidate_root": str(self.candidate),
            "changed_files": list(self.changed),
            "changed_file_sha256": dict(self.hashes),
            "source_job_revision": self.store.get_job(self.job_id)["revision"],
            "source_task_id": "TASK-SCOPE-1",
            "source_execution_id": "EXEC-SCOPE-1",
            "source_attempt_id": "ATTEMPT-SCOPE-1",
            "failure_code": "CONTROL_EXECUTION_ERROR",
            "worktree_disposition": "UNKNOWN",
        }

    def recover(self, request_id="recover-scope"):
        before = self.store.get_job(self.job_id)
        return self.store.reject_scope_mismatch_and_retry(
            self.job_id,
            expected_revision=before["revision"],
            corrected_request=self.contract(OLD_SCOPE + [TEST_PATH]),
            candidate_receipt=self.receipt(),
            request_id=request_id,
            confirmation=CONFIRMATION,
            integrity_snapshot_provider=self.integrity,
        )

    def test_recovery_preserves_rejected_candidate_and_exact_route(self):
        before_bytes = {p: (self.candidate / p).read_bytes() for p in self.changed}
        recovered, replayed = self.recover()
        self.assertFalse(replayed)
        self.assertEqual("QUEUED", recovered["status"])
        self.assertEqual("", recovered["candidate_id"])
        self.assertEqual(OLD_SCOPE + [TEST_PATH], recovered["request"]["target_resources"])
        rejected = recovered["rejected_candidates"][-1]
        self.assertEqual(OLD_CANDIDATE, rejected["candidate_id"])
        self.assertEqual("REJECTED_ISOLATED_CANDIDATE_PRESERVED", rejected["disposition"])
        self.assertFalse(rejected["accepted"])
        self.assertFalse(rejected["published"])
        self.assertEqual(before_bytes, {p: (self.candidate / p).read_bytes() for p in self.changed})
        route = recovered["technical_retry_route"]
        self.assertEqual(FROZEN_JOB_ROUTE_ONLY, route["policy"])
        self.assertEqual("opencode", route["worker"])
        self.assertEqual("opencode-go/deepseek-v4.1-flash", route["model"])
        self.assertEqual("medium", route["reasoning_effort"])
        self.assertNotIn("droid", json.dumps(route).lower())
        self.assertNotIn("glm", json.dumps(route).lower())

    def test_recovery_is_idempotent_and_reserves_only_technical_attempt(self):
        revision = self.store.get_job(self.job_id)["revision"]
        kwargs = dict(
            expected_revision=revision,
            corrected_request=self.contract(OLD_SCOPE + [TEST_PATH]),
            candidate_receipt=self.receipt(),
            request_id="recover-idempotent",
            confirmation=CONFIRMATION,
            integrity_snapshot_provider=self.integrity,
        )
        first, replayed1 = self.store.reject_scope_mismatch_and_retry(self.job_id, **kwargs)
        second, replayed2 = self.store.reject_scope_mismatch_and_retry(self.job_id, **kwargs)
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first["revision"], second["revision"])
        claimed = self.store.claim_next("fresh-owner")
        self.assertEqual(self.job_id, claimed["job_id"])
        reservation = claimed["active_attempt_reservation"]
        self.assertEqual("TECHNICAL_RECOVERY", reservation["attempt_kind"])
        self.assertEqual(1, reservation["normal_attempt_no"])
        self.assertEqual(1, reservation["technical_recovery_attempt_no"])
        self.assertEqual(1, len(claimed["rejected_candidates"]))

    def test_materialized_repository_accepts_only_the_audited_transition(self):
        queue = self.store._read_queue()
        job = self.store._read_job(self.job_id)
        repository = ControlRepository(self.root / "control.sqlite3", create=True)
        repository.bootstrap(
            queue,
            {self.job_id: job},
            {self.job_id: {"path": f"jobs/{self.job_id}.json"}},
        )
        store = JobStore(self.root / "repository-store", repository=repository)
        revision = store.get_job(self.job_id)["revision"]
        recovered, replayed = store.reject_scope_mismatch_and_retry(
            self.job_id,
            expected_revision=revision,
            corrected_request=self.contract(OLD_SCOPE + [TEST_PATH]),
            candidate_receipt={**self.receipt(), "source_job_revision": revision},
            request_id="recover-materialized-repository",
            confirmation=CONFIRMATION,
            integrity_snapshot_provider=self.integrity,
        )
        self.assertFalse(replayed)
        self.assertEqual("QUEUED", recovered["status"])
        row = repository.worklist()[0]
        self.assertEqual("PLANNED", row["phase"])
        self.assertEqual(2, row["contract_revision"])
        self.assertEqual(OLD_CANDIDATE, recovered["rejected_candidates"][0]["candidate_id"])

    def test_recovery_rejects_broad_or_non_scope_contract_changes(self):
        before = self.store.get_job(self.job_id)
        changed = self.contract(OLD_SCOPE + [TEST_PATH]).to_dict()
        changed["model"] = "opencode-go/another-model"
        changed_request = JobRequest.from_mapping(changed, allowed_modules=("suc", "sucui"))
        with self.assertRaisesRegex(QueueError, "SCOPE_CUSTODY_RECOVERY_CONTRACT_CHANGED"):
            self.store.reject_scope_mismatch_and_retry(
                self.job_id,
                expected_revision=before["revision"],
                corrected_request=changed_request,
                candidate_receipt=self.receipt(),
                request_id="recover-route-change",
                confirmation=CONFIRMATION,
                integrity_snapshot_provider=self.integrity,
            )

    def test_fresh_failure_stays_isolated_and_old_candidate_remains_queryable(self):
        self.recover()
        claimed = self.store.claim_next("fresh-owner")
        fresh_candidate = "OCAND-FRESH"
        self.assertNotEqual(OLD_CANDIDATE, fresh_candidate)
        self.store.mark_task_started(self.job_id, "fresh-owner", "TASK-SCOPE-2")
        self.store.record_result(self.job_id, "fresh-owner", {
            "task_id": "TASK-SCOPE-2",
            "candidate_id": fresh_candidate,
            "status": "AWAITING_QA",
            "stage": "AWAITING_QA",
            "success": False,
            "verification_status": "PARTIAL",
            "failure_stage": "CONTROL",
            "failure_code": "CONTROL_EXECUTION_ERROR",
            "failure_type": "infrastructure",
            "worktree_disposition": "UNKNOWN",
            "changed_files": [],
        })
        failed = self.store.get_job(self.job_id)
        self.assertEqual("AWAITING_QA", failed["status"])
        self.assertEqual(OLD_CANDIDATE, failed["rejected_candidates"][0]["candidate_id"])
        self.assertEqual([], self.store.queue_snapshot().get("batch_delta_files", []))

    def test_missing_scope_addition_and_wrong_confirmation_fail_closed(self):
        before = self.store.get_job(self.job_id)
        common = dict(
            expected_revision=before["revision"],
            corrected_request=self.contract(OLD_SCOPE),
            candidate_receipt=self.receipt(),
            request_id="recover-invalid",
            integrity_snapshot_provider=self.integrity,
        )
        with self.assertRaisesRegex(QueueError, "CONFIRMATION_REQUIRED"):
            self.store.reject_scope_mismatch_and_retry(
                self.job_id, confirmation="ACCEPT", **common
            )
        with self.assertRaisesRegex(QueueError, "EVIDENCE_INVALID"):
            self.store.reject_scope_mismatch_and_retry(
                self.job_id, confirmation=CONFIRMATION, **common
            )

    def test_task_materialization_keeps_exact_opencode_route(self):
        captured = []

        class CapturingManager:
            def __init__(self, **_kwargs):
                pass

            def submit_task(self, task):
                captured.append(task)
                task.status = "SUCCESS"
                task.stage = "DONE"
                task.verification_status = "VERIFIED"
                task.worktree_disposition = "NO_DELTA"
                future = Future()
                future.set_result(ManagerResult(success=True))
                return future

            def shutdown(self):
                pass

        profile = SimpleNamespace(
            id="e4u", schema_version=1, profile_dir=self.root,
            module_names=MODULES, modules=(), control_policy={},
        )
        service = HarnessService(
            self.root, self.root, task_root=self.root / ".tasks",
            manager_factory=CapturingManager,
            instance_lock=SimpleNamespace(acquire=lambda: True, release=lambda: None),
        )
        service._load_profile = lambda: (profile, list(MODULES))
        service._configuration = lambda *_: {
            "working_dir": str(self.root.resolve()),
            "profile_dir": str(self.root.resolve()),
            "available_modules": list(MODULES),
            "task_root": str((self.root / ".tasks").resolve()),
            **execution_context(),
        }
        contract = self.contract(OLD_SCOPE + [TEST_PATH])
        route = {
            "policy": FROZEN_JOB_ROUTE_ONLY,
            "worker": contract.worker,
            "model": contract.model,
            "reasoning_effort": contract.reasoning_effort,
        }
        self.assertTrue(service.acquire_execution_lease())
        try:
            outcome = service.execute({
                "request": contract.to_dict(),
                "current_requirement": contract.requirement,
                "execution_context": execution_context(),
                "technical_retry_route": route,
            }, on_task_started=lambda _task_id: None)
        finally:
            service.release_execution_lease()
        self.assertEqual("", outcome["failure_code"])
        self.assertEqual(1, len(captured))
        task = captured[0]
        self.assertEqual("opencode", task.worker)
        self.assertEqual("opencode-go/deepseek-v4.1-flash", task.opencode_model)
        self.assertEqual(FROZEN_JOB_ROUTE_ONLY, task.worker_routing_policy)

        bad = dict(route, model="opencode-go/another-model")
        self.assertTrue(service.acquire_execution_lease())
        try:
            with self.assertRaisesRegex(HarnessServiceError, "FROZEN_JOB_ROUTE_MISMATCH"):
                service.execute({
                    "request": contract.to_dict(),
                    "current_requirement": contract.requirement,
                    "execution_context": execution_context(),
                    "technical_retry_route": bad,
                }, on_task_started=lambda _task_id: None)
        finally:
            service.release_execution_lease()


if __name__ == "__main__":
    unittest.main()
