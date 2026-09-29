"""SQLite control, migration, revision and immutable execution regressions."""
import copy
import json
from pathlib import Path
import unittest

import harness_temp
from control_migration import rehearse, cutover, scheduling_source, projection
from control_repository import ControlRepository, RepositoryError, digest
from job_queue import JobStore, QueueError
from policy_catalog import resolve_effective_policy
from test_job_control import request, valid_enqueue, execution_context


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.legacy = self.root / "legacy"
        legacy = JobStore(self.legacy)
        contract = request(client_job_id="SQLITE-ONE")
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps({"jobs": [contract.to_dict()]}), encoding="utf-8")
        self.jobs, _ = valid_enqueue(legacy, "sqlite-fixture", [contract], source_manifest_path=manifest)
        self.job_id = self.jobs[0]["job_id"]
        self.database = self.root / "rehearsed.sqlite3"
        self.rehearsal = rehearse(self.legacy, self.database)
        self.repository = ControlRepository(self.database, legacy_root=self.legacy)

    def test_copy_rehearsal_cutover_restart_and_sqlite_wins(self):
        self.assertTrue(self.rehearsal["pass"])
        queue_before = (self.legacy / "queue.json").read_bytes()
        job_before = (self.legacy / "jobs" / (self.job_id + ".json")).read_bytes()
        result = cutover(self.legacy, self.database, self.rehearsal)
        self.assertEqual("VALID", result["status"])
        first = JobStore(self.legacy)
        expected = first.queue_snapshot()
        (self.legacy / "queue.json").write_text("broken obsolete file")
        second = JobStore(self.legacy)
        self.assertEqual(expected, second.queue_snapshot())
        self.assertEqual(job_before, (self.legacy / "jobs" / (self.job_id + ".json")).read_bytes())
        self.assertNotEqual(queue_before, (self.legacy / "queue.json").read_bytes())

    def test_atomic_failure_rolls_back_job_and_queue(self):
        before = self.repository.read_job(self.job_id)
        with self.assertRaisesRegex(RuntimeError, "injected"):
            with self.repository.hold():
                job = copy.deepcopy(before)
                job["revision"] += 1
                job["owner_id"] = "new-owner"
                self.repository.write_job(job, expected_revision=before["revision"])
                raise RuntimeError("injected")
        self.assertEqual(before, self.repository.read_job(self.job_id))
        stale = copy.deepcopy(before)
        stale["revision"] += 1
        with self.assertRaisesRegex(RepositoryError, "REVISION_CONFLICT"):
            self.repository.write_job(stale, expected_revision=999)

    def test_missing_authority_marker_never_falls_back_to_legacy(self):
        cutover(self.legacy, self.database, self.rehearsal)
        (self.legacy / "sqlite-authority.json").unlink()
        for read_only in (True, False):
            with self.assertRaisesRegex(QueueError, "CONTROL_AUTHORITY_INVALID"):
                JobStore(self.legacy, read_only=read_only)

    def test_global_stop_release_is_cas_and_does_not_dispatch(self):
        before = projection(self.repository, scheduling_source(self.legacy)[0])
        self.repository.set_global_stop("maintenance")
        stop = self.repository.global_stop()
        with self.assertRaisesRegex(RepositoryError, "REVISION_CONFLICT"):
            self.repository.release_global_stop(expected_revision=stop["revision"] + 1, reason="reviewed")
        self.assertTrue(self.repository.global_stop()["active"])
        self.repository.release_global_stop(expected_revision=stop["revision"], reason="reviewed")
        self.assertFalse(self.repository.global_stop()["active"])
        self.assertEqual(before, projection(self.repository, scheduling_source(self.legacy)[0]))

    def test_planned_revision_needs_no_queue_replacement_and_materialization_freezes(self):
        queue = self.repository.read_queue()
        original = self.repository.read_job(self.job_id)
        contract = dict(original["request"], requirement="revised approved scope")
        revised = self.repository.revise_planned(self.job_id, contract, expected_revision=original["revision"])
        self.assertEqual(queue, self.repository.read_queue())
        self.assertEqual(2, revised["contract_revision"])
        context = execution_context()
        policy = resolve_effective_policy(profile_id="e4u", policy_snapshot_sha256=context["policy_snapshot_sha256"])
        execution = self.repository.materialize(revised, context=context, policy=policy,
                    baseline_hash="a" * 64, execution_surface_hash="b" * 64)
        self.assertEqual("MATERIALIZED", self.repository.worklist()[0]["phase"])
        with self.assertRaisesRegex(RepositoryError, "MATERIALIZED_CONTRACT_IMMUTABLE"):
            self.repository.revise_planned(self.job_id, contract, expected_revision=revised["revision"])
        self.repository.start_attempt(execution, attempt_id="TASK-ONE-WORKER", role="WORKER", model="gpt-5.6-sol")
        self.assertEqual("RUNNING", self.repository.worklist()[0]["phase"])
        job = self.repository.read_job(self.job_id)
        job.update(status="AWAITING_QA", last_result={"failure_code": "PROFILE_DRIFT"}, revision=job["revision"] + 1)
        self.repository.write_job(job, expected_revision=job["revision"] - 1)
        self.assertEqual("TERMINAL", self.repository.worklist()[0]["phase"])
        with self.assertRaisesRegex(RepositoryError, "TERMINAL_EXECUTION_IMMUTABLE"):
            self.repository.start_attempt(execution, attempt_id="LATE", role="WORKER", model="gpt-5.6-sol")

    def test_jobstore_writes_are_authoritative_without_legacy_mirror(self):
        store = JobStore(self.legacy, repository=self.repository)
        job = store.claim_next("sqlite-owner")
        self.assertEqual(self.job_id, job["job_id"])
        restarted = JobStore(self.legacy, repository=ControlRepository(self.database, legacy_root=self.legacy))
        self.assertEqual("RUNNING", restarted.get_job(self.job_id)["status"])
        self.assertEqual("QUEUED", JobStore(self.legacy, read_only=True).get_job(self.job_id)["status"])
        self.assertEqual(store.queue_snapshot(), restarted.queue_snapshot())

    def test_changed_migration_source_rejects_cutover(self):
        path = self.legacy / "queue.json"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        with self.assertRaisesRegex(RepositoryError, "MIGRATION_SOURCE_CHANGED"):
            cutover(self.legacy, self.database, self.rehearsal)
        self.assertFalse((self.legacy / "sqlite-authority.json").exists())

    def test_global_stop_rejects_dispatch_without_consuming_attempt_or_changing_job(self):
        before = self.repository.read_job(self.job_id)
        self.repository.set_global_stop("HARNESS_091_MAINTENANCE_PRODUCT_EXECUTION_FROZEN")
        store = JobStore(self.legacy, repository=self.repository)
        self.assertIsNone(store.claim_next("owner"))
        self.assertEqual(before, self.repository.read_job(self.job_id))
        restarted = ControlRepository(self.database, legacy_root=self.legacy)
        self.assertTrue(restarted.global_stop()["active"])

    def test_revision_keeps_manifest_provenance_and_terminal_execution_survives_later_attempt(self):
        original = self.repository.read_job(self.job_id)
        revised = self.repository.revise_planned(self.job_id,
            dict(original["request"], requirement="approved revision"), expected_revision=original["revision"])
        store = JobStore(self.legacy, repository=self.repository)
        self.assertTrue(store.validate_batch_manifest(original["batch_id"])["valid"])
        execution = self.repository.materialize(revised, context=execution_context(),
            policy={"effective_policy_sha256": "p1"}, baseline_hash="baseline", execution_surface_hash="surface")
        terminal = dict(revised, status="AWAITING_QA", revision=revised["revision"] + 1, last_result={"status": "FAILED"})
        self.repository.write_job(terminal, expected_revision=revised["revision"])
        retry = dict(terminal, status="QUEUED", revision=terminal["revision"] + 1)
        self.repository.write_job(retry, expected_revision=terminal["revision"])
        second = self.repository.materialize(retry, context=execution_context(),
            policy={"effective_policy_sha256": "p2"}, baseline_hash="baseline", execution_surface_hash="surface2")
        self.assertNotEqual(execution["execution_id"], second["execution_id"])
        with self.assertRaisesRegex(RepositoryError, "TERMINAL_EXECUTION_IMMUTABLE"):
            self.repository.start_attempt(execution, attempt_id="late", role="WORKER", model="model")


if __name__ == "__main__":
    unittest.main()
