"""Focused regression coverage for explicit external baseline adoption."""

import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest

import harness_temp
from control_repository import ControlRepository
from job_queue import (
    BATCH_FINAL_COMMIT,
    EXTERNAL_BASELINE_ADOPTION_CONFIRMATION,
    JobStore,
    QueueError,
)
from manager import Manager


ROUTE = {
    "runtime": "opencode",
    "model": "zai-coding-plan/glm-5.3-flash",
    "reasoning_effort": "low",
    "fallback": "none",
}


class ExternalBaselineAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = JobStore(self.root / ".control")
        self.batch_id = "BATCH-" + "a" * 24
        self.job_ids = [f"JOB-20260909-12000{i}-deadbee{i}" for i in range(1, 3)]
        for sequence, job_id in enumerate(self.job_ids, 1):
            self.store._write_job({
                "schema_version": 1,
                "job_id": job_id,
                "client_job_id": f"J0{sequence}",
                "batch_id": self.batch_id,
                "generation": 13,
                "sequence": sequence,
                "status": "QUEUED",
                "outer_attempt": 0,
                "task_ids": [],
                "request": {
                    "worker": ROUTE["runtime"],
                    "model": ROUTE["model"],
                    "reasoning_effort": ROUTE["reasoning_effort"],
                },
                "revision": 0,
            })
        queue = self.store._empty_queue()
        queue.update({
            "generation": 13,
            "operator_paused": True,
            "operator_pause_reason": "fixture",
            "order": list(self.job_ids),
            "active_baseline_id": "BASELINE-OLD",
            "commit_policy": BATCH_FINAL_COMMIT,
            "batch_delta_files": ["sample-service/old.java"],
            "batches": {
                "sha256:fixture": {
                    "batch_id": self.batch_id,
                    "state": "COMMITTED",
                    "job_ids": list(self.job_ids),
                    "provenance": {
                        "batch_execution_policy": {"worker_route": dict(ROUTE)}
                    },
                }
            },
        })
        self.store._refresh_pause(queue)
        self.store._write_queue(queue)

    def fields(self):
        return {
            "commit_policy": BATCH_FINAL_COMMIT,
            "active_baseline_id": "BASELINE-G13-EXT-TEST",
            "task_owned_baseline_files": ["sample-service/a.java"],
            "external_frozen_baseline_files": ["sample-web/.env"],
            "unexpected_runtime_dirty_files": [],
            "repository_head_integrity": True,
            "dirty_overlay_path_set_match": True,
            "dirty_overlay_hash_match": True,
            "baseline_declaration_integrity": True,
            "external_frozen_integrity": True,
            "external_delta_adoption": {
                "classification": "USER_ACCEPTED_EXTERNAL_DELTA",
                "origin": "EXTERNAL",
                "adoption": "USER_APPROVED",
                "validation_boundary": "BASELINE_ADOPTION_ONLY",
                "generation_provenance": "UNKNOWN",
                "adoption_fingerprint": "f" * 64,
                "repository_heads": {"sample-service": "a" * 40},
                "dirty_overlay": [{"path": "sample-service/a.java", "sha256": "b" * 64}],
            },
        }

    def invoke(self, **changes):
        queue = self.store.queue_snapshot()
        args = {
            "batch_id": self.batch_id,
            "expected_generation": 13,
            "expected_queue_revision": queue["revision"],
            "expected_job_revisions": {
                job_id: self.store.get_job(job_id)["revision"] for job_id in self.job_ids
            },
            "fields": self.fields(),
            "request_id": "adoption-fixture",
            "confirmation": EXTERNAL_BASELINE_ADOPTION_CONFIRMATION,
            "execution_idle": True,
        }
        args.update(changes)
        return self.store.adopt_external_baseline(**args)

    def test_adopts_without_replacing_or_attempting_jobs(self):
        before = {job_id: self.store.get_job(job_id) for job_id in self.job_ids}
        result, replayed = self.invoke()
        self.assertFalse(replayed)
        queue = self.store.queue_snapshot()
        self.assertEqual("BASELINE-G13-EXT-TEST", queue["active_baseline_id"])
        self.assertEqual([], queue["batch_delta_files"])
        self.assertEqual("USER_ACCEPTED_EXTERNAL_DELTA", queue["external_delta_adoption"]["classification"])
        self.assertEqual(ROUTE, result["worker_route"])
        for job_id in self.job_ids:
            self.assertEqual(before[job_id], self.store.get_job(job_id))

    def test_stale_revision_rejects_without_mutation(self):
        before = self.store.queue_snapshot()
        with self.assertRaisesRegex(QueueError, "QUEUE_REVISION_CONFLICT"):
            self.invoke(expected_queue_revision=before["revision"] - 1)
        self.assertEqual(before, self.store.queue_snapshot())

    def test_worker_route_must_be_exact_and_fallback_free(self):
        queue = self.store._read_queue()
        next(iter(queue["batches"].values()))["provenance"]["batch_execution_policy"]["worker_route"]["fallback"] = "droid"
        self.store._write_queue(queue)
        with self.assertRaisesRegex(QueueError, "BATCH_WORKER_CONTRACT_INVALID"):
            self.invoke()

    def test_unverified_overlay_fingerprint_is_rejected(self):
        fields = self.fields()
        fields["dirty_overlay_hash_match"] = False
        with self.assertRaisesRegex(QueueError, "EXTERNAL_BASELINE_INTEGRITY_INVALID"):
            self.invoke(fields=fields)

    def test_materialized_execution_forbids_runtime_fallback(self):
        task = SimpleNamespace(materialized_execution={"request": {"worker": "opencode"}})
        self.assertEqual(
            ("", "", "frozen execution policy has no alternate Worker route"),
            Manager._fallback_route(object.__new__(Manager), task),
        )


class AcceptedCandidateHistoryTests(unittest.TestCase):
    def test_exact_accepted_hash_reclassifies_ownership_as_history(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        workspace = root / "product"
        name = "sample-service/accepted.java"
        path = workspace / name
        path.parent.mkdir(parents=True)
        path.write_bytes(b"accepted external bytes\r\n")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        queue = JobStore(root / "legacy")._empty_queue()
        job = {
            "schema_version": 1,
            "job_id": "JOB-20260909-120001-deadbeef",
            "client_job_id": "HISTORICAL",
            "status": "AWAITING_QA",
            "revision": 1,
            "request": {},
        }
        repository = ControlRepository(root / "control.sqlite3", create=True)
        repository.bootstrap(
            queue,
            {job["job_id"]: job},
            {job["job_id"]: {"path": f"jobs/{job['job_id']}.json"}},
        )
        repository.preserve_candidate_ownership(
            job["job_id"], {name: digest}, workspace=workspace, evidence_ref="fixture"
        )
        status = repository.candidate_ownership(
            workspace, accepted_baseline_hashes={name: digest}
        )
        self.assertTrue(status["valid"])
        self.assertEqual([], status["files"])
        self.assertEqual([name], [item["path"] for item in status["accepted_baseline_files"]])


if __name__ == "__main__":
    unittest.main()
