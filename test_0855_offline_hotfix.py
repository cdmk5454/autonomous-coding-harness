from __future__ import annotations

import hashlib
import json
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path

from batch_manifest import (
    BatchManifestError,
    prepare_ab_only,
)
from execution_lineage import (
    NORMAL,
    TECHNICAL_RECOVERY,
    AttemptAccountingError,
    accounting,
    consume_reservation,
    lineage_status,
    reserve_execution_attempt,
)
from git_collector import GitCollector
from job_queue import (
    AWAITING_QA,
    EXTERNAL_HANDOFF_CONFIRMATION,
    JobStore,
    QueueError,
)
from patch_diagnostics import (
    CONTEXT_MISMATCH,
    CONTEXT_STALE,
    classify_patch_context,
    write_patch_diagnostic,
)
from task_state import TaskState
from test_job_control import execution_context, request, valid_enqueue


def _technical_event(ref="ref-1"):
    return {
        "event": "AWAITING_QA_TECHNICAL_RETRY_QUEUED",
        "request_ref": ref,
        "expected_revision": 4,
    }


class AttemptAccounting0855Tests(unittest.TestCase):
    def queue(self, revision=7):
        return {"revision": revision}

    def test_01_third_normal_attempt_blocked(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 2, "max_outer_attempts": 2}
        with self.assertRaisesRegex(AttemptAccountingError, "OUTER_ATTEMPT_BUDGET_EXHAUSTED"):
            reserve_execution_attempt(job, self.queue(), attempt_kind=NORMAL)

    def test_02_technical_recovery_uses_its_own_budget(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 1, "max_outer_attempts": 2}
        reservation, replayed = reserve_execution_attempt(
            job, self.queue(), attempt_kind=TECHNICAL_RECOVERY,
            provenance={"request_ref": "tech-1", "recovery_reason": "PATCH"},
        )
        self.assertFalse(replayed)
        self.assertEqual(1, reservation["technical_recovery_attempt_no"])
        self.assertEqual((2, 3), (reservation["execution_attempt"], reservation["effective_execution_limit"]))
        self.assertEqual(1, reservation["normal_attempt_no"])

    def test_03_technical_recovery_does_not_consume_product_attempt(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 2, "max_outer_attempts": 2}
        reservation, _ = reserve_execution_attempt(
            job, self.queue(), attempt_kind=TECHNICAL_RECOVERY,
            provenance={"request_ref": "tech-1"},
        )
        self.assertEqual((3, 2, 1), (
            reservation["execution_attempt"], reservation["normal_attempt_no"],
            reservation["technical_recovery_attempt_no"],
        ))

    def test_04_second_technical_recovery_blocked(self):
        job = {
            "job_id": "JOB-X", "revision": 2, "outer_attempt": 1,
            "max_outer_attempts": 3, "technical_recovery_attempt_count": 1,
            "history": [_technical_event()],
        }
        with self.assertRaisesRegex(AttemptAccountingError, "TECHNICAL_RETRY_BUDGET_EXHAUSTED"):
            reserve_execution_attempt(job, self.queue(), attempt_kind=TECHNICAL_RECOVERY)

    def test_05_provenanced_technical_execution_is_readable(self):
        state = lineage_status({
            "outer_attempt": 3, "max_outer_attempts": 2,
            "technical_recovery_attempt_count": 1, "history": [_technical_event()],
        })
        self.assertTrue(state["valid"])
        self.assertEqual(2, state["normal_attempt_no"])
        self.assertTrue(state["legacy_compatible"])

    def test_06_legacy_overflow_without_provenance_blocked(self):
        with self.assertRaisesRegex(AttemptAccountingError, "ATTEMPT_ACCOUNTING_INCONSISTENT"):
            accounting({"outer_attempt": 4, "max_outer_attempts": 2, "technical_recovery_attempt_count": 1})

    def test_07_duplicate_request_ref_is_idempotent(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 0, "max_outer_attempts": 2}
        first, replay1 = reserve_execution_attempt(job, self.queue(), attempt_kind=NORMAL, provenance={"request_ref": "same"})
        second, replay2 = reserve_execution_attempt(job, self.queue(), attempt_kind=NORMAL, provenance={"request_ref": "same"})
        self.assertFalse(replay1)
        self.assertTrue(replay2)
        self.assertEqual(first["reservation_id"], second["reservation_id"])

    def test_08_duplicate_reservation_cannot_create_second_task(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 0, "max_outer_attempts": 2}
        reserve_execution_attempt(job, self.queue(), attempt_kind=NORMAL)
        consume_reservation(job, "TASK-ONE")
        with self.assertRaisesRegex(AttemptAccountingError, "ATTEMPT_RESERVATION_ALREADY_CONSUMED"):
            consume_reservation(job, "TASK-TWO")

    def test_09_revision_mismatch_is_no_mutation(self):
        job = {"job_id": "JOB-X", "revision": 2, "outer_attempt": 0, "max_outer_attempts": 2}
        before = json.dumps(job, sort_keys=True)
        with self.assertRaisesRegex(AttemptAccountingError, "STALE_JOB_REVISION"):
            reserve_execution_attempt(
                job, self.queue(), attempt_kind=NORMAL,
                provenance={"expected_job_revision": 1},
            )
        self.assertEqual(before, json.dumps(job, sort_keys=True))

    def test_10_claim_revalidates_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / "control")
            jobs, _ = valid_enqueue(store, "claim-budget", [request(max_outer_attempts=2)])
            job = store._read_job(jobs[0]["job_id"])
            job["outer_attempt"] = 3
            job["technical_recovery_attempt_count"] = 0
            store._write_job(job)
            with self.assertRaisesRegex(QueueError, "OUTER_ATTEMPT_BUDGET_EXHAUSTED"):
                store.claim_next("owner")
            self.assertEqual("QUEUED", store.get_job(job["job_id"])["status"])


class OrphanDetection0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = JobStore(self.root / "control")
        jobs, _ = valid_enqueue(self.store, "orphan", [request()])
        self.job_id = jobs[0]["job_id"]
        self.store.claim_next("owner")
        self.task_id = "TASK-20260830-220000"
        self.store.mark_task_started(self.job_id, "owner", self.task_id)
        task_dir = self.root / "tasks" / "20260830"
        task_dir.mkdir(parents=True)
        (task_dir / f"{self.task_id}.json").write_text(
            json.dumps({"task_id": self.task_id, "status": "RUNNING"}), encoding="utf-8"
        )

    def finding(self):
        values = self.store.inspect_orphaned_running(
            self.root / "tasks", execution_owner_active=False, worker_process_active=False
        )
        self.assertEqual(1, len(values))
        return values[0]

    def test_11_detects_running_without_live_owner(self): self.assertEqual("ORPHANED_RUNNING_EXECUTION", self.finding()["code"])
    def test_12_does_not_auto_claim(self): self.assertFalse(self.finding()["auto_claim"])
    def test_13_does_not_auto_retry(self): self.assertFalse(self.finding()["auto_retry"])
    def test_14_does_not_auto_rollback(self): self.assertFalse(self.finding()["auto_rollback"])
    def test_15_does_not_release_successor(self): self.assertFalse(self.finding()["successor_release"])
    def test_16_reports_operator_action_and_preserved_risk(self):
        finding = self.finding()
        self.assertTrue(finding["operator_action_required"] and finding["worktree_may_be_preserved"])


class ExternalHandoff0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = JobStore(self.root / "control")
        self.jobs, _ = valid_enqueue(self.store, "handoff", [request(client_job_id="A"), request(client_job_id="B")])
        self.batch_id = self.jobs[0]["batch_id"]
        self.store.claim_next("owner")
        self.store.mark_task_started(self.jobs[0]["job_id"], "owner", "TASK-20260830-220100")
        running = self.store._read_job(self.jobs[0]["job_id"])
        running["pre_job_snapshots"] = [{"snapshot_id": "SNAP-X"}]
        self.store._write_job(running)
        queue = self.store._read_queue()
        queue["operator_paused"] = True
        queue["operator_pause_reason"] = "external handoff"
        self.store._refresh_pause(queue)
        self.store._write_queue(queue)
        self.expected_queue_revision = self.store.queue_snapshot()["revision"]
        self.revisions = {job["job_id"]: self.store.get_job(job["job_id"])["revision"] for job in self.jobs}

    def run_handoff(self, request_id="handoff-1", revisions=None, queue_revision=None):
        return self.store.supersede_batch_for_external_handoff(
            self.batch_id,
            expected_generation=1,
            expected_queue_revision=self.expected_queue_revision if queue_revision is None else queue_revision,
            expected_job_revisions=revisions or self.revisions,
            external_handoff_ref="WEB-GPT-U03-U04",
            request_id=request_id,
            confirmation=EXTERNAL_HANDOFF_CONFIRMATION,
            execution_idle=True,
        )

    def test_17_running_task_becomes_non_success_terminal(self):
        self.run_handoff(); job = self.store.get_job(self.jobs[0]["job_id"])
        self.assertEqual("INTERRUPTED", job["status"]); self.assertFalse(job["last_result"]["success"])

    def test_18_running_source_disposition_is_preserved(self):
        self.run_handoff(); self.assertEqual("PRESERVED", self.store.get_job(self.jobs[0]["job_id"])["last_result"]["worktree_disposition"])

    def test_19_successor_is_cancelled_without_execution(self):
        self.run_handoff(); job = self.store.get_job(self.jobs[1]["job_id"])
        self.assertEqual("CANCELLED", job["status"]); self.assertEqual([], job["task_ids"])

    def test_20_batch_is_not_strict_success(self): self.assertFalse(self.run_handoff()[0]["strict_success"])

    def test_21_history_is_append_only(self):
        before = list(self.store.get_job(self.jobs[0]["job_id"])["history"])
        self.run_handoff(); after = self.store.get_job(self.jobs[0]["job_id"])["history"]
        self.assertEqual(before, after[:len(before)])

    def test_22_snapshot_metadata_is_retained(self):
        self.run_handoff(); self.assertEqual("SNAP-X", self.store.get_job(self.jobs[0]["job_id"])["pre_job_snapshots"][0]["snapshot_id"])

    def test_23_duplicate_request_is_idempotent(self):
        first, replay1 = self.run_handoff(); second, replay2 = self.run_handoff()
        self.assertFalse(replay1); self.assertTrue(replay2); self.assertEqual(first, second)

    def test_24_expected_revision_mismatch_is_no_mutation(self):
        queue_hash = hashlib.sha256((self.root / "control" / "queue.json").read_bytes()).hexdigest()
        bad = dict(self.revisions); bad[self.jobs[1]["job_id"]] += 1
        with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
            self.run_handoff(revisions=bad)
        self.assertEqual(queue_hash, hashlib.sha256((self.root / "control" / "queue.json").read_bytes()).hexdigest())

    def test_25_unrelated_source_fixture_is_unchanged(self):
        source = self.root / "source.txt"; source.write_bytes(b"original")
        digest = hashlib.sha256(source.read_bytes()).hexdigest(); self.run_handoff()
        self.assertEqual(digest, hashlib.sha256(source.read_bytes()).hexdigest())


class LegacyBatchSupersede0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = JobStore(self.root / "control")
        self.jobs, _ = valid_enqueue(
            self.store,
            "legacy-ready",
            [request(client_job_id="QA"), request(client_job_id="OLD-CANCELLED"), request(client_job_id="OLD-QUEUED")],
        )
        self.batch_id = self.jobs[0]["batch_id"]
        owner = "legacy-ready-owner"
        running = self.store.claim_next(owner)
        self.store.mark_task_started(running["job_id"], owner, "TASK-LEGACY-QA")
        qa = self.store._read_job(running["job_id"])
        qa["status"] = AWAITING_QA
        qa["owner_id"] = ""
        qa["candidate_id"] = "CAND-" + "A" * 24
        qa["candidate_manifest"] = "candidate-manifest.json"
        qa["pre_job_snapshots"] = [{"snapshot_id": "SNAP-LEGACY"}]
        qa["last_result"] = {
            "task_id": "TASK-LEGACY-QA",
            "status": "SUCCESS",
            "success": True,
            "build_status": "PASS",
            "test_status": "SKIPPED",
            "review_status": "REVIEW_PASS",
            "verification_status": "VERIFIED",
            "changed_files": ["app/qa.txt"],
            "worktree_disposition": "PRESERVED",
        }
        self.store._write_job(qa)
        self.store.cancel_queued_job(self.jobs[1]["job_id"], "already cancelled")
        queue = self.store._read_queue()
        queue["running_job_id"] = ""
        queue["blocked_by_job_id"] = qa["job_id"]
        queue["gate_reason"] = AWAITING_QA
        queue["operator_paused"] = True
        queue["operator_pause_reason"] = "legacy cleanup"
        queue["task_owned_baseline_files"] = ["app/inherited.txt"]
        queue["external_frozen_baseline_files"] = ["app/frozen.txt"]
        queue["batch_delta_files"] = ["app/inherited.txt"]
        queue["unexpected_runtime_dirty_files"] = []
        self.store._refresh_pause(queue)
        self.store._write_queue(queue)
        self.expected_queue_revision = self.store.queue_snapshot()["revision"]
        self.revisions = {
            job["job_id"]: self.store.get_job(job["job_id"])["revision"]
            for job in self.jobs
        }
        self.qa_before = self.store.get_job(self.jobs[0]["job_id"])
        self.cancelled_before = self.store.get_job(self.jobs[1]["job_id"])

    def run_supersede(self, *, request_id="legacy-ready-1", revisions=None, queue_revision=None):
        return self.store.supersede_batch_for_external_handoff(
            self.batch_id,
            expected_generation=1,
            expected_queue_revision=(
                self.expected_queue_revision if queue_revision is None else queue_revision
            ),
            expected_job_revisions=revisions or self.revisions,
            external_handoff_ref="USER_SUPERSEDED_FOR_NEW_BATCH",
            request_id=request_id,
            confirmation=EXTERNAL_HANDOFF_CONFIRMATION,
            execution_idle=True,
        )

    def test_25_awaiting_qa_and_cancelled_batch_can_be_superseded(self):
        result, replayed = self.run_supersede()
        self.assertFalse(replayed)
        self.assertFalse(result["strict_success"])
        self.assertEqual("SKIPPED", self.store.get_job(self.jobs[0]["job_id"])["status"])
        self.assertEqual("CANCELLED", self.store.get_job(self.jobs[1]["job_id"])["status"])
        self.assertEqual("CANCELLED", self.store.get_job(self.jobs[2]["job_id"])["status"])

    def test_26_cancelled_member_is_not_rewritten(self):
        self.run_supersede()
        after = self.store.get_job(self.jobs[1]["job_id"])
        self.assertEqual(self.cancelled_before["revision"], after["revision"])
        self.assertEqual(self.cancelled_before["last_result"], after["last_result"])

    def test_27_awaiting_qa_historical_result_and_candidate_are_preserved(self):
        self.run_supersede()
        after = self.store.get_job(self.jobs[0]["job_id"])
        self.assertEqual(self.qa_before["last_result"], after["last_result"])
        self.assertEqual(self.qa_before["candidate_id"], after["candidate_id"])
        self.assertEqual(self.qa_before["candidate_manifest"], after["candidate_manifest"])
        self.assertEqual(self.qa_before["pre_job_snapshots"], after["pre_job_snapshots"])
        self.assertEqual("AWAITING_QA", after["supersede"]["superseded_from_status"])
        self.assertEqual("PRESERVED", after["supersede"]["worktree_disposition"])

    def test_28_batch_closure_records_revision_safe_provenance(self):
        self.run_supersede()
        qa = self.store.get_job(self.jobs[0]["job_id"])
        self.assertEqual("USER_SUPERSEDED_FOR_NEW_BATCH", qa["supersede"]["supersede_reason"])
        self.assertEqual(1, qa["supersede"]["expected_generation"])
        self.assertEqual(self.expected_queue_revision, qa["supersede"]["expected_queue_revision"])
        self.assertTrue(qa["supersede"]["request_ref"])
        queue = self.store._read_queue()
        self.assertEqual("USER_SUPERSEDED_FOR_NEW_BATCH", queue["history"][-1]["supersede_reason"])

    def test_29_gate_is_cleared_and_new_batch_ready_pause_is_held(self):
        self.run_supersede()
        queue = self.store.queue_snapshot()
        self.assertEqual("", queue["blocked_by_job_id"])
        self.assertEqual("", queue["gate_reason"])
        self.assertEqual("", queue["running_job_id"])
        self.assertTrue(queue["paused"])
        self.assertTrue(queue["operator_paused"])
        self.assertEqual("NEW_BATCH_READY", queue["operator_pause_reason"])
        self.assertEqual("NEW_BATCH_READY", queue["pause_reason"])

    def test_30_new_jobs_can_enqueue_but_cannot_dispatch_while_ready(self):
        self.run_supersede()
        created, replayed = valid_enqueue(
            self.store, "next-batch", [request(client_job_id="NEXT")]
        )
        self.assertFalse(replayed)
        self.assertEqual("QUEUED", created[0]["status"])
        self.assertIsNone(self.store.claim_next("must-not-dispatch"))

    def test_31_duplicate_request_is_idempotent(self):
        first, replayed1 = self.run_supersede()
        second, replayed2 = self.run_supersede()
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first, second)

    def test_32_expected_revision_mismatch_changes_nothing(self):
        queue_path = self.root / "control" / "queue.json"
        job_paths = [self.root / "control" / "jobs" / f"{job['job_id']}.json" for job in self.jobs]
        before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in [queue_path, *job_paths]]
        bad = dict(self.revisions)
        bad[self.jobs[0]["job_id"]] += 1
        with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
            self.run_supersede(revisions=bad)
        after = [hashlib.sha256(path.read_bytes()).hexdigest() for path in [queue_path, *job_paths]]
        self.assertEqual(before, after)

    def test_33_inherited_and_external_frozen_declarations_are_preserved(self):
        self.run_supersede()
        queue = self.store.queue_snapshot()
        self.assertEqual(["app/inherited.txt"], queue["task_owned_baseline_files"])
        self.assertEqual(["app/inherited.txt"], queue["batch_delta_files"])
        self.assertEqual(["app/frozen.txt"], queue["external_frozen_baseline_files"])

    def test_34_unknown_dirty_refuses_ready_transition_without_mutation(self):
        queue = self.store._read_queue()
        queue["unexpected_runtime_dirty_files"] = ["app/unknown.txt"]
        self.store._write_queue(queue)
        self.expected_queue_revision = self.store.queue_snapshot()["revision"]
        queue_path = self.root / "control" / "queue.json"
        before = hashlib.sha256(queue_path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(QueueError, "UNEXPECTED_RUNTIME_DIRTY_FILES"):
            self.run_supersede()
        self.assertEqual(before, hashlib.sha256(queue_path.read_bytes()).hexdigest())

class BatchProvenance0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = JobStore(self.root / "control")
        self.requests = [request(client_job_id="WBS-A01").to_dict(), request(client_job_id="WBS-A02").to_dict()]
        self.manifest = self.root / "A.json"
        self.manifest.write_text(json.dumps({"jobs": self.requests}), encoding="utf-8")

    def enqueue(self):
        parsed = [request(**item) for item in []]  # keep fixture construction explicit below
        from job_contract import JobRequest
        parsed = [JobRequest.from_mapping(item, allowed_modules=("suc", "sucui")) for item in self.requests]
        return self.store.enqueue_batch(
            "manifest-enqueue", parsed, execution_context=execution_context(),
            source_manifest_path=self.manifest, enqueue_actor="test",
            batch_execution_policy={"build_test_review": {"strict": True}, "dependency": {"fifo": True}},
        )[0]

    def test_26_manifest_hash_is_stored(self):
        jobs = self.enqueue(); queue = self.store._read_queue(); batch = next(v for v in queue["batches"].values() if v.get("batch_id") == jobs[0]["batch_id"])
        self.assertEqual(hashlib.sha256(self.manifest.read_bytes()).hexdigest(), batch["provenance"]["source_manifest_sha256"])

    def test_27_per_job_request_hash_is_stored(self):
        jobs = self.enqueue(); self.assertTrue(all(self.store.get_job(job["job_id"])["request_sha256"] for job in jobs))

    def test_28_sequence_change_is_detected(self):
        jobs = self.enqueue(); a = self.store._read_job(jobs[0]["job_id"]); b = self.store._read_job(jobs[1]["job_id"]); a["sequence"], b["sequence"] = 2, 1; self.store._write_job(a); self.store._write_job(b)
        with self.assertRaisesRegex(QueueError, "BATCH_SEQUENCE_CHANGED"):
            self.store.validate_batch_manifest(jobs[0]["batch_id"])

    def test_29_request_change_is_detected(self):
        jobs = self.enqueue(); job = self.store._read_job(jobs[0]["job_id"]); job["request"]["requirement"] += " drift"; self.store._write_job(job)
        with self.assertRaisesRegex(QueueError, "BATCH_JOB_REQUEST_CHANGED"):
            self.store.validate_batch_manifest(jobs[0]["batch_id"])

    def test_30_manifest_content_change_is_detected(self):
        jobs = self.enqueue(); self.manifest.write_text(json.dumps({"jobs": self.requests, "changed": True}), encoding="utf-8")
        with self.assertRaisesRegex(QueueError, "BATCH_MANIFEST_CONTENT_CHANGED"):
            self.store.validate_batch_manifest(jobs[0]["batch_id"])

    def test_31_u03_u04_are_rejected_from_ab_batch(self):
        bad = self.root / "bad.json"; bad.write_text(json.dumps({"jobs": [{"client_job_id": "URG-U03"}]}), encoding="utf-8")
        with self.assertRaisesRegex(BatchManifestError, "U03_U04_FORBIDDEN_IN_AB_BATCH"):
            prepare_ab_only(bad, phase="A")

    def test_32_unverified_legacy_provenance_blocks_validation(self):
        jobs, _ = valid_enqueue(self.store, "legacy", [request(client_job_id="WBS-A03")])
        with self.assertRaisesRegex(QueueError, "LEGACY_PROVENANCE_UNVERIFIED"):
            self.store.validate_batch_manifest(jobs[0]["batch_id"])

    def test_32b_ab_builder_rejects_invalid_model_routing(self):
        bad = self.root / "routing.json"
        row = dict(self.requests[0])
        row.update({"worker": "droid", "model": "custom:codex-5.3-[chatgpt.com]-0"})
        bad.write_text(json.dumps({"jobs": [row]}), encoding="utf-8")
        with self.assertRaisesRegex(BatchManifestError, "AB_MANIFEST_JOB_CONTRACT_INVALID"):
            prepare_ab_only(bad, phase="A")


class PatchDiagnostics0855Tests(unittest.TestCase):
    TEXT = "apply_patch verification failed: target context not found"

    def test_33_string_only_context_error_is_mismatch(self):
        self.assertEqual(CONTEXT_MISMATCH, classify_patch_context(self.TEXT))

    def test_34_hash_change_with_provenance_is_stale(self):
        self.assertEqual(CONTEXT_STALE, classify_patch_context(
            self.TEXT, pre_read_sha256="a" * 64, pre_apply_sha256="b" * 64,
            mutation_provenance={"invocation_id": "INV-X"},
        ))

    def test_35_diagnostic_artifact_and_stream_hashes_are_stored(self):
        with tempfile.TemporaryDirectory() as temp:
            record = write_patch_diagnostic(temp, {"invocation_id": "INV-X", "stdout": "full-out", "stderr": "full-err"})
            self.assertEqual(record["sha256"], hashlib.sha256(Path(record["path"]).read_bytes()).hexdigest())
            self.assertEqual(hashlib.sha256(b"full-out").hexdigest(), record["evidence"]["stdout_sha256"])
            self.assertEqual("full-out", Path(record["evidence"]["stdout_artifact"]).read_text(encoding="utf-8"))


class RollbackScope0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for module in ("repo1", "repo2"):
            repo = self.root / module; repo.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
            (repo / "target.txt").write_text("base\n", encoding="utf-8")
            (repo / "outside.txt").write_text("outside\n", encoding="utf-8")
            (repo / "inherited.txt").write_text("base\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
        (self.root / "repo1" / "inherited.txt").write_text("previous-job\n", encoding="utf-8")
        self.collector = GitCollector(str(self.root))
        capture_task = TaskState(task_id="TASK-CAP", requirement="x", target_module=["repo1", "repo2"])
        self.baseline, error = self.collector.capture_baseline(capture_task)
        self.assertIsNotNone(self.baseline, error)
        (self.root / "repo1" / "target.txt").write_text("current-job\n", encoding="utf-8")
        self.task = TaskState(
            task_id="TASK-RB", requirement="x", target_module=["repo1"],
            scoped_rollback_targets=["repo1/target.txt"], task_owned_changed_files=["repo1/target.txt"],
            inherited_batch_delta_files=["repo1/inherited.txt"], external_frozen_integrity=True,
        )

    def test_36_task_owned_rollback_succeeds(self):
        ok, _ = self.collector.rollback(self.task, self.baseline)
        self.assertTrue(ok); self.assertEqual("base\n", (self.root / "repo1" / "target.txt").read_text(encoding="utf-8")); self.assertEqual("PASS", self.task.rollback_scope_result["task_owned_rollback"])

    def test_37_unrelated_repository_head_drift_is_reported_separately(self):
        repo2 = self.root / "repo2"; (repo2 / "outside.txt").write_text("drift\n", encoding="utf-8"); subprocess.run(["git", "add", "."], cwd=repo2, check=True); subprocess.run(["git", "commit", "-qm", "drift"], cwd=repo2, check=True)
        ok, _ = self.collector.rollback(self.task, self.baseline)
        self.assertTrue(ok); self.assertEqual(["repo2"], self.task.rollback_scope_result["unrelated_repository_drift"])

    def test_38_inherited_delta_is_preserved(self):
        ok, _ = self.collector.rollback(self.task, self.baseline)
        self.assertTrue(ok); self.assertEqual("previous-job\n", (self.root / "repo1" / "inherited.txt").read_text(encoding="utf-8"))

    def test_39_out_of_scope_file_is_unchanged(self):
        outside = self.root / "repo1" / "outside.txt"; outside.write_text("external\n", encoding="utf-8")
        digest = hashlib.sha256(outside.read_bytes()).hexdigest(); ok, _ = self.collector.rollback(self.task, self.baseline)
        self.assertTrue(ok); self.assertEqual(digest, hashlib.sha256(outside.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
