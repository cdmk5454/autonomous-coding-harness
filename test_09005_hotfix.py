from __future__ import annotations

import copy
import hashlib
import json
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from attempt_checkpoint import AttemptCheckpointError, AttemptCheckpointStore
from job_contract import JobRequest
from job_queue import CANCELLED, FAILED_FINAL, QUEUED, SUCCEEDED, JobStore, QueueError


def context() -> dict[str, object]:
    return {
        "profile_id": "test", "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }


def request(client: str, resources: list[str] | None = None) -> JobRequest:
    return JobRequest.from_mapping({
        "client_job_id": client,
        "requirement": f"{client} acceptance",
        "worker": "codex", "model": "gpt-5.6-sol",
        "reasoning_effort": "medium", "target_modules": ["app"],
        "target_resources": resources or [f"app/{client}.txt"],
        "max_outer_attempts": 1, "depends_on": [],
    }, allowed_modules=("app",))


def integrity(revision: int = 0) -> dict[str, object]:
    return {
        "generation": 1, "active_baseline_id": "",
        "external_frozen_integrity": True,
        "baseline_declaration_integrity": True,
        "unexpected_runtime_dirty_files": [], "revision": revision,
    }


def success(task_id: str = "TASK-X") -> dict[str, object]:
    return {
        "task_id": task_id, "status": "SUCCESS", "stage": "DONE",
        "success": True, "build_status": "PASS", "test_status": "PASS",
        "review_status": "REVIEW_PASS", "verification_status": "VERIFIED",
        "failure_code": "", "secondary_failures": [],
        "changed_files": ["app/a.txt"], "worktree_disposition": "PRESERVED",
    }


class AnchoredReplacementTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = JobStore(Path(self.temp.name) / "control")
        jobs, _ = self.store.enqueue_batch(
            "batch-09005", [request("J07"), request("J08"), request("J09"), request("J10")],
            execution_context=context(),
        )
        self.ids = [job["job_id"] for job in jobs]

    def fail_j07(self) -> dict:
        claimed = self.store.claim_next("owner")
        failed = self.store.record_result(claimed["job_id"], "owner", {
            "status": "FAILED", "success": False, "stage": "DONE",
            "failure_type": "review", "fix_scope": "NON_RETRYABLE",
            "worktree_disposition": "CHECKPOINTED_CLEAN_ROLLBACK",
            "failure_code": "REVIEWER_CODE_FINDING",
        })
        self.assertEqual(failed["status"], FAILED_FINAL)
        return failed

    def corrective(self, *, request_id: str = "corr-1", queue_revision: int | None = None,
                   job_revision: int | None = None, seed: dict | None = None):
        parent = self.store.get_job(self.ids[0])
        queue = self.store.queue_snapshot()
        valid_seed = {
            "valid": True, "parent_job_id": self.ids[0], "parent_client_job_id": "J07",
            "generation": 1, "active_baseline_id": "", "changed_files": ["app/J07.txt"],
            "manifest_path": "checkpoint.json", "manifest_sha256": "e" * 64,
        }
        return self.store.enqueue_anchored_replacement(
            self.ids[0], request("J07-C", ["app/J07.txt", "app/J07SecurityTest.txt"]),
            expected_queue_revision=queue["revision"] if queue_revision is None else queue_revision,
            expected_job_revision=parent["revision"] if job_revision is None else job_revision,
            request_id=request_id, reason="USER_CORRECTIVE", execution_context=context(),
            provenance={"status": "VERIFIED", "source_manifest_sha256": "f" * 64},
            integrity_snapshot=integrity(), checkpoint_seed=seed or valid_seed,
        )

    def test_failed_corrective_is_anchored_and_preserves_siblings_and_parent(self):
        parent = self.fail_j07()
        sibling_before = [copy.deepcopy(self.store.get_job(job_id)) for job_id in self.ids[1:]]
        replacement, replayed = self.corrective()
        self.assertFalse(replayed)
        self.assertEqual(self.store.get_job(parent["job_id"])["status"], FAILED_FINAL)
        self.assertEqual(self.store.queue_snapshot()["effective_order"][:3], [self.ids[0], replacement["job_id"], self.ids[1]])
        for before in sibling_before:
            after = self.store.get_job(before["job_id"])
            self.assertEqual(after["revision"], before["revision"])
            self.assertEqual(after["request"], before["request"])

    def test_corrective_success_releases_original_next_sibling_after_ack(self):
        self.fail_j07()
        replacement, _ = self.corrective()
        # A production start always reconciles durable state first.  The
        # historical FAILED_FINAL parent must not regain the live gate.
        self.store.reconcile_interrupted()
        reconciled = self.store.queue_snapshot()
        self.assertEqual(reconciled["blocked_by_job_id"], "")
        claimed = self.store.claim_next("owner-c")
        self.assertEqual(claimed["job_id"], replacement["job_id"])
        finished = self.store.record_result(claimed["job_id"], "owner-c", success())
        self.assertEqual(finished["status"], SUCCEEDED)
        self.store.acknowledge_success(finished["job_id"], "ack-c", expected_revision=finished["revision"])
        self.store.reconcile_interrupted()
        self.assertEqual(self.store.claim_next("owner-8")["job_id"], self.ids[1])

    def test_corrective_failure_moves_gate_owner(self):
        self.fail_j07()
        replacement, _ = self.corrective()
        claimed = self.store.claim_next("owner-c")
        failed = self.store.record_result(claimed["job_id"], "owner-c", {
            "status": "FAILED", "success": False, "stage": "DONE",
            "failure_type": "review", "fix_scope": "NON_RETRYABLE",
            "worktree_disposition": "CLEAN_ROLLBACK", "failure_code": "REVIEW_FAIL",
        })
        queue = self.store.queue_snapshot()
        self.assertEqual(failed["status"], FAILED_FINAL)
        self.assertEqual(queue["blocked_by_job_id"], replacement["job_id"])
        self.assertEqual(queue["corrective_handoff"]["state"], "FAILED")

    def test_chained_corrective_inherits_root_slot_before_siblings(self):
        self.fail_j07()
        first, _ = self.corrective()
        claimed = self.store.claim_next("owner-c1")
        self.assertEqual(first["job_id"], claimed["job_id"])
        failed = self.store.record_result(claimed["job_id"], "owner-c1", {
            "status": "FAILED", "success": False, "stage": "DONE",
            "failure_type": "review", "fix_scope": "NON_RETRYABLE",
            "worktree_disposition": "CHECKPOINTED_CLEAN_ROLLBACK",
            "failure_code": "REVIEW_FAIL",
        })
        queue = self.store.queue_snapshot()
        second, _ = self.store.enqueue_anchored_replacement(
            failed["job_id"], request("J07-C2", ["app/J07.txt", "app/J07SecurityTest.txt"]),
            expected_queue_revision=queue["revision"],
            expected_job_revision=failed["revision"], request_id="corr-2",
            reason="USER_CORRECTIVE", execution_context=context(),
            provenance={"status": "VERIFIED", "source_manifest_sha256": "f" * 64},
            integrity_snapshot=integrity(), checkpoint_seed={
                "valid": True, "parent_job_id": failed["job_id"],
                "parent_client_job_id": "J07-C", "generation": 1,
                "active_baseline_id": "", "changed_files": ["app/J07.txt"],
                "manifest_path": "checkpoint.json", "manifest_sha256": "e" * 64,
            },
        )
        effective = self.store.queue_snapshot()["effective_order"]
        self.assertEqual(effective[:4], [self.ids[0], first["job_id"], second["job_id"], self.ids[1]])

    def test_stale_cas_has_zero_mutation_and_duplicate_is_idempotent(self):
        self.fail_j07()
        before = self.store.queue_snapshot()
        parent_before = self.store.get_job(self.ids[0])
        with self.assertRaisesRegex(QueueError, "QUEUE_REVISION_CONFLICT"):
            self.corrective(queue_revision=before["revision"] - 1)
        self.assertEqual(self.store.queue_snapshot()["revision"], before["revision"])
        self.assertEqual(self.store.get_job(self.ids[0])["revision"], parent_before["revision"])
        with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
            self.corrective(job_revision=parent_before["revision"] - 1)
        original_queue_revision = self.store.queue_snapshot()["revision"]
        original_job_revision = self.store.get_job(self.ids[0])["revision"]
        replacement, _ = self.corrective(
            queue_revision=original_queue_revision, job_revision=original_job_revision
        )
        replay, replayed = self.corrective(
            queue_revision=original_queue_revision, job_revision=original_job_revision
        )
        self.assertTrue(replayed)
        self.assertEqual(replay["job_id"], replacement["job_id"])

    def test_running_job_rejects_replacement(self):
        self.store.claim_next("owner")
        queue = self.store.queue_snapshot()
        parent = self.store.get_job(self.ids[1])
        with self.assertRaisesRegex(QueueError, "ACTIVE_HARNESS_PROCESS"):
            self.store.enqueue_anchored_replacement(
                self.ids[1], request("J08-R"), expected_queue_revision=queue["revision"],
                expected_job_revision=parent["revision"], request_id="running-replace",
                reason="replace", execution_context=context(), provenance={"status": "VERIFIED"},
                integrity_snapshot=integrity(),
            )

    def test_single_queued_replacement_preserves_siblings_and_position(self):
        old = self.store.get_job(self.ids[2])
        siblings = {job_id: copy.deepcopy(self.store.get_job(job_id)) for job_id in (self.ids[1], self.ids[3])}
        queue = self.store.queue_snapshot()
        replacement, _ = self.store.enqueue_anchored_replacement(
            old["job_id"], request("J09-V1"), expected_queue_revision=queue["revision"],
            expected_job_revision=old["revision"], request_id="replace-j09",
            reason="USER_REPLACED_CONTRACT_BEFORE_EXECUTION", execution_context=context(),
            provenance={"status": "VERIFIED"}, integrity_snapshot=integrity(),
        )
        old_after = self.store.get_job(old["job_id"])
        self.assertEqual(old_after["status"], CANCELLED)
        self.assertEqual(old_after["request"], old["request"])
        effective = self.store.queue_snapshot()["effective_order"]
        self.assertLess(effective.index(replacement["job_id"]), effective.index(self.ids[3]))
        for job_id, before in siblings.items():
            after = self.store.get_job(job_id)
            self.assertEqual((after["revision"], after["request"]), (before["revision"], before["request"]))

    def test_preparing_replacement_recovers_after_process_boundary(self):
        old = self.store.get_job(self.ids[2])
        queue = self.store.queue_snapshot()
        with patch.object(
            self.store, "_finish_prepared_replacement", side_effect=RuntimeError("fault")
        ), self.assertRaises(RuntimeError):
            self.store.enqueue_anchored_replacement(
                old["job_id"], request("J09-V1"), expected_queue_revision=queue["revision"],
                expected_job_revision=old["revision"], request_id="recover-j09",
                reason="USER_REPLACED_CONTRACT_BEFORE_EXECUTION", execution_context=context(),
                provenance={"status": "VERIFIED"}, integrity_snapshot=integrity(),
            )
        recovered = JobStore(Path(self.temp.name) / "control")
        snapshot = recovered.queue_snapshot()
        replacement_id = snapshot["replacement_anchors"][old["job_id"]]["replacement_job_id"]
        self.assertEqual(recovered.get_job(old["job_id"])["status"], CANCELLED)
        self.assertEqual(recovered.get_job(replacement_id)["status"], QUEUED)

    def test_queued_replacement_rejects_task_invocation_or_delta(self):
        for field, value in (
            ("task_ids", ["TASK-X"]),
            ("worker_invocation_count", 1),
            ("candidate_id", "CAND-X"),
            ("last_result", {"changed_files": ["app/Q.txt"]}),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                store = JobStore(Path(temp) / "control")
                jobs, _ = store.enqueue_batch("one", [request("Q")], execution_context=context())
                old = jobs[0]
                edited = store._read_job(old["job_id"])
                edited[field] = value
                store._write_job(edited)
                queue = store.queue_snapshot()
                current = store.get_job(old["job_id"])
                with self.assertRaisesRegex(QueueError, "QUEUED_REPLACEMENT_NOT_PRISTINE"):
                    store.enqueue_anchored_replacement(
                        old["job_id"], request("Q2"), expected_queue_revision=queue["revision"],
                        expected_job_revision=current["revision"], request_id=f"bad-{field}",
                        reason="replace", execution_context=context(), provenance={"status": "VERIFIED"},
                        integrity_snapshot=integrity(),
                    )

    def test_checkpoint_integrity_inputs_fail_closed(self):
        self.fail_j07()
        base = {
            "valid": True, "parent_job_id": self.ids[0], "generation": 1,
            "active_baseline_id": "", "changed_files": ["app/J07.txt"],
        }
        for key, value, code in (
            ("parent_job_id", "JOB-20000101-000000-00000000", "CHECKPOINT_JOB_MISMATCH"),
            ("generation", 2, "CHECKPOINT_GENERATION_MISMATCH"),
            ("active_baseline_id", "wrong", "CHECKPOINT_BASELINE_MISMATCH"),
            ("changed_files", ["outside/x"], "CHECKPOINT_SCOPE_MISMATCH"),
        ):
            seed = dict(base); seed[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(QueueError, code):
                self.corrective(request_id=f"bad-seed-{key}", seed=seed)


class CorrectiveCheckpointTest(unittest.TestCase):
    def fixture(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        workspace, control = root / "workspace", root / "control"
        workspace.mkdir(); control.mkdir()
        target = workspace / "app" / "x.txt"
        target.parent.mkdir()
        target.write_bytes(b"a\r\nb\r\n")
        store = AttemptCheckpointStore(workspace, control)
        before, after = b"a\nb\n", b"changed\n"
        before_sha, after_sha = hashlib.sha256(before).hexdigest(), hashlib.sha256(after).hexdigest()
        store._put_blob(before); store._put_blob(after)
        manifest_dir = store.root / "BASE" / "JOB-PARENT" / "TASK-OLD" / "CHK-TEST"
        manifest_dir.mkdir(parents=True)
        manifest = {
            "schema_version": 2, "kind": "PRE_ROLLBACK_ATTEMPT_CHECKPOINT",
            "checkpoint_id": "CHK-TEST", "generation": 1,
            "active_baseline_id": "BASE", "job_id": "JOB-PARENT",
            "client_job_id": "J07", "task_owned_changed_files": ["app/x.txt"],
            "inherited_batch_delta_files": {"app/x.txt": after_sha},
            "review_findings": "focused fix",
            "files": [{
                "path": "app/x.txt", "before_state": "FILE", "after_state": "FILE",
                "before_sha256": before_sha, "after_sha256": after_sha,
                "before_blob": before_sha, "after_blob": after_sha,
                "before_mode": 0, "after_mode": 0,
            }],
        }
        payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        manifest_path = manifest_dir / "manifest.json"
        manifest_path.write_bytes(payload)
        (manifest_dir / "manifest.sha256").write_text(hashlib.sha256(payload).hexdigest(), encoding="ascii")
        return store, target, manifest_path

    def valid_integrity(self):
        return {
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [],
        }

    def test_cross_job_seed_accepts_eol_only_prestate_and_restores_scoped_candidate(self):
        store, target, manifest = self.fixture()
        seed = store.validate_corrective_seed(
            manifest, parent_job_id="JOB-PARENT", parent_client_job_id="J07",
            generation=1, active_baseline_id="BASE", allowed_resources=["app/x.txt"],
            integrity_snapshot=self.valid_integrity(),
        )
        self.assertEqual(seed["eol_only_prestate_files"], ["app/x.txt"])
        task = SimpleNamespace(
            queue_generation=1, active_baseline_id="BASE", job_id="JOB-NEW",
            client_job_id="J07-C", external_frozen_integrity=True,
            baseline_declaration_integrity=True, unexpected_external_dirty_files=[],
        )
        restored = store.restore_for_retry(
            task, manifest, source_job_id="JOB-PARENT", source_client_job_id="J07"
        )
        self.assertEqual(restored["files"], ["app/x.txt"])
        self.assertEqual(target.read_bytes(), b"changed\n")

    def test_seed_rejects_baseline_external_unexpected_scope_and_hash_mismatch(self):
        cases = (
            ("baseline", {"active_baseline_id": "WRONG"}, "CHECKPOINT_BASELINE_MISMATCH"),
            ("external", {"integrity_snapshot": {"external_frozen_integrity": False, "baseline_declaration_integrity": True, "unexpected_runtime_dirty_files": []}}, "CHECKPOINT_EXTERNAL_INTEGRITY_FAILED"),
            ("unexpected", {"integrity_snapshot": {"external_frozen_integrity": True, "baseline_declaration_integrity": True, "unexpected_runtime_dirty_files": ["x"]}}, "CHECKPOINT_UNEXPECTED_DIRTY"),
            ("scope", {"allowed_resources": ["app/y.txt"]}, "CHECKPOINT_SCOPE_MISMATCH"),
        )
        for name, override, code in cases:
            store, _, manifest = self.fixture()
            kwargs = {
                "parent_job_id": "JOB-PARENT", "parent_client_job_id": "J07",
                "generation": 1, "active_baseline_id": "BASE",
                "allowed_resources": ["app/x.txt"], "integrity_snapshot": self.valid_integrity(),
            }
            kwargs.update(override)
            with self.subTest(name=name), self.assertRaisesRegex(AttemptCheckpointError, code):
                store.validate_corrective_seed(manifest, **kwargs)
        store, _, manifest = self.fixture()
        manifest.write_bytes(manifest.read_bytes() + b" ")
        with self.assertRaisesRegex(AttemptCheckpointError, "CHECKPOINT_MANIFEST_HASH_MISMATCH"):
            store.validate_corrective_seed(
                manifest, parent_job_id="JOB-PARENT", parent_client_job_id="J07",
                generation=1, active_baseline_id="BASE", allowed_resources=["app/x.txt"],
                integrity_snapshot=self.valid_integrity(),
            )


if __name__ == "__main__":
    unittest.main()
