from __future__ import annotations

import hashlib
import json
import harness_temp as tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from chatgpt_control import ChatGPTControlService
from job_contract import EXECUTION_CONTEXT_KEYS, JobRequest
from job_queue import (
    BATCH_FINAL_COMMIT,
    BLOCKED,
    QUEUED,
    JobStore,
    QueueError,
)


def canonical_sha(value) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def context(policy: str) -> dict[str, object]:
    return {
        "profile_id": "e4u",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "4" * 64,
        "profile_snapshot_sha256": "5" * 64,
        "policy_snapshot_sha256": policy * 64,
        "workspace_identity_sha256": "8" * 64,
    }


def request(name: str, resource: str) -> JobRequest:
    return JobRequest(
        client_job_id=name,
        requirement=f"implement {name}",
        worker="codex",
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        target_modules=("app",),
        target_resources=(resource,),
        commit_summary=name,
        max_outer_attempts=2,
    )


def strict_success(changed: str) -> dict[str, object]:
    return {
        "task_id": "TASK-U02",
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "verification_status": "VERIFIED",
        "build_status": "PASS",
        "test_status": "NOT_REQUIRED",
        "review_status": "REVIEW_PASS",
        "failure_code": "",
        "secondary_failures": [],
        "changed_files": [changed],
        "worktree_disposition": "PRESERVED",
    }


class HarnessDouble:
    def __init__(self, root: Path, current: dict[str, object]):
        self.root = root
        self.current = current
        self.leased = False

    def validate_configuration(self):
        return {
            **self.current,
            "working_dir": str(self.root / "workspace"),
            "profile_dir": str(self.root / "profile"),
            "task_root": str(self.root / ".tasks"),
            "available_modules": ["app"],
            "commit_policy": BATCH_FINAL_COMMIT,
            "cumulative_worktree": True,
            "auto_continue_without_commit": True,
            "pre_job_snapshot_supported": True,
            "scoped_rollback_supported": True,
            "final_commit_required": True,
        }

    def cumulative_policy_status(self, active_baseline_id="", batch_delta_files=(), read_only=False):
        return {
            "commit_policy": BATCH_FINAL_COMMIT,
            "cumulative_worktree": True,
            "auto_continue_without_commit": True,
            "batch_owned_delta_supported": True,
            "pre_job_snapshot_supported": True,
            "scoped_rollback_supported": True,
            "final_commit_required": True,
            "active_baseline_id": active_baseline_id,
            "task_owned_baseline_files": ["app/predecessor.txt"],
            "external_frozen_baseline_files": [],
            "batch_delta_files": list(batch_delta_files),
            "unexpected_runtime_dirty_files": [],
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
        }

    def probe_execution_idle(self):
        return True

    def acquire_execution_lease(self):
        if self.leased:
            return False
        self.leased = True
        return True

    def release_execution_lease(self):
        self.leased = False


class ProfileRevalidationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / ".control").mkdir()
        (self.root / ".tasks").mkdir()
        (self.root / "workspace" / "app").mkdir(parents=True)
        (self.root / "profile").mkdir()
        self.pred = self.root / "workspace" / "app" / "predecessor.txt"
        self.pred.write_text("u02-preserved", encoding="utf-8")
        self.old = context("a")
        self.current = context("b")
        self.store = JobStore(self.root / ".control")
        self.store.activate_cumulative_policy(1, {
            "commit_policy": BATCH_FINAL_COMMIT,
            "cumulative_worktree": True,
            "auto_continue_without_commit": True,
            "batch_owned_delta_supported": True,
            "pre_job_snapshot_supported": True,
            "scoped_rollback_supported": True,
            "final_commit_required": True,
            "active_baseline_id": "BASELINE-TEST",
            "task_owned_baseline_files": ["app/predecessor.txt"],
            "external_frozen_baseline_files": [],
            "unexpected_runtime_dirty_files": [],
        })
        self.jobs, _ = self.store.enqueue_batch(
            "profile-revalidation-fixture-v1",
            [
                request("U02", "app/predecessor.txt"),
                request("U03", "app/u03.txt"),
                request("U04", "app/u04.txt"),
            ],
            execution_context=self.old,
        )
        first = self.store.claim_next("owner-u02")
        self.store.record_result(first["job_id"], "owner-u02", strict_success("app/predecessor.txt"))
        first = self.store.get_job(first["job_id"])
        self.store.acknowledge_success(
            first["job_id"], "continue-u02-v1",
            expected_revision=first["revision"], batch_id=first["batch_id"],
        )
        target = self.store.claim_next("owner-u03")
        self.store.record_blocked(target["job_id"], "owner-u03", "PROFILE_DRIFT")
        self.target_id = target["job_id"]
        self.harness = HarnessDouble(self.root, self.current)
        self.control = ChatGPTControlService(
            self.root / "workspace", self.root / "profile",
            control_root=self.root / ".control", task_root=self.root / ".tasks",
            state_base=self.root, harness=self.harness, store=self.store,
        )
        self.evidence = self._evidence()
        self.evidence_sha = canonical_sha(self.evidence)

    def tearDown(self):
        self.temp.cleanup()

    def _evidence(self):
        job = self.store.get_job(self.target_id)
        successor = self.store.get_job(self.jobs[2]["job_id"])
        resource_specs = (
            ("project-context/SKILL.md", "agents/skills/project-context/SKILL.md",
             "1732b91a49f4d09c2de6037307157b94822b0fd9dd3fdce618a6953f607f0312",
             "90374486685b99633c7b01b92b01f6e463679ff9132c57febf9fd515aa1739f4"),
            ("testing/SKILL.md", "agents/skills/testing/SKILL.md",
             "8c191ab0f10dd0810940830931d89445cfa4ea95772e726cc4789ef90caf6cb4",
             "155535e190971a3d3ef62edacd1aaa0b8395e09daaaa4571ef2f5bba867b787c"),
        )
        resources = []
        for path, artifact_path, old_sha, new_sha in resource_specs:
            resources.append({
                "path": path,
                "artifact_path": artifact_path,
                "old_sha256": old_sha,
                "new_sha256": new_sha,
                "classification": "APPROVED_HARNESS_CONTEXT_CHANGE",
                "semantic_change": False,
                "release_provenance": [{
                    "version": "0.8.5",
                    "artifact_path": artifact_path,
                    "sha256": new_sha,
                }],
            })
        return {
            "job_id": self.target_id,
            "old_execution_context_sha256": canonical_sha(self.old),
            "new_execution_context_sha256": canonical_sha(self.current),
            "profile_id": "e4u",
            "requirement_sha256": hashlib.sha256(job["request"]["requirement"].encode()).hexdigest(),
            "request_sha256": canonical_sha(job["request"]),
            "job_identity": {
                "job_id": self.target_id,
                "client_job_id": job["client_job_id"],
                "batch_id": job["batch_id"],
                "sequence": job["sequence"],
            },
            "target_modules": ["app"],
            "workspace_identity": {"old": "8" * 64, "new": "8" * 64, "same": True},
            "manifest_identity": {"old": "4" * 64, "new": "4" * 64, "same": True},
            "resources": resources,
            "classification_counts": {"A": 2, "B": 0, "C": 0, "D": 0, "E": 0},
            "baseline_id": "BASELINE-TEST",
            "generation": 1,
            "predecessor_hashes": {
                "app/predecessor.txt": hashlib.sha256(self.pred.read_bytes()).hexdigest()
            },
            "successor_request_hashes": {
                successor["job_id"]: {
                    "client_job_id": successor["client_job_id"],
                    "sequence": successor["sequence"],
                    "request_sha256": canonical_sha(successor["request"]),
                }
            },
            "integrity": {
                "external_frozen": True,
                "baseline_declaration": True,
                "unexpected_dirty": [],
            },
        }

    def _mutate(self, request_id="revalidate-test-v1", **overrides):
        args = {
            "job_id": self.target_id,
            "expected_revision": self.store.get_job(self.target_id)["revision"],
            "expected_old_execution_context": self.old,
            "classification_evidence": self.evidence,
            "classification_evidence_sha256": self.evidence_sha,
            "reason": "approved semantically-equivalent Agent Harness upgrade",
            "request_id": request_id,
            "start_immediately": False,
        }
        args.update(overrides)
        return self.control.revalidate_blocked_job_profile(**args)

    def test_positive_rebind_is_one_revision_and_stays_paused(self):
        before = self.store.get_job(self.target_id)
        preview = self.control.preview_blocked_job_profile_revalidation(
            self.target_id, before["revision"], self.old, self.evidence, self.evidence_sha
        )
        self.assertEqual("ELIGIBLE", preview["decision"])
        result = self._mutate(expected_revision=before["revision"])
        after = self.store.get_job(self.target_id)
        queue = self.store.queue_snapshot()
        self.assertEqual(QUEUED, after["status"])
        self.assertEqual(self.current, after["execution_context"])
        self.assertEqual(before["revision"] + 1, after["revision"])
        self.assertTrue(queue["paused"])
        self.assertEqual("", queue["running_job_id"])
        self.assertEqual(1, sum(e["event"] == "PROFILE_DRIFT_REVALIDATED" for e in after["history"]))
        self.assertFalse(result["supervisor"]["running"])

    def test_duplicate_request_is_revision_and_event_idempotent(self):
        old_revision = self.store.get_job(self.target_id)["revision"]
        first = self._mutate(expected_revision=old_revision)
        second = self._mutate(expected_revision=old_revision)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        after = self.store.get_job(self.target_id)
        self.assertEqual(old_revision + 1, after["revision"])
        self.assertEqual(1, sum(e["event"] == "PROFILE_DRIFT_REVALIDATED" for e in after["history"]))

    def test_restart_persists_rebound_context_without_dispatch(self):
        self._mutate()
        restarted = JobStore(self.root / ".control")
        self.assertEqual(self.current, restarted.get_job(self.target_id)["execution_context"])
        self.assertEqual(QUEUED, restarted.get_job(self.target_id)["status"])
        self.assertTrue(restarted.queue_snapshot()["paused"])

    def test_fail_closed_matrix(self):
        cases = {
            "stale": {"expected_revision": 999},
            "old-context": {"expected_old_execution_context": context("c")},
        }
        for name, overrides in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(QueueError):
                    self._mutate(request_id=f"reject-{name}", **overrides)
        for key, value in (("D", 1), ("E", 1)):
            evidence = json.loads(json.dumps(self.evidence))
            evidence["classification_counts"]["A"] = 1
            evidence["classification_counts"][key] = value
            with self.subTest(classification=key), self.assertRaisesRegex(
                QueueError, "SEMANTIC_OR_UNKNOWN"
            ):
                self._mutate(
                    request_id=f"reject-{key}", classification_evidence=evidence,
                    classification_evidence_sha256=canonical_sha(evidence),
                )
        evidence = json.loads(json.dumps(self.evidence))
        evidence["resources"][0]["new_sha256"] = "f" * 64
        with self.assertRaisesRegex(QueueError, "RELEASE_PROVENANCE_INVALID"):
            self._mutate(
                request_id="reject-provenance",
                classification_evidence=evidence,
                classification_evidence_sha256=canonical_sha(evidence),
            )

    def test_duplicate_request_with_changed_arguments_is_rejected(self):
        revision = self.store.get_job(self.target_id)["revision"]
        self._mutate(expected_revision=revision)
        with self.assertRaisesRegex(QueueError, "IDEMPOTENCY_CONFLICT"):
            self._mutate(expected_revision=revision, reason="different reason")

    def test_all_operator_preconditions_fail_closed(self):
        base_queue = self.control._queue_snapshot()
        base_job = self.store.get_job(self.target_id)

        def preview(*, queue_changes=None, job_changes=None, current_changes=None,
                    evidence_changes=None, old_context=None, revision=None,
                    supervisor_running=False):
            queue = {**base_queue, **(queue_changes or {})}
            job = {**base_job, **(job_changes or {})}
            current = {**self.current, **(current_changes or {})}
            evidence = json.loads(json.dumps(self.evidence))
            for key, value in (evidence_changes or {}).items():
                evidence[key] = value
            original_get = self.store.get_job

            def get_job(job_id):
                return job if job_id == self.target_id else original_get(job_id)

            with patch.object(self.control, "_queue_snapshot", return_value=queue), \
                 patch.object(self.store, "get_job", side_effect=get_job), \
                 patch.object(self.control.supervisor, "is_running", return_value=supervisor_running), \
                 patch.object(self.harness, "validate_configuration", return_value={
                     **self.harness.validate_configuration(), **current,
                 }):
                return self.control.preview_blocked_job_profile_revalidation(
                    self.target_id,
                    base_job["revision"] if revision is None else revision,
                    self.old if old_context is None else old_context,
                    evidence,
                    canonical_sha(evidence),
                )

        cases = [
            ("queue-not-paused", {"queue_changes": {"paused": False}}),
            ("running-job", {"queue_changes": {"running_job_id": "JOB-RUNNING"}}),
            ("supervisor-running", {"supervisor_running": True}),
            ("autonomous", {"queue_changes": {"execution_mode": "AUTONOMOUS"}}),
            ("not-blocker", {"queue_changes": {"blocked_by_job_id": self.jobs[0]["job_id"]}}),
            ("not-blocked", {"job_changes": {"status": QUEUED}}),
            ("wrong-failure", {"job_changes": {"last_result": {"failure_code": "OTHER"}}}),
            ("stale-revision", {"revision": base_job["revision"] - 1}),
            ("old-context-mismatch", {"old_context": context("c")}),
            ("profile-change", {"current_changes": {"profile_id": "other"}}),
            ("workspace-change", {"current_changes": {"workspace_identity_sha256": "9" * 64}}),
            ("manifest-change", {"current_changes": {"profile_manifest_sha256": "6" * 64}}),
            ("profile-facts-change", {"current_changes": {"profile_snapshot_sha256": "7" * 64}}),
            ("request-hash", {"evidence_changes": {"request_sha256": "0" * 64}}),
            ("requirement-hash", {"evidence_changes": {"requirement_sha256": "0" * 64}}),
            ("target-modules", {"evidence_changes": {"target_modules": ["other"]}}),
            ("baseline", {"queue_changes": {"active_baseline_id": "OTHER"}}),
            ("integrity-false", {"queue_changes": {"external_frozen_integrity": False}}),
            ("unexpected-dirty", {"queue_changes": {"unexpected_runtime_dirty_files": ["app/x"]}}),
            ("successor", {"evidence_changes": {"successor_request_hashes": {}}}),
            ("task-started", {"job_changes": {
                "current_claim_task_started": True,
                "last_result": {"failure_code": "PROFILE_DRIFT"},
            }}),
        ]
        for name, kwargs in cases:
            with self.subTest(name=name), self.assertRaises(QueueError):
                preview(**kwargs)

        original = self.pred.read_text(encoding="utf-8")
        self.pred.write_text("tampered", encoding="utf-8")
        try:
            with self.assertRaisesRegex(QueueError, "PREDECESSOR_CHANGED"):
                preview()
        finally:
            self.pred.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
