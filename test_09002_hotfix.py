from __future__ import annotations

import hashlib
import json
import subprocess
import harness_temp as tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import control_paths
from build_test_runner import BuildTestResult
from chatgpt_control import ChatGPTControlService
from control_paths import ControlPathError, atomic_state_write
from harness_service import HarnessService
from job_contract import JobRequest
from job_queue import AWAITING_QA, SUCCEEDED, JobStore, QueueError
from manager import Manager, ManagerResult
from notify import terminal_notification_projection
from policy_catalog import (
    HARNESS_CAUSED,
    classify_failure_origin,
    is_control_state_write_failure,
    worker_remediation_allowed,
)
from queue_supervisor import QueueSupervisor
from review_policy import assess_review_risk
from task_state import TaskState
from worker import QA_REQUEST_PREFIX, parse_qa_request


def _context() -> dict[str, object]:
    return {
        "profile_id": "test",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }


def _job(client: str, *, depends_on: tuple[str, ...] = ()) -> JobRequest:
    return JobRequest.from_mapping({
        "client_job_id": client,
        "requirement": f"{client} implementation",
        "worker": "codex",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "medium",
        "target_modules": ["app"],
        "target_resources": [f"app/{client}.txt"],
        "max_outer_attempts": 1,
        "depends_on": list(depends_on),
    }, allowed_modules=("app",))


def _success(task_id: str) -> dict[str, object]:
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "build_status": "PASS",
        "test_status": "NOT_REQUIRED",
        "review_status": "REVIEW_PASS",
        "verification_status": "VERIFIED",
        "failure_code": "",
        "secondary_failures": [],
        "changed_files": ["app/change.txt"],
        "worktree_disposition": "PRESERVED",
    }


class AtomicStateHotfixTest(unittest.TestCase):
    def test_same_path_concurrent_writes_are_serialized_and_json_is_parseable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / "task.json"
            original = control_paths.os.replace
            guard = threading.Lock()
            active = 0
            maximum = 0

            def observed(source, destination):
                nonlocal active, maximum
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.002)
                try:
                    return original(source, destination)
                finally:
                    with guard:
                        active -= 1

            errors: list[Exception] = []

            def writer(number: int) -> None:
                try:
                    atomic_state_write(
                        target,
                        json.dumps({"writer": number}).encode(),
                        root=root,
                        field="task_state",
                    )
                except Exception as exc:  # pragma: no cover - assertion evidence
                    errors.append(exc)

            with patch("control_paths.os.replace", side_effect=observed):
                threads = [threading.Thread(target=writer, args=(i,)) for i in range(40)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            self.assertEqual([], errors)
            self.assertEqual(1, maximum)
            self.assertIn("writer", json.loads(target.read_text(encoding="utf-8")))

    def test_different_paths_are_not_globally_serialized(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = control_paths.os.replace
            barrier = threading.Barrier(2)
            errors: list[Exception] = []

            def observed(source, destination):
                barrier.wait(timeout=2)
                return original(source, destination)

            def writer(name: str) -> None:
                try:
                    atomic_state_write(
                        root / name, b"{}", root=root, field="task_state"
                    )
                except Exception as exc:
                    errors.append(exc)

            with patch("control_paths.os.replace", side_effect=observed):
                threads = [threading.Thread(target=writer, args=(name,)) for name in ("a.json", "b.json")]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            self.assertEqual([], errors)

    def _replace_retry(self, winerror: int, failures: int) -> tuple[int, Path]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        target = root / "state.json"
        original = control_paths.os.replace
        calls = 0

        def transient(source, destination):
            nonlocal calls
            calls += 1
            if calls <= failures:
                error = PermissionError("transient")
                error.winerror = winerror
                raise error
            return original(source, destination)

        with patch("control_paths.os.replace", side_effect=transient), patch(
            "control_paths.time.sleep"
        ):
            atomic_state_write(target, b"ok", root=root, field="task_state")
        return calls, target

    def test_winerror_5_and_32_retry_then_succeed(self):
        for code in (5, 32):
            with self.subTest(code=code):
                calls, target = self._replace_retry(code, 2)
                self.assertEqual(3, calls)
                self.assertEqual(b"ok", target.read_bytes())

    def test_retry_exhaustion_preserves_stable_error(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = 0

            def denied(*_args):
                nonlocal calls
                calls += 1
                error = PermissionError("busy")
                error.winerror = 5
                raise error

            with patch("control_paths.os.replace", side_effect=denied), patch(
                "control_paths.time.sleep"
            ), self.assertRaises(ControlPathError) as raised:
                atomic_state_write(root / "state.json", b"x", root=root, field="task_state")
            self.assertEqual(4, calls)
            self.assertEqual("CONTROL_STATE_WRITE_FAILED", raised.exception.code)
            self.assertEqual("task_state", raised.exception.field)

    def test_unrelated_oserror_is_not_retried_and_escape_stays_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "state"
            root.mkdir()
            with patch("control_paths.os.replace", side_effect=OSError("disk")) as replace:
                with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_WRITE_FAILED"):
                    atomic_state_write(root / "x.json", b"x", root=root, field="task_state")
            self.assertEqual(1, replace.call_count)
            with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_PATH_ESCAPE"):
                atomic_state_write(root.parent / "escape.json", b"x", root=root, field="task_state")


class FailureRoutingHotfixTest(unittest.TestCase):
    def test_control_failure_precedes_user_qa_status_flag(self):
        origin = classify_failure_origin(
            "CONTROL_STATE_WRITE_FAILED",
            failure_stage="CONTROL",
            failure_type="infrastructure",
            user_qa_required=True,
        )
        self.assertEqual(HARNESS_CAUSED, origin)
        self.assertFalse(worker_remediation_allowed(origin, "CONTROL_STATE_WRITE_FAILED"))

    def test_control_failure_result_preserves_code_field_and_batch_hold(self):
        task = TaskState("TASK-20260901-000001", "test")
        task.changed_files = ["app/a.txt"]
        result = HarnessService._control_failure_result(
            task,
            "CONTROL_STATE_WRITE_FAILED",
            mutation_uncertain=True,
            control_field="task_state",
        )
        self.assertFalse(result.success)
        self.assertEqual("HARNESS_CAUSED", task.failure_origin)
        self.assertEqual("task_state", task.control_field)
        self.assertEqual("BATCH", task.hold_scope)
        self.assertEqual("SAFETY_INTEGRITY", task.qa_type)
        self.assertEqual("PRESERVED", task.worktree_disposition)

    def test_exact_legacy_incident_is_recoverable_but_generic_control_is_not(self):
        self.assertTrue(is_control_state_write_failure(
            "CONTROL_EXECUTION_ERROR",
            failure_stage="CONTROL",
            worker_stderr="ControlPathError: CONTROL_STATE_WRITE_FAILED: task_state",
        ))
        self.assertFalse(is_control_state_write_failure(
            "CONTROL_EXECUTION_ERROR",
            failure_stage="CONTROL",
            worker_stderr="unrelated",
        ))


class ControlRecoveryHotfixTest(unittest.TestCase):
    def _preview_fixture(self, root: Path):
        workspace = root / "workspace"
        tasks = root / ".tasks"
        workspace.mkdir()
        changed = "app/current.txt"
        inherited = "app/predecessor.txt"
        (workspace / "app").mkdir()
        (workspace / changed).write_bytes(b"current delta")
        (workspace / inherited).write_bytes(b"predecessor delta")
        snapshot = root / "snapshot.json"
        snapshot.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT",
            "snapshot_id": "SNAP-ONE",
            "baseline_id": "BASE-ONE",
            "files": [{
                "path": inherited,
                "sha256": hashlib.sha256(b"predecessor delta").hexdigest(),
            }],
        }), encoding="utf-8")
        task = TaskState("TASK-20260901-000002", "recover")
        task.job_id = "JOB-20260901-000000-00000001"
        task.changed_files = [changed]
        task.task_owned_changed_files = [changed]
        task.inherited_batch_delta_files = [inherited]
        task.pre_job_snapshot_id = "SNAP-ONE"
        task.pre_job_snapshot_manifest = str(snapshot)
        task.active_baseline_id = "BASE-ONE"
        task.failure_stage = "CONTROL"
        task.failure_code = "CONTROL_EXECUTION_ERROR"
        task.worker_stderr = "CONTROL_STATE_WRITE_FAILED: task_state"
        task.review_status = "PENDING"
        task.build = {"status": "PASS", "success": True}
        task.build_evidence = {"details": {"structured_evidence_valid": True}}
        task.save(tasks)
        job = {
            "job_id": task.job_id,
            "status": AWAITING_QA,
            "revision": 5,
            "last_result": {"task_id": task.task_id, "failure_stage": "CONTROL"},
        }
        queue = {
            "running_job_id": "",
            "blocked_by_job_id": task.job_id,
            "gate_reason": AWAITING_QA,
        }
        service = ChatGPTControlService.__new__(ChatGPTControlService)
        service.harness = SimpleNamespace(working_dir=workspace, task_root=tasks)
        service.store = SimpleNamespace(queue_snapshot=lambda: dict(queue))
        service._canonical_integrity_snapshot = lambda _queue: {
            "active_baseline_id": "BASE-ONE",
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [changed],
        }
        return service, job, task, workspace

    def test_preview_accepts_preserved_delta_and_requires_build_rebuild(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            service, job, _task, _workspace = self._preview_fixture(Path(temp))
            preview = service._preview_control_recovery(job, 5)
            self.assertTrue(preview["eligible"])
            self.assertEqual("BUILD", preview["recovery_stage"])
            self.assertFalse(preview["worker_will_run"])
            self.assertEqual([], preview["unexpected_external_dirty_files"])

    def test_preview_accepts_snapshot_omitted_git_clean_predecessor_by_repo_identity(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            service, job, task, workspace = self._preview_fixture(root)
            repo = workspace / "app"
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "fixture@example.invalid"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Fixture"], cwd=repo, check=True
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            index_tree = subprocess.run(
                ["git", "write-tree"], cwd=repo, check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            snapshot_path = Path(task.pre_job_snapshot_manifest)
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            snapshot["files"] = []
            snapshot["repos"] = [{
                "module": "app",
                "repo": str(repo),
                "head": head,
                "index_tree": index_tree,
                "tracked_files": ["app/current.txt", "app/predecessor.txt"],
            }]
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            (repo / "current.txt").write_bytes(b"current delta after snapshot")
            preview = service._preview_control_recovery(job, 5)
            self.assertEqual(
                "PRE_JOB_REPO_IDENTITY",
                preview["inherited_predecessor_provenance"]["app/predecessor.txt"],
            )
            (repo / "predecessor.txt").write_bytes(b"drift")
            with self.assertRaisesRegex(
                QueueError, "CONTROL_RECOVERY_PREDECESSOR_MISMATCH"
            ):
                service._preview_control_recovery(job, 5)

    def test_preview_rejects_stale_source_external_and_baseline_mismatch(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            service, job, task, _workspace = self._preview_fixture(root)
            task.changed_file_sha256 = {task.changed_files[0]: "0" * 64}
            task.save(service.harness.task_root)
            with self.assertRaisesRegex(QueueError, "CONTROL_RECOVERY_SOURCE_HASH_STALE"):
                service._preview_control_recovery(job, 5)

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            service, job, _task, _workspace = self._preview_fixture(Path(temp))
            service._canonical_integrity_snapshot = lambda _queue: {
                "active_baseline_id": "BASE-ONE",
                "external_frozen_integrity": False,
                "baseline_declaration_integrity": True,
                "unexpected_runtime_dirty_files": ["app/current.txt"],
            }
            with self.assertRaisesRegex(QueueError, "CONTROL_RECOVERY_WORKTREE_INTEGRITY_INVALID"):
                service._preview_control_recovery(job, 5)

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            service, job, task, _workspace = self._preview_fixture(Path(temp))
            data = json.loads(Path(task.pre_job_snapshot_manifest).read_text(encoding="utf-8"))
            data["baseline_id"] = "OTHER"
            Path(task.pre_job_snapshot_manifest).write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaisesRegex(QueueError, "CONTROL_RECOVERY_BASELINE_MISMATCH"):
                service._preview_control_recovery(job, 5)

    def test_manager_control_recovery_rebuilds_without_worker_or_git(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "app").mkdir()
            target = root / "app" / "a.txt"
            target.write_bytes(b"delta")
            task = TaskState("TASK-20260901-000003", "recover")
            task.changed_files = ["app/a.txt"]
            task.git_diff = "+delta"
            task.target_module = ["app"]
            task.scope_authority = "JOB_CONTRACT"
            task.active_baseline_id = "BASE"
            manager = Manager(str(root), "", max_retry=1, max_workers=1, task_root=root / ".tasks")
            build_calls: list[dict] = []
            manager.build_test_runner = SimpleNamespace(run=lambda **kwargs: (
                build_calls.append(kwargs)
                or BuildTestResult(True, "PASS", "NOT_REQUIRED", details={})
            ))
            manager._validate_structured_build_evidence = lambda *_args: (True, "")
            manager._log_stage = lambda *_args: None
            review_calls: list[str] = []
            manager.run_review_only = lambda current: (
                review_calls.append(current.task_id) or ManagerResult(success=True)
            )
            try:
                result = manager.run_control_recovery(task)
            finally:
                manager.shutdown()
            self.assertTrue(result.success)
            self.assertEqual(1, len(build_calls))
            self.assertEqual([task.task_id], review_calls)
            self.assertFalse(task.control_recovery_evidence["worker_invoked"])
            self.assertFalse(task.control_recovery_evidence["git_regenerated"])

    def test_control_review_result_is_revision_safe_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch("control-recovery", [_job("J1")], execution_context=_context())
            owner = "owner"
            running = store.claim_next(owner)
            store.mark_task_started(running["job_id"], owner, "TASK-RECOVERY")
            awaiting = store.record_result(running["job_id"], owner, {
                "task_id": "TASK-RECOVERY",
                "status": AWAITING_QA,
                "stage": AWAITING_QA,
                "success": False,
                "failure_stage": "CONTROL",
                "failure_code": "CONTROL_STATE_WRITE_FAILED",
                "failure_origin": "HARNESS_CAUSED",
                "failure_type": "infrastructure",
                "hold_scope": "BATCH",
                "worktree_disposition": "PRESERVED",
            })
            revision = awaiting["revision"]
            passed = {
                **_success("TASK-RECOVERY"),
                "recovery_kind": "CONTROL_STATE_WRITE_RECOVERY",
                "worker_invoked": False,
            }
            updated, replayed = store.record_review_only_result(
                awaiting["job_id"], expected_revision=revision,
                request_id="control-recovery-one", result=passed,
            )
            self.assertFalse(replayed)
            self.assertEqual(SUCCEEDED, updated["status"])
            replay, replayed = store.record_review_only_result(
                awaiting["job_id"], expected_revision=revision,
                request_id="control-recovery-one", result=passed,
            )
            self.assertTrue(replayed)
            self.assertEqual(updated["revision"], replay["revision"])
            with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
                store.record_review_only_result(
                    awaiting["job_id"], expected_revision=revision,
                    request_id="control-recovery-stale", result=passed,
                )


class ConditionalQAHotfixTest(unittest.TestCase):
    def test_requirement_keywords_do_not_trigger_qa_without_structured_signal(self):
        self.assertEqual({}, parse_qa_request(
            "QA-sensitive business contract needs review in ordinary prose"
        ))

    def test_structured_signal_is_validated_and_preserved(self):
        payload = {
            "required": True,
            "qa_type": "DB_CONTRACT",
            "hold_scope": "DEPENDENCY_CHAIN",
            "reason": "authoritative null mapping is missing",
            "evidence": ["DB catalog has no NULL contract"],
            "decision_independent_work_complete": True,
        }
        parsed = parse_qa_request(QA_REQUEST_PREFIX + " " + json.dumps(payload))
        self.assertEqual("DB_CONTRACT", parsed["qa_type"])
        self.assertTrue(parsed["decision_independent_work_complete"])

    def test_job_hold_allows_independent_job_and_batch_hold_stops_all(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "qa-job-hold", [_job("B"), _job("C")], execution_context=_context()
            )
            owner = "owner"
            b = store.claim_next(owner)
            store.mark_task_started(b["job_id"], owner, "TASK-B")
            store.record_result(b["job_id"], owner, {
                **_success("TASK-B"), "status": AWAITING_QA, "success": False,
                "qa_type": "HUMAN_ACCEPTANCE", "hold_scope": "JOB",
                "machine_verified": True, "candidate_id": "CAND-" + "A" * 24,
                "candidate_manifest": "candidate.json",
                "worktree_disposition": "CLEAN_ROLLBACK",
            })
            self.assertFalse(store.queue_snapshot()["paused"])
            self.assertEqual("C", store.claim_next(owner)["client_job_id"])

        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            store.enqueue_batch("qa-batch-hold", [_job("B"), _job("C")], execution_context=_context())
            owner = "owner"
            b = store.claim_next(owner)
            store.mark_task_started(b["job_id"], owner, "TASK-B")
            store.record_result(b["job_id"], owner, {
                "task_id": "TASK-B", "status": AWAITING_QA, "stage": AWAITING_QA,
                "success": False, "qa_type": "SAFETY_INTEGRITY", "hold_scope": "BATCH",
                "failure_origin": "HARNESS_CAUSED", "failure_type": "infrastructure",
                "worktree_disposition": "PRESERVED",
            })
            self.assertTrue(store.queue_snapshot()["paused"])
            self.assertIsNone(store.claim_next(owner))


class TerminalNotificationHotfixTest(unittest.TestCase):
    def test_terminal_attempt_is_after_durable_state_and_duplicate_is_suppressed(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            store.enqueue_batch("notify", [_job("J1")], execution_context=_context())
            owner = "owner"
            running = store.claim_next(owner)
            store.mark_task_started(running["job_id"], owner, "TASK-N")
            job = store.record_result(running["job_id"], owner, {
                "task_id": "TASK-N", "status": AWAITING_QA, "stage": AWAITING_QA,
                "success": False, "failure_code": "CONTROL_STATE_WRITE_FAILED",
                "failure_origin": "HARNESS_CAUSED", "failure_type": "infrastructure",
                "hold_scope": "BATCH", "worktree_disposition": "PRESERVED",
            })
            supervisor = QueueSupervisor(store, SimpleNamespace())
            with patch("queue_supervisor.send_notify_evidence", return_value={
                "delivery_status": "FAILED", "failure_code": "TELEGRAM_DOWN"
            }) as notify:
                first = supervisor.notify_terminal_transition(job, store.queue_snapshot())
                second = supervisor.notify_terminal_transition(job, store.queue_snapshot())
            self.assertFalse(first["duplicate_suppressed"])
            self.assertTrue(second["duplicate_suppressed"])
            self.assertEqual(1, notify.call_count)
            self.assertEqual(AWAITING_QA, store.get_job(job["job_id"])["status"])
            records = store.get_job(job["job_id"])["terminal_notifications"]
            self.assertEqual("ATTEMPTED", next(iter(records.values()))["delivery_status"])

    def test_projection_distinguishes_independent_continuation_and_batch_stop(self):
        queue = {"batches": {}, "order": ["J"], "gate_reason": ""}
        non_global = terminal_notification_projection({
            "job_id": "J", "status": AWAITING_QA, "hold_scope": "JOB",
            "candidate_id": "CAND-" + "A" * 24, "last_result": {},
        }, queue)
        self.assertEqual("INDEPENDENT_JOBS_CONTINUE", non_global["continuation"])
        self.assertTrue(non_global["user_action_required"])
        batch = terminal_notification_projection({
            "job_id": "J", "status": "BLOCKED",
            "last_result": {"failure_origin": "HARNESS_CAUSED"},
        }, queue)
        self.assertEqual("", batch["hold_scope"])
        self.assertEqual("AUTOMATIC_CONTINUATION_STOPPED", batch["continuation"])


class ReviewRiskHotfixTest(unittest.TestCase):
    def _risk(self, requirement: str):
        return assess_review_risk(
            requirement=requirement,
            target_modules=["app"],
            changed_files=["app/a.txt"],
            git_diff="+x",
            build_status="PASS",
            test_status="NOT_REQUIRED",
            test_required=False,
        )

    def test_authoritative_scope_is_not_auth_but_real_auth_terms_stay_high(self):
        neutral = self._risk("Authoritative scope and business data contract")
        self.assertNotIn("AUTHORIZATION_OR_PII", neutral.hard_flags)
        for text in (
            "authorization", "authenticate", "authentication", "permission",
            "role", "access control", "권한", "개인정보", "PII", "security",
        ):
            with self.subTest(text=text):
                decision = self._risk(text)
                self.assertIn("AUTHORIZATION_OR_PII", decision.hard_flags)
                self.assertIn(decision.risk_level, {"HIGH", "CRITICAL"})


if __name__ == "__main__":
    unittest.main()
