from __future__ import annotations

import json
import hashlib
import io
import os
import sys
import harness_temp as tempfile
import threading
import time
import types
import unittest
from dataclasses import replace
from concurrent.futures import Future
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from chatgpt_control import ChatGPTControlService
from chatgpt_control_mcp import (
    HTTP_SURFACE_INSPECTOR,
    HTTP_SURFACE_TUNNEL,
    INSPECTOR_LOCK_NAME,
    WRITABLE_HTTP_ENABLE_ENV,
    WRITABLE_LOCK_NAME,
    _is_loopback,
    _check_control,
    _process_lock_path,
    build_mcp_server,
    main as mcp_main,
)
from control_operator import LocalOperatorResolver, _public_inspection
from control_paths import (
    ControlPathError,
    validate_control_state_roots,
    validate_control_state_roots_read_only,
    validate_no_reparse_tree,
)
from harness_service import HarnessService, HarnessServiceError
from job_contract import JobContractError, JobRequest, compose_retry_requirement
from job_queue import (
    AWAITING_ENRICHMENT,
    AWAITING_QA,
    BLOCKED,
    CANCELLED,
    FAILED_FINAL,
    INTERRUPTED,
    QUEUED,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    SUCCESS_ACK_REQUIRED,
    MANUAL_RESOLUTION_ACCEPT_PRESERVED,
    MANUAL_RESOLUTION_CONFIRMATIONS,
    MANUAL_RESOLUTION_VERIFIED_CLEAN,
    JobStore,
    QueueError,
)
from manager import Manager, ManagerResult
from queue_supervisor import QueueSupervisor
from project_profile import ModuleProfile, ProfileError, ProjectProfile
from runtime_snapshot import RuntimeSnapshotError
from runtime_snapshot import _prepare_snapshot_root
from planner import _build_planner_prompt
from tester import _build_tester_prompt
from run import validate_runtime_rules
from runtime_safety import (
    _is_reparse_entry,
    safe_print,
    scrub_secrets,
    trusted_executable,
)
from task_state import TaskState
from worker import CodexWorker, DroidWorker, build_prompt
from reviewer import load_review_rules


MODULES = ("sample-service", "sample-web")


def execution_context(**overrides):
    context = {
        "profile_id": "sample-profile",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }
    context.update(overrides)
    return context


def valid_enqueue(store: JobStore, *args, **kwargs):
    kwargs.setdefault("execution_context", execution_context())
    return JobStore.enqueue_batch(store, *args, **kwargs)


def request(**overrides):
    data = {
        "client_job_id": "client-1",
        "requirement": "학과 통계 화면을 수정하고 범위 밖 변경은 금지한다.",
        "worker": "codex",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "medium",
        "target_modules": ["sample-service", "sample-web"],
        "max_outer_attempts": 2,
    }
    data.update(overrides)
    return JobRequest.from_mapping(data, allowed_modules=MODULES)


def success_result(task_id="TASK-20260826-120000"):
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "verification_status": "VERIFIED",
        "failure_code": "",
        "changed_files": ["sample-service/A.java"],
        "is_rolled_back": False,
    }


def failed_result(task_id="TASK-20260826-120001", *, rolled_back=True, changed=True):
    return {
        "task_id": task_id,
        "status": "FAILED",
        "stage": "DONE",
        "success": False,
        "verification_status": "FAILED",
        "failure_stage": "BUILD",
        "failure_code": "BUILD_FAILED",
        "failure_reason": "compile failed",
        "failure_type": "build",
        "fix_scope": "RETRYABLE",
        "build_status": "FAIL",
        "test_status": "SKIPPED",
        "review_status": "REVIEW_PASS",
        "changed_files": ["sample-service/A.java"] if changed else [],
        "is_rolled_back": rolled_back,
        "worktree_disposition": (
            "CLEAN_ROLLBACK" if rolled_back else "PRESERVED" if changed else "NO_DELTA"
        ),
    }


class JobContractTests(unittest.TestCase):
    def test_defaults_and_strict_fields(self):
        parsed = JobRequest.from_mapping(
            {"requirement": "수정", "target_modules": ["sample-service"]},
            allowed_modules=MODULES,
        )
        self.assertEqual("codex", parsed.worker)
        self.assertEqual("gpt-5.6-sol", parsed.model)
        self.assertEqual(("sample-service",), parsed.target_modules)
        with self.assertRaisesRegex(JobContractError, "UNKNOWN_FIELD"):
            JobRequest.from_mapping(
                {"requirement": "수정", "working_dir": "D:\\escape"},
                allowed_modules=MODULES,
            )

    def test_trusted_executable_skips_writable_project_shadow(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            trusted = root / "trusted"
            project.mkdir()
            trusted.mkdir()
            executable_name = "codex.exe" if os.name == "nt" else "codex"
            shadow = project / executable_name
            real = trusted / executable_name
            shadow.write_text("shadow", encoding="utf-8")
            real.write_text("real", encoding="utf-8")
            shadow.chmod(0o755)
            real.chmod(0o755)
            resolved = trusted_executable(
                "codex",
                forbidden_root=project,
                source={
                    "KKM_ENFORCE_TRUSTED_EXECUTABLES": "1",
                    "PATH": os.pathsep.join((str(project), str(trusted))),
                },
            )
            self.assertEqual(real.resolve(), Path(resolved))

    def test_trusted_executable_rejects_reparse_parent_escape(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            outside = root / "outside-tools"
            project.mkdir()
            outside.mkdir()
            executable = outside / "codex"
            executable.write_text("outside", encoding="utf-8")
            executable.chmod(0o755)
            simulated_reparse = outside.absolute()
            with patch(
                "runtime_safety._is_reparse_entry",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaises(FileNotFoundError):
                    trusted_executable(
                        "codex",
                        forbidden_root=project,
                        source={
                            "KKM_ENFORCE_TRUSTED_EXECUTABLES": "1",
                            "PATH": str(outside),
                        },
                    )

    def test_windows_reparse_attribute_is_recognized(self):
        fake_stat = SimpleNamespace(st_file_attributes=0x400)
        with patch.object(Path, "lstat", return_value=fake_stat), patch.object(
            Path, "is_symlink", return_value=False
        ), patch.object(Path, "is_junction", return_value=False, create=True):
            self.assertTrue(_is_reparse_entry(Path("C:/trusted/codex.exe")))

    def test_worker_fallbacks_resolve_trusted_executable_before_subprocess(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            trusted = root / "trusted"
            project.mkdir()
            trusted.mkdir()
            executable_suffix = ".exe" if os.name == "nt" else ""
            for name in ("droid", "codex"):
                shadow = project / f"{name}{executable_suffix}"
                executable = trusted / f"{name}{executable_suffix}"
                shadow.write_text("shadow", encoding="utf-8")
                executable.write_text("trusted", encoding="utf-8")
                shadow.chmod(0o755)
                executable.chmod(0o755)

            completed = SimpleNamespace(returncode=0, stdout="", stderr="")
            environment = {
                **os.environ,
                "KKM_ENFORCE_TRUSTED_EXECUTABLES": "1",
                "PATH": os.pathsep.join((str(project), str(trusted))),
            }
            for worker, expected in (
                (DroidWorker(timeout_config=None), trusted / f"droid{executable_suffix}"),
                (CodexWorker(timeout_config=None), trusted / f"codex{executable_suffix}"),
            ):
                with self.subTest(worker=type(worker).__name__), patch.dict(
                    os.environ, environment, clear=True
                ), patch("worker.build_prompt", return_value="prompt"), patch(
                    "worker.codex_host_pair_matches", return_value=True
                ), patch(
                    "worker.subprocess.run", return_value=completed
                ) as run_mock:
                    result = worker.execute("requirement", str(project))
                    self.assertTrue(result.success)
                    self.assertEqual(expected.resolve(), Path(run_mock.call_args.args[0][0]))

    def test_worker_model_effort_and_module_allowlists(self):
        invalid = (
            {"requirement": "x", "worker": "shell"},
            {"requirement": "x", "model": "arbitrary"},
            {"requirement": "x", "reasoning_effort": "ultra"},
            {"requirement": "x", "target_modules": ["core"]},
            {"requirement": "x", "max_outer_attempts": 4},
        )
        for item in invalid:
            with self.subTest(item=item), self.assertRaises(JobContractError):
                JobRequest.from_mapping(item, allowed_modules=MODULES)

    def test_retry_prompt_is_bounded_and_uses_safe_summary_only(self):
        composed = compose_retry_requirement(
            "원 요구사항",
            {
                "task_id": "TASK-1",
                "failure_code": "BUILD_FAILED",
                "failure_reason": "OPENAI_API_KEY=secret-value",
                "raw_stdout": "must-not-appear",
            },
            "컴파일 오류 위치를 먼저 확인한다.",
            2,
        )
        self.assertIn("원 요구사항", composed)
        self.assertIn("BUILD_FAILED", composed)
        self.assertNotIn("must-not-appear", composed)
        self.assertNotIn("secret-value", composed)
        self.assertIn("UNTRUSTED_FAILURE_EVIDENCE", composed)

    def test_authorization_headers_and_real_token_prefixes_are_fully_redacted(self):
        github_token = "github" + "_pat_" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
        oauth_token = "gh" + "o_" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        openai_token = "sk-" + "proj-" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        raw = (
            "Authorization: Bearer opaque-token-value\n"
            "Proxy-Authorization=Basic dXNlcjpwYXNz\n"
            "curl -H \"Authorization: Bearer command-context-secret\"\n"
            f"{github_token}\n"
            f"{oauth_token}\n"
            "glpat-ABCDEFGHIJKLMNOPQRSTUVWXYZ\n"
            f"{openai_token}"
        )
        cleaned = scrub_secrets(raw, env={})
        for secret in (
            "opaque-token-value",
            "dXNlcjpwYXNz",
            "command-context-secret",
            "github_pat_",
            "gho_",
            "glpat-",
            "sk-proj-",
        ):
            self.assertNotIn(secret, cleaned)

    def test_token_shaped_identifiers_are_rejected(self):
        token_shaped_id = "sk-" + "abcdefghijklmnopqrstuvwxyz"
        with self.assertRaisesRegex(JobContractError, "SENSITIVE_IDENTIFIER_FORBIDDEN"):
            JobRequest.from_mapping(
                {
                    "requirement": "수정",
                    "client_job_id": token_shaped_id,
                },
                allowed_modules=MODULES,
            )


class JobStoreTests(unittest.TestCase):
    def test_enqueue_requires_exact_execution_context(self):
        with self.assertRaises(JobContractError) as raised:
            self.store.enqueue_batch("missing-context", [request()])
        self.assertEqual("EXECUTION_CONTEXT_MISSING", raised.exception.code)
        self.assertEqual(0, self.store.queue_snapshot()["total"])

    def test_never_started_legacy_invalid_context_is_quarantined(self):
        jobs, _ = self.enqueue(key="legacy-invalid-context")
        job_id = jobs[0]["job_id"]
        self.store.claim_next("legacy-owner")
        self.store.record_blocked(job_id, "legacy-owner", "PROFILE_DRIFT")
        job = self.store._read_job(job_id)
        job["execution_context"] = {}
        self.store._write_job(job)

        restarted = JobStore(self.store.root)
        quarantined = restarted.get_job(job_id)
        self.assertEqual(CANCELLED, quarantined["status"])
        self.assertEqual(
            "LEGACY_EXECUTION_CONTEXT_QUARANTINED",
            quarantined["last_result"]["failure_code"],
        )
        snapshot = restarted.queue_snapshot()
        self.assertFalse(snapshot["paused"])
        self.assertEqual("", snapshot["blocked_by_job_id"])

    def test_generated_job_ids_are_unique_within_one_prepared_batch(self):
        generated = [
            "JOB-20260826-120000-aaaaaaaa",
            "JOB-20260826-120000-aaaaaaaa",
            "JOB-20260826-120000-bbbbbbbb",
        ]
        with patch.object(self.store, "_new_job_id", side_effect=generated):
            jobs, replayed = self.enqueue(
                key="in-batch-id-collision",
                requests=[
                    request(client_job_id="first"),
                    request(client_job_id="second"),
                ],
            )
        self.assertFalse(replayed)
        self.assertEqual(2, len({job["job_id"] for job in jobs}))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = JobStore(Path(self.temp.name) / ".control")

    def tearDown(self):
        self.temp.cleanup()

    def enqueue(self, key="batch-1", requests=None):
        return valid_enqueue(self.store, key, requests or [request()])

    def test_batch_idempotency_and_conflict(self):
        first, replayed = self.enqueue()
        second, replayed_again = self.enqueue()
        self.assertFalse(replayed)
        self.assertTrue(replayed_again)
        self.assertEqual(first[0]["job_id"], second[0]["job_id"])
        with self.assertRaisesRegex(QueueError, "IDEMPOTENCY_CONFLICT"):
            self.enqueue(requests=[request(requirement="다른 요구사항")])
        queue_text = self.store.queue_path.read_text(encoding="utf-8")
        self.assertNotIn("batch-1", queue_text)

    def test_prepared_batch_recovers_job_write_and_final_commit_crashes(self):
        requests = [request(client_job_id="a"), request(client_job_id="b")]
        for failure_point in ("job_write", "final_queue"):
            with self.subTest(failure_point=failure_point), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / ".control"
                store = JobStore(root)
                if failure_point == "job_write":
                    context = patch.object(
                        store, "_write_job", side_effect=OSError("job write crash")
                    )
                else:
                    original = store._write_queue
                    calls = 0

                    def fail_final(queue):
                        nonlocal calls
                        calls += 1
                        if calls == 2:
                            raise OSError("final queue crash")
                        return original(queue)

                    context = patch.object(store, "_write_queue", side_effect=fail_final)
                with context, self.assertRaises(OSError):
                    valid_enqueue(store, "durable-batch", requests)

                prepared_queue = json.loads(
                    (root / "queue.json").read_text(encoding="utf-8")
                )
                batch = next(iter(prepared_queue["batches"].values()))
                prepared_ids = list(batch["job_ids"])
                self.assertEqual("PREPARING", batch["state"])

                restarted = JobStore(root)
                replay, replayed = valid_enqueue(restarted, "durable-batch", requests)
                self.assertTrue(replayed)
                self.assertEqual(prepared_ids, [item["job_id"] for item in replay])
                self.assertEqual(2, len(list(restarted.jobs_dir.glob("JOB-*.json"))))
                self.assertEqual(2, restarted.queue_snapshot()["total"])

    def test_unreferenced_legacy_job_file_blocks_store_startup(self):
        jobs, _ = self.enqueue()
        orphan_id = "JOB-20260826-235959-deadbeef"
        payload = self.store._read_job(jobs[0]["job_id"])
        payload["job_id"] = orphan_id
        (self.store.jobs_dir / f"{orphan_id}.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        with self.assertRaisesRegex(QueueError, "CONTROL_STATE_ORPHAN_JOB"):
            JobStore(self.store.root)

    def test_legacy_raw_batch_index_migrates_without_duplicate_job(self):
        jobs, _ = self.enqueue(key="legacy-batch")
        queue = self.store._read_queue()
        hashed = next(iter(queue["batches"]))
        queue["batches"]["legacy-batch"] = queue["batches"].pop(hashed)
        self.store._write_queue(queue)
        replay, replayed = self.enqueue(key="legacy-batch")
        self.assertTrue(replayed)
        self.assertEqual(jobs[0]["job_id"], replay[0]["job_id"])
        raw = self.store.queue_path.read_text(encoding="utf-8")
        self.assertNotIn('"legacy-batch"', raw)

    def test_restart_eagerly_hashes_secret_shaped_legacy_keys(self):
        jobs, _ = self.enqueue(key="initial-batch")
        secret = "sk-" + "abcdefghijklmnopqrstuvwxyz123456"
        queue = self.store._read_queue()
        metadata = next(iter(queue["batches"].values()))
        queue["batches"] = {secret: metadata}
        self.store._write_queue(queue)
        job = self.store._read_job(jobs[0]["job_id"])
        job["resume_actions"] = {secret: {"queued_at": "legacy"}}
        self.store._write_job(job)

        restarted = JobStore(self.store.root)
        self.assertEqual(1, restarted.queue_snapshot()["total"])
        hashed = f"sha256:{hashlib.sha256(secret.encode()).hexdigest()}"
        for path in (
            restarted.queue_path,
            restarted._job_path(jobs[0]["job_id"]),
        ):
            persisted = path.read_text(encoding="utf-8")
            self.assertNotIn(secret, persisted)
            self.assertIn(hashed, persisted)

    def test_sensitive_action_identifier_is_rejected_before_persistence(self):
        jobs, _ = self.enqueue()
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-121000")
        self.store.record_result(job_id, "owner-1", success_result())
        secret_id = "github" + "_pat_" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
        with self.assertRaisesRegex(JobContractError, "SENSITIVE_IDENTIFIER_FORBIDDEN"):
            self.store.acknowledge_success(job_id, secret_id)
        raw = "\n".join(
            path.read_text(encoding="utf-8") for path in self.store.root.rglob("*.json")
        )
        self.assertNotIn(secret_id, raw)

    def test_fifo_success_then_next_claim(self):
        jobs, _ = self.enqueue(requests=[request(client_job_id="a"), request(client_job_id="b")])
        first = self.store.claim_next("owner-1")
        self.assertEqual(jobs[0]["job_id"], first["job_id"])
        self.store.mark_task_started(first["job_id"], "owner-1", "TASK-20260826-120000")
        done = self.store.record_result(first["job_id"], "owner-1", success_result())
        self.assertEqual(SUCCEEDED, done["status"])
        self.assertIsNone(self.store.claim_next("owner-1"))
        snapshot = self.store.queue_snapshot()
        self.assertEqual(SUCCESS_ACK_REQUIRED, snapshot["gate_reason"])
        self.store.acknowledge_success(first["job_id"], "success-ack-1")
        second = self.store.claim_next("owner-1")
        self.assertEqual(jobs[1]["job_id"], second["job_id"])

    def test_failed_rolled_back_waits_for_idempotent_enrichment(self):
        jobs, _ = self.enqueue()
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120001")
        failed = self.store.record_result(job_id, "owner-1", failed_result())
        self.assertEqual(AWAITING_ENRICHMENT, failed["status"])
        self.assertTrue(self.store.queue_snapshot()["paused"])

        retried, replayed = self.store.retry_with_supplement(
            job_id, "실패한 컴파일 구문만 바로잡는다.", "retry-action-1"
        )
        self.assertFalse(replayed)
        self.assertEqual(QUEUED, retried["status"])
        self.assertIn("외부 제어 보강 재실행 2", retried["current_requirement"])
        replay, replayed = self.store.retry_with_supplement(
            job_id, "실패한 컴파일 구문만 바로잡는다.", "retry-action-1"
        )
        self.assertTrue(replayed)
        self.assertEqual(retried["job_id"], replay["job_id"])

    def test_preserved_changes_require_manual_decision(self):
        jobs, _ = self.enqueue()
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120001")
        failed = self.store.record_result(
            job_id,
            "owner-1",
            failed_result(rolled_back=False, changed=True),
        )
        self.assertEqual(AWAITING_QA, failed["status"])
        self.assertEqual(
            "BUILD_FAILED",
            failed["last_result"]["failure_code"],
        )
        self.assertEqual(
            "PRESERVED_CHANGE_REQUIRES_MANUAL_DECISION",
            failed["last_result"]["queue_decision_code"],
        )
        with self.assertRaisesRegex(QueueError, "JOB_NOT_AWAITING_ENRICHMENT"):
            self.store.retry_with_supplement(job_id, "보강", "retry-action-1")

    def test_budget_exhaustion_is_final(self):
        jobs, _ = self.enqueue(requests=[request(max_outer_attempts=1)])
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120001")
        failed = self.store.record_result(job_id, "owner-1", failed_result())
        self.assertEqual(FAILED_FINAL, failed["status"])

    def test_restart_reconciliation_is_fail_closed(self):
        jobs, _ = self.enqueue()
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        interrupted = self.store.reconcile_interrupted()
        self.assertEqual([job_id], interrupted)
        self.assertEqual(INTERRUPTED, self.store.get_job(job_id)["status"])
        self.assertTrue(self.store.queue_snapshot()["paused"])

    def test_unicode_roundtrip_and_no_partial_json(self):
        jobs, _ = self.enqueue(requests=[request(requirement="학과명 변경")])
        path = self.store.jobs_dir / f"{jobs[0]['job_id']}.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual("학과명 변경", data["request"]["requirement"])
        self.assertEqual([], list(self.store.root.rglob("*.tmp")))

    def test_success_requires_explicit_success_flag_and_done_stage(self):
        jobs, _ = self.enqueue(requests=[request(max_outer_attempts=1)])
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120003")
        invalid = success_result()
        invalid["success"] = False
        result = self.store.record_result(job_id, "owner-1", invalid)
        self.assertNotEqual(SUCCEEDED, result["status"])

    def test_supplements_accumulate_across_outer_attempts(self):
        jobs, _ = self.enqueue(requests=[request(max_outer_attempts=3)])
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120004")
        self.store.record_result(job_id, "owner-1", failed_result())
        self.store.retry_with_supplement(job_id, "FIRST_FIX", "retry-1")
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120005")
        self.store.record_result(job_id, "owner-1", failed_result())
        third, _ = self.store.retry_with_supplement(job_id, "SECOND_FIX", "retry-2")
        self.assertIn("FIRST_FIX", third["current_requirement"])
        self.assertIn("SECOND_FIX", third["current_requirement"])

    def test_operator_pause_survives_retry_gate_resolution(self):
        jobs, _ = self.enqueue()
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120006")
        self.store.record_result(job_id, "owner-1", failed_result())
        self.store.pause("maintenance")
        self.store.retry_with_supplement(job_id, "FIX", "retry-1")
        snapshot = self.store.queue_snapshot()
        self.assertTrue(snapshot["paused"])
        self.assertTrue(snapshot["operator_paused"])
        self.assertEqual("maintenance", snapshot["operator_pause_reason"])

    def test_restart_repairs_terminal_job_partial_commit_without_rerun(self):
        jobs, _ = self.enqueue(requests=[request(), request(client_job_id="b")])
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-120007")
        with patch.object(self.store, "_write_queue", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.store.record_result(job_id, "owner-1", success_result())

        restarted = JobStore(self.store.root)
        self.assertEqual([], restarted.reconcile_interrupted())
        snapshot = restarted.queue_snapshot()
        self.assertEqual("", snapshot["running_job_id"])
        self.assertEqual(job_id, snapshot["blocked_by_job_id"])
        self.assertEqual(SUCCESS_ACK_REQUIRED, snapshot["gate_reason"])
        self.assertIsNone(restarted.claim_next("owner-2"))

    def test_action_replays_finish_queue_commit_after_crash(self):
        jobs, _ = self.enqueue(key="ack-case")
        ack_id = jobs[0]["job_id"]
        self.store.claim_next("owner-ack")
        self.store.mark_task_started(ack_id, "owner-ack", "TASK-20260826-121001")
        self.store.record_result(ack_id, "owner-ack", success_result())
        with patch.object(self.store, "_write_queue", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.store.acknowledge_success(ack_id, "ack-crash")
        _, replayed = self.store.acknowledge_success(ack_id, "ack-crash")
        self.assertTrue(replayed)
        self.assertNotEqual(ack_id, self.store.queue_snapshot()["blocked_by_job_id"])

        jobs, _ = self.enqueue(key="retry-case")
        retry_id = jobs[0]["job_id"]
        self.store.claim_next("owner-retry")
        self.store.mark_task_started(retry_id, "owner-retry", "TASK-20260826-121002")
        self.store.record_result(retry_id, "owner-retry", failed_result())
        with patch.object(self.store, "_write_queue", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.store.retry_with_supplement(retry_id, "FIX", "retry-crash")
        _, replayed = self.store.retry_with_supplement(retry_id, "FIX", "retry-crash")
        self.assertTrue(replayed)
        self.assertNotEqual(retry_id, self.store.queue_snapshot()["blocked_by_job_id"])

    def test_resume_and_skip_replays_finish_queue_commit_after_crash(self):
        jobs, _ = self.enqueue(key="resume-case")
        resume_id = jobs[0]["job_id"]
        self.store.claim_next("owner-resume")
        self.store.record_blocked(resume_id, "owner-resume", "LOCAL_TOOL_MISSING")
        with patch.object(self.store, "_write_queue", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.store.resume_job(resume_id, "resume-crash")
        resumed, replayed = self.store.resume_job(resume_id, "resume-crash")
        self.assertTrue(replayed)
        self.assertEqual(QUEUED, resumed["status"])
        self.assertFalse(self.store.queue_snapshot()["paused"])

        self.store.claim_next("owner-skip")
        self.store.mark_task_started(resume_id, "owner-skip", "TASK-20260826-121003")
        exhausted = failed_result()
        exhausted["worktree_disposition"] = "CLEAN_ROLLBACK"
        # This Job has already consumed its only configured outer attempt.
        job = self.store._read_job(resume_id)
        job["max_outer_attempts"] = 1
        self.store._write_job(job)
        self.store.record_result(resume_id, "owner-skip", exhausted)
        self.assertEqual(FAILED_FINAL, self.store.get_job(resume_id)["status"])
        with patch.object(self.store, "_write_queue", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.store.skip_failed_job(resume_id, "operator confirmed", "skip-crash")
        skipped, replayed = self.store.skip_failed_job(
            resume_id, "operator confirmed", "skip-crash"
        )
        self.assertTrue(replayed)
        self.assertEqual(SKIPPED, skipped["status"])
        self.assertFalse(self.store.queue_snapshot()["paused"])

    def test_pre_task_recovery_retry_has_persistent_budget(self):
        jobs, _ = self.enqueue(
            key="recovery-budget",
            requests=[request(max_outer_attempts=2)],
        )
        job_id = jobs[0]["job_id"]
        for index in (1, 2):
            self.store.claim_next(f"owner-{index}")
            self.store.record_blocked(job_id, f"owner-{index}", "PROFILE_DRIFT")
            resumed, replayed = self.store.resume_job(job_id, f"recovery-{index}")
            self.assertFalse(replayed)
            self.assertEqual(QUEUED, resumed["status"])
        self.store.claim_next("owner-3")
        self.store.record_blocked(job_id, "owner-3", "PROFILE_DRIFT")
        exhausted, replayed = self.store.resume_job(job_id, "recovery-3")
        self.assertFalse(replayed)
        self.assertEqual(FAILED_FINAL, exhausted["status"])
        self.assertEqual(2, exhausted["recovery_attempt_count"])
        self.assertEqual(
            "RECOVERY_RETRY_BUDGET_EXHAUSTED",
            exhausted["last_result"]["queue_decision_code"],
        )

    def test_blocked_job_at_outer_attempt_limit_becomes_failed_final(self):
        jobs, _ = self.enqueue(
            key="outer-attempt-budget",
            requests=[request(max_outer_attempts=1)],
        )
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-1")
        self.store.mark_task_started(job_id, "owner-1", "TASK-20260826-121099")
        self.store.record_blocked(job_id, "owner-1", "LOCAL_TOOL_MISSING")
        exhausted, replayed = self.store.resume_job(job_id, "resume-over-budget")
        self.assertFalse(replayed)
        self.assertEqual(FAILED_FINAL, exhausted["status"])
        self.assertEqual(
            "OUTER_ATTEMPT_BUDGET_EXHAUSTED",
            exhausted["last_result"]["queue_decision_code"],
        )
        self.assertEqual(job_id, self.store.queue_snapshot()["blocked_by_job_id"])

    def test_record_blocked_terminalizes_started_attempt_once(self):
        jobs, _ = self.enqueue(key="blocked-attempt-close")
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-attempt-close")
        self.store.mark_task_started(
            job_id, "owner-attempt-close", "TASK-20260826-181001"
        )
        blocked = self.store.record_blocked(
            job_id, "owner-attempt-close", "CONTROL_EXECUTION_ERROR"
        )
        attempt = blocked["attempts"][-1]
        self.assertTrue(attempt["finished_at"])
        self.assertEqual(BLOCKED, attempt["result"]["status"])
        self.assertEqual(
            "CONTROL_EXECUTION_ERROR", attempt["result"]["failure_code"]
        )
        finished_at = attempt["finished_at"]
        self.assertFalse(
            self.store._terminalize_current_attempt(
                blocked, {"status": "SHOULD_NOT_OVERWRITE"}
            )
        )
        self.assertEqual(finished_at, blocked["attempts"][-1]["finished_at"])
        self.assertEqual(BLOCKED, blocked["attempts"][-1]["result"]["status"])

    def test_reconcile_interrupted_terminalizes_started_attempt_idempotently(self):
        jobs, _ = self.enqueue(key="interrupted-attempt-close")
        job_id = jobs[0]["job_id"]
        self.store.claim_next("owner-interrupted-close")
        self.store.mark_task_started(
            job_id, "owner-interrupted-close", "TASK-20260826-181002"
        )
        self.assertEqual([job_id], self.store.reconcile_interrupted())
        first = self.store.get_job(job_id)
        attempt = first["attempts"][-1]
        self.assertTrue(attempt["finished_at"])
        self.assertEqual(INTERRUPTED, attempt["result"]["status"])
        revision = first["revision"]
        finished_at = attempt["finished_at"]
        self.assertEqual([], self.store.reconcile_interrupted())
        second = self.store.get_job(job_id)
        self.assertEqual(revision, second["revision"])
        self.assertEqual(finished_at, second["attempts"][-1]["finished_at"])


class FakeLease:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self.lease = False

    def validate_configuration(self):
        return {
            "working_dir": "D:\\project",
            "profile_dir": "D:\\agents\\profiles\\sample-profile",
            "profile_id": "sample-profile",
            "profile_schema_version": 1,
            "available_modules": list(MODULES),
            "task_root": "D:\\kkm_test\\.tasks",
            "profile_manifest_sha256": "a" * 64,
            "profile_snapshot_sha256": "b" * 64,
            "policy_snapshot_sha256": "c" * 64,
            "workspace_identity_sha256": "d" * 64,
        }

    def acquire_execution_lease(self):
        self.lease = True
        return True

    def release_execution_lease(self):
        self.lease = False

    def probe_execution_idle(self):
        return not self.lease

    def execute(self, job, *, on_task_started):
        task_id = f"TASK-20260826-1300{len(self.calls):02d}"
        on_task_started(task_id)
        self.calls.append(job["job_id"])
        result = dict(self.outcomes.pop(0))
        result["task_id"] = task_id
        return result


class LocalOperatorResolutionTests(unittest.TestCase):
    def _prepare(self, root: Path, status: str) -> tuple[JobStore, str]:
        store = JobStore(root / ".control")
        jobs, _ = valid_enqueue(
            store, f"manual-{status.casefold()}", [request()]
        )
        job_id = jobs[0]["job_id"]
        store.claim_next(f"owner-{status.casefold()}")
        if status == AWAITING_QA:
            store.mark_task_started(
                job_id,
                f"owner-{status.casefold()}",
                "TASK-20260826-182001",
            )
            store.record_result(
                job_id,
                f"owner-{status.casefold()}",
                failed_result(rolled_back=False),
            )
        elif status == BLOCKED:
            store.record_blocked(
                job_id, f"owner-{status.casefold()}", "LOCAL_TOOL_UNAVAILABLE"
            )
        else:
            store.reconcile_interrupted()
        self.assertEqual(status, store.get_job(job_id)["status"])
        return store, job_id

    def test_local_resolver_handles_each_manual_state_and_is_idempotent(self):
        cases = (
            (
                AWAITING_QA,
                MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                "PRESERVED",
            ),
            (BLOCKED, MANUAL_RESOLUTION_VERIFIED_CLEAN, "NO_DELTA"),
            (INTERRUPTED, MANUAL_RESOLUTION_VERIFIED_CLEAN, "NO_DELTA"),
        )
        for status, resolution, disposition in cases:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                store, job_id = self._prepare(root, status)
                expected_revision = store.get_job(job_id)["revision"]
                harness = FakeLease([])
                resolver = LocalOperatorResolver(lambda: store, harness)
                confirmation = MANUAL_RESOLUTION_CONFIRMATIONS[resolution]
                resolved, replayed = resolver.resolve(
                    job_id,
                    resolution=resolution,
                    confirmation=confirmation,
                    expected_revision=expected_revision,
                    request_id=f"manual-action-{status.casefold()}",
                )
                self.assertFalse(replayed)
                self.assertEqual(SKIPPED, resolved["status"])
                self.assertEqual(
                    resolution, resolved["manual_resolution"]["resolution"]
                )
                self.assertEqual(
                    disposition, resolved["last_result"]["worktree_disposition"]
                )
                self.assertEqual("", store.queue_snapshot()["blocked_by_job_id"])
                self.assertFalse(harness.lease)

                replay, replayed = resolver.resolve(
                    job_id,
                    resolution=resolution,
                    confirmation=confirmation,
                    expected_revision=expected_revision,
                    request_id=f"manual-action-{status.casefold()}",
                )
                self.assertTrue(replayed)
                self.assertEqual(resolved["revision"], replay["revision"])
                persisted = (root / ".control" / "jobs" / f"{job_id}.json").read_text(
                    encoding="utf-8"
                )
                self.assertNotIn(f"manual-action-{status.casefold()}", persisted)

    def test_manual_resolution_requires_revision_confirmation_and_idle_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            store, job_id = self._prepare(Path(temp), AWAITING_QA)
            revision = store.get_job(job_id)["revision"]
            resolver = LocalOperatorResolver(lambda: store, FakeLease([]))
            with self.assertRaisesRegex(
                QueueError, "MANUAL_RESOLUTION_CONFIRMATION_REQUIRED"
            ):
                resolver.resolve(
                    job_id,
                    resolution=MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                    confirmation="yes",
                    expected_revision=revision,
                    request_id="manual-invalid-confirmation",
                )
            with self.assertRaisesRegex(QueueError, "JOB_REVISION_CONFLICT"):
                resolver.resolve(
                    job_id,
                    resolution=MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                    confirmation=MANUAL_RESOLUTION_CONFIRMATIONS[
                        MANUAL_RESOLUTION_ACCEPT_PRESERVED
                    ],
                    expected_revision=revision + 1,
                    request_id="manual-stale-revision",
                )

            class BusyHarness(FakeLease):
                def probe_execution_idle(self):
                    return False

                def acquire_execution_lease(self):
                    raise AssertionError("must not acquire after a failed idle probe")

            def must_not_construct_store():
                raise AssertionError("writable store constructed before idle lease")

            with self.assertRaisesRegex(QueueError, "ACTIVE_HARNESS_PROCESS"):
                LocalOperatorResolver(
                    must_not_construct_store, BusyHarness([])
                ).resolve(
                    job_id,
                    resolution=MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                    confirmation=MANUAL_RESOLUTION_CONFIRMATIONS[
                        MANUAL_RESOLUTION_ACCEPT_PRESERVED
                    ],
                    expected_revision=revision,
                    request_id="manual-busy",
                )

            resolved, replayed = resolver.resolve(
                job_id,
                resolution=MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                confirmation=MANUAL_RESOLUTION_CONFIRMATIONS[
                    MANUAL_RESOLUTION_ACCEPT_PRESERVED
                ],
                expected_revision=revision,
                request_id="manual-idempotency",
            )
            self.assertFalse(replayed)
            self.assertEqual(SKIPPED, resolved["status"])
            with self.assertRaisesRegex(QueueError, "IDEMPOTENCY_CONFLICT"):
                resolver.resolve(
                    job_id,
                    resolution=MANUAL_RESOLUTION_VERIFIED_CLEAN,
                    confirmation=MANUAL_RESOLUTION_CONFIRMATIONS[
                        MANUAL_RESOLUTION_VERIFIED_CLEAN
                    ],
                    expected_revision=revision,
                    request_id="manual-idempotency",
                )

    def test_manual_resolution_replay_repairs_queue_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            store, job_id = self._prepare(Path(temp), BLOCKED)
            revision = store.get_job(job_id)["revision"]
            resolver = LocalOperatorResolver(lambda: store, FakeLease([]))
            kwargs = {
                "resolution": MANUAL_RESOLUTION_VERIFIED_CLEAN,
                "confirmation": MANUAL_RESOLUTION_CONFIRMATIONS[
                    MANUAL_RESOLUTION_VERIFIED_CLEAN
                ],
                "expected_revision": revision,
                "request_id": "manual-commit-repair",
            }
            with patch.object(store, "_write_queue", side_effect=OSError("crash")):
                with self.assertRaises(OSError):
                    resolver.resolve(job_id, **kwargs)
            self.assertEqual(SKIPPED, store.get_job(job_id)["status"])
            self.assertEqual(job_id, store.queue_snapshot()["blocked_by_job_id"])

            resolved, replayed = resolver.resolve(job_id, **kwargs)
            self.assertTrue(replayed)
            self.assertEqual(SKIPPED, resolved["status"])
            self.assertEqual("", store.queue_snapshot()["blocked_by_job_id"])

    def test_operator_inspect_uses_read_only_store_without_byte_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store, job_id = self._prepare(root, BLOCKED)
            control_root = root / ".control"

            def manifest():
                return {
                    path.relative_to(control_root).as_posix(): path.read_bytes()
                    for path in control_root.rglob("*")
                    if path.is_file()
                }

            before = manifest()
            read_only_store = JobStore(control_root, read_only=True)
            inspected = _public_inspection(read_only_store, job_id)
            self.assertTrue(inspected["ok"])
            self.assertEqual(BLOCKED, inspected["job"]["status"])
            self.assertEqual(before, manifest())


class SupervisorTests(unittest.TestCase):
    def test_success_ack_during_terminal_snapshot_wakes_same_dispatcher(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store,
                "wakeup-success",
                [request(client_job_id="a"), request(client_job_id="b")],
            )
            harness = FakeLease([success_result(), success_result()])
            supervisor = QueueSupervisor(store, harness)
            original_snapshot = store.queue_snapshot
            snapshot_seen = threading.Event()
            release_snapshot = threading.Event()
            intercepted = False

            def delayed_snapshot():
                nonlocal intercepted
                snapshot = original_snapshot()
                if (
                    not intercepted
                    and threading.current_thread().name == "kkm-control-supervisor"
                    and snapshot.get("gate_reason") == SUCCESS_ACK_REQUIRED
                ):
                    intercepted = True
                    snapshot_seen.set()
                    self.assertTrue(release_snapshot.wait(5))
                return snapshot

            with patch.object(store, "queue_snapshot", side_effect=delayed_snapshot):
                self.assertTrue(supervisor.start()["started"])
                self.assertTrue(snapshot_seen.wait(5))
                store.acknowledge_success(jobs[0]["job_id"], "ack-in-race")
                wake = supervisor.start()
                self.assertTrue(wake["already_running"])
                self.assertTrue(wake["wake_requested"])
                release_snapshot.set()
                self.assertTrue(supervisor.wait(5))

            self.assertEqual(2, len(harness.calls))
            self.assertEqual(SUCCEEDED, store.get_job(jobs[1]["job_id"])["status"])

    def test_snapshot_error_recovery_preserves_concurrent_success_wakeup(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store,
                "wakeup-snapshot-error",
                [request(client_job_id="a"), request(client_job_id="b")],
            )
            harness = FakeLease([success_result(), success_result()])
            supervisor = QueueSupervisor(store, harness)
            original_snapshot = store.queue_snapshot
            error_window = threading.Event()
            release_error = threading.Event()
            injected = False

            def failing_snapshot():
                nonlocal injected
                if (
                    not injected
                    and threading.current_thread().name == "kkm-control-supervisor"
                    and store.get_job(jobs[0]["job_id"])["status"] == SUCCEEDED
                ):
                    injected = True
                    error_window.set()
                    self.assertTrue(release_error.wait(5))
                    raise OSError("one-shot terminal snapshot failure")
                return original_snapshot()

            with patch.object(store, "queue_snapshot", side_effect=failing_snapshot):
                self.assertTrue(supervisor.start()["started"])
                self.assertTrue(error_window.wait(5))
                store.acknowledge_success(jobs[0]["job_id"], "ack-error-race")
                self.assertTrue(supervisor.start()["already_running"])
                release_error.set()
                self.assertTrue(supervisor.wait(5))

            self.assertEqual(2, len(harness.calls))
            self.assertEqual(SUCCEEDED, store.get_job(jobs[1]["job_id"])["status"])

    def test_blocked_resume_during_commit_wakes_same_dispatcher(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "wakeup-blocked", [request()])

            class RecoveringHarness(FakeLease):
                def execute(self, job, *, on_task_started):
                    self.calls.append(job["job_id"])
                    if len(self.calls) == 1:
                        raise HarnessServiceError("LOCAL_TOOL_UNAVAILABLE")
                    task_id = "TASK-20260826-131500"
                    on_task_started(task_id)
                    result = success_result(task_id)
                    return result

            harness = RecoveringHarness([])
            supervisor = QueueSupervisor(store, harness)
            original_record_blocked = store.record_blocked
            blocked_seen = threading.Event()
            release_blocked = threading.Event()

            def delayed_blocked(*args, **kwargs):
                result = original_record_blocked(*args, **kwargs)
                blocked_seen.set()
                self.assertTrue(release_blocked.wait(5))
                return result

            with patch.object(store, "record_blocked", side_effect=delayed_blocked):
                self.assertTrue(supervisor.start()["started"])
                self.assertTrue(blocked_seen.wait(5))
                resumed, _ = store.resume_job(jobs[0]["job_id"], "resume-in-race")
                self.assertEqual(QUEUED, resumed["status"])
                self.assertTrue(supervisor.start()["already_running"])
                release_blocked.set()
                self.assertTrue(supervisor.wait(5))

            self.assertEqual(2, len(harness.calls))
            self.assertEqual(SUCCEEDED, store.get_job(jobs[0]["job_id"])["status"])

    def test_sequential_dispatch_stops_at_first_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store,
                "batch-1",
                [request(client_job_id="a"), request(client_job_id="b"), request(client_job_id="c")],
            )
            harness = FakeLease([success_result(), failed_result()])
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            self.assertEqual(1, len(harness.calls))
            self.assertEqual(SUCCEEDED, store.get_job(jobs[0]["job_id"])["status"])
            store.acknowledge_success(jobs[0]["job_id"], "ack-1")
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            self.assertEqual(2, len(harness.calls))
            self.assertEqual(AWAITING_ENRICHMENT, store.get_job(jobs[1]["job_id"])["status"])
            self.assertEqual(QUEUED, store.get_job(jobs[2]["job_id"])["status"])
            self.assertTrue(store.queue_snapshot()["paused"])

    def test_claim_partial_commit_is_interrupted_before_any_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "claim-crash", [request(), request(client_job_id="b")])
            with patch.object(store, "_write_queue", side_effect=OSError("crash")):
                with self.assertRaises(OSError):
                    store.claim_next("crashed-owner")
            harness = FakeLease([success_result()])
            supervisor = QueueSupervisor(store, harness)
            started = supervisor.start()
            self.assertFalse(started["started"])
            self.assertEqual("INTERRUPTED_JOB_REQUIRES_DECISION", started["code"])
            self.assertEqual([], harness.calls)
            self.assertEqual(INTERRUPTED, store.get_job(jobs[0]["job_id"])["status"])
            self.assertEqual(QUEUED, store.get_job(jobs[1]["job_id"])["status"])
            self.assertFalse(harness.lease)

    def test_live_supervisor_recovers_one_shot_claim_commit_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "live-claim-crash", [request()])
            harness = FakeLease([success_result()])
            supervisor = QueueSupervisor(store, harness)
            original = store._write_queue
            calls = 0

            def fail_once(queue):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("one-shot crash")
                return original(queue)

            with patch.object(store, "_write_queue", side_effect=fail_once):
                self.assertTrue(supervisor.start()["started"])
                self.assertTrue(supervisor.wait(5))
            self.assertEqual(INTERRUPTED, store.get_job(jobs[0]["job_id"])["status"])
            self.assertEqual([], harness.calls)
            self.assertFalse(harness.lease)
            self.assertIn("CONTROL_QUEUE_CLAIM_ERROR", supervisor.status()["last_error"])

    def test_multiple_interrupted_jobs_remain_serially_gated(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store,
                "multiple-orphans", [request(client_job_id="a"), request(client_job_id="b")]
            )
            for job in jobs:
                state = store._read_job(job["job_id"])
                state["status"] = RUNNING
                state["owner_id"] = "dead-owner"
                store._write_job(state)
            interrupted = store.reconcile_interrupted()
            self.assertEqual([job["job_id"] for job in jobs], interrupted)
            snapshot = store.queue_snapshot()
            self.assertEqual(jobs[0]["job_id"], snapshot["blocked_by_job_id"])
            first = store._read_job(jobs[0]["job_id"])
            first["status"] = SKIPPED
            store._write_job(first)
            store.reconcile_interrupted()
            self.assertEqual(
                jobs[1]["job_id"], store.queue_snapshot()["blocked_by_job_id"]
            )

    def test_pre_thread_failures_release_execution_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            harness = FakeLease([])
            supervisor = QueueSupervisor(store, harness)
            with patch.object(store, "reconcile_interrupted", side_effect=OSError("bad")):
                result = supervisor.start()
            self.assertEqual("CONTROL_RECONCILIATION_ERROR", result["code"])
            self.assertFalse(harness.lease)

            class BrokenThread:
                def __init__(self, *args, **kwargs):
                    pass

                def start(self):
                    raise RuntimeError("cannot start")

            with patch("queue_supervisor.threading.Thread", BrokenThread):
                result = supervisor.start()
            self.assertEqual("CONTROL_SUPERVISOR_START_ERROR", result["code"])
            self.assertFalse(harness.lease)

    def test_result_queue_commit_failure_is_reconciled_to_terminal_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "result-crash", [request()])
            harness = FakeLease([success_result()])
            supervisor = QueueSupervisor(store, harness)
            original = store._write_queue
            calls = 0

            def fail_second(queue):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("result queue crash")
                return original(queue)

            with patch.object(store, "_write_queue", side_effect=fail_second):
                self.assertTrue(supervisor.start()["started"])
                self.assertTrue(supervisor.wait(5))
            job = store.get_job(jobs[0]["job_id"])
            self.assertEqual(SUCCEEDED, job["status"])
            snapshot = store.queue_snapshot()
            self.assertEqual(jobs[0]["job_id"], snapshot["blocked_by_job_id"])
            self.assertEqual(SUCCESS_ACK_REQUIRED, snapshot["gate_reason"])
            self.assertFalse(harness.lease)


class HarnessServiceTests(unittest.TestCase):
    def test_runtime_snapshot_is_content_addressed_and_execution_paths_are_frozen(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profiles" / "sample-profile"
            agents = root / "agents"
            schema = root / "project-profile.schema.json"
            for directory in (
                project / "sample-service",
                project / "sample-web",
                profile_dir / "hooks",
                agents / "hooks",
            ):
                directory.mkdir(parents=True, exist_ok=True)
            for skill in (
                "project-context",
                "backend",
                "frontend",
                "sql",
                "review",
            ):
                (agents / "skills" / skill).mkdir(parents=True, exist_ok=True)
                (agents / "skills" / skill / "SKILL.md").write_text(
                    f"{skill}-v1", encoding="utf-8"
                )
            (agents / "AGENTS.md").write_text("common-v1", encoding="utf-8")
            (agents / "hooks" / "validation.md").write_text(
                "validation-v1", encoding="utf-8"
            )
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            (profile_dir / "project-rules.md").write_text(
                "profile-rule-v1", encoding="utf-8"
            )
            (profile_dir / "analysis.md").write_text("# analysis", encoding="utf-8")
            (profile_dir / "hooks" / "always.md").write_text(
                "always-v1", encoding="utf-8"
            )
            schema.write_text("{}", encoding="utf-8")
            profile = ProjectProfile(
                schema_version=1,
                id="sample-profile",
                display_name="sample-profile",
                profile_dir=profile_dir,
                workspace_roots=(project.resolve(),),
                project_rules=profile_dir / "project-rules.md",
                analysis=profile_dir / "analysis.md",
                ui_map=None,
                db_catalog=None,
                modules=(
                    ModuleProfile("sample-service", "sample-service", "backend", False),
                    ModuleProfile("sample-web", "sample-web", "frontend", False),
                ),
                rules={"always": (profile_dir / "hooks" / "always.md",)},
                area_patterns={},
                protected_paths=(),
                local_dev_config_excludes=(),
            )
            runtime_files = (
                (agents / "AGENTS.md", "common_agents"),
                (agents / "hooks" / "validation.md", "common_validation"),
            )
            service = HarnessService(
                project,
                profile_dir,
                task_root=root / ".tasks",
                snapshot_root=root / ".control" / "snapshots",
            )

            loaded_schema_paths = []

            def rebase(destination, _workspace, **kwargs):
                destination = Path(destination)
                loaded_schema_paths.append(Path(kwargs["schema_file"]))
                return replace(
                    profile,
                    profile_dir=destination,
                    project_rules=destination / "project-rules.md",
                    analysis=destination / "analysis.md",
                    rules={"always": (destination / "hooks" / "always.md",)},
                )

            with patch("harness_service.PROFILE_SCHEMA_FILE", schema), patch(
                "harness_service.runtime_rule_paths", return_value=runtime_files
            ), patch("runtime_snapshot.load_project_profile", side_effect=rebase):
                context = service._configuration(profile, list(MODULES))
                (profile_dir / "project-rules.md").write_text(
                    "profile-rule-raced", encoding="utf-8"
                )
                with self.assertRaises(HarnessServiceError) as raced:
                    service._materialize_runtime_snapshot(profile, context)
                self.assertEqual(
                    "RUNTIME_SNAPSHOT_SOURCE_DRIFT", raced.exception.code
                )
                (profile_dir / "project-rules.md").write_text(
                    "profile-rule-v1", encoding="utf-8"
                )
                snapshot = service._materialize_runtime_snapshot(profile, context)

            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(
                "profile-rule-v1",
                snapshot.profile.project_rules.read_text(encoding="utf-8"),
            )
            self.assertEqual(
                "common-v1",
                (snapshot.policy_root / "AGENTS.md").read_text(encoding="utf-8"),
            )
            self.assertTrue(snapshot.profile.profile_dir.is_relative_to(snapshot.root))
            self.assertTrue(snapshot.policy_root.is_relative_to(snapshot.root))
            self.assertEqual(1, len(loaded_schema_paths))
            self.assertTrue(loaded_schema_paths[0].is_relative_to(snapshot.root))
            prompt_task = TaskState(
                task_id="TASK-SNAPSHOT-PROMPT",
                requirement="수정",
                working_dir=str(project),
                task_mode="MODIFICATION",
                target_module=["sample-service"],
                changed_files=["sample-service/A.java"],
            )
            planner_prompt = _build_planner_prompt(
                prompt_task, snapshot.profile, snapshot.policy_root
            )
            tester_prompt = _build_tester_prompt(
                prompt_task, snapshot.policy_root, snapshot.profile
            )
            worker_prompt = build_prompt(
                prompt_task.requirement,
                prompt_task.target_module,
                prompt_task.task_mode,
                task_id=prompt_task.task_id,
                profile=snapshot.profile,
                role="codex",
                policy_root=snapshot.policy_root,
            )
            reviewer_prompt = load_review_rules(
                snapshot.profile,
                prompt_task.changed_files,
                policy_root=snapshot.policy_root,
            )
            for rendered in (
                planner_prompt,
                tester_prompt,
                worker_prompt,
                reviewer_prompt,
            ):
                self.assertIn("common-v1", rendered)
                self.assertIn("validation-v1", rendered)
                self.assertIn("profile-rule-v1", rendered)
            for rendered in (planner_prompt, tester_prompt, worker_prompt):
                self.assertIn("project-context-v1", rendered)
            (profile_dir / "project-rules.md").write_text(
                "profile-rule-v2", encoding="utf-8"
            )
            (agents / "AGENTS.md").write_text("common-v2", encoding="utf-8")
            snapshot.validate()
            self.assertEqual(
                "profile-rule-v1",
                snapshot.profile.project_rules.read_text(encoding="utf-8"),
            )
            snapshot.profile.project_rules.write_text("tampered", encoding="utf-8")
            with self.assertRaises(RuntimeSnapshotError):
                snapshot.validate()

    def test_execution_context_is_exact_and_required_before_task_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile",
                profile_dir=profile_dir,
                schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class NeverManager:
                calls = 0

                def __init__(self, **kwargs):
                    NeverManager.calls += 1

            service = HarnessService(
                project,
                profile_dir,
                task_root=task_root,
                manager_factory=NeverManager,
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "profile_id": "sample-profile",
                "profile_schema_version": 1,
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                **execution_context(),
            }
            self.assertTrue(service.acquire_execution_lease())
            base = {
                "request": request().to_dict(),
                "current_requirement": request().requirement,
            }
            invalid_cases = (
                ({}, "EXECUTION_CONTEXT_MISSING"),
                ({"execution_context": {}}, "EXECUTION_CONTEXT_MISSING"),
                (
                    {"execution_context": {"profile_id": "sample-profile"}},
                    "EXECUTION_CONTEXT_MISSING",
                ),
                ({"execution_context": []}, "EXECUTION_CONTEXT_INVALID"),
                (
                    {"execution_context": execution_context(extra="value")},
                    "EXECUTION_CONTEXT_INVALID",
                ),
                (
                    {
                        "execution_context": execution_context(
                            profile_schema_version=True
                        )
                    },
                    "EXECUTION_CONTEXT_INVALID",
                ),
                (
                    {
                        "execution_context": execution_context(
                            profile_snapshot_sha256="B" * 64
                        )
                    },
                    "EXECUTION_CONTEXT_INVALID",
                ),
                (
                    {"execution_context": execution_context(profile_id="other")},
                    "PROFILE_DRIFT",
                ),
            )
            started: list[str] = []
            for extra, expected_code in invalid_cases:
                with self.subTest(expected_code=expected_code, extra=extra):
                    with self.assertRaises(HarnessServiceError) as raised:
                        service.execute(
                            {**base, **extra},
                            on_task_started=started.append,
                        )
                    self.assertEqual(expected_code, raised.exception.code)
            service.release_execution_lease()
            self.assertEqual(0, NeverManager.calls)
            self.assertEqual([], started)
            self.assertFalse(task_root.exists())

    def test_task_start_lineage_failure_creates_no_task_or_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile", profile_dir=profile_dir, schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class NeverManager:
                calls = 0

                def __init__(self, **_kwargs):
                    NeverManager.calls += 1

            service = HarnessService(
                project, profile_dir, task_root=task_root,
                manager_factory=NeverManager, instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                **execution_context(),
            }
            self.assertTrue(service.acquire_execution_lease())
            with self.assertRaises(HarnessServiceError) as raised:
                service.execute(
                    {
                        "request": request().to_dict(),
                        "current_requirement": request().requirement,
                        "execution_context": execution_context(),
                    },
                    on_task_started=lambda _task_id: (_ for _ in ()).throw(
                        OSError("queue commit failed")
                    ),
                )
            service.release_execution_lease()
            self.assertEqual("CONTROL_STATE_COMMIT_FAILED", raised.exception.code)
            self.assertEqual(0, NeverManager.calls)
            self.assertFalse(task_root.exists())

    def test_report_failure_preserves_gate_outcome_and_returns_diagnostic(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile", profile_dir=profile_dir, schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class FakeManager:
                def __init__(self, **_kwargs):
                    pass

                def submit_task(self, task):
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    task.completion_kind = "CHANGED"
                    task.changed_files = ["sample-service/A.java"]
                    task.worktree_disposition = "PRESERVED"
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    return None

            class BrokenReporter:
                def save_markdown(self, *_args, **_kwargs):
                    raise OSError("disk full")

            service = HarnessService(
                project, profile_dir, task_root=task_root,
                manager_factory=FakeManager, reporter=BrokenReporter(),
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                **execution_context(),
            }
            self.assertTrue(service.acquire_execution_lease())
            outcome = service.execute(
                {
                    "request": request().to_dict(),
                    "current_requirement": request().requirement,
                    "execution_context": execution_context(),
                },
                on_task_started=lambda _task_id: None,
            )
            service.release_execution_lease()
            self.assertFalse(outcome["success"])
            self.assertEqual("", outcome["failure_code"])
            self.assertEqual("CONTROL_REPORT_WRITE_FAILED", outcome["report_diagnostics"][-1]["code"])
            self.assertEqual("SUCCESS", outcome["status"])
            self.assertEqual("VERIFIED", outcome["verification_status"])
            self.assertEqual("PRESERVED", outcome["worktree_disposition"])
            self.assertEqual("", outcome["report_path"])
            persisted = TaskState.load(outcome["task_id"], task_root)
            self.assertEqual("", persisted.failure_code)

    def test_report_failure_is_diagnostic_in_queue_result(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile", profile_dir=profile_dir, schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class FakeManager:
                def __init__(self, **_kwargs):
                    pass

                def submit_task(self, task):
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    task.changed_files = ["sample-service/A.java"]
                    task.worktree_disposition = "PRESERVED"
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    return None

            class BrokenReporter:
                def save_markdown(self, *_args, **_kwargs):
                    raise OSError("disk full")

            service = HarnessService(
                project, profile_dir, task_root=task_root,
                manager_factory=FakeManager, reporter=BrokenReporter(),
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                **execution_context(),
            }
            store = JobStore(root / ".control")
            jobs, _ = valid_enqueue(store, "report-failure-integration", [request()])
            supervisor = QueueSupervisor(store, service)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            persisted_job = store.get_job(jobs[0]["job_id"])
            self.assertEqual(AWAITING_QA, persisted_job["status"])
            self.assertEqual(
                "CONTROL_REPORT_WRITE_FAILED",
                persisted_job["last_result"]["report_diagnostics"][-1]["code"],
            )
            self.assertEqual("PRESERVED", persisted_job["last_result"]["worktree_disposition"])

    def test_invalid_manager_result_is_terminalized_without_running_orphan(self):
        for invalid_result in (None, {"success": True}):
            with self.subTest(invalid_result=invalid_result), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                project = root / "project"
                profile_dir = root / "profile"
                task_root = root / ".tasks"
                project.mkdir()
                profile_dir.mkdir()
                (profile_dir / "project.json").write_text("{}", encoding="utf-8")
                profile = SimpleNamespace(
                    id="sample-profile", profile_dir=profile_dir, schema_version=1,
                    module_names=MODULES,
                )

                class Lock:
                    def acquire(self):
                        return True

                    def release(self):
                        return None

                class InvalidManager:
                    def __init__(self, **_kwargs):
                        pass

                    def submit_task(self, _task):
                        future = Future()
                        future.set_result(invalid_result)
                        return future

                    def shutdown(self):
                        return None

                class Reporter:
                    def save_markdown(self, task, model="", out_dir=".tasks"):
                        path = task.task_date_dir(task.task_id, out_dir) / "report.md"
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text(task.failure_code, encoding="utf-8")
                        return path

                service = HarnessService(
                    project, profile_dir, task_root=task_root,
                    manager_factory=InvalidManager, reporter=Reporter(),
                    instance_lock=Lock(),
                )
                service._load_profile = lambda: (profile, list(MODULES))
                service._configuration = lambda *_: {
                    "working_dir": str(project.resolve()),
                    "profile_dir": str(profile_dir.resolve()),
                    "available_modules": list(MODULES),
                    "task_root": str(task_root.resolve()),
                    **execution_context(),
                }
                self.assertTrue(service.acquire_execution_lease())
                outcome = service.execute(
                    {
                        "request": request().to_dict(),
                        "current_requirement": request().requirement,
                        "execution_context": execution_context(),
                    },
                    on_task_started=lambda _task_id: None,
                )
                service.release_execution_lease()
                self.assertFalse(outcome["success"])
                self.assertEqual("MANAGER_RESULT_INVALID", outcome["failure_code"])
                self.assertEqual("AWAITING_QA", outcome["status"])
                self.assertEqual("UNKNOWN", outcome["worktree_disposition"])
                persisted = TaskState.load(outcome["task_id"], task_root)
                self.assertEqual("AWAITING_QA", persisted.status)
                self.assertEqual("MANAGER_RESULT_INVALID", persisted.failure_code)

    def test_final_configuration_drift_overrides_fake_manager_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile",
                profile_dir=profile_dir,
                schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class FakeManager:
                def __init__(self, **kwargs):
                    self.guard = kwargs["configuration_guard"]

                def submit_task(self, task):
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    task.completion_kind = "CHANGED"
                    task.changed_files = ["sample-service/A.java"]
                    task.worktree_disposition = "PRESERVED"
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    return None

            service = HarnessService(
                project,
                profile_dir,
                task_root=task_root,
                manager_factory=FakeManager,
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            calls = 0

            def current_configuration(*_args):
                nonlocal calls
                calls += 1
                values = execution_context()
                if calls >= 2:
                    values["policy_snapshot_sha256"] = "d" * 64
                return {
                    "working_dir": str(project.resolve()),
                    "profile_dir": str(profile_dir.resolve()),
                    "available_modules": list(MODULES),
                    "task_root": str(task_root.resolve()),
                    **values,
                }

            service._configuration = current_configuration
            self.assertTrue(service.acquire_execution_lease())
            outcome = service.execute(
                {
                    "request": request().to_dict(),
                    "current_requirement": request().requirement,
                    "execution_context": execution_context(),
                },
                on_task_started=lambda _task_id: None,
            )
            service.release_execution_lease()
            self.assertFalse(outcome["success"])
            self.assertEqual("PROFILE_DRIFT", outcome["failure_code"])
            self.assertEqual("AWAITING_QA", outcome["status"])
            self.assertEqual("PRESERVED", outcome["worktree_disposition"])
            self.assertTrue(Path(outcome["task_state_path"]).is_file())
            self.assertTrue(Path(outcome["report_path"]).is_file())

    def test_programmatic_adapter_saves_task_and_report_at_explicit_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile",
                profile_dir=profile_dir,
                schema_version=1,
                module_names=MODULES,
            )

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            class FakeManager:
                def __init__(self, **kwargs):
                    self.kwargs = kwargs

                def submit_task(self, task):
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    task.completion_kind = "CHANGED"
                    task.build = {"status": "PASS"}
                    task.review_status = "REVIEW_PASS"
                    task.changed_files = ["sample-service/A.java"]
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    return None

            class FakeReporter:
                def save_markdown(self, task, model="", out_dir=".tasks"):
                    path = task.task_date_dir(task.task_id, out_dir) / f"{task.task_id}.md"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("verified", encoding="utf-8")
                    return path

            service = HarnessService(
                project,
                profile_dir,
                task_root=task_root,
                manager_factory=FakeManager,
                reporter=FakeReporter(),
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda _profile, _available: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "profile_id": "sample-profile",
                "profile_schema_version": 1,
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                "profile_manifest_sha256": "a" * 64,
                "profile_snapshot_sha256": "b" * 64,
                "policy_snapshot_sha256": "c" * 64,
                "workspace_identity_sha256": "d" * 64,
            }
            self.assertTrue(service.acquire_execution_lease())
            started = []
            outcome = service.execute(
                {
                    "request": request().to_dict(),
                    "current_requirement": request().requirement,
                    "execution_context": execution_context(),
                },
                on_task_started=started.append,
            )
            service.release_execution_lease()
            self.assertTrue(outcome["success"])
            self.assertEqual(1, len(started))
            self.assertTrue(Path(outcome["task_state_path"]).is_file())
            self.assertTrue(Path(outcome["report_path"]).is_file())
            self.assertFalse((project / ".tasks").exists())

    def test_configuration_snapshots_profile_tree_schema_and_runtime_rules(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            schema = root / "schema.json"
            common_rule = root / "AGENTS.md"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile_rule = profile_dir / "project-rules.md"
            profile_rule.write_text("rule-v1", encoding="utf-8")
            schema.write_text("{}", encoding="utf-8")
            common_rule.write_text("common-v1", encoding="utf-8")
            profile = SimpleNamespace(id="sample-profile", schema_version=1)
            service = HarnessService(project, profile_dir, task_root=root / ".tasks")

            with patch("harness_service.PROFILE_SCHEMA_FILE", schema), patch(
                "harness_service.runtime_rule_paths",
                return_value=((common_rule, "common_agents"),),
            ):
                first = service._configuration(profile, list(MODULES))
                profile_rule.write_text("rule-v2", encoding="utf-8")
                second = service._configuration(profile, list(MODULES))
                common_rule.write_text("common-v2", encoding="utf-8")
                third = service._configuration(profile, list(MODULES))

            self.assertNotEqual(
                first["profile_snapshot_sha256"], second["profile_snapshot_sha256"]
            )
            self.assertEqual(
                first["policy_snapshot_sha256"], second["policy_snapshot_sha256"]
            )
            self.assertNotEqual(
                second["policy_snapshot_sha256"], third["policy_snapshot_sha256"]
            )

    def test_manager_shutdown_error_still_persists_terminal_task_and_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            profile_dir = root / "profile"
            task_root = root / ".tasks"
            project.mkdir()
            profile_dir.mkdir()
            (profile_dir / "project.json").write_text("{}", encoding="utf-8")
            profile = SimpleNamespace(
                id="sample-profile",
                profile_dir=profile_dir,
                schema_version=1,
                module_names=MODULES,
            )

            class ManagerWithBrokenShutdown:
                def __init__(self, **kwargs):
                    pass

                def submit_task(self, task):
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    task.completion_kind = "CHANGED"
                    task.changed_files = ["sample-service/A.java"]
                    task.worktree_disposition = "PRESERVED"
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    raise RuntimeError("shutdown failure")

            class Reporter:
                def save_markdown(self, task, model="", out_dir=".tasks"):
                    path = task.task_date_dir(task.task_id, out_dir) / f"{task.task_id}.md"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(task.failure_code or "ok", encoding="utf-8")
                    return path

            class Lock:
                def acquire(self):
                    return True

                def release(self):
                    return None

            service = HarnessService(
                project,
                profile_dir,
                task_root=task_root,
                manager_factory=ManagerWithBrokenShutdown,
                reporter=Reporter(),
                instance_lock=Lock(),
            )
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: {
                "working_dir": str(project.resolve()),
                "profile_dir": str(profile_dir.resolve()),
                "profile_id": "sample-profile",
                "profile_schema_version": 1,
                "available_modules": list(MODULES),
                "task_root": str(task_root.resolve()),
                "profile_manifest_sha256": "a" * 64,
                "profile_snapshot_sha256": "b" * 64,
                "policy_snapshot_sha256": "c" * 64,
                "workspace_identity_sha256": "d" * 64,
            }
            self.assertTrue(service.acquire_execution_lease())
            outcome = service.execute(
                {
                    "request": request().to_dict(),
                    "current_requirement": request().requirement,
                    "execution_context": execution_context(),
                },
                on_task_started=lambda task_id: None,
            )
            service.release_execution_lease()
            self.assertFalse(outcome["success"])
            self.assertEqual("CONTROL_SHUTDOWN_ERROR", outcome["failure_code"])
            self.assertEqual("AWAITING_QA", outcome["status"])
            self.assertTrue(Path(outcome["task_state_path"]).is_file())
            self.assertTrue(Path(outcome["report_path"]).is_file())


class ManagerConfigurationGuardTests(unittest.TestCase):
    class Collector:
        def __init__(self):
            self.capture_calls = 0
            self.collect_calls = 0
            self.rollback_calls = 0

        def capture_baseline(self, _task):
            self.capture_calls += 1
            return object(), ""

        def collect(self, **_kwargs):
            self.collect_calls += 1
            raise AssertionError("git collection must not run after drift")

        def rollback(self, **_kwargs):
            self.rollback_calls += 1
            return True, "rolled back"

    class Worker:
        def __init__(self):
            self.calls = 0

        def execute(self, *_args, **_kwargs):
            self.calls += 1
            return SimpleNamespace(success=True, exit_code=0, stdout="", stderr="")

    def _manager(self, root: Path) -> Manager:
        return Manager(
            working_dir=root,
            model="",
            max_retry=1,
            timeout=5,
            worker_type="droid",
            max_workers=1,
            task_root=root / ".tasks",
        )

    def test_initial_drift_blocks_before_baseline_and_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager = self._manager(root)
            collector = self.Collector()
            worker = self.Worker()
            manager.collector = collector
            manager._get_worker = lambda _task: (worker, "droid")
            manager.configuration_guard = lambda: (_ for _ in ()).throw(
                RuntimeError("secret policy value")
            )
            task = TaskState(
                task_id="TASK-CONTROL-PRE",
                requirement="수정",
                working_dir=str(root),
                task_mode="MODIFICATION",
            )
            try:
                result = manager._run_pipeline(task)
            finally:
                manager.shutdown()
            self.assertFalse(result.success)
            self.assertEqual("PROFILE_DRIFT", task.failure_code)
            self.assertEqual("NO_DELTA", task.worktree_disposition)
            self.assertEqual(0, collector.capture_calls)
            self.assertEqual(0, worker.calls)
            self.assertNotIn("secret", task.failure_reason)

    def test_post_worker_drift_stops_without_retry_or_rollback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager = self._manager(root)
            collector = self.Collector()
            worker = self.Worker()
            manager.collector = collector
            manager._get_worker = lambda _task: (worker, "droid")
            guard_calls = 0

            def guard():
                nonlocal guard_calls
                guard_calls += 1
                if guard_calls >= 4:
                    raise RuntimeError("changed")

            manager.configuration_guard = guard
            task = TaskState(
                task_id="TASK-CONTROL-POST",
                requirement="수정",
                working_dir=str(root),
                task_mode="MODIFICATION",
            )
            try:
                result = manager.run_with_retry(task)
            finally:
                manager.shutdown()
            self.assertFalse(result.success)
            self.assertEqual(1, worker.calls)
            self.assertEqual(0, collector.collect_calls)
            self.assertEqual(0, collector.rollback_calls)
            self.assertEqual("PROFILE_DRIFT", task.failure_code)
            self.assertEqual("UNKNOWN", task.worktree_disposition)
            self.assertEqual("AWAITING_QA", task.status)
            self.assertEqual("PARTIAL", task.verification_status)

    def test_planner_uses_snapshot_profile_and_drift_preserves_unknown_delta(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = SimpleNamespace(
                profile_dir=root / "snapshot-profile", module_names=MODULES
            )
            manager = self._manager(root)
            manager.profile = profile
            collector = self.Collector()
            worker = self.Worker()
            manager.collector = collector
            manager._get_worker = lambda _task: (worker, "droid")
            guard_calls = 0

            def guard():
                nonlocal guard_calls
                guard_calls += 1
                if guard_calls >= 4:
                    raise RuntimeError("live source drift")

            manager.configuration_guard = guard
            seen_profiles = []

            def planner(_task, _working_dir, *, profile=None, policy_root=None):
                seen_profiles.append(profile)
                self.assertIsNone(policy_root)
                (root / "planner-delta.txt").write_text("delta", encoding="utf-8")
                return {"worker_brief": "brief"}

            task = TaskState(
                task_id="TASK-CONTROL-PLANNER",
                requirement="수정",
                working_dir=str(root),
                task_mode="MODIFICATION",
                use_planner=True,
            )
            try:
                with patch("manager.run_planner", side_effect=planner):
                    result = manager.run_with_retry(task)
            finally:
                manager.shutdown()
            self.assertFalse(result.success)
            self.assertEqual([profile], seen_profiles)
            self.assertEqual(0, worker.calls)
            self.assertEqual(0, collector.rollback_calls)
            self.assertEqual("PROFILE_DRIFT", task.failure_code)
            self.assertEqual("UNKNOWN", task.worktree_disposition)
            self.assertEqual("AWAITING_QA", task.status)


class ControlPathTests(unittest.TestCase):
    def test_snapshot_root_rejects_reparse_ancestor_without_outside_write(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_base = root / "harness"
            outside = root / "outside"
            state_base.mkdir()
            outside.mkdir()
            control_link = state_base / ".control"
            control_link.mkdir()
            simulated_reparse = control_link.absolute()
            with patch(
                "runtime_snapshot._is_reparse",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaises(RuntimeSnapshotError):
                    _prepare_snapshot_root(control_link / "snapshots")
            self.assertFalse((outside / "snapshots").exists())

    def test_read_only_root_validation_requires_existing_roots_and_never_probes(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            control = base / ".control"
            tasks = base / ".tasks"
            with self.assertRaises(ControlPathError) as missing:
                validate_control_state_roots_read_only(
                    control,
                    tasks,
                    state_base=base,
                )
            self.assertEqual("CONTROL_STATE_ROOT_MISSING", missing.exception.code)
            self.assertFalse(control.exists())
            self.assertFalse(tasks.exists())

            control.mkdir()
            tasks.mkdir()
            (control / "sentinel.bin").write_bytes(b"control")
            (tasks / "sentinel.bin").write_bytes(b"tasks")
            before = {
                path.relative_to(base).as_posix(): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in base.rglob("*")
                if path.is_file()
            }
            with patch(
                "control_paths._probe_atomic_io",
                side_effect=AssertionError("read-only validation probed filesystem"),
            ):
                actual_control, actual_tasks = validate_control_state_roots_read_only(
                    control,
                    tasks,
                    state_base=base,
                )
            after = {
                path.relative_to(base).as_posix(): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in base.rglob("*")
                if path.is_file()
            }
            self.assertEqual(control.resolve(), actual_control)
            self.assertEqual(tasks.resolve(), actual_tasks)
            self.assertEqual(before, after)

    def test_state_tree_walk_errors_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"
            root.mkdir()

            def failing_walk(*args, **kwargs):
                kwargs["onerror"](PermissionError("denied"))
                return iter(())

            with patch("control_paths.os.walk", side_effect=failing_walk):
                with self.assertRaises(ControlPathError) as raised:
                    validate_no_reparse_tree(root, "control_root")
            self.assertEqual(raised.exception.code, "CONTROL_STATE_TREE_UNREADABLE")

    def test_fixed_children_are_probed_without_leaving_empty_roots(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            control, tasks = validate_control_state_roots(
                base / ".control", base / ".tasks", state_base=base
            )
            self.assertEqual(base.resolve() / ".control", control)
            self.assertEqual(base.resolve() / ".tasks", tasks)
            self.assertFalse((base / ".control").exists())
            self.assertFalse((base / ".tasks").exists())

    def test_escape_and_reparse_roots_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_ROOT_ESCAPE"):
                validate_control_state_roots(
                    base.parent / ".control", base / ".tasks", state_base=base
                )
            target = base / "target"
            target.mkdir()
            link = base / ".control"
            link.mkdir()
            simulated_reparse = link.absolute()
            with patch(
                "control_paths._is_reparse",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_ROOT_REPARSE"):
                    validate_control_state_roots(link, base / ".tasks", state_base=base)

    def test_nested_state_reparse_entries_fail_closed_without_outside_write(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            outside = base / "outside"
            outside.mkdir()
            marker = outside / "marker.txt"
            marker.write_text("unchanged", encoding="utf-8")
            control = base / ".control"
            tasks = base / ".tasks"
            control.mkdir()
            tasks.mkdir()
            jobs = control / "jobs"
            jobs.mkdir()
            simulated_reparse = jobs.absolute()
            with patch(
                "control_paths._is_reparse",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_PATH_REPARSE"):
                    validate_control_state_roots(control, tasks, state_base=base)
            self.assertEqual("unchanged", marker.read_text(encoding="utf-8"))
            self.assertEqual([], list(outside.glob("JOB-*.json")))

        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            outside = base / "outside"
            outside.mkdir()
            marker = outside / "marker.txt"
            marker.write_text("unchanged", encoding="utf-8")
            tasks = base / ".tasks"
            tasks.mkdir()
            day = tasks / "20260826"
            day.mkdir()
            simulated_reparse = day.absolute()
            with patch(
                "control_paths._is_reparse",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaisesRegex(ControlPathError, "CONTROL_STATE_PATH_REPARSE"):
                    TaskState("TASK-20260826-999999", "test").save(tasks)
            self.assertEqual("unchanged", marker.read_text(encoding="utf-8"))
            self.assertFalse((outside / "TASK-20260826-999999.json").exists())

        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            control = base / ".control"
            control.mkdir()
            outside_lock = base / "outside.lock"
            outside_lock.write_bytes(b"unchanged")
            store_lock = control / "store.lock"
            store_lock.write_bytes(b"unchanged")
            simulated_reparse = store_lock.absolute()
            with patch(
                "control_paths._is_reparse",
                side_effect=lambda path: path.absolute() == simulated_reparse,
            ):
                with self.assertRaisesRegex(QueueError, "CONTROL_STATE_PATH_UNSAFE"):
                    JobStore(control)
            self.assertEqual(b"unchanged", outside_lock.read_bytes())

    def test_runtime_rule_globals_must_equal_source_agents(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source.md"
            codex = root / "codex.md"
            factory = root / "factory.md"
            for path in (source, codex, factory):
                path.write_text("same", encoding="utf-8")
            paths = (
                (source, "common_agents"),
                (codex, "codex_global_agents"),
                (factory, "factory_global_agents"),
            )
            with patch("run.runtime_rule_paths", return_value=paths):
                validate_runtime_rules()
                factory.write_text("stale", encoding="utf-8")
                with self.assertRaisesRegex(ProfileError, "RUNTIME_RULE_SYNC_MISMATCH"):
                    validate_runtime_rules()


class ControlServiceTests(unittest.TestCase):
    def test_health_reports_canary_pass_only_after_finalized_opencode_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            control_root = root / ".control"
            task_root = root / ".tasks"
            store = JobStore(control_root)
            valid_enqueue(store, "opencode-health", [request()])
            task_root.mkdir()
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=control_root,
                task_root=task_root,
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=root,
            )
            with patch.dict(os.environ, {
                "KKM_OPENCODE_LIVE_VALIDATED": "1",
                "KKM_OPENCODE_MODEL": "provider/model",
            }):
                self.assertEqual("PASS", service.health()["opencode"]["live_canary"])
            with patch.dict(os.environ, {"KKM_OPENCODE_LIVE_VALIDATED": "0"}):
                self.assertEqual("NOT_RUN", service.health()["opencode"]["live_canary"])

    def test_read_only_inspector_never_repairs_state_and_rejects_all_mutators(self):
        def manifest(base: Path) -> dict[str, str]:
            return {
                path.relative_to(base).as_posix(): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in base.rglob("*")
                if path.is_file()
            }

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            control_root = root / ".control"
            task_root = root / ".tasks"
            writable = JobStore(control_root)
            jobs, _ = valid_enqueue(writable, "inspector-fixture", [request()])
            job_id = jobs[0]["job_id"]
            task_root.mkdir()

            queue_path = control_root / "queue.json"
            job_path = control_root / "jobs" / f"{job_id}.json"
            queue = json.loads(queue_path.read_text(encoding="utf-8"))
            job = json.loads(job_path.read_text(encoding="utf-8"))
            job["execution_context"] = {}
            job["retry_actions"] = {"legacy-action-id": {"fixture": True}}
            job_path.write_text(
                json.dumps(job, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            batch_key, batch = next(iter(queue["batches"].items()))
            batch["state"] = "PREPARING"
            batch["prepared_jobs"] = [dict(job)]
            queue["batches"] = {"legacy-batch-id": batch}
            queue_path.write_text(
                json.dumps(queue, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self.assertTrue(batch_key.startswith("sha256:"))
            before = manifest(root)

            inspector_store = JobStore(control_root, read_only=True)
            recovery = inspector_store.read_only_recovery_status()
            self.assertTrue(recovery["required"])
            self.assertEqual("READ_ONLY_RECOVERY_REQUIRED", recovery["code"])
            self.assertEqual(
                {
                    "PREPARING_BATCH_RECOVERY_REQUIRED",
                    "LEGACY_EXECUTION_CONTEXT_RECOVERY_REQUIRED",
                    "LEGACY_IDENTIFIER_MIGRATION_REQUIRED",
                },
                set(recovery["reasons"]),
            )
            self.assertEqual(before, manifest(root))

            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=control_root,
                task_root=task_root,
                harness=FakeLease([]),
                reconcile=False,
                state_base=root,
                read_only=True,
            )
            health = service.health()
            self.assertEqual("READ_ONLY_RECOVERY_REQUIRED", health["status"])
            self.assertEqual(
                "READ_ONLY_RECOVERY_REQUIRED", health["recovery_code"]
            )
            self.assertTrue(health["read_only"])
            self.assertTrue(health["read_only_recovery"]["required"])
            self.assertEqual(job_id, service.get_job(job_id)["job_id"])
            self.assertEqual(1, len(service.list_jobs()["jobs"]))
            self.assertEqual(before, manifest(root))

            store_mutators = (
                lambda: inspector_store.enqueue_batch(
                    "read-only", [request()], execution_context=execution_context()
                ),
                lambda: inspector_store.claim_next("owner"),
                lambda: inspector_store.mark_task_started(job_id, "owner", "TASK-X"),
                lambda: inspector_store.record_result(job_id, "owner", success_result()),
                lambda: inspector_store.record_blocked(job_id, "owner", "BLOCKED"),
                lambda: inspector_store.retry_with_supplement(
                    job_id, "supplement", "request"
                ),
                lambda: inspector_store.acknowledge_success(job_id, "request"),
                lambda: inspector_store.resume_job(job_id, "request"),
                lambda: inspector_store.skip_failed_job(
                    job_id, "reason", "request"
                ),
                lambda: inspector_store.cancel_queued_job(job_id, "reason"),
                lambda: inspector_store.resolve_manually(
                    job_id,
                    resolution=MANUAL_RESOLUTION_VERIFIED_CLEAN,
                    confirmation="I_CONFIRM_WORKTREE_VERIFIED_CLEAN",
                    expected_revision=0,
                    request_id="request",
                ),
                lambda: inspector_store.pause("reason"),
                inspector_store.resume_queue,
                inspector_store.reconcile_interrupted,
            )
            for mutate in store_mutators:
                with self.subTest(store_mutator=repr(mutate)), self.assertRaises(
                    QueueError
                ) as rejected:
                    mutate()
                self.assertEqual("CONTROL_STORE_READ_ONLY", rejected.exception.code)

            service_mutators = (
                lambda: service.enqueue_jobs("read-only", [request().to_dict()]),
                service.start_queue,
                lambda: service.continue_after_success(job_id, "request"),
                lambda: service.retry_failed_job(
                    job_id, "supplement", "request"
                ),
                lambda: service.retry_blocked_job(job_id, "request"),
                lambda: service.pause_queue("reason"),
                service.resume_queue,
                lambda: service.skip_failed_job(job_id, "reason", "request"),
                lambda: service.resume_interrupted_job(
                    job_id,
                    "I_CONFIRMED_NO_ACTIVE_HARNESS_PROCESS",
                    "request",
                ),
            )
            for mutate in service_mutators:
                with self.subTest(service_mutator=repr(mutate)), self.assertRaises(
                    HarnessServiceError
                ) as rejected:
                    mutate()
                self.assertEqual("CONTROL_SURFACE_READ_ONLY", rejected.exception.code)
            self.assertEqual(before, manifest(root))

    def test_long_lived_service_rejects_configuration_drift_before_enqueue_or_start(self):
        with tempfile.TemporaryDirectory() as temp:
            harness = FakeLease([])
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=harness,
                reconcile=False,
                state_base=Path(temp),
            )
            drifted = harness.validate_configuration()
            drifted["policy_snapshot_sha256"] = "d" * 64
            harness.validate_configuration = lambda: dict(drifted)
            with self.assertRaises(HarnessServiceError) as enqueue_error:
                service.enqueue_jobs("drifted-enqueue", [request().to_dict()])
            self.assertEqual("PROFILE_DRIFT", enqueue_error.exception.code)
            self.assertEqual(0, service.store.queue_snapshot()["total"])
            with self.assertRaises(HarnessServiceError) as start_error:
                service.start_queue()
            self.assertEqual("PROFILE_DRIFT", start_error.exception.code)
            self.assertFalse(service.supervisor.is_running())

    def test_batch_limit_and_read_model(self):
        with tempfile.TemporaryDirectory() as temp:
            harness = FakeLease([])
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=harness,
                reconcile=False,
                state_base=Path(temp),
            )
            result = service.enqueue_jobs("batch-1", [request().to_dict()])
            job_id = result["jobs"][0]["job_id"]
            detail = service.get_job(job_id)
            self.assertEqual("START_OR_WAIT", detail["next_action"])
            with self.assertRaisesRegex(JobContractError, "BATCH_SIZE_OUT_OF_RANGE"):
                service.enqueue_jobs("batch-2", [request().to_dict()] * 21)

    def test_success_must_be_read_and_acknowledged_before_next_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            harness = FakeLease([success_result(), success_result()])
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=harness,
                reconcile=False,
                state_base=Path(temp),
            )
            queued = service.enqueue_jobs(
                "batch-ack",
                [request(client_job_id="a").to_dict(), request(client_job_id="b").to_dict()],
            )
            first_id = queued["jobs"][0]["job_id"]
            service.start_queue()
            self.assertTrue(service.supervisor.wait(5))
            first = service.get_job(first_id)
            self.assertEqual("READ_RESULT_THEN_ACKNOWLEDGE_SUCCESS", first["next_action"])
            self.assertEqual(1, len(harness.calls))
            continued = service.continue_after_success(first_id, "ack-action-1")
            self.assertFalse(continued["replayed"])
            self.assertTrue(service.supervisor.wait(5))
            self.assertEqual(2, len(harness.calls))

    def test_started_interrupted_job_never_auto_resumes_without_git_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "batch-interrupted", [request()])
            job_id = jobs[0]["job_id"]
            store.claim_next("owner-1")
            store.mark_task_started(job_id, "owner-1", "TASK-20260826-120008")
            store.reconcile_interrupted()
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=Path(temp),
            )
            with self.assertRaisesRegex(
                QueueError, "INTERRUPTED_STARTED_TASK_REQUIRES_MANUAL_REVIEW"
            ):
                service.resume_interrupted_job(
                    job_id,
                    "I_CONFIRMED_NO_ACTIVE_HARNESS_PROCESS",
                    "resume-1",
                )

    def test_resume_probe_never_releases_supervisor_owned_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "batch-held-lease", [request()])
            job_id = jobs[0]["job_id"]
            store.claim_next("owner-1")
            store.reconcile_interrupted()
            harness = FakeLease([])
            harness.lease = True
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=harness,
                store=store,
                reconcile=False,
                state_base=Path(temp),
            )
            with self.assertRaisesRegex(QueueError, "ACTIVE_HARNESS_PROCESS"):
                service.resume_interrupted_job(
                    job_id,
                    "I_CONFIRMED_NO_ACTIVE_HARNESS_PROCESS",
                    "resume-held-1",
                )
            self.assertTrue(harness.lease)

    def test_blocked_retry_is_bounded_safe_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "blocked-retry", [request()])
            job_id = jobs[0]["job_id"]
            store.claim_next("owner-1")
            store.record_blocked(job_id, "owner-1", "LOCAL_CODEX_UNAVAILABLE")
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=Path(temp),
            )
            first = service.retry_blocked_job(
                job_id, "blocked-action-1", start_immediately=False
            )
            self.assertFalse(first["replayed"])
            self.assertEqual(QUEUED, first["job"]["status"])
            replay = service.retry_blocked_job(
                job_id, "blocked-action-1", start_immediately=False
            )
            self.assertTrue(replay["replayed"])

    def test_current_claim_marker_allows_only_pretask_recovery_after_prior_attempt(self):
        for mode in ("blocked", "interrupted"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                store = JobStore(root / ".control")
                jobs, _ = valid_enqueue(store,
                    f"claim-scope-{mode}",
                    [request(max_outer_attempts=3)],
                )
                job_id = jobs[0]["job_id"]
                store.claim_next("owner-1")
                store.mark_task_started(
                    job_id, "owner-1", "TASK-20260826-131700"
                )
                store.record_result(job_id, "owner-1", failed_result())
                store.retry_with_supplement(job_id, "fix", "retry-claim-scope")
                store.claim_next("owner-2")

                if mode == "blocked":
                    store.record_blocked(job_id, "owner-2", "PROFILE_DRIFT")
                else:
                    store.reconcile_interrupted()

                current = store.get_job(job_id)
                self.assertEqual(1, len(current["task_ids"]))
                self.assertFalse(current["current_claim_task_started"])
                service = ChatGPTControlService(
                    "D:\\project",
                    "D:\\profile",
                    control_root=root / ".control",
                    task_root=root / ".tasks",
                    harness=FakeLease([]),
                    store=store,
                    reconcile=False,
                    state_base=root,
                )
                if mode == "blocked":
                    resumed = service.retry_blocked_job(
                        job_id,
                        "resume-current-claim",
                        start_immediately=False,
                    )
                else:
                    resumed = service.resume_interrupted_job(
                        job_id,
                        "I_CONFIRMED_NO_ACTIVE_HARNESS_PROCESS",
                        "resume-current-claim",
                    )
                self.assertEqual(QUEUED, resumed["job"]["status"])

    def test_start_queue_reconciles_terminal_partial_commit_even_when_operator_paused(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "paused-partial", [request()])
            job_id = jobs[0]["job_id"]
            store.claim_next("owner-1")
            store.mark_task_started(job_id, "owner-1", "TASK-20260826-121004")
            store.pause("maintenance")
            with patch.object(store, "_write_queue", side_effect=OSError("crash")):
                with self.assertRaises(OSError):
                    store.record_result(job_id, "owner-1", success_result())
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=Path(temp),
            )
            result = service.start_queue()
            self.assertFalse(result["supervisor"]["started"])
            self.assertEqual("", result["queue"]["running_job_id"])
            self.assertEqual(job_id, result["queue"]["blocked_by_job_id"])
            self.assertTrue(result["queue"]["operator_paused"])

    def test_health_flags_partial_claim_without_queue_running_pointer(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = valid_enqueue(store, "health-recovery", [request()])
            with patch.object(store, "_write_queue", side_effect=OSError("crash")):
                with self.assertRaises(OSError):
                    store.claim_next("dead-owner")
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=Path(temp) / ".control",
                task_root=Path(temp) / ".tasks",
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=Path(temp),
            )
            health = service.health()
            self.assertTrue(health["recovery_required"])
            self.assertEqual(
                "STALE_RUNNING_REQUIRES_START_RECONCILIATION",
                health["recovery_code"],
            )
            service.start_queue()
            self.assertEqual(INTERRUPTED, store.get_job(jobs[0]["job_id"])["status"])


class McpAdapterTests(unittest.TestCase):
    def test_check_control_uses_active_baseline_and_batch_delta_read_only(self):
        class Harness:
            def __init__(self):
                self.policy_calls = []

            def validate_configuration(self):
                return {"profile_id": "sample-profile"}

            def cumulative_policy_status(self, **kwargs):
                self.policy_calls.append(kwargs)
                return {
                    "commit_policy": "BATCH_FINAL_COMMIT",
                    "cumulative_worktree": True,
                    "auto_continue_without_commit": True,
                    "batch_owned_delta_supported": True,
                    "pre_job_snapshot_supported": True,
                    "scoped_rollback_supported": True,
                    "final_commit_required": True,
                    "unexpected_runtime_dirty_files": [],
                    "external_frozen_integrity": True,
                    "baseline_declaration_integrity": True,
                }

        harness = Harness()
        queue = {
            "active_baseline_id": "BASELINE-G5-TEST",
            "batch_delta_files": ["sample-service/U02.java"],
        }

        def store_factory(root, *, read_only=False):
            self.assertTrue(read_only)
            return SimpleNamespace(queue_snapshot=lambda: queue)

        args = SimpleNamespace(working_dir="D:\\project", profile_dir="D:\\profile")
        modules = {
            "mcp": types.ModuleType("mcp"),
            "mcp.server": types.ModuleType("mcp.server"),
            "mcp.server.fastmcp": types.ModuleType("mcp.server.fastmcp"),
            "mcp.types": types.ModuleType("mcp.types"),
        }
        modules["mcp.server.fastmcp"].FastMCP = object
        modules["mcp.types"].ToolAnnotations = object
        with patch(
            "chatgpt_control_mcp.validate_control_state_roots"
        ), patch(
            "chatgpt_control_mcp.HarnessService", return_value=harness
        ), patch(
            "chatgpt_control_mcp.JobStore", side_effect=store_factory
        ), patch.dict(
            sys.modules, modules
        ):
            result = _check_control(args)
        self.assertEqual("sample-profile", result["profile_id"])
        self.assertEqual(
            [{
                "active_baseline_id": "BASELINE-G5-TEST",
                "batch_delta_files": ["sample-service/U02.java"],
                "read_only": True,
            }],
            harness.policy_calls,
        )

    def test_mcp_launchers_strip_tunnel_credentials_before_python(self):
        required_clears = (
            "CONTROL_PLANE_API_KEY",
            "OPENAI_ADMIN_KEY",
            "CONTROL_PLANE_EXTRA_HEADERS",
            "MCP_EXTRA_HEADERS",
            "MCP_DISCOVERY_EXTRA_HEADERS",
            "CLOUDFLARED_TUNNEL_TOKEN",
            "TUNNEL_TOKEN",
            "OPENAI_API_KEY",
            "OPENAI_ORG_ID",
            "OPENAI_PROJECT_ID",
        )
        for name in (
            "mcp.bat",
            "mcp_http_sample-profile.bat",
            "mcp_tunnel_http_sample-profile.bat",
        ):
            with self.subTest(name=name):
                text = (Path(__file__).resolve().parent / name).read_text(
                    encoding="utf-8"
                )
                python_call = text.index('"%PYTHON_EXE%" -E -s')
                for variable in required_clears:
                    marker = f'set "{variable}="'
                    self.assertIn(marker, text)
                    self.assertLess(text.index(marker), python_call)

    def test_loopback_only(self):
        self.assertTrue(_is_loopback("127.0.0.1"))
        self.assertTrue(_is_loopback("::1"))
        self.assertFalse(_is_loopback("0.0.0.0"))
        with self.assertRaisesRegex(HarnessServiceError, "NON_LOOPBACK"):
            build_mcp_server(SimpleNamespace(), host="0.0.0.0", port=8765)

    def test_tool_surface_and_annotations_with_sdk_stub(self):
        class ToolAnnotations:
            def __init__(self, **kwargs):
                self.values = kwargs

        class FastMCP:
            def __init__(self, *args, **kwargs):
                self.tools = {}
                self.kwargs = kwargs

            def tool(self, **metadata):
                def decorate(function):
                    self.tools[function.__name__] = (function, metadata)
                    return function

                return decorate

        modules = {
            "mcp": types.ModuleType("mcp"),
            "mcp.server": types.ModuleType("mcp.server"),
            "mcp.server.fastmcp": types.ModuleType("mcp.server.fastmcp"),
            "mcp.types": types.ModuleType("mcp.types"),
        }
        modules["mcp.server.fastmcp"].FastMCP = FastMCP
        modules["mcp.types"].ToolAnnotations = ToolAnnotations
        control = SimpleNamespace(
            health=lambda: {},
            enqueue_jobs=lambda *args: {},
            enqueue_anchored_replacement=lambda *args, **kwargs: {},
            preview_scoped_checkpoint_reconstruction=lambda *args, **kwargs: {},
            create_scoped_checkpoint_reconstruction=lambda *args, **kwargs: {},
            record_qa_corrective_successor=lambda *args, **kwargs: {},
            reproject_strict_batch_result=lambda *args, **kwargs: {},
            start_queue=lambda: {},
            get_queue=lambda: {},
            list_jobs=lambda limit=50: {},
            get_job=lambda job_id: {},
            wait_for_job=lambda job_id, timeout: {},
            retry_failed_job=lambda *args: {},
            retry_review_only=lambda *args, **kwargs: {},
            retry_awaiting_qa_technical=lambda *args, **kwargs: {},
            preview_awaiting_qa_technical=lambda *args, **kwargs: {},
            preview_candidate_ownership_handoff=lambda *args, **kwargs: {},
            handoff_candidate_ownership=lambda *args, **kwargs: {},
            preview_qa_candidate_isolation=lambda *args, **kwargs: {},
            isolate_qa_candidate=lambda *args, **kwargs: {},
            preview_qa_candidate_resolution=lambda *args, **kwargs: {},
            resolve_qa_candidate_for_retry=lambda *args, **kwargs: {},
            resolve_job=lambda *args, **kwargs: {},
            preview_blocked_job_profile_revalidation=lambda *args, **kwargs: {},
            revalidate_blocked_job_profile=lambda *args, **kwargs: {},
            retry_blocked_job=lambda *args: {},
            continue_after_success=lambda *args: {},
            preview_integration_recovery=lambda *args, **kwargs: {},
            recover_integration_only=lambda *args, **kwargs: {},
            commit_job_result=lambda *args, **kwargs: {},
            set_execution_mode=lambda *args, **kwargs: {},
            reload_idle_profile=lambda *args, **kwargs: {},
            cancel_queued_batch=lambda *args, **kwargs: {},
            reset_terminal_queue=lambda *args, **kwargs: {},
            pause_queue=lambda reason: {},
            resume_queue=lambda: {},
            skip_failed_job=lambda *args: {},
            resume_interrupted_job=lambda *args: {},
            reconcile_orphaned_no_delta_job=lambda *args: {},
        )
        with patch.dict(sys.modules, modules):
            server = build_mcp_server(control)
        expected = {
            "get_worklist",
            "preview_corrective_baseline_binding",
            "bind_corrective_baseline",
            "preview_candidate_ownership_handoff",
            "handoff_candidate_ownership",
            "revise_planned_job",
            "release_global_stop",
            "get_control_health",
            "enqueue_jobs",
            "enqueue_anchored_replacement",
            "preview_scoped_checkpoint_reconstruction",
            "create_scoped_checkpoint_reconstruction",
            "record_qa_corrective_successor",
            "reproject_strict_batch_result",
            "start_queue",
            "reconcile_orphaned_no_delta_job",
            "get_queue",
            "list_jobs",
            "get_job",
            "wait_for_job",
            "retry_failed_job",
            "retry_review_only",
            "retry_awaiting_qa_technical",
            "preview_awaiting_qa_technical",
            "preview_qa_candidate_isolation",
            "isolate_qa_candidate",
            "preview_qa_candidate_resolution",
            "resolve_qa_candidate_for_retry",
            "resolve_job",
            "preview_blocked_job_profile_revalidation",
            "revalidate_blocked_job_profile",
            "continue_after_success",
            "preview_integration_recovery",
            "recover_integration_only",
            "commit_job_result",
            "set_execution_mode",
            "reload_idle_profile",
            "cancel_queued_batch",
            "reset_terminal_queue",
            "pause_queue",
            "resume_queue",
        }
        self.assertEqual(expected, set(server.tools))
        start_annotations = server.tools["start_queue"][1]["annotations"].values
        self.assertTrue(start_annotations["destructiveHint"])
        self.assertTrue(start_annotations["openWorldHint"])
        read_annotations = server.tools["get_job"][1]["annotations"].values
        self.assertTrue(read_annotations["readOnlyHint"])
        self.assertTrue(server.tools["preview_candidate_ownership_handoff"][1]["annotations"].values["readOnlyHint"])
        self.assertFalse(server.tools["handoff_candidate_ownership"][1]["annotations"].values["readOnlyHint"])

        with patch.dict(sys.modules, modules):
            inspector = build_mcp_server(control, read_only_surface=True)
        self.assertEqual(
            {
                "get_control_health",
                "get_worklist",
                "preview_corrective_baseline_binding",
                "preview_candidate_ownership_handoff",
                "get_queue",
                "list_jobs",
                "get_job",
                "wait_for_job",
                "preview_qa_candidate_isolation",
                "preview_qa_candidate_resolution",
                "preview_awaiting_qa_technical",
                "preview_scoped_checkpoint_reconstruction",
            },
            set(inspector.tools),
        )

    def test_http_main_constructs_only_a_read_only_service_and_tool_surface(self):
        class Lock:
            def __init__(self, *args, **kwargs):
                pass

            def acquire(self):
                return True

            def release(self):
                return None

        class Server:
            def __init__(self):
                self.transport = ""

            def run(self, transport):
                self.transport = transport

        control = SimpleNamespace(configuration={"profile_id": "sample-profile"})
        server = Server()
        with patch(
            "chatgpt_control_mcp.SingleInstanceLock", Lock
        ), patch(
            "chatgpt_control_mcp.ChatGPTControlService", return_value=control
        ) as service_factory, patch(
            "chatgpt_control_mcp.build_mcp_server", return_value=server
        ) as server_factory:
            exit_code = mcp_main(
                [
                    "--working-dir",
                    "D:\\project",
                    "--profile",
                    "D:\\profile",
                    "--transport",
                    "streamable-http",
                ]
            )
        self.assertEqual(0, exit_code)
        self.assertEqual("streamable-http", server.transport)
        self.assertTrue(service_factory.call_args.kwargs["read_only"])
        self.assertTrue(server_factory.call_args.kwargs["read_only_surface"])

    def test_tunnel_http_main_constructs_writable_service_and_tool_surface(self):
        class Lock:
            def __init__(self, *args, **kwargs):
                pass

            def acquire(self):
                return True

            def release(self):
                return None

        class Server:
            def run(self, transport):
                self.transport = transport

        control = SimpleNamespace(configuration={"profile_id": "sample-profile"})
        server = Server()
        with patch.dict(
            os.environ, {WRITABLE_HTTP_ENABLE_ENV: "1"}, clear=False
        ), patch(
            "chatgpt_control_mcp.SingleInstanceLock", Lock
        ), patch(
            "chatgpt_control_mcp.ChatGPTControlService", return_value=control
        ) as service_factory, patch(
            "chatgpt_control_mcp.build_mcp_server", return_value=server
        ) as server_factory:
            exit_code = mcp_main(
                [
                    "--working-dir",
                    "D:\\project",
                    "--profile",
                    "D:\\profile",
                    "--transport",
                    "streamable-http",
                    "--http-surface",
                    HTTP_SURFACE_TUNNEL,
                    "--port",
                    "8766",
                ]
            )
        self.assertEqual(0, exit_code)
        self.assertEqual("streamable-http", server.transport)
        self.assertFalse(service_factory.call_args.kwargs["read_only"])
        self.assertFalse(server_factory.call_args.kwargs["read_only_surface"])

    def test_writable_http_requires_explicit_environment_gate(self):
        stdout = io.StringIO()
        with patch.dict(os.environ, {}, clear=False), redirect_stdout(stdout):
            os.environ.pop(WRITABLE_HTTP_ENABLE_ENV, None)
            exit_code = mcp_main(
                [
                    "--working-dir",
                    "D:\\project",
                    "--profile",
                    "D:\\profile",
                    "--transport",
                    "streamable-http",
                    "--http-surface",
                    HTTP_SURFACE_TUNNEL,
                ]
            )
        self.assertEqual(2, exit_code)
        self.assertIn("WRITABLE_HTTP_NOT_ENABLED", stdout.getvalue())

    def test_inspector_and_writable_surfaces_use_expected_process_locks(self):
        inspector_args = SimpleNamespace(
            transport="streamable-http", http_surface=HTTP_SURFACE_INSPECTOR
        )
        tunnel_args = SimpleNamespace(
            transport="streamable-http", http_surface=HTTP_SURFACE_TUNNEL
        )
        stdio_args = SimpleNamespace(
            transport="stdio", http_surface=HTTP_SURFACE_INSPECTOR
        )
        self.assertEqual(INSPECTOR_LOCK_NAME, _process_lock_path(inspector_args).name)
        self.assertEqual(WRITABLE_LOCK_NAME, _process_lock_path(tunnel_args).name)
        self.assertEqual(WRITABLE_LOCK_NAME, _process_lock_path(stdio_args).name)

    def test_tunnel_http_launcher_is_loopback_writable_and_uses_separate_port(self):
        text = (
            Path(__file__).resolve().parent / "mcp_tunnel_http_sample-profile.bat"
        ).read_text(encoding="utf-8")
        self.assertIn('set "CONTROL_HOST=127.0.0.1"', text)
        self.assertIn('set "CONTROL_PORT=8766"', text)
        self.assertIn('set "KKM_ALLOW_WRITABLE_HTTP=1"', text)
        self.assertIn("--transport streamable-http", text)
        self.assertIn("--http-surface tunnel-writable", text)

    def test_http_startup_and_health_preserve_control_state_byte_manifest(self):
        class Lock:
            def __init__(self, *args, **kwargs):
                pass

            def acquire(self):
                return True

            def release(self):
                return None

        class Server:
            def __init__(self, control):
                self.control = control

            def run(self, transport):
                self.control.health()
                self.control.get_queue()
                self.control.list_jobs()

        def manifest(base: Path) -> dict[str, str]:
            return {
                path.relative_to(base).as_posix(): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in base.rglob("*")
                if path.is_file()
            }

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            control_root = root / ".control"
            task_root = root / ".tasks"
            store = JobStore(control_root)
            valid_enqueue(store, "http-manifest", [request()])
            task_root.mkdir()
            before = manifest(root)

            def service_factory(working_dir, profile_dir, **kwargs):
                return ChatGPTControlService(
                    working_dir,
                    profile_dir,
                    harness=FakeLease([]),
                    state_base=root,
                    **kwargs,
                )

            def server_factory(control, **kwargs):
                self.assertTrue(kwargs["read_only_surface"])
                self.assertTrue(control.read_only)
                return Server(control)

            with patch(
                "chatgpt_control_mcp.HARNESS_ROOT", root
            ), patch(
                "chatgpt_control_mcp.SingleInstanceLock", Lock
            ), patch(
                "chatgpt_control_mcp.ChatGPTControlService",
                side_effect=service_factory,
            ), patch(
                "chatgpt_control_mcp.build_mcp_server",
                side_effect=server_factory,
            ):
                exit_code = mcp_main(
                    [
                        "--working-dir",
                        "D:\\project",
                        "--profile",
                        "D:\\profile",
                        "--transport",
                        "streamable-http",
                    ]
                )
            self.assertEqual(0, exit_code)
            self.assertEqual(before, manifest(root))

    def test_stdio_runtime_routes_harness_logs_to_stderr(self):
        class Lock:
            def __init__(self, *args, **kwargs):
                pass

            def acquire(self):
                return True

            def release(self):
                return None

        class Server:
            def run(self, transport):
                self.transport = transport
                safe_print("manager progress")

        control = SimpleNamespace(configuration={"profile_id": "sample-profile"})
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.dict(os.environ, {}, clear=False), patch(
            "chatgpt_control_mcp.SingleInstanceLock", Lock
        ), patch(
            "chatgpt_control_mcp.ChatGPTControlService", return_value=control
        ) as service_factory, patch(
            "chatgpt_control_mcp.build_mcp_server", return_value=Server()
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            os.environ.pop("KKM_MCP_STDIO", None)
            exit_code = mcp_main(
                [
                    "--working-dir",
                    "D:\\project",
                    "--profile",
                    "D:\\profile",
                    "--transport",
                    "stdio",
                ]
            )
        self.assertEqual(0, exit_code)
        self.assertEqual("", stdout.getvalue())
        self.assertIn("manager progress", stderr.getvalue())
        self.assertFalse(service_factory.call_args.kwargs["read_only"])

    def test_stdio_disconnect_pauses_and_waits_for_active_supervisor(self):
        class Lock:
            def __init__(self, *args, **kwargs):
                pass

            def acquire(self):
                return True

            def release(self):
                return None

        class Supervisor:
            def __init__(self):
                self.running = True
                self.waited = False

            def is_running(self):
                return self.running

            def wait(self, timeout=None):
                self.waited = True
                self.running = False
                return True

        supervisor = Supervisor()
        pauses = []
        control = SimpleNamespace(
            configuration={"profile_id": "sample-profile"},
            supervisor=supervisor,
            pause_queue=pauses.append,
        )

        class Server:
            def run(self, transport):
                return None

        with patch("chatgpt_control_mcp.SingleInstanceLock", Lock), patch(
            "chatgpt_control_mcp.ChatGPTControlService", return_value=control
        ), patch("chatgpt_control_mcp.build_mcp_server", return_value=Server()):
            exit_code = mcp_main(
                [
                    "--working-dir",
                    "D:\\project",
                    "--profile",
                    "D:\\profile",
                    "--transport",
                    "stdio",
                ]
            )
        self.assertEqual(0, exit_code)
        self.assertEqual(["CONTROL_SERVER_SHUTDOWN"], pauses)
        self.assertTrue(supervisor.waited)


if __name__ == "__main__":
    unittest.main()
