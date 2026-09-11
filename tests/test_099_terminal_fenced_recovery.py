"""Focused regressions for terminal-fenced OpenCode technical recovery."""

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import harness_temp
from control_repository import ControlRepository
from execution_lineage import TECHNICAL_RECOVERY, reserve_execution_attempt
from opencode_runtime_adapter import OpenCodeRuntimeAdapter, PERSONAL
from runtime_adapter import RuntimeDescriptor, SessionQuery
from task_state import TaskState
from manager import Manager
from worker import OpenCodeWorker, codex_host_pair_matches
from harness_service import HarnessService


class TerminalFencedRecoveryTests(unittest.TestCase):
    def test_manager_honors_locked_opencode_worker_effort(self):
        manager = object.__new__(Manager)
        manager.timeout = 300
        manager.timeout_config = None
        manager.opencode_model = ""
        task = TaskState(
            task_id="T", requirement="J01", worker="opencode",
            opencode_model="zai-coding-plan/glm-5.3-flash",
            worker_effective_reasoning_effort="medium",
        )
        task.materialized_execution = {
            "effective_policy": {
                "resolved_worker": {
                    "worker": "opencode",
                    "model": "zai-coding-plan/glm-5.3-flash",
                    "reasoning_effort": "low",
                    "reasoning_effort_locked": True,
                }
            }
        }

        worker, runtime = manager._get_worker(task)

        self.assertEqual(runtime, "opencode")
        self.assertEqual(task.worker_effective_reasoning_effort, "low")
        self.assertEqual(worker.reasoning_effort, "low")

    @unittest.skipUnless(os.name == "nt", "Windows host-pair contract")
    def test_codex_host_pair_resolves_bare_command_from_path(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        installed = root / "Programs" / "OpenAI" / "Codex" / "bin"
        selected = root / "selected"
        installed.mkdir(parents=True)
        selected.mkdir()
        (installed / "codex-code-mode-host.exe").write_bytes(b"same-host")
        (selected / "codex-code-mode-host.exe").write_bytes(b"same-host")
        (selected / "codex.exe").write_bytes(b"cli")
        with patch.dict(os.environ, {"LOCALAPPDATA": str(root)}), patch(
            "worker.shutil.which", return_value=str(selected / "codex.exe")
        ):
            self.assertTrue(codex_host_pair_matches("codex"))

    def test_manager_accepts_only_exact_preserved_source_delta(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        target = root / "sample-service" / "file.xml"
        target.parent.mkdir()
        target.write_text("candidate", encoding="utf-8")
        manager = object.__new__(Manager)
        manager.working_dir = str(root)
        manager.collector = SimpleNamespace(collect=lambda _task, _baseline: SimpleNamespace(
            success=True, error="", changed_files=["sample-service/file.xml"], excluded_files=[],
        ))
        task = TaskState(task_id="T", requirement="J01", worker="codex")
        task.attempt_reservation = {
            "attempt_kind": "TECHNICAL_RECOVERY",
            "provenance": {
                "reuse_source_snapshot": True,
                "preserved_source_hashes": {
                    "sample-service/file.xml": hashlib.sha256(target.read_bytes()).hexdigest(),
                },
            },
        }
        self.assertEqual(
            (True, ""), manager._verify_preserved_source_baseline(task, object())
        )
        target.write_text("changed-again", encoding="utf-8")
        self.assertEqual(
            (False, "TECHNICAL_RECOVERY_SOURCE_HASH_CHANGED"),
            manager._verify_preserved_source_baseline(task, object()),
        )

    def test_same_job_technical_recovery_reuses_source_snapshot(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        workspace = root / "workspace"
        workspace.mkdir()
        source = workspace / "sample-service" / "file.xml"
        source.parent.mkdir()
        source.write_text("candidate", encoding="utf-8")
        snapshot = root / "snapshot.json"
        snapshot.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT", "job_id": "JOB-1",
            "baseline_id": "BASE-1", "snapshot_id": "SNAP-1",
        }), encoding="utf-8")
        service = object.__new__(HarnessService)
        service.working_dir = workspace.resolve()
        service._cumulative_manager = lambda: SimpleNamespace(
            load_job_baseline=lambda _path: SimpleNamespace(repos=[]),
        )
        service.cumulative_policy_status = lambda **_kwargs: {
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_runtime_dirty_files": [],
        }
        job = {
            "job_id": "JOB-1", "pre_job_snapshot_id": "SNAP-1",
            "pre_job_snapshot_manifest": str(snapshot),
            "request": {"target_resources": ["sample-service"]},
            "active_attempt_reservation": {
                "attempt_kind": "TECHNICAL_RECOVERY",
                "provenance": {
                    "reuse_source_snapshot": True,
                    "source_snapshot_id": "SNAP-1",
                    "preserved_source_hashes": {
                        "sample-service/file.xml": hashlib.sha256(source.read_bytes()).hexdigest(),
                    },
                },
            },
        }
        result = service.prepare_job_snapshot(job, {
            "active_baseline_id": "BASE-1", "batch_delta_files": [],
        })
        self.assertEqual("SNAP-1", result["snapshot_id"])
        self.assertEqual(str(snapshot.resolve()), result["manifest_path"])

    def test_frozen_ab_route_keeps_codex_effort_low_on_later_attempts(self):
        manager = object.__new__(Manager)
        manager.timeout = 300
        manager.timeout_config = None
        manager.profile = None
        manager.policy_root = None
        manager.task_root = Path(".tasks")
        manager.worker_type = "codex"
        task = TaskState(
            task_id="T", requirement="J01", worker="codex",
            codex_model="gpt-5.6-luna", codex_reasoning_effort="low",
        )
        task.retry_count = 2
        task.materialized_execution = {
            "effective_policy": {"resolved_worker": {
                "worker": "codex", "model": "gpt-5.6-luna",
                "reasoning_effort": "low", "reasoning_effort_locked": True,
            }}
        }
        worker, selected = manager._get_worker(task)
        self.assertEqual("codex", selected)
        self.assertEqual("low", worker.reasoning_effort)
        self.assertFalse(task.worker_effort_escalated)

    def test_second_technical_recovery_keeps_product_attempt_count(self):
        job = {
            "job_id": "JOB-1", "revision": 10, "outer_attempt": 2,
            "normal_attempt_budget": 2, "technical_recovery_budget": 2,
            "technical_recovery_attempt_count": 1,
            "attempt_reservations": [
                {"attempt_kind": "NORMAL"},
                {"attempt_kind": "TECHNICAL_RECOVERY"},
            ],
        }
        reservation, replayed = reserve_execution_attempt(
            job, {"revision": 20}, attempt_kind=TECHNICAL_RECOVERY,
            provenance={"request_ref": "terminal-fenced"},
        )
        self.assertFalse(replayed)
        self.assertEqual(3, reservation["execution_attempt"])
        self.assertEqual(1, reservation["normal_attempt_no"])
        self.assertEqual(2, reservation["technical_recovery_attempt_no"])

    def test_new_execution_reuses_source_view_without_reopening_terminal_source(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        repo = ControlRepository(Path(temp.name) / "control.sqlite3", create=True)
        job = {
            "job_id": "JOB-1", "logical_job_id": "LOGICAL-1",
            "client_job_id": "CLIENT-1", "status": "QUEUED", "revision": 0,
            "contract_revision": 1, "owner_id": "", "request": {},
            "depends_on": [],
        }
        repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-1"]},
            {"JOB-1": job}, {"JOB-1": {"path": "jobs/JOB-1.json"}},
        )
        context = {
            "profile_snapshot_sha256": "p", "policy_snapshot_sha256": "p",
            "workspace_identity_sha256": "workspace",
        }
        policy = {"effective_policy_sha256": "p", "resolved_worker": {}}
        old = repo.materialize(
            job, context=context, policy=policy, baseline_hash="baseline",
            execution_surface_hash="surface",
        )
        terminal = dict(job, status="AWAITING_QA", revision=1)
        repo.write_job(terminal, expected_revision=0)
        queued = dict(terminal, status="QUEUED", revision=2)
        queued["active_attempt_reservation"] = {
            "attempt_kind": "TECHNICAL_RECOVERY",
            "provenance": {
                "source_execution_id": old["execution_id"],
                "fresh_session_reason": "TERMINAL_EXECUTION_FENCED",
            },
        }
        repo.write_job(queued, expected_revision=1)
        new = repo.materialize(
            queued, context=context, policy=policy, baseline_hash="baseline",
            execution_surface_hash="surface",
        )
        self.assertNotEqual(old["execution_id"], new["execution_id"])
        self.assertEqual(old["source_view_id"], new["source_view_id"])
        self.assertEqual(old["execution_id"], new["handoff_source_execution_id"])
        self.assertEqual("TERMINAL_EXECUTION_FENCED", new["fresh_session_reason"])
        self.assertEqual("SUSPENDED", repo.writer_authority(old["execution_id"])["state"])
        repo.start_attempt(new, attempt_id="A-NEW", role="WORKER", model="m")
        self.assertEqual("ACTIVE", repo.writer_authority(new["execution_id"])["state"])
        self.assertEqual("SUSPENDED", repo.writer_authority(old["execution_id"])["state"])

    def test_opencode_receives_curated_technical_handoff(self):
        captured = {}

        class Adapter:
            def __init__(self, **_kwargs):
                self.base_url = "http://127.0.0.1:1"

            def preflight(self):
                return RuntimeDescriptor(runtime="opencode", version="v", schema_compatible=True)

            def start_local_server(self, **_kwargs):
                return self.base_url

            def stop_local_server(self):
                return None

            def create_session(self, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(
                    base_url=self.base_url, mode=PERSONAL
                ).create_session(prompt, **kwargs)

            def submit_turn(self, session_id, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(
                    base_url=self.base_url, mode=PERSONAL
                ).submit_turn(session_id, prompt, **kwargs)

            def execute_invocation(self, invocation):
                return {"id": "session-new"} if invocation.url.endswith("/session") else None

            def query_session(self, session_id, **_kwargs):
                return SessionQuery(session_id, "IDLE")

            def request(self, method, path, body=None):
                return [{
                    "info": {"role": "assistant", "id": "msg_final",
                             "parentID": captured["native_id"], "finish": "stop"},
                    "parts": [{"type": "text", "text": "done"}],
                }]

        def prompt_builder(requirement, *_args, **_kwargs):
            captured["requirement"] = requirement
            return requirement

        original_submit = Adapter.submit_turn

        def capture_submit(adapter, session_id, prompt, **kwargs):
            invocation = original_submit(adapter, session_id, prompt, **kwargs)
            captured["native_id"] = invocation.body["messageID"]
            return invocation

        Adapter.submit_turn = capture_submit
        task = TaskState(
            task_id="T", requirement="J01 contract", worker="opencode",
            opencode_model="provider/model", target_module=["sample-service"],
        )
        task.handoff_context = "validated evidence; unfinished work"
        with patch.dict(os.environ, {
            "KKM_OPENCODE_LIVE_VALIDATED": "1",
            "OPENCODE_SERVER_PASSWORD": "fixture",
        }), patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", Adapter), patch(
            "worker.build_prompt", side_effect=prompt_builder,
        ):
            result = OpenCodeWorker(timeout=1, model="provider/model").execute(
                task.requirement, ".", task,
            )
        self.assertTrue(result.success)
        self.assertIn("J01 contract", captured["requirement"])
        self.assertIn("[기술 복구 인계]", captured["requirement"])
        self.assertIn("validated evidence; unfinished work", captured["requirement"])


if __name__ == "__main__":
    unittest.main()
