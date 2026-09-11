import copy
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import harness_temp
from codex_runtime_adapter import CodexRuntimeAdapter
from control_repository import ControlRepository, RepositoryError
from opencode_runtime_adapter import OpenCodeAdapterError, OpenCodeRuntimeAdapter, PERSONAL
from product_readiness import derive_product_readiness
from runtime_adapter import (
    CommandDelivery, RetryDomain, SessionQuery, RuntimeDescriptor,
    ValidationDisposition, payload_sha256,
)
from job_contract import JobRequest
from job_queue import JobStore
from test_job_control import request, valid_enqueue
from runtime_selection import RuntimeBoundary, RuntimeSelectionError, select_runtime
from droid_runtime_adapter import DroidRuntimeAdapter
from manager import Manager
from task_state import TaskState
from worker import OpenCodeWorker
from types import SimpleNamespace
from validation_disposition import dispositions_for
from troubleshooting_bundle import capture as capture_troubleshooting


def job(job_id="JOB-1", *, status="QUEUED", revision=0):
    return {
        "job_id": job_id,
        "logical_job_id": "LOGICAL-1",
        "client_job_id": "CLIENT-1",
        "status": status,
        "revision": revision,
        "contract_revision": 1,
        "owner_id": "",
        "request": {
            "worker": "codex", "model": "gpt-5.6-sol",
            "target_modules": ["sample-service"], "target_resources": ["sample-service/file"],
        },
        "depends_on": [],
    }


class RuntimeRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "control.sqlite3"
        self.repo = ControlRepository(self.path, create=True)
        self.job = job()
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": [self.job["job_id"]]},
            {self.job["job_id"]: self.job},
            {self.job["job_id"]: {"path": "jobs/JOB-1.json", "sha256": "fixture"}},
        )
        self.execution = self.repo.materialize(
            self.job,
            context={"profile_snapshot_sha256": "profile", "policy_snapshot_sha256": "policy"},
            policy={"effective_policy_sha256": "policy", "resolved_worker": {"worker": "codex"}},
            baseline_hash="baseline", execution_surface_hash="surface",
            extra={"current_requirement": "fixture"},
        )
        self.attempt = self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-1", role="WORKER", model="gpt-5.6-sol"
        )

    def bind(self):
        return self.repo.bind_native_session(
            self.execution["execution_id"], runtime="codex",
            native_session_id="native-session-1", adapter_revision="fixture/1",
            runtime_version="fixture", durable=True,
        )

    def test_all_identities_are_distinct_and_materialization_is_durable(self):
        binding = self.bind()
        command = self.repo.prepare_command(
            self.execution["execution_id"], self.attempt["attempt_id"],
            idempotency_key="initial", payload_sha256=payload_sha256({"prompt": "x"}),
            binding_id=binding["binding_id"],
        )
        turn = self.repo.bind_native_turn(binding["binding_id"], command["command_id"], "native-turn-1")
        process = self.repo.record_runtime_process(
            self.attempt["attempt_id"], runtime="codex", pid=123,
            process_identity="123:start", binding_id=binding["binding_id"],
        )
        identities = {
            self.job["job_id"], self.execution["logical_job_id"],
            self.execution["materialization_id"], self.execution["execution_id"],
            self.attempt["attempt_id"], process["process_id"],
            binding["binding_id"], turn["turn_id"], command["command_id"],
        }
        self.assertEqual(9, len(identities))
        self.assertEqual(
            self.execution["materialization_id"],
            self.repo.materialization(self.execution["execution_id"])["materialization_id"],
        )

    def test_process_kill_resumes_same_session_without_product_retry(self):
        binding = self.bind()
        process = self.repo.record_runtime_process(
            self.attempt["attempt_id"], runtime="codex", pid=123,
            process_identity="123:start", binding_id=binding["binding_id"],
        )
        self.repo.finish_runtime_process(process["process_id"], state="KILLED", exit_code=-1)
        recovery = self.repo.record_runtime_recovery(
            self.execution["execution_id"], self.attempt["attempt_id"],
            failure_code="PROCESS_KILLED", binding_id=binding["binding_id"], state="RESUMED",
        )
        self.assertEqual(binding, self.repo.session_binding(self.execution["execution_id"], "codex"))
        self.assertEqual(0, recovery["product_retry_delta"])
        self.assertEqual(RetryDomain.RUNTIME_RECOVERY.value, recovery["retry_domain"])

    def test_ipc_failure_and_runtime_exhaustion_never_spend_product_retry(self):
        binding = self.bind()
        for state, code in (
            ("FAILED", "IPC_FAILURE"),
            ("FAILED", "OPENCODE_NATIVE_MESSAGE_ID_INVALID"),
            ("FAILED", "OPENCODE_ASSISTANT_ERROR"),
            ("FAILED", "OPENCODE_TURN_TIMEOUT"),
            ("EXHAUSTED", "RECOVERY_EXHAUSTED"),
        ):
            recovery = self.repo.record_runtime_recovery(
                self.execution["execution_id"], self.attempt["attempt_id"],
                failure_code=code, binding_id=binding["binding_id"], state=state,
            )
            self.assertEqual(0, recovery["product_retry_delta"])
        outcome = self.repo.finish_attempt(
            self.attempt["attempt_id"], status="FAILED",
            retry_domain=RetryDomain.RUNTIME_RECOVERY.value,
            failure_code="RECOVERY_EXHAUSTED",
        )
        self.assertEqual(0, outcome["product_retry_delta"])

    def test_real_product_defect_spends_only_semantic_retry(self):
        outcome = self.repo.finish_attempt(
            self.attempt["attempt_id"], status="FAILED",
            retry_domain=RetryDomain.PRODUCT_SEMANTIC_RETRY.value,
            failure_code="REVIEW_PRODUCT_DEFECT",
        )
        self.assertEqual(1, outcome["product_retry_delta"])

    def test_ack_unknown_is_queried_and_never_blindly_resent(self):
        binding = self.bind()
        digest = payload_sha256({"prompt": "once"})
        command = self.repo.prepare_command(
            self.execution["execution_id"], self.attempt["attempt_id"],
            idempotency_key="once", payload_sha256=digest, binding_id=binding["binding_id"],
        )
        self.repo.set_command_delivery(command["command_id"], CommandDelivery.SENT.value)
        decision = self.repo.reconcile_command(self.execution["execution_id"])
        self.assertEqual("QUERY_SESSION_DO_NOT_RESEND", decision["action"])
        replay = self.repo.prepare_command(
            self.execution["execution_id"], self.attempt["attempt_id"],
            idempotency_key="once", payload_sha256=digest, binding_id=binding["binding_id"],
        )
        self.assertEqual(command["command_id"], replay["command_id"])
        with self.assertRaisesRegex(RepositoryError, "COMMAND_IDEMPOTENCY_CONFLICT"):
            self.repo.prepare_command(
                self.execution["execution_id"], self.attempt["attempt_id"],
                idempotency_key="once", payload_sha256=payload_sha256({"prompt": "twice"}),
                binding_id=binding["binding_id"],
            )

    def test_opencode_worker_correlates_native_message_without_duplicate_write(self):
        class FakeOpenCodeAdapter:
            prompt_writes = 0
            session_creates = 0
            message_reads = 0
            submitted_message_id = ""

            def __init__(self, *, base_url="", mode="", username="", password="", timeout=0):
                self.base_url = base_url or "http://127.0.0.1:4096"

            def preflight(self):
                return RuntimeDescriptor(
                    runtime="opencode", version="fixture", adapter_revision="runtime-adapter/2",
                    schema_compatible=True, detail="CLI_AND_SERVER_SCHEMA_PREFLIGHT",
                )

            def start_local_server(self, **_kwargs):
                return self.base_url

            def stop_local_server(self):
                return None

            def create_session(self, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(base_url=self.base_url, mode=PERSONAL).create_session(
                    prompt, **kwargs,
                )

            def submit_turn(self, session_id, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(base_url=self.base_url, mode=PERSONAL).submit_turn(
                    session_id, prompt, **kwargs,
                )

            def execute_invocation(self, invocation):
                if invocation.url.endswith("/session"):
                    type(self).session_creates += 1
                    return {"id": "session-opencode-1"}
                type(self).prompt_writes += 1
                type(self).submitted_message_id = str(invocation.body["messageID"])
                return None

            def query_session(self, session_id, **_kwargs):
                return SessionQuery(session_id, "IDLE")

            def request(self, method, path, body=None):
                self.assert_no_write(method, body)
                type(self).message_reads += 1
                if type(self).message_reads == 1:
                    return [{"info": {"role": "user", "id": type(self).submitted_message_id},
                             "parts": [{"type": "text", "text": "fixture prompt"}]}]
                return [{"info": {"role": "assistant", "id": "msg_response",
                                  "parentID": type(self).submitted_message_id,
                                  "finish": "stop"},
                         "parts": [{"type": "text", "text": "fixture result"}]}]

            @staticmethod
            def assert_no_write(method, body):
                if method != "GET" or body is not None:
                    raise AssertionError("unexpected native write")

        FakeOpenCodeAdapter.prompt_writes = 0
        FakeOpenCodeAdapter.session_creates = 0
        FakeOpenCodeAdapter.message_reads = 0
        FakeOpenCodeAdapter.submitted_message_id = ""
        task = TaskState(
            task_id="JOB-1", requirement="fixture", worker="opencode",
            opencode_model="provider/model", target_module=["sample-service"],
        )
        task.control_repository_path = str(self.path)
        task.execution_id = self.execution["execution_id"]
        task.attempt_id = self.attempt["attempt_id"]
        worker = OpenCodeWorker(timeout=1, model="provider/model")
        environment = {
            "KKM_OPENCODE_LIVE_VALIDATED": "1",
            "KKM_OPENCODE_FALLBACK_RUNTIME": "droid",
            "OPENCODE_SERVER_PASSWORD": "fixture-password",
        }
        with patch.dict(os.environ, environment, clear=False), \
                patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", FakeOpenCodeAdapter), \
                patch("worker.build_prompt", return_value="fixture prompt"):
            first = worker.execute("fixture", str(Path(self.temp.name)), task)
            self.assertTrue(first.success)
            self.assertEqual("fixture result", first.stdout)
            command = self.repo.command(task.command_id)
            self.assertTrue(command["command_id"].startswith("CMD-"))
            self.assertTrue(command["native_command_id"].startswith("msg_"))
            self.assertEqual("", command["native_turn_id"])
            self.assertEqual("msg_response", command["native_response_message_id"])
            connection = sqlite3.connect(self.path)
            try:
                self.assertEqual(0, connection.execute(
                    "SELECT COUNT(*) FROM native_turns"
                ).fetchone()[0])
            finally:
                connection.close()
            second = worker.execute("fixture", str(Path(self.temp.name)), task)
        self.assertTrue(second.success)
        self.assertEqual("fixture result", second.stdout)
        self.assertEqual(1, FakeOpenCodeAdapter.prompt_writes)
        self.assertEqual(1, FakeOpenCodeAdapter.session_creates)
        self.assertGreaterEqual(FakeOpenCodeAdapter.message_reads, 2)

        task.retry_count = 1
        with patch.dict(os.environ, environment, clear=False), \
                patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", FakeOpenCodeAdapter), \
                patch("worker.build_prompt", return_value="review-fix prompt"):
            third = worker.execute("fixture", str(Path(self.temp.name)), task)
        self.assertTrue(third.success)
        self.assertEqual(2, FakeOpenCodeAdapter.prompt_writes)
        semantic_command = self.repo.command(task.command_id)
        self.assertEqual(
            f"{self.attempt['attempt_id']}:opencode:semantic:2:runtime:1",
            semantic_command["idempotency_key"],
        )

    def test_old_writer_fences_new_writer(self):
        first = self.repo.record_runtime_process(
            self.attempt["attempt_id"], runtime="codex", pid=123,
            process_identity="first",
        )
        with self.assertRaisesRegex(RepositoryError, "OLD_RUNTIME_WRITER_ACTIVE"):
            self.repo.record_runtime_process(
                self.attempt["attempt_id"], runtime="codex", pid=456,
                process_identity="second",
            )
        result = self.repo.reconcile_runtime_process(
            self.attempt["attempt_id"], process_alive=lambda *_: False,
        )
        self.assertEqual("PROCESS_LOST_SESSION_PRESERVED", result["action"])
        second = self.repo.record_runtime_process(
            self.attempt["attempt_id"], runtime="codex", pid=456,
            process_identity="second",
        )
        self.assertNotEqual(first["process_id"], second["process_id"])

    def test_terminal_execution_cannot_resurrect(self):
        terminal = copy.deepcopy(self.repo.read_job(self.job["job_id"]))
        terminal.update(status="FAILED_FINAL", revision=terminal["revision"] + 1)
        self.repo.write_job(terminal, expected_revision=terminal["revision"] - 1)
        with self.assertRaisesRegex(RepositoryError, "TERMINAL_EXECUTION_IMMUTABLE"):
            self.repo.start_attempt(
                self.execution, attempt_id="ATTEMPT-2", role="WORKER", model="gpt-5.6-sol"
            )

    def test_deferred_decision_is_idempotent_and_prevents_false_complete(self):
        decision = self.repo.record_decision(
            self.job["job_id"], contract_revision=1, decision_type="DEFERRED",
            question="Choose the authoritative contract", fingerprint="same",
        )
        replay = self.repo.record_decision(
            self.job["job_id"], contract_revision=1, decision_type="DEFERRED",
            question="Choose the authoritative contract", fingerprint="same",
        )
        self.assertEqual(decision["decision_id"], replay["decision_id"])
        pending = derive_product_readiness("SKELETON_READY", self.repo.open_decisions(self.job["job_id"]))
        self.assertEqual("SKELETON_READY", pending["product_readiness"])
        self.assertNotEqual("COMPLETE", pending["product_readiness"])
        resolved = self.repo.resolve_decision(decision["decision_id"], resolution="A")
        self.assertEqual("RESOLVED", resolved["status"])
        self.assertEqual(resolved, self.repo.resolve_decision(decision["decision_id"], resolution="A"))
        with self.assertRaisesRegex(RepositoryError, "DECISION_RESOLUTION_CONFLICT"):
            self.repo.resolve_decision(decision["decision_id"], resolution="B")
        with self.assertRaisesRegex(RepositoryError, "STALE_DECISION_CONTRACT"):
            self.repo.record_decision(
                self.job["job_id"], contract_revision=2, decision_type="CONTRACT",
                question="stale", fingerprint="stale",
            )

    def test_not_run_by_policy_is_neither_pass_fail_nor_retry(self):
        row = self.repo.record_validation(
            self.execution["execution_id"], kind="DB_CONNECT",
            disposition=ValidationDisposition.NOT_RUN_BY_POLICY.value,
        )
        self.assertEqual("NOT_RUN_BY_POLICY", row["disposition"])
        self.assertEqual(0, row["product_retry_delta"])
        with self.assertRaisesRegex(RepositoryError, "VALIDATION_NOT_PRODUCT_FAILURE"):
            self.repo.record_validation(
                self.execution["execution_id"], kind="E2E",
                disposition=ValidationDisposition.NOT_RUN_BY_POLICY.value,
                product_retry=True,
            )

    def test_troubleshooting_bundle_contains_identities_not_prompt_body(self):
        task = TaskState(
            task_id="TASK-1", requirement="sensitive prompt body", job_id=self.job["job_id"],
            logical_job_id=self.execution["logical_job_id"],
            materialization_id=self.execution["materialization_id"],
            execution_id=self.execution["execution_id"], attempt_id=self.attempt["attempt_id"],
            control_repository_path=str(self.path),
        )
        record = capture_troubleshooting(task, Path(self.temp.name) / "tasks")
        raw = Path(record["path"]).read_text(encoding="utf-8")
        self.assertIn(self.execution["execution_id"], raw)
        self.assertNotIn("sensitive prompt body", raw)


class AdapterContractTests(unittest.TestCase):
    def test_codex_normalizes_native_session_turn_and_permission(self):
        adapter = CodexRuntimeAdapter(executable="missing")
        session = adapter.normalize_event({"type": "thread.started", "thread_id": "s1"})[0]
        turn = adapter.normalize_event({"type": "turn.started", "turn_id": "t1"})[0]
        permission = adapter.normalize_event({"type": "approval.requested", "turn_id": "t1"})[0]
        self.assertEqual(("SESSION_BOUND", "s1"), (session.kind, session.session_id))
        self.assertEqual(("TURN_STARTED", "t1"), (turn.kind, turn.turn_id))
        self.assertEqual("TOOL_PERMISSION", permission.pending_kind)

    def test_stale_or_missing_runtime_fails_preflight_without_product_retry(self):
        descriptor = CodexRuntimeAdapter(executable="definitely-not-installed-codex").preflight()
        self.assertFalse(descriptor.schema_compatible)
        self.assertEqual("UNAVAILABLE", descriptor.validation_disposition)
        self.assertFalse(descriptor.live_model_validated)

    def test_opencode_provider_independent_contract_and_permissions(self):
        adapter = OpenCodeRuntimeAdapter(base_url="http://127.0.0.1:4096", mode=PERSONAL)
        create = adapter.create_session("ignored until turn", cwd=".")
        submit = adapter.submit_turn("s1", "hello", cwd=".", model="provider/model")
        permission = adapter.normalize_event({
            "type": "permission.asked", "properties": {"sessionID": "s1", "id": "p1"}
        })[0]
        self.assertEqual(("HTTP", "POST"), (create.transport, create.method))
        self.assertTrue(submit.url.endswith("/session/s1/prompt_async"))
        self.assertEqual("TOOL_PERMISSION", permission.pending_kind)
        self.assertFalse(adapter.preflight().live_model_validated)

    def test_opencode_keeps_canonical_command_id_separate_from_native_message_id(self):
        adapter = OpenCodeRuntimeAdapter(base_url="http://127.0.0.1:4096", mode=PERSONAL)
        first = adapter.submit_turn(
            "s1", "hello", cwd=".", model="provider/model", command_id="CMD-FIXED",
        )
        replay = adapter.submit_turn(
            "s1", "hello", cwd=".", model="provider/model", command_id="CMD-FIXED",
        )
        native_message_id = first.body["messageID"]
        self.assertEqual("CMD-FIXED", first.command_id)
        self.assertTrue(native_message_id.startswith("msg_"))
        self.assertNotEqual(first.command_id, native_message_id)
        self.assertEqual(native_message_id, replay.body["messageID"])

    def test_opencode_completion_correlates_parent_and_rejects_assistant_error(self):
        submitted = "msg_submitted"
        messages = [
            {"info": {"role": "assistant", "id": "msg_stale", "parentID": "msg_old"},
             "parts": [{"type": "text", "text": "stale"}]},
            {"info": {"role": "assistant", "id": "msg_response",
                      "parentID": submitted, "finish": "stop"},
             "parts": [{"type": "text", "text": "correlated"}]},
        ]
        self.assertEqual(
            ("msg_response", "correlated"),
            OpenCodeWorker._correlated_assistant_response(messages, submitted),
        )
        self.assertEqual(
            ("", ""),
            OpenCodeWorker._correlated_assistant_response(messages, "msg_wrong_parent"),
        )
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_ASSISTANT_ERROR"):
            OpenCodeWorker._correlated_assistant_response([{
                "info": {
                    "role": "assistant", "id": "msg_error", "parentID": submitted,
                    "error": {"name": "ProviderError"},
                },
                "parts": [],
            }], submitted)

    def test_runtime_selection_preserves_droid_fallback_without_fake_opencode_ready(self):
        deferred = select_runtime("opencode", env={})
        self.assertEqual("opencode", deferred.selected_runtime)
        self.assertEqual("droid", deferred.fallback_runtime)
        self.assertEqual("DEFERRED_USER_SETUP", deferred.provider_setup)
        self.assertFalse(deferred.opencode_live_validated)
        ready = select_runtime(
            "opencode", "example/model",
            env={"KKM_OPENCODE_LIVE_VALIDATED": "1", "KKM_OPENCODE_FALLBACK_RUNTIME": "droid"},
        )
        self.assertTrue(ready.opencode_live_validated)

    def test_personal_and_managed_writer_boundaries(self):
        personal = RuntimeBoundary.personal()
        with self.assertRaisesRegex(RuntimeSelectionError, "PERSONAL_PRODUCT_WRITER_FORBIDDEN"):
            personal.assert_operation("PRODUCT_WRITE")
        managed = RuntimeBoundary.managed("https://host.example", authenticated=True)
        with self.assertRaisesRegex(RuntimeSelectionError, "MANAGED_NATIVE_UI_MUTATION_FORBIDDEN"):
            managed.assert_operation("NATIVE_UI_MUTATION")
        with self.assertRaisesRegex(RuntimeSelectionError, "MANAGED_REMOTE_TLS_REQUIRED"):
            RuntimeBoundary.managed("http://host.example", authenticated=True)
        with self.assertRaisesRegex(RuntimeSelectionError, "MANAGED_REMOTE_AUTH_REQUIRED"):
            RuntimeBoundary.managed("https://host.example", authenticated=False)

    def test_droid_adapter_maps_installed_native_session_capability(self):
        descriptor = DroidRuntimeAdapter().preflight()
        if descriptor.validation_disposition == "UNAVAILABLE":
            self.skipTest("installed Droid CLI unavailable")
        self.assertTrue(descriptor.schema_compatible)
        self.assertTrue(descriptor.capabilities.durable_session)
        self.assertTrue(descriptor.capabilities.explicit_resume)

    def test_opencode_job_contract_requires_explicit_provider_model(self):
        mapping = request(worker="opencode", model="provider/model", target_modules=["sample-service"],
                          target_resources=["sample-service/file"]).to_dict()
        parsed = JobRequest.from_mapping(mapping, allowed_modules=["sample-service"])
        self.assertEqual(("opencode", "provider/model"), (parsed.worker, parsed.model))
        mapping.pop("model")
        with self.assertRaisesRegex(Exception, "OPENCODE_MODEL_REQUIRED"):
            JobRequest.from_mapping(mapping, allowed_modules=["sample-service"])

    def test_external_delivery_proof_is_separate_and_live_requires_target(self):
        base = request(target_modules=["sample-service"], target_resources=["sample-service/file"]).to_dict()
        with self.assertRaisesRegex(Exception, "EXTERNAL_DELIVERY_POLICY_INVALID"):
            JobRequest.from_mapping(
                {**base, "external_delivery": {"mode": "LIVE"}}, allowed_modules=["sample-service"]
            )
        parsed = JobRequest.from_mapping(
            {**base, "external_delivery": {"mode": "PROOF_ONLY"}}, allowed_modules=["sample-service"]
        )
        task = SimpleNamespace(
            build={"status": "PASS"}, test_status="PASS", review_status="REVIEW_PASS",
            verification_status="VERIFIED", requirement="",
            materialized_execution={"request": parsed.to_dict()},
        )
        delivery = next(item for item in dispositions_for(task) if item["kind"] == "EXTERNAL_DELIVERY")
        self.assertEqual("PASS", delivery["implementation_proof"])
        self.assertEqual("NOT_RUN_BY_POLICY", delivery["delivery_proof"])

    def test_manager_builds_opencode_adapter_without_changing_default_workers(self):
        manager = object.__new__(Manager)
        manager.timeout = 10
        manager.worker_type = "codex"
        manager.model = ""
        manager.codex_model = "gpt-5.6-sol"
        manager.opencode_model = "provider/model"
        manager.profile = None
        manager.policy_root = None
        manager.task_root = Path(".tasks")
        task = TaskState(
            task_id="T", requirement="x", worker="opencode",
            opencode_model="provider/model",
        )
        worker, selected = manager._get_worker(task)
        self.assertIsInstance(worker, OpenCodeWorker)
        self.assertEqual("opencode", selected)
        ordinary = TaskState(task_id="C", requirement="x", worker="codex", codex_model="gpt-5.6-sol")
        self.assertEqual("codex", manager.choose_worker(ordinary))

    def test_opencode_local_health_session_create_query_and_child_cleanup(self):
        # 45s: node/npm cold start can exceed 15s when the full suite is
        # running many other subprocesses on the same host.
        adapter = OpenCodeRuntimeAdapter(mode=PERSONAL, timeout=45)
        descriptor = adapter.preflight()
        if not descriptor.schema_compatible:
            self.skipTest("installed OpenCode CLI unavailable")
        session_id = ""
        server = None
        try:
            adapter.start_local_server(cwd=Path(__file__).parent)
            server = adapter._server
            self.assertTrue(adapter.health()["healthy"])
            created = adapter.execute_invocation(adapter.create_session("not submitted", cwd="."))
            session_id = str(created.get("id") or created.get("sessionID") or "")
            self.assertTrue(session_id)
            self.assertEqual("IDLE", adapter.query_session(session_id, cwd=".").state)
        finally:
            if session_id:
                adapter.request("DELETE", "/session/" + session_id)
            adapter.stop_local_server()
        self.assertIsNotNone(server)
        self.assertIsNotNone(server.poll())
        with self.assertRaises(OpenCodeAdapterError):
            adapter.health()


class MigrationTests(unittest.TestCase):
    def test_schema_v1_is_additively_migrated_without_projection_change(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name) / "v1.sqlite3"
        repo = ControlRepository(path, create=True)
        item = job()
        queue = {"revision": 0, "generation": 1, "order": [item["job_id"]]}
        repo.bootstrap(queue, {item["job_id"]: item}, {item["job_id"]: {"path": "x"}})
        before = repo.read_job(item["job_id"]), repo.read_queue()
        with sqlite3.connect(path) as connection:
            for table in (
                "verification_evidence", "inbound_events", "execution_authority",
                "product_readiness", "validation_dispositions", "decisions", "runtime_recoveries",
                "native_turns", "commands", "runtime_processes", "native_sessions",
                "attempt_outcomes", "materializations",
            ):
                connection.execute(f"DROP TABLE {table}")
            connection.execute("PRAGMA user_version=1")
            connection.commit()
        connection.close()
        migrated = ControlRepository(path)
        self.assertEqual(5, migrated.health()["schema_version"])
        self.assertEqual(before, (migrated.read_job(item["job_id"]), migrated.read_queue()))


class DecisionFinalizationTests(unittest.TestCase):
    def test_resolved_contract_decision_creates_fresh_revision_and_execution(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        legacy = JobStore(root)
        jobs, _ = valid_enqueue(
            legacy, "decision-finalization",
            [request(client_job_id="J-DEFERRED", depends_on=[], target_modules=["sample-service"],
                     target_resources=["sample-service/file"])],
        )
        parent = jobs[0]
        queue = legacy._read_queue()
        parent.update(status="SKELETON_READY", revision=parent["revision"] + 1)
        legacy._write_job(parent)
        repo = ControlRepository(root / "control.sqlite3", create=True)
        repo.bootstrap(
            queue, {parent["job_id"]: parent},
            {parent["job_id"]: {"path": "jobs/" + parent["job_id"] + ".json", "sha256": "fixture"}},
        )
        store = JobStore(root, repository=repo)
        decision = repo.record_decision(
            parent["job_id"], contract_revision=1, decision_type="CONTRACT",
            question="Choose schema", fingerprint="schema",
        )
        repo.resolve_decision(decision["decision_id"], resolution="Use A")
        replacement_mapping = dict(parent["request"])
        replacement_mapping["requirement"] = parent["request"]["requirement"] + "\nAuthoritative decision: Use A"
        replacement = JobRequest.from_mapping(replacement_mapping, allowed_modules=["sample-service"])
        created, replayed = store.enqueue_anchored_replacement(
            parent["job_id"], replacement,
            expected_queue_revision=repo.read_queue()["revision"],
            expected_job_revision=parent["revision"], request_id="finalize-decision",
            reason="resolved contract finalization", execution_context=parent["execution_context"],
            provenance={"status": "VERIFIED"},
            integrity_snapshot={
                "external_frozen_integrity": True, "baseline_declaration_integrity": True,
                "unexpected_runtime_dirty_files": [], "generation": queue["generation"],
                "active_baseline_id": queue.get("active_baseline_id", ""),
            },
            fresh_input={
                "kind": "DECISION_FINALIZATION_FRESH_INPUT", "parent_job_id": parent["job_id"],
                "parent_revision": parent["revision"], "generation": queue["generation"],
                "active_baseline_id": queue.get("active_baseline_id", ""),
            },
            decision_resolution={decision["decision_id"]: "Use A"},
        )
        self.assertFalse(replayed)
        self.assertEqual(2, created["contract_revision"])
        self.assertEqual(parent["client_job_id"], created["decision_resolution"]["logical_job_id"])
        execution = repo.materialize(
            created,
            context={"profile_snapshot_sha256": "profile2", "policy_snapshot_sha256": "policy2"},
            policy={"effective_policy_sha256": "policy2"}, baseline_hash="baseline2",
            execution_surface_hash="surface2", extra={"current_requirement": created["current_requirement"]},
        )
        self.assertEqual(2, execution["contract_revision"])
        self.assertNotEqual(parent["job_id"], execution["job_id"])


if __name__ == "__main__":
    unittest.main()
