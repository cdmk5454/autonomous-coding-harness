"""Focused OpenCode activity timeout and durable-session recovery regressions."""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import harness_temp
from control_repository import ControlRepository
from manager import Manager
from opencode_runtime_adapter import OpenCodeAdapterError, OpenCodeRuntimeAdapter, PERSONAL
from progress_projection import project_execution_health
from runtime_adapter import CommandDelivery, RuntimeDescriptor, SessionQuery, payload_sha256
from task_state import TaskState
from worker import OpenCodeWorker, TimeoutConfig


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


class PollingAdapter:
    def __init__(self, clock, polls):
        self.clock = clock
        self.polls = polls
        self.index = -1

    def query_session(self, session_id, **_kwargs):
        self.index += 1
        poll = self.polls[min(self.index, len(self.polls) - 1)]
        self.clock.now = poll[0]
        return SessionQuery(session_id, poll[1])

    def request(self, method, path, body=None):
        self.assert_no_write(method, body)
        return self.polls[min(self.index, len(self.polls) - 1)][2]

    @staticmethod
    def assert_no_write(method, body):
        if method != "GET" or body is not None:
            raise AssertionError("unexpected write")


def assistant(message_id, parent, *, finish="tool-calls", parts=None):
    return {
        "info": {
            "role": "assistant", "id": message_id,
            "parentID": parent, "finish": finish,
        },
        "parts": parts or [],
    }


class OpenCodeActivityTimeoutTests(unittest.TestCase):
    def worker(self, *, activity=400, heartbeat=15, lease=45):
        return OpenCodeWorker(
            timeout=300,
            model="provider/model",
            timeout_config=TimeoutConfig(
                analysis_max=activity,
                modification_max=activity,
                inactivity=activity,
                hard_limit=1,
                runtime_heartbeat=heartbeat,
                runtime_lease=lease,
            ),
        )

    def run_wait(self, polls, *, activity=400, heartbeat=15, lease=45, events=None):
        clock = Clock()
        adapter = PollingAdapter(clock, polls)
        task = None
        if events is not None:
            task = SimpleNamespace(
                attempt_id="A-1", _runtime_event_callback=events.append,
            )
        with patch("worker.time.monotonic", clock.monotonic), patch(
            "worker.time.sleep", lambda _seconds: None
        ):
            return self.worker(
                activity=activity, heartbeat=heartbeat, lease=lease,
            )._wait_for_response(
                adapter, "session", "msg_original", Path("."),
                requirement="modify source", task_mode="MODIFICATION",
                task=task,
            )

    def test_continuous_assistant_activity_beyond_300_seconds_does_not_timeout(self):
        result = self.run_wait([
            (0, "RUNNING", [assistant("msg_1", "msg_original")]),
            (350, "IDLE", [
                assistant("msg_1", "msg_original"),
                assistant("msg_final", "msg_original", finish="stop",
                          parts=[{"type": "text", "text": "done"}]),
            ]),
        ])
        self.assertEqual(("msg_final", "done"), result)

    def test_tool_activity_beyond_300_seconds_does_not_timeout(self):
        result = self.run_wait([
            (0, "RUNNING", [assistant(
                "msg_1", "msg_original",
                parts=[{"type": "tool", "id": "tool_1", "state": "running"}],
            )]),
            (350, "IDLE", [
                assistant("msg_1", "msg_original", parts=[{
                    "type": "tool", "id": "tool_1", "state": "completed",
                }]),
                assistant("msg_final", "msg_original", finish="stop",
                          parts=[{"type": "text", "text": "done"}]),
            ]),
        ])
        self.assertEqual("msg_final", result[0])

    def test_activity_resets_modification_timeout_window(self):
        result = self.run_wait([
            (0, "RUNNING", [assistant("msg_1", "msg_original")]),
            (2.5, "RUNNING", [
                assistant("msg_1", "msg_original"),
                assistant("msg_2", "msg_original"),
            ]),
            (5, "IDLE", [assistant(
                "msg_final", "msg_original", finish="stop",
                parts=[{"type": "text", "text": "done"}],
            )]),
        ], activity=3)
        self.assertEqual("done", result[1])

    def test_no_activity_for_configured_timeout_is_technical_timeout(self):
        unchanged = [assistant("msg_1", "msg_original")]
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "RUNNING", unchanged),
                (4, "RUNNING", unchanged),
            ], activity=3)

    def test_heartbeat_keeps_lease_alive_but_does_not_reset_progress_watchdog(self):
        events = []
        unchanged = [assistant("msg_1", "msg_original")]
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "RUNNING", unchanged),
                (2, "RUNNING", unchanged),
                (4, "RUNNING", unchanged),
            ], activity=3, heartbeat=1, lease=3, events=events)
        kinds = [event["event"] for event in events]
        self.assertEqual("STARTED", kinds[0])
        self.assertIn("HEARTBEAT", kinds)
        self.assertIn("TIMEOUT", kinds)
        timeout = next(event for event in events if event["event"] == "TIMEOUT")
        self.assertEqual("VALID", timeout["runtime_health"])
        self.assertEqual("STALLED", timeout["progress_health"])

    def test_expired_heartbeat_lease_is_a_technical_runtime_failure(self):
        now = datetime(2026, 9, 9, tzinfo=timezone.utc)
        task = SimpleNamespace(
            status="RUNNING", stage="WORKER", progress_snapshot={},
            execution_runtime={
                "execution_id": "INV-1", "execution_owner": "owner",
                "execution_role": "WORKER", "pid": 1,
                "last_runtime_heartbeat_at": (now - timedelta(seconds=4)).isoformat(),
                "lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
                "last_progress_at": (now - timedelta(seconds=1)).isoformat(),
                "progress_timeout_seconds": 300, "process_alive": False,
            },
        )
        health = project_execution_health(task, now=now)
        self.assertEqual("LEASE_EXPIRED", health["runtime_health"])
        self.assertEqual("WORKER_RUNTIME_LEASE_EXPIRED", health["failure_code"])
        self.assertTrue(health["reconciliation_required"])

    def test_rpc_timeout_is_not_converted_to_progress_timeout(self):
        class TimedOutAdapter(PollingAdapter):
            def request(self, method, path, body=None):
                raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR", "TimeoutError")

        clock = Clock()
        adapter = TimedOutAdapter(clock, [(0, "RUNNING", [])])
        with patch("worker.time.monotonic", clock.monotonic):
            with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_TRANSPORT_ERROR"):
                self.worker()._wait_for_response(
                    adapter, "session", "msg_original", Path(".")
                )

    def test_all_technical_runtime_timeouts_have_zero_product_retry_delta(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        repo = ControlRepository(Path(temp.name) / "control.sqlite3", create=True)
        job = {
            "job_id": "JOB-1", "logical_job_id": "LOGICAL-1",
            "client_job_id": "CLIENT-1", "status": "QUEUED", "revision": 0,
            "contract_revision": 1, "owner_id": "", "request": {}, "depends_on": [],
        }
        repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-1"]},
            {"JOB-1": job}, {"JOB-1": {"path": "jobs/JOB-1.json"}},
        )
        execution = repo.materialize(
            job,
            context={"profile_snapshot_sha256": "p", "policy_snapshot_sha256": "p"},
            policy={"effective_policy_sha256": "p", "resolved_worker": {}},
            baseline_hash="b", execution_surface_hash="s",
        )
        attempt = repo.start_attempt(execution, attempt_id="A-1", role="WORKER", model="m")
        for failure_code in (
            "OPENCODE_PROGRESS_TIMEOUT",
            "OPENCODE_TRANSPORT_ERROR",
            "OPENCODE_SERVER_START_TIMEOUT",
            "OPENCODE_RUNTIME_HEARTBEAT_PERSISTENCE_FAILED",
            "WORKER_RUNTIME_LEASE_EXPIRED",
        ):
            recovery = repo.record_runtime_recovery(
                execution["execution_id"], attempt["attempt_id"],
                failure_code=failure_code, state="FAILED",
            )
            self.assertEqual(0, recovery["product_retry_delta"], failure_code)

    def test_existing_durable_session_completes_without_redispatch(self):
        temp = harness_temp.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        repo = ControlRepository(root / "control.sqlite3", create=True)
        job = {
            "job_id": "JOB-1", "logical_job_id": "LOGICAL-1",
            "client_job_id": "CLIENT-1", "status": "QUEUED", "revision": 0,
            "contract_revision": 1, "owner_id": "",
            "request": {"worker": "opencode", "model": "provider/model"},
            "depends_on": [],
        }
        repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-1"]},
            {"JOB-1": job}, {"JOB-1": {"path": "jobs/JOB-1.json"}},
        )
        execution = repo.materialize(
            job,
            context={"profile_snapshot_sha256": "p", "policy_snapshot_sha256": "p"},
            policy={"effective_policy_sha256": "p", "resolved_worker": {}},
            baseline_hash="b", execution_surface_hash="s",
        )
        attempt = repo.start_attempt(execution, attempt_id="A-1", role="WORKER", model="m")
        binding = repo.bind_native_session(
            execution["execution_id"], runtime="opencode", native_session_id="session-1",
            adapter_revision="a", runtime_version="v", durable=True, state="IDLE",
        )
        command = repo.prepare_command(
            execution["execution_id"], attempt["attempt_id"], idempotency_key="original",
            payload_sha256=payload_sha256({"prompt": "original"}),
            binding_id=binding["binding_id"],
        )
        repo.set_command_delivery(command["command_id"], CommandDelivery.SENT.value)
        repo.set_command_delivery(
            command["command_id"], CommandDelivery.ACKNOWLEDGED.value,
            native_command_id="msg_original",
        )

        class Adapter:
            writes = 0

            def __init__(self, **_kwargs):
                self.base_url = "http://127.0.0.1:1"

            def preflight(self):
                return RuntimeDescriptor(runtime="opencode", version="v", schema_compatible=True)

            def start_local_server(self, **_kwargs):
                return self.base_url

            def stop_local_server(self):
                return None

            def query_session(self, session_id, **_kwargs):
                return SessionQuery(session_id, "IDLE")

            def request(self, method, path, body=None):
                return [assistant(
                    "msg_final", "msg_original", finish="stop",
                    parts=[{"type": "text", "text": "recovered"}],
                )]

            def execute_invocation(self, _invocation):
                type(self).writes += 1

        task = TaskState(
            task_id="TASK-1", requirement="original", worker="opencode",
            opencode_model="provider/model", target_module=["sample-service"],
        )
        task.control_repository_path = str(repo.path)
        task.execution_id = execution["execution_id"]
        task.attempt_id = attempt["attempt_id"]
        with patch.dict(os.environ, {
            "KKM_OPENCODE_LIVE_VALIDATED": "1",
            "OPENCODE_SERVER_PASSWORD": "fixture",
        }), patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", Adapter), patch(
            "worker.build_prompt", return_value="original"
        ):
            result = OpenCodeWorker(timeout=1, model="provider/model").execute(
                "original", str(root), task
            )
        self.assertTrue(result.success)
        self.assertEqual("recovered", result.stdout)
        self.assertEqual(0, Adapter.writes)
        self.assertEqual(
            "COMPLETED", repo.command(command["command_id"])["delivery_status"]
        )

    def test_completion_requires_correlated_final_assistant_and_idle(self):
        messages = [
            assistant("msg_stale", "msg_other", finish="stop"),
            assistant("msg_partial", "msg_original", finish="tool-calls"),
            assistant("msg_final", "msg_original", finish="stop",
                      parts=[{"type": "text", "text": "complete"}]),
        ]
        self.assertEqual(
            ("msg_final", "complete"),
            OpenCodeWorker._correlated_assistant_response(messages, "msg_original"),
        )
        self.assertEqual(
            ("", ""),
            OpenCodeWorker._correlated_assistant_response(messages, "msg_missing"),
        )

    def test_manager_passes_existing_timeout_config_to_opencode(self):
        manager = object.__new__(Manager)
        manager.timeout = 300
        manager.timeout_config = TimeoutConfig()
        manager.worker_type = "opencode"
        manager.model = ""
        manager.codex_model = ""
        manager.opencode_model = "provider/model"
        manager.profile = None
        manager.policy_root = None
        manager.task_root = Path(".tasks")
        task = TaskState(
            task_id="T", requirement="x", worker="opencode",
            opencode_model="provider/model",
        )
        worker, selected = manager._get_worker(task)
        self.assertEqual("opencode", selected)
        self.assertIs(manager.timeout_config, worker.timeout_config)


if __name__ == "__main__":
    unittest.main()
