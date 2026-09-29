"""Step 11 Discord transport contract tests.

Unit layer (fake transport/control): authorization, phase gating, thread→Job
binding lifecycle, duplicate idempotency, status projection, transport
outage.  Integration layer (real ChatGPTControlService + real store on a
disposable fixture): typed QA approve/reject authority, fail-closed accept,
multi-Job isolation, Telegram/Discord single-entry rule.
"""
from pathlib import Path
from types import SimpleNamespace
import json
import os
import unittest
from datetime import datetime, timezone
from unittest import mock
import io
import urllib.error

import harness_temp
from chatgpt_control import ChatGPTControlService
from chatgpt_control_mcp import remote_entry_conflict
from control_repository import ControlRepository
from discord_remote import (DiscordConfig, DiscordControlRemote,
                            DiscordTransport, ThreadBindingStore)
from job_contract import JobRequest, EXECUTION_CONTEXT_KEYS
from job_queue import JobStore, AWAITING_QA
from manager import Manager
from operator_notifications import terminal_projection
from qa_quarantine import QACandidateStore
from queue_supervisor import QueueSupervisor
from telegram_remote import ControlPlaneTelegramRemote
from task_state import TaskState

USER_ID = 7001
OTHER_USER_ID = 9999
GUILD_ID = 111
CHANNEL_ID = 222
QA_CHANNEL_ID = 223
ALERTS_CHANNEL_ID = 224
REPORTS_CHANNEL_ID = 225
THREAD_ID = 333


class DiscordRESTTransportTests(unittest.TestCase):
    def test_notification_supplies_user_agent_and_preserves_post_semantics(self):
        def endpoint(req, timeout):
            # Reproduce the observed HTTP 403 / 1010 for the default urllib UA.
            if req.get_header("User-agent") != "Harness/0.99.2.2":
                raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {},
                                             io.BytesIO(b"error code: 1010\n"))
            self.assertEqual(req.get_method(), "POST")
            self.assertEqual(req.get_header("Authorization"), "Bot unit-test-token")
            self.assertEqual(req.get_header("Content-type"), "application/json")
            self.assertEqual(json.loads(req.data), {"content": "notification"})
            self.assertEqual(timeout, 15.0)
            return io.BytesIO(b'{"id": "123"}')

        with mock.patch("discord_remote.urllib.request.urlopen", side_effect=endpoint) as request:
            result = DiscordTransport("unit-test-token").send_message(CHANNEL_ID, "notification")
        self.assertTrue(result["delivered"])
        self.assertEqual(result["delivery_status"], "SUCCESS")
        self.assertEqual(result["message_id"], "123")
        request.assert_called_once()

    def test_get_supplies_user_agent_without_body(self):
        with mock.patch("discord_remote.urllib.request.urlopen", return_value=io.BytesIO(b'{"id": "123"}')) as request:
            DiscordTransport("unit-test-token")._request("GET", "/users/@me")
        req = request.call_args.args[0]
        self.assertEqual(req.get_header("User-agent"), "Harness/0.99.2.2")
        self.assertEqual(req.get_method(), "GET")
        self.assertIsNone(req.data)


class DiscordGatewayTimeoutTests(unittest.TestCase):
    def run_gateway(self, failures):
        transport = DiscordTransport("fixture-token")
        ws = mock.Mock()
        incoming = {"id": "42", "content": "!h help", "author": {"id": str(USER_ID)}}
        frames = iter([
            json.dumps({"op": 10, "d": {"heartbeat_interval": 1000}}),
            *failures,
            json.dumps({"op": 0, "t": "MESSAGE_CREATE", "d": incoming}),
        ])

        def receive():
            try:
                frame = next(frames)
            except StopIteration:
                transport._stop.set()
                return json.dumps({"op": 11})
            if isinstance(frame, Exception):
                raise frame
            return frame

        ws.recv.side_effect = receive
        # End the fixture at the first real disconnect; never retry/network.
        ws.close.side_effect = transport._stop.set
        ticks = iter(range(20))
        with mock.patch.object(transport, "_connect", return_value=ws) as connect, \
                mock.patch("discord_remote.time.monotonic", side_effect=lambda: next(ticks)):
            transport._gateway_loop()
        connect.assert_called_once()
        ws.close.assert_called_once()
        self.assertIsNone(transport._ws)
        transport.stop()
        self.assertTrue(transport._stop.is_set())
        return transport.poll_messages(), ws, incoming

    def test_builtin_idle_timeout_keeps_receiving(self):
        messages, ws, incoming = self.run_gateway([TimeoutError(), TimeoutError()])
        self.assertEqual([incoming], messages)
        self.assertIn(1, [json.loads(call.args[0])["op"] for call in ws.send.call_args_list])

    def test_websocket_idle_timeouts_keep_receiving_and_heartbeating(self):
        from websocket import WebSocketTimeoutException
        messages, ws, incoming = self.run_gateway([
            WebSocketTimeoutException("idle"), WebSocketTimeoutException("idle")])
        self.assertEqual([incoming], messages)
        self.assertIn(1, [json.loads(call.args[0])["op"] for call in ws.send.call_args_list])

    def test_real_websocket_failures_still_close_connection(self):
        from websocket import WebSocketConnectionClosedException, WebSocketProtocolException
        for failure in (WebSocketConnectionClosedException("closed"),
                        WebSocketProtocolException("invalid frame"), RuntimeError("unexpected")):
            with self.subTest(exception=type(failure).__name__):
                messages, ws, _ = self.run_gateway([failure])
                self.assertEqual([], messages)
                self.assertEqual(2, ws.recv.call_count)


class FakeTransport:
    def __init__(self):
        self.sent = []
        self.threads = []
        self.next_thread = THREAD_ID
        self.messages = []
        self.fail_send = False
        self.started = False

    def verify_login(self):
        return True

    def start(self):
        self.started = True
        return True

    def stop(self):
        self.started = False

    def send_message(self, channel_id, text):
        if self.fail_send:
            return {"delivered": False, "delivery_status": "FAILED",
                    "failure_code": "DISCORD_CONNECTIONERROR"}
        self.sent.append((channel_id, text))
        return {"delivered": True, "delivery_status": "SUCCESS", "message_id": str(len(self.sent))}

    def create_thread(self, channel_id, name, content):
        self.send_message(channel_id, content)
        if self.fail_send:
            return None
        thread_id = self.next_thread
        self.next_thread += 1
        self.threads.append({"thread_id": thread_id, "name": name})
        return {"thread_id": thread_id, "anchor_message_id": "0"}

    def poll_messages(self):
        messages = self.messages
        self.messages = []
        return messages

    def replies(self):
        return [text for _channel, text in self.sent]


def message(msg_id, content, *, user_id=USER_ID, channel_id=CHANNEL_ID):
    return {"id": str(msg_id), "content": content, "channel_id": str(channel_id),
            "guild_id": str(GUILD_ID),
            "author": {"id": str(user_id), "username": "u"}}


class FakeControl:
    def __init__(self, jobs=None, queue=None):
        self.jobs = {job["job_id"]: dict(job) for job in (jobs or [])}
        self.queue = queue or {}
        self.calls = []

    def get_queue(self):
        return {"queue": self.queue}

    def list_jobs(self, limit=50):
        return {"jobs": [dict(job) for job in self.jobs.values()][:limit]}

    def list_actionable_qa_jobs(self):
        # These command-layer fixtures represent current open QA authority.
        return {"jobs": [{**job, "qa_authority": {"generation": 1, "contract_revision": 1}}
                         for job in self.jobs.values() if job["status"] == "AWAITING_QA"]}

    def get_job(self, job_id):
        if job_id not in self.jobs:
            raise KeyError(job_id)
        return dict(self.jobs[job_id])

    def get_operator_action_context(self, job_id):
        from control_repository import digest
        job = self.get_job(job_id)
        binding = {"job_id": job_id, "job_revision": job["revision"],
                   "contract_revision": 1, "queue_generation": 1,
                   "execution_id": job["last_result"]["execution_id"],
                   "candidate_id": job["candidate_id"],
                   "request_sha256": digest({}), "control_epoch": 1,
                   "decision_ids": ["DECISION-" + job_id]}
        return {"schema_revision": "operator-action/1", "binding": binding,
                "binding_sha256": digest(binding),
                "legal_next_actions": ["GET_STATUS", "GET_REPORT", "HUMAN_ACCEPT", "HUMAN_REJECT"]}

    def get_worklist(self):
        return {"jobs": [{"job_id": job["job_id"], "contract_revision": 1,
                          "generation": 1} for job in self.jobs.values()]}

    def continue_after_success(self, job_id, request_id, **kwargs):
        self.calls.append(("continue", job_id, request_id))
        return {"replayed": False}

    def resolve_qa_decision(self, job_id, **kwargs):
        self.calls.append(("resolve_qa_decision", job_id, kwargs))
        job = self.jobs[job_id]
        if kwargs.get("resolution") == "HUMAN_REJECT":
            job["status"] = "CANCELLED"
        else:
            job["status"] = "SUCCEEDED"
        return {"resolution": kwargs.get("resolution"), "status": job["status"]}


def qa_job(job_id="JOB-A", candidate="CAND-A1", client="A"):
    return {"job_id": job_id, "client_job_id": client, "status": "AWAITING_QA",
            "revision": 4, "candidate_id": candidate,
            "last_result": {"execution_id": "EXEC-A", "failure_code": "USER_QA_REQUIRED",
                            "developer_change_report": {"path": "x", "business_report": {"path": "y"}}}}


class Step11DiscordTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.transport = FakeTransport()
        self.control = FakeControl(jobs=[qa_job(), qa_job("JOB-B", "CAND-B1", "B")])
        self.control.queue = {
            "running_job_id": "", "blocked_by_job_id": "JOB-A", "gate_reason": "AWAITING_QA",
            "progress": {"schema_version": 1, "progress_revision": 7,
                         "job": {"id": "JOB-A", "status": "AWAITING_QA"},
                         "stage": {"name": "AWAITING_QA"},
                         "gates": {"build": "PASS", "test": "PASS"},
                         "active_jobs": [
                             {"job_id": "JOB-A", "client_job_id": "A", "stage": {"name": "QA", "attempt": 1},
                              "gates": {"build": "PASS"}, "next": {"stage": "-"}},
                             {"job_id": "JOB-B", "client_job_id": "B", "stage": {"name": "WORKER", "attempt": 1},
                              "gates": {}, "next": {"stage": "BUILD"}}],
                         "batch": {"current": 1, "total": 2}, "user_action_required": True},
        }
        self.bindings = ThreadBindingStore(self.root / "bindings.json")
        self.remote = self.make_remote()

    def make_remote(self, commands_enabled=True):
        config = DiscordConfig(token="t" * 8, allowed_user_ids={USER_ID},
                               allowed_guild_id=GUILD_ID, allowed_channel_id=CHANNEL_ID,
                               commands_enabled=commands_enabled, env_path=self.root / ".env",
                               qa_channel_id=QA_CHANNEL_ID, alerts_channel_id=ALERTS_CHANNEL_ID,
                               reports_channel_id=REPORTS_CHANNEL_ID)
        return DiscordControlRemote(self.control, config, transport=self.transport,
                                    bindings=self.bindings, control_root=self.root)

    # ------------------------------------------------------------------
    def test_config_missing_reports_user_action_required(self):
        config = DiscordConfig.load(self.root / "missing.env")
        self.assertEqual("USER_ACTION_REQUIRED", config.state)
        self.assertFalse(config.configured)
        summary = config.describe()
        self.assertFalse(summary["token_present"])
        self.assertNotIn('"token"', json.dumps(summary))

    def test_unauthorized_user_rejected_without_control_call(self):
        outcome = self.remote.handle(message(1, "!h status", user_id=OTHER_USER_ID))
        self.assertEqual("UNAUTHORIZED", outcome["outcome"])
        self.assertEqual([], self.control.calls)
        self.assertIn("허가되지 않은", self.transport.replies()[0])

    def test_unknown_command_and_wrong_location_rejected(self):
        self.assertEqual("UNKNOWN_COMMAND", self.remote.handle(message(1, "!h sql_drop"))["outcome"])
        outcome = self.remote.handle(message(2, "!h status", channel_id=987654))
        self.assertEqual("WRONG_LOCATION", outcome["outcome"])
        self.assertEqual([], self.control.calls)

    def test_phase_a_disables_state_changing_commands(self):
        remote = self.make_remote(commands_enabled=False)
        self.assertEqual("COMMANDS_DISABLED", remote.handle(message(1, "!h approve"))["outcome"])
        self.assertEqual("COMMANDS_DISABLED", remote.handle(message(2, "!h bind JOB-A"))["outcome"])
        self.assertEqual("OK", remote.handle(message(3, "!h status"))["outcome"])
        self.assertEqual([], self.control.calls)

    def test_status_projection_lists_active_jobs_separately(self):
        outcome = self.remote.handle(message(1, "!h status"))
        self.assertEqual("OK", outcome["outcome"])
        reply = self.transport.replies()[-1]
        self.assertIn("A:", reply)
        self.assertIn("B:", reply)

    def test_bind_creates_thread_binding_and_approve_routes_to_authority(self):
        bind = self.remote.handle(message(1, "!h bind JOB-A"))
        self.assertEqual("OK", bind["outcome"])
        binding = self.bindings.get(THREAD_ID)
        self.assertEqual("JOB-A", binding["job_id"])
        self.assertEqual("CAND-A1", binding["candidate_id"])
        self.assertEqual(QA_CHANNEL_ID, binding["parent_channel_id"])
        approve = self.remote.handle(message(2, "!h approve", channel_id=THREAD_ID))
        self.assertEqual("OK", approve["outcome"])
        self.assertEqual(1, len(self.control.calls))
        action, job_id, kwargs = self.control.calls[0]
        self.assertEqual("resolve_qa_decision", action)
        self.assertEqual("JOB-A", job_id)
        self.assertEqual("HUMAN_ACCEPT", kwargs["resolution"])
        self.assertEqual("discord-2-approve", kwargs["request_id"])
        # Binding closed after resolution.
        self.assertEqual("CLOSED", self.bindings.get(THREAD_ID)["state"])

    def test_duplicate_interaction_is_replayed_not_reexecuted(self):
        self.remote.handle(message(1, "!h bind JOB-A"))
        first = self.remote.handle(message(2, "!h reject", channel_id=THREAD_ID))
        self.assertEqual("OK", first["outcome"])
        second = self.remote.handle(message(2, "!h reject", channel_id=THREAD_ID))
        self.assertEqual("DUPLICATE_REPLAY", second["outcome"])
        self.assertEqual(1, len([c for c in self.control.calls if c[0] == "resolve_qa_decision"]))

    def test_stale_candidate_approval_rejected(self):
        self.remote.handle(message(1, "!h bind JOB-A"))
        self.control.jobs["JOB-A"]["candidate_id"] = "CAND-A2"
        outcome = self.remote.handle(message(2, "!h approve", channel_id=THREAD_ID))
        self.assertEqual("REJECTED", outcome["outcome"])
        self.assertEqual("STALE_BINDING_AUTHORITY_CHANGED", outcome["code"])
        self.assertEqual("STALE", self.bindings.get(THREAD_ID)["state"])
        self.assertEqual([], self.control.calls)

    def test_terminal_job_command_rejected_and_binding_closed(self):
        self.remote.handle(message(1, "!h bind JOB-A"))
        self.control.jobs["JOB-A"]["status"] = "SUCCEEDED"
        outcome = self.remote.handle(message(2, "!h approve", channel_id=THREAD_ID))
        self.assertEqual("REJECTED", outcome["outcome"])
        self.assertEqual("JOB_NOT_AWAITING_QA", outcome["code"])
        self.assertEqual("CLOSED", self.bindings.get(THREAD_ID)["state"])
        self.assertEqual([], self.control.calls)

    def test_foreign_thread_and_unbound_thread_rejected(self):
        self.remote.handle(message(1, "!h bind JOB-A"))
        # JOB-A bound to THREAD_ID; a different thread claiming JOB-B fails.
        self.control.jobs["JOB-B"]["status"] = "AWAITING_QA"
        outcome = self.remote.handle(message(2, "!h bind JOB-B", channel_id=THREAD_ID))
        self.assertEqual("WRONG_LOCATION", outcome["outcome"])
        unbound = self.remote.handle(message(3, "!h approve"))
        self.assertEqual("WRONG_LOCATION", unbound["outcome"])

    def test_two_active_jobs_are_isolated(self):
        self.control.jobs["JOB-B"]["status"] = "AWAITING_QA"
        self.remote.handle(message(1, "!h bind JOB-A"))
        self.remote.handle(message(2, "!h reject", channel_id=THREAD_ID))
        self.assertEqual("CANCELLED", self.control.jobs["JOB-A"]["status"])
        self.assertEqual("AWAITING_QA", self.control.jobs["JOB-B"]["status"])
        self.assertEqual("CAND-B1", self.control.jobs["JOB-B"]["candidate_id"])

    def test_transport_outage_keeps_qa_pending_without_mutation(self):
        before = json.dumps(self.control.jobs["JOB-A"], sort_keys=True)
        self.transport.fail_send = True
        result = self.remote.notify_once()
        self.assertFalse(result["emitted"])
        self.assertEqual(before, json.dumps(self.control.jobs["JOB-A"], sort_keys=True))
        self.assertEqual([], self.control.calls)
        self.assertEqual("AWAITING_QA", self.control.jobs["JOB-A"]["status"])
        self.assertFalse(self.bindings.active_for_job("JOB-A"))

    def test_notification_suppression_and_meaningful_change(self):
        first = self.remote.notify_once()
        self.assertTrue(first["emitted"])
        sent_after_first = len(self.transport.sent)
        second = self.remote.notify_once()
        self.assertFalse(second["emitted"])  # same revision → silent
        self.assertEqual(sent_after_first, len(self.transport.sent))
        self.control.queue["progress"]["progress_revision"] = 8
        third = self.remote.notify_once()
        self.assertTrue(third["emitted"])

    def test_qa_thread_auto_created_with_binding(self):
        self.remote.notify_once()
        binding = self.bindings.active_for_job("JOB-A")
        self.assertTrue(binding)
        self.assertEqual("CAND-A1", binding["candidate_id"])
        reply = "\n".join(self.transport.replies())
        self.assertIn("사용자 확인 필요", reply)

    def test_bot_restart_recovers_binding_from_durable_store(self):
        self.remote.handle(message(1, "!h bind JOB-A"))
        restarted = self.make_remote()
        outcome = restarted.handle(message(2, "!h reject", channel_id=THREAD_ID))
        self.assertEqual("OK", outcome["outcome"])
        self.assertEqual(1, len(restarted._control.calls))

    @mock.patch.dict(os.environ, {"KKM_TG_BOT_TOKEN": ""})
    def test_single_entry_rule_and_telegram_default_disabled(self):
        self.assertTrue(remote_entry_conflict(True, True))
        self.assertFalse(remote_entry_conflict(True, False))
        self.assertFalse(remote_entry_conflict(False, True))
        os.environ.pop("KKM_TG_ENABLED", None)
        telegram = ControlPlaneTelegramRemote(
            SimpleNamespace(), allowed_user_ids=[1],
            api_base="http://127.0.0.1:1")
        self.assertFalse(telegram.start())  # TELEGRAM_COMMAND_ENTRY=DISABLED
        try:
            os.environ["KKM_TG_ENABLED"] = "1"
            self.assertFalse(telegram.start())  # no token → still disabled, no network use
        finally:
            os.environ.pop("KKM_TG_ENABLED", None)


class FakeLeaseHarness:
    def __init__(self, working_dir):
        self.working_dir = Path(working_dir)

    def validate_configuration(self):
        return {"available_modules": ["app"]}

    def probe_execution_idle(self):
        return True

    def acquire_execution_lease(self):
        return True

    def release_execution_lease(self):
        return None

    def _load_profile(self):
        return SimpleNamespace(id="t"), ["app"]


class Step11QAAuthorityTests(unittest.TestCase):
    """Typed QA decision through the real control service (disposable store)."""

    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control_root = self.root / ".control"
        self.control_root.mkdir()
        repo = ControlRepository(self.control_root / "control.sqlite3", create=True)
        repo.bootstrap(JobStore._empty_queue(), {}, {})
        (self.control_root / "sqlite-authority.json").write_text(
            json.dumps({"authority": "SQLITE", "schema_version": 6}))
        self.store = JobStore(self.control_root, repository=repo)
        keys = EXECUTION_CONTEXT_KEYS - {"profile_id", "profile_schema_version"}
        context = {"profile_id": "t", "profile_schema_version": 1, **{k: "a" * 64 for k in keys}}
        jobs, _ = self.store.enqueue_batch(
            "step11-qa",
            [JobRequest(client_job_id="A", requirement="r", worker="hermes", model="m",
                        reasoning_effort="low", target_modules=("app",),
                        target_resources=("app/app.py",), commit_summary="s11",
                        max_outer_attempts=1)],
            execution_context=context)
        self.job_id = jobs[0]["job_id"]
        workspace = self.root / "workspace"
        workspace.mkdir()
        (workspace / "app.py").write_text("x=1\n", encoding="utf-8")
        self.harness = FakeLeaseHarness(workspace)
        self.service = ChatGPTControlService(
            str(workspace), str(self.root / "profile"),
            control_root=self.control_root, task_root=self.root / ".tasks",
            harness=self.harness, store=self.store, reconcile=False,
            state_base=self.root)
        self.candidate_id = self._quarantined_candidate(workspace)

    def _quarantined_candidate(self, workspace) -> str:
        patch = b"--- a/app.py\n+++ b/app.py\n"
        candidate_id = "CAND-A11A11A11A11A11A11A11A11"
        store = QACandidateStore(self.control_root / "candidates", workspace,
                                 SimpleNamespace(id="t"))
        directory = self.control_root / "candidates" / candidate_id
        directory.mkdir(parents=True)
        (directory / "candidate.patch").write_bytes(patch)
        from qa_quarantine import _sha
        store._write_manifest(directory, {
            "schema_version": 2, "candidate_id": candidate_id, "state": "QUARANTINED",
            "created_at": "2026-09-20T00:00:00+00:00", "job_id": self.job_id,
            "delta": {"changed_files": ["app.py"],
                      "files": [{"path": "app.py", "before_sha256": _sha(b""),
                                 "candidate_sha256": "DELETED", "state": "DELETED",
                                 "snapshot": ""}],
                      "patch_path": "candidate.patch", "diff_sha256": _sha(patch)}})
        return candidate_id

    def _awaiting_qa(self):
        job = self.store.get_job(self.job_id)
        job["status"] = AWAITING_QA
        job["candidate_id"] = self.candidate_id
        job["revision"] += 1
        self.store.repository.write_job(job, expected_revision=job["revision"] - 1)
        return job

    def _candidate_decision(self, job):
        self.store.repository.record_decision(
            self.job_id, contract_revision=job.get("contract_revision", 1),
            decision_type="CANDIDATE", question="Review fixture candidate",
            payload={"candidate_id": self.candidate_id},
        )

    def test_reject_records_typed_decision_and_cancels_without_promotion(self):
        job = self._awaiting_qa()
        self._candidate_decision(job)
        result = self.service.resolve_qa_decision(
            self.job_id, resolution="HUMAN_REJECT", expected_revision=job["revision"],
            request_id="discord-100-reject",
            confirmation="I_CONFIRM_QA_HUMAN_REJECT", candidate_id=self.candidate_id)
        self.assertEqual("HUMAN_REJECT", result["resolution"])
        final = self.store.get_job(self.job_id)
        self.assertEqual("CANCELLED", final["status"])
        self.assertEqual("HUMAN_REJECTED", final["last_result"]["failure_code"])
        decisions = self.store.repository.decisions(self.job_id, status="RESOLVED")
        self.assertEqual(1, len(decisions))
        self.assertEqual("HUMAN_REJECT", decisions[0]["resolution"])
        candidate = QACandidateStore(self.control_root / "candidates",
                                     self.harness.working_dir,
                                     SimpleNamespace(id="t")).load(self.candidate_id)
        self.assertEqual("REJECTED", candidate["state"])

    def test_accept_fails_closed_without_promotion_owner(self):
        job = self._awaiting_qa()
        with self.assertRaises(Exception) as caught:
            self.service.resolve_qa_decision(
                self.job_id, resolution="HUMAN_ACCEPT", expected_revision=job["revision"],
                request_id="discord-101-approve",
                confirmation="I_CONFIRM_QA_HUMAN_ACCEPT", candidate_id=self.candidate_id)
        self.assertIn("QA_ACCEPT_PROMOTION_UNAVAILABLE", str(caught.exception))
        # Fail-closed: nothing was recorded, job still awaiting QA.
        self.assertEqual([], self.store.repository.decisions(self.job_id, status="RESOLVED"))
        self.assertEqual(AWAITING_QA, self.store.get_job(self.job_id)["status"])

    def test_candidate_mismatch_and_bad_confirmation_rejected(self):
        job = self._awaiting_qa()
        with self.assertRaises(Exception) as caught:
            self.service.resolve_qa_decision(
                self.job_id, resolution="HUMAN_REJECT", expected_revision=job["revision"],
                request_id="discord-102-reject",
                confirmation="I_CONFIRM_QA_HUMAN_REJECT", candidate_id="CAND-FOREIGN")
        self.assertIn("QA_DECISION_CANDIDATE_MISMATCH", str(caught.exception))
        with self.assertRaises(Exception) as caught:
            self.service.resolve_qa_decision(
                self.job_id, resolution="HUMAN_REJECT", expected_revision=job["revision"],
                request_id="discord-103-reject",
                confirmation="yes", candidate_id=self.candidate_id)
        self.assertIn("QA_CONFIRMATION_REQUIRED", str(caught.exception))
        self.assertEqual("AWAITING_QA", self.store.get_job(self.job_id)["status"])

    def test_two_jobs_isolated_at_authority_layer(self):
        other = self.store.enqueue_batch(
            "step11-qa-2",
            [JobRequest(client_job_id="B", requirement="r2", worker="hermes", model="m",
                        reasoning_effort="low", target_modules=("app",),
                        target_resources=("app/app.py",), commit_summary="s11",
                        max_outer_attempts=1)],
            execution_context=self.service.store.get_job(self.job_id)["execution_context"])[0][0]
        other_before = self.store.get_job(other["job_id"])
        job = self._awaiting_qa()
        self._candidate_decision(job)
        self.service.resolve_qa_decision(
            self.job_id, resolution="HUMAN_REJECT", expected_revision=job["revision"],
            request_id="discord-104-reject",
            confirmation="I_CONFIRM_QA_HUMAN_REJECT", candidate_id=self.candidate_id)
        other_after = self.store.get_job(other["job_id"])
        self.assertEqual("QUEUED", other_after["status"])
        self.assertEqual(other_before["revision"], other_after["revision"])
        self.assertEqual([], self.store.repository.decisions(other["job_id"]))


class ActionableQASelectorTests(unittest.TestCase):
    setUp = Step11QAAuthorityTests.setUp
    _quarantined_candidate = Step11QAAuthorityTests._quarantined_candidate
    _awaiting_qa = Step11QAAuthorityTests._awaiting_qa

    def prepare(self):
        job = self._awaiting_qa()
        self.store.repository.record_decision(
            self.job_id, contract_revision=1, decision_type="CANDIDATE",
            question="Fixture human review", payload={"candidate_id": self.candidate_id})
        return job

    def change_job(self, **changes):
        job = self.store.get_job(self.job_id)
        revision = job["revision"]
        job.update(changes)
        job["revision"] = revision + 1
        self.store.repository.write_job(job, expected_revision=revision)

    def remote(self):
        self.transport = FakeTransport()
        self.bindings = ThreadBindingStore(self.root / "selector-bindings.json")
        config = DiscordConfig("fixture", {USER_ID}, GUILD_ID, CHANNEL_ID, False, self.root / "unused")
        return DiscordControlRemote(self.service, config, transport=self.transport, bindings=self.bindings)

    def assert_not_projected(self):
        self.assertEqual([], self.store.actionable_qa_jobs())
        remote = self.remote()
        self.assertFalse(remote._ensure_qa_threads({}))
        self.assertEqual([], self.transport.sent)
        self.assertEqual([], self.transport.threads)
        self.assertFalse((self.root / "selector-bindings.json").exists())

    def test_historical_job_outside_active_window_is_not_projected(self):
        self.prepare()
        queue = self.store.repository.read_queue()
        revision = queue["revision"]
        queue.update(active_from_index=len(queue["order"]), revision=revision + 1)
        self.store.repository.write_queue(queue, expected_revision=revision)
        self.assert_not_projected()

    def test_prior_generation_is_not_projected(self):
        job = self.prepare()
        self.change_job(generation=job["generation"] - 1)
        self.assert_not_projected()

    def test_transferred_candidate_ownership_is_not_projected(self):
        self.prepare()
        self.change_job(candidate_ownership_handoff={"event": "CANDIDATE_OWNERSHIP_HANDED_OFF", "target_job_id": "successor"})
        self.assert_not_projected()

    def test_superseded_job_is_not_projected(self):
        self.prepare()
        self.change_job(superseded_by_job_id="successor")
        self.assert_not_projected()

    def test_status_without_open_decision_is_not_projected(self):
        self._awaiting_qa()
        self.assert_not_projected()

    def test_wrong_candidate_decision_is_not_projected(self):
        self.prepare()
        self.change_job(candidate_id="CAND-NEW")
        self.assert_not_projected()

    def test_stale_contract_decision_is_not_projected(self):
        self.prepare()
        self.change_job(contract_revision=2)
        self.assert_not_projected()

    def test_resolved_decision_is_not_projected(self):
        self.prepare()
        decision = self.store.repository.open_decisions(self.job_id)[0]
        self.store.repository.resolve_decision(decision["decision_id"], resolution="HUMAN_REJECT")
        self.assert_not_projected()

    def test_current_qa_projects_once_with_current_authority(self):
        self.prepare()
        before = self.store.repository.path.read_bytes()
        selected = self.service.list_actionable_qa_jobs()["jobs"]
        self.assertEqual([self.job_id], [job["job_id"] for job in selected])
        self.assertEqual(before, self.store.repository.path.read_bytes())
        remote = self.remote()
        self.assertTrue(remote._ensure_qa_threads({}))
        binding = self.bindings.active_for_job(self.job_id)
        self.assertEqual(self.candidate_id, binding["candidate_id"])
        self.assertEqual(1, binding["contract_revision"])
        self.assertEqual(self.store.queue_snapshot()["generation"], binding["queue_generation"])
        self.assertFalse(remote._ensure_qa_threads({}))
        self.assertEqual(1, len(self.transport.threads))
        self.assertEqual(before, self.store.repository.path.read_bytes())


class ControlReadLivenessTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_read(self, script):
        import sys
        from runtime_safety import run_control_read, git_subprocess_env
        return run_control_read([sys.executable, "-c", script], cwd=self.root,
                                env=git_subprocess_env())

    def test_success_and_stderr_remain_separate(self):
        from runtime_safety import control_read_budget
        with control_read_budget():
            result = self.run_read("import sys; print('ok'); print('warning',file=sys.stderr)")
        self.assertEqual(0, result.returncode)
        self.assertEqual("ok", result.stdout.strip())
        self.assertEqual("warning", result.stderr.strip())

    def test_git_success_error_and_noncontrol_path(self):
        import subprocess
        from cumulative_policy import CumulativePolicyManager
        from runtime_safety import control_read_budget
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, timeout=15)
        manager = object.__new__(CumulativePolicyManager)
        manager.workspace = self.root
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        with control_read_budget():
            success = manager._run_git(self.root, ["rev-parse", "--is-inside-work-tree"])
            failure = manager._run_git(self.root, ["rev-parse", "--verify", "missing-ref"])
        self.assertEqual(0, success[0])
        self.assertNotEqual(0, failure[0])
        self.assertIn("fatal", failure[2])
        self.assertEqual(before, {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob('*') if p.is_file()})
        with mock.patch('cumulative_policy.subprocess.run', return_value=subprocess.CompletedProcess([], 0, 'ok', '')) as run:
            manager._run_git(self.root, ['rev-parse', '--verify', 'HEAD'])
        self.assertNotIn('timeout', run.call_args.kwargs)

    def test_timeout_reaps_owned_descendant_and_preserves_unrelated_process(self):
        import sys, subprocess, time
        from runtime_safety import ControlReadError, control_read_budget
        from hermes_process_boundary import identity
        sentinel = self.root / 'product-sentinel.txt'
        sentinel.write_text('unchanged')
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            script = ("import subprocess,sys,os,time,json; "
                      "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
                      "open('owned.json','w').write(json.dumps([os.getpid(),child.pid])); time.sleep(30)")
            started = time.monotonic()
            with mock.patch('runtime_safety.CONTROL_READ_GIT_TIMEOUT', .7), control_read_budget():
                with self.assertRaises(ControlReadError) as raised:
                    self.run_read(script)
            self.assertEqual('CONTROL_READ_TIMEOUT', raised.exception.code)
            self.assertLess(time.monotonic() - started, 5)
            for pid in json.loads((self.root/'owned.json').read_text()):
                self.assertFalse(identity(pid).get('alive', False))
            self.assertIsNone(unrelated.poll())
            self.assertEqual('unchanged', sentinel.read_text())
        finally:
            unrelated.terminate()
            unrelated.wait(timeout=5)

    def test_overall_budget_caps_next_call_and_recovers(self):
        from runtime_safety import ControlReadError, control_read_budget
        with mock.patch('runtime_safety.CONTROL_READ_BUDGET', .5), mock.patch('runtime_safety.CONTROL_READ_GIT_TIMEOUT', 5):
            with self.assertRaises(ControlReadError) as raised:
                with control_read_budget():
                    self.run_read('import time;time.sleep(.3)')
                    with control_read_budget():
                        self.run_read('import time;time.sleep(.3)')
            self.assertEqual('CONTROL_READ_TIMEOUT', raised.exception.code)
        with control_read_budget():
            self.assertEqual(0, self.run_read("print('next request')").returncode)

    def test_request_exit_checks_budget_and_maps_typed_error(self):
        import time
        from harness_service import HarnessServiceError
        service = object.__new__(ChatGPTControlService)
        service.supervisor = SimpleNamespace(status=lambda: {})
        with mock.patch('runtime_safety.CONTROL_READ_BUDGET', .01), \
                mock.patch.object(service, '_queue_snapshot_with_budget', side_effect=lambda: (time.sleep(.03) or {})):
            with self.assertRaises(HarnessServiceError) as raised:
                service.get_queue()
        self.assertEqual('CONTROL_READ_TIMEOUT', raised.exception.code)
        with mock.patch.object(service, '_queue_snapshot_with_budget', return_value={}):
            self.assertEqual({}, service.get_queue()['queue'])

    def test_launch_error_is_not_timeout(self):
        from runtime_safety import run_control_read, ControlReadError
        with self.assertRaises(ControlReadError) as raised:
            run_control_read([str(self.root/'missing.exe')], cwd=self.root, env={})
        self.assertEqual('CONTROL_READ_EXECUTION_ERROR', raised.exception.code)

    def test_cumulative_timeout_is_typed_and_next_read_works(self):
        import sys
        from cumulative_policy import CumulativePolicyManager, CumulativePolicyError
        from runtime_safety import control_read_budget
        manager = object.__new__(CumulativePolicyManager)
        manager.workspace = self.root
        with mock.patch('cumulative_policy.trusted_executable', return_value=sys.executable):
            with mock.patch('runtime_safety.CONTROL_READ_GIT_TIMEOUT', .2), control_read_budget():
                with self.assertRaises(CumulativePolicyError) as raised:
                    manager._run_git(self.root, ['-c', 'import time;time.sleep(30)'])
            self.assertEqual('CONTROL_READ_TIMEOUT', raised.exception.code)
            with control_read_budget():
                self.assertEqual(0, manager._run_git(self.root, ['-c', "print('recovered')"])[0])

    def test_owned_process_legacy_combined_output_is_preserved(self):
        import sys, tempfile
        from hermes_process_boundary import OwnedProcess
        from runtime_safety import git_subprocess_env
        with tempfile.TemporaryFile() as log:
            process = OwnedProcess(sys.executable, [sys.executable, '-c',
                "import sys;print('stdout');print('stderr',file=sys.stderr)"],
                git_subprocess_env(), str(self.root), log)
            try:
                self.assertEqual(0, process.wait(timeout=5))
                process.terminate_and_wait(timeout=5)
                self.assertEqual([], process.members())
                log.seek(0)
                value = log.read()
                self.assertIn(b'stdout', value)
                self.assertIn(b'stderr', value)
            finally:
                process.release()

    def test_discord_timeout_reply_then_subsequent_help(self):
        from harness_service import HarnessServiceError
        control = FakeControl()
        transport = FakeTransport()
        config = DiscordConfig('fixture', {USER_ID}, GUILD_ID, CHANNEL_ID, False, self.root/'unused')
        remote = DiscordControlRemote(control, config, transport=transport, control_root=self.root)
        with mock.patch.object(control, 'get_queue', side_effect=HarnessServiceError('CONTROL_READ_TIMEOUT')):
            result = remote.handle(message(1, '!h status'))
        self.assertEqual('CONTROL_READ_TIMEOUT', result['code'])
        self.assertEqual('CONTROL_READ_TIMEOUT', transport.replies()[-1])
        self.assertEqual('OK', remote.handle(message(2, '!h help'))['outcome'])
        self.assertFalse(config.commands_enabled)


class DiscordOutboundNotificationRegressionTests(unittest.TestCase):
    """Regression for the missing-operator-alerts defect.

    Covers: user_action_required immediacy, per-channel semantic dedup,
    terminal/QUARANTINED delivery, failure isolation from Job outcome and
    retry budget, delivery recovery, and commands_enabled independence.
    """

    class FakeRepo:
        def __init__(self):
            self.recorded = []

        def global_stop(self):
            return {"active": False}

        def notification(self, channel, subject):
            hits = [r for r in self.recorded if r[0] == channel and r[1] == subject]
            return {"fingerprint": hits[-1][2]} if hits else None

        def record_notification(self, channel, subject, fingerprint, projection):
            self.recorded.append((channel, subject, fingerprint))

    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    @staticmethod
    def projection(status="AWAITING_QA", job_id="JOB-R1"):
        return terminal_projection(
            {"job_id": job_id, "status": status, "revision": 1, "sequence": 1,
             "last_result": {"user_qa_required": status == "AWAITING_QA"}},
            {"batches": {}, "gate_reason": ""})

    def test_user_action_required_flip_emits_immediately(self):
        from progress_projection import should_emit
        now = datetime.now(timezone.utc)
        base = {"progress_revision": 0, "user_action_required": False,
                "job": {"id": "J", "status": "RUNNING"},
                "execution_health": {"runtime_health": "VALID", "progress_health": "RECENT",
                                      "failure_code": "", "reconciliation_required": False}}
        emit, reason = should_emit(base, {**base, "user_action_required": True},
                                   now=now, last_emitted_at=now)
        self.assertTrue(emit)
        self.assertEqual("USER_ACTION_REQUIRED_CHANGED", reason)
        emit, reason = should_emit({**base, "user_action_required": True},
                                   {**base, "user_action_required": True},
                                   now=now, last_emitted_at=now)
        self.assertFalse(emit)  # identical UAQR state must not spam

    def test_terminal_emits_once_per_channel_with_independent_dedup(self):
        from operator_notifications import deliver_semantic
        repo = self.FakeRepo()
        sent = []
        projection = self.projection()
        sender = lambda text: sent.append(text) or {"delivered": True}
        first = deliver_semantic(projection, repo, sender, channel="discord")
        duplicate = deliver_semantic(projection, repo, sender, channel="discord")
        telegram = deliver_semantic(projection, repo, sender, channel="telegram")
        self.assertTrue(first["delivered"])
        self.assertTrue(duplicate["duplicate_suppressed"])
        self.assertEqual(1, len([r for r in repo.recorded if r[0] == "discord"]))
        self.assertTrue(telegram["delivered"])  # separate channel budget
        self.assertEqual(["discord", "telegram"], [r[0] for r in repo.recorded])

    def test_terminal_and_quarantined_statuses_reach_both_channels(self):
        statuses = ["AWAITING_QA", "BLOCKED", "FAILED_FINAL", "SUCCEEDED", "QUARANTINED",
                    "JOB_SUCCEEDED_PLACEHOLDER"]
        statuses = [s for s in statuses if s != "JOB_SUCCEEDED_PLACEHOLDER"]
        for status in statuses:
            repo = self.FakeRepo()
            sends = []
            store = SimpleNamespace(repository=repo)
            supervisor = SimpleNamespace(store=store)
            with mock.patch("queue_supervisor.send_notify_evidence",
                            side_effect=lambda text, **kw: sends.append(("telegram", text))
                            or {"delivered": True}):
                with mock.patch("discord_remote.discord_notify_evidence",
                                side_effect=lambda text, **kw: sends.append(("discord", text))
                                or {"delivered": True}):
                    with mock.patch("discord_remote.discord_outbound_configured",
                                    return_value=True):
                        result = QueueSupervisor.notify_terminal_transition(
                            supervisor, {"job_id": "JOB-R1", "status": status,
                                         "revision": 1,
                                         "last_result": {"user_qa_required":
                                                         status == "AWAITING_QA"}},
                            {"batches": {}, "gate_reason": ""})
            self.assertIsNotNone(result, status)
            self.assertTrue(result["delivered"], status)
            self.assertTrue(result["discord"]["delivered"], status)
            self.assertEqual([c for c, _ in sends], ["telegram", "discord"], status)

    def test_discord_delivery_failure_never_alters_job_outcome_or_channel(self):
        repo = self.FakeRepo()
        telegram = []
        store = SimpleNamespace(repository=repo)
        supervisor = SimpleNamespace(store=store)
        job = {"job_id": "JOB-R1", "status": "FAILED_FINAL", "revision": 1, "last_result": {}}
        frozen = json.dumps(job, sort_keys=True)
        with mock.patch("queue_supervisor.send_notify_evidence",
                        side_effect=lambda text, **kw: telegram.append(text)
                        or {"delivered": True}):
            with mock.patch("discord_remote.discord_notify_evidence",
                            return_value={"delivered": False, "delivery_status": "FAILED",
                                          "failure_code": "DISCORD_HTTPERROR",
                                          "failure_origin": "NOTIFICATION"}):
                with mock.patch("discord_remote.discord_outbound_configured",
                                return_value=True):
                    result = QueueSupervisor.notify_terminal_transition(
                        supervisor, job, {"batches": {}, "gate_reason": ""})
        self.assertTrue(result["delivered"])  # Telegram result stays authoritative
        self.assertFalse(result["discord"]["delivered"])
        self.assertEqual("NOTIFICATION", result["discord"]["failure_origin"])
        self.assertEqual(frozen, json.dumps(job, sort_keys=True))  # no Job mutation

    def test_notification_failure_is_isolated_from_retry_budget_and_recovers(self):
        import tempfile
        task = SimpleNamespace(job_id="JOB-R1", status="RUNNING", retry_count=0,
                               notification_evidence=[])
        snapshot = {"progress_revision": 3, "user_action_required": False,
                    "job": {"id": "JOB-R1", "status": "RUNNING", "task_id": "T1"},
                    "stage": {"name": "BUILD"}, "gates": {}, "execution_health": {}}
        repo = self.FakeRepo()
        calls = {"count": 0}
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            (workspace / ".control").mkdir()
            (workspace / ".control" / "control.sqlite3").write_bytes(b"")  # is_file() == True
            manager_instance = SimpleNamespace(task_root=workspace / ".tasks")

            def discord_sender(text, **kw):
                if calls["count"] == 0:
                    calls["count"] += 1
                    return {"delivered": False, "failure_origin": "NOTIFICATION",
                            "failure_code": "DISCORD_HTTPERROR"}
                calls["count"] += 1
                return {"delivered": True}

            with mock.patch("control_repository.ControlRepository", return_value=repo):
                with mock.patch("manager.send_notify_evidence",
                                return_value={"delivered": True}):
                    with mock.patch("discord_remote.discord_notify_evidence",
                                    side_effect=discord_sender):
                        with mock.patch("discord_remote.discord_outbound_configured",
                                        return_value=True):
                            first = Manager._notify_progress(
                                manager_instance, task, snapshot, heartbeat=False)
                            second = Manager._notify_progress(
                                manager_instance, task, snapshot, heartbeat=False)
        self.assertTrue(first["delivered"])  # telegram outcome unaffected
        self.assertFalse(first["discord"]["delivered"])
        self.assertTrue(second["discord"]["delivered"])  # later send still possible
        self.assertEqual(0, task.retry_count)  # no Worker/Product retry consumed
        self.assertEqual("NOTIFICATION", first["discord"]["failure_origin"])

    def test_commands_enabled_false_does_not_disable_outbound_notifications(self):
        import discord_remote
        discord_remote._shared_outbound = None
        config = DiscordConfig("fixture-token", {USER_ID}, GUILD_ID, CHANNEL_ID,
                               False, self.root / "unused")
        self.assertFalse(config.commands_enabled)
        delivered = []
        with mock.patch.dict(os.environ, {"KKM_DISCORD_ENABLED": "1"}):
            with mock.patch.object(discord_remote.DiscordConfig, "load",
                                    return_value=config):
                with mock.patch.object(discord_remote.DiscordTransport, "send_message",
                                       side_effect=lambda channel_id, text:
                                       delivered.append((channel_id, text))
                                       or {"delivered": True, "delivery_status": "SUCCESS",
                                           "message_id": "m1", "failure_code": ""}):
                    evidence = discord_remote.discord_notify_evidence("outbound body",
                                                                      title="Harness progress")
                    status = discord_remote.discord_outbound_configured()
        discord_remote._shared_outbound = None
        self.assertTrue(evidence["delivered"])
        self.assertEqual("discord", evidence["channel"])
        self.assertTrue(status)
        self.assertEqual([(CHANNEL_ID, "Harness progress\noutbound body")], delivered)

    def test_env_flag_and_missing_configuration_disable_outbound_only(self):
        import discord_remote
        discord_remote._shared_outbound = None
        empty = DiscordConfig("", set(), None, None, False, self.root / "absent")
        with mock.patch.dict(os.environ, {"KKM_DISCORD_ENABLED": "1"}):
            with mock.patch.object(discord_remote.DiscordConfig, "load", return_value=empty):
                evidence = discord_remote.discord_notify_evidence("x")
                self.assertFalse(evidence["delivered"])
                self.assertEqual("NOTIFICATION_NOT_CONFIGURED", evidence["failure_code"])
                self.assertFalse(evidence["attempted"])
                self.assertFalse(discord_remote.discord_outbound_configured())
        discord_remote._shared_outbound = None
        with mock.patch.dict(os.environ, {"KKM_DISCORD_ENABLED": "0"}):
            evidence = discord_remote.discord_notify_evidence("x")
            self.assertFalse(evidence["delivered"])
            self.assertEqual("NOTIFICATION_DISABLED", evidence["failure_code"])

    def test_state_changing_commands_stay_disabled_by_default(self):
        env = self.root / "discord-env"
        env.write_text("DISCORD_BOT_TOKEN=tok\n"
                       "DISCORD_ALLOWED_USER_IDS=7001\n"
                       "DISCORD_ALLOWED_CHANNEL_ID=222\n", encoding="utf-8")
        config = DiscordConfig.load(env)
        self.assertFalse(config.commands_enabled)
        self.assertEqual("NOTIFICATION_ONLY", config.state)
        transport = FakeTransport()
        remote = DiscordControlRemote(FakeControl(), config, transport=transport,
                                      control_root=self.root)
        outcome = remote.handle(message(1, "!h approve"))
        self.assertEqual("COMMANDS_DISABLED", outcome["outcome"])


if __name__ == "__main__":
    unittest.main()
