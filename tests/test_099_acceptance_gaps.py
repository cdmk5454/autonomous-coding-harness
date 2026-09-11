"""0.99.1 release-acceptance gap regressions.

Covers the remaining contract gaps found by the 0.99.1 verification bundle:

- P0-1 persistent OpenCode old-writer quiescence (abort on technical failure,
  bounded confirmation, replacement denial when unconfirmed, stale late
  response never published).
- P0-2 normal Job snapshot path performs no historical successful-Job scan.
- P1-1 explicitly approved LOW deterministic review oracle may skip the
  separate LLM Reviewer with a bound REVIEW receipt.
- P1-2 meaningful progress is turn-correlated semantic state, not metadata.
- P1-3 MANAGED harness-owned local server uses an ephemeral credential.
- P1-4 external baseline adoption declares its BASELINE_ADOPTION_ONLY
  validation boundary.
"""

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import harness_temp
from control_repository import ControlRepository
from harness_service import HarnessService, HarnessServiceError
from job_queue import EXTERNAL_BASELINE_ADOPTION_CONFIRMATION, QueueError
from manager import Manager
from opencode_runtime_adapter import MANAGED, PERSONAL, OpenCodeAdapterError, OpenCodeRuntimeAdapter
from review_policy import assess_review_risk
from runtime_adapter import CommandDelivery, RuntimeDescriptor, SessionQuery, payload_sha256
from task_state import TaskState
from test_099_external_baseline_adoption import ExternalBaselineAdoptionTests
from test_plan import validate_test_contract
from worker import OpenCodeWorker, parse_qa_request
from reviewer import REVIEW_PASS


QA_PREFIX_MARKER = "HARNESS_QA_REQUEST_JSON: "


def assistant(message_id, parent, *, finish="tool-calls", parts=None, **info_extra):
    return {
        "info": {
            "role": "assistant", "id": message_id,
            "parentID": parent, "finish": finish, **info_extra,
        },
        "parts": parts or [],
    }


class PersistentOpenCodeQuiescenceTests(unittest.TestCase):
    """P0-1: the actual native writer handoff must close before replacement."""

    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = ControlRepository(self.root / "control.sqlite3", create=True)
        job = {
            "job_id": "JOB-1", "logical_job_id": "LOGICAL-1",
            "client_job_id": "CLIENT-1", "status": "QUEUED", "revision": 0,
            "contract_revision": 1, "owner_id": "", "request": {}, "depends_on": [],
        }
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-1"]},
            {"JOB-1": job}, {"JOB-1": {"path": "jobs/JOB-1.json"}},
        )
        self.execution = self.repo.materialize(
            job,
            context={"profile_snapshot_sha256": "p", "policy_snapshot_sha256": "p"},
            policy={"effective_policy_sha256": "p", "resolved_worker": {}},
            baseline_hash="b", execution_surface_hash="s",
        )
        self.attempt = self.repo.start_attempt(
            self.execution, attempt_id="A-1", role="WORKER", model="m",
        )

    def task(self):
        task = TaskState(
            task_id="TASK-1", requirement="fixture", worker="opencode",
            opencode_model="provider/model", target_module=["sample-service"],
        )
        task.control_repository_path = str(self.repo.path)
        task.execution_id = self.execution["execution_id"]
        task.attempt_id = self.attempt["attempt_id"]
        return task

    def run_worker(self, adapter_cls, task):
        with patch.dict(os.environ, {
            "KKM_OPENCODE_LIVE_VALIDATED": "1",
            "KKM_OPENCODE_SERVER_URL": "http://127.0.0.1:4096",
            "OPENCODE_SERVER_PASSWORD": "fixture-password",
        }), patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", adapter_cls), patch(
            "worker.build_prompt", return_value="fixture prompt",
        ):
            return OpenCodeWorker(timeout=1, model="provider/model").execute(
                "fixture", str(self.root), task,
            )

    @staticmethod
    def base_adapter():
        class Adapter:
            timeout = 0.05
            interrupt_calls = 0
            writes = 0
            submitted_message_id = ""

            def __init__(self, **_kwargs):
                self.base_url = "http://127.0.0.1:4096"

            def preflight(self):
                return RuntimeDescriptor(
                    runtime="opencode", version="fixture",
                    adapter_revision="runtime-adapter/2", schema_compatible=True,
                    detail="CLI_AND_SERVER_SCHEMA_PREFLIGHT",
                )

            def start_local_server(self, **_kwargs):
                return self.base_url

            def stop_local_server(self):
                return None

            def create_session(self, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(
                    base_url=self.base_url, mode=PERSONAL,
                ).create_session(prompt, **kwargs)

            def submit_turn(self, session_id, prompt, **kwargs):
                return OpenCodeRuntimeAdapter(
                    base_url=self.base_url, mode=PERSONAL,
                ).submit_turn(session_id, prompt, **kwargs)

            def execute_invocation(self, invocation):
                if invocation.url.endswith("/session"):
                    return {"id": "session-quiesce-1"}
                type(self).writes += 1
                type(self).submitted_message_id = str(invocation.body["messageID"])
                return None

            def interrupt(self, session_id, turn_id=""):
                type(self).interrupt_calls += 1
                return True

            def query_session(self, session_id, **_kwargs):
                return SessionQuery(session_id, "IDLE")

            def request(self, method, path, body=None):
                if method != "GET" or body is not None:
                    raise AssertionError("unexpected native write")
                return []
        return Adapter

    def test_persistent_timeout_aborts_native_session(self):
        adapter_cls = self.base_adapter()
        task = self.task()
        result = self.run_worker(adapter_cls, task)
        self.assertFalse(result.success)
        self.assertEqual("OPENCODE_PROGRESS_TIMEOUT", result.execution_failure_code)
        # The native session abort was delivered before returning the failure.
        self.assertEqual(1, adapter_cls.interrupt_calls)
        command = self.repo.command(task.command_id)
        self.assertEqual("ACKNOWLEDGED", command["delivery_status"])
        self.assertEqual("OPENCODE_PROGRESS_TIMEOUT", command["aborted"]["reason"])
        recovery = task.runtime_recovery_history[-1]
        quiescence = recovery["evidence"]["writer_quiescence"]
        self.assertTrue(quiescence["abort_delivered"])
        self.assertEqual("IDLE", quiescence["confirmed_state"])
        self.assertTrue(quiescence["quiesced"])
        self.assertEqual(
            "IDLE",
            self.repo.session_binding(self.execution["execution_id"], "opencode")["state"],
        )

    def test_abort_unknown_blocks_replacement_writer(self):
        adapter_cls = self.base_adapter()

        def unknown_interrupt(_session_id, _turn_id=""):
            adapter_cls.interrupt_calls += 1
            raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR", "TimeoutError")

        def unknown_query(_session_id, **_kwargs):
            return SessionQuery("session-quiesce-1", "UNKNOWN", detail="transport")

        adapter_cls.interrupt = staticmethod(unknown_interrupt)
        adapter_cls.query_session = staticmethod(unknown_query)

        task = self.task()
        first = self.run_worker(adapter_cls, task)
        self.assertFalse(first.success)
        self.assertEqual(1, adapter_cls.interrupt_calls)
        recovery = task.runtime_recovery_history[-1]
        self.assertFalse(recovery["evidence"]["writer_quiescence"]["quiesced"])
        self.assertEqual(0, recovery["product_retry_delta"])
        self.assertEqual("RUNTIME_RECOVERY", task.retry_domain)

        second_task = self.task()
        second = self.run_worker(adapter_cls, second_task)
        self.assertFalse(second.success)
        self.assertEqual(
            "OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED", second.execution_failure_code,
        )
        self.assertEqual("TECHNICAL_EXECUTION_RETRY", second_task.retry_domain)
        # No new canonical writer turn was submitted and the failed turn was
        # never resent: the candidate/delta stays preserved.
        self.assertEqual(1, adapter_cls.writes)
        self.assertEqual(1, adapter_cls.interrupt_calls)
        self.assertEqual(
            "UNKNOWN",
            self.repo.session_binding(self.execution["execution_id"], "opencode")["state"],
        )

    def test_late_aborted_response_is_not_published(self):
        adapter_cls = self.base_adapter()
        old_native_id = "msg_" + hashlib.sha256(b"old-command").hexdigest()

        def messages(_method, path, body=None):
            messages_list = [assistant(
                "msg_late_stale", old_native_id, finish="stop",
                parts=[{"type": "text", "text": "stale writer output"}],
            )]
            current = adapter_cls.submitted_message_id
            if current and current != old_native_id:
                messages_list.append(assistant(
                    "msg_new_final", current, finish="stop",
                    parts=[{"type": "text", "text": "fresh continuation output"}],
                ))
            return messages_list

        adapter_cls.request = staticmethod(messages)

        # Pre-existing ACKNOWLEDGED command whose turn was aborted: its late
        # response must never publish as the current result.
        binding = self.repo.bind_native_session(
            self.execution["execution_id"], runtime="opencode",
            native_session_id="session-quiesce-1", adapter_revision="a",
            runtime_version="v", durable=True, state="IDLE",
        )
        command = self.repo.prepare_command(
            self.execution["execution_id"], self.attempt["attempt_id"],
            idempotency_key="original", payload_sha256=payload_sha256({"prompt": "x"}),
            binding_id=binding["binding_id"],
        )
        self.repo.set_command_delivery(command["command_id"], CommandDelivery.SENT.value)
        self.repo.set_command_delivery(
            command["command_id"], CommandDelivery.ACKNOWLEDGED.value,
            native_command_id=old_native_id,
        )
        self.repo.mark_command_aborted(command["command_id"], reason="OPENCODE_PROGRESS_TIMEOUT")

        task = self.task()
        result = self.run_worker(adapter_cls, task)
        self.assertTrue(result.success)
        self.assertEqual("fresh continuation output", result.stdout)
        self.assertNotIn("stale writer output", result.stdout)
        # The aborted command was never completed and a fresh continuation
        # turn was submitted instead of replaying the stale response.
        self.assertEqual(
            "ACKNOWLEDGED", self.repo.command(command["command_id"])["delivery_status"],
        )
        self.assertEqual(1, adapter_cls.writes)
        self.assertNotEqual(task.command_id, command["command_id"])
        self.assertEqual(
            "COMPLETED", self.repo.command(task.command_id)["delivery_status"],
        )

    def test_confirmed_late_recheck_allows_replacement(self):
        adapter_cls = self.base_adapter()

        def failing_interrupt(_session_id, _turn_id=""):
            adapter_cls.interrupt_calls += 1
            raise OpenCodeAdapterError("OPENCODE_HTTP_ERROR", "503")

        adapter_cls.interrupt = staticmethod(failing_interrupt)

        task = self.task()
        first = self.run_worker(adapter_cls, task)
        self.assertFalse(first.success)
        quiescence = task.runtime_recovery_history[-1]["evidence"]["writer_quiescence"]
        # Abort failed but the bounded query still confirmed IDLE: the session
        # demonstrably has no active turn, so replacement stays bounded-allowed.
        self.assertFalse(quiescence["abort_delivered"])
        self.assertTrue(quiescence["quiesced"])

        adapter_cls.interrupt_calls = 0
        second_task = self.task()
        # The persisted task already advanced its runtime recovery counter.
        second_task.runtime_recovery_count = 1
        second = self.run_worker(adapter_cls, second_task)
        self.assertFalse(second.success)
        self.assertNotEqual(
            "OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED", second.execution_failure_code,
        )
        self.assertEqual(2, adapter_cls.writes)


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


class MeaningfulProgressTests(unittest.TestCase):
    """P1-2: only turn-correlated semantic change resets the watchdog."""

    def worker(self, *, activity=3):
        from worker import TimeoutConfig
        return OpenCodeWorker(
            timeout=300, model="provider/model",
            timeout_config=TimeoutConfig(
                analysis_max=activity, modification_max=activity,
                inactivity=activity, hard_limit=1, runtime_heartbeat=15,
                runtime_lease=45,
            ),
        )

    def run_wait(self, polls, *, activity=3):
        clock = Clock()
        adapter = SimpleNamespace(
            base_url="http://127.0.0.1:1",
            timeout=0.05,
        )
        index = {"i": -1}

        def query_session(_session_id, **_kwargs):
            index["i"] += 1
            if index["i"] >= len(polls):
                # Bounded fixture: never spin forever even if a regression
                # wrongly resets the watchdog.
                raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR", "fixture exhausted")
            poll = polls[index["i"]]
            clock.now = poll[0]
            return SessionQuery("session", poll[1])

        def request(_method, _path, body=None):
            return polls[min(index["i"], len(polls) - 1)][2]

        adapter.query_session = query_session
        adapter.request = request
        with patch("worker.time.monotonic", clock.monotonic), patch(
            "worker.time.sleep", lambda _seconds: None
        ):
            return self.worker(activity=activity)._wait_for_response(
                adapter, "session", "msg_original", Path("."),
            )

    def test_metadata_only_change_is_not_meaningful_progress(self):
        first = [assistant("msg_1", "msg_original", tokens=10, time="t1")]
        mutated = [assistant("msg_1", "msg_original", tokens=999, time="t2")]
        query = SessionQuery("session", "RUNNING")
        fingerprint = OpenCodeWorker._activity_fingerprint
        self.assertEqual(
            fingerprint(query, first, "msg_original"),
            fingerprint(query, mutated, "msg_original"),
        )
        # Behavioral: a metadata-only mutation must not reset the watchdog,
        # so the turn still times out with no semantic progress.
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "RUNNING", first),
                (2, "RUNNING", mutated),
                (4, "RUNNING", mutated),
            ], activity=3)

    def test_unrelated_message_change_is_not_meaningful_progress(self):
        query = SessionQuery("session", "RUNNING")
        fingerprint = OpenCodeWorker._activity_fingerprint
        with_unrelated = [
            assistant("msg_1", "msg_original"),
            assistant("msg_other", "msg_unrelated_turn", finish="stop",
                      parts=[{"type": "text", "text": "other session turn"}]),
        ]
        self.assertEqual(
            fingerprint(query, [assistant("msg_1", "msg_original")], "msg_original"),
            fingerprint(query, with_unrelated, "msg_original"),
        )

    def test_tool_state_transition_is_progress(self):
        running = [assistant("msg_1", "msg_original", parts=[
            {"type": "tool", "id": "tool_1", "state": "running", "time": {"ms": 1}},
        ])]
        completed = [assistant("msg_1", "msg_original", parts=[
            {"type": "tool", "id": "tool_1", "state": "completed", "time": {"ms": 2}},
        ])]
        query = SessionQuery("session", "RUNNING")
        fingerprint = OpenCodeWorker._activity_fingerprint
        self.assertNotEqual(
            fingerprint(query, running, "msg_original"),
            fingerprint(query, completed, "msg_original"),
        )
        # Behavioral: the running->completed transition resets the watchdog
        # so the turn completes instead of timing out.
        result = self.run_wait([
            (0, "RUNNING", running),
            (2.5, "RUNNING", completed),
            (4, "IDLE", completed + [assistant(
                "msg_final", "msg_original", finish="stop",
                parts=[{"type": "text", "text": "done"}],
            )]),
        ], activity=3)
        self.assertEqual("done", result[1])

    def test_correlated_assistant_content_delta_is_progress(self):
        query = SessionQuery("session", "RUNNING")
        fingerprint = OpenCodeWorker._activity_fingerprint
        before = [assistant("msg_1", "msg_original",
                            parts=[{"type": "text", "text": "partial"}])]
        after = [assistant("msg_1", "msg_original",
                           parts=[{"type": "text", "text": "partial complete"}])]
        self.assertNotEqual(
            fingerprint(query, before, "msg_original"),
            fingerprint(query, after, "msg_original"),
        )

    def test_session_state_and_new_correlated_message_are_progress(self):
        query_active = SessionQuery("session", "ACTIVE")
        query_idle = SessionQuery("session", "IDLE")
        fingerprint = OpenCodeWorker._activity_fingerprint
        messages = [assistant("msg_1", "msg_original")]
        self.assertNotEqual(
            fingerprint(query_active, messages, "msg_original"),
            fingerprint(query_idle, messages, "msg_original"),
        )
        grown = messages + [assistant("msg_2", "msg_original")]
        self.assertNotEqual(
            fingerprint(query_active, messages, "msg_original"),
            fingerprint(query_active, grown, "msg_original"),
        )


class NormalSnapshotNoHistoricalScanTests(unittest.TestCase):
    """P0-2: normal Job admission performs no historical successful-Job scan."""

    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        (self.workspace / "sample-service").mkdir(parents=True)
        target = self.workspace / "sample-service" / "file.xml"
        target.write_text("candidate", encoding="utf-8")
        self.digest = hashlib.sha256(target.read_bytes()).hexdigest()
        self.repo = ControlRepository(self.root / ".control" / "control.sqlite3", create=True)
        job = {
            "job_id": "JOB-OLD", "logical_job_id": "LOGICAL-1",
            "client_job_id": "CLIENT-1", "status": "AWAITING_QA", "revision": 1,
            "contract_revision": 1, "owner_id": "", "request": {}, "depends_on": [],
        }
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-OLD"]},
            {"JOB-OLD": job}, {"JOB-OLD": {"path": "jobs/JOB-OLD.json"}},
        )
        self.repo.preserve_candidate_ownership(
            "JOB-OLD", {"sample-service/file.xml": self.digest},
            workspace=self.workspace, evidence_ref="fixture",
        )

    def test_current_only_mode_skips_historical_verification(self):
        with patch.object(
            ControlRepository, "_verified_candidate_history",
            side_effect=AssertionError("historical scan in current-only mode"),
        ):
            ownership = self.repo.candidate_ownership(self.workspace, current_only=True)
        self.assertTrue(ownership["valid"])
        self.assertEqual(
            ["sample-service/file.xml"], [item["path"] for item in ownership["files"]],
        )
        # The default mode still exposes the forensic path.
        with patch.object(
            ControlRepository, "_verified_candidate_history", return_value={},
        ) as forensic:
            self.repo.candidate_ownership(self.workspace)
        forensic.assert_called_once()

    def test_current_only_mode_integrity_and_baseline_acceptance(self):
        ownership = self.repo.candidate_ownership(self.workspace, current_only=True)
        self.assertTrue(ownership["valid"])
        # Accepted current baseline input reclassifies the row as history.
        accepted = self.repo.candidate_ownership(
            self.workspace, accepted_baseline_hashes={"sample-service/file.xml": self.digest},
            current_only=True,
        )
        self.assertTrue(accepted["valid"])
        self.assertEqual([], accepted["files"])
        self.assertEqual(
            ["sample-service/file.xml"],
            [item["path"] for item in accepted["accepted_baseline_files"]],
        )
        # Exact current hash integrity is still enforced for active rows.
        (self.workspace / "sample-service" / "file.xml").write_text("tampered", encoding="utf-8")
        drifted = self.repo.candidate_ownership(self.workspace, current_only=True)
        self.assertFalse(drifted["valid"])
        self.assertEqual(["sample-service/file.xml"], drifted["invalid_files"])

    def test_normal_snapshot_preparation_does_no_historical_scan(self):
        service = object.__new__(HarnessService)
        service.working_dir = self.workspace
        service.control_root = self.root / ".control"
        service.task_root = self.root / ".tasks"
        (self.root / ".control" / "sqlite-authority.json").write_text("{}", encoding="utf-8")
        service._cumulative_manager = lambda: SimpleNamespace(
            adopted_external_hashes=lambda _baseline_id, repository_paths: {},
            prepare_job_snapshot=lambda _job, _baseline, _inherited: {
                "snapshot_id": "SNAP-NEW",
            },
        )
        queued_job = {
            "job_id": "JOB-NEW", "request": {"target_modules": ["cap"]},
            "corrects_job_id": "",
        }
        queue_snapshot = {"active_baseline_id": "BASE-1", "batch_delta_files": []}
        with patch.object(
            ControlRepository, "_verified_candidate_history",
            side_effect=AssertionError("historical scan in normal snapshot path"),
        ):
            result = service.prepare_job_snapshot(queued_job, queue_snapshot)
        self.assertEqual("SNAP-NEW", result["snapshot_id"])

    def test_normal_snapshot_unresolved_candidate_conflict_still_blocks(self):
        service = object.__new__(HarnessService)
        service.working_dir = self.workspace
        service.control_root = self.root / ".control"
        service.task_root = self.root / ".tasks"
        (self.root / ".control" / "sqlite-authority.json").write_text("{}", encoding="utf-8")
        service._cumulative_manager = lambda: SimpleNamespace(
            adopted_external_hashes=lambda _baseline_id, repository_paths: {},
        )
        queued_job = {
            "job_id": "JOB-NEW", "request": {"target_modules": ["sample-service"]},
            "corrects_job_id": "",
        }
        queue_snapshot = {"active_baseline_id": "BASE-1", "batch_delta_files": []}
        with patch.object(
            ControlRepository, "_verified_candidate_history",
            side_effect=AssertionError("historical scan in normal snapshot path"),
        ):
            with self.assertRaisesRegex(
                HarnessServiceError, "QUARANTINED_CANDIDATE_SCOPE_CONFLICT",
            ):
                service.prepare_job_snapshot(queued_job, queue_snapshot)


class DeterministicOracleReviewTests(unittest.TestCase):
    """P1-1: an explicitly approved LOW oracle may skip the LLM Reviewer."""

    def manager(self):
        return object.__new__(Manager)

    def base_task(self, **overrides):
        task = SimpleNamespace(
            test_plan={
                "review_oracle": "DETERMINISTIC_ORACLE",
                "mandatory": True,
                "scope_hash": "scope-1",
                "required_tests": ["sample-service/ATest.java"],
            },
            review_policy={"risk_level": "LOW", "hard_flags": []},
            worker_stdout="",
            changed_files=["sample-service/A.java"],
            git_diff="diff --git a/sample-service/A.java b/sample-service/A.java",
            test_status="PASS",
            official_tester_invoked=True,
            official_tester_execution={
                "exit_code": 0, "status": "ENDED",
                "test_scope_hash": "scope-1",
                "execution_surface_hash": "surface-1",
            },
            materialized_execution={
                "execution_surface_hash": "surface-1", "contract_revision": 1,
            },
            build={"status": "PASS", "success": True},
            gate_evidence={
                "BUILD": {
                    "candidate_hash": "c1", "status": "PASS", "freshness": "CURRENT",
                },
                "TEST": {
                    "candidate_hash": "c1", "test_scope_hash": "scope-1",
                    "execution_surface_hash": "surface-1",
                    "status": "PASS", "freshness": "CURRENT",
                },
            },
        )
        for key, value in overrides.items():
            setattr(task, key, value)
        return task

    identity = {
        "candidate_hash": "c1", "test_scope_hash": "scope-1",
        "execution_surface_hash": "surface-1", "diff_hash": "d1",
        "acceptance_hash": "a1",
    }

    def test_low_explicit_oracle_pass_skips_reviewer(self):
        task = self.base_task()
        result = self.manager()._deterministic_oracle_review(task, dict(self.identity))
        self.assertIsNotNone(result)
        self.assertEqual(REVIEW_PASS, result.status)
        self.assertTrue(result.passed)
        coverage = dict(result.coverage)
        self.assertEqual("DETERMINISTIC_ORACLE", coverage["review_mode"])
        self.assertFalse(coverage["reviewer_required"])
        self.assertEqual(0, coverage["reviewer_invocations"])
        self.assertTrue(coverage["oracle_evidence_sha256"])
        self.assertEqual(
            "scope-1", coverage["oracle_evidence_ref"]["test_scope_hash"],
        )
        # The decision receipt is truthful as well.
        self.assertFalse(task.review_policy["reviewer_required"])
        self.assertEqual("DETERMINISTIC_ORACLE", task.review_policy["review_mode"])

    def test_low_without_explicit_oracle_still_reviews(self):
        plan = dict(self.base_task().test_plan)
        plan.pop("review_oracle")
        task = self.base_task(test_plan=plan)
        self.assertIsNone(self.manager()._deterministic_oracle_review(task, dict(self.identity)))

    def test_mid_and_high_risk_never_use_oracle_fast_skip(self):
        for level in ("MID", "HIGH", "CRITICAL"):
            policy = {"risk_level": level, "hard_flags": []}
            task = self.base_task(review_policy=policy)
            self.assertIsNone(
                self.manager()._deterministic_oracle_review(task, dict(self.identity)),
                level,
            )

    def test_hard_escalation_flag_blocks_oracle_skip(self):
        policy = {"risk_level": "LOW", "hard_flags": ["EXTERNAL_SIDE_EFFECT"]}
        task = self.base_task(review_policy=policy)
        self.assertIsNone(self.manager()._deterministic_oracle_review(task, dict(self.identity)))

    def test_stale_oracle_evidence_requires_review(self):
        stale_scope = self.base_task()
        stale_scope.official_tester_execution = {
            "exit_code": 0, "status": "ENDED",
            "test_scope_hash": "scope-stale",
            "execution_surface_hash": "surface-1",
        }
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(stale_scope, dict(self.identity)),
        )
        stale_surface = self.base_task()
        stale_surface.official_tester_execution = {
            "exit_code": 0, "status": "ENDED",
            "test_scope_hash": "scope-1",
            "execution_surface_hash": "surface-old",
        }
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(stale_surface, dict(self.identity)),
        )
        not_run = self.base_task()
        not_run.official_tester_execution = {
            "exit_code": None, "status": "RUNNING",
            "test_scope_hash": "scope-1",
            "execution_surface_hash": "surface-1",
        }
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(not_run, dict(self.identity)),
        )

    def test_pending_qa_hold_or_failing_gates_require_review(self):
        qa_hold = self.base_task(worker_stdout=QA_PREFIX_MARKER + json.dumps({
            "required": True, "qa_type": "TOOL_PERMISSION", "hold_scope": "JOB",
            "reason": "OpenCode permission pending",
            "evidence": ["tool permission request observed"],
            "decision_independent_work_complete": False,
        }))
        self.assertTrue(parse_qa_request(qa_hold.worker_stdout))
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(qa_hold, dict(self.identity)),
        )
        test_fail = self.base_task(test_status="ERROR")
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(test_fail, dict(self.identity)),
        )
        build_fail = self.base_task(build={"status": "FAIL", "success": False})
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(build_fail, dict(self.identity)),
        )
        stale_gate = self.base_task()
        stale_gate.gate_evidence = {
            "BUILD": {"candidate_hash": "c0", "status": "PASS", "freshness": "CURRENT"},
            "TEST": {
                "candidate_hash": "c1", "test_scope_hash": "scope-1",
                "execution_surface_hash": "surface-1",
                "status": "PASS", "freshness": "CURRENT",
            },
        }
        self.assertIsNone(
            self.manager()._deterministic_oracle_review(stale_gate, dict(self.identity)),
        )

    def test_oracle_requires_mandatory_frozen_test_contract(self):
        plan = dict(self.base_task().test_plan)
        plan["mandatory"] = False
        task = self.base_task(test_plan=plan)
        self.assertIsNone(self.manager()._deterministic_oracle_review(task, dict(self.identity)))

    def test_typed_oracle_contract_validation(self):
        parsed = validate_test_contract({
            "review_oracle": "DETERMINISTIC_ORACLE", "mandatory": True,
        })
        self.assertEqual("DETERMINISTIC_ORACLE", parsed["review_oracle"])
        with self.assertRaisesRegex(ValueError, "REVIEW_ORACLE_INVALID"):
            validate_test_contract({"review_oracle": "WORKER_SELF_REVIEW", "mandatory": True})
        with self.assertRaisesRegex(ValueError, "REVIEW_ORACLE_REQUIRES_MANDATORY_TESTS"):
            validate_test_contract({"review_oracle": "DETERMINISTIC_ORACLE"})
        with self.assertRaisesRegex(ValueError, "TEST_PLAN_INVALID"):
            validate_test_contract({"review_oracle": "DETERMINISTIC_ORACLE", "unknown": 1})

    def test_review_decision_defaults_still_require_reviewer(self):
        decision = assess_review_risk(
            requirement="간단한 오타 수정", target_modules=["sample-service"],
            changed_files=["sample-service/A.java"], git_diff="+fix", build_status="PASS",
            test_status="PASS", test_required=False,
        )
        policy = decision.to_dict()
        self.assertTrue(policy["reviewer_required"])
        self.assertEqual("LLM_REVIEW", policy["review_mode"])


class ManagedLocalEphemeralAuthTests(unittest.TestCase):
    """P1-3: harness-owned MANAGED local server auth contract."""

    def spawn_capture(self):
        captured = {}

        class FakePopen:
            def __init__(self, *_args, **kwargs):
                captured.update(kwargs)
                self.pid = 424242

            def poll(self):
                return 0

            @property
            def stdout(self):
                return None

            @property
            def stderr(self):
                return None

            def wait(self, timeout=None):
                return 0

        return captured, FakePopen

    def test_managed_local_without_password_generates_ephemeral_credential(self):
        captured, FakePopen = self.spawn_capture()
        adapter = OpenCodeRuntimeAdapter(mode=MANAGED, timeout=0.2)
        executable = Path(adapter.executable)
        with patch(
            "opencode_runtime_adapter.shutil.which", return_value=str(executable),
        ), patch("opencode_runtime_adapter.subprocess.Popen", FakePopen):
            with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_SERVER"):
                adapter.start_local_server(cwd=".")
        # The ephemeral per-instance credential reached the child environment
        # and the adapter's request headers; nothing was persisted or logged.
        self.assertTrue(captured["env"]["OPENCODE_SERVER_PASSWORD"])
        self.assertTrue(adapter.password)
        self.assertEqual(captured["env"]["OPENCODE_SERVER_PASSWORD"], adapter.password)
        self.assertEqual("opencode", captured["env"]["OPENCODE_SERVER_USERNAME"])
        # A second instance gets its own credential.
        other = OpenCodeRuntimeAdapter(mode=MANAGED, timeout=0.2)
        captured2, FakePopen2 = self.spawn_capture()
        with patch(
            "opencode_runtime_adapter.shutil.which", return_value=str(executable),
        ), patch("opencode_runtime_adapter.subprocess.Popen", FakePopen2):
            with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_SERVER"):
                other.start_local_server(cwd=".")
        self.assertNotEqual(
            captured["env"]["OPENCODE_SERVER_PASSWORD"],
            captured2["env"]["OPENCODE_SERVER_PASSWORD"],
        )

    def test_managed_remote_without_password_still_requires_auth(self):
        remote = OpenCodeRuntimeAdapter(
            base_url="https://host.example", mode=MANAGED,
        )
        with self.assertRaisesRegex(OpenCodeAdapterError, "MANAGED_REMOTE_AUTH_REQUIRED"):
            remote.start_local_server(cwd=".")
        with self.assertRaisesRegex(OpenCodeAdapterError, "MANAGED_REMOTE_AUTH_REQUIRED"):
            remote.request("GET", "/global/health")


class DirectAdoptionValidationBoundaryTests(ExternalBaselineAdoptionTests):
    """P1-4: adoption is integrity-only baseline adoption, never validation."""

    def test_adoption_records_baseline_only_boundary(self):
        result, replayed = self.invoke()
        self.assertFalse(replayed)
        adoption = self.store.queue_snapshot()["external_delta_adoption"]
        self.assertEqual("BASELINE_ADOPTION_ONLY", adoption["validation_boundary"])
        self.assertEqual("UNKNOWN", adoption["generation_provenance"])
        self.assertEqual("EXTERNAL", adoption["origin"])
        # The adoption result itself claims no verification/publication.
        self.assertNotIn("verification_status", result)
        self.assertNotIn("review_status", result)

    def test_adoption_without_explicit_boundary_is_rejected(self):
        fields = self.fields()
        del fields["external_delta_adoption"]["validation_boundary"]
        with self.assertRaisesRegex(QueueError, "EXTERNAL_BASELINE_INTEGRITY_INVALID"):
            self.invoke(fields=fields)

    def test_adoption_cannot_claim_validated_publication_boundary(self):
        fields = self.fields()
        fields["external_delta_adoption"]["validation_boundary"] = "VERIFIED_PUBLICATION"
        with self.assertRaisesRegex(QueueError, "EXTERNAL_BASELINE_INTEGRITY_INVALID"):
            self.invoke(fields=fields)


if __name__ == "__main__":
    unittest.main()
