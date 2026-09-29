"""0.99.2.2 Real-Work Stabilization Hotfix focused tests.

Covers the incident regressions of TASK-20260916-141759 (MID frontend console
Job, OpenCode worker):
- HOTFIX A: OpenCode terminal/progress reconciliation (false timeout on a
  natively completed turn; real stalls still fail; ownership/abort guards).
- HOTFIX B: execution-evidence datetime normalization + reporter containment.
- HOTFIX C: infrastructure Worker failure without a QA subject no longer
  projects AWAITING_QA (task FAILED, job BLOCKED with technical decision).
- HOTFIX D: preserved-delta finalization continuation without Worker rerun.
- HOTFIX E: MID frontend context diet (area affinity, menu index, strong
  tokens, TaskSpec requested_changes de-duplication).
- HOTFIX F: reviewer prompt observability metrics.
- HOTFIX G: console execution-contract provenance (reasoning effort, test
  plan) and OpenCode effort provenance.
"""

from __future__ import annotations

import hashlib
import json
import os
import harness_temp as tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from context_foundation import build_context_pack
from execution_evidence import _duration_seconds, _parse_iso, stage_timing
from git_collector import GitDiffResult
from job_queue import AWAITING_QA, BLOCKED, QUEUED, JobStore
from manager import Manager
from opencode_runtime_adapter import OpenCodeAdapterError
from project_profile import ModuleProfile, ProjectProfile
from reporter import Reporter
from retry_domains import failure_disposition
from reviewer import REVIEW_PASS, ReviewResult, Reviewer
from task_state import TaskState
from worker import OpenCodeWorker, TimeoutConfig

from test_job_control import request as control_request
from test_job_control import valid_enqueue


# ---------------------------------------------------------------------------
# HOTFIX A — OpenCode progress reconciliation
# ---------------------------------------------------------------------------


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
        if self.index >= len(self.polls):
            raise AssertionError(
                "polled past the provided timeline — completion did not happen "
                "when this test expected it to"
            )
        poll = self.polls[self.index]
        self.clock.now = poll[0]
        return SimpleNamespace(session_id=session_id, state=poll[1])

    def request(self, method, path, body=None):
        if method != "GET" or body is not None:
            raise AssertionError("unexpected write")
        return self.polls[self.index][2]


def assistant(message_id, parent, *, finish="tool-calls", parts=None, error=None):
    info = {
        "role": "assistant", "id": message_id,
        "parentID": parent, "finish": finish,
    }
    if error is not None:
        info["error"] = error
    return {"info": info, "parts": parts or []}


class OpenCodeTerminalReconciliationTests(unittest.TestCase):
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

    def run_wait(self, polls, *, activity=400, baseline=frozenset(), events=None, out=None):
        clock = Clock()
        adapter = PollingAdapter(clock, polls)
        task = None
        if events is not None:
            task = SimpleNamespace(
                attempt_id="A-1", _runtime_event_callback=events.append,
            )
        if out is not None:
            out["clock"] = clock
            out["adapter"] = adapter
        with mock.patch("worker.time.monotonic", clock.monotonic), mock.patch(
            "worker.time.sleep", lambda _seconds: None
        ):
            return self.worker(activity=activity)._wait_for_response(
                adapter, "session", "msg_original", Path("."),
                requirement="modify source", task_mode="MODIFICATION",
                task=task,
                baseline_assistant_ids=baseline,
            )

    def test_a1_immediate_idle_reconciliation_without_timeout_wait(self):
        # The incident's runtime-latency shape: the turn finished at t=100
        # (session idle, owned final response present, correlation broken).
        # Completion must happen AT the t=100 poll — the strict test adapter
        # fails the test if the loop keeps polling toward the 400s activity
        # timeout, so a return proves the wait was removed.
        final = [
            assistant("msg_work", "msg_original"),
            assistant("msg_final", "msg_FOREIGN_parent", finish="stop",
                      parts=[{"type": "text", "text": "done"}]),
        ]
        out: dict = {}
        result = self.run_wait([
            (0, "RUNNING", [assistant("msg_work", "msg_original")]),
            (100, "IDLE", final),
        ], out=out)
        self.assertEqual(("msg_final", "done"), result)
        # Returned at the t=100 observation, well before activity_timeout=400.
        self.assertEqual(100, out["clock"].now)

    def test_a1b_immediate_reconciliation_records_terminal_evidence(self):
        events = []
        final = [
            assistant("msg_final", "msg_other", finish="stop",
                      parts=[{"type": "text", "text": "done"}]),
        ]
        self.run_wait([
            (0, "RUNNING", [assistant("msg_work", "msg_original")]),
            (100, "IDLE", final),
        ], events=events)
        ended = [event for event in events if event["event"] == "ENDED"]
        self.assertEqual(1, len(ended))
        self.assertEqual("EXITED", ended[0]["termination_reason"])
        self.assertEqual(
            "OPENCODE_TERMINAL_CONFIRMED", ended[0]["progress_reconciliation"]
        )
        self.assertEqual(0, ended[0]["exit_code"])
        self.assertNotIn("TIMEOUT", [event["event"] for event in events])

    def test_native_terminal_with_uncorrelated_owned_response_completes(self):
        # Timeout-candidate reconciliation (defense in depth): even when the
        # idle transition and the timeout threshold coincide, the owned final
        # response completes instead of a false OPENCODE_PROGRESS_TIMEOUT.
        final = [
            assistant("msg_work", "msg_original"),
            assistant("msg_final", "msg_FOREIGN_parent", finish="stop",
                      parts=[{"type": "text", "text": "done"}]),
        ]
        result = self.run_wait([
            (0, "RUNNING", [assistant("msg_work", "msg_original")]),
            (100, "IDLE", final),
            (500, "IDLE", final),
        ])
        self.assertEqual(("msg_final", "done"), result)

    def test_nonterminal_stall_keeps_progress_timeout(self):
        unchanged = [assistant("msg_work", "msg_original")]
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "RUNNING", unchanged),
                (500, "RUNNING", unchanged),
            ])

    def test_idle_without_owned_evidence_keeps_progress_timeout(self):
        # Terminal state alone is not success: no owned final assistant
        # response (only the pre-turn baseline response exists) must keep the
        # watchdog failure instead of promoting a stale message.
        stale = assistant("msg_stale", "msg_previous_turn", finish="stop",
                          parts=[{"type": "text", "text": "old"}])
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "IDLE", [stale]),
                (500, "IDLE", [stale]),
            ], baseline=frozenset({"msg_stale"}))

    def test_in_flight_turn_uses_first_busy_poll_as_ownership_baseline(self):
        # Cross-process in-flight recovery: an earlier turn's final response
        # is visible while the session is busy; only the response that
        # appears afterwards may be adopted at reconciliation.
        earlier = assistant("msg_earlier", "msg_previous", finish="stop",
                            parts=[{"type": "text", "text": "earlier"}])
        final = [
            earlier,
            assistant("msg_current", "msg_uncorrelated", finish="stop",
                      parts=[{"type": "text", "text": "current"}]),
        ]
        result = self.run_wait([
            (0, "RUNNING", [earlier]),
            (100, "IDLE", final),
            (500, "IDLE", final),
        ], baseline=None)
        self.assertEqual(("msg_current", "current"), result)

    def test_unknown_baseline_disables_fallback_promotion(self):
        # Baseline unknown (pre-submission snapshot could not be read) and
        # the session never went busy again: correlated-only reconciliation.
        stale = assistant("msg_stale", "msg_previous", finish="stop",
                          parts=[{"type": "text", "text": "old"}])
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"):
            self.run_wait([
                (0, "IDLE", [stale]),
                (500, "IDLE", [stale]),
            ], baseline=None)

    def test_owned_assistant_error_is_not_reported_as_timeout(self):
        final = [
            assistant("msg_final", "msg_other", finish="stop",
                      error={"name": "ProviderError"}),
        ]
        with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_ASSISTANT_ERROR"):
            self.run_wait([
                (0, "RUNNING", [assistant("msg_work", "msg_original")]),
                (100, "IDLE", final),
                (500, "IDLE", final),
            ])

    def test_aborted_command_is_never_promoted_at_reconciliation(self):
        from control_repository import ControlRepository
        from runtime_adapter import CommandDelivery

        temp = tempfile.TemporaryDirectory()
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
        attempt = repo.start_attempt(
            execution, attempt_id="A-1", role="WORKER", model="m"
        )
        binding = repo.bind_native_session(
            execution["execution_id"], runtime="opencode",
            native_session_id="session", adapter_revision="a",
            runtime_version="v", durable=True, state="IDLE",
        )
        command = repo.prepare_command(
            execution["execution_id"], attempt["attempt_id"],
            idempotency_key="k", payload_sha256="a" * 64,
            binding_id=binding["binding_id"],
        )
        repo.set_command_delivery(command["command_id"], CommandDelivery.SENT.value)
        repo.set_command_delivery(
            command["command_id"], CommandDelivery.ACKNOWLEDGED.value,
            native_command_id="msg_original",
        )
        repo.mark_command_aborted(command["command_id"], reason="prior timeout")

        clock = Clock()
        final = [
            assistant("msg_final", "msg_other", finish="stop",
                      parts=[{"type": "text", "text": "late"}]),
        ]
        adapter = PollingAdapter(clock, [
            (0, "RUNNING", [assistant("msg_work", "msg_original")]),
            (100, "IDLE", final),
            (500, "IDLE", final),
        ])
        task = SimpleNamespace(attempt_id="A-1", _runtime_event_callback=None)
        with mock.patch("worker.time.monotonic", clock.monotonic), mock.patch(
            "worker.time.sleep", lambda _seconds: None
        ):
            with self.assertRaisesRegex(
                OpenCodeAdapterError, "OPENCODE_PROGRESS_TIMEOUT"
            ):
                self.worker()._wait_for_response(
                    adapter, "session", "msg_original", Path("."),
                    repository=repo, binding=None,
                    command_id=command["command_id"],
                    requirement="r", task_mode="MODIFICATION", task=task,
                )


# ---------------------------------------------------------------------------
# HOTFIX B — datetime normalization + reporter containment
# ---------------------------------------------------------------------------


class ExecutionEvidenceDatetimeTests(unittest.TestCase):
    def test_mixed_aware_naive_duration_is_computable(self):
        seconds = _duration_seconds(
            "2026-09-16T14:17:59.180224+09:00",  # aware (runtime event)
            "2026-09-16T14:37:04.876557",        # naive (updated_at fallback)
        )
        self.assertIsNotNone(seconds)
        self.assertGreaterEqual(seconds, 0)

    def test_stage_timing_survives_aware_events_with_naive_updated_at(self):
        task = SimpleNamespace(
            progress_events=[
                {"stage": "WORKER", "at": "2026-09-16T14:18:06.960643+09:00"},
                {"stage": "WORKER", "at": "2026-09-16T14:37:03.942093+09:00"},
            ],
            attempt_history=[],
            reviewer_invocations=[],
            execution_runtime_history=[],
            queue_wait_seconds=-1,
            admission_control_seconds=-1,
            ended_at="",
            updated_at="2026-09-16T14:37:04.876557",  # naive
        )
        timing = stage_timing(task)  # must not raise
        self.assertIn("worker_seconds", timing)

    def test_parse_iso_normalizes_naive_to_aware(self):
        parsed = _parse_iso("2026-09-16T14:37:04.876557")
        self.assertIsNotNone(parsed)
        self.assertIsNotNone(parsed.tzinfo)
        aware = _parse_iso("2026-09-16T14:37:04.876557+09:00")
        self.assertIsNotNone(aware.tzinfo)
        # Comparison must not raise.
        self.assertLess(parsed, aware + timedelta(days=1))


class ReporterContainmentTests(unittest.TestCase):
    def _task(self):
        task = TaskState(task_id="TASK-B", requirement="req")
        task.status = "FAILED"
        task.stage = "FAILED"
        task.failure_stage = "WORKER"
        task.failure_code = "OPENCODE_PROGRESS_TIMEOUT"
        task.failure_origin = "HARNESS_CAUSED"
        task.failure_type = "infrastructure"
        task.progress_events = [
            {"stage": "WORKER", "at": "2026-09-16T14:18:06.960643+09:00"},
        ]
        task.started_at = "2026-09-16T14:17:59.180224+09:00"
        task.updated_at = "2026-09-16T14:37:04.876557"  # naive
        return task

    def test_incident_shaped_task_report_does_not_crash(self):
        markdown = Reporter().generate_markdown(self._task())
        self.assertIn("TASK-B", markdown)

    def test_summary_failure_is_contained_and_never_overwrites_failure(self):
        task = self._task()
        canonical = (task.failure_code, task.failure_origin, task.status)
        with mock.patch(
            "reporter.build_execution_summary", side_effect=TypeError("boom")
        ):
            markdown = Reporter().generate_markdown(task)
        self.assertIn("TASK-B", markdown)
        self.assertTrue(task.report_diagnostics)
        self.assertEqual(
            "REPORTER_SECONDARY_FAILURE", task.report_diagnostics[-1]["code"]
        )
        self.assertEqual(
            (task.failure_code, task.failure_origin, task.status), canonical
        )


# ---------------------------------------------------------------------------
# HOTFIX C — failure projection
# ---------------------------------------------------------------------------


def _runtime_failure_result(**overrides):
    result = {
        "task_id": "TASK-09922-C",
        "status": "FAILED",
        "stage": "FAILED",
        "success": False,
        "verification_status": "PARTIAL",
        "failure_stage": "WORKER",
        "failure_code": "OPENCODE_PROGRESS_TIMEOUT",
        "failure_origin": "HARNESS_CAUSED",
        "failure_type": "infrastructure",
        "fix_scope": "NON_RETRYABLE",
        "retry_domain": "RUNTIME_RECOVERY",
        "worktree_disposition": "PRESERVED",
        "execution_id": "EXE-09922",
        "runtime_recovery_count": 1,
        "technical_execution_retry_count": 0,
        "product_semantic_retry_count": 0,
        "runtime_recovery_history": [],
        "changed_files": ["sucui/src/pages/student/cs/SsucCs0010List.vue"],
    }
    result.update(overrides)
    return result


class FailureProjectionTests(unittest.TestCase):
    def test_runtime_recovery_preserved_delta_disposition_is_blocked_not_qa(self):
        status, decision = failure_disposition(
            _runtime_failure_result(), outer_attempt=1, max_outer_attempts=1,
        )
        self.assertEqual(BLOCKED, status)
        self.assertEqual("PRESERVED_DELTA_TECHNICAL_RECOVERY_REQUIRED", decision)

    def test_runtime_recovery_safe_worktree_disposition_stays_blocked(self):
        status, decision = failure_disposition(
            _runtime_failure_result(worktree_disposition="NO_DELTA"),
            outer_attempt=1, max_outer_attempts=1,
        )
        self.assertEqual(BLOCKED, status)
        self.assertEqual("RUNTIME_OR_TECHNICAL_RECOVERY_REQUIRED", decision)

    def test_explicit_qa_request_still_projects_awaiting_qa(self):
        status, decision = failure_disposition(
            {
                "failure_type": "contract",
                "fix_scope": "NON_RETRYABLE",
                "worktree_disposition": "PRESERVED",
                "retry_domain": "",
                "status": "AWAITING_QA",
                "qa_request": {"required": True, "qa_type": "BUSINESS_CONTRACT"},
            },
            outer_attempt=1, max_outer_attempts=1,
        )
        self.assertEqual("AWAITING_QA", status)


class ManagerInfraProjectionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._old_cwd)
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir,
            model="",
            max_retry=1,
            timeout=5,
            worker_type="droid",
            max_workers=1,
        )
        self.manager.profile = None
        self.addCleanup(self.manager.shutdown)

    def _run_worker_infra_failure(self):
        from worker import WorkerResult

        wr = WorkerResult(
            success=False, exit_code=-1, stdout="", stderr="OPENCODE_PROGRESS_TIMEOUT",
            command=["opencode"], execution_failure_code="OPENCODE_PROGRESS_TIMEOUT",
        )

        class _Collector:
            def capture_baseline(self, task=None):
                return object(), ""

        class _Worker:
            def execute(self, requirement, working_dir, task=None):
                return wr

        self.manager.collector = _Collector()
        self.manager._get_worker = lambda _task: (_Worker(), "opencode")
        task = TaskState(
            task_id="TASK-09922-PROJ",
            requirement="sucui/src/pages/App.vue 수정",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
        )
        result = self.manager._run_pipeline(task)
        return result, task

    def test_infra_worker_failure_without_qa_subject_is_not_awaiting_qa(self):
        result, task = self._run_worker_infra_failure()
        self.assertFalse(result.success)
        self.assertEqual("OPENCODE_PROGRESS_TIMEOUT", task.failure_code)
        self.assertEqual("WORKER", task.failure_stage)
        self.assertEqual("HARNESS_CAUSED", task.failure_origin)
        self.assertEqual("infrastructure", task.failure_type)
        self.assertEqual("PRESERVED", task.worktree_disposition)
        self.assertTrue(task.keep_working_tree)
        self.assertEqual("RUNTIME_RECOVERY", task.retry_domain)
        self.assertNotEqual("AWAITING_QA", task.status)
        self.assertEqual("FAILED", task.status)
        self.assertEqual("FAILED", task.stage)
        self.assertEqual({}, task.qa_request)
        self.assertEqual(0, task.product_semantic_retry_count)


# ---------------------------------------------------------------------------
# HOTFIX D — preserved-delta finalization continuation
# ---------------------------------------------------------------------------


class PreservedDeltaFinalizationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._old_cwd)
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir,
            model="",
            max_retry=1,
            timeout=5,
            worker_type="droid",
            max_workers=1,
        )
        self.manager.profile = None
        self.addCleanup(self.manager.shutdown)

    def _preserved_task(self):
        task = TaskState(
            task_id="TASK-09922-D",
            requirement="sucui/src/pages/student/cs/SsucCs0010List.vue 계약 정합성 수정",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
        )
        task.status = "FAILED"
        task.stage = "FAILED"
        task.failure_stage = "WORKER"
        task.failure_code = "OPENCODE_PROGRESS_TIMEOUT"
        task.failure_origin = "HARNESS_CAUSED"
        task.failure_type = "infrastructure"
        task.worktree_disposition = "PRESERVED"
        task.keep_working_tree = True
        task.worker = "opencode"
        task.actual_worker = "opencode"
        task.worker_stdout = "구현 완료"
        task.worker_stderr = "OPENCODE_PROGRESS_TIMEOUT"
        task.exit_code = -1
        task.actual_invocation_id = "INV-1"
        task.execution_runtime = {"process_alive": False, "pid": 0}
        task.review_status = "PENDING"
        return task

    def _install_gates(self, worker_calls):
        from build_test_runner import BuildTestResult
        from git_collector import EolRepairResult, GitDiffResult
        from worker import WorkerResult

        git = GitDiffResult(
            success=True,
            changed_files=["sucui/src/pages/student/cs/SsucCs0010List.vue"],
            diff=(
                "diff --git a/sucui/src/pages/student/cs/SsucCs0010List.vue "
                "b/sucui/src/pages/student/cs/SsucCs0010List.vue\n"
                "--- a/sucui/src/pages/student/cs/SsucCs0010List.vue\n"
                "+++ b/sucui/src/pages/student/cs/SsucCs0010List.vue\n"
                "@@ -1,2 +1,3 @@\n context\n+인권센터 contract\n final\n"
            ),
        )
        build = BuildTestResult(success=True, build_status="PASS", test_status="SKIPPED")

        class _Collector:
            def capture_baseline(self, task=None):
                return object(), ""

            def head_baseline(self, task=None):
                return object(), ""

            def collect(self, task=None, baseline=None):
                task.changed_files = list(git.changed_files)
                task.git_status = "M  SsucCs0010List.vue"
                task.git_diff = git.diff
                task.git_diff_stat = "1 file changed"
                return git

            def repair_eol_to_index(self, task=None, baseline=None):
                return EolRepairResult(True, [], [], "")

            def assess_commit_safety(self, task=None, baseline=None):
                return True, [], {"f": "a" * 64}

        class _Worker:
            def execute(self, requirement, working_dir, task=None):
                worker_calls.append(1)
                return WorkerResult(False, -1, "", "unexpected rerun", [])

        class _Runner:
            def run(self, changed_files=None, target_modules=None):
                return build

        class _Reviewer:
            invoked = 0

            def review(self, task=None, **kwargs):
                type(self).invoked += 1
                task.review_status = "REVIEW_PASS"
                return ReviewResult(
                    passed=True, status="REVIEW_PASS",
                    reviewed_files=list(kwargs.get("changed_files", [])),
                )

        self.manager.collector = _Collector()
        self.manager._get_worker = lambda _task: (_Worker(), "opencode")
        self.manager.build_test_runner = _Runner()
        self.manager.reviewer = _Reviewer()
        return _Reviewer

    def test_preserved_delta_continues_finalization_without_worker_rerun(self):
        worker_calls: list[int] = []
        reviewer_cls = self._install_gates(worker_calls)
        task = self._preserved_task()
        result = self.manager.run_preserved_delta_finalization(task)
        self.assertEqual([], worker_calls)
        self.assertEqual(1, reviewer_cls.invoked)
        self.assertEqual(0, task.retry_count)
        self.assertEqual(0, task.product_semantic_retry_count)
        self.assertIn("preserved_delta_finalization", task.control_recovery_evidence)
        self.assertFalse(
            task.control_recovery_evidence["preserved_delta_finalization"]["worker_rerun"]
        )
        self.assertEqual(
            ["sucui/src/pages/student/cs/SsucCs0010List.vue"], task.changed_files,
        )

    def test_alive_writer_refuses_continuation(self):
        worker_calls: list[int] = []
        self._install_gates(worker_calls)
        task = self._preserved_task()
        task.execution_runtime = {"process_alive": True, "pid": 123}
        with self.assertRaisesRegex(RuntimeError, "WRITER_PROCESS_ALIVE"):
            self.manager.run_preserved_delta_finalization(task)

    def test_opencode_unquiesced_session_refuses_continuation(self):
        worker_calls: list[int] = []
        self._install_gates(worker_calls)
        task = self._preserved_task()
        task.native_session_id = "ses_alive"
        with mock.patch(
            "worker.confirm_opencode_writer_quiesced",
            return_value=(False, "ACTIVE"),
        ):
            with self.assertRaisesRegex(RuntimeError, "WRITER_NOT_QUIESCED"):
                self.manager.run_preserved_delta_finalization(task)

    def test_control_repository_job_refuses_console_path(self):
        worker_calls: list[int] = []
        self._install_gates(worker_calls)
        task = self._preserved_task()
        task.control_repository_path = "D:/kkm_test/.control/control.sqlite3"
        with self.assertRaisesRegex(RuntimeError, "QUEUE_RECOVERY"):
            self.manager.run_preserved_delta_finalization(task)

    def test_qa_subject_refuses_continuation(self):
        worker_calls: list[int] = []
        self._install_gates(worker_calls)
        task = self._preserved_task()
        task.qa_request = {"required": True, "qa_type": "BUSINESS_CONTRACT"}
        with self.assertRaisesRegex(RuntimeError, "QA_SUBJECT_PRESENT"):
            self.manager.run_preserved_delta_finalization(task)


# ---------------------------------------------------------------------------
# HOTFIX E — MID frontend context diet
# ---------------------------------------------------------------------------


def _diet_profile(root: Path) -> ProjectProfile:
    profile_dir = root / "profile"
    (profile_dir / "ui_map" / "suc").mkdir(parents=True)
    (profile_dir / "references" / "db").mkdir(parents=True)
    (profile_dir / "hooks").mkdir(parents=True)
    (profile_dir / "project.json").write_text("{}", encoding="utf-8")
    (profile_dir / "project-rules.md").write_text(
        "# Rules\n- 최소 수정만 허용\n", encoding="utf-8",
    )
    (profile_dir / "PROJECT_ANALYSIS.md").write_text(
        "# 인권센터 상담\nSsucCs0010List 화면은 인권센터 상담 신규 등록을 담당한다.\n"
        "# 학사 지원 안내\nsucui 전반에 걸친 학사 지원 안내 문구 규격을 다룬다.\n",
        encoding="utf-8",
    )
    (profile_dir / "ui_map" / "suc" / "UI_MAP_suc.md").write_text(
        "# 인권센터 신규 등록 화면\nSsucCs0010List.vue 신규 등록 화면 매핑이다.\n",
        encoding="utf-8",
    )
    (profile_dir / "ui_map" / "MENU_ID_INDEX.md").write_text(
        "# 전체 메뉴 인덱스\nSsucCs0010List 메뉴 목록과 모든 화면의 menu id 인덱스.\n",
        encoding="utf-8",
    )
    (profile_dir / "references" / "frontend.md").write_text(
        "# 프론트엔드 규약\nVue 컴포넌트 수정 규약.\n", encoding="utf-8",
    )
    (profile_dir / "references" / "backend.md").write_text(
        "# 백엔드 규약\nSsucCs0010List 컨트롤러와 서비스 계층 규약.\n", encoding="utf-8",
    )
    (profile_dir / "references" / "db" / "README.md").write_text(
        "# DB 개요\nSsucCs0010List 관련 테이블 전체 스키마 설명과 운영 가이드.\n",
        encoding="utf-8",
    )
    always = profile_dir / "hooks" / "validation-project.md"
    always.write_text("# 검증\n- 증거 필수\n", encoding="utf-8")
    return ProjectProfile(
        schema_version=1,
        id="e4u",
        display_name="diet fixture",
        profile_dir=profile_dir,
        workspace_roots=(root,),
        project_rules=profile_dir / "project-rules.md",
        analysis=profile_dir / "PROJECT_ANALYSIS.md",
        ui_map=profile_dir / "ui_map",
        db_catalog=None,
        modules=(
            ModuleProfile(name="sucui", path="sucui", kind="frontend", protected=False),
            ModuleProfile(name="suc", path="suc", kind="backend", protected=False),
        ),
        rules={
            "always": (always,),
            "frontend": (profile_dir / "references" / "frontend.md",),
            "backend": (profile_dir / "references" / "backend.md",),
            "sql": (),
        },
        area_patterns={},
        protected_paths=(),
        local_dev_config_excludes=(),
    )


def _policy_root(root: Path) -> Path:
    policy = root / "agents"
    (policy / "hooks").mkdir(parents=True)
    (policy / "skills" / "project-context").mkdir(parents=True)
    (policy / "skills" / "frontend").mkdir(parents=True)
    (policy / "AGENTS.md").write_text("# Common\n최소 공통 규칙.\n", encoding="utf-8")
    (policy / "hooks" / "validation.md").write_text(
        "# 검증 규칙\n- 증거 없는 PASS 금지\n", encoding="utf-8",
    )
    (policy / "skills" / "project-context" / "SKILL.md").write_text(
        "# profile resolution\n프로필 해석 규칙.\n# selective loading\n선택적 로딩.\n",
        encoding="utf-8",
    )
    (policy / "skills" / "frontend" / "SKILL.md").write_text(
        "# workflow\n프론트엔드 워크플로.\n# checklist\n체크리스트.\n",
        encoding="utf-8",
    )
    return policy


class MidFrontendContextDietTests(unittest.TestCase):
    """A bounded MID frontend Job gets frontend-scoped context only."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = _diet_profile(self.root)
        self.policy = _policy_root(self.root)
        self.requirement = (
            "인권센터 상담 신규/수정 contract 정합성 수정.\n"
            "대상 화면: sucui/src/pages/student/cs/SsucCs0010List.vue, "
            "sucui/src/pages/student/cs/SsucCs0020List.vue\n"
            "수정: 인권센터 신규 등록을 ONLINE-only로 변경한다."
        )

    def _pack(self):
        return build_context_pack(
            role="WORKER",
            requirement=self.requirement,
            profile=self.profile,
            workspace=self.root,
            policy_root=self.policy,
            target_modules=["sucui"],
            changed_files=[],
            task_mode="MODIFICATION",
        )

    def test_backend_reference_not_selected_for_frontend_only_job(self):
        pack = self._pack()
        self.assertNotIn(
            "profile:references/backend.md",
            [item["path"] for item in pack.manifest["selected_resources"]],
        )

    def test_db_readme_not_selected_for_frontend_only_job(self):
        pack = self._pack()
        self.assertNotIn(
            "profile:references/db/README.md",
            [item["path"] for item in pack.manifest["selected_resources"]],
        )

    def test_menu_index_not_selected_when_exact_screen_map_matched(self):
        pack = self._pack()
        paths = [item["path"] for item in pack.manifest["selected_resources"]]
        self.assertNotIn("profile:ui_map/MENU_ID_INDEX.md", paths)
        self.assertIn("profile:ui_map/suc/UI_MAP_suc.md", paths)

    def test_unrelated_analysis_section_not_selected(self):
        pack = self._pack()
        self.assertNotIn("학사 지원 안내", pack.prompt)
        # The task-related analysis section remains available.
        self.assertIn("인권센터 상담", pack.prompt)

    def test_frontend_reference_and_exact_ui_map_are_selected(self):
        pack = self._pack()
        paths = [item["path"] for item in pack.manifest["selected_resources"]]
        self.assertIn("profile:references/frontend.md", paths)
        self.assertIn("policy:skills/frontend/SKILL.md", paths)

    def test_task_spec_requested_changes_not_duplicated_as_prose(self):
        pack = self._pack()
        self.assertNotIn('"requested_changes"', pack.prompt)
        self.assertIn("[Deterministic TaskSpec]", pack.prompt)
        # The approved prose itself stays authoritative and present.
        self.assertIn("ONLINE-only", pack.prompt)

    def test_reverse_lookup_prohibition_is_not_distorted_into_exclusions(self):
        # 0.99.2.2 completion patch regression: the extractor classifies the
        # approved heading "### 4. 수정 저장에서 reverse lookup 금지" as both a
        # constraint and an EXCLUSION (it is a required implementation). The
        # Worker-visible TaskSpec must not re-render that heuristic
        # interpretation; the approved prose stays the authority.
        from context_foundation import build_task_spec

        heading = "### 4. 수정 저장에서 reverse lookup 금지"
        requirement = (
            "인권센터 상담 수정 저장 계약을 맞춘다.\n"
            f"{heading}\nreverse lookup으로 choiceList를 구성하지 않는다.\n"
            "대상: sucui/src/pages/student/cs/SsucCs0010List.vue\n"
        )
        spec = build_task_spec(requirement, ["sucui"], task_mode="MODIFICATION")
        # Machine-internal extraction still records the heuristic fields...
        self.assertIn(heading, spec.exclusions)
        self.assertIn(heading, spec.constraints)
        # ...but the Worker-visible rendering omits every prose-derived field.
        pack = build_context_pack(
            role="WORKER",
            requirement=requirement,
            profile=self.profile,
            workspace=self.root,
            policy_root=self.policy,
            target_modules=["sucui"],
            changed_files=[],
            task_mode="MODIFICATION",
        )
        for field in ("requested_changes", "constraints", "exclusions",
                      "acceptance_criteria"):
            self.assertNotIn(f'"{field}"', pack.prompt, field)
        # The approved prose remains verbatim and authoritative.
        self.assertIn(heading, pack.prompt)
        self.assertIn("reverse lookup으로 choiceList를 구성하지 않는다", pack.prompt)

    def test_worker_visible_task_spec_keeps_structural_metadata_only(self):
        pack = self._pack()
        for field in ("intent", "target_modules", "explicit_paths", "screens",
                      "risk_tags", "unknowns"):
            self.assertIn(f'"{field}"', pack.prompt, field)
        for field in ("requested_changes", "constraints", "exclusions",
                      "acceptance_criteria"):
            self.assertNotIn(f'"{field}"', pack.prompt, field)

    def test_prose_field_removal_never_increases_context_size(self):
        import json as _json

        from context_foundation import build_task_spec

        requirement = (
            "### 4. 수정 저장에서 reverse lookup 금지\n"
            "반드시 기존 choiceList를 유지한다. 제외 없이 적용한다. "
            "완료 조건: 통과 확인.\n"
            "sucui/src/pages/student/cs/SsucCs0020List.vue 수정 구현 추가\n"
        )
        spec = build_task_spec(requirement, ["sucui"], task_mode="MODIFICATION")
        full = _json.dumps(spec.mapping(), ensure_ascii=False, sort_keys=True, indent=2)
        filtered = {
            key: value for key, value in spec.mapping().items()
            if key not in {
                "requested_changes", "constraints", "exclusions", "acceptance_criteria",
            }
        }
        trimmed = _json.dumps(filtered, ensure_ascii=False, sort_keys=True, indent=2)
        # The diet direction: removing prose-derived fields shrinks the
        # rendered contract whenever the extractor populated them.
        self.assertLess(len(trimmed), len(full))
        self.assertTrue(spec.constraints or spec.exclusions)


# ---------------------------------------------------------------------------
# HOTFIX F — reviewer prompt observability
# ---------------------------------------------------------------------------


class ReviewerPromptMetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = _diet_profile(self.root)
        self.policy = _policy_root(self.root)

    def test_reviewer_prompt_metrics_recorded(self):
        reviewer = Reviewer(
            profile=self.profile,
            policy_root=self.policy,
            working_dir=str(self.root),
        )
        task = TaskState(
            task_id="TASK-09922-F",
            requirement="SsucCs0010List.vue 인권센터 contract 수정",
            working_dir=str(self.root),
            task_mode="MODIFICATION",
        )
        task.review_policy = {"context_tier": "LOCAL"}
        prompt = reviewer._build_review_prompt(
            requirement=task.requirement,
            changed_files=["sucui/src/pages/student/cs/SsucCs0010List.vue"],
            git_diff="diff --git a/x b/x\n+delta\n",
            task=task,
        )
        metrics = dict(task.prompt_metrics.get("reviewer_prompt") or {})
        self.assertTrue(metrics)
        self.assertEqual(metrics["reviewer_prompt_chars"], len(prompt))
        self.assertGreater(metrics["reviewer_context_chars"], 0)
        self.assertGreater(metrics["reviewer_requirement_chars"], 0)
        self.assertGreater(metrics["reviewer_diff_chars"], 0)
        self.assertGreater(metrics["reviewer_prompt_tokens_est"], 0)
        self.assertGreaterEqual(metrics["reviewer_selected_resource_count"], 1)
        self.assertEqual("LOCAL", metrics["reviewer_context_tier"])
        self.assertEqual(1, metrics["recorded_invocations"])
        self.assertIn("reviewer_prompt_sha256", metrics)
        self.assertEqual(64, len(metrics["reviewer_prompt_sha256"]))
        # No prompt content is duplicated into durable state.
        self.assertNotIn(prompt[:80], json.dumps(task.prompt_metrics))


# ---------------------------------------------------------------------------
# HOTFIX G — provenance
# ---------------------------------------------------------------------------


class ConsoleExecutionContractTests(unittest.TestCase):
    CONTRACT = {
        "client_job_id": "DAILY-HUMAN-CENTER-20260916",
        "reasoning_effort": "low",
        "target_resources": [
            "sucui/src/pages/student/cs/SsucCs0010List.vue",
            "sucui/src/pages/student/cs/SsucCs0020List.vue",
        ],
        "test_plan": {"mandatory": True, "verification_depth": "TARGETED"},
        "commit_summary": "인권센터 상담 contract 정합성",
    }

    def _requirement(self):
        return (
            "인권센터 상담 contract 정합성 수정 요구사항 본문.\n\n"
            "## Execution contract\n```json\n"
            + json.dumps(self.CONTRACT, ensure_ascii=False)
            + "\n```\n"
        )

    def test_contract_block_is_parsed(self):
        from run import _parse_execution_contract
        parsed = _parse_execution_contract(self._requirement())
        self.assertEqual(self.CONTRACT, parsed)
        self.assertEqual({}, _parse_execution_contract("일반 요구사항 prose"))

    def test_create_task_applies_contract_provenance(self):
        from run import create_task
        task = create_task(
            requirement=self._requirement(),
            worker_type="opencode",
            droid_model="",
            codex_model=None,
            target_module=["sucui"],
            working_dir="D:/inha_dev",
            task_id="TASK-09922-G",
            codex_reasoning_effort="medium",
            opencode_model="opencode-go/deepseek-v4.1-flash",
            profile=None,
        )
        self.assertEqual("low", task.codex_reasoning_effort)
        self.assertTrue(task.test_plan.get("mandatory") is True)
        self.assertEqual("TARGETED", task.test_plan.get("verification_depth"))
        self.assertEqual(
            self.CONTRACT["target_resources"], list(task.target_resources),
        )
        self.assertEqual(self.CONTRACT["client_job_id"], task.client_job_id)
        self.assertEqual(self.CONTRACT["commit_summary"], task.commit_summary)

    def test_invalid_test_plan_is_rejected_not_distorted(self):
        from run import create_task
        requirement = (
            "요구사항\n```json\n"
            + json.dumps({
                "client_job_id": "X",
                "test_plan": {"mandatory": "yes please"},
            })
            + "\n```\n"
        )
        task = create_task(
            requirement=requirement, worker_type="opencode", droid_model="",
            codex_model=None, target_module=[], working_dir=".",
            task_id="TASK-09922-G2",
        )
        self.assertEqual({}, task.test_plan)

    def test_mandatory_test_plan_drives_tester_requirement(self):
        from tester import should_run_tester
        task = TaskState(task_id="T", requirement="r")
        task.test_plan = {"mandatory": True, "verification_depth": "TARGETED"}
        required, reason = should_run_tester(task)
        self.assertTrue(required)
        self.assertEqual("MANDATORY_TEST_PLAN", reason)


class OpenCodeEffortProvenanceTests(unittest.TestCase):
    def test_requested_low_is_effective_with_runtime_unsupported_provenance(self):
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
        task.codex_reasoning_effort = "low"
        worker, selected = manager._get_worker(task)
        self.assertEqual("opencode", selected)
        self.assertEqual("low", task.worker_effective_reasoning_effort)
        self.assertFalse(task.worker_effort_escalated)
        self.assertEqual("", task.worker_effort_escalation_reason)
        self.assertEqual(
            "RUNTIME_UNSUPPORTED_PROVIDER_DEFAULT", task.worker_effort_provenance
        )
        self.assertEqual("low", worker.reasoning_effort)

    def test_codex_effort_provenance_is_provider_requested(self):
        manager = object.__new__(Manager)
        manager.timeout = 300
        manager.timeout_config = TimeoutConfig()
        manager.worker_type = "codex"
        manager.model = ""
        manager.codex_model = "gpt-x"
        manager.opencode_model = ""
        manager.profile = None
        manager.policy_root = None
        manager.task_root = Path(".tasks")
        task = TaskState(
            task_id="T", requirement="x", worker="codex", codex_model="gpt-x",
        )
        task.codex_reasoning_effort = "medium"
        manager._get_worker(task)
        self.assertEqual("PROVIDER_REQUESTED", task.worker_effort_provenance)


class VerificationBundleTests(unittest.TestCase):
    """ISSUE R: the 0.99.2.2 verification bundle carries 0.99.2.2 evidence."""

    ROOT = Path(__file__).resolve().parent

    def test_bundle_carries_current_release_artifacts(self):
        import verification_bundle

        version = (self.ROOT / "VERSION").read_text(encoding="utf-8").strip()
        self.assertEqual("0.99.2.2", version)
        with tempfile.TemporaryDirectory() as td:
            result = verification_bundle.collect(self.ROOT, output_root=td)
            self.assertEqual("0.99.2.2", result["version"])
            self.assertTrue(result["build_id"].startswith("0.99.2.2+"))
            bundle_dir = Path(result["bundle_dir"])
            manifest = json.loads(
                (bundle_dir / "verification-manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual("HarnessVerificationBundle", manifest["kind"])
            self.assertEqual("0.99.2.2 Verification Bundle", manifest["title"])
            self.assertEqual("0.99.2.2", manifest["version"])
            listed = {entry["bundle_path"] for entry in manifest["entries"]}
            self.assertEqual({
                "VERSION",
                "docs/PATCH_0.99.2.2.md",
                "tests/test_09922_realwork_hotfix.py",
                "releases/0.99.2.2/runtime-manifest.json",
                "releases/0.99.2.2/SHA256SUMS.json",
            }, listed)
            for entry in manifest["entries"]:
                target = bundle_dir / entry["bundle_path"]
                self.assertTrue(target.is_file(), entry["bundle_path"])
                self.assertEqual(
                    entry["sha256"],
                    hashlib.sha256(target.read_bytes()).hexdigest(),
                    entry["bundle_path"],
                )
            release_manifest = json.loads(
                (bundle_dir / "releases" / "0.99.2.2" / "runtime-manifest.json")
                .read_text(encoding="utf-8")
            )
            self.assertEqual("0.99.2.2", release_manifest["version"])
            self.assertEqual(5, release_manifest["control_schema_version"])

    def test_collector_has_no_stale_hardcoded_release_path(self):
        source = (self.ROOT / "verification_bundle.py").read_text(encoding="utf-8")
        self.assertNotIn("0.99.2.1", source)
        self.assertNotIn("0.99.2\"", source)


if __name__ == "__main__":
    unittest.main()
