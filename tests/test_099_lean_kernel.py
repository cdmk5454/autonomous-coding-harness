"""0.99.1 Lean Execution Kernel focused regressions.

Covers the 0.99.1 release deltas against 0.99.0:

- Wave A: truthful OpenCode capability declarations, original runtime failure
  code preservation, managed-server pipe draining.
- Wave C: typed operation-capability floor (deny by default, no keyword scan).
- Wave D: evidence-proportional review reuse over the existing DiffBatch
  atom ledger (REUSED_VALID_EVIDENCE vs STALE).
- Wave 0: derived per-stage timing breakdown (queue wait, admission, worker,
  build/test, review, verification, human wait) without estimated values.
"""

import os
import subprocess
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import harness_temp
from job_contract import JobContractError, JobRequest
from review_policy import assess_review_risk
from reviewer import REVIEW_FAIL, REVIEW_PASS, ReviewResult, plan_diff_batches
import review_evidence
from manager import Manager
from worker import CODEX_IPC_FAILURES
from execution_evidence import job_admission_timing, stage_timing
from job_queue import job_lead_time
from opencode_runtime_adapter import OpenCodeRuntimeAdapter, PERSONAL
from opencode_runtime_adapter import OpenCodeAdapterError
from task_state import TaskState
from test_job_control import request


class OpenCodeDescriptorTruthfulnessTests(unittest.TestCase):
    """G1: capability declarations must match the implemented adapter surface."""

    def test_pending_approval_and_question_are_partial_not_supported(self):
        adapter = OpenCodeRuntimeAdapter(base_url="http://127.0.0.1:4096", mode=PERSONAL)
        descriptor = adapter.preflight()
        if descriptor.validation_disposition == "UNAVAILABLE":
            self.skipTest("installed OpenCode CLI unavailable")
        snapshot = descriptor.contract_snapshot()["capability_snapshot"]
        self.assertEqual("PARTIAL", snapshot["interaction.pending_approval"])
        self.assertEqual("PARTIAL", snapshot["interaction.pending_question"])
        self.assertEqual("UNSUPPORTED", snapshot["interaction.permission_response"])
        # Implemented surfaces stay declared as implemented.
        self.assertEqual("SUPPORTED", snapshot["session.create"])
        self.assertEqual("SUPPORTED", snapshot["turn.submit"])
        self.assertEqual("SUPPORTED", snapshot["interaction.response_delivery"])

    def test_query_session_never_reports_waiting_input(self):
        """The unreachable WAITING_INPUT path stays honest in the contract."""
        adapter = OpenCodeRuntimeAdapter(base_url="http://127.0.0.1:1", mode=PERSONAL)
        descriptor = adapter.preflight()
        if descriptor.validation_disposition == "UNAVAILABLE":
            self.skipTest("installed OpenCode CLI unavailable")
        self.assertEqual(
            "PARTIAL",
            descriptor.contract_snapshot()["capability_snapshot"]
            ["interaction.pending_approval"],
        )


class OriginalFailureCodePreservationTests(unittest.TestCase):
    """Wave A: the adapter's original failure code must survive the worker."""

    def test_technical_runtime_codes_are_ipc_failures(self):
        for code in (
            "OPENCODE_PROGRESS_TIMEOUT", "OPENCODE_ASSISTANT_ERROR",
            "OPENCODE_TRANSPORT_ERROR", "OPENCODE_NATIVE_MESSAGE_ID_INVALID",
            "OPENCODE_RUNTIME_HEARTBEAT_PERSISTENCE_FAILED",
            "OPENCODE_SERVER_START_TIMEOUT", "OPENCODE_HTTP_ERROR",
        ):
            self.assertIn(code, CODEX_IPC_FAILURES, code)

    def test_worker_returns_original_adapter_error_code(self):
        from worker import OpenCodeWorker

        class TimeoutAdapter:
            def __init__(self, **_kwargs):
                self.base_url = "http://127.0.0.1:1"

            def preflight(self):
                from runtime_adapter import RuntimeDescriptor
                return RuntimeDescriptor(
                    runtime="opencode", version="fixture", schema_compatible=True,
                    detail="fixture",
                )

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
                return {"id": "session-opencode-1"} if invocation.url.endswith("/session") else None

            def query_session(self, session_id, **_kwargs):
                from runtime_adapter import SessionQuery
                return SessionQuery(session_id, "ACTIVE")

            def request(self, method, path, body=None):
                raise OpenCodeAdapterError("OPENCODE_PROGRESS_TIMEOUT", "no new output")

        task = TaskState(
            task_id="T-CODE", requirement="fixture", worker="opencode",
            opencode_model="provider/model", target_module=["sample-service"],
        )
        with patch.dict(os.environ, {
            "KKM_OPENCODE_LIVE_VALIDATED": "1",
            "OPENCODE_SERVER_PASSWORD": "fixture-password",
        }), patch("opencode_runtime_adapter.OpenCodeRuntimeAdapter", TimeoutAdapter), patch(
            "worker.build_prompt", return_value="fixture prompt",
        ):
            result = OpenCodeWorker(timeout=1, model="provider/model").execute(
                "fixture", ".", task,
            )
        self.assertFalse(result.success)
        self.assertEqual(
            "OPENCODE_PROGRESS_TIMEOUT", result.execution_failure_code,
        )
        self.assertIn("OPENCODE_PROGRESS_TIMEOUT", result.stderr)


class ManagedServerPipeDrainTests(unittest.TestCase):
    """Wave A: the managed server must not deadlock on full OS pipes."""

    def test_local_server_spawn_uses_devnull_streams(self):
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

        adapter = OpenCodeRuntimeAdapter(mode=PERSONAL, timeout=0.2)
        executable = Path(adapter.executable)
        with patch("opencode_runtime_adapter.shutil.which", return_value=str(executable)), patch(
            "opencode_runtime_adapter.subprocess.Popen", FakePopen,
        ):
            with self.assertRaisesRegex(OpenCodeAdapterError, "OPENCODE_SERVER"):
                adapter.start_local_server(cwd=".")
        self.assertIs(subprocess.DEVNULL, captured.get("stdout"))
        self.assertIs(subprocess.DEVNULL, captured.get("stderr"))
        self.assertIs(subprocess.DEVNULL, captured.get("stdin"))


class TypedOperationCapabilityFloorTests(unittest.TestCase):
    """Wave C: dangerous external effects are denied by typed request only."""

    def test_unsupported_capability_is_rejected_fail_closed(self):
        for capability in (
            "PRODUCTION_DB_APPLY", "PRODUCTION_DB_WRITE", "REAL_EXTERNAL_SEND",
            "REAL_PAYMENT", "DESTRUCTIVE_EXTERNAL_OPERATION", "PII_EXTERNAL_EGRESS",
        ):
            with self.assertRaisesRegex(
                JobContractError, "OPERATION_CAPABILITY_UNSUPPORTED_AUTONOMOUS",
            ):
                request(operation_capabilities=[capability])

    def test_unknown_capability_is_rejected(self):
        with self.assertRaisesRegex(JobContractError, "OPERATION_CAPABILITY_UNKNOWN"):
            request(operation_capabilities=["TELEPORT_PRODUCT"])

    def test_default_request_needs_no_capability(self):
        parsed = request()
        self.assertEqual((), parsed.operation_capabilities)
        self.assertNotIn("operation_capabilities", parsed.to_dict())

    def test_prohibition_text_alone_never_changes_execution_capability(self):
        """Keyword-only false-positive regression (scenario 15).

        The dangerous wording raises review depth (HIGH/CRITICAL review with a
        stronger reviewer), but it neither grants nor blocks execution: the
        contract validates on typed fields only, and a request without typed
        capabilities remains a normal coding job.
        """
        text = (
            "production 운영 DB에 drop table 마이그레이션을 적용하고 "
            "실제 SMS/알림톡/email/kakao 발송과 payment 결제를 수행한다. "
            "개인정보 PII를 외부로 전송한다."
        )
        decision = assess_review_risk(
            requirement=text, target_modules=["sample-service"], changed_files=["sample-service/A.java"],
            git_diff="", build_status="PASS", test_status="PASS", test_required=True,
        )
        self.assertIn(decision.risk_level, {"HIGH", "CRITICAL"})
        self.assertIn("PRODUCTION_DESTRUCTIVE_OPERATION", decision.hard_flags)
        # The same wording validates as a normal coding contract with no typed
        # capability request: keywords alone never gate execution.
        parsed = request(requirement=text, target_modules=["sample-service"], target_resources=[])
        self.assertEqual((), parsed.operation_capabilities)
        # Writing a migration file is normal code editing and needs no
        # capability; applying one to production is a different capability.
        parsed_migration_edit = request(
            requirement="마이그레이션 SQL 파일을 작성한다 (적용 아님).",
            target_modules=["sample-service"],
        )
        self.assertEqual((), parsed_migration_edit.operation_capabilities)


MODULE_HEADER = "--- [module: sample-service] ---\n"


def _file_diff(name: str, added: str) -> str:
    return (
        f"{MODULE_HEADER}"
        f"diff --git a/{name} b/{name}\n"
        f"index 1111111..2222222 100644\n"
        f"--- a/{name}\n"
        f"+++ b/{name}\n"
        f"@@ -1,3 +1,4 @@\n"
        f" context line\n"
        f"+{added}\n"
        f" another context\n"
        f" final context\n"
    )


# Module "sample-service" plus path "A.java" -> planner file identity "sample-service/A.java".
DIFF_V1 = _file_diff("A.java", "added A") + _file_diff("B.java", "added B")
# The fix changes only B's content; A stays byte-identical.
DIFF_V2 = _file_diff("A.java", "added A") + _file_diff("B.java", "added B fixed")


def _result_for(batch, *, passed):
    return ReviewResult(
        passed=passed,
        status=REVIEW_PASS if passed else REVIEW_FAIL,
        reviewed_files=list(batch.changed_files),
        reason="" if passed else "defect in " + ", ".join(batch.changed_files),
    )


class ReviewEvidenceReuseTests(unittest.TestCase):
    """Wave D #6 / scenarios 16-17: evidence-proportional review."""

    def setUp(self):
        # Force one batch per file so PASS/FAIL scopes are separable.
        self.plan_v1 = plan_diff_batches(DIFF_V1, max_chars=300, max_batches=12)
        self.plan_v2 = plan_diff_batches(DIFF_V2, max_chars=300, max_batches=12)
        self.assertTrue(self.plan_v1.valid)
        self.assertTrue(self.plan_v2.valid)
        self.assertEqual(2, len(self.plan_v1.batches))
        self.assertEqual(2, len(self.plan_v2.batches))
        self.proof = {
            "acceptance_hash": "acc-hash",
            "contract_revision": 1,
            "execution_surface_hash": "surface-hash",
        }

    def test_round1_records_only_pass_units(self):
        pairs = [
            (batch, _result_for(batch, passed=("sample-service/B.java" not in batch.changed_files)))
            for batch in self.plan_v1.batches
        ]
        evidence = review_evidence.record(pairs, round_no=1, **self.proof)
        self.assertEqual(review_evidence.EVIDENCE_SCHEMA, evidence["schema"])
        units = evidence["units"]
        self.assertTrue(units)
        for unit in units.values():
            self.assertEqual("sample-service/A.java", unit["path"])

    def test_fresh_units_require_exact_proof_inputs(self):
        pairs = [
            (batch, _result_for(batch, passed=True)) for batch in self.plan_v1.batches
        ]
        evidence = review_evidence.record(pairs, round_no=1, **self.proof)
        self.assertTrue(review_evidence.fresh_units(evidence, **self.proof))
        for changed in (
            {"acceptance_hash": "other"},
            {"contract_revision": 2},
            {"execution_surface_hash": "other"},
        ):
            inputs = dict(self.proof)
            inputs.update(changed)
            self.assertEqual({}, review_evidence.fresh_units(evidence, **inputs), changed)
        self.assertEqual({}, review_evidence.fresh_units({"schema": "older"}, **self.proof))

    def test_partition_reuses_only_unaffected_pass_scope(self):
        # Round 1: A passes, B fails.
        pairs = [
            (batch, _result_for(batch, passed=("sample-service/B.java" not in batch.changed_files)))
            for batch in self.plan_v1.batches
        ]
        evidence = review_evidence.record(pairs, round_no=1, **self.proof)
        units = review_evidence.fresh_units(evidence, **self.proof)
        partition = review_evidence.partition(self.plan_v2, units)
        self.assertTrue(partition.enabled)
        self.assertIn("sample-service/A.java", partition.reused_files)
        self.assertIn("sample-service/B.java", partition.stale_files)
        self.assertEqual(1, partition.prior_round)

    def test_manager_reuse_plan_and_full_coverage_aggregate(self):
        manager = object.__new__(Manager)
        pairs1 = [
            (batch, _result_for(batch, passed=("sample-service/B.java" not in batch.changed_files)))
            for batch in self.plan_v1.batches
        ]
        task = SimpleNamespace(
            retry_count=1,
            review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1},
            review_evidence=review_evidence.record(pairs1, round_no=1, **self.proof),
        )
        identity = {
            "acceptance_hash": self.proof["acceptance_hash"],
            "execution_surface_hash": self.proof["execution_surface_hash"],
        }
        merged, partition, carrier = manager._plan_review_reuse(
            task, self.plan_v2, identity,
        )
        self.assertIsNotNone(carrier)
        self.assertIsNotNone(partition)
        self.assertTrue(merged.batches[0].batch_id.startswith("REUSED-"))
        residual_files = {
            name for batch in merged.batches[1:] for name in batch.changed_files
        }
        self.assertEqual({"sample-service/B.java"}, residual_files)
        self.assertIn("REUSED_VALID_EVIDENCE", carrier[1].reason)
        # Scenario 17 semantics: only the affected scope is re-reviewed; the
        # reused carrier keeps atom coverage complete so PASS cannot be faked.
        pairs2 = [carrier] + [
            (batch, _result_for(batch, passed=True)) for batch in merged.batches[1:]
        ]
        aggregated = Manager._aggregate_review_batches(
            pairs2, merged, ["sample-service/A.java", "sample-service/B.java"],
        )
        self.assertEqual(REVIEW_PASS, aggregated.status)
        self.assertEqual([], aggregated.coverage["missing_files"])
        self.assertEqual(100, aggregated.coverage["coverage_percent"])
        self.assertIn("sample-service/A.java", aggregated.reviewed_files)
        self.assertIn("sample-service/B.java", aggregated.reviewed_files)
        # Residual still failing keeps the aggregate failed (no false success).
        pairs_fail = [carrier] + [
            (batch, _result_for(batch, passed=False)) for batch in merged.batches[1:]
        ]
        aggregated_fail = Manager._aggregate_review_batches(
            pairs_fail, merged, ["sample-service/A.java", "sample-service/B.java"],
        )
        self.assertEqual(REVIEW_FAIL, aggregated_fail.status)

    def test_reuse_disabled_for_high_depth_or_first_round_or_stale_inputs(self):
        manager = object.__new__(Manager)
        pairs1 = [
            (batch, _result_for(batch, passed=True)) for batch in self.plan_v1.batches
        ]
        evidence = review_evidence.record(pairs1, round_no=1, **self.proof)
        identity = {
            "acceptance_hash": self.proof["acceptance_hash"],
            "execution_surface_hash": self.proof["execution_surface_hash"],
        }
        high = SimpleNamespace(
            retry_count=1, review_policy={"context_tier": "LOCAL_IMPACT"},
            materialized_execution={"contract_revision": 1}, review_evidence=evidence,
        )
        plan, partition, carrier = manager._plan_review_reuse(high, self.plan_v2, identity)
        self.assertIsNone(carrier)
        self.assertIsNone(partition)
        self.assertIs(self.plan_v2, plan)
        first_round = SimpleNamespace(
            retry_count=0, review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1}, review_evidence=evidence,
        )
        _, _, carrier0 = manager._plan_review_reuse(first_round, self.plan_v2, identity)
        self.assertIsNone(carrier0)
        stale_inputs = dict(identity)
        stale_inputs["acceptance_hash"] = "changed-acceptance"
        stale = SimpleNamespace(
            retry_count=1, review_policy={"context_tier": "DIFF_ONLY"},
            materialized_execution={"contract_revision": 1}, review_evidence=evidence,
        )
        _, _, carrier_s = manager._plan_review_reuse(stale, self.plan_v2, stale_inputs)
        self.assertIsNone(carrier_s)

    def test_restored_checkpoint_epoch_clears_reusable_evidence(self):
        task = TaskState(task_id="T", requirement="x", worker="codex")
        task.review_evidence = {"schema": review_evidence.EVIDENCE_SCHEMA, "units": {"k": {}}}
        Manager._prepare_restored_evidence_epoch(task, "CHK-1", ["sample-service/A.java"])
        self.assertEqual({}, task.review_evidence)


class StageTimingBreakdownTests(unittest.TestCase):
    """Wave 0: derived timing categories; NOT_CAPTURED instead of estimates."""

    def test_stage_timing_from_progress_events(self):
        base = datetime(2026, 9, 10, 12, 0, 0)
        stamp = lambda minutes: (base + timedelta(minutes=minutes)).isoformat()
        task = SimpleNamespace(
            progress_events=[
                {"stage": "WORKER", "at": stamp(0), "event": "STAGE_CHANGED"},
                {"stage": "GIT", "at": stamp(10), "event": "STAGE_CHANGED"},
                {"stage": "BUILD", "at": stamp(12), "event": "STAGE_CHANGED"},
                {"stage": "REVIEW", "at": stamp(30), "event": "STAGE_CHANGED"},
                {"stage": "VERIFY", "at": stamp(40), "event": "STAGE_CHANGED"},
            ],
            ended_at=stamp(42),
            updated_at=stamp(42),
            reviewer_invocations=[
                {"started_at": stamp(30), "ended_at": stamp(40)},
            ],
            queue_wait_seconds=3600,
            admission_control_seconds=5,
        )
        timing = stage_timing(task)
        self.assertEqual(3600, timing["queue_wait_seconds"])
        self.assertEqual(5, timing["admission_control_seconds"])
        self.assertEqual(600, timing["worker_seconds"])
        self.assertEqual(120, timing["git_seconds"])
        self.assertEqual(1080, timing["build_test_seconds"])
        self.assertEqual(600, timing["review_seconds"])
        self.assertEqual(120, timing["verification_seconds"])
        self.assertEqual("NOT_CAPTURED", timing["technical_recovery_seconds"])
        self.assertEqual("NOT_CAPTURED", timing["human_wait_seconds"])

    def test_stage_timing_marks_missing_categories_not_captured(self):
        task = SimpleNamespace(
            progress_events=[], ended_at="", updated_at="",
            reviewer_invocations=[], queue_wait_seconds=-1,
            admission_control_seconds=-1,
        )
        timing = stage_timing(task)
        self.assertEqual("NOT_CAPTURED", timing["queue_wait_seconds"])
        self.assertEqual("NOT_CAPTURED", timing["admission_control_seconds"])
        self.assertEqual(0, timing["worker_seconds"])

    def test_job_admission_timing_and_human_wait(self):
        job = {
            "history": [
                {"at": "2026-09-10T12:00:00+09:00", "event": "ENQUEUED"},
                {"at": "2026-09-10T13:00:00+09:00", "event": "CLAIMED"},
                {"at": "2026-09-10T13:02:00+09:00", "event": "TASK_STARTED"},
                {"at": "2026-09-10T14:00:00+09:00", "event": "TASK_FINISHED",
                 "status": "SUCCEEDED"},
                {"at": "2026-09-10T15:00:00+09:00", "event": "SUCCESS_ACKNOWLEDGED"},
            ],
        }
        admission = job_admission_timing(job)
        self.assertEqual(3600, admission["queue_wait_seconds"])
        self.assertEqual(120, admission["admission_control_seconds"])
        lead = job_lead_time(job)
        self.assertEqual(3600, lead["queue_wait_seconds"])
        self.assertEqual(120, lead["admission_control_seconds"])
        self.assertEqual(3600, lead["human_wait_seconds"])
        self.assertEqual(10800, lead["total_lead_time_seconds"])
        pending = job_lead_time({"history": [
            {"at": "2026-09-10T12:00:00+09:00", "event": "ENQUEUED"},
        ]})
        self.assertEqual(-1, pending["human_wait_seconds"])


class TaskStateEvidenceFieldsTests(unittest.TestCase):
    def test_new_fields_persist_and_load(self):
        with harness_temp.TemporaryDirectory() as directory:
            task = TaskState(task_id="TASK-20260910-120000-abcdef01", requirement="x", worker="codex")
            task.review_evidence = {"schema": review_evidence.EVIDENCE_SCHEMA, "units": {}}
            task.queue_wait_seconds = 12
            task.admission_control_seconds = 3
            task.save(directory)
            loaded = TaskState.load(task.task_id, directory)
            self.assertEqual(task.review_evidence, loaded.review_evidence)
            self.assertEqual(12, loaded.queue_wait_seconds)
            self.assertEqual(3, loaded.admission_control_seconds)


if __name__ == "__main__":
    unittest.main()
