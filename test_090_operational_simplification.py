from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import harness_temp as tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from build_test_runner import BuildTestRunner
from attempt_checkpoint import AttemptCheckpointStore
from chatgpt_control import ChatGPTControlService
from execution_evidence import build_execution_summary, metric_calculability
from git_collector import GitCollector
from harness_service import HarnessService
from job_contract import JobContractError, JobRequest
from job_queue import AWAITING_QA, SUCCEEDED, JobStore, QueueError
from manager import Manager
from policy_catalog import (
    HARNESS_CAUSED,
    NON_OVERRIDABLE_POLICY_REFS,
    REVIEWER_CODE_FINDING,
    classify_failure_origin,
    resolve_effective_policy,
    worker_remediation_allowed,
)
from progress_projection import (
    HEARTBEAT_SECONDS,
    meaningful_progress_update,
    project_execution_health,
    queue_progress_snapshot,
    render_progress,
    should_emit,
)
from qa_quarantine import CandidateError, QACandidateStore
from queue_supervisor import QueueSupervisor
from reporter import Reporter
from review_policy import CRITICAL, HIGH, LOW, MID, assess_review_risk
from supervisor_proposal import SupervisorGateError, frozen_policy, invocation_plan
from run import create_task
from task_state import TaskState
from telegram_remote import ControlPlaneTelegramRemote, RemoteCommand
from worker import TimeoutConfig, _run_with_activity_timeout, build_prompt
from project_profile import select_project_profile


def _context() -> dict[str, object]:
    return {
        "profile_id": "test",
        "profile_schema_version": 1,
        "profile_manifest_sha256": "a" * 64,
        "profile_snapshot_sha256": "b" * 64,
        "policy_snapshot_sha256": "c" * 64,
        "workspace_identity_sha256": "d" * 64,
    }


def _job(
    client: str,
    *,
    depends_on: tuple[str, ...] = (),
    qa: bool = False,
    target: str | None = None,
) -> JobRequest:
    return JobRequest.from_mapping({
        "client_job_id": client,
        "requirement": f"{client} 구현",
        "worker": "codex",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "medium",
        "target_modules": ["app"],
        "target_resources": [target or f"app/{client}.txt"],
        "max_outer_attempts": 1,
        "depends_on": list(depends_on),
        **({
            "qa_type": "HUMAN_ACCEPTANCE",
            "hold_scope": "JOB",
            "machine_verified": True,
        } if qa else {}),
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
        "worktree_disposition": "PRESERVED",
        "changed_files": ["app/change.txt"],
    }


class PolicyAndScope090Test(unittest.TestCase):
    def test_effective_policy_keeps_hard_defaults_and_versions_strict_overlay(self):
        effective = resolve_effective_policy(
            profile_id="e4u",
            policy_snapshot_sha256="a" * 64,
            job_policy_refs=("JOB.A06.CONTRACT",),
            policy_overlays=("REVIEW_STRICT",),
        )
        self.assertTrue(set(NON_OVERRIDABLE_POLICY_REFS).issubset(
            effective["effective_policy_refs"]
        ))
        self.assertIn("JOB.A06.CONTRACT", effective["effective_policy_refs"])
        self.assertEqual(["REVIEW_STRICT"], effective["policy_overlays"])
        self.assertEqual(64, len(effective["effective_policy_sha256"]))

    def test_job_policy_overlay_is_additive_and_unknown_overlay_is_rejected(self):
        base = {
            "requirement": "read-only analysis",
            "worker": "codex",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "medium",
            "target_modules": ["app"],
            "target_resources": ["app/A.txt"],
            "max_outer_attempts": 1,
        }
        request = JobRequest.from_mapping(
            {**base, "policy_overlays": ["ANALYSIS_READONLY"]},
            allowed_modules=("app",),
        )
        self.assertEqual(("ANALYSIS_READONLY",), request.policy_overlays)
        with self.assertRaisesRegex(JobContractError, "POLICY_OVERLAY_NOT_ALLOWED"):
            JobRequest.from_mapping(
                {**base, "policy_overlays": ["ALLOW_FORCE_PUSH"]},
                allowed_modules=("app",),
            )
    def test_profile_prompt_keeps_job_intent_and_records_diet_metrics(self):
        profile = select_project_profile(
            Path(r"D:\inha_dev"), r"D:\agents\profiles\e4u"
        )
        requirement = "A06 상담 통계 카드 조건을 현재 계약대로 수정한다."
        task = TaskState(
            task_id="TASK-PROMPT-DIET",
            requirement=requirement,
            working_dir=r"D:\inha_dev",
            task_mode="MODIFICATION",
            target_module=["suc", "sucui"],
        )
        with tempfile.TemporaryDirectory() as temp:
            prompt = build_prompt(
                requirement,
                task.target_module,
                task.task_mode,
                task.task_id,
                profile=profile,
                role="codex",
                policy_root=Path(r"D:\agents"),
                task=task,
                manifest_root=Path(temp),
            )
        self.assertIn(requirement, prompt)
        self.assertIn("[Harness policy references]", prompt)
        self.assertEqual(len(prompt), task.prompt_metrics["effective_prompt"]["chars"])
        self.assertEqual("NOT_AVAILABLE", task.prompt_metrics["execution_usage"])
        self.assertTrue(task.policy_refs)
        self.assertEqual(64, len(task.prompt_metrics["stable_context_sha256"]))
        self.assertEqual(64, len(task.prompt_metrics["dynamic_context_sha256"]))
        self.assertGreater(task.prompt_metrics["stable_resource_count"], 0)
        self.assertGreater(task.prompt_metrics["dynamic_resource_count"], 0)

    def test_regression_09_001_requirement_path_does_not_widen_scope(self):
        task = create_task(
            requirement="suc/s.java 수정; 안전 참고 capui/quasar.config.cjs",
            worker_type="codex",
            droid_model="",
            codex_model="gpt-5.6-sol",
            target_module=["suc", "sucui"],
            working_dir="D:/fixture",
            task_id="TASK-REGRESSION-09-001",
        )
        self.assertEqual(["suc", "sucui"], task.target_module)
        self.assertEqual("JOB_CONTRACT", task.scope_authority)
        self.assertFalse(task.scope_provenance["requirement_inference_used"])

    def test_legacy_empty_scope_infers_and_records_provenance(self):
        task = create_task(
            requirement="sucui/src/pages/A.vue 수정",
            worker_type="codex",
            droid_model="",
            codex_model="gpt-5.6-sol",
            target_module=[],
            working_dir="D:/fixture",
            task_id="TASK-LEGACY-090",
        )
        self.assertEqual(["sucui"], task.target_module)
        self.assertEqual("LEGACY_INFERRED_SCOPE", task.scope_authority)
        self.assertTrue(task.scope_provenance["requirement_inference_used"])

    def test_authoritative_build_scope_ignores_unrelated_changed_module(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for module in ("suc", "sucui", "capui"):
                (root / module).mkdir()
                (root / module / "package.json").write_text(
                    '{"scripts":{"build":"echo ok"}}', encoding="utf-8"
                )
            runner = BuildTestRunner(str(root))
            targets = runner._detect_build_targets(
                ["capui/quasar.config.cjs"], ["suc", "sucui"], True
            )
            self.assertEqual(["suc", "sucui"], [item[0] for item in targets])

    def test_harness_and_reviewer_origins_route_differently(self):
        self.assertEqual(
            HARNESS_CAUSED,
            classify_failure_origin("UNRELATED_MODULE_BUILD_FAILURE"),
        )
        self.assertFalse(worker_remediation_allowed(HARNESS_CAUSED))
        origin = classify_failure_origin(
            "REVIEW_FAILED", failure_stage="REVIEW", failure_type="review"
        )
        self.assertEqual(REVIEWER_CODE_FINDING, origin)
        self.assertTrue(worker_remediation_allowed(origin, "REVIEW_FAILED"))


class ReviewPolicy090Test(unittest.TestCase):
    def _risk(self, **overrides):
        values = {
            "requirement": "label text change",
            "target_modules": ["app"],
            "changed_files": ["app/A.vue"],
            "git_diff": "+label",
            "build_status": "PASS",
            "test_status": "PASS",
            "test_required": False,
        }
        values.update(overrides)
        return assess_review_risk(**values)

    def test_low_mid_high_critical_mapping_is_deterministic(self):
        low = self._risk()
        mid = self._risk(
            requirement="public controller api contract",
            target_modules=["app", "web"],
            changed_files=["app/A.java", "web/A.vue"],
        )
        high = self._risk(requirement="authorization permission check")
        critical = self._risk(requirement="unknown contract 협의 필요")
        self.assertEqual((LOW, "gpt-5.6-luna", "DIFF_ONLY"), (low.risk_level, low.model, low.context_tier))
        self.assertEqual(MID, mid.risk_level)
        self.assertEqual((HIGH, "LOCAL_IMPACT"), (high.risk_level, high.context_tier))
        self.assertEqual((CRITICAL, "DOMAIN"), (critical.risk_level, critical.context_tier))
        self.assertEqual(low, self._risk())

    def test_unavailable_model_does_not_silently_fallback(self):
        with self.assertRaisesRegex(ValueError, "REVIEW_MODEL_TIER_UNAVAILABLE"):
            self._risk(available_models=("gpt-5.6-sol",))

    def test_hard_flag_records_computed_and_effective_risk_without_score_faking(self):
        decision = self._risk(
            requirement="authorization permission check",
            changed_files=[],
            git_diff="",
        )
        self.assertEqual(LOW, decision.computed_risk_level)
        self.assertEqual(HIGH, decision.effective_risk_level)
        self.assertEqual(HIGH, decision.risk_level)
        self.assertIn("AUTHORIZATION_OR_PII", decision.override_reasons)
        strict = self._risk(policy_overlays=("REVIEW_STRICT",))
        self.assertEqual(LOW, strict.computed_risk_level)
        self.assertEqual(HIGH, strict.effective_risk_level)
        migration = self._risk(policy_overlays=("DB_MIGRATION_STRICT",))
        self.assertEqual(CRITICAL, migration.effective_risk_level)

    def test_prohibited_database_work_is_not_positive_risk_evidence(self):
        for requirement in (
            "no SQL/DDL/DML",
            "no DB schema changes",
            "DB schema 변경 금지",
            "NEVER modify migration files",
            "Keep the existing API contract. No SQL/DDL/DML or core changes. Reorder UI labels.",
        ):
            with self.subTest(requirement=requirement):
                decision = self._risk(requirement=requirement)
                self.assertNotIn("DB_SCHEMA_OR_MIGRATION", decision.hard_flags)
                self.assertEqual(LOW, decision.risk_level)

    def test_actual_migration_work_and_strict_override_remain_hard_risk(self):
        for requirement in ("add migration", "alter table jobs", "create index idx_jobs", "modify schema",
                            "Do not alter schema but add migration",
                            "DB schema 변경 금지 하지만 create index"):
            with self.subTest(requirement=requirement):
                self.assertIn("DB_SCHEMA_OR_MIGRATION", self._risk(requirement=requirement).hard_flags)
        self.assertIn("DB_SCHEMA_OR_MIGRATION", self._risk(
            requirement="no database changes", changed_files=["db/migrations/V2__jobs.sql"]
        ).hard_flags)
        self.assertIn("DB_SCHEMA_OR_MIGRATION", self._risk(
            requirement="UI labels", git_diff="diff --git a/db/V2.sql b/db/V2.sql\n+CREATE INDEX idx_jobs ON jobs(id)"
        ).hard_flags)
        self.assertIn("DB_SCHEMA_OR_MIGRATION", self._risk(
            requirement="no SQL changes; add migration"
        ).hard_flags)
        self.assertNotIn("DB_SCHEMA_OR_MIGRATION", self._risk(
            requirement="document the UI", changed_files=["docs/migration.md"]
        ).hard_flags)
        self.assertEqual(CRITICAL, self._risk(
            requirement="no SQL/DDL/DML", policy_overlays=("DB_MIGRATION_STRICT",)
        ).risk_level)


class SupervisorFastPathTest(unittest.TestCase):
    def test_prepared_low_mid_skip_supervisor_at_admission(self):
        for risk in (LOW, MID):
            with self.subTest(risk=risk):
                plan = invocation_plan(risk, prepared=True, boundary="ADMISSION")
                self.assertEqual("OFF", plan["planner"])
                self.assertFalse(plan["supervisor_required"])

    def test_high_critical_and_actual_triggers_require_supervisor(self):
        for risk in (HIGH, CRITICAL):
            with self.subTest(risk=risk):
                self.assertTrue(invocation_plan(risk, prepared=True, boundary="ADMISSION")["supervisor_required"])
        self.assertTrue(invocation_plan(MID, prepared=True, exception="AMBIGUITY")["supervisor_required"])
        self.assertTrue(invocation_plan(LOW, prepared=True, required=True)["supervisor_required"])
        self.assertTrue(invocation_plan(CRITICAL, prepared=True)["human_gate_required"])

    def test_explicit_contract_overlay_is_frozen(self):
        policy = {"resolved_reviewer": {"risk_level": MID},
                  "policy_overlays": ["SUPERVISOR_REQUIRED"]}
        self.assertTrue(frozen_policy(policy)["supervisor_required"])

    def test_transport_failure_preserves_primary_lineage_without_quarantine(self):
        manager = object.__new__(Manager)
        def fail(**_kwargs):
            raise SupervisorGateError("SUPERVISOR_TRANSPORT_DECODE_FAILED")
        task = SimpleNamespace(
            task_id="TASK-1", execution_id="EXEC-1", candidate_id="CAND-1",
            effective_policy={"supervisor_policy": {"policy_version": "test"}},
            _supervisor_boundary=fail, changed_files=["app/a.py"],
        )
        result = manager._supervisor_gate(task)
        self.assertFalse(result.success)
        self.assertEqual("SUPERVISOR_TRANSPORT_DECODE_FAILED", task.failure_code)
        self.assertEqual({}, task.qa_request)
        self.assertEqual("RUNTIME_RECOVERY", task.retry_domain)
        self.assertEqual(("TASK-1", "EXEC-1", "CAND-1"),
                         (task.task_id, task.execution_id, task.candidate_id))
        supervisor = QueueSupervisor(SimpleNamespace(), SimpleNamespace(task_root="missing"))
        supervisor._single_claim = object()
        supervisor.canonical_promoter = SimpleNamespace(quarantine=lambda *_: self.fail("quarantine called"))
        projected = {"status": "AWAITING_QA", "failure_code": task.failure_code,
                     "task_id": task.task_id, "execution_id": task.execution_id,
                     "candidate_id": task.candidate_id, "qa_request": task.qa_request}
        self.assertIs(projected, supervisor._quarantine_machine_verified_result(
            {}, projected, SimpleNamespace()))

    def test_secondary_machine_gate_failure_cannot_replace_primary_failure(self):
        supervisor = QueueSupervisor(SimpleNamespace(), SimpleNamespace(task_root="missing"))
        supervisor._single_claim = object()
        def unavailable(*_args):
            raise CandidateError("QA_MACHINE_GATES_REQUIRED")
        supervisor.canonical_promoter = SimpleNamespace(quarantine=unavailable)
        task = SimpleNamespace(qa_type="", hold_scope="", machine_verified=False, qa_request={})
        result = {"status": "AWAITING_QA", "failure_code": "SUPERVISOR_PROVIDER_UNAVAILABLE",
                  "task_id": "TASK-1", "execution_id": "EXEC-1", "candidate_id": "CAND-1",
                  "qa_request": {"required": True, "qa_type": "HARNESS_RECOVERY"}}
        with patch.object(TaskState, "load", return_value=task), \
             patch.object(JobStore, "_is_strict_batch_result", return_value=True):
            preserved = supervisor._quarantine_machine_verified_result({}, result, SimpleNamespace())
        self.assertEqual(result["failure_code"], preserved["failure_code"])
        self.assertEqual(["QA_MACHINE_GATES_REQUIRED"], preserved["secondary_failures"])
        self.assertEqual(("TASK-1", "EXEC-1", "CAND-1"),
                         (preserved["task_id"], preserved["execution_id"], preserved["candidate_id"]))


class Progress090Test(unittest.TestCase):
    def test_progress_revision_is_independent_from_queue_revision(self):
        task = TaskState(
            task_id="TASK-PROGRESS",
            requirement="progress",
            job_id="JOB-PROGRESS",
            client_job_id="A06",
            status="RUNNING",
            started_at="2026-09-01T00:00:00+00:00",
        )
        first = meaningful_progress_update(
            task,
            stage="WORKER",
            event_type="JOB_STARTED",
            summary="Worker start",
            clock=lambda: datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        duplicate = meaningful_progress_update(
            task,
            stage="WORKER",
            event_type="JOB_STARTED",
            summary="Worker start",
            clock=lambda: datetime(2026, 9, 1, 0, 1, tzinfo=timezone.utc),
        )
        projected = queue_progress_snapshot({"revision": 77}, [], duplicate)
        self.assertEqual(1, first["progress_revision"])
        self.assertEqual(1, duplicate["progress_revision"])
        self.assertEqual(77, projected["queue_revision"])

    def test_silence_and_fake_clock_heartbeat(self):
        snapshot = {"progress_revision": 3, "job": {"status": "RUNNING"}, "execution_health": {"runtime_health": "VALID"}}
        now = datetime(2026, 9, 1, tzinfo=timezone.utc)
        self.assertEqual(
            (False, "SILENT"),
            should_emit(snapshot, snapshot, now=now, last_emitted_at=now),
        )
        self.assertEqual(
            (True, "HEARTBEAT"),
            should_emit(
                snapshot,
                snapshot,
                now=now + timedelta(seconds=HEARTBEAT_SECONDS),
                last_emitted_at=now,
            ),
        )

    def test_control_reads_live_task_progress_without_job_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            task = TaskState(task_id="TASK-LIVE", requirement="live", progress_revision=9)
            task.progress_snapshot = {"progress_revision": 9, "stage": {"name": "BUILD"}}
            task.save(temp)
            service = object.__new__(ChatGPTControlService)
            service.configuration = {"task_root": temp}
            result = service._task_progress({
                "current_claim_task_id": "TASK-LIVE",
                "last_result": {"progress_revision": 2},
            })
            self.assertEqual(9, result["progress_revision"])


class StatusProjection090Test(unittest.TestCase):
    @staticmethod
    def _batch_jobs() -> list[dict[str, object]]:
        current = [
            {
                "job_id": f"JOB-A-{sequence}",
                "client_job_id": client,
                "batch_id": "BATCH-A-R2",
                "batch_label": "A-R2",
                "sequence": sequence,
                "status": status,
            }
            for sequence, client, status in (
                (1, "A06", "SUCCEEDED"),
                (2, "A07", "SUCCEEDED"),
                (3, "A10", "SUCCEEDED"),
                (4, "A01", "SUCCEEDED"),
                (5, "A02", "AWAITING_QA"),
                (6, "A08", "CANCELLED"),
            )
        ]
        previous = [
            {
                "job_id": f"JOB-OLD-{index}",
                "client_job_id": f"OLD-{index}",
                "batch_id": "BATCH-OLD",
                "sequence": index,
                "status": "SUCCEEDED",
            }
            for index in range(1, 80)
        ]
        return previous + current

    def test_current_batch_progress_ignores_history_and_previous_batches(self):
        snapshot = queue_progress_snapshot(
            {
                "revision": 173,
                "history_total": 85,
                "blocked_by_job_id": "JOB-A-5",
                "running_job_id": "",
                "paused": True,
                "gate_reason": "AWAITING_QA",
                "execution_mode": "ATTENDED",
            },
            self._batch_jobs(),
        )
        self.assertEqual("BATCH-A-R2", snapshot["batch"]["id"])
        self.assertEqual("A-R2", snapshot["batch"]["label"])
        self.assertEqual(5, snapshot["batch"]["current"])
        self.assertEqual(6, snapshot["batch"]["total"])
        self.assertEqual(4, snapshot["batch"]["completed"])
        self.assertEqual(1, snapshot["batch"]["qa"])
        self.assertEqual(0, snapshot["batch"]["quarantined"])
        self.assertEqual(0, snapshot["batch"]["blocked"])

    def test_current_batch_category_counts_do_not_leak_other_batches(self):
        jobs = [
            {"job_id": "OLD", "batch_id": "BATCH-OLD", "sequence": 1,
             "status": "BLOCKED_BY_DEPENDENCY", "candidate_id": "OLD-CANDIDATE"},
            {"job_id": "J1", "batch_id": "BATCH-NOW", "sequence": 1,
             "status": "SUCCEEDED"},
            {"job_id": "J2", "batch_id": "BATCH-NOW", "sequence": 2,
             "status": "AWAITING_QA"},
            {"job_id": "J3", "batch_id": "BATCH-NOW", "sequence": 3,
             "status": "QUARANTINED", "candidate_id": "CANDIDATE-NOW"},
            {"job_id": "J4", "batch_id": "BATCH-NOW", "sequence": 4,
             "status": "BLOCKED_BY_DEPENDENCY"},
            {"job_id": "J5", "batch_id": "BATCH-NOW", "sequence": 5,
             "status": "CANCELLED"},
            {"job_id": "J6", "batch_id": "BATCH-NOW", "sequence": 6,
             "status": "QUEUED"},
        ]
        snapshot = queue_progress_snapshot(
            {"revision": 8, "blocked_by_job_id": "J2"}, jobs
        )
        self.assertEqual(1, snapshot["batch"]["completed"])
        self.assertEqual(2, snapshot["batch"]["qa"])
        self.assertEqual(1, snapshot["batch"]["quarantined"])
        self.assertEqual(1, snapshot["batch"]["blocked"])

    def test_running_reviewer_fixture_renders_user_stage_and_attempt(self):
        task = {
            "progress_revision": 9,
            "job": {"id": "J3", "client_job_id": "A10", "elapsed_seconds": 37 * 60},
            "stage": {
                "name": "REVIEW", "index": 4, "total": 5,
                "reviewer_attempt": 1, "reviewer_attempt_limit": 2,
            },
            "gates": {
                "worker": "PASS", "build": "PASS", "test": "PASS",
                "review": "RUNNING", "verification": "PENDING",
            },
            "last_meaningful_event": {"summary": "Reviewer 실행 중"},
            "next": {"stage": "VERIFY"},
            "user_action_required": False,
        }
        snapshot = queue_progress_snapshot(
            {
                "revision": 77, "running_job_id": "J3", "paused": False,
                "gate_reason": "", "execution_mode": "AUTONOMOUS",
            },
            [
                {"job_id": f"J{i}", "client_job_id": "A10" if i == 3 else f"A{i}",
                 "batch_id": "BATCH-A-R2", "batch_label": "A-R2",
                 "sequence": i, "status": "RUNNING" if i == 3 else "QUEUED"}
                for i in range(1, 7)
            ],
            task,
        )
        snapshot["harness"] = {"version": "0.9.0", "profile_id": "e4u"}
        text = render_progress(snapshot, "USER")
        self.assertIn("🟢 E4U Harness 0.9.0", text)
        self.assertIn("A-R2 · 3/6", text)
        self.assertIn("A10", text)
        self.assertIn(
            "[✓ WORKER] [✓ BUILD] [✓ TEST] [● REVIEW] [○ VERIFY]", text
        )
        self.assertIn("현재: Reviewer 1/2", text)
        self.assertIn("경과: 37분", text)
        self.assertIn("다음: Verification", text)
        self.assertIn("사용자 개입: 필요 없음", text)

    def test_paused_qa_user_view_is_human_readable_and_noise_free(self):
        snapshot = queue_progress_snapshot(
            {
                "revision": 173, "history_total": 85,
                "blocked_by_job_id": "JOB-A-5", "running_job_id": "",
                "paused": True, "gate_reason": "AWAITING_QA",
                "execution_mode": "ATTENDED",
                "active_baseline_id": "BASELINE-SECRET-NOISE",
                "integrity_snapshot": {"predecessor_hashes": {"x": "f" * 64}},
                "task_owned_baseline_files": ["private/path.txt"],
                "final_staging_files": ["private/stage.txt"],
            },
            self._batch_jobs(),
        )
        snapshot["harness"] = {"version": "0.9.0", "profile_id": "e4u"}
        text = render_progress(snapshot, "USER")
        self.assertIn("현재 상태: QA 대기", text)
        self.assertIn("실행 중 Job: 없음", text)
        self.assertIn("✅ 완료: 4", text)
        self.assertIn("🧑 QA 대기: 1", text)
        self.assertIn("⏸ 기타: 1", text)
        self.assertIn("실행 모드: ATTENDED · PAUSED", text)
        self.assertIn("사용자 개입: 필요", text)
        for noise in (
            "BASELINE-SECRET-NOISE", "predecessor_hashes", "private/path.txt",
            "private/stage.txt", "schema_version", "history_total",
        ):
            self.assertNotIn(noise, text)
        self.assertFalse(text.lstrip().startswith("{"))

    def test_telegram_status_uses_the_same_canonical_user_projection(self):
        progress = {
            "harness": {"version": "0.9.0", "profile_id": "e4u"},
            "batch": {"id": "BATCH-A-R2", "label": "A-R2", "current": 1,
                      "total": 6, "completed": 0, "qa": 1,
                      "quarantined": 0, "blocked": 0},
            "queue": {"execution_mode": "ATTENDED", "paused": True,
                      "gate_reason": "AWAITING_QA", "running_job_id": ""},
            "job": {"client_job_id": "A06", "elapsed_seconds": 0},
            "stage": {"name": "QA"},
            "gates": {}, "next": {}, "user_action_required": True,
        }

        class Control:
            def get_queue(self):
                return {"queue": {"revision": 173, "progress": progress}}

        remote = ControlPlaneTelegramRemote(Control(), allowed_user_ids=[7])
        sent: list[str] = []
        remote.send_message = lambda _chat, text: sent.append(text) or True
        remote.handle(RemoteCommand(1, 7, "status", "", "/status"))
        self.assertEqual([render_progress(progress, "TELEGRAM")], sent)
        self.assertNotIn('"queue"', sent[0])

    def test_report_current_keeps_operator_job_information(self):
        class Control:
            def get_queue(self):
                return {"queue": {"blocked_by_job_id": "JOB-A06"}}

            def get_job(self, job_id):
                return {
                    "job_id": job_id, "status": "AWAITING_QA",
                    "task_ids": ["TASK-A06"],
                    "last_result": {"failure_code": "FIXTURE_FAILURE",
                                    "report_path": "fixture-report.md"},
                    "progress": {"progress_revision": 4},
                }

        remote = ControlPlaneTelegramRemote(Control(), allowed_user_ids=[7])
        sent: list[str] = []
        remote.send_message = lambda _chat, text: sent.append(text) or True
        remote.handle(RemoteCommand(1, 7, "report", "current", "/report current"))
        payload = json.loads(sent[0])
        self.assertEqual("JOB-A06", payload["job_id"])
        self.assertEqual(["TASK-A06"], payload["task_ids"])
        self.assertEqual("fixture-report.md", payload["report_path"])


class TelegramBoundary090Test(unittest.TestCase):
    class Control:
        def __init__(self):
            self.calls = []

        def get_queue(self):
            return {"queue": {"revision": 5, "progress": {"progress_revision": 2}}}

        def list_jobs(self, limit=200):
            return {"jobs": [], "queue": {"progress": {}}}

        def pause_queue(self, reason, **kwargs):
            self.calls.append(("pause", reason, kwargs))
            return {"ok": True}

    def _remote(self):
        control = self.Control()
        remote = ControlPlaneTelegramRemote(control, allowed_user_ids=[7])
        sent = []
        remote.send_message = lambda chat, text: sent.append((chat, text)) or True
        return control, remote, sent

    def test_forbidden_command_is_not_dispatched(self):
        control, remote, sent = self._remote()
        remote.handle(RemoteCommand(1, 7, "commit", "", "/commit"))
        self.assertEqual([], control.calls)
        self.assertIn("허용되지", sent[-1][1])

    def test_mutation_requires_revision_request_and_confirmation(self):
        control, remote, sent = self._remote()
        remote.handle(RemoteCommand(1, 7, "pause", "{}", "/pause {}"))
        self.assertEqual([], control.calls)
        self.assertIn("MISSING_OPERATOR_PROVENANCE", sent[-1][1])
        payload = json.dumps({
            "expected_revision": 5,
            "request_id": "telegram-pause-1",
            "confirmation": "I_CONFIRM_PAUSE_QUEUE",
            "reason": "operator",
        })
        remote.handle(RemoteCommand(1, 7, "pause", payload, "/pause"))
        self.assertEqual("pause", control.calls[-1][0])
        self.assertEqual(5, control.calls[-1][2]["expected_revision"])


class DependencyAndQA090Test(unittest.TestCase):
    def test_ten_job_semantic_dependency_is_distinct_from_same_file_write_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            requests = [
                _job("J1"), _job("J2"), _job("J3"),
                _job("J4", qa=True, target="app/decision.txt"),
                _job("J5"), _job("J6"), _job("J7"), _job("J8"),
                _job("J9", depends_on=("J4",), target="app/downstream.txt"),
                _job("J10", target="app/decision.txt"),
            ]
            jobs, _ = store.enqueue_batch(
                "semantic-ten-090", requests, execution_context=_context()
            )
            owner = "owner-semantic-090"
            for expected in ("J1", "J2", "J3"):
                current = store.claim_next(owner)
                self.assertEqual(expected, current["client_job_id"])
                task_id = "TASK-SEM-" + expected
                store.mark_task_started(current["job_id"], owner, task_id)
                current = store.record_result(current["job_id"], owner, _success(task_id))
                store.acknowledge_success(
                    current["job_id"], "ack-sem-" + expected,
                    expected_revision=current["revision"], batch_id=current["batch_id"],
                )
            qa = store.claim_next(owner)
            self.assertEqual("J4", qa["client_job_id"])
            store.mark_task_started(qa["job_id"], owner, "TASK-SEM-J4")
            store.record_result(qa["job_id"], owner, {
                **_success("TASK-SEM-J4"),
                "status": AWAITING_QA,
                "success": False,
                "qa_type": "HUMAN_ACCEPTANCE",
                "hold_scope": "JOB",
                "machine_verified": True,
                "candidate_id": "CAND-" + "C" * 24,
                "candidate_manifest": "candidate.json",
                "changed_files": ["app/decision.txt"],
                "worktree_disposition": "CLEAN_ROLLBACK",
            })
            for expected in ("J5", "J6", "J7", "J8"):
                current = store.claim_next(owner)
                self.assertEqual(expected, current["client_job_id"])
                task_id = "TASK-SEM-" + expected
                store.mark_task_started(current["job_id"], owner, task_id)
                current = store.record_result(current["job_id"], owner, _success(task_id))
                store.acknowledge_success(
                    current["job_id"], "ack-sem-" + expected,
                    expected_revision=current["revision"], batch_id=current["batch_id"],
                )
            dependent = store.claim_next(owner)
            self.assertEqual("J9", dependent["client_job_id"])
            self.assertEqual(["CAND-" + "C" * 24], dependent["speculative_candidate_ids"])
            held = store.get_job(qa["job_id"])
            self.assertEqual(AWAITING_QA, held["status"])
            self.assertEqual("CAND-" + "C" * 24, held["candidate_id"])
            j10 = store.get_job(next(job["job_id"] for job in jobs if job["client_job_id"] == "J10"))
            conflict = store._quarantined_scope_conflict(store._read_queue(), j10)
            self.assertEqual(qa["job_id"], conflict)

    def test_review_only_result_is_revision_safe_idempotent_and_adds_no_worker_attempt(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "review-only-090", [_job("J1")], execution_context=_context()
            )
            owner = "owner-review"
            running = store.claim_next(owner)
            store.mark_task_started(running["job_id"], owner, "TASK-REVIEW")
            awaiting = store.record_result(running["job_id"], owner, {
                "task_id": "TASK-REVIEW",
                "status": AWAITING_QA,
                "stage": "AWAITING_QA",
                "success": False,
                "build_status": "PASS",
                "test_status": "NOT_REQUIRED",
                "review_status": "REVIEW_ERROR",
                "verification_status": "PARTIAL",
                "failure_stage": "REVIEW",
                "failure_code": "REVIEW_OUTPUT_PARSE_FAILED",
                "failure_origin": "TOOL_INFRA",
                "worktree_disposition": "PRESERVED",
                "changed_files": ["app/change.txt"],
            })
            attempts = list(awaiting["attempts"])
            task_ids = list(awaiting["task_ids"])
            expected_revision = awaiting["revision"]
            passed = _success("TASK-REVIEW")
            updated, replayed = store.record_review_only_result(
                awaiting["job_id"],
                expected_revision=expected_revision,
                request_id="review-only-request-1",
                result=passed,
            )
            self.assertFalse(replayed)
            self.assertEqual(SUCCEEDED, updated["status"])
            self.assertEqual(attempts, updated["attempts"])
            self.assertEqual(task_ids, updated["task_ids"])
            action = next(iter(updated["review_only_actions"].values()))
            self.assertFalse(action["worker_invoked"])
            replayed_job, replayed = store.record_review_only_result(
                awaiting["job_id"],
                expected_revision=expected_revision,
                request_id="review-only-request-1",
                result=passed,
            )
            self.assertTrue(replayed)
            self.assertEqual(updated["revision"], replayed_job["revision"])

    def test_pause_resume_mutations_use_revision_confirmation_and_idempotency(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            revision = store.queue_snapshot()["revision"]
            store.pause(
                "operator",
                expected_revision=revision,
                request_id="pause-090-1",
                confirmation="I_CONFIRM_PAUSE_QUEUE",
            )
            paused = store.queue_snapshot()
            self.assertTrue(paused["operator_paused"])
            store.pause(
                "operator",
                expected_revision=revision,
                request_id="pause-090-1",
                confirmation="I_CONFIRM_PAUSE_QUEUE",
            )
            self.assertEqual(paused["revision"], store.queue_snapshot()["revision"])
            with self.assertRaisesRegex(QueueError, "QUEUE_REVISION_CONFLICT"):
                store.resume_queue(
                    expected_revision=revision,
                    request_id="resume-stale-090",
                    confirmation="I_CONFIRM_RESUME_QUEUE",
                )
            store.resume_queue(
                expected_revision=paused["revision"],
                request_id="resume-090-1",
                confirmation="I_CONFIRM_RESUME_QUEUE",
            )
            self.assertFalse(store.queue_snapshot()["operator_paused"])

    def test_eight_job_scenario_skips_job_qa_and_reaches_speculative_dependency(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            requests = [
                _job("J1"), _job("J2", qa=True), _job("J3"), _job("J4"),
                _job("J5"), _job("J6"), _job("J7", depends_on=("J2",)), _job("J8"),
            ]
            jobs, _ = store.enqueue_batch("scenario-090", requests, execution_context=_context())
            owner = "owner-090"
            first = store.claim_next(owner)
            store.mark_task_started(first["job_id"], owner, "TASK-J1")
            first = store.record_result(first["job_id"], owner, _success("TASK-J1"))
            store.acknowledge_success(
                first["job_id"], "ack-J1",
                expected_revision=first["revision"], batch_id=first["batch_id"],
            )

            second = store.claim_next(owner)
            self.assertEqual("J2", second["client_job_id"])
            store.mark_task_started(second["job_id"], owner, "TASK-J2")
            qa_result = {
                **_success("TASK-J2"),
                "status": AWAITING_QA,
                "success": False,
                "qa_type": "HUMAN_ACCEPTANCE",
                "hold_scope": "JOB",
                "machine_verified": True,
                "candidate_id": "CAND-" + "A" * 24,
                "candidate_manifest": "candidate.json",
                "worktree_disposition": "CLEAN_ROLLBACK",
            }
            store.record_result(second["job_id"], owner, qa_result)
            self.assertFalse(store.queue_snapshot()["paused"])

            for expected in ("J3", "J4", "J5", "J6"):
                current = store.claim_next(owner)
                self.assertEqual(expected, current["client_job_id"])
                task_id = "TASK-" + expected
                store.mark_task_started(current["job_id"], owner, task_id)
                current = store.record_result(current["job_id"], owner, _success(task_id))
                store.acknowledge_success(
                    current["job_id"], "ack-" + expected,
                    expected_revision=current["revision"], batch_id=current["batch_id"],
                )
            speculative = store.claim_next(owner)
            self.assertEqual("J7", speculative["client_job_id"])
            self.assertEqual(["CAND-" + "A" * 24], speculative["speculative_candidate_ids"])


class CandidateAndEvidence090Test(unittest.TestCase):
    def _candidate(self, root: Path):
        workspace = root / "workspace"
        state = root / "state"
        snapshots = state / "snapshots" / "SNAP-ONE"
        files = snapshots / "files"
        workspace.mkdir()
        files.mkdir(parents=True)
        target = workspace / "app.txt"
        target.write_bytes(b"base\n")
        (files / "app.txt").write_bytes(b"base\n")
        manifest = snapshots / "manifest.json"
        manifest.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT",
            "files": [{
                "path": "app.txt",
                "state": "PRESENT",
                "sha256": hashlib.sha256(b"base\n").hexdigest(),
                "snapshot": "files/app.txt",
            }],
        }), encoding="utf-8")
        target.write_bytes(b"candidate\n")
        task = SimpleNamespace(
            task_owned_changed_files=["app.txt"], changed_files=["app.txt"],
            pre_job_snapshot_manifest=str(manifest), pre_job_snapshot_id="SNAP-ONE",
            git_diff="-base\n+candidate", job_id="JOB-QA", task_id="TASK-QA",
            qa_type="HUMAN_ACCEPTANCE", hold_scope="JOB", machine_verified=True,
            active_baseline_id="BASELINE-ONE", build={"status": "PASS"},
            test_required=False, test_status="SKIPPED", review_status="REVIEW_PASS",
            review_coverage={"percent": 100}, verification_status="VERIFIED",
        )
        store = QACandidateStore(state / "candidates", workspace, None)
        return target, store, store.create(task)

    def test_candidate_clean_promotion_requires_full_integration_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            target, store, candidate = self._candidate(Path(temp))
            store.restore_base(candidate["candidate_id"])
            result = store.promote([candidate["candidate_id"]], lambda files: {
                "success": True,
                "build_status": "PASS",
                "test_status": "NOT_REQUIRED",
                "review_status": "REVIEW_PASS",
                "verification_status": "VERIFIED",
                "failure_code": "",
                "files": files,
            })
            self.assertEqual("PROMOTED", result["status"])
            self.assertEqual(b"candidate\n", target.read_bytes())

    def test_candidate_conflict_requires_focused_rebase_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            target, store, candidate = self._candidate(Path(temp))
            store.restore_base(candidate["candidate_id"])
            target.write_bytes(b"approved-edit\n")
            self.assertEqual(
                "FOCUSED_REBASE_REQUIRED",
                store.compatibility(candidate["candidate_id"])["decision"],
            )
            with self.assertRaisesRegex(CandidateError, "CANDIDATE_FOCUSED_REBASE_REQUIRED"):
                store.promote([candidate["candidate_id"]], lambda _files: {})
            self.assertEqual(b"approved-edit\n", target.read_bytes())

    def test_candidate_restart_load_and_integration_failure_restore_base(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target, store, candidate = self._candidate(root)
            store.restore_base(candidate["candidate_id"])
            restarted = QACandidateStore(
                root / "state" / "candidates", root / "workspace", None
            )
            self.assertEqual(
                candidate["candidate_id"],
                restarted.load(candidate["candidate_id"])["candidate_id"],
            )
            with self.assertRaisesRegex(
                CandidateError, "CANDIDATE_INTEGRATION_VERIFICATION_FAILED"
            ):
                restarted.promote([candidate["candidate_id"]], lambda _files: {
                    "success": False,
                    "build_status": "FAIL",
                    "test_status": "NOT_REQUIRED",
                    "review_status": "REVIEW_PASS",
                    "verification_status": "FAILED",
                    "failure_code": "BUILD_FAILED",
                })
            self.assertEqual(b"base\n", target.read_bytes())
            self.assertEqual("QUARANTINED", restarted.load(candidate["candidate_id"])["state"])

    def test_candidate_parent_revision_mismatch_fails_before_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target, store, candidate = self._candidate(root)
            store.restore_base(candidate["candidate_id"])
            child = dict(candidate)
            child_id = "CAND-" + "B" * 24
            child["candidate_id"] = child_id
            child["parent_candidate_id"] = candidate["candidate_id"]
            child["parent_candidate_revision"] = 999
            child["manifest_path"] = ""
            source_dir = Path(candidate["manifest_path"]).parent
            child_dir = store._candidate_dir(child_id, create=True)
            import shutil
            shutil.copytree(source_dir / "files", child_dir / "files")
            (child_dir / "candidate.patch").write_bytes(
                (source_dir / "candidate.patch").read_bytes()
            )
            store._write_manifest(
                child_dir, {k: v for k, v in child.items() if k != "manifest_path"}
            )
            with self.assertRaisesRegex(
                CandidateError, "CANDIDATE_PARENT_REVISION_MISMATCH"
            ):
                store.promote(
                    [candidate["candidate_id"], child_id],
                    lambda _files: {},
                )
            self.assertEqual(b"base\n", target.read_bytes())

    def test_execution_summary_and_markdown_are_additive_and_do_not_estimate_usage(self):
        task = TaskState(
            task_id="TASK-EVIDENCE", requirement="evidence", job_id="JOB-EVIDENCE", status="SUCCESS",
            started_at="2026-09-01T00:00:00+00:00",
            ended_at="2026-09-01T00:01:00+00:00",
            build={"status": "PASS"}, test_required=False, test_status="SKIPPED",
            review_status="REVIEW_PASS", verification_status="VERIFIED",
            attempt_history=[{"worker_execution_evidence": {"invocation_id": "W1"}}],
            reviewer_invocations=[{"ordinal": 1, "status": "REVIEW_PASS"}],
            progress_events=[{"type": "STAGE_CHANGED", "at": "2026-09-01T00:00:10+00:00", "stage": "WORKER"}],
            execution_runtime_history=[{"execution_id": "EXEC-1", "execution_role": "WORKER", "started_at": "2026-09-01T00:00:05+00:00", "ended_at": "2026-09-01T00:00:20+00:00"}],
            policy_refs=["POLICY.SCOPE.IMMUTABLE_JOB_CONTRACT"],
        )
        summary = build_execution_summary(task)
        task.execution_summary = summary
        self.assertEqual(1, summary["execution"]["worker_invocations"])
        self.assertEqual(1, summary["execution"]["reviewer_invocations"])
        self.assertEqual("NOT_AVAILABLE", summary["usage"]["cost"])
        self.assertEqual("UNRELIABLE", metric_calculability(summary)["cost"])
        self.assertGreaterEqual(len(summary["raw_lineage"]), 4)
        self.assertEqual(
            "PRESERVE_UNTIL_1_0_KNOWLEDGE_BOOTSTRAP",
            summary["retention"]["policy"],
        )
        self.assertFalse(summary["retention"]["destructive_cleanup_allowed"])
        markdown = Reporter().generate_markdown(task)
        self.assertEqual(1, markdown.count("## Execution summary"))
        self.assertIn("**Worker calls**: `1`", markdown)

    def test_legacy_task_record_loads_with_safe_defaults(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            task_id = "TASK-20260901-000000"
            day = root / "20260901"
            day.mkdir()
            (day / f"{task_id}.json").write_text(json.dumps({
                "task_id": task_id,
                "requirement": "legacy",
                "status": "SUCCESS",
                "stage": "DONE",
            }), encoding="utf-8")
            loaded = TaskState.load(task_id, root)
            self.assertEqual(0, loaded.progress_revision)
            self.assertEqual({}, loaded.execution_summary)
            self.assertEqual([], loaded.policy_refs)
            self.assertEqual([], loaded.policy_overlays)
            self.assertEqual({}, loaded.effective_policy)
            self.assertEqual({}, loaded.execution_runtime)
            self.assertEqual([], loaded.execution_runtime_history)


class RuntimeHeartbeat090Test(unittest.TestCase):
    def test_crash_salvage_service_captures_scoped_unverified_candidate_and_handoff(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            repo = workspace / "app"
            repo.mkdir(parents=True)
            subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "fixture@example.invalid"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repo, check=True)
            target = repo / "source.txt"
            target.write_text("before\n", encoding="utf-8")
            subprocess.run(["git", "add", "source.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "fixture"], cwd=repo, check=True, capture_output=True)
            task = TaskState(
                task_id="TASK-20260901-091000",
                requirement="crash salvage fixture",
                working_dir=str(workspace),
                target_module=["app"],
                target_resources=["app/source.txt"],
                job_id="JOB-SALVAGE",
                batch_id="BATCH-SALVAGE",
                queue_generation=5,
                active_baseline_id="BASELINE-SALVAGE",
                pre_job_snapshot_id="SNAP-SALVAGE",
                pre_job_snapshot_manifest="fixture-manifest.json",
                status="RUNNING",
                stage="WORKING",
                execution_runtime={
                    "execution_id": "EXEC-SALVAGE",
                    "execution_role": "WORKER",
                    "last_runtime_heartbeat_at": "2026-09-01T09:10:00+00:00",
                },
            )
            baseline, error = GitCollector(str(workspace)).capture_baseline(task)
            self.assertIsNotNone(baseline, error)
            target.write_text("candidate\n", encoding="utf-8")
            task_root = root / ".tasks"
            task.save(task_root)
            service = HarnessService(
                workspace,
                root / "profile",
                task_root=task_root,
                snapshot_root=root / ".control" / "snapshots",
            )
            service._lease_held = True
            service._load_profile = lambda: (None, ("app",))
            service._configuration = lambda _profile, _modules: _context()
            service._cumulative_manager = lambda read_only=False: SimpleNamespace(
                load_job_baseline=lambda _manifest: baseline
            )
            service.cumulative_policy_status = lambda **_kwargs: {
                "unexpected_runtime_dirty_files": [],
                "external_frozen_integrity": True,
                "baseline_declaration_integrity": True,
            }
            queued_job = {
                "job_id": "JOB-SALVAGE",
                "batch_id": "BATCH-SALVAGE",
                "generation": 5,
                "current_claim_task_id": task.task_id,
                "active_baseline_id": "BASELINE-SALVAGE",
                "pre_job_snapshot_id": "SNAP-SALVAGE",
                "pre_job_snapshot_manifest": "fixture-manifest.json",
                "inherited_batch_delta_files": [],
                "external_frozen_integrity": True,
                "baseline_declaration_integrity": True,
                "execution_context": _context(),
                "request": {
                    "requirement": task.requirement,
                    "worker": "codex",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "medium",
                    "target_modules": ["app"],
                    "target_resources": ["app/source.txt"],
                    "max_outer_attempts": 2,
                },
            }
            salvage = service.capture_crash_salvage(queued_job, {
                "generation": 5,
                "active_baseline_id": "BASELINE-SALVAGE",
                "batch_delta_files": [],
            })
            self.assertEqual("SALVAGE_CANDIDATE", salvage["status"])
            self.assertFalse(salvage["verified"])
            self.assertEqual(["app/source.txt"], salvage["task_owned_changed_files"])
            self.assertIn("NOT VERIFIED", salvage["recovery_handoff"])
            loaded = AttemptCheckpointStore(
                workspace, root / ".control"
            ).load(salvage["manifest_path"])
            self.assertEqual("CRASH_SALVAGE_CHECKPOINT", loaded["kind"])

    @staticmethod
    def _running_task(now: datetime) -> TaskState:
        task = TaskState(
            task_id="TASK-20260901-090000",
            requirement="runtime heartbeat fixture",
            status="RUNNING",
            stage="WORKING",
            started_at=(now - timedelta(minutes=2)).isoformat(),
        )
        task.progress_snapshot = {
            "progress_revision": 1,
            "job": {"status": "RUNNING", "task_id": task.task_id},
            "stage": {"name": "WORKER", "index": 1, "total": 6},
            "last_meaningful_event": {
                "type": "STAGE_CHANGED",
                "at": (now - timedelta(minutes=2)).isoformat(),
                "summary": "Worker started",
            },
        }
        task.execution_runtime = {
            "execution_id": "EXEC-NORMAL",
            "execution_owner": "manager-fixture",
            "execution_role": "WORKER",
            "pid": 4242,
            "started_at": (now - timedelta(minutes=2)).isoformat(),
            "last_runtime_heartbeat_at": (now - timedelta(seconds=5)).isoformat(),
            "lease_expires_at": (now + timedelta(seconds=40)).isoformat(),
            "last_progress_at": (now - timedelta(seconds=20)).isoformat(),
            "progress_timeout_seconds": 900,
            "process_alive": True,
        }
        return task

    def test_normal_execution_has_valid_lease_recent_progress(self):
        now = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)
        task = self._running_task(now)
        health = project_execution_health(task, now=now)
        self.assertEqual("VALID", health["runtime_health"])
        self.assertEqual("RECENT", health["progress_health"])
        text = render_progress({**task.progress_snapshot, "execution_health": health})
        self.assertIn("실행 중 · 최근 진척", text)
        self.assertNotIn("reconciliation 필요", text)

    def test_runtime_death_expires_lease_and_prevents_false_running_ux(self):
        now = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)
        task = self._running_task(now)
        task.execution_runtime["lease_expires_at"] = (
            now - timedelta(seconds=1)
        ).isoformat()
        task.execution_runtime["process_alive"] = False
        health = project_execution_health(task, now=now)
        self.assertEqual("LEASE_EXPIRED", health["runtime_health"])
        self.assertTrue(health["reconciliation_required"])
        self.assertEqual("WORKER_RUNTIME_LEASE_EXPIRED", health["failure_code"])
        text = render_progress({**task.progress_snapshot, "execution_health": health})
        self.assertIn("persisted RUNNING과 runtime 불일치", text)
        self.assertNotIn("실행 중 · 최근 진척", text)

    def test_process_alive_but_stalled_is_not_normal_running_ux(self):
        now = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)
        task = self._running_task(now)
        task.execution_runtime["last_progress_at"] = (
            now - timedelta(minutes=16)
        ).isoformat()
        health = project_execution_health(task, now=now)
        self.assertEqual("VALID", health["runtime_health"])
        self.assertEqual("STALLED", health["progress_health"])
        self.assertEqual("WORKER_PROGRESS_TIMEOUT", health["failure_code"])
        text = render_progress({**task.progress_snapshot, "execution_health": health})
        self.assertIn("의미 있는 진척이 없어", text)
        self.assertNotIn("실행 중 · 최근 진척", text)

    def test_existing_popen_owner_emits_runtime_lease_progress_and_terminal_events(self):
        events: list[dict[str, object]] = []
        cfg = TimeoutConfig(
            analysis_max=10,
            modification_max=10,
            hard_limit=10,
            inactivity=5,
            runtime_heartbeat=1,
            runtime_lease=3,
        )
        with tempfile.TemporaryDirectory() as temp:
            exit_code, stdout, stderr, timeout_reason = _run_with_activity_timeout(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    (
                        "import time; print('one', flush=True); "
                        "time.sleep(1.2); print('two', flush=True); time.sleep(0.2)"
                    ),
                ],
                None,
                temp,
                cfg,
                "read-only heartbeat fixture",
                task_mode="ANALYSIS",
                heartbeat=60,
                runtime_event=events.append,
                execution_id="EXEC-FIXTURE",
                execution_role="WORKER",
                execution_owner="manager-fixture",
            )
        self.assertEqual(0, exit_code)
        self.assertIn("two", stdout)
        self.assertEqual("", stderr)
        self.assertIsNone(timeout_reason)
        kinds = [event["event"] for event in events]
        self.assertEqual("STARTED", kinds[0])
        self.assertIn("HEARTBEAT", kinds)
        self.assertIn("PROGRESS", kinds)
        self.assertEqual("ENDED", kinds[-1])
        self.assertEqual("manager-fixture", events[-1]["execution_owner"])
        self.assertFalse(events[-1]["process_alive"])

    def test_restart_reconciles_expired_dead_runtime_without_losing_batch_delta(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            store = JobStore(root / ".control")
            store.activate_cumulative_policy(1, {
                "commit_policy": "BATCH_FINAL_COMMIT",
                "cumulative_worktree": True,
                "auto_continue_without_commit": True,
                "batch_owned_delta_supported": True,
                "pre_job_snapshot_supported": True,
                "scoped_rollback_supported": True,
                "final_commit_required": True,
                "active_baseline_id": "BASELINE-RESTART",
                "task_owned_baseline_files": ["app/a06.txt"],
                "external_frozen_baseline_files": [],
                "unexpected_runtime_dirty_files": [],
            })
            jobs, _ = store.enqueue_batch(
                "restart-runtime-090", [_job("J1"), _job("J2")],
                execution_context=_context(),
            )
            owner = "owner-runtime-090"
            manifests = root / "snapshots"
            manifests.mkdir()

            first = store.claim_next(owner)
            first_manifest = manifests / "first.json"
            first_manifest.write_text("{}", encoding="utf-8")
            store.record_pre_job_snapshot(first["job_id"], owner, {
                "snapshot_id": "SNAP-FIRST",
                "manifest_path": str(first_manifest),
                "inherited_batch_delta_files": [],
                "active_baseline_id": "BASELINE-RESTART",
                "external_frozen_integrity": True,
                "baseline_declaration_integrity": True,
            })
            store.mark_task_started(first["job_id"], owner, "TASK-20260901-090001")
            first = store.record_result(
                first["job_id"], owner, _success("TASK-20260901-090001")
            )
            store.acknowledge_success(
                first["job_id"], "ack-runtime-j1",
                expected_revision=first["revision"], batch_id=first["batch_id"],
            )
            inherited = list(store.queue_snapshot()["batch_delta_files"])
            self.assertEqual(["app/change.txt"], inherited)

            second = store.claim_next(owner)
            second_manifest = manifests / "second.json"
            second_manifest.write_text("{}", encoding="utf-8")
            store.record_pre_job_snapshot(second["job_id"], owner, {
                "snapshot_id": "SNAP-SECOND",
                "manifest_path": str(second_manifest),
                "inherited_batch_delta_files": inherited,
                "active_baseline_id": "BASELINE-RESTART",
                "external_frozen_integrity": True,
                "baseline_declaration_integrity": True,
            })
            task_id = "TASK-20260901-090002"
            store.mark_task_started(second["job_id"], owner, task_id)
            task = TaskState(
                task_id=task_id,
                requirement="restart runtime fixture",
                job_id=second["job_id"],
                status="RUNNING",
                stage="WORKING",
                active_baseline_id="BASELINE-RESTART",
                inherited_batch_delta_files=inherited,
                pre_job_snapshot_id="SNAP-SECOND",
                pre_job_snapshot_manifest=str(second_manifest),
                execution_runtime={
                    "execution_id": "EXEC-DEAD",
                    "execution_owner": "old-manager",
                    "execution_role": "WORKER",
                    "pid": 987654321,
                    "lease_expires_at": (
                        datetime.now().astimezone() - timedelta(minutes=1)
                    ).isoformat(),
                },
            )
            task.save(root / ".tasks")

            workspace = root / "workspace"
            repo = workspace / "app"
            repo.mkdir(parents=True)
            subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "fixture@example.invalid"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repo, check=True)
            target = repo / "work.txt"
            target.write_text("before\n", encoding="utf-8")
            subprocess.run(["git", "add", "work.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "fixture"], cwd=repo, check=True, capture_output=True)
            task.working_dir = str(workspace)
            task.target_module = ["app"]
            task.target_resources = ["app/work.txt"]
            task.queue_generation = 1
            task.batch_id = second["batch_id"]
            collector = GitCollector(str(workspace))
            baseline, error = collector.capture_baseline(task)
            self.assertIsNotNone(baseline, error)
            target.write_text("crash candidate\n", encoding="utf-8")
            delta = collector.collect(task, baseline)
            self.assertEqual(["app/work.txt"], delta.changed_files)
            task.task_owned_changed_files = list(delta.changed_files)
            task.scoped_rollback_targets = list(delta.changed_files)
            task.save(root / ".tasks")

            findings = store.inspect_orphaned_running(
                root / ".tasks",
                execution_owner_active=False,
                worker_process_active=False,
                pid_is_active=lambda _pid: False,
            )
            self.assertEqual("STALE_RUNNING_SALVAGE_REQUIRED", findings[0]["code"])
            checkpoint = AttemptCheckpointStore(
                workspace, root / ".control"
            ).capture(
                task,
                baseline,
                "RUNTIME_LEASE_EXPIRED_PROCESS_DEAD",
                artifact_kind="CRASH_SALVAGE_CHECKPOINT",
                recovery_metadata={"execution_id": "EXEC-DEAD"},
                allow_no_delta=True,
            )
            salvage = {
                "status": "SALVAGE_CANDIDATE",
                "integrity_status": "RESTORE_ELIGIBLE",
                "verified": False,
                "task_id": task_id,
                "execution_id": "EXEC-DEAD",
                "active_baseline_id": "BASELINE-RESTART",
                "checkpoint_id": checkpoint["checkpoint_id"],
                "manifest_path": checkpoint["manifest_path"],
                "manifest_sha256": checkpoint["manifest_sha256"],
                "task_owned_changed_files": ["app/work.txt"],
                "inherited_batch_delta_files": inherited,
                "recovery_handoff": "inspect immutable candidate; rerun gates",
            }
            current = store.get_job(second["job_id"])
            store.record_crash_salvage(
                second["job_id"],
                expected_revision=current["revision"],
                expected_task_id=task_id,
                expected_execution_id="EXEC-DEAD",
                salvage=salvage,
            )
            self.assertEqual([], store.inspect_orphaned_running(
                root / ".tasks", execution_owner_active=False,
                worker_process_active=False, pid_is_active=lambda _pid: False,
            ))
            interrupted = store.reconcile_interrupted()
            self.assertEqual([second["job_id"]], interrupted)
            after = store.queue_snapshot()
            self.assertEqual(inherited, after["batch_delta_files"])
            self.assertEqual("BASELINE-RESTART", after["active_baseline_id"])
            self.assertEqual("INTERRUPTED", store.get_job(second["job_id"])["status"])
            result = store.get_job(second["job_id"])["last_result"]
            self.assertEqual("SALVAGE_CANDIDATE", result["checkpoint_status"])
            self.assertEqual(["app/work.txt"], result["task_owned_changed_files"])
            loaded = AttemptCheckpointStore(workspace, root / ".control").load(
                checkpoint["manifest_path"]
            )
            self.assertEqual("CRASH_SALVAGE_CHECKPOINT", loaded["kind"])

    def test_restart_with_missing_snapshot_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = JobStore(root / ".control")
            store.activate_cumulative_policy(1, {
                "commit_policy": "BATCH_FINAL_COMMIT",
                "cumulative_worktree": True,
                "auto_continue_without_commit": True,
                "batch_owned_delta_supported": True,
                "pre_job_snapshot_supported": True,
                "scoped_rollback_supported": True,
                "final_commit_required": True,
                "active_baseline_id": "BASELINE-MISSING",
            })
            jobs, _ = store.enqueue_batch(
                "restart-missing-090", [_job("J1")], execution_context=_context()
            )
            job = store.claim_next("owner-missing-090")
            task_id = "TASK-20260901-090003"
            store.mark_task_started(job["job_id"], "owner-missing-090", task_id)
            TaskState(
                task_id=task_id,
                requirement="missing snapshot fixture",
                job_id=job["job_id"],
                status="RUNNING",
                stage="WORKING",
                execution_runtime={
                    "execution_id": "EXEC-MISSING",
                    "execution_role": "WORKER",
                    "pid": 987654321,
                    "lease_expires_at": (
                        datetime.now().astimezone() - timedelta(minutes=1)
                    ).isoformat(),
                },
            ).save(root / ".tasks")
            findings = store.inspect_orphaned_running(
                root / ".tasks",
                execution_owner_active=False,
                worker_process_active=False,
                pid_is_active=lambda _pid: False,
            )
            self.assertEqual(
                "STALE_RUNNING_RECOVERY_EVIDENCE_INCOMPLETE", findings[0]["code"]
            )
            self.assertEqual("RUNNING", store.get_job(job["job_id"])["status"])


if __name__ == "__main__":
    unittest.main()
