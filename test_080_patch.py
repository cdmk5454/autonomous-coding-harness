from __future__ import annotations

import json
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from chatgpt_control import ChatGPTControlService
from feature_context import load_feature_context
from job_contract import JobRequest
from job_queue import (
    AUTONOMOUS_CONFIRMATION,
    EXECUTION_MODE_AUTONOMOUS,
    MANUAL_RESOLUTION_ACCEPT_PRESERVED,
    MANUAL_RESOLUTION_CONFIRMATIONS,
    JobStore,
)
from manager import Manager
from queue_supervisor import QueueSupervisor
from task_state import TaskState


def execution_context() -> dict[str, object]:
    digest = "a" * 64
    return {
        "profile_id": "e4u",
        "profile_schema_version": 1,
        "profile_manifest_sha256": digest,
        "profile_snapshot_sha256": digest,
        "policy_snapshot_sha256": digest,
        "workspace_identity_sha256": digest,
    }


def request(client: str, attempts: int = 2) -> JobRequest:
    return JobRequest.from_mapping(
        {
            "client_job_id": client,
            "requirement": "SP0023 KEDI 취업통계를 수정한다.",
            "worker": "codex",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "medium",
            "target_modules": ["suc", "sucui"],
            "max_outer_attempts": attempts,
        },
        allowed_modules=["suc", "sucui"],
    )


def failed(disposition: str = "CLEAN_ROLLBACK") -> dict[str, object]:
    return {
        "status": "FAILED" if disposition != "PRESERVED" else "AWAITING_QA",
        "stage": "DONE" if disposition != "PRESERVED" else "AWAITING_QA",
        "success": False,
        "verification_status": "FAILED" if disposition != "PRESERVED" else "PARTIAL",
        "failure_stage": "REVIEW",
        "failure_code": "REVIEW_FAILED",
        "failure_reason": "조사기준이 실제 통계 조건에 적용되지 않음",
        "failure_type": "review",
        "fix_scope": "RETRYABLE",
        "review_status": "REVIEW_FAIL",
        "worktree_disposition": disposition,
    }


def succeeded() -> dict[str, object]:
    return {
        "status": "SUCCESS",
        "stage": "DONE",
        "success": True,
        "verification_status": "VERIFIED",
        "failure_code": "",
        "review_status": "REVIEW_PASS",
        "changed_files": ["suc/src/A.java"],
        "changed_file_sha256": {"suc/src/A.java": "a" * 64},
        "commit_eligible": True,
        "commit_blockers": [],
    }


class SequenceHarness:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.executed: list[str] = []
        self.committed: list[str] = []
        self.lease = False

    def acquire_execution_lease(self):
        self.lease = True
        return True

    def release_execution_lease(self):
        self.lease = False

    def execute(self, job, *, on_task_started):
        self.executed.append(job["job_id"])
        task_id = f"TASK-080-{len(self.executed)}"
        on_task_started(task_id)
        result = dict(self.outcomes.pop(0))
        result["task_id"] = task_id
        return result

    def commit_verified_job(self, job):
        self.committed.append(job["job_id"])
        return {"status": "COMMITTED", "commits": [{"commit": "f" * 40}]}


class AutonomousRecoveryTests(unittest.TestCase):
    def test_clean_review_failure_retries_then_commits(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "auto-retry-080", [request("A", 2)], execution_context=execution_context()
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS, "mode-080-a", AUTONOMOUS_CONFIRMATION
            )
            harness = SequenceHarness([failed(), succeeded()])
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            persisted = store.get_job(jobs[0]["job_id"])
            self.assertEqual("SUCCEEDED", persisted["status"])
            self.assertEqual(2, persisted["outer_attempt"])
            self.assertEqual(2, len(harness.executed))
            self.assertEqual([jobs[0]["job_id"]], harness.committed)
            self.assertTrue(persisted["retry_actions"])

    def test_exhausted_clean_failure_is_skipped_and_fifo_continues(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "auto-skip-080",
                [request("A", 1), request("B", 1)],
                execution_context=execution_context(),
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS, "mode-080-b", AUTONOMOUS_CONFIRMATION
            )
            harness = SequenceHarness([failed(), succeeded()])
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            self.assertEqual("SKIPPED", store.get_job(jobs[0]["job_id"])["status"])
            self.assertEqual("SUCCEEDED", store.get_job(jobs[1]["job_id"])["status"])
            self.assertEqual([job["job_id"] for job in jobs], harness.executed)

    def test_preserved_failure_still_stops_for_human_review(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "preserved-080", [request("A", 2)], execution_context=execution_context()
            )
            store.set_execution_mode(
                EXECUTION_MODE_AUTONOMOUS, "mode-080-c", AUTONOMOUS_CONFIRMATION
            )
            harness = SequenceHarness([failed("PRESERVED")])
            supervisor = QueueSupervisor(store, harness)
            self.assertTrue(supervisor.start()["started"])
            self.assertTrue(supervisor.wait(5))
            persisted = store.get_job(jobs[0]["job_id"])
            self.assertEqual("AWAITING_QA", persisted["status"])
            self.assertEqual(jobs[0]["job_id"], store.queue_snapshot()["blocked_by_job_id"])
            self.assertFalse(persisted["retry_actions"])


class ManagerRollbackTests(unittest.TestCase):
    def test_outer_retry_requires_proven_clean_rollback(self):
        class Collector:
            def rollback(self, *, task, baseline):
                task.is_rolled_back = True
                return True, "ok"

        with tempfile.TemporaryDirectory() as temp:
            manager = Manager.__new__(Manager)
            manager.collector = Collector()
            manager._task_baselines = {"TASK-080": object()}
            manager.task_root = Path(temp)
            task = TaskState(
                task_id="TASK-080",
                requirement="SP0023 수정",
                changed_files=["suc/src/A.java"],
                failure_type="review",
                fix_scope="RETRYABLE",
                failure_reason="review failed",
            )
            task.attempt_history.append({"worktree_disposition": "PRESERVED"})
            self.assertTrue(manager._rollback_for_outer_retry(task, "test"))
            self.assertEqual("CLEAN_ROLLBACK", task.worktree_disposition)
            self.assertEqual("FAILED", task.status)


class FeatureContextTests(unittest.TestCase):
    def test_sp0023_selects_map_and_matching_asis_section(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mapping = root / "feature-map.json"
            mapping.write_text(json.dumps({
                "schema_version": 1,
                "precedence": ["CURRENT_DB"],
                "global_constraints": ["NULL 허용"],
                "features": [{
                    "id": "SP0023",
                    "title": "KEDI 통계",
                    "aliases": ["SP0023"],
                    "screens": ["SP0023 Vue"],
                    "asis_functions": ["KEDI 상세"],
                    "tables": ["SUC.SSUC383"],
                    "tobe_additions": ["유지취업률"],
                    "ui_paths": [],
                    "constraints": ["임의 JOIN 금지"],
                    "asis_source_globs": ["asis_*.md"]
                }]
            }, ensure_ascii=False), encoding="utf-8")
            (root / "asis_kedi.md").write_text(
                "# 개요\n기타\n## SP0023 KEDI 상세\nSSUC383 원본 기능\n## 다른 기능\n제외",
                encoding="utf-8",
            )
            profile = SimpleNamespace(profile_dir=root, feature_map=mapping)
            context = load_feature_context("SP0023 Vue 수정", profile=profile)
            self.assertIn("SUC.SSUC383", context)
            self.assertIn("유지취업률", context)
            self.assertIn("asis_kedi.md", context)
            self.assertIn("SSUC383 원본 기능", context)
            self.assertNotIn("## 다른 기능", context)


class IdleProfileReloadTests(unittest.TestCase):
    def test_reload_requires_empty_idle_queue_and_updates_context(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")

            class Supervisor:
                def is_running(self): return False
                def status(self): return {"running": False}

            class Harness:
                def probe_execution_idle(self): return True
                def validate_configuration(self):
                    return {
                        **execution_context(),
                        "working_dir": "D:/inha_dev",
                        "profile_dir": "D:/agents/profiles/e4u",
                        "task_root": "D:/kkm_test/.tasks",
                        "available_modules": ["suc", "sucui"],
                    }

            service = ChatGPTControlService.__new__(ChatGPTControlService)
            service.read_only = False
            service.store = store
            service.supervisor = Supervisor()
            service.harness = Harness()
            service.configuration = {**Harness().validate_configuration(), "profile_snapshot_sha256": "b" * 64}
            service.allowed_modules = ["suc"]
            result = service.reload_idle_profile("I_CONFIRM_RELOAD_IDLE_PROFILE")
            self.assertTrue(result["reloaded"])
            self.assertIn("profile_snapshot_sha256", result["changed_context_keys"])
            self.assertEqual(["suc", "sucui"], service.allowed_modules)


class McpResolutionTests(unittest.TestCase):
    def test_writable_service_resolution_keeps_revision_confirmation_and_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            store = JobStore(Path(temp) / ".control")
            jobs, _ = store.enqueue_batch(
                "resolve-080", [request("A", 2)], execution_context=execution_context()
            )
            job_id = jobs[0]["job_id"]
            owner = "owner-resolve-080"
            store.claim_next(owner)
            store.mark_task_started(job_id, owner, "TASK-080-RESOLVE")
            store.record_result(job_id, owner, failed("PRESERVED"))
            revision = store.get_job(job_id)["revision"]

            class Supervisor:
                def is_running(self): return False
                def status(self): return {"running": False}

            class Harness:
                def __init__(self): self.lease = False
                def probe_execution_idle(self): return not self.lease
                def acquire_execution_lease(self): self.lease = True; return True
                def release_execution_lease(self): self.lease = False

            service = ChatGPTControlService.__new__(ChatGPTControlService)
            service.read_only = False
            service.store = store
            service.supervisor = Supervisor()
            service.harness = Harness()
            result = service.resolve_job(
                job_id,
                revision,
                MANUAL_RESOLUTION_ACCEPT_PRESERVED,
                MANUAL_RESOLUTION_CONFIRMATIONS[MANUAL_RESOLUTION_ACCEPT_PRESERVED],
                "mcp-resolve-080",
            )
            self.assertEqual("SKIPPED", result["job"]["status"])
            self.assertFalse(service.harness.lease)
            self.assertEqual("", result["queue"]["blocked_by_job_id"])


if __name__ == "__main__":
    unittest.main()
