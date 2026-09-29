"""0.9.0.6 Droid model registry compatibility hotfix regression.

Covers the canonical Droid custom model registry, the legacy
``custom:GLM-5.3-[Z.AI-Coding]-0`` compatibility alias, immutable queued
contract resolution, evidence taxonomy, and fail-closed behavior for unknown
models.  All tests are offline: no Queue state and no Droid invocation.
"""

from __future__ import annotations

import harness_temp as tempfile
import unittest
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

from harness_service import HarnessService, HarnessServiceError
from job_contract import (
    DROID_DEFAULT_CODING_MODEL,
    DROID_GLM_FAIL_CLOSED_MODEL,
    DROID_GLM_FAIL_CLOSED_POLICY,
    DROID_GLM_FLASH_FAST_MODEL,
    DROID_GLM_FLASH_THINKING_MODEL,
    DROID_GLM_THINKING_MODEL,
    DROID_MODELS,
    JobContractError,
    JobRequest,
    LEGACY_DROID_GLM_MODEL,
    MODEL_SELECTION_CANONICAL,
    MODEL_SELECTION_REASON_ALIAS,
    droid_model_selection_reason,
    request_fingerprint,
    resolve_droid_model,
)
from manager import Manager, ManagerResult
from task_state import TaskState
from test_job_control import MODULES, execution_context, request


UNKNOWN_MODELS = (
    "custom:GLM-5.2-[Z.AI-Coding]-0",
    "custom:GLM-5-Turbo-[Z.AI-Coding]-0",
    "custom:GLM-5.4-[Z.AI-Coding]-0",
)


class RegistryHotfix09006Tests(unittest.TestCase):
    def test_registry_contains_exactly_the_canonical_droid_models(self):
        self.assertEqual(
            (
                DROID_GLM_THINKING_MODEL,
                DROID_GLM_FLASH_THINKING_MODEL,
                DROID_GLM_FLASH_FAST_MODEL,
            ),
            DROID_MODELS,
        )
        self.assertEqual(DROID_GLM_THINKING_MODEL, DROID_DEFAULT_CODING_MODEL)
        self.assertEqual(DROID_GLM_THINKING_MODEL, DROID_GLM_FAIL_CLOSED_MODEL)
        self.assertNotIn(LEGACY_DROID_GLM_MODEL, DROID_MODELS)

    def test_canonical_ids_validate_and_resolve_to_themselves(self):
        for model in DROID_MODELS:
            with self.subTest(model=model):
                parsed = JobRequest.from_mapping(
                    {"requirement": "수정", "worker": "droid", "model": model,
                     "target_modules": ["suc"]},
                    allowed_modules=MODULES,
                )
                self.assertEqual(model, parsed.model)
                resolved, reason = resolve_droid_model(model)
                self.assertEqual((model, MODEL_SELECTION_CANONICAL), (resolved, reason))

    def test_legacy_alias_resolves_deterministically_to_thinking(self):
        for _ in range(5):
            self.assertEqual(
                (DROID_GLM_THINKING_MODEL, MODEL_SELECTION_REASON_ALIAS),
                resolve_droid_model(LEGACY_DROID_GLM_MODEL),
            )

    def test_unknown_custom_models_fail_closed(self):
        for model in UNKNOWN_MODELS:
            with self.subTest(model=model):
                with self.assertRaises(JobContractError):
                    resolve_droid_model(model)
                with self.assertRaises(JobContractError):
                    JobRequest.from_mapping(
                        {"requirement": "수정", "worker": "droid", "model": model,
                         "target_modules": ["suc"]},
                        allowed_modules=MODULES,
                    )
        # An empty request never resolves and must fail closed.
        with self.assertRaises(JobContractError):
            resolve_droid_model("")

    def test_immutable_legacy_contract_validates_without_rewrite(self):
        mapping = {
            "requirement": "수정",
            "worker": "droid",
            "model": LEGACY_DROID_GLM_MODEL,
            "target_modules": ["suc"],
        }
        parsed = JobRequest.from_mapping(mapping, allowed_modules=MODULES)
        self.assertEqual(LEGACY_DROID_GLM_MODEL, parsed.model)
        self.assertEqual(LEGACY_DROID_GLM_MODEL, parsed.to_dict()["model"])
        # Replaying validation must not mutate or re-fingerprint the contract.
        replay = JobRequest.from_mapping(mapping, allowed_modules=MODULES)
        self.assertEqual(
            request_fingerprint([parsed]), request_fingerprint([replay])
        )

    def test_fail_closed_gate_accepts_canonical_and_legacy_alias_only(self):
        accepted = (DROID_GLM_FAIL_CLOSED_MODEL, LEGACY_DROID_GLM_MODEL)
        for model in accepted:
            task = TaskState(
                task_id="TASK-09006-ACCEPT",
                requirement="수정",
                worker="droid",
                droid_model=model,
                worker_routing_policy=DROID_GLM_FAIL_CLOSED_POLICY,
            )
            self.assertIsNone(Manager._worker_routing_failure(task), model)
        rejected = (DROID_GLM_FLASH_FAST_MODEL,) + UNKNOWN_MODELS[:3]
        for model in rejected:
            task = TaskState(
                task_id="TASK-09006-REJECT",
                requirement="수정",
                worker="droid",
                droid_model=model,
                worker_routing_policy=DROID_GLM_FAIL_CLOSED_POLICY,
            )
            result = Manager._worker_routing_failure(task)
            self.assertIsNotNone(result, model)
            self.assertEqual("DROID_MODEL_ROUTING_POLICY_VIOLATION", task.failure_code)

    def test_manager_worker_construction_resolves_legacy_alias(self):
        manager = object.__new__(Manager)
        manager.worker_type = "droid"
        manager.profile = None
        manager.policy_root = None
        manager.task_root = None
        manager.timeout = 5
        manager.timeout_config = None
        manager.model = ""
        manager.codex_model = None
        for requested in (LEGACY_DROID_GLM_MODEL, DROID_GLM_THINKING_MODEL):
            with self.subTest(requested=requested):
                task = TaskState(
                    task_id="TASK-09006-WORKER",
                    requirement="수정",
                    worker="droid",
                    droid_model=requested,
                )
                worker, actual = manager._get_worker(task)
                self.assertEqual("droid", actual)
                self.assertEqual(DROID_GLM_THINKING_MODEL, worker.model)
        unknown = TaskState(
            task_id="TASK-09006-UNKNOWN",
            requirement="수정",
            worker="droid",
            droid_model=UNKNOWN_MODELS[0],
        )
        worker, _ = manager._get_worker(unknown)
        # Unknown models are never silently substituted.
        self.assertEqual(UNKNOWN_MODELS[0], worker.model)

    def test_selection_reason_evidence_taxonomy(self):
        self.assertEqual(
            MODEL_SELECTION_REASON_ALIAS,
            droid_model_selection_reason(
                LEGACY_DROID_GLM_MODEL, DROID_GLM_THINKING_MODEL
            ),
        )
        self.assertEqual(
            "REQUESTED_WORKER",
            droid_model_selection_reason(
                DROID_GLM_THINKING_MODEL, DROID_GLM_THINKING_MODEL
            ),
        )
        self.assertEqual(
            "REQUESTED_WORKER",
            droid_model_selection_reason("gpt-5.6-sol", "gpt-5.6-sol"),
        )
        self.assertEqual(
            "REQUESTED_WORKER",
            droid_model_selection_reason(LEGACY_DROID_GLM_MODEL, LEGACY_DROID_GLM_MODEL),
        )

    def test_taskstate_evidence_preserves_requested_model(self):
        with tempfile.TemporaryDirectory() as temp:
            task = TaskState(
                task_id="TASK-09006-EVIDENCE",
                requirement="수정",
                worker="droid",
                droid_model=DROID_GLM_THINKING_MODEL,
                working_dir=temp,
            )
            task.requested_model = LEGACY_DROID_GLM_MODEL
            task.planned_model = DROID_GLM_THINKING_MODEL
            task.actual_model = DROID_GLM_THINKING_MODEL
            task.selection_reason = MODEL_SELECTION_REASON_ALIAS
            task.save(temp)
            loaded = TaskState.load("TASK-09006-EVIDENCE", temp)
            self.assertEqual(LEGACY_DROID_GLM_MODEL, loaded.requested_model)
            self.assertEqual(DROID_GLM_THINKING_MODEL, loaded.planned_model)
            self.assertEqual(DROID_GLM_THINKING_MODEL, loaded.actual_model)
            self.assertEqual(MODEL_SELECTION_REASON_ALIAS, loaded.selection_reason)

    def _dispatch_service(self, temp: str, captured: list):
        root = Path(temp)
        project = root / "project"
        profile_dir = root / "profile"
        task_root = root / ".tasks"
        project.mkdir()
        profile_dir.mkdir()
        (profile_dir / "project.json").write_text("{}", encoding="utf-8")
        profile = SimpleNamespace(
            id="e4u", profile_dir=profile_dir, schema_version=1,
            module_names=MODULES,
        )

        class CapturingManager:
            def __init__(self, **_kwargs):
                pass

            def submit_task(self, task):
                captured.append(task)
                task.status = "SUCCESS"
                task.stage = "DONE"
                task.verification_status = "VERIFIED"
                task.changed_files = []
                task.worktree_disposition = "NO_DELTA"
                future = Future()
                future.set_result(ManagerResult(success=True))
                return future

            def shutdown(self):
                return None

        class Lock:
            def acquire(self):
                return True

            def release(self):
                return None

        service = HarnessService(
            project, profile_dir, task_root=task_root,
            manager_factory=CapturingManager, instance_lock=Lock(),
        )
        service._load_profile = lambda: (profile, list(MODULES))
        service._configuration = lambda *_: {
            "working_dir": str(project.resolve()),
            "profile_dir": str(profile_dir.resolve()),
            "available_modules": list(MODULES),
            "task_root": str(task_root.resolve()),
            **execution_context(),
        }
        return service

    def test_dispatch_resolves_legacy_request_to_canonical_execution(self):
        captured: list[TaskState] = []
        with tempfile.TemporaryDirectory() as temp:
            service = self._dispatch_service(temp, captured)
            legacy_request = request(
                worker="droid", model=LEGACY_DROID_GLM_MODEL
            ).to_dict()
            self.assertTrue(service.acquire_execution_lease())
            try:
                outcome = service.execute(
                    {
                        "request": legacy_request,
                        "current_requirement": legacy_request["requirement"],
                        "execution_context": execution_context(),
                    },
                    on_task_started=lambda _task_id: None,
                )
            finally:
                service.release_execution_lease()
            self.assertEqual("", outcome["failure_code"])
            self.assertEqual("SUCCESS", outcome["status"])
            self.assertEqual(1, len(captured))
            task = captured[0]
            self.assertEqual(LEGACY_DROID_GLM_MODEL, task.requested_model)
            self.assertEqual(LEGACY_DROID_GLM_MODEL, task.original_model)
            self.assertEqual(DROID_GLM_THINKING_MODEL, task.droid_model)
            self.assertEqual("droid", task.worker)

    def test_dispatch_technical_route_alias_compatibility(self):
        captured: list[TaskState] = []
        with tempfile.TemporaryDirectory() as temp:
            service = self._dispatch_service(temp, captured)
            base_request = request(worker="droid", model=LEGACY_DROID_GLM_MODEL)
            route = {
                "policy": DROID_GLM_FAIL_CLOSED_POLICY,
                "worker": "droid",
                "model": LEGACY_DROID_GLM_MODEL,
                "stability_evidence_sha256": "a" * 64,
            }
            self.assertTrue(service.acquire_execution_lease())
            try:
                outcome = service.execute(
                    {
                        "request": base_request.to_dict(),
                        "current_requirement": base_request.requirement,
                        "execution_context": execution_context(),
                        "technical_retry_route": route,
                    },
                    on_task_started=lambda _task_id: None,
                )
            finally:
                service.release_execution_lease()
            self.assertEqual("", outcome["failure_code"])
            task = captured[0]
            self.assertEqual(DROID_GLM_THINKING_MODEL, task.droid_model)
            self.assertEqual(DROID_GLM_FAIL_CLOSED_POLICY, task.worker_routing_policy)

            for bad_model in (DROID_GLM_FLASH_FAST_MODEL, UNKNOWN_MODELS[0]):
                with self.subTest(route_model=bad_model):
                    bad_route = dict(route, model=bad_model)
                    self.assertTrue(service.acquire_execution_lease())
                    try:
                        with self.assertRaises(HarnessServiceError) as raised:
                            service.execute(
                                {
                                    "request": base_request.to_dict(),
                                    "current_requirement": base_request.requirement,
                                    "execution_context": execution_context(),
                                    "technical_retry_route": bad_route,
                                },
                                on_task_started=lambda _task_id: None,
                            )
                    finally:
                        service.release_execution_lease()
                    self.assertEqual(
                        "DROID_MODEL_ROUTING_POLICY_VIOLATION", raised.exception.code
                    )


if __name__ == "__main__":
    unittest.main()
