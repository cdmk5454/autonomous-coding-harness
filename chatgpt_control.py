"""SDK-independent application service exposed by the ChatGPT MCP adapter."""

from __future__ import annotations

import hashlib
import json
import copy
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from control_paths import (
    ControlPathError,
    validate_control_state_roots,
    validate_control_state_roots_read_only,
)
from harness_service import HarnessService, HarnessServiceError
from job_contract import (
    CODEX_MODELS,
    EXECUTION_CONTEXT_KEYS,
    MAX_BATCH_JOBS,
    JobContractError,
    JobRequest,
    validate_execution_context,
)
from job_queue import (
    AWAITING_ENRICHMENT,
    AWAITING_QA,
    BLOCKED,
    CANCELLED,
    FAILED_FINAL,
    INTERRUPTED,
    QUEUED,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    JobStore,
    QueueError,
)
from queue_supervisor import QueueSupervisor
from runtime_identity import RuntimeIdentity
from execution_lineage import lineage_status
from execution_evidence import build_execution_summary
from progress_projection import project_execution_health, queue_progress_snapshot
from task_state import TaskState
from git_collector import GitCollector
from qa_quarantine import QACandidateStore, validate_qa_policy
from policy_catalog import is_control_state_write_failure
from runtime_safety import git_subprocess_env, trusted_executable
from worker import parse_qa_request
from attempt_checkpoint import AttemptCheckpointStore, AttemptCheckpointError
from batch_manifest import BatchManifestError, build_provenance


def _post_worker_recovery_integrity_valid(
    changed_files: Sequence[str], integrity: Mapping[str, Any]
) -> bool:
    changed = set(changed_files)
    unexpected = set(integrity.get("unexpected_runtime_dirty_files") or [])
    return bool(changed) and unexpected.issubset(changed) and (
        integrity.get("external_frozen_integrity") is True
        and integrity.get("baseline_declaration_integrity") is True
    )


def _next_action(status: str) -> str:
    return {
        QUEUED: "START_OR_WAIT",
        RUNNING: "WAIT",
        SUCCEEDED: "READ_RESULT_THEN_ACKNOWLEDGE_SUCCESS",
        AWAITING_ENRICHMENT: "READ_RESULT_AND_RETRY_WITH_SUPPLEMENT",
        AWAITING_QA: "MANUAL_REVIEW_REQUIRED",
        FAILED_FINAL: "OUTER_RETRY_BUDGET_EXHAUSTED",
        BLOCKED: "LOCAL_OPERATOR_RECOVERY_REQUIRED",
        INTERRUPTED: "CONFIRM_NO_ACTIVE_PROCESS_BEFORE_RESUME",
        SKIPPED: "CONTINUE",
        CANCELLED: "NONE",
    }.get(status, "INSPECT")


class ReadOnlySupervisor:
    """Inert status provider for the Inspector-only service."""

    @staticmethod
    def status() -> dict[str, Any]:
        return {
            "running": False,
            "owner_id": "",
            "processed_in_run": 0,
            "last_error": "",
            "reconciled_interrupted": [],
            "lease_quarantined": False,
            "wake_requested": False,
            "retiring": False,
            "read_only": True,
        }

    @staticmethod
    def is_running() -> bool:
        return False

    @staticmethod
    def start() -> dict[str, Any]:
        raise HarnessServiceError("CONTROL_SURFACE_READ_ONLY")

    @staticmethod
    def wait(timeout: float | None = None) -> bool:
        return True


class ChatGPTControlService:
    def __init__(
        self,
        working_dir: str | Path,
        profile_dir: str | Path,
        *,
        control_root: str | Path,
        task_root: str | Path,
        harness: HarnessService | None = None,
        store: JobStore | None = None,
        reconcile: bool = True,
        state_base: str | Path | None = None,
        read_only: bool = False,
        runtime_identity: RuntimeIdentity | None = None,
    ):
        self.read_only = bool(read_only)
        fixed_base = Path(state_base or Path(__file__).resolve().parent)
        # Freeze runtime identity once. Health must not reread mutable disk
        # VERSION state and thereby report an upgrade before a real restart.
        self.runtime_identity = runtime_identity or RuntimeIdentity.load(
            Path(__file__).resolve().parent
        )
        try:
            validator = (
                validate_control_state_roots_read_only
                if self.read_only
                else validate_control_state_roots
            )
            fixed_control_root, fixed_task_root = validator(
                control_root,
                task_root,
                state_base=fixed_base,
            )
        except ControlPathError as exc:
            from harness_service import HarnessServiceError

            raise HarnessServiceError(exc.code, exc.field) from exc
        self.harness = harness or HarnessService(
            working_dir,
            profile_dir,
            task_root=fixed_task_root,
            snapshot_root=fixed_control_root / "snapshots",
        )
        self.configuration = self.harness.validate_configuration()
        self.allowed_modules = list(self.configuration["available_modules"])
        self.store = store or JobStore(fixed_control_root, read_only=self.read_only)
        if self.read_only and not bool(getattr(self.store, "read_only", False)):
            raise HarnessServiceError("CONTROL_STORE_MODE_MISMATCH")
        # Reconciliation mutates queue ownership and is safe only while holding the
        # same OS execution lease as run.py. QueueSupervisor.start performs it after
        # acquiring that lease; constructor/health paths remain read-only.
        self.supervisor = (
            ReadOnlySupervisor()
            if self.read_only
            else QueueSupervisor(self.store, self.harness)
        )

    def _assert_writable(self) -> None:
        if self.read_only:
            raise HarnessServiceError("CONTROL_SURFACE_READ_ONLY")

    def _assert_configuration_current(self) -> None:
        current = self.harness.validate_configuration()
        if getattr(self.store, "repository", None) is not None:
            # New materializations use the current policy. Existing executions
            # retain their own immutable contract and snapshot.
            self.configuration = current
            self.allowed_modules = list(current["available_modules"])
            return
        if any(
            current.get(key) != self.configuration.get(key)
            for key in EXECUTION_CONTEXT_KEYS
        ):
            raise HarnessServiceError("PROFILE_DRIFT")

    def _queue_snapshot(self) -> dict[str, Any]:
        queue = self.store.queue_snapshot()
        status_method = getattr(self.harness, "cumulative_policy_status", None)
        if callable(status_method):
            policy = status_method(
                active_baseline_id=str(queue.get("active_baseline_id", "")),
                batch_delta_files=list(queue.get("batch_delta_files") or []),
                read_only=self.read_only,
            )
        else:  # compatibility for narrow injected harness doubles
            configuration = getattr(self, "configuration", {})
            policy = {
                "commit_policy": configuration.get("commit_policy", "PER_JOB"),
                "cumulative_worktree": False,
                "auto_continue_without_commit": False,
                "batch_owned_delta_supported": False,
                "pre_job_snapshot_supported": False,
                "scoped_rollback_supported": False,
                "final_commit_required": False,
                "active_baseline_id": "",
                "task_owned_baseline_files": [],
                "external_frozen_baseline_files": [],
                "unexpected_runtime_dirty_files": [],
            }
        integrity = self._canonical_integrity_snapshot(queue, policy=policy)
        jobs = self.store.list_jobs(limit=200)
        active_job = next(
            (
                job for job in jobs
                if job.get("job_id") == queue.get("running_job_id")
            ),
            None,
        )
        if active_job is None:
            active_job = next(
                (job for job in jobs if job.get("status") in {"AWAITING_QA", "AWAITING_DEPENDENCY_QA"}),
                None,
            )
        task_progress = self._task_progress(active_job or {})
        progress = queue_progress_snapshot(queue, jobs, task_progress)
        runtime_identity = getattr(self, "runtime_identity", None)
        configuration = getattr(self, "configuration", {}) or {}
        progress["harness"] = {
            "version": str(getattr(runtime_identity, "harness_version", "")),
            "profile_id": str(configuration.get("profile_id", "")),
        }
        readiness_items: list[dict[str, Any]] = []
        repository = getattr(self.store, "repository", None)
        if repository is not None:
            current_batch = str(queue.get("current_batch_id", ""))
            for job in jobs:
                if current_batch and str(job.get("batch_id", "")) != current_batch:
                    continue
                projection = repository.product_readiness(str(job.get("job_id", "")))
                readiness_items.append(projection)
        open_decisions = [
            decision for item in readiness_items for decision in item.get("open_decisions", [])
        ]
        if readiness_items and all(item.get("readiness") == "COMPLETE" for item in readiness_items):
            aggregate_readiness = "COMPLETE"
        elif any(item.get("readiness") == "SKELETON_READY" for item in readiness_items):
            aggregate_readiness = "SKELETON_READY"
        else:
            aggregate_readiness = "FINALIZATION_PENDING"
        progress.update({
            "product_readiness": aggregate_readiness,
            "user_action_required": bool(open_decisions),
            "open_decision_count": len(open_decisions),
            "open_decisions": open_decisions,
        })
        return {
            **queue,
            **policy,
            "integrity_snapshot": integrity,
            "progress_revision": int(progress.get("progress_revision", 0)),
            "progress": progress,
            "product_readiness": aggregate_readiness,
            "user_action_required": bool(open_decisions),
            "open_decision_count": len(open_decisions),
            "open_decisions": open_decisions,
        }

    def _canonical_integrity_snapshot(
        self,
        queue: Mapping[str, Any],
        *,
        policy: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Compute runtime integrity from the canonical cumulative policy source."""
        if policy is None:
            status_method = getattr(self.harness, "cumulative_policy_status", None)
            if not callable(status_method):
                raise QueueError("CANONICAL_INTEGRITY_UNAVAILABLE")
            policy = status_method(
                active_baseline_id=str(queue.get("active_baseline_id", "")),
                batch_delta_files=list(queue.get("batch_delta_files") or []),
                read_only=True,
            )
        predecessor_hashes: dict[str, str] = {}
        predecessor_files = list(queue.get("batch_delta_files") or [])
        configuration = dict(getattr(self, "configuration", {}) or {})
        workspace_value = configuration.get("working_dir") or getattr(
            self.harness, "working_dir", ""
        )
        if predecessor_files and not workspace_value:
            raise QueueError("CANONICAL_INTEGRITY_UNAVAILABLE")
        workspace = Path(str(workspace_value)) if workspace_value else Path()
        for relative in predecessor_files:
            target = workspace / str(relative)
            predecessor_hashes[str(relative)] = (
                hashlib.sha256(target.read_bytes()).hexdigest()
                if target.is_file()
                else "DELETED"
            )
        snapshot = {
            "generation": int(queue.get("generation", 0)),
            "active_baseline_id": str(queue.get("active_baseline_id", "")),
            "external_frozen_integrity": policy.get("external_frozen_integrity") is True,
            "baseline_declaration_integrity": policy.get("baseline_declaration_integrity") is True,
            "unexpected_runtime_dirty_files": list(
                policy.get("unexpected_runtime_dirty_files") or []
            ),
            "predecessor_delta_hashes": predecessor_hashes,
            "source": "CANONICAL_COMPUTED",
            "computed_at": datetime.now().astimezone().isoformat(),
        }
        snapshot["snapshot_sha256"] = self._canonical_sha256(snapshot)
        return snapshot

    def _task_progress(self, job: Mapping[str, Any]) -> dict[str, Any]:
        """Return live Task progress without mutating Queue/Job state.

        A running Task persists stage telemetry independently from Queue CAS
        revisions.  The Job result is intentionally terminal-only, so readers
        must consult the current Task record first and fall back to the last
        terminal result for legacy records.
        """
        task_id = str(job.get("current_claim_task_id", ""))
        if not task_id:
            task_ids = list(job.get("task_ids") or [])
            task_id = str(task_ids[-1]) if task_ids else ""
        if task_id:
            try:
                configuration = getattr(self, "configuration", {}) or {}
                task_root = configuration.get("task_root") or getattr(
                    getattr(self, "harness", None), "task_root", ""
                )
                if not task_root:
                    raise FileNotFoundError("task_root unavailable")
                task = TaskState.load(task_id, task_root)
                snapshot = dict(task.progress_snapshot or {})
                snapshot["execution_health"] = project_execution_health(task)
                return snapshot
            except (ControlPathError, FileNotFoundError, OSError, ValueError, TypeError):
                pass
        return dict(
            dict(job.get("last_result") or {}).get("progress_snapshot") or {}
        )

    def _public_job(self, job: Mapping[str, Any], *, include_requirement: bool = False) -> dict[str, Any]:
        request = dict(job.get("request") or {})
        public_request = {
            "client_job_id": request.get("client_job_id", ""),
            "worker": request.get("worker", ""),
            "model": request.get("model", ""),
            "reasoning_effort": request.get("reasoning_effort", ""),
            "target_modules": list(request.get("target_modules") or []),
            "target_resources": list(request.get("target_resources") or []),
            "commit_summary": request.get("commit_summary", ""),
            "max_outer_attempts": request.get("max_outer_attempts", 0),
        }
        for optional in (
            "depends_on", "qa_type", "hold_scope", "machine_verified",
            "policy_refs", "policy_overlays", "context_refs",
        ):
            if optional in request:
                public_request[optional] = request[optional]
        if include_requirement:
            public_request["requirement"] = request.get("requirement", "")
        progress = self._task_progress(job)
        readiness = dict(job.get("last_result") or {})
        repository = getattr(self.store, "repository", None)
        if repository is not None:
            readiness = repository.product_readiness(str(job.get("job_id", "")))
        result = {
            "job_id": job.get("job_id", ""),
            "client_job_id": job.get("client_job_id", ""),
            "batch_id": job.get("batch_id", ""),
            "sequence": job.get("sequence", 0),
            "status": job.get("status", ""),
            "next_action": _next_action(str(job.get("status", ""))),
            "outer_attempt": job.get("outer_attempt", 0),
            "max_outer_attempts": job.get("max_outer_attempts", 0),
            "task_ids": list(job.get("task_ids") or []),
            "request": public_request,
            "last_result": dict(job.get("last_result") or {}),
            "created_at": job.get("created_at", ""),
            "updated_at": job.get("updated_at", ""),
            "revision": job.get("revision", 0),
            "technical_retry_route": dict(job.get("technical_retry_route") or {}),
            "technical_recovery_attempt_count": int(
                job.get("technical_recovery_attempt_count", 0)
            ),
            "normal_attempt_budget": int(
                job.get("normal_attempt_budget", job.get("max_outer_attempts", 0))
            ),
            "technical_recovery_budget": int(job.get("technical_recovery_budget", 1)),
            "active_attempt_reservation": dict(job.get("active_attempt_reservation") or {}),
            "attempt_lineage": lineage_status(job),
            "dependency_semantics": job.get("dependency_semantics", "LEGACY_SEQUENCE"),
            "depends_on": list(job.get("depends_on") or []),
            "qa_type": job.get("qa_type", ""),
            "hold_scope": job.get("hold_scope", ""),
            "machine_verified": bool(job.get("machine_verified", False)),
            "candidate_id": job.get("candidate_id", ""),
            "candidate_manifest": job.get("candidate_manifest", ""),
            "speculative_candidate_ids": list(job.get("speculative_candidate_ids") or []),
            "speculative_ready": bool(job.get("speculative_ready", False)),
            "progress_revision": int(progress.get("progress_revision", 0)),
            "progress": progress,
            "crash_salvage": dict(job.get("crash_salvage") or {}),
            "replaces_job_id": job.get("replaces_job_id", ""),
            "corrects_job_id": job.get("corrects_job_id", ""),
            "replacement_kind": job.get("replacement_kind", ""),
            "replacement_anchor": dict(job.get("replacement_anchor") or {}),
            "checkpoint_seed": dict(job.get("checkpoint_seed") or {}),
            "manifest_provenance": dict(job.get("manifest_provenance") or {}),
            "superseded_by_job_id": job.get("superseded_by_job_id", ""),
            "product_readiness": readiness.get("readiness", readiness.get("product_readiness", "FINALIZATION_PENDING")),
            "user_action_required": bool(readiness.get("user_action_required", False)),
            "open_decision_count": int(readiness.get("open_decision_count", 0)),
            "open_decisions": list(readiness.get("open_decisions") or []),
        }
        if include_requirement:
            result["attempts"] = list(job.get("attempts") or [])[-3:]
            result["history"] = list(job.get("history") or [])[-20:]
            result["execution_context"] = dict(job.get("execution_context") or {})
            result["profile_revalidation"] = dict(
                job.get("profile_revalidation") or {}
            )
            result["pre_job_snapshots"] = list(
                job.get("pre_job_snapshots") or []
            )[-5:]
        return result

    def health(self) -> dict[str, Any]:
        self._assert_configuration_current()
        queue = self._queue_snapshot()
        supervisor = self.supervisor.status()
        stale_running = bool(
            (
                queue.get("running_job_id")
                or int(dict(queue.get("counts") or {}).get(RUNNING, 0)) > 0
            )
            and not supervisor.get("running")
        )
        worker_probe = getattr(self.harness, "probe_worker_process_active", None)
        worker_process_active = bool(worker_probe()) if callable(worker_probe) else False
        execution_idle_probe = getattr(self.harness, "probe_execution_idle", None)
        execution_owner_active = bool(supervisor.get("running")) or (
            callable(execution_idle_probe) and not bool(execution_idle_probe())
        )
        orphaned_executions = self.store.inspect_orphaned_running(
            self.configuration["task_root"],
            execution_owner_active=execution_owner_active,
            worker_process_active=worker_process_active,
            pid_is_active=getattr(
                self.harness, "probe_execution_pid_active", None
            ),
        )
        inspector_recovery = (
            self.store.read_only_recovery_status()
            if self.read_only
            else {
                "required": False,
                "code": "",
                "reasons": [],
                "preparing_batches": 0,
                "invalid_execution_contexts": 0,
                "legacy_identifiers": 0,
            }
        )
        recovery_required = bool(orphaned_executions) or stale_running or bool(inspector_recovery["required"])
        recovery_code = (
            "READ_ONLY_RECOVERY_REQUIRED"
            if self.read_only and recovery_required
            else "ORPHANED_RUNNING_EXECUTION"
            if orphaned_executions
            else "STALE_RUNNING_REQUIRES_START_RECONCILIATION"
            if stale_running
            else ""
        )
        from codex_runtime_adapter import CodexRuntimeAdapter
        from droid_runtime_adapter import DroidRuntimeAdapter
        from opencode_runtime_adapter import OpenCodeRuntimeAdapter, PERSONAL
        from runtime_selection import select_runtime
        runtime_adapters = {
            descriptor.runtime: descriptor.public()
            for descriptor in (
                CodexRuntimeAdapter().preflight(),
                DroidRuntimeAdapter().preflight(),
                OpenCodeRuntimeAdapter(mode=PERSONAL).preflight(),
            )
        }
        opencode_selection = select_runtime("opencode")
        return {
            "status": recovery_code or "READY",
            "control_repository": (self.store.repository.health()
                                   if getattr(self.store, "repository", None) is not None
                                   else {"authority": "LEGACY_FILES", "status": "VALID"}),
            **self.runtime_identity.public(),
            "profile_id": self.configuration["profile_id"],
            "working_dir": self.configuration["working_dir"],
            "profile_dir": self.configuration["profile_dir"],
            "task_root": self.configuration["task_root"],
            "control_root": str(self.store.root),
            "available_modules": list(self.allowed_modules),
            "available_codex_models": list(CODEX_MODELS),
            "runtime_adapters": runtime_adapters,
            "opencode": {
                "adapter_implemented": True,
                "provider_setup": opencode_selection.provider_setup,
                "live_model_call": "NOT_RUN" if not opencode_selection.opencode_live_validated else "AVAILABLE",
                "live_canary": "NOT_RUN" if not opencode_selection.opencode_live_validated else "PASS",
                "live_validated": opencode_selection.opencode_live_validated,
                "fallback_runtime": opencode_selection.fallback_runtime,
            },
            "recovery_required": recovery_required,
            "recovery_code": recovery_code,
            "read_only": self.read_only,
            "read_only_recovery": inspector_recovery,
            "orphaned_executions": orphaned_executions,
            "queue": queue,
            "supervisor": supervisor,
            **{
                key: queue[key]
                for key in (
                    "commit_policy",
                    "cumulative_worktree",
                    "auto_continue_without_commit",
                    "batch_owned_delta_supported",
                    "pre_job_snapshot_supported",
                    "scoped_rollback_supported",
                    "final_commit_required",
                    "active_baseline_id",
                    "task_owned_baseline_files",
                    "external_frozen_baseline_files",
                    "unexpected_runtime_dirty_files",
                )
            },
        }

    def enqueue_jobs(
        self,
        idempotency_key: str,
        jobs: Sequence[Mapping[str, Any]],
        *,
        source_manifest_path: str | Path | None = None,
        enqueue_actor: str = "control",
        batch_execution_policy: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._assert_writable()
        self._assert_configuration_current()
        if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
            raise JobContractError("INVALID_TYPE", "jobs")
        if not 1 <= len(jobs) <= MAX_BATCH_JOBS:
            raise JobContractError("BATCH_SIZE_OUT_OF_RANGE", "jobs")
        requests = [
            JobRequest.from_mapping(item, allowed_modules=self.allowed_modules)
            for item in jobs
        ]
        queue_before = self.store.queue_snapshot()
        if self.configuration.get("commit_policy") == "BATCH_FINAL_COMMIT":
            if not queue_before.get("active_baseline_id"):
                baseline = self.harness.prepare_batch_baseline(
                    int(queue_before.get("generation", 1))
                )
                self.store.activate_cumulative_policy(
                    int(queue_before.get("generation", 1)), baseline
                )
        created, replayed = self.store.enqueue_batch(
            idempotency_key,
            requests,
            execution_context={
                key: self.configuration[key] for key in EXECUTION_CONTEXT_KEYS
            },
            source_manifest_path=source_manifest_path,
            enqueue_actor=enqueue_actor,
            batch_execution_policy=batch_execution_policy,
        )
        return {
            "replayed": replayed,
            "job_count": len(created),
            "jobs": [self._public_job(job) for job in created],
            "queue": self._queue_snapshot(),
        }

    def enqueue_anchored_replacement(
        self,
        replaces_job_id: str,
        replacement_contract: Mapping[str, Any],
        expected_queue_revision: int,
        expected_job_revision: int,
        request_id: str,
        reason: str,
        source_manifest_path: str,
        *,
        checkpoint_manifest_path: str = "",
        actor: str = "harness-supervisor",
        batch_execution_policy: Mapping[str, Any] | None = None,
        policy_resolution: Mapping[str, str] | None = None,
        decision_resolution: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Revision-safe, single-Job corrective/supersede action with an anchor."""
        self._assert_writable()
        self._assert_configuration_current()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", replaces_job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", replaces_job_id)
        try:
            parent = self.store.get_job(replaces_job_id)
            queue = self.store.queue_snapshot()
            request = JobRequest.from_mapping(
                replacement_contract, allowed_modules=self.allowed_modules
            )
            context = {
                key: self.configuration[key] for key in EXECUTION_CONTEXT_KEYS
            }
            try:
                provenance = build_provenance(
                    source_manifest_path,
                    [request.to_dict()],
                    enqueue_request_id=request_id,
                    enqueue_actor=actor,
                    execution_context=context,
                    execution_policy=dict(batch_execution_policy or {}),
                )
            except BatchManifestError as exc:
                raise QueueError(exc.code, exc.detail) from exc
            integrity = self._canonical_integrity_snapshot(queue)
            checkpoint_seed: dict[str, Any] = {}
            fresh_input: dict[str, Any] = {}
            if policy_resolution is not None and decision_resolution is not None:
                raise QueueError("MULTIPLE_RESOLUTION_KINDS_FORBIDDEN")
            if decision_resolution is not None:
                repository = getattr(self.store, "repository", None)
                if repository is None:
                    raise QueueError("DECISION_REPOSITORY_REQUIRED")
                original = dict(parent["request"])
                proposed = request.to_dict()
                if any(proposed.get(k) != v for k, v in original.items() if k != "requirement"):
                    raise QueueError("DECISION_FINALIZATION_SCOPE_CHANGED")
                for decision_id, resolution in decision_resolution.items():
                    repository.resolve_decision(
                        str(decision_id), resolution=str(resolution), expected_status="OPEN"
                    )
                fresh_input = {
                    "kind": "DECISION_FINALIZATION_FRESH_INPUT",
                    "parent_job_id": replaces_job_id,
                    "parent_revision": int(parent.get("revision", 0)),
                    "generation": int(queue.get("generation", 0)),
                    "active_baseline_id": str(queue.get("active_baseline_id", "")),
                    "resolved_decision_ids": sorted(str(key) for key in decision_resolution),
                }
            elif policy_resolution is not None:
                from qa_policy_resolution import validate_resolution, resolution_contract
                from cumulative_policy import CumulativePolicyError
                try:
                    validate_resolution(policy_resolution)
                    if checkpoint_manifest_path or request.requirement != str(parent['current_requirement']) + '\n\n' + resolution_contract(policy_resolution):
                        raise ValueError('QA_POLICY_RESOLUTION_CONTRACT_INVALID')
                    original = dict(parent['request'])
                    proposed = request.to_dict()
                    if any(proposed.get(k) != v for k, v in original.items() if k != 'requirement'):
                        raise ValueError('QA_POLICY_RESOLUTION_SCOPE_CHANGED')
                    task = TaskState.load(str(parent.get('current_claim_task_id', '')), self.harness.task_root)
                    fresh_input = self.harness._cumulative_manager().validate_policy_fresh_input(parent, queue, integrity, task)
                except CumulativePolicyError as exc:
                    raise QueueError(exc.code, exc.detail) from exc
                except ValueError as exc:
                    raise QueueError(str(exc)) from exc
            elif checkpoint_manifest_path:
                try:
                    checkpoint_seed = AttemptCheckpointStore(
                        self.configuration["working_dir"], self.store.root
                    ).validate_corrective_seed(
                        checkpoint_manifest_path,
                        parent_job_id=replaces_job_id,
                        parent_client_job_id=str(parent.get("client_job_id", "")),
                        generation=int(queue.get("generation", 0)),
                        active_baseline_id=str(queue.get("active_baseline_id", "")),
                        allowed_resources=list(request.target_resources),
                        integrity_snapshot=integrity,
                    )
                except AttemptCheckpointError as exc:
                    raise QueueError(exc.code, exc.detail) from exc
            elif dict(parent.get("last_result") or {}).get("checkpoint_status") == "VERIFIED_NO_TASK_DELTA":
                from cumulative_policy import CumulativePolicyError
                try:
                    fresh_input = self.harness._cumulative_manager().validate_no_delta_fresh_input(
                        parent, queue, integrity)
                except CumulativePolicyError as exc:
                    raise QueueError(exc.code, exc.detail) from exc
            job, replayed = self.store.enqueue_anchored_replacement(
                replaces_job_id,
                request,
                expected_queue_revision=expected_queue_revision,
                expected_job_revision=expected_job_revision,
                request_id=request_id,
                reason=reason,
                execution_context=context,
                provenance=provenance,
                integrity_snapshot=integrity,
                checkpoint_seed=checkpoint_seed,
                fresh_input=fresh_input,
                actor=actor,
                policy_resolution=policy_resolution,
                decision_resolution=decision_resolution,
            )
        finally:
            self.harness.release_execution_lease()
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "queue": self._queue_snapshot(),
        }

    def preview_scoped_checkpoint_reconstruction(
        self,
        parent_job_id: str,
        source_checkpoint_manifest_path: str,
        authorized_scope: Sequence[str],
        excluded_scope: Sequence[str],
    ) -> dict[str, Any]:
        """Read-only, fail-closed projection from a failed Job's pre-Job state."""
        self._assert_configuration_current()
        parent = self.store.get_job(parent_job_id)
        queue = self.store.queue_snapshot()
        if parent.get("status") != FAILED_FINAL:
            raise QueueError("REPLACEMENT_PARENT_NOT_FAILED_FINAL", parent_job_id)
        if str(parent.get("pre_job_snapshot_manifest", "")) == "":
            raise QueueError("CHECKPOINT_PRE_JOB_SNAPSHOT_MISSING", parent_job_id)
        integrity = self._canonical_integrity_snapshot(queue)
        if integrity.get("external_frozen_integrity") is not True:
            raise QueueError("CHECKPOINT_EXTERNAL_INTEGRITY_FAILED", parent_job_id)
        if integrity.get("baseline_declaration_integrity") is not True:
            raise QueueError("CHECKPOINT_BASELINE_INTEGRITY_FAILED", parent_job_id)
        if list(integrity.get("unexpected_runtime_dirty_files") or []):
            raise QueueError("CHECKPOINT_UNEXPECTED_DIRTY", parent_job_id)
        try:
            preview = AttemptCheckpointStore(
                self.configuration["working_dir"], self.store.root
            ).preview_scoped_reconstruction(
                source_checkpoint_manifest_path,
                authorized_scope=authorized_scope,
                excluded_scope=excluded_scope,
            )
        except AttemptCheckpointError as exc:
            raise QueueError(exc.code, exc.detail) from exc
        if preview.get("source", {}).get("job_id") != parent_job_id:
            raise QueueError("CHECKPOINT_JOB_MISMATCH", parent_job_id)
        preview.pop("source", None)
        preview.pop("selected_files", None)
        return {"preview": preview, "queue": self._queue_snapshot()}

    def create_scoped_checkpoint_reconstruction(
        self,
        parent_job_id: str,
        source_checkpoint_manifest_path: str,
        authorized_scope: Sequence[str],
        excluded_scope: Sequence[str],
        expected_queue_revision: int,
        expected_job_revision: int,
        source_manifest_sha256: str,
        authorized_scope_sha256: str,
        request_id: str,
        reason: str = "SCOPED_CHECKPOINT_RECONSTRUCTION",
    ) -> dict[str, Any]:
        """Create one immutable derived checkpoint with revision-safe provenance."""
        self._assert_writable()
        self._assert_configuration_current()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", parent_job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", parent_job_id)
        try:
            parent = self.store.get_job(parent_job_id)
            queue = self.store.queue_snapshot()
            if int(queue.get("revision", -1)) != int(expected_queue_revision):
                raise QueueError("STALE_QUEUE_REVISION", parent_job_id)
            if int(parent.get("revision", -1)) != int(expected_job_revision):
                raise QueueError("STALE_JOB_REVISION", parent_job_id)
            if parent.get("status") != FAILED_FINAL:
                raise QueueError("REPLACEMENT_PARENT_NOT_FAILED_FINAL", parent_job_id)
            integrity = self._canonical_integrity_snapshot(queue)
            if integrity.get("external_frozen_integrity") is not True:
                raise QueueError("CHECKPOINT_EXTERNAL_INTEGRITY_FAILED", parent_job_id)
            if integrity.get("baseline_declaration_integrity") is not True:
                raise QueueError("CHECKPOINT_BASELINE_INTEGRITY_FAILED", parent_job_id)
            if list(integrity.get("unexpected_runtime_dirty_files") or []):
                raise QueueError("CHECKPOINT_UNEXPECTED_DIRTY", parent_job_id)
            store = AttemptCheckpointStore(self.configuration["working_dir"], self.store.root)
            preview = store.preview_scoped_reconstruction(
                source_checkpoint_manifest_path,
                authorized_scope=authorized_scope,
                excluded_scope=excluded_scope,
            )
            if preview.get("source", {}).get("job_id") != parent_job_id:
                raise QueueError("CHECKPOINT_JOB_MISMATCH", parent_job_id)
            if preview["source_manifest_sha256"] != source_manifest_sha256:
                raise QueueError("CHECKPOINT_SOURCE_MANIFEST_CHANGED", parent_job_id)
            if preview["authorized_scope_sha256"] != authorized_scope_sha256:
                raise QueueError("CHECKPOINT_SCOPE_HASH_MISMATCH", parent_job_id)
            result = store.create_scoped_reconstruction(
                source_checkpoint_manifest_path,
                authorized_scope=authorized_scope,
                excluded_scope=excluded_scope,
                request_id=request_id,
                reason=reason,
            )
            queue_after = self.store.queue_snapshot()
            parent_after = self.store.get_job(parent_job_id)
            if int(queue_after.get("revision", -1)) != int(expected_queue_revision) or int(parent_after.get("revision", -1)) != int(expected_job_revision):
                raise QueueError("SCOPED_CHECKPOINT_RECONSTRUCTION_FAILED", "revision changed during reconstruction")
        except AttemptCheckpointError as exc:
            raise QueueError("SCOPED_CHECKPOINT_RECONSTRUCTION_FAILED", f"{exc.code}:{exc.detail}") from exc
        finally:
            self.harness.release_execution_lease()
        result.pop("source", None)
        result.pop("selected_files", None)
        return {"checkpoint": result, "queue": self._queue_snapshot()}

    def record_qa_corrective_successor(
        self,
        qa_job_id: str,
        corrective_job_id: str,
        expected_queue_revision: int,
        expected_job_revision: int,
        request_id: str,
        reason: str,
        confirmation: str,
        actor: str = "harness-supervisor",
    ) -> dict[str, Any]:
        """Revision-safe handoff of a job-scoped QA hold's active path.

        The historical AWAITING_QA Job stays immutable; only the pristine
        QUEUED corrective receives the durable lineage link.
        """
        self._assert_writable()
        self._assert_configuration_current()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", corrective_job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", corrective_job_id)
        try:
            from contextlib import nullcontext
            repository = getattr(self.store, 'repository', None)
            with repository.hold() if repository is not None else nullcontext():
                job, replayed = self.store.record_qa_corrective_successor(
                    qa_job_id,
                    corrective_job_id,
                    expected_queue_revision=expected_queue_revision,
                    expected_job_revision=expected_job_revision,
                    request_id=request_id,
                    reason=reason,
                    confirmation=confirmation,
                    actor=actor,
                )
                if repository is not None and not replayed:
                    source = repository.read_job(qa_job_id)
                    if source.get('active_baseline_id'):
                        repository.bind_corrective_baseline(
                            qa_job_id, corrective_job_id,
                            expected_source_revision=source['revision'], expected_target_revision=job['revision'],
                            integrity_snapshot_provider=self._canonical_integrity_snapshot,
                            execution_safe=lambda: self.harness._lease_held and not self.supervisor.is_running())
                        job = repository.read_job(corrective_job_id)
        finally:
            self.harness.release_execution_lease()
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "queue": self._queue_snapshot(),
        }

    def reproject_strict_batch_result(
        self,
        job_id: str,
        expected_revision: int,
        request_id: str,
        reason: str,
    ) -> dict[str, Any]:
        """Re-record one preserved verified result through the corrected test contract.

        0.9.0.10 recovery for a Job whose only defect was the pre-hotfix
        test_status projection (CUMULATIVE_VERIFICATION_CONTRACT_INVALID with
        a passing official Tester on the preserved Task). Never auto-starts.
        """
        self._assert_writable()
        self._assert_configuration_current()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        try:
            job = self.store.get_job(job_id)
            result = self.harness.reproject_strict_batch_result(job)
            updated, replayed = self.store.record_review_only_result(
                job_id,
                expected_revision=expected_revision,
                request_id=request_id,
                result=result,
            )
        finally:
            self.harness.release_execution_lease()
        return {
            "replayed": replayed,
            "reason": scrub_secrets(str(reason))[:500],
            "job": self._public_job(updated, include_requirement=True),
            "queue": self._queue_snapshot(),
        }

    def start_queue(self) -> dict[str, Any]:
        self._assert_writable()
        self._assert_configuration_current()
        stale_gate_cleared = (
            self.store.clear_stale_terminal_predecessor_gate_for_batch_head()
        )
        started = self.supervisor.start()
        return {
            "supervisor": started,
            "queue": self._queue_snapshot(),
            "stale_terminal_predecessor_gate_cleared": stale_gate_cleared,
        }

    def get_queue(self) -> dict[str, Any]:
        return {
            "queue": self._queue_snapshot(),
            "supervisor": self.supervisor.status(),
        }

    def get_worklist(self) -> dict[str, Any]:
        repository = getattr(self.store, "repository", None)
        if repository is None:
            raise QueueError("SQLITE_CONTROL_REQUIRED")
        from sleep_disposition import disposition
        queue = repository.read_queue()
        worklist = repository.worklist()
        for row in worklist:
            row["disposition"] = disposition(repository.read_job(row["job_id"]), queue)
        return {"control_repository": repository.health(), "worklist": worklist,
                "global_stop": repository.global_stop()}

    def revise_planned_job(self, job_id: str, expected_revision: int, contract: dict[str, Any]) -> dict[str, Any]:
        self._assert_writable()
        repository = getattr(self.store, "repository", None)
        if repository is None:
            raise QueueError("SQLITE_CONTROL_REQUIRED")
        request = JobRequest.from_mapping(contract, allowed_modules=self.allowed_modules)
        return repository.revise_planned(job_id, request.to_dict(), expected_revision=expected_revision)

    def bind_corrective_baseline(self, source_job_id, target_job_id, expected_source_revision,
                                 expected_target_revision, preview=False):
        repository = getattr(self.store, 'repository', None)
        if repository is None:
            raise QueueError('SQLITE_CONTROL_REQUIRED')
        self._assert_configuration_current()
        if not preview:
            self._assert_writable()
            if self.supervisor.is_running() or not self.harness.acquire_execution_lease():
                raise QueueError('ACTIVE_EXECUTION')
        try:
            return repository.bind_corrective_baseline(source_job_id, target_job_id,
                expected_source_revision=expected_source_revision, expected_target_revision=expected_target_revision,
                integrity_snapshot_provider=self._canonical_integrity_snapshot,
                execution_safe=lambda: not self.supervisor.is_running() and (
                    self.harness.probe_execution_idle() if preview else self.harness._lease_held), preview=preview)
        finally:
            if not preview:
                self.harness.release_execution_lease()

    def preview_candidate_ownership_handoff(
        self, source_job_id: str, target_job_id: str = '',
        expected_source_revision: int | None = None, expected_target_revision: int | None = None,
        candidate_scope: list[str] | None = None, expected_candidate_hash: str | None = None,
    ) -> dict[str, Any]:
        repository = getattr(self.store, 'repository', None)
        if repository is None:
            raise QueueError('SQLITE_CONTROL_REQUIRED')
        return repository.preview_candidate_ownership_handoff(source_job_id, target_job_id,
            workspace=self.harness.working_dir,
            integrity_snapshot_provider=self._canonical_integrity_snapshot,
            execution_safe=lambda: not self.supervisor.is_running() and self.harness.probe_execution_idle(),
            expected_source_revision=expected_source_revision, expected_target_revision=expected_target_revision,
            candidate_scope=candidate_scope, expected_candidate_hash=expected_candidate_hash)

    def handoff_candidate_ownership(
        self, source_job_id: str, target_job_id: str, expected_source_revision: int,
        expected_target_revision: int, candidate_scope: list[str], expected_candidate_hash: str,
        confirmation: str, request_id: str,
    ) -> dict[str, Any]:
        self._assert_writable()
        self._assert_configuration_current()
        repository = getattr(self.store, 'repository', None)
        if repository is None:
            raise QueueError('SQLITE_CONTROL_REQUIRED')
        if self.supervisor.is_running() or not self.harness.acquire_execution_lease():
            raise QueueError('ACTIVE_EXECUTION')
        try:
            return repository.handoff_candidate_ownership(source_job_id, target_job_id,
                expected_source_revision=expected_source_revision, expected_target_revision=expected_target_revision,
                candidate_scope=candidate_scope, expected_candidate_hash=expected_candidate_hash,
                confirmation=confirmation, request_id=request_id, workspace=self.harness.working_dir,
                integrity_snapshot_provider=self._canonical_integrity_snapshot,
                execution_safe=lambda: self.harness._lease_held and not self.supervisor.is_running())
        finally:
            self.harness.release_execution_lease()

    def release_global_stop(self, expected_revision: int, reason: str, confirmation: str) -> dict[str, Any]:
        self._assert_writable()
        if confirmation != 'RELEASE_GLOBAL_STOP_WITHOUT_DISPATCH' or not str(reason).strip():
            raise QueueError('CONFIRMATION_REQUIRED')
        self._assert_configuration_current()
        repository = getattr(self.store,'repository',None)
        if repository is None:
            raise QueueError('SQLITE_CONTROL_REQUIRED')
        queue = self._queue_snapshot()
        if (queue.get('unexpected_runtime_dirty_files') or queue.get('baseline_declaration_integrity') is False
                or queue.get('external_frozen_integrity') is False or self.supervisor.is_running()):
            raise QueueError('GLOBAL_STOP_INTEGRITY_UNRESOLVED')
        return {'global_stop':repository.release_global_stop(expected_revision=expected_revision,reason=reason),'dispatched':False}

    def list_jobs(self, limit: int = 50) -> dict[str, Any]:
        jobs = self.store.list_jobs(limit=limit)
        return {
            "jobs": [self._public_job(job) for job in jobs],
            "queue": self._queue_snapshot(),
        }

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._public_job(self.store.get_job(job_id), include_requirement=True)

    def wait_for_job(
        self,
        job_id: str,
        timeout_seconds: int = 20,
        after_progress_revision: int | None = None,
    ) -> dict[str, Any]:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int):
            raise QueueError("INVALID_TIMEOUT")
        if not 0 <= timeout_seconds <= 50:
            raise QueueError("INVALID_TIMEOUT")
        if (
            after_progress_revision is not None
            and (
                isinstance(after_progress_revision, bool)
                or not isinstance(after_progress_revision, int)
                or after_progress_revision < 0
            )
        ):
            raise QueueError("INVALID_PROGRESS_REVISION")
        deadline = time.monotonic() + timeout_seconds
        while True:
            job = self.store.get_job(job_id)
            current_progress = int(
                self._task_progress(job).get("progress_revision", 0)
            )
            progress_changed = (
                after_progress_revision is not None
                and current_progress > after_progress_revision
            )
            if (
                progress_changed
                or job.get("status") not in (QUEUED, RUNNING)
                or time.monotonic() >= deadline
            ):
                return self._public_job(job, include_requirement=True)
            time.sleep(0.25)

    def _start_if_unpaused(self, start_immediately: bool) -> dict[str, Any]:
        self._assert_writable()
        if start_immediately:
            self._assert_configuration_current()
            return self.supervisor.start()
        return self.supervisor.status()

    def continue_after_success(
        self,
        job_id: str,
        request_id: str,
        *,
        expected_revision: int | None = None,
        batch_id: str = "",
        start_immediately: bool = True,
    ) -> dict[str, Any]:
        self._assert_writable()
        job, replayed = self.store.acknowledge_success(
            job_id,
            request_id,
            expected_revision=expected_revision,
            batch_id=batch_id,
        )
        start_result = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "supervisor": start_result,
            "queue": self._queue_snapshot(),
        }

    def preview_integration_recovery(
        self, job_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """Prove an EOL-only recovery is safe without mutating source or state."""
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job = self.store.get_job(job_id)
        queue = self.store.queue_snapshot()
        if (
            job.get("status") != AWAITING_QA
            or int(job.get("revision", -1)) != int(expected_revision)
            or queue.get("running_job_id")
            or queue.get("blocked_by_job_id") != job_id
        ):
            raise QueueError("INTEGRATION_RECOVERY_STATE_INVALID", job_id)
        result = dict(job.get("last_result") or {})
        task_id = str(result.get("task_id", ""))
        task = TaskState.load(task_id, self.harness.task_root) if task_id else None
        if task is None or task.job_id != job_id:
            raise QueueError("INTEGRATION_RECOVERY_LINEAGE_INVALID", job_id)
        if not task.commit_blockers or any(
            not str(item).startswith("EOL_REPAIR_ERROR:") for item in task.commit_blockers
        ):
            raise QueueError("INTEGRATION_RECOVERY_BLOCKER_NOT_ELIGIBLE", job_id)
        if (
            task.build.get("status") != "PASS"
            or (task.test_required and task.test_status != "PASS")
            or task.review_status != "REVIEW_PASS"
            or task.verification_status != "VERIFIED"
        ):
            raise QueueError("INTEGRATION_RECOVERY_EVIDENCE_NOT_FRESH", job_id)
        source_hashes = {
            relative: hashlib.sha256((self.harness.working_dir / relative).read_bytes()).hexdigest()
            for relative in sorted(set(task.changed_files))
            if (self.harness.working_dir / relative).is_file()
        }
        if source_hashes != dict(task.changed_file_sha256 or {}):
            raise QueueError("INTEGRATION_RECOVERY_SOURCE_HASH_STALE", job_id)
        try:
            baseline = self.harness._cumulative_manager().load_job_baseline(
                task.pre_job_snapshot_manifest
            )
        except Exception as exc:
            raise QueueError("INTEGRATION_RECOVERY_BASELINE_INVALID", job_id) from exc
        profile, _ = self.harness._load_profile()
        eol = GitCollector(str(self.harness.working_dir), profile).inspect_eol_to_index(
            task, baseline
        )
        if not eol.get("success"):
            raise QueueError("INTEGRATION_RECOVERY_EOL_MISMATCH", job_id)
        integrity = self._canonical_integrity_snapshot(queue)
        unknown = sorted(
            set(integrity.get("unexpected_runtime_dirty_files") or [])
            - set(task.changed_files)
        )
        if (
            integrity.get("active_baseline_id") != task.active_baseline_id
            or integrity.get("external_frozen_integrity") is not True
            or integrity.get("baseline_declaration_integrity") is not True
            or unknown
        ):
            raise QueueError("INTEGRATION_RECOVERY_WORKTREE_INTEGRITY_INVALID", job_id)
        return {
            "eligible": True,
            "decision": "ELIGIBLE_WORKER_FREE_INTEGRATION_RECOVERY",
            "job_id": job_id,
            "task_id": task_id,
            "expected_revision": expected_revision,
            "worker_will_run": False,
            "source_sha256": source_hashes,
            "eol": eol,
            "build_reused": True,
            "review_reused": True,
            "verification_reused": True,
            "integrity_snapshot": integrity,
            "authorized_task_delta_excluded_from_unknown": sorted(
                set(integrity.get("unexpected_runtime_dirty_files") or []) & set(task.changed_files)
            ),
        }

    def recover_integration_only(
        self,
        job_id: str,
        *,
        expected_revision: int,
        request_id: str,
        acknowledge_and_continue: bool = False,
    ) -> dict[str, Any]:
        """Revision-safe, idempotent integration recovery with no Worker."""
        self._assert_writable()
        self.preview_integration_recovery(job_id, expected_revision)
        if not self.harness.acquire_execution_lease():
            raise HarnessServiceError("CONTROL_EXECUTION_BUSY")
        try:
            job = self.store.get_job(job_id)
            result = self.harness.recover_integration_only(job)
            updated, replayed = self.store.record_review_only_result(
                job_id,
                expected_revision=expected_revision,
                request_id=request_id,
                result=result,
            )
            self.supervisor.notify_terminal_transition(updated, self.store.queue_snapshot())
        finally:
            self.harness.release_execution_lease()
        acknowledged = False
        if acknowledge_and_continue and updated.get("status") == SUCCEEDED:
            updated, _ = self.store.acknowledge_success(
                job_id,
                f"{request_id}-ack",
                expected_revision=int(updated.get("revision", 0)),
                batch_id=str(updated.get("batch_id", "")),
            )
            acknowledged = True
            supervisor = self._start_if_unpaused(True)
        else:
            supervisor = self.supervisor.status()
        return {
            "replayed": replayed,
            "acknowledged": acknowledged,
            "job": self._public_job(updated, include_requirement=True),
            "queue": self._queue_snapshot(),
            "supervisor": supervisor,
        }

    def preview_qa_candidate_isolation(
        self, job_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """Prove an existing structured-QA delta can be isolated without a Worker."""
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job = self.store.get_job(job_id)
        queue = self.store.queue_snapshot()
        if (
            job.get("status") != AWAITING_QA
            or int(job.get("revision", -1)) != int(expected_revision)
            or queue.get("running_job_id")
            or queue.get("blocked_by_job_id") != job_id
            or str(job.get("candidate_id", ""))
        ):
            raise QueueError("QA_CANDIDATE_ISOLATION_STATE_INVALID", job_id)
        result = dict(job.get("last_result") or {})
        task_id = str(result.get("task_id", ""))
        task = TaskState.load(task_id, self.harness.task_root) if task_id else None
        if task is None or task.job_id != job_id:
            raise QueueError("QA_CANDIDATE_ISOLATION_LINEAGE_INVALID", job_id)
        qa_request = dict(result.get("qa_request") or task.qa_request or {})
        qa_type = str(qa_request.get("qa_type") or job.get("qa_type", ""))
        hold_scope = str(qa_request.get("hold_scope") or job.get("hold_scope", ""))
        if qa_request.get("required") is not True:
            raise QueueError("QA_CANDIDATE_ISOLATION_QA_INVALID", job_id)
        try:
            validate_qa_policy(qa_type, hold_scope, False)
        except Exception as exc:
            raise QueueError("QA_CANDIDATE_ISOLATION_QA_INVALID", job_id) from exc
        changed = list(task.task_owned_changed_files or task.changed_files)
        if not changed or not task.pre_job_snapshot_manifest:
            raise QueueError("QA_CANDIDATE_ISOLATION_DELTA_INVALID", job_id)
        source_hashes = {
            relative: hashlib.sha256((self.harness.working_dir / relative).read_bytes()).hexdigest()
            for relative in changed
            if (self.harness.working_dir / relative).is_file()
        }
        recorded_hashes = {
            relative: digest for relative, digest in dict(task.changed_file_sha256 or {}).items()
            if relative in set(changed)
        }
        if recorded_hashes and source_hashes != recorded_hashes:
            raise QueueError("QA_CANDIDATE_ISOLATION_SOURCE_HASH_STALE", job_id)
        integrity = self._canonical_integrity_snapshot(queue)
        unknown = sorted(
            set(integrity.get("unexpected_runtime_dirty_files") or []) - set(changed)
        )
        if (
            integrity.get("active_baseline_id") != task.active_baseline_id
            or integrity.get("external_frozen_integrity") is not True
            or integrity.get("baseline_declaration_integrity") is not True
            or unknown
        ):
            raise QueueError("QA_CANDIDATE_ISOLATION_INTEGRITY_INVALID", job_id)
        return {
            "eligible": True,
            "decision": "ELIGIBLE_WORKER_FREE_QA_CANDIDATE_ISOLATION",
            "job_id": job_id,
            "task_id": task_id,
            "expected_revision": expected_revision,
            "worker_will_run": False,
            "qa_type": qa_type,
            "hold_scope": hold_scope,
            "changed_files": changed,
            "source_sha256": source_hashes,
            "integrity_snapshot": integrity,
        }

    def isolate_qa_candidate(
        self,
        job_id: str,
        *,
        expected_revision: int,
        request_id: str,
    ) -> dict[str, Any]:
        """Materialize and rollback one preserved structured-QA delta; never run Worker."""
        self._assert_writable()
        preview = self.preview_qa_candidate_isolation(job_id, expected_revision)
        if not self.harness.acquire_execution_lease():
            raise HarnessServiceError("CONTROL_EXECUTION_BUSY")
        try:
            job = self.store.get_job(job_id)
            prior = dict(job.get("last_result") or {})
            task = TaskState.load(str(preview["task_id"]), self.harness.task_root)
            task.qa_type = str(preview["qa_type"])
            task.hold_scope = str(preview["hold_scope"])
            task.machine_verified = False
            technical_failure = {
                "code": str(prior.get("failure_code", "")),
                "origin": str(prior.get("failure_origin", "")),
                "stage": str(prior.get("failure_stage", "")),
                "reason": str(prior.get("failure_reason", "")),
            }
            task.qa_secondary_failures = [technical_failure]
            profile, _ = self.harness._load_profile()
            candidate = QACandidateStore(
                self.store.root / "candidates", self.harness.working_dir, profile
            ).quarantine_and_rollback(task)
            task.status = "AWAITING_QA"
            task.stage = "AWAITING_QA"
            task.user_intervention_reason = "USER_QA_REQUIRED"
            task.failure_origin = "USER_QA"
            task.failure_code = "USER_QA_REQUIRED"
            task.candidate_id = str(candidate["candidate_id"])
            task.candidate_manifest = str(candidate["manifest_path"])
            task.worktree_disposition = "CLEAN_ROLLBACK"
            task.execution_summary = build_execution_summary(task)
            task.save(self.harness.task_root)
            self.harness.reporter.save_markdown(
                task,
                model=str(dict(job.get("request") or {}).get("model", "")),
                out_dir=self.harness.task_root,
            )
            result = {
                **prior,
                "recovery_kind": "QA_CANDIDATE_ISOLATION",
                "status": "AWAITING_QA",
                "stage": "AWAITING_QA",
                "success": False,
                "failure_origin": "USER_QA",
                "failure_code": "USER_QA_REQUIRED",
                "user_qa_required": True,
                "qa_type": task.qa_type,
                "hold_scope": task.hold_scope,
                "machine_verified": False,
                "candidate_id": candidate["candidate_id"],
                "candidate_manifest": candidate["manifest_path"],
                "candidate_kind": "QA_QUARANTINE",
                "worktree_disposition": "CLEAN_ROLLBACK",
                "technical_failure": technical_failure,
                "secondary_failures": list(dict.fromkeys([
                    *list(prior.get("secondary_failures") or []),
                    technical_failure["code"],
                ])),
                "worker_invoked": False,
                "execution_summary": dict(task.execution_summary),
            }
            updated, replayed = self.store.record_review_only_result(
                job_id,
                expected_revision=expected_revision,
                request_id=request_id,
                result=result,
            )
            self.supervisor.notify_terminal_transition(updated, self.store.queue_snapshot())
        finally:
            self.harness.release_execution_lease()
        return {
            "replayed": replayed,
            "job": self._public_job(updated, include_requirement=True),
            "queue": self._queue_snapshot(),
            "supervisor": self.supervisor.status(),
        }

    def preview_qa_candidate_resolution(
        self, job_id: str, expected_revision: int
    ) -> dict[str, Any]:
        """Verify a quarantined candidate can be reused by the same Job."""
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job = self.store.get_job(job_id)
        queue = self.store.queue_snapshot()
        candidate_id = str(job.get("candidate_id", ""))
        if (
            job.get("status") != AWAITING_QA
            or int(job.get("revision", -1)) != int(expected_revision)
            or not candidate_id
            or queue.get("running_job_id")
        ):
            raise QueueError("QA_CANDIDATE_RESOLUTION_STATE_INVALID", job_id)
        profile, _ = self.harness._load_profile()
        store = QACandidateStore(
            self.store.root / "candidates", self.harness.working_dir, profile
        )
        candidate = store.load(candidate_id)
        if (
            str(candidate.get("source_job_id", "")) != job_id
            or str(candidate.get("source_task_id", ""))
            != str(dict(job.get("last_result") or {}).get("task_id", ""))
        ):
            raise QueueError("QA_CANDIDATE_RESOLUTION_LINEAGE_INVALID", job_id)
        compatibility = store.compatibility(candidate_id)
        integrity = self._canonical_integrity_snapshot(queue)
        if (
            not compatibility.get("compatible")
            or integrity.get("external_frozen_integrity") is not True
            or integrity.get("baseline_declaration_integrity") is not True
            or list(integrity.get("unexpected_runtime_dirty_files") or [])
        ):
            raise QueueError("QA_CANDIDATE_RESOLUTION_INTEGRITY_INVALID", job_id)
        return {
            "eligible": True,
            "decision": "ELIGIBLE_SAME_JOB_CANDIDATE_REUSE",
            "job_id": job_id,
            "expected_revision": expected_revision,
            "candidate_id": candidate_id,
            "candidate_revision": int(candidate.get("candidate_revision", 0)),
            "changed_files": list(dict(candidate.get("delta") or {}).get("changed_files") or []),
            "compatibility": compatibility,
            "integrity_snapshot": integrity,
            "worker_route": dict(job.get("request") or {}).get("worker", ""),
            "model_route": dict(job.get("request") or {}).get("model", ""),
        }

    def resolve_qa_candidate_for_retry(
        self,
        job_id: str,
        *,
        expected_revision: int,
        resolution_text: str,
        request_id: str,
        start_immediately: bool = True,
    ) -> dict[str, Any]:
        """Record explicit user contract resolution and rerun the same Job lineage."""
        self._assert_writable()
        preview = self.preview_qa_candidate_resolution(job_id, expected_revision)
        job, replayed = self.store.resolve_qa_candidate_for_retry(
            job_id,
            expected_revision=expected_revision,
            resolution_text=resolution_text,
            candidate_id=str(preview["candidate_id"]),
            request_id=request_id,
        )
        supervisor = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "queue": self._queue_snapshot(),
            "supervisor": supervisor,
        }

    def retry_review_only(
        self,
        job_id: str,
        *,
        expected_revision: int,
        request_id: str,
        confirmation: str,
    ) -> dict[str, Any]:
        """Retry only an isolated Reviewer after Reviewer infrastructure QA."""
        self._assert_writable()
        if confirmation != "I_CONFIRM_RETRY_REVIEW":
            raise QueueError("RETRY_REVIEW_CONFIRMATION_REQUIRED")
        action_key = self.store.action_key(request_id)
        job = self.store.get_job(job_id)
        prior = dict(job.get("review_only_actions") or {}).get(action_key)
        if prior is not None:
            if prior.get("expected_revision") != expected_revision:
                raise QueueError("IDEMPOTENCY_CONFLICT", job_id)
            return {
                "replayed": True,
                "job": self._public_job(job, include_requirement=True),
                "queue": self._queue_snapshot(),
                "supervisor": self.supervisor.status(),
            }
        if int(job.get("revision", -1)) != int(expected_revision):
            raise QueueError("JOB_REVISION_CONFLICT", job_id)
        result = dict(job.get("last_result") or {})
        task_id = str(result.get("task_id", ""))
        task = TaskState.load(task_id, self.harness.task_root) if task_id else None
        control_recovery = bool(
            task is not None
            and is_control_state_write_failure(
                task.failure_code,
                failure_stage=task.failure_stage,
                worker_stderr=task.worker_stderr,
            )
        )
        reviewer_recovery = (
            result.get("failure_origin") == "TOOL_INFRA"
            and result.get("failure_stage") == "REVIEW"
        )
        try:
            parsed_qa = parse_qa_request(task.worker_stdout) if task is not None else {"invalid": True}
        except ValueError:
            parsed_qa = {"invalid": True}
        post_worker_recovery = bool(
            task is not None
            and task.failure_code == "CONTROL_EXECUTION_ERROR"
            and task.failure_stage == "CONTROL"
            and dict(task.worker_execution_evidence or {}).get("exit_code") == 0
            and dict(task.worker_execution_evidence or {}).get("timed_out") is not True
            and parsed_qa == {}
            and "HARNESS_QA_REQUEST_JSON:" in task.worker_stdout
        )
        completed_post_worker_recovery = bool(
            task is not None
            and task.failure_stage == "REVIEW"
            and task.review_status in {"REVIEW_PASS", "REVIEW_FAIL"}
            and task.build.get("status") == "PASS"
            and dict(task.control_recovery_evidence or {}).get("kind")
            == "POST_WORKER_QA_PARSE_RECOVERY"
            and dict(task.control_recovery_evidence or {}).get("worker_invoked") is False
            and dict(task.control_recovery_evidence or {}).get("worker_result_reused") is True
            and result.get("failure_code") == "CONTROL_EXECUTION_ERROR"
            and result.get("failure_stage") == "CONTROL"
        )
        post_worker_recovery = post_worker_recovery or completed_post_worker_recovery
        if job.get("status") != AWAITING_QA or not (
            control_recovery or reviewer_recovery or post_worker_recovery
        ):
            raise QueueError("REVIEW_ONLY_RETRY_NOT_ELIGIBLE", job_id)
        if control_recovery:
            self._preview_control_recovery(job, expected_revision)
        elif post_worker_recovery:
            queue = self.store.queue_snapshot()
            snapshot_manifest = task.pre_job_snapshot_manifest or str(
                job.get("pre_job_snapshot_manifest", "")
            )
            try:
                baseline = self.harness._cumulative_manager(read_only=True).load_job_baseline(
                    snapshot_manifest
                )
                probe = copy.deepcopy(task)
                git_result = GitCollector(str(self.harness.working_dir)).collect(
                    task=probe, baseline=baseline
                )
            except Exception as exc:
                raise QueueError("POST_WORKER_RECOVERY_BASELINE_INVALID", job_id) from exc
            changed = list(git_result.changed_files)
            integrity = self._canonical_integrity_snapshot(queue)
            if not git_result.success or not _post_worker_recovery_integrity_valid(
                changed, integrity
            ):
                raise QueueError("POST_WORKER_RECOVERY_INTEGRITY_INVALID", job_id)
        self._assert_configuration_current()
        if self.supervisor.is_running() or not self.harness.acquire_execution_lease():
            raise HarnessServiceError("CONTROL_EXECUTION_BUSY")
        try:
            review_result = (
                self.harness.post_worker_recovery_outcome(job)
                if completed_post_worker_recovery
                else self.harness.retry_review_only(job)
            )
            updated, replayed = self.store.record_review_only_result(
                job_id,
                expected_revision=expected_revision,
                request_id=request_id,
                result=review_result,
            )
            self.supervisor.notify_terminal_transition(
                updated, self.store.queue_snapshot()
            )
        finally:
            self.harness.release_execution_lease()
        return {
            "replayed": replayed,
            "job": self._public_job(updated, include_requirement=True),
            "queue": self._queue_snapshot(),
            "supervisor": self.supervisor.status(),
        }

    def commit_job_result(
        self,
        job_id: str,
        request_id: str,
        *,
        acknowledge_and_continue: bool = False,
    ) -> dict[str, Any]:
        """Commit one verified Job's exact files and optionally advance FIFO."""
        self._assert_writable()
        job = self.store.get_job(job_id)
        action_key = self.store.action_key(request_id)
        prior = dict(job.get("commit_actions") or {}).get(action_key)
        if prior is not None:
            committed, replayed = job, True
            commit_result = {
                "status": dict(job.get("last_result") or {}).get("commit_status", ""),
                "commits": list(dict(job.get("last_result") or {}).get("commits") or []),
            }
        else:
            self._assert_configuration_current()
            if self.supervisor.is_running() or not self.harness.acquire_execution_lease():
                raise HarnessServiceError("CONTROL_EXECUTION_BUSY")
            try:
                commit_result = self.harness.commit_verified_job(job)
            finally:
                self.harness.release_execution_lease()
            committed, replayed = self.store.record_commit_result(
                job_id, request_id, commit_result
            )
        supervisor = self.supervisor.status()
        acknowledged = False
        if acknowledge_and_continue:
            ack_id = f"ack-{job_id}-after-commit-v1"
            committed, _ = self.store.acknowledge_success(
                job_id,
                ack_id,
                expected_revision=int(committed.get("revision", 0)),
                batch_id=str(committed.get("batch_id", "")),
            )
            supervisor = self._start_if_unpaused(True)
            acknowledged = True
        return {
            "replayed": replayed,
            "acknowledged": acknowledged,
            "commit_result": commit_result,
            "job": self._public_job(committed, include_requirement=True),
            "supervisor": supervisor,
            "queue": self._queue_snapshot(),
        }

    def set_execution_mode(
        self,
        mode: str,
        request_id: str,
        confirmation: str = "",
        *,
        expected_revision: int | None = None,
        batch_id: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        self._assert_writable()
        if self.supervisor.is_running():
            raise QueueError("EXECUTION_MODE_CHANGE_WHILE_RUNNING")
        queue, replayed = self.store.set_execution_mode(
            mode,
            request_id,
            confirmation,
            expected_revision=expected_revision,
            batch_id=batch_id,
            reason=reason,
        )
        return {"replayed": replayed, "queue": self._queue_snapshot()}

    def reload_idle_profile(self, confirmation: str) -> dict[str, Any]:
        """Reload profile/policy fingerprints without restarting the MCP process."""
        self._assert_writable()
        if confirmation != "I_CONFIRM_RELOAD_IDLE_PROFILE":
            raise QueueError("PROFILE_RELOAD_CONFIRMATION_REQUIRED")
        queue = self._queue_snapshot()
        if (
            self.supervisor.is_running()
            or queue.get("running_job_id")
            or int(queue.get("total", 0)) != 0
            or not self.harness.probe_execution_idle()
        ):
            raise QueueError("PROFILE_RELOAD_REQUIRES_EMPTY_IDLE_QUEUE")
        previous = dict(self.configuration)
        current = self.harness.validate_configuration()
        self.configuration = current
        self.allowed_modules = list(current["available_modules"])
        changed = [
            key for key in EXECUTION_CONTEXT_KEYS
            if previous.get(key) != current.get(key)
        ]
        return {
            "reloaded": True,
            "changed_context_keys": sorted(changed),
            "profile_id": current["profile_id"],
            "queue": self._queue_snapshot(),
            "supervisor": self.supervisor.status(),
        }

    def cancel_queued_batch(
        self,
        batch_id: str,
        reason: str,
        request_id: str,
    ) -> dict[str, Any]:
        self._assert_writable()
        jobs, replayed = self.store.cancel_queued_batch(
            batch_id, reason, request_id
        )
        return {
            "replayed": replayed,
            "cancelled_count": len(jobs),
            "jobs": [self._public_job(job) for job in jobs],
            "queue": self._queue_snapshot(),
        }

    def reset_terminal_queue(
        self,
        request_id: str,
        confirmation: str,
    ) -> dict[str, Any]:
        self._assert_writable()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("QUEUE_RESET_NOT_IDLE")
        _, replayed = self.store.reset_terminal_queue(request_id, confirmation)
        return {"replayed": replayed, "queue": self._queue_snapshot()}

    def retry_failed_job(
        self,
        job_id: str,
        supplemental_prompt: str,
        request_id: str,
        *,
        start_immediately: bool = True,
    ) -> dict[str, Any]:
        self._assert_writable()
        job, replayed = self.store.retry_with_supplement(
            job_id, supplemental_prompt, request_id
        )
        start_result = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "supervisor": start_result,
            "queue": self._queue_snapshot(),
        }

    def retry_awaiting_qa_technical(
        self,
        job_id: str,
        expected_revision: int,
        stability_evidence_sha256: str,
        recovery_context: str,
        request_id: str,
        *,
        start_immediately: bool = False,
    ) -> dict[str, Any]:
        """Revision-safely requeue one code-fixable QA blocker on exact Droid GLM."""
        self._assert_writable()
        if start_immediately:
            raise QueueError("TECHNICAL_RETRY_MUST_NOT_START_IMMEDIATELY")
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job, replayed = self.store.retry_awaiting_qa_technical(
            job_id,
            expected_revision=expected_revision,
            stability_evidence_sha256=stability_evidence_sha256,
            recovery_context=recovery_context,
            request_id=request_id,
            integrity_snapshot_provider=self._canonical_integrity_snapshot,
        )
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "supervisor": self.supervisor.status(),
            "queue": self._queue_snapshot(),
        }

    def preview_awaiting_qa_technical(
        self,
        job_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        """Run the exact technical retry eligibility checks without mutation."""
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job = self.store.get_job(job_id)
        result = dict(job.get("last_result") or {})
        task_id = str(result.get("task_id", ""))
        task_root = getattr(self.harness, "task_root", None)
        if task_id and task_root is not None:
            task = TaskState.load(task_id, task_root)
            if is_control_state_write_failure(
                task.failure_code,
                failure_stage=task.failure_stage,
                worker_stderr=task.worker_stderr,
            ):
                return self._preview_control_recovery(job, expected_revision)
        return self.store.preview_awaiting_qa_technical(
            job_id,
            expected_revision=expected_revision,
            integrity_snapshot_provider=self._canonical_integrity_snapshot,
        )

    def _preview_control_recovery(
        self, job: Mapping[str, Any], expected_revision: int
    ) -> dict[str, Any]:
        """Validate a preserved control-failure delta without mutating it."""
        job_id = str(job.get("job_id", ""))
        queue = self.store.queue_snapshot()
        if (
            job.get("status") != AWAITING_QA
            or int(job.get("revision", -1)) != int(expected_revision)
            or queue.get("running_job_id")
            or queue.get("blocked_by_job_id") != job_id
            or queue.get("gate_reason") != AWAITING_QA
        ):
            raise QueueError("CONTROL_RECOVERY_STATE_INVALID", job_id)
        result = dict(job.get("last_result") or {})
        task_id = str(result.get("task_id", ""))
        if not task_id:
            raise QueueError("CONTROL_RECOVERY_TASK_MISSING", job_id)
        task = TaskState.load(task_id, self.harness.task_root)
        if task.job_id != job_id or not is_control_state_write_failure(
            task.failure_code,
            failure_stage=task.failure_stage,
            worker_stderr=task.worker_stderr,
        ):
            raise QueueError("CONTROL_RECOVERY_LINEAGE_INVALID", job_id)
        if (
            task.review_status not in {"", "PENDING"}
            or bool(task.reviewer_invocations)
            or task.build.get("status") != "PASS"
            or task.build.get("success") is not True
            or dict(task.build_evidence.get("details") or {}).get(
                "structured_evidence_valid"
            ) is not True
        ):
            raise QueueError("CONTROL_RECOVERY_STAGE_INVALID", job_id)
        snapshot_path = Path(task.pre_job_snapshot_manifest)
        try:
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise QueueError("CONTROL_RECOVERY_SNAPSHOT_INVALID", job_id) from exc
        if (
            snapshot.get("kind") != "PRE_JOB_SNAPSHOT"
            or snapshot.get("snapshot_id") != task.pre_job_snapshot_id
            or snapshot.get("baseline_id") != task.active_baseline_id
        ):
            raise QueueError("CONTROL_RECOVERY_BASELINE_MISMATCH", job_id)
        records = {
            str(item.get("path", "")): dict(item)
            for item in snapshot.get("files") or []
            if isinstance(item, Mapping) and item.get("path")
        }

        def current_hash(relative: str) -> str:
            target = self.harness.working_dir / relative
            return hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else "DELETED"

        source_hashes = {
            relative: current_hash(relative)
            for relative in sorted(set(task.changed_files))
        }
        if not source_hashes or any(value == "DELETED" for value in source_hashes.values()):
            raise QueueError("CONTROL_RECOVERY_SOURCE_MISSING", job_id)
        recorded_hashes = dict(task.changed_file_sha256 or {})
        if recorded_hashes and recorded_hashes != source_hashes:
            raise QueueError("CONTROL_RECOVERY_SOURCE_HASH_STALE", job_id)
        inherited_mismatch = []
        inherited_hashes: dict[str, str] = {}
        inherited_provenance: dict[str, str] = {}
        for relative in sorted(set(task.inherited_batch_delta_files)):
            record = records.get(relative)
            actual = current_hash(relative)
            inherited_hashes[relative] = actual
            if record is not None:
                expected = str(dict(record).get("sha256", "DELETED"))
                inherited_provenance[relative] = "PRE_JOB_SNAPSHOT_FILE"
                matches = actual == expected
            else:
                matches = self._snapshot_repo_identity_matches(
                    snapshot, relative, actual
                )
                inherited_provenance[relative] = "PRE_JOB_REPO_IDENTITY"
            if not matches:
                inherited_mismatch.append(relative)
        if inherited_mismatch:
            raise QueueError("CONTROL_RECOVERY_PREDECESSOR_MISMATCH", inherited_mismatch[0])
        integrity = self._canonical_integrity_snapshot(queue)
        unexpected = set(integrity.get("unexpected_runtime_dirty_files") or [])
        expected_task_delta = set(task.changed_files)
        external_unexpected = sorted(unexpected - expected_task_delta)
        if (
            integrity.get("active_baseline_id") != task.active_baseline_id
            or integrity.get("external_frozen_integrity") is not True
            or integrity.get("baseline_declaration_integrity") is not True
            or external_unexpected
        ):
            raise QueueError("CONTROL_RECOVERY_WORKTREE_INTEGRITY_INVALID", job_id)
        return {
            "eligible": True,
            "decision": "ELIGIBLE_CONTROL_RECOVERY",
            "recovery_action": "RETRY_REVIEW_ONLY",
            "recovery_stage": "BUILD",
            "build_evidence_fresh": False,
            "build_rebuild_required": True,
            "worker_will_run": False,
            "git_regeneration_will_run": False,
            "job_id": job_id,
            "task_id": task_id,
            "expected_revision": expected_revision,
            "normalized_failure_origin": "HARNESS_CAUSED",
            "source_sha256": source_hashes,
            "source_hash_binding": (
                "TASK_STATE" if recorded_hashes else "LEGACY_RECOVERY_CURRENT_STATE"
            ),
            "inherited_predecessor_sha256": inherited_hashes,
            "inherited_predecessor_provenance": inherited_provenance,
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "unexpected_external_dirty_files": external_unexpected,
            "review_status": task.review_status or "PENDING",
            "reviewer_invocation_count": len(task.reviewer_invocations),
            "integrity_snapshot": integrity,
        }

    def _snapshot_repo_identity_matches(
        self,
        snapshot: Mapping[str, Any],
        relative: str,
        actual_sha256: str,
    ) -> bool:
        """Prove a snapshot-omitted Git-clean file from frozen repo identity."""
        normalized = relative.replace("\\", "/").strip("/")
        workspace = Path(self.harness.working_dir).resolve()
        for raw in snapshot.get("repos") or []:
            if not isinstance(raw, Mapping):
                continue
            try:
                repo = Path(str(raw.get("repo", ""))).resolve()
                prefix = repo.relative_to(workspace).as_posix().strip("/")
            except (OSError, ValueError):
                continue
            if not prefix or not normalized.startswith(prefix + "/"):
                continue
            repo_relative = normalized[len(prefix) + 1 :]
            if normalized not in set(raw.get("tracked_files") or []):
                return False
            target = (repo / Path(*repo_relative.split("/"))).resolve()
            try:
                target.relative_to(repo)
            except ValueError:
                return False
            if not target.is_file() or target.is_symlink() or actual_sha256 == "DELETED":
                return False
            expected_head = str(raw.get("head", "")).strip()
            expected_index = str(raw.get("index_tree", "")).strip()
            if not expected_head or not expected_index:
                return False

            def git(*args: str) -> str:
                try:
                    result = subprocess.run(
                        [trusted_executable("git", forbidden_root=workspace), *args],
                        cwd=str(repo),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        env=git_subprocess_env(),
                        timeout=15,
                        check=False,
                    )
                except (OSError, subprocess.SubprocessError):
                    return ""
                return result.stdout.strip() if result.returncode == 0 else ""

            current_head = git("rev-parse", "--verify", "HEAD")
            current_index = git("write-tree")
            snapshot_blob = git(
                "rev-parse", "--verify", f"{expected_head}:{repo_relative}"
            )
            current_blob = git(
                "hash-object", f"--path={repo_relative}", "--", repo_relative
            )
            return bool(
                current_head == expected_head
                and current_index == expected_index
                and snapshot_blob
                and snapshot_blob == current_blob
                and hashlib.sha256(target.read_bytes()).hexdigest() == actual_sha256
            )
        return False

    def resolve_job(
        self,
        job_id: str,
        expected_revision: int,
        resolution: str,
        confirmation: str,
        request_id: str,
        *,
        start_immediately: bool = False,
    ) -> dict[str, Any]:
        """Expose the revision-checked local resolver through the writable MCP.

        The tunnel does not weaken the local proof: the service still requires
        the same OS execution lease used by run.py and the queue supervisor.
        """
        self._assert_writable()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        try:
            job, replayed = self.store.resolve_manually(
                job_id,
                resolution=resolution,
                confirmation=confirmation,
                expected_revision=expected_revision,
                request_id=request_id,
            )
        finally:
            self.harness.release_execution_lease()
        start_result = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "supervisor": start_result,
            "queue": self._queue_snapshot(),
        }

    def retry_blocked_job(
        self,
        job_id: str,
        request_id: str,
        *,
        start_immediately: bool = True,
    ) -> dict[str, Any]:
        """Retry a bounded, worktree-safe infrastructure block after local recovery."""
        self._assert_writable()
        job = self.store.get_job(job_id)
        resume_actions = dict(job.get("resume_actions") or {})
        if (
            self.store.action_key(request_id) in resume_actions
            or request_id in resume_actions
        ):
            job, replayed = self.store.resume_job(job_id, request_id)
            start_result = self._start_if_unpaused(start_immediately)
            return {
                "replayed": replayed,
                "job": self._public_job(job, include_requirement=True),
                "supervisor": start_result,
                "queue": self._queue_snapshot(),
            }
        if job.get("status") != BLOCKED:
            raise QueueError("JOB_NOT_BLOCKED", job_id)
        last_result = dict(job.get("last_result") or {})
        # Only the current claim matters. Historical task_ids can belong to a
        # previous safely rolled-back outer attempt. Missing legacy evidence is
        # intentionally conservative and requires manual review.
        blocked_before_task = (
            "current_claim_task_started" in job
            and job.get("current_claim_task_started") is False
        )
        safe_terminal_block = (
            last_result.get("failure_type") == "infrastructure"
            and last_result.get("worktree_disposition")
            in {"CLEAN_ROLLBACK", "NO_DELTA"}
        )
        if not (blocked_before_task or safe_terminal_block):
            raise QueueError("BLOCKED_JOB_REQUIRES_MANUAL_REVIEW", job_id)
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job, replayed = self.store.resume_job(job_id, request_id)
        start_result = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job, include_requirement=True),
            "supervisor": start_result,
            "queue": self._queue_snapshot(),
        }

    @staticmethod
    def _canonical_sha256(value: Any) -> str:
        return hashlib.sha256(
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()

    def _approved_release_files(self) -> dict[tuple[str, str], dict[str, Any]]:
        approved: dict[tuple[str, str], dict[str, Any]] = {}
        release_root = Path(__file__).resolve().parent / "releases"
        for version in ("0.8.5", "0.8.5.1"):
            manifest_path = release_root / version / "SHA256SUMS.json"
            try:
                raw = manifest_path.read_bytes()
                manifest = json.loads(raw.decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", version) from exc
            manifest_sha = hashlib.sha256(raw).hexdigest()
            for item in manifest.get("files", []):
                if not isinstance(item, Mapping):
                    continue
                path = str(item.get("path", "")).replace("\\", "/")
                sha = str(item.get("sha256", "")).lower()
                if path and len(sha) == 64:
                    approved[(path, sha)] = {
                        "version": version,
                        "manifest_path": str(manifest_path),
                        "manifest_sha256": manifest_sha,
                        "artifact_path": path,
                        "sha256": sha,
                    }
        return approved

    def _profile_revalidation_validation(
        self,
        job_id: str,
        expected_revision: int,
        expected_old_execution_context: Mapping[str, Any],
        classification_evidence: Mapping[str, Any],
        classification_evidence_sha256: str,
    ) -> dict[str, Any]:
        """Fail-closed validation shared by preview and mutation."""
        if self.read_only:
            raise HarnessServiceError("CONTROL_SURFACE_READ_ONLY")
        if self.supervisor.is_running():
            raise QueueError("PROFILE_REVALIDATION_SUPERVISOR_RUNNING")
        job = self.store.get_job(job_id)
        queue = self._queue_snapshot()
        if queue.get("execution_mode") != "ATTENDED":
            raise QueueError("PROFILE_REVALIDATION_REQUIRES_ATTENDED")
        if not queue.get("paused"):
            raise QueueError("PROFILE_REVALIDATION_REQUIRES_PAUSED_QUEUE")
        if queue.get("running_job_id"):
            raise QueueError("QUEUE_ALREADY_RUNNING")
        if queue.get("blocked_by_job_id") != job_id or queue.get("gate_reason") != BLOCKED:
            raise QueueError("JOB_NOT_ACTIVE_FIFO_BLOCKER", job_id)
        if job.get("status") != BLOCKED:
            raise QueueError("JOB_NOT_BLOCKED", job_id)
        if int(job.get("revision", -1)) != int(expected_revision):
            raise QueueError("STALE_JOB_REVISION", job_id)
        if dict(job.get("last_result") or {}).get("failure_code") != "PROFILE_DRIFT":
            raise QueueError("JOB_NOT_PROFILE_DRIFT_BLOCKED", job_id)
        old_context = validate_execution_context(expected_old_execution_context)
        if job.get("execution_context") != old_context:
            raise QueueError("OLD_EXECUTION_CONTEXT_MISMATCH", job_id)
        blocked_before_task = job.get("current_claim_task_started") is False
        last_result = dict(job.get("last_result") or {})
        safe_clean = (
            last_result.get("failure_type") == "infrastructure"
            and last_result.get("worktree_disposition")
            in {"CLEAN_ROLLBACK", "CHECKPOINTED_CLEAN_ROLLBACK", "NO_DELTA"}
            and not list(last_result.get("changed_files") or [])
        )
        if not (blocked_before_task or safe_clean):
            raise QueueError("BLOCKED_JOB_REQUIRES_MANUAL_REVIEW", job_id)

        current_full = self.harness.validate_configuration()
        current_context = {
            key: current_full[key] for key in EXECUTION_CONTEXT_KEYS
        }
        if old_context["profile_id"] != current_context["profile_id"]:
            raise QueueError("PROFILE_REVALIDATION_PROFILE_CHANGED")
        for key in (
            "profile_schema_version",
            "profile_manifest_sha256",
            "profile_snapshot_sha256",
            "workspace_identity_sha256",
        ):
            if old_context[key] != current_context[key]:
                raise QueueError("PROFILE_REVALIDATION_PROJECT_SEMANTICS_CHANGED", key)
        if old_context == current_context:
            raise QueueError("PROFILE_REVALIDATION_NO_DRIFT")

        evidence = dict(classification_evidence)
        evidence_sha = self._canonical_sha256(evidence)
        if evidence_sha != str(classification_evidence_sha256).strip().lower():
            raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_HASH_MISMATCH")
        old_context_sha = self._canonical_sha256(old_context)
        new_context_sha = self._canonical_sha256(current_context)
        request = dict(job.get("request") or {})
        requirement_sha = hashlib.sha256(
            str(request.get("requirement", "")).encode("utf-8")
        ).hexdigest()
        request_sha = self._canonical_sha256(request)
        expected_identity = {
            "job_id": job_id,
            "client_job_id": job.get("client_job_id", ""),
            "batch_id": job.get("batch_id", ""),
            "sequence": int(job.get("sequence", 0)),
        }
        exact = {
            "job_id": job_id,
            "old_execution_context_sha256": old_context_sha,
            "new_execution_context_sha256": new_context_sha,
            "profile_id": current_context["profile_id"],
            "requirement_sha256": requirement_sha,
            "request_sha256": request_sha,
            "job_identity": expected_identity,
            "target_modules": list(request.get("target_modules") or []),
            "baseline_id": queue.get("active_baseline_id", ""),
            "generation": int(queue.get("generation", 0)),
        }
        for key, value in exact.items():
            if evidence.get(key) != value:
                raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_INVALID", key)
        workspace_identity = dict(evidence.get("workspace_identity") or {})
        manifest_identity = dict(evidence.get("manifest_identity") or {})
        if workspace_identity != {
            "old": old_context["workspace_identity_sha256"],
            "new": current_context["workspace_identity_sha256"],
            "same": True,
        }:
            raise QueueError("PROFILE_REVALIDATION_WORKSPACE_CHANGED")
        if manifest_identity != {
            "old": old_context["profile_manifest_sha256"],
            "new": current_context["profile_manifest_sha256"],
            "same": True,
        }:
            raise QueueError("PROFILE_REVALIDATION_MANIFEST_CHANGED")

        predecessor_files = list(queue.get("batch_delta_files") or [])
        actual_predecessor_hashes: dict[str, str] = {}
        for relative in predecessor_files:
            path = Path(current_full["working_dir"]) / relative
            try:
                actual_predecessor_hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as exc:
                raise QueueError("PROFILE_REVALIDATION_PREDECESSOR_CHANGED", relative) from exc
        if evidence.get("predecessor_hashes") != actual_predecessor_hashes:
            raise QueueError("PROFILE_REVALIDATION_PREDECESSOR_CHANGED")

        successors = [
            item for item in self.store.list_jobs(limit=200)
            if item.get("batch_id") == job.get("batch_id")
            and int(item.get("sequence", 0)) > int(job.get("sequence", 0))
        ]
        successors.sort(key=lambda item: int(item.get("sequence", 0)))
        actual_successors = {
            item["job_id"]: {
                "client_job_id": item.get("client_job_id", ""),
                "sequence": int(item.get("sequence", 0)),
                "request_sha256": self._canonical_sha256(dict(item.get("request") or {})),
            }
            for item in successors
        }
        if evidence.get("successor_request_hashes") != actual_successors:
            raise QueueError("PROFILE_REVALIDATION_SUCCESSOR_CHANGED")

        integrity = dict(evidence.get("integrity") or {})
        if integrity != {
            "external_frozen": True,
            "baseline_declaration": True,
            "unexpected_dirty": [],
        }:
            raise QueueError("PROFILE_REVALIDATION_INTEGRITY_FAILED")
        if (
            not queue.get("external_frozen_integrity")
            or not queue.get("baseline_declaration_integrity")
            or list(queue.get("unexpected_runtime_dirty_files") or [])
        ):
            raise QueueError("PROFILE_REVALIDATION_INTEGRITY_FAILED")

        counts = dict(evidence.get("classification_counts") or {})
        if int(counts.get("D", 0)) or int(counts.get("E", 0)):
            raise QueueError("PROFILE_REVALIDATION_SEMANTIC_OR_UNKNOWN_DRIFT")
        allowed = {
            "APPROVED_HARNESS_CONTEXT_CHANGE",
            "APPROVED_RUNTIME_POLICY_CHANGE",
            "PROJECT_FACT_UNCHANGED_REHASH",
        }
        resources = list(evidence.get("resources") or [])
        if not resources:
            raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_INVALID", "resources")
        approved_files = self._approved_release_files()
        release_records: list[dict[str, Any]] = []
        for resource in resources:
            if not isinstance(resource, Mapping):
                raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_INVALID", "resources")
            classification = str(resource.get("classification", ""))
            if classification not in allowed or bool(resource.get("semantic_change")):
                raise QueueError("PROFILE_REVALIDATION_SEMANTIC_OR_UNKNOWN_DRIFT")
            artifact_path = str(resource.get("artifact_path", "")).replace("\\", "/")
            new_sha = str(resource.get("new_sha256", "")).lower()
            if not artifact_path.startswith("agents/skills/"):
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", artifact_path)
            provenance = approved_files.get((artifact_path, new_sha))
            if provenance is None:
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", artifact_path)
            live_path = Path("D:/") / artifact_path
            try:
                live_sha = hashlib.sha256(live_path.read_bytes()).hexdigest()
            except OSError as exc:
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", artifact_path) from exc
            if live_sha != new_sha:
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", artifact_path)
            supplied = list(resource.get("release_provenance") or [])
            if not any(
                isinstance(entry, Mapping)
                and entry.get("version") in {"0.8.5", "0.8.5.1"}
                and entry.get("artifact_path") == artifact_path
                and str(entry.get("sha256", "")).lower() == new_sha
                for entry in supplied
            ):
                raise QueueError("PROFILE_DRIFT_RELEASE_PROVENANCE_INVALID", artifact_path)
            release_records.append(provenance)
        if sum(int(counts.get(key, 0)) for key in ("A", "B", "C", "D", "E")) != len(resources):
            raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_INVALID", "classification_counts")

        audit = {
            "old_execution_context_sha256": old_context_sha,
            "new_execution_context_sha256": new_context_sha,
            "requirement_sha256": requirement_sha,
            "request_sha256": request_sha,
            "changed_resource_paths": [str(item.get("path", "")) for item in resources],
            "classification_codes": [str(item.get("classification", "")) for item in resources],
            "release_provenance": release_records,
            "baseline_id": queue.get("active_baseline_id", ""),
            "predecessor_files": predecessor_files,
            "predecessor_hashes": actual_predecessor_hashes,
            "successor_request_hashes": actual_successors,
            "generation": int(queue.get("generation", 0)),
        }
        return {
            "eligible": True,
            "decision": "ELIGIBLE",
            "job_id": job_id,
            "expected_revision": expected_revision,
            "old_execution_context": old_context,
            "current_execution_context": current_context,
            "classification_evidence_sha256": evidence_sha,
            "audit": audit,
        }

    def preview_blocked_job_profile_revalidation(
        self,
        job_id: str,
        expected_revision: int,
        expected_old_execution_context: Mapping[str, Any],
        classification_evidence: Mapping[str, Any],
        classification_evidence_sha256: str,
    ) -> dict[str, Any]:
        return self._profile_revalidation_validation(
            job_id,
            expected_revision,
            expected_old_execution_context,
            classification_evidence,
            classification_evidence_sha256,
        )

    def revalidate_blocked_job_profile(
        self,
        job_id: str,
        expected_revision: int,
        expected_old_execution_context: Mapping[str, Any],
        classification_evidence: Mapping[str, Any],
        classification_evidence_sha256: str,
        reason: str,
        request_id: str,
        *,
        start_immediately: bool = False,
    ) -> dict[str, Any]:
        self._assert_writable()
        if start_immediately:
            raise QueueError("PROFILE_REVALIDATION_START_IMMEDIATELY_FORBIDDEN")
        if self._canonical_sha256(dict(classification_evidence)) != str(
            classification_evidence_sha256
        ).strip().lower():
            raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_HASH_MISMATCH")
        existing = self.store.get_job(job_id)
        prior = dict(existing.get("profile_revalidation_actions") or {}).get(
            self.store.action_key(request_id)
        )
        if prior is not None:
            current_full = self.harness.validate_configuration()
            current_context = {
                key: current_full[key] for key in EXECUTION_CONTEXT_KEYS
            }
            stored = dict(existing.get("profile_revalidation") or {})
            audit_keys = (
                "old_execution_context_sha256",
                "new_execution_context_sha256",
                "requirement_sha256",
                "request_sha256",
                "changed_resource_paths",
                "classification_codes",
                "release_provenance",
                "baseline_id",
                "predecessor_files",
                "predecessor_hashes",
                "successor_request_hashes",
                "generation",
            )
            replay_job, replayed = self.store.revalidate_blocked_job_profile(
                job_id,
                expected_revision=expected_revision,
                expected_old_execution_context=expected_old_execution_context,
                current_execution_context=current_context,
                classification_evidence_sha256=classification_evidence_sha256,
                audit={key: stored.get(key) for key in audit_keys},
                reason=reason,
                request_id=request_id,
            )
            return {
                "replayed": replayed,
                "decision": "REVALIDATED",
                "job": self._public_job(replay_job, include_requirement=True),
                "queue": self._queue_snapshot(),
                "supervisor": self.supervisor.status(),
            }
        validation = self._profile_revalidation_validation(
            job_id,
            expected_revision,
            expected_old_execution_context,
            classification_evidence,
            classification_evidence_sha256,
        )
        if not self.harness.probe_execution_idle() or not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        try:
            # Recompute under the shared execution lease and before the atomic
            # JobStore transition; callers cannot inject a current context.
            validation = self._profile_revalidation_validation(
                job_id,
                expected_revision,
                expected_old_execution_context,
                classification_evidence,
                classification_evidence_sha256,
            )
            job, replayed = self.store.revalidate_blocked_job_profile(
                job_id,
                expected_revision=expected_revision,
                expected_old_execution_context=validation["old_execution_context"],
                current_execution_context=validation["current_execution_context"],
                classification_evidence_sha256=validation["classification_evidence_sha256"],
                audit=validation["audit"],
                reason=reason,
                request_id=request_id,
            )
        finally:
            self.harness.release_execution_lease()
        # Configuration baseline must move only after durable revalidation.
        self.configuration = self.harness.validate_configuration()
        return {
            "replayed": replayed,
            "decision": "REVALIDATED",
            "job": self._public_job(job, include_requirement=True),
            "queue": self._queue_snapshot(),
            "supervisor": self.supervisor.status(),
        }

    def pause_queue(
        self,
        reason: str,
        *,
        expected_revision: int | None = None,
        request_id: str = "",
        confirmation: str = "",
    ) -> dict[str, Any]:
        self._assert_writable()
        if request_id:
            self.store.pause(
                reason,
                expected_revision=expected_revision,
                request_id=request_id,
                confirmation=confirmation,
            )
        else:
            self.store.pause(reason)
        return self.get_queue()

    def resume_queue(
        self,
        *,
        expected_revision: int | None = None,
        request_id: str = "",
        confirmation: str = "",
    ) -> dict[str, Any]:
        self._assert_writable()
        if request_id:
            self.store.resume_queue(
                expected_revision=expected_revision,
                request_id=request_id,
                confirmation=confirmation,
            )
        else:
            self.store.resume_queue()
        return self.get_queue()

    def skip_failed_job(
        self,
        job_id: str,
        reason: str,
        request_id: str,
        *,
        start_immediately: bool = True,
    ) -> dict[str, Any]:
        self._assert_writable()
        job, replayed = self.store.skip_failed_job(job_id, reason, request_id)
        start_result = self._start_if_unpaused(start_immediately)
        return {
            "replayed": replayed,
            "job": self._public_job(job),
            "supervisor": start_result,
            "queue": self._queue_snapshot(),
        }

    def resume_interrupted_job(
        self,
        job_id: str,
        confirmation: str,
        request_id: str,
    ) -> dict[str, Any]:
        self._assert_writable()
        if confirmation != "I_CONFIRMED_NO_ACTIVE_HARNESS_PROCESS":
            raise QueueError("ACTIVE_PROCESS_CONFIRMATION_REQUIRED")
        job = self.store.get_job(job_id)
        resume_actions = dict(job.get("resume_actions") or {})
        if (
            self.store.action_key(request_id) in resume_actions
            or request_id in resume_actions
        ):
            job, replayed = self.store.resume_job(job_id, request_id)
            return {
                "replayed": replayed,
                "job": self._public_job(job),
                "queue": self._queue_snapshot(),
            }
        if job.get("status") != INTERRUPTED:
            raise QueueError("JOB_NOT_INTERRUPTED", job_id)

        # The same lock used by run.py is the local proof that no interactive or
        # control-plane harness currently owns the workspace. The later dispatcher
        # acquires it again, closing the race before actual execution.
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        # Once a Worker Task was started, an abrupt controller loss can occur
        # before Git collection. An empty TaskState.changed_files is therefore
        # not proof that the worktree is unchanged. Without a persisted Git
        # baseline, automatic resume would risk duplicating preserved edits.
        if (
            "current_claim_task_started" not in job
            or job.get("current_claim_task_started") is not False
        ):
            raise QueueError("INTERRUPTED_STARTED_TASK_REQUIRES_MANUAL_REVIEW", job_id)
        job, replayed = self.store.resume_job(job_id, request_id)
        return {
            "replayed": replayed,
            "job": self._public_job(job),
            "queue": self._queue_snapshot(),
        }

    def reconcile_orphaned_no_delta_job(
        self,
        job_id: str,
        expected_queue_revision: int,
        expected_job_revision: int,
        expected_task_id: str,
        expected_execution_id: str,
        request_id: str,
    ) -> dict[str, Any]:
        """Requeue one durably salvaged zero-delta orphan without erasing history."""
        self._assert_writable()
        if self.supervisor.is_running() or not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        job, replayed = self.store.resume_job(
            job_id,
            request_id,
            expected_queue_revision=expected_queue_revision,
            expected_job_revision=expected_job_revision,
            expected_task_id=expected_task_id,
            expected_execution_id=expected_execution_id,
        )
        return {
            "replayed": replayed,
            "job": self._public_job(job),
            "queue": self._queue_snapshot(),
        }
