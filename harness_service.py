"""Programmatic single-job adapter that preserves the existing Manager pipeline."""

from __future__ import annotations

import hashlib
import copy
import json
import os
import re
import subprocess
import threading
import traceback
from datetime import datetime, timezone
from execution_lifecycle import ShutdownBarrier, LifecycleError
from pathlib import Path
from typing import Any, Callable, Mapping

from job_contract import (
    DROID_GLM_FAIL_CLOSED_MODEL,
    DROID_GLM_FAIL_CLOSED_POLICY,
    EXECUTION_CONTEXT_KEYS,
    JobContractError,
    JobRequest,
    resolve_droid_model,
    validate_execution_context,
)
from manager import Manager, ManagerResult
from project_profile import PROFILE_SCHEMA_FILE, ProfileError, select_project_profile
from reporter import Reporter
from run import (
    HARNESS_ROOT,
    SingleInstanceLock,
    _next_task_id,
    create_task,
    discover_modules,
    runtime_rule_paths,
    validate_runtime_rules,
)
from runtime_safety import scrub_secrets
from runtime_snapshot import (
    RuntimeSnapshot,
    RuntimeSnapshotError,
    materialize_runtime_snapshot,
    load_frozen_runtime_snapshot,
    policy_surface_sha256,
    profile_surface_sha256,
)
from task_state import TaskState
from git_committer import GitCommitError, JobCommitter
from cumulative_policy import CumulativePolicyError, CumulativePolicyManager
from attempt_checkpoint import AttemptCheckpointStore
from control_paths import ControlPathError
from execution_evidence import build_execution_summary
from policy_catalog import (
    HARNESS_CAUSED,
    classify_failure_origin,
    is_control_state_write_failure,
    resolve_effective_policy,
)
from git_collector import GitCollector
from handoff import build_crash_recovery_handoff
from worker import parse_qa_request


class HarnessServiceError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = scrub_secrets(detail)[:1000]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


class HarnessService:
    """Execute validated queue jobs through ``Manager.submit_task`` only."""

    def __init__(
        self,
        working_dir: str | Path,
        profile_dir: str | Path,
        *,
        task_root: str | Path | None = None,
        snapshot_root: str | Path | None = None,
        manager_factory: type[Manager] = Manager,
        reporter: Reporter | None = None,
        instance_lock: SingleInstanceLock | None = None,
    ):
        self.working_dir = Path(working_dir).resolve()
        self.profile_dir = Path(profile_dir).resolve()
        self.task_root = Path(task_root or (HARNESS_ROOT / ".tasks")).resolve()
        self.snapshot_root = Path(
            snapshot_root or (self.task_root.parent / ".control" / "snapshots")
        ).absolute()
        self.control_root = self.snapshot_root.parent.resolve()
        self.manager_factory = manager_factory
        self.reporter = reporter or Reporter()
        self._instance_lock = instance_lock or SingleInstanceLock()
        self._lease_guard = threading.Lock()
        self._lease_held = False
        self._lifecycle = ShutdownBarrier()
        self._policy_files: tuple[tuple[Path, str], ...] | None = None

    def _policy_source_root(self) -> Path:
        if self._policy_files is None:
            self._policy_files = (
                (Path(PROFILE_SCHEMA_FILE).resolve(), "profile_schema"),
                *((Path(path).resolve(), label) for path, label in runtime_rule_paths()),
            )
        for path, label in self._policy_files:
            if label == "common_agents":
                return path.parent.resolve()
        return Path(os.environ.get("AGENTS_DIR", r"D:\agents")).resolve()

    def _log_control_exception(self, code: str, exc: BaseException) -> str:
        """Persist the swallowed control exception for local diagnosis.

        MCP stdio cannot print tracebacks to stdout. Write a bounded, scrubbed
        dump next to queue state instead of dropping the exception entirely.
        """
        detail = scrub_secrets(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        )[:4000]
        reason = f"{code}: {type(exc).__name__}"[:500]
        try:
            path = self.task_root.parent / ".control" / "last_control_exception.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                f"{code}\n{type(exc).__name__}: {exc}\n{detail}\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        return reason

    def acquire_execution_lease(self) -> bool:
        with self._lease_guard:
            if self._lease_held:
                return False
            acquired = self._instance_lock.acquire()
            self._lease_held = acquired
            return acquired

    def release_execution_lease(self) -> None:
        with self._lease_guard:
            if self._lease_held:
                self._instance_lock.release()
                self._lease_held = False

    def probe_execution_idle(self) -> bool:
        """Try the run.py OS lock without releasing another caller's lease."""
        with self._lease_guard:
            if self._lease_held:
                return False
            lock_path = getattr(self._instance_lock, "path", None)
        if lock_path is None:
            # Test doubles without a shared OS-lock path are idle when no lease is
            # held. Production SingleInstanceLock always supplies ``path``.
            return True
        probe = SingleInstanceLock(lock_path)
        if not probe.acquire():
            return False
        probe.release()
        return True

    def probe_worker_process_active(self) -> bool:
        """Conservative OS process-name probe used only by orphan diagnostics."""
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["tasklist", "/FO", "CSV", "/NH"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                text = result.stdout.casefold()
            else:
                result = subprocess.run(
                    ["ps", "-eo", "comm="],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                text = result.stdout.casefold()
            return any(name in text for name in ("droid.exe", "codex.exe", "droid\n", "codex\n"))
        except (OSError, subprocess.SubprocessError):
            # Unknown is fail-closed for orphan classification: do not assert that
            # a persisted RUNNING task has no live Worker.
            return True

    @staticmethod
    def probe_execution_pid_active(pid: int, expected_started_at: str = "") -> bool:
        """Check persisted Windows process liveness and creation identity."""
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            return False
        try:
            if os.name == "nt":
                import ctypes

                from ctypes import wintypes

                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.OpenProcess.argtypes = (
                    wintypes.DWORD, wintypes.BOOL, wintypes.DWORD
                )
                kernel32.OpenProcess.restype = wintypes.HANDLE
                kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
                kernel32.CloseHandle.restype = wintypes.BOOL
                kernel32.GetExitCodeProcess.argtypes = (
                    wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)
                )
                kernel32.GetExitCodeProcess.restype = wintypes.BOOL
                kernel32.GetProcessTimes.argtypes = (
                    wintypes.HANDLE,
                    ctypes.POINTER(wintypes.FILETIME),
                    ctypes.POINTER(wintypes.FILETIME),
                    ctypes.POINTER(wintypes.FILETIME),
                    ctypes.POINTER(wintypes.FILETIME),
                )
                kernel32.GetProcessTimes.restype = wintypes.BOOL
                handle = kernel32.OpenProcess(0x1000, False, pid)
                if not handle:
                    return False
                try:
                    exit_code = wintypes.DWORD()
                    if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                        return False
                    if exit_code.value != 259:  # STILL_ACTIVE
                        return False
                    if expected_started_at:
                        creation = wintypes.FILETIME()
                        exit_time = wintypes.FILETIME()
                        kernel_time = wintypes.FILETIME()
                        user_time = wintypes.FILETIME()
                        if not kernel32.GetProcessTimes(
                            handle, ctypes.byref(creation), ctypes.byref(exit_time),
                            ctypes.byref(kernel_time), ctypes.byref(user_time)
                        ):
                            return False
                        filetime = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
                        created = datetime.fromtimestamp(
                            (filetime - 116444736000000000) / 10_000_000,
                            tz=timezone.utc,
                        )
                        expected = datetime.fromisoformat(expected_started_at)
                        if expected.tzinfo is None:
                            expected = expected.replace(tzinfo=timezone.utc)
                        if abs((created - expected.astimezone(timezone.utc)).total_seconds()) > 15:
                            return False
                    return True
                finally:
                    kernel32.CloseHandle(handle)
            os.kill(pid, 0)
            return True
        except (OSError, AttributeError):
            return False

    def _load_profile(self):
        if not self.working_dir.is_dir():
            raise HarnessServiceError("PROJECT_ROOT_NOT_FOUND")
        try:
            profile = select_project_profile(self.working_dir, str(self.profile_dir))
            validate_runtime_rules()
        except ProfileError as exc:
            raise HarnessServiceError("PROFILE_INVALID", str(exc)) from exc
        available = discover_modules(self.working_dir, profile)
        return profile, available

    def _configuration(self, profile, available: list[str]) -> dict[str, Any]:
        from project_profile import ControlPolicyProfile

        control_policy = getattr(profile, "control_policy", ControlPolicyProfile())
        manifest_path = self.profile_dir / "project.json"
        try:
            manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise HarnessServiceError("PROFILE_MANIFEST_UNREADABLE") from exc
        try:
            profile_files: list[tuple[Path, str]] = []
            for path in self.profile_dir.rglob("*"):
                if not path.is_file():
                    continue
                resolved = path.resolve(strict=True)
                try:
                    relative = resolved.relative_to(self.profile_dir)
                except ValueError as exc:
                    raise HarnessServiceError("PROFILE_SNAPSHOT_PATH_ESCAPE") from exc
                profile_files.append((resolved, relative.as_posix()))
            profile_digest = profile_surface_sha256(profile_files)
        except HarnessServiceError:
            raise
        except OSError as exc:
            raise HarnessServiceError("PROFILE_SNAPSHOT_UNREADABLE") from exc
        if self._policy_files is None:
            self._policy_files = (
                (Path(PROFILE_SCHEMA_FILE).resolve(), "profile_schema"),
                *((Path(path).resolve(), label) for path, label in runtime_rule_paths()),
            )
        policy_files = self._policy_files
        try:
            policy_digest = policy_surface_sha256(
                self._policy_source_root(), policy_files
            )
        except (OSError, RuntimeSnapshotError) as exc:
            raise HarnessServiceError("POLICY_SNAPSHOT_UNREADABLE") from exc
        workspace_identity = json.dumps(
            {
                "working_dir": os.path.normcase(str(self.working_dir.resolve())),
                "profile_dir": os.path.normcase(str(self.profile_dir.resolve())),
                "task_root": os.path.normcase(str(self.task_root.resolve())),
                "available_modules": list(available),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return {
            "working_dir": str(self.working_dir),
            "profile_dir": str(self.profile_dir),
            "profile_id": profile.id,
            "profile_schema_version": profile.schema_version,
            "available_modules": list(available),
            "task_root": str(self.task_root),
            "profile_manifest_sha256": manifest_sha256,
            "profile_snapshot_sha256": profile_digest,
            "policy_snapshot_sha256": policy_digest,
            "workspace_identity_sha256": hashlib.sha256(
                workspace_identity.encode("utf-8")
            ).hexdigest(),
            "commit_policy": control_policy.commit_policy,
            "cumulative_worktree": control_policy.cumulative_worktree,
            "auto_continue_without_commit": (
                control_policy.auto_continue_without_commit
            ),
            "pre_job_snapshot_supported": (
                control_policy.pre_job_snapshot_supported
            ),
            "scoped_rollback_supported": (
                control_policy.scoped_rollback_supported
            ),
            "final_commit_required": control_policy.final_commit_required,
        }

    def validate_configuration(self) -> dict[str, Any]:
        profile, available = self._load_profile()
        return self._configuration(profile, available)

    def _cumulative_manager(
        self, *, read_only: bool = False
    ) -> CumulativePolicyManager:
        profile, _ = self._load_profile()
        return CumulativePolicyManager(
            self.working_dir,
            self.control_root,
            profile,
            read_only=read_only,
        )

    def cumulative_policy_status(
        self,
        *,
        active_baseline_id: str = "",
        batch_delta_files: list[str] | tuple[str, ...] = (),
        read_only: bool = False,
    ) -> dict[str, Any]:
        try:
            result = self._cumulative_manager(read_only=read_only).public_status(
                active_baseline_id, batch_delta_files
            )
            if (self.control_root / 'sqlite-authority.json').is_file():
                from control_repository import ControlRepository
                repository = ControlRepository(
                    self.control_root / 'control.sqlite3', read_only=True
                )
                initial = repository.candidate_ownership(self.working_dir)
                ownership_paths = [
                    item['path']
                    for item in initial['files'] + initial['historical_files']
                ]
                accepted = self._cumulative_manager(read_only=True).adopted_external_hashes(
                    active_baseline_id, repository_paths=ownership_paths
                )
                ownership = repository.candidate_ownership(
                    self.working_dir, accepted_baseline_hashes=accepted
                )
                known = {item['path'] for item in ownership['files']} if ownership['valid'] else set()
                result['preserved_unverified_candidates'] = ownership['files']
                result['historical_candidates'] = ownership['historical_files']
                result['historical_candidate_artifact_integrity'] = ownership['historical_artifact_integrity']
                result['candidate_preservation_integrity'] = ownership['valid']
                result['unexpected_runtime_dirty_files'] = sorted((set(result.get('unexpected_runtime_dirty_files') or []) - known) | set(ownership['invalid_files']))
            return result
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def prepare_batch_baseline(self, generation: int) -> dict[str, Any]:
        try:
            return self._cumulative_manager().prepare_batch_baseline(generation)
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def prepare_external_baseline_adoption(
        self,
        generation: int,
        previous_baseline_id: str,
        source_manifest_path: str | Path,
    ) -> dict[str, Any]:
        try:
            return self._cumulative_manager().prepare_external_baseline_adoption(
                generation, previous_baseline_id, source_manifest_path
            )
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def activate_prepared_baseline(self, baseline_id: str, generation: int) -> None:
        try:
            self._cumulative_manager().activate_prepared_baseline(
                baseline_id, generation
            )
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def prepare_job_snapshot(
        self,
        queued_job: Mapping[str, Any],
        queue_snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            reservation = dict(queued_job.get("active_attempt_reservation") or {})
            recovery = dict(reservation.get("provenance") or {})
            preserved = dict(recovery.get("preserved_source_hashes") or {})
            if (
                reservation.get("attempt_kind") == "TECHNICAL_RECOVERY"
                and recovery.get("reuse_source_snapshot") is True
                and preserved
            ):
                manifest_path = Path(str(queued_job.get("pre_job_snapshot_manifest") or "")).resolve()
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                baseline_id = str(queue_snapshot.get("active_baseline_id") or "")
                scope = list(
                    dict(queued_job.get("request") or {}).get("target_resources")
                    or dict(queued_job.get("request") or {}).get("target_modules")
                    or []
                )

                def inside(path: str) -> bool:
                    normalized = path.replace("\\", "/").strip("/")
                    return any(
                        normalized == str(root).replace("\\", "/").strip("/")
                        or normalized.startswith(
                            str(root).replace("\\", "/").strip("/") + "/"
                        )
                        for root in scope
                    )

                actual = {}
                for relative, expected in preserved.items():
                    target = (self.working_dir / relative).resolve()
                    if (
                        not target.is_relative_to(self.working_dir)
                        or not target.is_file()
                        or not inside(relative)
                    ):
                        raise HarnessServiceError("TECHNICAL_RECOVERY_SOURCE_SCOPE_INVALID")
                    actual[relative] = hashlib.sha256(target.read_bytes()).hexdigest()
                    if actual[relative] != str(expected):
                        raise HarnessServiceError("TECHNICAL_RECOVERY_SOURCE_HASH_CHANGED")
                if (
                    manifest.get("kind") != "PRE_JOB_SNAPSHOT"
                    or manifest.get("job_id") != queued_job.get("job_id")
                    or manifest.get("baseline_id") != baseline_id
                    or manifest.get("snapshot_id") != queued_job.get("pre_job_snapshot_id")
                    or str(recovery.get("source_snapshot_id") or "")
                    != manifest.get("snapshot_id")
                ):
                    raise HarnessServiceError("TECHNICAL_RECOVERY_SOURCE_SNAPSHOT_INVALID")
                status = self.cumulative_policy_status(
                    active_baseline_id=baseline_id,
                    batch_delta_files=sorted(set(
                        list(queue_snapshot.get("batch_delta_files") or []) + list(preserved)
                    )),
                    read_only=True,
                )
                if (
                    status.get("external_frozen_integrity") is not True
                    or status.get("baseline_declaration_integrity") is not True
                    or list(status.get("unexpected_runtime_dirty_files") or [])
                ):
                    raise HarnessServiceError("TECHNICAL_RECOVERY_SOURCE_INTEGRITY_INVALID")
                loaded = self._cumulative_manager().load_job_baseline(manifest_path)
                return {
                    "snapshot_id": manifest["snapshot_id"],
                    "manifest_path": str(manifest_path),
                    "active_baseline_id": baseline_id,
                    "external_frozen_integrity": True,
                    "baseline_declaration_integrity": True,
                    "pre_job_heads": [repo.head_oid for repo in loaded.repos],
                    "inherited_batch_delta_files": list(
                        queued_job.get("inherited_batch_delta_files") or []
                    ),
                    "unexpected_external_dirty_files": [],
                }
            fresh = dict(queued_job.get("fresh_input") or {})
            if fresh:
                from job_queue import JobStore
                parent = JobStore(self.control_root, read_only=True).get_job(
                    str(queued_job.get("replaces_job_id", "")))
                integrity = self.cumulative_policy_status(
                    active_baseline_id=str(queue_snapshot.get("active_baseline_id", "")),
                    batch_delta_files=list(queue_snapshot.get("batch_delta_files") or []),
                    read_only=True)
                if fresh.get('kind') == 'CANDIDATELESS_QA_POLICY_FRESH_INPUT':
                    from qa_policy_resolution import validate_resolution, resolution_contract
                    resolution = dict(queued_job.get('qa_policy_resolution') or {})
                    validate_resolution(resolution.get('policy'))
                    if queued_job.get('current_requirement') != str(parent['current_requirement']) + '\n\n' + resolution_contract(resolution.get('policy')):
                        raise HarnessServiceError('QA_POLICY_RESOLUTION_CONTRACT_INVALID')
                    task = TaskState.load(str(fresh.get('source_task_id', '')), self.task_root)
                    checked = self._cumulative_manager(read_only=True).validate_policy_fresh_input(
                        parent, queue_snapshot, integrity, task)
                else:
                    checked = self._cumulative_manager(read_only=True).validate_no_delta_fresh_input(
                        parent, queue_snapshot, integrity)
                if checked != fresh or queued_job.get("checkpoint_seed"):
                    raise HarnessServiceError("REPLACEMENT_WORKTREE_INTEGRITY_INVALID")
            inherited = list(queue_snapshot.get("batch_delta_files") or [])
            if (self.control_root / 'sqlite-authority.json').is_file():
                from control_repository import ControlRepository
                from qa_quarantine import paths_overlap
                repository = ControlRepository(self.control_root / 'control.sqlite3',read_only=True)
                # Current-custody query only: normal Job admission must not
                # re-run historical forensic scans (verified candidate history
                # or successful-Job genealogy) against the live authority.
                initial = repository.candidate_ownership(self.working_dir, current_only=True)
                ownership_paths = [
                    item['path']
                    for item in initial['files'] + initial['historical_files']
                ]
                manager = self._cumulative_manager()
                adopted_hashes = getattr(manager, "adopted_external_hashes", None)
                accepted = (
                    adopted_hashes(
                        str(queue_snapshot.get("active_baseline_id", "")),
                        repository_paths=ownership_paths,
                    )
                    if callable(adopted_hashes)
                    else {}
                )
                if not isinstance(accepted, Mapping):
                    accepted = {}
                ownership = repository.candidate_ownership(
                    self.working_dir, accepted_baseline_hashes=accepted,
                    current_only=True,
                )
                if not ownership['valid']:
                    raise HarnessServiceError('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')
                candidate_files = [item['path'] for item in ownership['files']]
                scope = queued_job.get('request',{}).get('target_resources') or queued_job.get('request',{}).get('target_modules') or []
                job_id = str(queued_job.get('job_id', ''))
                source_id = str(queued_job.get('corrects_job_id', ''))
                handed_off = bool(source_id and repository.candidate_handoff_allows(source_id, job_id))
                technical_input = bool(source_id and repository.technical_candidate_input_allows(source_id, job_id))
                incompatible = [row['path'] for row in ownership['files']
                                if not ((handed_off and row['job_id'] == job_id)
                                    or (technical_input and row['job_id'] == source_id))]
                if incompatible and (not scope or paths_overlap(
                    [str(path).casefold() for path in scope], [path.casefold() for path in incompatible]
                )):
                    raise HarnessServiceError('QUARANTINED_CANDIDATE_SCOPE_CONFLICT')
                # Snapshot known unverified input as protected inherited bytes.
                # It never enters queue.batch_delta_files or promotion eligibility.
                inherited = sorted(set(inherited + candidate_files))
            return self._cumulative_manager().prepare_job_snapshot(
                queued_job,
                str(queue_snapshot.get("active_baseline_id", "")),
                inherited,
            )
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def capture_crash_salvage(
        self,
        queued_job: Mapping[str, Any],
        queue_snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist an immutable, unverified candidate before stale reconciliation."""
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")
        expected_context = self._validated_execution_context(queued_job)
        profile, available_modules = self._load_profile()
        current_configuration = self._configuration(profile, available_modules)
        if not self._context_matches(expected_context, current_configuration):
            raise HarnessServiceError("PROFILE_DRIFT")
        try:
            request = JobRequest.from_mapping(
                dict(queued_job.get("request") or {}),
                allowed_modules=available_modules,
            )
        except JobContractError as exc:
            raise HarnessServiceError(exc.code, exc.field) from exc
        task_id = str(queued_job.get("current_claim_task_id", ""))
        try:
            task = TaskState.load(task_id, self.task_root)
        except Exception as exc:
            raise HarnessServiceError("CRASH_SALVAGE_TASK_UNREADABLE") from exc
        runtime = dict(task.execution_runtime or {})
        execution_id = str(runtime.get("execution_id", ""))
        if not execution_id:
            raise HarnessServiceError("CRASH_SALVAGE_EXECUTION_ID_MISSING")
        if any((
            task.job_id != str(queued_job.get("job_id", "")),
            task.batch_id != str(queued_job.get("batch_id", "")),
            int(task.queue_generation) != int(queue_snapshot.get("generation", -1)),
            task.active_baseline_id != str(queue_snapshot.get("active_baseline_id", "")),
            tuple(task.target_module) != tuple(request.target_modules),
        )):
            raise HarnessServiceError("CRASH_SALVAGE_LINEAGE_MISMATCH")
        manifest = str(queued_job.get("pre_job_snapshot_manifest", ""))
        try:
            baseline = self._cumulative_manager().load_job_baseline(manifest)
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

        collector = GitCollector(str(self.working_dir), profile=profile)
        delta = collector.collect(task, baseline)
        if not delta.success:
            raise HarnessServiceError("CRASH_SALVAGE_DELTA_CAPTURE_FAILED", delta.error)
        if delta.excluded_files:
            raise HarnessServiceError(
                "CRASH_SALVAGE_SCOPE_AMBIGUOUS", ",".join(delta.excluded_files[:20])
            )
        changed = list(dict.fromkeys(delta.changed_files))
        resources = [item.replace("\\", "/").strip("/") for item in request.target_resources]
        modules = [item.replace("\\", "/").strip("/") for item in request.target_modules]

        def allowed(path: str) -> bool:
            normalized = path.replace("\\", "/").strip("/")
            boundaries = resources or modules
            return any(
                normalized == boundary or normalized.startswith(boundary.rstrip("/") + "/")
                for boundary in boundaries
            )

        outside = [path for path in changed if not allowed(path)]
        if outside or (changed and not (resources or modules)):
            raise HarnessServiceError(
                "CRASH_SALVAGE_SCOPE_AMBIGUOUS", ",".join(outside[:20])
            )
        task.changed_files = list(changed)
        task.task_owned_changed_files = list(changed)
        task.scoped_rollback_targets = list(changed)
        task.inherited_batch_delta_files = list(
            queued_job.get("inherited_batch_delta_files")
            or queue_snapshot.get("batch_delta_files")
            or []
        )
        task.external_frozen_integrity = bool(
            queued_job.get("external_frozen_integrity", False)
        )
        task.baseline_declaration_integrity = bool(
            queued_job.get("baseline_declaration_integrity", False)
        )
        if not changed:
            task.no_task_delta_evidence = collector.prove_no_task_delta(baseline)
            if not task.no_task_delta_evidence.get("passed"):
                raise HarnessServiceError(
                    str(task.no_task_delta_evidence.get("failure_code") or "CRASH_SALVAGE_NO_DELTA_UNPROVEN")
                )
        candidate_delta = list(dict.fromkeys((
            *list(queue_snapshot.get("batch_delta_files") or []),
            *changed,
        )))
        integrity = self.cumulative_policy_status(
            active_baseline_id=task.active_baseline_id,
            batch_delta_files=candidate_delta,
            read_only=True,
        )
        if (
            integrity.get("unexpected_runtime_dirty_files")
            or not integrity.get("external_frozen_integrity")
            or not integrity.get("baseline_declaration_integrity")
        ):
            raise HarnessServiceError("CRASH_SALVAGE_INTEGRITY_FAILED")

        completed = {
            "build": str(dict(task.build or {}).get("status", "")),
            "test": "NOT_REQUIRED" if not task.test_required else str(task.test_status),
            "review": str(task.review_status),
            "verification": str(task.verification_status),
        }
        metadata = {
            "source_job_id": task.job_id,
            "source_task_id": task.task_id,
            "source_execution_id": execution_id,
            "interruption_reason": "RUNTIME_LEASE_EXPIRED_PROCESS_DEAD",
            "last_known_stage": str(task.stage),
            "last_progress_revision": int(task.progress_revision),
            "last_runtime_heartbeat_at": str(runtime.get("last_runtime_heartbeat_at", "")),
            "completed_evidence": completed,
        }
        checkpoint = AttemptCheckpointStore(
            self.working_dir, self.control_root
        ).capture(
            task,
            baseline,
            "RUNTIME_LEASE_EXPIRED_PROCESS_DEAD",
            artifact_kind="CRASH_SALVAGE_CHECKPOINT",
            recovery_metadata=metadata,
            allow_no_delta=True,
        )
        salvage = {
            "schema_version": 1,
            "status": "SALVAGE_CANDIDATE" if changed else "NO_TASK_DELTA",
            "integrity_status": "RESTORE_ELIGIBLE",
            "job_id": task.job_id,
            "task_id": task.task_id,
            "execution_id": execution_id,
            "outer_attempt": int(task.outer_attempt),
            "attempt_kind": str(task.attempt_kind),
            "active_baseline_id": task.active_baseline_id,
            "pre_job_snapshot_id": task.pre_job_snapshot_id,
            "pre_job_snapshot_manifest": task.pre_job_snapshot_manifest,
            "checkpoint_id": str(checkpoint.get("checkpoint_id", "")),
            "manifest_path": str(checkpoint.get("manifest_path", "")),
            "manifest_sha256": str(checkpoint.get("manifest_sha256", "")),
            "task_owned_changed_files": changed,
            "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
            "completed_evidence": completed,
            "last_known_stage": str(task.stage),
            "last_progress_revision": int(task.progress_revision),
            "interruption_reason": "RUNTIME_LEASE_EXPIRED_PROCESS_DEAD",
            "verified": False,
        }
        handoff = build_crash_recovery_handoff(task, salvage)
        salvage["recovery_handoff"] = handoff
        task.crash_salvage = dict(salvage)
        task.crash_recovery_handoff = handoff
        task.checkpoint_id = str(checkpoint.get("checkpoint_id", ""))
        task.checkpoint_manifest = str(checkpoint.get("manifest_path", ""))
        task.checkpoint_status = str(salvage["status"])
        task.checkpoint_event = "CRASH_SALVAGE_CAPTURED"
        task.execution_summary = build_execution_summary(task)
        task.save(self.task_root)
        return salvage

    def commit_verified_job(self, queued_job: Mapping[str, Any]) -> dict[str, Any]:
        """Create path-scoped local commits for one immutable verified result."""
        expected_context = self._validated_execution_context(queued_job)
        profile, available = self._load_profile()
        current = self._configuration(profile, available)
        if not self._context_matches(expected_context, current):
            raise HarnessServiceError("PROFILE_DRIFT")
        try:
            return JobCommitter(self.working_dir, profile).commit(queued_job)
        except GitCommitError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

    def retry_review_only(self, queued_job: Mapping[str, Any]) -> dict[str, Any]:
        """Execute a fresh read-only Reviewer against one preserved Task.

        No Task id, Worker attempt, source correction, Build, or Test is created.
        The caller owns the execution lease and the JobStore owns the subsequent
        revision-safe state transition.
        """
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")
        expected_context = self._validated_execution_context(queued_job)
        profile, available_modules = self._load_profile()
        current_configuration = self._configuration(profile, available_modules)
        if not self._context_matches(expected_context, current_configuration):
            raise HarnessServiceError("PROFILE_DRIFT")
        runtime_snapshot = self._materialize_runtime_snapshot(profile, expected_context)
        execution_profile = runtime_snapshot.profile if runtime_snapshot is not None else profile
        execution_policy_root = runtime_snapshot.policy_root if runtime_snapshot is not None else None

        def configuration_guard() -> None:
            if runtime_snapshot is not None:
                try:
                    runtime_snapshot.validate()
                except RuntimeSnapshotError as exc:
                    raise HarnessServiceError("PROFILE_DRIFT") from exc
            live_profile, live_modules = self._load_profile()
            live = self._configuration(live_profile, live_modules)
            if not self._context_matches(expected_context, live):
                raise HarnessServiceError("PROFILE_DRIFT")

        result_record = dict(queued_job.get("last_result") or {})
        task_id = str(result_record.get("task_id", ""))
        if not task_id:
            raise HarnessServiceError("REVIEW_ONLY_TASK_MISSING")
        try:
            task = TaskState.load(task_id, self.task_root)
        except Exception as exc:
            raise HarnessServiceError("REVIEW_ONLY_TASK_UNREADABLE") from exc
        if task.job_id and task.job_id != str(queued_job.get("job_id", "")):
            raise HarnessServiceError("REVIEW_ONLY_TASK_LINEAGE_INVALID")
        control_recovery = is_control_state_write_failure(
            task.failure_code,
            failure_stage=task.failure_stage,
            worker_stderr=task.worker_stderr,
        )
        reviewer_recovery = (
            task.failure_origin == "TOOL_INFRA" and task.failure_stage == "REVIEW"
        )
        try:
            parsed_qa = parse_qa_request(task.worker_stdout)
        except ValueError:
            parsed_qa = {"invalid": True}
        post_worker_recovery = (
            task.failure_code == "CONTROL_EXECUTION_ERROR"
            and task.failure_stage == "CONTROL"
            and dict(task.worker_execution_evidence or {}).get("exit_code") == 0
            and dict(task.worker_execution_evidence or {}).get("timed_out") is not True
            and parsed_qa == {}
            and "HARNESS_QA_REQUEST_JSON:" in task.worker_stdout
        )
        if not (control_recovery or reviewer_recovery or post_worker_recovery):
            raise HarnessServiceError("REVIEW_ONLY_FAILURE_NOT_ELIGIBLE")
        if reviewer_recovery and task.review_status not in {
            "REVIEW_ERROR", "REVIEW_UNAVAILABLE"
        }:
            raise HarnessServiceError("REVIEW_ONLY_FAILURE_NOT_ELIGIBLE")
        if control_recovery and (
            task.review_status not in {"", "PENDING"}
            or bool(task.reviewer_invocations)
            or task.build.get("status") != "PASS"
            or task.build.get("success") is not True
            or dict(task.build_evidence.get("details") or {}).get(
                "structured_evidence_valid"
            ) is not True
        ):
            raise HarnessServiceError("REVIEW_ONLY_FAILURE_NOT_ELIGIBLE")
        snapshot_manifest = task.pre_job_snapshot_manifest or str(
            queued_job.get("pre_job_snapshot_manifest", "")
        )
        if not snapshot_manifest:
            raise HarnessServiceError("REVIEW_ONLY_BASELINE_MISSING")
        try:
            initial_baseline = self._cumulative_manager().load_job_baseline(
                snapshot_manifest
            )
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc

        def source_state() -> dict[str, str]:
            state: dict[str, str] = {}
            for relative in sorted(set(task.changed_files)):
                path = (self.working_dir / relative).resolve()
                try:
                    path.relative_to(self.working_dir)
                except ValueError as exc:
                    raise HarnessServiceError("REVIEW_ONLY_PATH_ESCAPE") from exc
                if path.is_file():
                    state[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
                elif path.exists():
                    state[relative] = "NON_FILE"
                else:
                    state[relative] = "MISSING"
            return state

        before = source_state()
        manager = self.manager_factory(
            working_dir=str(self.working_dir),
            model="",
            max_retry=1,
            timeout=300,
            worker_type="codex",
            codex_model=None,
            max_workers=1,
            profile=execution_profile,
            task_root=self.task_root,
            configuration_guard=configuration_guard,
            policy_root=execution_policy_root,
            initial_baseline=initial_baseline,
        )
        try:
            configuration_guard()
            manager_result = (
                manager.run_control_recovery(task)
                if control_recovery
                else manager.run_post_worker_recovery(task)
                if post_worker_recovery
                else manager.run_review_only(task)
            )
            configuration_guard()
        except HarnessServiceError:
            raise
        except Exception as exc:
            raise HarnessServiceError(
                "REVIEW_ONLY_EXECUTION_FAILED", type(exc).__name__
            ) from exc
        finally:
            manager.shutdown()
        if not post_worker_recovery and source_state() != before:
            raise HarnessServiceError("REVIEW_ONLY_SOURCE_MUTATION_DETECTED")

        task.ended_at = __import__("datetime").datetime.now().astimezone().isoformat()
        task.execution_summary = build_execution_summary(task)
        task_path = task.save(self.task_root)
        report_path = self.reporter.save_markdown(
            task,
            model=str(dict(queued_job.get("request") or {}).get("model", "")),
            out_dir=self.task_root,
        )
        outcome = self._safe_outcome(
            task, manager_result, task_path, report_path
        )
        if control_recovery:
            outcome["recovery_kind"] = "CONTROL_STATE_WRITE_RECOVERY"
            outcome["worker_invoked"] = False
            outcome["git_regenerated"] = False
            outcome["build_rebuilt"] = True
            outcome["control_recovery_evidence"] = dict(
                task.control_recovery_evidence
            )
        elif post_worker_recovery:
            outcome["recovery_kind"] = "POST_WORKER_QA_PARSE_RECOVERY"
            outcome["worker_invoked"] = False
            outcome["worker_result_reused"] = True
            outcome["git_regenerated"] = True
            outcome["build_rebuilt"] = True
            outcome["control_recovery_evidence"] = dict(
                task.control_recovery_evidence
            )
        return outcome

    def recover_integration_only(self, queued_job: Mapping[str, Any]) -> dict[str, Any]:
        """Recover a post-verification integration blocker without a Worker."""
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")
        expected_context = self._validated_execution_context(queued_job)
        profile, available_modules = self._load_profile()
        current = self._configuration(profile, available_modules)
        if not self._context_matches(expected_context, current):
            raise HarnessServiceError("PROFILE_DRIFT")
        result_record = dict(queued_job.get("last_result") or {})
        task_id = str(result_record.get("task_id", ""))
        if not task_id:
            raise HarnessServiceError("INTEGRATION_RECOVERY_TASK_MISSING")
        task = TaskState.load(task_id, self.task_root)
        if task.job_id and task.job_id != str(queued_job.get("job_id", "")):
            raise HarnessServiceError("INTEGRATION_RECOVERY_LINEAGE_INVALID")
        if not task.commit_blockers or any(
            not str(item).startswith("EOL_REPAIR_ERROR:")
            for item in task.commit_blockers
        ):
            raise HarnessServiceError("INTEGRATION_RECOVERY_BLOCKER_NOT_ELIGIBLE")
        snapshot_manifest = task.pre_job_snapshot_manifest or str(
            queued_job.get("pre_job_snapshot_manifest", "")
        )
        try:
            baseline = self._cumulative_manager().load_job_baseline(snapshot_manifest)
        except CumulativePolicyError as exc:
            raise HarnessServiceError(exc.code, exc.detail) from exc
        before = {
            relative: hashlib.sha256((self.working_dir / relative).read_bytes()).hexdigest()
            for relative in sorted(set(task.changed_files))
            if (self.working_dir / relative).is_file()
        }
        if before != dict(task.changed_file_sha256 or {}):
            raise HarnessServiceError("INTEGRATION_RECOVERY_SOURCE_HASH_STALE")
        manager = self.manager_factory(
            working_dir=str(self.working_dir), model="", max_retry=1, timeout=300,
            worker_type="codex", codex_model=None, max_workers=1,
            profile=profile, task_root=self.task_root, initial_baseline=baseline,
        )
        try:
            manager_result = manager.run_integration_recovery(task)
        finally:
            manager.shutdown()
        after = {
            relative: hashlib.sha256((self.working_dir / relative).read_bytes()).hexdigest()
            for relative in sorted(set(task.changed_files))
            if (self.working_dir / relative).is_file()
        }
        if after != before:
            raise HarnessServiceError("INTEGRATION_RECOVERY_SOURCE_MUTATION_DETECTED")
        task.ended_at = __import__("datetime").datetime.now().astimezone().isoformat()
        task.execution_summary = build_execution_summary(task)
        task_path = task.save(self.task_root)
        report_path = self.reporter.save_markdown(
            task,
            model=str(dict(queued_job.get("request") or {}).get("model", "")),
            out_dir=self.task_root,
        )
        outcome = self._safe_outcome(task, manager_result, task_path, report_path)
        outcome.update({
            "recovery_kind": "WORKER_FREE_INTEGRATION_RECOVERY",
            "worker_invoked": False,
            "source_regenerated": False,
            "build_reused": True,
            "review_reused": True,
            "verification_reused": True,
        })
        return outcome

    def post_worker_recovery_outcome(
        self, queued_job: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Rehydrate an already-completed post-Worker verdict for Queue recording."""
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")
        expected_context = self._validated_execution_context(queued_job)
        profile, available_modules = self._load_profile()
        current = self._configuration(profile, available_modules)
        if not self._context_matches(expected_context, current):
            raise HarnessServiceError("PROFILE_DRIFT")
        prior = dict(queued_job.get("last_result") or {})
        task_id = str(prior.get("task_id", ""))
        task = TaskState.load(task_id, self.task_root) if task_id else None
        evidence = dict(task.control_recovery_evidence or {}) if task else {}
        if (
            task is None
            or task.job_id != str(queued_job.get("job_id", ""))
            or evidence.get("kind") != "POST_WORKER_QA_PARSE_RECOVERY"
            or evidence.get("worker_invoked") is not False
            or evidence.get("worker_result_reused") is not True
            or task.build.get("status") != "PASS"
            or task.review_status not in {"REVIEW_PASS", "REVIEW_FAIL"}
            or task.status not in {"SUCCESS", "AWAITING_QA"}
        ):
            raise HarnessServiceError("POST_WORKER_RESULT_RECORD_INVALID")
        expected_hashes = dict(task.changed_file_sha256 or evidence.get("source_sha256") or {})
        for relative, expected_hash in expected_hashes.items():
            path = (self.working_dir / relative).resolve()
            try:
                path.relative_to(self.working_dir)
            except ValueError as exc:
                raise HarnessServiceError("REVIEW_ONLY_PATH_ESCAPE") from exc
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
                raise HarnessServiceError("POST_WORKER_RESULT_SOURCE_STALE", relative)
        task_path = TaskState.task_date_dir(task.task_id, self.task_root) / f"{task.task_id}.json"
        outcome = self._safe_outcome(
            task,
            ManagerResult(success=task.status == "SUCCESS"),
            task_path,
            None,
        )
        outcome.update({
            "recovery_kind": "POST_WORKER_QA_PARSE_RECOVERY",
            "worker_invoked": False,
            "worker_result_reused": True,
            "git_regenerated": True,
            "build_rebuilt": True,
            "control_recovery_evidence": evidence,
        })
        return outcome

    def reproject_strict_batch_result(
        self, queued_job: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Re-project one preserved, fully verified Task through the corrected
        strict-batch test contract (0.9.0.10).

        No Worker, Build, Tester, or Reviewer runs: this only rebuilds the
        recorded outcome from the preserved Task evidence after the official
        Tester verdict stopped being masked by an approved skip-by-default
        build phase. Every gate must already be PASS on the Task and the
        worktree must still match the recorded source hashes.
        """
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")
        expected_context = self._validated_execution_context(queued_job)
        profile, available_modules = self._load_profile()
        current = self._configuration(profile, available_modules)
        if not self._context_matches(expected_context, current):
            raise HarnessServiceError("PROFILE_DRIFT")
        prior = dict(queued_job.get("last_result") or {})
        if (
            str(queued_job.get("status", "")) != "AWAITING_QA"
            or str(prior.get("queue_decision_code", ""))
            != "CUMULATIVE_VERIFICATION_CONTRACT_INVALID"
        ):
            raise HarnessServiceError("STRICT_REPROJECTION_NOT_ELIGIBLE")
        task_id = str(prior.get("task_id", ""))
        if not task_id:
            raise HarnessServiceError("STRICT_REPROJECTION_TASK_MISSING")
        task = TaskState.load(task_id, self.task_root)
        if task.job_id and task.job_id != str(queued_job.get("job_id", "")):
            raise HarnessServiceError("STRICT_REPROJECTION_LINEAGE_INVALID")
        if (
            task.status != "SUCCESS"
            or task.stage != "DONE"
            or dict(task.build or {}).get("status") != "PASS"
            or dict(task.build or {}).get("success") is not True
            or not bool(task.test_required)
            or str(task.test_status) != "PASS"
            or str(task.review_status) != "REVIEW_PASS"
            or str(task.verification_status) != "VERIFIED"
            or str(task.failure_code or "")
        ):
            raise HarnessServiceError("STRICT_REPROJECTION_EVIDENCE_INVALID")
        expected_hashes = dict(task.changed_file_sha256 or {})
        if not expected_hashes:
            raise HarnessServiceError("STRICT_REPROJECTION_EVIDENCE_INVALID")
        for relative, expected_hash in expected_hashes.items():
            path = (self.working_dir / relative).resolve()
            try:
                path.relative_to(self.working_dir)
            except ValueError as exc:
                raise HarnessServiceError("REVIEW_ONLY_PATH_ESCAPE") from exc
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
                raise HarnessServiceError("STRICT_REPROJECTION_SOURCE_STALE", relative)
        task_path = TaskState.task_date_dir(task.task_id, self.task_root) / f"{task.task_id}.json"
        outcome = self._safe_outcome(
            task,
            ManagerResult(success=True),
            task_path,
            None,
        )
        outcome.update({
            "recovery_kind": "STRICT_RESULT_REPROJECTION",
            "worker_invoked": False,
            "worker_result_reused": True,
            "git_regenerated": True,
            "build_reused": True,
            "review_reused": True,
            "tester_reused": True,
        })
        return outcome

    def _materialize_runtime_snapshot(
        self,
        profile: object,
        execution_context: Mapping[str, Any],
    ) -> RuntimeSnapshot | None:
        # Production profile selection always returns ProjectProfile.  The narrow
        # compatibility branch exists only for injected test doubles and legacy
        # manager factories; it never applies to a real validated Job.
        from project_profile import ProjectProfile

        if not isinstance(profile, ProjectProfile):
            return None
        try:
            return materialize_runtime_snapshot(
                snapshot_root=self.snapshot_root,
                profile=profile,
                policy_root=self._policy_source_root(),
                schema_file=Path(PROFILE_SCHEMA_FILE),
                workspace=self.working_dir,
                execution_context=execution_context,
                runtime_files=self._policy_files or (),
            )
        except RuntimeSnapshotError as exc:
            raise HarnessServiceError(exc.code) from exc
        except ProfileError as exc:
            raise HarnessServiceError("RUNTIME_SNAPSHOT_INVALID") from exc

    @staticmethod
    def _validated_execution_context(queued_job: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return validate_execution_context(queued_job.get("execution_context"))
        except JobContractError as exc:
            raise HarnessServiceError(exc.code, exc.field) from exc

    @staticmethod
    def _context_matches(
        expected: Mapping[str, Any], current: Mapping[str, Any]
    ) -> bool:
        return all(expected[key] == current.get(key) for key in EXECUTION_CONTEXT_KEYS)

    @staticmethod
    def _safe_outcome(
        task: TaskState,
        result: ManagerResult,
        task_path: Path,
        report_path: Path | None,
    ) -> dict[str, Any]:
        # 0.8.2/0.8.3 programmatic Manager adapters predate commit_eligible and
        # build_evidence. Preserve that fixture contract only when both newer
        # structures are entirely absent; operational Managers always emit them.
        legacy_commit_contract = (
            not task.build_evidence and not task.commit_blockers
        )
        fully_verified = (
            bool(result.success)
            and task.status == "SUCCESS"
            and task.stage == "DONE"
            and task.verification_status == "VERIFIED"
            and task.build.get("status") == "PASS"
            and (not task.test_required or task.test_status == "PASS")
            and task.review_status == "REVIEW_PASS"
            and not task.failure_code
            and bool(task.external_frozen_integrity)
            and bool(task.baseline_declaration_integrity)
            and not task.unexpected_external_dirty_files
            and (bool(task.commit_eligible) or legacy_commit_contract)
        )
        integration_blocked = bool(
            result.success
            and task.commit_blockers
            and task.build.get("status") == "PASS"
            and task.review_status == "REVIEW_PASS"
            and task.verification_status == "VERIFIED"
        )
        return {
            "task_id": task.task_id,
            "status": task.status,
            "stage": task.stage,
            "success": fully_verified,
            "verification_status": task.verification_status,
            "completion_kind": task.completion_kind,
            "failure_stage": "INTEGRATION" if integration_blocked else task.failure_stage,
            "failure_code": "INTEGRATION_TECHNICAL_RECOVERY_REQUIRED" if integration_blocked else task.failure_code,
            "failure_reason": scrub_secrets(
                task.commit_blockers[0] if integration_blocked else task.failure_reason or result.message
            )[:2000],
            "failure_type": "infrastructure" if integration_blocked else task.failure_type,
            "fix_scope": task.fix_scope,
            "severity": task.severity,
            "secondary_failures": list(result.secondary_failures[:20]),
            "build_status": str(task.build.get("status", "")),
            # 0.9.0.10: when the official Tester was required, its verdict is
            # the canonical test outcome. The build-embedded test phase stays
            # a fallback only for legacy states with no recorded verdict;
            # otherwise an approved skip-by-default build phase would mask a
            # passing official Tester as SKIPPED in the strict batch contract.
            "test_status": (
                "NOT_REQUIRED" if not task.test_required
                else str(task.test_status or task.test.get("status", ""))
            ),
            "tester_status": task.test_status,
            "review_status": task.review_status,
            "review_risk": task.review_risk,
            "review_summary": scrub_secrets(task.review_result)[:2000],
            "changed_files": list(task.changed_files[:200]),
            "task_owned_changed_files": list(task.task_owned_changed_files[:200]),
            "inherited_batch_delta_files": list(task.inherited_batch_delta_files[:200]),
            "unexpected_external_dirty_files": list(task.unexpected_external_dirty_files[:200]),
            "external_frozen_integrity": bool(task.external_frozen_integrity),
            "baseline_declaration_integrity": bool(task.baseline_declaration_integrity),
            "pre_job_snapshot_id": task.pre_job_snapshot_id,
            "failure_fingerprint": task.failure_fingerprint,
            "retry_strategy": task.retry_strategy,
            "original_worker": task.original_worker,
            "original_model": task.original_model,
            "retry_worker": task.retry_worker,
            "retry_model": task.retry_model,
            "reroute_reason": task.reroute_reason,
            "fallback_availability_evidence": task.fallback_availability_evidence,
            "planning_artifact_errors": list(task.planning_artifact_errors[-10:]),
            "no_diff_gate": dict(task.no_diff_gate),
            "no_task_delta_evidence": dict(task.no_task_delta_evidence),
            "scoped_rollback_targets": list(task.scoped_rollback_targets[:200]),
            "preserved_predecessor_delta_files": list(
                task.preserved_predecessor_delta_files[:200]
            ),
            "remediation_cycle": task.remediation_cycle,
            "user_intervention_reason": task.user_intervention_reason,
            "reviewed_task_files": list(task.reviewed_task_files[:200]),
            "inspected_unchanged_target_files": list(
                task.inspected_unchanged_target_files[:200]
            ),
            "profile_build_commands": list(task.profile_build_commands),
            "build_evidence": dict(task.build_evidence),
            "checkpoint_id": task.checkpoint_id,
            "checkpoint_status": task.checkpoint_status,
            "checkpoint_manifest": task.checkpoint_manifest,
            "checkpoint_event": task.checkpoint_event,
            "checkpoint_failure_evidence": dict(task.checkpoint_failure_evidence),
            "restored_checkpoint_id": task.restored_checkpoint_id,
            "restored_for_retry": bool(task.restored_for_retry),
            "restored_evidence_epoch": task.restored_evidence_epoch,
            "requested_worker": task.requested_worker,
            "requested_model": task.requested_model,
            "planned_worker": task.planned_worker,
            "planned_model": task.planned_model,
            "selected_worker": task.selected_worker,
            "selected_model": task.selected_model,
            "selection_reason": task.selection_reason,
            "actual_worker": task.actual_worker,
            "actual_model": task.actual_model,
            "actual_executable": task.actual_executable,
            "actual_invocation_id": task.actual_invocation_id,
            "worker_execution_evidence": dict(task.worker_execution_evidence),
            "context_pack_id": task.context_pack_id,
            "context_manifest_path": task.context_manifest_path,
            "context_total_chars": int(task.context_total_chars),
            "context_selected_count": int(task.context_selected_count),
            "context_warnings": list(task.context_warnings),
            "prompt_template_version": task.prompt_template_version,
            "eol_repairs": list(task.eol_repairs[:200]),
            "changed_file_sha256": dict(task.changed_file_sha256),
            "commit_eligible": bool(task.commit_eligible),
            "commit_blockers": list(task.commit_blockers[:100]),
            "commit_blocker_history": list(task.commit_blocker_history[-20:]),
            "eol_repair_diagnostics": list(task.eol_repair_diagnostics[-20:]),
            "artifact_files": list(task.artifact_files[:100]),
            "is_rolled_back": bool(task.is_rolled_back),
            "worktree_disposition": task.worktree_disposition,
            "retry_count": int(task.retry_count),
            "retry_domain": task.retry_domain,
            "runtime_recovery_count": int(task.runtime_recovery_count),
            "technical_execution_retry_count": int(task.technical_execution_retry_count),
            "product_semantic_retry_count": int(task.product_semantic_retry_count),
            "runtime_recovery_history": list(task.runtime_recovery_history),
            "logical_job_id": task.logical_job_id,
            "materialization_id": task.materialization_id,
            "execution_id": task.execution_id,
            "attempt_id": task.attempt_id,
            "runtime_process_id": task.runtime_process_id,
            "native_session_binding_id": task.native_session_binding_id,
            "native_session_id": task.native_session_id,
            "native_turn_id": task.native_turn_id,
            "command_id": task.command_id,
            "runtime_adapter": dict(task.runtime_adapter),
            "validation_dispositions": list(task.validation_dispositions),
            "selective_revalidation": dict(task.selective_revalidation),
            "product_readiness": task.product_readiness,
            "user_action_required": bool(task.user_action_required),
            "open_decision_count": int(task.open_decision_count),
            "open_decisions": list(task.open_decisions),
            "troubleshooting_bundle": dict(task.troubleshooting_bundle),
            "reviewer_recovery_count": int(task.reviewer_recovery_count),
            "reviewer_recovery_limit": int(task.reviewer_recovery_limit),
            "attempt_history": list(task.attempt_history[-3:]),
            "handoff_summary": scrub_secrets(task.handoff_context)[:2000],
            "task_state_path": task_path.as_posix(),
            "report_path": report_path.as_posix() if report_path is not None else "",
            "profile_id": task.profile_id,
            "scope_authority": task.scope_authority,
            "scope_provenance": dict(task.scope_provenance),
            "policy_refs": list(task.policy_refs),
            "policy_overlays": list(task.policy_overlays),
            "effective_policy": dict(task.effective_policy),
            "policy_changed_after_materialization": task.policy_changed_after_materialization,
            "materialized_execution": dict(task.materialized_execution),
            "test_plan": dict(task.test_plan),
            "test_scope_hash": task.test_scope_hash,
            "gate_evidence": dict(task.gate_evidence),
            "official_tester_invoked": task.official_tester_invoked,
            "official_tester_execution": dict(task.official_tester_execution),
            "developer_change_report": dict(task.developer_change_report),
            "report_diagnostics": list(task.report_diagnostics),
            "context_refs": list(task.context_refs),
            "prompt_metrics": dict(task.prompt_metrics),
            "failure_origin": "HARNESS_CAUSED" if integration_blocked else task.failure_origin,
            "user_qa_required": False if integration_blocked else bool(task.user_intervention_reason),
            "worker_remediation_allowed": False if integration_blocked else True,
            "control_field": task.control_field,
            "control_recovery_evidence": dict(task.control_recovery_evidence),
            "review_policy": dict(task.review_policy),
            "reviewer_invocations": list(task.reviewer_invocations),
            "progress_revision": int(task.progress_revision),
            "progress_snapshot": dict(task.progress_snapshot),
            "qa_type": task.qa_type,
            "hold_scope": task.hold_scope,
            "machine_verified": bool(task.machine_verified),
            "qa_request": dict(task.qa_request),
            "candidate_id": task.candidate_id,
            "candidate_manifest": task.candidate_manifest,
            "started_at": task.started_at,
            "ended_at": task.ended_at,
            "execution_summary": dict(task.execution_summary),
            "crash_salvage": dict(task.crash_salvage),
            "crash_recovery_handoff": scrub_secrets(task.crash_recovery_handoff)[:4000],
        }

    @staticmethod
    def _restore_checkpoint_candidate(
        prior_result: Mapping[str, Any], outer_attempt: int
    ) -> str:
        checkpoint_manifest = str(prior_result.get("checkpoint_manifest", ""))
        forbidden_restore = any(
            marker in str(prior_result.get("failure_code", "")).upper()
            for marker in (
                "USER_DECISION", "BUSINESS", "DB_CONTRACT", "SECURITY",
                "PII", "PROFILE_DRIFT", "UNEXPECTED", "INTEGRITY",
            )
        )
        forbidden_restore = forbidden_restore or (
            str(prior_result.get("severity", "")).upper() == "HIGH"
            or str(prior_result.get("fix_scope", "")).upper() == "NON_RETRYABLE"
        )
        if (
            checkpoint_manifest
            and int(outer_attempt) > 0
            and not forbidden_restore
            and str(prior_result.get("failure_type", ""))
            in {"technical", "build", "test", "review"}
        ):
            return checkpoint_manifest
        return ""

    @staticmethod
    def _control_failure_result(
        task: TaskState,
        code: str,
        *,
        mutation_uncertain: bool,
        control_field: str = "",
    ) -> ManagerResult:
        task.failure_stage = "CONTROL"
        task.failure_code = code
        task.failure_reason = code + (f": {control_field}" if control_field else "")
        task.failure_origin = HARNESS_CAUSED
        task.control_field = control_field
        task.failure_type = "infrastructure"
        task.fix_scope = "NON_RETRYABLE"
        task.severity = "HIGH"
        task.qa_type = "SAFETY_INTEGRITY"
        task.hold_scope = "BATCH"
        task.machine_verified = False
        if task.changed_files:
            task.worktree_disposition = "PRESERVED"
        elif mutation_uncertain:
            task.worktree_disposition = "UNKNOWN"
        else:
            task.worktree_disposition = "NO_DELTA"
        if task.worktree_disposition in {"PRESERVED", "UNKNOWN"}:
            task.status = "AWAITING_QA"
            task.stage = "AWAITING_QA"
            task.verification_status = "PARTIAL"
        else:
            task.status = "FAILED"
            task.stage = "DONE"
            task.verification_status = "FAILED"
        return ManagerResult(
            success=False,
            message=code,
            secondary_failures=(code,),
        )

    def execute(
        self,
        queued_job: Mapping[str, Any],
        *,
        on_task_started: Callable[[str], Mapping[str, Any] | None],
        initial_delta_applier: Callable[[], Mapping[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        try:
            with self._lifecycle.dispatch():
                return self._execute(queued_job, on_task_started=on_task_started,
                                     initial_delta_applier=initial_delta_applier)
        except LifecycleError as exc:
            raise HarnessServiceError(exc.code) from exc

    def materialize_execution(self, job, repository):
        """Resolve current inputs just before safe dispatch and durably freeze them."""
        from control_repository import digest
        with self._lifecycle.dispatch():
            existing = repository.active_execution(job["job_id"])
            if existing:
                return existing
            profile, available = self._load_profile()
            configuration = self._configuration(profile, available)
            context = {key: configuration[key] for key in EXECUTION_CONTEXT_KEYS}
            snapshot = self._materialize_runtime_snapshot(profile, context)
            if snapshot is None:
                raise HarnessServiceError("MATERIALIZATION_SNAPSHOT_REQUIRED")
            request = JobRequest.from_mapping(job["request"], allowed_modules=available)
            policy = resolve_effective_policy(profile_id=profile.id,
                policy_snapshot_sha256=context["policy_snapshot_sha256"],
                job_policy_refs=request.policy_refs, policy_overlays=request.policy_overlays)
            from materialization_policy import freeze_routing, PolicyConflict
            from test_plan import derive_test_plan
            try:
                policy = freeze_routing(request, policy)
                test_plan = derive_test_plan(request, self.working_dir)
            except (PolicyConflict, ValueError) as exc:
                raise HarnessServiceError(getattr(exc, "code", "TEST_PLAN_INVALID")) from exc
            from tester import TESTER_MODEL, TESTER_TIMEOUT
            policy["resolved_tester"] = {"model": TESTER_MODEL, "timeout": TESTER_TIMEOUT}
            from policy_catalog import effective_policy_hash
            policy["effective_policy_sha256"] = effective_policy_hash(policy)
            baseline_id = str(job.get("active_baseline_id", ""))
            if not baseline_id and job.get('qa_policy_resolution'):
                # Pre-claim freeze uses the already canonical fresh-input binding;
                # never freeze this cumulative successor against PER_JOB.
                baseline_id = str((job.get('fresh_input') or {}).get('active_baseline_id', ''))
                if not baseline_id:
                    raise HarnessServiceError('CUMULATIVE_BASELINE_MISSING')
            baseline = self.control_root / "cumulative/batch-baselines" / baseline_id / "manifest.json"
            if baseline_id and not baseline.is_file():
                raise HarnessServiceError("CUMULATIVE_BASELINE_MISSING")
            baseline_hash = hashlib.sha256(baseline.read_bytes()).hexdigest() if baseline_id else digest({"baseline": "PER_JOB"})
            from runtime_identity import RuntimeIdentity
            identity = RuntimeIdentity.load(HARNESS_ROOT)
            from runtime_contract import descriptor_for, freeze_runtime_contracts
            worker_descriptor = descriptor_for(request.worker)
            reviewer_descriptor = descriptor_for("codex")
            if not worker_descriptor.schema_compatible:
                raise HarnessServiceError("RUNTIME_CONTRACT_PREFLIGHT_FAILED", worker_descriptor.detail)
            if not reviewer_descriptor.schema_compatible:
                raise HarnessServiceError("REVIEWER_RUNTIME_CONTRACT_PREFLIGHT_FAILED", reviewer_descriptor.detail)
            runtime_contracts = freeze_runtime_contracts(
                worker=worker_descriptor, reviewer=reviewer_descriptor,
            )
            from verification_contract import derive_verification_contract
            verification_contract = derive_verification_contract(
                request.to_dict(), test_plan
            )
            verification_environment_hash = digest({
                "profile_snapshot_sha256": context["profile_snapshot_sha256"],
                "policy_snapshot_sha256": context["policy_snapshot_sha256"],
                "workspace_identity_sha256": context.get("workspace_identity_sha256", ""),
                "runtime_contract_hash": runtime_contracts["runtime_contract_hash"],
            })
            surface = digest({"schema_version": 2, "context": context,
                              "effective_policy_hash": policy["effective_policy_sha256"],
                              "runtime_manifest": identity.runtime_manifest_sha256,
                              "runtime_contract_hash": runtime_contracts["runtime_contract_hash"],
                              "verification_contract_hash": verification_contract["verification_contract_hash"],
                              "verification_environment_hash": verification_environment_hash})
            return repository.materialize(job, context=context, policy=policy,
                baseline_hash=baseline_hash, execution_surface_hash=surface,
                extra={"runtime_snapshot_ref": str(snapshot.root),
                       "test_plan": test_plan,
                       "runtime_contracts": runtime_contracts,
                       "runtime_contract_hash": runtime_contracts["runtime_contract_hash"],
                       "verification_contract": verification_contract,
                       "verification_environment_hash": verification_environment_hash,
                       "current_requirement": job.get("current_requirement", request.requirement)})

    def close(self) -> None:
        self._lifecycle.close()

    def restart(self) -> None:
        if self._lease_held:
            raise HarnessServiceError("HARNESS_BUSY")
        self._lifecycle.restart()

    def _execute(
        self,
        queued_job: Mapping[str, Any],
        *,
        on_task_started: Callable[[str], Mapping[str, Any] | None],
        initial_delta_applier: Callable[[], Mapping[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        if not self._lease_held:
            raise HarnessServiceError("HARNESS_LEASE_REQUIRED")

        frozen_execution = dict(queued_job.get("materialized_execution") or {})
        repository = None
        if frozen_execution:
            from control_repository import ControlRepository
            repository = ControlRepository(self.control_root / "control.sqlite3", read_only=True)
            if repository.global_stop().get("active"):
                raise HarnessServiceError("GLOBAL_STOP")
        expected_context = self._validated_execution_context(queued_job)
        if frozen_execution:
            if frozen_execution.get("execution_context") != expected_context:
                raise HarnessServiceError("MATERIALIZED_CONTRACT_MISMATCH")
            if (frozen_execution.get("request") != queued_job.get("request")
                    or frozen_execution.get("current_requirement") != queued_job.get("current_requirement")):
                raise HarnessServiceError("MATERIALIZED_CONTRACT_MISMATCH")
            runtime_snapshot = load_frozen_runtime_snapshot(self.snapshot_root, self.working_dir, expected_context)
            profile = runtime_snapshot.profile
            available_modules = discover_modules(self.working_dir, profile)
        else:
            profile, available_modules = self._load_profile()
            current_configuration = self._configuration(profile, available_modules)
            if not self._context_matches(expected_context, current_configuration):
                raise HarnessServiceError("PROFILE_DRIFT")
            runtime_snapshot = self._materialize_runtime_snapshot(profile, expected_context)
        execution_profile = (
            runtime_snapshot.profile if runtime_snapshot is not None else profile
        )
        execution_policy_root = (
            runtime_snapshot.policy_root if runtime_snapshot is not None else None
        )

        policy_diagnostics = {"changed": False}

        def configuration_guard() -> None:
            if repository is not None and repository.global_stop().get("active"):
                raise HarnessServiceError("GLOBAL_STOP")
            if runtime_snapshot is not None:
                try:
                    runtime_snapshot.validate()
                except RuntimeSnapshotError as exc:
                    raise HarnessServiceError("PROFILE_DRIFT") from exc
            try:
                live_profile, live_modules = self._load_profile()
                live_configuration = self._configuration(live_profile, live_modules)
            except HarnessServiceError as exc:
                if runtime_snapshot is not None and (
                    exc.code.startswith("POLICY_") or "RUNTIME_RULE_" in exc.detail
                ):
                    policy_diagnostics["changed"] = True
                    return
                raise HarnessServiceError("PROFILE_DRIFT") from exc
            comparison = dict(live_configuration)
            if runtime_snapshot is not None:
                policy_diagnostics["changed"] |= (
                    expected_context.get("policy_snapshot_sha256")
                    != comparison.get("policy_snapshot_sha256")
                )
                comparison["policy_snapshot_sha256"] = expected_context.get("policy_snapshot_sha256")
            if not self._context_matches(expected_context, comparison):
                raise HarnessServiceError("PROFILE_DRIFT")
        # Complete the pre/copy/post transaction before allocating a Task id.
        # If a source changed while being copied, no TaskState can be orphaned.
        if runtime_snapshot is not None:
            configuration_guard()
        raw_request = dict(queued_job.get("request") or {})
        try:
            request = JobRequest.from_mapping(raw_request, allowed_modules=available_modules)
        except JobContractError as exc:
            raise HarnessServiceError(exc.code, exc.field) from exc

        current_requirement = str(
            queued_job.get("current_requirement") or request.requirement
        ).strip()
        if not current_requirement:
            raise HarnessServiceError("REQUIREMENT_MISSING")

        technical_route = dict(queued_job.get("technical_retry_route") or {})
        effective_worker = request.worker
        effective_model = request.model
        if frozen_execution:
            route = frozen_execution["effective_policy"].get("resolved_worker", {})
            effective_worker = route.get("worker", effective_worker)
            effective_model = route.get("model", effective_model)
        routing_policy = ""
        routing_evidence_sha256 = ""
        if effective_worker == "droid":
            # Legacy alias compatibility: the immutable request keeps its
            # original requested model string (requested_model evidence);
            # only the execution target resolves to the canonical model.
            try:
                effective_model, _ = resolve_droid_model(request.model)
            except JobContractError as exc:
                raise HarnessServiceError(exc.code, exc.field) from exc
        if technical_route:
            try:
                route_model, _ = resolve_droid_model(technical_route.get("model"))
            except JobContractError:
                route_model = ""
            if (
                technical_route.get("policy") != DROID_GLM_FAIL_CLOSED_POLICY
                or technical_route.get("worker") != "droid"
                or route_model != DROID_GLM_FAIL_CLOSED_MODEL
            ):
                raise HarnessServiceError("DROID_MODEL_ROUTING_POLICY_VIOLATION")
            routing_evidence_sha256 = str(
                technical_route.get("stability_evidence_sha256", "")
            ).lower()
            if not re.fullmatch(r"[0-9a-f]{64}", routing_evidence_sha256):
                raise HarnessServiceError("TECHNICAL_RETRY_EVIDENCE_INVALID")
            effective_worker = "droid"
            effective_model = DROID_GLM_FAIL_CLOSED_MODEL
            routing_policy = DROID_GLM_FAIL_CLOSED_POLICY

        reservation = dict(queued_job.get("active_attempt_reservation") or {})
        if not reservation and not queued_job.get("job_id"):
            # Backward-compatible non-durable adapter. Durable Queue Jobs always
            # carry a reservation and never enter this branch.
            execution_attempt = int(queued_job.get("outer_attempt", 0)) + 1
            normal_budget = int(queued_job.get("max_outer_attempts", request.max_outer_attempts))
            if execution_attempt > normal_budget:
                raise HarnessServiceError("OUTER_ATTEMPT_BUDGET_EXHAUSTED")
            reservation = {
                "reservation_id": f"RSV-LEGACY-PROGRAMMATIC-{execution_attempt}",
                "attempt_kind": "TECHNICAL_RECOVERY" if technical_route else "NORMAL",
                "execution_attempt": execution_attempt,
                "normal_attempt_no": execution_attempt if not technical_route else max(0, execution_attempt - 1),
                "normal_attempt_budget": normal_budget,
                "technical_recovery_attempt_no": 1 if technical_route else 0,
                "technical_recovery_budget": 1,
                "effective_execution_limit": normal_budget,
                "provenance": {"source": "LEGACY_PROGRAMMATIC_ADAPTER"},
                "consumed_task_id": "",
            }
        if not reservation.get("reservation_id"):
            raise HarnessServiceError("ATTEMPT_RESERVATION_MISSING")
        if reservation.get("consumed_task_id"):
            raise HarnessServiceError("ATTEMPT_RESERVATION_ALREADY_CONSUMED")
        task_id = _next_task_id(self.task_root)
        task = create_task(
            requirement=current_requirement,
            worker_type=effective_worker,
            droid_model=effective_model if effective_worker == "droid" else "",
            codex_model=effective_model if effective_worker == "codex" else None,
            opencode_model=effective_model if effective_worker == "opencode" else "",
            target_module=list(request.target_modules),
            target_resources=list(request.target_resources),
            commit_summary=request.commit_summary,
            working_dir=str(self.working_dir),
            task_id=task_id,
            worker_no=1,
            codex_reasoning_effort=request.reasoning_effort,
            profile=execution_profile,
        )
        task.requested_worker = request.worker
        task.requested_model = request.model
        task.original_worker = request.worker
        task.original_model = request.model
        task.worker_routing_policy = routing_policy
        task.worker_routing_evidence_sha256 = routing_evidence_sha256
        task.handoff_context = "\n\n".join(
            item for item in (
                str(queued_job.get("crash_recovery_handoff") or "").strip(),
                str(queued_job.get("technical_retry_context") or "").strip(),
            ) if item
        )
        task.crash_recovery_handoff = str(
            queued_job.get("crash_recovery_handoff") or ""
        ).strip()
        task.pre_job_snapshot_id = str(queued_job.get("pre_job_snapshot_id", ""))
        task.pre_job_snapshot_manifest = str(queued_job.get("pre_job_snapshot_manifest", ""))
        task.queue_generation = int(queued_job.get("generation", 0))
        # 0.99.1 timing breakdown: queue-side wait/admission from job history.
        try:
            from execution_evidence import job_admission_timing
            admission_timing = job_admission_timing(queued_job)
            task.queue_wait_seconds = int(admission_timing["queue_wait_seconds"])
            task.admission_control_seconds = int(admission_timing["admission_control_seconds"])
        except Exception:
            task.queue_wait_seconds = -1
            task.admission_control_seconds = -1
        task.active_baseline_id = str(queued_job.get("active_baseline_id", ""))
        task.batch_id = str(queued_job.get("batch_id", ""))
        task.job_id = str(queued_job.get("job_id", ""))
        task.materialized_execution = frozen_execution
        task.logical_job_id = str(frozen_execution.get("logical_job_id", ""))
        task.materialization_id = str(frozen_execution.get("materialization_id", ""))
        task.execution_id = str(frozen_execution.get("execution_id", ""))
        task.control_repository_path = (
            str(self.control_root / "control.sqlite3") if frozen_execution else ""
        )
        task.test_plan = copy.deepcopy(frozen_execution.get("test_plan", {}))
        task.test_scope_hash = task.test_plan.get("scope_hash", "")
        if frozen_execution:
            def record_attempt(role, attempt_id, model):
                from control_repository import ControlRepository
                from gate_evidence import identity
                return ControlRepository(self.control_root / "control.sqlite3").start_attempt(
                    frozen_execution, attempt_id=attempt_id, role=role, model=model,
                    candidate_hash=identity(task, self.working_dir)["candidate_hash"])
            task._record_execution_attempt = record_attempt
        task.client_job_id = request.client_job_id
        task.operator_label = request.operator_label
        task.policy_overlays = list(request.policy_overlays)
        task.effective_policy = resolve_effective_policy(
            profile_id=str(expected_context.get("profile_id", "")),
            policy_snapshot_sha256=str(expected_context.get("policy_snapshot_sha256", "")),
            job_policy_refs=request.policy_refs,
            policy_overlays=request.policy_overlays,
        )
        if frozen_execution:
            task.effective_policy = copy.deepcopy(frozen_execution["effective_policy"])
        task.policy_refs = list(task.effective_policy["effective_policy_refs"])
        if "ANALYSIS_READONLY" in task.policy_overlays:
            task.task_mode = "ANALYSIS"
        task.context_refs = list(request.context_refs)
        task.qa_type = request.qa_type
        task.hold_scope = request.hold_scope
        task.machine_verified = request.machine_verified
        task.outer_attempt = int(reservation.get("execution_attempt", 0))
        if task.outer_attempt <= 0:
            raise HarnessServiceError("ATTEMPT_RESERVATION_INVALID")
        task.attempt_reservation = reservation
        task.reservation_id = str(reservation.get("reservation_id", ""))
        task.attempt_kind = str(reservation.get("attempt_kind", ""))
        task.execution_attempt = task.outer_attempt
        task.normal_attempt_no = int(reservation.get("normal_attempt_no", 0))
        task.normal_attempt_budget = int(reservation.get("normal_attempt_budget", 0))
        task.technical_recovery_attempt_no = int(
            reservation.get("technical_recovery_attempt_no", 0)
        )
        task.technical_recovery_budget = int(
            reservation.get("technical_recovery_budget", 0)
        )
        task.effective_execution_limit = int(
            reservation.get("effective_execution_limit", 0)
        )
        # Legacy 0.8.2/0.8.3 programmatic adapters did not carry cumulative
        # integrity fields. Explicit false still fails closed; absence preserves
        # the pre-cumulative contract. Current Queue claims always provide both.
        task.external_frozen_integrity = bool(
            queued_job.get("external_frozen_integrity", True)
        )
        task.baseline_declaration_integrity = bool(
            queued_job.get("baseline_declaration_integrity", True)
        )
        task.inherited_batch_delta_files = list(
            queued_job.get("inherited_batch_delta_files") or []
        )
        task.preserved_predecessor_delta_files = list(
            task.inherited_batch_delta_files
        )
        task.unexpected_external_dirty_files = list(
            queued_job.get("unexpected_external_dirty_files") or []
        )
        try:
            attempt_record = on_task_started(task_id)
            if isinstance(attempt_record, Mapping):
                task.attempt_id = str(attempt_record.get("attempt_id", ""))
            elif frozen_execution:
                task.attempt_id = task_id + "-WORKER"
        except Exception as exc:
            # Queue lineage owns the Task id.  If that commit fails, persisting a
            # TaskState/report would manufacture exactly the orphan that the
            # PREPARING transaction is designed to prevent.
            raise HarnessServiceError("CONTROL_STATE_COMMIT_FAILED") from exc
        # Queue lineage is committed before the TaskState file. If the process dies
        # between these operations, restart reconciliation can identify the exact
        # interrupted Job instead of leaving an unowned RUNNING TaskState.
        try:
            task_path = task.save(self.task_root)
        except ControlPathError as exc:
            raise HarnessServiceError(exc.code, exc.field) from exc
        except Exception as exc:
            raise HarnessServiceError("CONTROL_TASK_STATE_WRITE_FAILED") from exc

        manager: Manager | None = None
        manager_submitted = False
        try:
            manager_kwargs: dict[str, Any] = dict(
                working_dir=str(self.working_dir),
                model=request.model if request.worker == "droid" else "",
                max_retry=3,
                timeout=300,
                worker_type=request.worker,
                codex_model=request.model if request.worker == "codex" else None,
                opencode_model=request.model if request.worker == "opencode" else "",
                max_workers=1,
                profile=execution_profile,
                task_root=self.task_root,
                configuration_guard=configuration_guard,
                policy_root=execution_policy_root,
                checkpoint_store=AttemptCheckpointStore(
                    self.working_dir, self.control_root
                ),
                initial_delta_applier=initial_delta_applier,
            )
            prior_result = dict(queued_job.get("last_result") or {})
            checkpoint_seed = dict(queued_job.get("checkpoint_seed") or {})
            checkpoint_manifest = str(checkpoint_seed.get("manifest_path", ""))
            if not checkpoint_manifest:
                checkpoint_manifest = self._restore_checkpoint_candidate(
                    prior_result, int(queued_job.get("outer_attempt", 0))
                )
            if checkpoint_manifest:
                manager_kwargs["restore_checkpoint_manifest"] = checkpoint_manifest
            if checkpoint_seed:
                if checkpoint_seed.get("valid") is not True:
                    raise HarnessServiceError("CORRECTIVE_CHECKPOINT_INVALID")
                manager_kwargs["restore_checkpoint_source_job_id"] = str(
                    checkpoint_seed.get("parent_job_id", "")
                )
                manager_kwargs["restore_checkpoint_source_client_job_id"] = str(
                    checkpoint_seed.get("parent_client_job_id", "")
                )
            snapshot_manifest = str(
                queued_job.get("pre_job_snapshot_manifest", "")
            )
            if snapshot_manifest:
                try:
                    manager_kwargs["initial_baseline"] = (
                        self._cumulative_manager().load_job_baseline(
                            snapshot_manifest
                        )
                    )
                except CumulativePolicyError as exc:
                    raise HarnessServiceError(exc.code, exc.detail) from exc
            manager = self.manager_factory(**manager_kwargs)
            future = manager.submit_task(task)
            manager_submitted = True
            result = future.result()
            if not isinstance(result, ManagerResult):
                raise HarnessServiceError("MANAGER_RESULT_INVALID")
            try:
                configuration_guard()
            except HarnessServiceError as exc:
                result = self._control_failure_result(
                    task,
                    exc.code,
                    mutation_uncertain=manager_submitted,
                )
        except LifecycleError as exc:
            raise HarnessServiceError(exc.code) from exc
        except ControlPathError as exc:
            result = self._control_failure_result(
                task,
                exc.code,
                mutation_uncertain=manager_submitted,
                control_field=exc.field,
            )
        except HarnessServiceError as exc:
            result = self._control_failure_result(
                task,
                exc.code,
                mutation_uncertain=manager_submitted,
                control_field=exc.detail,
            )
        except Exception as exc:
            reason = self._log_control_exception("CONTROL_EXECUTION_ERROR", exc)
            result = self._control_failure_result(
                task,
                "CONTROL_EXECUTION_ERROR",
                mutation_uncertain=manager_submitted,
            )
            task.failure_reason = reason
        finally:
            if manager is not None:
                try:
                    manager.shutdown()
                except ControlPathError as exc:
                    result = self._control_failure_result(
                        task,
                        exc.code,
                        mutation_uncertain=manager_submitted,
                        control_field=exc.field,
                    )
                except Exception as exc:
                    reason = self._log_control_exception("CONTROL_SHUTDOWN_ERROR", exc)
                    result = self._control_failure_result(
                        task,
                        "CONTROL_SHUTDOWN_ERROR",
                        mutation_uncertain=manager_submitted,
                    )
                    task.failure_reason = reason

        task.ended_at = __import__("datetime").datetime.now().astimezone().isoformat()
        if frozen_execution and result.success:
            from source_view import validate_write_scope
            scope_result = validate_write_scope(
                task.task_owned_changed_files or task.changed_files,
                frozen_execution.get("mutation_scope") or {},
            )
            if not scope_result["valid"]:
                task.report_diagnostics.append({
                    "code": "DECLARED_WRITE_SCOPE_VIOLATION",
                    "outside_write_scope": scope_result["outside_write_scope"],
                })
                result = self._control_failure_result(
                    task, "DECLARED_WRITE_SCOPE_VIOLATION", mutation_uncertain=True
                )
        if frozen_execution and (result.success or task.status == 'SKELETON_READY'):
            from gate_evidence import identity, validate
            if validate(task, identity(task, self.working_dir), require_all=result.success):
                result = self._control_failure_result(task, "GATE_EVIDENCE_STALE", mutation_uncertain=True)
        if task.failure_code and not task.failure_origin:
            task.failure_origin = classify_failure_origin(
                task.failure_code,
                failure_stage=task.failure_stage,
                failure_type=task.failure_type,
                user_qa_required=task.status == "AWAITING_QA",
            )
        if frozen_execution and task.attempt_id:
            from runtime_adapter import RetryDomain
            from control_repository import ControlRepository
            execution_repository = ControlRepository(task.control_repository_path)
            if task.status == "AWAITING_QA" or task.qa_request.get("required") is True:
                try:
                    execution_repository.suspend_writer_authority(
                        task.execution_id,
                        reason=task.failure_code or task.qa_type or "PENDING_DECISION",
                    )
                except Exception as exc:
                    task.report_diagnostics.append({
                        "code": "WRITER_AUTHORITY_SUSPEND_FAILED",
                        "diagnostic": type(exc).__name__,
                    })
            retry_domain = task.retry_domain
            if not result.success and not retry_domain:
                retry_domain = (
                    RetryDomain.PRODUCT_SEMANTIC_RETRY.value
                    if task.failure_type in {"build", "test", "review"}
                    and task.failure_origin in {"WORKER_CAUSED", "REVIEWER_CODE_FINDING"}
                    else RetryDomain.TECHNICAL_EXECUTION_RETRY.value
                    if task.failure_type == "technical"
                    else RetryDomain.RUNTIME_RECOVERY.value
                )
            task.retry_domain = retry_domain
            if retry_domain == RetryDomain.PRODUCT_SEMANTIC_RETRY.value and not result.success:
                task.product_semantic_retry_count += 1
            try:
                execution_repository.finish_attempt(
                    task.attempt_id,
                    status="SUCCEEDED" if result.success else "FAILED",
                    retry_domain=retry_domain,
                    failure_code=task.failure_code,
                )
            except Exception as exc:
                task.report_diagnostics.append({
                    "code": "ATTEMPT_OUTCOME_PERSIST_FAILED",
                    "diagnostic": type(exc).__name__,
                })
        from decision_qa import decision_requests
        from product_readiness import derive_product_readiness
        projected_decisions = [
            {**item, "status": "OPEN"}
            for item in decision_requests({
                "qa_request": task.qa_request,
                "qa_type": task.qa_type,
                "candidate_id": task.candidate_id,
                "failure_reason": task.failure_reason,
                "deferred_contracts": list(task.qa_request.get("deferred_contracts") or []),
            })
        ]
        readiness = derive_product_readiness(
            "SUCCEEDED" if task.status == "SUCCESS" else task.status,
            projected_decisions,
        )
        task.product_readiness = readiness["product_readiness"]
        task.user_action_required = readiness["user_action_required"]
        task.open_decision_count = readiness["open_decision_count"]
        task.open_decisions = readiness["open_decisions"]
        from validation_disposition import dispositions_for
        task.validation_dispositions = dispositions_for(task)
        if frozen_execution:
            from control_repository import ControlRepository
            validation_repository = ControlRepository(task.control_repository_path)
            for disposition in task.validation_dispositions:
                try:
                    validation_repository.record_validation(
                        task.execution_id,
                        kind=disposition["kind"],
                        disposition=disposition["disposition"],
                        product_retry=(
                            disposition["disposition"] == "FAIL"
                            and task.retry_domain == "PRODUCT_SEMANTIC_RETRY"
                        ),
                        detail={key: value for key, value in disposition.items()
                                if key not in {"kind", "disposition"}},
                    )
                except Exception as exc:
                    task.report_diagnostics.append({
                        "code": "VALIDATION_DISPOSITION_PERSIST_FAILED",
                        "kind": disposition["kind"],
                        "diagnostic": type(exc).__name__,
                    })
        task.policy_changed_after_materialization = policy_diagnostics["changed"]
        task.execution_summary = build_execution_summary(task)
        if frozen_execution:
            try:
                from troubleshooting_bundle import capture as capture_troubleshooting
                task.troubleshooting_bundle = capture_troubleshooting(task, self.task_root)
            except Exception as exc:
                task.report_diagnostics.append({
                    "code": "TROUBLESHOOTING_BUNDLE_GENERATION_FAILED",
                    "diagnostic": type(exc).__name__,
                })
        if frozen_execution:
            try:
                from developer_change_report import save
                save(task, self.task_root)
            except Exception as exc:
                task.report_diagnostics.append({"code": "DEVELOPER_REPORT_GENERATION_FAILED", "diagnostic": type(exc).__name__})
        try:
            task_path = task.save(self.task_root)
        except Exception as exc:
            raise HarnessServiceError("CONTROL_TASK_STATE_WRITE_FAILED") from exc
        try:
            report_path = self.reporter.save_markdown(
                task, model=request.model, out_dir=self.task_root
            )
        except Exception as exc:
            task.report_diagnostics.append({"code": "CONTROL_REPORT_WRITE_FAILED", "diagnostic": type(exc).__name__})
            report_path = None
        return self._safe_outcome(task, result, task_path, report_path)
