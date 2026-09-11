"""Atomic persistent FIFO queue for ChatGPT-controlled harness jobs."""

from __future__ import annotations

import json
import hashlib
import copy
import os
import re
import secrets
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from job_contract import (
    DROID_GLM_FAIL_CLOSED_MODEL,
    DROID_GLM_FAIL_CLOSED_POLICY,
    JobRequest,
    JobContractError,
    compose_retry_requirement,
    request_fingerprint,
    validate_execution_context,
    validate_idempotency_key,
    validate_supplement,
)
from runtime_safety import scrub_secrets
from control_repository import ControlRepository, RepositoryError
from control_paths import (
    ControlPathError,
    atomic_state_write,
    ensure_safe_state_directory,
    ensure_safe_state_file,
    ensure_safe_state_root,
    validate_no_reparse_tree,
)
from batch_manifest import (
    BatchManifestError,
    build_provenance,
    request_sha256,
    validate_provenance,
)
from execution_lineage import (
    NORMAL,
    TECHNICAL_RECOVERY,
    VERIFICATION_RECOVERY,
    AttemptAccountingError,
    accounting as attempt_accounting,
    consume_reservation,
    lineage_status,
    reserve_execution_attempt,
)
from qa_quarantine import (
    HOLD_BATCH,
    HOLD_DEPENDENCY_CHAIN,
    HOLD_JOB,
    speculation_mode,
    paths_overlap,
)


QUEUE_SCHEMA_VERSION = 1
EXECUTION_MODE_ATTENDED = "ATTENDED"
EXECUTION_MODE_AUTONOMOUS = "AUTONOMOUS"
EXECUTION_MODES = {EXECUTION_MODE_ATTENDED, EXECUTION_MODE_AUTONOMOUS}
AUTONOMOUS_CONFIRMATION = "I_CONFIRM_AUTONOMOUS_COMMIT_AND_CONTINUE"
QA_CORRECTIVE_CONFIRMATION = "I_CONFIRM_QA_CORRECTIVE_SAME_LOGICAL_JOB"
RESET_CONFIRMATION = "I_CONFIRM_RESET_TERMINAL_QUEUE"
EXTERNAL_HANDOFF_CONFIRMATION = "I_CONFIRM_SUPERSEDE_LEGACY_BATCH_FOR_EXTERNAL_HANDOFF"
EXTERNAL_BASELINE_ADOPTION_CONFIRMATION = "I_CONFIRM_ADOPT_USER_ACCEPTED_EXTERNAL_DELTA"
QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
SKELETON_READY = "SKELETON_READY"
AWAITING_ENRICHMENT = "AWAITING_ENRICHMENT"
AWAITING_QA = "AWAITING_QA"
AWAITING_DEPENDENCY_QA = "AWAITING_DEPENDENCY_QA"
FAILED_FINAL = "FAILED_FINAL"
BLOCKED = "BLOCKED"
BLOCKED_BY_DEPENDENCY = "BLOCKED_BY_DEPENDENCY"
INTERRUPTED = "INTERRUPTED"
SKIPPED = "SKIPPED"
CANCELLED = "CANCELLED"

ACTIVE_STATUSES = {QUEUED, RUNNING}
PAUSING_STATUSES = {
    AWAITING_ENRICHMENT,
    AWAITING_QA,
    FAILED_FINAL,
    BLOCKED,
    INTERRUPTED,
}
TERMINAL_STATUSES = {
    SUCCEEDED, SKELETON_READY, FAILED_FINAL, AWAITING_QA, AWAITING_DEPENDENCY_QA, SKIPPED, CANCELLED,
    BLOCKED_BY_DEPENDENCY,
}
SUCCESS_ACK_REQUIRED = "SUCCESS_ACK_REQUIRED"
PREDECESSOR_SUCCESS_GATE_FAILED = "PREDECESSOR_SUCCESS_GATE_FAILED"
BATCH_FINAL_COMMIT = "BATCH_FINAL_COMMIT"
PER_JOB_COMMIT = "PER_JOB"

_JOB_ID_RE = re.compile(r"^JOB-\d{8}-\d{6}-[0-9a-f]{8}$")
_BATCH_ID_RE = re.compile(r"^BATCH-[0-9a-f]{24}$")
_HASHED_IDENTIFIER_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ACTION_MAP_FIELDS = (
    "retry_actions",
    "success_acknowledgements",
    "skip_actions",
    "resume_actions",
    "manual_resolution_actions",
    "commit_actions",
    "profile_revalidation_actions",
    "technical_retry_actions",
    "review_only_actions",
    "qa_resolution_actions",
)

TECHNICAL_AWAITING_QA_FAILURE_CODES = {
    "WORKER_PATCH_CONTEXT_STALE",
    "WORKER_PATCH_CONFLICT",
    "PATCH_EXPECTED_LINES_NOT_FOUND",
    "PATCH_MULTIPLE_OPERATIONS_SAME_TARGET",
    "PATCH_EOL_MISMATCH",
    "PATCH_ENCODING_MISMATCH",
    "CHECKPOINT_CAPTURE_FAILED",
}

MANUAL_RESOLUTION_ACCEPT_PRESERVED = "ACCEPT_PRESERVED"
MANUAL_RESOLUTION_VERIFIED_CLEAN = "VERIFIED_CLEAN"
MANUAL_RESOLUTIONS = {
    MANUAL_RESOLUTION_ACCEPT_PRESERVED,
    MANUAL_RESOLUTION_VERIFIED_CLEAN,
}
MANUAL_RESOLUTION_CONFIRMATIONS = {
    MANUAL_RESOLUTION_ACCEPT_PRESERVED: "I_CONFIRM_ACCEPT_PRESERVED_CHANGES",
    MANUAL_RESOLUTION_VERIFIED_CLEAN: "I_CONFIRM_WORKTREE_VERIFIED_CLEAN",
}
MANUALLY_RESOLVABLE_STATUSES = {AWAITING_QA, BLOCKED, INTERRUPTED}


class QueueError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail
        super().__init__(code + (f": {detail}" if detail else ""))


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def job_lead_time(job: Mapping[str, Any]) -> dict[str, int]:
    """0.99.1 queue-side lead-time breakdown from job history timestamps.

    Categories follow the release timing contract: queue wait, admission,
    human wait and total lead time.  Any category whose anchors are missing
    stays -1; nothing is estimated.
    """
    history = [
        item for item in list(dict(job or {}).get("history") or [])
        if isinstance(item, Mapping) and item.get("at")
    ]

    def first(event: str):
        return next((item for item in history if item.get("event") == event), None)

    def last(event: str):
        found = [item for item in history if item.get("event") == event]
        return found[-1] if found else None

    def seconds(start: Mapping[str, Any], end: Mapping[str, Any]) -> int:
        try:
            delta = datetime.fromisoformat(str(end.get("at"))) - datetime.fromisoformat(
                str(start.get("at"))
            )
            return max(0, int(delta.total_seconds()))
        except (TypeError, ValueError):
            return -1

    result = {
        "queue_wait_seconds": -1,
        "admission_control_seconds": -1,
        "human_wait_seconds": -1,
        "total_lead_time_seconds": -1,
    }
    enqueued = first("ENQUEUED")
    claimed = first("CLAIMED")
    last_claim = last("CLAIMED")
    started = last("TASK_STARTED")
    finished = last("TASK_FINISHED")
    acknowledged = last("SUCCESS_ACKNOWLEDGED")
    if enqueued is not None and claimed is not None:
        result["queue_wait_seconds"] = seconds(enqueued, claimed)
    if last_claim is not None and started is not None:
        result["admission_control_seconds"] = seconds(last_claim, started)
    if (
        finished is not None
        and str(finished.get("status")) == "SUCCEEDED"
        and acknowledged is not None
    ):
        result["human_wait_seconds"] = seconds(finished, acknowledged)
    terminal = acknowledged or finished
    if enqueued is not None and terminal is not None:
        result["total_lead_time_seconds"] = seconds(enqueued, terminal)
    return result


def _identifier_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _safe_json(value: Any) -> Any:
    if isinstance(value, str):
        return scrub_secrets(value)
    if isinstance(value, list):
        return [_safe_json(item) for item in value]
    if isinstance(value, tuple):
        return [_safe_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _safe_json(item) for key, item in value.items()}
    return value


def _atomic_json_write(path: Path, value: Mapping[str, Any], *, root: Path) -> None:
    payload = json.dumps(
        _safe_json(dict(value)), ensure_ascii=False, indent=2
    ).encode("utf-8")
    try:
        atomic_state_write(path, payload, root=root, field="control_state")
    except ControlPathError as exc:
        raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc


class FileMutex:
    """Short-lived cross-process lock with an in-process guard."""

    def __init__(self, path: Path, *, root: Path, timeout: float = 10.0):
        self.path = path
        self.root = root
        self.timeout = timeout
        self._thread_lock = threading.RLock()

    @contextmanager
    def hold(self) -> Iterator[None]:
        with self._thread_lock:
            try:
                ensure_safe_state_directory(
                    self.root, self.path.parent, field="queue_lock", create=True
                )
                ensure_safe_state_file(
                    self.root, self.path, field="queue_lock", allow_missing=True
                )
                handle = open(self.path, "a+b")
                ensure_safe_state_file(self.root, self.path, field="queue_lock")
            except ControlPathError as exc:
                raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc
            if self.path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
            deadline = time.monotonic() + self.timeout
            acquired = False
            try:
                while not acquired:
                    handle.seek(0)
                    try:
                        if os.name == "nt":
                            import msvcrt

                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            import fcntl

                            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        acquired = True
                    except OSError as exc:
                        if time.monotonic() >= deadline:
                            raise QueueError("QUEUE_LOCK_TIMEOUT") from exc
                        time.sleep(0.05)
                yield
            finally:
                if acquired:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()


class ReadOnlyMutex:
    """No-write read guard for an atomic-replace-backed Inspector view.

    Readers may observe two adjacent revisions across queue/job files, but each
    individual JSON file is complete because writers use ``os.replace``.  Opening
    ``store.lock`` would itself create or update local state, which is forbidden on
    the read-only HTTP surface.
    """

    @contextmanager
    def hold(self) -> Iterator[None]:
        yield


class JobStore:
    """One-file-per-job store plus an atomic FIFO manifest."""

    def __init__(self, root: str | Path, *, read_only: bool = False, repository=None):
        self.root = Path(os.path.abspath(str(root)))
        self.read_only = bool(read_only)
        try:
            ensure_safe_state_root(
                self.root, field="control_root", create=not self.read_only
            )
            validate_no_reparse_tree(self.root, "control_root")
        except ControlPathError as exc:
            raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc
        self.jobs_dir = self.root / "jobs"
        try:
            ensure_safe_state_directory(
                self.root,
                self.jobs_dir,
                field="jobs_dir",
                create=not self.read_only,
            )
        except ControlPathError as exc:
            raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc
        self.queue_path = self.root / "queue.json"
        self.repository = repository
        authority_path = self.root / "sqlite-authority.json"
        if self.repository is None and (self.root / "control.sqlite3").exists() and not authority_path.exists():
            raise QueueError("CONTROL_AUTHORITY_INVALID", "SQLite exists without its authority marker")
        if self.repository is None and authority_path.exists():
            try:
                authority = json.loads(authority_path.read_text(encoding="utf-8"))
                from control_repository import SCHEMA_VERSION
                if authority.get("authority") != "SQLITE" or authority.get("schema_version") not in {1, 2, SCHEMA_VERSION}:
                    raise QueueError("CONTROL_AUTHORITY_INVALID")
                self.repository = ControlRepository(self.root / "control.sqlite3", read_only=self.read_only)
            except (OSError, ValueError, RepositoryError) as exc:
                raise QueueError("CONTROL_REPOSITORY_INVALID") from exc
        self._mutex = self.repository or (ReadOnlyMutex() if self.read_only
                                         else FileMutex(self.root / "store.lock", root=self.root))
        if not self.read_only and self.repository is None:
            with self._mutex.hold():
                self._recover_preparing_replacements()
                self._migrate_legacy_identifier_keys()
                self._recover_preparing_batches()
                self._quarantine_invalid_execution_contexts()
                self._validate_orphan_jobs()

    def _assert_writable(self) -> None:
        if self.read_only:
            raise QueueError("CONTROL_STORE_READ_ONLY")

    @staticmethod
    def action_key(request_id: str) -> str:
        validated = validate_idempotency_key(request_id, "request_id")
        return f"sha256:{_identifier_digest(validated)}"

    @staticmethod
    def _lookup_action(
        actions: dict[str, Any], raw_key: str, hashed_key: str
    ) -> tuple[Any, bool]:
        prior = actions.get(hashed_key)
        if prior is not None:
            return prior, False
        if raw_key in actions:
            prior = actions.pop(raw_key)
            actions[hashed_key] = prior
            return prior, True
        return None, False

    @staticmethod
    def _hashed_identifier_map(
        values: Mapping[str, Any], *, field: str
    ) -> tuple[dict[str, Any], bool]:
        migrated: dict[str, Any] = {}
        changed = False
        for raw_key, metadata in values.items():
            key = str(raw_key)
            target = (
                key
                if _HASHED_IDENTIFIER_RE.fullmatch(key)
                else f"sha256:{_identifier_digest(key)}"
            )
            if target != key:
                changed = True
            if target in migrated and migrated[target] != metadata:
                raise QueueError("LEGACY_IDENTIFIER_MIGRATION_CONFLICT", field)
            migrated[target] = metadata
        return migrated, changed

    def _migrate_legacy_identifier_keys(self) -> None:
        """Eagerly remove raw legacy idempotency/action keys without API replay.

        The complete migration is planned before the first write. Atomic per-file
        writes make a process crash restartable; a subsequent constructor simply
        finishes any files that were not yet replaced.
        """
        queue = self._read_queue()
        batches = queue.get("batches", {})
        if not isinstance(batches, dict):
            raise QueueError("QUEUE_STATE_INVALID")
        migrated_batches, queue_changed = self._hashed_identifier_map(
            batches, field="batches"
        )
        queue["batches"] = migrated_batches

        planned_jobs: list[tuple[dict[str, Any], bool]] = []
        for job_id in queue.get("order", []):
            job = self._read_job(job_id)
            job_changed = False
            for field in _ACTION_MAP_FIELDS:
                actions = job.get(field, {})
                if not isinstance(actions, dict):
                    raise QueueError("JOB_STATE_INVALID", job_id)
                migrated, changed = self._hashed_identifier_map(
                    actions, field=field
                )
                job[field] = migrated
                job_changed = job_changed or changed
            planned_jobs.append((job, job_changed))

        for job, changed in planned_jobs:
            if changed:
                self._write_job(job)
        if queue_changed:
            self._write_queue(queue)

    def _finish_prepared_batch(
        self,
        queue: dict[str, Any],
        batch_key: str,
        batch: dict[str, Any],
    ) -> list[dict[str, Any]]:
        prepared = batch.get("prepared_jobs")
        job_ids = batch.get("job_ids")
        if (
            batch.get("state") != "PREPARING"
            or not isinstance(prepared, list)
            or not isinstance(job_ids, list)
            or len(prepared) != len(job_ids)
            or not prepared
        ):
            raise QueueError("PREPARED_BATCH_INVALID")
        if len(set(job_ids)) != len(job_ids):
            raise QueueError("PREPARED_BATCH_INVALID")
        created: list[dict[str, Any]] = []
        for expected, job_id in zip(prepared, job_ids):
            if not isinstance(expected, dict) or expected.get("job_id") != job_id:
                raise QueueError("PREPARED_BATCH_INVALID")
            path = self._job_path(job_id, for_write=True)
            if self._job_exists(job_id):
                actual = self._read_job(job_id)
                comparable_fields = (
                    "schema_version",
                    "job_id",
                    "client_job_id",
                    "batch_id",
                    "sequence",
                    "status",
                    "request",
                    "execution_context",
                    "current_requirement",
                    "max_outer_attempts",
                    "request_sha256",
                )
                if any(actual.get(name) != expected.get(name) for name in comparable_fields):
                    raise QueueError("PREPARED_JOB_CONFLICT", job_id)
            else:
                self._write_job(copy.deepcopy(expected))
                actual = self._read_job(job_id)
            created.append(actual)

        for job_id in job_ids:
            if job_id not in queue["order"]:
                queue["order"].append(job_id)
        batch["state"] = "COMMITTED"
        batch.pop("prepared_jobs", None)
        batch["committed_at"] = _now()
        queue["batches"][batch_key] = batch
        self._write_queue(queue)
        return created

    def _recover_preparing_batches(self) -> None:
        queue = self._read_queue()
        for batch_key, batch in list(queue.get("batches", {}).items()):
            if isinstance(batch, dict) and batch.get("state") == "PREPARING":
                self._finish_prepared_batch(queue, batch_key, batch)

    def _finish_prepared_replacement(
        self, queue: dict[str, Any], action_key: str, action: dict[str, Any]
    ) -> dict[str, Any]:
        prepared = action.get("prepared_job")
        if action.get("state") != "PREPARING" or not isinstance(prepared, dict):
            raise QueueError("PREPARED_REPLACEMENT_INVALID")
        job_id = str(action.get("replacement_job_id", ""))
        parent_id = str(action.get("replaces_job_id", ""))
        if prepared.get("job_id") != job_id:
            raise QueueError("PREPARED_REPLACEMENT_INVALID")
        path = self._job_path(job_id, for_write=True)
        if self._job_exists(job_id):
            actual = self._read_job(job_id)
            if any(actual.get(name) != prepared.get(name) for name in (
                "job_id", "client_job_id", "batch_id", "request", "request_sha256",
                "replaces_job_id", "replacement_kind", "checkpoint_seed",
            )):
                raise QueueError("PREPARED_REPLACEMENT_CONFLICT", job_id)
        else:
            self._write_job(copy.deepcopy(prepared))

        kind = str(action.get("kind", ""))
        parent = self._read_job(parent_id)
        if kind == "QUEUED_CONTRACT_REPLACEMENT":
            already = (
                parent.get("status") == CANCELLED
                and parent.get("superseded_by_job_id") == job_id
            )
            if not already:
                if (
                    parent.get("status") != QUEUED
                    or int(parent.get("revision", -1))
                    != int(action.get("expected_job_revision", -2))
                ):
                    raise QueueError("PREPARED_REPLACEMENT_PARENT_CONFLICT", parent_id)
                parent["status"] = CANCELLED
                parent["cancel_reason"] = "USER_REPLACED_CONTRACT_BEFORE_EXECUTION"
                parent["superseded_by_job_id"] = job_id
                self._event(
                    parent, "SUPERSEDED_BEFORE_EXECUTION",
                    replacement_job_id=job_id,
                    reason="USER_REPLACED_CONTRACT_BEFORE_EXECUTION",
                    request_ref=action.get("request_ref", ""),
                )
                self._write_job(parent)

        anchor = copy.deepcopy(dict(action.get("anchor") or {}))
        if job_id not in queue.get("order", []):
            queue["order"].append(job_id)
        queue.setdefault("replacement_anchors", {})[parent_id] = anchor
        if kind == "FAILED_FINAL_CORRECTIVE":
            queue["corrective_handoff"] = copy.deepcopy(
                dict(action.get("corrective_handoff") or {})
            )
        if kind in {"FAILED_FINAL_CORRECTIVE", "CANDIDATELESS_QA_POLICY_RESOLUTION", "DECISION_FINALIZATION"}:
            self._clear_gate(queue, parent_id)
        action["state"] = "COMMITTED"
        action["completed_at"] = _now()
        action.pop("prepared_job", None)
        queue.setdefault("anchored_replacement_actions", {})[action_key] = action
        if not any(
            item.get("event") == "ANCHORED_REPLACEMENT_CREATED"
            and item.get("replacement_job_id") == job_id
            for item in queue.setdefault("history", [])
            if isinstance(item, dict)
        ):
            queue["history"].append(copy.deepcopy(dict(action.get("history_event") or {})))
        action.pop("history_event", None)
        self._write_queue(queue)
        return self._read_job(job_id)

    def _recover_preparing_replacements(self) -> None:
        queue = self._read_queue()
        actions = queue.get("anchored_replacement_actions", {})
        if not isinstance(actions, dict):
            raise QueueError("QUEUE_STATE_INVALID")
        for action_key, action in list(actions.items()):
            if isinstance(action, dict) and action.get("state") == "PREPARING":
                self._finish_prepared_replacement(queue, str(action_key), action)

    def _validate_orphan_jobs(self) -> None:
        queue = self._read_queue()
        referenced = set(str(job_id) for job_id in queue.get("order", []))
        try:
            entries = tuple(self.jobs_dir.iterdir())
        except OSError as exc:
            raise QueueError("CONTROL_STATE_PATH_UNSAFE", "jobs_dir") from exc
        for path in entries:
            if not path.is_file() or not path.name.endswith(".json"):
                continue
            job_id = path.name[:-5]
            if not _JOB_ID_RE.fullmatch(job_id) or job_id not in referenced:
                raise QueueError("CONTROL_STATE_ORPHAN_JOB")

    def _quarantine_invalid_execution_contexts(self) -> None:
        """Cancel only legacy invalid-context Jobs proven never to have run.

        Started Jobs are not safe to classify automatically because their worktree
        ownership cannot be derived from a malformed legacy control record.
        """
        queue = self._read_queue()
        queue_changed = False
        for job_id in queue.get("order", []):
            job = self._read_job(job_id)
            try:
                validate_execution_context(job.get("execution_context"))
                continue
            except JobContractError:
                never_started = (
                    not job.get("task_ids")
                    and not job.get("current_claim_task_started")
                    and int(job.get("outer_attempt", 0)) == 0
                )
                if not never_started:
                    raise QueueError(
                        "CONTROL_STATE_EXECUTION_CONTEXT_INVALID", job_id
                    )
            if job.get("status") != CANCELLED:
                job["status"] = CANCELLED
                job["owner_id"] = ""
                job["last_result"] = {
                    "status": CANCELLED,
                    "failure_code": "LEGACY_EXECUTION_CONTEXT_QUARANTINED",
                    "worktree_disposition": "NO_DELTA",
                }
                self._event(job, "LEGACY_EXECUTION_CONTEXT_QUARANTINED")
                self._write_job(job)
            if queue.get("running_job_id") == job_id:
                queue["running_job_id"] = ""
                queue_changed = True
            if queue.get("blocked_by_job_id") == job_id:
                self._clear_gate(queue, job_id)
                queue_changed = True
        if queue_changed:
            self._write_queue(queue)

    @staticmethod
    def _event(job: dict[str, Any], event: str, **detail: Any) -> None:
        history = job.setdefault("history", [])
        history.append({"at": _now(), "event": event, **_safe_json(detail)})

    @staticmethod
    def _terminalize_current_attempt(
        job: dict[str, Any], result: Mapping[str, Any]
    ) -> bool:
        """Close the currently claimed Task attempt exactly once.

        A controller failure can bypass ``record_result`` after a Task lineage was
        committed.  Keeping an open attempt in that case makes later reconciliation
        look like an active Task forever.  The current claim marker identifies the
        only attempt this helper may close; historical attempts are never guessed.
        """
        if not job.get("current_claim_task_started"):
            return False
        task_id = str(job.get("current_claim_task_id", ""))
        attempts = job.get("attempts")
        if not task_id or not isinstance(attempts, list):
            return False
        for attempt in reversed(attempts):
            if not isinstance(attempt, dict) or str(attempt.get("task_id", "")) != task_id:
                continue
            if attempt.get("finished_at"):
                return False
            attempt["finished_at"] = _now()
            attempt["result"] = _safe_json(dict(result))
            return True
        return False

    @staticmethod
    def _empty_queue() -> dict[str, Any]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "order": [],
            "running_job_id": "",
            "operator_paused": False,
            "operator_pause_reason": "",
            "paused": False,
            "pause_reason": "",
            "gate_reason": "",
            "blocked_by_job_id": "",
            "batches": {},
            "execution_mode": EXECUTION_MODE_ATTENDED,
            "mode_actions": {},
            "reset_actions": {},
            "external_handoff_actions": {},
            "external_baseline_adoption_actions": {},
            "anchored_replacement_actions": {},
            "replacement_anchors": {},
            "corrective_handoff": {},
            "qa_corrective_successors": {},
            "qa_corrective_actions": {},
            "history": [],
            "active_from_index": 0,
            "generation": 1,
            "resets": [],
            "commit_policy": PER_JOB_COMMIT,
            "cumulative_worktree": False,
            "auto_continue_without_commit": False,
            "batch_owned_delta_supported": False,
            "pre_job_snapshot_supported": False,
            "scoped_rollback_supported": False,
            "final_commit_required": False,
            "active_baseline_id": "",
            "task_owned_baseline_files": [],
            "external_frozen_baseline_files": [],
            "batch_delta_files": [],
            "unexpected_runtime_dirty_files": [],
            "job_validation_success_count": 0,
            "job_delta_preserved_count": 0,
            "job_commit_deferred_count": 0,
            "batch_final_staging_eligible": False,
            "batch_final_commit_eligible": False,
            "baseline_advance_eligible": False,
            "revision": 0,
            "updated_at": _now(),
        }

    def _read_queue(self) -> dict[str, Any]:
        try:
            ensure_safe_state_file(
                self.root,
                self.queue_path,
                field="queue_state",
                allow_missing=True,
            )
        except ControlPathError as exc:
            raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc
        if self.repository is None and not self.queue_path.exists():
            return self._empty_queue()
        try:
            data = self.repository.read_queue() if self.repository is not None else json.loads(self.queue_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise QueueError("QUEUE_STATE_INVALID") from exc
        if data.get("schema_version") != QUEUE_SCHEMA_VERSION:
            raise QueueError("QUEUE_SCHEMA_UNSUPPORTED")
        required = ("order", "running_job_id", "paused", "batches", "revision")
        if any(key not in data for key in required):
            raise QueueError("QUEUE_STATE_INVALID")
        if not isinstance(data["order"], list) or not isinstance(data["batches"], dict):
            raise QueueError("QUEUE_STATE_INVALID")
        # Schema v1 predates the split between an operator pause and a state gate.
        # Defaults keep a newly created or already-normalized queue compatible while
        # ensuring that retry/ack actions cannot silently clear an operator pause.
        if "operator_paused" not in data:
            legacy_operator_pause = bool(
                data.get("paused") and not data.get("blocked_by_job_id")
            )
            data["operator_paused"] = legacy_operator_pause
            data["operator_pause_reason"] = (
                data.get("pause_reason", "") if legacy_operator_pause else ""
            )
        else:
            data.setdefault("operator_pause_reason", "")
        if "gate_reason" not in data:
            data["gate_reason"] = (
                data.get("pause_reason", "") if data.get("blocked_by_job_id") else ""
            )
        if data.get("execution_mode") not in EXECUTION_MODES:
            data["execution_mode"] = EXECUTION_MODE_ATTENDED
        data.setdefault("mode_actions", {})
        data.setdefault("reset_actions", {})
        data.setdefault("external_handoff_actions", {})
        data.setdefault("external_baseline_adoption_actions", {})
        data.setdefault("anchored_replacement_actions", {})
        data.setdefault("replacement_anchors", {})
        data.setdefault("corrective_handoff", {})
        data.setdefault("qa_corrective_successors", {})
        data.setdefault("qa_corrective_actions", {})
        data.setdefault("history", [])
        data.setdefault("active_from_index", 0)
        data.setdefault("generation", 1)
        data.setdefault("resets", [])
        data.setdefault("commit_policy", PER_JOB_COMMIT)
        data.setdefault("cumulative_worktree", False)
        data.setdefault("auto_continue_without_commit", False)
        data.setdefault("batch_owned_delta_supported", False)
        data.setdefault("pre_job_snapshot_supported", False)
        data.setdefault("scoped_rollback_supported", False)
        data.setdefault("final_commit_required", False)
        data.setdefault("active_baseline_id", "")
        data.setdefault("task_owned_baseline_files", [])
        data.setdefault("external_frozen_baseline_files", [])
        data.setdefault("batch_delta_files", [])
        data.setdefault("unexpected_runtime_dirty_files", [])
        data.setdefault("job_validation_success_count", 0)
        data.setdefault("job_delta_preserved_count", 0)
        data.setdefault("job_commit_deferred_count", 0)
        data.setdefault("batch_final_staging_eligible", False)
        data.setdefault("batch_final_commit_eligible", False)
        data.setdefault("baseline_advance_eligible", False)
        active_from = data.get("active_from_index")
        if (
            isinstance(active_from, bool)
            or not isinstance(active_from, int)
            or not 0 <= active_from <= len(data["order"])
        ):
            raise QueueError("QUEUE_STATE_INVALID")
        self._refresh_pause(data)
        return data

    @staticmethod
    def _active_job_ids(queue: Mapping[str, Any]) -> list[str]:
        order = list(queue.get("order") or [])
        start = int(queue.get("active_from_index", 0))
        active = [str(job_id) for job_id in order[start:]]
        anchors = dict(queue.get("replacement_anchors") or {})
        replacements = {
            str(record.get("replacement_job_id", ""))
            for record in anchors.values()
            if isinstance(record, Mapping) and record.get("replacement_job_id")
        }
        effective: list[str] = []
        for job_id in active:
            if job_id in replacements:
                continue
            current = job_id
            visited: set[str] = set()
            while current in active and current not in visited:
                effective.append(current)
                visited.add(current)
                record = anchors.get(current)
                if not isinstance(record, Mapping):
                    break
                replacement_id = str(record.get("replacement_job_id", ""))
                if not replacement_id or replacement_id not in active:
                    break
                current = replacement_id
        # Fail closed for malformed legacy metadata: never silently drop an active Job.
        effective.extend(job_id for job_id in active if job_id not in effective)
        return effective

    @staticmethod
    def _is_replacement_anchor_parent(
        queue: Mapping[str, Any], job_id: str
    ) -> bool:
        """Return true only for a durably anchored historical parent.

        Replaced Jobs stay in the active order for immutable history, but their
        terminal state must not be rebuilt as the live scheduling gate.  Fail
        closed when the additive anchor metadata is incomplete or malformed.
        """
        anchors = queue.get("replacement_anchors")
        if not isinstance(anchors, Mapping):
            return False
        record = anchors.get(job_id)
        if not isinstance(record, Mapping):
            return False
        replacement_id = str(record.get("replacement_job_id", ""))
        kind = str(record.get("kind", ""))
        order = {str(item) for item in list(queue.get("order") or [])}
        return (
            bool(replacement_id)
            and replacement_id != job_id
            and job_id in order
            and replacement_id in order
            and kind in {"FAILED_FINAL_CORRECTIVE", "QUEUED_CONTRACT_REPLACEMENT", "CANDIDATELESS_QA_POLICY_RESOLUTION"}
        )

    def _refresh_batch_lifecycle(self, queue: dict[str, Any], batch_id: str) -> None:
        jobs: list[dict[str, Any]] = []
        for job_id in self._active_job_ids(queue):
            job = self._read_job(job_id)
            if job.get("batch_id") == batch_id:
                jobs.append(job)
        successes = [job for job in jobs if job.get("status") == SUCCEEDED]
        queue["job_validation_success_count"] = len(successes)
        queue["job_delta_preserved_count"] = sum(
            1
            for job in successes
            if str((job.get("last_result") or {}).get("job_delta_preservation_status", ""))
            in {"PRESERVED", "NO_DELTA"}
        )
        queue["job_commit_deferred_count"] = sum(
            1
            for job in successes
            if str((job.get("last_result") or {}).get("job_commit_status", ""))
            == "DEFERRED_TO_BATCH_FINAL"
        )
        queue["batch_final_staging_eligible"] = bool(jobs) and len(successes) == len(jobs)
        # These two transitions require explicit final staging/commit evidence;
        # successful Jobs alone must never grant them.
        queue["batch_final_commit_eligible"] = False
        queue["baseline_advance_eligible"] = False

    @staticmethod
    def _is_strict_batch_result(result: Mapping[str, Any]) -> bool:
        test_status = str(result.get("test_status", ""))
        return (
            result.get("success") is True
            and result.get("status") == "SUCCESS"
            and result.get("stage") == "DONE"
            and result.get("verification_status") == "VERIFIED"
            and result.get("build_status") == "PASS"
            and result.get("review_status") == "REVIEW_PASS"
            and test_status in {"", "PASS", "NOT_REQUIRED"}
            and not str(result.get("failure_code", ""))
            and not list(result.get("secondary_failures") or [])
            and not bool(result.get("user_qa_required", False))
        )

    @staticmethod
    def _is_skeleton_ready(job):
        from skeleton_policy import valid_result
        return job.get('status') == SKELETON_READY and valid_result(job.get('last_result') or {})

    @staticmethod
    def _is_verified_batch_success(job: Mapping[str, Any]) -> bool:
        return (
            job.get("status") == SUCCEEDED
            and JobStore._is_strict_batch_result(
                dict(job.get("last_result") or {})
            )
        )

    @staticmethod
    def _should_auto_ack_success(
        queue: Mapping[str, Any], job: Mapping[str, Any]
    ) -> bool:
        """Keep execution release independent from cumulative commit timing."""
        return (
            queue.get("execution_mode") == EXECUTION_MODE_AUTONOMOUS
            and queue.get("commit_policy") == BATCH_FINAL_COMMIT
            and JobStore._is_verified_batch_success(job)
        )

    def _current_queue_batch_id(self, queue: Mapping[str, Any]) -> str:
        candidate_ids = [
            str(queue.get("running_job_id", "")),
            str(queue.get("blocked_by_job_id", "")),
        ]
        for job_id in candidate_ids:
            if not job_id:
                continue
            job = self._read_job(job_id)
            return str(job.get("batch_id", ""))
        for job_id in self._active_job_ids(queue):
            job = self._read_job(job_id)
            if job.get("status") == QUEUED:
                return str(job.get("batch_id", ""))
        return ""

    def _same_batch_predecessor(
        self, queue: Mapping[str, Any], candidate: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        batch_id = str(candidate.get("batch_id", ""))
        sequence = int(candidate.get("sequence", 0))
        if not batch_id or sequence <= 1:
            return None
        predecessor: dict[str, Any] | None = None
        for job_id in self._active_job_ids(queue):
            if self._is_replacement_anchor_parent(queue, job_id):
                continue
            job = self._read_job(job_id)
            if str(job.get("batch_id", "")) != batch_id:
                continue
            job_sequence = int(job.get("sequence", 0))
            if job_sequence >= sequence:
                continue
            if predecessor is None or job_sequence > int(
                predecessor.get("sequence", 0)
            ):
                predecessor = job
        return predecessor

    def _failed_predecessor_gate(
        self, queue: Mapping[str, Any], candidate: Mapping[str, Any]
    ) -> tuple[str, str] | None:
        if queue.get("commit_policy") != BATCH_FINAL_COMMIT:
            return None
        predecessor = self._same_batch_predecessor(queue, candidate)
        if predecessor is None or self._is_verified_batch_success(predecessor):
            return None
        return (
            str(predecessor.get("job_id", "")),
            PREDECESSOR_SUCCESS_GATE_FAILED,
        )

    @staticmethod
    def _qa_non_global(job: Mapping[str, Any]) -> bool:
        return (
            job.get("dependency_semantics") == "EXPLICIT"
            and str(job.get("status", "")) in {AWAITING_QA, AWAITING_DEPENDENCY_QA}
            and str(job.get("hold_scope", "")) in {HOLD_JOB, HOLD_DEPENDENCY_CHAIN}
            and bool(job.get("candidate_id"))
        )

    @staticmethod
    def _qa_corrective_scope_evidence(
        qa_job: Mapping[str, Any], corrective: Mapping[str, Any]
    ) -> bool:
        """Machine evidence that a corrective reworks the quarantined QA scope.

        A shared name alone is never identity evidence. Unknown (empty) scope
        on either side fails open here is unacceptable, so it fails closed.
        """
        corrective_request = dict(corrective.get("request") or {})
        corrective_scope = list(
            corrective_request.get("target_resources")
            or corrective_request.get("target_modules")
            or []
        )
        qa_scope = list(
            dict(qa_job.get("last_result") or {}).get("changed_files")
            or dict(qa_job.get("request") or {}).get("target_resources")
            or dict(qa_job.get("request") or {}).get("target_modules")
            or []
        )
        if not corrective_scope or not qa_scope:
            return False
        return paths_overlap(corrective_scope, qa_scope)

    def _active_qa_corrective_successor_id(
        self, queue: Mapping[str, Any], qa_job_id: str
    ) -> str:
        """Return the successor owning this QA's active path, else nothing.

        The link stays valid only while the successor is still QUEUED or
        RUNNING; once terminal the quarantine hold returns fail-closed for
        every overlapping Job until a fresh corrective is declared.
        """
        record = dict(
            dict(queue.get("qa_corrective_successors") or {}).get(str(qa_job_id)) or {}
        )
        successor_id = str(record.get("corrective_job_id", ""))
        if not successor_id or successor_id == str(qa_job_id):
            return ""
        successor = self._read_job(successor_id)
        if successor.get('status') == FAILED_FINAL and self.repository is not None:
            replacement_id = str(((queue.get('replacement_anchors') or {}).get(successor_id) or {}).get('replacement_job_id',''))
            if (replacement_id and successor.get('corrects_job_id') == str(qa_job_id)
                    and self.repository.technical_candidate_input_allows(successor_id,replacement_id)):
                return replacement_id
        if successor.get("status") not in {QUEUED, RUNNING}:
            return ""
        if str(successor.get("corrects_job_id", "")) != str(qa_job_id):
            return ""
        return successor_id

    def _completed_qa_corrective_successor_id(self, queue, qa_job_id):
        """Completed takeover is terminal VERIFIED evidence, never QA acceptance."""
        if self.repository is None:
            return ""
        link = (queue.get('qa_corrective_successors') or {}).get(qa_job_id) or {}
        parent_id = link.get('corrective_job_id')
        if not parent_id:
            return ""
        try:
            source, parent = self._read_job(qa_job_id), self._read_job(parent_id)
            target_id = ((queue.get('replacement_anchors') or {}).get(parent_id) or {}).get('replacement_job_id')
            if (parent.get('status') != FAILED_FINAL or not target_id
                    or parent.get('corrects_job_id') != qa_job_id
                    or not source.get('candidate_ownership_handoff')
                    or parent.get('candidate_ownership_received') != source['candidate_ownership_handoff']):
                return ""
            target = self._read_job(target_id)
            if target.get('status') != SUCCEEDED or target_id not in self._active_job_ids(queue):
                return ""
            with self.repository.hold():
                rows = self.repository.preserved_candidate_records(parent_id)
                proof = self.repository._verified_candidate_history(rows, target_job_id=target_id)
            return target_id if rows and set(proof) == {r['path'] for r in rows} and all(proof.values()) else ""
        except (RepositoryError, OSError, ValueError):
            return ""

    def _has_active_corrective_takeover(
        self, queue, source, *, workspace=None, integrity_snapshot_provider=None
    ) -> bool:
        """Read-only proof; historical QA is never changed into acceptance."""
        if (self.repository is None or workspace is None
                or integrity_snapshot_provider is None
                or source.get("status") != AWAITING_QA):
            return False
        source_id = source["job_id"]
        if self._completed_qa_corrective_successor_id(queue, source_id):
            integrity = integrity_snapshot_provider(queue)
            return bool(integrity.get('baseline_declaration_integrity') is True
                and integrity.get('external_frozen_integrity') is True
                and integrity.get('unexpected_runtime_dirty_files') == [])
        link = (queue.get("qa_corrective_successors") or {}).get(source_id) or {}
        target_id = self._active_qa_corrective_successor_id(queue, source_id)
        if (not target_id
                or target_id not in self._active_job_ids(queue)
                or not source.get("candidate_ownership_handoff")):
            return False
        try:
            target = self._read_job(target_id)
            parent_id = str(target.get('replaces_job_id',''))
            if parent_id and self.repository.technical_candidate_input_allows(parent_id,target_id):
                parent = self._read_job(parent_id)
                ownership = self.repository.candidate_ownership(workspace)
                integrity = integrity_snapshot_provider(queue)
                fresh = dict(target.get('fresh_input') or {})
                path = Path(str(fresh.get('manifest_path',''))).resolve()
                if (not path.is_relative_to((self.root/'cumulative'/'job-snapshots').resolve())
                        or not path.is_file()
                        or hashlib.sha256(path.read_bytes()).hexdigest() != fresh.get('manifest_sha256')):
                    return False
                snapshot = json.loads(path.read_text(encoding='utf-8'))
                if (snapshot.get('kind') != 'PRE_JOB_SNAPSHOT'
                        or snapshot.get('job_id') != parent_id
                        or snapshot.get('snapshot_id') != fresh.get('snapshot_id')
                        or snapshot.get('baseline_id') != queue.get('active_baseline_id')
                        or fresh.get('generation') != queue.get('generation')
                        or fresh.get('parent_revision') != parent.get('revision')):
                    return False
                for entry in snapshot.get('files') or []:
                    if entry.get('state') == 'PRESENT':
                        blob = (path.parent/str(entry.get('snapshot',''))).resolve()
                        if (not blob.is_relative_to(path.parent) or not blob.is_file()
                                or hashlib.sha256(blob.read_bytes()).hexdigest() != entry.get('sha256')):
                            return False
                event = target.get('technical_candidate_input') or {}
                hashes = event.get('candidate_hashes') or {}
                snapshot_hashes = {row['path']:row.get('sha256') for row in snapshot.get('files') or []}
                scope = set(target.get('request',{}).get('target_resources') or [])
                return bool(link.get('state') in {'ACTIVE','FAILED'}
                    and link.get('corrective_job_id') == parent_id
                    and parent.get('corrects_job_id') == source_id
                    and parent.get('candidate_ownership_received') == source.get('candidate_ownership_handoff')
                    and fresh.get('parent_job_id') == parent_id
                    and fresh.get('kind') == 'VERIFIED_NO_TASK_DELTA_FRESH_INPUT'
                    and hashes and set(hashes).issubset(scope)
                    and all(snapshot_hashes.get(p) == value for p,value in hashes.items())
                    and not any(row['job_id'] != parent_id and paths_overlap([row['path']],list(scope))
                                for row in ownership['files'])
                    and ownership['valid']
                    and parent.get('active_baseline_id') == queue.get('active_baseline_id')
                    and integrity.get('active_baseline_id') == queue.get('active_baseline_id')
                    and integrity.get('baseline_declaration_integrity') is True
                    and integrity.get('external_frozen_integrity') is True
                    and integrity.get('unexpected_runtime_dirty_files') == [])
            if link.get('state') != 'ACTIVE' or not self.repository.candidate_handoff_allows(source_id, target_id):
                return False
            proof = self.repository.preview_candidate_ownership_handoff(
                source_id, target_id, workspace=workspace,
                integrity_snapshot_provider=integrity_snapshot_provider,
                execution_safe=lambda: not queue.get("running_job_id"),
            )
            return proof["eligible"] is True and proof["replayed"] is True
        except (RepositoryError, OSError, ValueError):
            return False

    def _finish_qa_corrective_successor(
        self,
        queue: dict[str, Any],
        action_key: str,
        action: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Complete the durable handoff after the intent record survived."""
        qa_job_id = str(action.get("qa_job_id", ""))
        corrective_job_id = str(action.get("corrective_job_id", ""))
        now = _now()
        corrective = self._read_job(corrective_job_id)
        if str(corrective.get("corrects_job_id", "")) != qa_job_id:
            corrective["corrects_job_id"] = qa_job_id
            self._event(
                corrective,
                "QA_CORRECTIVE_SUCCESSOR_LINKED",
                qa_job_id=qa_job_id,
                reason=str(action.get("reason", "")),
                actor=str(action.get("actor", "")),
                request_ref=str(action.get("request_ref", "")),
            )
            self._write_job(corrective)
        successors = dict(queue.get("qa_corrective_successors") or {})
        successors[qa_job_id] = {
            "corrective_job_id": corrective_job_id,
            "state": "ACTIVE",
            "reason": str(action.get("reason", "")),
            "actor": str(action.get("actor", "")),
            "request_ref": str(action.get("request_ref", "")),
            "linked_at": now,
            "result_recorded_at": "",
        }
        queue["qa_corrective_successors"] = successors
        if (
            queue.get("blocked_by_job_id") == qa_job_id
            and queue.get("gate_reason") in {AWAITING_QA, AWAITING_DEPENDENCY_QA}
        ):
            self._clear_gate(queue, qa_job_id)
        queue.setdefault("qa_corrective_actions", {})[action_key] = {
            **dict(action),
            "state": "COMMITTED",
            "committed_at": now,
        }
        self._write_queue(queue)
        return self._read_job(corrective_job_id)

    def record_qa_corrective_successor(
        self,
        qa_job_id: str,
        corrective_job_id: str,
        *,
        expected_queue_revision: int,
        expected_job_revision: int,
        request_id: str,
        reason: str,
        confirmation: str,
        actor: str = "harness-supervisor",
    ) -> tuple[dict[str, Any], bool]:
        """Hand one job-scoped historical QA hold's active path to its corrective.

        The historical AWAITING_QA Job remains immutable evidence: its status,
        Review, Verification, failure_code, and history are never rewritten.
        Only the pristine QUEUED successor receives the durable lineage link,
        and only when the same logical scope is proven from the quarantined
        delta. Sequence or write overlap alone is not dependency and never
        creates a handoff.
        """
        self._assert_writable()
        if isinstance(expected_queue_revision, bool) or not isinstance(expected_queue_revision, int):
            raise QueueError("EXPECTED_QUEUE_REVISION_INVALID")
        if isinstance(expected_job_revision, bool) or not isinstance(expected_job_revision, int):
            raise QueueError("EXPECTED_REVISION_INVALID")
        if confirmation != QA_CORRECTIVE_CONFIRMATION:
            raise QueueError("QA_CORRECTIVE_CONFIRMATION_REQUIRED")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = self.action_key(action_id)
        action_ref = _identifier_digest(action_id)[:16]
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("QA_CORRECTIVE_REASON_REQUIRED")
        fingerprint_payload = {
            "qa_job_id": qa_job_id,
            "corrective_job_id": corrective_job_id,
            "expected_queue_revision": expected_queue_revision,
            "expected_job_revision": expected_job_revision,
            "reason": safe_reason,
            "actor": str(actor),
        }
        fingerprint = _canonical_sha256(fingerprint_payload)

        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("qa_corrective_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if prior.get("fingerprint") != fingerprint:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if prior.get("state") == "PREPARING":
                    recovered = self._finish_qa_corrective_successor(
                        queue, action_key, prior
                    )
                    return recovered, True
                if migrated:
                    self._write_queue(queue)
                return self._read_job(str(prior["corrective_job_id"])), True

            # All CAS and lineage checks precede the first write: stale or
            # unproven requests have mutation=0.
            if int(queue.get("revision", -1)) != expected_queue_revision:
                raise QueueError("QUEUE_REVISION_CONFLICT")
            if corrective_job_id == qa_job_id:
                raise QueueError("QA_CORRECTIVE_SELF_REFERENCE", qa_job_id)
            qa_job = self._read_job(qa_job_id)
            preserved = self.repository.preserved_candidate_records(qa_job_id) if self.repository is not None else []
            preserved_source = (
                qa_job.get('status') == AWAITING_QA and bool(preserved)
                and all(row['classification'] == 'PRE_EXISTING_UNVERIFIED_CANDIDATE' for row in preserved)
            )
            if not self._qa_non_global(qa_job) and not preserved_source:
                raise QueueError("QA_NOT_JOB_SCOPED", qa_job_id)
            corrective = self._read_job(corrective_job_id)
            if int(corrective.get("revision", -1)) != expected_job_revision:
                raise QueueError("JOB_REVISION_CONFLICT", corrective_job_id)
            if corrective_job_id not in self._active_job_ids(queue):
                raise QueueError("JOB_NOT_ACTIVE", corrective_job_id)
            if corrective.get("status") != QUEUED:
                raise QueueError("QA_CORRECTIVE_NOT_QUEUED", corrective_job_id)
            if (
                int(corrective.get("outer_attempt", 0)) != 0
                or list(corrective.get("attempts") or [])
                or list(corrective.get("task_ids") or [])
                or corrective.get("candidate_id")
                or corrective.get("owner_id")
                or dict(corrective.get("last_result") or {})
            ):
                raise QueueError("QA_CORRECTIVE_NOT_PRISTINE", corrective_job_id)
            existing_link = str(corrective.get("corrects_job_id", ""))
            if existing_link and existing_link != qa_job_id:
                raise QueueError("QA_CORRECTIVE_ALREADY_LINKED", corrective_job_id)
            for other_qa_id, record in dict(
                queue.get("qa_corrective_successors") or {}
            ).items():
                if (
                    isinstance(record, Mapping)
                    and str(record.get("corrective_job_id", "")) == corrective_job_id
                    and str(other_qa_id) != str(qa_job_id)
                ):
                    raise QueueError("QA_CORRECTIVE_ALREADY_LINKED", corrective_job_id)
            explicit_link = existing_link == qa_job_id or str(
                corrective.get("replaces_job_id", "")
            ) == qa_job_id
            if not explicit_link and not self._qa_corrective_scope_evidence(
                qa_job, corrective
            ):
                raise QueueError(
                    "QA_CORRECTIVE_LINEAGE_EVIDENCE_MISSING", corrective_job_id
                )

            now = _now()
            action = {
                "fingerprint": fingerprint,
                "qa_job_id": qa_job_id,
                "corrective_job_id": corrective_job_id,
                "state": "PREPARING",
                "expected_job_revision": expected_job_revision,
                "reason": safe_reason,
                "actor": str(actor),
                "request_ref": action_ref,
                "created_at": now,
            }
            actions[action_key] = dict(action)
            # Durable intent precedes every Job mutation. Replay can complete
            # this exact handoff after a process interruption.
            self._write_queue(queue)
            created = self._finish_qa_corrective_successor(queue, action_key, action)
            return created, False

    def _explicit_dependency_decision(
        self, candidate: Mapping[str, Any]
    ) -> dict[str, Any]:
        if candidate.get("dependency_semantics") != "EXPLICIT":
            return {"eligible": True, "speculative_candidate_ids": []}
        speculative: list[str] = []
        waiting: list[str] = []
        terminal: list[str] = []
        for dependency_id in list(candidate.get("depends_on") or []):
            dependency = self._read_job(str(dependency_id))
            from skeleton_policy import CONTRACT as skeleton_contract
            if (skeleton_contract in str(candidate.get('current_requirement', ''))
                    and self._is_skeleton_ready(dependency)):
                continue
            if self._is_verified_batch_success(dependency):
                continue
            if dependency.get("status") in {AWAITING_QA, AWAITING_DEPENDENCY_QA}:
                hold = str(dependency.get("hold_scope", ""))
                if hold == HOLD_BATCH:
                    if self.repository is not None:
                        from sleep_disposition import disposition
                        if disposition(dependency) != "GLOBAL_STOP":
                            waiting.append(str(dependency_id))
                            continue
                    return {
                        "eligible": False,
                        "global_gate": str(dependency_id),
                        "reason": AWAITING_QA,
                    }
                mode = speculation_mode({
                    "qa_type": dependency.get("qa_type", ""),
                    "machine_verified": dependency.get("machine_verified", False),
                })
                candidate_id = str(dependency.get("candidate_id", ""))
                if mode == "SOURCE_MUTATION_ALLOWED" and candidate_id:
                    speculative.append(candidate_id)
                else:
                    waiting.append(str(dependency_id))
                continue
            if dependency.get("status") in {
                FAILED_FINAL, BLOCKED, BLOCKED_BY_DEPENDENCY, INTERRUPTED,
                SKIPPED, CANCELLED,
            }:
                terminal.append(str(dependency_id))
            else:
                waiting.append(str(dependency_id))
        return {
            "eligible": not waiting and not terminal,
            "speculative_candidate_ids": speculative,
            "waiting_for": waiting,
            "terminal_blockers": terminal,
        }

    def _resolved_candidateless_scope(self, queue, source, target, *, workspace=None,
                                      integrity_snapshot_provider=None):
        """Read-only, same-successor proof. Absence of a candidate alone is insufficient."""
        if self.repository is None or workspace is None or integrity_snapshot_provider is None:
            return False
        from qa_policy_resolution import POLICY, resolution_contract
        source_id, target_id = source['job_id'], target['job_id']
        anchor = (queue.get('replacement_anchors') or {}).get(source_id) or {}
        resolution = target.get('qa_policy_resolution') or {}
        fresh = target.get('fresh_input') or {}
        try:
            expected = str(source.get('current_requirement', '')) + '\n\n' + resolution_contract(resolution.get('policy'))
        except ValueError:
            return False
        if (source.get('status') != AWAITING_QA or source.get('candidate_id')
                or source.get('candidate_manifest') or source.get('task_owned_delta_files')
                or source.get('last_result', {}).get('changed_files')
                or source.get('last_result', {}).get('failure_code') != 'CANDIDATE_DELTA_EMPTY'
                or self.repository.preserved_candidate_records(source_id)
                or anchor.get('kind') != 'CANDIDATELESS_QA_POLICY_RESOLUTION'
                or anchor.get('replacement_job_id') != target_id
                or target.get('replaces_job_id') != source_id
                or target.get('status') not in {QUEUED, RUNNING}
                or resolution.get('source_job_id') != source_id
                or resolution.get('source_revision') != source.get('revision')
                or resolution.get('source_task_id') != source.get('current_claim_task_id')
                or resolution.get('logical_job_id') != (source.get('logical_job_id') or source.get('client_job_id') or source_id)
                or resolution.get('logical_job_id') != (target.get('logical_job_id') or target.get('client_job_id') or target_id)
                or resolution.get('contract_revision') != target.get('contract_revision')
                or resolution.get('contract_sha256') != _canonical_sha256(expected)
                or target.get('current_requirement') != expected
                or fresh.get('kind') != 'CANDIDATELESS_QA_POLICY_FRESH_INPUT'
                or fresh.get('parent_job_id') != source_id
                or fresh.get('parent_revision') != source.get('revision')
                or fresh.get('generation') != queue.get('generation')):
            return False
        execution = self.repository.active_execution(target_id)
        if (not execution or execution.get('current_requirement') != expected
                or execution.get('logical_job_id') != resolution['logical_job_id']
                or execution.get('contract_revision') != resolution['contract_revision']
                or execution.get('request') != target.get('request')):
            return False
        baseline_id = queue.get('active_baseline_id')
        baseline = self.root/'cumulative'/'batch-baselines'/str(baseline_id)/'manifest.json'
        if (not baseline_id or source.get('active_baseline_id') != baseline_id
                or fresh.get('active_baseline_id') != baseline_id or not baseline.is_file()
                or execution.get('baseline_hash') != hashlib.sha256(baseline.read_bytes()).hexdigest()):
            return False
        integrity = integrity_snapshot_provider(queue)
        return bool(integrity.get('active_baseline_id') == baseline_id
            and integrity.get('baseline_declaration_integrity') is True
            and integrity.get('external_frozen_integrity') is True
            and integrity.get('unexpected_runtime_dirty_files') == []
            and self.repository.candidate_ownership(workspace)['valid'])

    def _resolved_skeleton_scope(self, queue, source, consumer, *, workspace=None,
                                 integrity_snapshot_provider=None):
        """Prove each immutable policy replacement link, never waive foreign QA."""
        from qa_policy_resolution import resolution_contract, SKELETON_POLICY
        from skeleton_policy import CONTRACT as skeleton_contract
        if (self.repository is None or workspace is None or integrity_snapshot_provider is None
                or skeleton_contract not in str(consumer.get('current_requirement', ''))):
            return False
        current, seen = source, set()
        baseline_id = queue.get('active_baseline_id')
        baseline = self.root/'cumulative'/'batch-baselines'/str(baseline_id)/'manifest.json'
        if not baseline_id or not baseline.is_file():
            return False
        while current['job_id'] not in seen:
            source_id = current['job_id']
            seen.add(source_id)
            anchor = (queue.get('replacement_anchors') or {}).get(source_id) or {}
            target_id = anchor.get('replacement_job_id')
            if not target_id or anchor.get('kind') != 'CANDIDATELESS_QA_POLICY_RESOLUTION':
                return False
            target = self._read_job(target_id)
            resolution, fresh = target.get('qa_policy_resolution') or {}, target.get('fresh_input') or {}
            try:
                expected = current['current_requirement'] + '\n\n' + resolution_contract(resolution.get('policy'))
            except (ValueError, KeyError):
                return False
            logical = current.get('logical_job_id') or current.get('client_job_id') or source_id
            if (current.get('status') != AWAITING_QA or current.get('candidate_id')
                    or current.get('candidate_manifest') or current.get('task_owned_delta_files')
                    or current.get('last_result', {}).get('changed_files')
                    or current.get('last_result', {}).get('failure_code') != 'CANDIDATE_DELTA_EMPTY'
                    or self.repository.preserved_candidate_records(source_id)
                    or current.get('active_baseline_id') != baseline_id
                    or target.get('replaces_job_id') != source_id
                    or resolution.get('source_job_id') != source_id
                    or resolution.get('source_revision') != current.get('revision')
                    or resolution.get('source_task_id') != current.get('current_claim_task_id')
                    or resolution.get('logical_job_id') != logical
                    or (target.get('logical_job_id') or target.get('client_job_id') or target_id) != logical
                    or target.get('current_requirement') != expected
                    or resolution.get('contract_revision') != target.get('contract_revision')
                    or resolution.get('contract_sha256') != _canonical_sha256(expected)
                    or fresh.get('kind') != 'CANDIDATELESS_QA_POLICY_FRESH_INPUT'
                    or fresh.get('parent_job_id') != source_id
                    or fresh.get('parent_revision') != current.get('revision')
                    or fresh.get('generation') != queue.get('generation')
                    or fresh.get('active_baseline_id') != baseline_id):
                return False
            with self.repository.hold():
                executions = [json.loads(row[0]) for row in self.repository._connection.execute(
                    'SELECT payload FROM executions WHERE job_id=?', (target_id,))]
            if not any(e.get('current_requirement') == expected and e.get('request') == target.get('request')
                       and e.get('contract_revision') == target.get('contract_revision')
                       and e.get('baseline_hash') == hashlib.sha256(baseline.read_bytes()).hexdigest()
                       for e in executions):
                return False
            eligible = (resolution.get('policy') == SKELETON_POLICY and (
                (target_id == consumer.get('job_id') and target.get('status') in {QUEUED, RUNNING})
                or (self._is_skeleton_ready(target) and target.get('batch_id') == consumer.get('batch_id'))))
            if eligible:
                integrity = integrity_snapshot_provider(queue)
                return bool(integrity.get('active_baseline_id') == baseline_id
                    and integrity.get('baseline_declaration_integrity') is True
                    and integrity.get('external_frozen_integrity') is True
                    and integrity.get('unexpected_runtime_dirty_files') == []
                    and self.repository.candidate_ownership(workspace)['valid'])
            current = target
        return False

    def _quarantined_scope_conflict(
        self, queue: Mapping[str, Any], candidate: Mapping[str, Any], *,
        workspace=None, integrity_snapshot_provider=None,
    ) -> str:
        request = dict(candidate.get("request") or {})
        candidate_scope = list(request.get("target_resources") or request.get("target_modules") or [])
        dependencies = set(str(item) for item in candidate.get("depends_on") or [])
        for job_id in self._active_job_ids(queue):
            if job_id == candidate.get("job_id") or job_id in dependencies:
                continue
            qa_job = self._read_job(job_id)
            if (
                qa_job.get("status") not in {AWAITING_QA, AWAITING_DEPENDENCY_QA}
                or (not qa_job.get("candidate_id") and self.repository is None)
            ):
                continue
            # The declared corrective successor owns this QA's active path, so
            # the quarantined scope no longer holds it (0.9.0.9). Every other
            # overlapping Job stays held fail-closed.
            if self._resolved_candidateless_scope(queue, qa_job, candidate,
                    workspace=workspace, integrity_snapshot_provider=integrity_snapshot_provider):
                continue
            if self._resolved_skeleton_scope(queue, qa_job, candidate,
                    workspace=workspace, integrity_snapshot_provider=integrity_snapshot_provider):
                continue
            if self._completed_qa_corrective_successor_id(queue, job_id):
                continue
            if self._active_qa_corrective_successor_id(queue, job_id) == str(
                candidate.get("job_id", "")
            ):
                continue
            qa_scope = list(
                dict(qa_job.get("last_result") or {}).get("changed_files")
                or dict(qa_job.get("request") or {}).get("target_resources")
                or dict(qa_job.get("request") or {}).get("target_modules")
                or []
            )
            if self.repository is not None and (not candidate_scope or not qa_scope):
                return job_id
            if paths_overlap(candidate_scope, qa_scope):
                return job_id
        return ""

    @staticmethod
    def _refresh_pause(queue: dict[str, Any]) -> None:
        operator_paused = bool(queue.get("operator_paused"))
        blocked = bool(queue.get("blocked_by_job_id"))
        queue["paused"] = operator_paused or blocked
        if operator_paused:
            queue["pause_reason"] = queue.get("operator_pause_reason", "") or "operator_pause"
        elif blocked:
            queue["pause_reason"] = queue.get("gate_reason", "") or "STATE_GATE"
        else:
            queue["pause_reason"] = ""

    @classmethod
    def _set_gate(cls, queue: dict[str, Any], job_id: str, reason: str) -> None:
        queue["blocked_by_job_id"] = job_id
        queue["gate_reason"] = reason
        cls._refresh_pause(queue)

    @classmethod
    def _clear_gate(cls, queue: dict[str, Any], job_id: str) -> None:
        if queue.get("blocked_by_job_id") == job_id:
            queue["blocked_by_job_id"] = ""
            queue["gate_reason"] = ""
        cls._refresh_pause(queue)

    def _restore_earliest_gate(self, queue: dict[str, Any]) -> None:
        """Rebuild the FIFO decision gate from durable Job states."""
        blocker = ""
        reason = ""
        for job_id in self._active_job_ids(queue):
            if self._is_replacement_anchor_parent(queue, job_id):
                continue
            job = self._read_job(job_id)
            status = job.get("status")
            if status in PAUSING_STATUSES and not self._qa_non_global(job):
                blocker, reason = job_id, str(status)
                break
            if status == SUCCEEDED and not job.get("success_acknowledgements"):
                blocker, reason = job_id, SUCCESS_ACK_REQUIRED
                break
            if status == QUEUED:
                predecessor_gate = self._failed_predecessor_gate(queue, job)
                if predecessor_gate is not None:
                    blocker, reason = predecessor_gate
                    break
        if blocker:
            self._set_gate(queue, blocker, reason)
        else:
            current = str(queue.get("blocked_by_job_id", ""))
            if current:
                self._clear_gate(queue, current)
            else:
                self._refresh_pause(queue)

    def _write_queue(self, queue: dict[str, Any]) -> None:
        self._assert_writable()
        queue["revision"] = int(queue.get("revision", 0)) + 1
        queue["updated_at"] = _now()
        if self.repository is not None:
            self.repository.write_queue(_safe_json(queue), expected_revision=queue["revision"] - 1)
        else:
            _atomic_json_write(self.queue_path, queue, root=self.root)

    def _repair_replayed_action(
        self,
        queue: dict[str, Any],
        job: dict[str, Any],
        transition: str,
    ) -> None:
        """Finish an action whose Job commit survived but queue commit did not."""
        job_id = str(job.get("job_id", ""))
        changed = False
        if transition in {"retry", "resume"} and job.get("status") == QUEUED:
            if queue.get("blocked_by_job_id") == job_id:
                self._clear_gate(queue, job_id)
                changed = True
            if queue.get("running_job_id") == job_id:
                queue["running_job_id"] = ""
                changed = True
        elif transition == "resume_exhausted" and job.get("status") == FAILED_FINAL:
            if (
                queue.get("blocked_by_job_id") != job_id
                or queue.get("gate_reason") != FAILED_FINAL
            ):
                self._set_gate(queue, job_id, FAILED_FINAL)
                queue["running_job_id"] = ""
                changed = True
        elif transition == "ack" and job.get("status") == SUCCEEDED:
            if (
                queue.get("blocked_by_job_id") == job_id
                and queue.get("gate_reason") == SUCCESS_ACK_REQUIRED
            ):
                self._clear_gate(queue, job_id)
                changed = True
        elif transition == "skip" and job.get("status") == SKIPPED:
            if queue.get("blocked_by_job_id") == job_id:
                self._clear_gate(queue, job_id)
                changed = True
            if queue.get("running_job_id") == job_id:
                queue["running_job_id"] = ""
                changed = True
        elif transition == "manual_resolution" and job.get("status") == SKIPPED:
            before = (
                queue.get("blocked_by_job_id"),
                queue.get("gate_reason"),
                queue.get("running_job_id"),
                queue.get("paused"),
                queue.get("pause_reason"),
            )
            if queue.get("running_job_id") == job_id:
                queue["running_job_id"] = ""
            self._restore_earliest_gate(queue)
            after = (
                queue.get("blocked_by_job_id"),
                queue.get("gate_reason"),
                queue.get("running_job_id"),
                queue.get("paused"),
                queue.get("pause_reason"),
            )
            changed = changed or before != after
        if changed:
            self._write_queue(queue)

    def _job_path(self, job_id: str, *, for_write: bool = False) -> Path:
        if not isinstance(job_id, str) or not _JOB_ID_RE.fullmatch(job_id):
            raise QueueError("INVALID_JOB_ID")
        if self.repository is not None:
            return self.jobs_dir / f"{job_id}.json"
        try:
            ensure_safe_state_directory(
                self.root,
                self.jobs_dir,
                field="jobs_dir",
                create=for_write,
            )
            path = self.jobs_dir / f"{job_id}.json"
            ensure_safe_state_file(
                self.root,
                path,
                field="job_state",
                allow_missing=for_write,
            )
            return path
        except ControlPathError as exc:
            raise QueueError("CONTROL_STATE_PATH_UNSAFE", exc.code) from exc

    def _read_job(self, job_id: str) -> dict[str, Any]:
        path = self._job_path(job_id)
        try:
            data = self.repository.read_job(job_id) if self.repository is not None else json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise QueueError("JOB_NOT_FOUND", job_id) from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise QueueError("JOB_STATE_INVALID", job_id) from exc
        if data.get("schema_version") != QUEUE_SCHEMA_VERSION or data.get("job_id") != job_id:
            raise QueueError("JOB_STATE_INVALID", job_id)
        return data

    def _write_job(self, job: dict[str, Any]) -> None:
        self._assert_writable()
        job["revision"] = int(job.get("revision", 0)) + 1
        job["updated_at"] = _now()
        if self.repository is not None:
            self.repository.write_job(_safe_json(job), expected_revision=job["revision"] - 1)
        else:
            _atomic_json_write(
                self._job_path(job["job_id"], for_write=True), job, root=self.root
            )

    def _job_exists(self, job_id: str) -> bool:
        return (self.repository.has_job(job_id) if self.repository is not None
                else self._job_path(job_id, for_write=True).exists())

    @staticmethod
    def _new_job_id() -> str:
        return f"JOB-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(4)}"

    def enqueue_batch(
        self,
        idempotency_key: str,
        requests: Sequence[JobRequest],
        *,
        execution_context: Mapping[str, Any] | None = None,
        source_manifest_path: str | Path | None = None,
        enqueue_actor: str = "control",
        batch_execution_policy: Mapping[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        self._assert_writable()
        key = validate_idempotency_key(idempotency_key)
        key_digest = _identifier_digest(key)
        persisted_key = f"sha256:{key_digest}"
        if not requests:
            raise QueueError("EMPTY_BATCH")
        context = _safe_json(validate_execution_context(execution_context))
        request_payloads = [request.to_dict() for request in requests]
        provenance: dict[str, Any]
        if source_manifest_path is None:
            provenance = {"status": "LEGACY_PROVENANCE_UNVERIFIED"}
        else:
            try:
                provenance = build_provenance(
                    source_manifest_path,
                    request_payloads,
                    enqueue_request_id=key,
                    enqueue_actor=str(enqueue_actor),
                    execution_context=context,
                    execution_policy=dict(batch_execution_policy or {}),
                )
            except BatchManifestError as exc:
                raise QueueError(exc.code, exc.detail) from exc
        context_raw = json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(
            f"{request_fingerprint(requests)}:{context_raw}:{provenance.get('source_manifest_sha256', '')}".encode("utf-8")
        ).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            if (
                queue.get("commit_policy") == BATCH_FINAL_COMMIT
                and not queue.get("active_baseline_id")
            ):
                raise QueueError("CUMULATIVE_BASELINE_NOT_ACTIVE")
            previous = queue["batches"].get(persisted_key)
            if previous is None and key in queue["batches"]:
                previous = queue["batches"].pop(key)
                queue["batches"][persisted_key] = previous
                self._write_queue(queue)
            if previous is not None:
                if previous.get("fingerprint") != fingerprint:
                    raise QueueError("IDEMPOTENCY_CONFLICT", key_digest[:16])
                if previous.get("state") == "PREPARING":
                    recovered = self._finish_prepared_batch(
                        queue, persisted_key, previous
                    )
                    return recovered, True
                return [self._read_job(job_id) for job_id in previous.get("job_ids", [])], True

            generation = int(queue.get("generation", 1))
            batch_digest = (
                key_digest
                if generation == 1
                else hashlib.sha256(
                    f"generation:{generation}:{key_digest}".encode("utf-8")
                ).hexdigest()
            )
            batch_id = f"BATCH-{batch_digest[:24]}"
            prepared: list[dict[str, Any]] = []
            reserved_ids: set[str] = set()
            allocated_ids: list[str] = []
            for _request in requests:
                job_id = self._new_job_id()
                while (
                    job_id in reserved_ids
                    or self._job_exists(job_id)
                ):
                    job_id = self._new_job_id()
                reserved_ids.add(job_id)
                allocated_ids.append(job_id)
            client_ids = [request.client_job_id for request in requests if request.client_job_id]
            if len(client_ids) != len(set(client_ids)):
                raise QueueError("DUPLICATE_CLIENT_JOB_ID")
            client_map = {
                request.client_job_id: allocated_ids[index]
                for index, request in enumerate(requests)
                if request.client_job_id
            }
            existing_ids = set(str(item) for item in queue.get("order") or [])
            for sequence, (request, job_id) in enumerate(
                zip(requests, allocated_ids), start=1
            ):
                dependency_refs = list(request.depends_on or ())
                resolved_dependencies: list[str] = []
                if request.depends_on is not None:
                    for reference in dependency_refs:
                        resolved = client_map.get(reference, reference)
                        if resolved not in reserved_ids and resolved not in existing_ids:
                            raise QueueError("DEPENDENCY_NOT_FOUND", reference)
                        if resolved == job_id:
                            raise QueueError("DEPENDENCY_CYCLE", reference)
                        if resolved not in resolved_dependencies:
                            resolved_dependencies.append(resolved)
                now = _now()
                job = {
                    "schema_version": QUEUE_SCHEMA_VERSION,
                    "job_id": job_id,
                    "client_job_id": request.client_job_id,
                    "batch_id": batch_id,
                    "generation": int(queue.get("generation", 0)),
                    "sequence": sequence,
                    "dependency_semantics": (
                        "EXPLICIT" if request.depends_on is not None else "LEGACY_SEQUENCE"
                    ),
                    "depends_on": resolved_dependencies,
                    "depends_on_refs": dependency_refs,
                    "qa_type": request.qa_type,
                    "hold_scope": request.hold_scope,
                    "machine_verified": request.machine_verified,
                    "candidate_id": "",
                    "candidate_manifest": "",
                    "speculative_candidate_ids": [],
                    "speculative_ready": False,
                    "status": QUEUED,
                    "request": request.to_dict(),
                    "execution_context": context,
                    "current_requirement": request.requirement,
                    "outer_attempt": 0,
                    "max_outer_attempts": request.max_outer_attempts,
                    "normal_attempt_budget": request.max_outer_attempts,
                    "technical_recovery_budget": 1,
                    "request_sha256": request_sha256(request.to_dict()),
                    "attempt_reservations": [],
                    "active_attempt_reservation": {},
                    "pending_attempt_kind": NORMAL,
                    "pending_attempt_provenance": {},
                    "task_ids": [],
                    "attempts": [],
                    "last_result": {},
                    "retry_actions": {},
                    "supplements": [],
                    "success_acknowledgements": {},
                    "skip_actions": {},
                    "resume_actions": {},
                    "manual_resolution_actions": {},
                    "commit_actions": {},
                    "profile_revalidation_actions": {},
                    "pre_job_snapshots": [],
                    "pre_job_snapshot_id": "",
                    "pre_job_snapshot_manifest": "",
                    "recovery_attempt_count": 0,
                    "history": [{"at": now, "event": "ENQUEUED"}],
                    "owner_id": "",
                    "current_claim_task_started": False,
                    "current_claim_task_id": "",
                    "last_claim_task_started": False,
                    "last_claim_task_id": "",
                    "created_at": now,
                    "updated_at": now,
                    "revision": 0,
                }
                prepared.append(job)
            graph = {
                str(job["job_id"]): [
                    dep for dep in job.get("depends_on", []) if dep in reserved_ids
                ]
                for job in prepared
            }
            visiting: set[str] = set()
            visited: set[str] = set()
            def visit(node: str) -> None:
                if node in visiting:
                    raise QueueError("DEPENDENCY_CYCLE", node)
                if node in visited:
                    return
                visiting.add(node)
                for dependency in graph.get(node, []):
                    visit(dependency)
                visiting.remove(node)
                visited.add(node)
            for node in graph:
                visit(node)
            queue["batches"][persisted_key] = {
                "fingerprint": fingerprint,
                "job_ids": [job["job_id"] for job in prepared],
                "batch_id": batch_id,
                "provenance": provenance,
                "state": "PREPARING",
                "prepared_jobs": prepared,
                "created_at": _now(),
            }
            # Intent is durable before the first Job file. A restart can finish
            # the exact prepared IDs without creating an unreferenced duplicate.
            self._write_queue(queue)
            created = self._finish_prepared_batch(
                queue, persisted_key, queue["batches"][persisted_key]
            )
            return created, False

    def enqueue_anchored_replacement(
        self,
        replaces_job_id: str,
        replacement: JobRequest,
        *,
        expected_queue_revision: int,
        expected_job_revision: int,
        request_id: str,
        reason: str,
        execution_context: Mapping[str, Any],
        provenance: Mapping[str, Any],
        integrity_snapshot: Mapping[str, Any],
        checkpoint_seed: Mapping[str, Any] | None = None,
        fresh_input: Mapping[str, Any] | None = None,
        actor: str = "harness-supervisor",
        policy_resolution: Mapping[str, str] | None = None,
        decision_resolution: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Append one immutable replacement and anchor its effective Queue position.

        This deliberately is not a general reorder API.  Only a clean FAILED_FINAL
        corrective or a provably unstarted QUEUED contract replacement is accepted.
        Raw Queue order and sibling Job records are never rewritten.
        """
        self._assert_writable()
        if isinstance(expected_queue_revision, bool) or not isinstance(expected_queue_revision, int):
            raise QueueError("EXPECTED_QUEUE_REVISION_INVALID")
        if isinstance(expected_job_revision, bool) or not isinstance(expected_job_revision, int):
            raise QueueError("EXPECTED_REVISION_INVALID")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = self.action_key(action_id)
        action_ref = _identifier_digest(action_id)[:16]
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("REPLACEMENT_REASON_REQUIRED")
        context = _safe_json(validate_execution_context(execution_context))
        safe_provenance = _safe_json(dict(provenance))
        if safe_provenance.get("status") != "VERIFIED":
            raise QueueError("BATCH_MANIFEST_PROVENANCE_INVALID")
        integrity = _safe_json(dict(integrity_snapshot))
        seed = _safe_json(dict(checkpoint_seed or {}))
        fresh = _safe_json(dict(fresh_input or {}))
        fingerprint_payload = {
            "replaces_job_id": replaces_job_id,
            "replacement": replacement.to_dict(),
            "expected_queue_revision": expected_queue_revision,
            "expected_job_revision": expected_job_revision,
            "reason": safe_reason,
            "context": context,
            "provenance": safe_provenance,
            "checkpoint_seed": seed,
            "actor": str(actor),
        }
        if fresh:
            fingerprint_payload["fresh_input"] = fresh
        if policy_resolution is not None:
            fingerprint_payload['policy_resolution'] = dict(policy_resolution)
        if decision_resolution is not None:
            fingerprint_payload['decision_resolution'] = dict(decision_resolution)
        fingerprint = _canonical_sha256(fingerprint_payload)

        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("anchored_replacement_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if prior.get("fingerprint") != fingerprint:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if prior.get("state") == "PREPARING":
                    recovered = self._finish_prepared_replacement(
                        queue, action_key, prior
                    )
                    return recovered, True
                if migrated:
                    self._write_queue(queue)
                return self._read_job(str(prior["replacement_job_id"])), True

            # All CAS and safety checks precede the first write: stale or unsafe
            # requests have mutation=0.
            if int(queue.get("revision", -1)) != expected_queue_revision:
                raise QueueError("QUEUE_REVISION_CONFLICT")
            if queue.get("running_job_id"):
                raise QueueError("ACTIVE_HARNESS_PROCESS", str(queue.get("running_job_id")))
            parent = self._read_job(replaces_job_id)
            if int(parent.get("revision", -1)) != expected_job_revision:
                raise QueueError("JOB_REVISION_CONFLICT", replaces_job_id)
            if replaces_job_id not in self._active_job_ids(queue):
                raise QueueError("JOB_NOT_ACTIVE", replaces_job_id)
            if replaces_job_id in dict(queue.get("replacement_anchors") or {}):
                raise QueueError("JOB_ALREADY_REPLACED", replaces_job_id)
            if (
                integrity.get("external_frozen_integrity") is not True
                or integrity.get("baseline_declaration_integrity") is not True
                or list(integrity.get("unexpected_runtime_dirty_files") or [])
                or int(integrity.get("generation", -1)) != int(queue.get("generation", 0))
                or str(integrity.get("active_baseline_id", ""))
                   != str(queue.get("active_baseline_id", ""))
            ):
                raise QueueError("REPLACEMENT_WORKTREE_INTEGRITY_INVALID", replaces_job_id)

            parent_status = str(parent.get("status", ""))
            if policy_resolution is not None and decision_resolution is not None:
                raise QueueError("MULTIPLE_RESOLUTION_KINDS_FORBIDDEN")
            if decision_resolution is not None:
                if self.repository is None:
                    raise QueueError("DECISION_REPOSITORY_REQUIRED")
                decisions = self.repository.decisions(replaces_job_id)
                supplied = {str(key): str(value) for key, value in decision_resolution.items()}
                if (parent_status not in {SKELETON_READY, AWAITING_QA}
                        or not decisions or any(item.get('status') != 'RESOLVED' for item in decisions)
                        or set(supplied) != {item['decision_id'] for item in decisions}
                        or any(supplied[item['decision_id']] != item.get('resolution') for item in decisions)
                        or seed or fresh.get('kind') != 'DECISION_FINALIZATION_FRESH_INPUT'
                        or fresh.get('parent_job_id') != replaces_job_id
                        or fresh.get('parent_revision') != parent.get('revision')
                        or fresh.get('generation') != queue.get('generation')
                        or fresh.get('active_baseline_id') != queue.get('active_baseline_id')
                        or any(replacement.to_dict().get(k) != v for k,v in parent['request'].items() if k != 'requirement')):
                    raise QueueError('DECISION_FINALIZATION_STATE_INVALID', replaces_job_id)
                kind = 'DECISION_FINALIZATION'
            elif policy_resolution is not None:
                from qa_policy_resolution import validate_resolution, resolution_contract
                try:
                    validate_resolution(policy_resolution)
                except ValueError as exc:
                    raise QueueError(str(exc)) from exc
                if (parent_status != AWAITING_QA or parent.get('candidate_id')
                        or parent.get('candidate_manifest') or parent.get('task_owned_delta_files')
                        or (self.repository is not None and self.repository.preserved_candidate_records(replaces_job_id))
                        or parent.get('last_result', {}).get('failure_code') != 'CANDIDATE_DELTA_EMPTY'
                        or fresh.get('kind') != 'CANDIDATELESS_QA_POLICY_FRESH_INPUT'
                        or fresh.get('parent_job_id') != replaces_job_id
                        or fresh.get('parent_revision') != parent.get('revision')
                        or fresh.get('generation') != queue.get('generation')
                        or fresh.get('active_baseline_id') != queue.get('active_baseline_id')
                        or fresh.get('source_task_id') != parent.get('current_claim_task_id')
                        or not fresh.get('manifest_sha256') or seed
                        or replacement.requirement != str(parent['current_requirement']) + '\n\n' + resolution_contract(policy_resolution)
                        or any(replacement.to_dict().get(k) != v for k,v in parent['request'].items() if k != 'requirement')
                        or queue.get('blocked_by_job_id') != replaces_job_id
                        or queue.get('gate_reason') != AWAITING_QA):
                    raise QueueError('QA_POLICY_RESOLUTION_STATE_INVALID', replaces_job_id)
                kind = 'CANDIDATELESS_QA_POLICY_RESOLUTION'
            elif parent_status == FAILED_FINAL:
                kind = "FAILED_FINAL_CORRECTIVE"
                disposition = str(dict(parent.get("last_result") or {}).get("worktree_disposition", ""))
                if disposition not in {"CLEAN_ROLLBACK", "CHECKPOINTED_CLEAN_ROLLBACK", "NO_DELTA"}:
                    raise QueueError("CORRECTIVE_SOURCE_OWNERSHIP_INVALID", replaces_job_id)
                last = dict(parent.get("last_result") or {})
                no_delta = bool(fresh and not seed
                    and fresh.get("kind") == "VERIFIED_NO_TASK_DELTA_FRESH_INPUT"
                    and fresh.get("parent_job_id") == replaces_job_id
                    and fresh.get("parent_revision") == parent.get("revision")
                    and fresh.get("generation") == queue.get("generation")
                    and fresh.get("active_baseline_id") == queue.get("active_baseline_id")
                    and fresh.get("manifest_path") == parent.get("pre_job_snapshot_manifest")
                    and fresh.get("snapshot_id") == parent.get("pre_job_snapshot_id")
                    and fresh.get("manifest_sha256")
                    and last.get("checkpoint_status") == "VERIFIED_NO_TASK_DELTA"
                    and not last.get("checkpoint_manifest")
                    and not last.get("task_owned_changed_files")
                    and not last.get("changed_files")
                    and not parent.get("checkpoint_seed")
                    and not parent.get("task_owned_delta_files")
                    and dict(last.get("no_task_delta_evidence") or {}).get("passed") is True)
                if (fresh and not no_delta) or (not no_delta and (not seed or seed.get("valid") is not True)):
                    raise QueueError("CORRECTIVE_CHECKPOINT_INVALID", replaces_job_id)
                if seed and str(seed.get("parent_job_id", "")) != replaces_job_id:
                    raise QueueError("CHECKPOINT_JOB_MISMATCH", replaces_job_id)
                if seed and int(seed.get("generation", -1)) != int(queue.get("generation", 0)):
                    raise QueueError("CHECKPOINT_GENERATION_MISMATCH")
                if seed and str(seed.get("active_baseline_id", "")) != str(queue.get("active_baseline_id", "")):
                    raise QueueError("CHECKPOINT_BASELINE_MISMATCH")
                allowed = {str(path).replace("\\", "/") for path in replacement.target_resources}
                changed = {str(path).replace("\\", "/") for path in seed.get("changed_files") or []}
                if not no_delta and (not changed or not changed.issubset(allowed)):
                    raise QueueError("CHECKPOINT_SCOPE_MISMATCH")
                if queue.get("blocked_by_job_id") != replaces_job_id or queue.get("gate_reason") != FAILED_FINAL:
                    raise QueueError("JOB_NOT_CURRENT_BLOCKER", replaces_job_id)
            elif parent_status == QUEUED:
                kind = "QUEUED_CONTRACT_REPLACEMENT"
                last = dict(parent.get("last_result") or {})
                has_delta = bool(
                    last.get("changed_files")
                    or parent.get("candidate_id")
                    or parent.get("candidate_manifest")
                    or parent.get("task_owned_delta_files")
                )
                if (
                    int(parent.get("outer_attempt", 0)) != 0
                    or list(parent.get("task_ids") or [])
                    or list(parent.get("attempts") or [])
                    or bool(parent.get("current_claim_task_started"))
                    or int(parent.get("worker_invocation_count", 0)) != 0
                    or has_delta
                ):
                    raise QueueError("QUEUED_REPLACEMENT_NOT_PRISTINE", replaces_job_id)
                if seed or fresh:
                    raise QueueError("QUEUED_REPLACEMENT_CHECKPOINT_NOT_ALLOWED")
            else:
                raise QueueError("JOB_NOT_REPLACEABLE", replaces_job_id)

            job_id = self._new_job_id()
            while self._job_exists(job_id):
                job_id = self._new_job_id()
            now = _now()
            dependency_refs = list(replacement.depends_on or ())
            known_ids = set(str(item) for item in queue.get("order") or [])
            known_clients = {
                str(self._read_job(item).get("client_job_id", "")): str(item)
                for item in known_ids
            }
            duplicate_client = known_clients.get(replacement.client_job_id)
            allowed_clients = {replaces_job_id}
            if policy_resolution is not None or decision_resolution is not None:
                ancestor = parent
                while ancestor.get('replaces_job_id'):
                    ancestor_id = ancestor['replaces_job_id']
                    anchor = (queue.get('replacement_anchors') or {}).get(ancestor_id) or {}
                    if (ancestor_id in allowed_clients
                            or anchor.get('kind') not in {'CANDIDATELESS_QA_POLICY_RESOLUTION', 'DECISION_FINALIZATION'}
                            or anchor.get('replacement_job_id') != ancestor['job_id']):
                        break
                    prior = self._read_job(ancestor_id)
                    resolution = ancestor.get('qa_policy_resolution') or ancestor.get('decision_resolution') or {}
                    if (prior.get('client_job_id') != replacement.client_job_id
                            or resolution.get('source_job_id') != ancestor_id
                            or resolution.get('source_revision') != prior.get('revision')):
                        break
                    allowed_clients.add(ancestor_id)
                    ancestor = prior
                duplicate_client = next((i for i in known_ids if i not in allowed_clients
                    and self._read_job(i).get('client_job_id') == replacement.client_job_id), None)
            if replacement.client_job_id and duplicate_client not in {None, *allowed_clients}:
                raise QueueError("DUPLICATE_CLIENT_JOB_ID", replacement.client_job_id)
            resolved_dependencies: list[str] = []
            for reference in dependency_refs:
                resolved = known_clients.get(reference, reference)
                if resolved not in known_ids:
                    raise QueueError("DEPENDENCY_NOT_FOUND", reference)
                if resolved not in resolved_dependencies:
                    resolved_dependencies.append(resolved)
            new_job = {
                "schema_version": QUEUE_SCHEMA_VERSION,
                "job_id": job_id,
                "client_job_id": replacement.client_job_id,
                "batch_id": parent.get("batch_id", ""),
                "generation": int(queue.get("generation", 0)),
                "sequence": parent.get("sequence", 0),
                "dependency_semantics": "EXPLICIT" if replacement.depends_on is not None else "LEGACY_SEQUENCE",
                "depends_on": resolved_dependencies,
                "depends_on_refs": dependency_refs,
                "qa_type": replacement.qa_type,
                "hold_scope": replacement.hold_scope,
                "machine_verified": replacement.machine_verified,
                "candidate_id": "", "candidate_manifest": "",
                "speculative_candidate_ids": [], "speculative_ready": False,
                "status": QUEUED,
                "request": replacement.to_dict(),
                "execution_context": context,
                "current_requirement": replacement.requirement,
                "outer_attempt": 0,
                "max_outer_attempts": replacement.max_outer_attempts,
                "normal_attempt_budget": replacement.max_outer_attempts,
                "technical_recovery_budget": 1,
                "request_sha256": request_sha256(replacement.to_dict()),
                "attempt_reservations": [], "active_attempt_reservation": {},
                "pending_attempt_kind": NORMAL, "pending_attempt_provenance": {},
                "task_ids": [], "attempts": [], "last_result": {},
                "retry_actions": {}, "supplements": [],
                "success_acknowledgements": {}, "skip_actions": {},
                "resume_actions": {}, "manual_resolution_actions": {},
                "commit_actions": {}, "profile_revalidation_actions": {},
                "technical_retry_actions": {}, "review_only_actions": {},
                "qa_resolution_actions": {},
                "pre_job_snapshots": [], "pre_job_snapshot_id": "", "pre_job_snapshot_manifest": "",
                "recovery_attempt_count": 0,
                "owner_id": "", "current_claim_task_started": False,
                "current_claim_task_id": "", "last_claim_task_started": False,
                "last_claim_task_id": "",
                "replaces_job_id": replaces_job_id,
                "corrects_job_id": replaces_job_id if kind == "FAILED_FINAL_CORRECTIVE" else "",
                "replacement_kind": kind,
                "replacement_anchor": {"job_id": replaces_job_id, "sequence": parent.get("sequence", 0)},
                "checkpoint_seed": seed,
                "fresh_input": fresh,
                "manifest_provenance": safe_provenance,
                "history": [{"at": now, "event": "ANCHORED_REPLACEMENT_ENQUEUED", "replaces_job_id": replaces_job_id, "kind": kind, "request_ref": action_ref}],
                "created_at": now, "updated_at": now, "revision": 0,
            }
            anchor = {
                "replacement_job_id": job_id,
                "kind": kind,
                "anchor_sequence": parent.get("sequence", 0),
                "reason": safe_reason,
                "actor": str(actor),
                "request_ref": action_ref,
                "created_at": now,
            }
            if policy_resolution is not None:
                new_job['contract_revision'] = int(parent.get('contract_revision') or 1) + 1
                new_job['qa_policy_resolution'] = {
                    'source_job_id': replaces_job_id, 'source_revision': parent['revision'],
                    'source_task_id': fresh['source_task_id'], 'policy': dict(policy_resolution),
                    'logical_job_id': parent.get('logical_job_id') or parent.get('client_job_id') or replaces_job_id,
                    'contract_revision': new_job['contract_revision'], 'actor': str(actor),
                    'request_ref': action_ref, 'recorded_at': now,
                    'contract_sha256': _canonical_sha256(replacement.requirement),
                }
            if decision_resolution is not None:
                new_job['contract_revision'] = int(parent.get('contract_revision') or 1) + 1
                new_job['decision_resolution'] = {
                    'source_job_id': replaces_job_id, 'source_revision': parent['revision'],
                    'logical_job_id': parent.get('logical_job_id') or parent.get('client_job_id') or replaces_job_id,
                    'contract_revision': new_job['contract_revision'], 'actor': str(actor),
                    'request_ref': action_ref, 'recorded_at': now,
                    'resolutions': dict(decision_resolution),
                    'contract_sha256': _canonical_sha256(replacement.requirement),
                }
            corrective_handoff = (
                {
                    "state": "ACTIVE",
                    "failed_job_id": replaces_job_id,
                    "corrective_job_id": job_id,
                    "reason": safe_reason,
                    "actor": str(actor),
                    "request_ref": action_ref,
                    "created_at": now,
                }
                if kind == "FAILED_FINAL_CORRECTIVE" else {}
            )
            history_event = {
                "at": now, "event": "ANCHORED_REPLACEMENT_CREATED",
                "replaces_job_id": replaces_job_id, "replacement_job_id": job_id,
                "kind": kind, "reason": safe_reason, "actor": str(actor),
                "expected_queue_revision": expected_queue_revision,
                "expected_job_revision": expected_job_revision,
                "request_ref": action_ref,
            }
            actions[action_key] = {
                "fingerprint": fingerprint,
                "replacement_job_id": job_id,
                "replaces_job_id": replaces_job_id,
                "kind": kind,
                "state": "PREPARING",
                "expected_job_revision": expected_job_revision,
                "request_ref": action_ref,
                "prepared_job": new_job,
                "anchor": anchor,
                "corrective_handoff": corrective_handoff,
                "history_event": history_event,
                "created_at": now,
            }
            # Durable intent precedes every Job/parent mutation. Constructor replay
            # can complete this exact allocation after a process interruption.
            self._write_queue(queue)
            created = self._finish_prepared_replacement(
                queue, action_key, actions[action_key]
            )
            return created, False

    def queue_snapshot(self) -> dict[str, Any]:
        with self._mutex.hold():
            queue = self._read_queue()
            counts: dict[str, int] = {}
            active_ids = self._active_job_ids(queue)
            lineage_anomalies: list[dict[str, str]] = []
            for job_id in active_ids:
                active_job = self._read_job(job_id)
                status = active_job.get("status", "UNKNOWN")
                counts[status] = counts.get(status, 0) + 1
                lineage = lineage_status(active_job)
                if not lineage.get("valid"):
                    lineage_anomalies.append(
                        {"job_id": job_id, "code": str(lineage.get("code", ""))}
                    )
            batch_id = self._current_queue_batch_id(queue)
            batch_record = next(
                (
                    item for item in queue.get("batches", {}).values()
                    if isinstance(item, dict) and item.get("batch_id") == batch_id
                ),
                {},
            )
            return {
                "schema_version": queue["schema_version"],
                "paused": bool(queue.get("paused")),
                "pause_reason": queue.get("pause_reason", ""),
                "operator_paused": bool(queue.get("operator_paused")),
                "operator_pause_reason": queue.get("operator_pause_reason", ""),
                "gate_reason": queue.get("gate_reason", ""),
                "blocked_by_job_id": queue.get("blocked_by_job_id", ""),
                "running_job_id": queue.get("running_job_id", ""),
                "total": len(active_ids),
                "history_total": len(queue["order"]),
                "execution_mode": queue.get("execution_mode", EXECUTION_MODE_ATTENDED),
                "generation": int(queue.get("generation", 1)),
                "counts": counts,
                "revision": queue.get("revision", 0),
                "updated_at": queue.get("updated_at", ""),
                "commit_policy": queue.get("commit_policy", PER_JOB_COMMIT),
                "cumulative_worktree": bool(queue.get("cumulative_worktree")),
                "auto_continue_without_commit": bool(
                    queue.get("auto_continue_without_commit")
                ),
                "batch_owned_delta_supported": bool(
                    queue.get("batch_owned_delta_supported")
                ),
                "pre_job_snapshot_supported": bool(
                    queue.get("pre_job_snapshot_supported")
                ),
                "scoped_rollback_supported": bool(
                    queue.get("scoped_rollback_supported")
                ),
                "final_commit_required": bool(queue.get("final_commit_required")),
                "active_baseline_id": queue.get("active_baseline_id", ""),
                "task_owned_baseline_files": list(
                    queue.get("task_owned_baseline_files") or []
                ),
                "external_frozen_baseline_files": list(
                    queue.get("external_frozen_baseline_files") or []
                ),
                "batch_delta_files": list(queue.get("batch_delta_files") or []),
                "unexpected_runtime_dirty_files": list(
                    queue.get("unexpected_runtime_dirty_files") or []
                ),
                "job_validation_success_count": int(
                    queue.get("job_validation_success_count", 0)
                ),
                "job_delta_preserved_count": int(
                    queue.get("job_delta_preserved_count", 0)
                ),
                "job_commit_deferred_count": int(
                    queue.get("job_commit_deferred_count", 0)
                ),
                "batch_final_staging_eligible": bool(
                    queue.get("batch_final_staging_eligible")
                ),
                "batch_final_commit_eligible": bool(
                    queue.get("batch_final_commit_eligible")
                ),
                "baseline_advance_eligible": bool(
                    queue.get("baseline_advance_eligible")
                ),
                "external_delta_adoption": copy.deepcopy(
                    dict(queue.get("external_delta_adoption") or {})
                ),
                "batch_manifest_provenance_status": str(
                    dict(batch_record.get("provenance") or {}).get(
                        "status", "LEGACY_PROVENANCE_UNVERIFIED"
                    )
                ),
                "attempt_accounting_anomalies": lineage_anomalies,
                "effective_order": active_ids,
                "replacement_anchors": copy.deepcopy(
                    dict(queue.get("replacement_anchors") or {})
                ),
                "corrective_handoff": copy.deepcopy(
                    dict(queue.get("corrective_handoff") or {})
                ),
                "qa_corrective_successors": copy.deepcopy(
                    dict(queue.get("qa_corrective_successors") or {})
                ),
            }

    def activate_cumulative_policy(
        self,
        generation: int,
        fields: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Attach one verified Batch baseline to an empty logical generation."""
        self._assert_writable()
        with self._mutex.hold():
            queue = self._read_queue()
            if int(queue.get("generation", 1)) != int(generation):
                raise QueueError("CUMULATIVE_BASELINE_GENERATION_MISMATCH")
            if self._active_job_ids(queue) or queue.get("running_job_id"):
                raise QueueError("CUMULATIVE_BASELINE_REQUIRES_EMPTY_QUEUE")
            if fields.get("commit_policy") != BATCH_FINAL_COMMIT:
                raise QueueError("CUMULATIVE_POLICY_INVALID")
            required_true = (
                "cumulative_worktree",
                "auto_continue_without_commit",
                "batch_owned_delta_supported",
                "pre_job_snapshot_supported",
                "scoped_rollback_supported",
                "final_commit_required",
            )
            if any(fields.get(name) is not True for name in required_true):
                raise QueueError("CUMULATIVE_POLICY_INVALID")
            baseline_id = str(fields.get("active_baseline_id", ""))
            if not baseline_id:
                raise QueueError("CUMULATIVE_BASELINE_NOT_ACTIVE")
            desired = {
                "commit_policy": BATCH_FINAL_COMMIT,
                **{name: True for name in required_true},
                "active_baseline_id": baseline_id,
                "task_owned_baseline_files": list(
                    fields.get("task_owned_baseline_files") or []
                ),
                "external_frozen_baseline_files": list(
                    fields.get("external_frozen_baseline_files") or []
                ),
                "batch_delta_files": [],
                "unexpected_runtime_dirty_files": list(
                    fields.get("unexpected_runtime_dirty_files") or []
                ),
                "job_validation_success_count": 0,
                "job_delta_preserved_count": 0,
                "job_commit_deferred_count": 0,
                "batch_final_staging_eligible": False,
                "batch_final_commit_eligible": False,
                "baseline_advance_eligible": False,
            }
            if all(queue.get(key) == value for key, value in desired.items()):
                return queue
            queue.update(desired)
            self._write_queue(queue)
            return queue

    def adopt_external_baseline(
        self,
        batch_id: str,
        *,
        expected_generation: int,
        expected_queue_revision: int,
        expected_job_revisions: Mapping[str, int],
        fields: Mapping[str, Any],
        request_id: str,
        confirmation: str,
        execution_idle: bool,
    ) -> tuple[dict[str, Any], bool]:
        """Revision-safely attach exact user-approved external bytes to a queued Batch."""
        self._assert_writable()
        if confirmation != EXTERNAL_BASELINE_ADOPTION_CONFIRMATION:
            raise QueueError("CONFIRMATION_REQUIRED")
        if not execution_idle:
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = self.action_key(action_id)
        adoption = dict(fields.get("external_delta_adoption") or {})
        fingerprint = _canonical_sha256({
            "batch_id": batch_id,
            "expected_generation": int(expected_generation),
            "expected_job_revisions": {
                str(key): int(value) for key, value in expected_job_revisions.items()
            },
            "baseline_id": str(fields.get("active_baseline_id", "")),
            "adoption_fingerprint": str(adoption.get("adoption_fingerprint", "")),
        })
        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("external_baseline_adoption_actions", {})
            prior = actions.get(action_key)
            if prior is not None:
                if prior.get("fingerprint") != fingerprint:
                    raise QueueError("IDEMPOTENCY_CONFLICT")
                return dict(prior), True
            if not queue.get("operator_paused"):
                raise QueueError("EXTERNAL_BASELINE_ADOPTION_REQUIRES_OPERATOR_PAUSE")
            if queue.get("running_job_id"):
                raise QueueError("ACTIVE_HARNESS_PROCESS")
            if int(queue.get("generation", -1)) != int(expected_generation):
                raise QueueError("QUEUE_GENERATION_CONFLICT")
            if int(queue.get("revision", -1)) != int(expected_queue_revision):
                raise QueueError("QUEUE_REVISION_CONFLICT")
            if fields.get("commit_policy") != BATCH_FINAL_COMMIT:
                raise QueueError("CUMULATIVE_POLICY_INVALID")
            if (
                not str(fields.get("active_baseline_id", ""))
                or adoption.get("classification") != "USER_ACCEPTED_EXTERNAL_DELTA"
                or adoption.get("origin") != "EXTERNAL"
                or adoption.get("adoption") != "USER_APPROVED"
                # The adoption disposition must state its truthful boundary:
                # integrity-only baseline adoption with no Build/Test/Review/
                # Verification of the external delta (Direct validated
                # publication is a separate, explicitly deferred contract).
                or adoption.get("validation_boundary") != "BASELINE_ADOPTION_ONLY"
                or str(adoption.get("generation_provenance", "")).upper() not in {"UNKNOWN", "PARTIAL"}
                or fields.get("repository_head_integrity") is not True
                or fields.get("dirty_overlay_path_set_match") is not True
                or fields.get("dirty_overlay_hash_match") is not True
                or fields.get("baseline_declaration_integrity") is not True
                or fields.get("external_frozen_integrity") is not True
                or list(fields.get("unexpected_runtime_dirty_files") or [])
            ):
                raise QueueError("EXTERNAL_BASELINE_INTEGRITY_INVALID")
            batch = next(
                (
                    item for item in queue.get("batches", {}).values()
                    if isinstance(item, Mapping) and item.get("batch_id") == batch_id
                ),
                None,
            )
            if batch is None or batch.get("state") != "COMMITTED":
                raise QueueError("BATCH_NOT_FOUND")
            target_ids = [str(item) for item in batch.get("job_ids") or []]
            if not target_ids or set(target_ids) != set(expected_job_revisions):
                raise QueueError("EXTERNAL_BASELINE_JOB_SET_MISMATCH")
            jobs = [self._read_job(job_id) for job_id in target_ids]
            if any(
                int(job.get("revision", -1)) != int(expected_job_revisions[job["job_id"]])
                for job in jobs
            ):
                raise QueueError("JOB_REVISION_CONFLICT")
            if any(
                job.get("status") != QUEUED
                or int(job.get("outer_attempt", -1)) != 0
                or job.get("task_ids")
                for job in jobs
            ):
                raise QueueError("EXTERNAL_BASELINE_JOB_STATE_INVALID")
            route = dict(
                dict(dict(batch.get("provenance") or {}).get("batch_execution_policy") or {})
                .get("worker_route") or {}
            )
            expected_route = {
                "runtime": "opencode",
                "model": "zai-coding-plan/glm-5.3-flash",
                "reasoning_effort": "low",
                "fallback": "none",
            }
            if route != expected_route or any(
                {
                    "runtime": job.get("request", {}).get("worker"),
                    "model": job.get("request", {}).get("model"),
                    "reasoning_effort": job.get("request", {}).get("reasoning_effort"),
                }
                != {key: expected_route[key] for key in ("runtime", "model", "reasoning_effort")}
                for job in jobs
            ):
                raise QueueError("BATCH_WORKER_CONTRACT_INVALID")
            previous_baseline_id = str(queue.get("active_baseline_id", ""))
            desired = {
                "active_baseline_id": str(fields["active_baseline_id"]),
                "task_owned_baseline_files": list(fields.get("task_owned_baseline_files") or []),
                "external_frozen_baseline_files": list(fields.get("external_frozen_baseline_files") or []),
                "batch_delta_files": [],
                "unexpected_runtime_dirty_files": [],
                "external_delta_adoption": {
                    **adoption,
                    "batch_id": batch_id,
                    "previous_baseline_id": previous_baseline_id,
                },
            }
            completed_at = _now()
            result = {
                "fingerprint": fingerprint,
                "batch_id": batch_id,
                "generation": int(expected_generation),
                "baseline_id": desired["active_baseline_id"],
                "previous_baseline_id": previous_baseline_id,
                "adoption_fingerprint": str(adoption["adoption_fingerprint"]),
                "repository_count": len(adoption.get("repository_heads") or {}),
                "overlay_count": len(adoption.get("dirty_overlay") or []),
                "worker_route": expected_route,
                "completed_at": completed_at,
            }
            queue.update(desired)
            actions[action_key] = result
            queue.setdefault("history", []).append({
                "at": completed_at,
                "event": "USER_ACCEPTED_EXTERNAL_DELTA_ADOPTED",
                "batch_id": batch_id,
                "baseline_id": desired["active_baseline_id"],
                "previous_baseline_id": previous_baseline_id,
                "request_ref": _identifier_digest(action_id)[:16],
            })
            self._write_queue(queue)
            return result, False

    def read_only_recovery_status(self) -> dict[str, Any]:
        """Report states that an Inspector observes but must never repair."""

        with self._mutex.hold():
            queue = self._read_queue()
            preparing = 0
            preparing_replacements = 0
            invalid_context = 0
            legacy_identifiers = 0

            batches = queue.get("batches", {})
            if not isinstance(batches, dict):
                raise QueueError("QUEUE_STATE_INVALID")
            for batch_key, batch in batches.items():
                if not _HASHED_IDENTIFIER_RE.fullmatch(str(batch_key)):
                    legacy_identifiers += 1
                if not isinstance(batch, dict):
                    raise QueueError("QUEUE_STATE_INVALID")
                if batch.get("state") == "PREPARING":
                    preparing += 1
                    for prepared in batch.get("prepared_jobs") or []:
                        if not isinstance(prepared, dict):
                            invalid_context += 1
                            continue
                        try:
                            validate_execution_context(prepared.get("execution_context"))
                        except JobContractError:
                            invalid_context += 1

            replacement_actions = queue.get("anchored_replacement_actions", {})
            if not isinstance(replacement_actions, dict):
                raise QueueError("QUEUE_STATE_INVALID")
            for action in replacement_actions.values():
                if not isinstance(action, dict):
                    raise QueueError("QUEUE_STATE_INVALID")
                if action.get("state") == "PREPARING":
                    preparing_replacements += 1
                    prepared = action.get("prepared_job")
                    if not isinstance(prepared, dict):
                        invalid_context += 1
                    else:
                        try:
                            validate_execution_context(prepared.get("execution_context"))
                        except JobContractError:
                            invalid_context += 1

            for job_id in queue.get("order", []):
                job = self._read_job(str(job_id))
                try:
                    validate_execution_context(job.get("execution_context"))
                except JobContractError:
                    invalid_context += 1
                for field in _ACTION_MAP_FIELDS:
                    actions = job.get(field, {})
                    if not isinstance(actions, dict):
                        raise QueueError("JOB_STATE_INVALID", str(job_id))
                    legacy_identifiers += sum(
                        1
                        for key in actions
                        if not _HASHED_IDENTIFIER_RE.fullmatch(str(key))
                    )

            reasons: list[str] = []
            if preparing:
                reasons.append("PREPARING_BATCH_RECOVERY_REQUIRED")
            if preparing_replacements:
                reasons.append("PREPARING_REPLACEMENT_RECOVERY_REQUIRED")
            if invalid_context:
                reasons.append("LEGACY_EXECUTION_CONTEXT_RECOVERY_REQUIRED")
            if legacy_identifiers:
                reasons.append("LEGACY_IDENTIFIER_MIGRATION_REQUIRED")
            return {
                "required": bool(reasons),
                "code": "READ_ONLY_RECOVERY_REQUIRED" if reasons else "",
                "reasons": reasons,
                "preparing_batches": preparing,
                "preparing_replacements": preparing_replacements,
                "invalid_execution_contexts": invalid_context,
                "legacy_identifiers": legacy_identifiers,
            }

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._mutex.hold():
            return self._read_job(job_id)

    def list_jobs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise QueueError("INVALID_LIMIT")
        with self._mutex.hold():
            queue = self._read_queue()
            selected = queue["order"][-limit:]
            return [self._read_job(job_id) for job_id in selected]

    def inspect_orphaned_running(
        self,
        task_root: str | Path,
        *,
        execution_owner_active: bool,
        worker_process_active: bool,
        pid_is_active: Callable[..., bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Read-only detection; it never repairs, retries, rolls back, or claims."""
        if execution_owner_active:
            return []
        root = Path(task_root)
        findings: list[dict[str, Any]] = []
        with self._mutex.hold():
            queue = self._read_queue()
            for job_id in self._active_job_ids(queue):
                job = self._read_job(job_id)
                if job.get("status") != RUNNING:
                    continue
                task_id = str(job.get("current_claim_task_id", ""))
                task_status = ""
                task_record: dict[str, Any] = {}
                if task_id.startswith("TASK-") and len(task_id) >= 13:
                    task_path = root / task_id[5:13] / f"{task_id}.json"
                    try:
                        task_record = dict(json.loads(task_path.read_text(encoding="utf-8")))
                        task_status = str(task_record.get("status", ""))
                    except (OSError, json.JSONDecodeError):
                        task_status = "UNKNOWN"
                current_attempt = next(
                    (
                        item for item in reversed(list(job.get("attempts") or []))
                        if isinstance(item, dict) and item.get("task_id") == task_id
                    ),
                    {},
                )
                current_terminal_result = bool(current_attempt.get("finished_at")) or bool(
                    dict(current_attempt.get("result") or {})
                )
                if task_status == "RUNNING" and not current_terminal_result:
                    runtime = dict(task_record.get("execution_runtime") or {})
                    if runtime.get("execution_id"):
                        lease_text = str(runtime.get("lease_expires_at", ""))
                        try:
                            lease_expired = datetime.fromisoformat(lease_text) < datetime.now().astimezone()
                        except (TypeError, ValueError):
                            lease_expired = False
                        pid = int(runtime.get("pid", 0) or 0)
                        if callable(pid_is_active) and pid:
                            try:
                                exact_process_active = bool(
                                    pid_is_active(pid, str(runtime.get("started_at", "")))
                                )
                            except TypeError:
                                # Backward-compatible path for existing probes and
                                # deterministic one-argument test doubles.
                                exact_process_active = bool(pid_is_active(pid))
                        else:
                            exact_process_active = bool(worker_process_active)
                        # A valid lease or still-live exact subprocess cannot be
                        # auto-reconciled after owner loss.  Once both are gone,
                        # the existing INTERRUPTED/PRESERVED reconciliation may
                        # be used, but a cumulative Batch first needs durable
                        # snapshot/baseline evidence.  This keeps restart repair
                        # scoped to the current Job and preserves predecessor
                        # deltas instead of guessing from a stale RUNNING flag.
                        if not lease_expired or exact_process_active:
                            findings.append({
                                "code": (
                                    "RUNNING_EXECUTION_LEASE_VALID_OWNER_UNAVAILABLE"
                                    if not lease_expired
                                    else "ORPHANED_RUNNING_EXECUTION"
                                ),
                                "job_id": job_id,
                                "task_id": task_id,
                                "task_status": task_status,
                                "execution_id": str(runtime.get("execution_id", "")),
                                "execution_role": str(runtime.get("execution_role", "")),
                                "pid": pid,
                                "lease_expires_at": lease_text,
                                "process_active": exact_process_active,
                                "worktree_may_be_preserved": True,
                                "operator_action_required": True,
                                "auto_claim": False,
                                "auto_retry": False,
                                "auto_rollback": False,
                                "successor_release": False,
                            })
                        elif queue.get("commit_policy") == BATCH_FINAL_COMMIT:
                            manifest = str(job.get("pre_job_snapshot_manifest", ""))
                            evidence_ok = all((
                                bool(job.get("pre_job_snapshot_id")),
                                bool(manifest),
                                Path(manifest).is_file(),
                                str(job.get("active_baseline_id", ""))
                                == str(queue.get("active_baseline_id", "")),
                                bool(job.get("external_frozen_integrity")),
                                bool(job.get("baseline_declaration_integrity")),
                            ))
                            if not evidence_ok:
                                findings.append({
                                    "code": "STALE_RUNNING_RECOVERY_EVIDENCE_INCOMPLETE",
                                    "job_id": job_id,
                                    "task_id": task_id,
                                    "execution_id": str(runtime.get("execution_id", "")),
                                    "pid": pid,
                                    "lease_expires_at": lease_text,
                                    "process_active": False,
                                    "active_baseline_id": str(
                                        queue.get("active_baseline_id", "")
                                    ),
                                    "snapshot_id": str(
                                        job.get("pre_job_snapshot_id", "")
                                    ),
                                    "snapshot_manifest": manifest,
                                    "worktree_may_be_preserved": True,
                                    "operator_action_required": True,
                                    "auto_claim": False,
                                    "auto_retry": False,
                                    "auto_rollback": False,
                                    "successor_release": False,
                                })
                            else:
                                salvage = dict(job.get("crash_salvage") or {})
                                salvage_manifest = Path(str(salvage.get("manifest_path", "")))
                                salvage_valid = all((
                                    salvage.get("status") in {"SALVAGE_CANDIDATE", "NO_TASK_DELTA"},
                                    salvage.get("integrity_status") == "RESTORE_ELIGIBLE",
                                    salvage.get("verified") is False,
                                    str(salvage.get("task_id", "")) == task_id,
                                    str(salvage.get("execution_id", "")) == str(runtime.get("execution_id", "")),
                                    str(salvage.get("active_baseline_id", "")) == str(queue.get("active_baseline_id", "")),
                                    salvage_manifest.is_file(),
                                ))
                                if salvage_valid:
                                    try:
                                        salvage_valid = (
                                            hashlib.sha256(salvage_manifest.read_bytes()).hexdigest()
                                            == str(salvage.get("manifest_sha256", ""))
                                        )
                                    except OSError:
                                        salvage_valid = False
                                if not salvage_valid:
                                    findings.append({
                                        "code": "STALE_RUNNING_SALVAGE_REQUIRED",
                                        "job_id": job_id,
                                        "job_revision": int(job.get("revision", -1)),
                                        "task_id": task_id,
                                        "execution_id": str(runtime.get("execution_id", "")),
                                        "pid": pid,
                                        "lease_expires_at": lease_text,
                                        "process_active": False,
                                        "active_baseline_id": str(queue.get("active_baseline_id", "")),
                                        "snapshot_id": str(job.get("pre_job_snapshot_id", "")),
                                        "snapshot_manifest": manifest,
                                        "worktree_may_be_preserved": True,
                                        "salvage_capture_required": True,
                                        "operator_action_required": False,
                                        "auto_claim": False,
                                        "auto_retry": False,
                                        "auto_rollback": False,
                                        "successor_release": False,
                                    })
                        continue
                    if worker_process_active:
                        continue
                    findings.append({
                        "code": "ORPHANED_RUNNING_EXECUTION",
                        "job_id": job_id,
                        "task_id": task_id,
                        "task_status": task_status,
                        "worktree_may_be_preserved": True,
                        "operator_action_required": True,
                        "auto_claim": False,
                        "auto_retry": False,
                        "auto_rollback": False,
                        "successor_release": False,
                    })
        return findings

    def validate_batch_manifest(self, batch_id: str) -> dict[str, Any]:
        with self._mutex.hold():
            queue = self._read_queue()
            batch = next(
                (
                    item for item in queue.get("batches", {}).values()
                    if isinstance(item, dict) and item.get("batch_id") == batch_id
                ),
                None,
            )
            if batch is None:
                raise QueueError("BATCH_NOT_FOUND", batch_id)
            jobs = [self._read_job(job_id) for job_id in batch.get("job_ids", [])]
            if self.repository is not None:
                from control_repository import digest
                validated = []
                for job in jobs:
                    if int(job.get("contract_revision", 1)) > 1:
                        revisions = job.get("contract_revisions") or []
                        if not revisions or revisions[-1].get("request_sha256") != digest(job["request"]):
                            raise QueueError("PLANNED_CONTRACT_INTEGRITY_FAILED")
                        validated.append({**job, "request": job["original_request"]})
                    else:
                        validated.append(job)
                jobs = validated
            try:
                result = validate_provenance(dict(batch.get("provenance") or {}), jobs)
            except BatchManifestError as exc:
                raise QueueError(exc.code, exc.detail) from exc
            return {"batch_id": batch_id, **result}

    def supersede_batch_for_external_handoff(
        self,
        batch_id: str,
        *,
        expected_generation: int,
        expected_queue_revision: int,
        expected_job_revisions: Mapping[str, int],
        external_handoff_ref: str,
        request_id: str,
        confirmation: str,
        execution_idle: bool,
    ) -> tuple[dict[str, Any], bool]:
        """End one legacy chain without manufacturing success or touching source."""
        self._assert_writable()
        if confirmation != EXTERNAL_HANDOFF_CONFIRMATION:
            raise QueueError("CONFIRMATION_REQUIRED")
        if not execution_idle:
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = self.action_key(action_id)
        handoff_ref = scrub_secrets(str(external_handoff_ref).strip())[:500]
        if not handoff_ref:
            raise QueueError("EXTERNAL_HANDOFF_REF_REQUIRED")
        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("external_handoff_actions", {})
            prior = actions.get(action_key)
            if prior is not None:
                return dict(prior), True
            if not queue.get("operator_paused"):
                raise QueueError("EXTERNAL_HANDOFF_REQUIRES_OPERATOR_PAUSE")
            if int(queue.get("generation", -1)) != int(expected_generation):
                raise QueueError("QUEUE_GENERATION_CONFLICT")
            if int(queue.get("revision", -1)) != int(expected_queue_revision):
                raise QueueError("QUEUE_REVISION_CONFLICT")
            target_ids = [
                job_id for job_id in self._active_job_ids(queue)
                if self._read_job(job_id).get("batch_id") == batch_id
            ]
            if not target_ids or set(target_ids) != set(expected_job_revisions):
                raise QueueError("EXTERNAL_HANDOFF_JOB_SET_MISMATCH")
            jobs = [self._read_job(job_id) for job_id in target_ids]
            if any(int(job.get("revision", -1)) != int(expected_job_revisions[job["job_id"]]) for job in jobs):
                raise QueueError("JOB_REVISION_CONFLICT")
            if queue.get("unexpected_runtime_dirty_files"):
                raise QueueError("UNEXPECTED_RUNTIME_DIRTY_FILES")
            if any(
                job.get("status") not in {RUNNING, QUEUED, AWAITING_QA, CANCELLED}
                for job in jobs
            ):
                raise QueueError("EXTERNAL_HANDOFF_STATUS_INVALID")

            terminal: list[dict[str, Any]] = []
            superseded_at = _now()
            for job in jobs:
                previous = str(job.get("status", ""))
                previous_result = dict(job.get("last_result") or {})
                supersede = {
                    "supersede_reason": "USER_SUPERSEDED_FOR_NEW_BATCH",
                    "superseded_at": superseded_at,
                    "superseded_from_status": previous,
                    "request_ref": _identifier_digest(action_id)[:16],
                    "expected_generation": int(expected_generation),
                    "expected_queue_revision": int(expected_queue_revision),
                    "actor": "local_operator",
                    "worktree_disposition": str(
                        previous_result.get("worktree_disposition")
                        or ("PRESERVED" if previous in {RUNNING, AWAITING_QA} else "NO_DELTA")
                    ),
                    "candidate_refs": {
                        "candidate_id": str(job.get("candidate_id", "")),
                        "candidate_manifest": str(job.get("candidate_manifest", "")),
                        "speculative_candidate_ids": list(
                            job.get("speculative_candidate_ids") or []
                        ),
                        "pre_job_snapshot_id": str(
                            job.get("pre_job_snapshot_id", "")
                        ),
                    },
                }
                if previous == CANCELLED:
                    terminal.append({
                        "job_id": job["job_id"],
                        "status": CANCELLED,
                        "superseded_from_status": CANCELLED,
                        "rewritten": False,
                    })
                    continue
                if previous == RUNNING:
                    result = {
                        "status": INTERRUPTED,
                        "success": False,
                        "failure_code": "EXTERNAL_HANDOFF_SUPERSEDED",
                        "verification_status": "PARTIAL",
                        "build_status": "NOT_RUN",
                        "review_status": "NOT_RUN",
                        "worktree_disposition": "PRESERVED",
                        "external_handoff_ref": handoff_ref,
                    }
                    job["status"] = INTERRUPTED
                    job["last_result"] = result
                    self._terminalize_current_attempt(job, result)
                elif previous == QUEUED:
                    job["status"] = CANCELLED
                    job["last_result"] = {
                        "status": CANCELLED,
                        "success": False,
                        "failure_code": "EXTERNAL_HANDOFF_SUPERSEDED",
                        "worktree_disposition": "NO_DELTA",
                        "external_handoff_ref": handoff_ref,
                    }
                else:
                    # Preserve the historical Task result byte-for-byte.  The
                    # Job status, not rewritten Task evidence, makes this chain
                    # a non-success terminal lineage.
                    job["status"] = SKIPPED
                job["owner_id"] = ""
                job["supersede"] = supersede
                self._event(
                    job,
                    "EXTERNAL_HANDOFF_SUPERSEDED",
                    previous_status=previous,
                    external_handoff_ref=handoff_ref,
                    supersede_reason="USER_SUPERSEDED_FOR_NEW_BATCH",
                    request_ref=_identifier_digest(action_id)[:16],
                )
                self._write_job(job)
                terminal.append({
                    "job_id": job["job_id"],
                    "status": job["status"],
                    "superseded_from_status": previous,
                    "rewritten": True,
                })
            last_index = max(queue["order"].index(job_id) for job_id in target_ids)
            queue["active_from_index"] = last_index + 1
            queue["running_job_id"] = ""
            queue["blocked_by_job_id"] = ""
            queue["gate_reason"] = ""
            queue["operator_paused"] = True
            queue["operator_pause_reason"] = "NEW_BATCH_READY"
            self._refresh_pause(queue)
            result = {
                "batch_id": batch_id,
                "generation": expected_generation,
                "external_handoff_ref": handoff_ref,
                "supersede_reason": "USER_SUPERSEDED_FOR_NEW_BATCH",
                "expected_queue_revision": int(expected_queue_revision),
                "request_ref": _identifier_digest(action_id)[:16],
                "strict_success": False,
                "jobs": terminal,
                "completed_at": _now(),
            }
            for batch in queue.get("batches", {}).values():
                if isinstance(batch, dict) and batch.get("batch_id") == batch_id:
                    batch["closure"] = dict(result)
                    break
            actions[action_key] = result
            queue.setdefault("history", []).append({
                "at": _now(),
                "event": "BATCH_SUPERSEDED_FOR_EXTERNAL_HANDOFF",
                "batch_id": batch_id,
                "external_handoff_ref": handoff_ref,
                "supersede_reason": "USER_SUPERSEDED_FOR_NEW_BATCH",
                "expected_generation": int(expected_generation),
                "expected_queue_revision": int(expected_queue_revision),
                "request_ref": _identifier_digest(action_id)[:16],
            })
            self._write_queue(queue)
            return result, False

    def claim_next(self, owner_id: str, *, workspace=None, integrity_snapshot_provider=None) -> dict[str, Any] | None:
        self._assert_writable()
        owner = validate_idempotency_key(owner_id, "owner_id")
        with self._mutex.hold():
            if self.repository is not None and self.repository.global_stop().get("active"):
                return None
            queue = self._read_queue()
            bypass = False
            if self.repository is not None and queue.get("blocked_by_job_id"):
                from sleep_disposition import may_bypass_gate
                bypass = may_bypass_gate(queue, self._read_job(queue["blocked_by_job_id"]))
            if (queue.get("paused") and not bypass) or queue.get("running_job_id"):
                return None
            for job_id in self._active_job_ids(queue):
                job = self._read_job(job_id)
                if job.get("status") != QUEUED:
                    continue
                if bypass and job.get("dependency_semantics") != "EXPLICIT":
                    continue
                if job.get("dependency_semantics") == "EXPLICIT":
                    conflict = self._quarantined_scope_conflict(queue, job,
                        workspace=workspace, integrity_snapshot_provider=integrity_snapshot_provider)
                    if conflict:
                        continue
                    dependency = self._explicit_dependency_decision(job)
                    if dependency.get("global_gate"):
                        self._set_gate(
                            queue,
                            str(dependency["global_gate"]),
                            str(dependency.get("reason", AWAITING_QA)),
                        )
                        self._write_queue(queue)
                        return None
                    if dependency.get("terminal_blockers"):
                        root_id = str(dependency["terminal_blockers"][0])
                        root = self._read_job(root_id)
                        job["blocked_by_job_id"] = str(root.get("blocked_by_job_id") or root_id)
                        job["root_failure_code"] = str(root.get("root_failure_code") or dict(root.get("last_result") or {}).get("failure_code", ""))
                        job["user_action_required"] = False
                        job["status"] = BLOCKED_BY_DEPENDENCY
                        job["last_result"] = {
                            "status": BLOCKED_BY_DEPENDENCY,
                            "success": False,
                            "failure_code": "DEPENDENCY_TERMINAL_NON_SUCCESS",
                            "blocked_by": list(dependency["terminal_blockers"]),
                            "worktree_disposition": "NO_DELTA",
                        }
                        self._event(
                            job,
                            "BLOCKED_BY_DEPENDENCY",
                            blocked_by=list(dependency["terminal_blockers"]),
                        )
                        self._write_job(job)
                        continue
                    if not dependency.get("eligible"):
                        continue
                    job["speculative_candidate_ids"] = list(
                        dependency.get("speculative_candidate_ids") or []
                    )
                else:
                    predecessor_gate = self._failed_predecessor_gate(queue, job)
                    if predecessor_gate is not None:
                        predecessor_id, reason = predecessor_gate
                        self._set_gate(queue, predecessor_id, reason)
                        self._write_queue(queue)
                        return None
                attempt_kind = str(job.get("pending_attempt_kind") or NORMAL)
                provenance = dict(job.get("pending_attempt_provenance") or {})
                try:
                    reservation, replayed = reserve_execution_attempt(
                        job,
                        queue,
                        attempt_kind=attempt_kind,
                        provenance=provenance,
                    )
                except AttemptAccountingError as exc:
                    self._set_gate(queue, job_id, exc.code)
                    self._write_queue(queue)
                    raise QueueError(exc.code, job_id) from exc
                if replayed and reservation.get("consumed_task_id"):
                    raise QueueError("ATTEMPT_RESERVATION_ALREADY_CONSUMED", job_id)
                job["status"] = RUNNING
                job["owner_id"] = owner
                job["current_claim_task_started"] = False
                job["current_claim_task_id"] = ""
                job["pending_attempt_kind"] = ""
                job["pending_attempt_provenance"] = {}
                self._event(
                    job,
                    "CLAIMED",
                    owner_id=owner,
                    reservation_id=reservation["reservation_id"],
                    attempt_kind=reservation["attempt_kind"],
                    execution_attempt=reservation["execution_attempt"],
                )
                self._write_job(job)
                queue["running_job_id"] = job_id
                self._write_queue(queue)
                return job
            return None

    def clear_stale_terminal_predecessor_gate_for_batch_head(self) -> bool:
        """Start a new logical Batch segment after a safely terminal blocker."""
        self._assert_writable()
        with self._mutex.hold():
            queue = self._read_queue()
            if (
                queue.get("gate_reason")
                not in {PREDECESSOR_SUCCESS_GATE_FAILED, FAILED_FINAL}
                or queue.get("running_job_id")
                or queue.get("operator_paused")
            ):
                return False
            blocker_id = str(queue.get("blocked_by_job_id", ""))
            if not blocker_id:
                return False
            blocker = self._read_job(blocker_id)
            safe_terminal = blocker.get("status") in {SKIPPED, CANCELLED} or (
                blocker.get("status") == FAILED_FINAL
                and dict(blocker.get("last_result") or {}).get("worktree_disposition")
                in {"CLEAN_ROLLBACK", "CHECKPOINTED_CLEAN_ROLLBACK", "NO_DELTA"}
            )
            if not safe_terminal:
                return False
            candidate = next(
                (
                    self._read_job(job_id)
                    for job_id in self._active_job_ids(queue)
                    if self._read_job(job_id).get("status") == QUEUED
                ),
                None,
            )
            if (
                candidate is None
                or int(candidate.get("sequence", 0)) != 1
                or candidate.get("batch_id") == blocker.get("batch_id")
                or self._failed_predecessor_gate(queue, candidate) is not None
            ):
                return False
            candidate_index = list(queue.get("order") or []).index(
                candidate["job_id"]
            )
            # The old Jobs remain immutable history and the cumulative baseline
            # remains active. Only the logical active segment advances to the
            # new Batch head, so later gate restoration cannot resurrect the
            # safely rolled-back FAILED_FINAL blocker.
            queue["active_from_index"] = candidate_index
            self._clear_gate(queue, blocker_id)
            queue["last_stale_gate_clear"] = {
                "blocked_job_id": blocker_id,
                "batch_head_job_id": candidate.get("job_id", ""),
                "active_from_index": candidate_index,
                "at": _now(),
            }
            self._write_queue(queue)
            return True

    def mark_task_started(self, job_id: str, owner_id: str, task_id: str) -> dict[str, Any]:
        self._assert_writable()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if (
                queue.get("running_job_id") != job_id
                or job.get("status") != RUNNING
                or job.get("owner_id") != owner_id
            ):
                raise QueueError("JOB_NOT_OWNED", job_id)
            if job.get("current_claim_task_started"):
                if str(job.get("current_claim_task_id", "")) == task_id:
                    return job
                raise QueueError("ATTEMPT_RESERVATION_ALREADY_CONSUMED", job_id)
            try:
                reservation = consume_reservation(job, task_id)
            except AttemptAccountingError as exc:
                raise QueueError(exc.code, job_id) from exc
            job["outer_attempt"] = int(reservation["execution_attempt"])
            job["technical_recovery_attempt_count"] = int(
                reservation["technical_recovery_attempt_no"]
            )
            job["current_claim_task_started"] = True
            job["current_claim_task_id"] = task_id
            job["task_ids"].append(task_id)
            job["attempts"].append({
                "outer_attempt": job["outer_attempt"],
                "execution_attempt": reservation["execution_attempt"],
                "attempt_kind": reservation["attempt_kind"],
                "normal_attempt_no": reservation["normal_attempt_no"],
                "normal_attempt_budget": reservation["normal_attempt_budget"],
                "technical_recovery_attempt_no": reservation["technical_recovery_attempt_no"],
                "technical_recovery_budget": reservation["technical_recovery_budget"],
                "effective_execution_limit": reservation["effective_execution_limit"],
                "reservation_id": reservation["reservation_id"],
                "retry_provenance": dict(reservation.get("provenance") or {}),
                "task_id": task_id,
                "started_at": _now(),
                "finished_at": "",
                "result": {},
            })
            self._event(
                job,
                "TASK_STARTED",
                outer_attempt=job["outer_attempt"],
                attempt_kind=reservation["attempt_kind"],
                reservation_id=reservation["reservation_id"],
                task_id=task_id,
            )
            self._write_job(job)
            return job

    def record_pre_job_snapshot(
        self,
        job_id: str,
        owner_id: str,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        self._assert_writable()
        snapshot_id = str(snapshot.get("snapshot_id", ""))
        manifest = str(snapshot.get("manifest_path", ""))
        if not snapshot_id or not manifest:
            raise QueueError("PRE_JOB_SNAPSHOT_INVALID")
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if (
                queue.get("running_job_id") != job_id
                or job.get("status") != RUNNING
                or job.get("owner_id") != owner_id
            ):
                raise QueueError("JOB_NOT_OWNED", job_id)
            history = job.setdefault("pre_job_snapshots", [])
            prior = next(
                (item for item in history if item.get("snapshot_id") == snapshot_id),
                None,
            )
            record = {
                "snapshot_id": snapshot_id,
                "manifest_path": manifest,
                "outer_attempt": int(
                    dict(job.get("active_attempt_reservation") or {}).get(
                        "execution_attempt", int(job.get("outer_attempt", 0)) + 1
                    )
                ),
                "recorded_at": _now(),
                "inherited_batch_delta_files": list(
                    snapshot.get("inherited_batch_delta_files") or []
                ),
                "unexpected_external_dirty_files": list(
                    snapshot.get("unexpected_external_dirty_files") or []
                ),
                "active_baseline_id": str(snapshot.get("active_baseline_id", "")),
                "external_frozen_integrity": bool(snapshot.get("external_frozen_integrity", False)),
                "baseline_declaration_integrity": bool(snapshot.get("baseline_declaration_integrity", False)),
            }
            if prior is not None:
                if prior.get("manifest_path") != manifest:
                    raise QueueError("PRE_JOB_SNAPSHOT_CONFLICT")
            else:
                history.append(record)
            job["pre_job_snapshot_id"] = snapshot_id
            job["pre_job_snapshot_manifest"] = manifest
            job["inherited_batch_delta_files"] = list(
                snapshot.get("inherited_batch_delta_files") or []
            )
            job["unexpected_external_dirty_files"] = list(
                snapshot.get("unexpected_external_dirty_files") or []
            )
            job["active_baseline_id"] = str(snapshot.get("active_baseline_id", ""))
            job["external_frozen_integrity"] = bool(snapshot.get("external_frozen_integrity", False))
            job["baseline_declaration_integrity"] = bool(snapshot.get("baseline_declaration_integrity", False))
            self._event(job, "PRE_JOB_SNAPSHOT_RECORDED", snapshot_id=snapshot_id)
            self._write_job(job)
            return job

    def record_crash_salvage(
        self,
        job_id: str,
        *,
        expected_revision: int,
        expected_task_id: str,
        expected_execution_id: str,
        salvage: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Attach one immutable crash artifact to the still-RUNNING lineage."""
        self._assert_writable()
        record = _safe_json(dict(salvage))
        manifest = Path(str(record.get("manifest_path", ""))).resolve()
        expected_sha = str(record.get("manifest_sha256", ""))
        try:
            manifest.relative_to(self.root.resolve())
            actual_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
        except (ValueError, OSError) as exc:
            raise QueueError("CRASH_SALVAGE_MANIFEST_INVALID", job_id) from exc
        if actual_sha != expected_sha:
            raise QueueError("CRASH_SALVAGE_MANIFEST_HASH_MISMATCH", job_id)
        if (
            record.get("status") not in {"SALVAGE_CANDIDATE", "NO_TASK_DELTA"}
            or record.get("integrity_status") != "RESTORE_ELIGIBLE"
            or record.get("verified") is not False
            or not str(record.get("recovery_handoff", ""))
        ):
            raise QueueError("CRASH_SALVAGE_RECORD_INVALID", job_id)
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if int(job.get("revision", -1)) != int(expected_revision):
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            if (
                queue.get("running_job_id") != job_id
                or job.get("status") != RUNNING
                or str(job.get("current_claim_task_id", "")) != expected_task_id
                or str(record.get("task_id", "")) != expected_task_id
                or str(record.get("execution_id", "")) != expected_execution_id
                or str(record.get("active_baseline_id", ""))
                != str(queue.get("active_baseline_id", ""))
            ):
                raise QueueError("CRASH_SALVAGE_LINEAGE_CONFLICT", job_id)
            prior = dict(job.get("crash_salvage") or {})
            if prior:
                if (
                    prior.get("manifest_sha256") == expected_sha
                    and prior.get("execution_id") == expected_execution_id
                ):
                    return job
                raise QueueError("CRASH_SALVAGE_CONFLICT", job_id)
            job["crash_salvage"] = record
            job["crash_recovery_handoff"] = str(record.get("recovery_handoff", ""))
            self._event(
                job,
                "CRASH_SALVAGE_CAPTURED",
                task_id=expected_task_id,
                execution_id=expected_execution_id,
                checkpoint_id=str(record.get("checkpoint_id", "")),
                manifest_path=str(record.get("manifest_path", "")),
                manifest_sha256=expected_sha,
                task_owned_changed_files=list(record.get("task_owned_changed_files") or []),
            )
            self._write_job(job)
            return job

    def record_result(self, job_id: str, owner_id: str, result: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_writable()
        safe_result = _safe_json(dict(result))
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if (
                queue.get("running_job_id") != job_id
                or job.get("status") != RUNNING
                or job.get("owner_id") != owner_id
            ):
                raise QueueError("JOB_NOT_OWNED", job_id)
            task_status = safe_result.get("status", "FAILED")
            batch_final = queue.get("commit_policy") == BATCH_FINAL_COMMIT
            verified_success = (
                safe_result.get("success") is True
                and task_status == "SUCCESS"
                and safe_result.get("stage") == "DONE"
                and safe_result.get("verification_status") == "VERIFIED"
            )
            batch_gate_pass = self._is_strict_batch_result(safe_result)
            from skeleton_policy import valid_result
            skeleton_ready = valid_result(safe_result)
            if skeleton_ready and self.repository is not None:
                execution = self.repository.active_execution(job_id)
                if (not execution or safe_result.get('materialized_execution') != execution
                        or execution.get('current_requirement') != job.get('current_requirement')):
                    raise QueueError('SKELETON_EXECUTION_BINDING_INVALID', job_id)
            if skeleton_ready:
                status = SKELETON_READY
                safe_result['strict_success'] = False
                safe_result['commit_eligible'] = False
                safe_result['deferred_contracts'] = safe_result['qa_request']['deferred_contracts']
            elif verified_success and (not batch_final or batch_gate_pass):
                status = SUCCEEDED
            elif verified_success and batch_final:
                status = AWAITING_QA
                safe_result["failure_code"] = (
                    "CUMULATIVE_VERIFICATION_CONTRACT_INVALID"
                )
                safe_result["queue_decision_code"] = (
                    "CUMULATIVE_VERIFICATION_CONTRACT_INVALID"
                )
            elif task_status in {"AWAITING_QA", AWAITING_DEPENDENCY_QA}:
                status = task_status
            else:
                from retry_domains import failure_disposition
                status, decision_code = failure_disposition(
                    safe_result,
                    outer_attempt=int(job.get("outer_attempt", 0)),
                    max_outer_attempts=int(job.get("max_outer_attempts", 1)),
                )
                if decision_code:
                    safe_result["queue_decision_code"] = decision_code

            if self.repository is not None:
                from decision_qa import decision_requests
                for request in decision_requests(safe_result):
                    self.repository.record_decision(
                        job_id,
                        contract_revision=int(job.get("contract_revision", 1)),
                        decision_type=request.pop("decision_type"),
                        question=request.pop("question"),
                        fingerprint=request.pop("fingerprint"),
                        payload=request,
                    )
                from product_readiness import derive_product_readiness, false_complete_guard
                readiness = derive_product_readiness(
                    status, self.repository.open_decisions(job_id)
                )
                false_complete_guard(status, readiness)
                safe_result.update(readiness)
            else:
                from product_readiness import derive_product_readiness
                safe_result.update(derive_product_readiness(status, ()))
            job["status"] = status
            for field in (
                "qa_type", "hold_scope", "machine_verified",
                "candidate_id", "candidate_manifest", "speculative_ready",
            ):
                if field in safe_result:
                    job[field] = safe_result[field]
            job["owner_id"] = ""
            job["last_claim_task_started"] = bool(
                job.get("current_claim_task_started")
            )
            job["last_claim_task_id"] = str(
                job.get("current_claim_task_id", "")
            )
            job["last_result"] = safe_result
            self._terminalize_current_attempt(job, safe_result)
            if status == SKELETON_READY:
                # Cumulative development input only. Success counts and final
                # commit/baseline eligibility still require strict SUCCEEDED.
                queue['batch_delta_files'] = sorted(set(queue.get('batch_delta_files') or [])
                                                    | set(safe_result.get('changed_files') or []))
                job['deferred_contracts'] = list(safe_result['deferred_contracts'])
            if status == SUCCEEDED and batch_final:
                safe_result["commit_policy"] = BATCH_FINAL_COMMIT
                safe_result["commit_status"] = "DEFERRED_TO_BATCH_FINAL"
                safe_result["auto_continued_without_commit"] = False
                safe_result["job_validation_status"] = "PASS"
                safe_result["job_delta_preservation_status"] = (
                    "PRESERVED" if safe_result.get("changed_files") else "NO_DELTA"
                )
                safe_result["job_commit_status"] = "DEFERRED_TO_BATCH_FINAL"
                safe_result["batch_final_staging_eligible"] = False
                safe_result["batch_final_commit_eligible"] = False
                safe_result["baseline_advance_eligible"] = False
                job["last_result"] = safe_result
                delta = {
                    str(path)
                    for path in queue.get("batch_delta_files") or []
                    if path
                }
                delta.update(
                    str(path)
                    for path in safe_result.get("changed_files") or []
                    if path
                )
                queue["batch_delta_files"] = sorted(delta)

            if status == SUCCEEDED and self._should_auto_ack_success(queue, job):
                auto_key = "sha256:" + hashlib.sha256(
                    f"batch-auto-ack:{job_id}".encode("utf-8")
                ).hexdigest()
                job.setdefault("success_acknowledgements", {})[auto_key] = {
                    "acknowledged_at": _now(),
                    "automatic": True,
                    "commit_policy": BATCH_FINAL_COMMIT,
                }
                safe_result["auto_continued_without_commit"] = True
                job["last_result"] = safe_result
                self._event(job, "SUCCESS_AUTO_ACKNOWLEDGED_WITHOUT_COMMIT")
            self._event(
                job,
                "TASK_FINISHED",
                status=status,
                task_id=safe_result.get("task_id", ""),
                failure_code=safe_result.get("failure_code", ""),
            )
            self._write_job(job)

            handoff = dict(queue.get("corrective_handoff") or {})
            if handoff.get("corrective_job_id") == job_id:
                handoff["state"] = (
                    "SUCCEEDED_AWAITING_ACK" if status == SUCCEEDED
                    else "FAILED" if status == FAILED_FINAL
                    else str(status)
                )
                handoff["result_recorded_at"] = _now()
                queue["corrective_handoff"] = handoff

            for qa_id, successor_record in dict(
                queue.get("qa_corrective_successors") or {}
            ).items():
                if not isinstance(successor_record, Mapping):
                    continue
                if str(successor_record.get("corrective_job_id", "")) != job_id:
                    continue
                successor_record = dict(successor_record)
                successor_record["state"] = (
                    "SUCCEEDED_AWAITING_ACK" if status == SUCCEEDED
                    else "FAILED" if status == FAILED_FINAL
                    else str(status)
                )
                successor_record["result_recorded_at"] = _now()
                queue["qa_corrective_successors"][qa_id] = successor_record

            if batch_final:
                self._refresh_batch_lifecycle(queue, str(job.get("batch_id", "")))

            queue["running_job_id"] = ""
            if status == SKELETON_READY or (status == SUCCEEDED and self._should_auto_ack_success(queue, job)):
                self._clear_gate(queue, job_id)
            elif self._qa_non_global(job):
                self._clear_gate(queue, job_id)
            else:
                self._set_gate(
                    queue,
                    job_id,
                    SUCCESS_ACK_REQUIRED if status == SUCCEEDED else status,
                )
            self._write_queue(queue)
            return job

    def reserve_terminal_notification(
        self,
        job_id: str,
        *,
        semantic_revision: int,
        projection: Mapping[str, Any],
    ) -> tuple[str, bool]:
        """Durably reserve one semantic transition before fail-soft delivery."""
        self._assert_writable()
        status = str(projection.get("status", ""))
        seed = f"{job_id}:{status}:{int(semantic_revision)}"
        key = "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()
        with self._mutex.hold():
            job = self._read_job(job_id)
            records = job.setdefault("terminal_notifications", {})
            if key in records:
                return key, False
            records[key] = {
                "semantic_revision": int(semantic_revision),
                "status": status,
                "projection": _safe_json(dict(projection)),
                "delivery_status": "PENDING",
                "reserved_at": _now(),
            }
            self._write_job(job)
            return key, True

    def complete_terminal_notification(
        self, job_id: str, key: str, evidence: Mapping[str, Any]
    ) -> None:
        """Record delivery evidence without changing execution semantics."""
        self._assert_writable()
        with self._mutex.hold():
            job = self._read_job(job_id)
            record = dict(job.setdefault("terminal_notifications", {}).get(key) or {})
            if not record or record.get("delivery_status") != "PENDING":
                return
            record["delivery_status"] = "ATTEMPTED"
            record["evidence"] = _safe_json(dict(evidence))
            record["completed_at"] = _now()
            job["terminal_notifications"][key] = record
            self._write_job(job)

    def record_review_only_result(
        self,
        job_id: str,
        *,
        expected_revision: int,
        request_id: str,
        result: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        """Commit one revision-safe Reviewer-only verdict without a Worker attempt."""
        self._assert_writable()
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int):
            raise QueueError("EXPECTED_REVISION_INVALID")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        safe_result = _safe_json(dict(result))
        fingerprint = hashlib.sha256(
            json.dumps(safe_result, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("review_only_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if (
                    prior.get("expected_revision") != expected_revision
                    or prior.get("result_fingerprint") != fingerprint
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                return job, True
            if int(job.get("revision", -1)) != expected_revision:
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            prior_result = dict(job.get("last_result") or {})
            control_recovery = (
                safe_result.get("recovery_kind") == "CONTROL_STATE_WRITE_RECOVERY"
                and prior_result.get("failure_stage") == "CONTROL"
                and safe_result.get("worker_invoked") is False
            )
            integration_recovery = (
                safe_result.get("recovery_kind") == "WORKER_FREE_INTEGRATION_RECOVERY"
                and prior_result.get("failure_code")
                in {"", "INTEGRATION_TECHNICAL_RECOVERY_REQUIRED"}
                and bool(prior_result.get("commit_blockers"))
                and safe_result.get("worker_invoked") is False
            )
            candidate_recovery = (
                safe_result.get("recovery_kind") == "QA_CANDIDATE_ISOLATION"
                and prior_result.get("task_id") == safe_result.get("task_id")
                and not str(job.get("candidate_id", ""))
                and safe_result.get("worker_invoked") is False
            )
            post_worker_recovery = (
                safe_result.get("recovery_kind") == "POST_WORKER_QA_PARSE_RECOVERY"
                and prior_result.get("failure_code") == "CONTROL_EXECUTION_ERROR"
                and prior_result.get("failure_stage") == "CONTROL"
                and prior_result.get("task_id") == safe_result.get("task_id")
                and safe_result.get("worker_invoked") is False
                and safe_result.get("worker_result_reused") is True
            )
            # 0.9.0.10: one fully verified execution whose only defect was the
            # pre-0.9.0.10 test_status projection may re-record its preserved
            # outcome through the corrected strict batch contract.
            strict_reprojection = (
                safe_result.get("recovery_kind") == "STRICT_RESULT_REPROJECTION"
                and prior_result.get("queue_decision_code")
                == "CUMULATIVE_VERIFICATION_CONTRACT_INVALID"
                and prior_result.get("task_id") == safe_result.get("task_id")
                and safe_result.get("worker_invoked") is False
                and safe_result.get("worker_result_reused") is True
            )
            if (
                job.get("status") != AWAITING_QA
                or not (
                    (
                        prior_result.get("failure_origin") == "TOOL_INFRA"
                        and prior_result.get("failure_stage") == "REVIEW"
                    )
                    or control_recovery
                    or integration_recovery
                    or candidate_recovery
                    or post_worker_recovery
                    or strict_reprojection
                )
                or queue.get("running_job_id")
                or queue.get("blocked_by_job_id") != job_id
            ):
                raise QueueError("REVIEW_ONLY_RETRY_NOT_ELIGIBLE", job_id)

            strict_success = self._is_strict_batch_result(safe_result)
            job["status"] = SUCCEEDED if strict_success else AWAITING_QA
            if candidate_recovery:
                for field in (
                    "qa_type", "hold_scope", "machine_verified",
                    "candidate_id", "candidate_manifest",
                ):
                    job[field] = safe_result.get(field, job.get(field, ""))
            if strict_success and queue.get("commit_policy") == BATCH_FINAL_COMMIT:
                safe_result.update({
                    "commit_policy": BATCH_FINAL_COMMIT,
                    "commit_status": "DEFERRED_TO_BATCH_FINAL",
                    "job_validation_status": "PASS",
                    "job_delta_preservation_status": (
                        "PRESERVED" if safe_result.get("changed_files") else "NO_DELTA"
                    ),
                    "job_commit_status": "DEFERRED_TO_BATCH_FINAL",
                    "batch_final_staging_eligible": False,
                    "batch_final_commit_eligible": False,
                    "baseline_advance_eligible": False,
                })
                delta = {str(path) for path in queue.get("batch_delta_files") or [] if path}
                delta.update(
                    str(path) for path in safe_result.get("changed_files") or [] if path
                )
                queue["batch_delta_files"] = sorted(delta)
            job["last_result"] = safe_result
            actions[action_key] = {
                "expected_revision": expected_revision,
                "result_fingerprint": fingerprint,
                "request_ref": action_ref,
                "finished_at": _now(),
                "worker_invoked": False,
            }
            self._event(
                job,
                (
                    "QA_CANDIDATE_ISOLATED"
                    if candidate_recovery else (
                        "INTEGRATION_RECOVERY_FINISHED"
                        if integration_recovery else "REVIEW_ONLY_RETRY_FINISHED"
                    )
                ),
                request_ref=action_ref,
                status=job["status"],
                review_status=safe_result.get("review_status", ""),
                worker_invoked=False,
            )
            if strict_success and self._should_auto_ack_success(queue, job):
                safe_result["auto_continued_without_commit"] = True
                job["last_result"] = safe_result
                self._clear_gate(queue, job_id)
            elif strict_success:
                self._set_gate(queue, job_id, SUCCESS_ACK_REQUIRED)
            elif candidate_recovery and self._qa_non_global(job):
                self._clear_gate(queue, job_id)
            else:
                self._set_gate(queue, job_id, AWAITING_QA)
            self._write_job(job)
            if queue.get("commit_policy") == BATCH_FINAL_COMMIT:
                self._refresh_batch_lifecycle(queue, str(job.get("batch_id", "")))
            self._write_queue(queue)
            return job, False

    def record_blocked(self, job_id: str, owner_id: str, code: str) -> dict[str, Any]:
        self._assert_writable()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if job.get("status") != RUNNING or job.get("owner_id") != owner_id:
                raise QueueError("JOB_NOT_OWNED", job_id)
            if not job.get("current_claim_task_started"):
                active_id = str(
                    dict(job.get("active_attempt_reservation") or {}).get(
                        "reservation_id", ""
                    )
                )
                for reservation in job.get("attempt_reservations") or []:
                    if (
                        reservation.get("reservation_id") == active_id
                        and not reservation.get("consumed_task_id")
                    ):
                        reservation["cancelled_at"] = _now()
                        reservation["cancellation_reason"] = str(code)[:80]
                job["active_attempt_reservation"] = {}
            job["status"] = BLOCKED
            job["owner_id"] = ""
            job["last_claim_task_started"] = bool(
                job.get("current_claim_task_started")
            )
            job["last_claim_task_id"] = str(
                job.get("current_claim_task_id", "")
            )
            blocked_result = {
                "status": BLOCKED,
                "failure_code": str(code)[:80],
            }
            job["last_result"] = blocked_result
            self._terminalize_current_attempt(job, blocked_result)
            self._event(job, "BLOCKED", failure_code=str(code)[:80])
            self._write_job(job)
            queue["running_job_id"] = ""
            self._set_gate(queue, job_id, BLOCKED)
            self._write_queue(queue)
            return job

    def retry_with_supplement(
        self,
        job_id: str,
        supplemental_prompt: str,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        self._assert_writable()
        supplement = validate_supplement(supplemental_prompt)
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        supplement_digest = hashlib.sha256(supplement.encode("utf-8")).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            prior, migrated = self._lookup_action(
                job.setdefault("retry_actions", {}), action_id, action_key
            )
            if prior is not None:
                if prior.get("supplement_digest") != supplement_digest:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(queue, job, "retry")
                return job, True
            if job.get("status") != AWAITING_ENRICHMENT:
                raise QueueError("JOB_NOT_AWAITING_ENRICHMENT", job_id)
            try:
                counters = attempt_accounting(job)
            except AttemptAccountingError as exc:
                raise QueueError(exc.code, job_id) from exc
            next_attempt = int(counters["normal_attempt_no"]) + 1
            if next_attempt > int(counters["normal_attempt_budget"]):
                raise QueueError("OUTER_ATTEMPT_BUDGET_EXHAUSTED", job_id)
            supplements = list(job.get("supplements") or [])
            cumulative = supplements + [supplement]
            cumulative_text = "\n\n".join(
                f"{index}차 보강점:\n{item}"
                for index, item in enumerate(cumulative, start=1)
            )
            # Validate the cumulative prompt, not just the latest fragment, so a
            # third attempt cannot silently lose or overgrow the second attempt.
            validate_supplement(cumulative_text)
            job["current_requirement"] = compose_retry_requirement(
                str(job["request"]["requirement"]),
                job.get("last_result", {}),
                cumulative_text,
                next_attempt,
            )
            job["supplements"] = cumulative
            job["status"] = QUEUED
            job["pending_attempt_kind"] = NORMAL
            job["pending_attempt_provenance"] = {
                "request_ref": action_ref,
                "operator_expected_job_revision": int(job.get("revision", -1)),
                "operator_expected_queue_revision": int(queue.get("revision", -1)),
                "recovery_reason": "ENRICHED_RETRY",
            }
            job["retry_actions"][action_key] = {
                "supplement_digest": supplement_digest,
                "queued_at": _now(),
                "next_outer_attempt": next_attempt,
            }
            self._event(
                job,
                "ENRICHED_RETRY_QUEUED",
                request_ref=action_ref,
                next_outer_attempt=next_attempt,
            )
            self._write_job(job)
            self._clear_gate(queue, job_id)
            self._write_queue(queue)
            return job, False

    def resolve_qa_candidate_for_retry(
        self,
        job_id: str,
        *,
        expected_revision: int,
        resolution_text: str,
        candidate_id: str,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        """Record an explicit QA decision and requeue the same candidate lineage."""
        self._assert_writable()
        supplement = validate_supplement(resolution_text)
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        resolution_sha = hashlib.sha256(supplement.encode("utf-8")).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("qa_resolution_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if (
                    prior.get("expected_revision") != expected_revision
                    or prior.get("candidate_id") != candidate_id
                    or prior.get("resolution_sha256") != resolution_sha
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                return job, True
            if int(job.get("revision", -1)) != int(expected_revision):
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            if (
                job.get("status") != AWAITING_QA
                or str(job.get("candidate_id", "")) != candidate_id
                or not str(job.get("candidate_manifest", ""))
                or queue.get("running_job_id")
            ):
                raise QueueError("QA_CANDIDATE_RESOLUTION_STATE_INVALID", job_id)
            try:
                counters = attempt_accounting(job)
            except AttemptAccountingError as exc:
                raise QueueError(exc.code, job_id) from exc
            next_attempt = int(counters["normal_attempt_no"]) + 1
            if next_attempt > int(counters["normal_attempt_budget"]):
                raise QueueError("OUTER_ATTEMPT_BUDGET_EXHAUSTED", job_id)
            job["current_requirement"] = (
                str(job["request"]["requirement"]).rstrip()
                + "\n\n[Authoritative QA resolution]\n"
                + supplement
            )
            job["qa_resolution_candidate_id"] = candidate_id
            job["qa_resolution"] = {
                "authority": "EXPLICIT_USER_BUSINESS_DATA_DECISION",
                "candidate_id": candidate_id,
                "resolution_sha256": resolution_sha,
                "request_ref": action_ref,
                "resolved_at": _now(),
            }
            job["status"] = QUEUED
            job["pending_attempt_kind"] = NORMAL
            job["pending_attempt_provenance"] = {
                "request_ref": action_ref,
                "operator_expected_job_revision": expected_revision,
                "operator_expected_queue_revision": int(queue.get("revision", -1)),
                "recovery_reason": "QA_CONTRACT_RESOLVED_CANDIDATE_REUSE",
                "candidate_id": candidate_id,
            }
            actions[action_key] = {
                **dict(job["qa_resolution"]),
                "expected_revision": expected_revision,
            }
            self._event(
                job,
                "QA_CONTRACT_RESOLVED_FOR_RETRY",
                request_ref=action_ref,
                candidate_id=candidate_id,
                next_outer_attempt=next_attempt,
            )
            self._write_job(job)
            if queue.get("blocked_by_job_id") == job_id:
                self._clear_gate(queue, job_id)
            self._write_queue(queue)
            return job, False

    def retry_awaiting_qa_technical(
        self,
        job_id: str,
        *,
        expected_revision: int,
        stability_evidence_sha256: str,
        recovery_context: str,
        request_id: str,
        integrity_snapshot_provider: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Requeue one technical AWAITING_QA Job with a fail-closed Droid GLM route."""
        self._assert_writable()
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        evidence_sha = str(stability_evidence_sha256).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", evidence_sha):
            raise QueueError("TECHNICAL_RETRY_EVIDENCE_INVALID")
        try:
            context = validate_supplement(recovery_context)
        except JobContractError as exc:
            raise QueueError(exc.code, exc.field) from exc
        context_sha = hashlib.sha256(context.encode("utf-8")).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("technical_retry_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if (
                    prior.get("expected_revision") != expected_revision
                    or prior.get("stability_evidence_sha256") != evidence_sha
                    or prior.get("recovery_context_sha256") != context_sha
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(queue, job, "retry")
                return job, True
            integrity = self._validate_technical_retry_locked(
                queue,
                job,
                expected_revision=expected_revision,
                integrity_snapshot_provider=integrity_snapshot_provider,
            )
            try:
                counters = attempt_accounting(job)
            except AttemptAccountingError as exc:
                raise QueueError(exc.code, job_id) from exc
            if int(counters["technical_recovery_attempt_no"]) >= int(
                counters["technical_recovery_budget"]
            ):
                raise QueueError("TECHNICAL_RETRY_BUDGET_EXHAUSTED", job_id)
            if int(counters["execution_attempt"]) >= int(
                counters["effective_execution_limit"]
            ):
                raise QueueError("OUTER_ATTEMPT_BUDGET_EXHAUSTED", job_id)
            route = {
                "policy": DROID_GLM_FAIL_CLOSED_POLICY,
                "worker": "droid",
                "model": DROID_GLM_FAIL_CLOSED_MODEL,
                "stability_evidence_sha256": evidence_sha,
                "recovery_context_sha256": context_sha,
                "integrity_snapshot_sha256": integrity["snapshot_sha256"],
                "request_ref": action_ref,
            }
            job["technical_retry_route"] = route
            job["technical_retry_context"] = context
            job["status"] = QUEUED
            job["owner_id"] = ""
            job["pending_attempt_kind"] = TECHNICAL_RECOVERY
            job["pending_attempt_provenance"] = {
                "request_ref": action_ref,
                "operator_expected_job_revision": expected_revision,
                "operator_expected_queue_revision": int(queue.get("revision", -1)),
                "recovery_reason": str(dict(job.get("last_result") or {}).get("failure_code", "")),
                "stability_evidence_sha256": evidence_sha,
                "integrity_snapshot_sha256": integrity["snapshot_sha256"],
            }
            actions[action_key] = {
                "expected_revision": expected_revision,
                "stability_evidence_sha256": evidence_sha,
                "recovery_context_sha256": context_sha,
                "queued_at": _now(),
            }
            self._event(
                job,
                "AWAITING_QA_TECHNICAL_RETRY_QUEUED",
                request_ref=action_ref,
                expected_revision=expected_revision,
                routing_policy=DROID_GLM_FAIL_CLOSED_POLICY,
                selected_worker="droid",
                selected_model=DROID_GLM_FAIL_CLOSED_MODEL,
                integrity_snapshot_sha256=integrity["snapshot_sha256"],
            )
            self._write_job(job)
            self._clear_gate(queue, job_id)
            queue["operator_paused"] = True
            queue["operator_pause_reason"] = "AWAITING_QA_TECHNICAL_RETRY_READY"
            self._refresh_pause(queue)
            self._write_queue(queue)
            return job, False

    def _validate_technical_retry_locked(
        self,
        queue: Mapping[str, Any],
        job: Mapping[str, Any],
        *,
        expected_revision: int,
        integrity_snapshot_provider: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None,
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id", ""))
        if job.get("status") != AWAITING_QA:
            raise QueueError("JOB_NOT_AWAITING_QA", job_id)
        if int(job.get("revision", -1)) != int(expected_revision):
            raise QueueError("STALE_JOB_REVISION", job_id)
        failure_code = str(dict(job.get("last_result") or {}).get("failure_code", ""))
        if failure_code not in TECHNICAL_AWAITING_QA_FAILURE_CODES:
            raise QueueError("AWAITING_QA_NOT_TECHNICAL_RETRYABLE", job_id)
        resolved_candidate_recovery = bool(job.get("qa_resolution_candidate_id"))
        allowed_execution_mode = (
            queue.get("execution_mode") == EXECUTION_MODE_ATTENDED
            or (
                queue.get("execution_mode") == EXECUTION_MODE_AUTONOMOUS
                and resolved_candidate_recovery
                and failure_code == "CHECKPOINT_CAPTURE_FAILED"
            )
        )
        if (
            not allowed_execution_mode
            or not queue.get("paused")
            or queue.get("running_job_id")
            or queue.get("blocked_by_job_id") != job_id
            or queue.get("gate_reason") != AWAITING_QA
        ):
            raise QueueError("TECHNICAL_RETRY_CONTROL_STATE_INVALID", job_id)
        if integrity_snapshot_provider is None:
            raise QueueError("CANONICAL_INTEGRITY_UNAVAILABLE", job_id)
        try:
            integrity = dict(integrity_snapshot_provider(queue))
        except Exception as exc:
            raise QueueError("TECHNICAL_RETRY_WORKTREE_INTEGRITY_INVALID", job_id) from exc
        expected_predecessors = dict(
            dict(job.get("profile_revalidation") or {}).get("predecessor_hashes") or {}
        )
        actual_predecessors = dict(integrity.get("predecessor_delta_hashes") or {})
        recorded_snapshot_sha = str(integrity.get("snapshot_sha256", ""))
        hash_payload = dict(integrity)
        hash_payload.pop("snapshot_sha256", None)
        invalid = (
            integrity.get("source") != "CANONICAL_COMPUTED"
            or not re.fullmatch(r"[0-9a-f]{64}", recorded_snapshot_sha)
            or recorded_snapshot_sha != _canonical_sha256(hash_payload)
            or int(integrity.get("generation", -1)) != int(queue.get("generation", 0))
            or integrity.get("active_baseline_id") != queue.get("active_baseline_id")
            or not queue.get("active_baseline_id")
            or integrity.get("external_frozen_integrity") is not True
            or integrity.get("baseline_declaration_integrity") is not True
            or bool(list(integrity.get("unexpected_runtime_dirty_files") or []))
            or (expected_predecessors and actual_predecessors != expected_predecessors)
        )
        if invalid:
            raise QueueError("TECHNICAL_RETRY_WORKTREE_INTEGRITY_INVALID", job_id)
        return integrity

    def preview_awaiting_qa_technical(
        self,
        job_id: str,
        *,
        expected_revision: int,
        integrity_snapshot_provider: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    ) -> dict[str, Any]:
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            integrity = self._validate_technical_retry_locked(
                queue,
                job,
                expected_revision=expected_revision,
                integrity_snapshot_provider=integrity_snapshot_provider,
            )
            return {
                "eligible": True,
                "decision": "ELIGIBLE",
                "job_id": job_id,
                "expected_revision": expected_revision,
                "integrity_snapshot": integrity,
            }

    def acknowledge_success(
        self,
        job_id: str,
        request_id: str,
        *,
        expected_revision: int | None = None,
        batch_id: str = "",
    ) -> tuple[dict[str, Any], bool]:
        """Acknowledge a read SUCCEEDED result before advancing the FIFO queue."""
        self._assert_writable()
        if expected_revision is not None and (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise QueueError("EXPECTED_REVISION_INVALID")
        if batch_id and (
            not isinstance(batch_id, str) or not _BATCH_ID_RE.fullmatch(batch_id)
        ):
            raise QueueError("INVALID_BATCH_ID")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            acknowledgements = job.setdefault("success_acknowledgements", {})
            prior, migrated = self._lookup_action(
                acknowledgements, action_id, action_key
            )
            if prior is not None:
                if (
                    prior.get("expected_revision") != expected_revision
                    or prior.get("batch_id", "") != batch_id
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(queue, job, "ack")
                return job, True
            if job.get("status") != SUCCEEDED:
                raise QueueError("JOB_NOT_SUCCEEDED", job_id)
            if expected_revision is not None and job.get("revision") != expected_revision:
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            if batch_id and str(job.get("batch_id", "")) != batch_id:
                raise QueueError("JOB_BATCH_CONFLICT", job_id)
            if (
                queue.get("commit_policy") == BATCH_FINAL_COMMIT
                and not self._is_verified_batch_success(job)
            ):
                raise QueueError("SUCCESS_CONTRACT_INVALID", job_id)
            if (
                queue.get("blocked_by_job_id") != job_id
                or queue.get("gate_reason") != SUCCESS_ACK_REQUIRED
            ):
                raise QueueError("SUCCESS_ALREADY_ACKNOWLEDGED", job_id)
            acknowledgements[action_key] = {
                "acknowledged_at": _now(),
                "expected_revision": expected_revision,
                "batch_id": batch_id,
            }
            # 0.99.1 timing breakdown: durable human-wait measurement between
            # the required-ack gate and this acknowledgement.
            acknowledgement_wait = job_lead_time(job).get("human_wait_seconds", -1)
            if isinstance(acknowledgement_wait, int) and acknowledgement_wait >= 0:
                job["acknowledgement_wait_seconds"] = acknowledgement_wait
            self._event(job, "SUCCESS_ACKNOWLEDGED", request_ref=action_ref)
            self._write_job(job)
            self._clear_gate(queue, job_id)
            handoff = dict(queue.get("corrective_handoff") or {})
            if handoff.get("corrective_job_id") == job_id:
                handoff["state"] = "COMPLETED"
                handoff["completed_at"] = _now()
                queue["corrective_handoff"] = handoff
            self._write_queue(queue)
            return job, False

    def record_commit_result(
        self,
        job_id: str,
        request_id: str,
        commit_result: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        """Persist a local path-scoped commit receipt without advancing FIFO."""
        self._assert_writable()
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        safe_result = _safe_json(dict(commit_result))
        fingerprint = hashlib.sha256(
            json.dumps(safe_result, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        with self._mutex.hold():
            job = self._read_job(job_id)
            actions = job.setdefault("commit_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if prior.get("fingerprint") != fingerprint:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                return job, True
            if job.get("status") != SUCCEEDED:
                raise QueueError("JOB_NOT_SUCCEEDED", job_id)
            actions[action_key] = {
                "fingerprint": fingerprint,
                "recorded_at": _now(),
                "request_ref": action_ref,
            }
            last_result = dict(job.get("last_result") or {})
            last_result["commit_status"] = safe_result.get("status", "")
            last_result["commits"] = list(safe_result.get("commits") or [])
            job["last_result"] = last_result
            self._event(
                job,
                "JOB_RESULT_COMMITTED" if safe_result.get("status") in {"COMMITTED", "NO_CHANGES"}
                else "JOB_RESULT_COMMIT_FAILED",
                request_ref=action_ref,
                commit_count=len(last_result["commits"]),
            )
            self._write_job(job)
            return job, False

    def set_execution_mode(
        self,
        mode: str,
        request_id: str,
        confirmation: str = "",
        *,
        expected_revision: int | None = None,
        batch_id: str = "",
        reason: str = "",
    ) -> tuple[dict[str, Any], bool]:
        self._assert_writable()
        normalized = str(mode).strip().upper()
        if normalized not in EXECUTION_MODES:
            raise QueueError("EXECUTION_MODE_INVALID")
        if normalized == EXECUTION_MODE_AUTONOMOUS and confirmation != AUTONOMOUS_CONFIRMATION:
            raise QueueError("AUTONOMOUS_CONFIRMATION_REQUIRED")
        if expected_revision is not None and (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise QueueError("EXPECTED_REVISION_INVALID")
        if batch_id and (
            not isinstance(batch_id, str) or not _BATCH_ID_RE.fullmatch(batch_id)
        ):
            raise QueueError("INVALID_BATCH_ID")
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("mode_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if (
                    prior.get("mode") != normalized
                    or prior.get("expected_revision") != expected_revision
                    or prior.get("batch_id", "") != batch_id
                    or prior.get("reason", "") != safe_reason
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_queue(queue)
                return queue, True
            if queue.get("running_job_id"):
                raise QueueError("EXECUTION_MODE_CHANGE_WHILE_RUNNING")
            if expected_revision is not None and queue.get("revision") != expected_revision:
                raise QueueError("QUEUE_REVISION_CONFLICT")
            current_batch_id = self._current_queue_batch_id(queue)
            if batch_id and current_batch_id != batch_id:
                raise QueueError("QUEUE_BATCH_CONFLICT", current_batch_id)
            queue["execution_mode"] = normalized
            actions[action_key] = {
                "mode": normalized,
                "expected_revision": expected_revision,
                "batch_id": batch_id,
                "reason": safe_reason,
                "changed_at": _now(),
                "request_ref": action_ref,
            }
            self._write_queue(queue)
            return queue, False

    def cancel_queued_batch(
        self,
        batch_id: str,
        reason: str,
        request_id: str,
    ) -> tuple[list[dict[str, Any]], bool]:
        self._assert_writable()
        if not isinstance(batch_id, str) or not _BATCH_ID_RE.fullmatch(batch_id):
            raise QueueError("INVALID_BATCH_ID")
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("CANCEL_REASON_REQUIRED")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        reason_digest = hashlib.sha256(safe_reason.encode("utf-8")).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            selected_key = ""
            selected: dict[str, Any] | None = None
            for key, metadata in queue.get("batches", {}).items():
                ids = list((metadata or {}).get("job_ids") or [])
                if ids and self._read_job(ids[0]).get("batch_id") == batch_id:
                    selected_key, selected = str(key), metadata
                    break
            if selected is None:
                raise QueueError("BATCH_NOT_FOUND", batch_id)
            actions = selected.setdefault("cancel_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if prior.get("reason_digest") != reason_digest:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    queue["batches"][selected_key] = selected
                    self._write_queue(queue)
                return [self._read_job(item) for item in prior.get("job_ids", [])], True
            cancelled: list[dict[str, Any]] = []
            for job_id in list(selected.get("job_ids") or []):
                job = self._read_job(job_id)
                if job.get("status") != QUEUED:
                    continue
                job["status"] = CANCELLED
                job["cancel_reason"] = safe_reason
                self._event(job, "CANCELLED", reason=safe_reason, request_ref=action_ref)
                self._write_job(job)
                cancelled.append(job)
            actions[action_key] = {
                "reason_digest": reason_digest,
                "job_ids": [job["job_id"] for job in cancelled],
                "cancelled_at": _now(),
            }
            queue["batches"][selected_key] = selected
            self._write_queue(queue)
            return cancelled, False

    def reset_terminal_queue(
        self,
        request_id: str,
        confirmation: str,
    ) -> tuple[dict[str, Any], bool]:
        """Start a new logical generation while retaining complete Job history."""
        self._assert_writable()
        if confirmation != RESET_CONFIRMATION:
            raise QueueError("RESET_CONFIRMATION_REQUIRED")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        with self._mutex.hold():
            queue = self._read_queue()
            actions = queue.setdefault("reset_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if migrated:
                    self._write_queue(queue)
                return queue, True
            if queue.get("running_job_id") or queue.get("operator_paused"):
                raise QueueError("QUEUE_RESET_NOT_IDLE")
            active = self._active_job_ids(queue)
            for job_id in active:
                job = self._read_job(job_id)
                status = str(job.get("status", ""))
                if status in ACTIVE_STATUSES or status in PAUSING_STATUSES:
                    raise QueueError("QUEUE_RESET_UNRESOLVED_JOB", job_id)
                if status == SUCCEEDED and not job.get("success_acknowledgements"):
                    raise QueueError("QUEUE_RESET_UNACKNOWLEDGED_SUCCESS", job_id)
            reset_at = _now()
            previous_generation = int(queue.get("generation", 1))
            reset_record = {
                "request_ref": action_ref,
                "reset_at": reset_at,
                "previous_generation": previous_generation,
                "archived_job_count": len(active),
            }
            queue["active_from_index"] = len(queue.get("order") or [])
            queue["generation"] = previous_generation + 1
            queue["batches"] = {}
            queue["execution_mode"] = EXECUTION_MODE_ATTENDED
            queue["blocked_by_job_id"] = ""
            queue["gate_reason"] = ""
            queue["operator_paused"] = False
            queue["operator_pause_reason"] = ""
            queue["commit_policy"] = PER_JOB_COMMIT
            queue["cumulative_worktree"] = False
            queue["auto_continue_without_commit"] = False
            queue["batch_owned_delta_supported"] = False
            queue["pre_job_snapshot_supported"] = False
            queue["scoped_rollback_supported"] = False
            queue["final_commit_required"] = False
            queue["active_baseline_id"] = ""
            queue["task_owned_baseline_files"] = []
            queue["external_frozen_baseline_files"] = []
            queue["batch_delta_files"] = []
            queue["unexpected_runtime_dirty_files"] = []
            queue.setdefault("resets", []).append(reset_record)
            actions[action_key] = reset_record
            self._refresh_pause(queue)
            self._write_queue(queue)
            return queue, False

    def revalidate_blocked_job_profile(
        self,
        job_id: str,
        *,
        expected_revision: int,
        expected_old_execution_context: Mapping[str, Any],
        current_execution_context: Mapping[str, Any],
        classification_evidence_sha256: str,
        audit: Mapping[str, Any],
        reason: str,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically rebind one safe PROFILE_DRIFT blocker and requeue it.

        Semantic/provenance validation belongs to the control service.  This
        persistence boundary repeats every mutable Queue/Job precondition while
        holding the store mutex, then commits the context and retry transition in
        the same Job revision.  The Queue remains operator-paused.
        """
        self._assert_writable()
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        old_context = _safe_json(validate_execution_context(expected_old_execution_context))
        new_context = _safe_json(validate_execution_context(current_execution_context))
        evidence_sha = str(classification_evidence_sha256).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", evidence_sha):
            raise QueueError("PROFILE_DRIFT_CLASSIFICATION_EVIDENCE_INVALID")
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("PROFILE_REVALIDATION_REASON_REQUIRED")
        safe_audit = _safe_json(dict(audit))
        arguments = {
            "job_id": job_id,
            "expected_revision": expected_revision,
            "expected_old_execution_context": old_context,
            "current_execution_context": new_context,
            "classification_evidence_sha256": evidence_sha,
            "audit": safe_audit,
            "reason": safe_reason,
        }
        argument_digest = hashlib.sha256(
            json.dumps(
                arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("profile_revalidation_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if prior.get("argument_sha256") != argument_digest:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                return job, True

            if job.get("status") != BLOCKED:
                if job.get("profile_revalidation"):
                    raise QueueError("PROFILE_DRIFT_ALREADY_REVALIDATED", job_id)
                raise QueueError("JOB_NOT_BLOCKED", job_id)
            if int(job.get("revision", -1)) != int(expected_revision):
                raise QueueError("STALE_JOB_REVISION", job_id)
            if dict(job.get("last_result") or {}).get("failure_code") != "PROFILE_DRIFT":
                raise QueueError("JOB_NOT_PROFILE_DRIFT_BLOCKED", job_id)
            if job.get("execution_context") != old_context:
                raise QueueError("OLD_EXECUTION_CONTEXT_MISMATCH", job_id)
            if queue.get("execution_mode") != EXECUTION_MODE_ATTENDED:
                raise QueueError("PROFILE_REVALIDATION_REQUIRES_ATTENDED")
            if not queue.get("paused"):
                raise QueueError("PROFILE_REVALIDATION_REQUIRES_PAUSED_QUEUE")
            if queue.get("running_job_id"):
                raise QueueError("QUEUE_ALREADY_RUNNING")
            if (
                queue.get("blocked_by_job_id") != job_id
                or queue.get("gate_reason") != BLOCKED
            ):
                raise QueueError("JOB_NOT_ACTIVE_FIFO_BLOCKER", job_id)
            if int(queue.get("generation", 0)) != int(safe_audit.get("generation", -1)):
                raise QueueError("PROFILE_REVALIDATION_GENERATION_CHANGED")
            if queue.get("active_baseline_id", "") != safe_audit.get("baseline_id", ""):
                raise QueueError("PROFILE_REVALIDATION_BASELINE_CHANGED")
            if list(queue.get("batch_delta_files") or []) != list(
                safe_audit.get("predecessor_files") or []
            ):
                raise QueueError("PROFILE_REVALIDATION_PREDECESSOR_CHANGED")
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
            recovery_count = int(job.get("recovery_attempt_count", 0))
            if recovery_count >= int(job.get("max_outer_attempts", 1)):
                raise QueueError("RECOVERY_RETRY_BUDGET_EXHAUSTED", job_id)
            try:
                counters = attempt_accounting(job)
            except AttemptAccountingError as exc:
                raise QueueError(exc.code, job_id) from exc
            if int(counters["execution_attempt"]) >= int(
                counters["effective_execution_limit"]
            ):
                raise QueueError("OUTER_ATTEMPT_BUDGET_EXHAUSTED", job_id)

            recorded_at = _now()
            record = {
                "argument_sha256": argument_digest,
                "request_ref": action_ref,
                "recorded_at": recorded_at,
                "classification_evidence_sha256": evidence_sha,
                "old_execution_context_sha256": safe_audit.get(
                    "old_execution_context_sha256", ""
                ),
                "new_execution_context_sha256": safe_audit.get(
                    "new_execution_context_sha256", ""
                ),
            }
            actions[action_key] = record
            job["execution_context"] = new_context
            job["profile_revalidation"] = {**record, **safe_audit, "reason": safe_reason}
            job["status"] = QUEUED
            job["owner_id"] = ""
            job["recovery_attempt_count"] = recovery_count + 1
            job["pending_attempt_kind"] = VERIFICATION_RECOVERY
            job["pending_attempt_provenance"] = {
                "request_ref": action_ref,
                "operator_expected_job_revision": expected_revision,
                "operator_expected_queue_revision": int(queue.get("revision", -1)),
                "recovery_reason": "PROFILE_DRIFT_REVALIDATION",
            }
            self._event(
                job,
                "PROFILE_DRIFT_REVALIDATED",
                actor="operator",
                request_ref=action_ref,
                expected_revision=expected_revision,
                classification_evidence_sha256=evidence_sha,
                old_execution_context_sha256=safe_audit.get(
                    "old_execution_context_sha256", ""
                ),
                new_execution_context_sha256=safe_audit.get(
                    "new_execution_context_sha256", ""
                ),
                requirement_sha256=safe_audit.get("requirement_sha256", ""),
                request_sha256=safe_audit.get("request_sha256", ""),
                changed_resource_paths=list(safe_audit.get("changed_resource_paths") or []),
                classification_codes=list(safe_audit.get("classification_codes") or []),
                release_provenance=list(safe_audit.get("release_provenance") or []),
                baseline_id=safe_audit.get("baseline_id", ""),
                predecessor_hashes=dict(safe_audit.get("predecessor_hashes") or {}),
                generation=safe_audit.get("generation", 0),
                reason=safe_reason,
                start_immediately=False,
            )
            # Exactly one Job revision contains both context rebind and retry state.
            self._write_job(job)
            self._clear_gate(queue, job_id)
            queue["operator_paused"] = True
            queue["operator_pause_reason"] = "PROFILE_DRIFT_REVALIDATED_READY"
            self._refresh_pause(queue)
            self._write_queue(queue)
            return job, False

    def resume_job(
        self,
        job_id: str,
        request_id: str,
        *,
        expected_queue_revision: int | None = None,
        expected_job_revision: int | None = None,
        expected_task_id: str = "",
        expected_execution_id: str = "",
    ) -> tuple[dict[str, Any], bool]:
        """Requeue a non-started BLOCKED/INTERRUPTED job without consuming budget."""
        self._assert_writable()
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            if expected_queue_revision is not None and int(queue.get("revision", -1)) != int(expected_queue_revision):
                raise QueueError("QUEUE_REVISION_CONFLICT")
            if expected_job_revision is not None and int(job.get("revision", -1)) != int(expected_job_revision):
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            if expected_task_id or expected_execution_id:
                salvage = dict(job.get("crash_salvage") or {})
                if (
                    job.get("status") != INTERRUPTED
                    or salvage.get("status") != "NO_TASK_DELTA"
                    or str(salvage.get("task_id", "")) != expected_task_id
                    or str(salvage.get("execution_id", "")) != expected_execution_id
                    or list(salvage.get("task_owned_changed_files") or [])
                ):
                    raise QueueError("ORPHAN_RECONCILIATION_LINEAGE_CONFLICT", job_id)
            actions = job.setdefault("resume_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(
                    queue,
                    job,
                    "resume_exhausted" if prior.get("exhausted") else "resume",
                )
                return job, True
            if job.get("status") not in (BLOCKED, INTERRUPTED):
                raise QueueError("JOB_NOT_RESUMABLE", job_id)
            if int(job.get("outer_attempt", 0)) >= int(
                job.get("max_outer_attempts", 1)
            ):
                job["status"] = FAILED_FINAL
                job["owner_id"] = ""
                job["last_result"] = {
                    **dict(job.get("last_result") or {}),
                    "queue_decision_code": "OUTER_ATTEMPT_BUDGET_EXHAUSTED",
                }
                actions[action_key] = {"exhausted": True, "finished_at": _now()}
                self._event(
                    job,
                    "OUTER_ATTEMPT_BUDGET_EXHAUSTED",
                    request_ref=action_ref,
                )
                self._write_job(job)
                queue["running_job_id"] = ""
                self._set_gate(queue, job_id, FAILED_FINAL)
                self._write_queue(queue)
                return job, False
            recovery_count = int(job.get("recovery_attempt_count", 0))
            if recovery_count >= int(job.get("max_outer_attempts", 1)):
                job["status"] = FAILED_FINAL
                job["owner_id"] = ""
                job["last_result"] = {
                    **dict(job.get("last_result") or {}),
                    "queue_decision_code": "RECOVERY_RETRY_BUDGET_EXHAUSTED",
                }
                actions[action_key] = {"exhausted": True, "finished_at": _now()}
                self._event(
                    job,
                    "RECOVERY_RETRY_BUDGET_EXHAUSTED",
                    request_ref=action_ref,
                )
                self._write_job(job)
                queue["running_job_id"] = ""
                self._set_gate(queue, job_id, FAILED_FINAL)
                self._write_queue(queue)
                return job, False
            job["status"] = QUEUED
            job["owner_id"] = ""
            job["recovery_attempt_count"] = recovery_count + 1
            job["pending_attempt_kind"] = VERIFICATION_RECOVERY
            job["pending_attempt_provenance"] = {
                "request_ref": action_ref,
                "operator_expected_job_revision": int(job.get("revision", -1)),
                "operator_expected_queue_revision": int(queue.get("revision", -1)),
                "recovery_reason": "INTERRUPTED_JOB_RESUME",
            }
            actions[action_key] = {"queued_at": _now()}
            self._event(job, "INTERRUPTED_JOB_REQUEUED", request_ref=action_ref)
            self._write_job(job)
            self._clear_gate(queue, job_id)
            self._write_queue(queue)
            return job, False

    def skip_failed_job(
        self,
        job_id: str,
        reason: str,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        self._assert_writable()
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("SKIP_REASON_REQUIRED")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]
        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("skip_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            reason_digest = hashlib.sha256(safe_reason.encode("utf-8")).hexdigest()
            if prior is not None:
                if prior.get("reason_digest") != reason_digest:
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(queue, job, "skip")
                return job, True
            if job.get("status") not in {AWAITING_ENRICHMENT, FAILED_FINAL}:
                raise QueueError("JOB_NOT_SKIPPABLE", job_id)
            last_result = dict(job.get("last_result") or {})
            safe_worktree = last_result.get("worktree_disposition") in {
                "CLEAN_ROLLBACK",
                "CHECKPOINTED_CLEAN_ROLLBACK",
                "NO_DELTA",
            }
            if not safe_worktree:
                raise QueueError("SKIP_REQUIRES_CLEAN_WORKTREE", job_id)
            job["status"] = SKIPPED
            job["skip_reason"] = safe_reason
            job["owner_id"] = ""
            actions[action_key] = {
                "reason_digest": reason_digest,
                "skipped_at": _now(),
            }
            self._event(job, "SKIPPED", reason=safe_reason, request_ref=action_ref)
            self._write_job(job)
            if queue.get("blocked_by_job_id") == job_id:
                self._clear_gate(queue, job_id)
                queue["running_job_id"] = ""
            self._write_queue(queue)
            return job, False

    def cancel_queued_job(self, job_id: str, reason: str) -> dict[str, Any]:
        self._assert_writable()
        safe_reason = scrub_secrets(str(reason).strip())[:1000]
        if not safe_reason:
            raise QueueError("CANCEL_REASON_REQUIRED")
        with self._mutex.hold():
            job = self._read_job(job_id)
            if job.get("status") != QUEUED:
                raise QueueError("JOB_NOT_CANCELLABLE", job_id)
            job["status"] = CANCELLED
            job["cancel_reason"] = safe_reason
            self._event(job, "CANCELLED", reason=safe_reason)
            self._write_job(job)
            return job

    def resolve_manually(
        self,
        job_id: str,
        *,
        resolution: str,
        confirmation: str,
        expected_revision: int,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        """Resolve the current FIFO blocker after an out-of-band operator check.

        This is intentionally a store primitive, not an MCP tool.  The caller must
        hold the harness execution lease so the OS-idle observation remains true
        through the state transition.
        """
        self._assert_writable()
        if resolution not in MANUAL_RESOLUTIONS:
            raise QueueError("MANUAL_RESOLUTION_INVALID")
        required_confirmation = MANUAL_RESOLUTION_CONFIRMATIONS[resolution]
        if confirmation != required_confirmation:
            raise QueueError("MANUAL_RESOLUTION_CONFIRMATION_REQUIRED")
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise QueueError("EXPECTED_REVISION_INVALID")
        action_id = validate_idempotency_key(request_id, "request_id")
        action_key = f"sha256:{_identifier_digest(action_id)}"
        action_ref = _identifier_digest(action_id)[:16]

        with self._mutex.hold():
            queue = self._read_queue()
            job = self._read_job(job_id)
            actions = job.setdefault("manual_resolution_actions", {})
            prior, migrated = self._lookup_action(actions, action_id, action_key)
            if prior is not None:
                if (
                    prior.get("resolution") != resolution
                    or prior.get("expected_revision") != expected_revision
                ):
                    raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                if migrated:
                    self._write_job(job)
                self._repair_replayed_action(
                    queue, job, "manual_resolution"
                )
                return job, True

            status = str(job.get("status", ""))
            if status not in MANUALLY_RESOLVABLE_STATUSES:
                raise QueueError("JOB_NOT_MANUALLY_RESOLVABLE", job_id)
            current_revision = job.get("revision")
            if isinstance(current_revision, bool) or not isinstance(
                current_revision, int
            ):
                raise QueueError("JOB_STATE_INVALID", job_id)
            if current_revision != expected_revision:
                raise QueueError("JOB_REVISION_CONFLICT", job_id)
            if (
                queue.get("blocked_by_job_id") != job_id
                or queue.get("gate_reason") != status
            ):
                raise QueueError("JOB_NOT_CURRENT_BLOCKER", job_id)

            resolved_at = _now()
            worktree_disposition = (
                "PRESERVED"
                if resolution == MANUAL_RESOLUTION_ACCEPT_PRESERVED
                else "NO_DELTA"
            )
            previous_result = dict(job.get("last_result") or {})
            manual_result = {
                **previous_result,
                "status": SKIPPED,
                "manual_resolution": resolution,
                "manual_resolution_code": f"MANUALLY_RESOLVED_{resolution}",
                "resolved_from_status": status,
                "worktree_disposition": worktree_disposition,
            }
            action_metadata = {
                "resolution": resolution,
                "expected_revision": expected_revision,
                "request_ref": action_ref,
                "resolved_from_status": status,
                "resolved_at": resolved_at,
            }
            actions[action_key] = action_metadata
            job["manual_resolution"] = dict(action_metadata)
            job["status"] = SKIPPED
            job["owner_id"] = ""
            job["last_claim_task_started"] = bool(
                job.get("current_claim_task_started")
            )
            job["last_claim_task_id"] = str(
                job.get("current_claim_task_id", "")
            )
            job["last_result"] = manual_result
            self._terminalize_current_attempt(job, manual_result)
            self._event(
                job,
                "MANUALLY_RESOLVED",
                resolution=resolution,
                resolved_from_status=status,
                request_ref=action_ref,
            )
            # Job-first commit makes an interrupted queue commit repairable by an
            # idempotent replay of the same request ID.
            self._write_job(job)

            if queue.get("running_job_id") == job_id:
                queue["running_job_id"] = ""
            self._restore_earliest_gate(queue)
            self._write_queue(queue)
            return job, False

    def pause(
        self,
        reason: str,
        *,
        expected_revision: int | None = None,
        request_id: str = "",
        confirmation: str = "",
    ) -> dict[str, Any]:
        self._assert_writable()
        safe_reason = scrub_secrets(str(reason).strip())[:1000] or "operator_pause"
        with self._mutex.hold():
            queue = self._read_queue()
            if request_id:
                if confirmation != "I_CONFIRM_PAUSE_QUEUE":
                    raise QueueError("PAUSE_CONFIRMATION_REQUIRED")
                action_id = validate_idempotency_key(request_id, "request_id")
                action_key = f"sha256:{_identifier_digest(action_id)}"
                action_ref = _identifier_digest(action_id)[:16]
                actions = queue.setdefault("operator_pause_actions", {})
                prior, migrated = self._lookup_action(actions, action_id, action_key)
                if prior is not None:
                    if (
                        prior.get("operation") != "PAUSE"
                        or prior.get("expected_revision") != expected_revision
                        or prior.get("reason") != safe_reason
                    ):
                        raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                    if migrated:
                        self._write_queue(queue)
                    return queue
                if expected_revision is None or queue.get("revision") != expected_revision:
                    raise QueueError("QUEUE_REVISION_CONFLICT")
                actions[action_key] = {
                    "operation": "PAUSE",
                    "expected_revision": expected_revision,
                    "reason": safe_reason,
                    "changed_at": _now(),
                    "request_ref": action_ref,
                }
            queue["operator_paused"] = True
            queue["operator_pause_reason"] = safe_reason
            self._refresh_pause(queue)
            self._write_queue(queue)
            return queue

    def resume_queue(
        self,
        *,
        expected_revision: int | None = None,
        request_id: str = "",
        confirmation: str = "",
    ) -> dict[str, Any]:
        self._assert_writable()
        with self._mutex.hold():
            queue = self._read_queue()
            if request_id:
                if confirmation != "I_CONFIRM_RESUME_QUEUE":
                    raise QueueError("RESUME_CONFIRMATION_REQUIRED")
                action_id = validate_idempotency_key(request_id, "request_id")
                action_key = f"sha256:{_identifier_digest(action_id)}"
                action_ref = _identifier_digest(action_id)[:16]
                actions = queue.setdefault("operator_pause_actions", {})
                prior, migrated = self._lookup_action(actions, action_id, action_key)
                if prior is not None:
                    if (
                        prior.get("operation") != "RESUME"
                        or prior.get("expected_revision") != expected_revision
                    ):
                        raise QueueError("IDEMPOTENCY_CONFLICT", action_ref)
                    if migrated:
                        self._write_queue(queue)
                    return queue
                if expected_revision is None or queue.get("revision") != expected_revision:
                    raise QueueError("QUEUE_REVISION_CONFLICT")
            if queue.get("running_job_id"):
                raise QueueError("QUEUE_ALREADY_RUNNING")
            if request_id:
                actions[action_key] = {
                    "operation": "RESUME",
                    "expected_revision": expected_revision,
                    "changed_at": _now(),
                    "request_ref": action_ref,
                }
            queue["operator_paused"] = False
            queue["operator_pause_reason"] = ""
            self._refresh_pause(queue)
            self._write_queue(queue)
            return queue

    def reconcile_interrupted(self, *, workspace=None, integrity_snapshot_provider=None) -> list[str]:
        """Repair partial commits, then fail closed for genuinely running claims."""
        self._assert_writable()
        interrupted: list[str] = []
        with self._mutex.hold():
            queue = self._read_queue()
            queue_changed = False

            # ``record_result`` deliberately commits the durable Job result before
            # clearing queue ownership. A crash between those two atomic replaces
            # leaves a terminal Job in ``running_job_id``. Repair that exact state
            # deterministically instead of stalling forever or rerunning the Job.
            running_id = queue.get("running_job_id", "")
            if running_id:
                running_job = self._read_job(running_id)
                running_status = running_job.get("status")
                if running_status != RUNNING:
                    queue["running_job_id"] = ""
                    if (
                        running_status == SUCCEEDED
                        and not running_job.get("success_acknowledgements")
                    ):
                        self._set_gate(queue, running_id, SUCCESS_ACK_REQUIRED)
                    elif running_status in PAUSING_STATUSES:
                        self._set_gate(queue, running_id, str(running_status))
                    queue_changed = True

            blocked_id = queue.get("blocked_by_job_id", "")
            if blocked_id:
                blocked_job = self._read_job(blocked_id)
                blocked_status = blocked_job.get("status")
                gate_reason = queue.get("gate_reason", "")
                valid_gate = (
                    blocked_status in PAUSING_STATUSES
                    or (
                        blocked_status == SUCCEEDED
                        and gate_reason == SUCCESS_ACK_REQUIRED
                        and not blocked_job.get("success_acknowledgements")
                    )
                )
                if not valid_gate:
                    self._clear_gate(queue, blocked_id)
                    queue_changed = True

            candidates = [queue.get("running_job_id", "")]
            candidates.extend(self._active_job_ids(queue))
            for job_id in dict.fromkeys(job_id for job_id in candidates if job_id):
                job = self._read_job(job_id)
                if job.get("status") != RUNNING:
                    continue
                salvage = dict(job.get("crash_salvage") or {})
                if (
                    queue.get("commit_policy") == BATCH_FINAL_COMMIT
                    and job.get("current_claim_task_started")
                    and not salvage
                ):
                    raise QueueError("STALE_RUNNING_SALVAGE_REQUIRED", job_id)
                job["status"] = INTERRUPTED
                job["owner_id"] = ""
                job["last_claim_task_started"] = bool(
                    job.get("current_claim_task_started")
                )
                job["last_claim_task_id"] = str(
                    job.get("current_claim_task_id", "")
                )
                no_delta = salvage.get("status") == "NO_TASK_DELTA"
                interrupted_result = {
                    "status": INTERRUPTED,
                    "failure_code": "CONTROL_PROCESS_INTERRUPTED",
                    "failure_type": "infrastructure",
                    "fix_scope": "NON_RETRYABLE",
                    "verification_status": "PARTIAL",
                    "worktree_disposition": "NO_DELTA" if no_delta else "PRESERVED",
                    "checkpoint_status": str(salvage.get("status") or "UNAVAILABLE_AFTER_PROCESS_INTERRUPTION"),
                    "checkpoint_id": str(salvage.get("checkpoint_id", "")),
                    "checkpoint_manifest": str(salvage.get("manifest_path", "")),
                    "checkpoint_manifest_sha256": str(salvage.get("manifest_sha256", "")),
                    "task_owned_changed_files": list(salvage.get("task_owned_changed_files") or []),
                    "crash_recovery_handoff": str(job.get("crash_recovery_handoff", "")),
                    "user_intervention_reason": (
                        "CRASH_SALVAGE_REVIEW_REQUIRED"
                        if salvage else "CHECKPOINT_REQUIRED_AFTER_INTERRUPTION"
                    ),
                    "rollback_performed": False,
                }
                job["last_result"] = interrupted_result
                self._terminalize_current_attempt(job, interrupted_result)
                self._event(job, "CONTROL_PROCESS_INTERRUPTED")
                self._write_job(job)
                interrupted.append(job_id)
            if interrupted:
                queue["running_job_id"] = ""
                queue_changed = True

            # Always gate the earliest unresolved Job. This also covers multiple
            # RUNNING claims left by historical partial commits; resolving one
            # interrupted Job can never silently bypass another. A job-scoped
            # historical QA hold is not Queue-wide ownership: its status stays
            # immutable evidence while independent Jobs keep dispatching, so it
            # must not resurrect a global gate here (0.9.0.9).
            next_blocker = ""
            next_reason = ""
            for job_id in self._active_job_ids(queue):
                if self._is_replacement_anchor_parent(queue, job_id):
                    continue
                job = self._read_job(job_id)
                status = job.get("status")
                if (status in PAUSING_STATUSES and not self._qa_non_global(job)
                        and not self._has_active_corrective_takeover(
                            queue, job, workspace=workspace,
                            integrity_snapshot_provider=integrity_snapshot_provider)):
                    next_blocker, next_reason = job_id, str(status)
                    break
                if (
                    status == SUCCEEDED
                    and not job.get("success_acknowledgements")
                ):
                    next_blocker, next_reason = job_id, SUCCESS_ACK_REQUIRED
                    break
            if next_blocker:
                if (
                    queue.get("blocked_by_job_id") != next_blocker
                    or queue.get("gate_reason") != next_reason
                ):
                    self._set_gate(queue, next_blocker, next_reason)
                    queue_changed = True
            elif queue.get("blocked_by_job_id"):
                self._clear_gate(queue, str(queue.get("blocked_by_job_id")))
                queue_changed = True
            if queue_changed:
                self._write_queue(queue)
            return interrupted
