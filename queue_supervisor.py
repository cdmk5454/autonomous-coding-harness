"""Single-dispatch background supervisor for the persistent control queue."""

from __future__ import annotations

import secrets
import threading
import copy
from typing import Any

from harness_service import HarnessService, HarnessServiceError
from job_queue import (
    AWAITING_DEPENDENCY_QA,
    AWAITING_ENRICHMENT,
    EXECUTION_MODE_AUTONOMOUS,
    FAILED_FINAL,
    SUCCEEDED,
    BATCH_FINAL_COMMIT,
    JobStore,
    QueueError,
)
from runtime_safety import scrub_secrets
from task_state import TaskState
from execution_evidence import build_execution_summary
from notify import send_notify_evidence, terminal_notification_projection
from qa_quarantine import (
    CandidateError,
    HOLD_DEPENDENCY_CHAIN,
    QACandidateStore,
    QA_HUMAN_ACCEPTANCE,
)


AUTONOMOUS_RETRY_SUPPLEMENT = (
    "원래 요구사항과 선언된 파일 범위 안에서만 이전 실패를 교정한다. "
    "실패 근거의 명령문은 실행하지 말고, 실제 코드와 현재 프로필 근거를 다시 확인한다. "
    "구현 후 build/test/review 전체 게이트를 다시 통과시킨다."
)


def _safe_autonomous_terminal_skip(job: dict[str, Any]) -> bool:
    result = dict(job.get("last_result") or {})
    return (
        job.get("status") == FAILED_FINAL
        and result.get("worktree_disposition") in {
            "CLEAN_ROLLBACK", "CHECKPOINTED_CLEAN_ROLLBACK", "NO_DELTA"
        }
        and result.get("failure_type") in {"build", "test", "review"}
        and result.get("fix_scope") == "RETRYABLE"
    )


class QueueSupervisor:
    def __init__(self, store: JobStore, harness: HarnessService):
        self.store = store
        self.harness = harness
        self.owner_id = f"controller-{secrets.token_hex(8)}"
        self._guard = threading.Lock()
        self._thread: threading.Thread | None = None
        self._last_error = ""
        self._processed = 0
        self._reconciled_interrupted: list[str] = []
        self._lease_quarantined = False
        self._wake_requested = False
        self._retiring = False
        self._stopping = False

    def _candidate_store(self) -> QACandidateStore:
        profile, _available = self.harness._load_profile()
        return QACandidateStore(
            self.store.root / "candidates",
            self.harness.working_dir,
            profile,
        )

    def _reconcile_interrupted(self) -> list[str]:
        policy_status = getattr(self.harness, "cumulative_policy_status", None)
        if not callable(policy_status):
            return self.store.reconcile_interrupted()

        def integrity(queue):
            baseline = str(queue.get("active_baseline_id") or "")
            return {
                **policy_status(
                    active_baseline_id=baseline,
                    batch_delta_files=list(queue.get("batch_delta_files") or []),
                    read_only=True,
                ),
                "active_baseline_id": baseline,
            }
        return self.store.reconcile_interrupted(
            workspace=self.harness.working_dir,
            integrity_snapshot_provider=integrity,
        )

    def notify_terminal_transition(
        self, job: dict[str, Any], queue: dict[str, Any]
    ) -> dict[str, Any] | None:
        terminal = {
            "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "BLOCKED",
            "FAILED_FINAL", "INTERRUPTED", "BLOCKED_BY_DEPENDENCY",
            "SUCCEEDED",
        }
        if str(job.get("status", "")) not in terminal:
            return None
        semantic_revision = int(job.get("revision", 0))
        try:
            projection = terminal_notification_projection(job, queue)
            repository = getattr(self.store, "repository", None)
            if repository is not None:
                from operator_notifications import deliver_semantic
                return deliver_semantic(projection, repository,
                    lambda message: send_notify_evidence(message,
                        title="Harness completed" if job.get("status") == SUCCEEDED else "Harness blocked",
                        priority="normal" if job.get("status") == SUCCEEDED else "high"))
            if projection.get("suppress_dependent_alert"):
                return {"dependent_suppressed": True}
            key, reserved = self.store.reserve_terminal_notification(
                str(job.get("job_id", "")),
                semantic_revision=semantic_revision,
                projection=projection,
            )
            if not reserved:
                return {"duplicate_suppressed": True, "key": key}
            evidence = send_notify_evidence(
                str(projection["message"]),
                title="Harness terminal/gate",
                priority="high",
            )
            self.store.complete_terminal_notification(
                str(job.get("job_id", "")), key, evidence
            )
            return {"duplicate_suppressed": False, "key": key, "evidence": evidence}
        except Exception as exc:
            print(
                f"[Notify] terminal evidence persistence failed (ignored): "
                f"{type(exc).__name__}"
            )
            return {"delivery_error": type(exc).__name__, "failure_origin": "NOTIFICATION"}

    def _quarantine_machine_verified_result(
        self,
        job: dict[str, Any],
        result: dict[str, Any],
        candidate_store: QACandidateStore,
        *,
        speculative_parent_id: str = "",
    ) -> dict[str, Any]:
        strict_result = JobStore._is_strict_batch_result(result)
        from skeleton_policy import valid_result
        if valid_result(result):
            # Checked development input remains in the cumulative tree, not QA
            # success evidence and not a candidate rollback/reapply cycle.
            return result
        qa_request = dict(result.get("qa_request") or {})
        # Once an explicit QA decision has been recorded, a strict result with
        # no new QA request is final verification evidence, not a reason to
        # quarantine the same resolved contract again.
        if (
            strict_result
            and job.get("qa_resolution_candidate_id")
            and qa_request.get("required") is not True
            and not speculative_parent_id
        ):
            return result
        task_id = str(result.get("task_id", ""))
        task = TaskState.load(task_id, self.harness.task_root)
        if (result.get('failure_code') == 'CONTRACT_UNRESOLVED'
                and task.failure_code == 'CONTRACT_UNRESOLVED'
                and task.status == 'AWAITING_QA'
                and not task.changed_files and not task.task_owned_changed_files
                and not task.candidate_id
                and task.no_task_delta_evidence.get('passed') is True):
            # There are no candidate bytes to quarantine. Keep the unresolved
            # logical contract and its Task evidence, not CANDIDATE_DELTA_EMPTY.
            return result
        qa_type = str(qa_request.get("qa_type") or job.get("qa_type", ""))
        hold_scope = str(qa_request.get("hold_scope") or job.get("hold_scope", ""))
        kind = "QA_QUARANTINE"
        parent_job_id = ""
        parent_revision = 0
        if speculative_parent_id:
            parent = candidate_store.load(speculative_parent_id)
            qa_type = str(dict(parent.get("qa") or {}).get("qa_type", QA_HUMAN_ACCEPTANCE))
            hold_scope = HOLD_DEPENDENCY_CHAIN
            kind = "SPECULATIVE_DEPENDENT"
            parent_job_id = str(parent.get("source_job_id", ""))
            parent_revision = int(parent.get("candidate_revision", 0))
        if not qa_type and not speculative_parent_id:
            return result
        if not strict_result and qa_request.get("required") is not True:
            return result
        task.qa_type = qa_type
        task.hold_scope = hold_scope
        task.machine_verified = strict_result
        if qa_request:
            task.qa_request = qa_request
        candidate = candidate_store.quarantine_and_rollback(
            task,
            kind=kind,
            parent_qa_job_id=parent_job_id,
            parent_candidate_id=speculative_parent_id,
            parent_candidate_revision=parent_revision,
        )
        technical_failure: dict[str, Any] = {}
        if str(result.get("failure_origin", "")) in {"TOOL_INFRA", "HARNESS_CAUSED"}:
            technical_failure = {
                "code": str(result.get("failure_code", "")),
                "origin": str(result.get("failure_origin", "")),
                "stage": str(result.get("failure_stage", "")),
                "reason": str(result.get("failure_reason", "")),
            }
            task.qa_secondary_failures = [technical_failure]
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
        projected_status = (
            AWAITING_DEPENDENCY_QA if speculative_parent_id else "AWAITING_QA"
        )
        return {
            **result,
            "status": projected_status,
            "stage": "AWAITING_QA",
            "success": False,
            "strict_machine_verification": strict_result,
            "verification_status": (
                "VERIFIED" if strict_result else str(result.get("verification_status", ""))
            ),
            "failure_origin": "USER_QA",
            "failure_code": "USER_QA_REQUIRED",
            "user_qa_required": True,
            "qa_type": qa_type,
            "hold_scope": hold_scope,
            "machine_verified": strict_result,
            "candidate_id": candidate["candidate_id"],
            "candidate_manifest": candidate["manifest_path"],
            "candidate_kind": kind,
            "speculative_ready": bool(speculative_parent_id),
            "execution_summary": dict(task.execution_summary),
            "worktree_disposition": "CLEAN_ROLLBACK",
            "technical_failure": technical_failure,
            "secondary_failures": list(dict.fromkeys([
                *list(result.get("secondary_failures") or []),
                *([technical_failure.get("code", "")] if technical_failure else []),
            ])),
        }

    @staticmethod
    def _restore_applied_candidates(
        candidate_store: QACandidateStore | None,
        candidate_ids: list[str],
    ) -> None:
        if candidate_store is None:
            return
        for candidate_id in reversed(candidate_ids):
            candidate_store.restore_base(candidate_id)
        candidate_ids.clear()

    def is_running(self) -> bool:
        with self._guard:
            return bool(self._thread and self._thread.is_alive())

    def status(self) -> dict[str, Any]:
        with self._guard:
            running = bool(self._thread and self._thread.is_alive())
            return {
                "running": running,
                "owner_id": self.owner_id if running else "",
                "processed_in_run": self._processed,
                "last_error": self._last_error,
                "reconciled_interrupted": list(self._reconciled_interrupted),
                "lease_quarantined": self._lease_quarantined,
                "wake_requested": self._wake_requested,
                "retiring": self._retiring,
            }

    def start(self) -> dict[str, Any]:
        # An action can clear a terminal gate while the old dispatcher is between
        # its final queue snapshot and thread exit.  Coordinate wakeup/retirement
        # under one guard so that request can neither be lost nor race a second
        # lease owner into existence.
        while True:
            retiring_thread: threading.Thread | None = None
            with self._guard:
                if self._stopping:
                    return {"started": False, "code": "CONTROL_SHUTTING_DOWN", **self.status_unlocked()}
                if self._thread and self._thread.is_alive():
                    if self._retiring:
                        retiring_thread = self._thread
                    else:
                        self._wake_requested = True
                        return {
                            "started": False,
                            "already_running": True,
                            **self.status_unlocked(),
                        }
                else:
                    break
            # A retiring thread releases (or deliberately quarantines) its lease
            # in finally. Join outside the guard, then retry acquisition.
            retiring_thread.join()

        with self._guard:
            # Another caller may have started a dispatcher after the join.
            if self._stopping:
                return {"started": False, "code": "CONTROL_SHUTTING_DOWN", **self.status_unlocked()}
            if self._thread and self._thread.is_alive():
                self._wake_requested = True
                return {"started": False, "already_running": True, **self.status_unlocked()}
            if not self.harness.acquire_execution_lease():
                return {
                    "started": False,
                    "already_running": False,
                    "code": "HARNESS_BUSY",
                    "running": False,
                }
            worker_probe = getattr(self.harness, "probe_worker_process_active", None)
            task_root = getattr(self.harness, "task_root", None)
            orphaned = (
                self.store.inspect_orphaned_running(
                    task_root,
                    execution_owner_active=False,
                    worker_process_active=(
                        bool(worker_probe()) if callable(worker_probe) else True
                    ),
                    pid_is_active=getattr(
                        self.harness, "probe_execution_pid_active", None
                    ),
                )
                if task_root is not None
                else []
            )
            salvage_required = [
                item for item in orphaned
                if item.get("code") == "STALE_RUNNING_SALVAGE_REQUIRED"
            ]
            if orphaned and len(salvage_required) == len(orphaned):
                try:
                    queue_snapshot = self.store.queue_snapshot()
                    for finding in salvage_required:
                        queued_job = self.store.get_job(str(finding["job_id"]))
                        salvage = self.harness.capture_crash_salvage(
                            queued_job, queue_snapshot
                        )
                        self.store.record_crash_salvage(
                            str(finding["job_id"]),
                            expected_revision=int(finding["job_revision"]),
                            expected_task_id=str(finding["task_id"]),
                            expected_execution_id=str(finding["execution_id"]),
                            salvage=salvage,
                        )
                    orphaned = self.store.inspect_orphaned_running(
                        task_root,
                        execution_owner_active=False,
                        worker_process_active=(
                            bool(worker_probe()) if callable(worker_probe) else True
                        ),
                        pid_is_active=getattr(
                            self.harness, "probe_execution_pid_active", None
                        ),
                    )
                except (QueueError, HarnessServiceError) as exc:
                    self.harness.release_execution_lease()
                    code = f"CRASH_SALVAGE_FAILED:{exc.code}"
                    self._last_error = code
                    return {
                        "started": False,
                        "already_running": False,
                        "code": code,
                        "running": False,
                    }
            if orphaned:
                self.harness.release_execution_lease()
                self._last_error = str(orphaned[0].get("code") or "ORPHANED_RUNNING_EXECUTION")
                return {
                    "started": False,
                    "already_running": False,
                    "code": self._last_error,
                    "running": False,
                    "orphaned_executions": orphaned,
                }
            try:
                self._reconciled_interrupted = self._reconcile_interrupted()
                reconciled_queue = self.store.queue_snapshot()
                for interrupted_id in self._reconciled_interrupted:
                    self.notify_terminal_transition(
                        self.store.get_job(interrupted_id), reconciled_queue
                    )
            except Exception as exc:
                self.harness.release_execution_lease()
                code = "CONTROL_RECONCILIATION_ERROR"
                self._last_error = f"{code}:{scrub_secrets(type(exc).__name__)}"
                return {
                    "started": False,
                    "already_running": False,
                    "code": code,
                    "running": False,
                }
            if reconciled_queue.get("paused"):
                self.harness.release_execution_lease()
                code = (
                    "INTERRUPTED_JOB_REQUIRES_DECISION"
                    if self._reconciled_interrupted
                    else "QUEUE_PAUSED_AFTER_RECONCILIATION"
                )
                self._last_error = code
                return {
                    "started": False,
                    "already_running": False,
                    "code": code,
                    "running": False,
                    "reconciled_interrupted": list(self._reconciled_interrupted),
                }
            self._last_error = ""
            self._processed = 0
            self._lease_quarantined = False
            self._wake_requested = False
            self._retiring = False
            try:
                self._thread = threading.Thread(
                    target=self._run,
                    name="kkm-control-supervisor",
                    daemon=False,
                )
                self._thread.start()
            except Exception as exc:
                self._thread = None
                self.harness.release_execution_lease()
                code = "CONTROL_SUPERVISOR_START_ERROR"
                self._last_error = f"{code}:{scrub_secrets(type(exc).__name__)}"
                return {
                    "started": False,
                    "already_running": False,
                    "code": code,
                    "running": False,
                }
            return {"started": True, "already_running": False, **self.status_unlocked()}

    def status_unlocked(self) -> dict[str, Any]:
        running = bool(self._thread and self._thread.is_alive())
        return {
            "running": running,
            "owner_id": self.owner_id if running else "",
            "processed_in_run": self._processed,
            "last_error": self._last_error,
            "reconciled_interrupted": list(self._reconciled_interrupted),
            "lease_quarantined": self._lease_quarantined,
            "wake_requested": self._wake_requested,
            "retiring": self._retiring,
        }

    def _retire_or_consume_wakeup(self) -> bool:
        """Return True only after atomically closing the wakeup window."""
        with self._guard:
            if self._wake_requested and not self._stopping:
                self._wake_requested = False
                return False
            self._retiring = True
            return True

    def _force_retiring(self) -> None:
        with self._guard:
            self._wake_requested = False
            self._retiring = True

    def _run(self) -> None:
        def recover(code: str, exc: Exception) -> bool:
            recovered = True
            try:
                self._reconciled_interrupted = self._reconcile_interrupted()
            except Exception:
                code = "CONTROL_QUEUE_RECOVERY_ERROR"
                self._lease_quarantined = True
                recovered = False
            self._last_error = f"{code}:{scrub_secrets(type(exc).__name__)}"
            return recovered

        try:
            while True:
                with self._guard:
                    if self._stopping or self._retiring:
                        return
                try:
                    claim_kwargs: dict[str, Any] = {}
                    policy_status = getattr(
                        self.harness, "cumulative_policy_status", None
                    )
                    if callable(policy_status):
                        claim_kwargs = {
                            "workspace": self.harness.working_dir,
                            "integrity_snapshot_provider": lambda q: policy_status(
                                active_baseline_id=str(
                                    q.get("active_baseline_id") or ""
                                ),
                                batch_delta_files=list(
                                    q.get("batch_delta_files") or []
                                ),
                                read_only=True,
                            ),
                        }
                    job = self.store.claim_next(self.owner_id, **claim_kwargs)
                except Exception as exc:
                    code = "CONTROL_QUEUE_CLAIM_ERROR"
                    recovered = True
                    try:
                        self._reconciled_interrupted = self._reconcile_interrupted()
                    except Exception:
                        code = "CONTROL_QUEUE_RECOVERY_ERROR"
                        self._lease_quarantined = True
                        recovered = False
                    self._last_error = f"{code}:{scrub_secrets(type(exc).__name__)}"
                    if recovered and not self._retire_or_consume_wakeup():
                        continue
                    if not recovered:
                        self._force_retiring()
                    return
                if job is None:
                    if self._retire_or_consume_wakeup():
                        return
                    continue

                job_id = job["job_id"]

                def mark_started(task_id: str) -> dict[str, Any] | None:
                    from contextlib import nullcontext
                    repository = getattr(self.store, "repository", None)
                    with repository.hold() if repository is not None else nullcontext():
                        self.store.mark_task_started(job_id, self.owner_id, task_id)
                        if repository is not None:
                            execution = repository.active_execution(job_id)
                            if execution is None:
                                raise HarnessServiceError("MATERIALIZED_EXECUTION_MISSING")
                            previous = repository.latest_attempt(execution["execution_id"], "WORKER")
                            binding = repository.session_binding(
                                execution["execution_id"],
                                execution['effective_policy'].get('resolved_worker', {}).get('worker', execution["request"]["worker"]),
                            )
                            if (previous and binding and binding.get("durable") is True
                                    and dict(previous.get("outcome") or {}).get("status") in {"RUNNING", "UNKNOWN"}):
                                return previous
                            return repository.start_attempt(execution, attempt_id=task_id + "-WORKER",
                                role="WORKER", model=execution['effective_policy'].get('resolved_worker',{}).get('model',execution["request"]["model"]))

                candidate_store: QACandidateStore | None = None
                applied_candidates: list[str] = []
                try:
                    speculative_ids = list(job.get("speculative_candidate_ids") or [])
                    resolved_candidate_id = str(job.get("qa_resolution_candidate_id", ""))
                    candidate_store = self._candidate_store() if (
                        speculative_ids or resolved_candidate_id or job.get("qa_type")
                    ) else None
                    if candidate_store is not None:
                        for candidate_id in speculative_ids:
                            candidate_store.apply_clean(str(candidate_id))
                            applied_candidates.append(str(candidate_id))
                    pre_snapshot = self.store.queue_snapshot()
                    if candidate_store is not None and applied_candidates:
                        pre_snapshot = copy.deepcopy(pre_snapshot)
                        inherited = set(pre_snapshot.get("batch_delta_files") or [])
                        for candidate_id in applied_candidates:
                            candidate = candidate_store.load(candidate_id)
                            inherited.update(
                                dict(candidate.get("delta") or {}).get("changed_files") or []
                            )
                        pre_snapshot["batch_delta_files"] = sorted(inherited)
                    if pre_snapshot.get("commit_policy") == BATCH_FINAL_COMMIT:
                        snapshot_record = self.harness.prepare_job_snapshot(
                            job, pre_snapshot
                        )
                        job = self.store.record_pre_job_snapshot(
                            job_id, self.owner_id, snapshot_record
                        )
                    # A resolved candidate is the current Job's reusable input,
                    # not inherited predecessor state.  Manager invokes this
                    # only after validating the new pre-Job snapshot and before
                    # Worker execution, so the candidate remains Task-owned.
                    initial_delta_applier = None
                    if candidate_store is not None and resolved_candidate_id:
                        initial_delta_applier = lambda: candidate_store.apply_clean(
                            resolved_candidate_id
                        )
                    execute_kwargs: dict[str, Any] = {
                        "on_task_started": mark_started,
                    }
                    if initial_delta_applier is not None:
                        execute_kwargs["initial_delta_applier"] = initial_delta_applier
                    repository = getattr(self.store, "repository", None)
                    if repository is not None:
                        execution = self.harness.materialize_execution(job, repository)
                        job = {**job, "materialized_execution": execution,
                               "execution_context": execution["execution_context"],
                               "request": execution["request"],
                               "current_requirement": execution["current_requirement"]}
                    result = self.harness.execute(job, **execute_kwargs)
                    if (
                        candidate_store is None
                        and dict(result.get("qa_request") or {}).get("required") is True
                    ):
                        candidate_store = self._candidate_store()
                    if candidate_store is not None:
                        result = self._quarantine_machine_verified_result(
                            job,
                            dict(result),
                            candidate_store,
                            speculative_parent_id=(applied_candidates[0] if applied_candidates else ""),
                        )
                    self._restore_applied_candidates(candidate_store, applied_candidates)
                    persisted = self.store.record_result(job_id, self.owner_id, result)
                    self._processed += 1
                    snapshot = self.store.queue_snapshot()
                    self.notify_terminal_transition(persisted, snapshot)
                    if snapshot.get("execution_mode") == EXECUTION_MODE_AUTONOMOUS:
                        if persisted.get("status") == AWAITING_ENRICHMENT:
                            next_attempt = int(persisted.get("outer_attempt", 0)) + 1
                            retry_request = f"auto-retry-{job_id}-outer-{next_attempt}-v1"
                            try:
                                self.store.retry_with_supplement(
                                    job_id,
                                    AUTONOMOUS_RETRY_SUPPLEMENT,
                                    retry_request,
                                )
                                self._last_error = ""
                            except (QueueError, HarnessServiceError) as exc:
                                self._last_error = f"AUTO_RETRY_FAILED:{exc.code}"
                                if self._retire_or_consume_wakeup():
                                    return
                            continue
                        if _safe_autonomous_terminal_skip(persisted):
                            skip_request = f"auto-skip-{job_id}-v1"
                            try:
                                self.store.skip_failed_job(
                                    job_id,
                                    "AUTONOMOUS_RETRY_BUDGET_EXHAUSTED_AFTER_CLEAN_ROLLBACK",
                                    skip_request,
                                )
                                self._last_error = ""
                            except QueueError as exc:
                                self._last_error = f"AUTO_SKIP_FAILED:{exc.code}"
                                if self._retire_or_consume_wakeup():
                                    return
                            continue
                    if (
                        persisted.get("status") == SUCCEEDED
                        and snapshot.get("execution_mode") == EXECUTION_MODE_AUTONOMOUS
                        and snapshot.get("commit_policy") != BATCH_FINAL_COMMIT
                    ):
                        commit_request = f"auto-commit-{job_id}-v1"
                        ack_request = f"auto-ack-{job_id}-v1"
                        try:
                            commit_result = self.harness.commit_verified_job(persisted)
                            self.store.record_commit_result(
                                job_id, commit_request, commit_result
                            )
                            committed_job = self.store.get_job(job_id)
                            self.store.acknowledge_success(
                                job_id,
                                ack_request,
                                expected_revision=int(
                                    committed_job.get("revision", 0)
                                ),
                                batch_id=str(committed_job.get("batch_id", "")),
                            )
                            self._last_error = ""
                        except HarnessServiceError as exc:
                            self.store.record_commit_result(
                                job_id,
                                commit_request,
                                {"status": "FAILED", "error_code": exc.code, "commits": []},
                            )
                            self._last_error = f"AUTO_COMMIT_FAILED:{exc.code}"
                            if self._retire_or_consume_wakeup():
                                return
                            continue
                except CandidateError as exc:
                    try:
                        self._restore_applied_candidates(
                            locals().get("candidate_store"),
                            locals().get("applied_candidates", []),
                        )
                    except Exception:
                        self._lease_quarantined = True
                    try:
                        persisted = self.store.record_result(
                            job_id,
                            self.owner_id,
                            {
                                "status": "AWAITING_QA",
                                "stage": "AWAITING_QA",
                                "success": False,
                                "verification_status": "PARTIAL",
                                "failure_code": exc.code,
                                "failure_origin": "HARNESS_CAUSED",
                                "failure_type": "infrastructure",
                                "fix_scope": "NON_RETRYABLE",
                                "worktree_disposition": "UNKNOWN",
                                "qa_type": "SAFETY_INTEGRITY",
                                "hold_scope": "BATCH",
                                "machine_verified": False,
                            },
                        )
                        self.notify_terminal_transition(
                            persisted, self.store.queue_snapshot()
                        )
                        self._last_error = exc.code
                    except Exception as persist_exc:
                        recover("QA_CANDIDATE_PERSIST_ERROR", persist_exc)
                    return
                except HarnessServiceError as exc:
                    try:
                        self._restore_applied_candidates(
                            locals().get("candidate_store"),
                            locals().get("applied_candidates", []),
                        )
                    except Exception:
                        self._lease_quarantined = True
                    blocked_persisted = False
                    recovered = False
                    try:
                        snapshot_failure = (
                            "SNAPSHOT" in exc.code
                            or exc.code.startswith("CUMULATIVE_")
                            or exc.code.startswith("UNEXPECTED_RUNTIME_")
                            or exc.code.startswith("EXTERNAL_FROZEN_")
                            or exc.code == "BASELINE_HASH_MISMATCH"
                        )
                        if snapshot_failure:
                            persisted = self.store.record_result(
                                job_id,
                                self.owner_id,
                                {
                                    "status": "AWAITING_QA",
                                    "stage": "AWAITING_QA",
                                    "success": False,
                                    "verification_status": "PARTIAL",
                                    "failure_code": exc.code,
                                    "failure_type": "infrastructure",
                                    "fix_scope": "NON_RETRYABLE",
                                    "worktree_disposition": "PRESERVED",
                                },
                            )
                            self.notify_terminal_transition(
                                persisted, self.store.queue_snapshot()
                            )
                        else:
                            persisted = self.store.record_blocked(
                                job_id, self.owner_id, exc.code
                            )
                            self.notify_terminal_transition(
                                persisted, self.store.queue_snapshot()
                            )
                        self._last_error = exc.code
                        blocked_persisted = True
                    except Exception as persist_exc:
                        recovered = recover("CONTROL_BLOCKED_COMMIT_ERROR", persist_exc)
                    if (blocked_persisted or recovered) and not self._retire_or_consume_wakeup():
                        continue
                    if not (blocked_persisted or recovered):
                        self._force_retiring()
                    return
                except QueueError as exc:
                    try:
                        self._restore_applied_candidates(
                            locals().get("candidate_store"),
                            locals().get("applied_candidates", []),
                        )
                    except Exception:
                        self._lease_quarantined = True
                    recovered = recover(exc.code, exc)
                    if recovered and not self._retire_or_consume_wakeup():
                        continue
                    if not recovered:
                        self._force_retiring()
                    return
                except Exception as exc:  # defensive boundary: never lose queue ownership silently
                    try:
                        self._restore_applied_candidates(
                            locals().get("candidate_store"),
                            locals().get("applied_candidates", []),
                        )
                    except Exception:
                        self._lease_quarantined = True
                    code = "CONTROL_SUPERVISOR_ERROR"
                    blocked_persisted = False
                    recovered = False
                    try:
                        self.store.record_blocked(job_id, self.owner_id, code)
                        self._last_error = f"{code}:{scrub_secrets(type(exc).__name__)}"
                        blocked_persisted = True
                    except Exception as persist_exc:
                        recovered = recover("CONTROL_BLOCKED_COMMIT_ERROR", persist_exc)
                    if (blocked_persisted or recovered) and not self._retire_or_consume_wakeup():
                        continue
                    if not (blocked_persisted or recovered):
                        self._force_retiring()
                    return

                try:
                    snapshot = self.store.queue_snapshot()
                except Exception as exc:
                    recovered = recover("CONTROL_QUEUE_SNAPSHOT_ERROR", exc)
                    if recovered and not self._retire_or_consume_wakeup():
                        continue
                    if not recovered:
                        self._force_retiring()
                    return
                if snapshot.get("paused"):
                    if self._retire_or_consume_wakeup():
                        return
        finally:
            if not self._lease_quarantined:
                self.harness.release_execution_lease()
            with self._guard:
                if self._thread is threading.current_thread():
                    self._thread = None
                self._retiring = False
                self._wake_requested = False

    def wait(self, timeout: float | None = None) -> bool:
        with self._guard:
            thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def stop(self) -> None:
        with self._guard:
            self._stopping = True
            self._wake_requested = False
        self.wait()
        close = getattr(self.harness, "close", None)
        if callable(close):
            close()

    def restart(self) -> dict[str, Any]:
        with self._guard:
            if self._thread and self._thread.is_alive() and not self._stopping:
                return {"started": False, "already_running": True, **self.status_unlocked()}
        self.stop()
        restart = getattr(self.harness, "restart", None)
        if callable(restart):
            restart()
        with self._guard:
            self._stopping = False
            self._retiring = False
        return self.start()
