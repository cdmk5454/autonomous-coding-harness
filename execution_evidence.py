"""Additive 0.9 execution summary derived from existing Task evidence."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Mapping


EXECUTION_EVIDENCE_SCHEMA_VERSION = 1


def _duration_seconds(started: str, finished: str) -> int | None:
    try:
        return max(0, int((datetime.fromisoformat(finished) - datetime.fromisoformat(started)).total_seconds()))
    except (TypeError, ValueError):
        return None


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


NOT_CAPTURED = "NOT_CAPTURED"
_STAGE_CATEGORIES = {
    "WORKER": "worker_seconds",
    "GIT": "git_seconds",
    "BUILD": "build_test_seconds",
    "TEST": "build_test_seconds",
    "REVIEW": "review_seconds",
    "VERIFY": "verification_seconds",
    "WORKER-DONE": "harness_maintenance_seconds",
}


def _task_field_seconds(task: Any, name: str) -> int | str:
    try:
        value = int(getattr(task, name, -1))
    except (TypeError, ValueError):
        return NOT_CAPTURED
    return value if value >= 0 else NOT_CAPTURED


def stage_timing(task: Any) -> dict[str, Any]:
    """Derive the 0.99.1 timing breakdown from already-durable timestamps.

    Only trustworthy measurements are reported; anything not persisted by the
    current runtime is marked NOT_CAPTURED rather than estimated.
    """
    events = [
        item for item in list(getattr(task, "progress_events", []) or [])
        if isinstance(item, Mapping) and item.get("stage") and item.get("at")
    ]
    ordered = sorted(events, key=lambda item: str(item.get("at")))
    totals: dict[str, int] = {}
    for index, item in enumerate(ordered):
        category = _STAGE_CATEGORIES.get(str(item.get("stage")).upper())
        if category is None:
            category = "harness_maintenance_seconds"
        start = _parse_iso(str(item.get("at")))
        end = (
            _parse_iso(str(ordered[index + 1].get("at"))) if index + 1 < len(ordered)
            else _parse_iso(str(getattr(task, "ended_at", "") or getattr(task, "updated_at", "")))
        )
        if start is None or end is None or end < start:
            continue
        totals[category] = totals.get(category, 0) + int((end - start).total_seconds())
    reviewer_seconds = sum(
        value for item in list(getattr(task, "reviewer_invocations", []) or [])
        if isinstance(item, Mapping)
        for value in [_duration_seconds(str(item.get("started_at", "")), str(item.get("ended_at", "")))]
        if value is not None
    )
    timing: dict[str, Any] = {
        "queue_wait_seconds": _task_field_seconds(task, "queue_wait_seconds"),
        "admission_control_seconds": _task_field_seconds(task, "admission_control_seconds"),
        "worker_seconds": totals.get("worker_seconds", 0),
        "build_test_seconds": totals.get("build_test_seconds", 0),
        "review_seconds": max(totals.get("review_seconds", 0), int(reviewer_seconds)),
        "verification_seconds": totals.get("verification_seconds", 0),
        "git_seconds": totals.get("git_seconds", 0),
        "harness_maintenance_seconds": totals.get("harness_maintenance_seconds", 0),
        # Recovery durations are not persisted per replacement; the count is.
        "technical_recovery_seconds": NOT_CAPTURED,
        # Human wait happens after this summary is built; the queue records it
        # as job acknowledgement_wait_seconds at acknowledge time.
        "human_wait_seconds": NOT_CAPTURED,
    }
    return timing


def job_admission_timing(job: Any) -> dict[str, int]:
    """Queue-side wait/admission from the job's own history events."""
    history = [
        item for item in list(dict(job or {}).get("history") or [])
        if isinstance(item, Mapping) and item.get("at")
    ]

    def first(event: str):
        return next((item for item in history if item.get("event") == event), None)

    def last(event: str):
        found = [item for item in history if item.get("event") == event]
        return found[-1] if found else None

    result = {"queue_wait_seconds": -1, "admission_control_seconds": -1}
    enqueued, claimed = first("ENQUEUED"), first("CLAIMED")
    started = last("TASK_STARTED")
    if enqueued and claimed:
        wait = _duration_seconds(str(enqueued.get("at")), str(claimed.get("at")))
        if wait is not None:
            result["queue_wait_seconds"] = wait
    claim_for_admission = last("CLAIMED") or claimed
    if claim_for_admission and started:
        admission = _duration_seconds(str(claim_for_admission.get("at")), str(started.get("at")))
        if admission is not None:
            result["admission_control_seconds"] = admission
    return result


def build_execution_summary(task: Any) -> dict[str, Any]:
    history = [dict(item) for item in list(getattr(task, "attempt_history", []) or []) if isinstance(item, Mapping)]
    reviewer_invocations = [dict(item) for item in list(getattr(task, "reviewer_invocations", []) or []) if isinstance(item, Mapping)]
    runtime_invocations = [
        dict(item) for item in list(
            getattr(task, "execution_runtime_history", []) or []
        ) if isinstance(item, Mapping)
    ]
    worker_invocations = [
        dict(item.get("worker_execution_evidence") or {})
        for item in history
        if item.get("worker_execution_evidence") or item.get("actual_worker")
    ]
    planner_count = 1 if bool(getattr(task, "planner_brief", {})) else 0
    tester_count = 1 if str(getattr(task, "test_status", "SKIPPED")) != "SKIPPED" else 0
    route_counts = Counter(
        f"{item.get('actual_worker', '')}:{item.get('actual_model', '')}"
        for item in history if item.get("actual_worker")
    )
    started = str(getattr(task, "started_at", "") or getattr(task, "created_at", ""))
    finished = str(getattr(task, "ended_at", "") or getattr(task, "updated_at", ""))
    lineage: list[dict[str, Any]] = []

    def add_lineage(source: str, item: Mapping[str, Any], ordinal: int) -> None:
        lineage.append({
            "source": source,
            "ordinal": ordinal,
            "event": str(item.get("event") or item.get("type") or item.get("status") or source),
            "timestamp": str(item.get("at") or item.get("timestamp") or item.get("started_at") or item.get("ended_at") or ""),
            "stage": str(item.get("stage") or item.get("failure_stage") or ""),
            "execution_id": str(item.get("execution_id") or item.get("invocation_id") or ""),
            "attempt": int(item.get("attempt") or item.get("ordinal") or 0),
            "role": str(item.get("role") or item.get("execution_role") or ""),
            "provider": str(item.get("provider") or item.get("actual_worker") or ""),
            "model": str(item.get("model") or item.get("actual_model") or ""),
            "failure_code": str(item.get("failure_code") or ""),
            "retry_reason": str(item.get("retry_reason") or item.get("recovery_reason") or ""),
            "artifact_ref": str(item.get("artifact_ref") or item.get("checkpoint_manifest") or item.get("candidate_manifest") or ""),
            "state_transition": str(item.get("state_transition") or item.get("worktree_disposition") or ""),
            "result": str(item.get("result") or item.get("review_status") or item.get("status") or ""),
            "failure_fingerprint": str(item.get("failure_fingerprint") or ""),
            "recovery_action": str(item.get("recovery_action") or item.get("retry_strategy") or ""),
            "duration_seconds": _duration_seconds(
                str(item.get("started_at") or ""), str(item.get("ended_at") or "")
            ),
        })

    for ordinal, item in enumerate(list(getattr(task, "progress_events", []) or []), 1):
        if isinstance(item, Mapping):
            add_lineage("PROGRESS", item, ordinal)
    for ordinal, item in enumerate(runtime_invocations, 1):
        add_lineage("RUNTIME", item, ordinal)
    current_runtime = dict(getattr(task, "execution_runtime", {}) or {})
    if current_runtime.get("execution_id") and not any(
        item.get("execution_id") == current_runtime.get("execution_id")
        for item in runtime_invocations
    ):
        add_lineage("RUNTIME_CURRENT", current_runtime, len(runtime_invocations) + 1)
    for ordinal, item in enumerate(history, 1):
        add_lineage("ATTEMPT", item, ordinal)
    for ordinal, item in enumerate(reviewer_invocations, 1):
        add_lineage("REVIEWER", item, ordinal)
    lineage.sort(key=lambda item: (item["timestamp"] or "9999", item["source"], item["ordinal"]))
    return {
        "schema_version": EXECUTION_EVIDENCE_SCHEMA_VERSION,
        "identity": {
            "job_id": str(getattr(task, "job_id", "")),
            "task_id": str(getattr(task, "task_id", "")),
            "batch_id": str(getattr(task, "batch_id", "")),
            "client_job_id": str(getattr(task, "client_job_id", "")),
            "queue_generation": int(getattr(task, "queue_generation", 0)),
            "baseline_id": str(getattr(task, "active_baseline_id", "")),
            "snapshot_id": str(getattr(task, "pre_job_snapshot_id", "")),
        },
        "execution": {
            "started_at": started,
            "finished_at": finished,
            "duration_seconds": _duration_seconds(started, finished),
            "outer_attempt": int(getattr(task, "outer_attempt", 0)),
            "job_global_attempt_count": int(
                getattr(task, "execution_attempt", 0)
                or getattr(task, "outer_attempt", 0)
            ),
            "global_attempt_limit": int(
                getattr(task, "effective_execution_limit", 0)
                or getattr(task, "normal_attempt_budget", 0)
            ),
            "attempt_kind": str(getattr(task, "attempt_kind", "")),
            "inner_attempt_count": len(history),
            "worker_local_attempt_count": len(history),
            "worker_local_attempt_limit_configured": int(
                getattr(task, "worker_attempt_limit_configured", 0)
            ),
            "worker_local_attempt_limit_effective": int(
                getattr(task, "worker_attempt_limit_effective", 0)
                or getattr(task, "progress_attempt_limit", 0)
            ),
            "planner_invocations": planner_count,
            "worker_invocations": len(worker_invocations),
            "reviewer_invocations": len(reviewer_invocations),
            "tester_invocations": tester_count,
            "reviewer_recovery_count": int(getattr(task, "reviewer_recovery_count", 0)),
            "reviewer_infra_retry_count": int(getattr(task, "reviewer_recovery_count", 0)),
            "technical_recovery_count": int(getattr(task, "technical_recovery_attempt_no", 0)),
            "rollback_count": sum(1 for item in history if "ROLLBACK" in str(item.get("worktree_disposition", ""))),
            "runtime_invocations": len(runtime_invocations),
            "failure_fingerprints": list(dict.fromkeys(
                str(item.get("failure_fingerprint"))
                for item in history if item.get("failure_fingerprint")
            )),
        },
        "timing": stage_timing(task),
        "runtime": {
            "current": dict(getattr(task, "execution_runtime", {}) or {}),
            "invocations": runtime_invocations,
        },
        "raw_lineage": lineage,
        "routing": {
            "requested_worker": str(getattr(task, "requested_worker", "")),
            "requested_model": str(getattr(task, "requested_model", "")),
            "actual_routes": dict(route_counts),
            "review_policy": dict(getattr(task, "review_policy", {}) or {}),
        },
        "verification": {
            "build": str(dict(getattr(task, "build", {}) or {}).get("status", "")),
            "test": "NOT_REQUIRED" if not bool(getattr(task, "test_required", False)) else str(getattr(task, "test_status", "")),
            "review": str(getattr(task, "review_status", "")),
            "verification": str(getattr(task, "verification_status", "")),
            "failure_origin": str(getattr(task, "failure_origin", "")),
            "failure_code": str(getattr(task, "failure_code", "")),
            "worktree_disposition": str(getattr(task, "worktree_disposition", "")),
        },
        "changes": {
            "task_owned_changed_files": list(getattr(task, "task_owned_changed_files", []) or []),
            "inherited_batch_delta_files": list(getattr(task, "inherited_batch_delta_files", []) or []),
            "changed_file_sha256": dict(getattr(task, "changed_file_sha256", {}) or {}),
        },
        "artifacts": {
            "context_manifest": str(getattr(task, "context_manifest_path", "")),
            "checkpoint_manifest": str(getattr(task, "checkpoint_manifest", "")),
            "snapshot_manifest": str(getattr(task, "pre_job_snapshot_manifest", "")),
            "artifact_files": list(getattr(task, "artifact_files", []) or []),
        },
        "policy": {
            "policy_refs": list(getattr(task, "policy_refs", []) or []),
            "policy_overlays": list(getattr(task, "policy_overlays", []) or []),
            "effective_policy": dict(getattr(task, "effective_policy", {}) or {}),
        },
        "retention": {
            "policy": "PRESERVE_UNTIL_1_0_KNOWLEDGE_BOOTSTRAP",
            "destructive_cleanup_allowed": False,
            "large_evidence_storage": "ARTIFACT_REFERENCE_AND_SHA256",
        },
        "human_intervention": {
            "required": str(getattr(task, "status", "")) == "AWAITING_QA",
            "reason": str(getattr(task, "user_intervention_reason", "")),
        },
        "usage": {
            "input_tokens": "NOT_AVAILABLE",
            "output_tokens": "NOT_AVAILABLE",
            "cost": "NOT_AVAILABLE",
            "estimated": False,
        },
    }


def metric_calculability(summary: Mapping[str, Any]) -> dict[str, str]:
    return {
        "duration_seconds": "DERIVABLE" if dict(summary.get("execution") or {}).get("duration_seconds") is not None else "MISSING",
        "invocation_counts": "ALREADY_PERSISTED",
        "routing": "ALREADY_PERSISTED",
        "retry_recovery_rollback": "DERIVABLE",
        "verification_terminal": "ALREADY_PERSISTED",
        "changed_files_hashes": "ALREADY_PERSISTED",
        "token_usage": "UNRELIABLE" if dict(summary.get("usage") or {}).get("input_tokens") == "NOT_AVAILABLE" else "ALREADY_PERSISTED",
        "cost": "UNRELIABLE" if dict(summary.get("usage") or {}).get("cost") == "NOT_AVAILABLE" else "ALREADY_PERSISTED",
    }
