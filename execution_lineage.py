"""Canonical execution-attempt accounting for durable Queue jobs.

The legacy ``outer_attempt`` field is retained as the total execution ordinal.
New work is authorized by a reservation that records normal and technical
recovery provenance and is consumed exactly once by Task creation. Product
attempts and explicitly budgeted technical recoveries keep separate budgets.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Mapping


NORMAL = "NORMAL"
TECHNICAL_RECOVERY = "TECHNICAL_RECOVERY"
VERIFICATION_RECOVERY = "VERIFICATION_RECOVERY"
ATTEMPT_KINDS = {NORMAL, TECHNICAL_RECOVERY, VERIFICATION_RECOVERY}


class AttemptAccountingError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail
        super().__init__(code + (f": {detail}" if detail else ""))


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _sha(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _technical_events(job: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        item for item in list(job.get("history") or [])
        if isinstance(item, Mapping)
        and item.get("event") == "AWAITING_QA_TECHNICAL_RETRY_QUEUED"
        and item.get("request_ref")
        and isinstance(item.get("expected_revision"), int)
    ]


def accounting(job: Mapping[str, Any]) -> dict[str, Any]:
    """Return canonical counters without legitimizing historical overflow."""
    execution = int(job.get("outer_attempt", 0))
    normal_budget = int(job.get("normal_attempt_budget", job.get("max_outer_attempts", 1)))
    technical_budget = int(job.get("technical_recovery_budget", 1))
    reservations = [
        item for item in list(job.get("attempt_reservations") or [])
        if isinstance(item, Mapping) and not item.get("cancelled_at")
    ]
    normal_reserved = sum(
        1 for item in reservations
        if item.get("attempt_kind") in {NORMAL, VERIFICATION_RECOVERY}
    )
    technical_reserved = sum(
        1 for item in reservations if item.get("attempt_kind") == TECHNICAL_RECOVERY
    )
    legacy_technical = int(job.get("technical_recovery_attempt_count", 0))
    technical_used = max(technical_reserved, legacy_technical)
    normal_used = max(normal_reserved, execution - technical_used)
    if technical_used > technical_budget:
        raise AttemptAccountingError("TECHNICAL_RETRY_BUDGET_EXHAUSTED")
    effective_limit = normal_budget + technical_budget
    if execution > effective_limit:
        raise AttemptAccountingError("ATTEMPT_ACCOUNTING_INCONSISTENT")
    if normal_used > normal_budget:
        raise AttemptAccountingError("OUTER_ATTEMPT_BUDGET_EXHAUSTED")
    return {
        "execution_attempt": execution,
        "normal_attempt_no": normal_used,
        "normal_attempt_budget": normal_budget,
        "technical_recovery_attempt_no": technical_used,
        "technical_recovery_budget": technical_budget,
        "effective_execution_limit": effective_limit,
        "legacy_compatible": bool(execution > normal_budget),
    }


def reserve_execution_attempt(
    job: dict[str, Any],
    queue: Mapping[str, Any],
    *,
    attempt_kind: str,
    provenance: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """Reserve one execution ordinal without creating a Task.

    The caller must persist the returned mutation under the Queue lock. Revision
    values in the reservation describe the state that authorized the claim.
    """
    if attempt_kind not in ATTEMPT_KINDS:
        raise AttemptAccountingError("ATTEMPT_KIND_INVALID")
    state = accounting(job)
    proof = dict(provenance or {})
    request_ref = str(proof.get("request_ref", ""))
    expected_job_revision = int(job.get("revision", -1))
    expected_queue_revision = int(queue.get("revision", -1))
    if "expected_job_revision" in proof and int(proof["expected_job_revision"]) != expected_job_revision:
        raise AttemptAccountingError("STALE_JOB_REVISION")
    if "expected_queue_revision" in proof and int(proof["expected_queue_revision"]) != expected_queue_revision:
        raise AttemptAccountingError("STALE_QUEUE_REVISION")
    for existing in list(job.get("attempt_reservations") or []):
        if request_ref and existing.get("request_ref") == request_ref:
            return dict(existing), True

    normal_no = state["normal_attempt_no"]
    technical_no = state["technical_recovery_attempt_no"]
    if attempt_kind == TECHNICAL_RECOVERY:
        technical_no += 1
        if technical_no > state["technical_recovery_budget"]:
            raise AttemptAccountingError("TECHNICAL_RETRY_BUDGET_EXHAUSTED")
    else:
        normal_no += 1
        if normal_no > state["normal_attempt_budget"]:
            raise AttemptAccountingError("OUTER_ATTEMPT_BUDGET_EXHAUSTED")
    execution = state["execution_attempt"] + 1
    if execution > state["effective_execution_limit"]:
        raise AttemptAccountingError("EFFECTIVE_EXECUTION_LIMIT_EXHAUSTED")
    seed = {
        "job_id": job.get("job_id", ""),
        "job_revision": expected_job_revision,
        "queue_revision": expected_queue_revision,
        "attempt_kind": attempt_kind,
        "execution_attempt": execution,
        "request_ref": request_ref,
    }
    reservation = {
        "reservation_id": "RSV-" + _sha(seed)[:24].upper(),
        "attempt_kind": attempt_kind,
        "execution_attempt": execution,
        "normal_attempt_no": normal_no,
        "normal_attempt_budget": state["normal_attempt_budget"],
        "technical_recovery_attempt_no": technical_no,
        "technical_recovery_budget": state["technical_recovery_budget"],
        "effective_execution_limit": state["effective_execution_limit"],
        "provenance": proof,
        "request_ref": request_ref,
        "reserved_at": _now(),
        "expected_job_revision": expected_job_revision,
        "expected_queue_revision": expected_queue_revision,
        "consumed_task_id": "",
        "consumed_at": "",
    }
    job.setdefault("attempt_reservations", []).append(reservation)
    job["active_attempt_reservation"] = reservation
    job["normal_attempt_budget"] = state["normal_attempt_budget"]
    job["technical_recovery_budget"] = state["technical_recovery_budget"]
    return reservation, False


def consume_reservation(job: dict[str, Any], task_id: str) -> dict[str, Any]:
    active = dict(job.get("active_attempt_reservation") or {})
    reservation_id = str(active.get("reservation_id", ""))
    if not reservation_id:
        raise AttemptAccountingError("ATTEMPT_RESERVATION_MISSING")
    for item in job.get("attempt_reservations") or []:
        if item.get("reservation_id") != reservation_id:
            continue
        consumed = str(item.get("consumed_task_id", ""))
        if consumed and consumed != task_id:
            raise AttemptAccountingError("ATTEMPT_RESERVATION_ALREADY_CONSUMED")
        if not consumed:
            item["consumed_task_id"] = task_id
            item["consumed_at"] = _now()
        job["active_attempt_reservation"] = dict(item)
        return dict(item)
    raise AttemptAccountingError("ATTEMPT_RESERVATION_MISSING")


def lineage_status(job: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return {"valid": True, "code": "", **accounting(job)}
    except AttemptAccountingError as exc:
        return {"valid": False, "code": exc.code}
