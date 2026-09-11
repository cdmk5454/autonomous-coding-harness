"""Compact operator state, durable semantic dedup, and failure isolation."""
from datetime import datetime
import re
import threading
from control_repository import digest

_delivery_lock = threading.RLock()


def operator_label(value):
    request = value.get("request") or {}
    explicit = value.get("operator_label") or request.get("operator_label") or value.get("short_title")
    if explicit:
        return " ".join(str(explicit).split())[:120]
    identity = str(value.get("client_job_id") or request.get("client_job_id") or value.get("job_id") or value.get("id") or value.get("task_id") or "Job")
    match = re.search(r"\bJ\d+(?:-R\d+)?(?=-|$)", identity)
    if match:
        title = identity[match.end():].strip("-").replace("-", " ")
        return (match.group() + (" — " + title if title else ""))[:120]
    return identity[:120]


def action_for(job, queue):
    result = job.get("last_result") or {}
    if int(result.get("open_decision_count", 0)) > 0:
        return "Resolve the recorded open decision(s)"
    origin = result.get("failure_origin", "")
    independent_decision = origin == "CONTRACT_BLOCKED" or bool(job.get("independent_human_decision"))
    dependent = (job.get("status") in {"BLOCKED_BY_DEPENDENCY", "AWAITING_DEPENDENCY_QA"}
                 or bool(job.get("blocked_by_job_id") and job["blocked_by_job_id"] != job.get("job_id")))
    if dependent and not independent_decision:
        return ""
    if result.get("recovery_pending") or job.get("technical_recovery_pending"):
        return ""
    if independent_decision:
        return "Resolve the recorded business/API/DB/PII decision"
    code = str(result.get("failure_code", ""))
    if (origin in {"HARNESS_CAUSED", "HARNESS_CONTROL"} or result.get("failure_stage") == "CONTROL") and code:
        return "Harness recovery required"
    if job.get("status") == "SUCCEEDED" and queue.get("gate_reason") == "SUCCESS_ACK_REQUIRED":
        return "Read result and acknowledge success"
    if (job.get("qa_type") or job.get("candidate_id") or result.get("user_qa_required") or dict(result.get("qa_request") or {}).get("required")):
        return "Review the candidate and record the required QA decision"
    actions = job.get("legal_next_actions") or []
    return str(actions[0])[:120] if actions else ""


def terminal_projection(job, queue):
    result = job.get("last_result") or {}
    root = str(job.get("blocked_by_job_id") or "")
    if not root and job.get("status") in {"BLOCKED_BY_DEPENDENCY", "AWAITING_DEPENDENCY_QA"}:
        root = str(queue.get("blocked_by_job_id") or next(iter(job.get("depends_on") or []), ""))
    action = action_for({**job, "blocked_by_job_id": root}, queue)
    dependent = bool(root and root != job.get("job_id")) and not action
    batch_id = str(job.get("batch_id") or "")
    batch = next((b for b in (queue.get("batches") or {}).values() if b.get("batch_id") == batch_id), {})
    total = len(batch.get("job_ids") or [])
    sequence = int(job.get("sequence") or 0)
    batch_label = f"Batch {sequence}/{total}" if total and 0 < sequence <= total else "Batch " + (batch_id or "unknown")
    origin = str(result.get("failure_origin") or "")
    if origin == "HARNESS_CAUSED":
        origin = "HARNESS_CONTROL"
    candidate = "PRESERVED_UNVERIFIED" if result.get("worktree_disposition") in {"PRESERVED", "UNKNOWN"} else str(result.get("worktree_disposition") or "NO_PRODUCT_CHANGE")
    fields = {"job_id": str(job.get("job_id", "")), "event_class": "TERMINAL", "status": str(job.get("status", "")),
              "stage": str(result.get("failure_stage") or result.get("stage") or ""),
              "failure_origin": origin, "failure_code": str(next(iter(result.get("commit_blockers") or []), "") or result.get("failure_code") or ""),
              "gate": str(queue.get("gate_reason") or ""), "hold_scope": str(job.get("hold_scope") or ""),
              "blocked_by_job_id": root, "root_failure_code": str(job.get("root_failure_code") or ""),
              "user_action_required": bool(action), "terminal_disposition": candidate,
              "product_readiness": str(result.get("product_readiness") or "FINALIZATION_PENDING"),
              "open_decision_count": int(result.get("open_decision_count", 0)),
              "root_blocker": str(queue.get("blocked_by_job_id") or "")}
    integrity = "OK" if (not queue.get("unexpected_runtime_dirty_files") and
            queue.get("baseline_declaration_integrity") is not False and queue.get("external_frozen_integrity") is not False) else "CHECK_REQUIRED"
    message = "\n".join([operator_label(job), batch_label, "", "Status: " + fields["status"],
        "Stage: " + (fields["stage"] or "—"), "Failure: " + (fields["failure_code"] or "—"),
        "Origin: " + (origin or "—"), "Candidate: " + candidate,
        "Product: " + fields["product_readiness"],
        "Open decisions: " + str(fields["open_decision_count"]), "Integrity: " + integrity,
        *( ["Blocked by: " + root] if root else []),
        "User action: " + (action or "None")])
    return {**fields, "batch_id": batch_id, "message": message, "suppress_dependent_alert": dependent,
            "continuation": "INDEPENDENT_JOBS_CONTINUE" if job.get("hold_scope") in {"JOB", "DEPENDENCY_CHAIN"} else "AUTOMATIC_CONTINUATION_STOPPED"}


def semantic_fingerprint(projection):
    keys = ("job_id", "event_class", "status", "stage", "failure_origin", "failure_code", "gate", "hold_scope",
            "blocked_by_job_id", "root_failure_code", "user_action_required", "terminal_disposition", "root_blocker", "gates",
            "product_readiness", "open_decision_count")
    return digest({key: projection.get(key) for key in keys})


def deliver_semantic(projection, repository, sender, *, heartbeat=False):
    try:
        if projection.get("suppress_dependent_alert"):
            return {"dependent_suppressed": True, "delivered": False}
        if repository.global_stop().get("active"):
            return {"quiet_mode": True, "delivered": False}
        subject = projection["job_id"] + ":" + projection["event_class"]
        fingerprint = semantic_fingerprint(projection)
        with _delivery_lock:
            previous = repository.notification("telegram", subject)
            if previous and previous["fingerprint"] == fingerprint and not heartbeat:
                return {"duplicate_suppressed": True, "delivered": False, "fingerprint": fingerprint}
            evidence = sender(projection["message"])
            if evidence.get("delivered") is True:
                repository.record_notification("telegram", subject, fingerprint, projection)
            return {**evidence, "fingerprint": fingerprint, "failure_origin": "NOTIFICATION" if not evidence.get("delivered") else ""}
    except Exception as exc:
        return {"delivered": False, "failure_origin": "NOTIFICATION", "failure_code": "NOTIFICATION_PROJECTION_ERROR", "diagnostic": type(exc).__name__}


def progress_fields(snapshot):
    job = snapshot.get("job") or {}
    stage = snapshot.get("stage") or {}
    return {"job_id": str(job.get("id") or job.get("task_id") or ""), "event_class": "PROGRESS",
            "status": job.get("status"), "stage": stage.get("name"), "gates": snapshot.get("gates"),
            "failure_code": (snapshot.get("execution_health") or {}).get("failure_code", ""),
            "user_action_required": bool(snapshot.get("user_action_required")),
            "product_readiness": str(snapshot.get("product_readiness", "")),
            "open_decision_count": int(snapshot.get("open_decision_count", 0))}


def render_compact(snapshot, *, heartbeat=False):
    job = snapshot.get("job") or {}
    stage = snapshot.get("stage") or {}
    gates = snapshot.get("gates") or {}
    def marker(value):
        return "✓" if value in {"PASS", "REVIEW_PASS", "VERIFIED"} else "!" if value in {"FAIL", "FAILED", "REVIEW_FAIL", "ERROR"} else "○"
    names = [("worker","W" if heartbeat else "WORKER"),("build","B" if heartbeat else "BUILD"),
             ("test","T" if heartbeat else "TEST"),("review","R" if heartbeat else "REVIEW"),("verification","V" if heartbeat else "VERIFY")]
    lines = [str(job.get("operator_label") or operator_label(job))]
    client = str(job.get("client_job_id") or "")
    if client and client != lines[0] and not heartbeat:
        lines.append(client[:128])
    if heartbeat:
        lines.append(f"{stage.get('name', 'RUNNING')} · {int(job.get('elapsed_seconds') or 0)//60}m")
        if snapshot.get("user_action_required"):
            lines.append(f"DECISION REQUIRED · {int(snapshot.get('open_decision_count', 0))} open")
    lines.extend(["", " ".join(f"[{marker(gates.get(key))} {label}]" for key,label in names)])
    if not heartbeat:
        lines.extend(["현재: " + str(stage.get("name") or "—"), "Task: " + str(job.get("task_id") or "—")])
        limit = int(stage.get("attempt_limit") or 0)
        if limit:
            lines.append(f"Attempt: {int(stage.get('attempt') or 1)}/{limit}")
        batch = snapshot.get("batch") or {}
        if batch:
            total = int(batch.get("total") or 0)
            current = int(batch.get("current") or 0)
            lines.append(f"Batch {current}/{total}" if 0 < current <= total else "Batch " + str(batch.get("id") or "unknown"))
        lines.append("Product: " + str(snapshot.get("product_readiness") or "FINALIZATION_PENDING"))
        if snapshot.get("user_action_required"):
            lines.append(f"Decision required: {int(snapshot.get('open_decision_count', 0))}")
    return "\n".join(lines)
