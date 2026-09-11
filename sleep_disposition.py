"""The four operator dispositions for the existing single-worker scheduler."""
GLOBAL_STOP = "GLOBAL_STOP"
HOLD = "HOLD"
QUARANTINE = "QUARANTINE"
CONTINUE = "CONTINUE"

SAFETY_CODES = {"CONTROL_REPOSITORY_INVALID", "CONTROL_STATE_CORRUPT", "BASELINE_HASH_MISMATCH",
                "CUMULATIVE_BASELINE_MISSING", "EXTERNAL_FROZEN_BASELINE_CHANGED",
                "UNEXPECTED_RUNTIME_DIRTY_FILES", "EXECUTION_SURFACE_CORRUPT", "DESTRUCTIVE_SAFETY_STOP"}


def disposition(job, queue=None):
    queue = queue or {}
    result = job.get("last_result") or {}
    code = str(result.get("failure_code") or job.get("failure_code") or "")
    status = job.get("status", "")
    if (queue.get("unexpected_runtime_dirty_files") or queue.get("baseline_declaration_integrity") is False
            or queue.get("external_frozen_integrity") is False or code in SAFETY_CODES
            or job.get("qa_type") == "SAFETY_INTEGRITY"):
        return GLOBAL_STOP
    if (result.get("failure_origin") == "CONTRACT_BLOCKED" or
            any(term in code for term in ("BUSINESS", "PII_MEANING", "API_MEANING", "DB_MEANING", "USER_DECISION"))
            or status in {"BLOCKED_BY_DEPENDENCY", "AWAITING_DEPENDENCY_QA"}):
        return HOLD
    if status in {"AWAITING_QA", "AWAITING_ENRICHMENT", "FAILED_FINAL", "BLOCKED", "INTERRUPTED"}:
        return QUARANTINE
    return CONTINUE


def may_bypass_gate(queue, blocker):
    return (queue.get("execution_mode") == "AUTONOMOUS" and not queue.get("operator_paused")
            and queue.get("gate_reason") != "SUCCESS_ACK_REQUIRED"
            and disposition(blocker, queue) in {HOLD, QUARANTINE})
