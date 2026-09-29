"""Normalize human gates into durable candidate/contract/policy/deferred decisions."""

from __future__ import annotations

from typing import Any, Mapping

from control_repository import digest


def decision_requests(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    qa = dict(result.get("qa_request") or {})
    requests: list[dict[str, Any]] = []
    for item in qa.get("deferred_contracts") or result.get("deferred_contracts") or []:
        if not isinstance(item, Mapping):
            continue
        question = str(item.get("decision") or item.get("question") or "").strip()
        if question:
            requests.append({
                "decision_type": "DEFERRED", "question": question,
                "fingerprint": digest({"type": "DEFERRED", "item": dict(item)}),
                "deferred_contract": dict(item),
            })
    if requests:
        return requests
    if qa.get("required") is not True:
        return []
    qa_type = str(qa.get("qa_type") or result.get("qa_type") or "").upper()
    if qa_type == "DEFERRED_DECISION":
        decision_type = "DEFERRED"
    elif "POLICY" in qa_type or qa_type == "SECURITY_APPROVAL":
        decision_type = "POLICY"
    elif any(token in qa_type for token in ("CONTRACT", "DB_DECISION", "API_DECISION", "BUSINESS_DECISION")):
        decision_type = "CONTRACT"
    elif result.get("candidate_id") or qa.get("candidate_id") or qa_type == "CANDIDATE_DECISION":
        decision_type = "CANDIDATE"
    else:
        return []
    question = str(
        qa.get("question") or qa.get("reason") or result.get("failure_reason")
        or f"Resolve {qa_type or decision_type} for finalization"
    ).strip()
    return [{
        "decision_type": decision_type,
        "question": question,
        "fingerprint": digest({
            "type": decision_type, "qa_type": qa_type, "question": question,
            "candidate_id": result.get("candidate_id") or qa.get("candidate_id") or "",
        }),
        "qa_type": qa_type,
        "candidate_id": result.get("candidate_id") or qa.get("candidate_id") or "",
    }]



def operator_projection(job: Mapping[str, Any]) -> dict[str, Any]:
    """Read-only explanation. Never grants recovery/publication authority.

    Only explicit authoritative fields establish canonical application or an
    allowed action. Missing fields remain UNKNOWN, not optimistic YES/NO.
    """
    raw_result = job.get("last_result")
    result = dict(raw_result) if isinstance(raw_result, Mapping) else {}
    raw_qa = result.get("qa_request")
    qa = dict(raw_qa) if isinstance(raw_qa, Mapping) else {}
    code = str(result.get("failure_code") or "")
    qa_type = str(qa.get("qa_type") or result.get("qa_type") or job.get("qa_type") or "")
    stage = str(result.get("failure_stage") or result.get("stage") or "UNKNOWN")
    if code.startswith(("SECURITY_", "CREDENTIAL_", "PERMISSION_", "SECRET_")) or qa_type == "SECURITY_APPROVAL":
        category = "SECURITY_APPROVAL"
    elif code.startswith(("PUBLICATION_", "CANONICAL_", "PROMOTION_")) or stage == "PUBLICATION":
        category = "PUBLICATION_RECOVERY"
    elif (code.startswith(("CONTROL_", "HARNESS_", "RUNTIME_", "TESTER_RUNTIME_", "TESTER_RECEIPT", "TESTER_WALL_CLOCK_", "REVIEW_EXECUTION_UNAVAILABLE", "REVIEW_SCHEMA_INVALID", "REVIEW_OUTPUT_PARSE_FAILED", "REVIEWER_INFRA_", "HERMES_", "OPENCODE_", "SUPERVISOR_"))
          or result.get("failure_origin") in {"TOOL_INFRA", "HARNESS", "HARNESS_CAUSED", "HARNESS_CONTROL", "INFRASTRUCTURE"}):
        category = "HARNESS_RECOVERY"
    elif qa_type in {"BUSINESS_QA", "BUSINESS_DECISION", "CANDIDATE_DECISION", "USER_ACCEPTANCE"}:
        category = "BUSINESS_QA"
    else:
        category = "OTHER_HUMAN_DECISION"
    candidate_id = str(job.get("candidate_id") or result.get("candidate_id") or "")
    disposition = str(result.get("worktree_disposition") or "")
    canonical = result.get("canonical_applied")
    applied = "YES" if canonical is True else "NO" if canonical is False else "UNKNOWN"
    promotion = result.get("canonical_promotion")
    if canonical is None and isinstance(promotion, Mapping) and promotion.get("status") == "PROMOTED":
        applied = "YES"  # recorded publication result, not a fresh filesystem probe
    actions = job.get("legal_next_actions")
    if not isinstance(actions, list) or any(not isinstance(x, str) or not x.strip() for x in actions):
        actions = []
    passed = []
    for name, key, values in (("BUILD", "build_status", {"PASS"}),
                               ("TEST", "test_status", {"PASS"}),
                               ("REVIEW", "review_status", {"REVIEW_PASS"}),
                               ("VERIFICATION", "verification_status", {"VERIFIED"})):
        if result.get(key) in values:
            passed.append(name)
    return {"schema_revision": "operator-decision/1", "category": category,
            "job_id": str(job.get("job_id") or ""), "job_revision": job.get("revision"),
            "what_stopped": stage, "why": code or "INSUFFICIENT_EVIDENCE",
            "observed_passed_stages": passed, "admissibility": "RECHECK_AT_ACTION_TIME",
            "candidate_id": candidate_id,
            "candidate_preserved": "YES" if candidate_id and disposition in {"PRESERVED", "ISOLATED_CANDIDATE_PRESERVED", "REJECTED_ISOLATED_CANDIDATE_PRESERVED"} else
                                   "NO" if disposition in {"NO_DELTA", "ROLLED_BACK", "DISCARDED"} else "UNKNOWN",
            "canonical_applied": applied,
            "observation_assurance": "RECORDED_CONTROL_RESULT_NOT_FILESYSTEM_REVERIFIED",
            "decision_required": str(qa.get("question") or qa.get("reason") or result.get("failure_reason") or
                                     "REFRESH_CONTROL_DECISION_REQUIRED"),
            "allowed_actions": list(actions),
            "next_legal_action": actions[0] if actions else "REFRESH_CONTROL_ACTIONS"}


def render_operator_projection(projection: Mapping[str, Any]) -> str:
    return "\n".join((
        str(projection.get("category", "OTHER_HUMAN_DECISION")) + ": " + str(projection.get("why", "UNKNOWN")),
        "Stopped: " + str(projection.get("what_stopped", "UNKNOWN")),
        "Observed PASS: " + (", ".join(projection.get("observed_passed_stages") or []) or "NONE_RECORDED"),
        "Candidate preserved: " + str(projection.get("candidate_preserved", "UNKNOWN")),
        "Canonical applied: " + str(projection.get("canonical_applied", "UNKNOWN")),
        "Decision: " + str(projection.get("decision_required", "UNKNOWN")),
        "Next: " + str(projection.get("next_legal_action", "REFRESH_CONTROL_ACTIONS"))))
