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
    elif result.get("candidate_id") or qa.get("candidate_id") or qa_type == "CANDIDATE_DECISION":
        decision_type = "CANDIDATE"
    elif "POLICY" in qa_type:
        decision_type = "POLICY"
    elif any(token in qa_type for token in ("CONTRACT", "DB_DECISION", "API_DECISION", "BUSINESS_DECISION")):
        decision_type = "CONTRACT"
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
