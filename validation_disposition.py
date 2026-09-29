"""Canonical validation outcomes without collapsing skipped policy into pass/fail."""

from __future__ import annotations

from typing import Any

from runtime_adapter import ValidationDisposition


def _gate(value: str) -> str:
    normalized = str(value or "").upper()
    if normalized in {"PASS", "REVIEW_PASS", "VERIFIED"}:
        return ValidationDisposition.PASS.value
    if normalized in {"FAIL", "FAILED", "REVIEW_FAIL", "REJECTED"}:
        return ValidationDisposition.FAIL.value
    if normalized in {"NOT_RUN_BY_POLICY"}:
        return ValidationDisposition.NOT_RUN_BY_POLICY.value
    if normalized in {"UNAVAILABLE", "ERROR", "REVIEW_UNAVAILABLE", "REVIEW_ERROR"}:
        return ValidationDisposition.UNAVAILABLE.value
    return ValidationDisposition.NOT_RUN.value


def dispositions_for(task: Any) -> list[dict[str, Any]]:
    values = [
        {"kind": "BUILD", "disposition": _gate(dict(task.build or {}).get("status"))},
        {"kind": "TEST", "disposition": _gate(task.test_status)},
        {"kind": "REVIEW", "disposition": _gate(task.review_status)},
        {"kind": "VERIFICATION", "disposition": _gate(task.verification_status)},
    ]
    try:
        from qa_policy_resolution import CONTRACT
        policy_deferred = CONTRACT in str(task.requirement or "")
    except Exception:
        policy_deferred = False
    if policy_deferred:
        values.extend([
            {"kind": "DB_CONNECT", "disposition": ValidationDisposition.NOT_RUN_BY_POLICY.value},
            {"kind": "E2E", "disposition": ValidationDisposition.NOT_RUN_BY_POLICY.value},
        ])
    request = dict((task.materialized_execution or {}).get("request") or {})
    delivery = dict(request.get("external_delivery") or {})
    if delivery and delivery.get("mode") == "PROOF_ONLY":
        values.append({
            "kind": "EXTERNAL_DELIVERY",
            "disposition": ValidationDisposition.NOT_RUN_BY_POLICY.value,
            "implementation_proof": _gate(task.verification_status),
            "delivery_proof": ValidationDisposition.NOT_RUN_BY_POLICY.value,
        })
    return values
