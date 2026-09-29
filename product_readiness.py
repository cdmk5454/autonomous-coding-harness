"""Product completion projection, intentionally separate from execution state."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


COMPLETE = "COMPLETE"
SKELETON_READY = "SKELETON_READY"
FINALIZATION_PENDING = "FINALIZATION_PENDING"


def derive_product_readiness(
    job_status: str,
    open_decisions: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    decisions = [dict(item) for item in open_decisions if item.get("status", "OPEN") == "OPEN"]
    if job_status == "SUCCEEDED" and not decisions:
        readiness = COMPLETE
    elif job_status == "SKELETON_READY" and decisions and all(
        item.get("decision_type") == "DEFERRED" for item in decisions
    ):
        readiness = SKELETON_READY
    else:
        readiness = FINALIZATION_PENDING
    return {
        "product_readiness": readiness,
        "user_action_required": bool(decisions),
        "open_decision_count": len(decisions),
        "open_decisions": decisions,
    }


def false_complete_guard(job_status: str, projection: Mapping[str, Any]) -> None:
    if projection.get("product_readiness") == COMPLETE and (
        job_status != "SUCCEEDED"
        or projection.get("user_action_required")
        or int(projection.get("open_decision_count", 0)) != 0
    ):
        raise ValueError("FALSE_COMPLETE_FORBIDDEN")

