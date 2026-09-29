"""Small gate-impact planner used to avoid unrelated Worker/gate reruns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


ORDER = ("RUNTIME_PREFLIGHT", "WORKER", "BUILD", "TEST", "REVIEW", "VERIFICATION")


@dataclass(frozen=True)
class RevalidationPlan:
    reason: str
    gates: tuple[str, ...]
    worker_rerun: bool

    def public(self) -> dict[str, Any]:
        return {"reason": self.reason, "gates": list(self.gates), "worker_rerun": self.worker_rerun}


def plan_revalidation(previous: Mapping[str, Any], current: Mapping[str, Any]) -> RevalidationPlan:
    def changed(key: str) -> bool:
        return previous.get(key) != current.get(key)

    if any(changed(key) for key in ("contract_revision", "profile_revision", "policy_revision", "execution_surface_hash")):
        return RevalidationPlan("MATERIALIZATION_CHANGED", ORDER, True)
    if changed("candidate_hash") or changed("source_hash"):
        return RevalidationPlan("PRODUCT_SOURCE_CHANGED", ORDER[1:], True)
    if changed("test_scope_hash") or changed("test_evidence_hash"):
        return RevalidationPlan("TEST_EVIDENCE_CHANGED", ("TEST", "REVIEW", "VERIFICATION"), False)
    if changed("review_evidence_hash"):
        return RevalidationPlan("REVIEW_EVIDENCE_CHANGED", ("REVIEW", "VERIFICATION"), False)
    if changed("runtime_process_id") or changed("runtime_health"):
        return RevalidationPlan("RUNTIME_RECOVERY_ONLY", ("RUNTIME_PREFLIGHT",), False)
    return RevalidationPlan("NO_MATERIAL_CHANGE", (), False)

