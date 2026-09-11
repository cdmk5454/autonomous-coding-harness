"""Small canonical guards for native-session reuse and Harness-owned dispatch."""

from __future__ import annotations

from typing import Any, Mapping

from runtime_adapter import payload_sha256


SESSION_REUSE_FIELDS = (
    "logical_job_id",
    "contract_revision",
    "role",
    "workspace_identity",
    "source_view_id",
    "execution_surface_hash",
    "candidate_hash",
    "review_input_hash",
)


def reviewer_session_contract(
    *,
    logical_job_id: str,
    contract_revision: int,
    role: str,
    workspace_identity: str,
    source_view_id: str,
    execution_surface_hash: str,
    candidate_hash: str,
    review_input_hash: str,
) -> dict[str, Any]:
    contract = {
        "logical_job_id": str(logical_job_id),
        "contract_revision": int(contract_revision),
        "role": str(role).upper(),
        "workspace_identity": str(workspace_identity),
        "source_view_id": str(source_view_id),
        "execution_surface_hash": str(execution_surface_hash),
        "candidate_hash": str(candidate_hash),
        "review_input_hash": str(review_input_hash),
    }
    contract["session_contract_hash"] = payload_sha256(contract)
    return contract


def session_reuse_allowed(
    bound: Mapping[str, Any] | None,
    requested: Mapping[str, Any] | None,
) -> bool:
    if not bound or not requested:
        return False
    return all(
        bound.get(key) == requested.get(key)
        and bound.get(key) not in (None, "")
        for key in SESSION_REUSE_FIELDS
    )


def assert_harness_dispatch(*, materialization_id: str, queued_turn_count: int) -> None:
    if not str(materialization_id).strip():
        raise RuntimeError("MATERIALIZATION_REQUIRED")
    if int(queued_turn_count) != 1:
        raise RuntimeError("NATIVE_BATCH_PRELOAD_FORBIDDEN")


def fork_reference(*, parent_binding_id: str, source_view_id: str,
                   materialization_id: str) -> dict[str, Any]:
    """A native fork is history only; it never clones canonical write authority."""
    if not all(str(value).strip() for value in (
        parent_binding_id, source_view_id, materialization_id
    )):
        raise RuntimeError("FORK_CANONICAL_BINDING_REQUIRED")
    return {
        "parent_binding_id": str(parent_binding_id),
        "source_view_id": str(source_view_id),
        "materialization_id": str(materialization_id),
        "writer_authority": "NONE",
        "canonical_rollback_evidence": False,
    }
