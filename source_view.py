"""Deterministic single-agent SourceView provenance and mutation scope."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from runtime_adapter import payload_sha256


def _paths(values: Sequence[str] | None) -> list[str]:
    return sorted({str(value).replace("\\", "/").strip("/") for value in values or () if str(value).strip()})


def scope_contract(request: Mapping[str, Any]) -> dict[str, Any]:
    modules = _paths(request.get("target_modules") or ())
    resources = _paths(request.get("target_resources") or ())
    write_scope = resources or modules
    contract = {
        "schema_revision": "mutation-scope/1",
        "read_scope": sorted(set(modules + resources)),
        "write_scope": write_scope,
        "external_side_effect_scope": _paths(request.get("external_side_effect_scope") or ()),
    }
    contract["scope_hash"] = payload_sha256(contract)
    return contract


def validate_write_scope(changed_files: Sequence[str], contract: Mapping[str, Any]) -> dict[str, Any]:
    changed = _paths(changed_files)
    allowed = _paths(contract.get("write_scope") or ())

    def inside(path: str) -> bool:
        return any(path == root or path.startswith(root.rstrip("/") + "/") for root in allowed)

    outside = [path for path in changed if not inside(path)]
    return {
        "valid": not outside and (not changed or bool(allowed)),
        "changed_files": changed,
        "outside_write_scope": outside,
        "scope_hash": str(contract.get("scope_hash") or ""),
    }


def source_view_manifest(
    *, logical_job_id: str, contract_revision: int, role: str,
    workspace_identity: str, baseline_hash: str, materialization_id: str,
    predecessor_delta: Sequence[str] = (), candidate_overlay: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = dict(candidate_overlay or {})
    base = {
        "baseline_hash": str(baseline_hash),
        "predecessor_delta": _paths(predecessor_delta),
        "predecessor_delta_hash": payload_sha256(_paths(predecessor_delta)),
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "candidate_hash": str(candidate.get("candidate_hash") or candidate.get("manifest_sha256") or ""),
    }
    manifest = {
        "schema_revision": "source-view/1",
        "logical_job_id": str(logical_job_id),
        "contract_revision": int(contract_revision),
        "role": str(role).upper(),
        "workspace_identity": str(workspace_identity),
        "materialization_id": str(materialization_id),
        "base": base,
    }
    manifest["base_manifest_hash"] = payload_sha256(base)
    manifest["source_view_id"] = "SV-" + payload_sha256(manifest)[:24].upper()
    return manifest


def runtime_namespace(base: str | Path, *, source_view_id: str, workspace_identity: str) -> Path:
    identity = payload_sha256({
        "source_view_id": str(source_view_id),
        "workspace_identity": str(workspace_identity),
    })[:20]
    return Path(base).resolve() / identity
