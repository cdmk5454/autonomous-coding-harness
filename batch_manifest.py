"""Durable manifest provenance and A/B-only batch preparation helpers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from job_contract import (
    CODEX_MODELS,
    DROID_MODELS,
    JobContractError,
    JobRequest,
    resolve_droid_model,
)


class BatchManifestError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail
        super().__init__(code + (f": {detail}" if detail else ""))


NORMALIZATION_VERSION = "0.9.0.6-1"
LEGACY_MODEL_ALIASES: dict[tuple[str, str], dict[str, str]] = {
    (
        "droid",
        "custom:codex-5.3-[chatgpt.com]-0",
    ): {
        "worker": "codex",
        "model": "gpt-5.6-sol",
        "evidence": (
            "A/B companion Markdown labels the affected jobs Codex; current "
            "JobStore Codex requests and job_contract.CODEX_MODELS[0] use gpt-5.6-sol"
        ),
    },
}


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def request_sha256(request: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(request))


def load_manifest(path: str | Path) -> tuple[dict[str, Any], bytes]:
    source = Path(path).resolve()
    try:
        payload = source.read_bytes()
        data = json.loads(payload.decode("utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BatchManifestError("BATCH_MANIFEST_UNREADABLE", str(source)) from exc
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), list) or not data["jobs"]:
        raise BatchManifestError("BATCH_MANIFEST_INVALID", str(source))
    return data, payload


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> bytes:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            Path(temporary).unlink()
        except FileNotFoundError:
            pass
    return payload


def normalize_model_routing(job: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Normalize only explicit, evidence-backed legacy aliases."""
    normalized = dict(job)
    if "model" not in normalized or not str(normalized.get("model", "")).strip():
        raise BatchManifestError("AB_MANIFEST_MODEL_MISSING")
    worker = str(normalized.get("worker", "")).strip().casefold()
    model = str(normalized.get("model", "")).strip()
    if worker == "droid":
        # The canonical registry decides: canonical IDs pass unchanged and a
        # declared legacy alias resolves to its canonical replacement while the
        # provenance sidecar preserves the original requested model.
        try:
            canonical_model, _ = resolve_droid_model(model)
        except JobContractError:
            canonical_model = ""
        if canonical_model:
            if canonical_model == model:
                evidence = {
                    "original_worker": worker,
                    "original_model": model,
                    "canonical_worker": worker,
                    "canonical_model": canonical_model,
                    "kind": "CANONICAL",
                    "evidence": "job_contract canonical worker/model pair",
                }
            else:
                normalized["model"] = canonical_model
                evidence = {
                    "original_worker": worker,
                    "original_model": model,
                    "canonical_worker": worker,
                    "canonical_model": canonical_model,
                    "kind": "LEGACY_ALIAS",
                    "evidence": "job_contract DROID_MODEL_ALIASES compatibility alias",
                }
            return normalized, evidence
    elif worker == "codex" and model in CODEX_MODELS:
        return normalized, {
            "original_worker": worker,
            "original_model": model,
            "canonical_worker": worker,
            "canonical_model": model,
            "kind": "CANONICAL",
            "evidence": "job_contract canonical worker/model pair",
        }
    elif worker == "opencode" and "/" in model:
        return normalized, {
            "original_worker": worker,
            "original_model": model,
            "canonical_worker": worker,
            "canonical_model": model,
            "kind": "EXPLICIT_PROVIDER_MODEL_UNVALIDATED",
            "evidence": "provider/model syntax only; live validation remains DEFERRED_USER_SETUP",
        }
    alias = LEGACY_MODEL_ALIASES.get((worker, model))
    if alias is None:
        if model in DROID_MODELS or model in CODEX_MODELS:
            raise BatchManifestError("AB_MANIFEST_MODEL_WORKER_MISMATCH", model)
        raise BatchManifestError("AB_MANIFEST_UNKNOWN_MODEL", model)
    normalized["worker"] = alias["worker"]
    normalized["model"] = alias["model"]
    return normalized, {
        "original_worker": worker,
        "original_model": model,
        "canonical_worker": alias["worker"],
        "canonical_model": alias["model"],
        "kind": "LEGACY_ALIAS",
        "evidence": alias["evidence"],
    }


def normalize_ab_manifest(
    source_path: str | Path,
    output_path: str | Path,
    *,
    phase: str,
    profile_id: str,
    profile_fingerprint: str,
    normalized_at: str | None = None,
) -> dict[str, Any]:
    """Publish a normalized A/B manifest and immutable provenance sidecar."""
    source, source_payload = load_manifest(source_path)
    jobs: list[dict[str, Any]] = []
    aliases: list[dict[str, str]] = []
    for raw in source["jobs"]:
        if not isinstance(raw, Mapping):
            raise BatchManifestError("BATCH_MANIFEST_INVALID", "jobs[]")
        job, evidence = normalize_model_routing(raw)
        jobs.append(job)
        aliases.append({"client_job_id": str(job.get("client_job_id", "")), **evidence})

    normalized = dict(source)
    normalized["jobs"] = jobs
    destination = Path(output_path).resolve()
    # Validate the complete normalized contract before publishing either file.
    allowed_modules = sorted({str(module) for job in jobs for module in job.get("target_modules", [])})
    for job in jobs:
        try:
            JobRequest.from_mapping(job, allowed_modules=allowed_modules)
        except JobContractError as exc:
            raise BatchManifestError(
                "AB_MANIFEST_JOB_CONTRACT_INVALID", f"{exc.code}:{exc.field}"
            ) from exc
    ids = [str(job.get("client_job_id", "")) for job in jobs]
    expected_prefix = f"WBS-{str(phase).upper()}"
    if str(phase).upper() not in {"A", "B"}:
        raise BatchManifestError("AB_PHASE_INVALID")
    if len(ids) != len(set(ids)):
        raise BatchManifestError("DUPLICATE_CLIENT_JOB_ID")
    if any(not item.startswith(expected_prefix) for item in ids):
        raise BatchManifestError("UNAPPROVED_AB_JOB")
    if any("U03" in item.upper() or "U04" in item.upper() for item in ids):
        raise BatchManifestError("U03_U04_FORBIDDEN_IN_AB_BATCH")

    normalized_payload = _atomic_write_json(destination, normalized)
    sidecar = destination.with_suffix(".provenance.json")
    provenance = {
        "status": "VERIFIED",
        "source_manifest_path": str(Path(source_path).resolve()),
        "source_manifest_sha256": hashlib.sha256(source_payload).hexdigest(),
        "normalized_manifest_path": str(destination),
        "normalized_manifest_sha256": hashlib.sha256(normalized_payload).hexdigest(),
        "normalization_version": NORMALIZATION_VERSION,
        "normalized_at": normalized_at or datetime.now().astimezone().isoformat(),
        "model_alias_mapping": aliases,
        "reviewer_routing": {
            "worker": "codex",
            "model": "gpt-5.6-sol",
            "isolation": "fresh_read_only_invocation",
        },
        "profile_id": str(profile_id),
        "profile_fingerprint": str(profile_fingerprint),
        "canonical_batch_sha256": canonical_sha256(jobs),
        "per_job_request_sha256": [request_sha256(job) for job in jobs],
        "ordered_sequence": ids,
        "execution_policy": {
            "mode": "ATTENDED",
            "strict_success_required": True,
            "automatic_commit": False,
            "automatic_push": False,
        },
        "dependency_policy": {
            "fifo": True,
            "successor_requires_strict_success": True,
        },
        "automatic_commit": False,
        "automatic_push": False,
    }
    _atomic_write_json(sidecar, provenance)
    return validate_normalized_ab_manifest(
        destination,
        sidecar,
        phase=phase,
        expected_profile_id=profile_id,
        expected_profile_fingerprint=profile_fingerprint,
    )


def validate_normalized_ab_manifest(
    manifest_path: str | Path,
    provenance_path: str | Path,
    *,
    phase: str,
    expected_profile_id: str,
    expected_profile_fingerprint: str,
) -> dict[str, Any]:
    prepared = prepare_ab_only(manifest_path, phase=phase)
    data, payload = load_manifest(manifest_path)
    try:
        provenance = json.loads(Path(provenance_path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BatchManifestError("AB_MANIFEST_PROVENANCE_UNREADABLE") from exc
    if not isinstance(provenance, dict) or provenance.get("status") != "VERIFIED":
        raise BatchManifestError("AB_MANIFEST_PROVENANCE_INVALID")
    source_path = Path(str(provenance.get("source_manifest_path", "")))
    try:
        source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    except OSError as exc:
        raise BatchManifestError("BATCH_MANIFEST_UNREADABLE", str(source_path)) from exc
    if source_hash != provenance.get("source_manifest_sha256"):
        raise BatchManifestError("BATCH_MANIFEST_CONTENT_CHANGED")
    if hashlib.sha256(payload).hexdigest() != provenance.get("normalized_manifest_sha256"):
        raise BatchManifestError("AB_NORMALIZED_MANIFEST_CHANGED")
    jobs = [dict(item) for item in data["jobs"]]
    if [request_sha256(job) for job in jobs] != provenance.get("per_job_request_sha256"):
        raise BatchManifestError("BATCH_JOB_REQUEST_CHANGED")
    if canonical_sha256(jobs) != provenance.get("canonical_batch_sha256"):
        raise BatchManifestError("BATCH_CANONICAL_HASH_CHANGED")
    if prepared["ordered_sequence"] != provenance.get("ordered_sequence"):
        raise BatchManifestError("BATCH_SEQUENCE_CHANGED")
    if provenance.get("profile_id") != expected_profile_id:
        raise BatchManifestError("AB_MANIFEST_PROFILE_ID_MISMATCH")
    if provenance.get("profile_fingerprint") != expected_profile_fingerprint:
        raise BatchManifestError("AB_MANIFEST_PROFILE_FINGERPRINT_MISMATCH")
    if provenance.get("normalization_version") != NORMALIZATION_VERSION:
        raise BatchManifestError("AB_MANIFEST_NORMALIZATION_VERSION_MISMATCH")
    execution = provenance.get("execution_policy") or {}
    if provenance.get("automatic_commit") is not False or execution.get("automatic_commit") is not False:
        raise BatchManifestError("AB_MANIFEST_AUTOMATIC_COMMIT_FORBIDDEN")
    if provenance.get("automatic_push") is not False or execution.get("automatic_push") is not False:
        raise BatchManifestError("AB_MANIFEST_AUTOMATIC_PUSH_FORBIDDEN")
    return {
        "valid": True,
        "status": "AB_MANIFEST_JOB_CONTRACT_VALID",
        "phase": prepared["phase"],
        "job_count": len(jobs),
        "ordered_sequence": prepared["ordered_sequence"],
        "source_manifest_sha256": provenance["source_manifest_sha256"],
        "normalized_manifest_sha256": provenance["normalized_manifest_sha256"],
        "canonical_batch_sha256": provenance["canonical_batch_sha256"],
        "profile_id": provenance["profile_id"],
        "profile_fingerprint": provenance["profile_fingerprint"],
        "provenance_path": str(Path(provenance_path).resolve()),
    }


def build_provenance(
    path: str | Path,
    requests: Sequence[Mapping[str, Any]],
    *,
    enqueue_request_id: str,
    enqueue_actor: str,
    execution_context: Mapping[str, Any],
    execution_policy: Mapping[str, Any],
) -> dict[str, Any]:
    data, payload = load_manifest(path)
    source_jobs = list(data["jobs"])
    persisted = [dict(item) for item in requests]
    if len(source_jobs) != len(persisted):
        raise BatchManifestError("BATCH_MANIFEST_JOB_COUNT_MISMATCH")
    source_hashes = [request_sha256(item) for item in source_jobs]
    persisted_hashes = [request_sha256(item) for item in persisted]
    if source_hashes != persisted_hashes:
        raise BatchManifestError("BATCH_MANIFEST_REQUEST_MISMATCH")
    profile_id = str(execution_context.get("profile_id", ""))
    profile_fingerprint = canonical_sha256(dict(execution_context))
    ordered = [str(item.get("client_job_id", "")) for item in persisted]
    return {
        "status": "VERIFIED",
        "source_manifest_path": str(Path(path).resolve()),
        "source_manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "canonical_batch_sha256": canonical_sha256(persisted),
        "per_job_request_sha256": persisted_hashes,
        "enqueue_request_id": enqueue_request_id,
        "enqueue_actor": enqueue_actor,
        "created_at": datetime.now().astimezone().isoformat(),
        "profile_id": profile_id,
        "profile_fingerprint": profile_fingerprint,
        "ordered_sequence": ordered,
        "batch_execution_policy": dict(execution_policy),
        "build_test_review_policy": dict(execution_policy.get("build_test_review", {})),
        "dependency_policy": dict(execution_policy.get("dependency", {})),
    }


def validate_provenance(
    provenance: Mapping[str, Any],
    persisted_jobs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if provenance.get("status") != "VERIFIED":
        raise BatchManifestError("LEGACY_PROVENANCE_UNVERIFIED")
    data, payload = load_manifest(str(provenance.get("source_manifest_path", "")))
    if hashlib.sha256(payload).hexdigest() != provenance.get("source_manifest_sha256"):
        raise BatchManifestError("BATCH_MANIFEST_CONTENT_CHANGED")
    requests = [dict(job.get("request") or {}) for job in persisted_jobs]
    if [int(job.get("sequence", 0)) for job in persisted_jobs] != list(
        range(1, len(persisted_jobs) + 1)
    ):
        raise BatchManifestError("BATCH_SEQUENCE_CHANGED")
    hashes = [request_sha256(item) for item in requests]
    if hashes != list(provenance.get("per_job_request_sha256") or []):
        raise BatchManifestError("BATCH_JOB_REQUEST_CHANGED")
    ordered = [str(job.get("client_job_id", "")) for job in persisted_jobs]
    if ordered != list(provenance.get("ordered_sequence") or []):
        raise BatchManifestError("BATCH_SEQUENCE_CHANGED")
    if canonical_sha256(requests) != provenance.get("canonical_batch_sha256"):
        raise BatchManifestError("BATCH_CANONICAL_HASH_CHANGED")
    source_jobs = list(data.get("jobs") or [])
    if [request_sha256(item) for item in source_jobs] != hashes:
        raise BatchManifestError("BATCH_MANIFEST_REQUEST_MISMATCH")
    return {"valid": True, "status": "VERIFIED", "job_count": len(requests)}


def prepare_ab_only(path: str | Path, *, phase: str) -> dict[str, Any]:
    phase = str(phase).upper()
    if phase not in {"A", "B"}:
        raise BatchManifestError("AB_PHASE_INVALID")
    data, payload = load_manifest(path)
    jobs = [dict(item) for item in data["jobs"]]
    ids = [str(item.get("client_job_id", "")) for item in jobs]
    if any("U03" in value.upper() or "U04" in value.upper() for value in ids):
        raise BatchManifestError("U03_U04_FORBIDDEN_IN_AB_BATCH")
    prefix = f"WBS-{phase}"
    if any(not value.startswith(prefix) for value in ids):
        raise BatchManifestError("UNAPPROVED_AB_JOB")
    if len(ids) != len(set(ids)):
        raise BatchManifestError("DUPLICATE_CLIENT_JOB_ID")
    allowed_modules = sorted(
        {
            str(module)
            for item in jobs
            for module in list(item.get("target_modules") or [])
            if str(module)
        }
    )
    try:
        for item in jobs:
            JobRequest.from_mapping(item, allowed_modules=allowed_modules)
    except JobContractError as exc:
        raise BatchManifestError(
            "AB_MANIFEST_JOB_CONTRACT_INVALID", f"{exc.code}:{exc.field}"
        ) from exc
    return {
        "phase": phase,
        "source_manifest_path": str(Path(path).resolve()),
        "source_manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "canonical_batch_sha256": canonical_sha256(jobs),
        "ordered_sequence": ids,
        "jobs": jobs,
        "execution_mode": "ATTENDED",
        "auto_commit": False,
        "auto_push": False,
    }
