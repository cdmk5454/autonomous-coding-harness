"""Canonical 0.9 policy identifiers and deterministic failure routing.

The catalog keeps long policy prose out of Worker/Reviewer prompts.  Runtime
records carry stable references and compact decisions instead.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable, Mapping


POLICY_CATALOG_VERSION = "0.9.0"

SYSTEM_ENFORCED = "SYSTEM_ENFORCED"
HARNESS_POLICY = "HARNESS_POLICY"
PROFILE_POLICY = "PROFILE_POLICY"
JOB_CONTRACT = "JOB_CONTRACT"
CONTEXT_ONLY = "CONTEXT_ONLY"

WORKER_CAUSED = "WORKER_CAUSED"
REVIEWER_CODE_FINDING = "REVIEWER_CODE_FINDING"
HARNESS_CAUSED = "HARNESS_CAUSED"
TOOL_INFRA = "TOOL_INFRA"
PROJECT_ENVIRONMENT = "PROJECT_ENVIRONMENT"
CONTRACT_BLOCKED = "CONTRACT_BLOCKED"
USER_QA = "USER_QA"

POLICY_REFS = {
    "POLICY.SCOPE.IMMUTABLE_JOB_CONTRACT": HARNESS_POLICY,
    "POLICY.SCOPE.LEGACY_INFERENCE_ONLY_WHEN_EMPTY": HARNESS_POLICY,
    "POLICY.FAILURE.ORIGIN_ROUTING": HARNESS_POLICY,
    "POLICY.REVIEW.DETERMINISTIC_RISK": HARNESS_POLICY,
    "POLICY.PROGRESS.CANONICAL_PROJECTION": HARNESS_POLICY,
    "POLICY.QA.QUARANTINE": HARNESS_POLICY,
    "POLICY.EVIDENCE.NO_ESTIMATED_EXECUTION_USAGE": HARNESS_POLICY,
}

NON_OVERRIDABLE_POLICY_REFS = tuple(sorted(POLICY_REFS))
DEFAULT_HARNESS_POLICY_ID = "HARNESS-SAFE-DEFAULT@0.9.0"
STRICT_POLICY_OVERLAYS = frozenset({
    "ANALYSIS_READONLY",
    "REVIEW_STRICT",
    "DB_MIGRATION_STRICT",
})


def effective_policy_hash(policy: Mapping[str, Any]) -> str:
    """Canonical semantic configuration; ordered enforcement precedence is retained."""
    volatile = {"timestamp", "generated_at", "generated-at", "runtime_pid",
                "observation_metadata", "object_identity", "effective_policy_sha256",
                "effective_policy_hash"}
    unordered = {"non_overridable_policy_refs", "job_policy_refs", "policy_overlays",
                 "effective_policy_refs", "safety_requirements"}

    def canonical(value, key=""):
        if isinstance(value, Mapping):
            return {str(k): canonical(v, str(k)) for k, v in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            items = [canonical(v) for v in value]
            if key in unordered or isinstance(value, (set, frozenset)):
                items.sort(key=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")))
            return items
        return value

    payload = canonical({k: v for k, v in policy.items() if k not in volatile})
    payload.setdefault("schema_version", 1)
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def validate_policy_overlays(values: Iterable[str]) -> tuple[str, ...]:
    overlays: list[str] = []
    for item in values:
        value = str(item or "").strip().upper()
        if value not in STRICT_POLICY_OVERLAYS:
            raise ValueError(f"POLICY_OVERLAY_NOT_ALLOWED:{value}")
        if value not in overlays:
            overlays.append(value)
    return tuple(overlays)


def resolve_effective_policy(
    *,
    profile_id: str,
    policy_snapshot_sha256: str,
    job_policy_refs: Iterable[str] = (),
    policy_overlays: Iterable[str] = (),
) -> dict[str, Any]:
    """Merge hard defaults with additive, stricter Job policy declarations."""
    job_refs = tuple(dict.fromkeys(str(item) for item in job_policy_refs if item))
    overlays = validate_policy_overlays(policy_overlays)
    effective_refs = tuple(dict.fromkeys((*NON_OVERRIDABLE_POLICY_REFS, *job_refs)))
    policy = {
        "schema_version": 1,
        "catalog_version": POLICY_CATALOG_VERSION,
        "harness_policy_id": DEFAULT_HARNESS_POLICY_ID,
        "precedence": ["SYSTEM_ENFORCED", "HARNESS_POLICY", "PROFILE_POLICY", "JOB_CONTRACT"],
        "profile_id": str(profile_id or ""),
        "policy_snapshot_sha256": str(policy_snapshot_sha256 or ""),
        "non_overridable_policy_refs": list(NON_OVERRIDABLE_POLICY_REFS),
        "job_policy_refs": list(job_refs),
        "policy_overlays": list(overlays),
        "effective_policy_refs": list(effective_refs),
    }
    policy["effective_policy_sha256"] = effective_policy_hash(policy)
    return policy

NON_WORKER_REMEDIATION_CODES = frozenset({
    "BUILD_TARGET_SCOPE_INVALID",
    "PROFILE_INVALID",
    "PROFILE_DRIFT",
    "REVIEWER_INFRA_FAILURE",
    "REVIEW_EXECUTION_UNAVAILABLE",
    "REVIEW_OUTPUT_PARSE_FAILED",
    "REVIEW_SCHEMA_INVALID",
    "NOTIFICATION_FAILURE",
    "RUNTIME_IDENTITY_FAILURE",
    "MANIFEST_PROVENANCE_FAILURE",
    "UNRELATED_MODULE_BUILD_FAILURE",
    "HARNESS_STATE_INVARIANT_FAILURE",
})

_CONTRACT_MARKERS = (
    "CONTRACT", "BUSINESS", "DB_MEANING", "UNKNOWN_REQUIREMENT",
    "USER_DECISION", "APPROVAL_REQUIRED",
)
_PROJECT_ENV_MARKERS = (
    "PROJECT_ENVIRONMENT", "BUILD_TOOL_UNAVAILABLE", "DEPENDENCY_UNAVAILABLE",
    "MODULE_BUILD_COMMAND_MISSING",
)
_TOOL_INFRA_MARKERS = (
    "TIMEOUT", "EXECUTABLE", "PROCESS", "TRANSPORT", "NOTIFICATION",
    "REVIEW_EXECUTION", "REVIEW_OUTPUT", "REVIEW_SCHEMA", "REVIEWER_INFRA",
)
_HARNESS_MARKERS = (
    "PROFILE_", "RUNTIME_", "MANIFEST_", "CONTROL_", "SNAPSHOT_",
    "BASELINE_", "INTEGRITY", "SCOPE_INVALID", "UNRELATED_MODULE",
)


def classify_failure_origin(
    failure_code: str,
    *,
    failure_stage: str = "",
    failure_type: str = "",
    user_qa_required: bool = False,
) -> str:
    """Return one stable origin without consulting an LLM."""
    code = str(failure_code or "").strip().upper()
    stage = str(failure_stage or "").strip().upper()
    kind = str(failure_type or "").strip().lower()
    if code in NON_WORKER_REMEDIATION_CODES:
        if code.startswith("REVIEW_") or code == "NOTIFICATION_FAILURE":
            return TOOL_INFRA
        return HARNESS_CAUSED
    if any(marker in code for marker in _CONTRACT_MARKERS):
        return CONTRACT_BLOCKED
    if any(marker in code for marker in _PROJECT_ENV_MARKERS):
        return PROJECT_ENVIRONMENT
    if any(marker in code for marker in _HARNESS_MARKERS):
        return HARNESS_CAUSED
    if user_qa_required:
        return USER_QA
    if kind == "infrastructure" or any(marker in code for marker in _TOOL_INFRA_MARKERS):
        return TOOL_INFRA
    if stage == "REVIEW" and kind == "review":
        return REVIEWER_CODE_FINDING
    return WORKER_CAUSED


def worker_remediation_allowed(origin: str, failure_code: str = "") -> bool:
    return (
        str(origin) in {WORKER_CAUSED, REVIEWER_CODE_FINDING}
        and str(failure_code or "").upper() not in NON_WORKER_REMEDIATION_CODES
    )


def is_control_state_write_failure(
    failure_code: str,
    *,
    failure_stage: str = "",
    worker_stderr: str = "",
) -> bool:
    """Recognize current records and the exact 0.9.0.1 legacy incident shape."""
    code = str(failure_code or "").strip().upper()
    if code in {"CONTROL_STATE_WRITE_FAILED", "CONTROL_TASK_STATE_WRITE_FAILED"}:
        return True
    return (
        code == "CONTROL_EXECUTION_ERROR"
        and str(failure_stage or "").strip().upper() == "CONTROL"
        and "CONTROL_STATE_WRITE_FAILED: task_state" in str(worker_stderr or "")
    )


def estimate_prompt_tokens(text: str) -> int:
    """Deterministic prompt-size estimate; never used as execution usage."""
    raw = str(text or "")
    if not raw:
        return 0
    # Conservative cross-language estimate. The method is recorded explicitly.
    return max(1, (len(raw.encode("utf-8")) + 3) // 4)


def prompt_metrics(
    *,
    raw_job_intent: str,
    policy_text: str,
    context_text: str,
    effective_prompt: str,
    removed_repeated_constraint_count: int,
    policy_refs: Iterable[str],
) -> dict[str, Any]:
    def values(text: str) -> dict[str, int]:
        return {
            "chars": len(text or ""),
            "tokens": estimate_prompt_tokens(text),
        }

    return {
        "schema_version": 1,
        "raw_job_intent": values(raw_job_intent),
        "policy_injected": values(policy_text),
        "context": values(context_text),
        "effective_prompt": values(effective_prompt),
        "removed_repeated_constraint_count": max(
            0, int(removed_repeated_constraint_count)
        ),
        "policy_refs": list(dict.fromkeys(str(item) for item in policy_refs if item)),
        "token_measurement": "DETERMINISTIC_ESTIMATE_UTF8_BYTES_DIV4",
        "execution_usage": "NOT_AVAILABLE",
    }


def repeated_constraint_count(text: str) -> int:
    """Count normalized repeated imperative lines for Prompt Diet evidence."""
    seen: set[str] = set()
    repeated = 0
    for line in str(text or "").splitlines():
        normalized = re.sub(r"\s+", " ", line).strip().casefold()
        if len(normalized) < 12:
            continue
        if normalized in seen:
            repeated += 1
        else:
            seen.add(normalized)
    return repeated


def policy_reference_summary(values: Mapping[str, str] | None = None) -> list[dict[str, str]]:
    source = values or POLICY_REFS
    return [
        {"policy_ref": key, "classification": source[key]}
        for key in sorted(source)
    ]
