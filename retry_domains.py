"""Queue disposition from explicit retry domains."""

from __future__ import annotations

from typing import Any, Mapping

from runtime_adapter import RetryDomain


def failure_disposition(result: Mapping[str, Any], *, outer_attempt: int,
                        max_outer_attempts: int) -> tuple[str, str]:
    failure_type = str(result.get("failure_type", ""))
    fix_scope = str(result.get("fix_scope", ""))
    disposition = str(result.get("worktree_disposition", ""))
    retry_domain = str(result.get("retry_domain", ""))
    safe_worktree = disposition in {"CLEAN_ROLLBACK", "CHECKPOINTED_CLEAN_ROLLBACK", "NO_DELTA"}
    if retry_domain in {
        RetryDomain.RUNTIME_RECOVERY.value,
        RetryDomain.TECHNICAL_EXECUTION_RETRY.value,
    }:
        # 0.99.2.2: a technical/infrastructure failure is never a human QA
        # subject. With a preserved delta it still needs an explicit operator
        # action (technical recovery or preserved-delta finalization), which
        # the BLOCKED pause + decision code expresses without pretending a
        # business QA decision is pending.
        return (
            ("BLOCKED", "RUNTIME_OR_TECHNICAL_RECOVERY_REQUIRED")
            if safe_worktree else
            ("BLOCKED", "PRESERVED_DELTA_TECHNICAL_RECOVERY_REQUIRED")
        )
    semantic = retry_domain == RetryDomain.PRODUCT_SEMANTIC_RETRY.value or (
        not retry_domain and fix_scope == "RETRYABLE" and failure_type in {"build", "test", "review", "qa"}
    )
    if failure_type == "infrastructure" and safe_worktree:
        return "BLOCKED", "INFRASTRUCTURE_FAILURE_REQUIRES_LOCAL_RECOVERY"
    if semantic and safe_worktree and int(outer_attempt) < int(max_outer_attempts):
        return "AWAITING_ENRICHMENT", ""
    if semantic and safe_worktree:
        return "FAILED_FINAL", "OUTER_RETRY_BUDGET_EXHAUSTED"
    if safe_worktree:
        return "FAILED_FINAL", "FAILURE_NOT_OUTER_RETRYABLE"
    return "AWAITING_QA", "PRESERVED_CHANGE_REQUIRES_MANUAL_DECISION"
