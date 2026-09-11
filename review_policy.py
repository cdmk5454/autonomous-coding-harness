"""Deterministic Reviewer risk, model, reasoning, and context selection."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from job_contract import CODEX_MODELS


REVIEW_POLICY_VERSION = "0.9.0"
REVIEW_MODEL_LUNA = "gpt-5.6-luna"
REVIEW_MODEL_SOL = "gpt-5.6-sol"

LOW = "LOW"
MID = "MID"
HIGH = "HIGH"
CRITICAL = "CRITICAL"

DIFF_ONLY = "DIFF_ONLY"
LOCAL = "LOCAL"
LOCAL_IMPACT = "LOCAL_IMPACT"
DOMAIN = "DOMAIN"

# Review execution modes. The separate LLM Reviewer remains the default for
# every risk level; DETERMINISTIC_ORACLE is only ever selected by the Manager
# when an explicitly approved deterministic oracle contract covers the current
# acceptance (never by risk level or keyword inference alone).
REVIEW_MODE_LLM = "LLM_REVIEW"
REVIEW_MODE_DETERMINISTIC_ORACLE = "DETERMINISTIC_ORACLE"

_DB = re.compile(r"(?i)(\bselect\b|\binsert\b|\bupdate\b|\bdelete\b|mapper|query|sql|ddl|dml|table|column|schema|\.xml$)")
_DB_SCHEMA = re.compile(r"(?i)(ddl|migration|alter\s+table|create\s+table|drop\s+table)")
_AUTH = re.compile(
    r"(?i)(\bauth(?:enticate|entication|ori[sz]ation)?\b|"
    r"\bpermission\b|\brole\b|권한|개인정보|\bpii\b|\bpassword\b|"
    r"\btoken\b|\bsecurity\b|\baccess\s+control\b)"
)
_SECURITY_BOUNDARY = re.compile(r"(?i)(security boundary|인증 경계|권한 우회|access control)")
_API = re.compile(r"(?i)(controller|endpoint|request|response|api|contract|dto|public\s+method)")
_EXTERNAL = re.compile(r"(?i)(sms|kakao|알림톡|email|message|webhook|external api|send\w*message)")
_DESTRUCTIVE = re.compile(r"(?i)(production|운영).*(drop|delete|truncate|force|발송|전송)")
_SHARED = re.compile(r"(?i)(common|shared|core|base|공통)")
_UNKNOWN_CONTRACT = re.compile(r"(?i)(unknown contract|contract unresolved|업무.*미정|계약.*미정|협의 필요)")


@dataclass(frozen=True)
class ReviewDecision:
    risk_score: int
    computed_risk_level: str
    effective_risk_level: str
    risk_level: str
    score_components: dict[str, int]
    score_reasons: tuple[str, ...]
    hard_flags: tuple[str, ...]
    override_reasons: tuple[str, ...]
    model: str
    reasoning_effort: str
    context_tier: str
    review_depth: str
    selection_provenance: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": REVIEW_POLICY_VERSION,
            "risk_score": self.risk_score,
            "computed_risk_level": self.computed_risk_level,
            "effective_risk_level": self.effective_risk_level,
            "risk_level": self.risk_level,
            "score_components": dict(self.score_components),
            "score_reasons": list(self.score_reasons),
            "hard_flags": list(self.hard_flags),
            "override_reasons": list(self.override_reasons),
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "context_tier": self.context_tier,
            "review_depth": self.review_depth,
            "selection_provenance": self.selection_provenance,
            # The deterministic decision computed here cannot know execution
            # outcomes, so a separate LLM Reviewer is always required at
            # decision time. LOW risk alone never skips review; only an
            # explicitly approved deterministic oracle contract evaluated at
            # review time (Manager) may set reviewer_required=false with
            # review_mode=DETERMINISTIC_ORACLE and a bound oracle receipt.
            "reviewer_required": True,
            "review_mode": REVIEW_MODE_LLM,
        }


def _joined(requirement: str, changed_files: Sequence[str], git_diff: str) -> str:
    return "\n".join((requirement or "", *changed_files, git_diff or ""))


def assess_review_risk(
    *,
    requirement: str,
    target_modules: Sequence[str],
    changed_files: Sequence[str],
    git_diff: str,
    build_status: str,
    test_status: str,
    test_required: bool,
    contract_known: bool = True,
    policy_overlays: Sequence[str] = (),
    available_models: Sequence[str] = CODEX_MODELS,
) -> ReviewDecision:
    """Score only review depth. It never changes success semantics."""
    text = _joined(requirement, changed_files, git_diff)
    modules = {str(item).casefold() for item in target_modules if item}
    file_modules = {
        str(path).replace("\\", "/").split("/", 1)[0].casefold()
        for path in changed_files if path
    }
    module_count = max(len(modules), len(file_modules))
    line_count = sum(
        1 for line in (git_diff or "").splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )
    components: dict[str, int] = {}
    components["scope_module_breadth"] = min(15, 3 + max(0, module_count - 1) * 6) if module_count else 0
    components["changed_diff_breadth"] = min(10, len(set(changed_files)) * 2 + line_count // 120)
    components["public_api_contract"] = 15 if _API.search(text) else 0
    components["db_query_data"] = 15 if _DB.search(text) else 0
    components["authorization_pii_security"] = 20 if _AUTH.search(text) else 0
    components["external_side_effect"] = 10 if _EXTERNAL.search(text) else 0
    components["shared_core_cross_module"] = (
        10 if _SHARED.search(text) or module_count >= 3 else 6 if module_count >= 2 else 0
    )
    weak = 0
    if str(build_status).upper() != "PASS":
        weak += 3
    if test_required and str(test_status).upper() != "PASS":
        weak += 2
    components["verification_weakness"] = min(5, weak)
    score = min(100, sum(components.values()))
    reasons = tuple(key for key, value in components.items() if value)
    flags: list[str] = []
    if _AUTH.search(text):
        flags.append("AUTHORIZATION_OR_PII")
    if _DB_SCHEMA.search(text):
        flags.append("DB_SCHEMA_OR_MIGRATION")
    if _EXTERNAL.search(text):
        flags.append("EXTERNAL_SIDE_EFFECT")
    if _DESTRUCTIVE.search(text):
        flags.append("PRODUCTION_DESTRUCTIVE_OPERATION")
    if _SECURITY_BOUNDARY.search(text):
        flags.append("SECURITY_BOUNDARY")
    if not contract_known or _UNKNOWN_CONTRACT.search(text):
        flags.append("UNKNOWN_CONTRACT")
    overlays = {str(item).upper() for item in policy_overlays}
    if "REVIEW_STRICT" in overlays:
        flags.append("POLICY_OVERLAY_REVIEW_STRICT")
    if "DB_MIGRATION_STRICT" in overlays:
        flags.append("POLICY_OVERLAY_DB_MIGRATION_STRICT")

    if score >= 90:
        computed_level = CRITICAL
    elif score >= 50:
        computed_level = HIGH
    elif score >= 25:
        computed_level = MID
    else:
        computed_level = LOW

    critical = any(flag in flags for flag in (
        "PRODUCTION_DESTRUCTIVE_OPERATION", "SECURITY_BOUNDARY", "UNKNOWN_CONTRACT",
        "POLICY_OVERLAY_DB_MIGRATION_STRICT",
    ))
    high_minimum = any(flag in flags for flag in (
        "AUTHORIZATION_OR_PII", "DB_SCHEMA_OR_MIGRATION", "EXTERNAL_SIDE_EFFECT",
        "POLICY_OVERLAY_REVIEW_STRICT",
    ))
    if critical or computed_level == CRITICAL:
        level = CRITICAL
    elif high_minimum or computed_level == HIGH:
        level = HIGH
    elif computed_level == MID:
        level = MID
    else:
        level = LOW
    override_reasons = tuple(flags) if level != computed_level else ()

    registry = set(available_models)
    wanted = REVIEW_MODEL_LUNA if level == LOW else REVIEW_MODEL_SOL
    if wanted not in registry:
        raise ValueError(f"REVIEW_MODEL_TIER_UNAVAILABLE:{wanted}")
    if level == LOW:
        effort, tier, depth = "low", DIFF_ONLY, "FOCUSED"
    elif level == MID:
        effort, tier, depth = ("low" if score <= 37 else "medium"), LOCAL, "LOCAL"
    elif level == HIGH:
        effort, tier, depth = "high", LOCAL_IMPACT, "IMPACT"
    else:
        # Current canonical Harness registry exposes high as its strongest
        # supported reasoning value. No invented XHigh identifier is used.
        effort, tier, depth = "high", DOMAIN, "DOMAIN"
    return ReviewDecision(
        risk_score=score,
        computed_risk_level=computed_level,
        effective_risk_level=level,
        risk_level=level,
        score_components=components,
        score_reasons=reasons,
        hard_flags=tuple(flags),
        override_reasons=override_reasons,
        model=wanted,
        reasoning_effort=effort,
        context_tier=tier,
        review_depth=depth,
        selection_provenance="CANONICAL_MODEL_REGISTRY_DETERMINISTIC_SCORE",
    )


def decision_from_task(task: Any, *, contract_known: bool = True) -> ReviewDecision:
    decision = assess_review_risk(
        requirement=str(getattr(task, "requirement", "")),
        target_modules=list(getattr(task, "target_module", []) or []),
        changed_files=list(getattr(task, "task_owned_changed_files", []) or getattr(task, "changed_files", []) or []),
        git_diff=str(getattr(task, "git_diff", "")),
        build_status=str(dict(getattr(task, "build", {}) or {}).get("status", "")),
        test_status=str(getattr(task, "test_status", "")),
        test_required=bool(getattr(task, "test_required", False)),
        contract_known=contract_known,
        policy_overlays=list(getattr(task, "policy_overlays", []) or []),
    )
    frozen = dict(getattr(task, "effective_policy", {}) or {}).get("resolved_reviewer", {})
    rank = {LOW: 1, MID: 2, HIGH: 3, CRITICAL: 4}
    if frozen and rank.get(frozen.get("risk_level"), 0) >= rank[decision.risk_level]:
        decision = replace(decision, model=frozen["model"], reasoning_effort=frozen["reasoning_effort"],
                           context_tier=frozen["context_tier"], review_depth=frozen["review_depth"])
    return decision
