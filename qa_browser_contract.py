"""0.99.2.1 QA Backend Hotfix - vendor-neutral browser QA contracts.

Ownership (fixed):
- Momentic / Stagehand are QA execution/evidence providers only.
- The QAAdapter normalizes provider results into QAExecutionResult.
- Harness Verification remains the only canonical PASS/FAIL/AWAITING_QA
  authority; a provider verdict is never a Job success authority.

Additive contract on the existing Task/evidence structure (SQLite schema 5
is unchanged; QA evidence lives in the TaskState payload).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from runtime_safety import scrub_secrets

SCHEMA_REVISION = "qa-browser-contract/1"

# Normalized QA statuses. Provider raw statuses are never canonical.
QA_STATUS_PASS = "PASS"
QA_STATUS_RECOVERED_PASS = "RECOVERED_PASS"
QA_STATUS_FAIL = "FAIL"
QA_STATUS_INFRA_FAILURE = "INFRA_FAILURE"
QA_STATUSES = (QA_STATUS_PASS, QA_STATUS_RECOVERED_PASS, QA_STATUS_FAIL,
               QA_STATUS_INFRA_FAILURE)
ACCEPTABLE_QA_STATUSES = frozenset({QA_STATUS_PASS, QA_STATUS_RECOVERED_PASS})

PROVIDER_MOMENTIC = "MOMENTIC"
PROVIDER_STAGEHAND = "STAGEHAND"
QA_PROVIDERS = (PROVIDER_MOMENTIC, PROVIDER_STAGEHAND)

BROWSER_GOOGLE_CHROME = "Google Chrome"
BROWSER_BROWSERBASE = "Browserbase"
MODEL_PROVIDER_OPENCODE_GO = "OpenCode Go"
MODEL_DEEPSEEK_V41_FLASH = "deepseek-v4.1-flash"

# --- Failure classification (provider/infra vs application/assertion) ---

# Provider / infrastructure failures: failure_type=infrastructure,
# failure_origin=TOOL_INFRA, Product semantic retry delta = 0.
QA_PROVIDER_QUOTA_EXHAUSTED = "QA_PROVIDER_QUOTA_EXHAUSTED"
QA_PROVIDER_AUTH_FAILURE = "QA_PROVIDER_AUTH_FAILURE"
QA_PROVIDER_SERVICE_UNAVAILABLE = "QA_PROVIDER_SERVICE_UNAVAILABLE"
QA_BROWSER_INFRA_FAILURE = "QA_BROWSER_INFRA_FAILURE"
QA_PROVIDER_TIMEOUT = "QA_PROVIDER_TIMEOUT"
QA_PROVIDER_COMPATIBILITY_ERROR = "QA_PROVIDER_COMPATIBILITY_ERROR"
QA_SCHEMA_VALIDATION_ERROR = "QA_SCHEMA_VALIDATION_ERROR"
# 0.99.2.1 completion hotfix: a provider result artifact that does not
# correlate to the requested scenario/qa_execution_id (stale or foreign run)
# is an invalid infra result — its PASS can never be reused.
QA_RESULT_CORRELATION_MISMATCH = "QA_RESULT_CORRELATION_MISMATCH"
PROVIDER_INFRA_FAILURE_CODES = frozenset({
    QA_PROVIDER_QUOTA_EXHAUSTED,
    QA_PROVIDER_AUTH_FAILURE,
    QA_PROVIDER_SERVICE_UNAVAILABLE,
    QA_BROWSER_INFRA_FAILURE,
    QA_PROVIDER_TIMEOUT,
    QA_PROVIDER_COMPATIBILITY_ERROR,
    QA_SCHEMA_VALIDATION_ERROR,
    QA_RESULT_CORRELATION_MISMATCH,
})

# Application / acceptance failures: failure_type=qa,
# failure_origin=QA_FINDING, candidates for Product semantic rework.
QA_ASSERTION_FAILED = "QA_ASSERTION_FAILED"
QA_FLOW_FAILED = "QA_FLOW_FAILED"
QA_EXPECTED_ELEMENT_MISSING = "QA_EXPECTED_ELEMENT_MISSING"
QA_UNEXPECTED_BEHAVIOR = "QA_UNEXPECTED_BEHAVIOR"
APPLICATION_FAILURE_CODES = frozenset({
    QA_ASSERTION_FAILED,
    QA_FLOW_FAILED,
    QA_EXPECTED_ELEMENT_MISSING,
    QA_UNEXPECTED_BEHAVIOR,
})

QA_INFRA_UNAVAILABLE = "QA_INFRA_UNAVAILABLE"
STALE_QA_EVIDENCE = "STALE_QA_EVIDENCE"
QA_SCENARIO_FORBIDDEN = "QA_SCENARIO_FORBIDDEN"
# 0.99.2.1 completion hotfix: typed action-contract enforcement codes.
QA_SCENARIO_ACTION_NOT_ALLOWED = "QA_SCENARIO_ACTION_NOT_ALLOWED"
QA_SCENARIO_SECRET_LITERAL_FORBIDDEN = "QA_SCENARIO_SECRET_LITERAL_FORBIDDEN"

# Deterministic step -> normalized action mapping (typed action contract
# only; no generic keyword policy engine). A step whose normalized action is
# absent from allowed_actions, or present in forbidden_actions, is denied
# before any provider subprocess starts.
STEP_ACTION_ALIASES = {
    "goto": "navigate",
    "navigate": "navigate",
    "assert": "assert",
    "assert_contains": "assert",
    "assert_visible": "assert",
    "click": "click",
    "doubleclick": "click",
    "type": "type",
    "fill": "type",
    "input": "type",
    "download": "download",
    "upload": "upload",
}
SUPPORTED_STEP_ACTIONS = frozenset(STEP_ACTION_ALIASES)


def normalize_step_action(action: str) -> str:
    """One step action -> one normalized allowed_actions token ("" if unknown)."""
    return STEP_ACTION_ALIASES.get(str(action or "").strip().lower(), "")

FAILURE_CLASS_INFRASTRUCTURE = "infrastructure"
FAILURE_CLASS_APPLICATION = "qa"

# --- Side-effect safety ---

SIDE_EFFECT_READ_ONLY = "READ_ONLY"
SIDE_EFFECT_IDEMPOTENT_UI = "IDEMPOTENT_UI"
# Classes that no autonomous QA provider may execute. Each entry requires an
# explicit operator authorization outside this contract before it can run.
FORBIDDEN_SIDE_EFFECT_CLASSES = frozenset({
    "EXTERNAL_SEND",
    "DB_MUTATION",
    "DESTRUCTIVE",
    "PAYMENT",
    "APPROVAL",
    "PERMISSION_CHANGE",
    "IRREVERSIBLE",
})
SIDE_EFFECT_CLASSES = frozenset({
    SIDE_EFFECT_READ_ONLY, SIDE_EFFECT_IDEMPOTENT_UI,
    *FORBIDDEN_SIDE_EFFECT_CLASSES,
})

# Action tokens that are denied by default in any QA scenario action list
# unless the side-effect class was explicitly authorized.
DEFAULT_FORBIDDEN_ACTION_TOKENS = (
    "save", "delete", "send", "approval", "approve", "payment", "pay",
    "submit", "mutation", "insert", "update", "drop", "permission",
    "발송", "저장", "삭제", "승인", "결재", "결제", "권한",
)

MAX_TEXT = 2000
MAX_LIST_ITEMS = 40


def _clean_str_list(value: Any, *, maximum: int = MAX_LIST_ITEMS) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(i, str) for i in value):
        raise ValueError("QA_SCENARIO_LIST_INVALID")
    cleaned = [scrub_secrets(v.strip())[:MAX_TEXT] for v in value if v.strip()]
    if not cleaned or len(cleaned) > maximum:
        raise ValueError("QA_SCENARIO_LIST_INVALID")
    return cleaned


def _canonical_digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _iter_declaration_strings(value: Any):
    """Yield every user-supplied string in a scenario declaration."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _iter_declaration_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_declaration_strings(item)
    elif isinstance(value, str):
        yield value


def contains_secret_literal(value: Any) -> bool:
    """Minimal secret-shaped literal detector reusing the harness scrubber.

    Detects the shapes the existing `runtime_safety` patterns already know:
    API keys (`sk-...`), `password/token/api_key: value` literals, Bearer
    authorization headers, JWTs, private key blocks, credential URLs. Env
    references (`{{ env.X }}`, `${X}`) are plain strings and pass. This is a
    typed admission guard, not a generic DLP subsystem.
    """
    for text in _iter_declaration_strings(value):
        if scrub_secrets(text, env={}) != text:
            return True
    return False


@dataclass(frozen=True)
class QAScenario:
    """Vendor-neutral canonical QA requirement.

    Momentic YAML and Stagehand scripts are provider-specific materializations
    of this contract and are never the canonical requirement themselves.
    """

    scenario_id: str
    title: str
    acceptance: tuple[str, ...]
    allowed_actions: tuple[str, ...]
    forbidden_actions: tuple[str, ...]
    side_effect_class: str = SIDE_EFFECT_READ_ONLY
    target_profile: str = ""
    start_url: str = ""
    steps: tuple[Mapping[str, Any], ...] = ()

    @classmethod
    def from_declaration(cls, value: Mapping[str, Any]) -> "QAScenario":
        if not isinstance(value, Mapping):
            raise ValueError("QA_SCENARIO_INVALID")
        scenario_id = str(value.get("scenario_id", "")).strip()
        title = str(value.get("title", "")).strip()
        if not scenario_id or len(scenario_id) > 128 or len(title) > 300:
            raise ValueError("QA_SCENARIO_ID_INVALID")
        side_effect_class = str(value.get("side_effect_class", SIDE_EFFECT_READ_ONLY)).strip().upper()
        if side_effect_class not in SIDE_EFFECT_CLASSES:
            raise ValueError("QA_SIDE_EFFECT_CLASS_INVALID")
        steps_value = value.get("steps", [])
        if not isinstance(steps_value, list) or any(not isinstance(s, Mapping) for s in steps_value):
            raise ValueError("QA_SCENARIO_STEPS_INVALID")
        if len(steps_value) > 40:
            raise ValueError("QA_SCENARIO_STEPS_INVALID")
        steps = tuple(dict(s) for s in steps_value)
        start_url = str(value.get("start_url", "")).strip()
        # Secret literals must never enter the canonical scenario payload:
        # it is persisted to .tasks, reports and provider materializations.
        if contains_secret_literal({
            "title": title,
            "acceptance": value.get("acceptance") or [],
            "allowed_actions": value.get("allowed_actions") or [],
            "forbidden_actions": value.get("forbidden_actions") or [],
            "target_profile": value.get("target_profile", ""),
            "start_url": start_url,
            "steps": steps_value,
        }):
            raise ValueError(QA_SCENARIO_SECRET_LITERAL_FORBIDDEN)
        return cls(
            scenario_id=scenario_id,
            title=title,
            acceptance=tuple(_clean_str_list(value.get("acceptance"))),
            allowed_actions=tuple(_clean_str_list(value.get("allowed_actions", ["navigate", "assert"]))),
            forbidden_actions=tuple(_clean_str_list(value.get("forbidden_actions"))),
            side_effect_class=side_effect_class,
            target_profile=str(value.get("target_profile", "")).strip()[:128],
            start_url=scrub_secrets(start_url)[:1000],
            steps=steps,
        )

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_revision": SCHEMA_REVISION,
            "scenario_id": self.scenario_id,
            "title": self.title,
            "acceptance": list(self.acceptance),
            "allowed_actions": list(self.allowed_actions),
            "forbidden_actions": list(self.forbidden_actions),
            "side_effect_class": self.side_effect_class,
            "target_profile": self.target_profile,
            "start_url": self.start_url,
            "steps": [dict(s) for s in self.steps],
        }

    @property
    def scenario_hash(self) -> str:
        return _canonical_digest(self.canonical_dict())

    @property
    def revision(self) -> str:
        return self.scenario_hash[:16]


@dataclass
class ProviderOutcome:
    """One provider execution, before failover normalization."""

    provider: str
    status: str                      # raw provider status string
    passed: bool
    recovery_used: bool = False
    failure_code: str = ""           # QA_* failure code (already classified)
    failure_message: str = ""
    assertions: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    browser: str = ""
    browser_provider: str = ""
    browser_version: str = ""
    model_provider: str = ""
    model: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
    provider_metadata: dict[str, Any] = field(default_factory=dict)


def new_qa_execution_id() -> str:
    return "QAX-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + hashlib.sha256(
        json.dumps(datetime.now(timezone.utc).isoformat(), default=str).encode("utf-8")
    ).hexdigest()[:8]


_CREDENTIAL_KEY_TOKENS = ("api_key", "apikey", "token", "secret", "password", "credential", "authorization")


def redact_credentials(value: Any) -> Any:
    """Recursively drop credential-shaped keys/values from QA evidence."""
    if isinstance(value, Mapping):
        safe = {}
        for key, item in value.items():
            key_text = str(key)
            if any(token in key_text.lower() for token in _CREDENTIAL_KEY_TOKENS):
                continue
            safe[key_text] = redact_credentials(item)
        return safe
    if isinstance(value, list):
        return [redact_credentials(item) for item in value]
    if isinstance(value, str):
        return scrub_secrets(value)
    return value


@dataclass
class QAExecutionResult:
    """Normalized QA evidence bound to one frozen candidate + acceptance."""

    provider: str
    browser_provider: str = ""
    model_provider: str = ""
    model: str = ""
    status: str = QA_STATUS_FAIL
    scenario_id: str = ""
    scenario_hash: str = ""
    candidate_id: str = ""
    candidate_hash: str = ""
    acceptance_hash: str = ""
    execution_surface_hash: str = ""
    qa_execution_id: str = ""
    attempt: int = 1
    browser: str = ""
    browser_version: str = ""
    assertions: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    recovery_used: bool = False
    fallback_from: str = ""
    fallback_reason: str = ""
    failure_class: str = ""          # infrastructure | qa | "" on PASS
    failure_code: str = ""
    started_at: str = ""
    finished_at: str = ""
    provider_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return redact_credentials({
            "schema_revision": SCHEMA_REVISION,
            "provider": self.provider,
            "browser_provider": self.browser_provider,
            "model_provider": self.model_provider,
            "model": self.model,
            "status": self.status,
            "scenario_id": self.scenario_id,
            "scenario_hash": self.scenario_hash,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "acceptance_hash": self.acceptance_hash,
            "execution_surface_hash": self.execution_surface_hash,
            "qa_execution_id": self.qa_execution_id,
            "attempt": self.attempt,
            "browser": self.browser,
            "browser_version": self.browser_version,
            "assertions": list(self.assertions),
            "artifacts": list(self.artifacts),
            "recovery_used": bool(self.recovery_used),
            "fallback_from": self.fallback_from,
            "fallback_reason": self.fallback_reason,
            "failure_class": self.failure_class,
            "failure_code": self.failure_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "provider_metadata": dict(self.provider_metadata),
        })

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "QAExecutionResult":
        return cls(
            provider=str(value.get("provider", "")),
            browser_provider=str(value.get("browser_provider", "")),
            model_provider=str(value.get("model_provider", "")),
            model=str(value.get("model", "")),
            status=str(value.get("status", QA_STATUS_FAIL)),
            scenario_id=str(value.get("scenario_id", "")),
            scenario_hash=str(value.get("scenario_hash", "")),
            candidate_id=str(value.get("candidate_id", "")),
            candidate_hash=str(value.get("candidate_hash", "")),
            acceptance_hash=str(value.get("acceptance_hash", "")),
            execution_surface_hash=str(value.get("execution_surface_hash", "")),
            qa_execution_id=str(value.get("qa_execution_id", "")),
            attempt=int(value.get("attempt", 1)),
            browser=str(value.get("browser", "")),
            browser_version=str(value.get("browser_version", "")),
            assertions=list(value.get("assertions") or []),
            artifacts=list(value.get("artifacts") or []),
            recovery_used=bool(value.get("recovery_used", False)),
            fallback_from=str(value.get("fallback_from", "")),
            fallback_reason=str(value.get("fallback_reason", "")),
            failure_class=str(value.get("failure_class", "")),
            failure_code=str(value.get("failure_code", "")),
            started_at=str(value.get("started_at", "")),
            finished_at=str(value.get("finished_at", "")),
            provider_metadata=dict(value.get("provider_metadata") or {}),
        )

    def validate(self) -> None:
        if self.provider not in QA_PROVIDERS:
            raise ValueError("QA_PROVIDER_INVALID")
        if self.status not in QA_STATUSES:
            raise ValueError("QA_STATUS_INVALID")
        if not (self.scenario_id and self.scenario_hash and self.qa_execution_id
                and self.candidate_hash and self.acceptance_hash):
            raise ValueError("QA_EVIDENCE_BINDING_INCOMPLETE")
        if self.status == QA_STATUS_FAIL and self.failure_code not in APPLICATION_FAILURE_CODES:
            raise ValueError("QA_FAILURE_CODE_INVALID")
        if self.status == QA_STATUS_INFRA_FAILURE and self.failure_code not in PROVIDER_INFRA_FAILURE_CODES | {QA_INFRA_UNAVAILABLE}:
            raise ValueError("QA_FAILURE_CODE_INVALID")


def evidence_freshness(
    result: Mapping[str, Any],
    *,
    candidate_hash: str,
    acceptance_hash: str,
    scenario_hash: str,
    execution_surface_hash: str = "",
) -> str:
    """CURRENT | STALE_QA_EVIDENCE | QA_EVIDENCE_BINDING_INCOMPLETE."""
    if not (result.get("scenario_hash") and result.get("candidate_hash")
            and result.get("acceptance_hash")):
        return "QA_EVIDENCE_BINDING_INCOMPLETE"
    if (str(result.get("scenario_hash")) != str(scenario_hash)
            or str(result.get("candidate_hash")) != str(candidate_hash)
            or str(result.get("acceptance_hash")) != str(acceptance_hash)
            or (execution_surface_hash
                and str(result.get("execution_surface_hash")) != str(execution_surface_hash))):
        return STALE_QA_EVIDENCE
    return "CURRENT"


def failure_classification(failure_code: str) -> tuple[str, str]:
    """failure_code -> (failure_type, failure_origin)."""
    code = str(failure_code or "").strip().upper()
    if code in PROVIDER_INFRA_FAILURE_CODES or code == QA_INFRA_UNAVAILABLE:
        return FAILURE_CLASS_INFRASTRUCTURE, "TOOL_INFRA"
    if code in APPLICATION_FAILURE_CODES:
        return FAILURE_CLASS_APPLICATION, "QA_FINDING"
    if code == QA_SCENARIO_FORBIDDEN:
        return "policy", "HARNESS_POLICY"
    raise ValueError("QA_FAILURE_CODE_UNKNOWN")


def provider_session_id(qa_execution_id: str) -> str:
    """Stable per-execution OpenCode Go session id (never a static value)."""
    return f"harness-qa-{qa_execution_id}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
