"""0.99.2.1 QAAdapter - provider normalization + bounded failover.

Failover policy (exact):
- Momentic PASS            -> QA PASS
- Momentic RECOVERED_PASS  -> QA RECOVERED_PASS
- Momentic application/assertion failure -> QA FAIL, Stagehand fallback
  FORBIDDEN (a fallback PASS may never cover an application finding)
- Momentic provider/infra failure -> Stagehand fallback
- Stagehand infra failure too -> INFRA_FAILURE / QA_INFRA_UNAVAILABLE

The adapter never assigns task status, Job SUCCESS, or canonical completion.
"""

from __future__ import annotations

from typing import Any, Mapping

from qa_browser_contract import (
    ACCEPTABLE_QA_STATUSES,
    APPLICATION_FAILURE_CODES,
    BROWSER_BROWSERBASE,
    BROWSER_GOOGLE_CHROME,
    MODEL_DEEPSEEK_V41_FLASH,
    MODEL_PROVIDER_OPENCODE_GO,
    PROVIDER_INFRA_FAILURE_CODES,
    PROVIDER_MOMENTIC,
    PROVIDER_STAGEHAND,
    QA_INFRA_UNAVAILABLE,
    QA_SCENARIO_ACTION_NOT_ALLOWED,
    QA_SCENARIO_FORBIDDEN,
    QA_STATUS_FAIL,
    QA_STATUS_INFRA_FAILURE,
    QA_STATUS_PASS,
    QA_STATUS_RECOVERED_PASS,
    QAExecutionResult,
    QAScenario,
    ProviderOutcome,
    failure_classification,
    new_qa_execution_id,
    normalize_step_action,
    now_iso,
)
from qa_browser_providers import QAScenarioDenied, run_momentic, run_stagehand

from runtime_safety import scrub_secrets

DEFAULT_POLICY = {
    "primary": PROVIDER_MOMENTIC,
    "fallback": PROVIDER_STAGEHAND,
    # Explicit operator authorization is the only way a forbidden side-effect
    # class may run; it is granted outside the QA path, never inferred.
    "authorized_side_effect_classes": [],
}


class SideEffectPolicy:
    """Section 14: QA providers cannot bypass Execution Safety policy."""

    def __init__(self, policy: Mapping[str, Any] | None = None):
        merged = {**DEFAULT_POLICY, **dict(policy or {})}
        self.authorized = frozenset(
            str(item).strip().upper() for item in merged.get("authorized_side_effect_classes", []) if item
        )
        self.primary = str(merged.get("primary", PROVIDER_MOMENTIC))
        self.fallback = str(merged.get("fallback", PROVIDER_STAGEHAND))

    def ensure_allowed(self, scenario: QAScenario) -> None:
        from qa_browser_contract import FORBIDDEN_SIDE_EFFECT_CLASSES
        if scenario.side_effect_class in FORBIDDEN_SIDE_EFFECT_CLASSES and scenario.side_effect_class not in self.authorized:
            raise QAScenarioDenied(
                f"side_effect_class={scenario.side_effect_class} requires explicit operator authorization"
            )
        allowed = {item.strip().lower() for item in scenario.allowed_actions}
        forbidden = {item.strip().lower() for item in scenario.forbidden_actions}
        overlap = sorted(allowed & forbidden)
        if overlap:
            raise QAScenarioDenied("allowed/forbidden action overlap: " + ", ".join(overlap))
        from qa_browser_contract import DEFAULT_FORBIDDEN_ACTION_TOKENS
        for action in allowed:
            for token in DEFAULT_FORBIDDEN_ACTION_TOKENS:
                if token in action and scenario.side_effect_class not in self.authorized:
                    raise QAScenarioDenied(
                        f"forbidden action token '{token}' in allowed action '{action}'"
                    )
        # Typed step/action contract: every step maps deterministically to one
        # normalized action token that must be declared in allowed_actions and
        # absent from forbidden_actions (forbidden always wins). Denied before
        # any provider subprocess starts.
        for step in scenario.steps:
            raw_action = str(step.get("action", "")).strip()
            normalized = normalize_step_action(raw_action)
            if not normalized:
                raise QAScenarioDenied(
                    f"unsupported step action '{raw_action}'",
                    code=QA_SCENARIO_ACTION_NOT_ALLOWED,
                )
            if normalized in forbidden:
                raise QAScenarioDenied(
                    f"step action '{raw_action}' -> '{normalized}' is forbidden",
                )
            if normalized not in allowed:
                raise QAScenarioDenied(
                    f"step action '{raw_action}' -> '{normalized}' is not declared in allowed_actions",
                    code=QA_SCENARIO_ACTION_NOT_ALLOWED,
                )


class QAAdapter:
    """Synchronous, minimal adapter inside the existing execution flow."""

    def __init__(self, *, momentic=run_momentic, stagehand=run_stagehand,
                 policy: Mapping[str, Any] | None = None):
        self._momentic = momentic
        self._stagehand = stagehand
        self.side_effects = SideEffectPolicy(policy)

    # -- normalization ------------------------------------------------------

    @staticmethod
    def _normalized_status(outcome: ProviderOutcome) -> str:
        if outcome.passed:
            return QA_STATUS_RECOVERED_PASS if outcome.recovery_used else QA_STATUS_PASS
        if outcome.failure_code in APPLICATION_FAILURE_CODES:
            return QA_STATUS_FAIL
        return QA_STATUS_INFRA_FAILURE

    def _result(self, outcome: ProviderOutcome, scenario: QAScenario, binding: Mapping[str, Any],
                *, attempt: int, fallback_from: str = "", fallback_reason: str = "") -> QAExecutionResult:
        status = self._normalized_status(outcome)
        failure_class = "" if status in ACCEPTABLE_QA_STATUSES else failure_classification(outcome.failure_code)[0]
        result = QAExecutionResult(
            provider=outcome.provider,
            browser_provider=outcome.browser_provider or (
                BROWSER_BROWSERBASE if outcome.provider == PROVIDER_STAGEHAND else BROWSER_GOOGLE_CHROME
            ),
            model_provider=outcome.model_provider or (
                MODEL_PROVIDER_OPENCODE_GO if outcome.provider == PROVIDER_STAGEHAND else ""
            ),
            model=outcome.model or (
                MODEL_DEEPSEEK_V41_FLASH if outcome.provider == PROVIDER_STAGEHAND else ""
            ),
            status=status,
            scenario_id=scenario.scenario_id,
            scenario_hash=scenario.scenario_hash,
            candidate_id=str(binding.get("candidate_id", "")),
            candidate_hash=str(binding.get("candidate_hash", "")),
            acceptance_hash=str(binding.get("acceptance_hash", "")),
            execution_surface_hash=str(binding.get("execution_surface_hash", "")),
            qa_execution_id=str(binding.get("qa_execution_id", "")) or new_qa_execution_id(),
            attempt=attempt,
            browser=outcome.browser,
            browser_version=outcome.browser_version,
            assertions=[dict(item) for item in outcome.assertions[:40]],
            artifacts=[dict(item) for item in outcome.artifacts[:20]],
            recovery_used=bool(outcome.recovery_used),
            fallback_from=fallback_from,
            fallback_reason=scrub_secrets(fallback_reason)[:500],
            failure_class=failure_class,
            failure_code="" if status in ACCEPTABLE_QA_STATUSES else outcome.failure_code,
            started_at=str(binding.get("started_at", "")) or now_iso(),
            finished_at=now_iso(),
            provider_metadata=dict(outcome.provider_metadata),
        )
        result.validate()
        return result

    # -- execution ----------------------------------------------------------

    def execute(self, *, scenario: QAScenario, binding: Mapping[str, Any],
                policy: Mapping[str, Any] | None = None) -> QAExecutionResult:
        """Run one QA scenario against one frozen candidate binding.

        Raises QAScenarioDenied when the side-effect/action policy blocks
        execution before any provider runs (execution denied, never a product
        FAIL).

        One canonical ``qa_execution_id`` correlates the whole execution:
        adapter -> provider invocation -> provider execution directory/result
        -> normalized QAExecutionResult. The fallback attempt reuses the same
        parent id (correlation lineage); Harness Verification compares it
        against the current identity.
        """
        side_effects = SideEffectPolicy(policy) if policy else self.side_effects
        side_effects.ensure_allowed(scenario)

        started_at = now_iso()
        qa_execution_id = str(binding.get("qa_execution_id", "")) or new_qa_execution_id()
        bound = {**dict(binding), "qa_execution_id": qa_execution_id, "started_at": started_at}

        primary = side_effects.primary
        primary_call = self._momentic if primary == PROVIDER_MOMENTIC else self._stagehand
        outcome = primary_call(scenario, qa_execution_id=qa_execution_id)
        result = self._result(outcome, scenario, bound, attempt=1)
        if result.status != QA_STATUS_INFRA_FAILURE:
            return result

        # Only provider/infrastructure failure may fall over. An application
        # finding is terminal for this candidate (no overwrite by fallback).
        fallback = side_effects.fallback
        if not fallback or fallback == primary:
            result.failure_code = QA_INFRA_UNAVAILABLE
            result.failure_class = "infrastructure"
            return result
        fallback_call = self._stagehand if fallback == PROVIDER_STAGEHAND else self._momentic
        fallback_outcome = fallback_call(scenario, qa_execution_id=qa_execution_id)
        fallback_result = self._result(
            fallback_outcome, scenario, bound, attempt=1,
            fallback_from=result.provider,
            fallback_reason=f"{result.provider}:{result.failure_code}",
        )
        if fallback_result.status == QA_STATUS_INFRA_FAILURE:
            fallback_result.failure_code = QA_INFRA_UNAVAILABLE
            fallback_result.failure_class = "infrastructure"
        return fallback_result
