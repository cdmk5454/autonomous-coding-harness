import unittest

from qa_browser_adapter import QAAdapter, QAScenarioDenied, SideEffectPolicy
from qa_browser_contract import (
    QA_ASSERTION_FAILED,
    QA_PROVIDER_TIMEOUT,
    QA_SCENARIO_SECRET_LITERAL_FORBIDDEN,
    PROVIDER_MOMENTIC,
    PROVIDER_STAGEHAND,
    ProviderOutcome,
    QAScenario,
    evidence_freshness,
)


def scenario(**overrides):
    data = {
        "scenario_id": "public-readonly",
        "title": "public read-only QA",
        "acceptance": ["Example Domain"],
        "allowed_actions": ["navigate", "assert"],
        "forbidden_actions": ["save", "send", "delete"],
        "side_effect_class": "READ_ONLY",
        "target_profile": "sample-profile",
        "start_url": "https://example.com",
        "steps": [
            {"action": "goto", "value": "https://example.com"},
            {"action": "assert_contains", "value": "Example Domain"},
        ],
    }
    data.update(overrides)
    return QAScenario.from_declaration(data)


class ContractTests(unittest.TestCase):
    def test_secret_literal_rejected(self):
        with self.assertRaisesRegex(ValueError, QA_SCENARIO_SECRET_LITERAL_FORBIDDEN):
            scenario(steps=[{"action": "type", "value": "token", "text": "sk-proj-public-example-secret"}])

    def test_undeclared_action_rejected_before_provider(self):
        s = scenario(
            allowed_actions=["navigate", "assert"],
            steps=[{"action": "click", "value": "button"}],
        )
        with self.assertRaises(QAScenarioDenied):
            SideEffectPolicy().ensure_allowed(s)

    def test_stale_binding_is_not_current(self):
        result = {
            "scenario_hash": "old",
            "candidate_hash": "candidate",
            "acceptance_hash": "acceptance",
            "execution_surface_hash": "surface",
        }
        self.assertEqual(
            evidence_freshness(
                result,
                candidate_hash="candidate",
                acceptance_hash="acceptance",
                scenario_hash="new",
                execution_surface_hash="surface",
            ),
            "STALE_QA_EVIDENCE",
        )


class FailoverTests(unittest.TestCase):
    def _binding(self):
        return {
            "candidate_id": "C1",
            "candidate_hash": "candidate",
            "acceptance_hash": "acceptance",
            "execution_surface_hash": "surface",
            "qa_execution_id": "QAX-PUBLIC-1",
        }

    def test_application_failure_does_not_fallback(self):
        calls = []
        def momentic(s, *, qa_execution_id):
            calls.append(("momentic", qa_execution_id))
            return ProviderOutcome(
                provider=PROVIDER_MOMENTIC,
                status="FAILED",
                passed=False,
                failure_code=QA_ASSERTION_FAILED,
            )
        def stagehand(s, *, qa_execution_id):
            calls.append(("stagehand", qa_execution_id))
            return ProviderOutcome(provider=PROVIDER_STAGEHAND, status="PASS", passed=True)
        result = QAAdapter(momentic=momentic, stagehand=stagehand).execute(
            scenario=scenario(), binding=self._binding()
        )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(calls, [("momentic", "QAX-PUBLIC-1")])

    def test_infra_failure_falls_back_with_same_execution_id(self):
        calls = []
        def momentic(s, *, qa_execution_id):
            calls.append(("momentic", qa_execution_id))
            return ProviderOutcome(
                provider=PROVIDER_MOMENTIC,
                status="TIMEOUT",
                passed=False,
                failure_code=QA_PROVIDER_TIMEOUT,
            )
        def stagehand(s, *, qa_execution_id):
            calls.append(("stagehand", qa_execution_id))
            return ProviderOutcome(provider=PROVIDER_STAGEHAND, status="PASS", passed=True)
        result = QAAdapter(momentic=momentic, stagehand=stagehand).execute(
            scenario=scenario(), binding=self._binding()
        )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.fallback_from, PROVIDER_MOMENTIC)
        self.assertEqual(result.qa_execution_id, "QAX-PUBLIC-1")
        self.assertEqual(calls, [
            ("momentic", "QAX-PUBLIC-1"),
            ("stagehand", "QAX-PUBLIC-1"),
        ])


if __name__ == "__main__":
    unittest.main()
