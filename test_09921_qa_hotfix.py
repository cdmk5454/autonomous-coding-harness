"""0.99.2.1 QA Backend Hotfix focused tests.

Covers the hotfix contract (impact-scoped; full regression is NOT required):
- 20.1 No-QA compatibility: plans without a browser_qa declaration keep the
  0.99.2 path completely unchanged.
- 20.2 Momentic normalization: PASS / RECOVERED_PASS / assertion FAIL (no
  fallback) / provider-auth-quota-service failure -> Stagehand fallback.
- 20.3 Stagehand: Browserbase fallback PASS, assertion failure -> QA_FAIL,
  double infra failure -> QA_INFRA_UNAVAILABLE.
- 20.4 Candidate/acceptance/scenario binding and STALE_QA_EVIDENCE.
- 20.5 False success: QA required + missing/stale/infra/assertion/mismatch
  evidence can never produce SUCCESS/VERIFIED.
- 20.6 Failure semantics: provider infra failure != product defect.
- 20.7 Side-effect safety: forbidden scenario actions are denied pre-run.
- 20.8 Credential safety: no API key/token/password in QA evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from job_contract import JobRequest
from manager import Manager
from policy_catalog import QA_FINDING, TOOL_INFRA, classify_failure_origin, worker_remediation_allowed
from qa_browser_adapter import QAAdapter, SideEffectPolicy
from qa_browser_contract import (
    ACCEPTABLE_QA_STATUSES,
    QA_ASSERTION_FAILED,
    QA_INFRA_UNAVAILABLE,
    QA_PROVIDER_AUTH_FAILURE,
    QA_PROVIDER_QUOTA_EXHAUSTED,
    QA_PROVIDER_SERVICE_UNAVAILABLE,
    QA_PROVIDER_TIMEOUT,
    QA_RESULT_CORRELATION_MISMATCH,
    QA_SCENARIO_ACTION_NOT_ALLOWED,
    QA_SCENARIO_FORBIDDEN,
    QA_SCENARIO_SECRET_LITERAL_FORBIDDEN,
    QA_STATUS_FAIL,
    QA_STATUS_INFRA_FAILURE,
    QA_STATUS_PASS,
    QA_STATUS_RECOVERED_PASS,
    STALE_QA_EVIDENCE,
    ProviderOutcome,
    QAScenario,
    QAExecutionResult,
    evidence_freshness,
    normalize_step_action,
    provider_session_id,
    redact_credentials,
)
import qa_browser_providers
from qa_browser_providers import QAScenarioDenied
from retry_domains import failure_disposition
from task_state import TaskState
from test_plan import derive_test_plan, validate_test_contract
from test_job_control import request


SCENARIO_DECLARATION = {
    "required": True,
    "scenario_id": "SsucSp0021ListEntry",
    "title": "SsucSp0021List 진입",
    "acceptance": ["학년 조회조건 없음", "조회 버튼 동작", "결과 grid 렌더링"],
    "allowed_actions": ["navigate", "click", "assert"],
    "forbidden_actions": ["save", "send", "delete"],
    "side_effect_class": "READ_ONLY",
    "target_profile": "e4u",
    "start_url": "https://example.com",
    "steps": [
        {"action": "goto", "value": "https://example.com"},
        {"action": "assert_contains", "value": "Example Domain"},
    ],
}

BINDING = {
    "candidate_id": "CAND-1",
    "candidate_hash": "c" * 64,
    "acceptance_hash": "a" * 64,
    "execution_surface_hash": "e" * 64,
    "qa_execution_id": "QAX-TEST-0001",
}


def scenario() -> QAScenario:
    return QAScenario.from_declaration(SCENARIO_DECLARATION)


def outcome(**kwargs) -> ProviderOutcome:
    base = {"provider": "MOMENTIC", "status": "PASSED", "passed": True,
            "browser": "Google Chrome"}
    base.update(kwargs)
    return ProviderOutcome(**base)


# --- 6.2 QAScenario contract -------------------------------------------------

class QAScenarioContractTests(unittest.TestCase):
    def test_declaration_validates_and_hashes_stably(self):
        first = scenario()
        second = scenario()
        self.assertEqual(first.scenario_hash, second.scenario_hash)
        self.assertTrue(first.scenario_hash)
        self.assertEqual(first.revision, first.scenario_hash[:16])

    def test_scenario_hash_tracks_acceptance_and_actions(self):
        changed = {**SCENARIO_DECLARATION, "acceptance": ["다른 허용 기준"]}
        self.assertNotEqual(scenario().scenario_hash,
                            QAScenario.from_declaration(changed).scenario_hash)
        changed = {**SCENARIO_DECLARATION, "forbidden_actions": ["save"]}
        self.assertNotEqual(scenario().scenario_hash,
                            QAScenario.from_declaration(changed).scenario_hash)

    def test_invalid_side_effect_class_rejected(self):
        bad = {**SCENARIO_DECLARATION, "side_effect_class": "SIDE_EFFECT_UNKNOWN"}
        with self.assertRaises(ValueError):
            QAScenario.from_declaration(bad)

    def test_empty_acceptance_rejected(self):
        bad = {**SCENARIO_DECLARATION, "acceptance": []}
        with self.assertRaises(ValueError):
            QAScenario.from_declaration(bad)


# --- 6.3 QAExecutionResult contract -------------------------------------------

class QAExecutionResultContractTests(unittest.TestCase):
    def _result(self, **overrides) -> QAExecutionResult:
        values = {
            "provider": "MOMENTIC",
            "status": QA_STATUS_PASS,
            "scenario_id": scenario().scenario_id,
            "scenario_hash": scenario().scenario_hash,
            "candidate_id": BINDING["candidate_id"],
            "candidate_hash": BINDING["candidate_hash"],
            "acceptance_hash": BINDING["acceptance_hash"],
            "execution_surface_hash": BINDING["execution_surface_hash"],
            "qa_execution_id": BINDING["qa_execution_id"],
        }
        values.update(overrides)
        return QAExecutionResult(**values)

    def test_validate_accepts_bound_pass(self):
        self._result().validate()

    def test_validate_rejects_fail_without_application_code(self):
        with self.assertRaises(ValueError):
            self._result(status=QA_STATUS_FAIL,
                         failure_code=QA_PROVIDER_TIMEOUT).validate()

    def test_binding_current_vs_stale(self):
        result = self._result().to_dict()
        self.assertEqual("CURRENT", evidence_freshness(
            result,
            candidate_hash=BINDING["candidate_hash"],
            acceptance_hash=BINDING["acceptance_hash"],
            scenario_hash=scenario().scenario_hash,
            execution_surface_hash=BINDING["execution_surface_hash"],
        ))
        self.assertEqual(STALE_QA_EVIDENCE, evidence_freshness(
            result,
            candidate_hash="d" * 64,
            acceptance_hash=BINDING["acceptance_hash"],
            scenario_hash=scenario().scenario_hash,
        ))
        self.assertEqual(STALE_QA_EVIDENCE, evidence_freshness(
            result,
            candidate_hash=BINDING["candidate_hash"],
            acceptance_hash="d" * 64,
            scenario_hash=scenario().scenario_hash,
        ))
        self.assertEqual(STALE_QA_EVIDENCE, evidence_freshness(
            result,
            candidate_hash=BINDING["candidate_hash"],
            acceptance_hash=BINDING["acceptance_hash"],
            scenario_hash="f" * 64,
        ))

    def test_missing_binding_is_incomplete(self):
        result = self._result().to_dict()
        result.pop("candidate_hash")
        self.assertEqual("QA_EVIDENCE_BINDING_INCOMPLETE", evidence_freshness(
            result,
            candidate_hash=BINDING["candidate_hash"],
            acceptance_hash=BINDING["acceptance_hash"],
            scenario_hash=scenario().scenario_hash,
        ))

    def test_session_id_is_per_execution(self):
        self.assertEqual("harness-qa-QAX-1", provider_session_id("QAX-1"))
        self.assertNotEqual(provider_session_id("QAX-1"), provider_session_id("QAX-2"))

    def test_recovered_pass_distinct_from_pass(self):
        self.assertIn(QA_STATUS_RECOVERED_PASS, ACCEPTABLE_QA_STATUSES)
        self.assertIn(QA_STATUS_PASS, ACCEPTABLE_QA_STATUSES)
        self.assertNotEqual(QA_STATUS_PASS, QA_STATUS_RECOVERED_PASS)

    def test_credentials_never_survive_redaction(self):
        payload = {
            "provider_metadata": {
                "api_key": "sk-live-123456",
                "session_id": "harness-qa-QAX-1",
                "auth_token": "tok",
            },
            "assertions": [{"name": "x", "Authorization": "Bearer z", "note": "ok"}],
        }
        redacted = redact_credentials(payload)
        dumped = json.dumps(redacted)
        self.assertNotIn("sk-live-123456", dumped)
        self.assertNotIn("tok", dumped)
        self.assertNotIn("Bearer z", dumped)
        self.assertEqual("harness-qa-QAX-1", redacted["provider_metadata"]["session_id"])


# --- 9/10 Failover policy ------------------------------------------------------

class QAAdapterFailoverTests(unittest.TestCase):
    def _adapter(self, momentic_fn, stagehand_fn, policy=None):
        return QAAdapter(momentic=momentic_fn, stagehand=stagehand_fn, policy=policy)

    def test_momentic_pass_is_final_without_fallback(self):
        calls = {"stagehand": 0}

        def momentic(_scenario, *, qa_execution_id=""):
            return outcome(passed=True)

        def stagehand(_scenario, *, qa_execution_id=""):
            calls["stagehand"] += 1
            return outcome(provider="STAGEHAND", passed=True)

        result = self._adapter(momentic, stagehand).execute(
            scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_PASS, result.status)
        self.assertEqual("MOMENTIC", result.provider)
        self.assertEqual("", result.fallback_from)
        self.assertEqual(0, calls["stagehand"])

    def test_momentic_recovered_pass_normalized(self):
        def momentic(_scenario, *, qa_execution_id=""):
            return outcome(passed=True, recovery_used=True)

        result = self._adapter(momentic, lambda _s, *, qa_execution_id="": outcome()).execute(
            scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_RECOVERED_PASS, result.status)
        self.assertTrue(result.recovery_used)

    def test_momentic_assertion_fail_never_falls_back(self):
        calls = {"stagehand": 0}

        def momentic(_scenario, *, qa_execution_id=""):
            return outcome(passed=False, status="FAILED",
                           failure_code=QA_ASSERTION_FAILED,
                           failure_message="The page does not contain 'X'.")

        def stagehand(_scenario, *, qa_execution_id=""):
            calls["stagehand"] += 1
            return outcome(provider="STAGEHAND", passed=True)

        result = self._adapter(momentic, stagehand).execute(
            scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_FAIL, result.status)
        self.assertEqual(QA_ASSERTION_FAILED, result.failure_code)
        self.assertEqual("qa", result.failure_class)
        self.assertEqual(0, calls["stagehand"],
                         "Momentic assertion FAIL must never be overwritten by fallback")

    def _infra_momentic(self, code):
        def momentic(_scenario, *, qa_execution_id=""):
            return outcome(passed=False, status="EXIT_1",
                           failure_code=code, failure_message="infra")
        return momentic

    def test_provider_auth_failure_falls_back_to_stagehand(self):
        result = self._adapter(
            self._infra_momentic(QA_PROVIDER_AUTH_FAILURE),
            lambda _s, *, qa_execution_id="": outcome(provider="STAGEHAND", passed=True,
                               browser="Browserbase"),
        ).execute(scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_PASS, result.status)
        self.assertEqual("STAGEHAND", result.provider)
        self.assertEqual("MOMENTIC", result.fallback_from)
        self.assertIn(QA_PROVIDER_AUTH_FAILURE, result.fallback_reason)
        self.assertEqual("OpenCode Go", result.model_provider)

    def test_provider_quota_and_service_failure_fall_back(self):
        for code in (QA_PROVIDER_QUOTA_EXHAUSTED, QA_PROVIDER_SERVICE_UNAVAILABLE,
                     QA_PROVIDER_TIMEOUT):
            result = self._adapter(
                self._infra_momentic(code),
                lambda _s, *, qa_execution_id="": outcome(provider="STAGEHAND", passed=True),
            ).execute(scenario=scenario(), binding=BINDING)
            self.assertEqual(QA_STATUS_PASS, result.status, code)
            self.assertEqual("MOMENTIC", result.fallback_from, code)

    def test_double_infra_failure_is_qa_infra_unavailable(self):
        result = self._adapter(
            self._infra_momentic(QA_PROVIDER_SERVICE_UNAVAILABLE),
            lambda _s, *, qa_execution_id="": outcome(provider="STAGEHAND", passed=False, status="EXIT_1",
                               failure_code=QA_PROVIDER_TIMEOUT),
        ).execute(scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_INFRA_FAILURE, result.status)
        self.assertEqual(QA_INFRA_UNAVAILABLE, result.failure_code)
        self.assertEqual("infrastructure", result.failure_class)

    def test_stagehand_assertion_failure_is_application_failure(self):
        result = self._adapter(
            self._infra_momentic(QA_PROVIDER_AUTH_FAILURE),
            lambda _s, *, qa_execution_id="": outcome(provider="STAGEHAND", passed=False, status="FAILED",
                               failure_code=QA_ASSERTION_FAILED),
        ).execute(scenario=scenario(), binding=BINDING)
        self.assertEqual(QA_STATUS_FAIL, result.status)
        self.assertEqual(QA_ASSERTION_FAILED, result.failure_code)

    def test_results_are_bound_to_candidate_acceptance_scenario(self):
        result = self._adapter(lambda _s, *, qa_execution_id="": outcome(), lambda _s, *, qa_execution_id="": outcome()).execute(
            scenario=scenario(), binding=BINDING)
        self.assertEqual(BINDING["candidate_hash"], result.candidate_hash)
        self.assertEqual(BINDING["acceptance_hash"], result.acceptance_hash)
        self.assertEqual(scenario().scenario_hash, result.scenario_hash)
        self.assertEqual(BINDING["execution_surface_hash"], result.execution_surface_hash)


# --- 14 Side-effect safety -----------------------------------------------------

class SideEffectSafetyTests(unittest.TestCase):
    def _deny_adapter(self, declaration):
        calls = {"momentic": 0, "stagehand": 0}

        def momentic(_scenario, *, qa_execution_id=""):
            calls["momentic"] += 1
            return outcome(passed=True)

        def stagehand(_scenario, *, qa_execution_id=""):
            calls["stagehand"] += 1
            return outcome(provider="STAGEHAND", passed=True)

        adapter = QAAdapter(momentic=momentic, stagehand=stagehand)
        with self.assertRaises(QAScenarioDenied):
            adapter.execute(scenario=QAScenario.from_declaration(declaration),
                            binding=BINDING)
        self.assertEqual(0, calls["momentic"] + calls["stagehand"],
                         "denied scenario must not execute any provider")

    def test_external_send_class_denied_before_execution(self):
        self._deny_adapter({**SCENARIO_DECLARATION, "side_effect_class": "EXTERNAL_SEND"})

    def test_db_mutation_and_destructive_denied(self):
        self._deny_adapter({**SCENARIO_DECLARATION, "side_effect_class": "DB_MUTATION"})
        self._deny_adapter({**SCENARIO_DECLARATION, "side_effect_class": "DESTRUCTIVE"})

    def test_allowed_forbidden_overlap_denied(self):
        self._deny_adapter({**SCENARIO_DECLARATION,
                            "allowed_actions": ["navigate", "send"],
                            "forbidden_actions": ["send"]})

    def test_default_forbidden_token_in_allowed_denied(self):
        self._deny_adapter({**SCENARIO_DECLARATION,
                            "allowed_actions": ["navigate", "결과 저장"],
                            "forbidden_actions": ["save"]})

    def test_read_only_scenario_passes_policy(self):
        SideEffectPolicy().ensure_allowed(scenario())  # no raise

    def test_explicit_authorization_required_even_with_policy_object(self):
        policy = SideEffectPolicy({"authorized_side_effect_classes": []})
        with self.assertRaises(QAScenarioDenied):
            policy.ensure_allowed(QAScenario.from_declaration(
                {**SCENARIO_DECLARATION, "side_effect_class": "APPROVAL"}))


# --- 20.6 Failure semantics ------------------------------------------------------

class FailureSemanticsTests(unittest.TestCase):
    def test_provider_infra_codes_route_to_tool_infra(self):
        for code in (QA_PROVIDER_TIMEOUT, QA_PROVIDER_AUTH_FAILURE,
                     QA_PROVIDER_QUOTA_EXHAUSTED, QA_PROVIDER_SERVICE_UNAVAILABLE):
            failure_type, origin = "infrastructure", None
            self.assertEqual(
                TOOL_INFRA,
                classify_failure_origin(code, failure_stage="QA",
                                        failure_type=failure_type),
                code,
            )

    def test_application_codes_route_to_qa_finding(self):
        for code in ("QA_ASSERTION_FAILED", "QA_FLOW_FAILED",
                     "QA_EXPECTED_ELEMENT_MISSING", "QA_UNEXPECTED_BEHAVIOR"):
            self.assertEqual(
                QA_FINDING,
                classify_failure_origin(code, failure_stage="QA", failure_type="qa"),
                code,
            )

    def test_qa_finding_allows_worker_remediation(self):
        self.assertTrue(worker_remediation_allowed(QA_FINDING, "QA_ASSERTION_FAILED"))
        self.assertFalse(worker_remediation_allowed(TOOL_INFRA, QA_INFRA_UNAVAILABLE))

    def test_provider_infra_failure_has_zero_product_retry_delta(self):
        # PRESERVED worktree + infrastructure + NON_RETRYABLE must stay a
        # human gate, never AWAITING_ENRICHMENT (product retry).
        disposition, reason = failure_disposition(
            {"failure_type": "infrastructure", "fix_scope": "NON_RETRYABLE",
             "worktree_disposition": "PRESERVED", "retry_domain": ""},
            outer_attempt=1, max_outer_attempts=3)
        self.assertEqual("AWAITING_QA", disposition)
        self.assertNotEqual("", reason)

    def test_qa_application_failure_routes_to_semantic_rework(self):
        disposition, reason = failure_disposition(
            {"failure_type": "qa", "fix_scope": "RETRYABLE",
             "worktree_disposition": "CLEAN_ROLLBACK", "retry_domain": ""},
            outer_attempt=1, max_outer_attempts=3)
        self.assertEqual("AWAITING_ENRICHMENT", disposition)
        self.assertEqual("", reason)


# --- Test plan / job contract plumbing -------------------------------------------

class TestPlanContractTests(unittest.TestCase):
    def test_plan_without_browser_qa_is_unchanged(self):
        declared = validate_test_contract({"required_tests": []})
        self.assertNotIn("browser_qa", declared)
        profile_request = SimpleNamespace(
                         requirement="suc/src/main/java/App.java 수정",
            target_resources=[], target_modules=["suc"],
            policy_overlays=(), test_plan={},
        )
        plan = derive_test_plan(profile_request, os.getcwd())
        self.assertNotIn("browser_qa", plan)

    def test_valid_declaration_freezes_into_plan(self):
        declared = validate_test_contract({"browser_qa": dict(SCENARIO_DECLARATION)})
        self.assertIn("browser_qa", declared)
        profile_request = SimpleNamespace(
                         requirement="suc/src/main/java/App.java 수정",
            target_resources=[], target_modules=["suc"],
            policy_overlays=(), test_plan={"browser_qa": dict(SCENARIO_DECLARATION)},
        )
        plan = derive_test_plan(profile_request, os.getcwd())
        self.assertEqual(dict(SCENARIO_DECLARATION), plan.get("browser_qa"))
        frozen = QAScenario.from_declaration(plan["browser_qa"])
        self.assertEqual(scenario().scenario_hash, frozen.scenario_hash)

    def test_invalid_declaration_rejected(self):
        with self.assertRaises(ValueError):
            validate_test_contract({"browser_qa": {"required": False}})
        with self.assertRaises(ValueError):
            validate_test_contract({"browser_qa": {
                **SCENARIO_DECLARATION, "side_effect_class": "WHATEVER"}})
        with self.assertRaises(ValueError):
            validate_test_contract({"browser_qa": {"required": True}})

    def test_job_contract_accepts_browser_qa_test_plan(self):
        job = request(test_plan={"browser_qa": dict(SCENARIO_DECLARATION)})
        self.assertEqual(dict(SCENARIO_DECLARATION), job.test_plan["browser_qa"])


# --- Manager gate (20.1 / 20.5) ---------------------------------------------------

from gate_evidence import identity as gate_identity


def _current_binding(task, working_dir):
    ident = gate_identity(task, working_dir)
    return {
        "candidate_id": task.candidate_id or task.task_id,
        "candidate_hash": ident["candidate_hash"],
        "acceptance_hash": ident["acceptance_hash"],
        "execution_surface_hash": ident["execution_surface_hash"],
    }

def _gate_result(status, binding=None, **overrides):
    bound = binding or BINDING
    # Synthetic provider report for this test fixture, never a live E2E claim.
    from harness_temp import scratch_root
    import uuid
    artifact = scratch_root("test") / ("qa-fixture-" + uuid.uuid4().hex + ".json")
    artifact.write_text('{"fixture":true,"observed":true}', encoding="utf8")
    values = {
        **{k: bound.get(k, "") for k in ("job_id", "execution_id", "attempt_id", "binding_identity", "verification_contract_hash")},
        "assertions": [{"name": "fixture scenario verdict", "passed": True}],
        "artifacts": [{"path": str(artifact)}],
        "provider": "MOMENTIC",
        "status": status,
        "scenario_id": scenario().scenario_id,
        "scenario_hash": scenario().scenario_hash,
        "candidate_id": bound.get("candidate_id", BINDING["candidate_id"]),
        "candidate_hash": bound.get("candidate_hash", BINDING["candidate_hash"]),
        "acceptance_hash": bound.get("acceptance_hash", BINDING["acceptance_hash"]),
        "execution_surface_hash": bound.get("execution_surface_hash",
                                            BINDING["execution_surface_hash"]),
        "qa_execution_id": bound.get("qa_execution_id", BINDING["qa_execution_id"]),
    }
    values.update(overrides)
    return QAExecutionResult(**values)


class BrowserQAGateTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._old_cwd)
        self.working_dir = self._tmp.name
        self.manager = Manager(working_dir=self.working_dir, model="",
                               max_retry=1, timeout=5, worker_type="droid",
                               max_workers=1)
        self.addCleanup(self.manager.shutdown)

    def _task(self, *, declaration=True):
        task = TaskState(task_id="TASK-20260915-QA0001",
                         requirement="suc/src/main/java/App.java 수정",
                         working_dir=self.working_dir, task_mode="MODIFICATION")
        if declaration:
            task.test_plan = {"production_scope": ["suc/src/main/java/App.java"],
                              "required_tests": [], "derived_tests": [],
                              "scope_hash": "t" * 64, "browser_qa": dict(SCENARIO_DECLARATION)}
        return task

    def _executor(self, result, calls):
        def wrapped(scenario=None, binding=None):
            calls.append((scenario, binding))
            # A synchronous fixture provider echoes the Harness invocation identity.
            for key in ("job_id", "execution_id", "attempt_id", "binding_identity", "qa_execution_id", "verification_contract_hash"):
                setattr(result, key, binding.get(key, ""))
            return result
        return wrapped

    def test_no_declaration_keeps_0992_path(self):
        calls = []
        task = self._task(declaration=False)
        self.manager.browser_qa_executor = self._executor(_gate_result(QA_STATUS_PASS), calls)
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["required"])
        self.assertTrue(gate["acceptable"])
        self.assertEqual("NOT_REQUIRED", gate["disposition"])
        self.assertEqual(0, len(calls))
        self.assertEqual({"required": False, "disposition": "NOT_REQUIRED"},
                         task.browser_qa)

    def test_required_pass_is_acceptable_and_records_binding(self):
        task = self._task()
        calls = []
        binding = _current_binding(task, self.working_dir)
        self.manager.browser_qa_executor = self._executor(
            _gate_result(QA_STATUS_PASS, binding=binding), calls)
        gate = self.manager._browser_qa_gate(task)
        self.assertTrue(gate["required"])
        self.assertTrue(gate["acceptable"])
        self.assertEqual("PASS", gate["disposition"])
        self.assertEqual(1, len(calls))
        executed_scenario, call_binding = calls[0]
        self.assertEqual(scenario().scenario_hash, executed_scenario.scenario_hash)
        self.assertTrue(call_binding["candidate_hash"])
        self.assertTrue(call_binding["acceptance_hash"])
        evidence = task.browser_qa["executions"][0]
        self.assertEqual(call_binding["candidate_hash"], evidence["candidate_hash"])
        self.assertEqual(call_binding["acceptance_hash"], evidence["acceptance_hash"])
        self.assertEqual(scenario().scenario_hash, evidence["scenario_hash"])

    def test_required_fail_blocks_success_as_qa_finding(self):
        task = self._task()
        binding = _current_binding(task, self.working_dir)
        self.manager.browser_qa_executor = self._executor(_gate_result(
            QA_STATUS_FAIL, binding=binding, failure_code=QA_ASSERTION_FAILED), [])
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["acceptable"])
        self.assertEqual("QA_FAIL", gate["disposition"])
        self.assertEqual(QA_ASSERTION_FAILED, gate["failure_code"])

    def test_required_infra_blocks_success_without_product_blame(self):
        task = self._task()
        binding = _current_binding(task, self.working_dir)
        self.manager.browser_qa_executor = self._executor(_gate_result(
            QA_STATUS_INFRA_FAILURE, binding=binding, failure_code=QA_INFRA_UNAVAILABLE,
            failure_class="infrastructure"), [])
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["acceptable"])
        self.assertEqual("QA_INFRA_UNAVAILABLE", gate["disposition"])

    def test_stale_binding_blocks_success(self):
        task = self._task()
        binding = _current_binding(task, self.working_dir)
        # Evidence bound to a DIFFERENT candidate must never verify.
        stale_binding = {**binding, "candidate_hash": "d" * 64}
        self.manager.browser_qa_executor = self._executor(
            _gate_result(QA_STATUS_PASS, binding=stale_binding), [])
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["acceptable"])
        self.assertEqual("STALE_QA_EVIDENCE", gate["disposition"])
        self.assertEqual(STALE_QA_EVIDENCE, gate["failure_code"])

    def test_adapter_exception_fails_closed_as_infra(self):
        task = self._task()

        def broken(scenario=None, binding=None):
            raise RuntimeError("provider transport exploded")

        self.manager.browser_qa_executor = broken
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["acceptable"])
        self.assertEqual("QA_INFRA_UNAVAILABLE", gate["disposition"])
        self.assertEqual(QA_INFRA_UNAVAILABLE, gate["failure_code"])

    def test_denied_scenario_blocks_without_execution(self):
        task = self._task()

        def denied(scenario=None, binding=None):
            raise QAScenarioDenied("side_effect_class=EXTERNAL_SEND requires authorization")

        self.manager.browser_qa_executor = denied
        gate = self.manager._browser_qa_gate(task)
        self.assertFalse(gate["acceptable"])
        self.assertEqual("DENIED", gate["disposition"])
        self.assertEqual(QA_SCENARIO_FORBIDDEN, gate["failure_code"])
        self.assertEqual([], task.browser_qa["executions"])


# --- Full pipeline integration (20.5 false success) ------------------------------

class PipelineVerificationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._old_cwd)
        self.working_dir = self._tmp.name
        self.manager = Manager(working_dir=self.working_dir, model="",
                               max_retry=1, timeout=5, worker_type="droid",
                               max_workers=1)
        self.addCleanup(self.manager.shutdown)

    def _run(self, *, qa_result_factory=None, qa_error=None, declare_qa=True):
        from worker import WorkerResult
        from git_collector import GitDiffResult
        from build_test_runner import BuildTestResult
        from reviewer import ReviewResult
        from task_state import ALREADY_SATISFIED_MARKER

        wr = WorkerResult(success=True, exit_code=0,
                          stdout=ALREADY_SATISFIED_MARKER, stderr="", command=[])
        git = GitDiffResult(success=True, changed_files=[], diff="")
        build = BuildTestResult(success=True, build_status="PASS", test_status="SKIPPED")

        class _Collector:
            def capture_baseline(self, task=None):
                return object(), ""

            def collect(self, task=None, baseline=None):
                task.changed_files = []
                task.git_status = ""
                task.git_diff = ""
                return git

            def repair_eol_to_index(self, task=None, baseline=None):
                from git_collector import EolRepairResult
                return EolRepairResult(True, [], [], "")

            def assess_commit_safety(self, task=None, baseline=None):
                return True, [], {}

        class _Worker:
            def execute(self, requirement, working_dir, task=None):
                return wr

        class _Runner:
            def run(self, changed_files=None, target_modules=None):
                return build

        class _Reviewer:
            def review(self, task=None, **kwargs):
                task.review_status = "REVIEW_PASS"
                return ReviewResult(passed=True, status="REVIEW_PASS")

        self.manager.collector = _Collector()
        self.manager._get_worker = lambda _task: (_Worker(), "droid")
        self.manager.build_test_runner = _Runner()
        self.manager.reviewer = _Reviewer()

        task = TaskState(task_id="TASK-20260915-PIPE001",
                         requirement="suc/src/main/java/App.java 현재 구현을 검증한다",
                         working_dir=self.working_dir, task_mode="MODIFICATION")
        task.verification_only = True
        if declare_qa:
            task.test_plan = {"production_scope": ["suc/src/main/java/App.java"],
                              "required_tests": [], "derived_tests": [],
                              "scope_hash": "t" * 64,
                              "browser_qa": dict(SCENARIO_DECLARATION)}

        calls = {"qa": 0}
        if declare_qa:
            binding = _current_binding(task, self.working_dir)
            if qa_error is not None:
                def executor(scenario=None, binding=None):
                    calls["qa"] += 1
                    raise qa_error
            else:
                def executor(scenario=None, binding=None):
                    calls["qa"] += 1
                    return qa_result_factory(binding)
            self.manager.browser_qa_executor = executor
        result = self.manager._run_pipeline(task)
        return result, task, calls

    def test_no_qa_declaration_identical_0992_success(self):
        result, task, calls = self._run(declare_qa=False)
        self.assertTrue(result.success)
        self.assertEqual("VERIFIED", task.verification_status)
        self.assertEqual(0, calls["qa"])

    def test_qa_required_current_pass_verifies(self):
        result, task, calls = self._run(
            qa_result_factory=lambda b: _gate_result(QA_STATUS_PASS, binding=b))
        self.assertTrue(result.success)
        self.assertEqual("VERIFIED", task.verification_status)
        self.assertEqual(1, calls["qa"])
        self.assertEqual("PASS", task.browser_qa["disposition"])

    def test_qa_required_recovered_pass_verifies_with_distinct_status(self):
        result, task, _ = self._run(
            qa_result_factory=lambda b: _gate_result(
                QA_STATUS_RECOVERED_PASS, binding=b, recovery_used=True))
        self.assertTrue(result.success)
        self.assertEqual("VERIFIED", task.verification_status)
        self.assertEqual(QA_STATUS_RECOVERED_PASS, task.browser_qa["latest_status"])

    def test_qa_required_missing_evidence_forbids_success(self):
        # Adapter produces a result that is not bound to this candidate.
        result, task, _ = self._run(
            qa_result_factory=lambda b: _gate_result(
                QA_STATUS_PASS, candidate_hash="0" * 64, acceptance_hash="0" * 64))
        self.assertFalse(result.success)
        self.assertNotEqual("VERIFIED", task.verification_status)
        self.assertEqual("STALE_QA_EVIDENCE", task.failure_code)

    def test_qa_assertion_fail_is_product_failure_not_success(self):
        result, task, _ = self._run(
            qa_result_factory=lambda b: _gate_result(
                QA_STATUS_FAIL, binding=b, failure_code=QA_ASSERTION_FAILED))
        self.assertFalse(result.success)
        self.assertEqual("QA", task.failure_stage)
        self.assertEqual("qa", task.failure_type)
        self.assertEqual("FAILED", task.verification_status)
        self.assertEqual("RETRYABLE", task.fix_scope)

    def test_qa_infra_unavailable_blocks_without_product_blame(self):
        result, task, _ = self._run(
            qa_result_factory=lambda b: _gate_result(
                QA_STATUS_INFRA_FAILURE, binding=b, failure_code=QA_INFRA_UNAVAILABLE,
                failure_class="infrastructure"))
        self.assertFalse(result.success)
        self.assertEqual("QA", task.failure_stage)
        self.assertEqual("infrastructure", task.failure_type)
        self.assertEqual("NON_RETRYABLE", task.fix_scope)
        self.assertEqual("PARTIAL", task.verification_status)

    def test_denied_scenario_in_pipeline_blocks_without_execution(self):
        result, task, calls = self._run(qa_error=QAScenarioDenied(
            "side_effect_class=EXTERNAL_SEND requires explicit authorization"))
        self.assertFalse(result.success)
        self.assertEqual(1, calls["qa"])
        self.assertEqual("QA_SCENARIO_FORBIDDEN", task.failure_code)
        self.assertEqual("QA", task.failure_stage)

    def test_qa_evidence_never_contains_credentials(self):
        result = _gate_result(
            QA_STATUS_PASS,
            provider_metadata={"MOMENTIC_API_KEY": "sk-live-secret", "run_id": "r1"},
        )
        payload = result.to_dict()
        self.assertNotIn("sk-live-secret", json.dumps(payload))
        self.assertEqual("r1", payload["provider_metadata"]["run_id"])


# --- 0.99.2.1 completion hotfix: stale result / correlation ----------------------

def _write_json(path: Path, payload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


def _momentic_report(runs, *, executed=None):
    executed = len(runs) if executed is None else executed
    return {
        "version": 1,
        "summary": {"total": executed, "executed": executed,
                    "passed": sum(1 for r in runs if r.get("status") == "PASSED"),
                    "failed": sum(1 for r in runs if r.get("status") != "PASSED")},
        "runs": runs,
        "skipped": [],
    }


class StaleMomenticResultTests(unittest.TestCase):
    """12.1: an OLD scenario's PASS must never satisfy a NEW execution."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.report = _write_json(
            Path(self._tmp.name) / "app.momentic.json",
            _momentic_report([{
                "testId": "oldscenario", "testName": "oldscenario",
                "status": "PASSED", "recovered": False, "attempts": 1,
                "failure": None,
            }]),
        )

    def test_foreign_scenario_pass_is_rejected(self):
        outcome = qa_browser_providers._parse_momentic_report(
            self.report, "NewScenario", qa_execution_id="QAX-NEW-1",
            started_epoch=time.time())
        self.assertIsNotNone(outcome)
        self.assertFalse(outcome.passed)
        self.assertEqual(QA_RESULT_CORRELATION_MISMATCH, outcome.failure_code)

    def test_report_predating_execution_is_rejected(self):
        # Same scenario id, but the artifact predates the current execution.
        outcome = qa_browser_providers._parse_momentic_report(
            self.report, "OLDSCENARIO", qa_execution_id="QAX-NEW-2",
            started_epoch=time.time() + 3600)
        self.assertIsNotNone(outcome)
        self.assertFalse(outcome.passed)
        self.assertEqual(QA_RESULT_CORRELATION_MISMATCH, outcome.failure_code)

    def test_matching_current_report_still_passes(self):
        outcome = qa_browser_providers._parse_momentic_report(
            self.report, "OLDSCENARIO", qa_execution_id="QAX-CUR-1",
            started_epoch=time.time())
        self.assertIsNotNone(outcome)
        self.assertTrue(outcome.passed)
        self.assertEqual("", outcome.failure_code)
        self.assertEqual("QAX-CUR-1",
                         outcome.provider_metadata.get("qa_execution_id"))


class StaleStagehandResultTests(unittest.TestCase):
    """12.2/12.4: stale result.json must never become a current PASS."""

    def _result(self, scenario_id, qa_execution_id):
        return {
            "schema_revision": "qa-browser-runner/1", "provider": "STAGEHAND",
            "status": "PASS", "passed": True, "recovery_used": False,
            "scenario_id": scenario_id, "qa_execution_id": qa_execution_id,
            "failure_code": "", "failure_message": "", "assertions": [],
            "browser": "Browserbase", "browser_version": "",
            "usage": {}, "provider_metadata": {"session_id": "harness-qa-old"},
        }

    def test_parser_rejects_stale_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            stale = _write_json(Path(tmp) / "result.json",
                                self._result("OldOne", "QAX-OLD-1"))
            outcome = qa_browser_providers._parse_stagehand_result(
                stale, scenario(), qa_execution_id="QAX-NEW-1")
        self.assertIsNotNone(outcome)
        self.assertFalse(outcome.passed)
        self.assertEqual(QA_RESULT_CORRELATION_MISMATCH, outcome.failure_code)

    def test_parser_rejects_scenario_mismatch_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            stale = _write_json(Path(tmp) / "result.json",
                                self._result("Different", "QAX-NEW-1"))
            outcome = qa_browser_providers._parse_stagehand_result(
                stale, scenario(), qa_execution_id="QAX-NEW-1")
        self.assertFalse(outcome.passed)
        self.assertEqual(QA_RESULT_CORRELATION_MISMATCH, outcome.failure_code)

    def test_parser_accepts_correlated_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            current = _write_json(
                Path(tmp) / "result.json",
                self._result(scenario().scenario_id, "QAX-NEW-1"))
            outcome = qa_browser_providers._parse_stagehand_result(
                current, scenario(), qa_execution_id="QAX-NEW-1")
        self.assertTrue(outcome.passed)

    def test_stale_file_is_deleted_before_failed_run(self):
        execution_id = "QAX-STALE-DEL-1"
        work = qa_browser_providers._execution_dir(execution_id, "stagehand")
        stale = _write_json(work / "result.json",
                            self._result("OldOne", "QAX-OLD-1"))

        def fake_run(command, *, timeout, cwd):
            return 1, "", "browser session exploded"

        with mock.patch.object(qa_browser_providers, "_run", fake_run):
            result = qa_browser_providers.run_stagehand(
                scenario(), qa_execution_id=execution_id)
        self.assertFalse(result.passed)
        self.assertNotEqual("PASS", result.status)
        self.assertFalse(stale.exists(),
                         "stale result must be removed before the run starts")

    def test_missing_result_after_failure_is_infra(self):
        def fake_run(command, *, timeout, cwd):
            return 1, "", "fatal"

        with mock.patch.object(qa_browser_providers, "_run", fake_run):
            result = qa_browser_providers.run_stagehand(
                scenario(), qa_execution_id="QAX-NORESULT-1")
        self.assertFalse(result.passed)
        self.assertEqual(qa_browser_providers.QA_BROWSER_INFRA_FAILURE,
                         result.failure_code)


class QAExecutionIdentityTests(unittest.TestCase):
    """12.3: one canonical qa_execution_id across the whole lineage."""

    def test_primary_execution_identity_flows_through(self):
        received = []

        def momentic(_scenario, *, qa_execution_id=""):
            received.append(("MOMENTIC", qa_execution_id))
            return outcome(passed=True)

        adapter = QAAdapter(momentic=momentic,
                            stagehand=lambda _s, *, qa_execution_id="": outcome())
        result = adapter.execute(scenario=scenario(), binding=BINDING)
        self.assertEqual([("MOMENTIC", BINDING["qa_execution_id"])], received)
        self.assertEqual(BINDING["qa_execution_id"], result.qa_execution_id)
        evidence = result.to_dict()
        self.assertEqual(BINDING["qa_execution_id"],
                         evidence.get("qa_execution_id"))

    def test_fallback_reuses_parent_identity(self):
        received = []

        def momentic(_scenario, *, qa_execution_id=""):
            received.append(("MOMENTIC", qa_execution_id))
            return outcome(passed=False, status="EXIT_1",
                           failure_code=QA_PROVIDER_AUTH_FAILURE)

        def stagehand(_scenario, *, qa_execution_id=""):
            received.append(("STAGEHAND", qa_execution_id))
            return outcome(provider="STAGEHAND", passed=True)

        adapter = QAAdapter(momentic=momentic, stagehand=stagehand)
        result = adapter.execute(scenario=scenario(), binding=BINDING)
        self.assertEqual(
            [("MOMENTIC", BINDING["qa_execution_id"]),
             ("STAGEHAND", BINDING["qa_execution_id"])],
            received,
        )
        self.assertEqual(BINDING["qa_execution_id"], result.qa_execution_id)
        self.assertEqual("MOMENTIC", result.fallback_from)

    def test_execution_directory_is_per_execution(self):
        first = qa_browser_providers._execution_dir("QAX-DIR-1", "momentic")
        second = qa_browser_providers._execution_dir("QAX-DIR-2", "momentic")
        self.assertNotEqual(first, second)
        self.assertIn("QAX-DIR-1", str(first))
        self.assertTrue(first.is_dir() and second.is_dir())


# --- 12.6/12.7/12.8 action contract ---------------------------------------------

class ScenarioActionPolicyTests(unittest.TestCase):
    def test_normalize_mapping(self):
        self.assertEqual("navigate", normalize_step_action("goto"))
        self.assertEqual("navigate", normalize_step_action("Navigate"))
        self.assertEqual("assert", normalize_step_action("assert_contains"))
        self.assertEqual("assert", normalize_step_action("assert_visible"))
        self.assertEqual("assert", normalize_step_action("assert"))
        self.assertEqual("click", normalize_step_action("doubleClick"))
        self.assertEqual("type", normalize_step_action("fill"))
        self.assertEqual("type", normalize_step_action("input"))
        self.assertEqual("download", normalize_step_action("download"))
        self.assertEqual("upload", normalize_step_action("upload"))
        self.assertEqual("", normalize_step_action("teleport"))

    def _denied(self, declaration):
        calls = []

        def momentic(_scenario, *, qa_execution_id=""):
            calls.append(1)
            return outcome(passed=True)

        def stagehand(_scenario, *, qa_execution_id=""):
            calls.append(1)
            return outcome(provider="STAGEHAND", passed=True)

        adapter = QAAdapter(momentic=momentic, stagehand=stagehand)
        with self.assertRaises(QAScenarioDenied) as caught:
            adapter.execute(scenario=QAScenario.from_declaration(declaration),
                            binding=BINDING)
        self.assertEqual(0, len(calls), "denied before any provider execution")
        return caught.exception

    def test_undeclared_step_action_denied_before_execution(self):
        declaration = {**SCENARIO_DECLARATION,
                       "allowed_actions": ["navigate", "assert"],
                       "steps": [{"action": "click", "value": "조회 버튼"}]}
        exc = self._denied(declaration)
        self.assertEqual(QA_SCENARIO_ACTION_NOT_ALLOWED, exc.code)

    def test_unsupported_step_action_denied(self):
        declaration = {**SCENARIO_DECLARATION,
                       "steps": [{"action": "teleport", "value": "x"}]}
        exc = self._denied(declaration)
        self.assertEqual(QA_SCENARIO_ACTION_NOT_ALLOWED, exc.code)

    def test_forbidden_step_action_wins_over_allowed(self):
        declaration = {**SCENARIO_DECLARATION,
                       "allowed_actions": ["navigate", "assert", "type"],
                       "forbidden_actions": ["save", "type"],
                       "steps": [{"action": "fill", "value": "메모",
                                  "text": "hello"}]}
        exc = self._denied(declaration)
        self.assertEqual(QA_SCENARIO_FORBIDDEN, exc.code)

    def test_declared_actions_admit(self):
        SideEffectPolicy().ensure_allowed(scenario())  # navigate + assert steps
        upload_scenario = QAScenario.from_declaration({**SCENARIO_DECLARATION,
            "allowed_actions": ["navigate", "assert", "download"],
            "steps": [{"action": "goto", "value": "https://example.com"},
                      {"action": "download", "value": "결과 엑셀"}]})
        SideEffectPolicy().ensure_allowed(upload_scenario)


# --- 12.9 secret literal ----------------------------------------------------------

class SecretLiteralScenarioTests(unittest.TestCase):
    def test_api_key_literal_rejected(self):
        bad = {**SCENARIO_DECLARATION,
               "steps": [{"action": "type", "value": "API 키 입력",
                          "text": "sk-abcdefghijklmnopqrstuvwxyz1234"}]}
        with self.assertRaises(ValueError) as caught:
            QAScenario.from_declaration(bad)
        self.assertEqual(QA_SCENARIO_SECRET_LITERAL_FORBIDDEN, str(caught.exception))

    def test_password_literal_rejected(self):
        bad = {**SCENARIO_DECLARATION,
               "steps": [{"action": "type", "value": "비밀번호",
                          "text": "password: hunter2secret"}]}
        with self.assertRaises(ValueError) as caught:
            QAScenario.from_declaration(bad)
        self.assertEqual(QA_SCENARIO_SECRET_LITERAL_FORBIDDEN, str(caught.exception))

    def test_bearer_header_rejected_in_url(self):
        bad = {**SCENARIO_DECLARATION,
               "start_url": "https://x.example/?Authorization=Bearer eyJaaaaaaaaaaaaaaaaaa.bbbbbbbbbbbbbbbbbb.cccccccccccccc"}
        with self.assertRaises(ValueError) as caught:
            QAScenario.from_declaration(bad)
        self.assertEqual(QA_SCENARIO_SECRET_LITERAL_FORBIDDEN, str(caught.exception))

    def test_env_reference_is_allowed(self):
        good = {**SCENARIO_DECLARATION,
                "allowed_actions": ["navigate", "assert", "type"],
                "steps": [{"action": "type", "value": "계정",
                           "text": "{{ env.QA_USER_EMAIL }}"}]}
        parsed = QAScenario.from_declaration(good)
        self.assertEqual("{{ env.QA_USER_EMAIL }}",
                         parsed.steps[0]["text"])

    def test_declaration_secret_blocked_at_test_plan_boundary(self):
        bad = {"browser_qa": {**SCENARIO_DECLARATION,
                              "steps": [{"action": "type", "value": "토큰",
                                         "text": "token: ghp_abcdefghijklmnopqrst"}]}}
        with self.assertRaises(ValueError):
            validate_test_contract(bad)


# --- 12.10 materialization cleanup -------------------------------------------------

class MaterializationCleanupTests(unittest.TestCase):
    def test_momentic_generated_yaml_cleaned_after_run(self):
        generated = (qa_browser_providers.MOMENTIC_QA_DIR
                     / f"{scenario().scenario_id.lower()[:60]}.test.yaml")

        def fake_run(command, *, timeout, cwd):
            return 1, "", "fatal before report"

        with mock.patch.object(qa_browser_providers, "_run", fake_run):
            result = qa_browser_providers.run_momentic(
                scenario(), qa_execution_id="QAX-CLEAN-1")
        self.assertFalse(result.passed)
        self.assertFalse(generated.exists(),
                         "generated momentic yaml must be cleaned up after the run")

    def test_stagehand_generated_input_cleaned_after_run(self):
        execution_id = "QAX-CLEAN-2"
        work = qa_browser_providers._execution_dir(execution_id, "stagehand")
        input_path = work / "input.json"

        def fake_run(command, *, timeout, cwd):
            return 1, "", "fatal"

        with mock.patch.object(qa_browser_providers, "_run", fake_run):
            qa_browser_providers.run_stagehand(
                scenario(), qa_execution_id=execution_id)
        self.assertFalse(input_path.exists(),
                         "generated stagehand input must be cleaned up after the run")


# --- 12.11 release manifest binding -------------------------------------------------

class ReleaseManifestTests(unittest.TestCase):
    REQUIRED_INTEGRATION_PATHS = (
        "../../integrations/stagehand/qa-runner.ts",
        "../../integrations/stagehand/opencode-go-deepseek-client.ts",
        "../../integrations/stagehand/run_stagehand.bat",
        "../../integrations/momentic/run_momentic.bat",
        "../../integrations/momentic/momentic.config.yaml",
        "../../integrations/momentic/canary/harness-canary-readonly.test.yaml",
    )

    def test_manifest_covers_and_matches_qa_runtime_files(self):
        # 0.99.2.2: bind the check to the current VERSION so each release's
        # manifest is verified against the live tree it shipped from.
        version = (Path(__file__).resolve().parent / "VERSION").read_text(
            encoding="utf-8"
        ).strip()
        manifest_path = (Path(__file__).resolve().parent
                         / "releases" / version / "SHA256SUMS.json")
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(version, data["version"])
        listed = {}
        for entry in data["files"]:
            normalized = str(entry["path"]).replace("\\", "/")
            listed[normalized] = str(entry["sha256"])
            target = (manifest_path.parent / entry["path"]).resolve()
            self.assertTrue(target.is_file(), f"missing file {entry['path']}")
            actual = hashlib.sha256(target.read_bytes()).hexdigest()
            self.assertEqual(
                str(entry["sha256"]), actual,
                f"hash mismatch for {entry['path']}")
        for required in self.REQUIRED_INTEGRATION_PATHS:
            self.assertIn(required, listed,
                          f"QA runtime file not bound in manifest: {required}")


if __name__ == "__main__":
    unittest.main()
