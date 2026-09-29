"""Explicit 1.0.x TestPlan boundary; no environment-based or inferred opt-in.

The owner of the prepared contract must affirm that the command assertions
suffice for the stated tests. This validates observation/identity, not the
semantic quality of user-authored tests. UI/business judgment remains agent QA.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

LEGACY_POLICY_VERSION = "deterministic-tester/1.0x-1"
POLICY_VERSION = "deterministic-tester/1.0x-2"
KIND = "DETERMINISTIC_TESTER"

def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()

def validate_declaration(plan: Mapping[str, Any]) -> None:
    evaluation = plan.get("evaluation")
    if evaluation is None:
        if "test_commands" in plan:
            raise ValueError("TEST_COMMAND_MAP_REQUIRES_VERSIONED_EVALUATOR")
        return
    if not isinstance(evaluation, dict) or set(evaluation) != {
        "policy_version", "kind", "coverage", "assertions"}:
        raise ValueError("DETERMINISTIC_TESTER_DECLARATION_INVALID")
    if (evaluation["policy_version"] not in {POLICY_VERSION, LEGACY_POLICY_VERSION} or evaluation["kind"] != KIND
            or evaluation["coverage"] != "COMMAND_ASSERTIONS"):
        raise ValueError("DETERMINISTIC_TESTER_POLICY_UNSUPPORTED")
    if plan.get("mandatory") is not True or plan.get("browser_qa"):
        raise ValueError("DETERMINISTIC_TESTER_SEMANTIC_SCOPE_UNSUPPORTED")
    if str(plan.get("verification_depth") or "").upper() not in {"", "STATIC", "TARGETED"}:
        raise ValueError("DETERMINISTIC_TESTER_SEMANTIC_SCOPE_UNSUPPORTED")
    required = plan.get("required_tests")
    if (not isinstance(required, list) or not required or
            any(not isinstance(x, str) or not x.strip() for x in required) or
            len(set(required)) != len(required)):
        raise ValueError("DETERMINISTIC_TESTER_REQUIRED_TESTS_INVALID")
    commands, assertions = plan.get("test_commands"), evaluation["assertions"]
    if (not isinstance(commands, dict) or not isinstance(assertions, dict)
            or set(commands) != set(required) or set(assertions) != set(required)):
        raise ValueError("DETERMINISTIC_TESTER_COVERAGE_INVALID")
    for test in required:
        command, checks = commands[test], assertions[test]
        if not isinstance(command, str) or not command.strip() or len(command) > 4000:
            raise ValueError("DETERMINISTIC_TESTER_COMMAND_INVALID")
        if not isinstance(checks, dict) or set(checks) != {"stdout_contains", "stderr_not_contains"}:
            raise ValueError("DETERMINISTIC_TESTER_ASSERTIONS_INVALID")
        for key, values in checks.items():
            if (not isinstance(values, list) or len(values) > 32 or
                    any(not isinstance(x, str) or not x.strip() or len(x) > 512 for x in values)):
                raise ValueError("DETERMINISTIC_TESTER_ASSERTIONS_INVALID")
        if not checks["stdout_contains"]:
            raise ValueError("DETERMINISTIC_TESTER_POSITIVE_ASSERTION_REQUIRED")

def selection(plan: Mapping[str, Any]) -> dict[str, Any] | None:
    if plan.get("evaluation") is None:
        return None
    validate_declaration(plan)
    return {"runtime": "deterministic", "kind": KIND, "model": "none",
            "policy_version": plan["evaluation"]["policy_version"], "technical_retries": 0,
            "evaluation_sha256": fingerprint({"evaluation": plan["evaluation"],
                                                "test_commands": plan["test_commands"]})}

def assert_frozen_selection(task: Any) -> None:
    selected = selection(task.test_plan)
    frozen = dict(getattr(task, "materialized_execution", {}) or {})
    active_policy = dict(getattr(task, "effective_policy", {}) or {}).get("resolved_tester")
    frozen_policy = dict(frozen.get("effective_policy") or {}).get("resolved_tester")
    # A test-plan amendment is not an implicit runtime/policy amendment.
    if (not selected or active_policy != selected or frozen_policy != selected
            or frozen.get("test_plan") != task.test_plan
            or not task.test_scope_hash or task.test_scope_hash != task.test_plan.get("scope_hash")
            or not task.execution_id or frozen.get("execution_id") != task.execution_id
            or frozen.get("job_id") != task.job_id or not task.candidate_id
            or not task.tester_attempt_id):
        raise ValueError("DETERMINISTIC_TESTER_FROZEN_CONTRACT_MISMATCH")
    from runtime_contract import assert_role_contract_compatible
    contracts = dict(frozen.get("runtime_contracts") or {})
    if not dict(contracts.get("roles") or {}).get("TESTER"):
        raise ValueError("DETERMINISTIC_TESTER_RUNTIME_CONTRACT_REQUIRED")
    assert_role_contract_compatible(contracts, descriptor(selected["policy_version"]), "TESTER")

def assertions_for(plan: Mapping[str, Any], tests: tuple[str, ...]) -> dict[str, Any]:
    return {test: plan["evaluation"]["assertions"][test] for test in tests}

def check_output(checks: Mapping[str, Any], stdout: str, stderr: str) -> bool:
    return all(all(token in stdout for token in spec["stdout_contains"])
               and all(token not in stderr for token in spec["stderr_not_contains"])
               for spec in checks.values())

def descriptor(version=POLICY_VERSION):
    if version not in {POLICY_VERSION, LEGACY_POLICY_VERSION}:
        raise ValueError("DETERMINISTIC_TESTER_POLICY_UNSUPPORTED")
    from runtime_adapter import RuntimeCapabilities, RuntimeDescriptor
    return RuntimeDescriptor(runtime="deterministic-tester", version=version,
        capabilities=RuntimeCapabilities(False, False, False, False, False, False),
        protocol_revision="authorized-host-command/1",
        structured_output_schema_revision=("test-execution-receipt/1.0x-2" if version == POLICY_VERSION
                                             else "test-execution-receipt/1.0x-1"),
        permission_tool_profile="FROZEN_TEST_COMMANDS_ONLY",
        sandbox_semantics="EXISTING_HOST_TEST_BOUNDARY_NOT_DOCKER",
        validation_disposition="NOT_RUN", live_model_validated=False)
