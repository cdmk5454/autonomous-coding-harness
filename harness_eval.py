"""Deterministic Harness behavior eval runner; no LLM judge is involved."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from runtime_adapter import ADAPTER_API_REVISION, payload_sha256


GRADER_REVISION = "harness-deterministic-graders/1"
CORPUS_REVISION = "harness-eval-corpus/1"
GRADERS = (
    "STATE_GRADER", "GATE_GRADER", "EVIDENCE_GRADER", "OWNERSHIP_GRADER",
    "RETRY_GRADER", "TRACE_GRADER", "SIDE_EFFECT_GRADER",
    "PRODUCT_READINESS_GRADER", "VERIFICATION_GRADER",
)


class EvalSchemaError(RuntimeError):
    pass


def _trace(state: dict[str, Any], event: str) -> None:
    state.setdefault("trace", []).append(event)


def simulate(case: Mapping[str, Any]) -> dict[str, Any]:
    state = copy.deepcopy(dict(case.get("initial_state") or {}))
    state.setdefault("state", "RUNNING")
    state.setdefault("gate", "OPEN")
    state.setdefault("disposition", "PENDING")
    state.setdefault("ownership", {"writer_count": 1, "writer": "WORKER"})
    state.setdefault("retry", {"domain": "", "delta": 0})
    state.setdefault("evidence", "PENDING")
    state.setdefault("product_readiness", "FINALIZATION_PENDING")
    state.setdefault("verification_depth", "STATIC")
    state.setdefault("side_effect", "NONE")
    state.setdefault("dispatch_count", 0)
    state.setdefault("notification_count", 0)
    state.setdefault("trace", [])
    state.setdefault("flags", {})

    for raw in case.get("events") or []:
        event = dict(raw)
        kind = str(event.get("type") or "")
        _trace(state, kind)
        retry0 = {"domain": str(event.get("retry_domain") or "TECHNICAL_EXECUTION_RETRY"), "delta": 0}
        if kind in {"POLICY_DRIFT", "RUNTIME_SCHEMA_MISMATCH", "MISSING_EVIDENCE_BINDING", "STALE_CANDIDATE"}:
            state.update(state="BLOCKED", gate="HOLD", disposition="TECHNICAL_FAILURE",
                         evidence="INVALID", retry=retry0, product_readiness="FINALIZATION_PENDING")
        elif kind in {"REVIEW_SCHEMA_INVALID", "REVIEW_PROVIDER_FAILURE"}:
            state.update(state="REVIEW_TECHNICAL_RECOVERY", gate="HOLD",
                         disposition="TECHNICAL_FAILURE", retry=retry0)
            state["flags"]["candidate_preserved"] = True
        elif kind == "IPC_RUNTIME_FAILURE":
            state.update(state="RUNTIME_RECOVERY", gate="HOLD", disposition="TECHNICAL_FAILURE",
                         retry={"domain": "RUNTIME_RECOVERY", "delta": 0})
            state["flags"]["no_meaningful_source_change"] = False
        elif kind == "VERIFIED_NO_TASK_DELTA":
            state.update(state="SUCCEEDED", gate="PASS", disposition="VERIFIED_NO_TASK_DELTA",
                         evidence="BOUND", product_readiness="COMPLETE", side_effect="NONE")
        elif kind == "FAILED_FINAL_CANDIDATE":
            state.update(state="FAILED_FINAL", gate="FAIL", disposition="PRESERVED_CANDIDATE",
                         product_readiness="FINALIZATION_PENDING")
            state["flags"]["candidate_preserved"] = True
        elif kind in {"QA_TAKEOVER", "CANDIDATELESS_QA", "DECISION_REQUIRED"}:
            state.update(state="AWAITING_QA", gate="DECISION", disposition="PENDING_DECISION",
                         ownership={"writer_count": 0, "writer": "SUSPENDED"},
                         retry={"domain": "", "delta": 0}, product_readiness="SKELETON_READY")
            state["flags"]["decision_visible"] = True
        elif kind == "OPERATOR_HOLD":
            state.update(state="BLOCKED", gate="OPERATOR_HOLD", disposition="PRESERVED")
            state["flags"]["independent_hold_preserved"] = True
        elif kind in {"TERMINAL_EXECUTION_REUSE", "COMPLETED_TAKEOVER"}:
            state.update(gate="REJECTED", disposition="IMMUTABLE_TERMINAL")
            state["flags"]["transition_rejected"] = True
        elif kind in {"LATE_ATTEMPT_RESULT", "LATE_RUNTIME_EVENT"}:
            state.update(gate="RECONCILE", disposition="LATE_IGNORED")
            state["flags"]["late_event_applied"] = False
        elif kind == "QA_RESOLUTION":
            if int(event.get("revision", -1)) != int(event.get("expected_revision", 0)):
                state.update(gate="REJECTED", disposition="STALE_QA_REJECTED")
                state["flags"]["stale_qa_accepted"] = False
            else:
                state.update(state="FINALIZATION_PENDING", gate="JIT_MATERIALIZATION",
                             disposition="QA_RESOLVED", product_readiness="FINALIZATION_PENDING",
                             ownership={"writer_count": 0, "writer": "NONE"})
        elif kind == "NOT_RUN_BY_POLICY":
            state.update(disposition="NOT_RUN_BY_POLICY", retry={"domain": "", "delta": 0})
        elif kind == "CONTRACT_UNRESOLVED":
            state.update(state="AWAITING_QA", gate="DECISION", disposition="PENDING_DECISION",
                         retry={"domain": "", "delta": 0}, product_readiness="SKELETON_READY")
        elif kind == "REPORT_COMPLETE":
            safe = state.get("evidence") == "BOUND" and int(event.get("open_decisions", 0)) == 0
            if safe:
                state.update(state="SUCCEEDED", gate="PASS", disposition="VERIFIED",
                             product_readiness="COMPLETE")
            else:
                state.update(gate="HOLD", disposition="FALSE_COMPLETE_BLOCKED",
                             product_readiness="FINALIZATION_PENDING")
                state["flags"]["false_complete"] = False
        elif kind == "NOTIFY":
            fingerprint = str(event.get("fingerprint") or "")
            seen = state.setdefault("notification_fingerprints", [])
            if fingerprint and fingerprint not in seen:
                seen.append(fingerprint)
                state["notification_count"] += 1
        elif kind == "PROCESS_KILL":
            state.update(state="RUNTIME_RECOVERY", gate="RECONCILE",
                         retry={"domain": "RUNTIME_RECOVERY", "delta": 0})
        elif kind == "SESSION_RESUME":
            state.update(state="RUNNING", gate="OPEN", disposition="RESUMED")
            state["flags"]["session_resumed"] = True
        elif kind == "RESUME_IMPOSSIBLE":
            state.update(state="FRESH_ATTEMPT", gate="JIT_ATTEMPT",
                         disposition="PRESERVED_CONTEXT", retry={"domain": "RUNTIME_RECOVERY", "delta": 0})
            state["flags"]["candidate_preserved"] = True
        elif kind == "RUNTIME_RECOVERY_EXHAUSTED":
            state.update(state="BLOCKED", gate="TECHNICAL_HOLD", disposition="TECHNICAL_FAILURE",
                         retry={"domain": "RUNTIME_RECOVERY", "delta": 0})
        elif kind == "COMMAND_SENT":
            state["dispatch_count"] += 1
        elif kind == "ACK_LOST":
            state.update(gate="RECONCILE", disposition="QUERY_DO_NOT_RESEND")
        elif kind == "QA_DUPLICATE":
            state["flags"]["canonical_resolution_count"] = 1
            state.update(disposition="IDEMPOTENT_QA")
        elif kind == "LEASE_EXPIRED_OLD_WRITER_ALIVE":
            state.update(gate="FENCED", disposition="OLD_WRITER_ACTIVE",
                         ownership={"writer_count": 1, "writer": "OLD_WRITER"})
        elif kind == "CANDIDATE_REVALIDATION":
            state.update(gate="VALIDATE", disposition="REVALIDATION_ONLY")
            state["flags"]["coding_worker_called"] = False
        elif kind == "DEFERRED_DECISION":
            state.update(state="SKELETON_READY", gate="DECISION", disposition="PENDING_DECISION",
                         product_readiness="SKELETON_READY")
            state["flags"]["independent_job_continue"] = True
        elif kind == "FINALIZATION_MATERIALIZED":
            state.update(state="RUNNING", gate="OPEN", disposition="FRESH_FINALIZATION",
                         ownership={"writer_count": 1, "writer": "WORKER"})
            state["flags"]["fresh_execution"] = True
        elif kind == "GLOBAL_POLICY_CHANGE":
            state["flags"]["frozen_policy_preserved"] = True
            state.update(disposition="FROZEN_EXECUTION_CONTINUES")
        elif kind == "MANAGED_DIRECT_MUTATION":
            state.update(gate="BLOCKED", disposition="UNAUTHORIZED_MUTATION_BLOCKED", side_effect="NONE")
        elif kind == "DROID_FALLBACK":
            state.update(state="FRESH_ATTEMPT", gate="JIT_ATTEMPT", disposition="DROID_FALLBACK",
                         retry={"domain": "RUNTIME_RECOVERY", "delta": 0})
            state["flags"].update(old_writer_stopped=True, candidate_preserved=True)
        elif kind == "DB_RESTORE":
            state.update(gate="PASS", disposition="RESTORED", evidence="BOUND", side_effect="CONTROL_COPY_ONLY")
            state["flags"].update(sqlite_consistent=True, artifact_refs_intact=True)
        elif kind == "INCIDENT_EXPORT":
            redacted = bool(event.get("secrets_redacted")) and bool(event.get("pii_redacted"))
            state.update(gate="PASS" if redacted else "HOLD", evidence="REDACTED" if redacted else "INVALID")
            state["flags"]["diagnostic_identities_preserved"] = bool(event.get("identities_preserved"))
        elif kind == "SOURCEVIEW_RECONNECT":
            state.update(disposition="SAME_SOURCE_VIEW", ownership={"writer_count": 1, "writer": "WORKER"})
            state["flags"]["source_view_preserved"] = True
        elif kind == "DUPLICATE_RUNTIME_EVENT":
            state["flags"]["event_apply_count"] = 1
            state.update(disposition="APPLY_ONCE")
        elif kind == "SESSION_FORK":
            state.update(disposition="REFERENCE_ONLY", ownership={"writer_count": 0, "writer": "NONE"})
            state["flags"]["rollback_evidence"] = False
        elif kind == "NATIVE_QUEUE_PRELOAD":
            state.update(gate="BLOCKED", disposition="NATIVE_BATCH_PRELOAD_FORBIDDEN")
            state["flags"]["scheduler_bypassed"] = False
        elif kind == "ROLE_SESSION_SHARE":
            state.update(gate="BLOCKED", disposition="NATIVE_SESSION_ROLE_SHARE_FORBIDDEN")
            state["flags"]["role_session_shared"] = False
        elif kind == "QA_BACKGROUND_FOLLOWUP":
            state.update(state="AWAITING_QA", gate="BLOCKED", disposition="WRITER_AUTHORITY_SUSPENDED",
                         ownership={"writer_count": 0, "writer": "SUSPENDED"}, side_effect="NONE")
        elif kind == "MCP_RUNTIME_FAILURE":
            state.update(state="BLOCKED", gate="TECHNICAL_HOLD", disposition="MCP_UNAVAILABLE",
                         retry={"domain": "TECHNICAL_EXECUTION_RETRY", "delta": 0})
        elif kind == "SECRET_IN_PROCESS_ARGV":
            flags = {
                "secret_in_process_argv": bool(event.get("secret_in_process_argv")),
                "secret_in_logs": bool(event.get("secret_in_logs")),
                "secret_in_evidence": bool(event.get("secret_in_evidence")),
                "secret_in_sqlite": bool(event.get("secret_in_sqlite")),
                "secret_in_incident_bundle": bool(event.get("secret_in_incident_bundle")),
            }
            state["flags"].update(flags)
            safe = (
                event.get("credential_transport") in {"ENVIRONMENT", "STDIN", "PROTECTED_CONFIG"}
                and not any(flags.values())
            )
            state.update(
                gate="PASS" if safe else "BLOCKED",
                disposition="CREDENTIAL_ENV_TRANSPORT" if safe else "SECRET_ARGV_REJECTED",
                evidence="REDACTED" if safe else "INVALID",
                retry={"domain": "TECHNICAL_EXECUTION_RETRY", "delta": 0},
            )
        elif kind in {"SOURCE_ONLY_VERIFICATION", "AGENT_SELF_REPORT_ONLY"}:
            state.update(gate="HOLD", evidence="INVALID", disposition="VERIFICATION_REJECTED")
        elif kind == "MANDATORY_E2E_STATIC_SUBSTITUTION":
            state.update(gate="HOLD", evidence="INVALID", disposition="VERIFICATION_DEPTH_NOT_MET",
                         verification_depth="E2E")
        elif kind == "VERIFICATION_EVIDENCE_BOUND":
            matching = all(bool(event.get(key)) for key in (
                "candidate_match", "contract_match", "environment_match", "artifact_present"
            ))
            state.update(gate="PASS" if matching else "HOLD",
                         evidence="BOUND" if matching else "INVALID",
                         disposition="VERIFIED" if matching else "VERIFICATION_BINDING_STALE")
        else:
            raise EvalSchemaError("EVAL_EVENT_UNSUPPORTED:" + kind)
    return state


def _matches(actual: Any, expected: Any) -> bool:
    if isinstance(expected, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _matches(actual[key], value) for key, value in expected.items()
        )
    return actual == expected


def _grade(name: str, actual: Mapping[str, Any], case: Mapping[str, Any]) -> list[str]:
    expected = dict(case.get("expected") or {})
    fields = {
        "STATE_GRADER": ("state",),
        "GATE_GRADER": ("gate", "disposition", "dispatch_count", "notification_count"),
        "EVIDENCE_GRADER": ("evidence",), "OWNERSHIP_GRADER": ("ownership",),
        "RETRY_GRADER": ("retry",), "SIDE_EFFECT_GRADER": ("side_effect",),
        "PRODUCT_READINESS_GRADER": ("product_readiness",),
        "VERIFICATION_GRADER": ("verification_depth",),
    }
    failures = []
    if name == "TRACE_GRADER":
        trace = list(actual.get("trace") or [])
        for assertion in case.get("trace_assertions") or []:
            if assertion.get("contains") not in trace:
                failures.append("trace_missing:" + str(assertion.get("contains")))
            if "count" in assertion and trace.count(assertion.get("contains")) != int(assertion["count"]):
                failures.append("trace_count:" + str(assertion.get("contains")))
        return failures
    for field in fields.get(name, ()):
        if field in expected and not _matches(actual.get(field), expected[field]):
            failures.append(field)
    if name in {"STATE_GRADER", "GATE_GRADER", "OWNERSHIP_GRADER", "RETRY_GRADER"}:
        for key, value in dict(expected.get("flags") or {}).items():
            if not _matches(dict(actual.get("flags") or {}).get(key), value):
                failures.append("flags." + key)
    return failures


def _forbidden(actual: Mapping[str, Any], case: Mapping[str, Any]) -> list[str]:
    forbidden = dict(case.get("forbidden") or {})
    violations = []
    for field in ("state", "gate", "disposition"):
        values = forbidden.get(field) or []
        if actual.get(field) in values:
            violations.append(field + ":" + str(actual.get(field)))
    flags = dict(actual.get("flags") or {})
    checks = {
        "product_retry_leakage": int(dict(actual.get("retry") or {}).get("delta", 0)) > 0,
        "writer_overlap": int(dict(actual.get("ownership") or {}).get("writer_count", 0)) > 1,
        "source_mutation": actual.get("side_effect") == "PRODUCT_MUTATION",
        "duplicate_dispatch": int(actual.get("dispatch_count", 0)) > 1,
        "fake_success": actual.get("state") == "SUCCEEDED" and actual.get("evidence") != "BOUND",
        "false_complete": actual.get("product_readiness") == "COMPLETE" and actual.get("evidence") != "BOUND",
        "stale_qa_acceptance": flags.get("stale_qa_accepted") is True,
        "secret_in_process_argv": flags.get("secret_in_process_argv") is True,
        "secret_in_logs": flags.get("secret_in_logs") is True,
        "secret_in_evidence": flags.get("secret_in_evidence") is True,
        "secret_in_sqlite": flags.get("secret_in_sqlite") is True,
        "secret_in_incident_bundle": flags.get("secret_in_incident_bundle") is True,
    }
    for name, occurred in checks.items():
        if forbidden.get(name) is True and occurred:
            violations.append(name)
    return violations


def validate_case(case: Mapping[str, Any]) -> None:
    required = {"eval_id", "category", "initial_state", "events", "expected", "forbidden", "trace_assertions"}
    if not required <= set(case) or not case.get("eval_id") or not case.get("events"):
        raise EvalSchemaError("EVAL_CASE_SCHEMA_INVALID")


def environment_manifest(*, harness_version: str, control_schema_revision: int,
                         runtime_contract: Mapping[str, Any], source_view: Mapping[str, Any],
                         execution_budget: Mapping[str, Any]) -> dict[str, Any]:
    manifest = {
        "harness_version": str(harness_version),
        "control_schema_revision": int(control_schema_revision),
        "runtime_adapter_revision": ADAPTER_API_REVISION,
        "runtime": str(runtime_contract.get("runtime") or "fixture"),
        "runtime_version": str(runtime_contract.get("runtime_version") or "fixture"),
        "protocol_capability_snapshot": copy.deepcopy(dict(runtime_contract)),
        "model_tier": str(runtime_contract.get("model_tier") or "NOT_CAPTURED"),
        "permission_tool_profile": str(runtime_contract.get("permission_tool_profile") or "fixture"),
        "source_view_base_identity": copy.deepcopy(dict(source_view)),
        "eval_corpus_revision": CORPUS_REVISION,
        "grader_revision": GRADER_REVISION,
        "execution_budget": copy.deepcopy(dict(execution_budget)),
    }
    manifest["environment_hash"] = payload_sha256(manifest)
    return manifest


def run_corpus(path: str | Path, *, environment: Mapping[str, Any]) -> dict[str, Any]:
    corpus = json.loads(Path(path).read_text(encoding="utf-8"))
    defaults = dict(corpus.get("case_defaults") or {})
    cases = []
    for raw in corpus.get("cases") or []:
        case = copy.deepcopy(defaults)
        for key, value in dict(raw).items():
            if isinstance(value, Mapping) and isinstance(case.get(key), Mapping):
                case[key] = {**dict(case[key]), **dict(value)}
            else:
                case[key] = copy.deepcopy(value)
        cases.append(case)
    results = []
    counters = {
        "forbidden_outcome_count": 0, "false_success_count": 0,
        "false_complete_count": 0, "runtime_to_product_retry_leak_count": 0,
        "stale_qa_acceptance_count": 0, "unsafe_writer_overlap_count": 0,
        "secret_in_process_argv_count": 0, "secret_persistence_leak_count": 0,
    }
    for case in cases:
        validate_case(case)
        actual = simulate(case)
        grader_failures = {name: _grade(name, actual, case) for name in GRADERS}
        violations = _forbidden(actual, case)
        passed = not any(grader_failures.values()) and not violations
        counters["forbidden_outcome_count"] += len(violations)
        counters["false_success_count"] += int("fake_success" in violations)
        counters["false_complete_count"] += int("false_complete" in violations)
        counters["runtime_to_product_retry_leak_count"] += int("product_retry_leakage" in violations)
        counters["stale_qa_acceptance_count"] += int("stale_qa_acceptance" in violations)
        counters["unsafe_writer_overlap_count"] += int("writer_overlap" in violations)
        counters["secret_in_process_argv_count"] += int("secret_in_process_argv" in violations)
        counters["secret_persistence_leak_count"] += sum(
            int(name in violations) for name in (
                "secret_in_logs", "secret_in_evidence", "secret_in_sqlite",
                "secret_in_incident_bundle",
            )
        )
        results.append({"eval_id": case["eval_id"], "critical": bool(case.get("critical", True)),
                        "passed": passed, "grader_failures": grader_failures,
                        "forbidden_violations": violations, "actual": actual})
    critical_pass = all(item["passed"] for item in results if item["critical"])
    release_gate = critical_pass and not any(counters.values()) and bool(results)
    return {
        "suite": "HARNESS_EVAL_SUITE", "corpus_revision": str(corpus.get("revision") or ""),
        "environment": copy.deepcopy(dict(environment)), "total": len(results),
        "passed": sum(item["passed"] for item in results), "failed": sum(not item["passed"] for item in results),
        "critical_all_pass": critical_pass, **counters,
        "release_gate": "PASS" if release_gate else "FAIL", "results": results,
    }
