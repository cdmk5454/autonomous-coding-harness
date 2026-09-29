"""Deterministic 0.99 operating-metric aggregation with honest missing values."""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence


UNKNOWN = "UNKNOWN"
NOT_CAPTURED = "NOT_CAPTURED"


def _ratio(numerator: int, denominator: int, observed: bool) -> float | str:
    return round(numerator / denominator, 6) if observed and denominator else UNKNOWN


def _percentile(values: Sequence[float], percentile: float) -> float | str:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return NOT_CAPTURED
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _number(value):
    return type(value) in {int, float} and math.isfinite(value) and value >= 0


def _count(value):
    return type(value) is int and value >= 0


def _boolean_rate(rows, field):
    return (_ratio(sum(row[field] is True for row in rows), len(rows), True)
            if rows and all(type(row.get(field)) is bool for row in rows) else UNKNOWN)


def aggregate_metrics(observations: Sequence[Mapping[str, Any]], *,
                      coverage: Mapping[str, bool] | None = None) -> dict[str, Any]:
    """Aggregate explicitly supplied cohorts; unknown outcomes are not failures.

    Sparse additive counters may default to zero ONLY when the caller explicitly
    attests complete collection through coverage=True. Token/latency samples expose
    their observed count; a partial sample is never labelled a complete Job cohort.
    """
    rows = [dict(item) for item in observations]
    covered = dict(coverage or {})
    jobs = [item for item in rows if item.get("kind") == "JOB"]
    recoveries = [item for item in rows if item.get("kind") == "RECOVERY"]
    def counter(name, missing=UNKNOWN):
        values = [row.get(name, 0) for row in rows]
        return sum(values) if covered.get(name) is True and all(_count(v) for v in values) else missing
    counters = {name: counter(name) for name in (
        "wrong_session_binding_count", "duplicate_command_effect_count",
        "stale_qa_acceptance_count", "unsafe_writer_overlap_count", "unexpected_dirty_count",
        "false_complete_report_count", "redundant_worker_runs_for_unchanged_candidate",
    )}
    token_names = ("input_tokens", "cached_input_tokens", "output_tokens", "context_size")
    samples = {name: [row[name] for row in rows if _count(row.get(name))] for name in token_names}
    token_metrics = {name: sum(values) if values else NOT_CAPTURED for name, values in samples.items()}
    intervention = {}
    for domain in ("QA", "INFRA", "SEMANTIC"):
        values = [row.get("operator_intervention", 0) for row in jobs
                  if str(row.get("intervention_domain") or "").upper() == domain]
        intervention[domain] = (round(sum(values) / len(jobs), 6)
            if covered.get("operator_intervention") is True and jobs and all(_count(v) for v in values) else UNKNOWN)
    by_origin = {}
    for origin in sorted({str(row["failure_origin"]) for row in recoveries if row.get("failure_origin")}):
        by_origin[origin] = _boolean_rate([row for row in recoveries if str(row.get("failure_origin")) == origin], "recovered")
    resumed = [row for row in recoveries if row.get("resume_attempted") is True]
    finalizing = [row for row in jobs if row.get("finalization_required") is True]
    complete_batches = bool(jobs) and all(isinstance(row.get("batch_id"), str) and row["batch_id"]
                                             and _count(row.get("open_decisions")) for row in jobs)
    open_per_batch = (round(sum(row["open_decisions"] for row in jobs) / len({row["batch_id"] for row in jobs}), 6)
                      if complete_batches else UNKNOWN)
    def distribution(group, name):
        values = [row[name] for row in group if _number(row.get(name))]
        return {"p50": _percentile(values, .50), "p95": _percentile(values, .95),
                "observed": len(values), "cohort_size": len(group),
                "coverage": "COMPLETE" if group and len(values) == len(group) else "INCOMPLETE_OR_ABSENT"}
    autonomous_known = bool(jobs) and all(type(row.get("autonomous_complete")) is bool
                         and type(row.get("harness_patch")) is bool for row in jobs)
    result = {
        "schema_revision": "operating-metrics/1",
        "strict_success_rate": _boolean_rate(jobs, "strict_success"),
        "autonomous_chain_completion_without_harness_patch": (_ratio(
            sum(row["autonomous_complete"] and not row["harness_patch"] for row in jobs), len(jobs), True)
            if autonomous_known else UNKNOWN),
        "operator_interventions_per_job": intervention,
        **counters,
        "recovery_success_rate_by_failure_origin": by_origin or UNKNOWN,
        "session_resume_success": _boolean_rate(resumed, "resume_success"),
        "skeleton_ready_jobs": (sum(row["product_readiness"] == "SKELETON_READY" for row in jobs)
            if jobs and all(isinstance(row.get("product_readiness"), str) and row["product_readiness"] for row in jobs) else UNKNOWN),
        "open_decisions_per_batch": open_per_batch,
        "decision_wait_time": _percentile([row["decision_wait_seconds"] for row in jobs
                                            if _number(row.get("decision_wait_seconds"))], .95),
        "finalization_completion_rate": _boolean_rate(finalizing, "finalization_complete"),
        "job_latency": distribution(jobs, "latency_ms"),
        "stage_latency": distribution([row for row in rows if row.get("kind") == "STAGE"], "latency_ms"),
        "token_telemetry": token_metrics,
        "token_observed_samples": {name: len(values) for name, values in samples.items()},
        "context_lifecycle": {label: counter(name, NOT_CAPTURED) for label, name in (
            ("AVAILABLE", "context_available"), ("SELECTED", "context_selected"),
            ("DELIVERED", "context_delivered"), ("ACCESSED", "context_accessed"))},
    }
    result["operational_compression"] = compression_metrics(rows, coverage=covered)
    return result



def compression_metrics(observations, *, coverage=None):
    """Explicit sampled-cohort metrics. Missing/partial cost is never zero.

    `accepted` is an authoritative Product acceptance observation, NOT a
    Worker completion/model claim. Callers must supply the whole chosen cohort.
    No time-to-money conversion or automatic ranking is performed.
    """
    covered = dict(coverage or {})
    jobs = [dict(row) for row in observations if row.get("kind") == "JOB"]
    identities = [row.get("job_id") for row in jobs]
    if any(not isinstance(x, str) or not x for x in identities) or len(set(identities)) != len(identities):
        # Legacy unnamed observations do not become an invented accepted cohort.
        jobs = []
    def number(value):
        return type(value) in {int, float} and math.isfinite(value) and value >= 0
    def complete_values(name):
        values = [row.get(name) for row in jobs]
        return values if jobs and all(number(x) and (not name.endswith("_count") or type(x) is int) for x in values) else None
    def total(name):
        values = complete_values(name)
        return sum(values) if values is not None else NOT_CAPTURED
    def distribution(name):
        values = complete_values(name)
        return {"p50": _percentile(values or [], .50), "p95": _percentile(values or [], .95),
                "coverage": "COMPLETE" if values is not None else "INCOMPLETE_OR_ABSENT"}
    known_outcomes = bool(jobs) and all(type(row.get("accepted")) is bool for row in jobs)
    accepted = sum(row.get("accepted") is True for row in jobs)
    currencies = {row.get("currency") if isinstance(row.get("currency"), str) else None for row in jobs}
    cost = total("cost")
    cost_per_accepted = (cost / accepted if known_outcomes and accepted and number(cost)
                        and len(currencies) == 1 and next(iter(currencies)) in {"USD", "KRW", "EUR"}
                        else NOT_CAPTURED)
    result = {"schema_revision": "operational-compression-metrics/1", "cohort_size": len(jobs),
        "accepted_job_completion_rate": accepted / len(jobs) if known_outcomes else UNKNOWN,
        "accepted_jobs": accepted if known_outcomes else UNKNOWN,
        "end_to_end_elapsed": distribution("end_to_end_elapsed"),
        "human_intervention_seconds": total("human_intervention_seconds"),
        "cost_per_accepted_job": cost_per_accepted,
        "currency": next(iter(currencies)) if len(currencies) == 1 and None not in currencies else UNKNOWN,
        "harness_caused_recovery_count": total("harness_caused_recovery_count"),
        "redundant_stage_rerun_count": total("redundant_stage_rerun_count"),
        "runtime_startup_elapsed": distribution("runtime_startup_elapsed"),
        "runtime_handoff_elapsed": distribution("runtime_handoff_elapsed")}
    for name in ("false_success_count", "lost_work_count", "unsafe_canonical_overwrite_count",
                 "duplicate_writer_incident_count", "unrecoverable_orchestration_corruption_count"):
        result[name] = total(name) if covered.get(name) is True else UNKNOWN
    return result


PAIR_KEYS = ("job_contract_sha256", "acceptance_sha256", "baseline_sha256", "model",
             "build_test_policy_sha256", "review_policy_sha256", "verification_sha256")

def paired_comparison(left, right):
    """Compare matched observations, never launch a runtime or select a winner."""
    for key in PAIR_KEYS:
        if not isinstance(left.get(key), str) or not left[key] or left[key] != right.get(key):
            raise ValueError("COMPARISON_INPUT_MISMATCH:" + key)
    if {left.get("path"), right.get("path")} != {"DIRECT", "HERMES_MANAGED"}:
        raise ValueError("COMPARISON_PATH_INVALID")
    measures = ("runtime_startup_elapsed", "runtime_handoff_elapsed", "end_to_end_elapsed",
                "technical_recovery_count", "human_intervention_seconds", "cost")
    for observation in (left, right):
        if type(observation.get("accepted")) is not bool:
            raise ValueError("COMPARISON_OUTCOME_MISSING")
        for name in measures:
            value = observation.get(name)
            if (type(value) not in {int, float} or not math.isfinite(value) or value < 0
                    or (name.endswith("_count") and type(value) is not int)):
                raise ValueError("COMPARISON_METRIC_MISSING:" + name)
    if not left.get("currency") or left["currency"] != right.get("currency"):
        raise ValueError("COMPARISON_CURRENCY_MISMATCH")
    return {"schema_revision": "paired-runtime-comparison/1", "matched": True,
            "cohort": "ONE_MATCHED_PAIR_NOT_GENERAL_BENCHMARK", "held_constant": {k:left[k] for k in PAIR_KEYS},
            "observations": [dict(left), dict(right)], "automatic_promotion": False}
