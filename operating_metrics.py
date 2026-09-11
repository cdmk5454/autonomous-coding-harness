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


def aggregate_metrics(observations: Sequence[Mapping[str, Any]], *,
                      coverage: Mapping[str, bool] | None = None) -> dict[str, Any]:
    rows = [dict(item) for item in observations]
    covered = dict(coverage or {})
    jobs = [item for item in rows if item.get("kind") == "JOB"]
    recoveries = [item for item in rows if item.get("kind") == "RECOVERY"]
    counters = {}
    for name in (
        "wrong_session_binding_count", "duplicate_command_effect_count",
        "stale_qa_acceptance_count", "unsafe_writer_overlap_count", "unexpected_dirty_count",
        "false_complete_report_count", "redundant_worker_runs_for_unchanged_candidate",
    ):
        counters[name] = sum(int(item.get(name, 0)) for item in rows) if covered.get(name) else UNKNOWN
    token_names = ("input_tokens", "cached_input_tokens", "output_tokens", "context_size")
    token_metrics = {
        name: sum(int(item[name]) for item in rows if isinstance(item.get(name), int))
        if any(isinstance(item.get(name), int) for item in rows) else NOT_CAPTURED
        for name in token_names
    }
    intervention = {domain: sum(
        int(item.get("operator_intervention", 0)) for item in jobs
        if str(item.get("intervention_domain") or "").upper() == domain
    ) for domain in ("QA", "INFRA", "SEMANTIC")}
    if not covered.get("operator_intervention"):
        intervention = {domain: UNKNOWN for domain in intervention}
    by_origin: dict[str, Any] = {}
    for origin in sorted({str(item.get("failure_origin")) for item in recoveries if item.get("failure_origin")}):
        group = [item for item in recoveries if str(item.get("failure_origin")) == origin]
        by_origin[origin] = _ratio(sum(bool(item.get("recovered")) for item in group), len(group), True)
    result = {
        "schema_revision": "operating-metrics/1",
        "strict_success_rate": _ratio(sum(bool(item.get("strict_success")) for item in jobs), len(jobs), bool(jobs)),
        "autonomous_chain_completion_without_harness_patch": _ratio(
            sum(bool(item.get("autonomous_complete")) and not item.get("harness_patch") for item in jobs),
            len(jobs), bool(jobs),
        ),
        "operator_interventions_per_job": {
            key: (round(value / len(jobs), 6) if isinstance(value, int) and jobs else value)
            for key, value in intervention.items()
        },
        **counters,
        "recovery_success_rate_by_failure_origin": by_origin or UNKNOWN,
        "session_resume_success": _ratio(
            sum(bool(item.get("resume_success")) for item in recoveries),
            sum(item.get("resume_attempted") is True for item in recoveries),
            any(item.get("resume_attempted") is True for item in recoveries),
        ),
        "skeleton_ready_jobs": sum(item.get("product_readiness") == "SKELETON_READY" for item in jobs) if jobs else UNKNOWN,
        "open_decisions_per_batch": (
            round(sum(int(item.get("open_decisions", 0)) for item in jobs) / len(jobs), 6) if jobs else UNKNOWN
        ),
        "decision_wait_time": _percentile([
            item["decision_wait_seconds"] for item in jobs if isinstance(item.get("decision_wait_seconds"), (int, float))
        ], .95),
        "finalization_completion_rate": _ratio(
            sum(bool(item.get("finalization_complete")) for item in jobs),
            sum(item.get("finalization_required") is True for item in jobs),
            any(item.get("finalization_required") is True for item in jobs),
        ),
        "job_latency": {"p50": _percentile([item["latency_ms"] for item in jobs if isinstance(item.get("latency_ms"), (int, float))], .50),
                        "p95": _percentile([item["latency_ms"] for item in jobs if isinstance(item.get("latency_ms"), (int, float))], .95)},
        "stage_latency": {"p50": _percentile([item["latency_ms"] for item in rows if item.get("kind") == "STAGE" and isinstance(item.get("latency_ms"), (int, float))], .50),
                          "p95": _percentile([item["latency_ms"] for item in rows if item.get("kind") == "STAGE" and isinstance(item.get("latency_ms"), (int, float))], .95)},
        "token_telemetry": token_metrics,
        "context_lifecycle": {
            "AVAILABLE": sum(int(item.get("context_available", 0)) for item in rows) if covered.get("context_available") else NOT_CAPTURED,
            "SELECTED": sum(int(item.get("context_selected", 0)) for item in rows) if covered.get("context_selected") else NOT_CAPTURED,
            "DELIVERED": sum(int(item.get("context_delivered", 0)) for item in rows) if covered.get("context_delivered") else NOT_CAPTURED,
            "ACCESSED": sum(int(item.get("context_accessed", 0)) for item in rows) if covered.get("context_accessed") else NOT_CAPTURED,
        },
    }
    return result
