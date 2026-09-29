"""Deterministic goal-convergence signals from durable job evidence (0.99.2).

0.99.1 distinguished activity from meaningful progress.  These signals take
the next observational step — meaningful progress != goal convergence —
while staying strictly non-blocking: they are recorded evidence for review
attention and telemetry, never a success/fail gate, and they never influence
destructive capability.  There is no LLM observer; every signal derives from
durable queue/job records only.

Signal semantics (typed, not one opaque score):
- CONVERGENCE_REWORK_HIGH: the semantic family needed >= 2 reworks.
- CONVERGENCE_FINDING_RECURS: the same review violation code reappeared in
  >= 2 family results.
- CONVERGENCE_SOURCE_CHURN: the same file was changed in >= 3 family
  results (root + 2 reworks still touching it).
- CONVERGENCE_VALIDATION_LOOP: the same candidate hash was validated in
  >= 2 family results without progress.
- CONVERGENCE_DIFF_EXPANSION_NO_PROOF: the last >= 3 family results grew
  the diff monotonically while none of them succeeded.

Normal multi-step progress must not raise drift signals: a root job plus
one rework touching the same file is expected remediation, not churn.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

CONVERGENCE_REWORK_HIGH = "CONVERGENCE_REWORK_HIGH"
CONVERGENCE_FINDING_RECURS = "CONVERGENCE_FINDING_RECURS"
CONVERGENCE_SOURCE_CHURN = "CONVERGENCE_SOURCE_CHURN"
CONVERGENCE_VALIDATION_LOOP = "CONVERGENCE_VALIDATION_LOOP"
CONVERGENCE_DIFF_EXPANSION_NO_PROOF = "CONVERGENCE_DIFF_EXPANSION_NO_PROOF"

REWORK_HIGH_THRESHOLD = 2
FINDING_RECURRENCE_THRESHOLD = 2
SOURCE_CHURN_REPEAT_THRESHOLD = 3
VALIDATION_LOOP_REPEAT_THRESHOLD = 2
DIFF_EXPANSION_WINDOW = 3


def _result(job: Mapping[str, Any]) -> dict[str, Any]:
    result = job.get("last_result")
    return dict(result) if isinstance(result, Mapping) else {}


def _ordinal(job: Mapping[str, Any]) -> int:
    try:
        return int(job.get("semantic_rework_ordinal", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _sequence(job: Mapping[str, Any]) -> int:
    try:
        return int(job.get("sequence", 0) or 0)
    except (TypeError, ValueError):
        return 0


def family_records(jobs: Iterable[Mapping[str, Any]], root_job_id: str) -> list[Mapping[str, Any]]:
    """Return the semantic family (root included) in durable order."""
    root = str(root_job_id or "")
    records = [
        job for job in jobs
        if str(job.get("job_id", "")) == root
        or str(job.get("semantic_root_job_id", "")) == root
    ]
    records.sort(key=lambda job: (_ordinal(job), _sequence(job), str(job.get("job_id", ""))))
    return records


def _violations(result: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for item in list(result.get("review_violations") or []):
        code = str(item or "").strip()
        if code and code not in codes:
            codes.append(code)
    return codes


def signals_for_family(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Compute typed convergence signals for one semantic family."""
    jobs = [dict(job) for job in records]
    if not jobs:
        return []
    signals: list[dict[str, Any]] = []
    reworks = [job for job in jobs if str(job.get("rework_of_job_id", "") or "")]
    if len(reworks) >= REWORK_HIGH_THRESHOLD:
        signals.append({
            "signal": CONVERGENCE_REWORK_HIGH,
            "evidence": {
                "semantic_root_job_id": str(
                    jobs[0].get("semantic_root_job_id")
                    or jobs[0].get("job_id", "")
                ),
                "rework_count": len(reworks),
                "rework_job_ids": [str(job.get("job_id", "")) for job in reworks],
            },
        })
    violation_counts: dict[str, list[str]] = {}
    file_counts: dict[str, list[str]] = {}
    candidate_counts: dict[str, list[str]] = {}
    terminal_results: list[dict[str, Any]] = []
    for job in jobs:
        job_id = str(job.get("job_id", ""))
        result = _result(job)
        if result:
            terminal_results.append(result)
        for code in _violations(result):
            violation_counts.setdefault(code, []).append(job_id)
        for path in list(result.get("changed_files") or [])[:200]:
            key = str(path)
            if key not in file_counts:
                file_counts[key] = []
            if job_id not in file_counts[key]:
                file_counts[key].append(job_id)
        candidate = str(result.get("candidate_id", "") or "")
        if candidate:
            if candidate not in candidate_counts:
                candidate_counts[candidate] = []
            if job_id not in candidate_counts[candidate]:
                candidate_counts[candidate].append(job_id)
    recurring = sorted(
        (code, job_ids) for code, job_ids in violation_counts.items()
        if len(job_ids) >= FINDING_RECURRENCE_THRESHOLD
    )
    if recurring:
        signals.append({
            "signal": CONVERGENCE_FINDING_RECURS,
            "evidence": {
                "recurring_violation_codes": [code for code, _ in recurring],
                "occurrences": {
                    code: job_ids for code, job_ids in recurring
                },
            },
        })
    churned = sorted(
        (path, job_ids) for path, job_ids in file_counts.items()
        if len(job_ids) >= SOURCE_CHURN_REPEAT_THRESHOLD
    )
    if churned:
        signals.append({
            "signal": CONVERGENCE_SOURCE_CHURN,
            "evidence": {
                "churned_files": [path for path, _ in churned],
                "occurrences": {path: job_ids for path, job_ids in churned},
            },
        })
    looped = sorted(
        (candidate, job_ids) for candidate, job_ids in candidate_counts.items()
        if len(job_ids) >= VALIDATION_LOOP_REPEAT_THRESHOLD
    )
    if looped:
        signals.append({
            "signal": CONVERGENCE_VALIDATION_LOOP,
            "evidence": {
                "repeated_candidate_ids": [candidate for candidate, _ in looped],
                "occurrences": {
                    candidate: job_ids for candidate, job_ids in looped
                },
            },
        })
    window = terminal_results[-DIFF_EXPANSION_WINDOW:]
    if (
        len(window) == DIFF_EXPANSION_WINDOW
        and all(not bool(item.get("success")) for item in window)
        and all(
            len(list(window[index].get("changed_files") or []))
            < len(list(window[index + 1].get("changed_files") or []))
            for index in range(len(window) - 1)
        )
    ):
        signals.append({
            "signal": CONVERGENCE_DIFF_EXPANSION_NO_PROOF,
            "evidence": {
                "changed_file_counts": [
                    len(list(item.get("changed_files") or [])) for item in window
                ],
            },
        })
    return signals
