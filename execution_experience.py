"""Deterministic execution-experience projection (Step 12).

Projects the EXISTING execution evidence (fixture Control DBs, Job result
payloads, Step10 ReportFacts) into structured ExecutionExperienceRecords with
decision/action/outcome separation, technical-vs-semantic retry classification,
explicit human-intervention observation and Step13 comparison metrics.

This module is observational only:

* it never mutates Control DB / Job state / policy / runtime selection,
* unknown values stay ``UNVERIFIED``/``None`` instead of narrative,
* running it twice on unchanged evidence yields semantically identical output
  (no wall-clock values inside the deterministic payload).

If projection fails, execution evidence remains authoritative; a report/export
failure must never fail a product Job.
"""
from __future__ import annotations

from pathlib import Path
import hashlib
import json
import sqlite3

from runtime_safety import scrub_secrets

SEMANTIC_FAILURE_ORIGINS = {"WORKER_CAUSED", "REVIEWER_CODE_FINDING"}
SEMANTIC_FAILURE_TYPES = {"build", "test", "review"}
INFRASTRUCTURE_FAILURE_TYPES = {"infrastructure", "technical", "control"}
UNVERIFIED = "UNVERIFIED"


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _run_payload(run_dir: Path):
    payload = _load_json(run_dir / "result.json")
    if payload is None:
        payload = _load_json(run_dir / "canary-result.json")
    return payload or {}


def _clean(value):
    """Secret-scrubbed, None-preserving copy of an evidence fragment."""
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, str):
        return scrub_secrets(value)
    return value


def _elapsed(started, finished):
    import datetime
    try:
        a = datetime.datetime.fromisoformat(str(started))
        b = datetime.datetime.fromisoformat(str(finished))
        return round((b - a).total_seconds(), 3)
    except (TypeError, ValueError):
        return None


def _attempt_type(attempt_no: int, worker_cycle) -> str:
    if isinstance(worker_cycle, int) and worker_cycle > 1:
        return "FIX"
    return "INITIAL" if attempt_no <= 1 else UNVERIFIED


def classify_retry(*, retry_domain: str = "", failure_type: str = "",
                   failure_origin: str = "", semantic_retries=0,
                   fix_attempt: bool = False) -> dict:
    """Technical vs semantic classification from recorded evidence only."""
    domain = str(retry_domain or "")
    ftype = str(failure_type or "").lower()
    origin = str(failure_origin or "")
    semantic = bool(
        domain == "PRODUCT_SEMANTIC_RETRY"
        or int(semantic_retries or 0) > 0
        or fix_attempt
        or (ftype in SEMANTIC_FAILURE_TYPES and origin in SEMANTIC_FAILURE_ORIGINS)
    )
    technical = bool(
        domain in {"TECHNICAL_EXECUTION_RETRY", "RUNTIME_RECOVERY"}
        or ftype in INFRASTRUCTURE_FAILURE_TYPES
    )
    if semantic and technical:
        retry_class = "MIXED"
    elif semantic:
        retry_class = "SEMANTIC"
    elif technical:
        retry_class = "TECHNICAL"
    elif domain or ftype:
        retry_class = UNVERIFIED
    else:
        retry_class = "NONE"
    return {"retry_class": retry_class, "semantic_rework": semantic,
            "technical_retry": technical}


def failure_fingerprint(*, stage, failure_code, runtime, provider, model,
                        retry_domain, failure_type, failure_origin,
                        contract_identity) -> dict | None:
    """Repeated-failure-class identity; never merges on message text alone."""
    code = str(failure_code or "").strip()
    if not code:
        return None
    return _clean({
        "stage": str(stage or ""),
        "failure_code": code,
        "failure_type": str(failure_type or ""),
        "failure_origin": str(failure_origin or ""),
        "runtime": str(runtime or ""),
        "provider": str(provider or ""),
        "model": str(model or ""),
        "retry_domain": str(retry_domain or ""),
        "contract_identity": contract_identity or "",
    })


def _human_intervention(job, decisions, waivers_for_job) -> dict:
    """Explicit human involvement only; absence of evidence is UNVERIFIED."""
    interventions = []
    for decision in decisions:
        resolution = str(decision.get("resolution") or "")
        if resolution in {"HUMAN_ACCEPT", "HUMAN_REJECT"}:
            interventions.append({
                "kind": "QA_DECISION", "resolution": resolution,
                "decision_id": str(decision.get("decision_id") or ""),
                "resolved_at": str(decision.get("resolved_at") or ""),
            })
    result = dict(job.get("last_result") or {})
    if result.get("qa_decision_id"):
        interventions.append({
            "kind": "QA_DECISION_RESULT_BOUND",
            "decision_id": str(result.get("qa_decision_id")),
        })
    for action_table, label in (
        ("manual_resolution_actions", "MANUAL_RESOLUTION"),
        ("review_only_actions", "MANUAL_REVIEW_RETRY"),
        ("success_acknowledgements", "SUCCESS_ACKNOWLEDGEMENT"),
    ):
        if job.get(action_table):
            interventions.append({"kind": label, "count": len(job[action_table])})
    for waiver in waivers_for_job:
        interventions.append({"kind": "USER_WAIVER", "item": waiver.get("item", ""),
                              "state": waiver.get("state", "")})
    if interventions:
        return {"observed": True, "events": interventions}
    return {"observed": UNVERIFIED, "events": []}


def record_from_attempt(*, job, execution, attempt, outcome, binding,
                        report_facts, waivers_for_job, source_identity) -> dict:
    """One ExecutionExperienceRecord: identity / policy / decision / action /
    outcome / recovery / classification, projected from recorded evidence."""
    result = dict(job.get("last_result") or {})
    policy = dict(execution.get("effective_policy") or {})
    execution_context = dict(execution.get("execution_context") or {})
    role = str(attempt.get("role") or binding.get("role") or "")
    resolved = dict(policy.get(f"resolved_{role.lower()}" or "resolved_worker") or {})
    reviewer_policy = dict(policy.get("resolved_reviewer") or {})
    outcome_row = dict(outcome or {})
    failure_code = str(outcome_row.get("failure_code") or result.get("failure_code") or "")
    failure_type = str(result.get("failure_type") or "")
    failure_origin = str(result.get("failure_origin") or "")
    retry_domain = str(outcome_row.get("retry_domain") or result.get("retry_domain") or "")
    manifest = dict(binding.get("manifest") or {})
    contract_identity = str(execution.get("execution_surface_hash")
                            or result.get("execution_surface_hash") or "")
    gates = {}
    if report_facts:
        gates = {row.get("gate"): row.get("result")
                 for row in report_facts.get("actual_result") or []}
    started = attempt.get("started_at")
    finished = outcome_row.get("finished_at")
    worker_cycle = None
    implementation = json.loads(str(manifest.get("implementation_context") or "{}") or "{}") \
        if isinstance(manifest.get("implementation_context"), str) \
        else dict(manifest.get("implementation_context") or {})
    worker_cycle = implementation.get("worker_cycle")
    semantic_retries = int(result.get("product_semantic_retry_count") or 0)
    record = {
        "schema_version": 1,
        "kind": "ExecutionExperienceRecord",
        "identity": {
            "job_id": str(job.get("job_id") or ""),
            "client_job_id": str(job.get("client_job_id") or ""),
            "execution_id": str(attempt.get("execution_id") or ""),
            "attempt_id": str(attempt.get("attempt_id") or ""),
            "role": role,
            "candidate_id": str(result.get("candidate_id") or job.get("candidate_id") or ""),
            "batch_id": str(job.get("batch_id") or ""),
            "dataset": str(binding.get("dataset") or ""),
        },
        "policy": {
            "harness_version": source_identity.get("harness_version", ""),
            "source_fingerprint": source_identity.get("source_fingerprint", ""),
            "policy_snapshot_sha256": execution_context.get("policy_snapshot_sha256", ""),
            "profile_id": execution_context.get("profile_id", "") or policy.get("profile_id", ""),
            "effective_policy_sha256": policy.get("effective_policy_sha256", ""),
            "risk_tier": reviewer_policy.get("effective_risk_level", "")
                         or reviewer_policy.get("risk_level", ""),
            "context_tier": reviewer_policy.get("context_tier", ""),
            "contract_revision": execution.get("contract_revision"),
            "write_scope": sorted((dict(execution.get("mutation_scope") or {})
                                   .get("write_scope") or [])) or None,
        },
        "decision": {  # what Harness selected/allowed
            "runtime": str(binding.get("runtime") or "hermes"),
            "provider": str(binding.get("provider") or ""),
            "model": str(binding.get("model") or attempt.get("model") or ""),
            "reasoning": (resolved.get("reasoning_effort") or ""),
            "retry_domain": retry_domain,
            "reviewer_required": bool(reviewer_policy.get("reviewer_required")),
        },
        "action": {  # what actually executed
            "attempt_type": _attempt_type(True, worker_cycle),
            "worker_cycle": worker_cycle,
            "worker_invocations": result.get("worker_invocations"),
            "tester_cycles": result.get("tester_cycle_count"),
            "reviewer_cycles": result.get("reviewer_cycle_count"),
            "technical_retries": int(result.get("technical_execution_retry_count") or 0),
            "semantic_retries": semantic_retries,
            "runtime_recoveries": result.get("runtime_recovery_count"),
            "changed_files": sorted(result.get("changed_files") or []) or None,
        },
        "outcome": {  # what happened
            "attempt_status": str(outcome_row.get("status") or attempt.get("status") or ""),
            "job_status": str(job.get("status") or ""),
            "final_disposition": str(result.get("status") or job.get("status") or ""),
            "elapsed_seconds": _elapsed(started, finished),
            "build": str(result.get("build_status") or gates.get("Build") or "") or None,
            "test": str(result.get("test_status") or gates.get("Test") or "") or None,
            "tester": str(result.get("tester_status") or "") or None,
            "review": str(result.get("review_status") or gates.get("Review") or "") or None,
            "verification": str(result.get("verification_status")
                                or gates.get("Verification") or "") or None,
            "qa_state": (report_facts or {}).get("qa", {}).get("state", "")
                        or ("AWAITING_QA" if job.get("status") == "AWAITING_QA" else ""),
            "worktree_disposition": str(result.get("worktree_disposition") or ""),
            "canonical_promotion": (result.get("canonical_promotion") or {}).get("status", "")
                                   or None,
        },
        "recovery": {
            "failure_code": failure_code,
            "failure_type": failure_type,
            "failure_origin": failure_origin,
            "retry_domain": retry_domain,
            "rollback": bool(result.get("is_rolled_back")),
            "runtime_recovery_count": result.get("runtime_recovery_count"),
            "human_intervention": _human_intervention(job, binding.get("decisions") or [],
                                                      waivers_for_job),
        },
        "quality": {},  # evidence-backed quality fields only; none invented
        "classification": classify_retry(
            retry_domain=retry_domain, failure_type=failure_type,
            failure_origin=failure_origin, semantic_retries=semantic_retries,
            fix_attempt=isinstance(worker_cycle, int) and worker_cycle > 1),
        "failure_fingerprint": failure_fingerprint(
            stage=result.get("failure_stage") or attempt.get("role"),
            failure_code=failure_code, runtime="hermes",
            provider=binding.get("provider", ""), model=binding.get("model", ""),
            retry_domain=retry_domain, failure_type=failure_type,
            failure_origin=failure_origin, contract_identity=contract_identity),
    }
    return _clean(record)


def records_from_run(run_dir: Path, *, waivers=None, source_identity=None) -> list[dict]:
    """Project one preserved native run directory into experience records."""
    run_dir = Path(run_dir)
    waivers = list(waivers or [])
    source_identity = dict(source_identity or {})
    result_payload = _run_payload(run_dir)
    fixture = result_payload.get("fixture")
    if not fixture:
        fixture = str(next(iter([p for p in run_dir.glob("fixture-*")]), ""))
    if not fixture:
        return []
    db = Path(fixture) / ".control" / "control.sqlite3"
    if not db.is_file():
        return []
    connection = sqlite3.connect(f"{db.as_uri()}?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        jobs = {row["job_id"]: json.loads(row["payload"])
                for row in connection.execute("SELECT job_id, payload FROM logical_jobs")}
        executions = {row["execution_id"]: json.loads(row["payload"])
                      for row in connection.execute(
                          "SELECT execution_id, payload FROM executions")}
        decisions = {}
        for row in connection.execute(
                "SELECT job_id, payload FROM decisions WHERE status='RESOLVED'"):
            decisions.setdefault(row["job_id"], []).append(json.loads(row["payload"]))
    finally:
        connection.close()
    bindings = {}
    for path in run_dir.glob("owner-*/*/binding.json"):
        binding = _load_json(path)
        if binding:
            bindings[binding.get("attempt_id")] = {
                **binding, "dataset": run_dir.name,
                "manifest": _load_json(path.parent / "context" / "manifest.json") or {},
            }
    outcomes = {}
    for job_id, job in sorted(jobs.items()):
        attempts = sorted(
            (dict(row) for row in _attempt_rows(run_dir, job_id)),
            key=lambda row: str(row.get("attempt_id")))
        for attempt in attempts:
            attempt_id = str(attempt.get("attempt_id"))
            binding = bindings.get(attempt_id, {})
            binding["decisions"] = decisions.get(job_id, [])
            execution = executions.get(str(attempt.get("execution_id")), {})
            # The Control DB job payload is the authority; the run's captured
            # result.json is only a fallback for pre-terminal snapshots.
            job_result = job if job.get("last_result") else \
                (_job_result_from_payload(result_payload, job_id) or job)
            report_facts = _report_facts(Path(fixture), job_result)
            waivers_for_job = [w for w in waivers if w.get("job_id") in (None, job_id)]
            outcomes[attempt_id] = record_from_attempt(
                job=job_result, execution=execution, attempt=attempt,
                outcome=attempt, binding=binding,
                report_facts=report_facts, waivers_for_job=waivers_for_job,
                source_identity=source_identity)
    return [outcomes[key] for key in sorted(outcomes)]


def _attempt_rows(run_dir: Path, job_id: str):
    """Attempt facts come from the fixture DB (authoritative rows)."""
    fixture = next(iter([p for p in run_dir.glob("fixture-*")]), None)
    if fixture is None:
        return []
    db = fixture / ".control" / "control.sqlite3"
    if not db.is_file():
        return []
    connection = sqlite3.connect(f"{db.as_uri()}?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT a.attempt_id, a.execution_id, a.role, a.model, a.started_at, "
            "o.status, o.failure_code, o.retry_domain, o.finished_at, o.product_retry_delta "
            "FROM attempts a LEFT JOIN attempt_outcomes o USING(attempt_id) "
            "WHERE a.execution_id IN (SELECT execution_id FROM executions WHERE job_id=?) "
            "ORDER BY a.started_at, a.rowid", (job_id,)).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def _job_result_from_payload(result_payload, job_id):
    for job in result_payload.get("job_results") or []:
        if isinstance(job, dict) and job.get("job_id") == job_id:
            return job
    return None


def _report_facts(fixture: Path, job):
    result = dict(job.get("last_result") or {})
    meta = dict(result.get("developer_change_report") or {})
    facts_path = meta.get("facts_path")
    if facts_path and Path(facts_path).is_file():
        return _load_json(Path(facts_path))
    task_id = str(result.get("task_id") or "")
    if task_id:
        candidate = fixture / ".tasks" / "developer-changes" / task_id / "ReportFacts.json"
        if candidate.is_file():
            return _load_json(candidate)
    return None



def step13_metrics(records: list[dict], *, incidents=None,
                   harness_modifications=None, waivers=None) -> dict:
    """Deterministic Step13 comparison metrics from projected records."""
    incidents = list(incidents or [])
    harness_modifications = list(harness_modifications or [])
    waivers = list(waivers or [])
    dispositions: dict[str, int] = {}
    elapsed_by_role: dict[str, float] = {}
    interventions = []
    semantic_rework_records = 0
    fix_attempts = 0
    reviewer_cycles = 0
    tester_cycles = 0
    fingerprints: dict[str, int] = {}
    for record in records:
        outcome = record["outcome"]
        dispositions[outcome["final_disposition"]] = \
            dispositions.get(outcome["final_disposition"], 0) + 1
        role = record["identity"]["role"]
        elapsed = outcome.get("elapsed_seconds")
        if isinstance(elapsed, (int, float)):
            elapsed_by_role[role] = round(
                elapsed_by_role.get(role, 0) + elapsed, 3)
        human = record["recovery"]["human_intervention"]
        if human.get("observed") is True:
            interventions.extend(human["events"])
        if record["classification"]["semantic_rework"]:
            semantic_rework_records += 1
        if record["action"]["attempt_type"] == "FIX":
            fix_attempts += 1
        reviewer_value = record["action"].get("reviewer_cycles")
        if isinstance(reviewer_value, int):
            reviewer_cycles += reviewer_value
        tester_value = record["action"].get("tester_cycles")
        if isinstance(tester_value, int):
            tester_cycles += tester_value
        fingerprint = record.get("failure_fingerprint")
        if fingerprint:
            key = json.dumps(fingerprint, sort_keys=True, ensure_ascii=False)
            fingerprints[key] = fingerprints.get(key, 0) + 1
    unique_interventions = []
    seen = set()
    for event in interventions:
        key = json.dumps(event, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            unique_interventions.append(event)
    return _clean({
        "completion": {
            "dispositions": dispositions,
            "record_count": len(records),
            "false_success_incidents": [
                i for i in incidents if i.get("kind") == "FALSE_SUCCESS"],
        },
        "time": {"elapsed_by_role_seconds": elapsed_by_role},
        "human_cost": {
            "intervention_count": len(unique_interventions),
            "interventions": unique_interventions,
        },
        "rework": {
            "semantic_rework_records": semantic_rework_records,
            "fix_attempts": fix_attempts,
            "reviewer_cycles_total": reviewer_cycles,
            "tester_cycles_total": tester_cycles,
        },
        "harness_cost": {
            # §22: only canary-defect-driven Harness corrections count.
            "self_modifications": harness_modifications,
            "self_modification_count": len(harness_modifications),
            "infrastructure_recoveries": sum(
                1 for r in records
                if (r["action"].get("runtime_recoveries") or 0)
                and not r["classification"]["semantic_rework"]),
            "waivers": waivers,
        },
        "safety": {
            "duplicate_application": [i for i in incidents
                                      if i.get("kind") == "DUPLICATE_APPLICATION"],
            "lost_change": [i for i in incidents if i.get("kind") == "LOST_CHANGE"],
            "rollback_records": [r["identity"]["attempt_id"] for r in records
                                 if r["recovery"]["rollback"]],
            "stale_foreign_rejection": [i for i in incidents
                                        if i.get("kind") == "STALE_FOREIGN_EVIDENCE_REJECTED"],
        },
        "failure_fingerprint_counts": fingerprints,
    })


def source_identity(harness_root: Path, checkpoint_path: Path | None = None) -> dict:
    """Version + local source fingerprint (never only the global VERSION)."""
    root = Path(harness_root)
    version = ""
    try:
        version = (root / "VERSION").read_text(encoding="utf-8").strip()
    except OSError:
        pass
    fingerprint = ""
    if checkpoint_path and Path(checkpoint_path).is_file():
        payload = _load_json(Path(checkpoint_path)) or {}
        digest = hashlib.sha256()
        for name in sorted(payload_all_files(payload)):
            digest.update(name.encode("utf-8"))
            digest.update(str(payload_all_files(payload).get(name, "")).encode("utf-8"))
        fingerprint = digest.hexdigest()
    return {"harness_version": version, "source_fingerprint": fingerprint}


def payload_all_files(payload):
    files = dict(payload.get("files", {}))
    for key in ("new_files_vs_step9a", "new_files_vs_step10"):
        files.update(payload.get(key) or {})
    return files


def export(run_dirs, *, waivers=None, incidents=None, harness_modifications=None,
           harness_root=r"D:\kkm_test", checkpoint_path=None,
           experiments=None) -> dict:
    """Deterministic execution-experience export (no wall-clock inside)."""
    identity = source_identity(Path(harness_root), checkpoint_path)
    records = []
    for run_dir in sorted(map(Path, run_dirs)):
        records.extend(records_from_run(run_dir, waivers=waivers,
                                        source_identity=identity))
    return _clean({
        "schema_version": 1,
        "kind": "ExecutionExperienceExport",
        "purpose": "OBSERVATION_ONLY_NO_AUTOMATIC_POLICY_CHANGE",
        "source_identity": identity,
        "experiments": experiments or {},
        "records": records,
        "step13_metrics": step13_metrics(records, incidents=incidents,
                                         harness_modifications=harness_modifications,
                                         waivers=waivers),
        "unknown_policy": "null and UNVERIFIED are not PASS or zero",
    })
