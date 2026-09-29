"""Harness-owned current-input gate successors; prior Product proofs stay immutable.

This module never dispatches a Worker or changes canonical Product. Publication
continues through IsolatedCanonicalPromotion and the existing decision ledger.
"""
from __future__ import annotations

import copy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import shutil
import uuid

from control_repository import RepositoryError, canonical, digest, now
from hermes_boundary import atomic, read, write_once
from qa_quarantine import CandidateError, paths_overlap

KINDS = {"CANONICAL_REVALIDATION", "BATCH_VERIFICATION"}
CONFIRMATION = "I_CONFIRM_CURRENT_INPUT_REVALIDATION"


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _key(request_id):
    from operator_actions import receipt_key
    return "canonical-revalidation/1/" + receipt_key(request_id).rsplit("/", 1)[-1]


def _receipt(repo, key):
    row = repo._connection.execute("SELECT payload FROM control_state WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def _write_receipt(repo, key, value):
    prior = _receipt(repo, key)
    if prior is None:
        repo._connection.execute("INSERT INTO control_state VALUES(?,0,?)", (key, canonical(value)))
    else:
        if prior["binding"] != value["binding"]:
            raise RepositoryError("REVALIDATION_BINDING_IMMUTABLE")
        repo._connection.execute("UPDATE control_state SET revision=revision+1,payload=? WHERE key=?",
                                 (canonical(value), key))


def assert_quiescent(repo, job_ids):
    """No gate successor can conceal a live/uncertain predecessor runtime."""
    for job_id in job_ids:
        row = repo._connection.execute(
            "SELECT 1 FROM attempts a JOIN executions e USING(execution_id) "
            "JOIN attempt_outcomes o USING(attempt_id) WHERE e.job_id=? "
            "AND o.status IN ('RUNNING','UNKNOWN') UNION ALL "
            "SELECT 1 FROM runtime_processes p JOIN attempts a USING(attempt_id) "
            "JOIN executions e USING(execution_id) WHERE e.job_id=? "
            "AND p.state IN ('STARTING','RUNNING','UNKNOWN','LOST') LIMIT 1",
            (job_id, job_id)).fetchone()
        if row or repo.active_execution(job_id):
            raise RepositoryError("REVALIDATION_PREDECESSOR_NOT_QUIESCED")


def source_job_binding(repo, job_id):
    job = repo.read_job(job_id)
    return dict(job_id=job_id, revision=job["revision"],
                contract_revision=job.get("contract_revision", 1),
                request_sha256=digest(job["request"]), status=job["status"],
                candidate_id=job.get("candidate_id", ""),
                publication_id=(job.get("last_result", {}).get("canonical_promotion") or {}).get("publication_id", ""))


def validate_source_jobs(repo, binding):
    for frozen in binding["source_jobs"]:
        current = source_job_binding(repo, frozen["job_id"])
        # A revalidated Job projection may advance only through this receipt.
        job = repo.read_job(frozen["job_id"])
        own = (binding["kind"] == "CANONICAL_REVALIDATION"
               and frozen["job_id"] == binding["job_id"]
               and job.get("canonical_revalidation", {}).get("binding_sha256") == digest(binding))
        keys = ("contract_revision", "request_sha256") if own else tuple(frozen)
        if any(current.get(k) != frozen.get(k) for k in keys):
            raise RepositoryError("REVALIDATION_SOURCE_JOB_CHANGED")


def admit_gate_execution(repo, task, *, binding, expected_revision):
    """Add a gate-only Execution and durable operation receipt, never revive WORK.

The caller first prepares an owned isolated candidate and immutable admission
file. Binding contains that file's hash, full input identity and approved Jobs.
"""
    if binding.get("kind") not in KINDS or binding.get("job_id") != task.job_id:
        raise RepositoryError("REVALIDATION_KIND_INVALID")
    if (binding.get("candidate_id") != task.candidate_id
            or Path(binding.get("workspace", "")).resolve() != Path(task.working_dir).resolve()
            or _sha(binding["admission_path"]) != binding["admission_sha256"]):
        raise RepositoryError("REVALIDATION_ADMISSION_INVALID")
    proof = read(binding["admission_path"])
    if (proof.get("input_content") != binding.get("input_content")
            or proof.get("candidate_id") != task.candidate_id
            or proof.get("job_id") != task.job_id):
        raise RepositoryError("REVALIDATION_ADMISSION_INVALID")
    with repo.hold():
        if repo.global_stop().get("active"):
            raise RepositoryError("GLOBAL_STOP")
        if repo._connection.execute('PRAGMA user_version').fetchone()[0] not in {5,6}:
            raise RepositoryError('GATE_REVALIDATION_SCHEMA_UNSUPPORTED')
        key = _key(binding["request_id"])
        prior = _receipt(repo, key)
        if prior:
            if prior["binding"] != binding:
                raise RepositoryError("REVALIDATION_IDEMPOTENCY_CONFLICT")
            raise RepositoryError("REVALIDATION_ALREADY_ADMITTED")
        job = repo.read_job(task.job_id)
        if job["revision"] != expected_revision:
            raise RepositoryError("JOB_REVISION_CONFLICT")
        expected_status = "AWAITING_QA" if binding["kind"] == "CANONICAL_REVALIDATION" else "SUCCEEDED"
        if job["status"] != expected_status:
            raise RepositoryError("REVALIDATION_JOB_STATE_INVALID")
        validate_source_jobs(repo, binding)
        assert_quiescent(repo, [v["job_id"] for v in binding["source_jobs"]])
        old = repo.execution(binding["predecessor_execution_id"])
        if not old or old["job_id"] != task.job_id:
            raise RepositoryError("REVALIDATION_PREDECESSOR_MISMATCH")
        frozen = copy.deepcopy(task.materialized_execution)
        eid = "EXEC-REVALIDATE-" + uuid.uuid4().hex.upper()
        mid = "MAT-REVALIDATE-" + uuid.uuid4().hex.upper()
        frozen.update(execution_id=eid, materialization_id=mid, role=binding["kind"],
                      contract_revision=job.get("contract_revision", 1), request=copy.deepcopy(job["request"]),
                      materialized_at=now(), baseline_hash=digest(binding["canonical_base"]),
                      execution_surface_hash=digest({"prior": old["execution_surface_hash"],
                                                     "input": binding["input_content"]}),
                      current_requirement=task.requirement, test_plan=copy.deepcopy(task.test_plan),
                      revalidation_binding=copy.deepcopy(binding),
                      handoff_source_execution_id=old["execution_id"])
        frozen["handoff_source_view_id"] = old.get("source_view_id", "")
        frozen["source_view_id"] = "SOURCE-REVALIDATE-" + digest(binding)
        import bounded_execution
        slot_owner = "REVALIDATION:" + eid
        if bounded_execution.enabled(repo):
            if bounded_execution.slots(repo):
                raise RepositoryError("REVALIDATION_EXECUTION_CAPACITY_BUSY")
            if bounded_execution.claim_slot(repo, task.job_id, slot_owner) is None:
                raise RepositoryError("EXECUTION_CAPACITY_EXHAUSTED")
            bounded_execution.bind_candidate(repo, task.job_id, slot_owner, task.working_dir)
            _refresh_slots(repo)
        frozen["revalidation_slot_owner"] = slot_owner
        repo._connection.execute("INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
            (eid, task.job_id, frozen["logical_job_id"], frozen["contract_revision"], "MATERIALIZED",
             frozen["profile_revision"], frozen["policy_revision"], frozen["effective_policy_hash"],
             frozen["baseline_hash"], frozen["execution_surface_hash"], frozen["materialized_at"], canonical(frozen)))
        repo._connection.execute("INSERT INTO materializations VALUES(?,?,?,?)",
                                 (mid, eid, frozen["materialized_at"], canonical(dict(materialization_id=mid,
                                  execution_id=eid, revalidation_binding_sha256=digest(binding)))))
        authority = dict(execution_id=eid, state="RELEASED", revision=0,
                         reason="GATE_ONLY_NO_WORKER_AUTHORITY", updated_at=now())
        repo._connection.execute("INSERT INTO execution_authority VALUES(?,?,?,?,?,?)",
                                 (eid, "RELEASED", 0, authority["reason"], authority["updated_at"], canonical(authority)))
        _write_receipt(repo, key, dict(binding=copy.deepcopy(binding), execution_id=eid,
                                     task_id=task.task_id, state="GATING", started_at=now()))
    task.materialized_execution = frozen
    task.execution_id, task.materialization_id = eid, mid
    task.execution_epoch = str(frozen.get("control_epoch", ""))
    task.test_plan_amendment = {}
    task._record_execution_attempt = lambda role, aid, model: repo.start_attempt(
        frozen, attempt_id=aid, role=role, model=model,
        candidate_hash=__import__("gate_evidence").identity(task, task.working_dir)["candidate_hash"])
    return frozen


def current_contract(repo, task):
    frozen = repo.execution(task.execution_id)
    binding = dict((frozen or {}).get("revalidation_binding") or {})
    if (not binding or frozen != task.materialized_execution or frozen.get("role") not in KINDS
            or binding.get("candidate_id") != task.candidate_id
            or Path(binding["workspace"]).resolve() != Path(task.working_dir).resolve()
            or frozen.get("test_plan") != task.test_plan
            or frozen.get("current_requirement") != task.requirement or task.test_plan_amendment):
        raise RepositoryError("REVALIDATION_CONTRACT_STALE")
    _verify_task_scope(task, binding)
    receipt = _receipt(repo, _key(binding["request_id"]))
    if (not receipt or receipt["binding"] != binding or receipt["execution_id"] != task.execution_id
            or receipt["state"] not in {"GATING", "VERIFIED", "AWAITING_QA", "PUBLISHED"}):
        raise RepositoryError("REVALIDATION_RECEIPT_INVALID")
    if binding.get('predecessor_execution_sha256') and digest(repo.execution(binding['predecessor_execution_id'])) != binding['predecessor_execution_sha256']:
        raise RepositoryError('REVALIDATION_PREDECESSOR_EVIDENCE_CHANGED')
    validate_source_jobs(repo, binding)
    verify_input(task)
    return repo.read_job(task.job_id)


def verify_input(task):
    from isolated_promotion import content, module_heads
    from project_profile import load_project_profile
    binding = task.materialized_execution["revalidation_binding"]
    _verify_task_scope(task, binding)
    profile = load_project_profile(task.profile_dir, task.working_dir)
    preserve = binding["preserve"]
    if (_sha(binding["admission_path"]) != binding["admission_sha256"]
            or content(task.working_dir, profile, preserve) != binding["input_content"]
            or module_heads(task.working_dir, profile) != binding["canonical_heads"]):
        raise RepositoryError("REVALIDATION_INPUT_CHANGED")
    proof = read(binding["admission_path"])
    if proof.get('source_path'):
        if (content(proof['source_path'], profile, preserve) != binding['canonical_base']
                or module_heads(proof['source_path'], profile) != binding['canonical_heads']):
            raise RepositoryError('REVALIDATION_BASE_CHANGED')
    if proof.get('profile_origin'):
        from production_isolation import inventory
        if (inventory(proof['profile_origin'], include_git=True) != proof['profile_origin_inventory']
                or inventory(task.profile_dir, include_git=True) != proof['profile_copy_inventory']):
            raise RepositoryError('REVALIDATION_PROFILE_CHANGED')
    for path, value in proof.get("immutable_predecessor_files", {}).items():
        if _sha(path) != value:
            raise RepositoryError("REVALIDATION_PREDECESSOR_EVIDENCE_CHANGED")
    return binding


def task_scope(task):
    return dict(changed_files=list(task.changed_files), target_resources=list(task.target_resources),
                target_modules=list(task.target_module), git_diff_sha256=digest(task.git_diff))


def _verify_task_scope(task, binding):
    if any(binding.get(k) != v for k,v in task_scope(task).items()):
        raise RepositoryError('REVALIDATION_TASK_SCOPE_CHANGED')


def finish_gate_execution(repo, task, result):
    """Close only the new gate Execution/slot; original outcomes stay untouched."""
    binding = task.materialized_execution["revalidation_binding"]
    with repo.hold():
        active = repo._connection.execute("SELECT 1 FROM attempts a JOIN attempt_outcomes o USING(attempt_id) "
            "WHERE a.execution_id=? AND o.status IN ('RUNNING','UNKNOWN')", (task.execution_id,)).fetchone()
        processes = repo._connection.execute("SELECT 1 FROM runtime_processes p JOIN attempts a USING(attempt_id) "
            "WHERE a.execution_id IN (?,?) AND p.state IN ('STARTING','RUNNING','UNKNOWN','LOST')",
            (task.execution_id, task.reviewer_execution.get('execution_id', ''))).fetchone()
        if active or processes:
            raise RepositoryError("REVALIDATION_ATTEMPT_NOT_QUIESCED")
        row = repo._connection.execute("SELECT phase FROM executions WHERE execution_id=?", (task.execution_id,)).fetchone()
        if row and row[0] != "TERMINAL":
            repo._connection.execute("UPDATE executions SET phase='TERMINAL',result=? WHERE execution_id=?",
                                     (canonical(result), task.execution_id))
        import bounded_execution
        if bounded_execution.enabled(repo):
            verify_input(task)
            repo._connection.execute("DELETE FROM execution_slots WHERE job_id=? AND owner_id=?",
                (task.job_id, task.materialized_execution["revalidation_slot_owner"]))
            _refresh_slots(repo)
        record = _receipt(repo, _key(binding["request_id"]))
        record.update(state="VERIFIED" if result.get("success") is True else "FAILED",
                      result=copy.deepcopy(result), finished_at=now())
        _write_receipt(repo, _key(binding["request_id"]), record)
        repo._connection.execute("UPDATE logical_jobs SET phase='TERMINAL' WHERE job_id=?", (task.job_id,))


def _refresh_slots(repo):
    rows = repo._connection.execute("SELECT job_id FROM execution_slots ORDER BY slot").fetchall()
    queue = repo.read_queue()
    previous = int(queue.get("revision", 0))
    queue.update(running_job_id=rows[0][0] if rows else "", active_execution_count=len(rows),
                 revision=previous + 1, updated_at=now())
    repo.write_queue(queue, expected_revision=previous)


def run_fresh_gates(service, task, *, baseline):
    """Fresh Build -> TEST Attempt -> read-only Reviewer successor -> Verification."""
    if not service._lease_held:
        raise RepositoryError("HARNESS_LEASE_REQUIRED")
    from gate_evidence import identity, validate
    from runtime_snapshot import load_frozen_runtime_snapshot
    from harness_service import HarnessService
    from control_repository import ControlRepository
    repo = ControlRepository(task.control_repository_path)
    with repo.hold():
        current_contract(repo, task)
    frozen = load_frozen_runtime_snapshot(service.snapshot_root, service.working_dir,
                                         task.materialized_execution["execution_context"])
    if task.effective_policy != task.materialized_execution.get("effective_policy"):
        raise RepositoryError("REVALIDATION_POLICY_CHANGED")
    if task.effective_policy.get("supervisor_policy"):
        task._supervisor_boundary = lambda **kwargs: service.enforce_supervisor_boundary(task, **kwargs)
    def guard():
        frozen.validate()
        verify_input(task)
    manager = service.manager_factory(working_dir=str(service.working_dir), model="", max_retry=1,
        timeout=300, worker_type="codex", codex_model=None, max_workers=1, profile=frozen.profile,
        task_root=service.task_root, configuration_guard=guard, policy_root=frozen.policy_root,
        initial_baseline=baseline)
    task._runtime_event_callback = manager._runtime_event_callback(task)
    try:
        guard()
        manager_result = manager.run_control_recovery(task, canonical_revalidation=True)
        guard()
        if manager_result.success and validate(task, identity(task, service.working_dir), require_all=True):
            raise RepositoryError("REVALIDATION_GATE_EVIDENCE_STALE")
        task.ended_at = now()
        task_path = task.save(service.task_root)
        result = HarnessService._safe_outcome(task, manager_result, task_path, None)
        result.update(worker_invoked=False, recovery_kind=task.materialized_execution["role"],
                      predecessor_execution_id=task.materialized_execution["handoff_source_execution_id"],
                      revalidation_binding_sha256=digest(task.materialized_execution["revalidation_binding"]))
        finish_gate_execution(repo, task, result)
        return result
    except BaseException:
        # No fallback publication; durable GATING is an explicit interrupted operation.
        raise
    finally:
        manager.shutdown()


def approved_delta(repo, promoter, source_base, canonical_base, predecessor_ids, *, own_scope):
    """Every changed canonical byte must have an exact accepted publication owner."""
    from qa_quarantine import QACandidateStore
    declared = {}
    source_jobs = []
    for job_id in predecessor_ids:
        job = repo.read_job(job_id)
        result = job.get("last_result") or {}
        pub = result.get("canonical_promotion") or {}
        if job["status"] != "SUCCEEDED" or pub.get("status") != "PROMOTED":
            raise CandidateError("REVALIDATION_PREDECESSOR_NOT_PUBLISHED")
        store = QACandidateStore(promoter.root / "candidates", promoter.canonical, None)
        journal = store.load_publication(pub.get("publication_id", ""))
        if (journal.get("state") != "COMPLETED" or journal.get("disposition") != "PROMOTED"
                or journal.get("authority", {}).get("job_id") != job_id
                or pub.get("candidate_ids") != journal.get("candidate_ids")):
            raise CandidateError("REVALIDATION_PREDECESSOR_PUBLICATION_INVALID")
        for candidate_id in pub["candidate_ids"]:
            candidate = store.load(candidate_id)
            if candidate["state"] != "PROMOTED":
                raise CandidateError("REVALIDATION_PREDECESSOR_PUBLICATION_INVALID")
            paths = candidate["delta"]["changed_files"]
            if paths_overlap(paths, own_scope):
                raise CandidateError("REVALIDATION_SCOPE_OVERLAP")
            for record in candidate["delta"]["files"]:
                path = record["path"]
                after = record.get("candidate_sha256", "DELETED")
                if path in declared or canonical_base.get(path, "DELETED") != after:
                    raise CandidateError("REVALIDATION_CANONICAL_FOREIGN_BYTES")
                declared[path] = after
        source_jobs.append(source_job_binding(repo, job_id))
    drift = sorted(p for p in set(source_base) | set(canonical_base)
                   if source_base.get(p, "DELETED") != canonical_base.get(p, "DELETED"))
    if not drift or not set(drift).issubset(declared):
        raise CandidateError("REVALIDATION_CANONICAL_DELTA_UNOWNED")
    return dict(changed_files=drift, accepted_publications=source_jobs)


def successor_task(parent, *, candidate, profile_dir, candidate_id):
    """Copy contract/lineage only; every gate/role receipt begins empty."""
    from task_state import TaskState
    task = TaskState(**asdict(parent))
    defaults = TaskState(task_id="", requirement="", worker=parent.worker)
    for name in ("gate_evidence", "test_checkpoint", "test_receipts", "test_evidence", "build", "build_evidence",
                 "reviewer_execution", "reviewer_attempt_id", "reviewer_invocations", "review_evidence",
                 "inherited_review_evidence", "review_result", "review_status", "review_coverage",
                 "tester_attempt_id", "tester_attempt_history", "official_tester_execution", "official_tester_invoked",
                 "test_status", "test_summary", "test_plan_amendment", "verification_status", "qa_request",
                 "failure_code", "failure_stage", "failure_origin", "failure_reason", "failure_type",
                 "commit_blockers", "control_recovery_evidence", "notification_evidence", "execution_runtime",
                 "execution_runtime_history", "attempt_history", "secondary_failures"):
        if hasattr(task, name):
            setattr(task, name, copy.deepcopy(getattr(defaults, name)))
    for name in ("attempt_id", "native_session_binding_id", "native_session_id", "runtime_process_id",
                 "reviewer_native_session_binding_id", "reviewer_native_session_id",
                 "actual_worker", "actual_model", "worker_execution_evidence", "worker_results",
                 "worker_stdout", "worker_stderr", "worker_attempt_count"):
        if hasattr(task, name):
            setattr(task, name, copy.deepcopy(getattr(defaults, name)))
    task.task_id = "TASK-REVALIDATE-" + uuid.uuid4().hex.upper()
    task.working_dir, task.profile_dir = str(candidate), str(profile_dir)
    task.candidate_id = candidate_id
    task.status, task.stage = "RUNNING", "BUILD"
    task.qa_type = task.hold_scope = ""
    task.started_at, task.ended_at = now(), ""
    task.selective_revalidation = dict(reason="POST_PUBLICATION_DISJOINT_CANONICAL_DELTA",
        worker_rerun=False, gates=["BUILD", "TEST", "REVIEW", "VERIFICATION"])
    return task


class RevalidationCustody:
    """Owned host copy/import proof. It never claims a native Worker ran."""
    def __init__(self, root, task):
        self.root, self.task = Path(root), task
        self.candidate = Path(task.working_dir)
        self.record = read(self.root / "allocation.json")

    def verify_origin(self):
        verify_input(self.task)

    def close(self):
        self.verify_origin()


def verify_custody(task, service, record, cleanup):
    binding = verify_input(task)
    if (cleanup.get("boundary") != "harness-current-input/1"
            or record.get("boundary") != cleanup["boundary"]
            or record.get("state") != "CLOSED" or cleanup.get("state") != "QUIESCED"
            or cleanup.get("quiesced") is not True or cleanup.get("worker_invoked") is not False
            or cleanup.get("host_input") != binding["input_content"]
            or cleanup.get("binding_sha256") != digest(binding)
            or record.get("execution_id") != task.execution_id
            or cleanup.get("execution_id") != task.execution_id
            or cleanup.get("candidate_id") != task.candidate_id
            or cleanup.get("job_id") != task.job_id
            or record.get("candidate_id") != task.candidate_id
            or record.get("job_id") != task.job_id
            or Path(record["candidate"]).resolve() != Path(service.working_dir).resolve()
            or Path(record['source']).resolve() != Path(read(binding['admission_path'])['source_path']).resolve()):
        raise CandidateError("REVALIDATION_CUSTODY_INVALID")


def revalidate(control, job_id, *, expected_revision, request_id, predecessor_ids,
               confirmation):
    """Revision-bound operator continuation. Returns a new held candidate, no publish."""
    from canonical_ownership import canonical_ownership
    from gate_evidence import identity, validate
    from git_collector import GitCollector
    from harness_service import HarnessService, EXECUTION_CONTEXT_KEYS
    from isolated_promotion import content, module_heads
    from production_isolation import inventory, validate_git_ownership
    from run import SingleInstanceLock
    from task_state import TaskState
    from runtime_identity import source_process_identity
    control._assert_writable()
    if confirmation != CONFIRMATION or not predecessor_ids or job_id in predecessor_ids:
        raise CandidateError("REVALIDATION_CONFIRMATION_REQUIRED")
    repo, promoter = control.store.repository, control.isolation.promoter
    request = dict(job_id=job_id, expected_revision=expected_revision,
                   request_id=request_id, predecessor_ids=list(predecessor_ids))
    with repo.hold():
        prior = _receipt(repo, _key(request_id))
        if prior:
            if prior["binding"].get("operator_request") != request:
                raise CandidateError("REVALIDATION_IDEMPOTENCY_CONFLICT")
            if prior["state"] != "AWAITING_QA":
                raise CandidateError("REVALIDATION_RECONCILIATION_REQUIRED")
            return dict(replayed=True, historical_only=True, result=prior["result"])
        if repo._connection.execute('PRAGMA user_version').fetchone()[0] != 6:
            raise CandidateError('REVALIDATION_SCHEMA6_REQUIRED')
    if control.supervisor.is_running() or not control.harness.acquire_execution_lease():
        raise CandidateError("ACTIVE_HARNESS_PROCESS")
    child = None
    try:
        with canonical_ownership(control.harness.working_dir):
            job = repo.read_job(job_id)
            if job["revision"] != expected_revision or job["status"] != "AWAITING_QA":
                raise CandidateError("JOB_REVISION_CONFLICT")
            scope = list(job['request'].get('target_resources') or [])
            if not scope or any(any(mark in path for mark in ('*','?','[',']')) for path in scope):
                raise CandidateError('REVALIDATION_EXACT_FILE_SCOPE_REQUIRED')
            with repo.hold():
                assert_quiescent(repo, [job_id, *predecessor_ids])
            old = promoter.allocations.get(job_id)
            if not old or not old.get("held_verification"):
                raise CandidateError("REVALIDATION_PRESERVED_OWNER_REQUIRED")
            held = old["held_verification"]
            parent_execution = repo.execution((job["last_result"] or {})["execution_id"])
            bound = {**job, "materialized_execution": parent_execution,
                     "execution_context": parent_execution["execution_context"]}
            parent, _ = promoter._task(bound, old["service"], held["machine_result"])
            # The current approved canary is LOW. Broader semantic/policy
            # continuations need their own contract; never weaken their floors.
            if (parent.review_policy.get("risk_level") != "LOW"
                    or parent.effective_policy != parent_execution.get("effective_policy")):
                raise CandidateError("REVALIDATION_POLICY_UNSUPPORTED")
            if validate(parent, identity(parent, old["service"].working_dir), require_all=True):
                raise CandidateError("REVALIDATION_PARENT_GATES_STALE")
            from reviewer_successor import verify_accepted_review
            verify_accepted_review(repo, parent, candidate_alias=held["quarantine_candidate_id"])
            profile = old["profile"]
            if profile.control_policy.commit_policy != "BATCH_FINAL_COMMIT":
                raise CandidateError("REVALIDATION_BATCH_COMMIT_POLICY_REQUIRED")
            preserve = old["preserve"]
            canonical_base = content(promoter.canonical, profile, preserve)
            heads = module_heads(promoter.canonical, profile)
            if heads != old["source_heads"]:
                raise CandidateError("REVALIDATION_GIT_HEAD_CHANGED")
            delta = approved_delta(repo, promoter, old["source_full_base"] if "source_full_base" in old else old["full_base"],
                                   canonical_base, predecessor_ids, own_scope=job["request"]["target_resources"])
            changed = sorted(parent.task_owned_changed_files or parent.changed_files)
            if not changed or paths_overlap(changed, delta["changed_files"]):
                raise CandidateError("REVALIDATION_SCOPE_OVERLAP")
            expected_candidate = dict(canonical_base)
            original_input = content(old["service"].working_dir, profile, preserve)
            for rel in changed:
                if rel in original_input:
                    expected_candidate[rel] = original_input[rel]
                else:
                    expected_candidate.pop(rel, None)
            root = control.isolation.root / ("revalidate-" + uuid.uuid4().hex[:16])
            root.mkdir(parents=True, exist_ok=False)
            candidate, source = root / "candidate", root / "source"
            full = inventory(promoter.canonical, include_git=True)
            from production_isolation import copy_workspace_tree
            copy_workspace_tree(promoter.canonical, candidate)
            copy_workspace_tree(promoter.canonical, source)
            if (inventory(promoter.canonical, include_git=True) != full
                    or inventory(candidate, include_git=True) != full
                    or inventory(source, include_git=True) != full):
                raise CandidateError("REVALIDATION_COPY_RACE")
            validate_git_ownership(candidate, profile)
            validate_git_ownership(source, profile)
            profile_dir = root / "profiles" / profile.id
            profile_origin = inventory(control.harness.profile_dir, include_git=True)
            original_allocation = read(parent.candidate_manifest)
            if (original_allocation.get('profile_origin_inventory') is not None
                    and original_allocation['profile_origin_inventory'] != profile_origin):
                raise CandidateError('PROFILE_DRIFT')
            shutil.copytree(control.harness.profile_dir, profile_dir)
            if (inventory(control.harness.profile_dir, include_git=True) != profile_origin
                    or inventory(profile_dir, include_git=True) != profile_origin):
                raise CandidateError('PROFILE_DRIFT')
            profile_manifest = read(profile_dir / "project.json")
            profile_manifest["workspace_roots"] = [*profile_manifest["workspace_roots"], str(candidate)]
            atomic(profile_dir / "project.json", profile_manifest)
            profile_copy = inventory(profile_dir, include_git=True)
            child = HarnessService(candidate, profile_dir, task_root=control.harness.task_root,
                snapshot_root=control.harness.snapshot_root, manager_factory=control.harness.manager_factory,
                instance_lock=SingleInstanceLock(root / "execution.lock"),
                supervisor_advisor=getattr(control.harness, "supervisor_advisor", None))
            task = successor_task(parent, candidate=candidate, profile_dir=profile_dir,
                                  candidate_id="RCAND-" + uuid.uuid4().hex)
            child_profile, available = child._load_profile()
            collector = GitCollector(str(candidate), profile=child_profile)
            baseline, error = collector.capture_baseline(task)
            if baseline is None:
                raise CandidateError("REVALIDATION_BASELINE_FAILED", error)
            # Source records, unlike a synthetic success, provide the actual preimages.
            snapshot = root / "pre-job.json"
            write_once(snapshot, dict(kind="PRE_JOB_SNAPSHOT", job_id=job_id, snapshot_id=task.task_id,
                files=[dict(path=p, sha256=canonical_base.get(p, "DELETED"),
                            state="PRESENT" if p in canonical_base else "DELETED",
                            snapshot=str(source / p)) for p in changed]))
            baseline = replace(baseline, snapshot_manifest=snapshot)
            for rel in changed:
                target = candidate / rel
                if rel not in original_input:
                    if target.is_file(): target.unlink()
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((Path(parent.working_dir) / rel).read_bytes())
            if content(candidate, child_profile, preserve) != expected_candidate:
                raise CandidateError("REVALIDATION_IMPORT_SCOPE_MISMATCH")
            diff = collector.collect_selected(task, baseline, changed)
            if not diff.success or sorted(task.changed_files) != changed or not task.git_diff.strip():
                raise CandidateError("REVALIDATION_DIFF_MISMATCH")
            task.task_owned_changed_files = changed
            task.changed_file_sha256 = {p: expected_candidate.get(p, "DELETED") for p in changed}
            task.pre_job_snapshot_manifest, task.pre_job_snapshot_id = str(snapshot), task.task_id
            immutable = {parent.candidate_manifest: _sha(parent.candidate_manifest),
                         str(Path(parent.candidate_manifest).parent / "cleanup.json"):
                         _sha(Path(parent.candidate_manifest).parent / "cleanup.json"),
                         str(promoter.root / (parent.execution_id + "-qa-verification.json")):
                         _sha(promoter.root / (parent.execution_id + "-qa-verification.json"))}
            parent_task_path = TaskState.task_date_dir(parent.task_id, old['service'].task_root) / (parent.task_id + '.json')
            immutable[str(parent_task_path)] = _sha(parent_task_path)
            proof = dict(job_id=job_id, candidate_id=task.candidate_id, input_content=expected_candidate,
                canonical_base=canonical_base, canonical_heads=heads, changed_files=changed,
                canonical_delta=delta, immutable_predecessor_files=immutable,
                predecessor_execution_id=parent.execution_id, predecessor_candidate_id=parent.candidate_id,
                predecessor_worker_attempt_id=parent.attempt_id, source_path=str(source),
                profile_origin=str(control.harness.profile_dir), profile_origin_inventory=profile_origin,
                profile_copy_inventory=profile_copy)
            admission_path = root / "admission.json"
            write_once(admission_path, proof)
            binding = dict(kind="CANONICAL_REVALIDATION", job_id=job_id, request_id=request_id,
                operator_request=request, predecessor_execution_id=parent.execution_id,
                predecessor_execution_sha256=digest(parent_execution),
                candidate_id=task.candidate_id, workspace=str(candidate),
                admission_path=str(admission_path), admission_sha256=_sha(admission_path),
                input_content=expected_candidate, canonical_base=canonical_base, canonical_heads=heads,
                preserve=preserve, source_jobs=[source_job_binding(repo, job_id), *delta["accepted_publications"]])
            binding.update(task_scope(task))
            configuration = child._configuration(child_profile, available)
            context = {key: configuration[key] for key in EXECUTION_CONTEXT_KEYS}
            if (context['policy_snapshot_sha256'] != parent_execution['policy_revision']
                    or context['policy_snapshot_sha256'] != parent_execution['execution_context']['policy_snapshot_sha256']):
                raise CandidateError('REVALIDATION_POLICY_DRIFT')
            frozen_runtime = child._materialize_runtime_snapshot(child_profile, context)
            if frozen_runtime is None:
                raise CandidateError("MATERIALIZATION_SNAPSHOT_REQUIRED")
            task.materialized_execution["execution_context"] = context
            task.materialized_execution["profile_revision"] = context["profile_snapshot_sha256"]
            task.materialized_execution["runtime_snapshot_ref"] = str(frozen_runtime.root)
            provenance = source_process_identity(Path(__file__).resolve().parent)
            task.materialized_execution.update(harness_source_identity=provenance["source"],
                                              harness_materializer_identity=provenance)
            admit_gate_execution(repo, task, binding=binding, expected_revision=expected_revision)
            task.candidate_manifest = str(root / "allocation.json")
            write_once(root / "allocation.json", dict(job_id=job_id, candidate_id=task.candidate_id,
                execution_id=task.execution_id, candidate=str(candidate), source=str(source),
                state="CLOSED", boundary="harness-current-input/1"))
            write_once(root / "cleanup.json", dict(state="QUIESCED", quiesced=True,
                job_id=job_id, candidate_id=task.candidate_id, execution_id=task.execution_id,
                worker_invoked=False, boundary="harness-current-input/1", host_input=expected_candidate,
                binding_sha256=digest(binding)))
            child.isolated_execution = RevalidationCustody(root, task)
            if not child.acquire_execution_lease():
                raise CandidateError("CHILD_LEASE_BUSY")
            try:
                result = run_fresh_gates(child, task, baseline=baseline)
            finally:
                child.release_execution_lease()
            if content(promoter.canonical, profile, preserve) != canonical_base or module_heads(promoter.canonical, profile) != heads:
                raise CandidateError("REVALIDATION_CANONICAL_CHANGED")
            if result.get("success") is not True:
                return dict(replayed=False, published=False, result=result)
            allocation = dict(service=child, source=source, profile=child_profile, preserve=preserve,
                base=canonical_base, full_base=canonical_base, source_full_base=canonical_base,
                source_heads=heads, canonical_heads=heads)
            promoter.allocations[job_id] = allocation
            new_bound = {**job, "materialized_execution": task.materialized_execution,
                         "execution_context": context}
            held_result = promoter.quarantine(new_bound, child, result)
            with control.store._mutex.hold(), repo.hold():
                current = repo.read_job(job_id)
                if current["revision"] != expected_revision:
                    raise CandidateError("JOB_REVISION_CONFLICT")
                validate_source_jobs(repo, binding)
                for decision in repo.open_decisions(job_id):
                    if decision["decision_type"] != "CANDIDATE" or decision.get("candidate_id") != job["candidate_id"]:
                        raise CandidateError("REVALIDATION_OTHER_DECISION_PENDING")
                    stale = {**decision, "status": "STALE", "resolution": "SUPERSEDED_BY_REVALIDATION",
                             "resolved_at": now()}
                    repo._connection.execute("UPDATE decisions SET status='STALE',resolution=?,resolved_at=?,payload=? WHERE decision_id=? AND status='OPEN'",
                        (stale["resolution"], stale["resolved_at"], canonical(stale), decision["decision_id"]))
                current.setdefault("canonical_revalidation_history", []).append(dict(
                    source_task_id=parent.task_id, source_candidate_id=job["candidate_id"],
                    source_result=job["last_result"], binding_sha256=digest(binding)))
                current["canonical_revalidation"] = dict(binding_sha256=digest(binding), request_id=request_id)
                current.update(candidate_id=held_result["candidate_id"], candidate_manifest=held_result["candidate_manifest"],
                               last_result=held_result, status="AWAITING_QA")
                control.store._event(current, "CANONICAL_REVALIDATION_VERIFIED", request_id=request_id)
                control.store._write_job(current)
                repo.record_decision(job_id, contract_revision=current.get("contract_revision", 1),
                    decision_type="CANDIDATE", question="Accept current-input revalidated candidate?",
                    fingerprint=digest(dict(candidate_id=held_result["candidate_id"], request_id=request_id)),
                    payload=dict(candidate_id=held_result["candidate_id"], qa_type="HUMAN_ACCEPTANCE"))
                receipt = _receipt(repo, _key(request_id))
                receipt.update(state="AWAITING_QA", result=held_result)
                _write_receipt(repo, _key(request_id), receipt)
            return dict(replayed=False, published=False, result=held_result,
                        job=repo.read_job(job_id), operation_binding_sha256=digest(binding))
    finally:
        control.harness.release_execution_lease()
