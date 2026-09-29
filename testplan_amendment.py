"""Audited TestPlan overlays for closed, preserved candidates.

Product executions and their accepted Build proofs remain immutable. Only the
logical Job contract changes; applying the overlay never starts a gate.
"""
import copy
import hashlib
import json
from pathlib import Path

from control_repository import RepositoryError, canonical, digest, now
from job_contract import JobRequest
from test_plan import derive_test_plan, validate_test_contract

CONFIRMATION = "I_CONFIRM_AMEND_TEST_PLAN_PRESERVED_CANDIDATE"


def candidate_inventory(task, workspace, preserve):
    """Complete source inventory using the preserved-candidate exclusion policy."""
    from production_isolation import inventory
    from project_profile import load_project_profile
    profile = load_project_profile(task.profile_dir, workspace)
    return inventory(Path(workspace), profile=profile, preserve=preserve)


def _source_hashes(task, workspace):
    root = Path(workspace).resolve()
    hashes = {}
    for name in task.changed_files:
        path = (root / name).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise RepositoryError("TEST_PLAN_SOURCE_CHANGED")
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def validate_candidate(job, task, workspace, *, required_tests=()):
    """Validate closed custody and accepted Build without modifying either."""
    from production_isolation import inventory, normalized_preserved_inventory
    from project_profile import load_project_profile

    result = job.get("last_result") or {}
    if (job.get("status") != "AWAITING_QA" or job.get("owner_id")
            or result.get("worktree_disposition") != "PRESERVED"
            or not job.get("candidate_id")
            or task.candidate_id != job["candidate_id"]
            or task.job_id != job["job_id"]
            or task.execution_id != result.get("execution_id")
            or task.task_id != result.get("task_id")
            or Path(task.working_dir).resolve() != Path(workspace).resolve()):
        raise RepositoryError("TEST_PLAN_CANDIDATE_NOT_ELIGIBLE")
    allocation = Path(job.get("candidate_manifest") or "")
    try:
        record = json.loads(allocation.read_text(encoding="utf-8"))
        cleanup = json.loads((allocation.parent / "cleanup.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RepositoryError("PRESERVED_CANDIDATE_CUSTODY_INVALID") from exc
    if (allocation.name != "allocation.json" or record.get("state") != "CLOSED"
            or any(record.get(key) != value for key, value in {
                "job_id": job["job_id"], "candidate_id": task.candidate_id,
                "execution_id": task.execution_id}.items())
            or Path(record.get("candidate") or "").resolve() != Path(workspace).resolve()
            or cleanup.get("state") != "QUIESCED" or cleanup.get("quiesced") is not True
            or cleanup.get("imported") is not True):
        raise RepositoryError("PRESERVED_CANDIDATE_CUSTODY_INVALID")
    profile = load_project_profile(task.profile_dir, workspace)
    preserve = [*job["request"].get("target_resources", []),
                *job["request"].get("test_plan", {}).get("required_tests", []),
                *required_tests]
    expected = normalized_preserved_inventory(Path(workspace), cleanup.get("host_input") or {},
                                             profile=profile, preserve=preserve)
    if inventory(Path(workspace), profile=profile, preserve=preserve) != expected:
        raise RepositoryError("PRESERVED_CANDIDATE_CONTENT_CHANGED")
    validate_build(task)
    hashes = _source_hashes(task, workspace)
    if not hashes or hashes != task.build_evidence.get("details", {}).get("source_sha256"):
        raise RepositoryError("TEST_PLAN_SOURCE_CHANGED")
    return hashes


def validate_build(task):
    """Recheck accepted receipt bindings and the actual immutable log bytes."""
    from build_test_runner import BuildTestRunner, BuildTestResult
    from manager import Manager
    build = task.build_evidence
    validator = object.__new__(Manager)
    validator.build_test_runner = object.__new__(BuildTestRunner)
    ok, code = validator._validate_structured_build_evidence(task, BuildTestResult(
        success=build.get("success") is True, build_status=build.get("status", ""),
        details=build.get("details", {})))
    if not ok:
        raise RepositoryError(code)
    receipts = validator._build_executions(build.get("details", {}))
    ids = [receipt.get("execution_id") for receipt in receipts]
    if (not ids or not all(ids) or len(ids) != len(set(ids))
            or any(receipt.get("candidate_id") != task.candidate_id
                   or receipt.get("product_execution_id") != task.execution_id
                   or receipt.get("job_id") != task.job_id for receipt in receipts)):
        raise RepositoryError("TEST_PLAN_BUILD_BINDING_INVALID")


def amend(repository, job_id, *, expected_revision, request_id, confirmation,
          candidate_id, source_hashes, test_plan, task, workspace):
    """Canonical repository operation; exact replay is read-only and idempotent."""
    if repository.read_only:
        raise RepositoryError("CONTROL_REPOSITORY_READ_ONLY")
    if confirmation != CONFIRMATION:
        raise RepositoryError("TEST_PLAN_AMENDMENT_CONFIRMATION_REQUIRED")
    if (not isinstance(request_id, str) or not request_id.strip()
            or type(expected_revision) is not int or not candidate_id
            or not isinstance(source_hashes, dict) or not source_hashes):
        raise RepositoryError("TEST_PLAN_AMENDMENT_BINDING_REQUIRED")
    declared = validate_test_contract(test_plan)
    fingerprint = digest(dict(job_id=job_id, expected_revision=expected_revision,
        request_id=request_id, confirmation=confirmation, candidate_id=candidate_id,
        source_hashes=source_hashes, test_plan=declared))
    with repository.hold():
        row = repository._connection.execute(
            "SELECT payload,phase FROM logical_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise RepositoryError("JOB_NOT_FOUND")
        job = json.loads(row[0])
        for prior in job.get("test_plan_amendments", []):
            if prior["request_id"] == request_id:
                if prior["request_fingerprint"] != fingerprint:
                    raise RepositoryError("IDEMPOTENCY_CONFLICT")
                return {"job": job, "replayed": True, "dispatched": False}
        if job.get("revision") != expected_revision:
            raise RepositoryError("JOB_REVISION_CONFLICT")
        if row["phase"] != "TERMINAL" or repository.active_execution(job_id):
            raise RepositoryError("TEST_PLAN_EXECUTION_NOT_TERMINAL")
        execution = repository.execution(task.execution_id)
        if (not execution or execution.get("job_id") != job_id
                or job.get("candidate_id") != candidate_id):
            raise RepositoryError("TEST_PLAN_CANDIDATE_MISMATCH")
        if validate_candidate(job, task, workspace,
                              required_tests=declared.get("required_tests", [])) != source_hashes:
            raise RepositoryError("TEST_PLAN_SOURCE_CHANGED")
        request = copy.deepcopy(job["request"])
        request["test_plan"] = declared
        parsed = JobRequest.from_mapping(request, allowed_modules=request.get("target_modules", []))
        plan = derive_test_plan(parsed, workspace)
        # The official receipt runner accepts argv syntax and must bind every
        # required test to the approved command before this contract is frozen.
        from types import SimpleNamespace
        from tester import _authorized_test_actions
        import shlex
        if not plan["required_tests"] or not shlex.split(plan["command"]):
            raise RepositoryError("TEST_PLAN_COMMAND_REQUIRED")
        if any(not action["command"] for action in _authorized_test_actions(SimpleNamespace(test_plan=plan))):
            raise RepositoryError("TEST_PLAN_COMMAND_UNBOUND")
        revision = int(job.get("contract_revision", 1)) + 1
        preserve = sorted(set([*plan["production_scope"], *plan["required_tests"],
            *plan["derived_tests"], *task.changed_files,
            *job["request"].get("test_plan", {}).get("required_tests", [])]))
        overlay = dict(request_id=request_id, request_fingerprint=fingerprint,
            expected_revision=expected_revision, candidate_id=candidate_id,
            execution_id=task.execution_id, source_hashes=copy.deepcopy(source_hashes),
            build_evidence_sha256=digest(task.build_evidence), contract_revision=revision,
            test_plan=plan, prior_request=copy.deepcopy(job["request"]),
            prior_contract_revision=int(job.get("contract_revision", 1)),
            confirmation=confirmation, approved_at=now())
        overlay.update(candidate_workspace=str(Path(workspace).resolve()),
            inventory_preserve=preserve,
            candidate_inventory=candidate_inventory(task, workspace, preserve),
            original_build_gate=copy.deepcopy(task.gate_evidence.get("BUILD") or {}),
            original_build_gate_sha256=digest(task.gate_evidence.get("BUILD") or {}))
        job.setdefault("original_request", copy.deepcopy(job["request"]))
        job["request"] = request
        job["request_sha256"] = digest(request)
        job["contract_revision"] = revision
        job["revision"] += 1
        job["updated_at"] = now()
        job["test_plan_amendment"] = copy.deepcopy(overlay)
        job.setdefault("test_plan_amendments", []).append(overlay)
        job.setdefault("contract_revisions", []).append(dict(revision=revision,
            request_sha256=digest(request), approved_at=overlay["approved_at"],
            reason="PRESERVED_CANDIDATE_TEST_PLAN_AMENDMENT"))
        cursor = repository._connection.execute(
            "UPDATE logical_jobs SET payload=?,revision=?,contract_revision=? "
            "WHERE job_id=? AND revision=? AND phase='TERMINAL'",
            (canonical(job), job["revision"], revision, job_id, expected_revision))
        if cursor.rowcount != 1:
            raise RepositoryError("JOB_REVISION_CONFLICT")
        return {"job": job, "replayed": False, "dispatched": False}


def validate_overlay(task, overlay):
    """Check byte freshness; a historical approval alone cannot restore authority."""
    if (overlay.get("candidate_id") != task.candidate_id
            or overlay.get("execution_id") != task.execution_id
            or overlay.get("candidate_workspace") != str(Path(task.working_dir).resolve())
            or overlay.get("source_hashes") != _source_hashes(task, task.working_dir)
            or overlay.get("build_evidence_sha256") != digest(task.build_evidence)
            or overlay.get("candidate_inventory") != candidate_inventory(
                task, task.working_dir, overlay.get("inventory_preserve", []))):
        raise RepositoryError("TEST_PLAN_OVERLAY_BINDING_INVALID")
    validate_build(task)


def apply_to_task(task, job):
    """Apply the approved overlay, retaining original immutable custody proofs."""
    overlay = job.get("test_plan_amendment")
    if not overlay:
        return False
    if overlay.get("contract_revision") != job.get("contract_revision"):
        raise RepositoryError("TEST_PLAN_OVERLAY_BINDING_INVALID")
    validate_overlay(task, overlay)
    task.test_plan_amendment = copy.deepcopy(overlay)
    task.test_plan = copy.deepcopy(overlay["test_plan"])
    task.test_scope_hash = task.test_plan["scope_hash"]
    task.test_required = bool(task.test_plan["mandatory"])
    return True
