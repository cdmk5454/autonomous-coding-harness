"""Content identity binding for the four existing execution gates."""
from pathlib import Path
import hashlib
import os
from control_repository import digest, now

GATE_FIELDS = {"BUILD": ("candidate_hash",),
               "TEST": ("candidate_hash", "test_scope_hash", "execution_surface_hash"),
               "REVIEW": ("candidate_hash", "diff_hash", "acceptance_hash", "execution_surface_hash"),
               "VERIFICATION": ("candidate_hash", "acceptance_hash")}
IGNORED_DIRS = {".git", "node_modules", "target", "dist", ".control", ".tasks", ".venv", "__pycache__"}


def source_hashes(workspace, paths):
    root = Path(workspace).resolve()
    files = {}
    for relative in sorted(set(paths)):
        target = (root / relative).resolve()
        if not target.is_relative_to(root):
            raise ValueError("EXECUTION_SCOPE_PATH_ESCAPE")
        if target.is_dir():
            for directory, dirs, names in os.walk(target, followlinks=False):
                dirs[:] = sorted(d for d in dirs if d not in IGNORED_DIRS and not (Path(directory) / d).is_symlink())
                for name in sorted(names):
                    path = Path(directory) / name
                    if not path.resolve().is_relative_to(root) or path.is_symlink():
                        raise ValueError("EXECUTION_SCOPE_PATH_ESCAPE")
                    files[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            files[target.relative_to(root).as_posix()] = hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else "ABSENT"
    return files


# Runtime authority and content are distinct. Workspace path is deliberately not
# identity: promotion may reuse proofs on an identical canonical input under its lock.
AUTHORITY_FIELDS = ("job_id", "execution_id", "materialization_id", "candidate_id",
                    "worker_attempt_id", "worker_binding_id", "execution_epoch",
                    "base_hash", "verification_contract_hash")


def identity(task, workspace):
    plan = task.test_plan
    frozen = dict(task.materialized_execution or {})
    amendment = dict(getattr(task, 'test_plan_amendment', {}) or {})
    contract = amendment or frozen
    paths = [*plan.get("production_scope", []), *plan.get("required_tests", []),
             *plan.get("derived_tests", []), *task.changed_files]
    semantic = {"requirement": task.requirement, "test_plan": plan,
                "targets": list(getattr(task, "target_resources", [])),
                "modules": list(getattr(task, "target_module", [])),
                "contract_revision": contract.get("contract_revision", 1),
                "frozen_contract": frozen.get("verification_contract", {})}
    contract_current = (not contract.get("test_plan") or plan == contract["test_plan"])
    if frozen.get("current_requirement"):
        contract_current = contract_current and task.requirement == frozen["current_requirement"]
    return {"candidate_hash": digest(source_hashes(workspace, paths)),
            "test_scope_hash": plan.get("scope_hash") or digest(plan),
            "execution_surface_hash": frozen.get("execution_surface_hash", ""),
            "diff_hash": digest(task.git_diff),
            "acceptance_hash": digest({"requirement": task.requirement,
                                       "scenario": plan.get("browser_qa"),
                                       "contract_revision": contract.get("contract_revision", 1)}),
            "job_id": getattr(task, "job_id", "") or task.task_id,
            "execution_id": getattr(task, "execution_id", "") or task.task_id,
            "materialization_id": frozen.get("materialization_id", ""),
            "candidate_id": getattr(task, "candidate_id", "") or task.task_id,
            "worker_attempt_id": getattr(task, "attempt_id", ""),
            "worker_binding_id": getattr(task, "native_session_binding_id", ""),
            "execution_epoch": getattr(task, "execution_epoch", ""),
            "base_hash": frozen.get("baseline_hash", ""),
            "verification_contract_hash": digest(semantic),
            "contract_current": bool(contract_current)}


def role_attempt(task, gate):
    return getattr(task, {"TEST": "tester_attempt_id", "REVIEW": "reviewer_attempt_id"}.get(gate, "attempt_id"), "")


def bind(task, gate, current, status):
    task.gate_evidence[gate] = {
        **{key: current.get(key, "") for key in (*GATE_FIELDS[gate], *AUTHORITY_FIELDS)},
        "role_attempt_id": role_attempt(task, gate), "gate": gate,
        "status": status, "recorded_at": now(), "freshness": "CURRENT"}


def validate(task, current, *, require_all=False):
    stale = []
    candidate_changed = False
    for name, evidence in task.gate_evidence.items():
        amendment = getattr(task, 'test_plan_amendment', {}) or {}
        if (name == 'BUILD' and amendment
                and amendment.get('original_build_gate_sha256') == digest(evidence)):
            from testplan_amendment import validate_overlay
            validate_overlay(task, amendment)
            continue
        candidate_changed |= bool(evidence.get('candidate_hash') and evidence['candidate_hash'] != current.get('candidate_hash'))
    for gate, fields in GATE_FIELDS.items():
        evidence = task.gate_evidence.get(gate)
        if not evidence:
            if require_all and (gate != "TEST" or task.test_plan.get("mandatory")):
                stale.append(gate + ":MISSING")
            continue
        authority_fields = AUTHORITY_FIELDS
        amendment = getattr(task, 'test_plan_amendment', {}) or {}
        if (gate == 'BUILD' and amendment
                and amendment.get('execution_id') == task.execution_id
                and amendment.get('candidate_id') == task.candidate_id
                and amendment.get('build_evidence_sha256') == digest(task.build_evidence)
                and amendment.get('original_build_gate_sha256') == digest(evidence)):
            from testplan_amendment import validate_overlay
            validate_overlay(task, amendment)
            # The operator amendment retains the original Build proof. Only its
            # test-contract identity changes; candidate/base/worker authority do not.
            authority_fields = tuple(k for k in AUTHORITY_FIELDS if k != 'verification_contract_hash')
            fields = ()  # full unchanged inventory supersedes the prior scope hash
        if (current.get("contract_current") is not True
                or evidence.get("gate") != gate
                or evidence.get("role_attempt_id", "") != role_attempt(task, gate)
                or any(evidence.get(key) != current.get(key, "") for key in authority_fields)
                or (gate == 'REVIEW' and candidate_changed)
                or any(not current.get(key) or evidence.get(key) != current[key] for key in fields)):
            evidence["freshness"] = "STALE"
            stale.append(gate + ":STALE")
        elif require_all and evidence.get("status") != "PASS":
            stale.append(gate + ":NOT_PASS")
    return stale


def reuse_build(task, previous, workspace):
    """Explicit Harness-only deterministic BUILD reuse; never Tester/Reviewer.

    Same Job/Execution/candidate/base/complete declared input/contract is required.
    An Attempt change alone is allowed only through this checked receipt. FIX
    normally reruns all gates. Canonical promotion separately checks full bytes
    and compatibility under canonical ownership; this helper cannot authorize it.
    """
    current = identity(task, workspace)
    stable = set(AUTHORITY_FIELDS) - {"worker_attempt_id", "worker_binding_id", "execution_epoch"}
    stable.update(GATE_FIELDS["BUILD"])
    if (previous.get("gate") != "BUILD" or previous.get("status") != "PASS"
            or not current["contract_current"] or not current.get("base_hash")
            or any(previous.get(k) != current.get(k, "") for k in stable)):
        raise ValueError("BUILD_EVIDENCE_REUSE_DENIED")
    bind(task, "BUILD", current, previous["status"])
    task.gate_evidence["BUILD"]["reuse"] = {"source": dict(previous),
        "reason": "IDENTICAL_DECLARED_INPUT_CONTRACT_BASE", "checked_at": now()}
    return task.gate_evidence["BUILD"]


def allowed_scope(task):
    resources = list(task.target_resources)
    if resources and task.materialized_execution:
        resources.extend(task.test_plan.get("required_tests", []))
        resources.extend(task.test_plan.get("derived_tests", []))
    return sorted(set(resources))
