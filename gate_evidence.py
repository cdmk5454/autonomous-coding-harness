"""Content identity binding for the four existing execution gates."""
from pathlib import Path
import hashlib
import os
from control_repository import digest, now

GATE_FIELDS = {"BUILD": ("candidate_hash",),
               "TEST": ("candidate_hash", "test_scope_hash", "execution_surface_hash"),
               "REVIEW": ("diff_hash", "acceptance_hash", "execution_surface_hash"),
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


def identity(task, workspace):
    plan = task.test_plan
    paths = [*plan.get("production_scope", []), *plan.get("required_tests", []),
             *plan.get("derived_tests", []), *task.changed_files]
    return {"candidate_hash": digest(source_hashes(workspace, paths)),
            "test_scope_hash": plan.get("scope_hash", ""),
            "execution_surface_hash": task.materialized_execution.get("execution_surface_hash", ""),
            "diff_hash": digest(task.git_diff),
            "acceptance_hash": digest({"requirement": task.requirement, "production_scope": plan.get("production_scope", []),
                                       "contract_revision": task.materialized_execution.get("contract_revision", 1)})}


def bind(task, gate, current, status):
    task.gate_evidence[gate] = {**{key: current[key] for key in GATE_FIELDS[gate]},
                                "status": status, "recorded_at": now(), "freshness": "CURRENT"}


def validate(task, current, *, require_all=False):
    stale = []
    candidate_changed = any(e.get('candidate_hash') and e['candidate_hash'] != current.get('candidate_hash')
                            for e in task.gate_evidence.values())
    for gate, fields in GATE_FIELDS.items():
        evidence = task.gate_evidence.get(gate)
        if not evidence:
            if require_all and (gate != "TEST" or task.test_plan.get("mandatory")):
                stale.append(gate + ":MISSING")
            continue
        if (gate == 'REVIEW' and candidate_changed) or any(not current.get(key) or evidence.get(key) != current[key] for key in fields):
            evidence["freshness"] = "STALE"
            stale.append(gate + ":STALE")
        elif require_all and evidence.get("status") != "PASS":
            stale.append(gate + ":NOT_PASS")
    return stale


def allowed_scope(task):
    resources = list(task.target_resources)
    if resources and task.materialized_execution:
        resources.extend(task.test_plan.get("required_tests", []))
        resources.extend(task.test_plan.get("derived_tests", []))
    return sorted(set(resources))
