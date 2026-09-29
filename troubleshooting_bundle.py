"""Bounded, secret-free execution evidence for operator troubleshooting."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from control_paths import atomic_state_write
from control_repository import ControlRepository, canonical


def _rows(repository: ControlRepository, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    with repository.hold():
        return [json.loads(row[0]) for row in repository._connection.execute(sql, params)]


def capture(task: Any, output_root: str | Path) -> dict[str, Any]:
    repository = ControlRepository(task.control_repository_path, read_only=True)
    execution_id = str(task.execution_id)
    payload = {
        "schema_version": 1,
        "kind": "HarnessTroubleshootingBundle",
        "job_id": str(task.job_id),
        "task_id": str(task.task_id),
        "identity": {
            "logical_job_id": task.logical_job_id,
            "materialization_id": task.materialization_id,
            "execution_id": execution_id,
            "attempt_id": task.attempt_id,
            "runtime_process_id": task.runtime_process_id,
            "native_session_binding_id": task.native_session_binding_id,
            "native_session_id": task.native_session_id,
            "native_turn_id": task.native_turn_id,
            "command_id": task.command_id,
        },
        "repository_health": repository.health(),
        "attempts": _rows(repository, "SELECT payload FROM attempts WHERE execution_id=? ORDER BY started_at", (execution_id,)),
        "attempt_outcomes": _rows(repository, "SELECT o.payload FROM attempt_outcomes o JOIN attempts a ON a.attempt_id=o.attempt_id WHERE a.execution_id=? ORDER BY a.started_at", (execution_id,)),
        "runtime_processes": _rows(repository, "SELECT p.payload FROM runtime_processes p JOIN attempts a ON a.attempt_id=p.attempt_id WHERE a.execution_id=? ORDER BY p.started_at", (execution_id,)),
        "native_sessions": _rows(repository, "SELECT payload FROM native_sessions WHERE execution_id=? ORDER BY bound_at", (execution_id,)),
        "commands": _rows(repository, "SELECT payload FROM commands WHERE execution_id=? ORDER BY ordinal", (execution_id,)),
        "recoveries": _rows(repository, "SELECT payload FROM runtime_recoveries WHERE execution_id=? ORDER BY ordinal", (execution_id,)),
        "validations": _rows(repository, "SELECT payload FROM validation_dispositions WHERE execution_id=? ORDER BY kind", (execution_id,)),
        "decisions": repository.decisions(str(task.job_id)),
        "product_readiness": {
            "status": task.product_readiness,
            "user_action_required": task.user_action_required,
            "open_decision_count": task.open_decision_count,
        },
        "diagnostics": list(task.report_diagnostics or []),
    }
    root = Path(output_root).resolve()
    destination = root / "troubleshooting" / str(task.task_id) / "bundle.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical(payload).encode("utf-8")
    atomic_state_write(destination, raw, root=root, field="troubleshooting_bundle")
    return {
        "path": str(destination),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "schema_version": 1,
    }

