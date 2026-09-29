"""Explicit local operator acceptance of the current workspace; never dispatches work."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from canonical_ownership import canonical_ownership
from control_repository import ControlRepository, RepositoryError
from cumulative_policy import CumulativePolicyManager
from harness_service import HarnessService
from job_queue import JobStore
from project_profile import load_project_profile
from qa_quarantine import QACandidateStore


def local_product_processes(workspace):
    """Conservative local runtime fence; never assumes a dev/build process is idle."""
    if os.name != 'nt':
        raise RepositoryError('REANCHOR_LOCAL_PROCESS_PROBE_UNSUPPORTED')
    env = dict(os.environ, REANCHOR_SCAN_ROOT=str(Path(workspace).resolve()), REANCHOR_OPERATOR_PID=str(os.getpid()))
    script = """$ErrorActionPreference='Stop'; @(Get-CimInstance Win32_Process | Where-Object {
      $_.ProcessId -ne [int]$env:REANCHOR_OPERATOR_PID -and
      $_.Name -match '^(node|java|python|pythonw|opencode|codex|dotnet|droid|hermes)\\.exe$' -and
      $_.CommandLine -and $_.CommandLine.IndexOf($env:REANCHOR_SCAN_ROOT,[StringComparison]::OrdinalIgnoreCase) -ge 0
    } | Select-Object @{n='pid';e={$_.ProcessId}},@{n='name';e={$_.Name}}) | ConvertTo-Json -Compress"""
    shell = Path(os.environ['SystemRoot']) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
    result = subprocess.run([str(shell), '-NoProfile', '-NonInteractive', '-Command', script],
                            env=env, capture_output=True, text=True, timeout=20,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode:
        raise RepositoryError('REANCHOR_LOCAL_PROCESS_PROBE_FAILED')
    value = json.loads(result.stdout or '[]')
    return [value] if isinstance(value, dict) else value


def process_alive(pid: int) -> bool:
    """Fail closed on inaccessible PIDs; never terminate a process to gain admission."""
    if pid <= 0:
        raise RepositoryError("REANCHOR_PROCESS_IDENTITY_UNKNOWN")
    if os.name == "nt":
        from hermes_process_boundary import identity
        value = identity(pid)
        if value.get("accessible") is False and value.get("error_code") != 87:
            raise RepositoryError("REANCHOR_PROCESS_IDENTITY_UNKNOWN")
        return bool(value.get("alive"))
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        raise RepositoryError("REANCHOR_PROCESS_IDENTITY_UNKNOWN") from None


def current_delta(manager, baseline_id):
    manifest = manager.load_batch_manifest(baseline_id)
    before = manifest["operator_inventory"]
    after = manager.workspace_inventory()
    old = {p: h for r in before["repos"] for p, h in r["file_hashes"].items()}
    new = {p: h for r in after["repos"] for p, h in r["file_hashes"].items()}
    return {"changed_files": sorted(p for p in old.keys() | new.keys() if old.get(p) != new.get(p)),
            "repository_state_equal": before == after}


def reanchor(harness, *, expected_revision, request_id, accept_current_workspace=False,
             confirmation="", pid_probe=process_alive, writer_probe=local_product_processes):
    profile, _ = harness._load_profile()
    if not accept_current_workspace or confirmation != f"REANCHOR {profile.id.upper()}":
        raise RepositoryError("REANCHOR_ACKNOWLEDGEMENT_REQUIRED")
    # Inspect before opening a writable repository: this operation never migrates.
    readonly = ControlRepository(harness.control_root / "control.sqlite3", read_only=True)
    if readonly.health()["schema_version"] not in (5, 6):
        raise RepositoryError("REANCHOR_SCHEMA_UNSUPPORTED")
    if not harness.acquire_execution_lease():
        raise RepositoryError("REANCHOR_ACTIVE_WRITER")
    try:
        with canonical_ownership(harness.working_dir):
            repository = ControlRepository(harness.control_root / "control.sqlite3")
            store = JobStore(harness.control_root, repository=repository)
            manager = CumulativePolicyManager(harness.working_dir, harness.control_root, profile)
            candidates = QACandidateStore(harness.control_root / "candidates", harness.working_dir, profile)
            initial = None

            def verify():
                nonlocal initial
                if writer_probe(harness.working_dir):
                    raise RepositoryError("REANCHOR_LOCAL_PRODUCT_PROCESS_ACTIVE")
                if candidates.unresolved_publications():
                    raise RepositoryError("REANCHOR_PUBLICATION_UNRESOLVED")
                if store.actionable_qa_jobs():
                    raise RepositoryError("REANCHOR_ACTIONABLE_QA")
                observations = []
                for row in repository._connection.execute("SELECT pid,state FROM runtime_processes WHERE state IN ('RUNNING','STARTING','UNKNOWN','LOST')"):
                    alive = pid_probe(int(row["pid"]))
                    observations.append({'pid':row['pid'], 'persisted_state':row['state'], 'alive':alive})
                    if alive:
                        raise RepositoryError("REANCHOR_LIVE_RUNTIME")
                tables = {r[0] for r in repository._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if 'execution_slots' in tables and repository._connection.execute('SELECT 1 FROM execution_slots LIMIT 1').fetchone():
                    raise RepositoryError("REANCHOR_EXECUTION_SLOT_HELD")
                current = manager.workspace_inventory()
                if initial is None:
                    initial = current
                elif current != initial:
                    raise RepositoryError("REANCHOR_WORKSPACE_CHANGED")
                return {'local_product_processes':[], 'runtime_observations':observations,
                        'unresolved_publications':[], 'actionable_qa':False, 'stable_inventory':True}

            def prepare(generation, previous):
                return manager.prepare_operator_reanchor(generation, previous, initial, request_id)

            receipt, replayed = repository.operator_reanchor(
                expected_revision=expected_revision, request_id=request_id, acknowledgement=confirmation,
                workspace=harness.working_dir, profile_id=profile.id, prepare=prepare, verify=verify)
            # A retry after a committed receipt does not activate or capture again.
            delta = current_delta(manager, receipt["baseline_id"])
            return {"receipt": receipt, "replayed": replayed, "product_delta": delta}
    finally:
        harness.release_execution_lease()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--working-dir')
    parser.add_argument('--control-root', default=str(Path(__file__).resolve().parent / '.control'))
    parser.add_argument('--accept-current-workspace', action='store_true')
    parser.add_argument('--confirmation', required=True)
    parser.add_argument('--expected-revision', required=True, type=int)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--receipt-file', type=Path)
    args = parser.parse_args(argv)
    try:
        workspace = args.working_dir
        if not workspace:
            data = json.loads((Path(args.profile) / 'project.json').read_text(encoding='utf-8-sig'))
            roots = data.get('workspace_roots', [])
            if len(roots) != 1:
                raise RepositoryError('REANCHOR_WORKSPACE_REQUIRED')
            workspace = roots[0]
        load_project_profile(args.profile, workspace)
        control = Path(args.control_root).resolve()
        if control.name != '.control' or control.is_relative_to(Path(workspace).resolve()):
            raise RepositoryError('REANCHOR_CONTROL_ROOT_INVALID')
        if args.receipt_file and args.receipt_file.resolve().is_relative_to(Path(workspace).resolve()):
            raise RepositoryError('REANCHOR_RECEIPT_INSIDE_PRODUCT')
        harness = HarnessService(workspace, args.profile, task_root=control.parent / '.tasks')
        result = reanchor(harness, expected_revision=args.expected_revision, request_id=args.request_id,
                          accept_current_workspace=args.accept_current_workspace, confirmation=args.confirmation)
        if args.receipt_file:
            args.receipt_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        receipt = result['receipt']
        print(json.dumps({k: receipt[k] for k in ('receipt_key','generation','baseline_id','queue_revision','accepted_dirty_path_count')}, ensure_ascii=False))
        print(json.dumps({'replayed':result['replayed'], 'product_delta':result['product_delta']}))
        return 0
    except Exception as exc:
        # Do not render config values, credentials or raw subprocess output.
        print(json.dumps({'ok':False,'code':getattr(exc,'code',type(exc).__name__)}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
