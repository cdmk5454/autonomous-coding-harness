"""Startup-frozen Harness runtime identity for read-only health reporting."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


class RuntimeIdentityError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail[:300]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


@dataclass(frozen=True)
class RuntimeIdentity:
    harness_version: str
    runtime_build_id: str
    runtime_manifest_sha256: str
    runtime_started_at: str
    runtime_pid: int

    def public(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def load(
        cls,
        harness_root: str | Path,
        *,
        pid: int | None = None,
        started_at: str | None = None,
    ) -> "RuntimeIdentity":
        """Read VERSION and its release manifest once for this process lifetime."""
        root = Path(harness_root).resolve()
        version_path = root / "VERSION"
        try:
            version = version_path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as exc:
            raise RuntimeIdentityError("RUNTIME_VERSION_UNREADABLE") from exc
        if version in {".", ".."} or not __import__("re").fullmatch(r"[A-Za-z0-9_+.-]{1,64}", version):
            raise RuntimeIdentityError("RUNTIME_VERSION_INVALID")
        manifest_path = root / "releases" / version / "SHA256SUMS.json"
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeIdentityError("RUNTIME_MANIFEST_UNREADABLE", str(manifest_path)) from exc
        if not isinstance(manifest, dict) or str(manifest.get("version", "")) != version:
            raise RuntimeIdentityError("RUNTIME_MANIFEST_VERSION_MISMATCH")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        build_id = str(manifest.get("build_id") or manifest.get("release_id") or "").strip()
        if not build_id:
            build_id = f"{version}+{manifest_sha[:16]}"
        return cls(
            harness_version=version,
            runtime_build_id=build_id,
            runtime_manifest_sha256=manifest_sha,
            runtime_started_at=started_at or datetime.now().astimezone().isoformat(),
            runtime_pid=int(pid if pid is not None else os.getpid()),
        )


# Additive provenance API. The old public() health schema remains stable.
# No DB, queue, subprocess, environment dump or provider credential is consulted.
import copy as _copy
import re as _re
import threading as _threading
import uuid as _uuid
from pathlib import PurePosixPath as _PurePosixPath

_PROVENANCE_LOCK = _threading.RLock()
_PROCESS_IDENTITIES = {}

def _canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":")).encode()).hexdigest()

def verify_source_manifest(harness_root):
    """Verify packaged source bytes, NOT signatures or loaded-memory attestation."""
    root = Path(harness_root).resolve()
    runtime = RuntimeIdentity.load(root)
    manifest_path = root / "releases" / runtime.harness_version / "SHA256SUMS.json"
    if (manifest_path.is_symlink() or not manifest_path.resolve().is_relative_to(root)
            or any(p.is_symlink() for p in manifest_path.parents if p != root and p.is_relative_to(root))):
        raise RuntimeIdentityError("RUNTIME_SOURCE_MANIFEST_PATH_INVALID")
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != runtime.runtime_manifest_sha256:
        raise RuntimeIdentityError("RUNTIME_SOURCE_MANIFEST_RACED")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    files = manifest.get("source_files")
    if not isinstance(files, dict) or not files or "VERSION" not in files:
        raise RuntimeIdentityError("RUNTIME_SOURCE_MANIFEST_REQUIRED")
    for relative, expected in files.items():
        if not isinstance(relative, str) or "\\" in relative:
            raise RuntimeIdentityError("RUNTIME_SOURCE_PATH_INVALID")
        portable = _PurePosixPath(relative)
        if (portable.is_absolute() or ".." in portable.parts or ":" in relative
                or portable.as_posix() != relative or not portable.parts):
            raise RuntimeIdentityError("RUNTIME_SOURCE_PATH_INVALID")
        target = root.joinpath(*portable.parts)
        if (not target.is_file() or target.is_symlink()
                or not target.resolve().is_relative_to(root)
                or any(parent.is_symlink() for parent in target.parents if parent != root and parent.is_relative_to(root))):
            raise RuntimeIdentityError("RUNTIME_SOURCE_PATH_INVALID")
        if not isinstance(expected, str) or not _re.fullmatch(r"[0-9a-f]{64}", expected):
            raise RuntimeIdentityError("RUNTIME_SOURCE_DIGEST_INVALID")
        if hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            raise RuntimeIdentityError("RUNTIME_SOURCE_DRIFT", relative)
    # Source files added outside the manifest are not silently accepted.
    actual_python = {p.relative_to(root).as_posix() for p in root.glob("*.py")}
    if actual_python != {name for name in files if name.endswith(".py") and "/" not in name}:
        raise RuntimeIdentityError("RUNTIME_SOURCE_INVENTORY_DRIFT")
    fingerprint = _canonical_hash(files)
    if manifest.get("source_fingerprint") != fingerprint:
        raise RuntimeIdentityError("RUNTIME_SOURCE_FINGERPRINT_MISMATCH")
    for key in ("release_id", "source_revision"):
        if not isinstance(manifest.get(key), str) or not _re.fullmatch(r"[A-Za-z0-9_.+:-]{1,160}", manifest[key]):
            raise RuntimeIdentityError("RUNTIME_SOURCE_METADATA_INVALID")
    source = {"harness_version": runtime.harness_version,
              "release_id": manifest["release_id"],
              "source_revision": manifest["source_revision"],
              "source_fingerprint": fingerprint,
              "release_manifest_sha256": runtime.runtime_manifest_sha256}
    return runtime, source

def source_process_identity(harness_root):
    """Freeze a per-PID incarnation and recheck source at Attempt boundaries.

    A cold process with the packaged module search path is required. This is
    traceability, not proof against malicious PYTHONPATH/module hot-reload.
    Forks get a new incarnation; PID reuse does not reuse an old persisted ID.
    """
    root = Path(harness_root).resolve()
    key = (os.getpid(), str(root))
    with _PROVENANCE_LOCK:
        runtime, source = verify_source_manifest(root)
        previous = _PROCESS_IDENTITIES.get(key)
        if previous is not None:
            if previous["source"] != source:
                raise RuntimeIdentityError("RUNTIME_PROCESS_SOURCE_CHANGED")
            return _copy.deepcopy(previous)
        identity = {"schema_revision": "harness-source-process/1",
                    "source": source,
                    "process_incarnation": "HPROC-" + _uuid.uuid4().hex,
                    "pid": os.getpid(), "identity_loaded_at": runtime.runtime_started_at,
                    "assurance": "MANIFEST_AND_DISK_VERIFIED_NOT_MEMORY_ATTESTED"}
        _PROCESS_IDENTITIES[key] = identity
        return _copy.deepcopy(identity)

def attempt_provenance(execution, *, attempt_id, role, model, harness_root):
    """Create metadata inside the existing Attempt transaction, without schema changes."""
    current = source_process_identity(harness_root)
    expected = execution.get("harness_source_identity")
    if expected != current["source"]:
        raise RuntimeIdentityError("ATTEMPT_HARNESS_SOURCE_MISMATCH")
    contracts = dict(execution.get("runtime_contracts") or {}).get("roles") or {}
    descriptor = dict(contracts.get(role) or {})
    policy = execution.get("effective_policy") or {}
    declared = policy.get("resolved_tester") if role == "TESTER" else policy.get("resolved_worker") if role == "WORKER" else {}
    declared = declared if isinstance(declared, dict) else {}
    runtime = str(descriptor.get("runtime") or declared.get("runtime") or declared.get("worker") or "UNKNOWN")
    provider = (str(model).split("/", 1)[0] if "/" in str(model) else
                "NOT_APPLICABLE" if runtime == "deterministic-tester" else "UNKNOWN")
    return {**current, "execution_id": execution["execution_id"],
            "attempt_id": str(attempt_id), "role": str(role), "runtime": runtime,
            "runtime_version": str(descriptor.get("runtime_version") or "UNKNOWN"),
            "adapter_revision": str(descriptor.get("adapter_revision") or "UNKNOWN"),
            "factory_identity": str((execution.get("role_factory_identity") or {}).get(role) or "UNKNOWN"),
            "provider": provider, "model": str(model),
            "profile_snapshot": str(execution.get("profile_revision") or "UNKNOWN"),
            "execution_surface_hash": execution["execution_surface_hash"],
            "route_identity_assurance": "FROZEN_DESCRIPTOR_RUNTIME_RECEIPTS_REQUIRED" if descriptor else "POLICY_ONLY_NATIVE_DESCRIPTOR_NOT_CAPTURED",
            "provider_identity_assurance": "MODEL_PREFIX_ONLY_NOT_PROVIDER_ATTESTED",
            "runtime_receipt_status": "REQUIRED_NOT_YET_OBSERVED"}


def factory_identity(value):
    """Qualified implementation identity only; never repr(instance/config/secrets)."""
    if value is None:
        return 'NOT_CONFIGURED'
    target = value if isinstance(value, type) or hasattr(value, '__qualname__') else type(value)
    name = str(getattr(target, '__module__', 'UNKNOWN')) + '.' + str(getattr(target, '__qualname__', 'UNKNOWN'))
    if not _re.fullmatch(r'[A-Za-z0-9_.<>-]{1,240}', name):
        raise RuntimeIdentityError('FACTORY_IDENTITY_INVALID')
    return name


def configured_role_factories(service, *, worker, deterministic=False):
    """Observe currently configured implementation owners, not presumed delegation.

    Mapping is bound into new materializations and compared before task submission.
    No imports of native factories or provider startup occur here.
    """
    from worker import DroidWorker, CodexWorker, OpenCodeWorker
    from reviewer import Reviewer
    from manager import Manager
    from tester import run_tester, _run_deterministic_tester
    owner = getattr(service, 'hermes_execution_factory', None)
    isolated = getattr(service, 'isolated_execution', None)
    w = {'droid': DroidWorker, 'codex': CodexWorker, 'opencode': OpenCodeWorker}.get(worker)
    rv = Reviewer
    t = _run_deterministic_tester if deterministic else run_tester
    if isolated is not None:
        w = isolated
    elif worker == 'hermes':
        w = owner or getattr(service, 'hermes_worker_factory', None)
    if worker == 'hermes':
        rv = owner or getattr(service, 'hermes_reviewer_factory', None)
        if not deterministic:
            t = owner
    return {'MANAGER': factory_identity(getattr(service, 'manager_factory', Manager)),
            'WORKER': factory_identity(w), 'REVIEWER': factory_identity(rv),
            'TESTER': factory_identity(t)}


def assert_role_factory_binding(service, task):
    """Compare the actual submission owners to the frozen factory identities."""
    expected = (task.materialized_execution or {}).get("role_factory_identity")
    if not expected:
        return {"status": "LEGACY_NOT_CAPTURED"}
    policy = (task.effective_policy or {}).get("resolved_tester") or {}
    actual = configured_role_factories(service, worker=task.worker,
                                      deterministic=policy.get("runtime") == "deterministic")
    if expected != actual:
        raise RuntimeIdentityError("RUNTIME_FACTORY_BINDING_DRIFT")
    return {"status": "MATCH", "role_factory_identity": actual}
