"""Immutable task-owned pre-rollback checkpoints for cumulative retries."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from git_collector import GitTaskBaseline
from runtime_safety import git_subprocess_env, scrub_secrets, trusted_executable


SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = {1, 2}
CHECKPOINT_KINDS = {
    "PRE_ROLLBACK_ATTEMPT_CHECKPOINT",
    "CRASH_SALVAGE_CHECKPOINT",
    "SCOPED_DERIVED_CHECKPOINT",
}


class AttemptCheckpointError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(code)
        self.code = code
        self.detail = scrub_secrets(detail)[:1000]


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(data: Mapping[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _eol(data: bytes | None) -> str:
    if data is None or b"\0" in data:
        return "BINARY" if data is not None else "MISSING"
    if b"\r\n" in data:
        return "CRLF"
    if b"\n" in data:
        return "LF"
    if b"\r" in data:
        return "CR"
    return "NONE"


def _bom(data: bytes | None) -> str:
    if not data:
        return ""
    for marker, name in ((b"\xef\xbb\xbf", "UTF8"), (b"\xff\xfe", "UTF16LE"), (b"\xfe\xff", "UTF16BE")):
        if data.startswith(marker):
            return name
    return ""


class AttemptCheckpointStore:
    """Store deduplicated blobs and one atomically published manifest per checkpoint."""

    def __init__(self, workspace: str | Path, control_root: str | Path):
        self.workspace = Path(workspace).resolve()
        self.root = Path(control_root).resolve() / "cumulative" / "attempt-checkpoints"
        self.blob_root = self.root / "blobs" / "sha256"

    @staticmethod
    def _locate(baseline: GitTaskBaseline, changed: str):
        normalized = changed.replace("\\", "/").strip("/")
        for repo in baseline.repos:
            prefix = f"{repo.module}/" if repo.module else ""
            if prefix and normalized.startswith(prefix):
                return repo, normalized[len(prefix):]
            if not prefix:
                return repo, normalized
        return None

    def _path(self, changed: str) -> Path:
        target = (self.workspace / Path(*PurePosixPath(changed).parts)).resolve()
        try:
            target.relative_to(self.workspace)
        except ValueError as exc:
            raise AttemptCheckpointError("CHECKPOINT_PATH_ESCAPE", changed) from exc
        return target

    def _tree_bytes(self, repo, tree: str, relative: str) -> bytes | None:
        proc = subprocess.run(
            [trusted_executable("git", forbidden_root=self.workspace), "show", f"{tree}:{relative}"],
            cwd=str(repo.git_dir), capture_output=True, env=git_subprocess_env(),
        )
        if proc.returncode == 0:
            return proc.stdout
        exists = subprocess.run(
            [trusted_executable("git", forbidden_root=self.workspace), "cat-file", "-e", f"{tree}:{relative}"],
            cwd=str(repo.git_dir), capture_output=True, env=git_subprocess_env(),
        )
        if exists.returncode != 0:
            return None
        raise AttemptCheckpointError("CHECKPOINT_BASELINE_READ_FAILED", relative)

    def _tree_mode(self, repo, tree: str, relative: str) -> int:
        proc = subprocess.run(
            [trusted_executable("git", forbidden_root=self.workspace), "ls-tree", "-z", tree, "--", relative],
            cwd=str(repo.git_dir), capture_output=True, env=git_subprocess_env(),
        )
        if proc.returncode != 0:
            raise AttemptCheckpointError("CHECKPOINT_BASELINE_MODE_READ_FAILED", relative)
        if not proc.stdout:
            return 0
        try:
            return int(proc.stdout.split(None, 1)[0], 8) & 0o777
        except (ValueError, IndexError) as exc:
            raise AttemptCheckpointError("CHECKPOINT_BASELINE_MODE_INVALID", relative) from exc

    def _put_blob(self, data: bytes) -> str:
        digest = _sha(data)
        destination = self.blob_root / digest[:2] / digest
        if destination.exists():
            if _sha(destination.read_bytes()) != digest:
                raise AttemptCheckpointError("CHECKPOINT_BLOB_HASH_MISMATCH", digest)
            return digest
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{digest}.", dir=str(destination.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(name, destination)
        finally:
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass
        return digest

    def _blob(self, digest: str) -> bytes:
        path = self.blob_root / digest[:2] / digest
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise AttemptCheckpointError("CHECKPOINT_BLOB_MISSING", digest) from exc
        if _sha(data) != digest:
            raise AttemptCheckpointError("CHECKPOINT_BLOB_HASH_MISMATCH", digest)
        return data

    @staticmethod
    def _normalized_scope(paths: Sequence[str]) -> list[str]:
        normalized = [str(path).replace("\\", "/").strip("/") for path in paths]
        if any(not path or path.startswith("../") or "/../" in path for path in normalized):
            raise AttemptCheckpointError("CHECKPOINT_SCOPE_INVALID")
        if len(normalized) != len(set(normalized)):
            raise AttemptCheckpointError("CHECKPOINT_SCOPE_INVALID")
        return sorted(normalized)

    def _load_pre_job_snapshot(self, source: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
        raw = str(source.get("pre_job_snapshot_manifest", ""))
        if not raw:
            raise AttemptCheckpointError("CHECKPOINT_PRE_JOB_SNAPSHOT_MISSING")
        path = Path(raw).resolve()
        try:
            path.relative_to(self.root.parent / "job-snapshots")
            payload = path.read_bytes()
            snapshot = json.loads(payload.decode("utf-8"))
        except (ValueError, OSError, json.JSONDecodeError) as exc:
            raise AttemptCheckpointError("CHECKPOINT_PRE_JOB_SNAPSHOT_INVALID") from exc
        checks = (
            (snapshot.get("kind") == "PRE_JOB_SNAPSHOT", "CHECKPOINT_PRE_JOB_SNAPSHOT_INVALID"),
            (snapshot.get("snapshot_id") == source.get("pre_job_snapshot_id"), "CHECKPOINT_PRE_JOB_SNAPSHOT_MISMATCH"),
            (snapshot.get("job_id") == source.get("job_id"), "CHECKPOINT_PRE_JOB_SNAPSHOT_MISMATCH"),
            (snapshot.get("baseline_id") == source.get("active_baseline_id"), "CHECKPOINT_PRE_JOB_SNAPSHOT_MISMATCH"),
        )
        for passed, code in checks:
            if not passed:
                raise AttemptCheckpointError(code)
        return snapshot, _sha(payload)

    def _snapshot_tree_bytes(self, snapshot: Mapping[str, Any], changed: str) -> bytes | None:
        normalized = changed.replace("\\", "/").strip("/")
        for repo in snapshot.get("repos") or []:
            module = str(repo.get("module", ""))
            prefix = f"{module}/" if module else ""
            if prefix and not normalized.startswith(prefix):
                continue
            relative = normalized[len(prefix):] if prefix else normalized
            tree = str(repo.get("worktree_tree", ""))
            repo_path = Path(str(repo.get("repo", ""))).resolve()
            if not tree or not repo_path.is_dir():
                raise AttemptCheckpointError("CHECKPOINT_PRE_JOB_TREE_INVALID", changed)
            proc = subprocess.run(
                [trusted_executable("git", forbidden_root=self.workspace), "show", f"{tree}:{relative}"],
                cwd=str(repo_path), capture_output=True, env=git_subprocess_env(),
            )
            if proc.returncode == 0:
                return proc.stdout
            exists = subprocess.run(
                [trusted_executable("git", forbidden_root=self.workspace), "cat-file", "-e", f"{tree}:{relative}"],
                cwd=str(repo_path), capture_output=True, env=git_subprocess_env(),
            )
            if exists.returncode != 0:
                return None
            raise AttemptCheckpointError("CHECKPOINT_PRE_JOB_TREE_READ_FAILED", changed)
        raise AttemptCheckpointError("CHECKPOINT_PATH_UNMAPPED", changed)

    def preview_scoped_reconstruction(
        self,
        manifest_path: str | Path,
        *,
        authorized_scope: Sequence[str],
        excluded_scope: Sequence[str],
    ) -> dict[str, Any]:
        """Validate and project a checkpoint subset without creating an artifact."""
        source_path = Path(manifest_path).resolve()
        source = self.load(source_path)
        authorized = self._normalized_scope(authorized_scope)
        excluded = self._normalized_scope(excluded_scope)
        if not authorized or set(authorized) & set(excluded):
            raise AttemptCheckpointError("CHECKPOINT_SCOPE_INVALID")
        source_items = {str(item["path"]).replace("\\", "/"): item for item in source["files"]}
        source_paths = set(source_items)
        if not set(authorized).issubset(source_paths):
            raise AttemptCheckpointError("CHECKPOINT_AUTHORIZED_DELTA_MISSING")
        if source_paths != set(authorized) | set(excluded):
            raise AttemptCheckpointError("CHECKPOINT_SCOPE_ATTRIBUTION_AMBIGUOUS")
        snapshot, snapshot_sha = self._load_pre_job_snapshot(source)
        prestate: dict[str, str] = {}
        selected: list[dict[str, Any]] = []
        for relative in sorted(source_paths):
            item = source_items[relative]
            tree_bytes = self._snapshot_tree_bytes(snapshot, relative)
            tree_sha = _sha(tree_bytes) if tree_bytes is not None else "MISSING"
            if tree_sha != str(item.get("before_sha256", "")):
                raise AttemptCheckpointError("CHECKPOINT_PRE_JOB_FILE_MISMATCH", relative)
            prestate[relative] = tree_sha
            if relative in authorized:
                selected.append(dict(item))
        source_sha = _sha(source_path.read_bytes())
        scope_sha = _sha(_canonical({"authorized": authorized, "excluded": excluded}))
        base_identity = _sha(_canonical({
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_sha256": snapshot_sha,
            "repo_trees": sorted(
                (str(repo.get("module", "")), str(repo.get("worktree_tree", "")))
                for repo in snapshot.get("repos") or []
            ),
        }))
        result_files = {path: prestate[path] for path in sorted(source_paths)}
        result_files.update({str(item["path"]): str(item["after_sha256"]) for item in selected})
        candidate_hash = _sha(_canonical({"base": base_identity, "files": sorted(result_files.items())}))
        seed = {
            "source_checkpoint_id": source["checkpoint_id"],
            "source_manifest_sha256": source_sha,
            "base_identity": base_identity,
            "scope_sha256": scope_sha,
            "resulting_candidate_sha256": candidate_hash,
        }
        checkpoint_id = "CHK-" + hashlib.sha256(_canonical(seed)).hexdigest()[:24].upper()
        return {
            "valid": True,
            "checkpoint_id": checkpoint_id,
            "source_checkpoint_id": source["checkpoint_id"],
            "source_manifest_path": str(source_path),
            "source_manifest_sha256": source_sha,
            "pre_job_snapshot_id": snapshot["snapshot_id"],
            "pre_job_snapshot_manifest_sha256": snapshot_sha,
            "reconstruction_base_ref": base_identity,
            "predecessor_delta_identity": base_identity,
            "authorized_scope": authorized,
            "excluded_scope": excluded,
            "authorized_scope_sha256": scope_sha,
            "selected_current_delta": authorized,
            "excluded_current_delta": excluded,
            "resulting_changed_files": authorized,
            "resulting_candidate_sha256": candidate_hash,
            "pre_job_file_sha256": prestate,
            "resulting_file_sha256": result_files,
            "source": source,
            "selected_files": selected,
        }

    def create_scoped_reconstruction(
        self,
        manifest_path: str | Path,
        *,
        authorized_scope: Sequence[str],
        excluded_scope: Sequence[str],
        request_id: str,
        reason: str = "SCOPED_CHECKPOINT_RECONSTRUCTION",
    ) -> dict[str, Any]:
        if not request_id.strip() or reason != "SCOPED_CHECKPOINT_RECONSTRUCTION":
            raise AttemptCheckpointError("CHECKPOINT_RECONSTRUCTION_REQUEST_INVALID")
        preview = self.preview_scoped_reconstruction(
            manifest_path, authorized_scope=authorized_scope, excluded_scope=excluded_scope
        )
        source = preview.pop("source")
        selected = preview.pop("selected_files")
        checkpoint_id = preview["checkpoint_id"]
        final_dir = self.root / str(source["active_baseline_id"]) / str(source["job_id"]) / str(source["task_id"]) / checkpoint_id
        manifest_file = final_dir / "manifest.json"
        if final_dir.exists():
            existing = self.load(manifest_file)
            metadata = dict(existing.get("reconstruction_metadata") or {})
            if metadata.get("request_id") != request_id:
                raise AttemptCheckpointError("CHECKPOINT_RECONSTRUCTION_IDEMPOTENCY_CONFLICT")
            return {**preview, "manifest_path": str(manifest_file), "manifest_sha256": _sha(manifest_file.read_bytes()), "replayed": True}
        renames = [
            item for item in source.get("renames") or []
            if item.get("from") in preview["authorized_scope"] and item.get("to") in preview["authorized_scope"]
        ]
        manifest = dict(source)
        manifest.update({
            "schema_version": SCHEMA_VERSION,
            "kind": "SCOPED_DERIVED_CHECKPOINT",
            "checkpoint_id": checkpoint_id,
            "created_at": _now(),
            "task_owned_changed_files": list(preview["authorized_scope"]),
            "files": selected,
            "renames": renames,
            "unified_diff": "",
            "rollback_reason": reason,
            "reconstruction_metadata": {
                "request_id": request_id,
                "reason": reason,
                "derived_from_checkpoint_id": preview["source_checkpoint_id"],
                "source_checkpoint_manifest_sha256": preview["source_manifest_sha256"],
                "reconstruction_base_ref": preview["reconstruction_base_ref"],
                "pre_job_snapshot_manifest_sha256": preview["pre_job_snapshot_manifest_sha256"],
                "predecessor_delta_identity": preview["predecessor_delta_identity"],
                "authorized_scope": list(preview["authorized_scope"]),
                "excluded_scope": list(preview["excluded_scope"]),
                "authorized_scope_sha256": preview["authorized_scope_sha256"],
                "resulting_file_sha256": dict(preview["resulting_file_sha256"]),
                "resulting_candidate_sha256": preview["resulting_candidate_sha256"],
            },
        })
        temp_dir = final_dir.parent / f".{uuid.uuid4().hex}.tmp"
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=False, exist_ok=False)
        try:
            payload = _canonical(manifest)
            (temp_dir / "manifest.json").write_bytes(payload)
            (temp_dir / "manifest.sha256").write_text(_sha(payload), encoding="ascii")
            os.replace(temp_dir, final_dir)
            self.load(manifest_file)
        except Exception:
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        return {**preview, "manifest_path": str(manifest_file), "manifest_sha256": _sha(manifest_file.read_bytes()), "replayed": False}

    def capture(
        self,
        task,
        baseline: GitTaskBaseline,
        rollback_reason: str,
        *,
        artifact_kind: str = "PRE_ROLLBACK_ATTEMPT_CHECKPOINT",
        recovery_metadata: Mapping[str, Any] | None = None,
        allow_no_delta: bool = False,
    ) -> dict[str, Any]:
        if baseline is None:
            raise AttemptCheckpointError("CHECKPOINT_BASELINE_MISSING")
        if artifact_kind not in CHECKPOINT_KINDS:
            raise AttemptCheckpointError("CHECKPOINT_KIND_INVALID")
        paths = list(dict.fromkeys(
            task.scoped_rollback_targets or task.task_owned_changed_files or []
        ))
        if not paths and not allow_no_delta:
            return {}
        files: list[dict[str, Any]] = []
        for changed in paths:
            located = self._locate(baseline, changed)
            if located is None:
                raise AttemptCheckpointError("CHECKPOINT_PATH_UNMAPPED", changed)
            repo, relative = located
            before = self._tree_bytes(repo, repo.worktree_tree, relative)
            before_mode = self._tree_mode(repo, repo.worktree_tree, relative)
            target = self._path(changed)
            if target.exists() and (not target.is_file() or target.is_symlink()):
                raise AttemptCheckpointError("CHECKPOINT_FILE_TYPE_UNSUPPORTED", changed)
            after = target.read_bytes() if target.is_file() else None
            if before == after:
                continue
            before_blob = self._put_blob(before) if before is not None else ""
            after_blob = self._put_blob(after) if after is not None else ""
            mode = stat.S_IMODE(target.stat().st_mode) if target.is_file() else 0
            files.append({
                "path": changed, "before_state": "FILE" if before is not None else "MISSING",
                "after_state": "FILE" if after is not None else "DELETED",
                "before_sha256": _sha(before) if before is not None else "MISSING",
                "after_sha256": _sha(after) if after is not None else "DELETED",
                "before_blob": before_blob, "after_blob": after_blob,
                "untracked": before is None and after is not None,
                "binary": bool(after is not None and b"\0" in after),
                "before_eol": _eol(before), "after_eol": _eol(after),
                "before_bom": _bom(before), "after_bom": _bom(after),
                # 0.8.3 compatibility aliases describe the checkpoint post-image.
                "eol": _eol(after), "bom": _bom(after), "file_mode": mode,
                "before_mode": before_mode, "after_mode": mode,
            })
        if not files and not allow_no_delta:
            return {}
        no_delta = dict(getattr(task, "no_task_delta_evidence", {}) or {})
        if not files and (
            artifact_kind != "CRASH_SALVAGE_CHECKPOINT"
            or not no_delta.get("passed")
        ):
            raise AttemptCheckpointError("CHECKPOINT_NO_DELTA_PROOF_MISSING")
        inherited = {}
        for changed in task.inherited_batch_delta_files or []:
            target = self._path(changed)
            inherited[changed] = _sha(target.read_bytes()) if target.is_file() else "DELETED"
        seed = {
            "generation": int(getattr(task, "queue_generation", 0)),
            "baseline": getattr(task, "active_baseline_id", ""),
            "job": getattr(task, "job_id", ""), "task": task.task_id,
            "internal_attempt": int(task.retry_count) + 1,
            "files": [(item["path"], item["after_sha256"]) for item in files],
            "kind": artifact_kind,
        }
        checkpoint_id = "CHK-" + hashlib.sha256(_canonical(seed)).hexdigest()[:24].upper()
        final_dir = self.root / str(seed["baseline"]) / str(seed["job"]) / task.task_id / checkpoint_id
        if final_dir.exists():
            manifest_file = final_dir / "manifest.json"
            existing = self.load(manifest_file)
            return {
                "checkpoint_id": existing["checkpoint_id"],
                "manifest_path": str(manifest_file),
                "manifest_sha256": _sha(manifest_file.read_bytes()),
                "files": list(existing["task_owned_changed_files"]),
                "kind": str(existing.get("kind", artifact_kind)),
            }
        temp_dir = final_dir.parent / f".{uuid.uuid4().hex}.tmp"
        deleted_by_sha = {
            item["before_sha256"]: item["path"] for item in files
            if item["before_state"] == "FILE" and item["after_state"] == "DELETED"
        }
        renames = [
            {"from": deleted_by_sha[item["after_sha256"]], "to": item["path"]}
            for item in files
            if item["before_state"] == "MISSING"
            and item["after_state"] == "FILE"
            and item["after_sha256"] in deleted_by_sha
        ]
        manifest = {
            "schema_version": SCHEMA_VERSION, "kind": artifact_kind,
            "checkpoint_id": checkpoint_id, "created_at": _now(),
            "generation": seed["generation"], "active_baseline_id": seed["baseline"],
            "batch_id": getattr(task, "batch_id", ""), "job_id": seed["job"],
            "client_job_id": getattr(task, "client_job_id", ""), "task_id": task.task_id,
            "outer_attempt": int(getattr(task, "outer_attempt", 0)),
            "internal_attempt": int(task.retry_count) + 1,
            "pre_job_snapshot_id": task.pre_job_snapshot_id,
            "pre_job_snapshot_manifest": getattr(task, "pre_job_snapshot_manifest", ""),
            "worker": task.worker, "model": task.retry_model or task.codex_model or task.droid_model,
            "planned_worker": getattr(task, "planned_worker", ""),
            "planned_model": getattr(task, "planned_model", ""),
            "actual_worker": getattr(task, "actual_worker", ""),
            "actual_model": getattr(task, "actual_model", ""),
            "retry_strategy": task.retry_strategy,
            "task_owned_changed_files": [item["path"] for item in files], "files": files,
            "renames": renames,
            "unified_diff": task.git_diff, "inherited_batch_delta_files": inherited,
            "unexpected_external_dirty_files": list(task.unexpected_external_dirty_files),
            "external_frozen_integrity": bool(getattr(task, "external_frozen_integrity", True)),
            "baseline_declaration_integrity": bool(getattr(task, "baseline_declaration_integrity", True)),
            "build": dict(task.build), "test": dict(task.test),
            "review_status": task.review_status, "verification_status": task.verification_status,
            "review_findings": task.review_result, "failure_code": task.failure_code,
            "failure_fingerprint": task.failure_fingerprint, "rollback_reason": rollback_reason,
            "no_task_delta_evidence": no_delta,
            "recovery_metadata": dict(recovery_metadata or {}),
        }
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=False, exist_ok=False)
        try:
            payload = _canonical(manifest)
            (temp_dir / "manifest.json").write_bytes(payload)
            (temp_dir / "manifest.sha256").write_text(_sha(payload), encoding="ascii")
            if _sha((temp_dir / "manifest.json").read_bytes()) != (temp_dir / "manifest.sha256").read_text(encoding="ascii"):
                raise AttemptCheckpointError("CHECKPOINT_MANIFEST_HASH_MISMATCH")
            os.replace(temp_dir, final_dir)
            published = self.load(final_dir / "manifest.json")
            if published.get("checkpoint_id") != checkpoint_id:
                raise AttemptCheckpointError("CHECKPOINT_PUBLISH_VERIFICATION_FAILED")
        except Exception:
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        manifest_file = final_dir / "manifest.json"
        return {
            "checkpoint_id": checkpoint_id,
            "manifest_path": str(manifest_file),
            "manifest_sha256": _sha(manifest_file.read_bytes()),
            "files": [item["path"] for item in files],
            "kind": artifact_kind,
        }

    def load(self, manifest_path: str | Path) -> dict[str, Any]:
        path = Path(manifest_path).resolve()
        try:
            path.relative_to(self.root)
            payload = path.read_bytes()
            expected = (path.parent / "manifest.sha256").read_text(encoding="ascii").strip()
        except (ValueError, OSError) as exc:
            raise AttemptCheckpointError("CHECKPOINT_MANIFEST_MISSING") from exc
        if _sha(payload) != expected:
            raise AttemptCheckpointError("CHECKPOINT_MANIFEST_HASH_MISMATCH")
        data = json.loads(payload.decode("utf-8"))
        self._validate_manifest(data)
        for item in data["files"]:
            for key in ("before_blob", "after_blob"):
                if item.get(key):
                    self._blob(str(item[key]))
        return data

    def _validate_manifest(self, data: Mapping[str, Any]) -> None:
        version = data.get("schema_version")
        kind = data.get("kind")
        if version not in SUPPORTED_SCHEMA_VERSIONS or kind not in CHECKPOINT_KINDS:
            raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INVALID")
        files = data.get("files")
        allow_empty = (
            kind == "CRASH_SALVAGE_CHECKPOINT"
            and bool(dict(data.get("no_task_delta_evidence") or {}).get("passed"))
        )
        if not isinstance(files, list) or (not files and not allow_empty):
            raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INCOMPLETE")
        paths: set[str] = set()
        for item in files:
            if not isinstance(item, dict):
                raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INCOMPLETE")
            required = {
                "path", "before_state", "after_state", "before_sha256",
                "after_sha256",
            }
            if version == 2:
                required.update({"before_mode", "after_mode"})
            if not required.issubset(item):
                raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INCOMPLETE")
            path = str(item.get("path", ""))
            if not path or path in paths:
                raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INCOMPLETE", path)
            paths.add(path)
            if version == 1:
                item.setdefault("before_mode", 0)
                item.setdefault("after_mode", int(item.get("file_mode", 0)))
            for state_key, sha_key, blob_key in (
                ("before_state", "before_sha256", "before_blob"),
                ("after_state", "after_sha256", "after_blob"),
            ):
                is_file = item.get(state_key) == "FILE"
                digest = str(item.get(blob_key, ""))
                if is_file != bool(digest):
                    raise AttemptCheckpointError("CHECKPOINT_MANIFEST_INCOMPLETE", path)
                if is_file and str(item.get(sha_key, "")) != digest:
                    raise AttemptCheckpointError("CHECKPOINT_MANIFEST_HASH_MISMATCH", path)

    @staticmethod
    def _state(target: Path) -> tuple[bytes | None, int]:
        if target.exists() and (not target.is_file() or target.is_symlink()):
            raise AttemptCheckpointError("CHECKPOINT_FILE_TYPE_UNSUPPORTED", str(target))
        if not target.is_file():
            return None, 0
        return target.read_bytes(), stat.S_IMODE(target.stat().st_mode)

    @staticmethod
    def _same_text_ignoring_eol(actual: bytes, expected: bytes) -> bool:
        """Accept only byte-identical content or a pure CRLF/LF representation change."""
        try:
            if b"\0" in actual or b"\0" in expected:
                return False
            return actual.replace(b"\r\n", b"\n") == expected.replace(b"\r\n", b"\n")
        except Exception:
            return False

    def validate_corrective_seed(
        self,
        manifest_path: str | Path,
        *,
        parent_job_id: str,
        parent_client_job_id: str,
        generation: int,
        active_baseline_id: str,
        allowed_resources: Sequence[str],
        integrity_snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Read-only validation for an explicitly authorized cross-Job corrective seed."""
        data = self.load(manifest_path)
        checks = (
            (data.get("job_id") == parent_job_id, "CHECKPOINT_JOB_MISMATCH"),
            (data.get("client_job_id") == parent_client_job_id, "CHECKPOINT_CLIENT_JOB_MISMATCH"),
            (int(data.get("generation", 0)) == int(generation), "CHECKPOINT_GENERATION_MISMATCH"),
            (data.get("active_baseline_id") == active_baseline_id, "CHECKPOINT_BASELINE_MISMATCH"),
            (integrity_snapshot.get("external_frozen_integrity") is True, "CHECKPOINT_EXTERNAL_INTEGRITY_FAILED"),
            (integrity_snapshot.get("baseline_declaration_integrity") is True, "CHECKPOINT_BASELINE_INTEGRITY_FAILED"),
            (not list(integrity_snapshot.get("unexpected_runtime_dirty_files") or []), "CHECKPOINT_UNEXPECTED_DIRTY"),
        )
        for passed, code in checks:
            if not passed:
                raise AttemptCheckpointError(code)
        checkpoint_files = {
            str(item.get("path", "")): item
            for item in data.get("files") or []
            if isinstance(item, Mapping) and item.get("path")
        }
        # Older checkpoint capture can list a task-owned path as inherited
        # with its candidate post-state hash.  A cross-Job corrective may use
        # that overlap only when both records describe the same post-state.
        for relative, expected in dict(
            data.get("inherited_batch_delta_files") or {}
        ).items():
            item = checkpoint_files.get(str(relative))
            if item is not None and str(item.get("after_sha256", "")) != str(expected):
                raise AttemptCheckpointError(
                    "CHECKPOINT_INHERITED_TASK_DELTA_MISMATCH", str(relative)
                )
        allowed = {str(path).replace("\\", "/") for path in allowed_resources}
        changed = {str(path).replace("\\", "/") for path in data.get("task_owned_changed_files") or []}
        if not changed or not changed.issubset(allowed):
            raise AttemptCheckpointError("CHECKPOINT_SCOPE_MISMATCH")
        eol_only: list[str] = []
        for item in data.get("files") or []:
            relative = str(item["path"])
            target = self._path(relative)
            actual = target.read_bytes() if target.is_file() else None
            actual_sha = _sha(actual) if actual is not None else "MISSING"
            if actual_sha == item["before_sha256"]:
                continue
            before = self._blob(str(item["before_blob"])) if item.get("before_blob") else None
            if actual is not None and before is not None and self._same_text_ignoring_eol(actual, before):
                eol_only.append(relative)
                continue
            raise AttemptCheckpointError("CHECKPOINT_RESTORE_CONFLICT", relative)
        return {
            "valid": True,
            "manifest_path": str(Path(manifest_path).resolve()),
            "manifest_sha256": _sha(Path(manifest_path).resolve().read_bytes()),
            "checkpoint_id": data["checkpoint_id"],
            "parent_job_id": parent_job_id,
            "parent_client_job_id": parent_client_job_id,
            "generation": int(generation),
            "active_baseline_id": active_baseline_id,
            "changed_files": sorted(changed),
            "eol_only_prestate_files": sorted(eol_only),
        }

    @staticmethod
    def _apply_state(target: Path, payload: bytes | None, mode: int) -> None:
        if payload is None:
            if target.exists() or target.is_symlink():
                if target.is_dir() and not target.is_symlink():
                    raise AttemptCheckpointError("CHECKPOINT_FILE_TYPE_UNSUPPORTED", str(target))
                target.unlink()
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{target.name}.restore-", dir=str(target.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            if mode:
                os.chmod(name, mode)
            os.replace(name, target)
        finally:
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass

    def restore_for_retry(
        self,
        task,
        manifest_path: str | Path,
        *,
        source_job_id: str = "",
        source_client_job_id: str = "",
    ) -> dict[str, Any]:
        data = self.load(manifest_path)
        expected_job_id = source_job_id or task.job_id
        expected_client_job_id = source_client_job_id or task.client_job_id
        checks = (
            (int(data.get("generation", 0)) == int(task.queue_generation), "CHECKPOINT_GENERATION_MISMATCH"),
            (data.get("active_baseline_id") == task.active_baseline_id, "CHECKPOINT_BASELINE_MISMATCH"),
            (data.get("job_id") == expected_job_id, "CHECKPOINT_JOB_MISMATCH"),
            (data.get("client_job_id") == expected_client_job_id, "CHECKPOINT_CLIENT_JOB_MISMATCH"),
            (bool(getattr(task, "external_frozen_integrity", False)), "CHECKPOINT_EXTERNAL_INTEGRITY_FAILED"),
            (bool(getattr(task, "baseline_declaration_integrity", False)), "CHECKPOINT_BASELINE_INTEGRITY_FAILED"),
            (not task.unexpected_external_dirty_files, "CHECKPOINT_UNEXPECTED_DIRTY"),
        )
        for passed, code in checks:
            if not passed:
                raise AttemptCheckpointError(code)
        task_owned = {
            str(path) for path in data.get("task_owned_changed_files") or []
        }
        cross_job_restore = bool(source_job_id or source_client_job_id)
        for changed, expected in dict(data.get("inherited_batch_delta_files") or {}).items():
            # The current file is deliberately still at checkpoint pre-state;
            # defer only an exact task-owned overlap to post-apply validation.
            if cross_job_restore and str(changed) in task_owned:
                continue
            target = self._path(changed)
            actual = _sha(target.read_bytes()) if target.is_file() else "DELETED"
            if actual != expected:
                raise AttemptCheckpointError("CHECKPOINT_INHERITED_DELTA_MISMATCH", changed)
        pending: list[tuple[Path, bytes | None, int, str]] = []
        for item in data.get("files") or []:
            target = self._path(str(item["path"]))
            actual = _sha(target.read_bytes()) if target.is_file() else "MISSING"
            if actual != item["before_sha256"]:
                before = self._blob(str(item["before_blob"])) if item.get("before_blob") else None
                current = target.read_bytes() if target.is_file() else None
                if current is None or before is None or not self._same_text_ignoring_eol(current, before):
                    raise AttemptCheckpointError("CHECKPOINT_RESTORE_CONFLICT", str(item["path"]))
            after = self._blob(str(item["after_blob"])) if item.get("after_blob") else None
            pending.append((target, after, int(item.get("after_mode", item.get("file_mode", 0))), str(item["after_sha256"])))
        originals = {target: self._state(target) for target, _, _, _ in pending}
        applied: list[Path] = []
        try:
            for target, after, mode, expected_after in pending:
                self._apply_state(target, after, mode)
                applied.append(target)
                actual_after, actual_mode = self._state(target)
                digest = _sha(actual_after) if actual_after is not None else "DELETED"
                if digest != expected_after or (actual_after is not None and mode and actual_mode != mode):
                    raise AttemptCheckpointError("CHECKPOINT_RESTORE_VERIFY_FAILED", str(target))
            for changed, expected in dict(data.get("inherited_batch_delta_files") or {}).items():
                target = self._path(changed)
                actual = _sha(target.read_bytes()) if target.is_file() else "DELETED"
                if actual != expected:
                    raise AttemptCheckpointError("CHECKPOINT_INHERITED_DELTA_MISMATCH", changed)
        except Exception as exc:
            recovery_errors: list[str] = []
            for target in reversed(applied):
                payload, mode = originals[target]
                try:
                    self._apply_state(target, payload, mode)
                    restored, restored_mode = self._state(target)
                    if restored != payload or (payload is not None and mode and restored_mode != mode):
                        recovery_errors.append(str(target))
                except Exception:
                    recovery_errors.append(str(target))
            if recovery_errors:
                raise AttemptCheckpointError(
                    "CHECKPOINT_RESTORE_TRANSACTION_FAILED", ",".join(recovery_errors)
                ) from exc
            if isinstance(exc, AttemptCheckpointError):
                raise
            raise AttemptCheckpointError(
                "CHECKPOINT_RESTORE_APPLY_FAILED", type(exc).__name__
            ) from exc
        return {"checkpoint_id": data["checkpoint_id"], "files": list(data["task_owned_changed_files"]), "review_findings": data.get("review_findings", ""), "manifest": data}
