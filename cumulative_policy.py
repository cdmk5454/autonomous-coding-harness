"""Persistent cumulative Batch baseline and pre-Job snapshot support."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Sequence

from control_paths import ControlPathError, ensure_safe_state_directory
from git_collector import GitCollector, GitRepoBaseline, GitTaskBaseline
from project_profile import (
    COMMIT_POLICY_BATCH_FINAL,
    BaselineFileProfile,
    ProjectProfile,
    normalize_changed_path,
)
from runtime_safety import git_subprocess_env, scrub_secrets, trusted_executable


MANIFEST_SCHEMA_VERSION = 1
EXTERNAL_ADOPTION_KIND = "USER_ACCEPTED_EXTERNAL_DELTA"
EXTERNAL_ADOPTION_ORIGIN = "EXTERNAL"
EXTERNAL_ADOPTION_APPROVAL = "USER_APPROVED"
EXTERNAL_ADOPTION_CLASSES = {
    "EXTERNAL_PRODUCT_CHANGE_COHERENT",
    "PRODUCT_EXECUTION_SURFACE",
    "LOCAL_ENVIRONMENT_SURFACE",
    "DOCUMENTATION_SURFACE",
    "VERIFIED_PRESERVED_PRODUCT_DELTA",
    "INHERITED_EXTERNAL_FROZEN",
    "INHERITED_PRESERVED_PRODUCT_DELTA",
}
EXTERNAL_ADOPTION_ROLES = {"TASK_OWNED", "EXTERNAL_FROZEN"}


class CumulativePolicyError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = scrub_secrets(detail)[:1000]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(data: Mapping[str, Any]) -> bytes:
    return json.dumps(
        data, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _atomic_json_write(path: Path, data: Mapping[str, Any], root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise CumulativePolicyError("CUMULATIVE_STATE_PATH_ESCAPE") from exc
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


class CumulativePolicyManager:
    """Owns policy evidence outside project repositories.

    Git trees remain the fast rollback primitive.  Every pre-Job snapshot also
    stores an external binary patch plus actual bytes for the dirty/target files,
    so restart recovery can validate the tree and recover without a broad reset.
    """

    def __init__(
        self,
        workspace: str | Path,
        control_root: str | Path,
        profile: ProjectProfile,
        *,
        read_only: bool = False,
    ):
        self.workspace = Path(workspace).resolve()
        self.control_root = Path(control_root).resolve()
        self.root = self.control_root / "cumulative"
        self.profile = profile
        self.policy = profile.control_policy
        self.read_only = bool(read_only)
        if not self.read_only:
            try:
                ensure_safe_state_directory(
                    self.control_root,
                    self.root,
                    field="cumulative_state",
                    create=True,
                )
            except ControlPathError as exc:
                raise CumulativePolicyError(exc.code, exc.field) from exc

    @property
    def enabled(self) -> bool:
        return self.policy.commit_policy == COMMIT_POLICY_BATCH_FINAL

    def declared_task_owned(self) -> tuple[str, ...]:
        return tuple(item.path for item in self.policy.task_owned_baseline_files)

    def declared_external_frozen(self) -> tuple[str, ...]:
        return tuple(item.path for item in self.policy.external_frozen_baseline_files)

    def base_public_fields(self) -> dict[str, Any]:
        return {
            "commit_policy": self.policy.commit_policy,
            "cumulative_worktree": bool(self.policy.cumulative_worktree),
            "auto_continue_without_commit": bool(
                self.policy.auto_continue_without_commit
            ),
            "batch_owned_delta_supported": bool(
                self.policy.cumulative_worktree
            ),
            "pre_job_snapshot_supported": bool(
                self.policy.pre_job_snapshot_supported
            ),
            "scoped_rollback_supported": bool(
                self.policy.scoped_rollback_supported
            ),
            "final_commit_required": bool(self.policy.final_commit_required),
            "task_owned_baseline_files": list(self.declared_task_owned()),
            "external_frozen_baseline_files": list(
                self.declared_external_frozen()
            ),
        }

    def _run_git(
        self, repo: Path, args: Sequence[str], *, binary: bool = False
    ) -> tuple[int, bytes | str, bytes | str]:
        try:
            result = subprocess.run(
                [trusted_executable("git", forbidden_root=self.workspace), *args],
                cwd=str(repo),
                capture_output=True,
                text=not binary,
                encoding=None if binary else "utf-8",
                errors=None if binary else "replace",
                env=git_subprocess_env(),
            )
        except Exception as exc:
            return -1, b"" if binary else "", scrub_secrets(str(exc))
        return result.returncode, result.stdout, result.stderr

    def _repo_for_path(self, workspace_path: str) -> tuple[Path, str, str]:
        normalized = normalize_changed_path(workspace_path)
        module = self.profile.module_for_path(normalized)
        if module is None:
            raise CumulativePolicyError("CUMULATIVE_PATH_UNMAPPED", normalized)
        prefix = module.path.rstrip("/")
        relative = normalized[len(prefix) :].lstrip("/")
        if not relative:
            raise CumulativePolicyError("CUMULATIVE_PATH_INVALID", normalized)
        repo = (self.workspace / Path(*PurePosixPath(prefix).parts)).resolve()
        try:
            repo.relative_to(self.workspace)
        except ValueError as exc:
            raise CumulativePolicyError("CUMULATIVE_PATH_ESCAPE", normalized) from exc
        return repo, prefix, relative

    def _actual_file(self, path: str) -> dict[str, str]:
        repo, _, relative = self._repo_for_path(path)
        file_path = (repo / Path(*PurePosixPath(relative).parts)).resolve()
        try:
            file_path.relative_to(repo)
        except ValueError as exc:
            raise CumulativePolicyError("CUMULATIVE_PATH_ESCAPE", path) from exc
        if not file_path.exists():
            return {"state": "DELETED", "sha256": "DELETED"}
        if not file_path.is_file() or file_path.is_symlink():
            raise CumulativePolicyError("CUMULATIVE_FILE_TYPE_UNSUPPORTED", path)
        try:
            return {"state": "PRESENT", "sha256": _sha256_bytes(file_path.read_bytes())}
        except OSError as exc:
            raise CumulativePolicyError("CUMULATIVE_FILE_UNREADABLE", path) from exc

    def _verify_declaration(self, item: BaselineFileProfile) -> dict[str, str]:
        actual = self._actual_file(item.path)
        if actual["sha256"].casefold() != item.sha256.casefold():
            raise CumulativePolicyError("BASELINE_HASH_MISMATCH", item.path)
        return actual

    @staticmethod
    def _split_z(output: str) -> list[str]:
        return [item for item in output.split("\0") if item]

    def _tracked_path_semantically_dirty(
        self, repo: Path, relative: str
    ) -> bool:
        """Compare HEAD/index/hash-object blobs before trusting porcelain dirtiness."""
        code, head_record, _ = self._run_git(
            repo, ["ls-tree", "-z", "HEAD", "--", relative]
        )
        head_blob = ""
        if code == 0 and str(head_record):
            metadata = str(head_record).split("\t", 1)[0].split()
            if len(metadata) >= 3 and metadata[1] == "blob":
                head_blob = metadata[2]

        code, index_record, _ = self._run_git(
            repo, ["ls-files", "--stage", "-z", "--", relative]
        )
        index_blob = ""
        if code == 0 and str(index_record):
            metadata = str(index_record).split("\t", 1)[0].split()
            if len(metadata) >= 3 and metadata[2] == "0":
                index_blob = metadata[1]

        code, worktree_blob, _ = self._run_git(
            repo, ["hash-object", f"--path={relative}", "--", relative]
        )
        normalized_blob = str(worktree_blob).strip() if code == 0 else ""
        if (
            head_blob
            and head_blob == index_blob
            and head_blob == normalized_blob
        ):
            return False

        code, _, _ = self._run_git(
            repo, ["diff", "--quiet", "HEAD", "--", relative]
        )
        if code == 0:
            return False
        if code == 1:
            return True
        raise CumulativePolicyError(
            "CUMULATIVE_DIRTY_NORMALIZATION_FAILED", relative
        )

    def _repo_inventory(self, repo: Path, module: str) -> dict[str, Any]:
        code, head, err = self._run_git(repo, ["rev-parse", "--verify", "HEAD"])
        if code != 0:
            raise CumulativePolicyError(
                "CUMULATIVE_REPO_HEAD_UNAVAILABLE", module
            )
        code, tracked, err = self._run_git(repo, ["ls-files", "-z"])
        if code != 0:
            raise CumulativePolicyError("CUMULATIVE_TRACKED_LIST_FAILED", module)
        code, untracked, err = self._run_git(
            repo, ["ls-files", "--others", "--exclude-standard", "-z"]
        )
        if code != 0:
            raise CumulativePolicyError("CUMULATIVE_UNTRACKED_LIST_FAILED", module)
        code, status, err = self._run_git(
            repo,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--no-renames"],
        )
        if code != 0:
            raise CumulativePolicyError("CUMULATIVE_DIRTY_LIST_FAILED", module)
        dirty: list[dict[str, str]] = []
        for entry in self._split_z(str(status)):
            if len(entry) < 4:
                raise CumulativePolicyError("CUMULATIVE_STATUS_INVALID", module)
            relative = entry[3:]
            porcelain = entry[:2]
            if porcelain != "??" and not self._tracked_path_semantically_dirty(
                repo, relative
            ):
                continue
            dirty.append({"status": porcelain, "path": f"{module}/{relative}"})
        return {
            "module": module,
            "repo": str(repo),
            "head": str(head).strip(),
            "tracked_files": [f"{module}/{item}" for item in self._split_z(str(tracked))],
            "untracked_files": [
                f"{module}/{item}" for item in self._split_z(str(untracked))
            ],
            "dirty_files": dirty,
        }

    def _inventories(self) -> list[dict[str, Any]]:
        inventories: list[dict[str, Any]] = []
        for module in self.profile.modules:
            repo = (self.workspace / Path(*PurePosixPath(module.path).parts)).resolve()
            if not (repo / ".git").exists():
                continue
            inventories.append(self._repo_inventory(repo, module.path))
        if not inventories:
            raise CumulativePolicyError("CUMULATIVE_REPOSITORY_MISSING")
        return inventories

    @staticmethod
    def _dirty_paths(inventories: Iterable[Mapping[str, Any]]) -> set[str]:
        return {
            str(item.get("path", ""))
            for repo in inventories
            for item in (repo.get("dirty_files") or [])
            if item.get("path")
        }

    def _copy_file(self, source_path: str, files_root: Path) -> dict[str, Any]:
        actual = self._actual_file(source_path)
        result: dict[str, Any] = {"path": source_path, **actual}
        if actual["state"] == "DELETED":
            result["snapshot"] = ""
            return result
        repo, _, relative = self._repo_for_path(source_path)
        source = (repo / Path(*PurePosixPath(relative).parts)).resolve()
        digest = hashlib.sha256(source_path.encode("utf-8")).hexdigest()
        destination = files_root / digest[:2] / digest
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        result["snapshot"] = destination.relative_to(files_root.parent).as_posix()
        result["size"] = destination.stat().st_size
        return result

    def _write_patch(
        self,
        repo: GitRepoBaseline,
        destination: Path,
    ) -> dict[str, Any]:
        code, patch, err = self._run_git(
            repo.git_dir,
            ["diff", "--binary", "--full-index", repo.head_oid, repo.worktree_tree],
            binary=True,
        )
        if code != 0:
            raise CumulativePolicyError(
                "CUMULATIVE_BINARY_PATCH_FAILED", repo.module or repo.git_dir.name
            )
        payload = bytes(patch)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "path": destination.name,
            "sha256": _sha256_bytes(payload),
            "size": len(payload),
        }

    def _snapshot_repos(
        self, baseline: GitTaskBaseline, destination: Path
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for repo in baseline.repos:
            module = repo.module or repo.git_dir.name
            inventory = self._repo_inventory(repo.git_dir, module)
            patch_name = hashlib.sha256(module.encode("utf-8")).hexdigest() + ".patch"
            patch = self._write_patch(repo, destination / "patches" / patch_name)
            result.append(
                {
                    **inventory,
                    "worktree_tree": repo.worktree_tree,
                    "index_tree": repo.index_tree,
                    "binary_patch": {
                        **patch,
                        "path": f"patches/{patch_name}",
                    },
                }
            )
        return result

    def _manifest_path(self, baseline_id: str) -> Path:
        if not baseline_id or any(char not in "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for char in baseline_id):
            raise CumulativePolicyError("CUMULATIVE_BASELINE_ID_INVALID")
        return self.root / "batch-baselines" / baseline_id / "manifest.json"

    @staticmethod
    def _adoption_record(manifest: Mapping[str, Any]) -> dict[str, Any]:
        adoption = dict(manifest.get("external_delta_adoption") or {})
        if not adoption:
            return {}
        fingerprint = str(adoption.pop("adoption_fingerprint", ""))
        if (
            adoption.get("origin") != EXTERNAL_ADOPTION_ORIGIN
            or adoption.get("adoption") != EXTERNAL_ADOPTION_APPROVAL
            or adoption.get("classification") != EXTERNAL_ADOPTION_KIND
            or not fingerprint
            or fingerprint != _sha256_bytes(_canonical_json(adoption))
        ):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        adoption["adoption_fingerprint"] = fingerprint
        return adoption

    def _adopted_declarations(
        self, manifest: Mapping[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        adoption = self._adoption_record(manifest)
        if not adoption:
            return [], [], {}
        overlay = list(adoption.get("dirty_overlay") or [])
        if not overlay or any(not isinstance(item, Mapping) for item in overlay):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        paths = [str(item.get("path", "")) for item in overlay]
        if len(paths) != len(set(paths)) or any(not path for path in paths):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        if any(
            str(item.get("baseline_role", "")) not in EXTERNAL_ADOPTION_ROLES
            or str(item.get("classification", "")) not in EXTERNAL_ADOPTION_CLASSES
            or not str(item.get("sha256", ""))
            for item in overlay
        ):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        files = {str(item.get("path", "")): dict(item) for item in manifest.get("files") or []}
        if set(files) != set(paths) or any(
            files[path].get("sha256") != item.get("sha256")
            for path, item in ((str(row["path"]), row) for row in overlay)
        ):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        task_owned = [dict(item) for item in overlay if item["baseline_role"] == "TASK_OWNED"]
        external = [dict(item) for item in overlay if item["baseline_role"] == "EXTERNAL_FROZEN"]
        declared_task = {
            (str(item.get("path", "")), str(item.get("sha256", "")).casefold())
            for item in manifest.get("task_owned_baseline_files") or []
        }
        declared_external = {
            (str(item.get("path", "")), str(item.get("sha256", "")).casefold())
            for item in manifest.get("external_frozen_baseline_files") or []
        }
        expected_task = {(item["path"], str(item["sha256"]).casefold()) for item in task_owned}
        expected_external = {(item["path"], str(item["sha256"]).casefold()) for item in external}
        if declared_task != expected_task or declared_external != expected_external:
            raise CumulativePolicyError("EXTERNAL_ADOPTION_MANIFEST_INVALID")
        return task_owned, external, adoption

    def adopted_external_hashes(
        self, baseline_id: str, *, repository_paths: Iterable[str] = ()
    ) -> dict[str, str]:
        if not baseline_id:
            return {}
        manifest = self.load_batch_manifest(baseline_id)
        _, _, adoption = self._adopted_declarations(manifest)
        accepted = {
            str(item["path"]): str(item["sha256"])
            for item in adoption.get("dirty_overlay") or []
        }
        if not adoption or not repository_paths:
            return accepted
        inventories = self._inventories()
        current_heads = {str(item["module"]): str(item["head"]) for item in inventories}
        if current_heads != dict(adoption.get("repository_heads") or {}):
            return accepted
        dirty = self._dirty_paths(inventories)
        tracked = {
            str(path)
            for inventory in inventories
            for path in inventory.get("tracked_files") or []
        }
        for raw in repository_paths:
            path = normalize_changed_path(str(raw))
            if path in tracked and path not in dirty:
                actual = self._actual_file(path)
                if actual["state"] == "PRESENT":
                    accepted[path] = actual["sha256"]
        return accepted

    def prepare_external_baseline_adoption(
        self,
        generation: int,
        previous_baseline_id: str,
        source_manifest_path: str | Path,
    ) -> dict[str, Any]:
        """Capture exact user-approved external bytes without rewriting source."""
        if not self.enabled:
            raise CumulativePolicyError("CUMULATIVE_POLICY_REQUIRED")
        if self.read_only:
            raise CumulativePolicyError("CUMULATIVE_STATE_READ_ONLY")
        source_path = Path(source_manifest_path).resolve()
        try:
            source_payload = source_path.read_bytes()
            spec = json.loads(source_payload.decode("utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CumulativePolicyError("EXTERNAL_ADOPTION_SOURCE_INVALID") from exc
        if (
            not isinstance(spec, dict)
            or spec.get("schema_version") != 1
            or spec.get("kind") != EXTERNAL_ADOPTION_KIND
            or spec.get("origin") != EXTERNAL_ADOPTION_ORIGIN
            or spec.get("adoption") != EXTERNAL_ADOPTION_APPROVAL
            or int(spec.get("generation", -1)) != int(generation)
            or str(spec.get("previous_baseline_id", "")) != str(previous_baseline_id)
        ):
            raise CumulativePolicyError("EXTERNAL_ADOPTION_SOURCE_INVALID")
        raw_heads = spec.get("repository_heads")
        raw_overlay = spec.get("dirty_overlay")
        if not isinstance(raw_heads, dict) or not isinstance(raw_overlay, list) or not raw_overlay:
            raise CumulativePolicyError("EXTERNAL_ADOPTION_SOURCE_INVALID")

        inventories = self._inventories()
        current_heads = {str(item["module"]): str(item["head"]) for item in inventories}
        expected_heads = {str(key): str(value) for key, value in raw_heads.items()}
        if expected_heads != current_heads:
            raise CumulativePolicyError("EXTERNAL_ADOPTION_HEAD_MISMATCH")

        overlay: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in raw_overlay:
            if not isinstance(raw, Mapping):
                raise CumulativePolicyError("EXTERNAL_ADOPTION_SOURCE_INVALID")
            path = normalize_changed_path(str(raw.get("path", "")))
            role = str(raw.get("baseline_role", ""))
            classification = str(raw.get("classification", ""))
            expected_hash = str(raw.get("sha256", "")).casefold()
            evidence = scrub_secrets(str(raw.get("evidence", "")))[:1000]
            if (
                not path or path in seen or role not in EXTERNAL_ADOPTION_ROLES
                or classification not in EXTERNAL_ADOPTION_CLASSES
                or not evidence
            ):
                raise CumulativePolicyError("EXTERNAL_ADOPTION_SOURCE_INVALID")
            actual = self._actual_file(path)
            if actual["state"] != "PRESENT" or actual["sha256"].casefold() != expected_hash:
                raise CumulativePolicyError("EXTERNAL_ADOPTION_OVERLAY_HASH_MISMATCH", path)
            seen.add(path)
            overlay.append({
                "path": path,
                "sha256": expected_hash,
                "state": "PRESENT",
                "baseline_role": role,
                "classification": classification,
                "origin": EXTERNAL_ADOPTION_ORIGIN,
                "adoption": EXTERNAL_ADOPTION_APPROVAL,
                "evidence": evidence,
            })
        actual_dirty = self._dirty_paths(inventories)
        if actual_dirty != seen:
            detail = sorted(actual_dirty.symmetric_difference(seen))
            raise CumulativePolicyError("EXTERNAL_ADOPTION_DIRTY_SET_MISMATCH", ",".join(detail[:20]))

        baseline, error = GitCollector(str(self.workspace), profile=self.profile).capture_baseline()
        if baseline is None:
            raise CumulativePolicyError("CUMULATIVE_BASELINE_CAPTURE_FAILED", error)
        embedded = {
            "schema_version": 1,
            "origin": EXTERNAL_ADOPTION_ORIGIN,
            "adoption": EXTERNAL_ADOPTION_APPROVAL,
            "classification": EXTERNAL_ADOPTION_KIND,
            # Truthful adoption boundary: this path attaches exact user-approved
            # external bytes as baseline input. It runs no Build/Test/Review/
            # Verification on the delta and creates no publication evidence;
            # validated Direct publication is a separate (deferred) contract.
            "validation_boundary": "BASELINE_ADOPTION_ONLY",
            # The delta was produced outside this harness; no generation
            # provenance was captured, and none is fabricated here.
            "generation_provenance": "UNKNOWN",
            "previous_baseline_id": str(previous_baseline_id),
            "source_manifest_path": str(source_path),
            "source_manifest_sha256": _sha256_bytes(source_payload),
            "repository_heads": expected_heads,
            "dirty_overlay": overlay,
            "surface_classifications": dict(spec.get("surface_classifications") or {}),
            "external_product_delta": list(spec.get("external_product_delta") or []),
            "approved_at": str(spec.get("approved_at", "")),
        }
        embedded["adoption_fingerprint"] = _sha256_bytes(_canonical_json(embedded))
        seed = {
            "generation": int(generation),
            "profile_id": self.profile.id,
            "adoption_fingerprint": embedded["adoption_fingerprint"],
            "heads": expected_heads,
        }
        baseline_id = (
            f"BASELINE-G{int(generation)}-EXT-"
            f"{hashlib.sha256(_canonical_json(seed)).hexdigest()[:16].upper()}"
        )
        final_dir = self._manifest_path(baseline_id).parent
        if final_dir.exists():
            manifest = self.load_batch_manifest(baseline_id)
            self._verify_manifest_files(manifest, final_dir)
            self._adopted_declarations(manifest)
            return self.public_status(baseline_id, ())

        final_dir.parent.mkdir(parents=True, exist_ok=True)
        temp_dir = final_dir.parent / f".{uuid.uuid4().hex}.tmp"
        temp_dir.mkdir(parents=False, exist_ok=False)
        try:
            copied = [self._copy_file(item["path"], temp_dir / "files") for item in overlay]
            repos = self._snapshot_repos(baseline, temp_dir)
            task_owned = [item for item in overlay if item["baseline_role"] == "TASK_OWNED"]
            external = [item for item in overlay if item["baseline_role"] == "EXTERNAL_FROZEN"]
            manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "kind": "BATCH_BASELINE",
                "baseline_id": baseline_id,
                "generation": int(generation),
                "profile_id": self.profile.id,
                "created_at": _now(),
                "commit_policy": COMMIT_POLICY_BATCH_FINAL,
                "task_owned_baseline_files": task_owned,
                "external_frozen_baseline_files": external,
                "initial_dirty_files": sorted(actual_dirty),
                "repos": repos,
                "files": copied,
                "external_delta_adoption": embedded,
            }
            _atomic_json_write(temp_dir / "manifest.json", manifest, temp_dir)
            os.replace(temp_dir, final_dir)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        return self.public_status(baseline_id, ())

    def activate_prepared_baseline(self, baseline_id: str, generation: int) -> None:
        manifest = self.load_batch_manifest(baseline_id)
        if int(manifest.get("generation", -1)) != int(generation):
            raise CumulativePolicyError("CUMULATIVE_BASELINE_GENERATION_MISMATCH")
        self._adopted_declarations(manifest)
        _atomic_json_write(
            self.root / "active.json",
            {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "baseline_id": baseline_id,
                "generation": int(generation),
                "manifest": str(self._manifest_path(baseline_id)),
                "updated_at": _now(),
            },
            self.root,
        )

    def prepare_batch_baseline(self, generation: int) -> dict[str, Any]:
        if not self.enabled:
            return {**self.base_public_fields(), "active_baseline_id": ""}
        if self.read_only:
            raise CumulativePolicyError("CUMULATIVE_STATE_READ_ONLY")
        task_owned = [
            {"path": item.path, "evidence": item.evidence, **self._verify_declaration(item)}
            for item in self.policy.task_owned_baseline_files
        ]
        external = [
            {"path": item.path, "evidence": item.evidence, **self._verify_declaration(item)}
            for item in self.policy.external_frozen_baseline_files
        ]
        inventories = self._inventories()
        actual_dirty = self._dirty_paths(inventories)
        declared = {item["path"] for item in task_owned + external}
        unexpected = sorted(actual_dirty - declared)
        missing_dirty = sorted(declared - actual_dirty)
        if unexpected:
            raise CumulativePolicyError(
                "UNEXPECTED_BASELINE_DIRTY_FILES", ",".join(unexpected[:20])
            )
        if missing_dirty:
            raise CumulativePolicyError(
                "BASELINE_DECLARED_FILE_NOT_DIRTY", ",".join(missing_dirty[:20])
            )

        collector = GitCollector(str(self.workspace), profile=self.profile)
        baseline, error = collector.capture_baseline()
        if baseline is None:
            raise CumulativePolicyError("CUMULATIVE_BASELINE_CAPTURE_FAILED", error)
        seed = {
            "generation": int(generation),
            "profile_id": self.profile.id,
            "task_owned": task_owned,
            "external_frozen": external,
            "heads": [repo.head_oid for repo in baseline.repos],
        }
        baseline_id = (
            f"BASELINE-G{int(generation)}-"
            f"{hashlib.sha256(_canonical_json(seed)).hexdigest()[:16].upper()}"
        )
        final_dir = self._manifest_path(baseline_id).parent
        if final_dir.exists():
            manifest = self.load_batch_manifest(baseline_id)
            self._verify_manifest_files(manifest, final_dir)
            return self.public_status(baseline_id, ())

        parent = final_dir.parent
        parent.mkdir(parents=True, exist_ok=True)
        temp_dir = parent / f".{uuid.uuid4().hex}.tmp"
        temp_dir.mkdir(parents=False, exist_ok=False)
        try:
            files_root = temp_dir / "files"
            copied = [
                self._copy_file(item["path"], files_root)
                for item in task_owned + external
            ]
            repos = self._snapshot_repos(baseline, temp_dir)
            manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "kind": "BATCH_BASELINE",
                "baseline_id": baseline_id,
                "generation": int(generation),
                "profile_id": self.profile.id,
                "created_at": _now(),
                "commit_policy": COMMIT_POLICY_BATCH_FINAL,
                "task_owned_baseline_files": task_owned,
                "external_frozen_baseline_files": external,
                "initial_dirty_files": sorted(actual_dirty),
                "repos": repos,
                "files": copied,
            }
            _atomic_json_write(temp_dir / "manifest.json", manifest, temp_dir)
            os.replace(temp_dir, final_dir)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        _atomic_json_write(
            self.root / "active.json",
            {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "baseline_id": baseline_id,
                "generation": int(generation),
                "manifest": str(self._manifest_path(baseline_id)),
                "updated_at": _now(),
            },
            self.root,
        )
        return self.public_status(baseline_id, ())

    def load_batch_manifest(self, baseline_id: str) -> dict[str, Any]:
        path = self._manifest_path(baseline_id)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise CumulativePolicyError("CUMULATIVE_BASELINE_MISSING") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise CumulativePolicyError("CUMULATIVE_BASELINE_INVALID") from exc
        if (
            data.get("schema_version") != MANIFEST_SCHEMA_VERSION
            or data.get("kind") != "BATCH_BASELINE"
            or data.get("baseline_id") != baseline_id
        ):
            raise CumulativePolicyError("CUMULATIVE_BASELINE_INVALID")
        return data

    def _verify_manifest_files(self, manifest: Mapping[str, Any], root: Path) -> None:
        for file_record in manifest.get("files") or []:
            if file_record.get("state") == "DELETED":
                continue
            relative = str(file_record.get("snapshot", ""))
            snapshot = (root / Path(*PurePosixPath(relative).parts)).resolve()
            try:
                snapshot.relative_to(root.resolve())
            except ValueError as exc:
                raise CumulativePolicyError("SNAPSHOT_PATH_ESCAPE") from exc
            try:
                digest = _sha256_bytes(snapshot.read_bytes())
            except OSError as exc:
                raise CumulativePolicyError("SNAPSHOT_CONTENT_MISSING") from exc
            if digest != file_record.get("sha256"):
                raise CumulativePolicyError("SNAPSHOT_HASH_MISMATCH", file_record.get("path", ""))
        for repo in manifest.get("repos") or []:
            patch_record = repo.get("binary_patch") or {}
            relative = str(patch_record.get("path", ""))
            patch = (root / Path(*PurePosixPath(relative).parts)).resolve()
            try:
                patch.relative_to(root.resolve())
                digest = _sha256_bytes(patch.read_bytes())
            except (ValueError, OSError) as exc:
                raise CumulativePolicyError("SNAPSHOT_PATCH_MISSING") from exc
            if digest != patch_record.get("sha256"):
                raise CumulativePolicyError("SNAPSHOT_PATCH_HASH_MISMATCH", str(repo.get("module", "")))

    def public_status(
        self,
        baseline_id: str = "",
        batch_delta_files: Iterable[str] = (),
    ) -> dict[str, Any]:
        fields = self.base_public_fields()
        fields["active_baseline_id"] = baseline_id or ""
        fields["batch_delta_files"] = sorted(
            {normalize_changed_path(path) for path in batch_delta_files if path}
        )
        fields["unexpected_runtime_dirty_files"] = []
        fields["external_frozen_integrity"] = True
        fields["baseline_declaration_integrity"] = True
        fields["repository_head_integrity"] = True
        fields["dirty_overlay_path_set_match"] = True
        fields["dirty_overlay_hash_match"] = True
        fields["final_staging_files"] = []
        fields["final_staging_excluded_files"] = list(
            self.declared_external_frozen()
        )
        if not self.enabled:
            return fields
        adopted_task: list[dict[str, Any]] = []
        adopted_external: list[dict[str, Any]] = []
        adoption: dict[str, Any] = {}
        manifest: dict[str, Any] = {}
        if baseline_id:
            manifest = self.load_batch_manifest(baseline_id)
            self._verify_manifest_files(
                manifest, self._manifest_path(baseline_id).parent
            )
            adopted_task, adopted_external, adoption = self._adopted_declarations(manifest)
            if adoption:
                fields["task_owned_baseline_files"] = [item["path"] for item in adopted_task]
                fields["external_frozen_baseline_files"] = [item["path"] for item in adopted_external]
                fields["external_delta_adoption"] = adoption
                fields["baseline_declaration_integrity"] = manifest.get("profile_id") == self.profile.id
            else:
                declared_profile = {
                    (item.path, item.sha256.casefold())
                    for item in (
                        *self.policy.task_owned_baseline_files,
                        *self.policy.external_frozen_baseline_files,
                    )
                }
                declared_manifest = {
                    (str(item.get("path", "")), str(item.get("sha256", "")).casefold())
                    for item in (
                        list(manifest.get("task_owned_baseline_files") or [])
                        + list(manifest.get("external_frozen_baseline_files") or [])
                    )
                }
                fields["baseline_declaration_integrity"] = (
                    manifest.get("profile_id") == self.profile.id
                    and declared_manifest == declared_profile
                )
        else:
            for item in (
                *self.policy.task_owned_baseline_files,
                *self.policy.external_frozen_baseline_files,
            ):
                self._verify_declaration(item)
        inventories = self._inventories()
        actual_dirty = self._dirty_paths(inventories)
        declared = set(fields["task_owned_baseline_files"]) | set(
            fields["external_frozen_baseline_files"]
        )
        batch_delta = set(fields["batch_delta_files"])
        fields["unexpected_runtime_dirty_files"] = sorted(
            actual_dirty - declared - batch_delta
        )
        frozen_integrity = True
        frozen_records = adopted_external or [
            {"path": item.path, "sha256": item.sha256}
            for item in self.policy.external_frozen_baseline_files
        ]
        for item in frozen_records:
            path = str(item["path"])
            expected = str(item["sha256"])
            if path in batch_delta and not adoption:
                continue
            if self._actual_file(path)["sha256"].casefold() != expected.casefold():
                frozen_integrity = False
        if adoption:
            manifest_heads = {
                str(item.get("module", "")): str(item.get("head", ""))
                for item in manifest.get("repos") or []
            }
            current_heads = {str(item["module"]): str(item["head"]) for item in inventories}
            fields["repository_head_integrity"] = manifest_heads == current_heads
            frozen_integrity = frozen_integrity and fields["repository_head_integrity"]
            overlay = list(adoption.get("dirty_overlay") or [])
            overlay_paths = {str(item["path"]) for item in overlay}
            fields["dirty_overlay_path_set_match"] = (
                actual_dirty == overlay_paths | batch_delta
            )
            fields["dirty_overlay_hash_match"] = all(
                str(item["path"]) in batch_delta
                or self._actual_file(str(item["path"]))["sha256"].casefold()
                   == str(item["sha256"]).casefold()
                for item in overlay
            )
            frozen_integrity = frozen_integrity and fields["dirty_overlay_path_set_match"]
        fields["external_frozen_integrity"] = frozen_integrity
        frozen = set(fields["external_frozen_baseline_files"])
        task_owned = (
            {item["path"] for item in adopted_task}
            if adoption
            else set(self.declared_task_owned())
        )
        fields["final_staging_files"] = sorted(
            (task_owned | batch_delta) - frozen
        )
        fields["final_staging_excluded_files"] = sorted(frozen)
        return fields

    def _target_snapshot_paths(
        self,
        job: Mapping[str, Any],
        inventories: Sequence[Mapping[str, Any]],
    ) -> list[str]:
        paths = self._dirty_paths(inventories)
        existing = {
            str(path)
            for repo in inventories
            for field in ("tracked_files", "untracked_files")
            for path in (repo.get(field) or [])
        }
        resources = [
            normalize_changed_path(item)
            for item in (dict(job.get("request") or {}).get("target_resources") or [])
            if item
        ]
        for resource in resources:
            if resource in existing:
                paths.add(resource)
                continue
            prefix = resource.rstrip("/") + "/"
            paths.update(path for path in existing if path.startswith(prefix))
        if len(paths) > 5000:
            raise CumulativePolicyError("SNAPSHOT_FILE_LIMIT_EXCEEDED")
        return sorted(paths)

    def prepare_job_snapshot(
        self,
        job: Mapping[str, Any],
        baseline_id: str,
        batch_delta_files: Iterable[str],
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        if self.read_only:
            raise CumulativePolicyError("CUMULATIVE_STATE_READ_ONLY")
        batch_manifest = self.load_batch_manifest(baseline_id)
        self._verify_manifest_files(batch_manifest, self._manifest_path(baseline_id).parent)
        status = self.public_status(baseline_id, batch_delta_files)
        if status["unexpected_runtime_dirty_files"]:
            raise CumulativePolicyError(
                "UNEXPECTED_RUNTIME_DIRTY_FILES",
                ",".join(status["unexpected_runtime_dirty_files"][:20]),
            )
        if not status["external_frozen_integrity"]:
            raise CumulativePolicyError("EXTERNAL_FROZEN_BASELINE_CHANGED")

        collector = GitCollector(str(self.workspace), profile=self.profile)
        baseline, error = collector.capture_baseline()
        if baseline is None:
            raise CumulativePolicyError("PRE_JOB_SNAPSHOT_CAPTURE_FAILED", error)
        inventories = self._inventories()
        target_paths = self._target_snapshot_paths(job, inventories)
        job_id = str(job.get("job_id", ""))
        attempt = int(job.get("outer_attempt", 0)) + 1
        seed = {
            "baseline_id": baseline_id,
            "job_id": job_id,
            "attempt": attempt,
            "heads": [repo.head_oid for repo in baseline.repos],
            "worktree_trees": [repo.worktree_tree for repo in baseline.repos],
        }
        snapshot_id = (
            f"SNAP-{hashlib.sha256(_canonical_json(seed)).hexdigest()[:20].upper()}"
        )
        final_dir = self.root / "job-snapshots" / baseline_id / snapshot_id
        manifest_path = final_dir / "manifest.json"
        if final_dir.exists():
            loaded = self.load_job_baseline(manifest_path)
            return {
                "snapshot_id": snapshot_id,
                "manifest_path": str(manifest_path),
                "active_baseline_id": baseline_id,
                "external_frozen_integrity": bool(status["external_frozen_integrity"]),
                "baseline_declaration_integrity": bool(status["baseline_declaration_integrity"]),
                "pre_job_heads": [repo.head_oid for repo in loaded.repos],
                "inherited_batch_delta_files": sorted(
                    {normalize_changed_path(path) for path in batch_delta_files if path}
                ),
                "unexpected_external_dirty_files": list(
                    status["unexpected_runtime_dirty_files"]
                ),
            }
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        temp_dir = final_dir.parent / f".{uuid.uuid4().hex}.tmp"
        temp_dir.mkdir(parents=False, exist_ok=False)
        try:
            copied = [self._copy_file(path, temp_dir / "files") for path in target_paths]
            repos = self._snapshot_repos(baseline, temp_dir)
            manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "kind": "PRE_JOB_SNAPSHOT",
                "snapshot_id": snapshot_id,
                "baseline_id": baseline_id,
                "job_id": job_id,
                "outer_attempt": attempt,
                "created_at": _now(),
                "target_resources": list(
                    dict(job.get("request") or {}).get("target_resources") or []
                ),
                "pre_job_dirty_files": sorted(self._dirty_paths(inventories)),
                "repos": repos,
                "files": copied,
            }
            _atomic_json_write(temp_dir / "manifest.json", manifest, temp_dir)
            os.replace(temp_dir, final_dir)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        loaded = self.load_job_baseline(manifest_path)
        return {
            "snapshot_id": snapshot_id,
            "manifest_path": str(manifest_path),
            "active_baseline_id": baseline_id,
            "external_frozen_integrity": bool(status["external_frozen_integrity"]),
            "baseline_declaration_integrity": bool(status["baseline_declaration_integrity"]),
            "pre_job_heads": [repo.head_oid for repo in loaded.repos],
            "inherited_batch_delta_files": sorted(
                {normalize_changed_path(path) for path in batch_delta_files if path}
            ),
            "unexpected_external_dirty_files": list(
                status["unexpected_runtime_dirty_files"]
            ),
        }

    def validate_no_delta_fresh_input(self, parent, queue, integrity):
        """Revalidate a failed execution's canonical input, never a checkpoint."""
        last = dict(parent.get("last_result") or {})
        evidence = dict(last.get("no_task_delta_evidence") or {})
        if (parent.get("status") != "FAILED_FINAL"
                or last.get("checkpoint_status") != "VERIFIED_NO_TASK_DELTA"
                or last.get("checkpoint_manifest") or parent.get("checkpoint_seed")
                or last.get("task_owned_changed_files") or last.get("changed_files")
                or parent.get("task_owned_delta_files")
                or evidence.get("kind") != "VERIFIED_NO_TASK_DELTA"
                or evidence.get("passed") is not True
                or evidence.get("stable_hash_comparison") is not True
                or any(evidence.get(k) for k in (
                    "raw_task_delta_files", "task_owned_source_delta_files",
                    "untracked_new_deleted_renamed_delta_files", "failure_code"))):
            raise CumulativePolicyError("CORRECTIVE_CHECKPOINT_INVALID")
        return self._validate_no_delta_input_tree(parent, queue, integrity, evidence)

    def validate_policy_fresh_input(self, parent, queue, integrity, task):
        """No fake checkpoint: validate the historical Task and its actual input."""
        evidence = dict(task.no_task_delta_evidence or {})
        if (parent.get('status') != 'AWAITING_QA'
                or parent.get('candidate_id') or parent.get('candidate_manifest')
                or parent.get('checkpoint_seed') or parent.get('task_owned_delta_files')
                or parent.get('last_result', {}).get('failure_code') != 'CANDIDATE_DELTA_EMPTY'
                or task.job_id != parent.get('job_id')
                or task.task_id != parent.get('current_claim_task_id')
                or task.task_id not in parent.get('task_ids', [])
                or task.pre_job_snapshot_manifest != parent.get('pre_job_snapshot_manifest')
                or task.changed_files or task.task_owned_changed_files
                or task.qa_request.get('required') is not True
                or task.qa_request.get('qa_type') not in {'DB_CONTRACT', 'HUMAN_ACCEPTANCE'}
                or evidence.get('kind') != 'VERIFIED_NO_TASK_DELTA'
                or evidence.get('passed') is not True
                or evidence.get('stable_hash_comparison') is not True
                or any(evidence.get(k) for k in ('raw_task_delta_files',
                    'task_owned_source_delta_files', 'untracked_new_deleted_renamed_delta_files', 'failure_code'))):
            raise CumulativePolicyError('QA_POLICY_RESOLUTION_STATE_INVALID')
        fresh = self._validate_no_delta_input_tree(parent, queue, integrity, evidence)
        fresh['kind'] = 'CANDIDATELESS_QA_POLICY_FRESH_INPUT'
        fresh['source_task_id'] = task.task_id
        return fresh

    def _validate_no_delta_input_tree(self, parent, queue, integrity, evidence):
        baseline_id = str(queue.get("active_baseline_id", ""))
        if (not baseline_id or parent.get("active_baseline_id") != baseline_id
                or integrity.get("baseline_declaration_integrity") is not True
                or integrity.get("external_frozen_integrity") is not True
                or integrity.get("unexpected_runtime_dirty_files")):
            raise CumulativePolicyError("REPLACEMENT_WORKTREE_INTEGRITY_INVALID")
        path = Path(str(parent.get("pre_job_snapshot_manifest", "")))
        baseline = self.load_job_baseline(path)
        manifest_bytes = path.read_bytes()
        manifest = json.loads(manifest_bytes)
        if (manifest.get("job_id") != parent.get("job_id")
                or manifest.get("snapshot_id") != parent.get("pre_job_snapshot_id")
                or manifest.get("baseline_id") != baseline_id):
            raise CumulativePolicyError("CHECKPOINT_JOB_MISMATCH")
        recorded = evidence.get("repositories") or []
        expected = {(r.module or "", r.head_oid, r.worktree_tree, r.index_tree)
                    for r in baseline.repos}
        actual = {(r.get("module"), r.get("head_oid"),
                   r.get("pre_job_worktree_tree"), r.get("pre_job_index_tree"))
                  for r in recorded}
        if (actual != expected or len(recorded) != len(expected)
                or any(r.get("matches_pre_job") is not True
                       or r.get("stable") is not True or r.get("raw_delta_files")
                       for r in recorded)):
            raise CumulativePolicyError("CORRECTIVE_CHECKPOINT_INVALID")
        current = GitCollector(str(self.workspace), profile=self.profile).prove_no_task_delta(baseline)
        if current.get("passed") is not True:
            raise CumulativePolicyError("REPLACEMENT_WORKTREE_INTEGRITY_INVALID",
                                        str(current.get("failure_code", "")))
        return {"kind": "VERIFIED_NO_TASK_DELTA_FRESH_INPUT",
                "parent_job_id": parent["job_id"], "parent_revision": parent["revision"],
                "generation": queue["generation"], "active_baseline_id": baseline_id,
                "snapshot_id": manifest["snapshot_id"], "manifest_path": str(path),
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest()}

    def load_job_baseline(self, manifest_path: str | Path) -> GitTaskBaseline:
        path = Path(manifest_path).resolve()
        try:
            path.relative_to(self.root.resolve())
        except ValueError as exc:
            raise CumulativePolicyError("SNAPSHOT_PATH_ESCAPE") from exc
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise CumulativePolicyError("PRE_JOB_SNAPSHOT_MISSING") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise CumulativePolicyError("PRE_JOB_SNAPSHOT_INVALID") from exc
        if (
            manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
            or manifest.get("kind") != "PRE_JOB_SNAPSHOT"
        ):
            raise CumulativePolicyError("PRE_JOB_SNAPSHOT_INVALID")
        self._verify_manifest_files(manifest, path.parent)
        repos: list[GitRepoBaseline] = []
        for record in manifest.get("repos") or []:
            repo = Path(str(record.get("repo", ""))).resolve()
            try:
                repo.relative_to(self.workspace)
            except ValueError as exc:
                raise CumulativePolicyError("SNAPSHOT_REPOSITORY_ESCAPE") from exc
            baseline = GitRepoBaseline(
                git_dir=repo,
                module=str(record.get("module", "")) or None,
                head_oid=str(record.get("head", "")),
                worktree_tree=str(record.get("worktree_tree", "")),
                index_tree=str(record.get("index_tree", "")),
            )
            for tree in (baseline.worktree_tree, baseline.index_tree):
                code, _, _ = self._run_git(repo, ["cat-file", "-e", f"{tree}^{{tree}}"])
                if code != 0:
                    raise CumulativePolicyError("SNAPSHOT_GIT_TREE_MISSING", baseline.module or repo.name)
            repos.append(baseline)
        if not repos:
            raise CumulativePolicyError("PRE_JOB_SNAPSHOT_INVALID")
        return GitTaskBaseline(tuple(repos), snapshot_manifest=path)
