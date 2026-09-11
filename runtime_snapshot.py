"""Immutable, content-addressed runtime inputs for one harness Job.

The queue stores hashes of the live profile and policy surface.  Immediately
before a Job is created, those exact inputs are copied into a persistent
content-addressed directory.  Harness-built prompts and profile reads use that
directory.  Engine-owned global discovery cannot be fully disabled for every
third-party CLI, so its known policy surface remains live-hash guarded before
and after each external invocation.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from project_profile import ProjectProfile, load_project_profile


SNAPSHOT_FORMAT = 1
_REPARSE_FLAG = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
_PUBLISH_LOCK = threading.RLock()


class RuntimeSnapshotError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(str(path)))


def _existing(path: Path) -> bool:
    return os.path.lexists(str(path))


def _assert_ancestor_chain_no_reparse(path: Path) -> None:
    absolute = _lexical_absolute(path)
    current = Path(absolute.anchor) if absolute.anchor else Path()
    for part in absolute.parts[1:] if absolute.anchor else absolute.parts:
        current = current / part
        if _existing(current) and _is_reparse(current):
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_ROOT_UNSAFE")


def _assert_safe_contained(
    base: Path,
    candidate: Path,
    *,
    directory: bool | None = None,
) -> Path:
    lexical_base = _lexical_absolute(base)
    lexical_candidate = _lexical_absolute(candidate)
    try:
        lexical_candidate.relative_to(lexical_base)
    except ValueError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_ESCAPE") from exc
    _assert_ancestor_chain_no_reparse(lexical_base)
    current = lexical_base
    try:
        relative = lexical_candidate.relative_to(lexical_base)
        for part in relative.parts:
            current = current / part
            if _existing(current) and _is_reparse(current):
                raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_ESCAPE")
        resolved_base = lexical_base.resolve(strict=True)
        resolved = lexical_candidate.resolve(strict=True)
        resolved.relative_to(resolved_base)
    except RuntimeSnapshotError:
        raise
    except (OSError, ValueError) as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_ESCAPE") from exc
    if directory is True and not lexical_candidate.is_dir():
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_TYPE_INVALID")
    if directory is False and not lexical_candidate.is_file():
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_TYPE_INVALID")
    return lexical_candidate


def _prepare_snapshot_root(snapshot_root: Path) -> Path:
    parent = _lexical_absolute(snapshot_root)
    control_root = parent.parent
    state_base = control_root.parent
    _assert_ancestor_chain_no_reparse(state_base)
    try:
        if not _existing(control_root):
            control_root.mkdir()
        _assert_safe_contained(state_base, control_root, directory=True)
        if not _existing(parent):
            parent.mkdir()
        return _assert_safe_contained(control_root, parent, directory=True)
    except RuntimeSnapshotError:
        raise
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_ROOT_UNWRITABLE") from exc


def _safe_cleanup_stage(parent: Path, stage: Path) -> None:
    if not _existing(stage):
        return
    try:
        safe_stage = _assert_safe_contained(parent, stage, directory=True)
        # Reject every descendant reparse before recursive removal.  A swapped
        # or unreadable stage is deliberately leaked for local inspection rather
        # than risking deletion outside the control root.
        _tree_files(safe_stage)
    except RuntimeSnapshotError:
        return
    shutil.rmtree(safe_stage, ignore_errors=True)


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
    is_junction = getattr(path, "is_junction", None)
    return bool(
        path.is_symlink()
        or (is_junction is not None and is_junction())
        or int(getattr(info, "st_file_attributes", 0)) & _REPARSE_FLAG
    )


def _tree_files(root: Path) -> tuple[tuple[Path, str], ...]:
    """Inventory a regular, reparse-free tree without following links."""
    if _is_reparse(root):
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
    try:
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
    if not resolved_root.is_dir():
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")

    result: list[tuple[Path, str]] = []

    def fail_walk(error: OSError) -> None:
        raise error

    try:
        for current, directories, files in os.walk(
            root, topdown=True, followlinks=False, onerror=fail_walk
        ):
            current_path = Path(current)
            if _is_reparse(current_path):
                raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
            for name in directories:
                if _is_reparse(current_path / name):
                    raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
            for name in files:
                candidate = current_path / name
                if _is_reparse(candidate) or not candidate.is_file():
                    raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
                try:
                    relative = candidate.resolve(strict=True).relative_to(resolved_root)
                except (OSError, ValueError) as exc:
                    raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE") from exc
                result.append((candidate, relative.as_posix()))
    except RuntimeSnapshotError:
        raise
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
    return tuple(sorted(result, key=lambda item: item[1]))


def _digest_records(records: Iterable[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for label, content in sorted(records, key=lambda item: item[0]):
        label_bytes = label.encode("utf-8")
        digest.update(len(label_bytes).to_bytes(8, "big"))
        digest.update(label_bytes)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def profile_surface_sha256(
    files: Sequence[tuple[Path, str]],
    read_bytes: Callable[[Path], bytes] | None = None,
) -> str:
    """Hash a profile exactly like HarnessService's queued context contract."""
    reader = read_bytes or (lambda path: path.read_bytes())
    digest = hashlib.sha256()
    for path, relative in sorted(files, key=lambda item: item[1]):
        relative_bytes = relative.encode("utf-8")
        content = reader(path)
        digest.update(len(relative_bytes).to_bytes(8, "big"))
        digest.update(relative_bytes)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def effective_policy_files(
    policy_root: Path,
    runtime_files: Sequence[tuple[Path, str]],
) -> tuple[tuple[Path, str], ...]:
    """Return the full explicitly injected policy surface plus global guards.

    A real agents root contains AGENTS.md, hooks/, and skills/.  Tests and older
    installations that expose only individual files retain the fixed-file
    behavior instead of accidentally hashing unrelated sibling directories.
    """
    if _is_reparse(policy_root):
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
    try:
        root = policy_root.resolve(strict=True)
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
    records: list[tuple[Path, str]] = []

    def ignored(relative: str) -> bool:
        parts = Path(relative).parts
        return "__pycache__" in parts or relative.endswith((".pyc", ".pyo"))

    if (root / "AGENTS.md").is_file() and (root / "hooks").is_dir() and (
        root / "skills"
    ).is_dir():
        records.append((root / "AGENTS.md", "source/AGENTS.md"))
        for subtree in ("hooks", "skills"):
            for path, relative in _tree_files(root / subtree):
                if ignored(relative):
                    continue
                records.append((path, f"source/{subtree}/{relative}"))

    covered = {path.resolve() for path, _ in records}
    for path, label in runtime_files:
        candidate = Path(path)
        if _is_reparse(candidate):
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
        if resolved in covered:
            continue
        if not resolved.is_file():
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
        records.append((resolved, f"guard/{label}"))
        covered.add(resolved)

    # Codex/Factory/Droid may discover additional globally installed skills.
    # They are not trusted as frozen prompt input, but the entire existing trees
    # are included in the live pre/post guard so a mid-queue change fails closed.
    global_skill_roots: list[tuple[Path, str]] = []
    for path, label in runtime_files:
        if label in {"codex_global_agents", "factory_global_agents"}:
            global_skill_roots.append((Path(path).parent / "skills", label))
        elif label.startswith("global_") and "_skill" in label:
            global_skill_roots.append((Path(path).parents[1], "shared_global_skills"))
    seen_roots: set[Path] = set()
    for skill_root, label in global_skill_roots:
        try:
            resolved_root = skill_root.resolve(strict=True)
        except OSError:
            continue
        if resolved_root in seen_roots or not resolved_root.is_dir():
            continue
        seen_roots.add(resolved_root)
        for path, relative in _tree_files(skill_root):
            if ignored(relative):
                continue
            resolved = path.resolve(strict=True)
            if resolved in covered:
                continue
            records.append((resolved, f"live/{label}/{relative}"))
            covered.add(resolved)
    return tuple(sorted(records, key=lambda item: item[1]))


def policy_surface_sha256(
    policy_root: Path,
    runtime_files: Sequence[tuple[Path, str]],
) -> str:
    try:
        return _digest_records(
            (label, path.read_bytes())
            for path, label in effective_policy_files(policy_root, runtime_files)
        )
    except RuntimeSnapshotError:
        raise
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc


def _snapshot_sources(
    profile_root: Path,
    schema_file: Path,
    profile_files: Sequence[tuple[Path, str]],
    policy_files: Sequence[tuple[Path, str]],
) -> tuple[tuple[Path, str], ...]:
    records: list[tuple[Path, str]] = [
        (path, f"profile/{profile_root.name}/{relative}")
        for path, relative in profile_files
    ]
    source_policy = [
        (path, label.removeprefix("source/"))
        for path, label in policy_files
        if label.startswith("source/")
    ]
    source_labels = {label for _, label in source_policy}
    if (
        "AGENTS.md" not in source_labels
        or not any(label.startswith("hooks/") for label in source_labels)
        or not any(label.startswith("skills/") for label in source_labels)
    ):
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_POLICY_INCOMPLETE")
    records.extend(
        (path, f"policy/agents/{relative}") for path, relative in source_policy
    )
    if _is_reparse(schema_file):
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
    try:
        resolved_schema = schema_file.resolve(strict=True)
    except OSError as exc:
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNREADABLE") from exc
    if not resolved_schema.is_file():
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_UNSAFE")
    records.append((resolved_schema, "schema/project-profile.schema.json"))
    labels = [label for _, label in records]
    if len(labels) != len(set(labels)):
        raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_MANIFEST_INVALID")
    return tuple(sorted(records, key=lambda item: item[1]))


def _file_manifest(root: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for path, relative in _tree_files(root):
        if relative == "snapshot.json":
            continue
        try:
            files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_UNREADABLE") from exc
    return files


@dataclass(frozen=True)
class RuntimeSnapshot:
    root: Path
    profile_dir: Path
    policy_root: Path
    profile: ProjectProfile
    execution_context: Mapping[str, Any]

    def validate(self) -> None:
        try:
            root = _assert_safe_contained(
                self.root.parent, self.root, directory=True
            ).resolve(strict=True)
            manifest_path = root / "snapshot.json"
            if _is_reparse(manifest_path):
                raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_UNSAFE")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except RuntimeSnapshotError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_MANIFEST_INVALID") from exc
        if not isinstance(manifest, dict) or manifest.get("format") != SNAPSHOT_FORMAT:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_MANIFEST_INVALID")
        if manifest.get("execution_context") != dict(self.execution_context):
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_CONTEXT_MISMATCH")
        expected_identity = hashlib.sha256(
            json.dumps(
                dict(self.execution_context),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if root.name != expected_identity:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_CONTEXT_MISMATCH")
        expected = manifest.get("files")
        if not isinstance(expected, dict) or any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or len(value) != 64
            for key, value in expected.items()
        ):
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_MANIFEST_INVALID")
        actual = _file_manifest(root)
        if actual != expected:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_INTEGRITY_MISMATCH")
        for required in (self.profile_dir, self.policy_root):
            try:
                required.resolve(strict=True).relative_to(root)
            except (OSError, ValueError) as exc:
                raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_PATH_ESCAPE") from exc


def load_frozen_runtime_snapshot(snapshot_root: Path, workspace: Path,
                                 execution_context: Mapping[str, Any]) -> RuntimeSnapshot:
    identity = hashlib.sha256(json.dumps(dict(execution_context), ensure_ascii=True,
                                        sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    root = snapshot_root / identity
    _assert_safe_contained(snapshot_root, root, directory=True)
    profile_dir = root / "profile" / str(execution_context["profile_id"])
    profile = load_project_profile(profile_dir, workspace, schema_file=root / "schema/project-profile.schema.json")
    snapshot = RuntimeSnapshot(root, profile_dir, root / "policy/agents", profile, dict(execution_context))
    snapshot.validate()
    return snapshot


def materialize_runtime_snapshot(
    *,
    snapshot_root: Path,
    profile: ProjectProfile,
    policy_root: Path,
    schema_file: Path,
    workspace: Path,
    execution_context: Mapping[str, Any],
    runtime_files: Sequence[tuple[Path, str]],
) -> RuntimeSnapshot:
    """Create or reuse the immutable snapshot selected by exact queued context."""
    identity = hashlib.sha256(
        json.dumps(
            dict(execution_context),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    with _PUBLISH_LOCK:
        parent = _prepare_snapshot_root(snapshot_root)
        final = parent / identity
        profile_dir = final / "profile" / profile.id
        policy_snapshot_root = final / "policy" / "agents"
        snapshot_schema = final / "schema" / "project-profile.schema.json"
        if _existing(final):
            _assert_safe_contained(parent, final, directory=True)
            reused_profile = load_project_profile(
                profile_dir, workspace, schema_file=snapshot_schema
            )
            snapshot = RuntimeSnapshot(
                root=final,
                profile_dir=profile_dir,
                policy_root=policy_snapshot_root,
                profile=reused_profile,
                execution_context=dict(execution_context),
            )
            snapshot.validate()
            return snapshot

        stage = parent / f".snapshot-{uuid.uuid4().hex}.tmp"
        try:
            stage.mkdir()
            _assert_safe_contained(parent, stage, directory=True)
            profile_files = _tree_files(profile.profile_dir)
            policy_files = effective_policy_files(policy_root, runtime_files)
            content_cache: dict[Path, bytes] = {}

            def read_once(path: Path) -> bytes:
                try:
                    key = path.resolve(strict=True)
                    if key not in content_cache:
                        content_cache[key] = key.read_bytes()
                    return content_cache[key]
                except OSError as exc:
                    raise RuntimeSnapshotError(
                        "RUNTIME_SNAPSHOT_SOURCE_UNREADABLE"
                    ) from exc

            copied_profile_sha = profile_surface_sha256(profile_files, read_once)
            copied_policy_sha = _digest_records(
                (label, read_once(path)) for path, label in policy_files
            )
            project_manifest = next(
                (
                    path
                    for path, relative in profile_files
                    if relative == "project.json"
                ),
                None,
            )
            copied_manifest_sha = (
                hashlib.sha256(read_once(project_manifest)).hexdigest()
                if project_manifest is not None
                else ""
            )
            if (
                copied_profile_sha
                != execution_context.get("profile_snapshot_sha256")
                or copied_policy_sha
                != execution_context.get("policy_snapshot_sha256")
                or copied_manifest_sha
                != execution_context.get("profile_manifest_sha256")
            ):
                raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_SOURCE_DRIFT")

            sources = _snapshot_sources(
                profile.profile_dir,
                schema_file,
                profile_files,
                policy_files,
            )
            for source, relative in sources:
                _assert_safe_contained(parent, stage, directory=True)
                destination = stage.joinpath(*relative.split("/"))
                destination.parent.mkdir(parents=True, exist_ok=True)
                _assert_safe_contained(stage, destination.parent, directory=True)
                with destination.open("xb") as stream:
                    stream.write(read_once(source))
            # Retain immutable evidence for live global guard inputs that are
            # not part of the explicitly injected source agents tree.
            policy_resolved = policy_root.resolve(strict=True)
            guard_index = 0
            for source, label in policy_files:
                try:
                    source.resolve(strict=True).relative_to(policy_resolved)
                    continue
                except ValueError:
                    pass
                _assert_safe_contained(parent, stage, directory=True)
                guard_destination = stage / "guards" / f"{guard_index:04d}.bin"
                guard_destination.parent.mkdir(parents=True, exist_ok=True)
                _assert_safe_contained(
                    stage, guard_destination.parent, directory=True
                )
                with guard_destination.open("xb") as stream:
                    stream.write(read_once(source))
                guard_index += 1
            manifest = {
                "format": SNAPSHOT_FORMAT,
                "execution_context": dict(execution_context),
                "files": _file_manifest(stage),
            }
            manifest_bytes = json.dumps(
                manifest, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            manifest_path = stage / "snapshot.json"
            with manifest_path.open("xb") as stream:
                stream.write(manifest_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            _assert_safe_contained(parent, stage, directory=True)
            _assert_safe_contained(parent, stage / "snapshot.json", directory=False)
            if _existing(final):
                _assert_safe_contained(parent, final, directory=True)
                _safe_cleanup_stage(parent, stage)
            else:
                os.replace(stage, final)
            _assert_safe_contained(parent, final, directory=True)
            reused_profile = load_project_profile(
                profile_dir, workspace, schema_file=snapshot_schema
            )
            snapshot = RuntimeSnapshot(
                root=final,
                profile_dir=profile_dir,
                policy_root=policy_snapshot_root,
                profile=reused_profile,
                execution_context=dict(execution_context),
            )
            snapshot.validate()
            return snapshot
        except RuntimeSnapshotError:
            raise
        except Exception as exc:
            raise RuntimeSnapshotError("RUNTIME_SNAPSHOT_CREATE_FAILED") from exc
        finally:
            _safe_cleanup_stage(parent, stage)
