"""Validated, project-agnostic profile loading for the external harness."""

from __future__ import annotations

import fnmatch
import json
import os
import posixpath
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Mapping, Sequence

from db_catalog import DbCatalog, DbCatalogError, load_db_catalog


SUPPORTED_SCHEMA_VERSION = 1
PROFILE_FILENAME = "project.json"
PROFILE_SCHEMA_FILE = Path(r"D:\agents\profiles\project-profile.schema.json")
REQUIRED_TOP_LEVEL = (
    "schema_version",
    "id",
    "display_name",
    "workspace_roots",
    "context",
    "modules",
    "rules",
    "area_patterns",
    "protected_paths",
    "local_dev_config_excludes",
)
VALID_MODULE_KINDS = {"backend", "frontend", "library", "infrastructure"}
COMMIT_POLICY_PER_JOB = "PER_JOB"
COMMIT_POLICY_BATCH_FINAL = "BATCH_FINAL_COMMIT"
VALID_COMMIT_POLICIES = {COMMIT_POLICY_PER_JOB, COMMIT_POLICY_BATCH_FINAL}
DEFAULT_NON_SOURCE_ARTIFACT_PATTERNS = (
    "task_plan.md",
    "**/task_plan.md",
    "**/*plan*.md",
    "**/VERIFY*.md",
    ".tasks/**",
    "doc/**",
)


class ProfileError(ValueError):
    """Safe profile validation/selection error (never includes file contents)."""

    def __init__(self, code: str, path: str = "$"):
        self.code = code
        self.path = path if path.startswith(("$", "schema$")) else f"$.{path}"
        super().__init__(f"[{self.code}] {self.path}")


def normalize_relative_path(value: str, label: str = "path") -> str:
    """Return a slash-normalized relative path and reject absolute/escaping paths."""
    if not isinstance(value, str) or not value.strip():
        raise ProfileError("PROFILE_PATH_INVALID", label)
    raw = value.strip().replace("\\", "/")
    if PurePosixPath(raw).is_absolute() or PureWindowsPath(raw).is_absolute():
        raise ProfileError("PROFILE_PATH_INVALID", label)
    normalized = posixpath.normpath(raw)
    if normalized in ("", ".") or normalized == ".." or normalized.startswith("../"):
        raise ProfileError("PROFILE_PATH_ESCAPE", label)
    return normalized


def normalize_changed_path(value: str) -> str:
    """Normalize a workspace-relative changed path for profile comparisons."""
    raw = (value or "").strip().replace("\\", "/")
    while raw.startswith("./"):
        raw = raw[2:]
    return posixpath.normpath(raw).lstrip("/") if raw else ""


def _canonical_fs_path(value: str | Path) -> str:
    return os.path.normcase(os.path.normpath(str(Path(value).resolve())))


def _dedupe_strings(values: Sequence[str], label: str, *, paths: bool = False) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise ProfileError("PROFILE_VALUE_INVALID", label)
    result: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            raise ProfileError("PROFILE_VALUE_INVALID", f"{label}[{index}]")
        item = normalize_relative_path(value, f"{label}[{index}]") if paths else value.strip()
        key = item.casefold() if paths else item
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)


def _resolve_profile_path(profile_dir: Path, value: str, label: str) -> Path:
    rel = normalize_relative_path(value, label)
    path = (profile_dir / Path(*PurePosixPath(rel).parts)).resolve()
    try:
        path.relative_to(profile_dir)
    except ValueError as exc:
        raise ProfileError("PROFILE_PATH_OUTSIDE", label) from exc
    if not path.exists():
        raise ProfileError("PROFILE_PATH_MISSING", label)
    return path


def _validate_feature_map(path: Path, profile_id: str) -> None:
    """Fail profile bootstrap before a malformed routing map reaches a prompt."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProfileError("FEATURE_MAP_JSON_INVALID", "context.feature_map") from exc
    if (
        not isinstance(data, dict)
        or data.get("schema_version") != 1
        or data.get("profile_id") != profile_id
        or not isinstance(data.get("precedence"), list)
        or not isinstance(data.get("features"), list)
    ):
        raise ProfileError("FEATURE_MAP_INVALID", "context.feature_map")
    identifiers: set[str] = set()
    for index, feature in enumerate(data["features"]):
        label = f"context.feature_map.features[{index}]"
        if not isinstance(feature, dict):
            raise ProfileError("FEATURE_MAP_INVALID", label)
        identifier = feature.get("id")
        if not isinstance(identifier, str) or not identifier.strip():
            raise ProfileError("FEATURE_MAP_INVALID", f"{label}.id")
        folded = identifier.strip().casefold()
        if folded in identifiers:
            raise ProfileError("FEATURE_MAP_DUPLICATE_ID", f"{label}.id")
        identifiers.add(folded)
        for field in (
            "aliases", "screens", "asis_functions", "tables", "tobe_additions",
            "ui_paths", "constraints", "asis_source_globs",
        ):
            values = feature.get(field, [])
            if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
                raise ProfileError("FEATURE_MAP_INVALID", f"{label}.{field}")


def _schema_error_path(error: object, prefix: str = "$") -> str:
    parts = list(getattr(error, "absolute_path", ()) or ())
    path = prefix
    for part in parts:
        path += f"[{part}]" if isinstance(part, int) else f".{part}"
    if getattr(error, "validator", "") == "required":
        required = getattr(error, "validator_value", ())
        instance = getattr(error, "instance", {})
        if isinstance(required, list) and isinstance(instance, dict):
            missing = next((name for name in required if name not in instance), None)
            if isinstance(missing, str):
                path += f".{missing}"
    return path


def _validate_manifest_schema(
    data: object, schema_file: str | Path | None = None
) -> None:
    """Validate the canonical schema and manifest with the schema-declared draft."""
    schema_path = Path(schema_file) if schema_file is not None else PROFILE_SCHEMA_FILE
    if not schema_path.is_file():
        raise ProfileError("PROFILE_SCHEMA_MISSING", "schema$project-profile.schema.json")
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProfileError(
            "PROFILE_SCHEMA_JSON_INVALID", "schema$project-profile.schema.json"
        ) from exc
    if not isinstance(schema, dict) or not isinstance(schema.get("$schema"), str):
        raise ProfileError("PROFILE_SCHEMA_INVALID", "schema$.$schema")

    try:
        from jsonschema import SchemaError
        from jsonschema.validators import validator_for
    except ImportError as exc:
        raise ProfileError("PROFILE_SCHEMA_ENGINE_UNAVAILABLE", "schema$jsonschema") from exc

    validator_class = validator_for(schema, default=None)
    if validator_class is None:
        raise ProfileError("PROFILE_SCHEMA_DRAFT_UNSUPPORTED", "schema$.$schema")
    try:
        validator_class.check_schema(schema)
    except SchemaError as exc:
        path = _schema_error_path(exc, "schema$")
        raise ProfileError("PROFILE_SCHEMA_INVALID", path) from exc

    errors = sorted(
        validator_class(schema).iter_errors(data),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        raise ProfileError(
            "PROFILE_MANIFEST_SCHEMA_INVALID", _schema_error_path(errors[0])
        )


@dataclass(frozen=True)
class ModuleProfile:
    name: str
    path: str
    kind: str
    protected: bool
    role: str = ""
    build_cwd: str = ""
    build_argv: tuple[str, ...] = ()


@dataclass(frozen=True)
class AreaEvidence:
    areas: tuple[str, ...]
    reasons: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class BaselineFileProfile:
    path: str
    sha256: str
    evidence: str = ""


@dataclass(frozen=True)
class ControlPolicyProfile:
    commit_policy: str = COMMIT_POLICY_PER_JOB
    cumulative_worktree: bool = False
    auto_continue_without_commit: bool = False
    pre_job_snapshot_supported: bool = False
    scoped_rollback_supported: bool = False
    final_commit_required: bool = False
    task_owned_baseline_files: tuple[BaselineFileProfile, ...] = ()
    external_frozen_baseline_files: tuple[BaselineFileProfile, ...] = ()


@dataclass(frozen=True)
class ProjectProfile:
    schema_version: int
    id: str
    display_name: str
    profile_dir: Path
    workspace_roots: tuple[Path, ...]
    project_rules: Path
    analysis: Path
    ui_map: Path | None
    db_catalog: DbCatalog | None
    modules: tuple[ModuleProfile, ...]
    rules: Mapping[str, tuple[Path, ...]]
    area_patterns: Mapping[str, tuple[str, ...]]
    protected_paths: tuple[str, ...]
    local_dev_config_excludes: tuple[str, ...]
    # Optional, profile-local feature routing index.  Kept at the end with a
    # default so injected test profiles and older callers remain compatible.
    feature_map: Path | None = None
    control_policy: ControlPolicyProfile = ControlPolicyProfile()
    # Harness-owned defaults keep old manifests valid. Programmatic/next-schema
    # profiles may replace this tuple without hard-coding project paths in Manager.
    non_source_artifact_patterns: tuple[str, ...] = DEFAULT_NON_SOURCE_ARTIFACT_PATTERNS

    @property
    def module_names(self) -> tuple[str, ...]:
        return tuple(module.name for module in self.modules)

    def module_for_path(self, path: str) -> ModuleProfile | None:
        normalized = normalize_changed_path(path).casefold()
        matches = [
            module
            for module in self.modules
            if normalized == module.path.casefold()
            or normalized.startswith(module.path.casefold().rstrip("/") + "/")
        ]
        return max(matches, key=lambda item: len(item.path), default=None)

    def is_local_dev_config(self, path: str) -> bool:
        normalized = normalize_changed_path(path)
        name = normalized.rsplit("/", 1)[-1]
        return any(
            fnmatch.fnmatchcase(name.casefold(), pattern.casefold())
            or fnmatch.fnmatchcase(normalized.casefold(), pattern.casefold())
            for pattern in self.local_dev_config_excludes
        )

    def is_non_source_artifact(self, path: str) -> bool:
        normalized = normalize_changed_path(path)
        name = normalized.rsplit("/", 1)[-1]
        patterns = self.non_source_artifact_patterns or DEFAULT_NON_SOURCE_ARTIFACT_PATTERNS
        return any(
            fnmatch.fnmatchcase(name.casefold(), pattern.casefold())
            or fnmatch.fnmatchcase(normalized.casefold(), pattern.casefold())
            for pattern in patterns
        )

    def is_frontend_only(self, files: Sequence[str] | None) -> bool:
        normalized = [normalize_changed_path(path) for path in (files or []) if path]
        return bool(normalized) and all(
            (module := self.module_for_path(path)) is not None and module.kind == "frontend"
            for path in normalized
        )

    def is_protected_path(self, path: str) -> bool:
        normalized = normalize_changed_path(path).casefold()
        module = self.module_for_path(normalized)
        if module is not None and module.protected:
            return True
        return any(
            fnmatch.fnmatchcase(normalized, pattern.casefold())
            for pattern in self.protected_paths
        )

    def detect_areas(
        self,
        requirement: str = "",
        target_modules: Sequence[str] | None = None,
        changed_files: Sequence[str] | None = None,
    ) -> AreaEvidence:
        """Detect areas in evidence priority order and retain testable reasons."""
        reasons: dict[str, list[str]] = {}

        def add(area: str, reason: str) -> None:
            if area in self.rules or area in self.area_patterns:
                reasons.setdefault(area, [])
                if reason not in reasons[area]:
                    reasons[area].append(reason)

        normalized_files = [normalize_changed_path(path) for path in (changed_files or []) if path]
        for path in normalized_files:
            module = self.module_for_path(path)
            if module:
                area = "backend" if module.kind in ("backend", "library") else module.kind
                add(area, f"changed_file module={module.name} kind={module.kind}: {path}")
                if module.protected:
                    add("architecture", f"changed_file protected module={module.name}: {path}")
            for area, patterns in self.area_patterns.items():
                if any(fnmatch.fnmatchcase(path.casefold(), pat.casefold()) for pat in patterns):
                    add(area, f"changed_file pattern: {path}")
            lower = path.casefold()
            name = lower.rsplit("/", 1)[-1]
            if name.endswith(".java"):
                add("backend", f"changed_file extension=.java: {path}")
            if name.endswith(("qry.xml", "mapper.xml")) or "/mapper/" in lower:
                add("sql", f"changed_file query/mapper XML: {path}")
            if name.endswith((".vue", ".js", ".jsx", ".ts", ".tsx")):
                add("frontend", f"changed_file frontend extension: {path}")
            if name == "web.xml" or any(token in lower for token in ("spring", "context", "session", "redis")):
                add("architecture", f"changed_file infrastructure configuration: {path}")
                add("backend", f"changed_file backend configuration: {path}")
                add("platform", f"changed_file platform configuration: {path}")

        if not normalized_files:
            wanted = {name.casefold() for name in (target_modules or [])}
            for module in self.modules:
                if module.name.casefold() in wanted:
                    area = "backend" if module.kind in ("backend", "library") else module.kind
                    add(area, f"target_module={module.name} kind={module.kind}")

            text = requirement or ""
            keyword_map = {
                "sql": ("sql", "query", "qry", "mapper", "mybatis", "쿼리", "매퍼"),
                "backend": ("java", "spring", "backend", "controller", "service", "백엔드", "컨트롤러", "서비스", "svc", "ctr", "dao", "vo"),
                "frontend": ("vue", "vite", "frontend", "프론트", "component", "router", "ui"),
                "architecture": ("architecture", "dependency", "boundary", "아키텍처", "의존", "경계"),
                "platform": ("platform", "session", "redis", "web.xml", "sso", "세션", "레디스"),
            }
            for area, words in keyword_map.items():
                for word in words:
                    if re.search(rf"(?<![\w]){re.escape(word)}(?![\w])", text, re.IGNORECASE):
                        add(area, f"requirement word={word}")
                        break

        ordered = tuple(area for area in self.rules if area in reasons and area != "always")
        return AreaEvidence(
            areas=ordered,
            reasons={area: tuple(reasons[area]) for area in ordered},
        )

    def build_mode(self, workspace: str | Path) -> str:
        if any(module.build_argv for module in self.modules):
            return "explicit"
        root = Path(workspace).resolve()
        build_files = ("pom.xml", "build.gradle", "build.gradle.kts", "package.json")
        candidates = [root] + [root / Path(*PurePosixPath(module.path).parts) for module in self.modules]
        return "autodetect" if any((base / name).is_file() for base in candidates for name in build_files) else "unavailable"


def load_project_profile(
    profile_dir: str | Path,
    workspace: str | Path,
    *,
    schema_file: str | Path | None = None,
) -> ProjectProfile:
    profile_root = Path(profile_dir).resolve()
    project_file = profile_root / PROFILE_FILENAME
    if not project_file.is_file():
        raise ProfileError("PROFILE_MANIFEST_MISSING", "project.json")
    try:
        data = json.loads(project_file.read_text(encoding="utf-8"))
    except UnicodeError as exc:
        raise ProfileError("PROFILE_MANIFEST_JSON_INVALID", "project.json") from exc
    except json.JSONDecodeError as exc:
        raise ProfileError("PROFILE_MANIFEST_JSON_INVALID", "project.json") from exc
    if not isinstance(data, dict):
        raise ProfileError("PROFILE_MANIFEST_SCHEMA_INVALID", "$")

    _validate_manifest_schema(data, schema_file)

    missing = [field for field in REQUIRED_TOP_LEVEL if field not in data]
    if missing:
        raise ProfileError("PROFILE_REQUIRED_MISSING", missing[0])
    if data.get("schema_version") != SUPPORTED_SCHEMA_VERSION:
        raise ProfileError("PROFILE_SCHEMA_VERSION_UNSUPPORTED", "schema_version")
    profile_id = data.get("id")
    if not isinstance(profile_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", profile_id):
        raise ProfileError("PROFILE_ID_INVALID", "id")
    if profile_id.casefold() != profile_root.name.casefold():
        raise ProfileError("PROFILE_ID_DIRECTORY_MISMATCH", "id")
    display_name = data.get("display_name")
    if not isinstance(display_name, str) or not display_name.strip():
        raise ProfileError("PROFILE_VALUE_INVALID", "display_name")

    roots_raw = data.get("workspace_roots")
    if not isinstance(roots_raw, list) or not roots_raw:
        raise ProfileError("PROFILE_VALUE_INVALID", "workspace_roots")
    roots: list[Path] = []
    seen_roots: set[str] = set()
    for index, value in enumerate(roots_raw):
        if not isinstance(value, str) or not value.strip():
            raise ProfileError("PROFILE_VALUE_INVALID", f"workspace_roots[{index}]")
        root = Path(value).resolve()
        key = _canonical_fs_path(root)
        if key not in seen_roots:
            seen_roots.add(key)
            roots.append(root)
    workspace_path = Path(workspace).resolve()
    if _canonical_fs_path(workspace_path) not in seen_roots:
        raise ProfileError("PROFILE_WORKSPACE_MISMATCH", "workspace_roots")
    if not workspace_path.is_dir():
        raise ProfileError("PROFILE_WORKSPACE_MISSING", "workspace_roots")

    context = data.get("context")
    if not isinstance(context, dict):
        raise ProfileError("PROFILE_VALUE_INVALID", "context")
    missing_context = [name for name in ("project_rules", "analysis") if not context.get(name)]
    if missing_context:
        raise ProfileError("PROFILE_REQUIRED_MISSING", f"context.{missing_context[0]}")
    project_rules = _resolve_profile_path(profile_root, context["project_rules"], "context.project_rules")
    analysis = _resolve_profile_path(profile_root, context["analysis"], "context.analysis")
    ui_map = None
    if context.get("ui_map"):
        ui_map = _resolve_profile_path(profile_root, context["ui_map"], "context.ui_map")
        if not ui_map.is_dir():
            raise ProfileError("PROFILE_PATH_TYPE_INVALID", "context.ui_map")
    db_catalog = None
    if context.get("db_catalog"):
        db_catalog_path = _resolve_profile_path(
            profile_root, context["db_catalog"], "context.db_catalog"
        )
        if not db_catalog_path.is_file():
            raise ProfileError("PROFILE_PATH_TYPE_INVALID", "context.db_catalog")
        try:
            db_catalog = load_db_catalog(db_catalog_path, profile_root)
        except DbCatalogError as exc:
            raise ProfileError(exc.code, exc.path) from exc
    feature_map = None
    if context.get("feature_map"):
        feature_map = _resolve_profile_path(
            profile_root, context["feature_map"], "context.feature_map"
        )
        if not feature_map.is_file():
            raise ProfileError("PROFILE_PATH_TYPE_INVALID", "context.feature_map")
        _validate_feature_map(feature_map, profile_id)

    modules_raw = data.get("modules")
    if not isinstance(modules_raw, list) or not modules_raw:
        raise ProfileError("PROFILE_VALUE_INVALID", "modules")
    modules: list[ModuleProfile] = []
    module_names: set[str] = set()
    module_paths: set[str] = set()
    for index, item in enumerate(modules_raw):
        label = f"modules[{index}]"
        if not isinstance(item, dict):
            raise ProfileError("PROFILE_VALUE_INVALID", label)
        for field in ("name", "path", "kind", "protected"):
            if field not in item:
                raise ProfileError("PROFILE_REQUIRED_MISSING", f"{label}.{field}")
        name = item["name"]
        if not isinstance(name, str) or not name.strip():
            raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.name")
        name_key = name.casefold()
        if name_key in module_names:
            raise ProfileError("PROFILE_MODULE_NAME_DUPLICATE", f"{label}.name")
        module_names.add(name_key)
        rel_path = normalize_relative_path(item["path"], f"{label}.path")
        path_key = rel_path.casefold()
        if path_key in module_paths:
            raise ProfileError("PROFILE_MODULE_PATH_DUPLICATE", f"{label}.path")
        module_paths.add(path_key)
        module_dir = (workspace_path / Path(*PurePosixPath(rel_path).parts)).resolve()
        try:
            module_dir.relative_to(workspace_path)
        except ValueError as exc:
            raise ProfileError("PROFILE_PATH_OUTSIDE", f"{label}.path") from exc
        if not module_dir.is_dir():
            raise ProfileError("PROFILE_PATH_MISSING", f"{label}.path")
        kind = item["kind"]
        if kind not in VALID_MODULE_KINDS:
            raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.kind")
        if not isinstance(item["protected"], bool):
            raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.protected")
        role = item.get("role", "")
        if role and not isinstance(role, str):
            raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.role")
        build_cwd = ""
        build_argv: tuple[str, ...] = ()
        if "build" in item:
            build = item["build"]
            if not isinstance(build, dict) or set(("cwd", "argv")) - set(build):
                raise ProfileError("PROFILE_REQUIRED_MISSING", f"{label}.build")
            build_cwd = normalize_relative_path(build["cwd"], f"{label}.build.cwd")
            build_dir = (workspace_path / Path(*PurePosixPath(build_cwd).parts)).resolve()
            try:
                build_dir.relative_to(workspace_path)
            except ValueError as exc:
                raise ProfileError("PROFILE_PATH_OUTSIDE", f"{label}.build.cwd") from exc
            if not build_dir.is_dir():
                raise ProfileError("PROFILE_PATH_MISSING", f"{label}.build.cwd")
            argv = build["argv"]
            if not isinstance(argv, list) or not argv:
                raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.build.argv")
            if any(not isinstance(value, str) or not value for value in argv):
                raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.build.argv")
            build_argv = tuple(argv)
        modules.append(ModuleProfile(
            name=name.strip(), path=rel_path, kind=kind, protected=item["protected"],
            role=(role or "").strip(), build_cwd=build_cwd, build_argv=build_argv,
        ))

    rules_raw = data.get("rules")
    if not isinstance(rules_raw, dict) or "always" not in rules_raw:
        raise ProfileError("PROFILE_VALUE_INVALID", "rules")
    rules: dict[str, tuple[Path, ...]] = {}
    for area, values in rules_raw.items():
        if not isinstance(area, str) or not area:
            raise ProfileError("PROFILE_VALUE_INVALID", "rules")
        rel_paths = _dedupe_strings(values, f"rules.{area}", paths=True)
        rules[area] = tuple(
            _resolve_profile_path(profile_root, rel, f"rules.{area}") for rel in rel_paths
        )

    patterns_raw = data.get("area_patterns")
    if not isinstance(patterns_raw, dict):
        raise ProfileError("PROFILE_VALUE_INVALID", "area_patterns")
    area_patterns = {
        area: _dedupe_strings(values, f"area_patterns.{area}", paths=True)
        for area, values in patterns_raw.items()
        if isinstance(area, str) and area
    }
    if len(area_patterns) != len(patterns_raw):
        raise ProfileError("PROFILE_VALUE_INVALID", "area_patterns")

    protected_paths = _dedupe_strings(data.get("protected_paths"), "protected_paths", paths=True)
    local_excludes = _dedupe_strings(
        data.get("local_dev_config_excludes"), "local_dev_config_excludes"
    )
    control_policy = ControlPolicyProfile()
    control_raw = data.get("control_policy")
    if control_raw is not None:
        if not isinstance(control_raw, dict):
            raise ProfileError("PROFILE_VALUE_INVALID", "control_policy")
        commit_policy = str(control_raw.get("commit_policy", ""))
        if commit_policy not in VALID_COMMIT_POLICIES:
            raise ProfileError(
                "PROFILE_VALUE_INVALID", "control_policy.commit_policy"
            )
        flag_names = (
            "cumulative_worktree",
            "auto_continue_without_commit",
            "pre_job_snapshot_supported",
            "scoped_rollback_supported",
            "final_commit_required",
        )
        flags: dict[str, bool] = {}
        for name in flag_names:
            value = control_raw.get(name, False)
            if not isinstance(value, bool):
                raise ProfileError("PROFILE_VALUE_INVALID", f"control_policy.{name}")
            flags[name] = value

        def baseline_files(name: str) -> tuple[BaselineFileProfile, ...]:
            values = control_raw.get(name, [])
            if not isinstance(values, list):
                raise ProfileError("PROFILE_VALUE_INVALID", f"control_policy.{name}")
            parsed: list[BaselineFileProfile] = []
            seen: set[str] = set()
            for index, value in enumerate(values):
                label = f"control_policy.{name}[{index}]"
                if not isinstance(value, dict):
                    raise ProfileError("PROFILE_VALUE_INVALID", label)
                path = normalize_relative_path(value.get("path"), f"{label}.path")
                key = path.casefold()
                if key in seen:
                    raise ProfileError("PROFILE_BASELINE_PATH_DUPLICATE", f"{label}.path")
                seen.add(key)
                sha256 = value.get("sha256")
                if not isinstance(sha256, str) or not (
                    sha256 == "DELETED" or re.fullmatch(r"[0-9a-fA-F]{64}", sha256)
                ):
                    raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.sha256")
                evidence = value.get("evidence", "")
                if not isinstance(evidence, str):
                    raise ProfileError("PROFILE_VALUE_INVALID", f"{label}.evidence")
                parsed.append(BaselineFileProfile(path, sha256.lower(), evidence.strip()))
            return tuple(parsed)

        task_owned = baseline_files("task_owned_baseline_files")
        external_frozen = baseline_files("external_frozen_baseline_files")
        overlap = {
            item.path.casefold() for item in task_owned
        } & {item.path.casefold() for item in external_frozen}
        if overlap:
            raise ProfileError(
                "PROFILE_BASELINE_CLASSIFICATION_CONFLICT", "control_policy"
            )
        if commit_policy == COMMIT_POLICY_BATCH_FINAL and not all(flags.values()):
            raise ProfileError(
                "PROFILE_CUMULATIVE_POLICY_INCOMPLETE", "control_policy"
            )
        control_policy = ControlPolicyProfile(
            commit_policy=commit_policy,
            task_owned_baseline_files=task_owned,
            external_frozen_baseline_files=external_frozen,
            **flags,
        )
    return ProjectProfile(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        id=profile_id,
        display_name=display_name.strip(),
        profile_dir=profile_root,
        workspace_roots=tuple(roots),
        project_rules=project_rules,
        analysis=analysis,
        ui_map=ui_map,
        db_catalog=db_catalog,
        modules=tuple(modules),
        rules=rules,
        area_patterns=area_patterns,
        protected_paths=protected_paths,
        local_dev_config_excludes=local_excludes,
        feature_map=feature_map,
        control_policy=control_policy,
    )


def select_project_profile(
    workspace: str | Path,
    cli_profile: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
    agents_dir: str | Path | None = None,
    schema_file: str | Path | None = None,
) -> ProjectProfile:
    """Select CLI, env, or one exact-workspace profile; never choose a default."""
    environment = os.environ if env is None else env
    if cli_profile:
        return load_project_profile(cli_profile, workspace, schema_file=schema_file)
    env_profile = environment.get("AGENT_PROFILE_DIR", "").strip()
    if env_profile:
        return load_project_profile(env_profile, workspace, schema_file=schema_file)

    common_root = Path(
        agents_dir
        or environment.get("AGENTS_DIR", "")
        or r"D:\agents"
    ).resolve()
    profiles_root = common_root / "profiles"
    matches: list[ProjectProfile] = []
    matching_errors: list[str] = []
    workspace_key = _canonical_fs_path(workspace)
    for project_file in sorted(profiles_root.glob(f"*/{PROFILE_FILENAME}")):
        try:
            raw = json.loads(project_file.read_text(encoding="utf-8"))
            raw_roots = raw.get("workspace_roots", []) if isinstance(raw, dict) else []
            if not any(_canonical_fs_path(root) == workspace_key for root in raw_roots if isinstance(root, str)):
                continue
            matches.append(
                load_project_profile(
                    project_file.parent, workspace, schema_file=schema_file
                )
            )
        except (OSError, UnicodeError, json.JSONDecodeError, ProfileError) as exc:
            matching_errors.append(f"{project_file.parent}: {type(exc).__name__}")
    if len(matches) > 1:
        raise ProfileError("PROFILE_SELECTION_AMBIGUOUS", "profiles")
    if len(matches) == 1:
        return matches[0]
    if matching_errors:
        raise ProfileError("PROFILE_DISCOVERY_INVALID", "profiles")
    raise ProfileError("PROFILE_SELECTION_MISSING", "profiles")


def profile_summary(profile: ProjectProfile, workspace: str | Path) -> list[str]:
    counts = Counter(module.kind for module in profile.modules)
    kinds = ", ".join(f"{kind}={counts[kind]}" for kind in sorted(counts)) or "none"
    rule_count = sum(len(paths) for paths in profile.rules.values())
    return [
        f"profile id: {profile.id}",
        f"schema version: {profile.schema_version}",
        f"project root: {Path(workspace).resolve()}",
        f"modules: {len(profile.modules)} ({kinds})",
        f"project rules: {'present' if profile.project_rules.is_file() else 'missing'}",
        f"analysis: {'present' if profile.analysis.is_file() else 'missing'}",
        f"ui map: {'present' if profile.ui_map and profile.ui_map.exists() else 'not configured'}",
        f"db catalog: {'present' if profile.db_catalog else 'not configured'}",
        f"feature map: {'present' if profile.feature_map else 'not configured'}",
        f"commit policy: {profile.control_policy.commit_policy}",
        f"cumulative worktree: {str(profile.control_policy.cumulative_worktree).lower()}",
        f"task-owned baseline files: {len(profile.control_policy.task_owned_baseline_files)}",
        f"external-frozen baseline files: {len(profile.control_policy.external_frozen_baseline_files)}",
        f"rules references: valid ({rule_count})",
        f"build mode: {profile.build_mode(workspace)}",
    ]
