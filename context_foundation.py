"""Deterministic, profile-resolved context packs for Harness 0.8.5.

The module deliberately stays inside the existing profile/prompt architecture:
it selects and records existing resources, but does not create a second policy
engine or delegate task normalization to a model.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable, Mapping, Sequence

from feature_context import load_feature_context
from project_profile import ProjectProfile, normalize_changed_path
from runtime_safety import scrub_secrets


MANIFEST_VERSION = 2
PROMPT_TEMPLATE_VERSION = "0.99-context-v2"
DEFAULT_CONTEXT_BUDGET = 40_000
GENERIC_OVERVIEW_WORDS = {
    "backend", "frontend", "ui", "api", "module", "sample-service", "cap", "itn", "com",
}

COMPACT_COMMON_RULES = """# Common Harness Rules
- [COMMON.SCOPE.NO_EXPANSION] Approved requirements define scope; add no unrelated work.
- [COMMON.CHANGE.NO_UNRELATED_REFACTOR] Make the smallest change; preserve project patterns.
- [COMMON.SECRET.NO_EXPOSURE] Expose or add no secret, credential, token, key, or private config.
- [COMMON.SECURITY.DESTRUCTIVE_BOUNDARY] Destructive, protected, production, external-service, and broad-data actions require explicit authority.
- [COMMON.VERIFY.EVIDENCE_REQUIRED] Claim completion only from current relevant evidence.
- [COMMON.PROFILE.EXPLICIT_SELECTION] Derive facts only from the selected valid profile and current source; never infer a profile from the workspace.
- [COMMON.FAIL.LOUD] Stop on ambiguity, missing facts, protected scope, or unverifiable high risk.
- External-harness MODIFICATION is approved only in stated scope; Planner/model prose creates no authority.
"""


class ContextError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = scrub_secrets(detail)[:500]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


class Authority(str, Enum):
    HARD_RULE = "HARD_RULE"
    PROCEDURE = "PROCEDURE"
    VERIFIED_FACT = "VERIFIED_FACT"
    REFERENCE = "REFERENCE"
    TASK_EVIDENCE = "TASK_EVIDENCE"


AUTHORITY_ORDER = {
    Authority.HARD_RULE: 0,
    Authority.VERIFIED_FACT: 1,
    Authority.TASK_EVIDENCE: 2,
    Authority.PROCEDURE: 3,
    Authority.REFERENCE: 4,
}


@dataclass(frozen=True)
class TaskSpec:
    intent: str
    target_modules: tuple[str, ...] = ()
    explicit_paths: tuple[str, ...] = ()
    screens: tuple[str, ...] = ()
    menu_ids: tuple[str, ...] = ()
    controllers: tuple[str, ...] = ()
    services: tuple[str, ...] = ()
    query_files: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()
    requested_changes: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    exclusions: tuple[str, ...] = ()
    acceptance_criteria: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()
    unknowns: tuple[str, ...] = ()

    def mapping(self) -> dict[str, Any]:
        return {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in asdict(self).items()
        }


@dataclass(frozen=True)
class ProfileFact:
    fact_id: str
    value: str
    scope: tuple[str, ...]
    evidence_paths: tuple[str, ...]
    evidence_sha256: Mapping[str, str]
    status: str = "VERIFIED"


@dataclass(frozen=True)
class ContextResource:
    path: str
    section: str
    authority: Authority
    text: str
    rule_ids: tuple[str, ...] = ()
    selection_reason: str = "ROLE_REQUIRED"
    source_sha256: str = ""
    score: int = 0
    required: bool = False

    @property
    def chars(self) -> int:
        return len(self.text)


@dataclass
class ContextPack:
    prompt: str
    manifest: dict[str, Any]
    task_spec: TaskSpec


@dataclass(frozen=True)
class ApprovalDecision:
    allowed: bool
    requires_human: bool
    code: str


def evaluate_approval(
    *,
    task_mode: str,
    manager_approved: bool,
    scope_expansion: bool = False,
    high_risk_unknown: bool = False,
    destructive_action: bool = False,
) -> ApprovalDecision:
    """Keep approval mechanics deterministic and separate from model prose."""
    if scope_expansion:
        return ApprovalDecision(False, True, "SCOPE_EXPANSION_REQUIRES_HUMAN")
    if high_risk_unknown:
        return ApprovalDecision(False, True, "HIGH_RISK_UNKNOWN_REQUIRES_HUMAN")
    if destructive_action:
        return ApprovalDecision(False, True, "DESTRUCTIVE_ACTION_REQUIRES_HUMAN")
    if task_mode == "MODIFICATION" and not manager_approved:
        return ApprovalDecision(False, True, "INTERACTIVE_MODIFICATION_REQUIRES_APPROVAL")
    return ApprovalDecision(True, False, "APPROVED_SCOPE")


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_text(value: str) -> str:
    return _sha_bytes(value.encode("utf-8"))


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _intent(requirement: str, task_mode: str) -> str:
    if task_mode in {"MODIFICATION", "ANALYSIS"}:
        return task_mode
    lowered = requirement.casefold()
    if any(word in lowered for word in ("수정", "구현", "추가", "fix", "change", "implement")):
        return "MODIFICATION"
    return "ANALYSIS"


def _normalize_explicit_path(value: str, workspace: Path | None) -> str:
    raw = value.strip("`'\".,;:()[]{}<> ").replace("\\", "/")
    if not raw:
        return ""
    if workspace:
        prefix = str(workspace.resolve()).replace("\\", "/").rstrip("/") + "/"
        if raw.casefold().startswith(prefix.casefold()):
            raw = raw[len(prefix):]
    if PureWindowsPath(raw).is_absolute() or PurePosixPath(raw).is_absolute():
        return raw
    try:
        return normalize_changed_path(raw)
    except Exception:
        return raw


def build_task_spec(
    requirement: str,
    target_modules: Sequence[str] | None = None,
    *,
    changed_files: Sequence[str] | None = None,
    task_mode: str = "",
    workspace: str | Path | None = None,
) -> TaskSpec:
    """Extract only explicit, deterministic identifiers; never infer business rules."""
    text = requirement or ""
    workspace_path = Path(workspace).resolve() if workspace else None
    path_pattern = re.compile(
        r"(?i)(?:[A-Z]:[\\/])?[\w.@+() -]+(?:[\\/][\w.@+() -]+)+"
        r"\.(?:java|xml|vue|js|jsx|ts|tsx|json|md|properties|yml|yaml|sql)"
    )
    paths = [_normalize_explicit_path(match.group(0), workspace_path) for match in path_pattern.finditer(text)]
    paths.extend(normalize_changed_path(path) for path in (changed_files or []) if path)
    identifiers = re.findall(r"\b[A-Z][A-Za-z0-9_]{3,}\b", text)
    filenames = [Path(path.replace("/", os.sep)).name for path in paths]
    names = _unique([*identifiers, *filenames])
    screens = _unique(
        name for name in names
        if re.fullmatch(r"[A-Z][A-Za-z0-9]*(?:List|View|Detail|Popup|Pop|Form)", name)
        or re.fullmatch(r"[A-Z][A-Za-z]{2,}\d{4}", name)
    )
    menu_ids = _unique(re.findall(r"(?<![A-Z0-9])M\d{5,8}(?![A-Z0-9])", text, re.IGNORECASE))
    controllers = _unique(name for name in names if name.casefold().endswith(("ctr", "controller")))
    services = _unique(name for name in names if name.casefold().endswith(("svc", "service")))
    query_files = _unique(
        name for name in names
        if name.casefold().endswith(("qry.xml", "mapper.xml"))
    )
    tables = _unique(
        value.upper() for value in re.findall(
            r"(?<![A-Z0-9_])(?:(?:SAMPLE_RESOURCE|SAMPLE_PORTAL)\d{3,5}(?:_STG)?|CAP[A-Z]?\d{3,5})(?![A-Z0-9_])",
            text,
            re.IGNORECASE,
        )
    )
    lines = [line.strip(" -*\t") for line in text.splitlines() if line.strip()]
    constraints = _unique(
        line for line in lines
        if any(word in line.casefold() for word in ("반드시", "금지", "하지 마", "must", "never", "only", "no "))
    )
    exclusions = _unique(
        line for line in lines
        if any(word in line.casefold() for word in ("제외", "금지", "하지 마", "do not", "exclude", "without"))
    )
    acceptance = _unique(
        line for line in lines
        if any(word in line.casefold() for word in ("pass", "통과", "확인", "검증", "완료 조건", "acceptance"))
    )
    risks: list[str] = []
    lowered = text.casefold()
    if any(token in lowered for token in ("web.xml", "session", "security", "pii", "개인정보", "권한")):
        risks.append("HIGH_RISK_BOUNDARY")
    if any(token in lowered for token in ("ddl", "dml", "delete", "drop", "운영 db", "live db")):
        risks.append("DATA_MUTATION")
    if paths and any(path.casefold().endswith("web.xml") for path in paths):
        risks.append("PROTECTED_PATH")
    requested = _unique(
        line for line in lines
        if any(word in line.casefold() for word in ("수정", "구현", "추가", "변경", "fix", "implement", "change", "add"))
    )
    return TaskSpec(
        intent=_intent(text, task_mode),
        target_modules=_unique(target_modules or ()),
        explicit_paths=_unique(paths),
        screens=screens,
        menu_ids=menu_ids,
        controllers=controllers,
        services=services,
        query_files=query_files,
        tables=tables,
        requested_changes=requested,
        constraints=constraints,
        exclusions=exclusions,
        acceptance_criteria=acceptance,
        risk_tags=_unique(risks),
        unknowns=(),
    )


def validate_profile_fact(fact: ProfileFact, roots: Sequence[Path]) -> ProfileFact:
    root_values = [root.resolve() for root in roots]
    status = "VERIFIED"
    for display_path, expected in fact.evidence_sha256.items():
        candidates = []
        if display_path.startswith("profile:"):
            candidates = [root / display_path[8:] for root in root_values]
        elif display_path.startswith("workspace:"):
            candidates = [root / display_path[10:] for root in root_values]
        else:
            candidates = [Path(display_path)]
        path = next((candidate for candidate in candidates if candidate.is_file()), None)
        if path is None or _sha_bytes(path.read_bytes()) != expected:
            status = "STALE"
            break
    return ProfileFact(**{**asdict(fact), "status": status})


def resolve_profile_resource(
    profile: ProjectProfile,
    name: str,
    *,
    legacy_root: str | Path | None = None,
) -> tuple[Path, list[str]]:
    """Resolve canonical profile context and make any legacy fallback visible."""
    aliases = {"analysis": profile.analysis, "ui_map": profile.ui_map, "project_rules": profile.project_rules}
    path = aliases.get(name)
    if path is not None and Path(path).exists():
        return Path(path).resolve(), []
    if legacy_root is None:
        raise ContextError("CONTEXT_RESOURCE_UNREADABLE", name)
    fallback = Path(legacy_root).resolve() / ("PROJECT_ANALYSIS.md" if name == "analysis" else name)
    if not fallback.exists():
        raise ContextError("CONTEXT_RESOURCE_UNREADABLE", name)
    return fallback, [f"LEGACY_CONTEXT_FALLBACK:{name}"]


def _display_path(path: Path, profile: ProjectProfile, policy_root: Path, workspace: Path) -> str:
    resolved = path.resolve()
    for prefix, root in (
        ("profile:", profile.profile_dir.resolve()),
        ("policy:", policy_root.resolve()),
        ("workspace:", workspace.resolve()),
    ):
        try:
            return prefix + resolved.relative_to(root).as_posix()
        except ValueError:
            continue
    return "resource:" + path.name


def _resource(
    path: Path,
    *,
    profile: ProjectProfile,
    policy_root: Path,
    workspace: Path,
    authority: Authority,
    section: str,
    reason: str,
    rule_ids: Sequence[str] = (),
    score: int = 0,
    required: bool = False,
    text: str | None = None,
) -> ContextResource:
    try:
        raw = path.read_text(encoding="utf-8") if text is None else text
        source_hash = _sha_bytes(path.read_bytes())
    except (OSError, UnicodeError) as exc:
        raise ContextError("CONTEXT_RESOURCE_UNREADABLE", str(path)) from exc
    if not raw.strip():
        raise ContextError("CONTEXT_RESOURCE_UNREADABLE", str(path))
    return ContextResource(
        path=_display_path(path, profile, policy_root, workspace),
        section=section,
        authority=authority,
        text=raw.strip(),
        rule_ids=tuple(rule_ids),
        selection_reason=reason,
        source_sha256=source_hash,
        score=score,
        required=required,
    )


def _virtual_resource(
    path: str,
    text: str,
    authority: Authority,
    *,
    section: str,
    reason: str,
    rule_ids: Sequence[str] = (),
    score: int = 0,
    required: bool = False,
) -> ContextResource:
    return ContextResource(
        path=path,
        section=section,
        authority=authority,
        text=text.strip(),
        rule_ids=tuple(rule_ids),
        selection_reason=reason,
        source_sha256=_sha_text(text.strip()),
        score=score,
        required=required,
    )


def _matching_sections(path: Path, tokens: Sequence[str], max_sections: int = 4) -> list[tuple[str, str]]:
    text = path.read_text(encoding="utf-8")
    headings = list(re.finditer(r"(?m)^#{1,4}\s+(.+)$", text))
    if not headings:
        return []
    exact_tokens = [token for token in _unique(tokens) if token.casefold() not in GENERIC_OVERVIEW_WORDS and len(token) >= 4]
    if not exact_tokens:
        return []
    matches: list[tuple[int, int, str, str]] = []
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        section = text[heading.start():end].strip()
        folded = section.casefold()
        hit_count = sum(1 for token in exact_tokens if token.casefold() in folded)
        if hit_count:
            matches.append((hit_count, -index, heading.group(1).strip(), section))
    matches.sort(reverse=True)
    return [(heading, section) for _, _, heading, section in matches[:max_sections]]


def _named_markdown_sections(path: Path, names: Sequence[str]) -> str:
    text = path.read_text(encoding="utf-8")
    headings = list(re.finditer(r"(?m)^#{1,4}\s+(.+)$", text))
    selected: list[str] = []
    wanted = [name.casefold() for name in names]
    for index, heading in enumerate(headings):
        title = heading.group(1).strip().casefold()
        if not any(name in title for name in wanted):
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        selected.append(text[heading.start():end].strip())
    return "\n\n".join(selected)


def _compact_rule_document(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    return "\n".join(
        line for line in lines
        if line.startswith("#") or line.lstrip().startswith("-")
    ).strip()


def _procedure_sections(path: Path, name: str) -> str:
    names = (
        ("profile resolution", "selective loading", "evidence rules", "automation behavior")
        if name == "project-context"
        else ("workflow", "checklist", "action", "strategy", "rules", "authorization modes", "execution modes")
    )
    selected = _named_markdown_sections(path, names)
    return selected or path.read_text(encoding="utf-8").strip()


def _profile_fact_resources(
    profile: ProjectProfile,
    workspace: Path,
    policy_root: Path,
    modules: Sequence[str],
) -> list[ContextResource]:
    wanted = {name.casefold() for name in modules}
    selected = [module for module in profile.modules if not wanted or module.name.casefold() in wanted]
    if not selected:
        return []
    project_json = profile.profile_dir / "project.json"
    project_hash = _sha_bytes(project_json.read_bytes())
    fact_records: list[ProfileFact] = []

    def add_fact(
        fact_id: str,
        value: str,
        scope: Sequence[str],
        evidence: Mapping[str, str],
    ) -> None:
        fact_records.append(ProfileFact(
            fact_id=fact_id,
            value=value,
            scope=tuple(scope),
            evidence_paths=tuple(evidence),
            evidence_sha256=dict(evidence),
        ))

    profile_evidence = {"profile:project.json": project_hash}
    for module in selected:
        module_root = workspace / Path(*PurePosixPath(module.path).parts)
        config = module_root / ("package.json" if module.kind == "frontend" else "pom.xml")
        scope = (module.name,)
        add_fact(f"module.{module.name}.type", module.kind, scope, profile_evidence)
        add_fact(f"module.{module.name}.root", module.path, scope, profile_evidence)
        add_fact(f"module.{module.name}.protected", str(module.protected).lower(), scope, profile_evidence)
        if module.build_argv:
            add_fact(
                f"module.{module.name}.build_command",
                f"cwd={module.build_cwd}; argv={json.dumps(list(module.build_argv), ensure_ascii=False)}",
                scope,
                profile_evidence,
            )
        if config.is_file():
            config_hash = _sha_bytes(config.read_bytes())
            config_evidence = {
                **profile_evidence,
                f"workspace:{config.relative_to(workspace).as_posix()}": config_hash,
            }
            if config.name == "package.json":
                try:
                    package = json.loads(config.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise ContextError("PROFILE_SOURCE_MISMATCH", str(config)) from exc
                dependencies = {**dict(package.get("dependencies") or {}), **dict(package.get("devDependencies") or {})}
                for name in ("vue", "quasar"):
                    if name in dependencies:
                        add_fact(f"module.{module.name}.frontend_framework.{name}", str(dependencies[name]), scope, config_evidence)
                bundler = next((name for name in ("@quasar/app-vite", "vite") if name in dependencies), "")
                if bundler:
                    add_fact(f"module.{module.name}.bundler", f"vite via {bundler}={dependencies[bundler]}", scope, config_evidence)
                if package.get("packageManager"):
                    add_fact(f"module.{module.name}.package_manager", str(package["packageManager"]), scope, config_evidence)
                elif module.build_argv:
                    add_fact(f"module.{module.name}.package_manager", str(module.build_argv[0]), scope, config_evidence)
            else:
                pom = config.read_text(encoding="utf-8", errors="replace")
                for key in ("maven.compiler.release", "maven.compiler.source", "maven.compiler.target", "java.version"):
                    match = re.search(rf"<{re.escape(key)}>([^<]+)</{re.escape(key)}>", pom)
                    if match:
                        add_fact(f"module.{module.name}.{key}", match.group(1).strip(), scope, config_evidence)
                namespaces = [name for name in ("jakarta", "javax") if f"{name}." in pom]
                if namespaces:
                    add_fact(f"module.{module.name}.java_namespace", "+".join(namespaces), scope, config_evidence)
                add_fact(
                    f"module.{module.name}.spring_boot",
                    str("spring-boot" in pom).lower(),
                    scope,
                    config_evidence,
                )

    project_rules_hash = _sha_bytes(profile.project_rules.read_bytes())
    profile_rule_evidence = {
        "profile:project.json": project_hash,
        f"profile:{profile.project_rules.relative_to(profile.profile_dir).as_posix()}": project_rules_hash,
    }
    module_prefixes = tuple(module.path.rstrip("/") + "/" for module in selected)
    relevant_protected = [
        path for path in profile.protected_paths
        if any(path.casefold().startswith(prefix.casefold()) for prefix in module_prefixes)
    ]
    if relevant_protected:
        add_fact("profile.protected_paths", json.dumps(relevant_protected, ensure_ascii=False), (profile.id,), profile_evidence)
    add_fact("profile.ui_com_boundary", "UI shared functions use the configured common-path boundary", (profile.id, "frontend"), profile_rule_evidence)
    sql_hook = profile.profile_dir / "hooks" / "sql.md"
    if sql_hook.is_file():
        add_fact(
            "profile.db_dialect",
            "existing relational database/vendor-specific SQL",
            (profile.id, "sql"),
            {f"profile:{sql_hook.relative_to(profile.profile_dir).as_posix()}": _sha_bytes(sql_hook.read_bytes())},
        )
    evidence_catalog: dict[str, tuple[str, str]] = {}
    evidence_alias: dict[str, str] = {}
    for fact in fact_records:
        for path, sha256 in fact.evidence_sha256.items():
            if path not in evidence_alias:
                alias = f"E{len(evidence_alias) + 1}"
                evidence_alias[path] = alias
                evidence_catalog[alias] = (path, sha256)
    lines = ["[Verified project facts]"]
    lines.extend(
        f"evidence {alias}: {path}; sha256={sha256}"
        for alias, (path, sha256) in evidence_catalog.items()
    )
    lines.extend(
        "fact " + fact.fact_id + "=" + fact.value
        + "; scope=" + ",".join(fact.scope)
        + "; evidence=" + ",".join(evidence_alias[path] for path in fact.evidence_paths)
        + "; status=" + fact.status
        for fact in fact_records
    )
    text = "\n".join(lines)
    return [_virtual_resource(
        "derived:profile-facts",
        text,
        Authority.VERIFIED_FACT,
        section="module-build-facts",
        reason="TARGET_MODULE_FALLBACK",
        score=700,
    )]


def deduplicate_resources(
    resources: Sequence[ContextResource],
) -> tuple[list[ContextResource], list[str], list[dict[str, Any]]]:
    selected: list[ContextResource] = []
    rule_values: dict[str, tuple[Authority, str]] = {}
    content_seen: dict[tuple[Authority, str], str] = {}
    deduped: list[str] = []
    conflicts: list[dict[str, Any]] = []
    ordered = sorted(
        enumerate(resources),
        key=lambda item: (AUTHORITY_ORDER[item[1].authority], item[0]),
    )
    for _, resource in ordered:
        normalized = re.sub(r"\s+", " ", resource.text).strip().casefold()
        active_rule_ids: list[str] = []
        for rule_id in resource.rule_ids:
            prior = rule_values.get(rule_id)
            current = (resource.authority, normalized)
            if prior and prior[0] == resource.authority and prior[1] != normalized:
                conflicts.append({"rule_id": rule_id, "paths": [next((item.path for item in selected if rule_id in item.rule_ids), ""), resource.path]})
            elif prior:
                # The higher-authority copy was selected first. A lower
                # authority resource cannot redefine or duplicate its rule.
                deduped.append(rule_id)
            else:
                rule_values[rule_id] = current
                active_rule_ids.append(rule_id)
        if resource.rule_ids and not active_rule_ids:
            continue
        if tuple(active_rule_ids) != resource.rule_ids:
            resource = ContextResource(**{
                **resource.__dict__,
                "rule_ids": tuple(active_rule_ids),
            })
        content_key = (resource.authority, _sha_text(normalized))
        if content_key in content_seen:
            deduped.extend(resource.rule_ids or (resource.path,))
            continue
        content_seen[content_key] = resource.path
        selected.append(resource)
    if conflicts:
        raise ContextError("CONTEXT_RULE_CONFLICT", json.dumps(conflicts, ensure_ascii=False))
    return selected, list(dict.fromkeys(deduped)), conflicts


def _role_skill(role: str) -> str:
    return {
        "WORKER": "project-context",
        "PLANNER": "planning",
        "REVIEWER": "review",
        "TESTER": "testing",
    }[role]


def _collect_resources(
    role: str,
    requirement: str,
    spec: TaskSpec,
    profile: ProjectProfile,
    workspace: Path,
    policy_root: Path,
    changed_files: Sequence[str],
    task_evidence: Mapping[str, Any] | None,
) -> tuple[list[ContextResource], list[dict[str, Any]], list[str]]:
    resources: list[ContextResource] = []
    omitted: list[dict[str, Any]] = []
    warnings: list[str] = []
    actual_files = list(changed_files)
    area_inputs = actual_files or list(spec.explicit_paths)
    area_evidence = profile.detect_areas(requirement, spec.target_modules, area_inputs)
    common_agents = policy_root / "AGENTS.md"
    validation = policy_root / "hooks" / "validation.md"
    profile_sections = [
        "source of truth", "scope and automation", "modules and boundaries",
        "protected and high-risk", "stop conditions",
    ]
    if "frontend" in area_evidence.areas:
        profile_sections.extend(("frontend conventions", "menu url and ui map"))
    if any(area in area_evidence.areas for area in ("backend", "sql", "platform", "architecture")):
        profile_sections.append("backend conventions")
    compact_profile_rules = (
        _named_markdown_sections(profile.project_rules, profile_sections)
        or profile.project_rules.read_text(encoding="utf-8").strip()
    )
    common_raw = common_agents.read_text(encoding="utf-8").strip()
    compact_common = (
        common_raw + "\n" + COMPACT_COMMON_RULES
        if len(common_raw) < 200
        else COMPACT_COMMON_RULES
    )
    common_rule_ids = [
        "COMMON.SCOPE.NO_EXPANSION", "COMMON.CHANGE.NO_UNRELATED_REFACTOR",
        "COMMON.SECRET.NO_EXPOSURE", "COMMON.VERIFY.EVIDENCE_REQUIRED",
    ]
    if not validation.is_file():
        common_rule_ids.extend((
            "COMMON.SECURITY.DESTRUCTIVE_BOUNDARY",
            "COMMON.VERIFY.COMPLETION_GATE",
        ))
    resources.extend((
        _resource(common_agents, profile=profile, policy_root=policy_root, workspace=workspace,
                  authority=Authority.HARD_RULE, section="common-rules", reason="HARD_RULE_ALWAYS",
                  rule_ids=tuple(common_rule_ids), score=1000, required=True,
                  text=compact_common),
        _resource(profile.project_rules, profile=profile, policy_root=policy_root, workspace=workspace,
                  authority=Authority.HARD_RULE, section="profile-rules", reason="HARD_RULE_ALWAYS",
                  rule_ids=(f"{profile.id.upper()}.SCOPE.PROFILE", f"{profile.id.upper()}.BUSINESS.UNKNOWN_REQUIRES_HUMAN"), score=980, required=True,
                  text=compact_profile_rules),
    ))
    if validation.is_file():
        compact_validation = (
            _compact_rule_document(validation)
            or validation.read_text(encoding="utf-8").strip()
        )
        resources.append(_resource(
            validation, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=Authority.HARD_RULE, section="validation", reason="HARD_RULE_ALWAYS",
            rule_ids=("COMMON.SECURITY.DESTRUCTIVE_BOUNDARY", "COMMON.VERIFY.COMPLETION_GATE"),
            score=990, required=True, text=compact_validation,
        ))
    else:
        warnings.append("CONTEXT_POLICY_COMPAT_FALLBACK:common_validation")
    for path in profile.rules.get("always", ()):
        compact_always = (
            _compact_rule_document(path)
            or path.read_text(encoding="utf-8").strip()
        )
        resources.append(_resource(
            path, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=Authority.HARD_RULE, section=path.stem,
            reason="HARD_RULE_ALWAYS",
            rule_ids=(f"{profile.id.upper()}.ALWAYS.{path.stem.upper().replace('-', '_')}",),
            score=970, required=True, text=compact_always,
        ))

    task_contract = json.dumps(spec.mapping(), ensure_ascii=False, sort_keys=True, indent=2)
    resources.append(_virtual_resource(
        "task:approved-contract", requirement + "\n\n[Deterministic TaskSpec]\n" + task_contract,
        Authority.TASK_EVIDENCE, section="approved-task-contract",
        reason="ROLE_REQUIRED", score=950, required=True,
    ))
    permission_text = (
        (
            "Approved external-harness MODIFICATION may proceed within the exact contract without "
            "generic confirmation. Scope expansion, destructive/protected changes, and unresolved "
            "high risk still require human authority."
            if spec.intent == "MODIFICATION"
            else
            "This ANALYSIS task is read-only unless the approved task contract explicitly grants a mutation."
        )
    )
    resources.append(_virtual_resource(
        "policy:tool-permission-envelope", permission_text,
        Authority.HARD_RULE, section="tool-permission-envelope",
        reason="HARD_RULE_ALWAYS", rule_ids=("COMMON.AUTHORIZATION.ENVELOPE",),
        score=995, required=True,
    ))
    resources.append(_virtual_resource(
        "policy:completion-contract",
        "Report current Build/Test/Review evidence. PASS, FAIL, NOT_REQUIRED, UNAVAILABLE, and "
        "NOT_RUN_DUE_TO_PRIOR_GATE are distinct. Missing evidence, model prose, and restored state are not success.",
        Authority.HARD_RULE, section="completion-contract",
        reason="HARD_RULE_ALWAYS", rule_ids=("COMMON.VERIFY.COMPLETION_CONTRACT",),
        score=994, required=True,
    ))

    skill_names = ["project-context"]
    role_skill = _role_skill(role)
    if role_skill not in skill_names:
        skill_names.append(role_skill)
    if role == "WORKER":
        for area in area_evidence.areas:
            if area in {"backend", "frontend", "sql"} and area not in skill_names:
                skill_names.append(area)
    for name in skill_names:
        skill_path = policy_root / "skills" / name / "SKILL.md"
        if not skill_path.is_file():
            if name == "project-context":
                raise ContextError("CONTEXT_RESOURCE_UNREADABLE", str(skill_path))
            warnings.append(f"CONTEXT_POLICY_COMPAT_FALLBACK:role_skill:{name}")
            continue
        resources.append(_resource(
            skill_path, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=Authority.PROCEDURE, section=name,
            reason="ROLE_REQUIRED", rule_ids=(f"PROCEDURE.{role}.{name.upper().replace('-', '_')}",),
            score=800 if name == role_skill else 760,
            text=_procedure_sections(skill_path, name),
        ))

    evidence = area_evidence
    selected_rule_paths: list[Path] = []
    for area in evidence.areas:
        selected_rule_paths.extend(profile.rules.get(area, ()))
    for path in dict.fromkeys(selected_rule_paths):
        is_reference = "references" in {part.casefold() for part in path.parts}
        authority = Authority.REFERENCE if is_reference else Authority.HARD_RULE
        reason = "CHANGED_FILE_EXTENSION" if actual_files else "TARGET_MODULE_FALLBACK"
        resources.append(_resource(
            path, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=authority, section=path.stem, reason=reason,
            rule_ids=((f"{profile.id.upper()}.{path.stem.upper().replace('-', '_')}",) if authority == Authority.HARD_RULE else ()),
            score=850 if authority == Authority.HARD_RULE else 600,
            required=authority == Authority.HARD_RULE,
        ))

    exact_tokens = [
        *spec.explicit_paths, *spec.screens, *spec.menu_ids, *spec.controllers,
        *spec.services, *spec.query_files, *spec.tables,
    ]
    exact_tokens.extend(
        token for token in re.findall(r"\b[A-Z][A-Za-z0-9_]{3,}\b", requirement)
        if token.casefold() not in GENERIC_OVERVIEW_WORDS | {
            "build", "test", "review", "worker", "planner", "tester", "pass",
        }
    )
    if exact_tokens:
        exact_tokens.extend(
            module for module in spec.target_modules
            if module.casefold() not in GENERIC_OVERVIEW_WORDS
        )
    exact_tokens.extend(Path(path.replace("/", os.sep)).name for path in spec.explicit_paths)
    for heading, section_text in _matching_sections(profile.analysis, exact_tokens, max_sections=2):
        resources.append(_resource(
            profile.analysis, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=Authority.VERIFIED_FACT, section=heading,
            reason=("EXPLICIT_PATH_MATCH" if spec.explicit_paths else "SCREEN_EXACT_MATCH" if spec.screens else "TABLE_EXACT_MATCH"),
            score=740, text=section_text,
        ))
    if not exact_tokens:
        omitted.append({"path": _display_path(profile.analysis, profile, policy_root, workspace), "reason": "LOW_RELEVANCE_OMITTED"})

    if profile.ui_map:
        ui_tokens = [*spec.screens, *spec.menu_ids]
        if ui_tokens:
            for path in sorted(profile.ui_map.rglob("*.md")):
                sections = _matching_sections(path, ui_tokens, max_sections=2)
                for heading, section_text in sections:
                    resources.append(_resource(
                        path, profile=profile, policy_root=policy_root, workspace=workspace,
                        authority=Authority.VERIFIED_FACT, section=heading,
                        reason="SCREEN_EXACT_MATCH", score=780, text=section_text,
                    ))

    # Exact identifiers may live in profile-local reference documents rather
    # than the broad analysis. Select only matching sections and never the
    # entire reference tree.
    selected_rule_keys = {path.resolve() for path in selected_rule_paths}
    reference_matches = 0
    references_root = profile.profile_dir / "references"
    if exact_tokens and references_root.is_dir():
        for path in sorted(references_root.rglob("*.md")):
            if path.resolve() in selected_rule_keys:
                continue
            for heading, section_text in _matching_sections(path, exact_tokens, max_sections=2):
                resources.append(_resource(
                    path, profile=profile, policy_root=policy_root, workspace=workspace,
                    authority=Authority.VERIFIED_FACT, section=heading,
                    reason="TABLE_EXACT_MATCH" if spec.tables else "EXPLICIT_PATH_MATCH",
                    score=770, text=section_text,
                ))
                reference_matches += 1
                if reference_matches >= 4:
                    break
            if reference_matches >= 4:
                break

    if profile.db_catalog:
        table_ids = list(profile.db_catalog.table_ids_for_text("\n".join((requirement, *actual_files))))
        if spec.screens:
            table_ids.extend(profile.db_catalog.table_ids_for_screens(spec.screens))
        table_ids = list(dict.fromkeys(table_ids))
        if table_ids:
            rendered = profile.db_catalog.render_context(table_ids, requirement, max_chars=9000)
            resources.append(_resource(
                profile.db_catalog.catalog_path, profile=profile, policy_root=policy_root, workspace=workspace,
                authority=Authority.VERIFIED_FACT, section=",".join(table_ids),
                reason="TABLE_EXACT_MATCH", score=790,
                text="[선택된 DB 메타데이터]\n" + rendered,
            ))

    feature = load_feature_context(requirement, changed_files=actual_files, profile=profile, cap=4500)
    if feature and profile.feature_map:
        resources.append(_resource(
            profile.feature_map, profile=profile, policy_root=policy_root, workspace=workspace,
            authority=Authority.REFERENCE, section="matched-feature",
            reason="FEATURE_MAP_EXACT_MATCH", score=710, text=feature,
        ))

    resources.extend(_profile_fact_resources(profile, workspace, policy_root, spec.target_modules))
    if task_evidence:
        safe_evidence = {
            key: task_evidence[key]
            for key in sorted(task_evidence)
            if key in {
                "changed_files", "task_owned_changed_files", "inherited_batch_delta_files",
                "build_evidence", "test_status", "test_summary", "review_findings",
                "failure_code", "failure_fingerprint", "patch_failure_target",
            }
        }
        resources.append(_virtual_resource(
            "task:actual-evidence", json.dumps(safe_evidence, ensure_ascii=False, sort_keys=True, indent=2),
            Authority.TASK_EVIDENCE, section="task-evidence",
            reason="CHANGED_FILE_EXTENSION" if actual_files else "ROLE_REQUIRED", score=900,
        ))
    return resources, omitted, warnings


def _apply_budget(
    resources: Sequence[ContextResource],
    budget: int,
) -> tuple[list[ContextResource], list[dict[str, Any]]]:
    ordered = sorted(
        resources,
        key=lambda item: (
            AUTHORITY_ORDER[item.authority], -int(item.required), -item.score,
            item.path.casefold(), item.section.casefold(),
        ),
    )
    required_chars = sum(item.chars for item in ordered if item.required)
    if required_chars > budget:
        raise ContextError("CONTEXT_BUDGET_EXCEEDED", f"required={required_chars};budget={budget}")
    selected: list[ContextResource] = []
    omitted: list[dict[str, Any]] = []
    used = 0
    for item in ordered:
        if item.required or used + item.chars <= budget:
            selected.append(item)
            used += item.chars
        else:
            omitted.append({"path": item.path, "section": item.section, "reason": "CONTEXT_BUDGET_LOW_PRIORITY_OMITTED", "chars": item.chars})
    return selected, omitted


def _render(resources: Sequence[ContextResource]) -> str:
    section_names = {
        Authority.TASK_EVIDENCE: "Task Evidence",
        Authority.HARD_RULE: "Hard Rules",
        Authority.VERIFIED_FACT: "Verified Project Facts",
        Authority.PROCEDURE: "Role Procedure",
        Authority.REFERENCE: "Task-Specific References",
    }
    groups: dict[Authority, list[str]] = {authority: [] for authority in Authority}
    approved_contract: list[str] = []
    permission_envelope: list[str] = []
    completion_contract: list[str] = []
    for item in resources:
        rendered = f"[{item.path}#{item.section}]\n{item.text}"
        if item.section == "approved-task-contract":
            approved_contract.append(rendered)
        elif item.section == "tool-permission-envelope":
            permission_envelope.append(rendered)
        elif item.section == "completion-contract":
            completion_contract.append(rendered)
        else:
            groups[item.authority].append(rendered)
    order = (
        Authority.HARD_RULE, Authority.VERIFIED_FACT, Authority.PROCEDURE,
        Authority.REFERENCE, Authority.TASK_EVIDENCE,
    )
    blocks = []
    if approved_contract:
        blocks.append("[Approved Task Contract]\n" + "\n\n---\n\n".join(approved_contract))
    for authority in order:
        if groups[authority]:
            blocks.append(f"[{section_names[authority]}]\n" + "\n\n---\n\n".join(groups[authority]))
    if permission_envelope:
        blocks.append("[Tool Permission Envelope]\n" + "\n\n---\n\n".join(permission_envelope))
    if completion_contract:
        blocks.append("[Completion Contract]\n" + "\n\n---\n\n".join(completion_contract))
    return "\n\n".join(blocks)


def _surface_sha(paths: Sequence[Path]) -> str:
    records = []
    for path in sorted(dict.fromkeys(path.resolve() for path in paths), key=lambda value: str(value).casefold()):
        records.append({"name": path.name, "sha256": _sha_bytes(path.read_bytes())})
    return _sha_bytes(_json_bytes(records))


def build_context_pack(
    *,
    role: str,
    requirement: str,
    profile: ProjectProfile,
    workspace: str | Path,
    policy_root: str | Path,
    target_modules: Sequence[str] | None = None,
    changed_files: Sequence[str] | None = None,
    task_mode: str = "",
    task_id: str = "",
    job_id: str = "",
    task_evidence: Mapping[str, Any] | None = None,
    budget: int = DEFAULT_CONTEXT_BUDGET,
    context_tier: str = "DOMAIN",
) -> ContextPack:
    role = role.upper()
    if role not in {"WORKER", "PLANNER", "REVIEWER", "TESTER"}:
        raise ContextError("CONTEXT_ROLE_INVALID", role)
    workspace_path = Path(workspace).resolve()
    policy_path = Path(policy_root).resolve()
    spec = build_task_spec(
        requirement, target_modules, changed_files=changed_files,
        task_mode=task_mode, workspace=workspace_path,
    )
    resources, pre_omitted, warnings = _collect_resources(
        role, requirement, spec, profile, workspace_path, policy_path,
        list(changed_files or ()), task_evidence,
    )
    deduped, dedup_ids, conflicts = deduplicate_resources(resources)
    tier = str(context_tier or "DOMAIN").upper()
    if role == "REVIEWER":
        if tier == "DIFF_ONLY":
            deduped = [item for item in deduped if item.required]
        elif tier == "LOCAL":
            deduped = [
                item for item in deduped
                if item.required or item.authority in {Authority.TASK_EVIDENCE, Authority.PROCEDURE}
            ]
        elif tier == "LOCAL_IMPACT":
            deduped = [
                item for item in deduped
                if item.required or item.authority != Authority.REFERENCE
            ]
        elif tier != "DOMAIN":
            raise ContextError("CONTEXT_TIER_INVALID", tier)
    selected, budget_omitted = _apply_budget(deduped, budget)
    prompt = _render(selected)
    profile_paths = [path for path in profile.profile_dir.rglob("*") if path.is_file()]
    policy_paths = [
        policy_path / "AGENTS.md", policy_path / "hooks" / "validation.md",
        *(policy_path / "skills" / name / "SKILL.md" for name in ("project-context", _role_skill(role))),
    ]
    profile_sha = _surface_sha(profile_paths)
    policy_sha = _surface_sha([path for path in policy_paths if path.is_file()])
    task_spec_sha = _sha_bytes(_json_bytes(spec.mapping()))
    stable_resources = [item for item in selected if item.authority != Authority.TASK_EVIDENCE]
    dynamic_resources = [item for item in selected if item.authority == Authority.TASK_EVIDENCE]
    stable_input = {
        "prompt_template_version": PROMPT_TEMPLATE_VERSION,
        "role": role,
        "context_tier": tier,
        "selected": [
            [item.path, item.section, item.source_sha256, item.authority.value]
            for item in stable_resources
        ],
        "profile_snapshot_sha256": profile_sha,
        "policy_snapshot_sha256": policy_sha,
    }
    dynamic_input = {
        "task_spec_sha256": task_spec_sha,
        "selected": [
            [item.path, item.section, item.source_sha256, item.authority.value]
            for item in dynamic_resources
        ],
    }
    stable_context_sha = _sha_bytes(_json_bytes(stable_input))
    dynamic_context_sha = _sha_bytes(_json_bytes(dynamic_input))
    context_pack_id = "CTX-" + _sha_bytes(_json_bytes({
        "stable": stable_context_sha, "dynamic": dynamic_context_sha,
    }))[:24].upper()
    authority_chars = {
        authority.value: sum(item.chars for item in selected if item.authority == authority)
        for authority in Authority
    }
    manifest = {
        "context_pack_id": context_pack_id,
        "manifest_version": MANIFEST_VERSION,
        "prompt_template_version": PROMPT_TEMPLATE_VERSION,
        "role": role,
        "task_id": task_id,
        "job_id": job_id,
        "profile_id": profile.id,
        "profile_snapshot_sha256": profile_sha,
        "policy_snapshot_sha256": policy_sha,
        "requirement_sha256": _sha_text(requirement),
        "task_spec_sha256": task_spec_sha,
        "stable_context_sha256": stable_context_sha,
        "dynamic_context_sha256": dynamic_context_sha,
        "context_layers": {
            "stable": [item.path for item in stable_resources],
            "dynamic": [item.path for item in dynamic_resources],
        },
        "selected_resources": [
            {
                "path": item.path,
                "section": item.section,
                "authority": item.authority.value,
                "rule_ids": list(item.rule_ids),
                "selection_reason": item.selection_reason,
                "source_sha256": item.source_sha256,
                "chars": item.chars,
            }
            for item in selected
        ],
        "omitted_resources": [*pre_omitted, *budget_omitted],
        "deduplicated_rule_ids": dedup_ids,
        "conflicts": conflicts,
        "total_chars": len(prompt),
        "authority_chars": authority_chars,
        "context_lifecycle": {
            "AVAILABLE": len(deduped),
            "SELECTED": len(selected),
            "DELIVERED": len(selected),
            "ACCESSED": "NOT_CAPTURED",
        },
        "warnings": warnings,
    }
    return ContextPack(prompt=prompt, manifest=manifest, task_spec=spec)


def persist_context_manifest(
    pack: ContextPack,
    task: Any,
    manifest_root: str | Path,
) -> Path:
    root = Path(manifest_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    task_id = scrub_secrets(str(getattr(task, "task_id", "TASK-UNKNOWN")))
    safe_task_id = re.sub(r"[^A-Za-z0-9_.-]", "_", task_id)
    role = str(pack.manifest["role"]).casefold()
    filename = f"{safe_task_id}-{role}-{pack.manifest['context_pack_id']}.json"
    destination = root / filename
    payload = json.dumps(pack.manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
    fd, temp_name = tempfile.mkstemp(prefix=filename + ".", suffix=".tmp", dir=str(root))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, destination)
    finally:
        try:
            Path(temp_name).unlink(missing_ok=True)
        except OSError:
            pass
    task.context_pack_id = str(pack.manifest["context_pack_id"])
    task.context_manifest_path = destination.as_posix()
    task.context_total_chars = int(pack.manifest["total_chars"])
    task.context_selected_count = len(pack.manifest["selected_resources"])
    task.context_warnings = list(pack.manifest["warnings"])
    task.prompt_template_version = str(pack.manifest["prompt_template_version"])
    return destination


def build_and_record_context(
    *,
    task: Any,
    role: str,
    profile: ProjectProfile,
    workspace: str | Path,
    policy_root: str | Path,
    manifest_root: str | Path | None,
    requirement: str | None = None,
    changed_files: Sequence[str] | None = None,
    task_evidence: Mapping[str, Any] | None = None,
    budget: int = DEFAULT_CONTEXT_BUDGET,
    context_tier: str = "DOMAIN",
) -> ContextPack:
    pack = build_context_pack(
        role=role,
        requirement=requirement if requirement is not None else str(task.requirement),
        profile=profile,
        workspace=workspace,
        policy_root=policy_root,
        target_modules=list(getattr(task, "target_module", []) or []),
        changed_files=list(changed_files if changed_files is not None else getattr(task, "changed_files", []) or []),
        task_mode=str(getattr(task, "task_mode", "")),
        task_id=str(getattr(task, "task_id", "")),
        job_id=str(getattr(task, "job_id", "")),
        task_evidence=task_evidence,
        budget=budget,
        context_tier=context_tier,
    )
    if manifest_root is not None:
        persist_context_manifest(pack, task, manifest_root)
    return pack


def manifest_has_sensitive_payload(manifest: Mapping[str, Any]) -> bool:
    serialized = json.dumps(manifest, ensure_ascii=False).casefold()
    forbidden_keys = ("prompt_body", "environment", "credential", "private_key", "access_token")
    return any(key in serialized for key in forbidden_keys)


def generated_at() -> str:
    """Evidence helper; deliberately excluded from context_pack_id inputs."""
    return datetime.now().astimezone().isoformat()
