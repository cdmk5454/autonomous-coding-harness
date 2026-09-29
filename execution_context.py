"""Opt-in, runtime-neutral execution context boundary. Never starts a runtime.

Reuse the existing profile resource selector, authority ordering and budget.
The legacy prompt path is deliberately unchanged. Output is a self-contained
snapshot; source paths are provenance, never requests to mount the policy tree.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping

from context_foundation import (
    Authority, ContextError, ContextPack, _apply_budget, _collect_resources,
    _render, build_task_spec, deduplicate_resources,
)
from project_profile import load_project_profile, normalize_relative_path
from runtime_safety import scrub_secrets


@dataclass(frozen=True)
class ExecutionContextContract:
    job_id: str
    execution_id: str
    attempt_id: str
    candidate_id: str
    profile_id: str
    role: str
    risk_tier: str
    target_modules: tuple[str, ...]
    allowed_write_scope: tuple[str, ...]
    acceptance: tuple[str, ...]
    verification: tuple[str, ...]
    external_permissions: tuple[str, ...] = ()  # empty means no external side effects
    context_tier: str = "LOCAL"
    required_context: tuple[tuple[str, str], ...] = ()  # logical resource path, section
    impact_evidence: tuple[str, ...] = ()


# Mount/epoch/runtime facts are supplied by the future boundary owner, never by
# model prose. This function checks completeness only; it grants no execution.
ADMISSION_FIELDS = (
    "profile_resolved", "scope_resolved", "context_pack_materialized",
    "risk_policy_resolved", "candidate_identity_resolved", "mount_policy_generated",
    "runtime_fingerprint_accepted", "docker_boundary_available",
    "role_mount_policy_verified", "execution_epoch_allocated",
)


def unresolved_admission(facts: Mapping[str, bool]) -> tuple[str, ...]:
    return tuple(key for key in ADMISSION_FIELDS if facts.get(key) is not True)


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_text(text: str) -> str:
    # Reject instead of silently rewriting a business or authorization contract.
    probe = text.replace(chr(92) + chr(34), chr(34))
    if scrub_secrets(probe) != probe or re.search(
        r"(?i)[\"'](?:api[_-]?key|access[_-]?token|password|client[_-]?secret|secret|token|private[_-]?key)[\"']\s*:\s*[\"'][^\"']+", probe
    ):
        raise ContextError("EXECUTION_CONTEXT_SECRET_REJECTED")
    return text


_LOCAL_PATH = re.compile(r"(?i)(?:[a-z]:[\\/]|\\\\)[^\s`\"<>|,;\]\)]+")


def _portable_document(text: str) -> str:
    # Historical host paths in docs are non-executable provenance. Job contract
    # prose is handled separately and is never transformed by this function.
    return _LOCAL_PATH.sub("[LOCAL_PROVENANCE_OMITTED]", _safe_text(text))


def _check_document(path: Path, roots: tuple[Path, ...], *, db_source: bool = False) -> None:
    resolved = path.resolve()
    if resolved != path.absolute():
        raise ContextError("EXECUTION_CONTEXT_PATH_ESCAPE")
    if not any(resolved.is_relative_to(root) for root in roots):
        raise ContextError("EXECUTION_CONTEXT_PATH_ESCAPE")
    if path.suffix.lower() not in ({".xlsx"} if db_source else {".md", ".json"}) or any(
        part.startswith(".") or part.lower() in {"auth.json", "credentials.json"}
        or re.search(r"(?i)(?:^|[-_.])(secret|credential|token)(?:[-_.]|$)", part)
        for part in path.parts[len(next(root for root in roots if resolved.is_relative_to(root)).parts):]
    ):
        raise ContextError("EXECUTION_CONTEXT_FILE_DENIED")
    if not path.is_file():
        raise ContextError("EXECUTION_CONTEXT_FILE_MISSING")


def validate_execution_contract(c: ExecutionContextContract, requirement: str) -> None:
    """Completeness/admission validation of a frozen execution contract.

    HIGH/CRITICAL roles require approved impact evidence; the caller supplies
    it from the authoritative frozen Job contract only.  Kept as its own
    function so admission behavior is testable without materializing a pack.
    """
    for values in (c.target_modules, c.allowed_write_scope, c.acceptance, c.verification,
                   c.external_permissions, c.impact_evidence):
        if not isinstance(values, tuple) or any(not isinstance(v, str) or not v.strip() for v in values):
            raise ContextError("EXECUTION_CONTRACT_INCOMPLETE")
    if c.role not in {"WORKER", "REVIEWER", "TESTER"} or c.risk_tier not in {"LOW", "MID", "HIGH", "CRITICAL"}:
        raise ContextError("EXECUTION_CONTEXT_ROLE_OR_RISK_INVALID")
    if c.context_tier not in {"DIFF_ONLY", "LOCAL", "LOCAL_IMPACT", "DOMAIN"}:
        raise ContextError("EXECUTION_CONTEXT_TIER_INVALID")
    if not all(isinstance(x, str) and x.strip() for x in (
        c.job_id, c.execution_id, c.attempt_id, c.candidate_id, c.profile_id, requirement,
        *c.target_modules, *c.acceptance, *c.verification,
    )) or not c.target_modules or not c.acceptance or not c.verification:
        raise ContextError("EXECUTION_CONTRACT_INCOMPLETE")
    if c.risk_tier in {"HIGH", "CRITICAL"} and not c.impact_evidence:
        raise ContextError("EXECUTION_IMPACT_EVIDENCE_REQUIRED")


def build_execution_context_pack(
    *, contract: ExecutionContextContract, requirement: str,
    profile_dir: str | Path, workspace: str | Path, policy_root: str | Path,
    changed_files: tuple[str, ...] = (), implementation_context: str = "",
    review_evidence: Mapping[str, object] | None = None,
    tester_evidence: Mapping[str, object] | None = None,
    budget: int = 40000,
    schema_file: str | Path | None = None,
) -> ContextPack:
    """Resolve explicit profile/scope and freeze a role-specific portable pack.

    Risk class does not set filesystem permission or context volume. The caller
    selects a relevance tier and pins required sections, which bypass diet but
    never the total budget (overflow rejects the pack).
    """
    c = contract
    validate_execution_contract(c, requirement)
    root, policy, profile_root = (Path(x).resolve() for x in (workspace, policy_root, profile_dir))
    if not root.is_dir():
        raise ContextError("EXECUTION_WORKSPACE_INVALID")
    # Before loading profile references, reject unsafe declared file paths.
    manifest_path = profile_root / "project.json"
    _check_document(manifest_path, (profile_root,))
    raw = json.loads(manifest_path.read_text(encoding="utf8"))
    for key, value in raw.get("context", {}).items():
        path = profile_root / value
        if key == "ui_map":
            if not path.resolve().is_relative_to(profile_root):
                raise ContextError("EXECUTION_CONTEXT_PATH_ESCAPE")
        else:
            _check_document(path, (profile_root,))
    for paths in raw.get("rules", {}).values():
        for rel in paths:
            _check_document(profile_root / rel, (profile_root,))
    if raw.get("context", {}).get("db_catalog"):
        catalog_path = profile_root / raw["context"]["db_catalog"]
        catalog = json.loads(catalog_path.read_text(encoding="utf8"))
        nested = [entry.get("path") for entry in catalog.get("schemas", [])]
        for entry in catalog.get("sources", []):
            source = catalog_path.parent / str(entry.get("file", ""))
            _check_document(source, (profile_root,), db_source=True)
        nested += [catalog.get("quality_issues"), catalog.get("ui_map_refs")]
        for rel in nested:
            if rel:
                _check_document(catalog_path.parent / rel, (profile_root,))
    schema = Path(schema_file).resolve() if schema_file is not None else policy / "profiles" / "project-profile.schema.json"
    if not schema.is_file():
        raise ContextError("EXECUTION_PROFILE_SCHEMA_MISSING")
    profile = load_project_profile(profile_root, root, schema_file=schema)
    if profile.id != c.profile_id or root not in profile.workspace_roots:
        raise ContextError("EXECUTION_PROFILE_MISMATCH")
    modules = {m.name: m for m in profile.modules}
    if set(c.target_modules) - modules.keys():
        raise ContextError("EXECUTION_MODULE_UNKNOWN")
    for name in c.target_modules:
        path = root / modules[name].path
        if not path.is_dir() or not path.resolve().is_relative_to(root):
            raise ContextError("EXECUTION_MODULE_WORKSPACE_MISMATCH")
    def scope_path(value: str) -> str:
        rel = normalize_relative_path(value)
        module = profile.module_for_path(rel)
        if module is None or module.name not in c.target_modules:
            raise ContextError("EXECUTION_SCOPE_MISMATCH")
        if not (root / rel.split('*', 1)[0]).resolve().is_relative_to(root):
            raise ContextError("EXECUTION_SCOPE_ESCAPE")
        return rel
    if any(any(char in path for char in "*?[") for path in c.allowed_write_scope):
        raise ContextError("EXECUTION_EXACT_WRITE_SCOPE_REQUIRED")
    writes = [scope_path(path) for path in c.allowed_write_scope]
    if c.role == "WORKER" and not writes:
        raise ContextError("EXECUTION_WRITE_SCOPE_MISSING")
    for path in writes:
        if profile.is_protected_path(path) or profile.is_local_dev_config(path):
            raise ContextError("EXECUTION_PROTECTED_SCOPE_REQUIRES_SEPARATE_AUTHORITY")
    for path in changed_files:
        if scope_path(path) not in writes:
            raise ContextError("EXECUTION_CHANGED_FILE_OUTSIDE_WRITE_SCOPE")
    spec = build_task_spec(requirement, c.target_modules, changed_files=changed_files, task_mode="MODIFICATION", workspace=root)
    # Legacy TaskSpec may include prose before a slash path ("Change module/x").
    # Do not promote that heuristic prefix into authoritative scope.
    explicit = []
    for path in spec.explicit_paths:
        match = re.search(r"(?<![\w/])([A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+\.[A-Za-z0-9]+)", path)
        actual = match.group(1) if match else path
        explicit.append(scope_path(actual))
    spec = replace(spec, explicit_paths=tuple(explicit))
    # Validate doc metadata before any reference scan. Hidden/secret non-doc
    # files are not opened and are not included in any fingerprint.
    roots = (profile_root, policy)
    docs = [policy / "AGENTS.md", profile.project_rules, profile.analysis]
    docs += [p for paths in profile.rules.values() for p in paths]
    for folder in (profile_root / "references", profile.ui_map):
        if folder and folder.is_dir():
            docs += list(folder.rglob("*.md"))
    docs += [p for p in (policy / "hooks" / "validation.md", *(policy / "skills" / name / "SKILL.md" for name in ("project-context", "review", "backend", "frontend", "sql"))) if p.is_file()]
    for path in docs:
        _check_document(path, roots)
    if c.role == "REVIEWER":
        if implementation_context or not review_evidence or set(review_evidence) != {"candidate_id", "diff", "verification", "criteria"}:
            raise ContextError("EXECUTION_REVIEW_EVIDENCE_INVALID")
        if any(not isinstance(value, str) or not value.strip() for value in review_evidence.values()):
            raise ContextError('EXECUTION_REVIEW_EVIDENCE_INVALID')
        if review_evidence['candidate_id'] != c.candidate_id:
            raise ContextError("EXECUTION_CANDIDATE_MISMATCH")
    elif review_evidence:
        raise ContextError("EXECUTION_ROLE_EVIDENCE_MISMATCH")
    if c.role == 'TESTER':
        if (implementation_context or not tester_evidence
                or set(tester_evidence) != {'candidate_id','build','test_plan','criteria'}
                or tester_evidence.get('candidate_id') != c.candidate_id
                or any(not isinstance(v,str) or not v.strip() for v in tester_evidence.values())):
            raise ContextError('EXECUTION_TESTER_EVIDENCE_INVALID')
    elif tester_evidence:
        raise ContextError('EXECUTION_ROLE_EVIDENCE_MISMATCH')
    _safe_text(_json([requirement, c.__dict__, implementation_context, review_evidence, tester_evidence]))
    if _LOCAL_PATH.search(requirement) or _LOCAL_PATH.search(implementation_context):
        raise ContextError("EXECUTION_CONTRACT_HOST_PATH_UNRESOLVED")
    resources, omitted, warnings = _collect_resources(
        c.role, requirement, spec, profile, root, policy, changed_files, None,
        include_profile_facts=False,
    )
    required = set(c.required_context)
    available = {(r.path, r.section) for r in resources}
    if required - available:
        raise ContextError("EXECUTION_REQUIRED_CONTEXT_MISSING")
    selected = []
    for resource in resources:
        if c.role in {'REVIEWER','TESTER'} and resource.section == 'tool-permission-envelope':
            resource = replace(resource, text=c.role + ' source/candidate are read-only. Write only scratch review output. Never implement, fix, retry, apply, or grant final success.')
        pinned = (resource.path, resource.section) in required
        keep = resource.required or pinned or (c.context_tier != "DIFF_ONLY" and resource.authority == Authority.PROCEDURE)
        if c.context_tier == "LOCAL_IMPACT":
            keep |= resource.authority == Authority.VERIFIED_FACT
        elif c.context_tier == "DOMAIN":
            keep = True
        if keep:
            # Do not export whole source hashes: identity covers delivered text.
            text = _portable_document(resource.text) if resource.path != 'task:approved-contract' else _safe_text(resource.text)
            selected.append(replace(resource, text=text, required=resource.required or pinned,
                                    source_sha256=hashlib.sha256(text.encode()).hexdigest()))
        else:
            omitted.append({"path": resource.path, "section": resource.section, "reason": "EXECUTION_RELEVANCE_TIER"})
    selected, _, conflicts = deduplicate_resources(selected)
    selected, budget_omitted = _apply_budget(selected, budget)
    envelope = {
        **c.__dict__, "workspace": "workspace:/", "profile_modules": [
            {"name": modules[n].name, "path": modules[n].path, "kind": modules[n].kind}
            for n in c.target_modules],
        "boundary": {"candidate": "RW" if c.role == "WORKER" else "RO",
                     "canonical": "NOT_MOUNTED" if c.role == "WORKER" else "RO",
                     "scratch": "RW" if c.role in {"REVIEWER","TESTER"} else "NOT_MOUNTED",
                     "control_harness_agents": "NEVER_MOUNT", "context_pack": "RO"},
        "implementation_context": implementation_context if c.role == "WORKER" else "",
        "review_evidence": dict(review_evidence or {}),
        "tester_evidence": dict(tester_evidence or {}),
        "context_delivery": "HARNESS_MATERIALIZED: profile resolution is complete; this pack contains the selected context. Resource paths are provenance, not host access or raw profile mount instructions. Missing required context must be requested from Harness, never discovered from another profile.",
        "runtime_admission": "NOT_GRANTED", "final_state_authority": "HARNESS",
    }
    envelope_text = _safe_text(_json(envelope))
    # Inspect actual string values, not JSON escape syntax (e.g. a serialized
    # newline/unicode escape is not a UNC host path).
    def strings(value):
        if isinstance(value,str):yield value
        elif isinstance(value,Mapping):
            for key,item in value.items():
                yield from strings(key)
                yield from strings(item)
        elif isinstance(value,(list,tuple)):
            for item in value:yield from strings(item)
    if any(_LOCAL_PATH.search(value) for value in strings(envelope)):
        raise ContextError("EXECUTION_CONTRACT_HOST_PATH_UNRESOLVED")
    prompt = _render(selected) + "\n\n[Execution contract]\n" + envelope_text
    _safe_text(prompt)
    if len(prompt) > budget:
        raise ContextError("CONTEXT_BUDGET_EXCEEDED")
    digest = hashlib.sha256(prompt.encode()).hexdigest()
    manifest = {"manifest_version": 1, "context_pack_id": "ECTX-" + digest[:24],
                "content_sha256": digest, "profile_id": c.profile_id, "role": c.role,
                "risk_tier": c.risk_tier, "contract": envelope,
                "selected_resources": [{"path": r.path, "section": r.section,
                    "delivered_sha256": r.source_sha256, "required": r.required} for r in selected],
                "omitted_resources": omitted + budget_omitted, "warnings": warnings,
                "conflicts": conflicts, "snapshot_scope": "DELIVERED_CONTEXT_ONLY",
                "historical_profile_manifest": "NOT_ASSERTED"}
    manifest['pack_sha256'] = hashlib.sha256((prompt + _json(manifest)).encode()).hexdigest()
    return ContextPack(prompt, manifest, spec)


def materialize_execution_context(pack: ContextPack, destination: str | Path) -> Path:
    """Create a new snapshot only. Caller must mount it RO; chmod is not a sandbox."""
    payload = pack.prompt.encode('utf8')
    if hashlib.sha256(payload).hexdigest() != pack.manifest.get('content_sha256'):
        raise ContextError('EXECUTION_CONTEXT_INTEGRITY_MISMATCH')
    expected = dict(pack.manifest)
    digest = expected.pop('pack_sha256', None)
    if hashlib.sha256((pack.prompt + _json(expected)).encode()).hexdigest() != digest:
        raise ContextError('EXECUTION_CONTEXT_INTEGRITY_MISMATCH')
    _safe_text(pack.prompt)
    manifest = _safe_text(_json(pack.manifest))
    root = Path(destination)
    root.mkdir(parents=True, exist_ok=False)
    try:
        (root / 'context.md').write_bytes(payload)
        (root / 'manifest.json').write_text(manifest, encoding='utf8')
    except Exception:
        # Partial artifacts never grant admission; no overwrite/reuse API exists.
        raise ContextError('EXECUTION_CONTEXT_MATERIALIZATION_FAILED') from None
    return root
