"""Selective profile context for screen / AS-IS / database feature mappings."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from project_profile import ProjectProfile, normalize_changed_path
from runtime_safety import scrub_secrets


FEATURE_MAP_SCHEMA_VERSION = 1
DEFAULT_CONTEXT_CAP = 14_000
MAX_FEATURES = 8
MAX_ASIS_FILES = 8


class FeatureContextError(RuntimeError):
    pass


def _tokens(feature: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for field in ("id", "title"):
        value = feature.get(field)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    for field in ("aliases", "screens", "tables", "ui_paths"):
        raw = feature.get(field)
        if isinstance(raw, list):
            values.extend(str(item).strip() for item in raw if str(item).strip())
    return list(dict.fromkeys(values))


def _matches(feature: Mapping[str, Any], focus: str) -> bool:
    folded = focus.casefold()
    return any(token.casefold() in folded for token in _tokens(feature))


def _safe_source_files(profile: ProjectProfile, patterns: Sequence[str]) -> list[Path]:
    selected: list[Path] = []
    root = profile.profile_dir.resolve()
    for pattern in patterns:
        # AS-IS evidence is deliberately restricted to profile-local Markdown.
        if not isinstance(pattern, str) or not pattern.startswith("asis_") or not pattern.endswith(".md"):
            continue
        for candidate in sorted(root.glob(pattern)):
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(root)
            except (OSError, ValueError):
                continue
            if resolved.is_file() and resolved.suffix.casefold() == ".md" and resolved not in selected:
                selected.append(resolved)
                if len(selected) >= MAX_ASIS_FILES:
                    return selected
    return selected


def _matching_sections(text: str, tokens: Sequence[str], cap: int) -> str:
    headings = list(re.finditer(r"(?m)^#{1,4}\s+.+$", text))
    if not headings:
        return text[:cap] if any(token.casefold() in text.casefold() for token in tokens) else ""
    selected: list[str] = []
    used = 0
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        section = text[heading.start():end].strip()
        if not any(token.casefold() in section.casefold() for token in tokens):
            continue
        if used + len(section) > cap:
            break
        selected.append(section)
        used += len(section)
    return "\n\n---\n\n".join(selected)


def load_feature_context(
    requirement: str,
    *,
    changed_files: Sequence[str] | None = None,
    profile: ProjectProfile | None = None,
    cap: int = DEFAULT_CONTEXT_CAP,
) -> str:
    """Load only mappings and AS-IS sections selected by task evidence.

    The map is trusted profile configuration.  AS-IS Markdown remains evidence,
    not authority: the map's precedence and constraints are always rendered
    before any extracted source section.
    """
    if profile is None or profile.feature_map is None:
        return ""
    try:
        data = json.loads(profile.feature_map.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FeatureContextError("FEATURE_MAP_UNREADABLE") from exc
    if not isinstance(data, dict) or data.get("schema_version") != FEATURE_MAP_SCHEMA_VERSION:
        raise FeatureContextError("FEATURE_MAP_INVALID")
    features = data.get("features")
    if not isinstance(features, list):
        raise FeatureContextError("FEATURE_MAP_INVALID")

    paths = [normalize_changed_path(path) for path in (changed_files or []) if path]
    focus = "\n".join((requirement or "", *paths))
    matched = [item for item in features if isinstance(item, dict) and _matches(item, focus)]
    if not matched:
        return ""
    matched = matched[:MAX_FEATURES]

    precedence = data.get("precedence") if isinstance(data.get("precedence"), list) else []
    global_constraints = (
        data.get("global_constraints") if isinstance(data.get("global_constraints"), list) else []
    )
    lines = ["[기능 근거 우선순위]"]
    lines.extend(f"{index}. {scrub_secrets(str(value))}" for index, value in enumerate(precedence, 1))
    if global_constraints:
        lines.append("\n[공통 제약]")
        lines.extend(f"- {scrub_secrets(str(value))}" for value in global_constraints)

    source_patterns: list[str] = []
    evidence_tokens: list[str] = []
    for feature in matched:
        feature_id = scrub_secrets(str(feature.get("id", "")))
        title = scrub_secrets(str(feature.get("title", "")))
        lines.append(f"\n[기능 매핑: {feature_id} - {title}]")
        for field, label in (
            ("screens", "화면/라우트"),
            ("asis_functions", "AS-IS LEGACY_REFERENCE 기능"),
            ("tables", "DB"),
            ("tobe_additions", "TO-BE 확장"),
            ("ui_paths", "소스 경로"),
            ("constraints", "기능 제약"),
        ):
            values = feature.get(field)
            if isinstance(values, list) and values:
                lines.append(f"- {label}: " + "; ".join(scrub_secrets(str(item)) for item in values))
        patterns = feature.get("asis_source_globs")
        if isinstance(patterns, list):
            source_patterns.extend(str(item) for item in patterns)
        evidence_tokens.extend(_tokens(feature))

    rendered = "\n".join(lines)
    remaining = max(0, cap - len(rendered) - 100)
    if remaining:
        per_file = max(600, remaining // max(1, min(MAX_ASIS_FILES, len(source_patterns) or 1)))
        evidence: list[str] = []
        for path in _safe_source_files(profile, source_patterns):
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                continue
            sections = _matching_sections(text, evidence_tokens, min(per_file, remaining))
            if not sections:
                continue
            block = f"[AS-IS 근거: {path.name}]\n{sections}"
            if len(block) > remaining:
                break
            evidence.append(block)
            remaining -= len(block)
        if evidence:
            rendered += "\n\n" + "\n\n---\n\n".join(evidence)
    return rendered[:cap]
