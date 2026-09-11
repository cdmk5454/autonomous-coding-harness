"""Immutable, profile-local DB catalog loading and selective prompt rendering."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import MappingProxyType
from typing import Any, Mapping, Sequence


CATALOG_SCHEMA_VERSION = 1
EVIDENCE_PRECEDENCE = (
    "CURRENT_DB_METADATA",
    "CURRENT_MYBATIS_SQL",
    "SUPPLIED_SPECIFICATION",
    "INFERRED_RELATIONSHIP",
)
ALLOWED_EVIDENCE_TYPES = {
    "DB_FK",
    "DB_PK",
    "DB_INDEX",
    "KEY_INCLUSION",
    "COLUMN_MATCH",
    "BUSINESS_INFERENCE",
    "MYBATIS_CONFIRMED",
}
ALLOWED_CONFIDENCE = {"confirmed", "high", "medium", "low"}


class DbCatalogError(ValueError):
    def __init__(self, code: str, path: str):
        self.code = code
        self.path = path if path.startswith("$") else f"$.{path}"
        super().__init__(f"[{code}] {self.path}")


def _safe_relative(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DbCatalogError("DB_CATALOG_PATH_INVALID", "path")
    raw = value.strip().replace("\\", "/")
    if PurePosixPath(raw).is_absolute() or PureWindowsPath(raw).is_absolute():
        raise DbCatalogError("DB_CATALOG_PATH_INVALID", "path")
    normalized = os.path.normpath(raw).replace("\\", "/")
    if normalized in ("", ".", "..") or normalized.startswith("../"):
        raise DbCatalogError("DB_CATALOG_PATH_ESCAPE", "path")
    return normalized


def _is_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    if is_junction is not None and is_junction():
        return True
    try:
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError:
        return False
    return bool(attributes & 0x400)


def _resolve_file(root: Path, value: Any, label: str) -> Path:
    try:
        relative = _safe_relative(value)
    except DbCatalogError as exc:
        raise DbCatalogError(exc.code, label) from exc
    candidate = root.joinpath(*PurePosixPath(relative).parts)
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except FileNotFoundError as exc:
        raise DbCatalogError("DB_CATALOG_PATH_MISSING", label) from exc
    except (OSError, RuntimeError, ValueError) as exc:
        raise DbCatalogError("DB_CATALOG_PATH_ESCAPE", label) from exc
    if _is_reparse(candidate):
        raise DbCatalogError("DB_CATALOG_PATH_ESCAPE", label)
    if not resolved.is_file():
        raise DbCatalogError("DB_CATALOG_PATH_TYPE_INVALID", label)
    return resolved


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DbCatalogError("DB_CATALOG_JSON_INVALID", label) from exc
    if not isinstance(value, dict):
        raise DbCatalogError("DB_CATALOG_DOCUMENT_INVALID", label)
    return value


@dataclass(frozen=True)
class DbColumn:
    name: str
    description: str
    ddl_type: str
    data_type: str
    not_null: bool | None
    pk_ordinal_source: int | None


@dataclass(frozen=True)
class DbTable:
    id: str
    schema: str
    name: str
    description: str
    key_type: str
    key_name: str
    key_columns: tuple[str, ...]
    as_of: str
    columns: tuple[DbColumn, ...]


@dataclass(frozen=True)
class DbRelationship:
    parent_table_id: str
    child_table_id: str
    join_expression: str
    evidence_type: str
    confidence: str
    verified_against_db: bool
    verified_against_mybatis: bool


@dataclass(frozen=True)
class DbQualityIssue:
    code: str
    schema: str
    severity: str
    detail: str
    table_ids: tuple[str, ...]


@dataclass(frozen=True)
class DbCatalog:
    catalog_path: Path
    tables: Mapping[str, DbTable]
    table_ids_by_name: Mapping[str, str]
    relationships: tuple[DbRelationship, ...]
    quality_issues: tuple[DbQualityIssue, ...]
    screen_tables: Mapping[str, tuple[str, ...]]
    schema_dates: Mapping[str, str]

    def table_ids_for_text(self, text: str) -> tuple[str, ...]:
        source = text or ""
        found: list[str] = []
        seen: set[str] = set()

        def add_name(name: str) -> None:
            table_id = self.table_ids_by_name.get(name.upper())
            if table_id and table_id not in seen:
                seen.add(table_id)
                found.append(table_id)

        for name in self.table_ids_by_name:
            if re.search(rf"(?<![A-Z0-9_]){re.escape(name)}(?![A-Z0-9_])", source, re.IGNORECASE):
                add_name(name)
        for match in re.finditer(r"\b((?:SAMPLE_RESOURCE|SAMPLE_PORTAL|CAP[A-Z]?))(\d{3,4})((?:/\d{3,4})+)", source, re.IGNORECASE):
            for suffix in match.group(3)[1:].split("/"):
                add_name(f"{match.group(1)}{suffix}")
        for match in re.finditer(r"\b((?:SAMPLE_RESOURCE|SAMPLE_PORTAL|CAP[A-Z]?))(\d{3,4})\s*[~～-]\s*(?:(?:SAMPLE_RESOURCE|SAMPLE_PORTAL|CAP[A-Z]?))?(\d{3,4})\b", source, re.IGNORECASE):
            start, end = int(match.group(2)), int(match.group(3))
            if end >= start and end - start <= 25:
                for value in range(start, end + 1):
                    add_name(f"{match.group(1)}{value:0{len(match.group(2))}d}")
        return tuple(found)

    def table_ids_for_screens(self, screen_ids: Sequence[str]) -> tuple[str, ...]:
        found: list[str] = []
        seen: set[str] = set()
        for screen_id in screen_ids:
            for table_id in self.screen_tables.get(screen_id.upper(), ()):
                if table_id not in seen:
                    seen.add(table_id)
                    found.append(table_id)
        return tuple(found)

    def render_context(
        self,
        table_ids: Sequence[str],
        focus_text: str = "",
        max_chars: int = 10000,
    ) -> str:
        selected = [self.tables[table_id] for table_id in dict.fromkeys(table_ids) if table_id in self.tables]
        if not selected:
            return ""
        focus_upper = (focus_text or "").upper()
        lines = [
            "[DB catalog snapshot - structure only]",
            "Evidence order: current DB metadata > current MyBatis SQL > supplied specification > inferred relationship.",
            "Common-code rows and current business data are not present in this catalog.",
        ]
        selected_ids = {table.id for table in selected}
        for table in selected:
            lines.append(f"\n### {table.id} - {table.description or 'description unavailable'} (as-of {table.as_of})")
            key_columns = ", ".join(table.key_columns) or "none recorded"
            lines.append(f"- key: {table.key_type or 'unknown'} / {table.key_name or 'unnamed'} / {key_columns}")
            prioritized = [column for column in table.columns if column.name in focus_upper or column.name in table.key_columns]
            prioritized.extend(column for column in table.columns if column not in prioritized)
            rendered_columns = prioritized[:16]
            column_text = []
            for column in rendered_columns:
                flags = []
                if column.name in table.key_columns:
                    flags.append("KEY")
                if column.not_null is True:
                    flags.append("NN")
                suffix = f" [{' '.join(flags)}]" if flags else ""
                column_text.append(f"{column.name} {column.ddl_type or column.data_type or '?'}{suffix}: {column.description or '-'}")
            lines.append("- columns: " + "; ".join(column_text))
            if len(table.columns) > len(rendered_columns):
                lines.append(f"- columns omitted from prompt: {len(table.columns) - len(rendered_columns)}; inspect {self.catalog_path.name} schema reference when needed")
            if len("\n".join(lines)) >= max_chars:
                break

        relevant_relations = [
            relation for relation in self.relationships
            if relation.parent_table_id in selected_ids and relation.child_table_id in selected_ids
        ]
        if relevant_relations and len("\n".join(lines)) < max_chars:
            lines.append("\n### Direct relationships among selected tables")
            for relation in relevant_relations[:20]:
                lines.append(
                    f"- {relation.parent_table_id} -> {relation.child_table_id}: "
                    f"{relation.join_expression or '?'}; evidence={relation.evidence_type}; confidence={relation.confidence}"
                )
        relevant_issues = [
            issue for issue in self.quality_issues
            if not issue.table_ids or selected_ids.intersection(issue.table_ids)
            or any(table.schema == issue.schema for table in selected)
        ]
        if relevant_issues and len("\n".join(lines)) < max_chars:
            lines.append("\n### Snapshot quality warnings")
            for issue in relevant_issues[:12]:
                lines.append(f"- {issue.severity} {issue.code}: {issue.detail}")

        rendered = "\n".join(lines)
        if len(rendered) > max_chars:
            notice = "\n- Context cap reached; read the selected profile DB catalog for omitted metadata."
            body_limit = max(0, max_chars - len(notice))
            body = rendered[:body_limit].rsplit("\n", 1)[0]
            rendered = (body + notice)[:max_chars]
        return rendered


def load_db_catalog(catalog_path: str | Path, profile_root: str | Path) -> DbCatalog:
    profile_dir = Path(profile_root).resolve(strict=True)
    catalog_file = Path(catalog_path).resolve(strict=True)
    try:
        catalog_file.relative_to(profile_dir)
    except ValueError as exc:
        raise DbCatalogError("DB_CATALOG_PATH_ESCAPE", "context.db_catalog") from exc
    if _is_reparse(catalog_file):
        raise DbCatalogError("DB_CATALOG_PATH_ESCAPE", "context.db_catalog")
    catalog_root = catalog_file.parent
    manifest = _read_json(catalog_file, "context.db_catalog")
    if manifest.get("catalog_schema_version") != CATALOG_SCHEMA_VERSION:
        raise DbCatalogError("DB_CATALOG_VERSION_UNSUPPORTED", "context.db_catalog.catalog_schema_version")
    if manifest.get("snapshot_only") is not True:
        raise DbCatalogError("DB_CATALOG_SNAPSHOT_FLAG_REQUIRED", "context.db_catalog.snapshot_only")
    if tuple(manifest.get("precedence", ())) != EVIDENCE_PRECEDENCE:
        raise DbCatalogError("DB_CATALOG_PRECEDENCE_INVALID", "context.db_catalog.precedence")
    schema_entries = manifest.get("schemas")
    source_entries = manifest.get("sources")
    if not isinstance(schema_entries, list) or not schema_entries:
        raise DbCatalogError("DB_CATALOG_SCHEMAS_INVALID", "context.db_catalog.schemas")
    if not isinstance(source_entries, list) or not source_entries:
        raise DbCatalogError("DB_CATALOG_SOURCES_INVALID", "context.db_catalog.sources")

    tables: dict[str, DbTable] = {}
    table_ids_by_name: dict[str, str] = {}
    relationships: list[DbRelationship] = []
    schema_dates: dict[str, str] = {}
    for index, entry in enumerate(schema_entries):
        if not isinstance(entry, dict):
            raise DbCatalogError("DB_CATALOG_SCHEMA_ENTRY_INVALID", f"context.db_catalog.schemas[{index}]")
        schema = entry.get("schema")
        schema_file = _resolve_file(catalog_root, entry.get("path"), f"context.db_catalog.schemas[{index}].path")
        document = _read_json(schema_file, f"context.db_catalog.schemas[{index}]")
        if document.get("catalog_schema_version") != CATALOG_SCHEMA_VERSION or document.get("schema") != schema:
            raise DbCatalogError("DB_CATALOG_SCHEMA_MISMATCH", f"context.db_catalog.schemas[{index}]")
        if document.get("source_sha256") != entry.get("source_sha256") or document.get("as_of") != entry.get("as_of"):
            raise DbCatalogError("DB_CATALOG_SOURCE_MISMATCH", f"context.db_catalog.schemas[{index}]")
        schema_dates[str(schema)] = str(entry.get("as_of"))
        raw_tables = document.get("tables")
        raw_columns = document.get("columns")
        raw_relationships = document.get("relationships")
        raw_indexes = document.get("indexes")
        raw_constraints = document.get("constraints")
        if not all(isinstance(values, list) for values in (raw_tables, raw_columns, raw_relationships, raw_indexes, raw_constraints)):
            raise DbCatalogError("DB_CATALOG_COLLECTION_INVALID", f"context.db_catalog.schemas[{index}]")
        raw_columns_by_table: dict[str, list[dict[str, Any]]] = {}
        for column in raw_columns:
            if not isinstance(column, dict) or not isinstance(column.get("table_id"), str):
                raise DbCatalogError("DB_CATALOG_COLUMN_INVALID", f"context.db_catalog.schemas[{index}].columns")
            raw_columns_by_table.setdefault(column["table_id"], []).append(column)
        schema_table_ids: set[str] = set()
        for item in raw_tables:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not isinstance(item.get("name"), str):
                raise DbCatalogError("DB_CATALOG_TABLE_INVALID", f"context.db_catalog.schemas[{index}].tables")
            table_id = item["id"]
            if table_id in tables or item["name"].upper() in table_ids_by_name:
                raise DbCatalogError("DB_CATALOG_TABLE_DUPLICATE", f"context.db_catalog.schemas[{index}].tables")
            raw_table_columns = raw_columns_by_table.get(table_id, [])
            if item.get("column_count") != len(raw_table_columns):
                raise DbCatalogError("DB_CATALOG_COLUMN_COUNT_MISMATCH", f"context.db_catalog.schemas[{index}].tables")
            column_names = [str(column.get("name") or "") for column in raw_table_columns]
            if not all(column_names) or len(set(column_names)) != len(column_names):
                raise DbCatalogError("DB_CATALOG_COLUMN_DUPLICATE", f"context.db_catalog.schemas[{index}].columns")
            columns = tuple(
                DbColumn(
                    name=str(column.get("name") or ""),
                    description=str(column.get("description") or ""),
                    ddl_type=str(column.get("ddl_type") or ""),
                    data_type=str(column.get("data_type") or ""),
                    not_null=column.get("not_null") if isinstance(column.get("not_null"), bool) else None,
                    pk_ordinal_source=column.get("pk_ordinal_source") if isinstance(column.get("pk_ordinal_source"), int) else None,
                )
                for column in sorted(raw_table_columns, key=lambda value: value.get("ordinal", 0))
            )
            table = DbTable(
                id=table_id,
                schema=str(schema),
                name=item["name"].upper(),
                description=str(item.get("description") or ""),
                key_type=str(item.get("key_type") or ""),
                key_name=str(item.get("key_name") or ""),
                key_columns=tuple(str(value) for value in item.get("key_columns", [])),
                as_of=str(entry.get("as_of")),
                columns=columns,
            )
            tables[table_id] = table
            table_ids_by_name[table.name] = table_id
            schema_table_ids.add(table_id)
        if set(raw_columns_by_table) - schema_table_ids:
            raise DbCatalogError("DB_CATALOG_COLUMN_TABLE_MISSING", f"context.db_catalog.schemas[{index}].columns")
        for relation in raw_relationships:
            if not isinstance(relation, dict):
                raise DbCatalogError("DB_CATALOG_RELATION_INVALID", f"context.db_catalog.schemas[{index}].relationships")
            parent = relation.get("parent_table_id")
            child = relation.get("child_table_id")
            if parent not in schema_table_ids or child not in schema_table_ids:
                raise DbCatalogError("DB_CATALOG_RELATION_ENDPOINT_MISSING", f"context.db_catalog.schemas[{index}].relationships")
            if relation.get("evidence_type") not in ALLOWED_EVIDENCE_TYPES or relation.get("confidence") not in ALLOWED_CONFIDENCE:
                raise DbCatalogError("DB_CATALOG_RELATION_EVIDENCE_INVALID", f"context.db_catalog.schemas[{index}].relationships")
            relationships.append(DbRelationship(
                parent_table_id=parent,
                child_table_id=child,
                join_expression=str(relation.get("join_expression") or ""),
                evidence_type=relation["evidence_type"],
                confidence=relation["confidence"],
                verified_against_db=relation.get("verified_against_db") is True,
                verified_against_mybatis=relation.get("verified_against_mybatis") is True,
            ))
        column_names_by_table = {
            table_id: {column.name for column in tables[table_id].columns}
            for table_id in schema_table_ids
        }
        for collection_name, items in (("indexes", raw_indexes), ("constraints", raw_constraints)):
            for item in items:
                if not isinstance(item, dict) or item.get("table_id") not in schema_table_ids:
                    raise DbCatalogError("DB_CATALOG_ITEM_TABLE_MISSING", f"context.db_catalog.schemas[{index}].{collection_name}")
                table_id = item["table_id"]
                if any(column not in column_names_by_table[table_id] for column in item.get("columns", [])):
                    raise DbCatalogError("DB_CATALOG_ITEM_COLUMN_MISSING", f"context.db_catalog.schemas[{index}].{collection_name}")
                referenced = item.get("referenced_table_id")
                if referenced is not None and referenced not in schema_table_ids:
                    raise DbCatalogError("DB_CATALOG_REFERENCE_TABLE_MISSING", f"context.db_catalog.schemas[{index}].{collection_name}")
                if referenced is not None and any(
                    column not in column_names_by_table[referenced]
                    for column in item.get("referenced_columns", [])
                ):
                    raise DbCatalogError("DB_CATALOG_REFERENCE_COLUMN_MISSING", f"context.db_catalog.schemas[{index}].{collection_name}")
        actual_counts = {
            "tables": len(raw_tables),
            "columns": len(raw_columns),
            "relationships": len(raw_relationships),
            "indexes": len(raw_indexes),
            "constraints": len(raw_constraints),
        }
        if document.get("counts") != actual_counts or entry.get("counts") != actual_counts:
            raise DbCatalogError("DB_CATALOG_COUNT_MISMATCH", f"context.db_catalog.schemas[{index}].counts")

    for index, source in enumerate(source_entries):
        if not isinstance(source, dict):
            raise DbCatalogError("DB_CATALOG_SOURCE_INVALID", f"context.db_catalog.sources[{index}]")
        source_file = _resolve_file(catalog_root, source.get("file"), f"context.db_catalog.sources[{index}].file")
        expected = source.get("sha256")
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise DbCatalogError("DB_CATALOG_SOURCE_HASH_INVALID", f"context.db_catalog.sources[{index}].sha256")
        if hashlib.sha256(source_file.read_bytes()).hexdigest() != expected:
            raise DbCatalogError("DB_CATALOG_SOURCE_HASH_MISMATCH", f"context.db_catalog.sources[{index}].file")

    quality_document = _read_json(
        _resolve_file(catalog_root, manifest.get("quality_issues"), "context.db_catalog.quality_issues"),
        "context.db_catalog.quality_issues",
    )
    quality_issues = []
    for index, issue in enumerate(quality_document.get("issues", [])):
        if not isinstance(issue, dict):
            raise DbCatalogError("DB_CATALOG_QUALITY_ISSUE_INVALID", f"context.db_catalog.quality_issues[{index}]")
        table_ids = tuple(value for value in (issue.get("table_id"), *issue.get("table_ids", [])) if value)
        if any(table_id not in tables for table_id in table_ids):
            raise DbCatalogError("DB_CATALOG_QUALITY_TABLE_MISSING", f"context.db_catalog.quality_issues[{index}]")
        quality_issues.append(DbQualityIssue(
            code=str(issue.get("code") or ""),
            schema=str(issue.get("schema") or ""),
            severity=str(issue.get("severity") or ""),
            detail=str(issue.get("detail") or ""),
            table_ids=table_ids,
        ))

    ui_document = _read_json(
        _resolve_file(catalog_root, manifest.get("ui_map_refs"), "context.db_catalog.ui_map_refs"),
        "context.db_catalog.ui_map_refs",
    )
    screen_tables: dict[str, tuple[str, ...]] = {}
    for index, screen in enumerate(ui_document.get("screens", [])):
        if not isinstance(screen, dict) or not isinstance(screen.get("screen_id"), str):
            raise DbCatalogError("DB_CATALOG_UI_REF_INVALID", f"context.db_catalog.ui_map_refs[{index}]")
        table_ids = tuple(screen.get("table_ids", []))
        if screen.get("unresolved_internal_refs") or any(table_id not in tables for table_id in table_ids):
            raise DbCatalogError("DB_CATALOG_UI_REF_UNRESOLVED", f"context.db_catalog.ui_map_refs[{index}]")
        existing = screen_tables.get(screen["screen_id"].upper(), ())
        screen_tables[screen["screen_id"].upper()] = tuple(dict.fromkeys((*existing, *table_ids)))

    return DbCatalog(
        catalog_path=catalog_file,
        tables=MappingProxyType(tables),
        table_ids_by_name=MappingProxyType(table_ids_by_name),
        relationships=tuple(relationships),
        quality_issues=tuple(quality_issues),
        screen_tables=MappingProxyType(screen_tables),
        schema_dates=MappingProxyType(schema_dates),
    )
