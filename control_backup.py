"""Verified SQLite backup/restore and migration rehearsal for the Control DB."""

from __future__ import annotations

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any


class ControlBackupError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _connect_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def database_fingerprint(path: str | Path) -> dict[str, Any]:
    source = Path(path).resolve()
    if not source.is_file():
        raise ControlBackupError("CONTROL_BACKUP_SOURCE_MISSING")
    with closing(_connect_read_only(source)) as connection:
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        projections = []
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            rows = connection.execute(f"SELECT * FROM {quoted} ORDER BY rowid").fetchall()
            serialized = json.dumps(
                [[row[key] for key in row.keys()] for row in rows],
                ensure_ascii=False, sort_keys=False, separators=(",", ":"), default=str,
            ).encode("utf-8")
            projections.append({"table": table, "rows": len(rows),
                                "sha256": hashlib.sha256(serialized).hexdigest()})
        return {
            "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
            "integrity": "PASS" if integrity == ["ok"] and not foreign_keys else "FAIL",
            "foreign_key_errors": len(foreign_keys), "projections": projections,
        }


def _sqlite_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix="." + destination.name + ".", suffix=".sqlite3", dir=str(destination.parent)
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with closing(_connect_read_only(source)) as input_db, closing(sqlite3.connect(temporary)) as output_db:
            input_db.backup(output_db)
            output_db.commit()
        if database_fingerprint(temporary)["integrity"] != "PASS":
            raise ControlBackupError("CONTROL_BACKUP_INTEGRITY_FAILED")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def create_backup(source: str | Path, destination: str | Path) -> dict[str, Any]:
    source_path, destination_path = Path(source).resolve(), Path(destination).resolve()
    if source_path == destination_path or destination_path.exists():
        raise ControlBackupError("CONTROL_BACKUP_TARGET_INVALID")
    before = database_fingerprint(source_path)
    _sqlite_copy(source_path, destination_path)
    after = database_fingerprint(destination_path)
    if before != after:
        destination_path.unlink(missing_ok=True)
        raise ControlBackupError("CONTROL_BACKUP_FINGERPRINT_MISMATCH")
    return {"source_fingerprint": before, "backup_fingerprint": after,
            "backup_sha256": hashlib.sha256(destination_path.read_bytes()).hexdigest()}


def restore_backup(backup: str | Path, destination: str | Path, *, overwrite=False) -> dict[str, Any]:
    backup_path, destination_path = Path(backup).resolve(), Path(destination).resolve()
    if destination_path.exists() and not overwrite:
        raise ControlBackupError("CONTROL_RESTORE_TARGET_EXISTS")
    expected = database_fingerprint(backup_path)
    _sqlite_copy(backup_path, destination_path)
    restored = database_fingerprint(destination_path)
    if expected != restored:
        raise ControlBackupError("CONTROL_RESTORE_FINGERPRINT_MISMATCH")
    return {"backup_fingerprint": expected, "restored_fingerprint": restored,
            "artifact_references_intact": expected["projections"] == restored["projections"]}


def rehearse_migration(source: str | Path, destination: str | Path) -> dict[str, Any]:
    from control_repository import ControlRepository, SCHEMA_VERSION
    source_path, destination_path = Path(source).resolve(), Path(destination).resolve()
    _sqlite_copy(source_path, destination_path)
    before = database_fingerprint(destination_path)
    ControlRepository(destination_path)
    migrated = database_fingerprint(destination_path)
    ControlRepository(destination_path)
    repeated = database_fingerprint(destination_path)
    if migrated != repeated:
        raise ControlBackupError("CONTROL_MIGRATION_NOT_IDEMPOTENT")
    if migrated["schema_version"] != SCHEMA_VERSION or migrated["integrity"] != "PASS":
        raise ControlBackupError("CONTROL_MIGRATION_VALIDATION_FAILED")
    return {"before": before, "migrated": migrated, "repeated": repeated,
            "migration_idempotent": True}
