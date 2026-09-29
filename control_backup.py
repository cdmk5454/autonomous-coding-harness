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
        # All tables, schema and constraints are observed from ONE read snapshot.
        connection.execute("BEGIN")
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        projections = []
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            rows = connection.execute(f"SELECT * FROM {quoted}").fetchall()
            def typed(value):
                if isinstance(value, bytes):
                    return {"sqlite_type": "blob", "hex": value.hex()}
                return value
            # Physical row order/rowid is not semantic and WITHOUT ROWID is legal.
            encoded = sorted(json.dumps([typed(row[key]) for key in row.keys()],
                ensure_ascii=False, separators=(",", ":"), allow_nan=False) for row in rows)
            serialized = ("[" + ",".join(encoded) + "]").encode("utf-8")
            projections.append({"table": table, "rows": len(rows),
                                "sha256": hashlib.sha256(serialized).hexdigest()})
        return {
            "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
            "schema_sha256": hashlib.sha256(json.dumps([list(row) for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' "
                "ORDER BY type,name,tbl_name")], ensure_ascii=False,
                separators=(",", ":")).encode("utf-8")).hexdigest(),
            "integrity": "PASS" if integrity == ["ok"] and not foreign_keys else "FAIL",
            "foreign_key_errors": len(foreign_keys), "projections": projections,
        }


def _sqlite_copy(source: Path, destination: Path) -> None:
    # Never overwrite an existing database, inode, symlink, or an install in use.
    if source == destination or destination.exists() or destination.is_symlink():
        raise ControlBackupError("CONTROL_BACKUP_TARGET_INVALID")
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
        # Atomic no-clobber publish. A competing creator wins rather than loses work.
        try:
            os.link(temporary, destination)
        except FileExistsError as exc:
            raise ControlBackupError("CONTROL_BACKUP_TARGET_INVALID") from exc
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
    if backup_path == destination_path:
        raise ControlBackupError("CONTROL_RESTORE_SOURCE_EQUALS_TARGET")
    if destination_path.exists() or destination_path.is_symlink():
        raise ControlBackupError("CONTROL_RESTORE_IN_PLACE_UNSAFE" if overwrite else "CONTROL_RESTORE_TARGET_EXISTS")
    expected = database_fingerprint(backup_path)
    _sqlite_copy(backup_path, destination_path)
    restored = database_fingerprint(destination_path)
    if expected != restored:
        raise ControlBackupError("CONTROL_RESTORE_FINGERPRINT_MISMATCH")
    return {"backup_fingerprint": expected, "restored_fingerprint": restored,
            "database_reference_rows_equal": expected["projections"] == restored["projections"],
            "artifact_references_intact": False,
            "artifact_verification_status": "NOT_VERIFIED",
            "external_artifact_bytes_verified": False,
            "stale_writers_invalidated": False,
            "safe_restart_authorized": False}


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



def verify_restore_artifacts(root, references):
    """Read-only artifact consistency check for a DISPOSABLE restore rehearsal.

    A reference row surviving SQLite restore does not establish that a writer
    lease is invalid, a candidate is intact, or a publication is reconciled.
    No restart permission is ever minted here.
    """
    from pathlib import PurePosixPath
    base = Path(root).resolve()
    observations = []
    for entry in references:
        relative = entry.get("path")
        expected = entry.get("sha256")
        if (not isinstance(relative, str) or "\\" in relative or ":" in relative
                or PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts
                or not relative or PurePosixPath(relative).as_posix() != relative):
            raise ControlBackupError("RESTORE_ARTIFACT_PATH_INVALID")
        target = base / relative
        if (target.is_symlink() or not target.resolve().is_relative_to(base)
                or any(p.is_symlink() for p in target.parents if p != base and p.is_relative_to(base))):
            raise ControlBackupError("RESTORE_ARTIFACT_PATH_INVALID")
        if entry.get("kind") not in {"SNAPSHOT", "CANDIDATE", "JOURNAL"}:
            raise ControlBackupError("RESTORE_ARTIFACT_KIND_INVALID")
        if not isinstance(expected, str) or not __import__('re').fullmatch(r"[0-9a-f]{64}", expected):
            raise ControlBackupError("RESTORE_ARTIFACT_DIGEST_INVALID")
        actual = hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else None
        observations.append({"path": relative, "kind": entry["kind"], "matches": actual == expected})
    return {"artifact_bytes_match": bool(observations) and all(x["matches"] for x in observations),
            "observations": observations, "coverage": "SUPPLIED_REFERENCES_ONLY",
            "stale_writers_invalidated": False, "safe_restart_authorized": False,
            "next": "FENCE_OLD_EPOCHS_AND_RECONCILE_EXTERNAL_JOURNALS"}


def prepare_restore_candidate(backup, destination, *, artifact_root, references, restore_id):
    """Create a NEW offline restore candidate, verify supplied artifacts, fence it.

    Never installs a DB at the live authority path, kills a process, releases a
    global stop, reconciles Product journals, or authorizes restart/cutover.
    """
    from control_repository import ControlRepository
    from control_epoch import fence_restored_repository
    if not isinstance(restore_id, str) or not __import__('re').fullmatch(r'[A-Za-z0-9_.:-]{1,128}', restore_id):
        raise ControlBackupError('RESTORE_FENCE_INPUT_INVALID')
    restored = restore_backup(backup, destination)
    repo = ControlRepository(destination)
    fence = fence_restored_repository(repo, restore_id=restore_id)
    # Even malformed/missing artifact input leaves only a quarantined copy.
    artifacts = verify_restore_artifacts(artifact_root, references)
    return {'database_restore': restored, 'artifact_verification': artifacts,
            'restore_fence': fence, 'safe_restart_authorized': False,
            'next': 'LOCAL_NATIVE_QUIESCENCE_AND_PUBLICATION_RECONCILIATION_REQUIRED'}
