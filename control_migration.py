"""Copy-only rehearsal and verified publication of the current control projection."""

from pathlib import Path
import hashlib
import json

from control_repository import (
    SCHEMA_VERSION, ControlRepository, RepositoryError, canonical, digest, field_diff,
)
from job_queue import JobStore


def scheduling_source(root):
    root = Path(root)
    queue = json.loads((root / "queue.json").read_text(encoding="utf-8"))
    if queue.get("running_job_id"):
        raise RepositoryError("MIGRATION_RUNNING_EXECUTION")
    all_jobs, refs = {}, {}
    for job_id in queue["order"]:
        relative = "jobs/" + job_id + ".json"
        raw = (root / relative).read_bytes()
        all_jobs[job_id] = json.loads(raw)
        refs[job_id] = {"path": relative, "sha256": hashlib.sha256(raw).hexdigest()}
    selected = set(JobStore._active_job_ids(queue))
    if queue.get("blocked_by_job_id"):
        selected.add(queue["blocked_by_job_id"])
    # Only current scheduling dependencies and explicit corrective/root links.
    pending = list(selected)
    while pending:
        job = all_jobs[pending.pop()]
        links = list(job.get("depends_on") or [])
        for key in ["replaces_job_id", "parent_job_id", "qa_parent_job_id", "blocked_by_job_id"]:
            if job.get(key):
                links.append(job[key])
        for linked in links:
            if linked not in all_jobs:
                raise RepositoryError("MIGRATION_DEPENDENCY_MISSING")
            if linked not in selected:
                selected.add(linked)
                pending.append(linked)
    return {"queue": queue, "jobs": {key: all_jobs[key] for key in queue["order"] if key in selected}}, refs


def projection(repository, source):
    return {"queue": repository.read_queue(), "jobs": {key: repository.read_job(key) for key in source["jobs"]}}


def rehearse(legacy_copy, database):
    source, refs = scheduling_source(legacy_copy)
    repository = ControlRepository(database, create=True, legacy_root=legacy_copy)
    repository.bootstrap(source["queue"], source["jobs"], refs)
    after = projection(repository, source)
    mismatches = field_diff(source, after)
    restart = ControlRepository(database, legacy_root=legacy_copy)
    restart_mismatches = field_diff(after, projection(restart, source))
    legacy_scheduling = JobStore(legacy_copy, read_only=True).queue_snapshot()
    sqlite_scheduling = JobStore(legacy_copy, read_only=True, repository=restart).queue_snapshot()
    scheduling_mismatches = field_diff(legacy_scheduling, sqlite_scheduling)
    health = restart.health()
    result = {"pass": not mismatches and not restart_mismatches and not scheduling_mismatches and health["status"] == "VALID",
              "source_hash": digest(source), "sqlite_projection_hash": digest(after),
              "scheduling_jobs": len(source["jobs"]), "legacy_evidence_refs": len(refs),
              "comparison": "ALL_CURRENT_QUEUE_AND_JOB_FIELDS", "mismatches": mismatches,
              "restart_mismatches": restart_mismatches, "repository": health,
              "scheduling_projection_hash": digest(legacy_scheduling),
              "scheduling_mismatches": scheduling_mismatches,
              "source_file_hashes": {"queue.json": hashlib.sha256((Path(legacy_copy) / "queue.json").read_bytes()).hexdigest(),
                                     **{value["path"]: value["sha256"] for value in refs.values()}}}
    return result


def cutover(live_root, rehearsed_database, rehearsal):
    """No live migration: publish the already-verified database, then the authority marker."""
    from control_paths import atomic_state_write
    live_root = Path(live_root).resolve()
    if not rehearsal.get("pass"):
        raise RepositoryError("MIGRATION_REHEARSAL_REQUIRED")
    for relative, expected in rehearsal["source_file_hashes"].items():
        if hashlib.sha256((live_root / relative).read_bytes()).hexdigest() != expected:
            raise RepositoryError("MIGRATION_SOURCE_CHANGED")
    database = live_root / "control.sqlite3"
    if database.exists() or (live_root / "sqlite-authority.json").exists():
        raise RepositoryError("CONTROL_AUTHORITY_ALREADY_EXISTS")
    source, _ = scheduling_source(live_root)
    if digest(source) != rehearsal["source_hash"]:
        raise RepositoryError("MIGRATION_PROJECTION_CHANGED")
    atomic_state_write(database, Path(rehearsed_database).read_bytes(), root=live_root, field="sqlite_cutover")
    repository = ControlRepository(database)
    if field_diff(source, projection(repository, source)) or repository.health()["status"] != "VALID":
        raise RepositoryError("MIGRATION_EQUIVALENCE_FAILED")
    marker = {"authority": "SQLITE", "schema_version": SCHEMA_VERSION, "migration_hash": digest(rehearsal),
              "legacy_projection_hash": rehearsal["source_hash"], "compatibility_mirror": False}
    atomic_state_write(live_root / "sqlite-authority.json", canonical(marker).encode("utf-8"),
                       root=live_root, field="sqlite_authority")
    return repository.health()
