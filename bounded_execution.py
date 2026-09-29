"""Opt-in schema6 bounded admission. Existing schema5 is never auto-migrated."""
from pathlib import Path
import sqlite3
import json

SCHEMA_VERSION = 6
MAX_ACTIVE_EXECUTIONS = 2
DDL = [
"CREATE TABLE execution_observations (attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), identity TEXT NOT NULL CHECK(json_valid(identity)), payload TEXT NOT NULL CHECK(json_valid(payload)))",
"CREATE TABLE execution_capacity (id INTEGER PRIMARY KEY CHECK(id=1), max_active_executions INTEGER NOT NULL CHECK(max_active_executions=2))",
"INSERT INTO execution_capacity VALUES(1,2)",
"CREATE TABLE execution_slots (slot INTEGER PRIMARY KEY CHECK(slot BETWEEN 1 AND 2), job_id TEXT NOT NULL UNIQUE REFERENCES logical_jobs(job_id), owner_id TEXT NOT NULL, candidate_path TEXT UNIQUE)",
"CREATE UNIQUE INDEX one_running_execution_per_job ON executions(job_id) WHERE phase='RUNNING'",
"CREATE TABLE current_writer_attempt (execution_id TEXT PRIMARY KEY REFERENCES executions(execution_id), attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(attempt_id))",
"""CREATE TRIGGER bounded_execution_admission BEFORE UPDATE OF phase ON executions WHEN NEW.phase='RUNNING' AND NOT EXISTS(SELECT 1 FROM execution_slots WHERE job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'EXECUTION_ADMISSION_REQUIRED'); END""",
"""CREATE TRIGGER bounded_writer_insert AFTER INSERT ON attempts WHEN NEW.role='WORKER' BEGIN INSERT INTO current_writer_attempt VALUES(NEW.execution_id,NEW.attempt_id); END""",
"""CREATE TRIGGER bounded_writer_finish AFTER UPDATE OF status ON attempt_outcomes WHEN NEW.status!='RUNNING' BEGIN DELETE FROM current_writer_attempt WHERE attempt_id=NEW.attempt_id; END""",
"""CREATE TRIGGER bounded_claim_release AFTER UPDATE OF status ON logical_jobs WHEN OLD.status='RUNNING' AND NEW.status!='RUNNING' BEGIN DELETE FROM execution_slots WHERE job_id=NEW.job_id; END""",
"""CREATE TRIGGER bounded_execution_insert BEFORE INSERT ON executions WHEN NEW.phase='RUNNING' AND NOT EXISTS(SELECT 1 FROM execution_slots WHERE job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'EXECUTION_ADMISSION_REQUIRED'); END""",
"DROP INDEX one_active_execution",
"PRAGMA user_version=6",
]

def migrate_disposable(path, *, allocation_root, backup_path):
    path,root,backup=map(lambda p:Path(p).resolve(),(path,allocation_root,backup_path))
    forbidden=(Path(r'D:\kkm_test').resolve(),Path(r'D:\agents').resolve())
    if (not path.is_relative_to(root) or path==root or not backup.is_relative_to(root)
        or any(path.is_relative_to(p) or backup.is_relative_to(p) for p in forbidden)):
        raise ValueError('DISPOSABLE_MIGRATION_REQUIRED')
    if backup.exists():raise ValueError('BACKUP_ALREADY_EXISTS')
    src=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True)
    try:
        if src.execute('PRAGMA user_version').fetchone()[0]!=5:raise ValueError('SCHEMA5_REQUIRED')
        if src.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise ValueError('DB_INTEGRITY_FAILED')
        # Never turn an existing live schema5 writer into an unreserved third slot.
        # Historical RUNNING Attempts under terminal Executions remain untouched.
        if src.execute("SELECT 1 FROM executions WHERE phase='RUNNING' UNION ALL SELECT 1 FROM logical_jobs WHERE status='RUNNING' LIMIT 1").fetchone():
            raise ValueError('ACTIVE_EXECUTION_MIGRATION_REQUIRES_DRAIN')
        # Full schema5 restore image, including stale/terminal rows, BEFORE any DDL.
        with sqlite3.connect(backup) as dst:src.backup(dst)
        src.execute('BEGIN IMMEDIATE')
        try:
            for statement in DDL:src.execute(statement)
            if src.execute('PRAGMA foreign_key_check').fetchall():raise ValueError('FOREIGN_KEY_FAILED')
            src.commit()
        except BaseException:src.rollback();raise
    finally:src.close()
    return {'schema':6,'cap':2,'restore_image':str(backup)}

def enabled(repository):
    if repository is None:return False
    with repository.hold():return repository._connection.execute('PRAGMA user_version').fetchone()[0]==6

def capacity(repository):
    with repository.hold():
        return repository._connection.execute('SELECT max_active_executions FROM execution_capacity WHERE id=1').fetchone()[0]

def slots(repository):
    with repository.hold():return [dict(x) for x in repository._connection.execute('SELECT * FROM execution_slots ORDER BY slot')]

def claim_slot(repository,job_id,owner_id):
    # Caller holds the SAME transaction as the Job QUEUED -> RUNNING update.
    occupied={x['slot'] for x in slots(repository)}
    for slot in range(1,capacity(repository)+1):
        if slot not in occupied:
            repository._connection.execute('INSERT INTO execution_slots(slot,job_id,owner_id) VALUES(?,?,?)',(slot,job_id,owner_id))
            return slot
    return None

def owns(repository,job_id,owner_id):
    return any(x['job_id']==job_id and x['owner_id']==owner_id for x in slots(repository))

def bind_candidate(repository,job_id,owner_id,path):
    with repository.hold():
        if not owns(repository,job_id,owner_id):raise ValueError('EXECUTION_ADMISSION_NOT_OWNED')
        current=Path(path).resolve()
        for slot in slots(repository):
            if slot['job_id']!=job_id and slot['candidate_path']:
                other=Path(slot['candidate_path']).resolve()
                if current.is_relative_to(other) or other.is_relative_to(current):raise ValueError('CANDIDATE_SCOPE_OVERLAP')
        repository._connection.execute('UPDATE execution_slots SET candidate_path=? WHERE job_id=? AND owner_id=?',(str(Path(path).resolve()).casefold(),job_id,owner_id))

def scope_conflict(repository,job):
    # Conservative hold, NEVER add depends_on. Missing/ambiguous scopes fail closed.
    wanted={p.replace('\\','/').strip('/').casefold() for p in job['request'].get('target_resources') or ()}
    active=slots(repository)
    if not wanted and active:return 'WRITE_SCOPE_UNKNOWN'
    for slot in active:
        other=repository.read_job(slot['job_id'])
        paths={p.replace('\\','/').strip('/').casefold() for p in other['request'].get('target_resources') or ()}
        if not paths or any(a==b or a.startswith(b.rstrip('/')+'/') or b.startswith(a.rstrip('/')+'/') or '*' in a or '*' in b for a in wanted for b in paths):
            return 'WRITE_CONFLICT'
    return ''


def isolated_runtime_hold(repository,job):
    """Only enumerated ordinary runtime loss with every native writer proven quiet.

    A held Job keeps its recovery/domain evidence; it does not become success or
    consume semantic retries, and does not acquire a second automatic budget.
    """
    if not enabled(repository):return False
    result=job.get('last_result') or {}
    if result.get('failure_code') not in {
        'HERMES_CONTAINER_LOST','HERMES_RUNTIME_LIVENESS_LOST',
        'HERMES_RUNTIME_NOT_SUCCESSFUL','HERMES_TRANSPORT_UNKNOWN'}:return False
    if result.get('retry_domain')!='RUNTIME_RECOVERY':return False
    ex=result.get('execution_id') or (repository.active_execution(job['job_id']) or {}).get('execution_id')
    if not ex:return False
    with repository.hold():
        rows=repository._connection.execute('SELECT attempt_id FROM attempts WHERE execution_id=?',(ex,)).fetchall()
        if not rows:return False
        for row in rows:
            record=repository._connection.execute('SELECT payload FROM execution_observations WHERE attempt_id=?',(row[0],)).fetchone()
            if not record or json.loads(record[0]).get('state')!='QUIESCED':return False
            proof=repository._connection.execute('SELECT payload FROM runtime_recoveries WHERE attempt_id=? ORDER BY ordinal DESC LIMIT 1',(row[0],)).fetchone()
            if not proof or json.loads(proof[0]).get('evidence',{}).get('writer_quiescence',{}).get('quiesced') is not True:return False
        return True
