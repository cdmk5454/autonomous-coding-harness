"""Explicit bounded admission migrations. Startup never changes the schema."""
from pathlib import Path
import sqlite3
import json
import time
from dataclasses import asdict
from datetime import datetime

from harness_11.capacity import CapacityPolicy

SCHEMA_VERSION = 6
MAX_ACTIVE_EXECUTIONS = 2
N_WORKER_SCHEMA_VERSION = 7
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
        dst = sqlite3.connect(backup)
        try:
            src.backup(dst)
        finally:
            dst.close()
        src.execute('BEGIN IMMEDIATE')
        try:
            for statement in DDL:src.execute(statement)
            if src.execute('PRAGMA foreign_key_check').fetchall():raise ValueError('FOREIGN_KEY_FAILED')
            src.commit()
        except BaseException:src.rollback();raise
    finally:src.close()
    return {'schema':6,'cap':2,'restore_image':str(backup)}


def migrate_capacity_disposable(path, *, allocation_root, backup_path, policy, quiescence_probe):
    """Explicit 6->7 upgrade in an owned disposable tree, with a restore image.

    The live Control is intentionally outside this function's permitted roots.
    A caller must stop dispatch and independently prove all native writers quiet.
    """
    path, root, backup = map(lambda p: Path(p).resolve(), (path, allocation_root, backup_path))
    forbidden = (Path(r'D:\kkm_test').resolve(), Path(r'D:\agents').resolve(),
                 Path(r'D:\inha_dev').resolve())
    if (not path.is_relative_to(root) or path == root or not backup.is_relative_to(root)
            or any(path.is_relative_to(p) or backup.is_relative_to(p) for p in forbidden)
            or backup.exists() or backup == path):
        raise ValueError('DISPOSABLE_MIGRATION_REQUIRED')
    return _migrate_capacity_verified(path, backup, policy, quiescence_probe)


def migrate_capacity_control(path, *, control_root, backup_root, backup_path,
                             policy, quiescence_probe, operator_approval):
    """Explicit operator-only live migration entry point for later promotion.

    No startup path calls this. Approval must be separately supplied after final
    review; this source entry point is not invoked during blocker closure.
    """
    path, control, backup_root, backup = map(lambda p: Path(p).resolve(),
        (path, control_root, backup_root, backup_path))
    if (operator_approval != 'APPROVED_AFTER_FINAL_PRO_REVIEW'
            or path != control/'control.sqlite3' or not backup.is_relative_to(backup_root)
            or backup == path or backup.exists() or backup_root.is_relative_to(control)
            or control.is_relative_to(backup_root)):
        raise ValueError('EXPLICIT_CONTROL_MIGRATION_APPROVAL_REQUIRED')
    return _migrate_capacity_verified(path, backup, policy, quiescence_probe)


def _migrate_capacity_verified(path, backup, policy, quiescence_probe):
    if not isinstance(policy, CapacityPolicy) or not callable(quiescence_probe):
        raise ValueError('CAPACITY_MIGRATION_CONTRACT_INVALID')
    source = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True)
    source.row_factory = sqlite3.Row
    try:
        source.execute('PRAGMA foreign_keys=ON')
        if source.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION:
            raise ValueError('SCHEMA6_REQUIRED')
        if source.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('DB_INTEGRITY_FAILED')
        session = source.execute("SELECT payload FROM control_state WHERE key='session'").fetchone()
        if session is None or json.loads(session[0]).get('paused') is not True:
            raise ValueError('QUEUE_PAUSE_REQUIRED')
        active = source.execute("""SELECT 1 FROM logical_jobs WHERE status='RUNNING'
            UNION ALL SELECT 1 FROM executions WHERE phase='RUNNING'
            UNION ALL SELECT 1 FROM execution_slots
            UNION ALL SELECT 1 FROM current_writer_attempt
            UNION ALL SELECT 1 FROM runtime_processes WHERE state IN ('STARTING','RUNNING','UNKNOWN','LOST')
            LIMIT 1""").fetchone()
        if active or quiescence_probe(path) is not True:
            raise ValueError('ACTIVE_EXECUTION_MIGRATION_REQUIRES_DRAIN')
        destination = sqlite3.connect(backup)
        try:
            source.backup(destination)
        finally:
            destination.close()
        source.execute('BEGIN IMMEDIATE')
        try:
            source.execute('DROP TRIGGER bounded_claim_release')
            source.execute('DROP TRIGGER bounded_execution_admission')
            source.execute('DROP TRIGGER bounded_execution_insert')
            source.execute('ALTER TABLE execution_capacity RENAME TO execution_capacity_v6')
            source.execute('''CREATE TABLE execution_capacity (
                id INTEGER PRIMARY KEY CHECK(id=1),
                max_active_executions INTEGER NOT NULL CHECK(max_active_executions BETWEEN 1 AND 64),
                policy TEXT NOT NULL CHECK(json_valid(policy)))''')
            source.execute('INSERT INTO execution_capacity VALUES(1,?,?)',
                           (policy.workers, json.dumps(asdict(policy), sort_keys=True)))
            source.execute('DROP TABLE execution_capacity_v6')
            source.execute('ALTER TABLE execution_slots RENAME TO execution_slots_v6')
            source.execute('''CREATE TABLE execution_slots (
                slot INTEGER PRIMARY KEY CHECK(slot BETWEEN 1 AND 64),
                job_id TEXT NOT NULL UNIQUE REFERENCES logical_jobs(job_id),
                owner_id TEXT NOT NULL, candidate_path TEXT UNIQUE,
                state TEXT NOT NULL CHECK(state IN ('RESERVED','RUNNING','CANCELLING','STALE','QUARANTINED')),
                lease_until REAL NOT NULL, process_identity TEXT NOT NULL DEFAULT '')''')
            source.execute('DROP TABLE execution_slots_v6')
            source.execute("""CREATE TRIGGER bounded_execution_admission BEFORE UPDATE OF phase ON executions
                WHEN NEW.phase='RUNNING' AND NOT EXISTS(SELECT 1 FROM execution_slots WHERE job_id=NEW.job_id)
                BEGIN SELECT RAISE(ABORT,'EXECUTION_ADMISSION_REQUIRED'); END""")
            source.execute("""CREATE TRIGGER bounded_execution_insert BEFORE INSERT ON executions
                WHEN NEW.phase='RUNNING' AND NOT EXISTS(SELECT 1 FROM execution_slots WHERE job_id=NEW.job_id)
                BEGIN SELECT RAISE(ABORT,'EXECUTION_ADMISSION_REQUIRED'); END""")
            source.execute('PRAGMA user_version=7')
            if source.execute('PRAGMA foreign_key_check').fetchall():
                raise ValueError('FOREIGN_KEY_FAILED')
            if source.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('DB_INTEGRITY_FAILED')
            source.commit()
        except BaseException:
            source.rollback()
            raise
    finally:
        source.close()
    return {'schema': N_WORKER_SCHEMA_VERSION, 'policy': asdict(policy),
            'restore_image': str(backup)}

def enabled(repository):
    if repository is None:return False
    with repository.hold():return repository._connection.execute('PRAGMA user_version').fetchone()[0] in {6,7}


def explicit_release_enabled(repository):
    if repository is None:return False
    with repository.hold():return repository._connection.execute('PRAGMA user_version').fetchone()[0] == 7

def capacity(repository):
    with repository.hold():
        return repository._connection.execute('SELECT max_active_executions FROM execution_capacity WHERE id=1').fetchone()[0]


def capacity_policy(repository):
    with repository.hold():
        version = repository._connection.execute('PRAGMA user_version').fetchone()[0]
        if version == 7:
            row = repository._connection.execute('SELECT policy FROM execution_capacity WHERE id=1').fetchone()
            return CapacityPolicy(**json.loads(row[0]))
        return CapacityPolicy()


def resize_capacity(repository, policy):
    if not isinstance(policy, CapacityPolicy):
        raise ValueError('CAPACITY_POLICY_INVALID')
    with repository.hold():
        if repository._connection.execute('PRAGMA user_version').fetchone()[0] != 7:
            raise ValueError('NWORKER_SCHEMA_REQUIRED')
        # A reduction changes admissions only. Existing slots retain custody.
        repository._connection.execute('UPDATE execution_capacity SET max_active_executions=?,policy=? WHERE id=1',
            (policy.workers, json.dumps(asdict(policy), sort_keys=True)))

def slots(repository):
    with repository.hold():return [dict(x) for x in repository._connection.execute('SELECT * FROM execution_slots ORDER BY slot')]

def claim_slot(repository,job_id,owner_id):
    # Caller holds the SAME transaction as the Job QUEUED -> RUNNING update.
    occupied={x['slot'] for x in slots(repository)}
    if len(occupied) >= capacity(repository):
        return None
    for slot in range(1,capacity(repository)+1):
        if slot not in occupied:
            if explicit_release_enabled(repository):
                repository._connection.execute('''INSERT INTO execution_slots
                    (slot,job_id,owner_id,state,lease_until) VALUES(?,?,?,'RESERVED',?)''',
                    (slot,job_id,owner_id,time.time()+capacity_policy(repository).lease_seconds))
            else:
                repository._connection.execute('INSERT INTO execution_slots(slot,job_id,owner_id) VALUES(?,?,?)',(slot,job_id,owner_id))
            return slot
    return None


def bind_process(repository, job_id, process_identity):
    with repository.hold():
        if not explicit_release_enabled(repository):
            return
        row = repository._connection.execute('SELECT state,process_identity FROM execution_slots WHERE job_id=?',
                                             (job_id,)).fetchone()
        if row is None or row['state'] in {'STALE','QUARANTINED'}:
            raise ValueError('EXECUTION_SLOT_NOT_ACTIVE')
        if row['process_identity'] and row['process_identity'] != process_identity:
            raise ValueError('PROCESS_IDENTITY_CONFLICT')
        repository._connection.execute('''UPDATE execution_slots SET state='RUNNING',process_identity=?,lease_until=?
            WHERE job_id=?''', (process_identity,time.time()+capacity_policy(repository).lease_seconds,job_id))


def renew_process(repository, job_id, process_identity):
    with repository.hold():
        if not explicit_release_enabled(repository):
            return
        row = repository._connection.execute('SELECT state,process_identity,lease_until FROM execution_slots WHERE job_id=?',
                                             (job_id,)).fetchone()
        now = time.time()
        if (row is None or row['state'] != 'RUNNING' or row['process_identity'] != process_identity
                or row['lease_until'] <= now):
            raise ValueError('EXECUTION_SLOT_LEASE_STALE')
        repository._connection.execute('UPDATE execution_slots SET lease_until=? WHERE job_id=?',
            (now+capacity_policy(repository).lease_seconds,job_id))


def reconcile_slots(repository):
    """Restart holds every residual schema7 slot until an owner proves fencing."""
    with repository.hold():
        if not explicit_release_enabled(repository):
            return []
        rows = slots(repository)
        for row in rows:
            repository._connection.execute("UPDATE execution_slots SET state='QUARANTINED' WHERE slot=?",
                                           (row['slot'],))
        return [row['job_id'] for row in rows]


def request_cancel(repository, job_id, owner_id):
    """Mark cancellation without releasing writer ownership or killing by PID."""
    with repository.hold():
        if not explicit_release_enabled(repository):
            raise ValueError('NWORKER_SCHEMA_REQUIRED')
        row = repository._connection.execute(
            'SELECT state FROM execution_slots WHERE job_id=? AND owner_id=?',
            (job_id, owner_id)).fetchone()
        if row is None or row['state'] not in {'RESERVED','RUNNING','CANCELLING'}:
            raise ValueError('EXECUTION_ADMISSION_NOT_OWNED')
        repository._connection.execute("UPDATE execution_slots SET state='CANCELLING' WHERE job_id=?",
                                       (job_id,))
        return {'cancel_requested': True, 'slot_released': False, 'writer_fenced': False}


def cancel_requested(repository, job_id, process_identity):
    with repository.hold():
        if not explicit_release_enabled(repository):
            return False
        row = repository._connection.execute(
            'SELECT state,process_identity FROM execution_slots WHERE job_id=?',
            (job_id,)).fetchone()
        if row is None or row['process_identity'] != process_identity:
            raise ValueError('EXECUTION_SLOT_PROCESS_MISMATCH')
        return row['state'] == 'CANCELLING'


def release_slot(repository, job_id, owner_id, *, custody=None):
    """Release a schema7 slot only after the owned candidate and writer are quiet."""
    with repository.hold():
        if repository._connection.execute('PRAGMA user_version').fetchone()[0] != 7:
            return False  # schema6 retains its original result-transition trigger
        row = repository._connection.execute(
            'SELECT candidate_path FROM execution_slots WHERE job_id=? AND owner_id=?',
            (job_id, owner_id)).fetchone()
        if row is None:
            raise ValueError('EXECUTION_ADMISSION_NOT_OWNED')
        job = repository._connection.execute(
            'SELECT status FROM logical_jobs WHERE job_id=?', (job_id,)).fetchone()
        if job is None or job[0] == 'RUNNING':
            raise ValueError('EXECUTION_NOT_TERMINAL')
        attempts = repository._connection.execute('''SELECT a.attempt_id,o.status
            FROM attempts a JOIN executions e ON e.execution_id=a.execution_id
            LEFT JOIN attempt_outcomes o ON o.attempt_id=a.attempt_id
            WHERE e.job_id=?''', (job_id,)).fetchall()
        if any(status is None or status in {'RUNNING', 'UNKNOWN'} for _, status in attempts):
            raise ValueError('WRITER_ATTEMPT_NOT_TERMINAL')
        active = repository._connection.execute('''SELECT 1 FROM runtime_processes p
            JOIN attempts a ON a.attempt_id=p.attempt_id
            JOIN executions e ON e.execution_id=a.execution_id
            WHERE e.job_id=? AND p.state NOT IN ('EXITED','KILLED') LIMIT 1''',
            (job_id,)).fetchone()
        if active or repository._connection.execute('''SELECT 1 FROM current_writer_attempt w
            JOIN executions e ON e.execution_id=w.execution_id WHERE e.job_id=? LIMIT 1''',
            (job_id,)).fetchone():
            raise ValueError('WRITER_QUIESCENCE_NOT_PROVEN')
        if custody is not None:
            from hermes_boundary import read
            if str(Path(row[0]).resolve()).casefold() != str(Path(custody.candidate).resolve()).casefold():
                raise ValueError('CANDIDATE_SCOPE_MISMATCH')
            allocation = read(custody.root/'allocation.json')
            if allocation.get('job_id') != job_id or allocation.get('state') != 'CLOSED':
                raise ValueError('CANDIDATE_CUSTODY_NOT_CLOSED')
            cleanup_path = custody.root/'cleanup.json'
            if attempts or cleanup_path.exists():
                cleanup = read(cleanup_path)
                if (cleanup.get('job_id') != job_id or cleanup.get('candidate_id') != allocation.get('candidate_id')
                        or cleanup.get('state') != 'QUIESCED' or cleanup.get('quiesced') is not True):
                    raise ValueError('CANDIDATE_WRITER_NOT_QUIESCED')
        elif attempts or row[0]:
            raise ValueError('CANDIDATE_CUSTODY_REQUIRED')
        repository._connection.execute('DELETE FROM execution_slots WHERE job_id=? AND owner_id=?',
                                       (job_id, owner_id))
        # Schema 7 deliberately removed the schema-6 auto-release trigger so
        # candidate/process custody can be proven before the permit is reused.
        # JobStore records the terminal Job while that slot still exists, which
        # means its legacy queue projection still points at the completed Job.
        # Refresh the projection in this SAME transaction as the slot deletion;
        # otherwise a quiescent Control can retain a false running_job_id and
        # block operator/restart paths indefinitely.
        queue = repository.read_queue()
        remaining = [dict(entry) for entry in repository._connection.execute(
            'SELECT * FROM execution_slots ORDER BY slot')]
        queue['running_job_id'] = remaining[0]['job_id'] if remaining else ''
        queue['active_execution_count'] = len(remaining)
        expected_revision = int(queue.get('revision', 0))
        queue['revision'] = expected_revision + 1
        queue['updated_at'] = datetime.now().astimezone().isoformat()
        repository.write_queue(queue, expected_revision=expected_revision)
        return True

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
    if explicit_release_enabled(repository):
        policy = capacity_policy(repository)
        if any(row['state'] in {'STALE','QUARANTINED','CANCELLING'}
               or row['lease_until'] <= time.time() for row in active):
            return 'RECONCILIATION_REQUIRED'
        if len(active) >= policy.wip:
            return 'WIP_EXHAUSTED'
        roles = dict(repository._connection.execute('''SELECT a.role,COUNT(DISTINCT e.job_id)
            FROM execution_slots s JOIN executions e ON e.job_id=s.job_id
            JOIN attempts a ON a.execution_id=e.execution_id
            JOIN attempt_outcomes o ON o.attempt_id=a.attempt_id
            WHERE o.status='RUNNING' AND a.role IN ('REVIEWER','TESTER') GROUP BY a.role''').fetchall())
        if (roles.get('REVIEWER',0) >= policy.review_backlog
                or roles.get('TESTER',0) >= policy.test_backlog):
            return 'DOWNSTREAM_WIP_EXHAUSTED'
    if not wanted and active:return 'WRITE_SCOPE_UNKNOWN'
    from harness_11.core import Resource, BoundaryError
    try:
        requested_shared = [Resource(**entry) for entry in job['request'].get('shared_resources') or ()]
    except (BoundaryError, TypeError, ValueError):
        return 'SHARED_RESOURCE_INVALID'
    for slot in active:
        other=repository.read_job(slot['job_id'])
        paths={p.replace('\\','/').strip('/').casefold() for p in other['request'].get('target_resources') or ()}
        if not paths or any(a==b or a.startswith(b.rstrip('/')+'/') or b.startswith(a.rstrip('/')+'/') or '*' in a or '*' in b for a in wanted for b in paths):
            return 'WRITE_CONFLICT'
        try:
            other_shared = [Resource(**entry) for entry in other['request'].get('shared_resources') or ()]
        except (BoundaryError, TypeError, ValueError):
            return 'SHARED_RESOURCE_INVALID'
        if any(a.conflicts(b) for a in requested_shared for b in other_shared):
            return 'SHARED_RESOURCE_CONFLICT'
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
