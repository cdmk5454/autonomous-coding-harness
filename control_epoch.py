"""Restore epoch fencing over existing Control schema; no imports perform writes.

The epoch fences patched repository clients and frozen execution tickets. It is
NOT proof that a pre-patch/native process has stopped. Restored processes stay
UNKNOWN, writer authority stays suspended, and restart remains unauthorized.
"""
from __future__ import annotations
import json
import re
import uuid

KEY = 'restore_fence/1'
LEGACY_EPOCH = 'LEGACY_UNFENCED'

def current(connection):
    from control_repository import RepositoryError
    row = connection.execute('SELECT payload FROM control_state WHERE key=?', (KEY,)).fetchone()
    if row is None:
        return LEGACY_EPOCH
    value = json.loads(row[0])
    token = value.get('epoch')
    if not isinstance(token, str) or not re.fullmatch(r'RESTORE-[0-9a-f]{32}', token):
        raise RepositoryError('CONTROL_RESTORE_EPOCH_INVALID')
    return token

def assert_execution(repository, execution_id):
    from control_repository import RepositoryError
    row = repository._connection.execute('SELECT payload FROM executions WHERE execution_id=?',
                                          (execution_id,)).fetchone()
    if row is None:
        raise RepositoryError('EXECUTION_NOT_FOUND')
    execution = json.loads(row[0])
    if execution.get('control_epoch', LEGACY_EPOCH) != current(repository._connection):
        raise RepositoryError('RESTORE_EXECUTION_EPOCH_STALE')
    return execution

def assert_attempt(repository, attempt_id):
    from control_repository import RepositoryError
    row = repository._connection.execute('SELECT execution_id FROM attempts WHERE attempt_id=?',
                                          (attempt_id,)).fetchone()
    if row is None:
        raise RepositoryError('ATTEMPT_NOT_FOUND')
    return assert_execution(repository, row[0])

def fence_restored_repository(repository, *, restore_id):
    """Quarantine a restored disposable DB. Old observations are retained.

    Call only AFTER copying into a new restore-candidate path. This function
    mints no success, process-death proof, Product permission or cutover approval.
    """
    from control_repository import RepositoryError, canonical, now
    if (not isinstance(restore_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', restore_id)
            or repository.read_only):
        raise RepositoryError('RESTORE_FENCE_INPUT_INVALID')
    if getattr(repository._local, 'connection', None) is not None:
        raise RepositoryError('RESTORE_FENCE_OUTER_TRANSACTION_REQUIRED')
    token = 'RESTORE-' + uuid.uuid4().hex
    stamp = now()
    with repository.hold():
        connection = repository._connection
        previous = current(connection)
        prior = connection.execute('SELECT revision FROM control_state WHERE key=?', (KEY,)).fetchone()
        revision = int(prior[0]) + 1 if prior else 0
        record = {'schema_revision': KEY, 'epoch': token, 'previous_epoch': previous,
                  'restore_id': restore_id, 'fenced_at': stamp,
                  'native_quiescence': 'NOT_VERIFIED', 'safe_restart_authorized': False}
        connection.execute('INSERT INTO control_state VALUES(?,?,?) ON CONFLICT(key) DO UPDATE '
                           'SET revision=excluded.revision,payload=excluded.payload',
                           (KEY, revision, canonical(record)))
        # Preserve immutable Attempt/Execution/validation payloads. Only current
        # operational projections change; RUNNING cannot survive as proof of life.
        attempts = connection.execute("SELECT attempt_id,payload FROM attempt_outcomes WHERE status='RUNNING'").fetchall()
        for row in attempts:
            value = json.loads(row['payload'])
            value.update(status='INTERRUPTED', retry_domain='RUNTIME_RECOVERY',
                         failure_code='RESTORE_RECONCILIATION_REQUIRED', product_retry_delta=0,
                         finished_at=stamp, restore_epoch=token)
            connection.execute("UPDATE attempt_outcomes SET status='INTERRUPTED',retry_domain='RUNTIME_RECOVERY',"
                               "failure_code='RESTORE_RECONCILIATION_REQUIRED',product_retry_delta=0,finished_at=?,payload=? "
                               'WHERE attempt_id=?', (stamp, canonical(value), row['attempt_id']))
        processes = connection.execute("SELECT process_id,payload FROM runtime_processes WHERE state IN ('STARTING','RUNNING','LOST','UNKNOWN')").fetchall()
        for row in processes:
            value = json.loads(row['payload'])
            value.update(state='UNKNOWN', restore_epoch=token, quiescence='NOT_VERIFIED')
            connection.execute("UPDATE runtime_processes SET state='UNKNOWN',payload=? WHERE process_id=?",
                               (canonical(value), row['process_id']))
        for row in connection.execute("SELECT execution_id FROM execution_authority WHERE state!='RELEASED'").fetchall():
            repository._set_writer_authority_locked(row[0], 'SUSPENDED', 'RESTORE_RECONCILIATION_REQUIRED')
        sessions = connection.execute("SELECT binding_id,payload FROM native_sessions WHERE state NOT IN ('CLOSED','RESTORE_FENCED')").fetchall()
        for row in sessions:
            value = json.loads(row['payload'])
            value.update(state='RESTORE_FENCED', restore_epoch=token)
            connection.execute("UPDATE native_sessions SET state='RESTORE_FENCED',payload=? WHERE binding_id=?",
                               (canonical(value), row['binding_id']))
        repository.set_global_stop('RESTORE_RECONCILIATION_REQUIRED')
    # Update this one object only AFTER commit. Other instances keep old fences.
    repository._bound_control_epoch = token
    return {**record, 'repository_clients_fenced': True,
            'interrupted_attempts': len(attempts), 'processes_requiring_quiescence': len(processes),
            'sessions_fenced': len(sessions), 'physical_processes_killed': False,
            'observed_evidence_modified': False}
