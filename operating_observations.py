"""Source-bound metric collection; telemetry is never execution/acceptance authority.

Operational rows are projected from the existing Control repository. Optional
provider cost/usage receipts are immutable, idempotent, numeric-only observations.
No token cost is guessed and no Worker completion is promoted to Product acceptance.
"""
from __future__ import annotations
import hashlib
import json
import math
import re
from datetime import datetime

PREFIX = 'operating-usage/1/'
NUMERIC = {'input_tokens', 'cached_input_tokens', 'output_tokens', 'context_size', 'cost'}

def _elapsed(start, end):
    try:
        first, last = datetime.fromisoformat(start), datetime.fromisoformat(end)
        # An unknown timezone is not silently converted into a portable measurement.
        if first.tzinfo is None or last.tzinfo is None:
            return None
        duration = (last-first).total_seconds()
        return duration if math.isfinite(duration) and duration >= 0 else None
    except (ValueError, TypeError):
        return None

def record_usage(repository, attempt_id, *, receipt_id, receipt_sha256, values):
    """Explicit adapter recording hook: observed usage only, not prices or estimates.

    Receipt identity denotes ONE complete attempt usage observation. Partial or
    cumulative provider streams must be reconciled by the adapter before calling.
    A second receipt for the same Attempt is rejected instead of double-counted.
    """
    from control_repository import RepositoryError, canonical, now
    from control_epoch import assert_attempt
    if (not isinstance(receipt_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', receipt_id)
            or not isinstance(receipt_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', receipt_sha256)
            or not isinstance(values, dict) or not values or set(values) - (NUMERIC | {'currency'})):
        raise RepositoryError('METRIC_RECEIPT_INVALID')
    for name, value in values.items():
        if name == 'currency':
            if value not in ('USD', 'KRW', 'EUR'):
                raise RepositoryError('METRIC_CURRENCY_INVALID')
        elif (type(value) not in {int, float} or not math.isfinite(value) or value < 0
              or (name != 'cost' and type(value) is not int)):
            raise RepositoryError('METRIC_VALUE_INVALID')
    if ('cost' in values) != ('currency' in values):
        raise RepositoryError('METRIC_COST_CURRENCY_REQUIRED')
    with repository.hold():
        assert_attempt(repository, attempt_id)
        attempt = repository.attempt_identity(attempt_id)
        key = PREFIX + hashlib.sha256(str(attempt_id).encode()).hexdigest()
        identity = {'schema_revision': 'observed-usage/1', 'attempt_id': attempt_id,
                    'execution_id': attempt['execution_id'], 'receipt_id': receipt_id,
                    'receipt_sha256': receipt_sha256, 'values': dict(values),
                    'authority': 'TELEMETRY_ONLY_NOT_ACCEPTANCE'}
        row = repository._connection.execute('SELECT payload FROM control_state WHERE key=?', (key,)).fetchone()
        if row:
            prior = json.loads(row[0])
            if {k: v for k, v in prior.items() if k != 'recorded_at'} != identity:
                raise RepositoryError('METRIC_RECEIPT_CONFLICT')
            return prior
        result = {**identity, 'recorded_at': now()}
        repository._connection.execute('INSERT INTO control_state VALUES(?,?,?)', (key, 1, canonical(result)))
        return result

def collect(repository):
    """Read one coherent Control snapshot; costs/accepted/incident coverage may be absent."""
    rows = []
    with repository.hold():
        for row in repository._connection.execute('SELECT job_id,status,payload FROM logical_jobs ORDER BY job_id'):
            job = json.loads(row['payload'])
            from product_readiness import derive_product_readiness
            derived = derive_product_readiness(row['status'], repository.open_decisions(row['job_id']))
            readiness = {'readiness': derived['product_readiness'], 'open_decision_count': derived['open_decision_count']}
            sample = {'kind': 'JOB', 'job_id': row['job_id'], 'batch_id': job.get('batch_id', ''),
                      'control_status': row['status'], 'product_readiness': readiness['readiness'],
                      'open_decisions': readiness['open_decision_count'],
                      'observation_basis': 'CONTROL_ROWS_NOT_PRODUCT_CANARY'}
            # Neither status alone nor last_result/model text proves accepted Product.
            elapsed = _elapsed(job.get('created_at'), job.get('updated_at'))
            if elapsed is not None and row['status'] in {'SUCCEEDED', 'FAILED', 'CANCELLED', 'SKIPPED'}:
                sample.update(end_to_end_elapsed=elapsed, latency_ms=elapsed*1000)
            rows.append(sample)
        usage = {}
        for row in repository._connection.execute('SELECT payload FROM control_state WHERE key LIKE ?', (PREFIX+'%',)):
            receipt = json.loads(row[0]); usage[receipt['attempt_id']] = receipt
        for row in repository._connection.execute('''SELECT a.attempt_id,a.execution_id,a.role,a.started_at,
                o.status,o.retry_domain,o.finished_at FROM attempts a JOIN attempt_outcomes o
                ON a.attempt_id=o.attempt_id ORDER BY a.started_at,a.attempt_id'''):
            sample = {'kind': 'STAGE', 'attempt_id': row['attempt_id'], 'execution_id': row['execution_id'],
                      'role': row['role'], 'outcome': row['status'], 'retry_domain': row['retry_domain']}
            elapsed = _elapsed(row['started_at'], row['finished_at'])
            if elapsed is not None:
                sample['latency_ms'] = elapsed*1000
            receipt = usage.get(row['attempt_id'])
            if receipt:
                sample.update(receipt['values']); sample['receipt_sha256'] = receipt['receipt_sha256']
            rows.append(sample)
    return {'schema_revision': 'operating-observations/1', 'observations': rows,
            'coverage': {}, 'accepted_product_evidence': 'NOT_CAPTURED',
            'cost_scope': 'EXPLICIT_OBSERVED_ATTEMPT_RECEIPTS_ONLY',
            'changes_routing': False, 'real_product_canary': False}
