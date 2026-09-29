"""Current operator action bindings and crash-explicit receipts. No scheduler."""
from __future__ import annotations
import copy
import json
import re

SCHEMA = 'operator-action/1'

def context(store, job_id, *, can_publish=False, execution_idle=False):
    from control_repository import digest
    from control_epoch import current
    with store._mutex.hold():
        repository = store.repository
        if repository is None:
            return {'schema_revision':SCHEMA,'binding':{},'legal_next_actions':['GET_STATUS','GET_REPORT'],
                    'write_admissibility':'SQLITE_AUTHORITY_REQUIRED'}
        with repository.hold():
            job = store.get_job(job_id)
            queue = store._read_queue()
            selected = next((j for j in store.actionable_qa_jobs() if j['job_id']==job_id), None)
            decisions = repository.open_decisions(job_id)
            matching = [d for d in decisions if d.get('contract_revision')==job.get('contract_revision',1)
                        and (not d.get('candidate_id') or d['candidate_id']==job.get('candidate_id'))]
            binding = {'job_id':job_id,'job_revision':job['revision'],
                'contract_revision':job.get('contract_revision',1),'queue_generation':queue['generation'],
                'execution_id':str((job.get('last_result') or {}).get('execution_id') or ''),
                'candidate_id':str(job.get('candidate_id') or ''),
                'request_sha256':digest(job.get('request') or {}),
                'control_epoch':current(repository._connection),
                'decision_ids':sorted(d['decision_id'] for d in matching)}
            legal=['GET_STATUS','GET_REPORT']
            # Candidate acceptance never doubles as business/policy/security resolution.
            candidate_only = bool(matching) and len(matching)==len(decisions) and all(
                d.get('decision_type')=='CANDIDATE' and d.get('candidate_id')==binding['candidate_id'] for d in matching)
            if selected and binding['candidate_id'] and candidate_only and execution_idle:
                legal.append('HUMAN_REJECT')
                if can_publish and not repository.global_stop().get('active'):
                    legal.append('HUMAN_ACCEPT')
            return {'schema_revision':SCHEMA,'binding':binding,'legal_next_actions':legal,
                    'decision_types':sorted({d['decision_type'] for d in matching}),
                    'write_admissibility':'RECHECK_CURRENT_STATE_AND_CANDIDATE_BEFORE_EFFECT',
                    'binding_sha256':digest(binding)}


def validate(supplied, current, action):
    from control_repository import RepositoryError
    if (not isinstance(supplied,dict) or supplied.get('schema_revision')!=SCHEMA
            or supplied.get('binding')!=current.get('binding')
            or supplied.get('binding_sha256')!=current.get('binding_sha256')):
        raise RepositoryError('STALE_OPERATOR_ACTION')
    if action not in current['legal_next_actions']:
        raise RepositoryError('OPERATOR_ACTION_NOT_ADMISSIBLE')
    return copy.deepcopy(current['binding'])


def receipt_key(request_id):
    from control_repository import digest, RepositoryError
    if not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9:._-]{1,180}',request_id):
        raise RepositoryError('OPERATOR_REQUEST_ID_INVALID')
    return 'operator-receipt/1/'+digest(request_id)


def prior(repository, request_id, identity):
    from control_repository import RepositoryError
    key=receipt_key(request_id)
    with repository.hold():
        row=repository._connection.execute('SELECT payload FROM control_state WHERE key=?',(key,)).fetchone()
        if not row:return None
        value=json.loads(row[0])
        if value['identity']!=identity:
            raise RepositoryError('OPERATOR_REQUEST_ID_CONFLICT')
        if value['state']=='COMPLETED':
            return {**value['result'],'replayed':True}
        # Never repeat an unacknowledged publication. A completed authoritative
        # Job can close the crash window without repeating the external effect.
        job=repository.read_job(identity['job_id']);result=job.get('last_result') or {}
        if (result.get('qa_decision_id')==value.get('decision_id')
                and result.get('candidate_id')==value.get('effective_candidate_id', identity['candidate_id'])
                and ((identity['resolution']=='HUMAN_REJECT' and job.get('status')=='CANCELLED'
                      and result.get('failure_code')=='HUMAN_REJECTED')
                     or (identity['resolution']=='HUMAN_ACCEPT' and job.get('status')=='SUCCEEDED'
                         and (result.get('canonical_promotion') or {}).get('status')=='PROMOTED'))):
            final={'resolution':identity['resolution'],'decision_id':value['decision_id'],
                   'status':job['status'],'job':job,'replayed':True,'reconciled_from_control':True}
            return complete(repository,request_id,identity,final)['result']
        raise RepositoryError('QA_ACTION_RECONCILIATION_REQUIRED')


def prepare(repository, request_id, identity, decision_id):
    from control_repository import canonical, RepositoryError, now
    key=receipt_key(request_id)
    with repository.hold():
        if repository._connection.execute('SELECT 1 FROM control_state WHERE key=?',(key,)).fetchone():
            raise RepositoryError('OPERATOR_REQUEST_ALREADY_PREPARED')
        record={'identity':copy.deepcopy(identity),'state':'PENDING','decision_id':decision_id,'prepared_at':now(),
                'effective_candidate_id': repository.read_job(identity['job_id']).get('candidate_id', '')}
        repository._connection.execute('INSERT INTO control_state VALUES(?,?,?)',(key,1,canonical(record)))


def result_projection(result):
    """Durable operator receipt contains identifiers/outcome, never prompt/source text."""
    from runtime_safety import scrub_secrets
    result = copy.deepcopy(result)
    job = result.get('job') or {}
    projected = {key:job[key] for key in ('job_id','revision','contract_revision','status','candidate_id') if key in job}
    answer = {key:result[key] for key in ('resolution','decision_id','status','replayed','reconciled_from_control') if key in result}
    answer['job'] = projected
    return json.loads(scrub_secrets(json.dumps(answer,ensure_ascii=False)))


def complete(repository, request_id, identity, result):
    from control_repository import canonical, RepositoryError, now
    result = result_projection(result)
    key=receipt_key(request_id)
    with repository.hold():
        row=repository._connection.execute('SELECT payload FROM control_state WHERE key=?',(key,)).fetchone()
        if not row or json.loads(row[0])['identity']!=identity:
            raise RepositoryError('OPERATOR_RECEIPT_IDENTITY_MISMATCH')
        value=json.loads(row[0])
        if value['state']=='COMPLETED':
            if value['result']!=result:raise RepositoryError('OPERATOR_RECEIPT_IMMUTABLE')
            return value
        # Persist only outcome/identity fields; no raw Job request, source or prompt.
        value.update(state='COMPLETED',result=copy.deepcopy(result),completed_at=now())
        repository._connection.execute('UPDATE control_state SET revision=revision+1,payload=? WHERE key=?',
                                       (canonical(value),key))
        return value
