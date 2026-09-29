"""A read-only Reviewer execution; Product custody identity never moves."""
import copy
import json

from control_repository import RepositoryError, canonical, digest, now
from test_checkpoint import accepted, current_contract


def admit(repo, task, *, expected_revision, checkpoint):
    with repo.hold():
        job = current_contract(repo, task)
        if job['revision'] != expected_revision:
            raise RepositoryError('CONTROL_REVISION_CONFLICT')
        if repo.global_stop().get('active'):
            raise RepositoryError('GLOBAL_STOP')
        proof = accepted(task, task.working_dir, repository=repo)
        if not proof or proof != checkpoint:
            raise RepositoryError('REVIEWER_TEST_CHECKPOINT_REQUIRED')
        detail = proof['detail']
        binding = dict(job_id=task.job_id, predecessor_execution_id=task.execution_id,
                       candidate_id=task.candidate_id, candidate_hash=detail['identity']['candidate_hash'],
                       build_evidence_sha256=detail['build_evidence_sha256'],
                       test_checkpoint_id=proof['validation_id'],
                       test_checkpoint_sha256=digest(proof),
                       contract_revision=detail['contract_revision'], role='REVIEWER')
        execution_id = 'EXEC-REVIEW-' + digest(binding).upper()
        previous = repo._connection.execute('SELECT phase,payload FROM executions WHERE execution_id=?',
                                           (execution_id,)).fetchone()
        recovery = str(task.reviewer_recovery_request_id or '')
        if previous and previous['phase'] == 'TERMINAL' and recovery:
            prior_result = repo._connection.execute('SELECT result FROM executions WHERE execution_id=?',
                                                   (execution_id,)).fetchone()[0]
            verdict = json.loads(prior_result or '{}').get('review_status')
            if verdict == 'REVIEW_PASS':
                return json.loads(previous['payload'])
            if verdict not in {'REVIEW_ERROR', 'REVIEW_UNAVAILABLE'}:
                raise RepositoryError('REVIEWER_SEMANTIC_FIX_REQUIRED')
            binding.update(recovery_request_sha256=digest(recovery), retry_of_reviewer_execution_id=execution_id)
            execution_id = 'EXEC-REVIEW-' + digest(binding).upper()
            previous = repo._connection.execute('SELECT phase,payload FROM executions WHERE execution_id=?',
                                               (execution_id,)).fetchone()
        if previous:
            if previous['phase'] == 'TERMINAL':
                raise RepositoryError('REVIEWER_SUCCESSOR_TERMINAL')
            frozen = json.loads(previous['payload'])
            if frozen.get('review_binding') != binding:
                raise RepositoryError('REVIEWER_SUCCESSOR_CONFLICT')
            return frozen
        if repo.global_stop().get('active'):
            raise RepositoryError('GLOBAL_STOP')
        active = repo.active_execution(task.job_id)
        if active and active['execution_id'] != task.execution_id:
            raise RepositoryError('REVIEWER_SUCCESSOR_CONFLICT')
        # No role admission can hide an unfinished Product process/attempt.
        if repo._connection.execute(
            "SELECT 1 FROM attempts a JOIN attempt_outcomes o USING(attempt_id) "
            "WHERE a.execution_id=? AND o.status IN ('RUNNING','UNKNOWN')", (task.execution_id,)).fetchone():
            raise RepositoryError('PRODUCT_EXECUTION_NOT_QUIESCED')
        if repo._connection.execute(
            "SELECT 1 FROM runtime_processes p JOIN attempts a USING(attempt_id) "
            "WHERE a.execution_id=? AND p.state IN ('STARTING','RUNNING','UNKNOWN','LOST')",
                (task.execution_id,)).fetchone():
            raise RepositoryError('PRODUCT_EXECUTION_NOT_QUIESCED')
        import bounded_execution
        slot_owner = 'REVIEWER:' + execution_id
        owns_slot = False
        if bounded_execution.enabled(repo):
            existing_slot = next((s for s in bounded_execution.slots(repo) if s['job_id'] == task.job_id), None)
            if not existing_slot:
                if bounded_execution.scope_conflict(repo, job):
                    raise RepositoryError('REVIEWER_SCOPE_CONFLICT')
                if bounded_execution.claim_slot(repo, task.job_id, slot_owner) is None:
                    raise RepositoryError('EXECUTION_CAPACITY_EXHAUSTED')
                bounded_execution.bind_candidate(repo, task.job_id, slot_owner, task.working_dir)
                owns_slot = True
        predecessor = repo._connection.execute('SELECT phase FROM executions WHERE execution_id=?',
                                               (task.execution_id,)).fetchone()
        if predecessor['phase'] != 'TERMINAL':
            repo._connection.execute("UPDATE executions SET phase='TERMINAL',result=? WHERE execution_id=?",
                (canonical({'test_checkpoint_id': proof['validation_id'], 'test_status': 'PASS'}), task.execution_id))
            repo._set_writer_authority_locked(task.execution_id, 'RELEASED', 'TEST_CHECKPOINT_ACCEPTED')
        frozen = copy.deepcopy(task.materialized_execution)
        frozen.update(execution_id=execution_id, materialization_id='MAT-REVIEW-' + digest(binding).upper(),
                      contract_revision=detail['contract_revision'], materialized_at=now(),
                      role='REVIEWER', review_binding=binding,
                      handoff_source_execution_id=task.execution_id,
                      admission_job_revision=expected_revision, owns_reviewer_slot=owns_slot,
                      reviewer_slot_owner=slot_owner, test_plan=copy.deepcopy(task.test_plan),
                      request=copy.deepcopy(job['request']))
        repo._connection.execute('INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)',
            (execution_id, task.job_id, frozen['logical_job_id'], frozen['contract_revision'], 'MATERIALIZED',
             frozen['profile_revision'], frozen['policy_revision'], frozen['effective_policy_hash'],
             frozen['baseline_hash'], frozen['execution_surface_hash'], frozen['materialized_at'], canonical(frozen)))
        repo._connection.execute('INSERT INTO materializations VALUES(?,?,?,?)',
            (frozen['materialization_id'], execution_id, frozen['materialized_at'], canonical(frozen)))
        authority = dict(execution_id=execution_id, state='RELEASED', revision=0,
                         reason='REVIEWER_READ_ONLY', updated_at=frozen['materialized_at'])
        repo._connection.execute('INSERT INTO execution_authority VALUES(?,?,?,?,?,?)',
            (execution_id, 'RELEASED', 0, 'REVIEWER_READ_ONLY', frozen['materialized_at'], canonical(authority)))
        return frozen


def finish(repo, execution_id, *, result):
    with repo.hold():
        row = repo._connection.execute('SELECT phase,payload,result FROM executions WHERE execution_id=?',
                                      (execution_id,)).fetchone()
        if not row or json.loads(row['payload']).get('role') != 'REVIEWER':
            raise RepositoryError('REVIEWER_SUCCESSOR_REQUIRED')
        if row['phase'] == 'TERMINAL':
            if row['result'] != canonical(result):
                raise RepositoryError('TERMINAL_EXECUTION_IMMUTABLE')
            return
        if repo._connection.execute("SELECT 1 FROM attempts JOIN attempt_outcomes USING(attempt_id) "
            "WHERE execution_id=? AND status IN ('RUNNING','UNKNOWN')", (execution_id,)).fetchone():
            raise RepositoryError('REVIEWER_ATTEMPT_NOT_TERMINAL')
        if repo._connection.execute("SELECT 1 FROM runtime_processes p JOIN attempts a USING(attempt_id) "
            "WHERE a.execution_id=? AND p.state IN ('STARTING','RUNNING','UNKNOWN','LOST')", (execution_id,)).fetchone():
            raise RepositoryError('REVIEWER_PROCESS_NOT_QUIESCED')
        repo._connection.execute("UPDATE executions SET phase='TERMINAL',result=? WHERE execution_id=?",
                                 (canonical(result), execution_id))
        frozen = json.loads(row['payload'])
        if frozen.get('owns_reviewer_slot'):
            repo._connection.execute('DELETE FROM execution_slots WHERE job_id=? AND owner_id=?',
                                     (frozen['job_id'], frozen['reviewer_slot_owner']))


def prepare(task, workspace, task_root):
    from control_repository import ControlRepository
    from test_checkpoint import enabled
    if not enabled(task):
        return
    repo = ControlRepository(task.control_repository_path)
    proof = accepted(task, workspace, restore=True)
    if not proof:
        raise RepositoryError('REVIEWER_TEST_CHECKPOINT_REQUIRED')
    previous_id = task.reviewer_execution.get('execution_id')
    task.reviewer_execution = repo.admit_reviewer_successor(task,
        expected_revision=repo.read_job(task.job_id)['revision'], checkpoint=proof)
    if task.reviewer_execution['execution_id'] != previous_id:
        task.reviewer_attempt_id = ''
        task.reviewer_native_session_id = ''
        task.reviewer_native_session_binding_id = ''
        task.reviewer_session_contract = {}
        task.reviewer_session_reuse_mode = ''
    task.save(task_root)


def start_attempt(task, model):
    from control_repository import ControlRepository
    if task.worker == 'hermes':
        from hermes_identity import ProviderModelIdentity
        provider = ProviderModelIdentity.parse(task.hermes_model).provider
        model = ProviderModelIdentity.parse(model, provider=provider).qualified
    repo = ControlRepository(task.control_repository_path)
    frozen = task.reviewer_execution
    proof = accepted(task, task.working_dir)
    if not proof or digest(proof) != frozen['review_binding']['test_checkpoint_sha256']:
        raise RepositoryError('REVIEWER_TEST_CHECKPOINT_REQUIRED')
    attempt_id = frozen['execution_id'] + '-ATTEMPT'
    with repo.hold():
        if repo.read_job(task.job_id)['revision'] != frozen['admission_job_revision']:
            raise RepositoryError('CONTROL_REVISION_CONFLICT')
        previous = repo.attempt_outcome(attempt_id)
        if previous:
            # A reserved-but-undispatched execution has no attempt. Once registered,
            # ambiguous delivery requires existing explicit runtime recovery.
            raise RepositoryError('REVIEWER_DISPATCH_ALREADY_REGISTERED')
        repo.start_attempt(frozen, attempt_id=attempt_id, role='REVIEWER', model=model,
                           candidate_hash=frozen['review_binding']['candidate_hash'])
    task.reviewer_attempt_id = attempt_id


def complete(task, result, *, review_result=None):
    if not task.reviewer_execution:
        return
    from control_repository import ControlRepository
    repo = ControlRepository(task.control_repository_path)
    if task.reviewer_attempt_id:
        repo.finish_attempt(task.reviewer_attempt_id, status='SUCCEEDED' if result in {'REVIEW_PASS','REVIEW_FAIL'} else 'FAILED',
                            retry_domain='' if result in {'REVIEW_PASS','REVIEW_FAIL'} else 'TECHNICAL_EXECUTION_RETRY',
                            failure_code='' if result in {'REVIEW_PASS','REVIEW_FAIL'} else 'REVIEWER_TECHNICAL_FAILURE')
    from dataclasses import asdict
    outcome = {'review_status': result}
    if review_result is not None:
        outcome['review_result'] = asdict(review_result)
    repo.finish_reviewer_successor(task.reviewer_execution['execution_id'], result=outcome)


def completed_review(task):
    """Resume VERIFY after a durable Review verdict without another dispatch."""
    if not task.reviewer_execution:
        return None
    from control_repository import ControlRepository
    from reviewer import ReviewResult
    repo = ControlRepository(task.control_repository_path)
    with repo.hold():
        row = repo._connection.execute('SELECT phase,result FROM executions WHERE execution_id=?',
                                      (task.reviewer_execution['execution_id'],)).fetchone()
        if row and row['phase'] == 'TERMINAL':
            result = json.loads(row['result'] or '{}')
            if result.get('review_status') == 'REVIEW_PASS' and result.get('review_result'):
                verify_accepted_review(repo, task)
                return ReviewResult(**result['review_result'])
    return None


def verify_accepted_review(repo, task, *, candidate_alias=''):
    """Publication consumes the role verdict, never a Task-only PASS claim."""
    if not task.reviewer_execution:
        return
    with repo.hold():
        row = repo._connection.execute('SELECT phase,payload,result FROM executions WHERE execution_id=?',
            (task.reviewer_execution['execution_id'],)).fetchone()
        proof = accepted(task, task.working_dir, repository=repo, candidate_alias=candidate_alias)
        if (not row or row['phase'] != 'TERMINAL'
                or json.loads(row['payload']) != task.reviewer_execution
                or json.loads(row['result'] or '{}').get('review_status') != 'REVIEW_PASS'
                or not proof or digest(proof) != task.reviewer_execution['review_binding']['test_checkpoint_sha256']):
            raise RepositoryError('REVIEWER_SUCCESSOR_PROOF_INVALID')
