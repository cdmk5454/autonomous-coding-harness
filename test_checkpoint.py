"""Durable canonical TEST acceptance, independent of Reviewer availability."""
import copy
import json

from control_repository import ControlRepository, RepositoryError, digest
from gate_evidence import identity, bind
from tester import TestExecutionReceipt, TesterResult, arbitrate_tester_result


def source_inventory(task, workspace):
    from project_profile import load_project_profile
    from production_isolation import inventory
    profile = load_project_profile(task.profile_dir, workspace)
    return inventory(workspace, profile=profile, preserve=[*task.target_resources,
        *task.test_plan.get('required_tests', []), *task.test_plan.get('derived_tests', [])])


def validate_build(task):
    from manager import Manager
    from build_test_runner import BuildTestResult, BuildTestRunner
    validator = object.__new__(Manager)
    validator.build_test_runner = object.__new__(BuildTestRunner)
    build = task.build_evidence
    ok, code = validator._validate_structured_build_evidence(task,
        BuildTestResult(success=build.get('success') is True,
                        build_status=build.get('status', ''), details=build.get('details', {})))
    if not ok:
        raise RepositoryError(code)
    receipts = validator._build_executions(build.get('details', {}))
    if any(r.get('candidate_id') != task.candidate_id or r.get('product_execution_id') != task.execution_id
           or r.get('job_id') != task.job_id for r in receipts):
        raise RepositoryError('TEST_CHECKPOINT_BUILD_BINDING_INVALID')
    from gate_evidence import validate
    if (task.gate_evidence.get('BUILD', {}).get('status') != 'PASS'
            or any(item.startswith('BUILD:') for item in validate(task, identity(task, task.working_dir)))):
        raise RepositoryError('TEST_CHECKPOINT_BUILD_BINDING_INVALID')


def enabled(task):
    return bool(task.control_repository_path and task.materialized_execution
                and task.candidate_id and task.test_plan.get('mandatory'))


def contract_revision(task):
    return int((task.test_plan_amendment or task.materialized_execution).get('contract_revision', 1))


def validate_receipts(task):
    if task.test_plan.get("evaluation") is not None:
        from deterministic_test_policy import assert_frozen_selection
        assert_frozen_selection(task)
    if task.test_status != 'PASS' or not task.official_tester_invoked:
        raise RepositoryError('TEST_CHECKPOINT_PASS_REQUIRED')
    try:
        receipts = [TestExecutionReceipt(
            **{k: r[k] for k in ('job_id', 'execution_id', 'tester_attempt_id', 'candidate_id',
                                'test_scope_hash', 'command', 'exit_code', 'status',
                                'tool_identity', 'workspace')},
            required_tests=tuple(r['required_tests']), started_at=0,
            finished_at=float(r.get('elapsed_seconds', 0)),
            assertion_fingerprint=r.get('assertion_fingerprint', ''),
            assertions_passed=r.get('assertions_passed'),
            stdout_sha256=r.get('stdout_sha256', ''),
            stderr_sha256=r.get('stderr_sha256', ''),
            observation_version=r.get('observation_version', ''),
            termination_reason=r.get('termination_reason', ''),
            output_complete=r.get('output_complete'), quiescent=r.get('quiescent'),
            process_boundary=r.get('process_boundary', ''),
            observed_process_id=r.get('observed_process_id', 0)) for r in task.test_receipts]
    except (KeyError, TypeError, ValueError) as exc:
        raise RepositoryError('TEST_CHECKPOINT_RECEIPT_INVALID') from exc
    result = arbitrate_tester_result(task, TesterResult('PASS', task.test_summary,
                                      evidence=task.test_evidence), receipts)
    if result.status != 'PASS':
        raise RepositoryError(result.summary)


def current_contract(repo, task, *, candidate_alias=''):
    if task.materialized_execution.get('role') in {'CANONICAL_REVALIDATION', 'BATCH_VERIFICATION'}:
        from canonical_revalidation import current_contract as revalidation_contract
        return revalidation_contract(repo, task)
    job = repo.read_job(task.job_id)
    original = repo.execution(task.execution_id)
    if (not original or original.get('job_id') != task.job_id
            or original != task.materialized_execution
            or job.get('status') not in {'RUNNING', 'AWAITING_QA'}
            or (job.get('candidate_id') and job['candidate_id'] not in {task.candidate_id, candidate_alias})
            or int(job.get('contract_revision', 1)) != contract_revision(task)):
        raise RepositoryError('TEST_CHECKPOINT_CONTRACT_STALE')
    overlay = job.get('test_plan_amendment') or {}
    frozen = overlay.get('test_plan') if overlay else original.get('test_plan')
    if (frozen != task.test_plan or overlay != task.test_plan_amendment
            or task.requirement != job.get('current_requirement', job['request']['requirement'])):
        raise RepositoryError('TEST_CHECKPOINT_CONTRACT_STALE')
    return job


def payload(task, workspace, *, expected_identity=None, expected_inventory=None):
    current = identity(task, workspace)
    inventory = source_inventory(task, workspace)
    if ((expected_identity is not None and current != expected_identity)
            or (expected_inventory is not None and inventory != expected_inventory)):
        raise RepositoryError('TEST_CHECKPOINT_SOURCE_CHANGED')
    validate_receipts(task)
    validate_build(task)
    if current['contract_current'] is not True:
        raise RepositoryError('TEST_CHECKPOINT_CONTRACT_STALE')
    return dict(job_id=task.job_id, execution_id=task.execution_id,
                candidate_id=task.candidate_id, contract_revision=contract_revision(task),
                identity=current, source_inventory=inventory,
                build_evidence_sha256=digest(task.build_evidence),
                test_plan=copy.deepcopy(task.test_plan),
                tester_attempt_id=task.tester_attempt_id, test_status='PASS',
                test_summary=task.test_summary, test_evidence=list(task.test_evidence),
                test_receipts=copy.deepcopy(task.test_receipts),
                official_tester_execution=copy.deepcopy(task.official_tester_execution))


def persist(task, workspace, task_root, *, expected_identity=None, expected_inventory=None):
    """Commit receipt first, then Task projection, before completing Tester role."""
    if not enabled(task) or task.test_status != 'PASS':
        return
    repo = ControlRepository(task.control_repository_path)
    accepted = payload(task, workspace, expected_identity=expected_identity, expected_inventory=expected_inventory)
    if expected_identity is not None and accepted['identity'] != expected_identity:
        raise RepositoryError('TEST_CHECKPOINT_SOURCE_CHANGED')
    if expected_inventory is not None and accepted['source_inventory'] != expected_inventory:
        raise RepositoryError('TEST_CHECKPOINT_SOURCE_CHANGED')
    checkpoint_id = 'TEST-' + digest(accepted)
    with repo.hold():
        current_contract(repo, task)
        proof = repo.record_validation(task.execution_id, kind=checkpoint_id,
            disposition='PASS', evidence_ref=checkpoint_id, detail=accepted)
    task.test_checkpoint = proof
    bind(task, 'TEST', accepted['identity'], 'PASS')
    task.save(task_root)


def accepted(task, workspace, *, restore=False, repository=None, candidate_alias=''):
    """Read canonical checkpoint; an on-disk Task is only a recoverable projection."""
    repo = repository or ControlRepository(task.control_repository_path)
    with repo.hold():
        current_contract(repo, task, candidate_alias=candidate_alias)
        rows = repo._connection.execute(
            "SELECT payload FROM validation_dispositions WHERE execution_id=? "
            "AND kind LIKE 'TEST-%' ORDER BY rowid DESC", (task.execution_id,)).fetchall()
    current = identity(task, workspace)
    for row in rows:
        proof = json.loads(row[0])
        detail = proof.get('detail') or {}
        if (detail.get('contract_revision') != contract_revision(task)
                or detail.get('candidate_id') != task.candidate_id):
            continue
        if (proof.get('disposition') != 'PASS'
                or proof.get('evidence_ref') != 'TEST-' + digest(detail)
                or detail.get('identity') != current
                or detail.get('test_plan') != task.test_plan
                or detail.get('source_inventory') != source_inventory(task, workspace)
                or detail.get('build_evidence_sha256') != digest(task.build_evidence)):
            raise RepositoryError('TEST_CHECKPOINT_STALE')
        validate_build(task)
        # Only the TEST fields below are restored. Runtime custody callbacks on
        # the live Task can own open handles and must never be deep-copied.
        projected = copy.copy(task)
        for key in ('tester_attempt_id', 'test_status', 'test_summary', 'test_evidence',
                    'test_receipts', 'official_tester_execution'):
            setattr(projected, key, copy.deepcopy(detail[key]))
        projected.official_tester_invoked = True
        validate_receipts(projected)
        if restore:
            with repo.hold():
                attempt = repo.attempt_identity(detail['tester_attempt_id'])
                outcome = repo.attempt_outcome(detail['tester_attempt_id'])
                if attempt and outcome and outcome['status'] == 'RUNNING':
                    if (attempt['execution_id'] != task.execution_id or attempt['role'] != 'TESTER'
                            or repo.active_runtime_process(detail['tester_attempt_id'])):
                        raise RepositoryError('TEST_CHECKPOINT_RUNTIME_NOT_QUIESCED')
                    # Host receipts were committed only after subprocess return.
                    # No registered live process remains; repair the projection.
                    repo.finish_attempt(detail['tester_attempt_id'], status='SUCCEEDED')
            for key in ('tester_attempt_id', 'test_status', 'test_summary', 'test_evidence',
                        'test_receipts', 'official_tester_execution', 'official_tester_invoked'):
                setattr(task, key, getattr(projected, key))
            task.test_checkpoint = proof
            bind(task, 'TEST', current, 'PASS')
        return proof
    return None


def configure(task, workspace, task_root):
    if enabled(task):
        before = identity(task, workspace)
        inventory_before = source_inventory(task, workspace)
        task._checkpoint_test = lambda: persist(task, workspace, task_root,
            expected_identity=before, expected_inventory=inventory_before)
        task._restore_test_checkpoint = lambda: bool(accepted(task, workspace, restore=True))


def can_resume_review(task):
    """Read-only admission check for a crash before the final Job/Task projection."""
    return bool(task is not None and enabled(task)
                and task.review_status != 'REVIEW_FAIL'
                and task.qa_request.get('required') is not True
                and accepted(task, task.working_dir))
