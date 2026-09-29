"""Harness-owned Tester lifecycle; runtime failures never schedule Worker work."""
import uuid
from tester import TesterResult, _apply_to_state
from control_repository import ControlRepository


def run_attempts(task, invoke):
    restore = getattr(task, '_restore_test_checkpoint', None)
    if callable(restore) and restore():
        return task.test_status
    policy=task.effective_policy.get('resolved_tester',{})
    cap=policy.get('technical_retries',0) if policy.get('runtime')=='hermes' else 0
    if type(cap) is not int or not 0 <= cap <= 1:
        raise ValueError('TESTER_RETRY_POLICY_INVALID')
    record=getattr(task,'_record_execution_attempt',None)
    repo=ControlRepository(task.control_repository_path) if callable(record) else None
    for index in range(cap+1):
        if index:
            task.technical_execution_retry_count=getattr(task,"technical_execution_retry_count",0)+1
        aid=task.task_id+'-TESTER-'+uuid.uuid4().hex
        task.tester_attempt_id=aid
        task.official_tester_invoked=False
        task.official_tester_execution={}
        task.test_status='ERROR'
        task.test_evidence=[]
        started=False
        interrupted=False
        try:
            if callable(record):
                record('TESTER',aid,policy['model'])
                started=True
            invoke()
            if task.test_status not in {'PASS','FAIL','ERROR'}:
                _apply_to_state(task,TesterResult('ERROR','TESTER_OUTCOME_INVALID'))
            if task.test_status in {'PASS','FAIL'} and (not task.official_tester_invoked or not task.test_evidence):
                _apply_to_state(task,TesterResult('ERROR','OFFICIAL_TESTER_EVIDENCE_MISSING'))
            checkpoint = getattr(task, '_checkpoint_test', None)
            if task.test_status == 'PASS' and callable(checkpoint):
                checkpoint()
        except Exception as exc:
            _apply_to_state(task,TesterResult('ERROR',getattr(exc,'code','TESTER_INVOCATION_EXCEPTION')))
        except BaseException:
            interrupted=True
            raise
        finally:
            domain=('PRODUCT_SEMANTIC_RETRY' if task.test_status=='FAIL' else
                'TECHNICAL_EXECUTION_RETRY' if task.test_status=='ERROR' or interrupted else '')
            interrupted = interrupted or task.test_summary == 'TESTER_CANCELLED'
            status='INTERRUPTED' if interrupted else 'SUCCEEDED' if task.test_status=='PASS' else 'FAILED'
            if started:
                repo.finish_attempt(aid,status=status,retry_domain=domain,
                    failure_code='' if task.test_status=='PASS' else 'TESTER_SEMANTIC_FAIL' if task.test_status=='FAIL' else 'TESTER_RUNTIME_ERROR')
            task.tester_attempt_history.append({'attempt_id':aid,'status':status,'test_status':task.test_status,
                'retry_domain':domain,'technical_retry_index':index,
                'execution':dict(task.official_tester_execution),'evidence':list(task.test_evidence)})
        if task.test_status!='ERROR' or not started or interrupted:
            break
    return task.test_status


def run_recovery_attempt(task, invoke, *, recovery_limit=1, operator_rearm=False):
    """Run one bounded Tester-only technical recovery without Worker work."""
    restore = getattr(task, '_restore_test_checkpoint', None)
    if callable(restore) and restore():
        return task.test_status
    if recovery_limit != 1:
        raise ValueError("TESTER_RECOVERY_LIMIT_INVALID")
    history = list(getattr(task, "tester_attempt_history", []) or [])
    recoveries = [item for item in history if item.get("kind") == "TESTER_TECHNICAL_RECOVERY"]
    if any(item.get('kind') == 'TESTER_OPERATOR_REARM' for item in history):
        raise ValueError('TESTER_REARM_EXHAUSTED')
    if operator_rearm and len(recoveries) != 1:
        raise ValueError('TESTER_REARM_PRIOR_RECOVERY_REQUIRED')
    if not operator_rearm and len(recoveries) >= recovery_limit:
        raise ValueError("TESTER_RECOVERY_LIMIT_EXHAUSTED")
    if not history or history[-1].get("test_status") != "ERROR":
        raise ValueError("TESTER_RECOVERY_PRIOR_ERROR_REQUIRED")
    task.technical_execution_retry_count = int(
        getattr(task, "technical_execution_retry_count", 0)
    ) + 1
    before = len(task.tester_attempt_history)
    status = run_attempts(task, invoke)
    if len(task.tester_attempt_history) != before + 1:
        raise ValueError("TESTER_RECOVERY_ATTEMPT_MISSING")
    task.tester_attempt_history[-1]["kind"] = (
        'TESTER_OPERATOR_REARM' if operator_rearm else 'TESTER_TECHNICAL_RECOVERY')
    task.tester_attempt_history[-1]["recovery_ordinal"] = len(recoveries) + 1
    return status


def finish_worker_runtime(task, result=None):
    if (task.worker not in {'hermes','pi'} and not getattr(task, '_isolated_opencode', False)) or not task.control_repository_path:return
    repo=ControlRepository(task.control_repository_path)
    prior=repo.attempt_outcome(task.attempt_id)
    if prior and prior.get('status')=='RUNNING':
        ok=result is not None and result.success
        repo.finish_attempt(task.attempt_id,status='SUCCEEDED' if ok else 'FAILED',
            retry_domain='' if ok else 'RUNTIME_RECOVERY',
            failure_code='' if ok else getattr(result,'execution_failure_code','WORKER_INVOCATION_EXCEPTION'))


def fresh_worker_attempt(task):
    if task.worker not in {'hermes','pi'} and not getattr(task, '_isolated_opencode', False):return
    # Called only after Manager has authorized a semantic retry and applied its
    # existing disposition. A new identity never revives the old runtime.
    if task.failure_type not in {'build','test','review'} or task.fix_scope=='NON_RETRYABLE':
        raise ValueError('HERMES_SEMANTIC_RETRY_REQUIRED')
    repo=ControlRepository(task.control_repository_path)
    previous=repo.attempt_outcome(task.attempt_id)
    if not previous or previous.get('status')=='RUNNING':
        raise ValueError('HERMES_PREVIOUS_ATTEMPT_NOT_TERMINAL')
    aid=task.task_id+'-WORKER-'+uuid.uuid4().hex
    model = (task.pi_model if task.worker == 'pi' else task.opencode_model
             if getattr(task, '_isolated_opencode', False) else task.hermes_model)
    task._record_execution_attempt('WORKER',aid,model)
    task.attempt_id=aid
    task.product_semantic_retry_count+=1
    task.retry_domain='PRODUCT_SEMANTIC_RETRY'
