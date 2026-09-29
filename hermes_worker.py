"""Opt-in Harness role dispatch using an already admitted Hermes binding.

Does not decide Job success or apply canonical changes. This runner returns
WorkerResult execution evidence, leaving existing verification and JobStore
publication gates in place.
"""
import json,time,sqlite3
from control_repository import RepositoryError
from pathlib import Path
from hermes_runtime_adapter import HermesError
from hermes_boundary import snapshot,read
from execution_liveness import SignalError
from hermes_supervision import HermesSupervision

class HermesWorker:
    def __init__(self, adapter, *, timeout=None, timer_config=None, clock=time.monotonic, reasoning_effort='low', approval_authorizer=None, resume_session_id=''):
        self.adapter=adapter
        if timeout is not None:raise HermesError("ABSOLUTE_TIMEOUT_UNSUPPORTED")
        self.timer_config=timer_config
        self.clock=clock
        self.supervision=None
        self.reasoning_effort=reasoning_effort
        self.approval_authorizer=approval_authorizer
        self.last_status=None
        self.resume_session_id=resume_session_id
    def execute(self, requirement, working_dir, task=None):
        from worker import WorkerResult
        a=self.adapter;b=a.binding
        if b.role!='WORKER':raise HermesError('WORKER_ROLE_MISMATCH')
        return self._bounded_execute(requirement,working_dir,task)
    def _bounded_execute(self,requirement,working_dir,task):
        try:
            return self._execute(requirement,working_dir,task)
        except (HermesError,OSError,ValueError,SignalError,RepositoryError,sqlite3.Error) as exc:
            from worker import WorkerResult
            code=str(exc) if isinstance(exc,SignalError) else getattr(exc,'code','HERMES_BOUNDARY_OR_TRANSPORT_FAILURE')
            gateway=getattr(self.adapter.boundary.process,'gateway',None)
            if gateway is not None and gateway.process.poll() is not None:
                code='SHARED_GATEWAY_INFRASTRUCTURE_FAILURE'
                # Shared substrate loss stops new admission, never semantic retry.
                self.adapter.repository.set_global_stop('SHARED_RUNTIME_INFRASTRUCTURE_UNAVAILABLE')
            import traceback
            from hermes_boundary import atomic
            atomic(self.adapter.boundary.root/'runtime-error.json',{'code':code,'exception_type':type(exc).__name__,
                'frames':[{'file':f.filename,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(exc.__traceback__)]})
            if self.supervision is not None and not self.supervision.closed:self.supervision.recover(code)
            if task is not None:task.retry_domain='RUNTIME_RECOVERY'
            return WorkerResult(False,1,'',code,[],execution_failure_code='HERMES_'+code.removeprefix('HERMES_'))
        finally:
            if not (self.adapter.boundary.root/'quiescence.json').exists():
                proof=self.adapter.boundary.fence(stable_seconds=3)
                if proof['state']!='QUIESCED':raise HermesError('WRITER_QUIESCENCE_UNKNOWN')
                if self.supervision is not None and not self.supervision.closed:self.supervision.finish(proof)
            proof_path=self.adapter.boundary.root/'quiescence.json'
            if proof_path.exists() and read(proof_path).get('state')=='QUIESCED':
                process=self.adapter.repository.active_runtime_process(self.adapter.binding.attempt_id)
                if process:self.adapter.repository.finish_runtime_process(process['process_id'],state='KILLED')
    def _execute(self,requirement,working_dir,task):
        from worker import WorkerResult
        a=self.adapter;b=a.binding
        reviewer_execution = getattr(task, 'reviewer_execution', {}) or {}
        execution = ((reviewer_execution if reviewer_execution.get('execution_id') else task.materialized_execution)
                     if task is not None and b.role == 'REVIEWER'
                     else getattr(task, 'materialized_execution', None))
        if task is None or dict(execution or {}).get('execution_id')!=b.execution_id:
            raise HermesError('WORKER_DISPATCH_CONTEXT_MISSING')
        if getattr(task,{'REVIEWER':'reviewer_attempt_id','TESTER':'tester_attempt_id'}.get(b.role,'attempt_id'),'')!=b.attempt_id:
            raise HermesError('FOREIGN_ATTEMPT')
        before=snapshot(a.boundary.candidate)
        manifest=a.boundary.validate_binding(b)
        i=a.submit_turn(self.resume_session_id,requirement,cwd=working_dir,model=b.model,
            reasoning_effort=self.reasoning_effort,command_id=b.command_id)
        a.dispatch(i)
        task.runtime_adapter=a.preflight().public()
        task.control_repository_path=str(a.repository.path)
        task.command_id=b.command_id
        process=a.repository.active_runtime_process(b.attempt_id)
        if process:task.runtime_process_id=process['process_id']
        if b.role=='WORKER':
            task.native_session_id=a.session_id
            task.native_session_binding_id=a.native_binding_id
            task.native_turn_id=a.run_id
            task.worker_effective_reasoning_effort=self.reasoning_effort
            task.worker_effort_provenance='PROVIDER_REQUESTED'
        elif b.role=='TESTER':
            task.tester_native_session_id=a.session_id
            task.tester_native_session_binding_id=a.native_binding_id
        else:
            task.reviewer_native_session_id=a.session_id
            task.reviewer_native_session_binding_id=a.native_binding_id
        self.supervision=HermesSupervision(a,task,config=self.timer_config,clock=self.clock)
        while True:
            self.supervision.signals.check()
            status=a.observe_status()
            self.supervision.tick(status)
            self.last_status=status
            if status['status']=='waiting_for_approval':
                if self.approval_authorizer is not None:
                    self.approval_authorizer(a,status['approval'])
                    continue
                return WorkerResult(False,1,'','HERMES_APPROVAL_PENDING',[],execution_failure_code='HERMES_APPROVAL_PENDING')
            if status['status'] in {'completed','failed','cancelled','interrupted'}:break
            time.sleep(.5)
        self.public_messages=a.request('GET','/api/sessions/'+a.session_id+'/messages')
        proof=a.boundary.fence(stable_seconds=3)
        if proof['state']!='QUIESCED':raise HermesError('WRITER_QUIESCENCE_UNKNOWN')
        after=snapshot(a.boundary.candidate)
        changed={p for p in set(before)|set(after) if before.get(p,{}).get('sha256')!=after.get(p,{}).get('sha256')}
        product={p for p in changed if not p.startswith('.git/')}
        scope=set(manifest['contract']['allowed_write_scope']) if b.role=='WORKER' else set()
        if product-scope:raise HermesError('OUT_OF_SCOPE_WRITE')
        output=status.get('output','')
        if not isinstance(output,str):raise HermesError('RUNTIME_OUTPUT_INVALID')
        success=status['status']=='completed'
        if not success:
            raise HermesError('HERMES_RUNTIME_NOT_SUCCESSFUL')
        qa={}
        structured=a.structured_outcome(output) if success else {}
        if b.role=='WORKER' and success and structured.get('outcome') not in {'EVIDENCE','NEEDS_INPUT'}:
            raise HermesError('WORKER_OUTCOME_REQUIRED')
        if structured.get('outcome')=='NEEDS_INPUT':
            decision=a.repository.record_decision(b.job_id,contract_revision=int(task.materialized_execution['contract_revision']),
                decision_type='CONTRACT',question=structured['question'],
                payload={'runtime_binding':b.fingerprint,'native_session_id':a.session_id,'writer_quiesced':True})
            success=False;qa={'required':True,'qa_type':'CONTRACT_DECISION','hold_scope':'JOB','reason':structured['question'],'question':structured['question'],'binding_id':b.binding_id,'decision_id':decision['decision_id'],'evidence':{'decision_id':decision['decision_id'],'runtime_session':a.session_id},'decision_independent_work_complete':False}
        self.supervision.finish(proof,qa=bool(qa))
        a.repository.set_command_delivery(b.command_id,'COMPLETED')
        return WorkerResult(success,0 if success else 1,output,'',[],actual_invocation_id=a.run_id,qa_request=qa,
            execution_failure_code='' if success or qa else 'HERMES_RUNTIME_NOT_SUCCESSFUL')

class HermesReviewer(HermesWorker):
    def review(self, requirement, working_dir, task):
        if self.adapter.binding.role!='REVIEWER':raise HermesError('REVIEWER_ROLE_MISMATCH')
        result=self._bounded_execute(requirement,working_dir,task)
        if not result.success:return None,None,[result.execution_failure_code],False,'execution_unavailable',''
        outcome=self.adapter.structured_outcome(result.stdout)
        verdict={'REVIEW_PASS':'PASS','REVIEW_FAIL':'FAIL'}.get(outcome['outcome'])
        if not verdict:raise HermesError('REVIEW_OUTCOME_REQUIRED')
        return verdict,str(outcome.get('reason','')),list(outcome.get('details',[])),True,'','LOW'
