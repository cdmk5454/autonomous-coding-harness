"""Hermes observations mapped into existing Harness ownership/progress ledgers."""
from dataclasses import asdict
from datetime import datetime,timezone,timedelta
import json,subprocess,time
from execution_liveness import ExecutionSignals,SignalError
from hermes_runtime_adapter import HermesError
from hermes_boundary import atomic,read,snapshot,inspect_receipt,DOCKER
from runtime_adapter import payload_sha256

class HermesSupervision:
    def __init__(self,adapter,task=None,*,config=None,clock=time.monotonic):
        from worker import TimeoutConfig
        self.a=adapter;self.b=adapter.binding;self.task=task
        self.config=config or TimeoutConfig.from_env();self.clock=clock
        self.identity={**asdict(self.b),'native_run_id':adapter.run_id,'native_session_id':adapter.session_id}
        self.signals=ExecutionSignals(self.identity,self.config.runtime_heartbeat,self.config.runtime_lease,self.config.inactivity,clock)
        self.sequence=0;self.next_heartbeat=clock();self.closed=False
        self.last_candidate=self.candidate_digest();self.last_progress_wall=self.wall()
        self.path=adapter.boundary.root/'liveness.json'
        if self.path.exists():raise HermesError('RESTART_RECONCILIATION_REQUIRED')
        self.persist('STARTED')

    @staticmethod
    def wall():return datetime.now(timezone.utc).isoformat()

    def candidate_digest(self):
        # mtime/timer-only activity is not meaningful progress.
        return payload_sha256({p:r['sha256'] for p,r in snapshot(self.a.boundary.candidate,exclude_git=True).items() if not p.startswith('.git/')})

    def containers(self):
        receipts=[read(p) for p in self.a.boundary.root.glob('inspect-*.json')]
        if not receipts:raise HermesError('CONTAINER_OWNERSHIP_MISSING')
        result=[]
        for receipt in receipts:
            cid=receipt['container_id']
            if receipt['binding']!=asdict(self.b):raise HermesError('STALE_CONTAINER_RECEIPT')
            retired=self.a.boundary.root/('lifecycle-'+cid+'.json')
            if retired.exists():
                lifecycle=read(retired)
                if lifecycle.get('binding')!=asdict(self.b):raise HermesError('STALE_CONTAINER_RECEIPT')
                if lifecycle.get('state') in {'REQUESTED','ACKNOWLEDGED'}:continue
            try:call=subprocess.run([DOCKER,'inspect',cid],capture_output=True,text=True,timeout=10)
            except subprocess.TimeoutExpired:raise HermesError('CONTAINER_OBSERVATION_UNKNOWN') from None
            if call.returncode:raise HermesError('CONTAINER_LOST')
            obj=json.loads(call.stdout)[0];checked=inspect_receipt(obj,self.a.boundary.policy)
            if checked['state']!='BOUNDARY_VERIFIED' or obj['Id']!=cid:raise HermesError('STALE_CONTAINER_RECEIPT')
            if not obj['State']['Running']:raise HermesError('CONTAINER_LOST')
            result.append({'container_id':cid,'running':True})
        return result

    def persist(self,event,extra=None):
        observed=self.wall();remaining=max(0,(self.signals.lease_until or self.clock())-self.clock())
        value={'event':event,'identity':self.identity,'state':self.signals.state,'observed_at':observed,
          'last_runtime_heartbeat_at':getattr(self,'heartbeat_wall',''),
          'lease_expires_at':(datetime.now(timezone.utc)+timedelta(seconds=remaining)).isoformat() if self.signals.state=='ACTIVE' else '',
          'last_progress_at':self.last_progress_wall,'progress_timeout_seconds':self.config.inactivity,
          'heartbeat_seconds':self.config.runtime_heartbeat,'lease_seconds':self.config.runtime_lease,
          'progress_kind':self.signals.progress_kind,'local_sequence':self.sequence,**(extra or {})}
        # Schema-5 runtime process payload is durable; no new DB/table/sequence.
        self.a.repository.record_execution_observation(self.b.attempt_id,identity=self.identity,observation=value)
        atomic(self.path,value)
        import uuid
        atomic(self.path.with_name('observation-'+uuid.uuid4().hex+'.json'),value)
        if self.task is not None:
            projection={**value,'execution_id':self.b.execution_id,'attempt_id':self.b.attempt_id,
                'execution_role':self.b.role,'execution_owner':self.b.host_boundary_identity,
                'binding_id':self.b.binding_id,'execution_epoch':self.b.execution_epoch,'runtime':'hermes',
                'process_alive':self.signals.state=='ACTIVE','pid':self.a.boundary.server_identity['pid']}
            if event in {'QA_WAIT','ENDED','RECOVERY_REQUIRED'}:projection['ended_at']=observed
            self.task.execution_runtime=projection
            from progress_projection import project_execution_health
            current=dict(self.task.progress_snapshot or {})
            if not getattr(self.task,'_runtime_event_callback',None):
                current['execution_health']=project_execution_health(self.task)
            if event=='PROGRESS':
                self.task.progress_revision+=1
                current['progress_revision']=self.task.progress_revision
                current['last_meaningful_event']={'type':self.signals.progress_kind,'at':observed,'summary':self.signals.progress_kind}
            self.task.progress_snapshot=current
            callback=getattr(self.task,'_runtime_event_callback',None)
            # Existing callback persists TaskState and only reports semantic changes.
            # No STARTED: process ledger was already admitted with PID+ctime.
            if callback:callback({**projection,'event':'PROGRESS' if event=='PROGRESS' else 'HEARTBEAT'})
        return value

    def tick(self,status,*,renew=True):
        self.signals.check()  # An expired lease must never be resurrected by a late response.
        self.a._current(terminal_ok=True)
        if status.get('run_id')!=self.a.run_id or status.get('session_id')!=self.a.session_id:
            raise HermesError('FOREIGN_RUN_OR_SESSION')
        terminal=status.get('status') in {'completed','failed','cancelled','interrupted'}
        # Query completion is evidence; no synthetic terminal SSE or Job success.
        if terminal:
            self.a.gap('TERMINAL_STATUS_REQUIRES_EVIDENCE_GATES')
            self.persist('STATUS_RECONCILIATION',{'native_status':status['status'],'candidate_digest':self.candidate_digest()})
            return
        if self.a.repository.global_stop().get('active') or (self.b.role=='WORKER' and self.a.repository.writer_authority(self.b.execution_id)['state']!='ACTIVE'):
            raise HermesError('WRITER_AUTHORITY_REVOKED')
        observed=self.clock()
        if renew and observed>=self.next_heartbeat:
            containers=self.containers()
            from hermes_process_boundary import identity
            atomic(self.a.boundary.root/'host-members.json',[identity(pid) for pid in self.a.boundary.process.members()])
            self.signals.heartbeat(self.identity,sequence=self.sequence,observed=observed,
                gateway_alive=True,run_alive=status.get('status') in {'running','queued','waiting_for_approval'},containers_alive=bool(containers))
            self.sequence+=1;self.next_heartbeat=observed+self.config.runtime_heartbeat
            self.heartbeat_wall=self.wall()
            self.persist('HEARTBEAT',{'source':['OWNED_PROCESS','RUN_STATUS','ACTUAL_CONTAINER_INSPECT'],'containers':containers})
        for path in self.a.boundary.root.glob('exec-completed-*.json'):
            receipt=read(path)
            if receipt.get('binding')!=asdict(self.b):raise HermesError('FOREIGN_TOOL_RECEIPT')
            if self.signals.progress('TOOL_COMPLETED',path.stem,verified=True):
                self.last_progress_wall=self.wall();self.persist('PROGRESS',{'tool_receipt':str(path),'origin':'HARNESS_PROXY_COMMAND_RECEIPT'})
        try: digest=self.candidate_digest()
        except FileNotFoundError:
            # Atomic replacement may briefly remove a candidate file. This is
            # neither progress nor loss of authority; a later sample must prove it.
            return
        if digest!=self.last_candidate:
            self.last_candidate=digest
            if self.signals.progress('CANDIDATE_DIFF',digest,verified=True):
                self.last_progress_wall=self.wall();self.persist('PROGRESS',{'candidate_digest':digest})

    def finish(self,proof,*,qa=False):
        self.signals.stop('QA_WAIT' if qa else 'QUIESCED',quiesced=proof.get('state')=='QUIESCED')
        self.persist('QA_WAIT' if qa else 'ENDED');self.closed=True

    def recover(self,code,*,stable_seconds=3):
        self.signals.state='FENCING'
        if self.b.role=='WORKER':
            self.a.repository.suspend_writer_authority(self.b.execution_id,reason='HERMES_'+code)
        self.persist('RECOVERY_REQUIRED',{'failure_code':code,'retry_domain':'RUNTIME_RECOVERY','product_retry_delta':0})
        proof=self.a.boundary.fence(stable_seconds=stable_seconds)
        self.a.repository.record_runtime_recovery(self.b.execution_id,self.b.attempt_id,
            failure_code='HERMES_'+code,state='FRESH_ATTEMPT_REQUIRED',binding_id=self.a.native_binding_id,
            evidence={'binding':asdict(self.b),'writer_quiescence':{'quiesced':proof['state']=='QUIESCED','proof':str(self.a.boundary.root/'quiescence.json')},
              'candidate_digest':self.candidate_digest(),'command_reconciliation_required':True,'candidate_disposition_required':True})
        if proof['state']!='QUIESCED':raise HermesError('WRITER_QUIESCENCE_UNKNOWN')
        self.finish(proof)
        return proof


def verify_recovery_candidate(boundary_root,candidate):
    """Recheck a durable quiescence claim before policy admits any new Attempt."""
    from pathlib import Path
    proof=read(Path(boundary_root)/'quiescence.json')
    if proof.get('state')!='QUIESCED' or not proof.get('samples') or snapshot(candidate)!=proof['samples'][-1]['candidate']:
        raise HermesError('CANDIDATE_CHANGED_AFTER_QUIESCENCE')
    return proof


def authorize_replacement(repository,boundary_root,candidate):
    """Explicit Harness policy choice: retain partial delta for new verification."""
    from pathlib import Path
    proof=verify_recovery_candidate(boundary_root,candidate)
    b=proof['binding']
    state=read(Path(boundary_root)/'state.json')
    if state.get('state')!='FENCING' or state.get('epoch')!=b['execution_epoch']:
        raise HermesError('OLD_EPOCH_NOT_INVALID')
    digest=payload_sha256({p:r['sha256'] for p,r in snapshot(candidate).items() if not p.startswith('.git/')})
    return repository.authorize_runtime_recovery(b['execution_id'],b['attempt_id'],
        candidate_disposition='PRESERVE_FOR_REVERIFICATION',candidate_digest=digest,
        confirmation='AUTHORIZE_NEW_ATTEMPT_AFTER_RECONCILIATION')
