"""Single-attempt Pi RPC adapter conforming to runtime-adapter/2.

Only an explicitly supplied Harness boundary can own/start/fence the process.
No host subprocess fallback, provider autodiscovery, commit, publish or acceptance.
Native session resume/fork/steer remain UNSUPPORTED in this first Worker adapter.
"""
from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
from typing import Mapping, Protocol, Any
import time
import uuid
from runtime_adapter import (RuntimeDescriptor, RuntimeCapabilities, RuntimeCapabilityError,
    NormalizedRuntimeEvent, TurnInvocation, SessionQuery, CancellationResult, CancellationState)
from .core import (Binding, ProcessIdentity, Scope, FenceProof, BoundaryError, require,
                   strict_json, canonical, digest, text, number)

class PiProcessBoundary(Protocol):
    """Trusted host port. Calls must have bounded I/O and no uncontrolled children.

    verify_scope checks OS-enforced candidate-only writes + read-only context,
    no canonical mounts, isolated home/extensions, no commit credentials.
    fence returns evidence from the owner, not parsed model stdout.
    """
    def verify_scope(self, binding: Binding, scope: Scope) -> bool: ...
    def start(self, binding: Binding, scope: Scope, *, timeout: float) -> ProcessIdentity: ...
    def send(self, raw: bytes, *, timeout: float) -> None: ...
    def poll(self) -> int | None: ...
    def read(self, *, timeout: float, maximum: int) -> tuple[bytes, bytes]: ...
    def fence(self, binding: Binding, process: ProcessIdentity, token: int, *, timeout: float) -> FenceProof: ...

class LFJsonDecoder:
    def __init__(self, frame_limit=65_536, total_limit=8_388_608):
        from .core import integer
        integer(frame_limit,1,1_048_576); integer(total_limit,frame_limit,67_108_864)
        self.frame_limit, self.total_limit = frame_limit, total_limit
        self.buffer = bytearray(); self.total = 0; self.failed = False
    def feed(self, chunk: bytes) -> tuple[dict,...]:
        require(not self.failed and isinstance(chunk,bytes), 'PI_DECODER_CLOSED')
        try:
            self.total += len(chunk)
            require(self.total <= self.total_limit, 'PI_STREAM_LIMIT')
            self.buffer.extend(chunk); result=[]
            while True:
                end=self.buffer.find(b'\n')
                if end<0: break
                require(end<=self.frame_limit, 'PI_FRAME_LIMIT')
                raw=bytes(self.buffer[:end]); del self.buffer[:end+1]
                if raw.endswith(b'\r'): raw=raw[:-1]
                require(bool(raw), 'PI_EMPTY_RECORD')
                event=strict_json(raw,self.frame_limit)
                require(isinstance(event,dict) and isinstance(event.get('type'),str), 'PI_EVENT_SCHEMA')
                result.append(event)
            require(len(self.buffer)<=self.frame_limit,'PI_FRAME_LIMIT')
            return tuple(result)
        except BoundaryError:
            self.failed=True; self.buffer.clear(); raise
    def finish(self):
        require(not self.failed and not self.buffer, 'PI_PARTIAL_RECORD_AT_EOF')

class _OwnedRecord(Mapping):
    def __init__(self,payload,seal,process_key):
        self._payload=dict(payload);self.seal=seal;self.process_key=process_key
    def __iter__(self):return iter(self._payload)
    def __len__(self):return len(self._payload)
    def __getitem__(self,key):return self._payload[key]

class PiRuntimeAdapter:
    runtime_name='pi'
    def __init__(self, binding: Binding, scope: Scope, *, boundary: PiProcessBoundary | None=None,
                 runtime_version: str='', provider: str='', model: str='', enabled: bool=False,
                 startup_timeout: float=10, ack_timeout: float=10, io_timeout: float=1,
                 heartbeat_timeout: float=60, progress_timeout: float=900, clock=time.monotonic):
        require(binding.role=='WORKER','PI_WORKER_ONLY')
        require(type(enabled) is bool,'PI_ENABLE_INVALID')
        for value in (startup_timeout,ack_timeout,io_timeout,heartbeat_timeout,progress_timeout): number(value,0.001,86400)
        self.binding,self.scope,self.boundary=binding,scope,boundary
        self.runtime_version,self.provider,self.model=runtime_version,provider,model
        self.enabled=enabled; self.clock=clock
        self.startup_timeout=startup_timeout; self.ack_timeout=ack_timeout; self.io_timeout=io_timeout
        self.heartbeat_timeout=heartbeat_timeout; self.progress_timeout=progress_timeout
        self.state='NEW'; self.process=None; self.decoder=LFJsonDecoder()
        self.session_id=''; self.native_session_id=''; self.command_id=''; self.startup_id=''; self.cancel_id=''
        self.started_at=None; self.sent_at=None; self.last_heartbeat=None; self.last_progress=None
        self.progress_fingerprint=''; self.started_run=False; self.native_settled=False
        self.events=[]; self.result_text=''; self.stderr_bytes=0; self.failure_code=''
        self.token=0; self.last_proof=None;self._channel_seal=object()

    def preflight(self) -> RuntimeDescriptor:
        supported='PARTIAL' if self.runtime_version else 'UNKNOWN'
        from pi_process_boundary import DockerPiProcessBoundary
        owned = isinstance(self.boundary, DockerPiProcessBoundary)
        return RuntimeDescriptor(runtime='pi',version=self.runtime_version,
            capabilities=RuntimeCapabilities(False,False,True,False,False,True,interrupt=True),
            protocol_revision='pi-rpc-jsonl-single-attempt/1',
            structured_output_schema_revision='harness11-pi-observation/1',
            permission_tool_profile=('pi-candidate-rw-context-ro-docker/1' if owned
                                     else 'pi-candidate-rw-context-ro-host-boundary/1'),
            session_process_lifetime='SINGLE_ATTEMPT_PROCESS_NO_REUSE',
            sandbox_semantics='DOCKER_ENFORCED' if owned else 'REQUIRES_VERIFIED_EXTERNAL_BOUNDARY',
            execution_environment='PINNED_DOCKER_CANDIDATE' if owned else 'EXPLICIT_HOST_BOUNDARY',
            capability_contract={'session.create':supported,'session.read_query':'PARTIAL',
                'turn.submit':supported,'turn.interrupt':'PARTIAL','events.subscribe':'PARTIAL',
                'output.structured':'PARTIAL','output.schema_revision':'SUPPORTED',
                'runtime.protocol_revision':'PARTIAL','runtime.permission_tool_semantics':'PARTIAL'},
            live_model_validated=False,validation_disposition='NOT_RUN',
            detail='SOURCE_BOUNDARY_CANDIDATE; NATIVE_RUNTIME_ATTESTATION_REQUIRED')

    def start(self) -> ProcessIdentity:
        require(self.enabled and self.boundary is not None,'PI_DISABLED_OR_BOUNDARY_UNAVAILABLE')
        require(self.state=='NEW','PI_ALREADY_STARTED')
        text(self.runtime_version,'PI_PINNED_VERSION_REQUIRED');text(self.provider,'PI_PROVIDER_REQUIRED');text(self.model,'PI_MODEL_REQUIRED')
        require(self.boundary.verify_scope(self.binding,self.scope) is True,'PI_BOUNDARY_NOT_ENFORCED')
        self.state='STARTING'; self.started_at=number(self.clock())
        self.last_progress=self.started_at; self.last_heartbeat=self.started_at
        try:
            p=self.boundary.start(self.binding,self.scope,timeout=self.startup_timeout)
            if isinstance(p,ProcessIdentity):self.process=p  # Preserve custody even if identity is rejected.
            require(isinstance(p,ProcessIdentity) and p.runtime=='pi' and p.provider==self.provider,'PI_PROCESS_IDENTITY_INVALID')
            self.startup_id='PI-START-'+uuid.uuid4().hex
            self.boundary.send(canonical({'id':self.startup_id,'type':'get_state'})+b'\n',timeout=self.io_timeout)
            self.check_timeouts()
            return p
        except Exception as exc:
            self.failure_code=getattr(exc,'code','PI_STARTUP_FAILED');self.state='FAILED'
            raise BoundaryError(self.failure_code) from None

    def bind_admission(self, token: int):
        from .core import integer
        integer(token,1);require(self.token==0 and self.process is not None,'PI_ADMISSION_BINDING_INVALID');self.token=token

    def _cwd(self,cwd):
        require(str(Path(cwd).resolve())==self.scope.candidate_root,'PI_CWD_MISMATCH')
        require(self.boundary is not None and self.boundary.verify_scope(self.binding,self.scope) is True,'PI_BOUNDARY_DRIFT')

    def create_session(self,prompt,*,cwd,model='',reasoning_effort='medium',command_id='') -> TurnInvocation:
        self._cwd(cwd)
        require(self.state=='READY' and self.token>0,'PI_NOT_READY_OR_UNADMITTED')
        require(isinstance(prompt,str) and 0<len(prompt.encode('utf-8'))<=262_144,'PI_PROMPT_BOUNDARY')
        require(not model or model==self.model,'PI_MODEL_DRIFT')
        # Reasoning level is fixed at boundary construction/version setup, not a mutable command.
        cmd=command_id or 'PI-PROMPT-'+uuid.uuid4().hex; text(cmd)
        body={'id':cmd,'type':'prompt','message':prompt}
        return TurnInvocation(command_id=cmd,session_id=self.session_id,transport='PI_OWNED_RPC',method='prompt',body=body)

    def submit_turn(self,session_id,prompt,*,cwd,model='',reasoning_effort='medium',command_id=''):
        require(not session_id or session_id==self.session_id,'PI_SESSION_MISMATCH')
        return self.create_session(prompt,cwd=cwd,model=model,reasoning_effort=reasoning_effort,command_id=command_id)

    def dispatch(self,invocation: TurnInvocation):
        require(self.state=='READY' and self.token>0,'PI_NOT_READY_OR_UNADMITTED')
        require(isinstance(invocation,TurnInvocation) and invocation.transport=='PI_OWNED_RPC'
                and invocation.method=='prompt' and invocation.session_id==self.session_id,'PI_INVOCATION_INVALID')
        body=dict(invocation.body)
        require(set(body)=={'id','type','message'} and body['type']=='prompt'
                and body['id']==invocation.command_id,'PI_INVOCATION_INVALID')
        self._cwd(self.scope.candidate_root)
        require(isinstance(body['message'],str) and 0<len(body['message'].encode('utf-8'))<=262_144,'PI_PROMPT_BOUNDARY')
        self.command_id=invocation.command_id;self.state='SENT';self.sent_at=number(self.clock())
        self.last_progress=self.sent_at;self.last_heartbeat=self.sent_at
        try: self.boundary.send(canonical(body)+b'\n',timeout=self.io_timeout)
        except Exception:
            self.state='FAILED';self.failure_code='PI_SEND_DELIVERY_UNKNOWN';raise BoundaryError(self.failure_code) from None

    def _event(self,kind,*,terminal=False,error='',text_value='',raw_type=''):
        return NormalizedRuntimeEvent(kind=kind,runtime='pi',session_id=self.session_id,
            turn_id=self.command_id,terminal=terminal,success=None,result_text=text_value,error_code=error,
            raw_type=raw_type,execution_id=self.binding.execution_id,attempt_id=self.binding.attempt_id,
            binding_id=self.binding.key,command_id=self.command_id,execution_epoch=self.binding.epoch,
            server_incarnation=self.process.key if self.process else '',observation_origin='OWNED_PI_RPC_NOT_PRODUCT_EVIDENCE')

    def observe_owned(self,event: Mapping[str,Any],*,process_key: str):
        require(self.process is not None and process_key==self.process.key,'PI_UNOWNED_EVENT_CHANNEL')
        require(isinstance(event,Mapping) and self.state not in ('NEW','QUIESCED','FAILED'),'PI_EVENT_STATE_INVALID')
        require(len(canonical(dict(event)))<=65_536,'PI_FRAME_LIMIT')
        # Native events lack command IDs. Fresh process + one prompt is the custody contract.
        if 'execution_epoch' in event: require(event['execution_epoch']==self.binding.epoch,'PI_STALE_EVENT')
        now=number(self.clock());self.check_timeouts()
        kind=event.get('type'); result=None
        if kind=='response':
            require(type(event.get('success')) is bool,'PI_RESPONSE_SCHEMA')
            eid=event.get('id'); command=event.get('command')
            if self.state=='STARTING' and eid==self.startup_id and command=='get_state':
                require(event['success'] and isinstance(event.get('data'),dict),'PI_STARTUP_REJECTED')
                native=event['data'].get('sessionId','')
                require(isinstance(native,str),'PI_NATIVE_SESSION_SCHEMA')
                if native:text(native,'PI_NATIVE_SESSION_SCHEMA',200)
                self.native_session_id=native;self.session_id=native or 'HARNESS-PI-'+self.binding.key
                self.state='READY';result=self._event('SESSION_READY',raw_type=kind)
            elif eid==self.command_id and command=='prompt' and self.state=='SENT':
                if not event['success']:
                    self.state='FAILED';self.failure_code='PI_COMMAND_REJECTED';result=self._event('COMMAND_REJECTED',error=self.failure_code,raw_type=kind)
                else:
                    self.state='SETTLED' if self.native_settled else 'ACTIVE'
                    result=self._event('TURN_SETTLED' if self.native_settled else 'COMMAND_ACK',terminal=self.native_settled,raw_type=kind)
            elif eid==self.cancel_id and command=='abort' and self.state=='CANCELLING':
                require(event['success'],'PI_ABORT_REJECTED');result=self._event('CANCEL_ACK_NOT_QUIESCED',raw_type=kind)
            else: raise BoundaryError('PI_UNCORRELATED_OR_DUPLICATE_RESPONSE')
        elif kind=='agent_start':
            require(self.state in ('SENT','ACTIVE'),'PI_RUN_STATE_INVALID')
            self.started_run=True;result=self._event('NATIVE_RUN_STARTED',raw_type=kind)
        elif kind in ('agent_end','agent_settled'):
            require(self.state in ('SENT','ACTIVE','CANCELLING') and self.started_run,'PI_STALE_OR_PRESTART_TERMINAL')
            if kind=='agent_settled':
                self.native_settled=True
                if self.state=='ACTIVE': self.state='SETTLED'
            result=self._event('TURN_SETTLED' if self.state=='SETTLED' else 'NATIVE_END_NOT_FENCE',terminal=self.state=='SETTLED',raw_type=kind)
        elif kind=='message_update':
            require(self.state in ('SENT','ACTIVE','CANCELLING'),'PI_MESSAGE_STATE_INVALID')
            update=event.get('assistantMessageEvent')
            require(isinstance(update,dict),'PI_MESSAGE_SCHEMA')
            delta=update.get('delta','') if update.get('type')=='text_delta' else ''
            require(isinstance(delta,str),'PI_RESULT_SCHEMA')
            require(len(self.result_text.encode('utf-8'))+len(delta.encode('utf-8'))<=262_144,'PI_RESULT_LIMIT')
            self.result_text+=delta;result=self._event('NARRATIVE_ONLY',text_value=delta,raw_type=kind)
        elif kind=='message_end':
            require(self.state in ('SENT','ACTIVE','CANCELLING'),'PI_MESSAGE_STATE_INVALID')
            msg=event.get('message');require(isinstance(msg,dict),'PI_MESSAGE_SCHEMA')
            reason=msg.get('stopReason')
            if reason in ('error','aborted'):
                self.failure_code='PI_PROVIDER_ERROR' if reason=='error' else 'PI_NATIVE_ABORTED'
            result=self._event('MESSAGE_OBSERVATION',error=self.failure_code,raw_type=kind)
        else:
            require(isinstance(kind,str) and self.state in ('SENT','ACTIVE','CANCELLING'),'PI_EVENT_SCHEMA')
            result=self._event('NATIVE_OBSERVATION',raw_type=kind)
        self.last_heartbeat=now
        return (result,)

    def normalize_event(self,event):
        # JSON cannot mint this in-memory host-owned observation capability.
        require(isinstance(event,_OwnedRecord) and event.seal is self._channel_seal,'PI_UNOWNED_EVENT_CHANNEL')
        return self.observe_owned(event,process_key=event.process_key)

    def poll_once(self):
        try:
            return self._poll_once()
        except BoundaryError as exc:
            self.state='FAILED';self.failure_code=exc.code;raise
        except Exception:
            self.state='FAILED';self.failure_code='PI_TRANSPORT_FAILED'
            raise BoundaryError(self.failure_code) from None

    def _poll_once(self):
        require(self.boundary is not None and self.process is not None,'PI_NOT_STARTED')
        self.check_timeouts()
        try: stdout,stderr=self.boundary.read(timeout=self.io_timeout,maximum=65_536)
        except TimeoutError:
            self.check_timeouts();return ()
        require(isinstance(stdout,bytes) and isinstance(stderr,bytes),'PI_TRANSPORT_SCHEMA')
        require(len(stdout)<=65_536 and len(stderr)<=65_536,'PI_TRANSPORT_BOUNDARY')
        self.stderr_bytes+=len(stderr)
        require(self.stderr_bytes<=1_048_576,'PI_STDERR_LIMIT')
        # No stderr content is copied into evidence, errors or user logs.
        out=[]
        for event in self.decoder.feed(stdout):
            out.extend(self.normalize_event(_OwnedRecord(event,self._channel_seal,self.process.key)))
        code=self.boundary.poll()
        if code is not None:
            self.decoder.finish()
            if code!=0 or self.state not in ('SETTLED','CANCELLING','QUIESCED'):
                self.failure_code='PI_PROCESS_EXITED';self.state='FAILED'
                out.append(self._event('PROCESS_EXIT',error=self.failure_code))
        self.check_timeouts()
        return tuple(out)

    def observe_progress(self, fingerprint: str):
        from .core import hash64
        hash64(fingerprint)
        require(self.state in ('SENT','ACTIVE'),'PI_PROGRESS_STATE_INVALID')
        # Caller is the host snapshot/test observer. Native token activity is not progress.
        if fingerprint!=self.progress_fingerprint:
            self.progress_fingerprint=fingerprint;self.last_progress=number(self.clock())

    def check_timeouts(self):
        now=number(self.clock())
        code=''
        if self.started_at is not None and now<self.started_at: code='PI_CLOCK_REGRESSED'
        elif self.state=='STARTING' and now-self.started_at>=self.startup_timeout: code='PI_STARTUP_TIMEOUT'
        elif self.state=='SENT' and now-self.sent_at>=self.ack_timeout: code='PI_ACK_TIMEOUT'
        elif self.state=='ACTIVE' and now-self.last_heartbeat>=self.heartbeat_timeout: code='PI_HEARTBEAT_STALE'
        elif self.state=='ACTIVE' and now-self.last_progress>=self.progress_timeout: code='PI_PROGRESS_TIMEOUT'
        if code:
            self.failure_code=code;self.state='FAILED';raise BoundaryError(code)

    def interrupt(self,session_id,turn_id=''):
        require(session_id==self.session_id and (not turn_id or turn_id==self.command_id),'PI_CANCEL_BINDING_MISMATCH')
        require(self.state in ('SENT','ACTIVE','SETTLED','CANCELLING','FAILED'),'PI_CANCEL_STATE_INVALID')
        if self.state!='CANCELLING':
            self.cancel_id='PI-CANCEL-'+uuid.uuid4().hex;self.state='CANCELLING'
            try: self.boundary.send(canonical({'id':self.cancel_id,'type':'abort'})+b'\n',timeout=self.io_timeout)
            except Exception: self.failure_code='PI_CANCEL_DELIVERY_UNKNOWN'
        return CancellationResult(CancellationState.REQUESTED,self.binding.key,self.command_id)

    def fence(self,*,timeout=10):
        number(timeout,0.001,300)
        require(self.process is not None and self.boundary is not None and (self.token>0 or not self.command_id),'PI_FENCE_BINDING_REQUIRED')
        proof=self.boundary.fence(self.binding,self.process,self.token,timeout=timeout)
        require(isinstance(proof,FenceProof) and proof.matches(self.binding,self.process,self.token),'PI_QUIESCENCE_UNKNOWN')
        self.last_proof=proof;self.state='QUIESCED'
        return CancellationResult(CancellationState.QUIESCED,self.binding.key,self.command_id)

    def query_session(self,session_id,*,cwd):
        require(str(Path(cwd).resolve())==self.scope.candidate_root,'PI_CWD_MISMATCH')
        require(session_id==self.session_id,'PI_SESSION_MISMATCH')
        mapped={'READY':'IDLE','SETTLED':'IDLE','QUIESCED':'TERMINAL','ACTIVE':'ACTIVE','SENT':'ACTIVE'}
        return SessionQuery(session_id,mapped.get(self.state,'UNKNOWN'),current_turn_id=self.command_id,
                            terminal=self.state=='QUIESCED',detail='HOST_OBSERVATION_NOT_PRODUCT_ACCEPTANCE')
    def query_terminal(self,session_id,turn_id='',*,cwd):
        require(not turn_id or turn_id==self.command_id,'PI_TURN_MISMATCH')
        return self.query_session(session_id,cwd=cwd)
    def reconcile(self,session_id,*,cwd):
        return self.query_session(session_id,cwd=cwd)
    def pending_approval_or_question(self,session_id,*,cwd):
        return self.query_session(session_id,cwd=cwd)
    def subscribe_events(self,session_id,*,cursor=''):
        require(session_id==self.session_id and not cursor,'PI_EVENT_REPLAY_UNSUPPORTED')
        return self.poll_once()
    def resume_session(self,*args,**kwargs): raise RuntimeCapabilityError('PI_RESUME_UNSUPPORTED')
    def fork_session(self,*args,**kwargs): raise RuntimeCapabilityError('PI_FORK_UNSUPPORTED')
    def steer_turn(self,*args,**kwargs): raise RuntimeCapabilityError('PI_STEER_UNSUPPORTED')
    def replay_events(self,*args,**kwargs): raise RuntimeCapabilityError('PI_REPLAY_UNSUPPORTED')
    def respond_interaction(self,*args,**kwargs): raise RuntimeCapabilityError('PI_PERMISSION_OVERRIDE_FORBIDDEN')
