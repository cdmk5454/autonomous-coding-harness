"""Monotonic execution signals, independent of transport and Job final state.

Uses the existing TimeoutConfig values. This is an in-process observation clock;
restart must reconcile durable evidence, never reconstruct an active lease.
"""
from dataclasses import dataclass
import time

class SignalError(RuntimeError):
    pass

@dataclass
class ExecutionSignals:
    identity: dict
    heartbeat_seconds: float = 15
    lease_seconds: float = 45
    progress_seconds: float = 900
    clock: object = time.monotonic

    def __post_init__(self):
        if min(self.heartbeat_seconds,self.lease_seconds,self.progress_seconds)<=0 or self.heartbeat_seconds>=self.lease_seconds:
            raise SignalError('INVALID_TIMER_CONFIGURATION')
        self.started=self.clock();self.last_heartbeat=self.started
        self.lease_until=self.started+self.lease_seconds
        self.last_progress=self.started;self.last_sequence=-1
        self.state='ACTIVE';self.progress_evidence=set();self.progress_kind='ADMITTED'

    def check(self):
        if self.state in {'QA_WAIT','QUIESCED'}:return self.state
        if self.state!='ACTIVE':raise SignalError('LEASE_RENEWAL_AFTER_FENCING')
        now=self.clock()
        if now>=self.lease_until:
            self.state='FENCING';raise SignalError('LEASE_EXPIRED')
        if now-self.last_progress>=self.progress_seconds:
            self.state='FENCING';raise SignalError('PROGRESS_TIMEOUT')
        return 'ACTIVE'

    def heartbeat(self, identity, *, sequence, observed, gateway_alive, run_alive, containers_alive):
        self.check()
        if self.state!='ACTIVE':raise SignalError('LEASE_NOT_ACTIVE')
        if identity!=self.identity:raise SignalError('HEARTBEAT_IDENTITY_MISMATCH')
        now=self.clock()
        if observed>now or observed<self.last_heartbeat or now-observed>=self.heartbeat_seconds:
            raise SignalError('DELAYED_HEARTBEAT')
        if sequence<=self.last_sequence:raise SignalError('DUPLICATE_HEARTBEAT')
        if not gateway_alive or not run_alive or not containers_alive:
            raise SignalError('RUNTIME_LIVENESS_LOST')
        self.last_sequence=sequence;self.last_heartbeat=observed
        # Observation time, never receipt time, defines renewal.
        self.lease_until=observed+self.lease_seconds

    def progress(self, kind, evidence_id, *, verified=False):
        self.check()
        if self.state!='ACTIVE':raise SignalError('PROGRESS_NOT_ACTIVE')
        allowed={'CANDIDATE_DIFF','TOOL_COMPLETED','BUILD_STAGE','TEST_STAGE','REVIEW_STAGE','COMMAND_COMPLETED','QA_TRANSITION','FIX_STARTED','VERIFICATION'}
        if not verified or kind not in allowed or not evidence_id:
            raise SignalError('MEANINGFUL_EVIDENCE_REQUIRED')
        key=(kind,evidence_id)
        if key in self.progress_evidence:return False
        self.progress_evidence.add(key);self.last_progress=self.clock();self.progress_kind=kind
        return True

    def stop(self, state, *, quiesced=False):
        if state not in {'QA_WAIT','QUIESCED'} or not quiesced:
            raise SignalError('QUIESCENCE_REQUIRED')
        self.state=state;self.lease_until=None
