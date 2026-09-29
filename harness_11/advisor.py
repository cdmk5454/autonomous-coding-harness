"""Semantic-only advisory escalation. Host adjudication, never keyword guessing."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Callable
import time
from .core import Binding, BoundaryError, Mode, canonical, digest, require, text, integer, number

DOMAINS=frozenset(('PRODUCT_SEMANTIC_RETRY','TECHNICAL_EXECUTION_RETRY','RUNTIME_RECOVERY','NONE'))
SEMANTIC_REASONS=frozenset(('REQUIREMENT_MISUNDERSTOOD','ACCEPTANCE_MISMATCH','SEMANTIC_REWORK'))

@dataclass(frozen=True)
class Failure:
    event_id: str
    job_id: str
    execution_id: str
    attempt_id: str
    contract_hash: str
    retry_domain: str
    reason: str
    host_receipt: str
    def __post_init__(self):
        from .core import hash64
        for x in (self.event_id,self.job_id,self.execution_id,self.attempt_id,self.reason,self.host_receipt): text(x,maximum=1000)
        hash64(self.contract_hash)
        require(self.retry_domain in DOMAINS,'RETRY_DOMAIN_INVALID')

@dataclass(frozen=True)
class AdvisorPolicy:
    mode: Mode=Mode.OFF
    threshold: int=2
    timeout_seconds: float=10
    max_output_bytes: int=16384
    def __post_init__(self):
        require(self.mode in (Mode.OFF,Mode.SHADOW),'ADVISOR_AUTHORITY_UNSUPPORTED')
        integer(self.threshold,1,100); number(self.timeout_seconds,.01,120); integer(self.max_output_bytes,128,65536)

class Advisor:
    """Callback must be an existing HOST bounded-runtime call (not a model handle).

    Future failures cannot alter past escalation receipts. Only same frozen
    contract/execution history is counted; technical retries are never promoted.
    """
    def __init__(self, policy: AdvisorPolicy=AdvisorPolicy(), *, clock=time.monotonic):
        self.policy=policy; self.clock=clock
    def evaluate(self,binding: Binding,history: tuple[Failure,...], *,
                 verify_receipt: Callable[[Failure],bool], invoke: Callable[...,dict] | None=None,
                 previously_invoked: frozenset[str]=frozenset()) -> dict:
        require(isinstance(binding,Binding) and type(history) is tuple and len(history)<=1000,
                'ADVISOR_HISTORY_INVALID')
        require(type(previously_invoked) is frozenset,'ADVISOR_HISTORY_INVALID')
        result={'binding_key':binding.key,'semantic_count':0,'status':'OFF','invoked':False,
                'recommendation':None,'elapsed_seconds':0.0,'cost_usd':None,
                'product_pass':False,'publication_authority':False,'contract_changed':False}
        if self.policy.mode==Mode.OFF:return result
        seen={}; relevant=[]
        for f in history:
            require(isinstance(f,Failure),'ADVISOR_HISTORY_INVALID')
            if f.event_id in seen:
                require(seen[f.event_id]==f,'FAILURE_RECEIPT_CONFLICT'); continue
            seen[f.event_id]=f
            if (f.job_id,f.execution_id,f.contract_hash)!=(binding.job_id,binding.execution_id,binding.contract_hash):continue
            # Only independently observed domain/reason can increment semantic count.
            if f.retry_domain=='PRODUCT_SEMANTIC_RETRY' and f.reason in SEMANTIC_REASONS:
                require(verify_receipt(f) is True,'ADVISOR_UNVERIFIED_FAILURE')
                relevant.append(f)
        # One semantic retry per failed Attempt, even when that Attempt emitted
        # several findings/events. The supplied tuple is chronological HOST history.
        by_attempt={}
        for f in relevant:by_attempt.setdefault(f.attempt_id,f)
        relevant=list(by_attempt.values())
        result['semantic_count']=len(relevant)
        trigger=history[-1] if history else None
        if trigger is None:
            result['status']='NO_TRIGGER';return result
        if (trigger.job_id,trigger.execution_id,trigger.contract_hash,trigger.attempt_id)!=(binding.job_id,binding.execution_id,binding.contract_hash,binding.attempt_id):
            result['status']='NO_CURRENT_TRIGGER';return result
        if trigger.retry_domain!='PRODUCT_SEMANTIC_RETRY' or trigger.reason not in SEMANTIC_REASONS:
            result['status']='NON_SEMANTIC_TRIGGER';return result
        if len(relevant)<self.policy.threshold:result['status']='BELOW_THRESHOLD';return result
        fingerprint=digest({'job':binding.job_id,'execution':binding.execution_id,'contract':binding.contract_hash,
                            'semantic_attempts':sorted(f.attempt_id for f in relevant)})
        result['escalation_key']=fingerprint
        if fingerprint in previously_invoked:result['status']='ALREADY_OBSERVED';return result
        if invoke is None:result['status']='ADVISOR_UNAVAILABLE';return result
        started=self.clock();result['invoked']=True
        try:
            response=invoke({'binding':asdict(binding),'semantic_failures':[asdict(f) for f in relevant],
                             'authority':'ADVISORY_ONLY'},timeout=self.policy.timeout_seconds,
                            max_bytes=self.policy.max_output_bytes)
            require(self.clock()-started<=self.policy.timeout_seconds,'ADVISOR_TIMEOUT')
            require(isinstance(response,dict) and set(response).issubset({'analysis','suggestions','cost_usd'}),'ADVISOR_SCHEMA_INVALID')
            require(len(canonical(response))<=self.policy.max_output_bytes,'ADVISOR_RESPONSE_TOO_LARGE')
            analysis=response.get('analysis'); suggestions=response.get('suggestions')
            require(isinstance(analysis,str) and len(analysis)<=8000 and type(suggestions) is list and len(suggestions)<=12,'ADVISOR_SCHEMA_INVALID')
            for s in suggestions:text(s,'ADVISOR_SCHEMA_INVALID',2000)
            cost=response.get('cost_usd')
            if cost is not None:number(cost)
            result.update(status='ADVISORY_EVIDENCE',recommendation={'analysis':analysis,'suggestions':list(suggestions)},cost_usd=cost)
        except BoundaryError as exc:result.update(status='SAFE_FALLBACK',error_code=exc.code)
        except TimeoutError:result.update(status='SAFE_FALLBACK',error_code='ADVISOR_TIMEOUT')
        except Exception:result.update(status='SAFE_FALLBACK',error_code='ADVISOR_FAILED')
        finally:result['elapsed_seconds']=max(0,self.clock()-started)
        return result
