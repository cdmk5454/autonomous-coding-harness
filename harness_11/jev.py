"""JEv typed SHADOW adapter, protocol: https://docs.typesafe.ai/api.

No state handle, publication hook, route mutation, import I/O, SDK auto-retry,
or credential in argv. A shadow failure returns the exact deterministic policy.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Protocol, Mapping
import os
import re
import subprocess
import sys
import threading
import time
from .core import (Binding, Mode, PolicySnapshot, BoundaryError, require, text,
                   number, integer, canonical, digest, strict_json)

class UseCase(str, Enum):
    NEEDS_SUPERVISOR = 'NEEDS_SUPERVISOR'
    RUNTIME_ROUTE_SIGNAL = 'RUNTIME_ROUTE_SIGNAL'
    SEMANTIC_FAILURE_SIGNAL = 'SEMANTIC_FAILURE_SIGNAL'
    CONTEXT_RELEVANCE = 'CONTEXT_RELEVANCE'
    OFFLINE_EVALUATOR = 'OFFLINE_EVALUATOR'

@dataclass(frozen=True)
class Noul:
    instructions: str
    def __post_init__(self): text(self.instructions, 'QUESTION_INVALID', 8000)
    def wire(self): return {'type':'noul', 'instructions':self.instructions}

@dataclass(frozen=True)
class Choice:
    instructions: str
    options: tuple[tuple[str,str | None], ...]
    def __post_init__(self):
        text(self.instructions, 'QUESTION_INVALID', 8000)
        require(type(self.options) is tuple and 1<=len(self.options)<=255, 'CHOICE_INVALID')
        for item in self.options:
            require(type(item) is tuple and len(item)==2, 'CHOICE_INVALID')
            text(item[0], 'CHOICE_INVALID', 200)
            if item[1] is not None: text(item[1], 'CHOICE_INVALID', 2000)
        require(len(dict(self.options))==len(self.options), 'CHOICE_DUPLICATE')
    def wire(self): return {'type':'choice','instructions':self.instructions,'criteria':dict(self.options)}

@dataclass(frozen=True)
class Score:
    instructions: str
    levels: tuple[str,...]
    def __post_init__(self):
        text(self.instructions, 'QUESTION_INVALID', 8000)
        require(type(self.levels) is tuple and 2<=len(self.levels)<=10, 'SCORE_INVALID')
        for level in self.levels: text(level, 'SCORE_INVALID', 2000)
        require(len(set(self.levels))==len(self.levels), 'SCORE_DUPLICATE_LEVEL')
    def wire(self): return {'type':'score','instructions':self.instructions,'criteria':list(self.levels)}

@dataclass(frozen=True)
class Question:
    question_id: str
    use_case: UseCase
    decision: Noul | Choice | Score
    def __post_init__(self):
        require(isinstance(self.question_id,str) and bool(re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,63}',self.question_id)), 'QUESTION_ID_INVALID')
        require(isinstance(self.use_case,UseCase) and isinstance(self.decision,(Noul,Choice,Score)), 'QUESTION_INVALID')
        if self.use_case==UseCase.RUNTIME_ROUTE_SIGNAL:
            require(isinstance(self.decision,Choice), 'ROUTING_REQUIRES_CHOICE')

@dataclass(frozen=True)
class Price:
    """Optional operator-supplied rate sheet, not a claimed provider invoice."""
    model: str
    input_per_million_usd: float
    output_per_million_usd: float
    source_ref: str
    def __post_init__(self):
        text(self.model); text(self.source_ref, maximum=1000)
        number(self.input_per_million_usd,0,100000); number(self.output_per_million_usd,0,100000)

@dataclass(frozen=True)
class JevConfig:
    model: str
    mode: Mode = Mode.SHADOW
    external_calls_authorized: bool = False
    timeout_seconds: float = 5.0
    max_calls: int = 1
    request_limit: int = 65536
    response_limit: int = 65536
    api_key_env: str = 'JEV_API_KEY'
    def __post_init__(self):
        text(self.model)
        require(self.mode in (Mode.OFF,Mode.SHADOW), 'JEV_PRODUCTION_AUTHORITY_UNSUPPORTED')
        require(type(self.external_calls_authorized) is bool, 'AUTHORIZATION_BOOLEAN_REQUIRED')
        number(self.timeout_seconds,0.05,60); integer(self.max_calls,0,10000)
        integer(self.request_limit,256,262144); integer(self.response_limit,256,1048576)
        require(bool(re.fullmatch('[A-Z][A-Z0-9_]{0,63}',self.api_key_env)), 'SECRET_REFERENCE_INVALID')

class Transport(Protocol):
    def __call__(self, body: bytes, secret: str, *, timeout: float, limit: int) -> tuple[int,bytes]: ...

class IsolatedHttpsTransport:
    """Hard wall-clock deadline also covers DNS/TLS; no redirect or HTTP fallback.

    The owned one-shot stdlib child has no model, shell, or Product tools. Its
    stdout is bounded by trusted code before parent communicate() collects it.
    Credential is piped, never passed in argv, inherited env, errors or logs.
    """
    def __call__(self, body: bytes, secret: str, *, timeout: float, limit: int) -> tuple[int,bytes]:
        worker=Path(__file__).with_name('_jev_http_worker.py')
        env={k:os.environ[k] for k in ('SYSTEMROOT','WINDIR') if k in os.environ}
        request=canonical({'body':body.decode('utf-8'),'secret':secret,'timeout':timeout,'limit':limit})
        try:
            completed=subprocess.run([sys.executable,'-I',str(worker)],input=request,
                                     stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
                                     timeout=timeout,env=env,check=False)
        except subprocess.TimeoutExpired:
            raise BoundaryError('JEV_TIMEOUT') from None
        except (OSError,ValueError):
            raise BoundaryError('JEV_TRANSPORT_UNAVAILABLE') from None
        require(completed.returncode==0, 'JEV_TRANSPORT_FAILED')
        result=strict_json(completed.stdout,limit*6+4096)
        require(isinstance(result,dict),'JEV_TRANSPORT_INVALID')
        if result.get('error'):
            code=result['error']
            raise BoundaryError(code if code in ('JEV_TIMEOUT','JEV_NETWORK_ERROR','JEV_RESPONSE_TOO_LARGE') else 'JEV_TRANSPORT_FAILED')
        integer(result.get('status'),100,599,'JEV_HTTP_INVALID')
        raw=result.get('body')
        require(isinstance(raw,str),'JEV_TRANSPORT_INVALID')
        return result['status'],raw.encode('utf-8')

class JevAdapter:
    def __init__(self, config: JevConfig, *, transport: Transport | None=None,
                 env: Mapping[str,str] | None=None, clock=time.monotonic):
        require(isinstance(config,JevConfig),'JEV_CONFIG_REQUIRED')
        self.config=config; self.transport=transport; self.env=env; self.clock=clock
        self._lock=threading.Lock(); self._calls=0

    @staticmethod
    def _probabilities(value, keys):
        require(isinstance(value,dict) and set(value)==set(keys), 'JEV_PROBABILITIES_INVALID')
        result={k:number(v,0,1,'JEV_PROBABILITIES_INVALID') for k,v in value.items()}
        require(abs(sum(result.values())-1)<=1e-5,'JEV_PROBABILITIES_INVALID')
        return result

    @classmethod
    def validate(cls, raw: bytes, questions: tuple[Question,...], limit: int) -> dict:
        result=strict_json(raw,limit)
        require(isinstance(result,dict) and set(result)=={'model','answers','usage'},'JEV_SCHEMA_INVALID')
        text(result['model'],'JEV_SCHEMA_INVALID')
        answers=result['answers']; usage=result['usage']
        require(isinstance(answers,dict) and set(answers)=={q.question_id for q in questions},'JEV_SCHEMA_INVALID')
        require(isinstance(usage,dict) and set(usage)=={'input_tokens','output_tokens'},'JEV_USAGE_INVALID')
        for count in usage.values(): integer(count,0,2**40,'JEV_USAGE_INVALID')
        cleaned={}
        for q in questions:
            a=answers[q.question_id]; decision=q.decision
            require(isinstance(a,dict) and a.get('type')==decision.wire()['type'],'JEV_SCHEMA_INVALID')
            if isinstance(decision,Noul):
                require(set(a)=={'type','noul'},'JEV_SCHEMA_INVALID')
                cleaned[q.question_id]={'type':'noul','noul':number(a['noul'],0,1,'JEV_SCHEMA_INVALID')}
            elif isinstance(decision,Choice):
                require(set(a)=={'type','choice','probabilities','confidence'},'JEV_SCHEMA_INVALID')
                p=cls._probabilities(a['probabilities'],dict(decision.options))
                require(isinstance(a['choice'],str) and a['choice'] in p,'JEV_SCHEMA_INVALID')
                require(p[a['choice']]+1e-5>=max(p.values()),'JEV_CHOICE_INCONSISTENT')
                cleaned[q.question_id]={'type':'choice','choice':a['choice'],'probabilities':p,
                                       'confidence':number(a['confidence'],0,1,'JEV_SCHEMA_INVALID')}
            else:
                require(set(a)=={'type','score','legend','probabilities','confidence'},'JEV_SCHEMA_INVALID')
                legend={str(i):v for i,v in enumerate(decision.levels)}
                require(a['legend']==legend,'JEV_LEGEND_MISMATCH')
                p=cls._probabilities(a['probabilities'],legend)
                score=number(a['score'],0,len(legend)-1,'JEV_SCHEMA_INVALID')
                require(abs(score-sum(int(k)*v for k,v in p.items()))<=1e-5,'JEV_SCORE_INCONSISTENT')
                cleaned[q.question_id]={'type':'score','score':score,'legend':legend,'probabilities':p,
                                       'confidence':number(a['confidence'],0,1,'JEV_SCHEMA_INVALID')}
        return {'model':result['model'],'answers':cleaned,'usage':dict(usage)}

    def shadow(self, binding: Binding, policy: PolicySnapshot, *, state,
               approved_state_hash: str, questions: tuple[Question,...], price: Price | None=None) -> dict:
        """Explicit sampled call. approved_state_hash is supplied by a HOST data gate.

        Hash equality binds consent to bytes; it does not independently detect
        secrets/PII. The caller must use its existing external-data approval policy.
        No response or exception text from the service is logged.
        """
        require(isinstance(binding,Binding) and isinstance(policy,PolicySnapshot),'JEV_CONTEXT_REQUIRED')
        result={'schema':'h11.jev.shadow/1','binding_key':binding.key,'mode':self.config.mode.value,
                'policy_hash':policy.policy_hash,'actual':{'route':policy.actual_route,
                  'needs_supervisor':policy.actual_needs_supervisor,'risk':policy.risk.value,
                  'required_controls':sorted(policy.required_controls)},
                'decision':None,'status':'NOT_CALLED','error_code':'','elapsed_seconds':0.0,
                'usage':None,'cost_usd':None,'cost_kind':'UNKNOWN','credit_delta':None,
                'product_pass':False,'publication_authority':False,'applied':False}
        started=self.clock()
        try:
            if self.config.mode==Mode.OFF: result['status']='OFF'; return result
            if not self.config.external_calls_authorized: result['status']='SHADOW_NOT_AUTHORIZED'; return result
            require(type(questions) is tuple and 1<=len(questions)<=16 and all(isinstance(q,Question) for q in questions),'JEV_QUESTIONS_INVALID')
            require(len({q.question_id for q in questions})==len(questions),'JEV_QUESTION_DUPLICATE')
            require(isinstance(state,(str,dict,list)) and approved_state_hash==digest(state),'JEV_DATA_NOT_APPROVED')
            for q in questions:
                if q.use_case==UseCase.RUNTIME_ROUTE_SIGNAL:
                    require(set(dict(q.decision.options)).issubset(policy.approved_routes),'JEV_UNAPPROVED_ROUTE')
            body=canonical({'model':self.config.model,'state':state,'questions':{q.question_id:q.decision.wire() for q in questions}})
            require(len(body)<=self.config.request_limit,'JEV_REQUEST_TOO_LARGE')
            secret=(os.environ if self.env is None else self.env).get(self.config.api_key_env,'')
            require(isinstance(secret,str) and 0<len(secret)<=16384 and '\r' not in secret and '\n' not in secret,'JEV_API_KEY_MISSING_OR_INVALID')
            with self._lock:
                require(self._calls<self.config.max_calls,'JEV_CALL_BUDGET_EXHAUSTED')
                # Ambiguous failures consume budget: never retry a possibly billed call.
                self._calls+=1
            result['request_hash']=digest(strict_json(body)); result['request_bytes']=len(body)
            result['question_uses']={q.question_id:q.use_case.value for q in questions}
            transport=self.transport or IsolatedHttpsTransport()
            status,raw=transport(body,secret,timeout=self.config.timeout_seconds,limit=self.config.response_limit)
            require(self.clock()-started<=self.config.timeout_seconds,'JEV_TIMEOUT')
            integer(status,100,599,'JEV_HTTP_INVALID')
            require(status==200, 'JEV_HTTP_'+str(status))
            parsed=self.validate(raw,questions,self.config.response_limit)
            require(secret not in canonical(parsed).decode('utf-8'),'JEV_SECRET_ECHO_REJECTED')
            result['usage']=parsed['usage']; result['resolved_model']=parsed['model']
            result['response_hash']=digest(parsed)
            if price is not None:
                require(isinstance(price,Price) and parsed['model']==price.model,'JEV_COST_MODEL_MISMATCH')
                result['cost_usd']=(parsed['usage']['input_tokens']*price.input_per_million_usd+
                                    parsed['usage']['output_tokens']*price.output_per_million_usd)/1_000_000
                result['cost_kind']='OPERATOR_RATE_ESTIMATE_NOT_INVOICE'; result['price_source_ref']=price.source_ref
            result['decision']=parsed['answers']; result['status']='SHADOW_RECORDED'
        except BoundaryError as exc:
            result['status']='SAFE_FALLBACK'; result['error_code']=exc.code
        except TimeoutError:
            result['status']='SAFE_FALLBACK'; result['error_code']='JEV_TIMEOUT'
        except Exception:
            result['status']='SAFE_FALLBACK'; result['error_code']='JEV_TRANSPORT_OR_CONFIG_ERROR'
        finally:
            result['elapsed_seconds']=max(0.0,self.clock()-started)
        return result
