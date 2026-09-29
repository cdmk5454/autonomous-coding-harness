"""Alibaba OpenCodeReview SHADOW evidence and risk-gated security profile."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
import time
from .core import Binding, Scope, Mode, Risk, BoundaryError, require, text, integer, number, digest, canonical
from .context import ToolIdentity, relative_source

class Verdict(str,Enum):
    VALID='VALID'
    FALSE_POSITIVE='FALSE_POSITIVE'
    UNRESOLVED='UNRESOLVED'

@dataclass(frozen=True)
class Finding:
    finding_id: str
    rule_id: str
    path: str
    line: int
    severity: str
    summary: str
    def __post_init__(self):
        for v in (self.finding_id,self.rule_id,self.path):text(v,maximum=500)
        text(self.summary,maximum=4000);integer(self.line,1,10_000_000)
        require(self.severity in ('INFO','LOW','MEDIUM','HIGH','CRITICAL'),'FINDING_SEVERITY_INVALID')
    @property
    def key(self):return digest(asdict(self))

class ShadowReviewer:
    def __init__(self, *, mode: Mode=Mode.SHADOW,external_calls_authorized=False,
                 timeout_seconds=30,max_bytes=65536,clock=time.monotonic):
        require(mode in (Mode.OFF,Mode.SHADOW),'REVIEWER_PRODUCT_AUTHORITY_UNSUPPORTED')
        require(type(external_calls_authorized) is bool,'REVIEW_AUTHORIZATION_INVALID')
        number(timeout_seconds,.01,120);integer(max_bytes,256,262144)
        self.mode=mode;self.authorized=external_calls_authorized;self.timeout=timeout_seconds;self.limit=max_bytes;self.clock=clock
    def review(self,binding: Binding,scope: Scope, *, identity: ToolIdentity | None=None,
               invoke=None,verify_readonly=None) -> dict:
        result={'binding_key':binding.key,'contract_hash':binding.contract_hash,'candidate_hash':binding.candidate_hash,
                'diff_hash':binding.diff_hash,'status':'OFF','findings':[],'elapsed_seconds':0.0,'cost_usd':None,
                'product_pass':False,'publication_authority':False,'existing_review_mutated':False}
        if self.mode==Mode.OFF:return result
        if not self.authorized:result['status']='SHADOW_NOT_AUTHORIZED';return result
        started=self.clock()
        try:
            require(isinstance(identity,ToolIdentity) and identity.name=='alibaba_review' and callable(invoke),'SHADOW_REVIEWER_UNAVAILABLE')
            require(callable(verify_readonly) and verify_readonly(identity,binding,scope) is True,'REVIEWER_READONLY_ATTESTATION_REQUIRED')
            response=invoke({'binding':asdict(binding),'scope':asdict(scope),'authority':'SHADOW_ONLY'},timeout=self.timeout,max_bytes=self.limit)
            require(self.clock()-started<=self.timeout,'SHADOW_REVIEWER_TIMEOUT')
            require(type(response) is dict and set(response)=={'binding_key','findings','cost_usd'},'SHADOW_REVIEW_SCHEMA_INVALID')
            require(len(canonical(response))<=self.limit,'SHADOW_REVIEW_TOO_LARGE')
            require(response['binding_key']==binding.key,'SHADOW_REVIEW_STALE_BINDING')
            rows=response['findings'];require(type(rows) is list and len(rows)<=100,'SHADOW_REVIEW_SCHEMA_INVALID')
            findings=[];seen=set()
            for row in rows:
                require(type(row) is dict,'SHADOW_REVIEW_SCHEMA_INVALID')
                finding=Finding(**row)
                relative_source(scope,finding.path)
                require(finding.finding_id not in seen,'SHADOW_DUPLICATE_FINDING');seen.add(finding.finding_id)
                findings.append(asdict(finding))
            if response['cost_usd'] is not None:number(response['cost_usd'])
            result.update(status='SHADOW_EVIDENCE',findings=findings,cost_usd=response['cost_usd'],identity=asdict(identity))
        except BoundaryError as exc:result.update(status='SAFE_FALLBACK',error_code=exc.code,findings=[])
        except TimeoutError:result.update(status='SAFE_FALLBACK',error_code='SHADOW_REVIEWER_TIMEOUT',findings=[])
        except Exception:result.update(status='SAFE_FALLBACK',error_code='SHADOW_REVIEWER_FAILED',findings=[])
        finally:result['elapsed_seconds']=max(0,self.clock()-started)
        return result

def compare_findings(existing: tuple[Finding,...],shadow: tuple[Finding,...], *, adjudications: dict[str,Verdict],
                     equivalence_groups: dict[str,str] | None=None) -> dict:
    require(type(existing) is tuple and type(shadow) is tuple and len(existing)+len(shadow)<=2000,'FINDING_COHORT_INVALID')
    require(type(adjudications) is dict,'FINDING_ADJUDICATION_INVALID')
    equivalence_groups=equivalence_groups or {}
    all_keys={f.key for f in (*existing,*shadow)}
    require(set(adjudications).issubset(all_keys) and set(equivalence_groups).issubset(all_keys),'FINDING_ADJUDICATION_UNKNOWN')
    for verdict in adjudications.values():require(isinstance(verdict,Verdict),'FINDING_ADJUDICATION_INVALID')
    for group in equivalence_groups.values():text(group)
    def group(f):
        # Only explicit human/host-adjudicated equivalence merges different
        # findings. Similar prose/rule IDs alone are not proof of overlap.
        return equivalence_groups.get(f.key,f.key)
    l={group(f) for f in existing if adjudications.get(f.key)==Verdict.VALID}
    r={group(f) for f in shadow if adjudications.get(f.key)==Verdict.VALID}
    return {'unique_valid_shadow_findings':len(r-l),'valid_overlap':len(l & r),
            'shadow_false_positives':len({f.key for f in shadow if adjudications.get(f.key)==Verdict.FALSE_POSITIVE}),
            'shadow_unresolved':len({f.key for f in shadow if adjudications.get(f.key,Verdict.UNRESOLVED)==Verdict.UNRESOLVED}),
            'product_pass':False,'automatic_reviewer_replacement':False}

SECURITY_TAGS=frozenset(('AUTH','PERMISSION','PII','CREDENTIALS','SECURITY','CRYPTOGRAPHY','UNTRUSTED_INPUT'))

def security_activation(*,risk: Risk,tags: frozenset[str],explicit_audit: bool=False,
                        mode: Mode=Mode.OFF,installed_identity: ToolIdentity | None=None) -> dict:
    require(isinstance(risk,Risk) and type(tags) is frozenset and all(isinstance(x,str) for x in tags),'SECURITY_CONTRACT_INVALID')
    require(type(explicit_audit) is bool and mode in (Mode.OFF,Mode.SHADOW),'SECURITY_CONFIG_INVALID')
    relevant=bool(tags & SECURITY_TAGS)
    trigger=explicit_audit or (relevant and risk in (Risk.HIGH,Risk.CRITICAL))
    activate=mode==Mode.SHADOW and trigger and isinstance(installed_identity,ToolIdentity) and installed_identity.name=='security_audit'
    status=('OFF' if mode==Mode.OFF else 'NOT_APPLICABLE' if not trigger else
            'RISK_GATED_SHADOW' if activate else 'REQUIRED_PROFILE_UNAVAILABLE')
    return {'status':status,'would_trigger':trigger,'activate':activate,'risk':risk.value,
            'security_tags':sorted(tags & SECURITY_TAGS),'explicit_contract':explicit_audit,
            'identity':asdict(installed_identity) if activate else None,
            'existing_mandatory_controls_unchanged':True,'product_pass':False,'publication_authority':False}
