"""Offline/sampled evaluation. Classification evidence is not Product status.

Cases and observations are immutable inputs. Aggregate reads never write or
rewrite receipts. Null measurements are unknown, NOT zero-cost/zero-latency.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from .core import Binding, require, text, number, integer, hash64, digest

class Outcome(str,Enum):
    POSITIVE='POSITIVE'
    NEGATIVE='NEGATIVE'
    UNRESOLVED='UNRESOLVED'

DIMENSIONS=frozenset(('RUNTIME','REVIEWER_STACK','EXECUTION_MODE','SUPERVISOR_STRATEGY','JEV_SHADOW','CONTEXT_DISCOVERY'))

@dataclass(frozen=True)
class Case:
    case_id: str
    case_version: str
    dataset_hash: str
    expected: Outcome
    adjudication_ref: str
    def __post_init__(self):
        for item in (self.case_id,self.case_version,self.adjudication_ref):text(item,maximum=1000)
        hash64(self.dataset_hash);require(isinstance(self.expected,Outcome),'OUTCOME_INVALID')
    @property
    def key(self):return digest(asdict(self))

@dataclass(frozen=True)
class Observation:
    observation_id: str
    case_key: str
    binding: Binding
    dimension: str
    strategy: str
    prediction: Outcome
    candidate_output_hash: str
    source_receipt_ref: str
    runtime: str
    runtime_version: str
    model: str
    elapsed_seconds: float | None=None
    cost_usd: float | None=None
    human_interventions: int | None=None
    accepted: bool | None=None
    rework_count: int | None=None
    provenance: str='FIXTURE'
    def __post_init__(self):
        for item in (self.observation_id,self.strategy,self.source_receipt_ref,self.runtime,self.runtime_version,self.model):text(item,maximum=1000)
        hash64(self.case_key);hash64(self.candidate_output_hash)
        require(isinstance(self.binding,Binding) and isinstance(self.prediction,Outcome) and self.dimension in DIMENSIONS,'OBSERVATION_INVALID')
        for x in (self.elapsed_seconds,self.cost_usd):
            if x is not None:number(x)
        for x in (self.human_interventions,self.rework_count):
            if x is not None:integer(x)
        require(self.accepted is None or type(self.accepted) is bool,'ACCEPTANCE_INVALID')
        require(self.provenance in ('FIXTURE','HOST_OBSERVED','OFFLINE_ADJUDICATED'),'PROVENANCE_INVALID')

def classification(case: Case,observation: Observation) -> str:
    require(observation.case_key==case.key,'EVAL_CASE_MISMATCH')
    if Outcome.UNRESOLVED in (case.expected,observation.prediction):return 'UNRESOLVED'
    return {('POSITIVE','POSITIVE'):'TP',('NEGATIVE','POSITIVE'):'FP',
            ('POSITIVE','NEGATIVE'):'FN',('NEGATIVE','NEGATIVE'):'TN'}[(case.expected.value,observation.prediction.value)]

def aggregate(cases: tuple[Case,...],observations: tuple[Observation,...]) -> dict:
    require(type(cases) is tuple and type(observations) is tuple and len(cases)<=100000 and len(observations)<=100000,'COHORT_INVALID')
    case_map={}; rows={}; case_identities=set()
    for case in cases:
        require(isinstance(case,Case),'EVAL_CASE_INVALID')
        identity=(case.case_id,case.case_version,case.dataset_hash)
        require(case.key not in case_map and identity not in case_identities,'EVAL_DUPLICATE_CASE')
        case_identities.add(identity);case_map[case.key]=case
    for row in observations:
        require(isinstance(row,Observation) and row.case_key in case_map,'EVAL_CASE_UNKNOWN')
        require(row.observation_id not in rows,'EVAL_DUPLICATE_OBSERVATION')
        rows[row.observation_id]=row
    # A cohort must not blend real and fixture results or silently average repeated
    # observations of the same case/strategy. Repeated trials require own strategy IDs.
    groups={}
    seen=set()
    for row in rows.values():
        key=(row.dimension,row.strategy,row.provenance)
        require((*key,row.case_key) not in seen,'EVAL_DUPLICATE_TRIAL');seen.add((*key,row.case_key))
        g=groups.setdefault(key,{'dimension':key[0],'strategy':key[1],'provenance':key[2],
             'TP':0,'FP':0,'FN':0,'TN':0,'UNRESOLVED':0,'count':0,'accepted_count':0,'acceptance_observed_count':0,
             'elapsed_sum':0.0,'elapsed_observed_count':0,'cost_sum_usd':0.0,'cost_observed_count':0,
             'human_interventions_sum':0,'human_observed_count':0,'rework_sum':0,'rework_observed_count':0})
        g[classification(case_map[row.case_key],row)]+=1;g['count']+=1
        if row.accepted is not None:g['acceptance_observed_count']+=1;g['accepted_count']+=int(row.accepted)
        for field,total,count in (('elapsed_seconds','elapsed_sum','elapsed_observed_count'),('cost_usd','cost_sum_usd','cost_observed_count'),
                                  ('human_interventions','human_interventions_sum','human_observed_count'),('rework_count','rework_sum','rework_observed_count')):
            value=getattr(row,field)
            if value is not None:g[total]+=value;g[count]+=1
    result=[]
    for key,g in sorted(groups.items()):
        g['precision']=g['TP']/(g['TP']+g['FP']) if g['TP']+g['FP'] else None
        g['recall']=g['TP']/(g['TP']+g['FN']) if g['TP']+g['FN'] else None
        g['accepted_completion_rate']=g['accepted_count']/g['count'] if g['acceptance_observed_count']==g['count'] else None
        g['cost_per_accepted_job']=(g['cost_sum_usd']/g['accepted_count'] if g['accepted_count'] and
            g['cost_observed_count']==g['count'] and g['acceptance_observed_count']==g['count'] else None)
        result.append(g)
    return {'schema':'h11.evaluation/1','groups':result,'input_fingerprint':digest({'cases':[asdict(c) for c in cases],
            'observations':[asdict(o) for o in observations]}),'historical_receipts_modified':False,'product_pass':False}

def paired_comparison(cases,observations, *, dimension: str,left: str,right: str,provenance: str) -> dict:
    """Report matched cohort only; coverage differences remain visible."""
    aggregate(cases,observations)  # Validate IDs, provenance and duplicate trials.
    l={o.case_key:o for o in observations if (o.dimension,o.strategy,o.provenance)==(dimension,left,provenance)}
    r={o.case_key:o for o in observations if (o.dimension,o.strategy,o.provenance)==(dimension,right,provenance)}
    matches=sorted(l.keys() & r.keys()); timings=[];costs=[]
    for key in matches:
        if l[key].elapsed_seconds is not None and r[key].elapsed_seconds is not None:timings.append(r[key].elapsed_seconds-l[key].elapsed_seconds)
        if l[key].cost_usd is not None and r[key].cost_usd is not None:costs.append(r[key].cost_usd-l[key].cost_usd)
    return {'matched_cases':len(matches),'left_only':len(l.keys()-r.keys()),'right_only':len(r.keys()-l.keys()),
            'elapsed_right_minus_left_mean':sum(timings)/len(timings) if timings else None,'paired_elapsed_count':len(timings),
            'cost_right_minus_left_mean':sum(costs)/len(costs) if costs else None,'paired_cost_count':len(costs),'automatic_promotion':False}
