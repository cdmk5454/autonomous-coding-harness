"""Bounded structured retrieval; no vector store or independent truth authority."""
from __future__ import annotations
from dataclasses import dataclass
import re
from .core import Mode, hash64, text, require, integer

@dataclass(frozen=True)
class Record:
    record_id: str
    kind: str
    project: str
    profile_hash: str
    source_revision: str
    summary: str
    evidence_ref: str
    tags: tuple[str,...]
    def __post_init__(self):
        for v in (self.record_id,self.project,self.source_revision,self.evidence_ref):text(v,maximum=1000)
        text(self.summary,maximum=4000);hash64(self.profile_hash)
        require(self.kind in ('DEFECT','RESOLUTION','OWNERSHIP','VERIFICATION'),'RETRIEVAL_KIND_INVALID')
        require(type(self.tags) is tuple and len(self.tags)<=32,'RETRIEVAL_TAGS_INVALID')
        for tag in self.tags:text(tag,maximum=100)

def retrieve(query: str, records: tuple[Record,...], *, project: str, profile_hash: str,
             source_revision: str, limit=5, mode: Mode=Mode.OFF, verify_evidence=None) -> dict:
    integer(limit,0,20)
    result={'status':'OFF','items':[],'current_source_authority':True,'contract_override':False,'product_pass':False}
    if mode==Mode.OFF:return result
    require(mode==Mode.SHADOW,'RETRIEVAL_MODE_INVALID')
    require(type(records) is tuple and len(records)<=1000,'RETRIEVAL_INPUT_TOO_LARGE')
    text(query,maximum=2000);text(project);hash64(profile_hash);text(source_revision)
    tokens=set(re.findall(r'[\w.-]{2,}',query.casefold()))
    ranked=[];seen=set()
    try:
        for row in records:
            require(isinstance(row,Record) and row.record_id not in seen,'RETRIEVAL_DUPLICATE_OR_INVALID')
            seen.add(row.record_id)
            if (row.project,row.profile_hash)!=(project,profile_hash):continue
            # History from a different revision may be useful as a hint, never
            # current implementation evidence. Unverified receipts are excluded.
            if verify_evidence is None or verify_evidence(row) is not True:continue
            overlap=len(tokens & set(re.findall(r'[\w.-]{2,}',(row.summary+' '+' '.join(row.tags)).casefold())))
            if overlap:ranked.append((-overlap,row.record_id,row))
        result['items']=[{'record_id':r.record_id,'summary':r.summary,'evidence_ref':r.evidence_ref,
                          'source_revision':r.source_revision,'current_revision':r.source_revision==source_revision,
                          'authority':'RETRIEVED_HINT_REVALIDATE_AGAINST_CURRENT_SOURCE'}
                         for _,_,r in sorted(ranked)[:limit]]
        result['status']='BOUNDED_HINTS'
    except Exception:
        result.update(status='SAFE_FALLBACK',items=[],error_code='RETRIEVAL_UNAVAILABLE_OR_INVALID')
    return result
