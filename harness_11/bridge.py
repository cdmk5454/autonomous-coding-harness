"""Existing ControlRepository materialization boundary, not a second scheduler.

The frozen extra is passed through existing repo.materialize(extra=...), and its
hash is verified against the immutable execution payload on every admission.
This module intentionally does not monkeypatch the 1.0 dispatcher/registry.
The existing dispatcher owns schema7 admissions and runtime factories.
"""
from __future__ import annotations
from dataclasses import asdict
import json
from .core import (Binding, Scope, Resource, FeatureConfig, BoundaryError,
                   require, text, hash64, digest)

def freeze_extension(*,scope: Scope,candidate_id: str,epoch: str,diff_hash: str) -> dict:
    require(isinstance(scope,Scope),'KERNEL_SCOPE_REQUIRED')
    text(candidate_id);text(epoch);hash64(diff_hash)
    return {'h11_contract':{'schema':'h11.frozen-contract/1','scope':asdict(scope),
                            'candidate_id':candidate_id,'epoch':epoch,'diff_hash':diff_hash}}

def scope_from_record(record: dict) -> Scope:
    require(type(record) is dict and set(record)=={'candidate_root','canonical_root','write_paths','resources'},'KERNEL_SCOPE_INVALID')
    return Scope(record['candidate_root'],record['canonical_root'],tuple(record['write_paths']),
                 tuple(Resource(**r) for r in record['resources']))

def kernel_binding(repository,execution_id: str,attempt_id: str) -> tuple[Binding,Scope]:
    """A real baseline transaction/read, not an externally supplied Job ID map."""
    text(execution_id);text(attempt_id)
    with repository.hold():
        c=repository._connection
        row=c.execute('SELECT payload,phase FROM executions WHERE execution_id=?',(execution_id,)).fetchone()
        attempt=c.execute('SELECT execution_id,role,candidate_hash FROM attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
        outcome=c.execute('SELECT status FROM attempt_outcomes WHERE attempt_id=?',(attempt_id,)).fetchone()
        require(row is not None and row['phase']=='RUNNING' and attempt is not None
                and attempt['execution_id']==execution_id and outcome is not None and outcome['status']=='RUNNING',
                'KERNEL_ACTIVE_ATTEMPT_REQUIRED')
        execution=json.loads(row['payload']);extra=execution.get('h11_contract')
        require(type(extra) is dict and set(extra)=={'schema','scope','candidate_id','epoch','diff_hash'}
                and extra['schema']=='h11.frozen-contract/1','KERNEL_FROZEN_11_CONTRACT_REQUIRED')
        scope=scope_from_record(extra['scope'])
        b=Binding(execution['job_id'],execution_id,attempt_id,extra['candidate_id'],digest(execution),
                  attempt['candidate_hash'],extra['diff_hash'],extra['epoch'],attempt['role'])
        return b,scope

def assert_kernel_binding(repository,binding: Binding,scope: Scope) -> None:
    actual,actual_scope=kernel_binding(repository,binding.execution_id,binding.attempt_id)
    require(actual==binding and actual_scope.fingerprint==scope.fingerprint,'KERNEL_FROZEN_BINDING_MISMATCH')

def integration_readiness(config: FeatureConfig=FeatureConfig()) -> dict:
    """Source status only; final review, Product canary and Git promotion remain gates."""
    return {'default_config':config.public(),'baseline_behavior_changed':False,
            'source_integration_complete':True,'ready_for_final_pro_review':True,
            'can_promote_nworker':False,'can_promote_pi':False,
            'non_local_blockers':[],
            'promotion_gates':['INDEPENDENT_PRO_FINAL_REVIEW','FRESH_PRODUCT_S1_A_B',
                               'RELEASE_GIT_COMMIT_TAG'],
            'product_pass':False,'publication_authority':False}
