"""N-worker admission on the *existing* ControlRepository transaction.

This is not a dispatcher: callers are the Harness, never external models. Slots
are retained on stale/cancel/crash until a trusted boundary observer proves both
process-tree quiescence and revocation. Expiry alone never frees a writer.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Callable
from pathlib import Path
import json
import time
from .core import (Binding, ProcessIdentity, FenceProof, Resource, Scope,
                   BoundaryError, require, integer, number, canonical, digest, text)

WRITER_STATES = ('RUNNING', 'CANCELLING', 'STALE', 'QUARANTINED')
LIVE_STATES = (*WRITER_STATES, 'REVIEW', 'TEST', 'PUBLICATION')

@dataclass(frozen=True)
class CapacityPolicy:
    workers: int = 2
    wip: int = 4
    review_backlog: int = 4
    test_backlog: int = 4
    publication_backlog: int = 4
    lease_seconds: float = 60.0
    def __post_init__(self):
        integer(self.workers, 1, 64); integer(self.wip, 1, 1024)
        for v in (self.review_backlog, self.test_backlog, self.publication_backlog): integer(v,1,1024)
        number(self.lease_seconds, 0.001, 86400)

DDL = (
    'CREATE TABLE h11_capacity (id INTEGER PRIMARY KEY CHECK(id=1), policy TEXT NOT NULL CHECK(json_valid(policy)))',
    '''CREATE TABLE h11_admissions (
       token INTEGER PRIMARY KEY AUTOINCREMENT, binding_key TEXT UNIQUE NOT NULL,
       job_id TEXT NOT NULL REFERENCES logical_jobs(job_id), execution_id TEXT NOT NULL REFERENCES executions(execution_id),
       attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id), binding TEXT NOT NULL CHECK(json_valid(binding)),
       process_key TEXT NOT NULL, process TEXT NOT NULL CHECK(json_valid(process)), scope TEXT NOT NULL CHECK(json_valid(scope)),
       state TEXT NOT NULL CHECK(state IN ('RUNNING','CANCELLING','STALE','QUARANTINED','REVIEW','TEST','PUBLICATION','TERMINAL')),
       lease_until REAL NOT NULL, last_seen REAL NOT NULL, observer_epoch TEXT NOT NULL, fence_ref TEXT NOT NULL DEFAULT '',
       outcome_ref TEXT NOT NULL DEFAULT '')''',
    "CREATE UNIQUE INDEX h11_one_live_job ON h11_admissions(job_id) WHERE state!='TERMINAL'",
    "CREATE UNIQUE INDEX h11_one_live_process ON h11_admissions(process_key) WHERE state IN ('RUNNING','CANCELLING','STALE','QUARANTINED')",
    'CREATE TABLE h11_publication (id INTEGER PRIMARY KEY CHECK(id=1), token INTEGER NOT NULL UNIQUE REFERENCES h11_admissions(token), owner TEXT NOT NULL)',
)

class NWorkerAdmission:
    def __init__(self, repository, *, observer_epoch: str, verify_fence: Callable[[FenceProof], bool],
                 verify_outcome: Callable[[Binding, str], bool], clock=time.time):
        text(observer_epoch)
        require(callable(verify_fence) and callable(verify_outcome), 'HOST_OBSERVERS_REQUIRED')
        self.repo, self.observer_epoch = repository, observer_epoch
        self.verify_fence, self.verify_outcome, self.clock = verify_fence, verify_outcome, clock
        # No DDL, route selection, heartbeat or recovery at construction.

    def install_disposable(self, *, allocation_root: Path, policy: CapacityPolicy) -> None:
        """Explicit side-table installation only in a caller-owned disposable tree.

        No user_version change. Baseline execution_slots/schema5/6 constraints
        are deliberately not bypassed; bridge.py documents the dispatch seam.
        """
        root=Path(allocation_root).resolve(); db=Path(self.repo.path).resolve()
        require(db.is_relative_to(root) and db != root and root != Path(root.anchor), 'DISPOSABLE_DB_REQUIRED')
        require(isinstance(policy, CapacityPolicy), 'CAPACITY_POLICY_REQUIRED')
        with self.repo.hold():
            c=self.repo._connection
            require(not c.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'h11_%' LIMIT 1").fetchone(), 'EXTENSION_ALREADY_INSTALLED')
            require(not c.execute("SELECT 1 FROM executions WHERE phase='RUNNING' UNION ALL SELECT 1 FROM logical_jobs WHERE status='RUNNING' LIMIT 1").fetchone(), 'DRAIN_REQUIRED')
            for sql in DDL: c.execute(sql)
            c.execute('INSERT INTO h11_capacity VALUES(1,?)', (canonical(asdict(policy)).decode(),))

    def _policy(self):
        c=self.repo._connection
        require(c.execute("SELECT 1 FROM sqlite_master WHERE name='h11_capacity'").fetchone() is not None, 'NWORKER_NOT_INSTALLED')
        return CapacityPolicy(**json.loads(c.execute('SELECT policy FROM h11_capacity WHERE id=1').fetchone()[0]))

    def resize(self, policy: CapacityPolicy) -> None:
        require(isinstance(policy, CapacityPolicy), 'CAPACITY_POLICY_REQUIRED')
        with self.repo.hold():
            self._policy()
            # Shrinking is an admission limit, never cancellation of admitted work.
            self.repo._connection.execute('UPDATE h11_capacity SET policy=? WHERE id=1', (canonical(asdict(policy)).decode(),))

    def _rows(self):
        return [dict(r) for r in self.repo._connection.execute("SELECT * FROM h11_admissions WHERE state!='TERMINAL' ORDER BY token")]

    def _row(self, token):
        integer(token,1)
        row=self.repo._connection.execute('SELECT * FROM h11_admissions WHERE token=?',(token,)).fetchone()
        require(row is not None, 'ADMISSION_NOT_FOUND')
        return dict(row)

    def snapshot(self):
        with self.repo.hold():
            policy=self._policy(); rows=self._rows()
            return {'policy':asdict(policy),'wip':len(rows),'writers':sum(r['state'] in WRITER_STATES for r in rows),
                    'rows':[{k:r[k] for k in ('token','binding_key','job_id','state','lease_until','observer_epoch')} for r in rows]}

    def _assert_kernel_binding(self, binding, scope):
        from .bridge import assert_kernel_binding
        assert_kernel_binding(self.repo,binding,scope)
        require(binding.role=='WORKER', 'ONLY_WORKER_ADMISSION')

    def admit(self, binding: Binding, process: ProcessIdentity, scope: Scope, *,
              dependencies: tuple[str,...]=(), kernel_backlogs: tuple[int,int,int]=(0,0,0)) -> dict:
        require(isinstance(binding,Binding) and isinstance(process,ProcessIdentity) and isinstance(scope,Scope), 'ADMISSION_CONTRACT_INVALID')
        require(type(dependencies) is tuple and type(kernel_backlogs) is tuple and len(kernel_backlogs)==3, 'ADMISSION_CONTRACT_INVALID')
        for item in dependencies: text(item)
        for item in kernel_backlogs: integer(item)
        now=number(self.clock())
        with self.repo.hold():
            policy=self._policy(); self._assert_kernel_binding(binding,scope)
            c=self.repo._connection
            prior=c.execute('SELECT * FROM h11_admissions WHERE binding_key=?',(binding.key,)).fetchone()
            if prior:
                require(prior['process_key']==process.key and prior['scope']==canonical(asdict(scope)).decode(), 'ADMISSION_REPLAY_MISMATCH')
                require(prior['state']=='RUNNING' and prior['observer_epoch']==self.observer_epoch
                        and now<prior['lease_until'] and now>=prior['last_seen'], 'ADMISSION_REPLAY_NOT_ACTIVE')
                return {'admitted':True,'token':prior['token'],'replayed':True,'product_pass':False}
            rows=self._rows()
            if any(r['observer_epoch']!=self.observer_epoch or
                   (r['state'] in WRITER_STATES and (r['lease_until']<=now or now<r['last_seen'])) for r in rows):
                return {'admitted':False,'reason':'RECONCILIATION_REQUIRED'}
            if len(rows)>=policy.wip or sum(r['state'] in WRITER_STATES for r in rows)>=policy.workers:
                return {'admitted':False,'reason':'CAPACITY_EXHAUSTED'}
            for state, supplied, limit in zip(('REVIEW','TEST','PUBLICATION'),kernel_backlogs,
                                              (policy.review_backlog,policy.test_backlog,policy.publication_backlog)):
                if max(sum(r['state']==state for r in rows),supplied)>=limit:
                    return {'admitted':False,'reason':'DOWNSTREAM_BACKPRESSURE'}
            kernel_job=self.repo.read_job(binding.job_id)
            frozen_deps=kernel_job.get('depends_on',())
            require(isinstance(frozen_deps,(list,tuple)) and all(isinstance(d,str) for d in frozen_deps),'KERNEL_DEPENDENCY_INVALID')
            for dep in sorted(set(dependencies) | set(frozen_deps)):
                result=c.execute('SELECT status FROM logical_jobs WHERE job_id=?',(dep,)).fetchone()
                if result is None or result[0]!='SUCCEEDED':
                    return {'admitted':False,'reason':'DEPENDENCY_NOT_SUCCESSFUL'}
            for row in rows:
                other=json.loads(row['scope'])
                root=Path(scope.candidate_root); other_root=Path(other['candidate_root'])
                a=str(root).replace('\\','/').casefold().rstrip('/');b=str(other_root).replace('\\','/').casefold().rstrip('/')
                same_candidate=(a==b or a.startswith(b+'/') or b.startswith(a+'/')
                                or json.loads(row['binding'])['candidate_id']==binding.candidate_id)
                if row['job_id']==binding.job_id or row['process_key']==process.key or same_candidate:
                    return {'admitted':False,'reason':'OWNERSHIP_CONFLICT'}
                if any(a.conflicts(Resource(**b)) for a in scope.resources for b in other['resources']):
                    return {'admitted':False,'reason':'SHARED_RESOURCE_CONFLICT'}
            token=c.execute('''INSERT INTO h11_admissions(binding_key,job_id,execution_id,attempt_id,binding,
                    process_key,process,scope,state,lease_until,last_seen,observer_epoch)
                    VALUES(?,?,?,?,?,?,?,?,'RUNNING',?,?,?)''',
                    (binding.key,binding.job_id,binding.execution_id,binding.attempt_id,canonical(asdict(binding)).decode(),
                     process.key,canonical(asdict(process)).decode(),canonical(asdict(scope)).decode(),
                     now+policy.lease_seconds,now,self.observer_epoch)).lastrowid
            return {'admitted':True,'token':token,'replayed':False,'product_pass':False}

    def heartbeat(self, token: int, binding: Binding, process: ProcessIdentity) -> None:
        now=number(self.clock())
        with self.repo.hold():
            policy=self._policy(); row=self._row(token)
            require(row['binding_key']==binding.key and row['process_key']==process.key, 'FOREIGN_HEARTBEAT')
            require(row['state']=='RUNNING' and row['observer_epoch']==self.observer_epoch
                    and row['last_seen']<=now<row['lease_until'], 'LEASE_NOT_RENEWABLE')
            self.repo._connection.execute('UPDATE h11_admissions SET lease_until=?,last_seen=? WHERE token=?',
                                          (now+policy.lease_seconds,now,token))

    def request_cancel(self, token: int) -> dict:
        with self.repo.hold():
            row=self._row(token)
            require(row['state'] in WRITER_STATES, 'CANCEL_STATE_INVALID')
            self.repo._connection.execute("UPDATE h11_admissions SET state='CANCELLING' WHERE token=?",(token,))
            return {'cancel_requested':True,'slot_released':False,'process_fenced':False}

    def reconcile(self) -> dict:
        now=number(self.clock()); held=[]
        with self.repo.hold():
            self._policy()
            for row in self._rows():
                # Restart never adopts a process using a PID alone, including post-work WIP.
                if row['observer_epoch']!=self.observer_epoch:
                    state='QUARANTINED'
                elif row['state'] in WRITER_STATES and (row['lease_until']<=now or now<row['last_seen']):
                    state='STALE'
                else: continue
                self.repo._connection.execute('UPDATE h11_admissions SET state=? WHERE token=?',(state,row['token']))
                held.append(row['token'])
        return {'held_tokens':held,'released':0,'requires_host_fencing':True}

    def quiesce(self, token: int, proof: FenceProof, *, next_phase: str='REVIEW') -> None:
        require(next_phase in ('REVIEW','TEST','PUBLICATION'), 'PHASE_INVALID')
        with self.repo.hold():
            row=self._row(token); b=Binding(**json.loads(row['binding'])); p=ProcessIdentity(**json.loads(row['process']))
            require(row['state'] in WRITER_STATES and isinstance(proof,FenceProof) and proof.matches(b,p,token), 'FENCE_BINDING_MISMATCH')
            require(self.verify_fence(proof) is True, 'FENCE_NOT_INDEPENDENTLY_OBSERVED')
            self.repo._connection.execute('UPDATE h11_admissions SET state=?,fence_ref=?,observer_epoch=? WHERE token=?',
                                          (next_phase,proof.receipt_ref,self.observer_epoch,token))

    def stage(self, token: int, phase: str) -> None:
        require(phase in ('REVIEW','TEST','PUBLICATION'), 'PHASE_INVALID')
        with self.repo.hold():
            row=self._row(token)
            require(row['state'] in ('REVIEW','TEST','PUBLICATION') and row['fence_ref']
                    and row['observer_epoch']==self.observer_epoch, 'WRITER_NOT_QUIESCED')
            require(not self.repo._connection.execute('SELECT 1 FROM h11_publication WHERE token=?',(token,)).fetchone(), 'PUBLICATION_IN_FLIGHT')
            self.repo._connection.execute('UPDATE h11_admissions SET state=? WHERE token=?',(phase,token))

    def begin_publication(self, token: int, *, owner: str) -> dict:
        """Serialization ONLY. The existing publisher must still evaluate its gates."""
        text(owner)
        with self.repo.hold():
            row=self._row(token)
            require(row['state']=='PUBLICATION' and row['fence_ref'] and row['observer_epoch']==self.observer_epoch, 'PUBLICATION_NOT_READY')
            prior=self.repo._connection.execute('SELECT token,owner FROM h11_publication').fetchone()
            if prior:
                # No automatic re-entry: a timed-out publisher may still be writing.
                return {'acquired':False,'reason':'PUBLICATION_BUSY','product_pass':False}
            self.repo._connection.execute('INSERT INTO h11_publication VALUES(1,?,?)',(token,owner))
            return {'acquired':True,'token':token,'owner':owner,'product_pass':False,'gate_authority':'EXISTING_HARNESS_PUBLISHER'}

    def finish(self, token: int, *, outcome_ref: str, publication_owner: str='') -> None:
        text(outcome_ref, maximum=1000)
        with self.repo.hold():
            row=self._row(token)
            require(row['state'] in ('REVIEW','TEST','PUBLICATION') and row['fence_ref']
                    and row['observer_epoch']==self.observer_epoch, 'UNQUIESCED_OR_STALE_COMPLETION')
            b=Binding(**json.loads(row['binding']))
            require(self.verify_outcome(b,outcome_ref) is True, 'KERNEL_OUTCOME_REQUIRED')
            lock=self.repo._connection.execute('SELECT token,owner FROM h11_publication WHERE token=?',(token,)).fetchone()
            if lock:
                require(lock['owner']==publication_owner, 'PUBLICATION_OWNER_MISMATCH')
                self.repo._connection.execute('DELETE FROM h11_publication WHERE token=?',(token,))
            self.repo._connection.execute("UPDATE h11_admissions SET state='TERMINAL',outcome_ref=? WHERE token=?",(outcome_ref,token))

    def recover_publication(self, token: int, *, owner: str, publisher_quiescence_ref: str,
                            verify_publisher_quiescence: Callable[[int,str,str],bool]) -> None:
        """Explicit repair after publisher crash; never reuse the worker's fence proof."""
        text(owner); text(publisher_quiescence_ref,maximum=1000)
        with self.repo.hold():
            lock=self.repo._connection.execute('SELECT token,owner FROM h11_publication WHERE token=?',(token,)).fetchone()
            require(lock is not None and lock['owner']==owner, 'PUBLICATION_OWNER_MISMATCH')
            require(verify_publisher_quiescence(token,owner,publisher_quiescence_ref) is True, 'PUBLISHER_QUIESCENCE_REQUIRED')
            self.repo._connection.execute('DELETE FROM h11_publication WHERE token=?',(token,))
