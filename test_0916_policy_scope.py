import copy
import hashlib
from test_0916_candidateless_policy import PolicyResolutionTests
from control_repository import ControlRepository
from job_queue import JobStore


class PolicyScopeTests(PolicyResolutionTests):
    def scope_fixture(self, materialized=True):
        self.policy_fixture()
        self.successor = self.resolve()
        queue = self.store._read_queue()
        jobs = {j:self.store.get_job(j) for j in queue['order']}
        repository = ControlRepository(self.control/'scope.sqlite3',create=True)
        repository.bootstrap(queue,jobs,{j:{'path':j+'.json'} for j in jobs})
        self.store = JobStore(self.control,repository=repository)
        self.repository = repository
        if materialized:
            baseline = self.control/'cumulative'/'batch-baselines'/queue['active_baseline_id']/'manifest.json'
            self.execution = repository.materialize(self.successor,
                context={'profile_snapshot_sha256':'p','policy_snapshot_sha256':'p'},
                policy={'effective_policy_sha256':'p'},baseline_hash=hashlib.sha256(baseline.read_bytes()).hexdigest(),
                execution_surface_hash='s',extra={'current_requirement':self.successor['current_requirement']})

    def conflict(self, target=None):
        return self.store._quarantined_scope_conflict(self.repository.read_queue(),target or self.successor,
            workspace=self.workspace,integrity_snapshot_provider=lambda q:self.integrity)

    def test_resolved_materialized_scope(self):
        self.scope_fixture()
        self.assertEqual('',self.conflict())
        original = self.integrity['external_frozen_integrity']
        self.integrity['external_frozen_integrity']=False
        self.assertEqual(self.parent['job_id'],self.conflict())
        self.integrity['external_frozen_integrity']=original
        self.successor['current_requirement']+='tampered'
        self.assertEqual(self.parent['job_id'],self.conflict())

    def test_unresolved_scope(self):
        self.scope_fixture(materialized=False)
        self.assertEqual(self.parent['job_id'],self.conflict())
        self.successor['qa_policy_resolution']={}
        self.assertEqual(self.parent['job_id'],self.conflict())

    def test_active_candidate_scope(self):
        self.scope_fixture()
        source=self.repository.read_job(self.parent['job_id'])
        previous=source['revision']
        source.update(candidate_id='REAL-CANDIDATE',revision=previous+1)
        self.repository.write_job(source,expected_revision=previous)
        self.assertEqual(self.parent['job_id'],self.conflict())

    def test_foreign_scope(self):
        self.scope_fixture()
        foreign=copy.deepcopy(self.successor)
        foreign['job_id']='FOREIGN'
        self.assertEqual(self.parent['job_id'],self.conflict(foreign))

    def test_historical_immutable(self):
        self.scope_fixture()
        before=self.repository.read_job(self.parent['job_id'])
        self.assertEqual('',self.conflict())
        self.assertEqual(before,self.repository.read_job(self.parent['job_id']))
        self.assertEqual([],self.repository.preserved_candidate_records(self.parent['job_id']))
