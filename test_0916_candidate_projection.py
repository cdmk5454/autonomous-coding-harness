import copy
import hashlib
from unittest.mock import Mock, patch

from harness_service import HarnessService
from test_0916_takeover import TechnicalTakeoverTests


class CandidateProjectionTests(TechnicalTakeoverTests):
    def success(self):
        self.chain()
        f = self.f
        f.repo.materialize(f.repo.read_job(f.other),
            context={'profile_snapshot_sha256':'p','policy_snapshot_sha256':'p'},
            policy={'effective_policy_sha256':'p'}, baseline_hash='b', execution_surface_hash='s')
        hashes = {}
        for name in f.hashes:
            (f.product/name).write_bytes(b'new authoritative implementation')
            hashes[name] = hashlib.sha256((f.product/name).read_bytes()).hexdigest()
        f.edit_job(f.other,status='SUCCEEDED',last_result={'success':True,
            'verification_status':'VERIFIED','task_id':'NEW-TASK','changed_file_sha256':hashes})

    def projection(self, dirty=()):
        manager = Mock()
        manager.public_status.return_value = {'unexpected_runtime_dirty_files':list(dirty)}
        # 0.99 sqlite-authority projection: the real cumulative manager returns
        # the adopted-overlay hash map; an empty dict means "no adopted delta".
        manager.adopted_external_hashes.return_value = {}
        with patch.object(self.f.harness,'_cumulative_manager',return_value=manager):
            return HarnessService.cumulative_policy_status(self.f.harness,read_only=True)

    def test_verified_successor_history_only(self):
        self.success()
        f = self.f
        old = copy.deepcopy(f.repo.read_job(f.target))
        owners = f.repo.preserved_candidate_records(f.target)
        result = self.projection()
        self.assertEqual([],result['unexpected_runtime_dirty_files'])
        self.assertTrue(result['candidate_preservation_integrity'])
        self.assertTrue(result['historical_candidate_artifact_integrity'])
        self.assertEqual(4,len(result['historical_candidates']))
        self.assertEqual([],result['preserved_unverified_candidates'])
        self.assertEqual(old,f.repo.read_job(f.target))
        self.assertEqual(owners,f.repo.preserved_candidate_records(f.target))

    def test_historical_artifact_corruption_blocked(self):
        self.success()
        folder = self.f.control_root/'cumulative/job-snapshots'/self.f.baseline/'INPUT'
        (folder/'0').write_bytes(b'corrupted evidence')
        result = self.projection()
        self.assertFalse(result['historical_candidate_artifact_integrity'])
        self.assertFalse(result['candidate_preservation_integrity'])

    def test_active_quarantine_protected(self):
        self.chain()
        (self.f.product/next(iter(self.f.hashes))).write_bytes(b'unexpected mutation')
        for state in ('QUEUED','FAILED_FINAL','AWAITING_QA'):
            self.f.edit_job(self.f.other,status=state)
            result = self.projection()
            self.assertFalse(result['candidate_preservation_integrity'])
            self.assertEqual(4,len(result['preserved_unverified_candidates']))
            self.assertTrue(result['unexpected_runtime_dirty_files'])

    def test_unrelated_real_dirty_preserved(self):
        self.success()
        result = self.projection(['unrelated/file.txt'])
        self.assertEqual(['unrelated/file.txt'],result['unexpected_runtime_dirty_files'])

    def test_successful_tree_tamper_blocked(self):
        self.success()
        name = next(iter(self.f.hashes))
        (self.f.product/name).write_bytes(b'not successful tree')
        self.assertIn(name,self.projection()['unexpected_runtime_dirty_files'])
