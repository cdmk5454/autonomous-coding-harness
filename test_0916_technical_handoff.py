import copy
from unittest.mock import Mock, patch
from test_0911_handoff import CandidateHandoffTests, FILES
from control_repository import RepositoryError


class TechnicalHandoffTests(CandidateHandoffTests):
    canonical_authority = True
    def technical_fixture(self):
        source = self.edit_job(self.source, status='FAILED_FINAL', last_result={
            'failure_origin':'CONTROL','failure_code':'PROFILE_DRIFT',
            'checkpoint_status':'VERIFIED_NO_TASK_DELTA','checkpoint_manifest':'',
            'task_id':'TASK-OLD','review_status':'SKIPPED'})
        fresh = {'kind':'VERIFIED_NO_TASK_DELTA_FRESH_INPUT',
                 'parent_job_id':self.source,'parent_revision':source['revision'],
                 'active_baseline_id':self.baseline}
        target = self.edit_job(self.target, replaces_job_id=self.source, fresh_input=fresh)
        q = self.repo.read_queue()
        revision = q['revision']
        q['replacement_anchors'] = {self.source:{'replacement_job_id':self.target}}
        q['revision'] = revision+1
        self.repo.write_queue(q,expected_revision=revision)
        self.args.update(expected_source_revision=source['revision'], expected_target_revision=target['revision'])
        return fresh

    def test_a_technical_input_preserves_owner(self):
        fresh = self.technical_fixture()
        before = copy.deepcopy(self.repo.read_job(self.source))
        rows = self.repo.preserved_candidate_records(self.source)
        files = self.bytes()
        with patch('worker.CodexWorker.execute') as worker:
            result = self.control.handoff_candidate_ownership(**self.args)
            worker.assert_not_called()
        self.assertEqual(before,self.repo.read_job(self.source))
        self.assertEqual(rows,self.repo.preserved_candidate_records(self.source))
        self.assertEqual(files,self.bytes())
        self.assertEqual('PRE_EXISTING_UNVERIFIED_CANDIDATE',result['event']['classification'])
        self.assertTrue(result['event']['target_execution_id'].startswith('EXEC-'))
        self.assertTrue(self.repo.technical_candidate_input_allows(self.source,self.target))
        manager = Mock()
        manager.validate_no_delta_fresh_input.return_value = fresh
        manager.prepare_job_snapshot.return_value = {'snapshot_id':'NEW-INPUT'}
        with patch.object(self.harness,'_cumulative_manager',return_value=manager), \
             patch.object(self.harness,'cumulative_policy_status',return_value=self.integrity):
            self.assertEqual({'snapshot_id':'NEW-INPUT'},self.harness.prepare_job_snapshot(
                self.repo.read_job(self.target),self.repo.read_queue()))

    def test_b_product_rejection(self):
        self.technical_fixture()
        source = self.repo.read_job(self.source)
        last = dict(source['last_result'],review_status='REVIEW_FAILED')
        source = self.edit_job(self.source,last_result=last)
        self.args['expected_source_revision'] = source['revision']
        self.rejected('CANDIDATE_PRODUCT_REJECTED')

    def test_c_foreign_scope(self):
        self.technical_fixture()
        target = self.repo.read_job(self.target)
        target = self.edit_job(self.target,request=dict(target['request'],target_resources=['other/file']))
        self.args['expected_target_revision'] = target['revision']
        self.rejected('QUARANTINED_CANDIDATE_SCOPE_CONFLICT')

    def test_d_hash_mismatch(self):
        self.technical_fixture()
        (self.product/FILES[0]).write_bytes(b'changed externally')
        self.rejected('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')
