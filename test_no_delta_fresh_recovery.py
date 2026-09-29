import copy
import unittest
from unittest.mock import patch

from cumulative_policy import CumulativePolicyError
from git_collector import GitCollector
from harness_service import HarnessService
from job_queue import JobStore, QueueError
from test_09005_hotfix import context, request
from test_batch_final_commit import CumulativeSnapshotTest


class FreshRecoveryTest(CumulativeSnapshotTest):
    def fixture(self):
        self.store = JobStore(self.control)
        jobs, _ = self.store.enqueue_batch('fresh-test', [request('source', ['app/same.txt'])],
                                            execution_context=context())
        baseline = self.manager.prepare_batch_baseline(1)['active_baseline_id']
        q = self.store._read_queue()
        q['active_baseline_id'] = baseline
        self.store._write_queue(q)
        parent = self.store.claim_next('fixture')
        snap = self.manager.prepare_job_snapshot(parent, baseline, [])
        proof = GitCollector(str(self.workspace), profile=self.profile).prove_no_task_delta(
            self.manager.load_job_baseline(snap['manifest_path']))
        parent = self.store.record_result(parent['job_id'], 'fixture', {
            'status': 'FAILED', 'success': False, 'stage': 'DONE',
            'failure_type': 'technical', 'fix_scope': 'NON_RETRYABLE',
            'worktree_disposition': 'CLEAN_ROLLBACK',
            'checkpoint_status': 'VERIFIED_NO_TASK_DELTA', 'checkpoint_manifest': '',
            'changed_files': [], 'task_owned_changed_files': [],
            'no_task_delta_evidence': proof})
        parent.update(active_baseline_id=baseline, pre_job_snapshot_id=snap['snapshot_id'],
                      pre_job_snapshot_manifest=snap['manifest_path'])
        self.store._write_job(parent)
        self.parent = self.store.get_job(parent['job_id'])
        self.q = self.store.queue_snapshot()
        self.integrity = self.manager.public_status(baseline, [])
        self.integrity.update(generation=self.q['generation'], active_baseline_id=baseline)

    def replace(self, fresh):
        return self.store.enqueue_anchored_replacement(
            self.parent['job_id'], request('fresh', ['app/same.txt']),
            expected_queue_revision=self.q['revision'], expected_job_revision=self.parent['revision'],
            request_id='fresh-recovery', reason='approved no-delta recovery',
            execution_context=context(), provenance={'status': 'VERIFIED'},
            integrity_snapshot=self.integrity, fresh_input=fresh)[0]

    def test_a_fresh_input(self):
        self.fixture()
        before = copy.deepcopy(self.parent)
        fresh = self.manager.validate_no_delta_fresh_input(self.parent, self.q, self.integrity)
        new = self.replace(fresh)
        service = object.__new__(HarnessService)
        service.control_root = self.control
        service.working_dir = self.workspace
        with patch.object(service, '_cumulative_manager', return_value=self.manager), \
             patch.object(service, 'cumulative_policy_status', return_value=self.integrity):
            snapshot = service.prepare_job_snapshot(new, self.store.queue_snapshot())
        self.assertNotEqual(new['job_id'], before['job_id'])
        self.assertEqual(new['checkpoint_seed'], {})
        self.assertNotEqual(snapshot['snapshot_id'], before['pre_job_snapshot_id'])
        self.assertEqual(self.store.get_job(before['job_id']), before)

    def test_b_inherited_delta_requires_checkpoint(self):
        self.fixture()
        self.parent['last_result']['task_owned_changed_files'] = ['app/same.txt']
        with self.assertRaisesRegex(CumulativePolicyError, 'CORRECTIVE_CHECKPOINT_INVALID'):
            self.manager.validate_no_delta_fresh_input(self.parent, self.q, self.integrity)
        with self.assertRaisesRegex(QueueError, 'CORRECTIVE_CHECKPOINT_INVALID'):
            self.replace({})

    def test_c_input_mismatch_fails_closed(self):
        self.fixture()
        (self.repo / 'same.txt').write_text('unexpected mutation\n', encoding='utf-8')
        before = (self.repo / 'same.txt').read_bytes()
        queue_before = self.store.queue_snapshot()
        with self.assertRaisesRegex(CumulativePolicyError, 'REPLACEMENT_WORKTREE_INTEGRITY_INVALID'):
            self.manager.validate_no_delta_fresh_input(self.parent, self.q, self.integrity)
        self.assertEqual((self.repo / 'same.txt').read_bytes(), before)
        self.assertEqual(self.store.queue_snapshot(), queue_before)


if __name__ == '__main__':
    unittest.main()
