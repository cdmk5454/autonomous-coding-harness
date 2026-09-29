import copy
import unittest
from test_0911_handoff import CandidateHandoffTests
from control_repository import RepositoryError
from job_queue import QA_CORRECTIVE_CONFIRMATION
from unittest.mock import patch


class BaselineBindingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = CandidateHandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture
        self.edit(active_baseline_id=None)
        self.source_before = self.f.repo.read_job(self.f.source)
        self.bytes_before = {p: (self.f.product / p).read_bytes() for p in self.f.hashes}

    def edit(self, **fields):
        job = self.f.repo.read_job(self.f.target)
        revision = job['revision']
        job.update(fields)
        job['revision'] += 1
        self.f.repo.write_job(job, expected_revision=revision)

    def bind(self, **overrides):
        args = dict(source_job_id=self.f.source, target_job_id=self.f.target,
                    expected_source_revision=self.f.repo.read_job(self.f.source)['revision'],
                    expected_target_revision=self.f.repo.read_job(self.f.target)['revision'])
        args.update(overrides)
        return self.f.control.bind_corrective_baseline(**args)

    def tearDown(self):
        self.assertEqual(self.source_before, self.f.repo.read_job(self.f.source))
        self.assertEqual(self.bytes_before, {p: (self.f.product / p).read_bytes() for p in self.f.hashes})

    def test_existing_and_handoff(self):
        self.assertFalse(self.bind()['replayed'])
        self.assertEqual(self.f.baseline, self.f.repo.read_job(self.f.target)['active_baseline_id'])
        self.assertTrue(self.f.control.preview_candidate_ownership_handoff(self.f.source, self.f.target)['eligible'])

    def test_stale_cas(self):
        with self.assertRaisesRegex(RepositoryError, 'CONTROL_REVISION_CONFLICT'):
            self.bind(expected_target_revision=0)

    def test_source_current_mismatch(self):
        queue = self.f.repo.read_queue()
        revision = queue['revision']
        queue['active_baseline_id'] = 'OTHER'
        self.f.repo.write_queue(queue, expected_revision=revision)
        with self.assertRaisesRegex(RepositoryError, 'BASELINE_INTEGRITY_INVALID'):
            self.bind()

    def test_started(self):
        self.edit(outer_attempt=1)
        with self.assertRaisesRegex(RepositoryError, 'ALREADY_STARTED'):
            self.bind()

    def test_idempotent(self):
        self.bind()
        before = self.f.repo.read_job(self.f.target)
        self.assertTrue(self.bind()['replayed'])
        self.assertEqual(before, self.f.repo.read_job(self.f.target))

    def test_conflicting_baseline(self):
        self.edit(active_baseline_id='OTHER')
        with self.assertRaisesRegex(RepositoryError, 'BASELINE_CONFLICT'):
            self.bind()

    def test_preview_read_only(self):
        before = self.f.repo.read_job(self.f.target)
        self.assertTrue(self.bind(preview=True)['eligible'])
        self.assertEqual(before, self.f.repo.read_job(self.f.target))

    def test_future_link_preparation(self):
        target = self.f.repo.read_job(self.f.other)
        revision = target['revision']
        target['active_baseline_id'] = None
        target['revision'] += 1
        self.f.repo.write_job(target, expected_revision=revision)
        with patch.object(self.f.control, '_public_job', side_effect=lambda job, **kw: job), \
             patch.object(self.f.control, '_queue_snapshot', return_value={}):
            result = self.f.control.record_qa_corrective_successor(
                self.f.source, self.f.other, self.f.repo.read_queue()['revision'], target['revision'],
                'new-link-baseline', 'fixture', QA_CORRECTIVE_CONFIRMATION)
        self.assertEqual(self.f.baseline, result['job']['active_baseline_id'])


if __name__ == '__main__':
    unittest.main()
