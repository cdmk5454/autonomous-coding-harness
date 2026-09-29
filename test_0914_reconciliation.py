"""Focused gate projection only: disposable repository, never dispatch."""
import copy
import unittest
from unittest.mock import Mock, patch

from control_repository import ControlRepository
from job_queue import JobStore
from queue_supervisor import QueueSupervisor
from test_0911_handoff import CandidateHandoffTests


class CorrectiveGateTests(unittest.TestCase):
    def setUp(self):
        self.f = CandidateHandoffTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.supervisor = QueueSupervisor(self.f.store, self.f.harness)
        self.f.harness.cumulative_policy_status = Mock(
            side_effect=lambda **kw: dict(self.f.integrity))
        self.f.harness.execute_job = Mock(side_effect=AssertionError('NO WORKER'))

    def takeover(self):
        self.f.control.handoff_candidate_ownership(**self.f.args)

    def set_link(self, **fields):
        queue = self.f.repo.read_queue()
        rev = queue['revision']
        queue['qa_corrective_successors'][self.f.source].update(fields)
        self.f.repo.write_queue(queue, expected_revision=rev)

    def reconcile(self, paused):
        before = [self.f.repo.read_job(i) for i in (self.f.source, self.f.target)]
        ownership = self.f.repo.candidate_ownership(self.f.product)
        files = self.f.bytes()
        baseline = self.f.repo.read_queue()['active_baseline_id']
        self.assertEqual([], self.supervisor._reconcile_interrupted())
        queue = self.f.repo.read_queue()
        self.assertEqual(paused, queue['paused'])
        self.assertEqual(self.f.source if paused else '', queue['blocked_by_job_id'])
        self.assertEqual(before, [self.f.repo.read_job(i) for i in (self.f.source, self.f.target)])
        self.assertEqual(ownership, self.f.repo.candidate_ownership(self.f.product))
        self.assertEqual(files, self.f.bytes())
        self.assertEqual(baseline, queue['active_baseline_id'])
        self.f.harness.execute_job.assert_not_called()
        self.assertFalse(self.supervisor.is_running())
        return queue

    def test_a_no_successor(self):
        self.set_link(corrective_job_id='')
        self.reconcile(True)

    def test_b_valid_takeover(self):
        self.takeover()
        self.reconcile(False)

    def test_c_no_ownership_transfer(self):
        self.reconcile(True)

    def test_d_invalidated_takeover(self):
        self.takeover()
        self.reconcile(False)
        for status in ('CANCELLED', 'FAILED_FINAL', 'AWAITING_QA'):
            self.f.edit_job(self.f.target, status=status)
            self.reconcile(True)

    def test_e_unrelated_or_inactive_link(self):
        self.takeover()
        self.set_link(corrective_job_id=self.f.other)
        self.reconcile(True)
        self.set_link(corrective_job_id=self.f.target, state='INACTIVE')
        self.reconcile(True)

    def test_f_idempotency(self):
        self.takeover()
        first = self.reconcile(False)
        self.assertEqual(first, self.reconcile(False))

    def test_g_reload(self):
        self.takeover()
        first = self.reconcile(False)
        repo = ControlRepository(self.f.control_root / 'control.sqlite3')
        self.supervisor = QueueSupervisor(JobStore(self.f.control_root, repository=repo), self.f.harness)
        self.assertEqual(first, self.reconcile(False))

    def test_h_historical_immutable(self):
        self.takeover()
        source = copy.deepcopy(self.f.repo.read_job(self.f.source))
        self.reconcile(False)
        self.assertEqual(source, self.f.repo.read_job(self.f.source))

    def test_i_integrity_and_binding_fail_closed(self):
        self.takeover()
        self.reconcile(False)
        self.f.integrity['baseline_declaration_integrity'] = False
        self.reconcile(True)
        self.f.integrity['baseline_declaration_integrity'] = True
        self.f.edit_job(self.f.target, active_baseline_id='OTHER')
        self.reconcile(True)
        self.f.edit_job(self.f.target, active_baseline_id=self.f.baseline)
        path = self.f.product / next(iter(self.f.hashes))
        path.write_bytes(b'invalid fixture candidate')
        self.reconcile(True)

    def test_j_no_worker_or_dispatch(self):
        self.takeover()
        with patch.object(self.f.store, 'claim_next', side_effect=AssertionError('NO DISPATCH')):
            self.reconcile(False)
        self.assertEqual([], self.f.repo.read_job(self.f.target)['task_ids'])


if __name__ == '__main__':
    unittest.main()
