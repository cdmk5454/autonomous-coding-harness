import copy
from test_0916_candidate_projection import CandidateProjectionTests


class CompletedTakeoverTests(CandidateProjectionTests):
    def completed(self):
        self.success()
        # Cumulative success acknowledgement is a separate, already-satisfied gate.
        self.f.edit_job(self.f.other,success_acknowledgements={'cumulative-auto-ack':True})

    def test_verified_completed(self):
        self.completed()
        self.reconcile(False)
        self.assertEqual('', self.f.store._quarantined_scope_conflict(
            self.f.repo.read_queue(), {'job_id':'NEXT','request':{'target_resources':list(self.f.hashes)}}))

    def test_unverified_preserved(self):
        self.completed()
        last = dict(self.f.repo.read_job(self.f.other)['last_result'],verification_status='PARTIAL')
        self.f.edit_job(self.f.other,last_result=last)
        self.reconcile(True)

    def test_failed_preserved(self):
        self.completed()
        for state in ('FAILED','FAILED_FINAL','BLOCKED','CANCELLED','SKIPPED'):
            self.f.edit_job(self.f.other,status=state)
            self.reconcile(True)

    def test_operator_hold_preserved(self):
        self.completed()
        q = self.f.repo.read_queue()
        self.f.store.pause('Independent hold',expected_revision=q['revision'],
            request_id='independent',confirmation='I_CONFIRM_PAUSE_QUEUE')
        self.supervisor._reconcile_interrupted()
        after = self.f.repo.read_queue()
        self.assertTrue(after['paused'])
        self.assertTrue(after['operator_paused'])
        self.assertEqual('',after['blocked_by_job_id'])

    def test_history_immutable(self):
        self.completed()
        before = [copy.deepcopy(self.f.repo.read_job(i)) for i in (self.f.source,self.f.target,self.f.other)]
        owners = self.f.repo.preserved_candidate_records(self.f.target)
        self.reconcile(False)
        self.assertEqual(before,[self.f.repo.read_job(i) for i in (self.f.source,self.f.target,self.f.other)])
        self.assertEqual(owners,self.f.repo.preserved_candidate_records(self.f.target))
