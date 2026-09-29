"""Schema7 admission checks through the real JobStore path on disposable Control."""

from pathlib import Path
import tempfile
import threading
import time
import subprocess
import sys
import sqlite3
from contextlib import closing
import unittest
from unittest.mock import patch

from bounded_execution import (migrate_disposable, migrate_capacity_disposable, migrate_capacity_control,
    slots, capacity, resize_capacity, release_slot, renew_process, reconcile_slots,
    request_cancel)
from control_repository import ControlRepository
from canonical_ownership import canonical_ownership, CanonicalBusy
from harness_11.capacity import CapacityPolicy
from hermes_boundary import atomic
from job_contract import JobRequest
from job_queue import JobStore
from queue_supervisor import QueueSupervisor


class NWorkerDispatchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='h11-dispatch-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control = self.root/'.control'
        self.control.mkdir()
        self.repo = ControlRepository(self.control/'control.sqlite3', create=True)
        self.repo.bootstrap(JobStore._empty_queue(), {}, {})
        atomic(self.control/'sqlite-authority.json', {'authority':'SQLITE','schema_version':5})
        self.store = JobStore(self.control)
        self.store.pause('explicit disposable migration')
        migrate_disposable(self.repo.path, allocation_root=self.root,
                           backup_path=self.root/'schema5.bak')
        migrate_capacity_disposable(self.repo.path, allocation_root=self.root,
            backup_path=self.root/'schema6.bak',
            policy=CapacityPolicy(workers=3,wip=3), quiescence_probe=lambda _: True)
        self.store.resume_queue()

    def request(self, number, *, resources=None, dependencies=(), shared=()):
        return JobRequest.from_mapping({
            'client_job_id': f'job-{number}', 'requirement': f'Change module {number}',
            'worker': 'codex', 'model': 'gpt-5.6-sol', 'target_modules': ['app'],
            'target_resources': resources or [f'app/file{number}.py'],
            'depends_on': list(dependencies),
            'shared_resources': list(shared),
        }, allowed_modules=['app'])

    def enqueue(self, requests):
        context = {'profile_id':'disposable','profile_schema_version':1,
            'profile_manifest_sha256':'a'*64,'profile_snapshot_sha256':'b'*64,
            'policy_snapshot_sha256':'c'*64,'workspace_identity_sha256':'d'*64}
        return self.store.enqueue_batch('capacity-fixture', requests, execution_context=context)[0]

    def materialize(self, job):
        return self.repo.materialize(job, context=job['execution_context'],
            policy={'effective_policy_sha256':'f'*64},
            baseline_hash='e'*64, execution_surface_hash='d'*64)

    def test_explicit_2_to_3_migration_and_three_real_claims(self):
        with closing(sqlite3.connect(self.root/'schema6.bak')) as backup:
            self.assertEqual(6,backup.execute('PRAGMA user_version').fetchone()[0])
            self.assertEqual('ok',backup.execute('PRAGMA quick_check').fetchone()[0])
        with closing(sqlite3.connect(self.repo.path)) as migrated:
            self.assertEqual(7,migrated.execute('PRAGMA user_version').fetchone()[0])
            self.assertEqual('ok',migrated.execute('PRAGMA quick_check').fetchone()[0])
            self.assertEqual([],migrated.execute('PRAGMA foreign_key_check').fetchall())
        self.assertEqual(3, capacity(self.repo))
        jobs = self.enqueue([self.request(i) for i in (1,2,3)])
        claimed = [self.store.claim_next(f'owner-{i}') for i in (1,2,3)]
        self.assertEqual([j['job_id'] for j in jobs], [j['job_id'] for j in claimed])
        self.assertEqual(3, len(slots(self.repo)))

    def test_live_migration_entry_requires_separate_post_review_approval(self):
        with self.assertRaisesRegex(ValueError,'EXPLICIT_CONTROL_MIGRATION_APPROVAL_REQUIRED'):
            migrate_capacity_control(self.repo.path,control_root=self.control,
                backup_root=self.root,backup_path=self.root/'not-approved.bak',
                policy=CapacityPolicy(workers=4,wip=4),quiescence_probe=lambda _:True,
                operator_approval='')
        self.assertFalse((self.root/'not-approved.bak').exists())

    def test_write_scope_conflict_skips_only_conflicting_job(self):
        jobs = self.enqueue([self.request(1, resources=['app/common']),
            self.request(2, resources=['app/common/nested.py']), self.request(3)])
        self.assertEqual(jobs[0]['job_id'], self.store.claim_next('owner-1')['job_id'])
        self.assertEqual(jobs[2]['job_id'], self.store.claim_next('owner-2')['job_id'])
        self.assertEqual('QUEUED', self.store.get_job(jobs[1]['job_id'])['status'])

    def test_dependency_blocks_before_claim(self):
        jobs = self.enqueue([self.request(1), self.request(2, dependencies=('job-1',)),
                             self.request(3)])
        self.assertEqual(jobs[0]['job_id'], self.store.claim_next('owner-1')['job_id'])
        self.assertEqual(jobs[2]['job_id'], self.store.claim_next('owner-2')['job_id'])
        self.assertEqual('QUEUED', self.store.get_job(jobs[1]['job_id'])['status'])

    def _assert_shared_conflict(self, namespace):
        first = self.request(1, shared=({'key':f'{namespace}/shared','access':'WRITE'},))
        second = self.request(2, shared=({'key':f'{namespace}/shared','access':'WRITE'},))
        jobs = self.enqueue([first, second, self.request(3)])
        self.assertEqual(jobs[0]['job_id'], self.store.claim_next('owner-1')['job_id'])
        self.assertEqual(jobs[2]['job_id'], self.store.claim_next('owner-2')['job_id'])

    def test_shared_build_conflict(self): self._assert_shared_conflict('build')
    def test_shared_port_conflict(self): self._assert_shared_conflict('port')
    def test_shared_db_conflict(self): self._assert_shared_conflict('db')

    def test_shrink_preserves_existing_slots_and_blocks_new_claim(self):
        jobs = self.enqueue([self.request(i) for i in (1,2,3,4)])
        for i in (1,2,3):
            self.assertEqual(jobs[i-1]['job_id'], self.store.claim_next(f'owner-{i}')['job_id'])
        resize_capacity(self.repo, CapacityPolicy(workers=2,wip=3))
        self.assertEqual(3, len(slots(self.repo)))
        self.assertIsNone(self.store.claim_next('owner-4'))
        self.assertEqual('QUEUED', self.store.get_job(jobs[3]['job_id'])['status'])
        self.store.record_blocked(jobs[0]['job_id'], 'owner-1', 'DISPOSABLE_DONE')
        self.assertTrue(release_slot(self.repo, jobs[0]['job_id'], 'owner-1'))
        projection = self.repo.read_queue()
        self.assertEqual(jobs[1]['job_id'], projection['running_job_id'])
        self.assertEqual(2, projection['active_execution_count'])
        # Slots 2 and 3 still occupy both permits after the shrink.
        self.assertIsNone(self.store.claim_next('owner-4'))

    def test_start_failure_releases_only_unstarted_slot(self):
        job = self.enqueue([self.request(1)])[0]
        self.store.claim_next('owner-1')
        self.store.record_blocked(job['job_id'], 'owner-1', 'WORKER_STARTUP_FAILED')
        self.assertEqual(1, len(slots(self.repo)))
        self.assertTrue(release_slot(self.repo, job['job_id'], 'owner-1'))
        self.assertEqual([], slots(self.repo))
        projection = self.repo.read_queue()
        self.assertEqual('', projection['running_job_id'])
        self.assertEqual(0, projection['active_execution_count'])

    def test_restart_quarantines_residual_slot(self):
        job = self.enqueue([self.request(1)])[0]
        self.store.claim_next('owner-1')
        reopened = ControlRepository(self.repo.path)
        restarted = JobStore(self.control, repository=reopened)
        class Harness:
            def acquire_execution_lease(self): return True
            def release_execution_lease(self): pass
        class Factory:
            def preflight(self): pass
        result = QueueSupervisor(restarted, Harness(), execution_service_factory=Factory()).start()
        self.assertEqual('EXECUTION_SLOT_RECONCILIATION_REQUIRED', result['code'])
        self.assertEqual([job['job_id']], result['held_jobs'])
        self.assertEqual('QUARANTINED', slots(reopened)[0]['state'])
        self.assertIsNone(self.store.claim_next('owner-2'))

    def test_stale_lease_blocks_new_claim_and_preserves_slot(self):
        resize_capacity(self.repo, CapacityPolicy(workers=3,wip=3,lease_seconds=0.02))
        jobs = self.enqueue([self.request(1), self.request(2)])
        self.store.claim_next('owner-1')
        time.sleep(0.04)
        self.assertIsNone(self.store.claim_next('owner-2'))
        self.assertEqual([jobs[0]['job_id']], reconcile_slots(self.repo))
        self.assertEqual('QUARANTINED', slots(self.repo)[0]['state'])

    def test_cancel_with_live_child_never_reuses_slot(self):
        job = self.enqueue([self.request(1)])[0]
        claimed = self.store.claim_next('owner-1')
        execution = self.materialize(claimed)
        attempt = self.repo.start_attempt(execution, attempt_id='DISPOSABLE-WORKER-1',
                                          role='WORKER', model='gpt-5.6-sol')
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        try:
            process = self.repo.record_runtime_process(attempt['attempt_id'], runtime='disposable',
                pid=child.pid, process_identity=f'{child.pid}:disposable-birth')
            self.assertEqual(f'{child.pid}:disposable-birth', slots(self.repo)[0]['process_identity'])
            self.assertEqual({'cancel_requested':True,'slot_released':False,'writer_fenced':False},
                             request_cancel(self.repo, job['job_id'], 'owner-1'))
            self.assertEqual(1, len(slots(self.repo)))
            with self.assertRaisesRegex(ValueError, 'EXECUTION_NOT_TERMINAL'):
                release_slot(self.repo, job['job_id'], 'owner-1')
        finally:
            child.terminate()
            child.wait(timeout=5)
        self.repo.finish_runtime_process(process['process_id'], state='KILLED', exit_code=child.returncode)
        self.repo.finish_attempt(attempt['attempt_id'], status='INTERRUPTED')
        self.store.record_blocked(job['job_id'], 'owner-1', 'DISPOSABLE_CANCEL')
        self.assertEqual(1, len(slots(self.repo)))
        with self.assertRaisesRegex(ValueError, 'CANDIDATE_CUSTODY_REQUIRED'):
            release_slot(self.repo, job['job_id'], 'owner-1')

    def test_test_and_review_wip_backpressure(self):
        resize_capacity(self.repo, CapacityPolicy(workers=3,wip=3,test_backlog=1,review_backlog=1))
        jobs = self.enqueue([self.request(1), self.request(2)])
        first = self.store.claim_next('owner-1')
        execution = self.materialize(first)
        attempt = self.repo.start_attempt(execution, attempt_id='DISPOSABLE-TESTER-1',
                                          role='TESTER', model='fixture')
        self.assertIsNone(self.store.claim_next('owner-2'))
        self.repo.finish_attempt(attempt['attempt_id'], status='SUCCEEDED')
        reviewer = self.repo.start_attempt(execution, attempt_id='DISPOSABLE-REVIEWER-1',
                                           role='REVIEWER', model='fixture')
        self.assertIsNone(self.store.claim_next('owner-2'))
        self.repo.finish_attempt(reviewer['attempt_id'], status='SUCCEEDED')
        self.assertEqual(jobs[1]['job_id'], self.store.claim_next('owner-2')['job_id'])

    def test_qa_held_job_releases_only_after_result_and_custody(self):
        job = self.enqueue([self.request(1)])[0]
        self.store.claim_next('owner-1')
        held = self.store.record_result(job['job_id'], 'owner-1', {
            'status':'AWAITING_QA','stage':'AWAITING_QA','success':False,
            'qa_type':'HUMAN_ACCEPTANCE','hold_scope':'JOB',
            'verification_status':'PARTIAL','worktree_disposition':'NO_DELTA'})
        self.assertEqual('AWAITING_QA', held['status'])
        self.assertEqual(1, len(slots(self.repo)))
        self.assertTrue(release_slot(self.repo, job['job_id'], 'owner-1'))
        self.assertEqual([], slots(self.repo))

    def test_queue_supervisor_dispatches_three_real_claims(self):
        self.enqueue([self.request(i) for i in (1,2,3)])
        barrier = threading.Barrier(3)
        fixture_root = self.root

        class Custody:
            def __init__(self, job, candidate):
                self.candidate = candidate
                self.root = candidate.parent
                atomic(self.root/'allocation.json', {
                    'job_id': job['job_id'], 'candidate_id': 'DISPOSABLE', 'state': 'ALLOCATED'})
            def close(self):
                atomic(self.root/'allocation.json', {
                    'job_id': self.job_id, 'candidate_id': 'DISPOSABLE', 'state': 'CLOSED'})
            @property
            def job_id(self):
                return self.root.name

        class Service:
            def __init__(self, job=None):
                self.job = job
                self.working_dir = fixture_root/'candidates'/job['job_id']/'candidate' if job else fixture_root
                if job:
                    self.working_dir.mkdir(parents=True)
                    self.isolated_execution = Custody(job, self.working_dir)
                else:
                    self.isolated_execution = None
            def acquire_execution_lease(self): return True
            def release_execution_lease(self): pass
            def close(self): pass
            def materialize_execution(self, job, repository):
                return {'execution_id':'DISPOSABLE-'+job['job_id'],
                        'execution_context':dict(job.get('execution_context') or {}),
                        'request':job['request'],
                        'current_requirement':job['current_requirement']}
            def execute(self, job, **kwargs):
                barrier.wait(timeout=10)
                return {'status':'AWAITING_QA','stage':'AWAITING_QA','success':False,
                        'verification_status':'PARTIAL','failure_code':'DISPOSABLE_HOLD',
                        'qa_type':'HUMAN_ACCEPTANCE','hold_scope':'JOB',
                        'worktree_disposition':'NO_DELTA'}

        class Factory:
            def preflight(self): pass
            def __call__(self, job, repository): return Service(job)

        supervisor = QueueSupervisor(self.store, Service(), execution_service_factory=Factory())
        with patch('queue_supervisor.HarnessService', Service):
            supervisor._run_bounded()
        self.assertEqual(3, supervisor._processed, supervisor._last_error)
        self.assertEqual('', supervisor._last_error)
        self.assertEqual([], slots(self.repo))
        projection = self.repo.read_queue()
        self.assertEqual('', projection['running_job_id'])
        self.assertEqual(0, projection['active_execution_count'])

    def test_simultaneous_publication_has_one_canonical_writer(self):
        canonical = self.root/'canonical'
        canonical.mkdir()
        entered, release = threading.Event(), threading.Event()
        def first_writer():
            with canonical_ownership(canonical):
                entered.set()
                release.wait(timeout=5)
        owner = threading.Thread(target=first_writer)
        owner.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            with self.assertRaises(CanonicalBusy):
                with canonical_ownership(canonical):
                    self.fail('second canonical writer entered')
        finally:
            release.set()
            owner.join(timeout=5)
        self.assertFalse(owner.is_alive())


if __name__ == '__main__':
    unittest.main()
