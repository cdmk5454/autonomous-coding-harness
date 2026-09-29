import copy
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from test_0916_technical_handoff import TechnicalHandoffTests


class ExecutionIdentityTests(TechnicalHandoffTests):
    def materialize(self, job_id=None):
        job = self.repo.read_job(job_id or self.target)
        return self.repo.materialize(
            job, context={'profile_snapshot_sha256': 'profile', 'policy_snapshot_sha256': 'policy'},
            policy={'effective_policy_sha256': 'policy'}, baseline_hash='baseline',
            execution_surface_hash='surface',
            extra={'current_requirement': job.get('current_requirement', 'historical contract')})

    def terminal_fixture(self):
        self.technical_fixture()
        self.control.handoff_candidate_ownership(**self.args)
        old = self.materialize()
        self.edit_job(self.target, status='AWAITING_QA')
        with self.repo.hold():
            row = dict(self.repo._connection.execute(
                'SELECT * FROM executions WHERE execution_id=?', (old['execution_id'],)).fetchone())
        self.assertEqual('TERMINAL', row['phase'])
        self.edit_job(self.target, status='QUEUED', current_requirement='Academic year Y: August Y-1 + February Y; exclude August Y')
        return old, row

    def test_a_terminal_gets_fresh_identity(self):
        old, _ = self.terminal_fixture()
        fresh = self.materialize()
        self.assertNotEqual(old['execution_id'], fresh['execution_id'])
        self.assertEqual(old['execution_id'], fresh['handoff_source_execution_id'])
        self.assertEqual(self.target, fresh['job_id'])

    def test_b_history_immutable(self):
        old, row = self.terminal_fixture()
        source = copy.deepcopy(self.repo.read_job(self.source))
        owners = self.repo.preserved_candidate_records(self.source)
        self.materialize()
        with self.repo.hold():
            after = dict(self.repo._connection.execute(
                'SELECT * FROM executions WHERE execution_id=?', (old['execution_id'],)).fetchone())
        self.assertEqual(row, after)
        self.assertEqual(source, self.repo.read_job(self.source))
        self.assertEqual(owners, self.repo.preserved_candidate_records(self.source))

    def test_c_current_contract(self):
        historical_requirement = self.repo.read_job(self.target)['current_requirement']
        old, _ = self.terminal_fixture()
        fresh = self.materialize()
        self.assertEqual(self.repo.read_job(self.target)['current_requirement'], fresh['current_requirement'])
        self.assertEqual(historical_requirement, old['current_requirement'])
        self.assertEqual(self.repo.read_job(self.target).get('contract_revision', 1), fresh['contract_revision'])

    def test_d_duplicate_identity_rejected(self):
        active = self.materialize()
        with patch('control_repository.uuid.uuid4', return_value=SimpleNamespace(hex=active['execution_id'][5:])):
            with self.assertRaises(sqlite3.IntegrityError):
                self.materialize(self.other)
        self.assertEqual(active, self.repo.active_execution(self.target))
