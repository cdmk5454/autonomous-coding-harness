"""Cold integrated Batch verification operator; extends the A/B revalidation flow.

Both parents are quarantined through the real child pipeline, A is published
through the actual QA decision flow, B through post-A current-input
revalidation and QA. Batch then runs fresh gate-only Build/Test/Review/
Verification over the exact combined published input with real Git, profile,
runtime snapshots, decisions and receipts. Negative conditions are exercised
against the same real published state and restored afterwards.
"""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_canonical_revalidation_operator import ColdCanonicalOperatorTests


class ColdBatchVerificationOperatorTests(ColdCanonicalOperatorTests):
    def prepare_quarantined_parents(self):
        from production_isolation import MODEL
        requests = [dict(client_job_id=name.upper(), requirement=f'MODIFICATION: Set app/{name}.py value to 1. Preserve all other source. Run app/test_{name}.py.',
            worker='opencode', model=MODEL, target_modules=['app'], target_resources=[f'app/{name}.py'], depends_on=[],
            test_plan=dict(required_tests=[f'app/test_{name}.py'], command=f'"{Path(sys.executable).as_posix()}" -B app/test_{name}.py', mandatory=True, verification_depth='TARGETED')) for name in ('a', 'b')]
        jobs = self.control.enqueue_jobs('cold-approved-batch-ab', requests)['jobs']
        claims = []
        for name in ('a', 'b'):
            job = self.control.store.claim_next('owner-' + name)
            self.assertIsNotNone(job)
            child = self.control.isolation(job, self.control.store.repository)
            claims.append((job, child, 'owner-' + name))
        self.parents = []
        for job, child, owner in claims:
            snapshot = child.prepare_job_snapshot(job, self.control.store.queue_snapshot())
            job = self.control.store.record_pre_job_snapshot(job['job_id'], owner, snapshot)
            execution = child.materialize_execution(job, self.control.store.repository)
            bound = {**job, 'materialized_execution': execution, 'execution_context': execution['execution_context'],
                   'request': execution['request'], 'current_requirement': execution['current_requirement']}
            def started(task_id, job=job, owner=owner, execution=execution):
                self.control.store.mark_task_started(job['job_id'], owner, task_id)
                return self.repo.start_attempt(execution, attempt_id=task_id + '-WORKER', role='WORKER', model=MODEL)
            self.assertTrue(child.acquire_execution_lease())
            try:
                result = child.execute(bound, on_task_started=started)
            finally:
                child.release_execution_lease()
            self.assertTrue(result['success'], json.dumps(result, ensure_ascii=False, default=str)[-18000:])
            self.parents.append((bound, child, result, owner))
        # Both candidates originated at exactly the same canonical baseline and
        # hold at AWAITING_QA with preserved quarantine custody (real QA flow).
        a, b = self.parents
        for bound, child, result, owner in (b, a):
            held = self.control.isolation.promoter.quarantine(bound, child, result)
            self.control.store.record_result(bound['job_id'], owner, held)
            # Same durable CANDIDATE decision the sanctioned hold flow records.
            from control_repository import digest
            job = self.control.store.get_job(bound['job_id'])
            self.repo.record_decision(bound['job_id'],
                contract_revision=job.get('contract_revision', 1),
                decision_type='CANDIDATE', question='Accept quarantined candidate?',
                fingerprint=digest(dict(candidate_id=held['candidate_id'], job_id=bound['job_id'])),
                payload=dict(candidate_id=held['candidate_id'], qa_type='HUMAN_ACCEPTANCE'))
        return a, b

    def accept_qa(self, job_id, request_id):
        cold = self.control
        job = cold.store.get_job(job_id)
        context = cold.get_operator_action_context(job_id)
        self.assertIn('HUMAN_ACCEPT', context['legal_next_actions'])
        accepted = cold.resolve_qa_decision(job_id, resolution='HUMAN_ACCEPT',
            expected_revision=job['revision'], request_id=request_id,
            confirmation='I_CONFIRM_QA_HUMAN_ACCEPT', candidate_id=job['candidate_id'],
            decision_context=context)
        self.assertEqual('SUCCEEDED', accepted['status'], accepted)
        return accepted

    def test_cold_batch_after_real_ab_publication(self):
        from canonical_revalidation import CONFIRMATION, _key, _receipt, _write_receipt
        from task_state import TaskState
        from qa_quarantine import QACandidateStore, CandidateError
        a, b = self.prepare_quarantined_parents()
        a_id, b_id = a[0]['job_id'], b[0]['job_id']
        cold = self.control
        # --- A publication through the real QA decision flow ---
        self.accept_qa(a_id, 'cold-batch-a-accept')
        self.assertEqual('value = 1\n', (self.app / 'a.py').read_text())
        worker_count = len([x for x in self.calls if x[0] == 'WORKER'])
        # --- B post-A current-input revalidation + QA publication ---
        old_revision = cold.store.get_job(b_id)['revision']
        revalidated = cold.revalidate_canonical_candidate(b_id, expected_revision=old_revision,
            request_id='cold-batch-post-a-revalidation', predecessor_ids=[a_id], confirmation=CONFIRMATION)
        self.assertFalse(revalidated['published'])
        self.assertEqual('AWAITING_QA', cold.store.get_job(b_id)['status'])
        self.accept_qa(b_id, 'cold-batch-b-accept')
        self.assertEqual('value = 1\n', (self.app / 'b.py').read_text())
        self.assertEqual(worker_count, len([x for x in self.calls if x[0] == 'WORKER']))
        # --- Batch: fresh gate-only verification over the combined input ---
        ja, jb = cold.store.get_job(a_id), cold.store.get_job(b_id)
        self.assertEqual('SUCCEEDED', ja['status'])
        self.assertEqual('SUCCEEDED', jb['status'])
        batch = cold.verify_batch_current_input([a_id, b_id],
            expected_revisions={a_id: ja['revision'], b_id: jb['revision']},
            request_id='cold-batch-verify-1',
            confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        result = batch['result']
        self.assertTrue(result['success'], json.dumps(result, ensure_ascii=False, default=str)[-8000:])
        self.assertTrue(batch['worker_invoked'] is False)
        self.assertTrue(batch['publication_performed'] is False)
        self.assertTrue(batch['commit_performed'] is False)
        self.assertEqual(worker_count, len([x for x in self.calls if x[0] == 'WORKER']))
        batch_execution = self.repo.execution(result['execution_id'])
        self.assertEqual('BATCH_VERIFICATION', batch_execution['role'])
        self.assertEqual(b_id, batch_execution['job_id'])
        with self.repo.hold():
            phase = self.repo._connection.execute('SELECT phase FROM executions WHERE execution_id=?',
                                                  (result['execution_id'],)).fetchone()[0]
            roles = [row[0] for row in self.repo._connection.execute(
                'SELECT role FROM attempts WHERE execution_id=?', (result['execution_id'],)).fetchall()]
            self.assertEqual('TERMINAL', phase)
            self.assertIn('TESTER', roles)
            self.assertNotIn('WORKER', roles)
            reviewer = self.repo._connection.execute(
                "SELECT execution_id,phase FROM executions WHERE job_id=? AND payload LIKE ? "
                "AND execution_id LIKE 'EXEC-REVIEW-%'",
                (b_id, f'%{result["execution_id"]}%')).fetchall()
            self.assertTrue(reviewer, 'reviewer successor execution for the batch gate')
            for row in reviewer:
                self.assertEqual('TERMINAL', row['phase'])
                reviewer_roles = [r[0] for r in self.repo._connection.execute(
                    'SELECT role FROM attempts WHERE execution_id=?', (row['execution_id'],)).fetchall()]
                self.assertIn('REVIEWER', reviewer_roles)
            self.assertEqual(0, self.repo._connection.execute('SELECT count(*) FROM execution_slots').fetchone()[0])
            self.assertEqual(0, self.repo._connection.execute(
                "SELECT count(*) FROM attempt_outcomes WHERE status IN ('RUNNING','UNKNOWN')").fetchone()[0])
            self.assertEqual(0, self.repo._connection.execute(
                "SELECT count(*) FROM runtime_processes WHERE state NOT IN ('EXITED','KILLED')").fetchone()[0])
        with self.repo.hold():
            receipt = _receipt(self.repo, _key('cold-batch-verify-1'))
        self.assertEqual('VERIFIED', receipt['state'])
        self.assertTrue(receipt['batch_completed'] is True)
        # Canonical unchanged by the gate-only batch execution.
        self.assertEqual('value = 1\n', (self.app / 'a.py').read_text())
        self.assertEqual('value = 1\n', (self.app / 'b.py').read_text())
        self.assertEqual(self.initial_head, self.git('rev-parse', 'HEAD'))
        store = QACandidateStore(cold.isolation.promoter.root / 'candidates', self.work, None)
        self.assertEqual([], store.unresolved_publications())
        # --- Idempotent replay of the completed batch receipt ---
        replay = cold.verify_batch_current_input([a_id, b_id],
            expected_revisions={a_id: ja['revision'], b_id: jb['revision']},
            request_id='cold-batch-verify-1',
            confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        self.assertTrue(replay['replayed'] and replay['historical_only'])
        revisions = {a_id: ja['revision'], b_id: jb['revision']}
        # --- Negative: foreign drift on the published canonical input ---
        drift_target = self.app / 'test_a.py'
        drift_original = drift_target.read_bytes()
        try:
            drift_target.write_text('# drifted foreign bytes\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'BATCH_UNOWNED_POST_PUBLICATION_DRIFT'):
                cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
                    request_id='cold-batch-neg-drift',
                    confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        finally:
            drift_target.write_bytes(drift_original)
        # --- Negative: mismatching hash of published canonical bytes ---
        hash_target = self.app / 'a.py'
        hash_original = hash_target.read_bytes()
        try:
            hash_target.write_text('value = 2\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'BATCH_UNOWNED_POST_PUBLICATION_DRIFT'):
                cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
                    request_id='cold-batch-neg-hash',
                    confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        finally:
            hash_target.write_bytes(hash_original)
        # --- Negative: incomplete publication journal ---
        jb_now = cold.store.get_job(b_id)
        publication_id = jb_now['last_result']['canonical_promotion']['publication_id']
        journal_path = cold.isolation.promoter.root / 'candidates' / 'publications' / f'{publication_id}.json'
        self.assertTrue(journal_path.is_file(), journal_path)
        journal_original = journal_path.read_bytes()
        try:
            journal = json.loads(journal_original.decode('utf-8'))
            journal['state'] = 'PUBLISHING'
            journal_path.write_text(json.dumps(journal, ensure_ascii=False, sort_keys=True, indent=1), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'BATCH_UNRESOLVED_PUBLICATION'):
                cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
                    request_id='cold-batch-neg-journal',
                    confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        finally:
            journal_path.write_bytes(journal_original)
        # --- Negative: stale parent gate evidence ---
        task_id = jb_now['last_result']['task_id']
        task_path = TaskState.task_date_dir(task_id, self.tasks) / (task_id + '.json')
        task_original = task_path.read_bytes()
        try:
            task = json.loads(task_original.decode('utf-8'))
            task['gate_evidence'] = {}
            task_path.write_text(json.dumps(task, ensure_ascii=False), encoding='utf-8')
            with self.assertRaisesRegex(CandidateError, 'QA_VERIFICATION_INPUT_CHANGED'):
                cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
                    request_id='cold-batch-neg-stale',
                    confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        finally:
            task_path.write_bytes(task_original)
        # --- Negative: partial replay of an incomplete batch receipt ---
        with self.repo.hold():
            receipt = _receipt(self.repo, _key('cold-batch-verify-1'))
            receipt.pop('batch_completed', None)
            _write_receipt(self.repo, _key('cold-batch-verify-1'), receipt)
        try:
            with self.assertRaisesRegex(ValueError, 'BATCH_RECONCILIATION_REQUIRED'):
                cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
                    request_id='cold-batch-verify-1',
                    confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        finally:
            with self.repo.hold():
                receipt = _receipt(self.repo, _key('cold-batch-verify-1'))
                receipt['batch_completed'] = True
                _write_receipt(self.repo, _key('cold-batch-verify-1'), receipt)
        # Final integrity: replay still works after all negatives were restored.
        final = cold.verify_batch_current_input([a_id, b_id], expected_revisions=revisions,
            request_id='cold-batch-verify-1',
            confirmation='I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION')
        self.assertTrue(final['replayed'] and final['historical_only'])
        self.assertEqual('value = 1\n', (self.app / 'a.py').read_text())
        self.assertEqual('value = 1\n', (self.app / 'b.py').read_text())


class BatchEmptyDiffContractTests(unittest.TestCase):
    """Empty combined diff cannot become an authoritative Batch contract."""

    def test_empty_changed_files_rejected(self):
        from batch_verification import freeze_batch_contract
        parents = []
        for name in ('A', 'B'):
            request = {'requirement': 'r ' + name, 'target_modules': ['app'],
                       'target_resources': [f'app/{name}.py']}
            plan = {'production_scope': [f'app/{name}.py'], 'required_tests': [f'app/test_{name}.py'],
                    'derived_tests': [], 'mandatory': True, 'verification_depth': 'TARGETED',
                    'command': f'python -B app/test_{name}.py'}
            execution = {'job_id': 'J-' + name, 'execution_id': 'E-' + name, 'contract_revision': 1,
                         'request': request, 'test_plan': plan, 'profile_revision': 'p',
                         'policy_revision': 'q', 'harness_source_identity': {'source_fingerprint': 'a' * 64},
                         'execution_context': {'profile': 'same'},
                         'effective_policy': {'resolved_tester': {'runtime': 'hermes', 'model': 'x'},
                                              'resolved_reviewer': {'risk_level': 'LOW'},
                                              'supervisor_policy': {'supervisor_required': False}}}
            promotion = {'status': 'PROMOTED', 'publication_id': 'P-' + name,
                         'candidate_ids': ['C-' + name], 'changed_files': []}
            job = {'job_id': 'J-' + name, 'revision': 3, 'contract_revision': 1, 'status': 'SUCCEEDED',
                   'request': request, 'current_requirement': 'r ' + name,
                   'last_result': {'success': True, 'verification_status': 'VERIFIED',
                                   'candidate_id': 'C-' + name, 'canonical_promotion': promotion}}
            intent = {'state': 'PUBLISHED', 'job_id': 'J-' + name, 'execution_id': 'E-' + name,
                      'candidate_id': 'C-' + name, 'publication_id': 'P-' + name}
            journal = {'state': 'COMPLETED', 'disposition': 'PROMOTED', 'publication_id': 'P-' + name,
                       'candidate_ids': ['C-' + name],
                       'authority': {'job_id': 'J-' + name, 'execution_id': 'E-' + name, 'contract_revision': 1},
                       'paths': []}
            parents.append({'job': job, 'execution': execution, 'intent': intent, 'journal': journal,
                            'task': {'job_id': 'J-' + name, 'execution_id': 'E-' + name},
                            'profile_contract_sha256': 'f' * 64})
        with self.assertRaisesRegex(ValueError, 'BATCH_SCOPE_REQUIRED'):
            freeze_batch_contract(parents, request_id='empty-diff-check',
                                  canonical_content={'app/A.py': 'a' * 64},
                                  canonical_heads={'app': {'head': 'f' * 40}})


if __name__ == '__main__':
    unittest.main(verbosity=2)
