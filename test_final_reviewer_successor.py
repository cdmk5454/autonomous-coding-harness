"""Real host TEST receipts and schema6 Reviewer successor regression fixtures."""
import copy
import hashlib
import gc
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import bounded_execution
import gate_evidence
import harness_temp
import reviewer_successor
import test_checkpoint
from control_repository import ControlRepository, RepositoryError, canonical, digest
from task_state import TaskState
from tester import TesterResult, _apply_to_state, execute_test_receipts
from tester_lifecycle import run_attempts


class ReviewerSuccessorTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(gc.collect)
        self.root = Path(self.temp.name)
        self.workspace = self.root / 'candidate'
        self.workspace.mkdir()
        (self.workspace / 'app.py').write_text('value = 7\n', encoding='utf8')
        (self.workspace / 'test_app.py').write_text(
            'from pathlib import Path\nfrom app import value\n'
            'assert value == 7\n'
            'p=Path(' + repr(str(self.root/'invocations.txt')) + ')\n'
            'p.write_text(str(int(p.read_text())+1) if p.exists() else "1")\n', encoding='utf8')
        self.path = self.root / 'control.sqlite3'
        self.repo = ControlRepository(self.path, create=True)
        bounded_execution.migrate_disposable(self.path, allocation_root=self.root,
                                             backup_path=self.root / 'schema5.sqlite3')
        gc.collect()
        profile = self.root / 'fixture'
        (profile/'ui_map').mkdir(parents=True)
        (profile/'rules.md').write_text('# Fixture\n')
        (profile/'analysis.md').write_text('# Fixture\n')
        (profile/'project.json').write_text(json.dumps(dict(schema_version=1, id='fixture',
            display_name='Fixture', workspace_roots=[str(self.workspace)],
            context=dict(project_rules='rules.md', analysis='analysis.md', ui_map='ui_map'),
            modules=[dict(name='app', path='.', kind='backend', protected=False)],
            rules=dict(always=[], backend=[], frontend=[], sql=[], architecture=[], egov=[]),
            area_patterns=dict(backend=[], frontend=[], sql=[], architecture=[]),
            protected_paths=[], local_dev_config_excludes=[])))
        self.job = dict(job_id='J-REVIEW', logical_job_id='L-REVIEW', client_job_id='CLIENT',
                        status='RUNNING', revision=0, contract_revision=1, owner_id='',
                        request=dict(requirement='Verify seven', worker='codex', model='fixture',
                                     target_modules=['app'], target_resources=['app.py']), depends_on=[])
        self.repo.bootstrap(dict(revision=0, generation=1, order=[self.job['job_id']]),
                            {self.job['job_id']: self.job},
                            {self.job['job_id']: dict(path='jobs/J-REVIEW.json', sha256='fixture')})
        command = '"' + Path(sys.executable).as_posix() + '" -B test_app.py'
        plan = dict(mandatory=True, production_scope=['app.py'], required_tests=['test_app.py'],
                    derived_tests=[], command=command, scope_hash='fixture-test-scope')
        frozen = self.repo.materialize(self.job,
            context=dict(profile_snapshot_sha256='profile', policy_snapshot_sha256='policy'),
            policy=dict(effective_policy_sha256='policy', resolved_worker=dict(worker='codex')),
            baseline_hash='base', execution_surface_hash='surface',
            extra=dict(current_requirement='Verify seven', test_plan=copy.deepcopy(plan)))
        self.task = TaskState(task_id='TASK-20260922-120000', requirement='Verify seven',
                              working_dir=str(self.workspace), worker='codex')
        t = self.task
        t.job_id = self.job['job_id']; t.execution_id = frozen['execution_id']
        t.materialized_execution = frozen; t.control_repository_path = str(self.path)
        t.candidate_id = 'CAND-FIXTURE'; t.test_plan = plan; t.test_scope_hash = plan['scope_hash']
        t.profile_dir = str(profile); t.target_module = ['app']; t.active_baseline_id = 'base'
        t.tester_attempt_id = 'TESTER-FIXTURE'; t.changed_files = ['app.py']
        log = self.root/'build.log'
        log.write_bytes(b'fixture structured build acceptance\n')
        receipt = dict(execution_id='BUILD-FIXTURE', product_execution_id=t.execution_id,
            candidate_id=t.candidate_id, job_id=t.job_id, task_id=t.task_id, module='app',
            cwd=str(self.workspace), argv=['build'], command_source='profile',
            started_at='start', finished_at='finish', exit_code=0, status='PASS',
            log_path=str(log), log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
            baseline_id='base', checkpoint_id='', restored_checkpoint_id='', restored_evidence_epoch='')
        t.build_evidence = dict(success=True, status='PASS', details=dict(source_sha256={
            'app.py': hashlib.sha256((self.workspace/'app.py').read_bytes()).hexdigest()}, executions=[receipt]))
        gate_evidence.bind(t, 'BUILD', gate_evidence.identity(t, self.workspace), 'PASS')
        self.tasks = self.root / 'tasks'

    def real_test(self):
        receipts = execute_test_receipts(self.task, str(self.workspace), timeout=20)
        self.assertEqual(['PASS'], [r.status for r in receipts])
        self.assertEqual([0], [r.exit_code for r in receipts])
        self.task.official_tester_invoked = True
        self.task.official_tester_execution = {'boundary': 'canonical-host', 'status': 'PASS'}
        _apply_to_state(self.task, TesterResult('PASS', 'actual Python assertions passed',
                                               ['host test_app.py'], receipts=receipts))

    def checkpoint(self):
        self.real_test()
        test_checkpoint.persist(self.task, self.workspace, self.tasks)
        return test_checkpoint.accepted(self.task, self.workspace)

    def admit(self, checkpoint=None, revision=0):
        return self.repo.admit_reviewer_successor(self.task, expected_revision=revision,
            checkpoint=checkpoint if checkpoint is not None else self.checkpoint())

    def execution_row(self, execution_id=None):
        with self.repo.hold():
            return tuple(self.repo._connection.execute('SELECT * FROM executions WHERE execution_id=?',
                (execution_id or self.task.execution_id,)).fetchone())

    def terminal_predecessor(self):
        with self.repo.hold():
            self.repo._connection.execute("UPDATE executions SET phase='TERMINAL', result=? WHERE execution_id=?",
                (canonical({'preserved': 'preexisting-terminal-result'}), self.task.execution_id))
            self.repo._set_writer_authority_locked(self.task.execution_id, 'RELEASED', 'FIXTURE_TERMINAL')
        return self.execution_row()

    def test_checkpoint_is_canonical_before_reviewer_and_task_save_crash_recovers_without_tester(self):
        self.task.save(self.tasks)  # Stale projection predates Tester execution.
        self.real_test()
        with patch.object(self.task, 'save', side_effect=RuntimeError('crash after canonical commit')):
            with self.assertRaisesRegex(RuntimeError, 'canonical commit'):
                test_checkpoint.persist(self.task, self.workspace, self.tasks)
        restored = TaskState.load(self.task.task_id, self.tasks)
        self.assertNotEqual('PASS', restored.test_status)
        test_checkpoint.configure(restored, self.workspace, self.tasks)
        invoke = Mock(side_effect=AssertionError('Tester must not rerun'))
        self.assertEqual('PASS', run_attempts(restored, invoke))
        invoke.assert_not_called()
        self.assertEqual('1', (self.root/'invocations.txt').read_text())
        self.assertEqual(self.task.test_receipts, restored.test_receipts)
        self.assertTrue(restored.test_checkpoint)
        self.assertFalse(restored.reviewer_execution)

    def test_wrong_receipt_identity_command_or_missing_receipts_cannot_persist(self):
        self.real_test()
        valid = copy.deepcopy(self.task.test_receipts)
        for key, value in [('candidate_id', 'FOREIGN'), ('execution_id', 'FOREIGN'),
                           ('tester_attempt_id', 'FOREIGN'), ('command', 'python unrelated.py'),
                           ('workspace', str(self.root)), ('exit_code', 1)]:
            with self.subTest(key=key):
                self.task.test_receipts = copy.deepcopy(valid)
                self.task.test_receipts[0][key] = value
                with self.assertRaises(RepositoryError):
                    test_checkpoint.persist(self.task, self.workspace, self.tasks)
        self.task.test_receipts = []
        with self.assertRaises(RepositoryError):
            test_checkpoint.persist(self.task, self.workspace, self.tasks)
        self.assertIsNone(test_checkpoint.accepted(self.task, self.workspace))

    def test_stale_or_missing_build_gate_cannot_checkpoint_test_pass(self):
        self.real_test()
        original = copy.deepcopy(self.task.gate_evidence)
        for missing in (False, True):
            with self.subTest(missing=missing):
                self.task.gate_evidence = copy.deepcopy(original)
                if missing:
                    self.task.gate_evidence.pop('BUILD')
                else:
                    self.task.gate_evidence['BUILD']['candidate_hash'] = 'stale'
                with self.assertRaisesRegex(RepositoryError, 'TEST_CHECKPOINT_BUILD_BINDING_INVALID'):
                    test_checkpoint.persist(self.task, self.workspace, self.tasks)
                self.assertEqual({}, self.task.test_checkpoint)

    def test_lifecycle_commits_checkpoint_before_finishing_tester_attempt(self):
        with self.repo.hold():
            bounded_execution.claim_slot(self.repo, self.task.job_id, 'FIXTURE-OWNER')
        self.task.effective_policy = {'resolved_tester': {'runtime': 'droid', 'model': 'fixture'}}
        self.task._record_execution_attempt = lambda role, aid, model: self.repo.start_attempt(
            self.task.materialized_execution, attempt_id=aid, role=role, model=model)
        test_checkpoint.configure(self.task, self.workspace, self.tasks)
        finish = ControlRepository.finish_attempt
        observed = []
        def checked_finish(repo, aid, **kwargs):
            proof = test_checkpoint.accepted(self.task, self.workspace)
            self.assertIsNotNone(proof)
            self.assertEqual(aid, proof['detail']['tester_attempt_id'])
            self.assertEqual('PASS', TaskState.load(self.task.task_id, self.tasks).test_status)
            observed.append(aid)
            return finish(repo, aid, **kwargs)
        with patch.object(ControlRepository, 'finish_attempt', new=checked_finish):
            self.assertEqual('PASS', run_attempts(self.task, self.real_test))
        self.assertEqual([self.task.tester_attempt_id], observed)
        self.assertFalse(self.task.reviewer_execution)

    def test_tester_source_mutation_prevents_durable_pass(self):
        test_checkpoint.configure(self.task, self.workspace, self.tasks)
        def mutate_after_test():
            self.real_test()
            (self.workspace/'unlisted.py').write_text('unexpected = True\n')
        self.assertEqual('ERROR', run_attempts(self.task, mutate_after_test))
        self.assertEqual('TEST_CHECKPOINT_SOURCE_CHANGED', self.task.test_summary)
        self.assertIsNone(test_checkpoint.accepted(self.task, self.workspace))

    def test_changed_candidate_bytes_invalidate_checkpoint(self):
        proof = self.checkpoint()
        (self.workspace/'app.py').write_text('value = 8\n')
        with self.assertRaisesRegex(RepositoryError, 'TEST_CHECKPOINT_STALE'):
            self.admit(proof)
        self.assertEqual([], bounded_execution.slots(self.repo))

    def test_changed_candidate_identity_and_frozen_command_are_not_accepted(self):
        self.checkpoint()
        old_candidate = self.task.candidate_id
        self.task.candidate_id = 'FOREIGN'
        self.assertIsNone(test_checkpoint.accepted(self.task, self.workspace))
        self.task.candidate_id = old_candidate
        self.task.test_plan['command'] += ' changed'
        with self.assertRaisesRegex(RepositoryError, 'CONTRACT_STALE'):
            test_checkpoint.accepted(self.task, self.workspace)

    def test_admission_requires_checkpoint_and_current_cas(self):
        with self.assertRaisesRegex(RepositoryError, 'CHECKPOINT_REQUIRED'):
            self.repo.admit_reviewer_successor(self.task, expected_revision=0, checkpoint={})
        proof = self.checkpoint()
        before = self.execution_row()
        with self.assertRaisesRegex(RepositoryError, 'CONTROL_REVISION_CONFLICT'):
            self.admit(proof, revision=1)
        self.assertEqual(before, self.execution_row())
        self.assertEqual([], bounded_execution.slots(self.repo))

    def test_terminal_predecessor_immutable_successor_distinct_bound_and_read_only(self):
        proof = self.checkpoint()
        before = self.terminal_predecessor()
        successor = self.admit(proof)
        self.assertEqual(before, self.execution_row())
        self.assertNotEqual(self.task.execution_id, successor['execution_id'])
        self.assertNotEqual(self.task.materialized_execution['materialization_id'], successor['materialization_id'])
        binding = successor['review_binding']
        self.assertEqual(self.task.execution_id, binding['predecessor_execution_id'])
        self.assertEqual(self.task.candidate_id, binding['candidate_id'])
        self.assertEqual(proof['validation_id'], binding['test_checkpoint_id'])
        self.assertEqual(digest(proof), binding['test_checkpoint_sha256'])
        self.assertEqual(proof['detail']['identity']['candidate_hash'], binding['candidate_hash'])
        self.assertEqual('RELEASED', self.repo.writer_authority(successor['execution_id'])['state'])
        with self.assertRaisesRegex(RepositoryError, 'REVIEWER_SUCCESSOR_ROLE_REQUIRED'):
            self.repo.start_attempt(successor, attempt_id='ILLEGAL-WRITER', role='WORKER', model='fixture')
        self.assertEqual(before, self.execution_row())

    def test_reservation_idempotent_dispatch_single_use_and_slot_cleaned(self):
        proof = self.checkpoint()
        before = self.terminal_predecessor()
        successor = self.admit(proof)
        self.assertEqual(successor, self.admit(proof))
        self.assertEqual(1, len(bounded_execution.slots(self.repo)))
        self.task.reviewer_execution = successor
        reviewer_successor.start_attempt(self.task, 'fixture')
        with self.assertRaisesRegex(RepositoryError, 'DISPATCH_ALREADY_REGISTERED'):
            reviewer_successor.start_attempt(self.task, 'fixture')
        with self.assertRaisesRegex(RepositoryError, 'ATTEMPT_NOT_TERMINAL'):
            self.repo.finish_reviewer_successor(successor['execution_id'], result={'review_status': 'REVIEW_PASS'})
        reviewer_successor.complete(self.task, 'REVIEW_PASS')
        self.assertEqual([], bounded_execution.slots(self.repo))
        self.assertEqual(before, self.execution_row())
        terminal_row = self.execution_row(successor['execution_id'])
        reviewer_successor.complete(self.task, 'REVIEW_PASS')
        self.assertEqual(terminal_row, self.execution_row(successor['execution_id']))
        with self.assertRaisesRegex(RepositoryError, 'SUCCESSOR_TERMINAL'):
            self.admit(proof)
        with self.assertRaisesRegex(RepositoryError, 'TERMINAL_EXECUTION_IMMUTABLE'):
            self.repo.finish_reviewer_successor(successor['execution_id'], result={'review_status': 'OTHER'})

    def test_reviewer_pass_alone_does_not_satisfy_verification_or_publish(self):
        self.task.reviewer_execution = self.admit()
        reviewer_successor.start_attempt(self.task, 'fixture')
        reviewer_successor.complete(self.task, 'REVIEW_PASS')
        stale = gate_evidence.validate(self.task, gate_evidence.identity(self.task, self.workspace), require_all=True)
        self.assertIn('VERIFICATION:MISSING', stale)
        self.assertEqual('RUNNING', self.repo.read_job(self.task.job_id)['status'])
        self.assertEqual('value = 7\n', (self.workspace/'app.py').read_text())
        self.assertNotIn('VERIFICATION', self.task.gate_evidence)

    def start_original_tester(self):
        with self.repo.hold():
            bounded_execution.claim_slot(self.repo, self.task.job_id, 'FIXTURE-OWNER')
        self.repo.start_attempt(self.task.materialized_execution,
            attempt_id=self.task.tester_attempt_id, role='TESTER', model='fixture')

    def test_crash_after_checkpoint_reconciles_registered_tester_before_admission(self):
        self.start_original_tester()
        self.task.save(self.tasks)
        proof = self.checkpoint()
        self.assertEqual('RUNNING', self.repo.attempt_outcome(self.task.tester_attempt_id)['status'])
        restored = TaskState.load(self.task.task_id, self.tasks)
        restored.test_status = 'ERROR'
        test_checkpoint.configure(restored, self.workspace, self.tasks)
        tester = Mock(side_effect=AssertionError('must reuse TEST'))
        self.assertEqual('PASS', run_attempts(restored, tester))
        tester.assert_not_called()
        self.assertEqual('SUCCEEDED', self.repo.attempt_outcome(restored.tester_attempt_id)['status'])
        successor = self.repo.admit_reviewer_successor(restored, expected_revision=0, checkpoint=proof)
        self.assertNotEqual(restored.execution_id, successor['execution_id'])
        self.assertEqual('1', (self.root/'invocations.txt').read_text())

    def test_checkpoint_cannot_reconcile_a_registered_active_tester_runtime(self):
        self.start_original_tester()
        self.repo.record_runtime_process(self.task.tester_attempt_id, runtime='fixture',
                                         pid=987654, process_identity='fixture-not-a-real-process')
        self.checkpoint()
        with self.assertRaisesRegex(RepositoryError, 'RUNTIME_NOT_QUIESCED'):
            test_checkpoint.accepted(self.task, self.workspace, restore=True)
        self.assertEqual('RUNNING', self.repo.attempt_outcome(self.task.tester_attempt_id)['status'])
        self.assertEqual('1', (self.root/'invocations.txt').read_text())

    def test_explicit_infra_recovery_gets_new_reviewer_without_rerunning_test(self):
        proof = self.checkpoint()
        self.task.reviewer_execution = self.admit(proof)
        old_id = self.task.reviewer_execution['execution_id']
        reviewer_successor.start_attempt(self.task, 'fixture')
        reviewer_successor.complete(self.task, 'REVIEW_ERROR')
        old_row = self.execution_row(old_id)
        with self.assertRaisesRegex(RepositoryError, 'SUCCESSOR_TERMINAL'):
            self.admit(proof)
        self.task.reviewer_recovery_request_id = 'EXPLICIT-REVIEW-RECOVERY-1'
        successor = self.admit(proof)
        self.assertNotEqual(old_id, successor['execution_id'])
        self.assertEqual(old_id, successor['review_binding']['retry_of_reviewer_execution_id'])
        self.assertEqual(old_row, self.execution_row(old_id))
        self.assertEqual(successor, self.admit(proof))
        self.task.reviewer_execution = successor
        reviewer_successor.start_attempt(self.task, 'fixture')
        reviewer_successor.complete(self.task, 'REVIEW_PASS')
        self.assertEqual([], bounded_execution.slots(self.repo))
        self.assertEqual('1', (self.root/'invocations.txt').read_text())

    def test_completed_review_resumes_manager_verification_without_provider(self):
        from manager import Manager
        from reviewer import ReviewResult
        self.task.git_diff = 'diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-value = 6\n+value = 7\n'
        self.task.build = {'status': 'PASS', 'success': True}
        self.task.reviewer_execution = self.admit()
        reviewer_successor.start_attempt(self.task, 'fixture')
        verdict = ReviewResult(True, 'reviewed seven', reviewed_files=['app.py'])
        reviewer_successor.complete(self.task, 'REVIEW_PASS', review_result=verdict)
        self.task.save(self.tasks)
        restored = TaskState.load(self.task.task_id, self.tasks)
        manager = object.__new__(Manager)
        manager.working_dir = str(self.workspace); manager.task_root = self.tasks
        manager.max_retry = 1; manager._initial_baseline = object()
        manager._prepare_reviewer_effort = Mock()
        manager._browser_qa_gate = Mock(return_value={'acceptable': True})
        manager.collector = Mock()
        manager.collector.assess_commit_safety.return_value = (True, [], {'app.py': 'fixture'})
        manager.reviewer = Mock()
        manager.reviewer.review.side_effect = AssertionError('provider must not run twice')
        result = manager.run_review_only(restored)
        self.assertTrue(result.success, result.message)
        manager.reviewer.review.assert_not_called()
        self.assertEqual('VERIFIED', restored.verification_status)
        self.assertEqual('PASS', restored.gate_evidence['VERIFICATION']['status'])
        self.assertEqual('1', (self.root/'invocations.txt').read_text())

    def test_configuration_failure_finishes_successor_and_releases_its_slot(self):
        from manager import Manager
        self.task.reviewer_execution = self.admit()
        reviewer_successor.start_attempt(self.task, 'fixture')
        manager = object.__new__(Manager)
        manager.configuration_guard = Mock(side_effect=RuntimeError('fixture drift'))
        result = manager._configuration_failure(self.task)
        self.assertFalse(result.success)
        self.assertEqual('PROFILE_DRIFT', self.task.failure_code)
        self.assertEqual('FAILED', self.repo.attempt_outcome(self.task.reviewer_attempt_id)['status'])
        self.assertIsNone(self.repo.active_execution(self.task.job_id))
        self.assertEqual([], bounded_execution.slots(self.repo))

    def test_revision_change_after_reservation_blocks_dispatch(self):
        self.task.reviewer_execution = self.admit()
        updated = self.repo.read_job(self.task.job_id)
        updated['revision'] += 1
        self.repo.write_job(updated, expected_revision=0)
        with self.assertRaisesRegex(RepositoryError, 'CONTROL_REVISION_CONFLICT'):
            reviewer_successor.start_attempt(self.task, 'fixture')
        self.assertIsNone(self.repo.attempt_outcome(self.task.reviewer_execution['execution_id']+'-ATTEMPT'))

    def test_approved_scope_change_retains_build_without_poisoning_fresh_review(self):
        from testplan_amendment import candidate_inventory, validate_overlay
        t = self.task
        old_hash = t.gate_evidence['BUILD']['candidate_hash']
        (self.workspace/'test_extra.py').write_text('assert True\n')
        t.test_plan = copy.deepcopy(t.test_plan)
        t.test_plan.update(required_tests=['test_extra.py'], command='python -B test_extra.py', scope_hash='amended')
        preserve = ['app.py', 'test_app.py', 'test_extra.py']
        t.test_plan_amendment = dict(execution_id=t.execution_id, candidate_id=t.candidate_id,
            candidate_workspace=str(self.workspace.resolve()), contract_revision=2,
            test_plan=copy.deepcopy(t.test_plan), inventory_preserve=preserve,
            candidate_inventory=candidate_inventory(t, self.workspace, preserve),
            source_hashes=copy.deepcopy(t.build_evidence['details']['source_sha256']),
            build_evidence_sha256=digest(t.build_evidence),
            original_build_gate_sha256=digest(t.gate_evidence['BUILD']))
        validate_overlay(t, t.test_plan_amendment)
        current = gate_evidence.identity(t, self.workspace)
        self.assertNotEqual(old_hash, current['candidate_hash'])
        for gate in ('TEST', 'REVIEW', 'VERIFICATION'):
            gate_evidence.bind(t, gate, current, 'PASS')
        self.assertEqual([], gate_evidence.validate(t, current, require_all=True))


if __name__ == '__main__':
    unittest.main()
