import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import skeleton_policy as sp
from qa_policy_resolution import SKELETON_POLICY, resolution_contract
from worker import parse_qa_request
from job_queue import JobStore
from job_contract import JobRequest
from control_repository import ControlRepository
from test_0916_candidateless_policy import PolicyResolutionTests
from test_09005_hotfix import context
from queue_supervisor import QueueSupervisor
from test_0916_candidate_projection import CandidateProjectionTests


def signal():
    return {'required': True, 'qa_type': 'DB_CONTRACT', 'hold_scope': 'JOB',
        'reason': 'COURSE_CD unresolved', 'evidence': ['catalog'],
        'decision_independent_work_complete': True, 'skeleton_safe': True,
        'deferred_contracts': [{'decision': 'physical column/type/mapping',
            'candidates': 'none established', 'evidence': 'offline catalog',
            'areas': 'app/same.txt', 'verification_required': 'DB/E2E/integration'}]}


def result(execution):
    return {'status': 'SKELETON_READY', 'stage': 'DONE', 'success': False,
        'verification_status': 'PARTIAL', 'build_status': 'PASS', 'test_status': 'PASS',
        'review_status': 'REVIEW_PASS', 'failure_code': 'DEFERRED_CONTRACT',
        'failure_type': 'contract', 'failure_origin': 'USER_QA', 'fix_scope': 'NON_RETRYABLE',
        'task_id': 'TASK-SKELETON', 'materialized_execution': execution,
        'external_frozen_integrity': True, 'baseline_declaration_integrity': True,
        'qa_request': signal(), 'changed_files': [], 'changed_file_sha256': {},
        'worktree_disposition': 'NO_DELTA', 'gate_evidence': {
            g: {'status': 'PARTIAL' if g == 'VERIFICATION' else 'PASS', 'freshness': 'CURRENT'}
            for g in ('BUILD', 'TEST', 'REVIEW', 'VERIFICATION')}}


class SkeletonSignalTests(unittest.TestCase):
    def test_tester_parse_error_preserves_input_without_product_retry(self):
        import harness_temp
        from pathlib import Path
        from test_081_patch import Patch081Test, _Collector, _diff, SP0021
        from worker import WorkerResult
        from tester import _parse_tester_output
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/SP0021
            source.parent.mkdir(parents=True)
            source.write_text('safe skeleton', encoding='utf-8')
            helper = Patch081Test()
            worker = Mock()
            worker.execute.return_value = WorkerResult(True, 0, 'implemented', '', [], qa_request=signal())
            manager = helper._manager(root, _Collector([SP0021], _diff()), worker)
            self.addCleanup(manager.shutdown)
            task = helper._task(root)
            task.requirement += '\n' + sp.CONTRACT
            task.materialized_execution = {'current_requirement':task.requirement,
                'execution_id':'EXEC-TEST', 'execution_surface_hash':'surface', 'contract_revision':2}
            task.test_plan = {'mandatory':True, 'production_scope':[SP0021], 'scope_hash':'test-scope'}
            def tester(t, *args, **kwargs):
                parsed = _parse_tester_output('unreadable tester response')
                t.test_status, t.test_summary = parsed.status, parsed.summary
                t.official_tester_invoked = True
            with patch('manager.should_run_tester', return_value=(True, 'focused fixture')), \
                    patch('manager.run_tester', side_effect=tester):
                outcome = manager.run_with_retry(task)
            self.assertFalse(outcome.success)
            self.assertEqual('STRUCTURED_OUTPUT_INVALID', task.failure_code)
            self.assertEqual('TOOL_INFRA', task.failure_origin)
            self.assertEqual('AWAITING_QA', task.status)
            self.assertEqual(1, worker.execute.call_count)
            self.assertEqual(0, task.retry_count)
            self.assertEqual(0, manager.reviewer.calls)
            self.assertFalse(task.is_rolled_back)
            self.assertEqual('safe skeleton', source.read_text(encoding='utf-8'))
            self.assertEqual('ERROR', task.gate_evidence['TEST']['status'])
            self.assertNotIn('REVIEW', task.gate_evidence)
            self.assertNotIn('VERIFICATION', task.gate_evidence)
            self.assertEqual('FAIL', _parse_tester_output('TEST_RESULT: FAIL').status)

    def test_review_auxiliary_evidence_is_not_a_final_gate(self):
        from reviewer import Reviewer
        from task_state import TaskState
        task = TaskState(task_id='TASK-SKELETON-REVIEW', requirement=sp.CONTRACT)
        task.materialized_execution = {'current_requirement': sp.CONTRACT}
        task.eol_repairs = ['app/view.vue']
        task.qa_request = signal()
        task.gate_evidence = {'BUILD': {'status': 'PASS', 'freshness': 'STALE'}}
        reviewer = Reviewer()
        with patch('reviewer.load_review_rules', return_value='rules'), \
                patch.object(reviewer, '_remote_supplementary_context', return_value=('', False)):
            prompt = reviewer._build_review_prompt(sp.CONTRACT, ['app/view.vue'], '', task=task)
            self.assertIn('[Skeleton evidence authority]', prompt)
            self.assertIn('"freshness": "STALE"', prompt)
            self.assertIn('missing, stale or mismatched canonical evidence must still fail', prompt)
            self.assertIn('app/view.vue', prompt)
            self.assertIn('Manager Verification follows Review', prompt)
            self.assertIn('"decision_independent_work_complete": true', prompt)
            self.assertIn('does not introduce a field named completion or completion_kind', prompt)
            self.assertIn('missing or contradictory canonical QA data must still fail', prompt)
            task.materialized_execution = {}
            prompt = reviewer._build_review_prompt(sp.CONTRACT, [], '', task=task)
            self.assertNotIn('[Skeleton evidence authority]', prompt)

    def test_manager_pipeline_gets_fresh_gates_without_product_retry(self):
        import harness_temp
        from pathlib import Path
        from test_081_patch import Patch081Test, _Collector, _diff, SP0021
        from worker import WorkerResult
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/SP0021
            source.parent.mkdir(parents=True)
            source.write_text('safe skeleton', encoding='utf-8')
            helper = Patch081Test()
            worker = Mock()
            qa = signal()
            worker.execute.return_value = WorkerResult(True, 0, 'implemented', '', [], qa_request=qa)
            manager = helper._manager(root, _Collector([SP0021], _diff()), worker)
            self.addCleanup(manager.shutdown)
            task = helper._task(root)
            task.requirement += '\n' + sp.CONTRACT
            task.materialized_execution = {'current_requirement':task.requirement,
                'execution_id':'EXEC-TEST', 'execution_surface_hash':'surface', 'contract_revision':2}
            task.test_plan = {'mandatory':True, 'production_scope':[SP0021], 'scope_hash':'test-scope'}
            task.external_frozen_integrity = task.baseline_declaration_integrity = True
            def tester(t, *args, **kwargs):
                t.test_status = 'PASS'
                t.official_tester_invoked = True
                t.test_evidence = ['focused fixture evidence']
            with patch('manager.should_run_tester', return_value=(True, 'focused fixture')), \
                    patch('manager.run_tester', side_effect=tester):
                outcome = manager.run_with_retry(task)
            self.assertEqual('SKELETON_READY', task.status, task.failure_code)
            self.assertFalse(outcome.success)
            self.assertEqual(1, worker.execute.call_count)
            self.assertEqual(0, task.retry_count)
            self.assertEqual(1, manager.build_test_runner.calls)
            self.assertEqual(1, manager.reviewer.calls)
            self.assertEqual('PARTIAL', task.gate_evidence['VERIFICATION']['status'])
            self.assertEqual('PASS', task.gate_evidence['TEST']['status'])
            self.assertFalse(task.commit_eligible)

    def test_structured_signal_and_safety(self):
        qa = parse_qa_request('HARNESS_QA_REQUEST_JSON: ' + json.dumps(signal()))
        task = SimpleNamespace(requirement=sp.CONTRACT,
            materialized_execution={'current_requirement': sp.CONTRACT}, qa_request=qa)
        self.assertTrue(sp.deferred_signal(task))
        task.qa_request['skeleton_safe'] = False
        self.assertFalse(sp.deferred_signal(task))
        task.qa_request['skeleton_safe'] = True
        task.materialized_execution = {}
        self.assertFalse(sp.deferred_signal(task))

    def test_malformed_deferred_rejected(self):
        qa = signal()
        del qa['deferred_contracts'][0]['evidence']
        with self.assertRaisesRegex(ValueError, 'DEFERRED_CONTRACT_RECORD_INVALID'):
            parse_qa_request('HARNESS_QA_REQUEST_JSON: ' + json.dumps(qa))

    def test_missing_gate_or_fake_success_rejected(self):
        r = result({'execution_id': 'EXEC-FRESH', 'current_requirement': sp.CONTRACT})
        self.assertTrue(sp.valid_result(r))
        self.assertFalse(JobStore._is_strict_batch_result(r))
        for field, value in [('success', True), ('verification_status', 'VERIFIED'),
                             ('test_status', 'UNAVAILABLE'), ('review_status', 'REVIEW_FAIL')]:
            self.assertFalse(sp.valid_result(dict(r, **{field: value})))
        del r['gate_evidence']['BUILD']
        self.assertFalse(sp.valid_result(r))

    def test_skeleton_does_not_quarantine(self):
        r = result({'execution_id': 'EXEC-FRESH', 'current_requirement': sp.CONTRACT})
        candidates = Mock()
        supervisor = object.__new__(QueueSupervisor)
        self.assertEqual(r, supervisor._quarantine_machine_verified_result({}, r, candidates))
        candidates.quarantine_and_rollback.assert_not_called()


class SkeletonTransitionTests(PolicyResolutionTests):
    def skeleton_fixture(self):
        self.policy_fixture()
        self.contract = JobRequest.from_mapping(dict(self.parent['request'],
            requirement=self.parent['current_requirement']+'\n\n'+resolution_contract(SKELETON_POLICY)),
            allowed_modules=('app',))
        fresh = self.manager.validate_policy_fresh_input(self.parent, self.q, self.integrity, self.task)
        self.successor = self.store.enqueue_anchored_replacement(self.parent['job_id'], self.contract,
            expected_queue_revision=self.q['revision'], expected_job_revision=self.parent['revision'],
            request_id='skeleton-policy', reason='approved skeleton', execution_context=context(),
            provenance={'status':'VERIFIED'}, integrity_snapshot=self.integrity,
            fresh_input=fresh, policy_resolution=SKELETON_POLICY)[0]
        queue = self.store._read_queue()
        jobs = {i:self.store.get_job(i) for i in queue['order']}
        repo = ControlRepository(self.control/'skeleton.sqlite3', create=True)
        repo.bootstrap(queue, jobs, {i:{'path':i+'.json'} for i in jobs})
        self.store = JobStore(self.control, repository=repo)
        self.repo = repo
        import hashlib
        baseline = self.control/'cumulative'/'batch-baselines'/queue['active_baseline_id']/'manifest.json'
        self.execution = repo.materialize(self.successor,
            context={'profile_snapshot_sha256':'p','policy_snapshot_sha256':'p'},
            policy={'effective_policy_sha256':'p'}, baseline_hash=hashlib.sha256(baseline.read_bytes()).hexdigest(),
            execution_surface_hash='s', extra={'current_requirement':self.successor['current_requirement']})

    def test_fresh_policy_scope_and_history(self):
        self.skeleton_fixture()
        before = self.repo.read_job(self.parent['job_id'])
        self.assertIn(sp.CONTRACT, self.execution['current_requirement'])
        self.assertTrue(self.store._resolved_skeleton_scope(self.repo.read_queue(), before, self.successor,
            workspace=self.workspace, integrity_snapshot_provider=lambda q:self.integrity))
        self.assertEqual(before, self.repo.read_job(before['job_id']))
        self.integrity['external_frozen_integrity'] = False
        self.assertFalse(self.store._resolved_skeleton_scope(self.repo.read_queue(), before, self.successor,
            workspace=self.workspace, integrity_snapshot_provider=lambda q:self.integrity))

    def test_skeleton_result_is_not_final_success(self):
        self.skeleton_fixture()
        # Use canonical claim; fixture has no independent operator hold.
        q = self.repo.read_queue()
        q['operator_paused'] = False
        self.store._clear_gate(q, q.get('blocked_by_job_id',''))
        self.store._write_queue(q)
        claimed = self.store.claim_next('skeleton-owner', workspace=self.workspace,
            integrity_snapshot_provider=lambda q:self.integrity)
        self.assertEqual(self.successor['job_id'], claimed['job_id'])
        saved = self.store.record_result(claimed['job_id'], 'skeleton-owner', result(self.execution))
        self.assertEqual('SKELETON_READY', saved['status'])
        self.assertFalse(saved['last_result']['success'])
        self.assertFalse(self.repo.read_queue()['paused'])
        self.assertFalse(self.repo.read_queue().get('batch_final_commit_eligible'))
        self.assertFalse(self.store._is_verified_batch_success(saved))
        self.assertEqual(self.parent, self.repo.read_job(self.parent['job_id']))

    def test_dependency_is_skeleton_scoped(self):
        self.skeleton_fixture()
        dependency = dict(self.successor, status='SKELETON_READY', last_result=result(self.execution))
        consumer = {'dependency_semantics':'EXPLICIT', 'depends_on':[dependency['job_id']],
                    'current_requirement':sp.CONTRACT}
        with patch.object(self.store, '_read_job', return_value=dependency):
            self.assertTrue(self.store._explicit_dependency_decision(consumer)['eligible'])
            consumer['current_requirement'] = 'final implementation'
            self.assertFalse(self.store._explicit_dependency_decision(consumer)['eligible'])
            dependency['last_result']['test_status'] = 'FAIL'
            consumer['current_requirement'] = sp.CONTRACT
            self.assertFalse(self.store._explicit_dependency_decision(consumer)['eligible'])

    def test_policy_chain_preserves_all_ancestors(self):
        self.skeleton_fixture()
        first = self.repo.read_job(self.successor['job_id'])
        # Canonical terminal result represented in an isolated fixture, no live rows.
        old_revision = first['revision']
        first.update(status='AWAITING_QA', revision=old_revision+1,
            current_claim_task_id='TASK-SECOND', task_ids=['TASK-SECOND'],
            active_baseline_id=self.q['active_baseline_id'], last_result={'failure_code':'CANDIDATE_DELTA_EMPTY'})
        self.repo.write_job(first, expected_revision=old_revision)
        q = self.repo.read_queue()
        q.update(blocked_by_job_id=first['job_id'], gate_reason='AWAITING_QA')
        self.store._write_queue(q)
        q = self.repo.read_queue()
        fresh = dict(first['fresh_input'], parent_job_id=first['job_id'], parent_revision=first['revision'],
                     source_task_id='TASK-SECOND')
        contract = JobRequest.from_mapping(dict(first['request'],
            requirement=first['current_requirement']+'\n\n'+resolution_contract(SKELETON_POLICY)), allowed_modules=('app',))
        second = self.store.enqueue_anchored_replacement(first['job_id'], contract,
            expected_queue_revision=q['revision'], expected_job_revision=first['revision'],
            request_id='second-policy', reason='approved scoped continuation', execution_context=context(),
            provenance={'status':'VERIFIED'}, integrity_snapshot=self.integrity,
            fresh_input=fresh, policy_resolution=SKELETON_POLICY)[0]
        self.repo.materialize(second, context=self.execution['execution_context'],
            policy=self.execution['effective_policy'], baseline_hash=self.execution['baseline_hash'],
            execution_surface_hash='s', extra={'current_requirement':second['current_requirement']})
        before = [self.repo.read_job(i) for i in (self.parent['job_id'], first['job_id'])]
        self.assertEqual('',self.store._quarantined_scope_conflict(self.repo.read_queue(),second,
            workspace=self.workspace, integrity_snapshot_provider=lambda q:self.integrity))
        self.assertEqual(before,[self.repo.read_job(i) for i in (self.parent['job_id'],first['job_id'])])


class SkeletonProjectionTests(CandidateProjectionTests):
    def test_checked_skeleton_tree_preserves_historical_artifact_guard(self):
        import hashlib
        self.success()
        f = self.f
        historical = f.repo.read_job(f.other)
        owners = f.repo.preserved_candidate_records(f.target)
        newer = copy.deepcopy(historical)
        newer.update(job_id='JOB-20990101-000000-abcdef01', revision=1,
                     status='SKELETON_READY', updated_at='2099-01-01')
        r = result({'execution_id':'EXEC-SKELETON', 'current_requirement':sp.CONTRACT})
        r['changed_file_sha256'] = {}
        for name in f.hashes:
            (f.product/name).write_bytes(b'checked but unfinished skeleton')
            r['changed_file_sha256'][name] = hashlib.sha256((f.product/name).read_bytes()).hexdigest()
        r['changed_files'] = list(f.hashes)
        newer['last_result'] = r
        f.repo.write_job(newer, expected_revision=0)
        self.assertTrue(f.repo.candidate_ownership(f.product)['valid'])
        self.assertEqual(historical, f.repo.read_job(f.other))
        self.assertEqual(owners, f.repo.preserved_candidate_records(f.target))
        (f.product/next(iter(f.hashes))).write_bytes(b'foreign dirty')
        self.assertFalse(f.repo.candidate_ownership(f.product)['valid'])
