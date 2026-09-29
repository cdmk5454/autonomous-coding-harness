"""Real execution regressions: policy constraints, mandatory Tester and bound gates."""
from pathlib import Path
import json
import unittest
from unittest.mock import patch

import harness_temp
from control_repository import ControlRepository, digest
from control_migration import rehearse
from job_queue import JobStore
from test_job_control import request, valid_enqueue
from task_state import TaskState
from materialization_policy import resolve_constraints, freeze_routing, PolicyConflict
from policy_catalog import resolve_effective_policy
from test_plan import derive_test_plan
from tester import should_run_tester, run_tester, _apply_to_state, TesterResult
from gate_evidence import identity, bind, validate
from sleep_disposition import disposition
from manager import Manager
from reviewer import ReviewResult, REVIEW_PASS
from test_gate_scenarios import FakeWorker, FakeCollector, FakeRunner, FakeReviewer, _wr_ok, _git, _build


class PhaseCTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = patch.dict('os.environ', {'KKM_TG_ENABLED': '0', 'KKM_TEST_STRICT': '0'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_required_preferred_max_exact_and_safety_minimum(self):
        upgraded = resolve_constraints({'required':'HIGH', 'preferred':'LOW'})
        self.assertEqual(('HIGH','AUTO_UPGRADE'), (upgraded['tier'],upgraded['action']))
        for constraints in ({'required':'HIGH','max':'MID'}, {'exact':'gpt-5.6-luna'}):
            with self.assertRaises(PolicyConflict):
                resolve_constraints(constraints, safety_minimum='HIGH')
        exact = resolve_constraints({'required':'MID','exact':'gpt-5.6-sol'})
        self.assertEqual('gpt-5.6-sol', exact['model'])
        constrained = request(requirement='authorization security boundary', execution_policy={'max':'LOW'})
        with self.assertRaises(PolicyConflict):
            freeze_routing(constrained, resolve_effective_policy(profile_id='e4u', policy_snapshot_sha256='policy'))

    def test_derived_java_security_envelope_is_stable_and_separate_from_production(self):
        production='suc/src/main/java/suc/sp/service/SsucSp0020Svc.java'
        related='suc/src/test/java/suc/sp/service/SsucSp0020SecurityTest.java'
        path=self.root/related
        path.parent.mkdir(parents=True)
        path.write_text('class Test {}')
        contract=request(target_resources=[production],test_plan={'required_tests':[related]})
        first=derive_test_plan(contract,self.root)
        self.assertTrue(first['mandatory'])
        self.assertEqual([production],first['production_scope'])
        self.assertIn(related,first['required_tests'])
        self.assertEqual(first,derive_test_plan(contract,self.root))
        task=TaskState(task_id='TASK-PLAN',requirement='fix',test_plan=first)
        self.assertEqual((True,'MANDATORY_TEST_PLAN'),should_run_tester(task))

    def task(self):
        (self.root/'suc').mkdir(exist_ok=True)
        (self.root/'suc/A.py').write_text('value=2\n')
        plan=derive_test_plan(request(target_resources=['suc/A.py']),self.root)
        return TaskState(task_id='TASK-BOUND', requirement='bounded fix', working_dir=str(self.root),
            task_mode='MODIFICATION', target_module=['suc'],target_resources=['suc/A.py'],
            test_plan=plan,test_scope_hash=plan['scope_hash'],changed_files=['suc/A.py'],git_diff='-value=1\n+value=2\n',
            materialized_execution={'contract_revision':1,'execution_surface_hash':'frozen-surface'})

    def test_candidate_acceptance_and_surface_changes_invalidate_only_bound_gates(self):
        task=self.task()
        original=identity(task,self.root)
        for gate in ('BUILD','TEST','REVIEW','VERIFICATION'):
            bind(task,gate,original,'PASS')
        self.assertEqual([],validate(task,original,require_all=True))
        (self.root/'suc/A.py').write_text('value=3\n')
        stale=validate(task,identity(task,self.root))
        self.assertIn('BUILD:STALE',stale)
        self.assertIn('TEST:STALE',stale)
        task.requirement='changed acceptance'
        self.assertIn('REVIEW:STALE',validate(task,identity(task,self.root)))
        task.materialized_execution['execution_surface_hash']='other-surface'
        self.assertIn('TEST:STALE',validate(task,identity(task,self.root)))

    def pipeline(self, task, tester_status='PASS', mutate_review=False):
        manager=Manager(str(self.root),model='',max_retry=1,max_workers=1,task_root=self.root/'.tasks')
        self.addCleanup(manager.shutdown)
        manager._get_worker=lambda task:(FakeWorker(_wr_ok()),'codex')
        manager.collector=FakeCollector(_git(diff=task.git_diff,files=['suc/A.py']))
        manager.build_test_runner=FakeRunner(_build('PASS'))
        reviewer=FakeReviewer(ReviewResult(passed=True,status=REVIEW_PASS))
        if mutate_review:
            original=reviewer.review
            def review(**kwargs):
                (self.root/'suc/A.py').write_text('unexpected=3\n')
                return original(**kwargs)
            reviewer.review=review
        manager.reviewer=reviewer
        def official(task,*args,**kwargs):
            task.official_tester_invoked=True
            return _apply_to_state(task,TesterResult(status=tester_status,evidence=['official fixture execution']))
        with patch('manager.run_tester',side_effect=official) as tester:
            result=manager._run_pipeline(task)
            self.assertEqual(1,tester.call_count)
        return result

    def test_mandatory_official_tester_runs_without_brief_and_produces_bound_evidence(self):
        task=self.task()
        result=self.pipeline(task)
        self.assertTrue(result.success, task.failure_reason)
        self.assertTrue(task.official_tester_invoked)
        self.assertEqual([],validate(task,identity(task,self.root),require_all=True))

    def test_mandatory_tester_skipped_is_strict_failure_without_strict_env(self):
        task=self.task()
        self.assertFalse(self.pipeline(task,tester_status='SKIPPED').success)
        self.assertEqual('TEST',task.failure_stage)
        self.assertNotEqual('VERIFIED',task.verification_status)

    def test_candidate_changed_by_review_cannot_reuse_prior_build(self):
        task=self.task()
        self.assertFalse(self.pipeline(task,mutate_review=True).success)
        self.assertEqual('GATE_EVIDENCE_STALE',task.failure_code)

    def test_historical_qa_does_not_globally_block_independent_sleep_job(self):
        legacy=JobStore(self.root/'legacy')
        jobs,_=valid_enqueue(legacy,'sleep-fixture',[
            request(client_job_id='ROOT',target_resources=['suc/A.py'],depends_on=[]),
            request(client_job_id='INDEPENDENT',target_resources=['suc/B.py'],depends_on=[])])
        root=legacy.get_job(jobs[0]['job_id'])
        root.update(status='AWAITING_QA',last_result={'failure_code':'PROFILE_DRIFT','worktree_disposition':'PRESERVED'})
        legacy._write_job(root)
        queue=legacy._read_queue()
        queue.update(execution_mode='AUTONOMOUS',paused=True,gate_reason='AWAITING_QA',blocked_by_job_id=root['job_id'])
        legacy._write_queue(queue)
        db=self.root/'sleep.sqlite3'
        self.assertTrue(rehearse(legacy.root,db)['pass'])
        repo=ControlRepository(db,legacy_root=legacy.root)
        store=JobStore(legacy.root,repository=repo)
        frozen=repo.read_job(root['job_id'])
        claimed=store.claim_next('sleep-owner')
        self.assertEqual(jobs[1]['job_id'],claimed['job_id'])
        self.assertEqual(frozen,repo.read_job(root['job_id']))
        self.assertEqual('QUARANTINE',disposition(root))
        self.assertEqual('GLOBAL_STOP',disposition(root,{'unexpected_runtime_dirty_files':['unknown.py']}))


if __name__=='__main__':
    unittest.main()
