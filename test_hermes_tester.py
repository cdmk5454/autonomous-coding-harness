import unittest,json
from unittest.mock import Mock,patch
from types import SimpleNamespace as NS
from dataclasses import replace
from tester import TesterResult,_apply_to_state,run_tester
from tester_lifecycle import run_attempts,fresh_worker_attempt
from hermes_runtime_adapter import HermesRuntimeAdapter,HermesError
from hermes_execution_factory import HermesExecutionConfig,HermesExecutionFactory

class TesterLifecycleTests(unittest.TestCase):
    def task(self):
        return NS(task_id='T',worker='hermes',control_repository_path='disposable',
          _record_execution_attempt=Mock(),effective_policy={'resolved_tester':{'runtime':'hermes','model':'zai/model','technical_retries':1}},
          tester_attempt_history=[],test_status='',test_summary='',test_evidence=[],test_frontend='',test_db='',
          attempt_id='worker1',hermes_model='zai/model',product_semantic_retry_count=0,
          failure_type='test',fix_scope='RETRYABLE')
    def run_statuses(self,statuses,exception=None):
        t=self.task();r=Mock();seq=iter(statuses)
        def invoke():
            if exception:raise exception
            t.official_tester_invoked=True
            return _apply_to_state(t,TesterResult(next(seq),'summary',['actual command evidence']))
        with patch('tester_lifecycle.ControlRepository',return_value=r):run_attempts(t,invoke)
        return t,r
    def test_pass_terminal(self):
        t,r=self.run_statuses(['PASS']);self.assertEqual(r.finish_attempt.call_args.kwargs['status'],'SUCCEEDED')
        self.assertEqual(len(t.tester_attempt_history),1)
    def test_semantic_fail_not_technical_retry(self):
        t,r=self.run_statuses(['FAIL']);self.assertEqual(len(t.tester_attempt_history),1)
        self.assertEqual(r.finish_attempt.call_args.kwargs['retry_domain'],'PRODUCT_SEMANTIC_RETRY')
    def test_error_cap_fresh_ids_no_product_budget(self):
        t,r=self.run_statuses(['ERROR','ERROR']);ids=[x['attempt_id'] for x in t.tester_attempt_history]
        self.assertEqual(len(set(ids)),2);self.assertEqual(t.product_semantic_retry_count,0)
        self.assertTrue(all(c.kwargs['retry_domain']=='TECHNICAL_EXECUTION_RETRY' for c in r.finish_attempt.call_args_list))
    def test_error_then_pass(self):
        t,r=self.run_statuses(['ERROR','PASS']);self.assertEqual(t.test_status,'PASS');self.assertEqual(r.finish_attempt.call_count,2)
    def test_exception_terminal(self):
        t,r=self.run_statuses([],RuntimeError('x'));self.assertEqual(r.finish_attempt.call_count,2)
        self.assertEqual(t.test_status,'ERROR')
    def test_cancel_terminal(self):
        t=self.task();r=Mock()
        with patch('tester_lifecycle.ControlRepository',return_value=r):
            with self.assertRaises(KeyboardInterrupt):run_attempts(t,Mock(side_effect=KeyboardInterrupt()))
        self.assertEqual(r.finish_attempt.call_args.kwargs['status'],'INTERRUPTED')
    def test_native_cancel_no_retry(self):
        t=self.task();r=Mock()
        def invoke():
            t.official_tester_invoked=True
            _apply_to_state(t,TesterResult('ERROR','TESTER_CANCELLED'))
        with patch('tester_lifecycle.ControlRepository',return_value=r):run_attempts(t,invoke)
        self.assertEqual(r.finish_attempt.call_count,1)
        self.assertEqual(r.finish_attempt.call_args.kwargs['status'],'INTERRUPTED')
    def test_missing_evidence_error(self):
        t=self.task();r=Mock()
        def invoke():t.test_status='PASS';t.official_tester_invoked=True
        with patch('tester_lifecycle.ControlRepository',return_value=r):run_attempts(t,invoke)
        self.assertEqual(t.test_status,'ERROR')
    def test_fresh_worker_semantic(self):
        t=self.task();r=Mock();r.attempt_outcome.return_value={'status':'SUCCEEDED'}
        with patch('tester_lifecycle.ControlRepository',return_value=r):fresh_worker_attempt(t)
        self.assertNotEqual(t.attempt_id,'worker1');self.assertEqual(t.product_semantic_retry_count,1)
    def test_runtime_error_cannot_make_worker(self):
        t=self.task();t.failure_type='infrastructure'
        with self.assertRaisesRegex(ValueError,'SEMANTIC'):fresh_worker_attempt(t)
        t._record_execution_attempt.assert_not_called()
    def test_running_previous_worker_denied(self):
        t=self.task();r=Mock();r.attempt_outcome.return_value={'status':'RUNNING'}
        with patch('tester_lifecycle.ControlRepository',return_value=r):
            with self.assertRaisesRegex(ValueError,'TERMINAL'):fresh_worker_attempt(t)
    def test_hermes_selection_no_droid(self):
        t=self.task()
        with patch('tester._run_hermes_tester',return_value='selected') as h,patch('tester.trusted_executable') as d:
            self.assertEqual(run_tester(t,'unused'),'selected');d.assert_not_called()
    def test_output_contract(self):
        for status in ['PASS','FAIL','ERROR']:
            self.assertEqual(HermesRuntimeAdapter.structured_outcome(json.dumps({'outcome':'TEST_RESULT','status':status,'summary':'s','evidence':['command exit evidence']}))['status'],status)
        for obj in [{'outcome':'TEST_RESULT','status':'PASS','summary':'s','evidence':[]},{'outcome':'TEST_RESULT','status':'SKIPPED','summary':'s','evidence':['x']}]:
            with self.assertRaises(HermesError):HermesRuntimeAdapter.structured_outcome(json.dumps(obj))
    def test_fresh_fix_identity_reaches_runtime_prompt(self):
        from hermes_execution_factory import OwnedWorker
        w=OwnedWorker(Mock());w.scope=Mock();task=NS(retry_count=1,attempt_id='fresh-worker')
        with patch('hermes_execution_factory.HermesWorker.execute',return_value=NS(success=True)) as call:
            w.execute('approved contract','workspace',task)
        prompt=call.call_args.args[0]
        self.assertIn('cycle 2: FIX',prompt);self.assertIn('fresh-worker',prompt)
        self.assertIn('previous independent Tester/Reviewer findings',prompt)
    def test_current_candidate_producer_context(self):
        from hermes_execution_factory import candidate_criteria
        task=NS(attempt_id='fresh-worker-id',retry_count=1)
        result=json.loads(candidate_criteria(task,'value must equal 2'))
        self.assertEqual(result,{'candidate_worker_attempt_id':'fresh-worker-id','worker_cycle':2,'acceptance':'value must equal 2'})
    def test_explicit_role_policy(self):
        c=HermesExecutionConfig(None,None,None,'',{},tester_model='zai/model')
        self.assertEqual(HermesExecutionFactory(c).tester_policy()['model'],'zai/model')
        with self.assertRaises(ValueError):HermesExecutionFactory(replace(c,tester_model='custom:droid-model')).tester_policy()
    def test_retry_cap(self):
        c=HermesExecutionConfig(None,None,None,'',{},tester_model='zai/model',tester_technical_retries=2)
        with self.assertRaises(HermesError):HermesExecutionFactory(c).tester_policy()

class TesterContextTests(unittest.TestCase):
    def test_retry_feedback_json_escapes_are_not_host_paths(self):
        from test_execution_context import ExecutionContextTests
        from execution_context import build_execution_context_pack
        f=ExecutionContextTests();f.setUp();self.addCleanup(f.doCleanups)
        feedback=json.dumps({'review':'line one\nline two — 리뷰','command':'python -c "assert value()==2"'})
        pack=build_execution_context_pack(contract=f.c,requirement=f.requirement,profile_dir=f.profile,
            workspace=f.workspace,policy_root=f.policy,implementation_context=feedback)
        self.assertEqual(pack.manifest['contract']['implementation_context'],feedback)
    def test_actual_unc_feedback_still_rejected(self):
        from test_execution_context import ExecutionContextTests
        from execution_context import build_execution_context_pack
        from context_foundation import ContextError
        f=ExecutionContextTests();f.setUp();self.addCleanup(f.doCleanups)
        with self.assertRaises(ContextError):
            build_execution_context_pack(contract=f.c,requirement=f.requirement,profile_dir=f.profile,
                workspace=f.workspace,policy_root=f.policy,implementation_context=chr(92)*2+'host/share/private')
    def test_independent_context(self):
        from test_execution_context import ExecutionContextTests
        from execution_context import build_execution_context_pack
        f=ExecutionContextTests();f.setUp();self.addCleanup(f.doCleanups)
        c=replace(f.c,role='TESTER')
        pack=build_execution_context_pack(contract=c,requirement=f.requirement,profile_dir=f.profile,
            workspace=f.workspace,policy_root=f.policy,tester_evidence={'candidate_id':c.candidate_id,'build':'PASS receipt','test_plan':'execute test','criteria':'acceptance'})
        envelope=pack.manifest['contract'];self.assertEqual(envelope['role'],'TESTER')
        self.assertEqual(envelope['boundary']['candidate'],'RO');self.assertEqual(envelope['boundary']['scratch'],'RW')
        self.assertFalse(envelope['implementation_context']);self.assertFalse(envelope['review_evidence'])

class TesterLedgerTests(unittest.TestCase):
    def test_real_schema5_error_then_pass(self):
        import test_099_execution_runtime as fixtures
        f=fixtures.RuntimeRepositoryTests();f.setUp();self.addCleanup(f.doCleanups)
        t=TesterLifecycleTests().task();t.control_repository_path=str(f.repo.path)
        t._record_execution_attempt=lambda role,aid,model:f.repo.start_attempt(f.execution,attempt_id=aid,role=role,model=model)
        outcomes=iter(['ERROR','PASS'])
        def invoke():
            t.official_tester_invoked=True
            _apply_to_state(t,TesterResult(next(outcomes),'evidence',['executed fixture test']))
        run_attempts(t,invoke)
        rows=[f.repo.attempt_outcome(x['attempt_id']) for x in t.tester_attempt_history]
        self.assertEqual([x['status'] for x in rows],['FAILED','SUCCEEDED'])
        self.assertEqual(sum(x['product_retry_delta'] for x in rows),0)
        self.assertEqual(len({x['attempt_id'] for x in rows}),2)

class AtomicObservationTests(unittest.TestCase):
    def test_transient_sharing_violation_preserves_atomic_replace(self):
        from hermes_boundary import atomic
        with patch('hermes_boundary.write_once') as write,patch('hermes_boundary.os.replace',side_effect=[PermissionError(),None]) as replace_file,patch('hermes_boundary.time.sleep'):
            atomic('fixture.json',{'event':'heartbeat'})
        self.assertEqual(write.call_count,1);self.assertEqual(replace_file.call_count,2)
    def test_permanent_failure_stays_closed(self):
        from hermes_boundary import atomic
        with patch('hermes_boundary.write_once'),patch('hermes_boundary.os.replace',side_effect=PermissionError()) as replace_file,patch('hermes_boundary.time.sleep'):
            with self.assertRaises(PermissionError):atomic('fixture.json',{})
        self.assertEqual(replace_file.call_count,8)

if __name__=='__main__':unittest.main()
