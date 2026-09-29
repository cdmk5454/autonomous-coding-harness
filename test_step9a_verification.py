import copy,json,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import harness_temp
from task_state import TaskState
import gate_evidence as gate
from qa_browser_adapter import QAAdapter
from qa_browser_contract import QAScenario,QA_SCHEMA_VALIDATION_ERROR,QA_ASSERTION_FAILED
from qa_browser_providers import _parse_stagehand_result,_momentic_yaml_steps,QAScenarioDenied,ensure_provider_capability
from test_09921_qa_hotfix import PipelineVerificationTests,_gate_result,scenario,outcome,BINDING
from harness_service import HarnessService

class Step9ABindingTests(unittest.TestCase):
 def setUp(self):
  self.tmp=harness_temp.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);(self.root/'app.py').write_text('x=1\n')
  self.task=TaskState(task_id='T1',requirement='x=1',working_dir=str(self.root),task_mode='MODIFICATION')
  t=self.task;t.job_id='J1';t.execution_id='E1';t.attempt_id='A1';t.candidate_id='C1';t.native_session_binding_id='B1';t.execution_epoch=1
  t.test_plan={'production_scope':['app.py'],'required_tests':[],'derived_tests':[],'command':'python test.py','mandatory':True}
  t.materialized_execution={'baseline_hash':'BASE1','execution_surface_hash':'SURFACE','test_plan':copy.deepcopy(t.test_plan),'current_requirement':t.requirement}
  for name in gate.GATE_FIELDS:gate.bind(t,name,gate.identity(t,self.root),'PASS')
 def test_same_authority_current(self):self.assertEqual(gate.validate(self.task,gate.identity(self.task,self.root),require_all=True),[])
 def test_every_foreign_identity_rejected(self):
  for field in ('job_id','execution_id','attempt_id','candidate_id','native_session_binding_id','execution_epoch','tester_attempt_id','reviewer_attempt_id'):
   with self.subTest(field=field):
    t=copy.deepcopy(self.task);setattr(t,field,'FOREIGN');self.assertTrue(gate.validate(t,gate.identity(t,self.root),require_all=True))
 def test_command_and_acceptance_changes_rejected(self):
  for field in ('command','browser_qa','mandatory'):
   t=copy.deepcopy(self.task);t.test_plan[field]='changed';self.assertTrue(gate.validate(t,gate.identity(t,self.root),require_all=True))
  t=copy.deepcopy(self.task);t.requirement='different';self.assertTrue(gate.validate(t,gate.identity(t,self.root),require_all=True))
 def test_content_drift_rejected(self):
  (self.root/'app.py').write_text('x=2\n');self.assertTrue(gate.validate(self.task,gate.identity(self.task,self.root),require_all=True))
 def test_build_explicit_same_input_reuse_only(self):
  old=copy.deepcopy(self.task.gate_evidence['BUILD']);self.task.attempt_id='FIX2'
  self.assertTrue(gate.validate(self.task,gate.identity(self.task,self.root)))
  receipt=gate.reuse_build(self.task,old,self.root);self.assertEqual(receipt['reuse']['source'],old)
  self.assertNotIn('BUILD:STALE',gate.validate(self.task,gate.identity(self.task,self.root)))
  (self.root/'app.py').write_text('x=3');
  with self.assertRaises(ValueError):gate.reuse_build(self.task,old,self.root)
 def test_no_cross_candidate_build_reuse(self):
  old=copy.deepcopy(self.task.gate_evidence['BUILD']);self.task.candidate_id='C2'
  with self.assertRaises(ValueError):gate.reuse_build(self.task,old,self.root)

class Step9ABrowserGateTests(unittest.TestCase):
 def run_gate(self,changes):
  t=PipelineVerificationTests();t.setUp()
  try:
   result,task,_=t._run(qa_result_factory=lambda b:_gate_result('PASS',binding=b,**changes))
   result=t.manager.run_with_retry(task)
   return HarnessService._safe_outcome(task,result,Path(t.working_dir)/'task.json',None),task
  finally:t.doCleanups()
 def test_current_complete_required_succeeds(self):self.assertTrue(self.run_gate({})[0]['success'])
 def test_missing_required_assertion_or_artifact_blocks(self):
  for values in ({'assertions':[]},{'artifacts':[]},{'artifacts':[{'path':'missing-provider-report.json'}]}):
   with self.subTest(values=values):
    result,t=self.run_gate(values);self.assertFalse(result['success']);self.assertEqual(t.failure_code,'VERIFICATION_INCOMPLETE')
 def test_foreign_browser_identity_blocks_same_bytes(self):
  for key in ('candidate_id','job_id','execution_id','attempt_id','binding_identity','qa_execution_id','verification_contract_hash'):
   with self.subTest(key=key):
    result,t=self.run_gate({key:'FOREIGN'});self.assertFalse(result['success']);self.assertEqual(t.failure_code,'STALE_QA_EVIDENCE')
 def test_failure_assertion_cannot_be_pass(self):
  self.assertFalse(self.run_gate({'assertions':[{'name':'expected login','passed':False}]})[0]['success'])

class Step9AHumanAcceptanceTests(unittest.TestCase):
 def setUp(self):
  from control_repository import ControlRepository
  from job_contract import JobRequest,EXECUTION_CONTEXT_KEYS
  from job_queue import JobStore as QueueStore,SUCCEEDED,SUCCESS_ACK_REQUIRED
  from isolated_promotion import IsolatedCanonicalPromotion
  self.SUCCEEDED,self.gate=SUCCEEDED,SUCCESS_ACK_REQUIRED
  self.tmp=harness_temp.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  (self.root/'can').mkdir();(self.root/'src').mkdir();w=self.root/'work';w.mkdir();(w/'app.py').write_text('x=1\n')
  control=self.root/'.control';control.mkdir()
  self.repo=ControlRepository(control/'control.sqlite3',create=True);self.repo.bootstrap(QueueStore._empty_queue(),{},{})
  (control/'sqlite-authority.json').write_text('{"authority":"SQLITE","schema_version":6}')
  self.store=QueueStore(control,repository=self.repo)
  keys=EXECUTION_CONTEXT_KEYS-{'profile_id','profile_schema_version'}
  self.enqueue_context={'profile_id':'t','profile_schema_version':1,**{k:'a'*64 for k in keys}}
  self.frozen_context={'profile_id':'t','profile_schema_version':1,**{k:'b'*64 for k in keys}}
  jobs,_=self.store.enqueue_batch('s9a-accept',[JobRequest(client_job_id='A',requirement='accept candidate',worker='hermes',model='m',reasoning_effort='low',target_modules=('app',),target_resources=('app/app.py',),commit_summary='s9a accept test',max_outer_attempts=1)],execution_context=self.enqueue_context)
  job=self.store.get_job(jobs[0]['job_id']);self.job_id=job['job_id']
  job['status']='AWAITING_QA';job['candidate_id']='CAND-9';job['revision']+=1
  job['last_result']={'status':'AWAITING_QA','success':False,'failure_code':'USER_QA_REQUIRED'}
  self.repo.write_job(job,expected_revision=job['revision']-1)
  self.frozen={'execution_id':'EXEC-9','job_id':self.job_id,'contract_revision':1,
   'execution_context':self.frozen_context,'request':{'requirement':'accept candidate','client_job_id':'A'},
   'current_requirement':'accept candidate'}
  with self.repo.hold():
   self.repo._connection.execute('INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)',
    ('EXEC-9',self.job_id,'A',1,'MATERIALIZED','p','q','e','b','s','2026-01-01T00:00:00+09:00',json.dumps(self.frozen)))
   self.repo._connection.execute('INSERT INTO execution_authority VALUES(?,?,0,?,?,?)',
    ('EXEC-9','SUSPENDED','PENDING_DECISION','2026-01-01T00:00:00+09:00','{}'))
  d=self.repo.record_decision(self.job_id,contract_revision=1,decision_type='CANDIDATE',question='accept',payload={'candidate_id':'CAND-9'})
  self.repo.resolve_decision(d['decision_id'],resolution='HUMAN_ACCEPT');self.decision_id=d['decision_id']
  task=TaskState(task_id='TQA',requirement='accept candidate',working_dir=str(w),task_mode='MODIFICATION')
  task.job_id=self.job_id;task.execution_id='EXEC-9';task.attempt_id='A9';task.candidate_id='CAND-9';task.execution_epoch=1
  self.task_root=self.root/'tasks';self.task_root.mkdir();task.save(self.task_root)
  self.machine={'task_id':'TQA','status':'SUCCESS','stage':'DONE','success':True,'verification_status':'VERIFIED',
   'build_status':'PASS','test_status':'','review_status':'REVIEW_PASS','failure_code':'','secondary_failures':[],
   'user_qa_required':False,'changed_files':[]}
  held={'runtime_candidate_id':'HCAND-9','runtime_manifest':'','quarantine_candidate_id':'CAND-9',
        'verified_input':{},'gate_evidence':{},'machine_result':self.machine}
  class Capturing(IsolatedCanonicalPromotion):
   def __call__(self,job,service,result):
    self.captured=dict(job)
    return {**result,'canonical_promotion':{'status':'PROMOTED'},'canonical_commit':{'status':'COMMITTED'},
            'worktree_disposition':'CANONICAL_COMMITTED'}
  self.promoter=Capturing(self.root/'can',self.root/'promo',self.repo)
  self.promoter.allocations[self.job_id]={'service':SimpleNamespace(working_dir=w,task_root=self.task_root),
   'source':self.root/'src','base':{},'held_verification':held}
 def test_accept_promotes_with_frozen_materialized_execution_context(self):
  resolved=self.promoter.resolve_human(self.store,self.job_id,decision_id=self.decision_id,
                                       expected_revision=self.store.get_job(self.job_id)['revision'])
  job=self.promoter.captured
  # The snapshot authority is the frozen materialized context, never the enqueue context.
  self.assertEqual(job['execution_context'],self.frozen_context)
  self.assertNotEqual(job['execution_context'],self.enqueue_context)
  self.assertEqual(job['materialized_execution']['execution_id'],'EXEC-9')
  self.assertEqual(job['request'],self.frozen['request'])
  self.assertEqual(resolved['status'],self.SUCCEEDED)
  self.assertEqual(self.store.get_job(self.job_id)['status'],self.SUCCEEDED)
  self.assertEqual(self.store.queue_snapshot()['gate_reason'],self.gate)
  self.assertEqual(self.store.get_job(self.job_id)['last_result']['qa_decision_id'],self.decision_id)

class Step9AProviderTests(unittest.TestCase):
 def setUp(self):
  self.tmp=harness_temp.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.path=Path(self.tmp.name)/'result.json'
 def parse(self,**changes):
  payload={'scenario_id':scenario().scenario_id,'qa_execution_id':'Q1','status':'PASS','passed':True,'failure_code':'','assertions':[{'name':'check','passed':True}]};payload.update(changes);self.path.write_text(json.dumps(payload));return _parse_stagehand_result(self.path,scenario(),qa_execution_id='Q1')
 def test_strict_boolean(self):
  for value in ('false','true','yes','1',0,1,None,[],{}):
   with self.subTest(value=value):self.assertEqual(self.parse(passed=value).failure_code,QA_SCHEMA_VALIDATION_ERROR)
 def test_inconsistent_status_and_failure(self):
  for values in ({'status':'FAIL'},{'passed':False},{'failure_code':QA_ASSERTION_FAILED},{'assertions':[{'name':'check','passed':False}]}):
   self.assertEqual(self.parse(**values).failure_code,QA_SCHEMA_VALIDATION_ERROR)
 def test_actual_boolean_pass_fail(self):
  self.assertTrue(self.parse().passed)
  self.assertFalse(self.parse(status='FAIL',passed=False,failure_code=QA_ASSERTION_FAILED,assertions=[{'name':'check','passed':False}]).passed)
 def test_unsupported_action_never_dispatches_or_materializes(self):
  d={'scenario_id':'upload','title':'upload','acceptance':['persisted upload'],'allowed_actions':['navigate','upload','assert'],'forbidden_actions':['delete'],'side_effect_class':'IDEMPOTENT_UI','steps':[{'action':'goto','value':'http://127.0.0.1/'},{'action':'upload','value':'file.txt'},{'action':'assert_contains','value':'ready'}]}
  sc=QAScenario.from_declaration(d);primary=Mock();fallback=Mock()
  with self.assertRaisesRegex(QAScenarioDenied,'complete scenario'):QAAdapter(momentic=primary,stagehand=fallback).execute(scenario=sc,binding=BINDING)
  primary.assert_not_called();fallback.assert_not_called()
  with self.assertRaises(QAScenarioDenied):_momentic_yaml_steps(sc)
  with self.assertRaises(QAScenarioDenied):ensure_provider_capability('STAGEHAND',sc)
 def test_malformed_never_falls_back(self):
  fallback=Mock(return_value=outcome());bad=outcome(passed=False,status='REPORT_INVALID',failure_code=QA_SCHEMA_VALIDATION_ERROR)
  r=QAAdapter(momentic=Mock(return_value=bad),stagehand=fallback).execute(scenario=scenario(),binding=BINDING)
  self.assertNotEqual(r.status,'PASS');fallback.assert_not_called()
 def test_application_fail_never_falls_back(self):
  fallback=Mock(return_value=outcome());bad=outcome(passed=False,status='FAIL',failure_code=QA_ASSERTION_FAILED)
  r=QAAdapter(momentic=Mock(return_value=bad),stagehand=fallback).execute(scenario=scenario(),binding=BINDING)
  self.assertEqual(r.status,'FAIL');fallback.assert_not_called()
 def test_materialized_step_count_exact(self):self.assertEqual(len(_momentic_yaml_steps(scenario())),len(scenario().steps))

if __name__=='__main__':unittest.main()
