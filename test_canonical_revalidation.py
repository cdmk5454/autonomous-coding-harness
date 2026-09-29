"""Disposable gate successor tests; no Product or Git operation is executed."""
import copy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from canonical_revalidation import (admit_gate_execution, approved_delta, current_contract,
    finish_gate_execution, run_fresh_gates, source_job_binding, successor_task, task_scope, verify_input)
from control_repository import RepositoryError, canonical, digest
from hermes_boundary import read, write_once
from production_isolation import inventory
from isolated_promotion import content
from build_test_runner import BuildTestRunner
from task_state import TaskState
from reviewer import ReviewResult
from tester import execute_test_receipts, arbitrate_tester_result, TesterResult, _apply_to_state
from qa_quarantine import CandidateError


class CurrentInputSuccessorTests(unittest.TestCase):
    def setUp(self):
        from test_final_blocker_lifecycle import FinalBlockerLifecycleTests
        self.flow = FinalBlockerLifecycleTests()
        from control_repository import ControlRepository
        materialize=ControlRepository.materialize
        def frozen_policy(repo,job,**kwargs):
            kwargs['policy']={**kwargs['policy'],
                'resolved_tester':dict(runtime='droid',model='fixture'),
                'resolved_reviewer':dict(model='gpt-5.6-sol',reasoning_effort='high')}
            return materialize(repo,job,**kwargs)
        with patch.object(ControlRepository,'materialize',new=frozen_policy):
            self.flow.setUp()
        self.addCleanup(self.flow.doCleanups)
        self.fx = self.flow.fixture
        self.repo = self.fx.repo
        self.parent = self.flow.task
        self.parent.effective_policy=copy.deepcopy(self.fx.execution['effective_policy'])
        self.assertTrue(self.flow.run_manager().success)
        self.parent.save(self.flow.root)
        self.parent_bytes = (self.flow.root / (self.parent.task_id + '.json')).read_bytes()
        self.parent_execution = copy.deepcopy(self.repo.execution(self.parent.execution_id))
        self.old_proofs = copy.deepcopy(self.parent.gate_evidence)
        self.workspace = self.fx.root / 'revalidated'
        import shutil
        shutil.copytree(self.fx.workspace, self.workspace)
        (self.workspace/'app/accepted_a.py').write_text('accepted = True\n')
        # Existing profile snapshot fixture is copied and extended only for the
        # disposable workspace, exactly as ProductionIsolation does.
        self.profile_dir = self.fx.root/'revalidated-profile'/'fixture'
        shutil.copytree(self.fx.profile.profile_dir, self.profile_dir)
        manifest = read(self.profile_dir/'project.json')
        manifest['workspace_roots'].append(str(self.workspace))
        manifest['control_policy']=dict(commit_policy='BATCH_FINAL_COMMIT',cumulative_worktree=True,
            auto_continue_without_commit=True,pre_job_snapshot_supported=True,
            scoped_rollback_supported=True,final_commit_required=True,
            task_owned_baseline_files=[],external_frozen_baseline_files=[])
        manifest['modules'][0]['build'] = dict(cwd='app', argv=[sys.executable, '-B', '-c',
            "from pathlib import Path; assert Path('a.py').read_text() == 'value = 1\\n'"])
        (self.profile_dir/'project.json').write_text(json.dumps(manifest))
        from project_profile import load_project_profile
        self.profile = load_project_profile(self.profile_dir, self.workspace)
        self.task = successor_task(self.parent, candidate=self.workspace,
            profile_dir=self.profile_dir, candidate_id='RCAND-FIXTURE')
        self.task.control_repository_path = str(self.repo.path)
        self.task.materialized_execution['effective_policy'] = copy.deepcopy(self.task.effective_policy)
        self.task.materialized_execution['test_plan'] = copy.deepcopy(self.task.test_plan)
        self.task.materialized_execution['current_requirement'] = self.task.requirement
        self.task.changed_file_sha256 = dict(self.fx.hashes)
        self.proof_path = self.fx.root/'admission.json'
        self.input = content(self.workspace, self.profile, ['app/a.py','app/test_a.py'])
        write_once(self.proof_path, dict(job_id=self.task.job_id, candidate_id=self.task.candidate_id,
            input_content=self.input, immutable_predecessor_files={
                str(self.flow.root/(self.parent.task_id+'.json')):hashlib.sha256(self.parent_bytes).hexdigest()}))
        self.binding = dict(kind='CANONICAL_REVALIDATION', job_id=self.task.job_id,
            request_id='post-a-1', predecessor_execution_id=self.parent.execution_id,
            candidate_id=self.task.candidate_id, workspace=str(self.workspace),
            admission_path=str(self.proof_path), admission_sha256=hashlib.sha256(self.proof_path.read_bytes()).hexdigest(),
            input_content=self.input, canonical_base={'app/a.py':'before'}, canonical_heads={},
            preserve=['app/a.py','app/test_a.py'], source_jobs=[source_job_binding(self.repo,self.task.job_id)])
        self.binding.update(task_scope(self.task))
        self.heads = patch('isolated_promotion.module_heads', return_value={})
        self.heads.start();self.addCleanup(self.heads.stop)
        self.calls = []

    def admit(self):
        return admit_gate_execution(self.repo,self.task,binding=self.binding,
            expected_revision=self.repo.read_job(self.task.job_id)['revision'])

    def factory(self, **kwargs):
        manager = self.flow.manager
        manager.working_dir = str(self.workspace)
        manager.profile = self.profile
        manager.policy_root = None
        manager.task_root = self.flow.root
        manager.configuration_guard = kwargs['configuration_guard']
        manager.build_test_runner = BuildTestRunner(str(self.workspace),profile=self.profile,
            evidence_root=self.fx.root/'fresh-build')
        manager.collector = SimpleNamespace(assess_commit_safety=lambda *_:(True,[],dict(self.fx.hashes)))
        manager.shutdown = lambda: None
        manager._log_stage = lambda *_: None
        import threading
        manager._state_lock=threading.Lock()
        manager._browser_qa_gate = lambda *_: {'acceptable':True}
        manager.reviewer = SimpleNamespace(review=self.review)
        return manager

    def review(self, **kwargs):
        self.calls.append(('REVIEW',kwargs['task'].execution_id,kwargs['task'].candidate_id))
        from worker import TimeoutConfig
        from owned_role_transport import run as run_owned_role
        task=kwargs['task']
        code,stdout,stderr,reason=run_owned_role(
            cmd=[sys.executable,'-B','-c',"print('review fixture')"],stdin_data='',
            cwd=str(self.workspace),cfg=TimeoutConfig(analysis_max=20,hard_limit=20),
            requirement='read-only fixture',task_mode='ANALYSIS',
            runtime_event=task._runtime_event_callback,execution_id='INV-REVIEW-FIXTURE',
            execution_role='REVIEWER',attempt_id=task.reviewer_attempt_id,runtime_name='fixture-reviewer')
        self.assertEqual(0,code,stderr)
        self.assertFalse(reason)
        return ReviewResult(True,status='REVIEW_PASS',reviewed_files=['app/a.py'])

    def _invoke_tester(self, task, workspace, **kwargs):
        self.calls.append(('TEST',task.execution_id,task.candidate_id))
        receipts=execute_test_receipts(task,workspace)
        task.official_tester_invoked=True
        task.official_tester_execution=dict(runtime='fixture',status='SUCCEEDED',stdout='TEST_RESULT: PASS')
        return _apply_to_state(task,arbitrate_tester_result(task,
            TesterResult('PASS','fresh subprocess assertion',evidence=['app/test_a.py']),receipts))

    def run_gates(self):
        service=SimpleNamespace(_lease_held=True,working_dir=self.workspace,
            snapshot_root=self.fx.root/'snapshots',manager_factory=self.factory,task_root=self.flow.root)
        frozen=SimpleNamespace(profile=self.profile,policy_root=None,validate=lambda:None)
        with patch('runtime_snapshot.load_frozen_runtime_snapshot',return_value=frozen),\
             patch('manager.run_tester',side_effect=self._invoke_tester):
            return run_fresh_gates(service,self.task,baseline=object())

    def test_fresh_gates_and_immutable_parent(self):
        fresh=self.admit()
        result=self.run_gates()
        self.assertTrue(result['success'],result)
        self.assertFalse(result['worker_invoked'])
        self.assertEqual('VERIFIED',result['verification_status'])
        self.assertNotEqual(self.parent.execution_id,fresh['execution_id'])
        self.assertEqual(self.parent_execution,self.repo.execution(self.parent.execution_id))
        self.assertEqual(self.parent_bytes,(self.flow.root/(self.parent.task_id+'.json')).read_bytes())
        self.assertEqual(self.old_proofs,self.parent.gate_evidence)
        self.assertEqual(['TEST','REVIEW'],[x[0] for x in self.calls])
        self.assertEqual('',self.task.attempt_id)
        with self.repo.hold():
            rows=self.repo._connection.execute('SELECT role FROM attempts WHERE execution_id=?',
                (fresh['execution_id'],)).fetchall()
        self.assertEqual(['TESTER'],[r[0] for r in rows])
        self.assertEqual(self.task.candidate_id,self.task.test_receipts[0]['candidate_id'])
        self.assertNotEqual(self.parent.tester_attempt_id,self.task.tester_attempt_id)
        with self.repo.hold():
            processes=self.repo._connection.execute('SELECT p.state,a.role FROM runtime_processes p '
                'JOIN attempts a USING(attempt_id) WHERE a.execution_id=?',
                (self.task.reviewer_execution['execution_id'],)).fetchall()
        self.assertEqual([('EXITED','REVIEWER')],[tuple(p) for p in processes])

    def test_source_mutation_after_admission_blocks_before_gates(self):
        self.admit()
        (self.workspace/'app/accepted_a.py').write_text('changed = True\n')
        with self.assertRaisesRegex(RepositoryError,'INPUT_CHANGED'):
            self.run_gates()
        self.assertEqual([],self.calls)

    def test_prior_receipt_mutation_blocks(self):
        self.admit()
        (self.flow.root/(self.parent.task_id+'.json')).write_bytes(b'changed')
        with self.assertRaisesRegex(RepositoryError,'PREDECESSOR_EVIDENCE_CHANGED'):
            verify_input(self.task)

    def test_worker_cannot_start_on_gate_successor(self):
        frozen=self.admit()
        with self.assertRaisesRegex(RepositoryError,'REVALIDATION_TESTER_ROLE_REQUIRED'):
            self.repo.start_attempt(frozen,attempt_id='forbidden-worker',role='WORKER',model='fixture')

    def test_changed_task_scope_cannot_reuse_full_byte_input(self):
        self.admit()
        self.task.changed_files=[]
        with self.assertRaisesRegex(RepositoryError,'TASK_SCOPE_CHANGED'):
            self.run_gates()

    def test_semantic_review_failure_has_no_success(self):
        self.admit()
        self.review=lambda **kwargs:ReviewResult(False,status='REVIEW_FAIL',reason='semantic',reviewed_files=['app/a.py'])
        result=self.run_gates()
        self.assertFalse(result['success'])
        self.assertEqual('REVIEW_FAIL',result['review_status'])
        self.assertEqual(self.parent_execution,self.repo.execution(self.parent.execution_id))

    def test_false_native_test_pass_without_bound_receipts_cannot_pass(self):
        self.admit()
        def native_only(task,*args,**kwargs):
            task.official_tester_invoked=True
            return _apply_to_state(task,TesterResult('PASS','native prose only',evidence=['claim']))
        self._invoke_tester=native_only
        result=self.run_gates()
        self.assertFalse(result['success'])
        self.assertEqual('ERROR',result['test_status'])
        self.assertEqual({},self.task.test_checkpoint)

    def test_malformed_review_cannot_pass(self):
        self.admit()
        self.review=lambda **kwargs:ReviewResult(False,status='MALFORMED',reason='invalid schema',reviewed_files=[])
        result=self.run_gates()
        self.assertFalse(result['success'])

    def test_lingering_reviewer_process_blocks_completion(self):
        self.admit()
        import subprocess
        from control_repository import now
        children=[]
        def lingering(**kwargs):
            task=kwargs['task']
            proc=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(30)'])
            children.append(proc)
            task._runtime_event_callback(dict(event='STARTED',execution_id='INV-LINGER',
                attempt_id=task.reviewer_attempt_id,execution_role='REVIEWER',runtime='fixture-reviewer',
                pid=proc.pid,started_at=now(),observed_at=now(),process_alive=True))
            return ReviewResult(True,status='REVIEW_PASS',reviewed_files=['app/a.py'])
        self.review=lingering
        try:
            with self.assertRaisesRegex(RepositoryError,'PROCESS_NOT_QUIESCED'):
                self.run_gates()
        finally:
            for proc in children:
                proc.terminate();proc.wait(timeout=10)
        self.assertEqual(self.parent_execution,self.repo.execution(self.parent.execution_id))

    def test_uncertain_ended_event_remains_unknown(self):
        self.admit()
        import subprocess
        from control_repository import now
        children=[]
        def uncertain(**kwargs):
            task=kwargs['task'];proc=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(30)'])
            children.append(proc)
            event=dict(execution_id='INV-UNKNOWN',attempt_id=task.reviewer_attempt_id,
                execution_role='REVIEWER',runtime='fixture-reviewer',pid=proc.pid,
                started_at=now(),observed_at=now(),process_alive=True)
            task._runtime_event_callback(dict(event,event='STARTED'))
            task._runtime_event_callback(dict(event,event='ENDED',quiescent=False,termination_reason='CLEANUP_FAILED',exit_code=-1))
            return ReviewResult(True,status='REVIEW_PASS',reviewed_files=['app/a.py'])
        self.review=uncertain
        try:
            with self.assertRaisesRegex(RepositoryError,'PROCESS_NOT_QUIESCED'):self.run_gates()
            with self.repo.hold():
                state=self.repo._connection.execute('SELECT state FROM runtime_processes ORDER BY rowid DESC LIMIT 1').fetchone()[0]
            self.assertEqual('UNKNOWN',state)
        finally:
            for proc in children:proc.terminate();proc.wait(timeout=10)

    def test_schema6_slot_and_queue_projection_close_together(self):
        from bounded_execution import migrate_disposable,slots
        migrate_disposable(self.repo.path,allocation_root=self.fx.root,backup_path=self.fx.root/'schema5.bak')
        import gc
        gc.collect()
        self.admit()
        self.assertEqual(self.task.job_id,self.repo.read_queue()['running_job_id'])
        self.assertEqual(1,len(slots(self.repo)))
        result=self.run_gates()
        self.assertTrue(result['success'])
        self.assertEqual([],slots(self.repo))
        self.assertEqual('',self.repo.read_queue()['running_job_id'])
        self.assertEqual(0,self.repo.read_queue()['active_execution_count'])

    def test_revision_and_idempotency_fail_closed(self):
        with self.assertRaisesRegex(RepositoryError,'REVISION_CONFLICT'):
            admit_gate_execution(self.repo,self.task,binding=self.binding,expected_revision=-1)
        self.admit()
        with self.assertRaisesRegex(RepositoryError,'ALREADY_ADMITTED'):
            self.admit()

    def test_unquiesced_predecessor_blocks_admission(self):
        with self.repo.hold():
            aid=self.repo._connection.execute('SELECT attempt_id FROM attempts LIMIT 1').fetchone()[0]
            self.repo._connection.execute("UPDATE attempt_outcomes SET status='UNKNOWN' WHERE attempt_id=?",(aid,))
        with self.assertRaisesRegex(RepositoryError,'NOT_QUIESCED'):
            self.admit()

    def test_operator_route_returns_new_held_candidate_with_old_bytes_preserved(self):
        """Actual operator/custody/DB/gates/QA path; Git/provider discovery are fixtures."""
        import shutil
        from bounded_execution import migrate_disposable
        migrate_disposable(self.repo.path,allocation_root=self.fx.root,backup_path=self.fx.root/'schema5.bak')
        import gc
        gc.collect()
        from harness_service import HarnessService
        from isolated_promotion import IsolatedCanonicalPromotion
        from qa_quarantine import QACandidateStore
        from hermes_boundary import atomic
        from manager import ManagerResult
        from git_collector import GitTaskBaseline
        from canonical_revalidation import revalidate, CONFIRMATION
        source=self.fx.root/'source'
        canonical_root=self.fx.root/'canonical'
        shutil.copytree(self.fx.workspace,source)
        (source/'app/a.py').write_bytes(b'value = 0\n')
        shutil.copytree(source,canonical_root)
        promoter=IsolatedCanonicalPromotion(canonical_root,self.fx.root/'publication',self.repo)
        old_service=SimpleNamespace(working_dir=self.fx.workspace,task_root=self.flow.root,
            snapshot_root=self.fx.root/'snapshots',_load_profile=lambda:(self.profile,None))
        bound={**self.repo.read_job(self.parent.job_id),'materialized_execution':self.parent.materialized_execution,
               'execution_context':self.parent.materialized_execution['execution_context']}
        atomic(promoter.root/(self.parent.job_id+'-base.json'),dict(job_id=self.parent.job_id,
            candidate=str(self.fx.workspace),source=str(source),base=content(source,self.profile),source_heads={}))
        promoter.register_preserved(bound,old_service,source)
        self.parent.candidate_manifest=str(self.fx.root/'allocation.json')
        self.parent.save(self.flow.root)
        result=HarnessService._safe_outcome(self.parent,ManagerResult(True,'PASS'),self.flow.root/'task.json',None)
        held=promoter.quarantine(bound,old_service,result)
        old_job=self.repo.read_job(self.parent.job_id)
        old_revision=old_job['revision']
        old_job.update(revision=old_revision+1,candidate_id=held['candidate_id'],
            candidate_manifest=held['candidate_manifest'],last_result=held)
        self.repo.write_job(old_job,expected_revision=old_revision)
        self.repo.record_decision(self.parent.job_id,contract_revision=old_job['contract_revision'],
            decision_type='CANDIDATE',question='Accept B?',payload=dict(candidate_id=held['candidate_id']))
        original_bytes=(self.flow.root/(self.parent.task_id+'.json')).read_bytes()
        original_input=content(self.fx.workspace,self.profile)
        # Create and publish a disjoint accepted A fixture through the real journal owner.
        a_work=self.fx.root/'a-work';shutil.copytree(source,a_work)
        (a_work/'app/accepted_a.py').write_text('accepted = True\n')
        a_snapshot=self.fx.root/'a-before.json'
        write_once(a_snapshot,dict(kind='PRE_JOB_SNAPSHOT',files=[dict(path='app/accepted_a.py',sha256='DELETED',state='DELETED')]))
        a_task=TaskState(task_id='TASK-ACCEPTED-A',requirement='accepted A',worker='codex')
        a_task.job_id='JOB-PREDECESSOR';a_task.changed_files=['app/accepted_a.py']
        a_task.git_diff='diff --git a/app/accepted_a.py b/app/accepted_a.py\n+accepted = True\n'
        a_task.pre_job_snapshot_manifest=str(a_snapshot);a_task.pre_job_snapshot_id='A-BEFORE'
        a_task.qa_type='HUMAN_ACCEPTANCE';a_task.hold_scope='JOB';a_task.machine_verified=True
        ac=QACandidateStore(promoter.root/'candidates',a_work,self.profile).create(a_task)
        pub=QACandidateStore(promoter.root/'candidates',canonical_root,self.profile).promote(
            [ac['candidate_id']],lambda _:result,authority=dict(job_id=a_task.job_id,execution_id='EXEC-A',contract_revision=1))
        predecessor=copy.deepcopy(old_job)
        predecessor.update(job_id=a_task.job_id,revision=1,status='SUCCEEDED',candidate_id=ac['candidate_id'],
            last_result={**result,'canonical_promotion':pub})
        self.repo.write_job(predecessor,expected_revision=0)
        expected_canonical=content(canonical_root,self.profile)
        owner=SimpleNamespace(working_dir=canonical_root,profile_dir=self.profile_dir,
            task_root=self.flow.root,snapshot_root=self.fx.root/'snapshots',manager_factory=self.factory,
            acquire_execution_lease=lambda:True,release_execution_lease=lambda:None)
        def event(job,*args,**kwargs):job['revision']+=1
        store=SimpleNamespace(repository=self.repo,_mutex=SimpleNamespace(hold=self.repo.hold),
            _event=event,_write_job=lambda j:self.repo.write_job(j,expected_revision=j['revision']-1))
        control=SimpleNamespace(_assert_writable=lambda:None,harness=owner,store=store,
            isolation=SimpleNamespace(root=self.fx.root/'new-runs',promoter=promoter),
            supervisor=SimpleNamespace(is_running=lambda:False))
        test=self
        class Child:
            _safe_outcome=staticmethod(HarnessService._safe_outcome)
            def __init__(self,workspace,profile_dir,**kwargs):
                from project_profile import load_project_profile
                self.working_dir=Path(workspace);self.profile_dir=Path(profile_dir)
                self.task_root=kwargs['task_root'];self.snapshot_root=kwargs['snapshot_root']
                self.manager_factory=kwargs['manager_factory'];self._lease_held=False
                self.profile=load_project_profile(profile_dir,workspace)
                test.workspace=Path(workspace);test.profile=self.profile
            def _load_profile(self):return self.profile,['app']
            def _configuration(self,*args):return copy.deepcopy(test.parent.materialized_execution['execution_context'])
            def _materialize_runtime_snapshot(self,*args):return SimpleNamespace(root=self.snapshot_root)
            def acquire_execution_lease(self):self._lease_held=True;return True
            def release_execution_lease(self):self._lease_held=False
        def collect(_collector,task,baseline,paths):
            task.changed_files=list(paths)
            return SimpleNamespace(success=True)
        def frozen(*args,**kwargs):return SimpleNamespace(profile=test.profile,policy_root=None,validate=lambda:None)
        with patch('harness_service.HarnessService',Child),\
             patch('production_isolation.validate_git_ownership'),\
             patch('git_collector.GitCollector.capture_baseline',return_value=(GitTaskBaseline(()),'')),\
             patch('git_collector.GitCollector.collect_selected',new=collect),\
             patch('runtime_snapshot.load_frozen_runtime_snapshot',side_effect=frozen),\
             patch('runtime_identity.source_process_identity',return_value={'source':{}}),\
             patch('runtime_identity.attempt_provenance',return_value={'fixture':True}),\
             patch('manager.run_tester',side_effect=self._invoke_tester):
            answer=revalidate(control,self.parent.job_id,expected_revision=old_job['revision'],
                request_id='actual-operator-route',predecessor_ids=[a_task.job_id],confirmation=CONFIRMATION)
        self.assertEqual('AWAITING_QA',answer['job']['status'])
        self.assertFalse(answer['published'])
        self.assertNotEqual(held['candidate_id'],answer['result']['candidate_id'])
        self.assertEqual(original_input,content(self.fx.workspace,self.fx.profile))
        self.assertEqual(original_bytes,(self.flow.root/(self.parent.task_id+'.json')).read_bytes())
        self.assertEqual(expected_canonical,content(canonical_root,self.profile))
        self.assertEqual(self.parent_execution,self.repo.execution(self.parent.execution_id))
        self.assertEqual(1,len(self.repo.open_decisions(self.parent.job_id)))
        # Publication is a separate explicit action in this fixture. The
        # revalidation call above itself did not alter canonical Product.
        new_job=self.repo.read_job(self.parent.job_id)
        new_execution=self.repo.execution(answer['result']['execution_id'])
        new_bound={**new_job,'materialized_execution':new_execution,
                   'execution_context':new_execution['execution_context']}
        allocation=promoter.allocations[self.parent.job_id]
        with patch('runtime_snapshot.load_frozen_runtime_snapshot',side_effect=frozen):
            published=promoter(new_bound,allocation['service'],allocation['held_verification']['machine_result'])
        self.assertEqual('PROMOTED',published['canonical_promotion']['status'])
        self.assertEqual('DEFERRED_TO_BATCH_FINAL',published['commit_status'])
        self.assertEqual((self.fx.workspace/'app/a.py').read_bytes(),(canonical_root/'app/a.py').read_bytes())
        self.assertEqual((a_work/'app/accepted_a.py').read_bytes(),(canonical_root/'app/accepted_a.py').read_bytes())
        self.assertEqual(original_input,content(self.fx.workspace,self.fx.profile))
        self.assertEqual(original_bytes,(self.flow.root/(self.parent.task_id+'.json')).read_bytes())


class FrozenCanaryGeometryTests(unittest.TestCase):
    def setUp(self):
        manifest=Path(r'D:\hermes\harness-1.0-real-product-canary-20260924-004600\canary-manifest.json')
        self.contract=json.loads(manifest.read_text(encoding='utf-8'))
        a,b=self.contract['multi_batch']['jobs']
        self.a_paths=list(a['target_resources']);self.b_paths=list(b['target_resources'])
        self.base={p:'base-'+str(i) for i,p in enumerate(self.a_paths+self.b_paths)}
        self.current={**self.base,**{p:'accepted-'+str(i) for i,p in enumerate(self.a_paths)}}
        self.job=dict(job_id='JOB-A',revision=1,contract_revision=1,status='SUCCEEDED',request=a,
            candidate_id='CAND-A',last_result=dict(canonical_promotion=dict(status='PROMOTED',
                publication_id='PUB-A',candidate_ids=['CAND-A'])))
        self.repo=SimpleNamespace(read_job=lambda _id:copy.deepcopy(self.job))
        self.promoter=SimpleNamespace(root=Path('fixture-publication'),canonical=Path('fixture-canonical'))
        self.journal=dict(state='COMPLETED',disposition='PROMOTED',authority=dict(job_id='JOB-A'),candidate_ids=['CAND-A'])
        self.candidate=dict(state='PROMOTED',delta=dict(changed_files=self.a_paths,
            files=[dict(path=p,candidate_sha256=self.current[p]) for p in self.a_paths]))

    def invoke(self):
        store=SimpleNamespace(load_publication=lambda _:self.journal,load=lambda _:self.candidate)
        with patch('qa_quarantine.QACandidateStore',return_value=store):
            return approved_delta(self.repo,self.promoter,self.base,self.current,['JOB-A'],own_scope=self.b_paths)

    def test_exact_approved_A_B_geometry_admits_post_A_delta(self):
        result=self.invoke()
        self.assertEqual(sorted(self.a_paths),result['changed_files'])
        self.assertEqual([],self.contract['multi_batch']['jobs'][0]['depends_on'])
        self.assertEqual([],self.contract['multi_batch']['jobs'][1]['depends_on'])
        self.assertEqual('HUMAN_ACCEPTANCE',self.contract['multi_batch']['jobs'][1]['qa_type'])

    def test_B_scope_overlap_never_admits(self):
        self.b_paths.append(self.a_paths[0])
        with self.assertRaisesRegex(CandidateError,'SCOPE_OVERLAP'):self.invoke()

    def test_unrelated_canonical_delta_never_admits(self):
        self.current['sucui/src/foreign.js']='foreign'
        with self.assertRaisesRegex(CandidateError,'DELTA_UNOWNED'):self.invoke()

    def test_changed_A_postimage_never_admits(self):
        self.current[self.a_paths[0]]='foreign'
        with self.assertRaisesRegex(CandidateError,'FOREIGN_BYTES'):self.invoke()

    def test_unfinished_publication_never_admits(self):
        self.journal['state']='APPLYING'
        with self.assertRaisesRegex(CandidateError,'PUBLICATION_INVALID'):self.invoke()


if __name__ == '__main__':
    unittest.main()
