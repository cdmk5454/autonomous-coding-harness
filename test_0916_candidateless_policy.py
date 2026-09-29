import copy
import json
from types import SimpleNamespace
from unittest.mock import patch

from test_no_delta_fresh_recovery import FreshRecoveryTest
from test_09005_hotfix import context
from job_contract import JobRequest
from job_queue import QueueError
from cumulative_policy import CumulativePolicyError
from harness_service import HarnessService
from control_repository import ControlRepository
from qa_policy_resolution import POLICY, CONTRACT, exempt_qa_signal
from test_081_patch import Patch081Test, _Worker, _Collector
from worker import WorkerResult, parse_qa_request


class PolicyResolutionTests(FreshRecoveryTest):
    def policy_fixture(self):
        self.fixture()
        evidence = self.parent['last_result']['no_task_delta_evidence']
        self.parent.update(status='AWAITING_QA', current_claim_task_id='TASK-POLICY',
                           task_ids=['TASK-POLICY'], last_result={'failure_code':'CANDIDATE_DELTA_EMPTY'})
        self.store._write_job(self.parent)
        self.parent = self.store.get_job(self.parent['job_id'])
        q = self.store._read_queue()
        q.update(blocked_by_job_id=self.parent['job_id'], gate_reason='AWAITING_QA', paused=True)
        self.store._write_queue(q)
        self.q = self.store.queue_snapshot()
        self.task = SimpleNamespace(job_id=self.parent['job_id'], task_id='TASK-POLICY',
            pre_job_snapshot_manifest=self.parent['pre_job_snapshot_manifest'],
            no_task_delta_evidence=evidence, changed_files=[], task_owned_changed_files=[],
            qa_request={'required':True,'qa_type':'DB_CONTRACT'})
        self.contract = JobRequest.from_mapping(dict(self.parent['request'],
            requirement=self.parent['current_requirement']+'\n\n'+CONTRACT), allowed_modules=('app',))

    def resolve(self):
        fresh = self.manager.validate_policy_fresh_input(self.parent,self.q,self.integrity,self.task)
        return self.store.enqueue_anchored_replacement(self.parent['job_id'],self.contract,
            expected_queue_revision=self.q['revision'],expected_job_revision=self.parent['revision'],
            request_id='policy-resolution',reason='explicit DB/E2E waiver',execution_context=context(),
            provenance={'status':'VERIFIED'},integrity_snapshot=self.integrity,
            fresh_input=fresh,policy_resolution=POLICY)[0]

    def test_policy_record_and_history(self):
        self.policy_fixture()
        before = copy.deepcopy(self.parent)
        fresh = self.resolve()
        self.assertEqual(before,self.store.get_job(before['job_id']))
        self.assertNotEqual(before['job_id'],fresh['job_id'])
        self.assertEqual(POLICY,fresh['qa_policy_resolution']['policy'])
        self.assertEqual('',fresh['candidate_id'])
        self.assertEqual({},fresh['last_result'])
        self.assertTrue(self.store._is_replacement_anchor_parent(self.store._read_queue(),before['job_id']))

    def test_fresh_contract_and_input(self):
        self.policy_fixture()
        fresh = self.resolve()
        service = object.__new__(HarnessService)
        service.control_root,self_work = self.control,self.workspace
        service.working_dir,service.task_root = self_work,self.control/'tasks'
        with patch.object(service,'_cumulative_manager',return_value=self.manager), \
             patch.object(service,'cumulative_policy_status',return_value=self.integrity), \
             patch('harness_service.TaskState.load',return_value=self.task):
            snap = service.prepare_job_snapshot(fresh,self.store.queue_snapshot())
        self.assertNotEqual(self.parent['pre_job_snapshot_id'],snap['snapshot_id'])
        repo = ControlRepository(self.control/'test-materialization.sqlite3',create=True)
        jobs = {j:self.store.get_job(j) for j in self.store._read_queue()['order']}
        repo.bootstrap(self.store._read_queue(),jobs,{j:{'path':j+'.json'} for j in jobs})
        execution = repo.materialize(fresh,context={'profile_snapshot_sha256':'p','policy_snapshot_sha256':'p'},
            policy={'effective_policy_sha256':'p'},baseline_hash='b',execution_surface_hash='s',
            extra={'current_requirement':fresh['current_requirement']})
        self.assertIn(CONTRACT,execution['current_requirement'])
        self.assertEqual(2,execution['contract_revision'])
        self.assertTrue(execution['execution_id'].startswith('EXEC-'))

    def test_candidate_dependent_still_rejected(self):
        self.policy_fixture()
        from chatgpt_control import ChatGPTControlService
        control=object.__new__(ChatGPTControlService)
        control.supervisor=SimpleNamespace(is_running=lambda:False)
        control.harness=SimpleNamespace(probe_execution_idle=lambda:True)
        control.store=self.store
        with self.assertRaisesRegex(QueueError,'QA_CANDIDATE_RESOLUTION_STATE_INVALID'):
            control.preview_qa_candidate_resolution(self.parent['job_id'],self.parent['revision'])
        self.task.qa_request['qa_type']='BUSINESS_CONTRACT'
        with self.assertRaisesRegex(CumulativePolicyError,'QA_POLICY_RESOLUTION_STATE_INVALID'):
            self.resolve()
        self.assertEqual(self.parent,self.store.get_job(self.parent['job_id']))

    def test_dirty_still_rejected(self):
        self.policy_fixture()
        (self.repo/'same.txt').write_text('real dirty',encoding='utf-8')
        with self.assertRaisesRegex(CumulativePolicyError,'REPLACEMENT_WORKTREE_INTEGRITY_INVALID'):
            self.resolve()

    def test_exempt_gate_no_product_retry(self):
        qa={'required':True,'qa_type':'DB_CONTRACT','hold_scope':'JOB','reason':'DB unavailable',
            'evidence':['connection unavailable'],'decision_independent_work_complete':True,
            'blocked_gates':['DB_CONNECT','E2E']}
        helper=Patch081Test()
        stdout='HARNESS_QA_REQUEST_JSON: '+json.dumps(qa)
        worker=SimpleNamespace(execute=lambda *args,**kwargs: WorkerResult(
            True,0,stdout,'',[],qa_request=parse_qa_request(stdout)))
        manager=helper._manager(self.workspace,_Collector([]),worker)
        self.addCleanup(manager.shutdown)
        task=helper._task(self.workspace)
        task.requirement+='\n\n'+CONTRACT
        task.materialized_execution={'current_requirement':task.requirement}
        # Binding is independent of this classification test; no success gates run.
        with patch.object(manager,'_binding_failure',return_value=None):
            result=manager._run_pipeline(task)
        self.assertFalse(result.success)
        self.assertEqual('POLICY_EXEMPT_GATE_STOPPED_EXECUTION',task.failure_code)
        self.assertEqual('NON_RETRYABLE',task.fix_scope)
        self.assertEqual(0,task.retry_count)
        self.assertEqual(0,manager.build_test_runner.calls)
        self.assertEqual(0,manager.reviewer.calls)
        task.qa_request=dict(qa,decision_independent_work_complete=False)
        self.assertFalse(exempt_qa_signal(task))
