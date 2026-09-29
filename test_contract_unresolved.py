import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import harness_temp
from test_081_patch import Patch081Test, _Collector
from worker import WorkerResult, parse_qa_request
from queue_supervisor import QueueSupervisor


class ContractUnresolvedTests(unittest.TestCase):
    def test_contract_hold_never_retries_or_builds(self):
        with harness_temp.TemporaryDirectory() as directory:
            root=Path(directory)
            qa={'required':True,'qa_type':'DB_CONTRACT','hold_scope':'JOB',
                'reason':'Physical COURSE_CD mapping is unresolved',
                'evidence':['offline mapper/catalog disagree'],'decision_independent_work_complete':False}
            stdout='HARNESS_QA_REQUEST_JSON: '+json.dumps(qa)
            worker=Mock()
            worker.execute.return_value=WorkerResult(True,0,stdout,'',[],qa_request=parse_qa_request(stdout))
            helper=Patch081Test()
            manager=helper._manager(root,_Collector([]),worker)
            self.addCleanup(manager.shutdown)
            task=helper._task(root)
            result=manager.run_with_retry(task)
            self.assertFalse(result.success)
            self.assertEqual('CONTRACT_UNRESOLVED',task.failure_code)
            self.assertEqual('USER_QA',task.failure_origin)
            self.assertEqual('AWAITING_QA',task.status)
            self.assertEqual(0,task.retry_count)
            self.assertEqual(1,worker.execute.call_count)
            self.assertEqual([],task.attempt_history)
            self.assertEqual(0,manager.collector.rollbacks)
            self.assertEqual(0,manager.build_test_runner.calls)
            self.assertEqual(0,manager.reviewer.calls)

    def test_no_candidate_quarantine_for_proven_empty_contract_hold(self):
        supervisor=object.__new__(QueueSupervisor)
        supervisor.harness=SimpleNamespace(task_root=Path('unused'))
        task=SimpleNamespace(failure_code='CONTRACT_UNRESOLVED',status='AWAITING_QA',
            changed_files=[],task_owned_changed_files=[],candidate_id='',
            no_task_delta_evidence={'passed':True})
        result={'task_id':'TASK-CONTRACT','status':'AWAITING_QA','success':False,
            'failure_code':'CONTRACT_UNRESOLVED','qa_request':{'required':True}}
        candidates=Mock()
        with patch('queue_supervisor.TaskState.load',return_value=task):
            self.assertEqual(result,supervisor._quarantine_machine_verified_result({},result,candidates))
        candidates.quarantine_and_rollback.assert_not_called()

    def test_ordinary_no_delta_guard_unchanged(self):
        with harness_temp.TemporaryDirectory() as directory:
            root=Path(directory)
            worker=Mock()
            worker.execute.return_value=WorkerResult(True,0,'done','',[])
            helper=Patch081Test()
            manager=helper._manager(root,_Collector([]),worker)
            self.addCleanup(manager.shutdown)
            task=helper._task(root)
            manager._run_pipeline(task)
            self.assertEqual('NO_MEANINGFUL_SOURCE_CHANGE',task.failure_code)
