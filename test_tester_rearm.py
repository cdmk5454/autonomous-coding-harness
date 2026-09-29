import copy
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from pathlib import Path
import hashlib
import harness_temp
from task_state import TaskState
from tester_lifecycle import run_recovery_attempt
from tester import TesterResult, _apply_to_state
from manager import Manager


class TesterRearmTests(unittest.TestCase):
    def task(self):
        t=TaskState(task_id='T',requirement='fixture',worker='opencode')
        t.candidate_id='C';t.effective_policy={'resolved_tester':{'runtime':'droid','model':'same','timeout':600}}
        t.tester_attempt_history=[dict(attempt_id='original',test_status='ERROR',execution={'stdout':'bad'}),
            dict(attempt_id='retry',test_status='ERROR',kind='TESTER_TECHNICAL_RECOVERY',execution={'timeout':600})]
        t.technical_execution_retry_count=1
        return t

    def test_one_rearm_preserves_history_candidate_and_semantic_count(self):
        t=self.task();old=copy.deepcopy(t.tester_attempt_history)
        def run():
            t.official_tester_invoked=True
            _apply_to_state(t,TesterResult('PASS','verified',['fixture']))
        self.assertEqual('PASS',run_recovery_attempt(t,run,operator_rearm=True))
        self.assertEqual(old,t.tester_attempt_history[:2]);self.assertEqual('C',t.candidate_id)
        self.assertEqual(0,t.product_semantic_retry_count);self.assertEqual(2,t.technical_execution_retry_count)
        with self.assertRaisesRegex(ValueError,'REARM_EXHAUSTED'):
            run_recovery_attempt(t,run,operator_rearm=True)

    def test_unarmed_retry_denied(self):
        run=Mock()
        with self.assertRaisesRegex(ValueError,'LIMIT_EXHAUSTED'):run_recovery_attempt(self.task(),run)
        run.assert_not_called()

    def test_repeat_error_is_bounded(self):
        t=self.task()
        def run():_apply_to_state(t,TesterResult('ERROR','timeout'))
        self.assertEqual('ERROR',run_recovery_attempt(t,run,operator_rearm=True))
        with self.assertRaisesRegex(ValueError,'REARM_EXHAUSTED'):run_recovery_attempt(t,run,operator_rearm=True)

    def test_manager_reuses_build_and_same_candidate_for_review(self):
        with harness_temp.TemporaryDirectory() as d:
            root=Path(d);(root/'A.java').write_bytes(b'source')
            t=self.task();t.changed_files=['A.java'];t.build_evidence={'success':True,'status':'PASS',
                'details':{'source_sha256':{'A.java':hashlib.sha256(b'source').hexdigest()}}}
            m=object.__new__(Manager);m.working_dir=d;m.profile=None;m.policy_root=None
            m._validate_structured_build_evidence=Mock(return_value=(True,''))
            m.build_test_runner=Mock();m.run_review_only=Mock(return_value='review')
            def run(t,*a,**kw):
                t.official_tester_invoked=True;_apply_to_state(t,TesterResult('PASS','ok',['fixture']))
            with patch('manager.run_tester',side_effect=run):
                self.assertEqual('review',m.run_tester_rearm(t))
            m.build_test_runner.run.assert_not_called();m.run_review_only.assert_called_once_with(t)
            self.assertEqual('C',t.candidate_id)
            self.assertEqual('PASS',t.gate_evidence['BUILD']['status'])
            self.assertEqual('PASS',t.gate_evidence['TEST']['status'])
