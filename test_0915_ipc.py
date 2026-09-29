import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from worker import CodexWorker, codex_host_pair_matches
from task_state import TaskState
from test_gate_scenarios import GateScenarioTest, FakeWorker, FakeCollector, FakeRunner, FakeReviewer, _git, _build
from reviewer import ReviewResult, REVIEW_PASS


class IPCGuardTests(unittest.TestCase):
    def test_a_installed_host_match(self):
        self.assertTrue(codex_host_pair_matches(str(Path(__file__).parent / 'trusted-bin/codex.exe')))

    def result(self, stderr='', host_match=True):
        with tempfile.TemporaryDirectory() as td, \
                patch('worker.build_prompt', return_value='scratch read'), \
                patch('worker.trusted_executable', return_value='codex.exe'), \
                patch('worker.codex_host_pair_matches', return_value=host_match), \
                patch('worker.subprocess.run', return_value=subprocess.CompletedProcess([], 0, 'done', stderr)) as run:
            result = CodexWorker().execute('scratch read', td)
            self.assertEqual(1 if host_match else 0, run.call_count)
            return result

    def test_c_exit_zero_ipc_failure_no_delta_no_retry(self):
        wr = self.result('ERROR codex_core::tools::router: failed to decode code-mode IPC frame: missing field `code_mode_host_duration_ns`')
        self.assertFalse(wr.success)
        self.assertEqual(0, wr.exit_code)
        self.assertEqual('WORKER_IPC_DECODE_ERROR', wr.execution_failure_code)
        fixture = GateScenarioTest()
        fixture.setUp()
        try:
            m = fixture.manager
            m.max_retry = 3
            w = FakeWorker(wr)
            m._get_worker = lambda _: (w, 'codex')
            m.collector = FakeCollector(_git())
            m.build_test_runner = FakeRunner(_build('PASS'))
            m.reviewer = FakeReviewer(ReviewResult(passed=True, status=REVIEW_PASS))
            task = TaskState(task_id='TASK-IPC-GUARD', requirement='scratch', working_dir=fixture.working_dir, task_mode='MODIFICATION')
            with patch.object(m.collector, 'collect', side_effect=AssertionError('SOURCE DELTA MUST NOT RUN')):
                result = m.run_with_retry(task)
            self.assertFalse(result.success)
            self.assertEqual('WORKER_IPC_DECODE_ERROR', task.failure_code)
            self.assertEqual('HARNESS_CAUSED', task.failure_origin)
            # 0.99.2.2: an infrastructure Worker failure with no QA subject
            # projects technical FAILED, not the AWAITING_QA human gate.
            self.assertEqual('FAILED', task.status)
            self.assertEqual(0, task.retry_count)
            self.assertEqual(1, w.calls)
            self.assertEqual([], m.build_test_runner.calls)
            self.assertEqual(0, m.reviewer.calls)
            self.assertEqual(0, m.collector.rollback_calls)
            self.assertEqual([], task.attempt_history)
        finally:
            fixture.tearDown()

    def test_surface_mismatch_prevents_invocation(self):
        wr = self.result(host_match=False)
        self.assertFalse(wr.success)
        self.assertEqual('CODEX_EXECUTION_SURFACE_MISMATCH', wr.execution_failure_code)

    def test_existing_success_unchanged(self):
        self.assertTrue(self.result().success)


if __name__ == '__main__':
    unittest.main()
