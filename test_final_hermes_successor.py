"""Hermes successor selection without gateway, Docker, or provider operations."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from hermes_execution_factory import ExecutionScope
from hermes_runtime_adapter import HermesError
from hermes_worker import HermesWorker
from task_state import TaskState


class HermesSuccessorSelectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='hermes-successor-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.task = TaskState(task_id='TASK-FIXTURE', requirement='Change app/a.py', worker='hermes')
        self.task.job_id = 'JOB-FIXTURE'
        self.task.execution_id = 'PRODUCT-EXECUTION'
        self.task.attempt_id = 'WORKER-ATTEMPT'
        self.task.reviewer_attempt_id = 'REVIEWER-ATTEMPT'
        self.task.tester_attempt_id = 'TESTER-ATTEMPT'
        self.task.reviewer_execution = {'execution_id': 'REVIEWER-SUCCESSOR'}
        self.task.target_module = ['app']
        self.task.effective_policy = {key: {'reasoning_effort': 'low'}
            for key in ('resolved_worker', 'resolved_reviewer', 'resolved_tester')}
        self.task.materialized_execution = {'execution_id': self.task.execution_id,
            'request': {'requirement': self.task.requirement, 'target_resources': ['app/a.py']}}
        self.scope = ExecutionScope.__new__(ExecutionScope)
        self.scope.task = self.task
        self.scope.closed = False
        self.scope.root = self.root
        self.scope.path = self.root/'allocation'
        self.scope.candidate = self.root/'candidate'
        self.scope.source = self.root/'source'
        for path in (self.scope.path, self.scope.candidate, self.scope.source):
            path.mkdir()
        (self.scope.candidate/'a.py').write_bytes(b'candidate')
        (self.scope.source/'a.py').write_bytes(b'original')
        self.scope.candidate_id = 'ORIGINAL-CANDIDATE'
        self.task.candidate_id = self.scope.candidate_id
        self.task.candidate_manifest = str(self.scope.path/'allocation.json')
        self.scope.profile = SimpleNamespace(id='fixture', profile_dir=self.root/'profile')
        self.scope.policy_root = self.root/'policy'
        self.scope.config = SimpleNamespace(image='sha256:fixture', fingerprints={},
                                             proxy=self.root/'proxy', port=18000)
        self.gateway = SimpleNamespace(key='disposable-fixture', attach=Mock())
        self.scope.factory = SimpleNamespace(shared_gateway=self.gateway, approval_authorizer=None)
        self.scope.owners = []
        self.scope._attempts = set()
        self.scope.repository = Mock()
        self.scope._check_runtime = Mock()
        self.scope._handoff = Mock()

    def create(self, role, expected_execution):
        aid = {'WORKER': self.task.attempt_id, 'REVIEWER': self.task.reviewer_attempt_id,
               'TESTER': self.task.tester_attempt_id}[role]
        self.scope.repository.latest_attempt.side_effect = lambda execution: (
            {'attempt_id': aid, 'role': role, 'model': 'zai/fixture-model'}
            if execution == expected_execution else None)
        original = copy.deepcopy(self.task.materialized_execution)
        custody = (self.task.execution_id, self.task.candidate_id, self.task.candidate_manifest)
        with patch('hermes_execution_factory.build_execution_context_pack',
                   return_value=SimpleNamespace(manifest={'pack_sha256': 'fixture-digest'})) as pack, \
                patch('hermes_execution_factory.materialize_execution_context'), \
                patch('hermes_execution_factory.HermesBoundaryOwner') as boundary, \
                patch('hermes_execution_factory.HermesRuntimeAdapter') as adapter, \
                patch('hermes_execution_factory.identity', return_value={'pid': 'fixture'}), \
                patch('hermes_execution_factory.OwnedProcess',
                      side_effect=AssertionError('must not launch gateway')) as process:
            runner = self.scope._create(self.task, role, requirement=self.task.requirement)
        self.scope.repository.latest_attempt.assert_called_once_with(expected_execution)
        contract = pack.call_args.kwargs['contract']
        binding = adapter.call_args.kwargs['binding']
        self.assertEqual(expected_execution, contract.execution_id)
        self.assertEqual(expected_execution, binding.execution_id)
        self.assertEqual(aid, binding.attempt_id)
        self.assertEqual(role, binding.role)
        self.assertEqual('ORIGINAL-CANDIDATE', contract.candidate_id)
        self.assertEqual('ORIGINAL-CANDIDATE', binding.candidate_id)
        self.assertEqual(self.scope.candidate, pack.call_args.kwargs['workspace'])
        self.assertEqual(self.scope.candidate, boundary.call_args.kwargs['candidate'])
        self.assertEqual(self.scope.root, boundary.call_args.kwargs['allocation_root'])
        self.assertEqual(self.scope.source if role != 'WORKER' else None,
                         boundary.call_args.kwargs['source'])
        self.assertEqual(original, self.task.materialized_execution)
        self.assertEqual(custody, (self.task.execution_id, self.task.candidate_id,
                                  self.task.candidate_manifest))
        self.assertEqual(b'candidate', (self.scope.candidate/'a.py').read_bytes())
        self.assertEqual(b'original', (self.scope.source/'a.py').read_bytes())
        self.assertIs(self.scope, runner.scope)
        self.gateway.attach.assert_called_once()
        process.assert_not_called()

    def test_reviewer_uses_successor_attempt_context_and_binding(self):
        self.create('REVIEWER', 'REVIEWER-SUCCESSOR')

    def test_worker_keeps_product_execution_with_successor_present(self):
        self.create('WORKER', 'PRODUCT-EXECUTION')

    def test_tester_keeps_product_execution_with_successor_present(self):
        self.create('TESTER', 'PRODUCT-EXECUTION')

    def test_reviewer_without_successor_keeps_original_execution(self):
        self.task.reviewer_execution = {}
        self.create('REVIEWER', 'PRODUCT-EXECUTION')

    def test_reviewer_with_empty_successor_id_keeps_original_execution(self):
        self.task.reviewer_execution = {'execution_id': ''}
        self.create('REVIEWER', 'PRODUCT-EXECUTION')

    def test_missing_successor_attempt_never_falls_back_to_product_attempt(self):
        self.scope.repository.latest_attempt.side_effect = lambda execution: (
            {'attempt_id': self.task.reviewer_attempt_id, 'role': 'REVIEWER', 'model': 'zai/fixture'}
            if execution == 'PRODUCT-EXECUTION' else None)
        with patch('hermes_execution_factory.HermesBoundaryOwner') as boundary:
            with self.assertRaisesRegex(HermesError, 'FOREIGN_ATTEMPT'):
                self.scope._create(self.task, 'REVIEWER', requirement=self.task.requirement)
        self.scope.repository.latest_attempt.assert_called_once_with('REVIEWER-SUCCESSOR')
        boundary.assert_not_called()

    def dispatch_guard(self, role, execution_id, *, admitted):
        aid = {'REVIEWER': self.task.reviewer_attempt_id,
               'TESTER': self.task.tester_attempt_id, 'WORKER': self.task.attempt_id}[role]
        worker = HermesWorker.__new__(HermesWorker)
        worker.adapter = SimpleNamespace(binding=SimpleNamespace(role=role,
            execution_id=execution_id, attempt_id=aid),
            boundary=SimpleNamespace(candidate=self.scope.candidate))
        with patch('hermes_worker.snapshot', side_effect=RuntimeError('identity-admitted')) as snapshot:
            if admitted:
                with self.assertRaisesRegex(RuntimeError, 'identity-admitted'):
                    worker._execute('fixture', str(self.scope.candidate), self.task)
                snapshot.assert_called_once_with(self.scope.candidate)
            else:
                with self.assertRaisesRegex(HermesError, 'WORKER_DISPATCH_CONTEXT_MISSING'):
                    worker._execute('fixture', str(self.scope.candidate), self.task)
                snapshot.assert_not_called()

    def test_downstream_reviewer_dispatch_accepts_successor_identity(self):
        self.dispatch_guard('REVIEWER', 'REVIEWER-SUCCESSOR', admitted=True)

    def test_downstream_worker_rejects_reviewer_identity(self):
        self.dispatch_guard('WORKER', 'REVIEWER-SUCCESSOR', admitted=False)

    def test_downstream_tester_rejects_reviewer_identity(self):
        self.dispatch_guard('TESTER', 'REVIEWER-SUCCESSOR', admitted=False)

    def test_downstream_worker_keeps_product_identity(self):
        self.dispatch_guard('WORKER', 'PRODUCT-EXECUTION', admitted=True)

    def test_downstream_reviewer_empty_successor_falls_back(self):
        self.task.reviewer_execution = {'execution_id': ''}
        self.dispatch_guard('REVIEWER', 'PRODUCT-EXECUTION', admitted=True)


if __name__ == '__main__':
    unittest.main()
