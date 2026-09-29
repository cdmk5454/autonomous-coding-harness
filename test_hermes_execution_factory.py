import unittest, tempfile, json, threading
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace
from dataclasses import replace, asdict
from hermes_identity import ProviderModelIdentity as Identity
from hermes_execution_factory import HermesExecutionFactory, HermesExecutionConfig, ExecutionScope, OwnedWorker
from hermes_runtime_adapter import HermesError
from test_hermes_runtime_adapter import HermesAdapterTests
from manager import Manager

class FactoryTests(unittest.TestCase):
    def test_provider_model_normalization(self):
        self.assertEqual(Identity.parse('openai-codex/gpt-5.6-luna'), Identity.parse('gpt-5.6-luna',provider='openai-codex'))
    def test_other_provider_not_equivalent(self):
        with self.assertRaises(ValueError):Identity.parse('zai/glm-5',provider='openai-codex')
    def test_bare_without_provider_denied(self):
        with self.assertRaises(ValueError):Identity.parse('gpt-5.6-luna')
    def test_empty_provider_model_denied(self):
        for s in ('/model','provider/','model with space',''):
            with self.subTest(s=s),self.assertRaises(ValueError):Identity.parse(s)
    def test_nonhermes_does_not_allocate(self):
        f=HermesExecutionFactory(None)
        with patch('hermes_execution_factory.ExecutionScope') as allocate:
            for runtime in ('codex','droid','opencode'):
                with self.assertRaisesRegex(HermesError,'SELECTION'):f.bind(SimpleNamespace(worker=runtime),candidate=None,profile=None,policy_root=None)
            allocate.assert_not_called()
    def test_factory_serial_scope(self):
        f=HermesExecutionFactory(None);f._scope=object()
        with self.assertRaisesRegex(HermesError,'BUSY'):f.bind(SimpleNamespace(worker='hermes'),candidate=None,profile=None,policy_root=None)
    def test_owner_missing_still_denied(self):
        m=Manager.__new__(Manager);m.worker_type='hermes';m.choose_worker=lambda _: 'hermes'
        with self.assertRaisesRegex(HermesError,'OWNER_REQUIRED'):m._get_worker(SimpleNamespace())
    def test_creation_failure_does_not_latch_factory(self):
        f=HermesExecutionFactory(None)
        with patch('hermes_execution_factory.ExecutionScope',side_effect=HermesError('ALLOCATION_FAILED')):
            with self.assertRaises(HermesError):f.bind(SimpleNamespace(worker='hermes'),candidate=None,profile=None,policy_root=None)
        self.assertIsNone(f._scope)
    def test_wrong_task_denied_before_launch(self):
        s=ExecutionScope.__new__(ExecutionScope);s.closed=False;s.task=object()
        with self.assertRaisesRegex(HermesError,'FOREIGN_TASK'):s._create(object(),'WORKER',requirement='x')
    def test_fingerprint_mismatch_precedes_process(self):
        s=ExecutionScope.__new__(ExecutionScope);s.config=SimpleNamespace(image='sha256:abc',fingerprints={x['path']:'wrong' for x in json.loads(Path(r'D:\hermes\evidence\STEP5-CANDIDATE-20260919T202324+0900\runtime-fingerprints.json').read_text())},proxy=Path('missing'))
        with patch('hermes_execution_factory.sha',return_value='actual'),patch('hermes_execution_factory.subprocess.run') as docker:
            with self.assertRaisesRegex(HermesError,'FINGERPRINT'):s._check_runtime()
            docker.assert_not_called()
    def test_release_unknown_not_hidden(self):
        with tempfile.TemporaryDirectory() as d:
            b=Mock();b.root=Path(d);b.fence.return_value={'state':'UNKNOWN'}
            s=ExecutionScope.__new__(ExecutionScope)
            with self.assertRaisesRegex(HermesError,'QUIESCENCE'):s.release(b)
    def test_cleanup_failure_retains_owner(self):
        with tempfile.TemporaryDirectory() as d:
            s=ExecutionScope.__new__(ExecutionScope);s.closed=False;s.path=Path(d);s.owners=[object()];s.logs=[];s._config_bytes=None
            s.release=Mock(side_effect=HermesError('FENCE_FAILURE'))
            s.factory=SimpleNamespace(_guard=threading.Lock(),_scope=s)
            with self.assertRaisesRegex(HermesError,'CLEANUP_FAILED'):s.close()
            self.assertFalse(s.closed);self.assertIs(s.factory._scope,s)
            self.assertEqual('FAILED',json.loads((s.path/'cleanup.json').read_text())['state'])
    def test_worker_exception_finally_releases(self):
        w=OwnedWorker(Mock());w.scope=Mock()
        with patch('hermes_execution_factory.HermesWorker.execute',side_effect=RuntimeError('harness exception')):
            with self.assertRaises(RuntimeError):w.execute('x','x')
        w.scope.release.assert_called_once_with(w.adapter.boundary)
    def test_cleanup_qa_and_failure_result(self):
        w=OwnedWorker(Mock());w.scope=Mock()
        for result in (SimpleNamespace(success=False,qa_request={'required':True}),SimpleNamespace(success=False),SimpleNamespace(success=True)):
            with patch('hermes_execution_factory.HermesWorker.execute',return_value=result):self.assertIs(result,w.execute('x','x'))
        self.assertEqual(3,w.scope.release.call_count)

class QualifiedAttemptTests(HermesAdapterTests):
    def test_qualified_attempt_model_dispatch(self):
        aid='qualified'
        self.repo.start_attempt(self.f.execution,attempt_id=aid,role='WORKER',model='zai/'+self.b.model)
        self.a.binding=replace(self.b,attempt_id=aid,command_id='qualified-cmd')
        self.assertEqual(self.b.model,self.a.create_session('fixture',cwd=self.boundary.candidate,model=self.b.model,command_id='qualified-cmd').body['model'])
    def test_foreign_qualified_provider_rejected(self):
        aid='foreign-provider'
        self.repo.start_attempt(self.f.execution,attempt_id=aid,role='WORKER',model='opencode-go/'+self.b.model)
        self.a.binding=replace(self.b,attempt_id=aid)
        with self.assertRaisesRegex(HermesError,'MODEL_BINDING'):self.invocation()

class ContextAdmissionTests(unittest.TestCase):
    def setUp(self):
        from test_execution_context import ExecutionContextTests
        from execution_context import materialize_execution_context
        from hermes_runtime_adapter import HermesBinding
        from hermes_boundary import HermesBoundaryOwner,sha
        self.f=ExecutionContextTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        for name in ('root','policy','profile','workspace'):setattr(self.f,name,getattr(self.f,name).resolve())
        pack=self.f.build();self.pack=pack
        root=self.f.root/'boundary';root.mkdir();context=self.f.root/'context'
        materialize_execution_context(pack,context)
        b=HermesBinding('J1','E1','A1','B1','CMD1','C1',pack.manifest['pack_sha256'],'epoch','inc','zai','model','WORKER','host')
        policy={'binding':asdict(b),'fingerprints':{}}
        (root/'policy.json').write_text(json.dumps(policy));(root/'state.json').write_text(json.dumps({'state':'ACTIVE','epoch':'epoch'}))
        self.boundary=HermesBoundaryOwner.__new__(HermesBoundaryOwner)
        self.boundary.root=root;self.boundary.context_pack=context;self.boundary.binding=b
        self.boundary.policy=policy;self.boundary.policy_digest=sha(root/'policy.json')
        self.assertEqual('NOT_GRANTED',self.boundary.validate_binding(b)['contract']['runtime_admission'])
    def test_context_digest_mismatch(self):
        p=self.boundary.context_pack/'context.md';p.write_text(p.read_text()+'tampered')
        with self.assertRaisesRegex(HermesError,'CONTEXT_DIGEST'):self.boundary.validate_binding(self.boundary.binding)
    def test_stale_epoch(self):
        (self.boundary.root/'state.json').write_text(json.dumps({'state':'ACTIVE','epoch':'old'}))
        with self.assertRaisesRegex(HermesError,'STALE_EPOCH'):self.boundary.validate_binding(self.boundary.binding)
    def test_policy_tamper(self):
        with (self.boundary.root/'policy.json').open('a') as f:f.write(' ')
        with self.assertRaisesRegex(HermesError,'POLICY_TAMPERED'):self.boundary.validate_binding(self.boundary.binding)
    def test_frozen_snapshot_schema_consumption(self):
        from execution_context import build_execution_context_pack
        p=self.f.policy/'profiles'/'project-profile.schema.json'
        frozen=self.f.root/'frozen-schema.json';p.rename(frozen)
        pack=build_execution_context_pack(contract=self.f.c,requirement=self.f.requirement,profile_dir=self.f.profile,workspace=self.f.workspace,policy_root=self.f.policy,schema_file=frozen)
        self.assertEqual('NOT_GRANTED',pack.manifest['contract']['runtime_admission'])


class AvailabilityTests(unittest.TestCase):
    def scope(self):
        from hermes_execution_factory import RUNTIME,DOCKER
        files=('tools/environments/docker.py','tools/credential_files.py','tools/terminal_tool_backends.py','tools/code_execution_tool.py','tools/file_tools.py','pyproject.toml','hermes_cli/__init__.py')
        s=ExecutionScope.__new__(ExecutionScope)
        s.config=SimpleNamespace(image='sha256:fixture',fingerprints={**{str(RUNTIME/r):'pin' for r in files},DOCKER:'pin'},proxy=Path('proxy'))
        return s
    def test_missing_runtime_file(self):
        with patch('pathlib.Path.is_file',return_value=False):
            with self.assertRaisesRegex(HermesError,'PINNED_RUNTIME_UNAVAILABLE'):self.scope()._check_runtime()
    def test_docker_unavailable(self):
        with patch('pathlib.Path.is_file',return_value=True),patch('hermes_execution_factory.sha',return_value='pin'),patch('hermes_execution_factory.subprocess.run',side_effect=OSError):
            with self.assertRaisesRegex(HermesError,'DOCKER_UNAVAILABLE'):self.scope()._check_runtime()
    def test_missing_fingerprint_list(self):
        s=self.scope();s.config.fingerprints={}
        with self.assertRaisesRegex(HermesError,'PINNED_RUNTIME_REQUIRED'):s._check_runtime()
