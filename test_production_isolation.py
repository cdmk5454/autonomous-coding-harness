"""Disposable deterministic composition checks; no provider calls or Product IDs."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from bounded_execution import migrate_disposable
from chatgpt_control import ChatGPTControlService
from control_repository import ControlRepository
from job_queue import JobStore
from hermes_boundary import atomic, read
from production_isolation import (
    ExecutionCustody, MODEL, ProductionIsolation, inventory,
    normalized_preserved_inventory,
)
from qa_quarantine import CandidateError
from task_state import TaskState


class CompositionTests(unittest.TestCase):
    def setUp(self):
        notifications = patch.dict(os.environ, {'KKM_TG_ENABLED':'0'})
        notifications.start()
        self.addCleanup(notifications.stop)
        base = Path(r'D:\hermes\isolation')
        base.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='rdy-compose-', dir=base)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root/'workspace'
        (self.workspace/'app').mkdir(parents=True)
        (self.workspace/'app/a.py').write_text('value = 1\n')
        (self.workspace/'app/.env').write_text('fixture-local-only')
        import subprocess
        subprocess.run(['git','-C',str(self.workspace/'app'),'init','-q'],check=True,capture_output=True)
        self.profile = self.root/'rdy'
        (self.profile/'ui_map').mkdir(parents=True)
        for name in ('rules.md', 'analysis.md', 'ui_map/index.md'):
            (self.profile/name).write_text('# Generic disposable fixture\n')
        atomic(self.profile/'project.json', {
            'schema_version': 1, 'id': 'rdy', 'display_name': 'Disposable',
            'workspace_roots': [str(self.workspace)],
            'context': {'project_rules':'rules.md','analysis':'analysis.md','ui_map':'ui_map'},
            'modules':[{'name':'app','path':'app','kind':'backend','protected':False}],
            'rules':{'always':['rules.md'],'backend':[],'frontend':[],'sql':[],'architecture':[],'egov':[]},
            'area_patterns':{'backend':['app/**'],'frontend':[],'sql':[],'architecture':[]},'protected_paths':['app/protected.py'],
            'local_dev_config_excludes':['.env'],
        })
        control = self.root/'.control'
        control.mkdir()
        self.repo = ControlRepository(control/'control.sqlite3', create=True)
        self.repo.bootstrap(JobStore._empty_queue(), {}, {})
        atomic(control/'sqlite-authority.json', {'authority':'SQLITE','schema_version':5})
        self.control = ChatGPTControlService(self.workspace, self.profile,
            control_root=control, task_root=self.root/'.tasks', state_base=self.root,
            isolation_root=self.root/'custody')

    def schema6(self):
        migrate_disposable(self.repo.path, allocation_root=self.root, backup_path=self.root/'schema5.bak')
        import gc
        gc.collect()  # existing disposable migrator retains its backup connection until GC

    def allocate(self):
        self.schema6()
        return self.control.isolation({'job_id':'RDY-A','request':{'worker':'opencode','model':MODEL}},
                                      self.control.store.repository)

    def task(self, child):
        task = TaskState(task_id='RDY-T', requirement='Change app/a.py', worker='opencode',
            opencode_model=MODEL, target_module=['app'])
        task.job_id, task.execution_id, task.attempt_id = 'RDY-A', 'RDY-E', 'RDY-W'
        task.target_resources = ['app/a.py']
        child.isolated_execution.bind(task)
        return task

    def fake_adapter(self, custody, *, quiesced=True):
        class Adapter:
            def __init__(self, root, execution, epoch, candidate, job, attempt):
                self.root = root
                self.stopped = False
                self.identity = dict(execution_id=execution, execution_epoch=epoch,
                    candidate_id=candidate, job_id=job, attempt_id=attempt)
            def stop_local_server(self): self.stopped = True
            def confirm_quiescence(self): return self.stopped and quiesced
            def boundary_receipt(self):
                return {**self.identity,'quiesced':self.confirm_quiescence(),'boundary':'docker-opencode/1'}
        return patch.object(custody.owner, 'adapter', side_effect=Adapter)

    def test_actual_constructor_composes_and_schema5_rejects_before_claim(self):
        self.assertIsInstance(self.control.isolation, ProductionIsolation)
        self.assertIs(self.control.supervisor.execution_service_factory, self.control.isolation)
        self.assertIs(self.control.supervisor.canonical_promoter, self.control.isolation.promoter)
        with patch.object(self.control.store, 'claim_next', side_effect=AssertionError('claimed')):
            result = self.control.supervisor.start()
        self.assertFalse(result['started'])
        self.assertEqual('BOUNDED_SCHEMA_REQUIRED', result['code'])
        self.assertEqual([], self.repo.worklist())

    def test_missing_boundary_rejects_before_claim(self):
        self.schema6()
        self.control.isolation.runtime_root = self.root/'absent-runtime'
        with patch.object(self.control.store, 'claim_next', side_effect=AssertionError('claimed')):
            result = self.control.supervisor.start()
        self.assertFalse(result['started'])
        self.assertIn('PATH_MISSING', result['code'])

    def test_allocation_preserves_source_and_local_config_and_has_separate_lease(self):
        before = inventory(self.workspace, include_git=True)
        child = self.allocate()
        self.assertEqual(before, inventory(child.working_dir, include_git=True))
        self.assertEqual(before, inventory(self.workspace, include_git=True))
        self.assertNotEqual(self.control.harness._instance_lock.path, child._instance_lock.path)
        loaded, _ = child._load_profile()
        self.assertIn(child.working_dir, loaded.workspace_roots)

    def test_supervisor_advisor_survives_isolated_child_allocation(self):
        from supervisor_proposal import HermesSupervisorAdvisor
        advisor = HermesSupervisorAdvisor(
            invoke_json=lambda _: '{}', provider='zai', model='glm-5',
            runtime_version='runtime-v2026.9.14', adapter_revision='local-no-tools/1',
        )
        self.control.harness.supervisor_advisor = advisor
        child = self.allocate()
        self.assertIs(child.supervisor_advisor, advisor)

    def test_wrong_route_rejects_without_allocation(self):
        self.schema6()
        with self.assertRaisesRegex(CandidateError,'ISOLATED_WORKER_ROUTE_REQUIRED'):
            self.control.isolation({'job_id':'RDY-A','request':{'worker':'codex','model':'x'}}, self.control.store.repository)
        self.assertFalse(list((self.root/'custody').glob('run-*')))

    def test_sealed_handoff_and_new_epoch_per_work(self):
        child = self.allocate()
        task = self.task(child)
        custody = child.isolated_execution
        test = self
        class Worker:
            def __init__(self, **kwargs): self.adapter = kwargs['adapter_factory']()
            def execute(self, requirement, working_dir, task):
                path = Path(working_dir)
                test.assertFalse((path/'app/.env').exists())
                test.assertFalse((path/'.git').exists())
                (path/'app/a.py').write_text('value = '+task.attempt_id+'\n')
                return SimpleNamespace(success=True)
        with self.fake_adapter(custody):
            custody.work(Worker, {}, task.requirement, task)
            first = task.execution_epoch
            task.attempt_id = 'RDY-W2'
            custody.work(Worker, {}, task.requirement, task)
        self.assertNotEqual(first, task.execution_epoch)
        self.assertTrue(all(adapter.confirm_quiescence() for adapter in custody.adapters))
        self.assertEqual('value = RDY-W2\n', (child.working_dir/'app/a.py').read_text())
        self.assertEqual('value = 1\n', (self.workspace/'app/a.py').read_text())
        self.assertEqual('RDY-W2', read(custody.root/'cleanup.json')['attempt_id'])

    def test_false_success_failure_and_unquiesced_writer_never_import(self):
        child = self.allocate()
        task = self.task(child)
        custody = child.isolated_execution
        class Worker:
            def __init__(self, **kwargs): pass
            def execute(self, *args, **kwargs): return SimpleNamespace(success=True)
        with self.fake_adapter(custody), self.assertRaisesRegex(CandidateError,'NO_MEANINGFUL_DELTA'):
            custody.work(Worker, {}, '', task)
        class FailedWorker(Worker):
            def execute(self, requirement, working_dir, task):
                (Path(working_dir)/'app/a.py').write_text('untrusted failure')
                return SimpleNamespace(success=False)
        with self.fake_adapter(custody):
            self.assertFalse(custody.work(FailedWorker, {}, '', task).success)
        with self.fake_adapter(custody, quiesced=False), self.assertRaisesRegex(CandidateError,'NOT_QUIESCED'):
            custody.work(FailedWorker, {}, '', task)
        self.assertEqual('value = 1\n', (child.working_dir/'app/a.py').read_text())

    def test_undeclared_protected_secret_and_hardlink_writes_rejected(self):
        child = self.allocate()
        task = self.task(child)
        custody = child.isolated_execution
        for name in ('other.py','protected.py','.env','hard.py'):
            class Worker:
                def __init__(self, **kwargs): pass
                def execute(self, requirement, working_dir, task):
                    path = Path(working_dir)/'app'/name
                    if name == 'hard.py': os.link(path.parent/'a.py',path)
                    else: path.write_text('forbidden')
                    return SimpleNamespace(success=True)
            with self.fake_adapter(custody), self.assertRaises(CandidateError):
                custody.work(Worker, {}, '', task)
        self.assertEqual('value = 1\n', (child.working_dir/'app/a.py').read_text())

    def test_host_drift_and_unfinished_custody_fence(self):
        child = self.allocate()
        task = self.task(child)
        class Worker:
            def __init__(self, **kwargs): pass
            def execute(self, *args, **kwargs):
                (child.working_dir/'app/a.py').write_text('external edit')
                return SimpleNamespace(success=True)
        with self.fake_adapter(child.isolated_execution), self.assertRaisesRegex(CandidateError,'HOST_INPUT_CHANGED'):
            child.isolated_execution.work(Worker, {}, '', task)
        with self.assertRaisesRegex(CandidateError,'CUSTODY_RECOVERY_REQUIRED'):
            self.control.isolation.preflight()

    def test_uncertain_publication_fences_restart(self):
        self.schema6()
        atomic(self.control.isolation.promoter.root/'RDY-E-intent.json', {'state':'PUBLISHING'})
        with self.assertRaisesRegex(CandidateError,'PUBLICATION_RECOVERY_REQUIRED'):
            self.control.isolation.preflight()

    def test_publication_requires_exact_custody_and_gates_and_rejects_replay(self):
        child = self.allocate()
        task = self.task(child)
        task.changed_files = ['app/a.py']
        class Worker:
            def __init__(self, **kwargs): pass
            def execute(self, requirement, working_dir, task):
                (Path(working_dir)/'app/a.py').write_text('value = 2\n')
                return SimpleNamespace(success=True)
        with self.fake_adapter(child.isolated_execution):
            child.isolated_execution.work(Worker, {}, '', task)
        task.save(child.task_root)
        job = {'job_id':task.job_id,'materialized_execution':{'execution_id':task.execution_id}}
        result = {'task_id':task.task_id,'success':True}
        promoter = self.control.isolation.promoter
        with self.assertRaisesRegex(CandidateError,'CANONICAL_MACHINE_GATES_REQUIRED'):
            promoter(job,child,result)
        cleanup_path = child.isolated_execution.root/'cleanup.json'
        original = read(cleanup_path)
        for field in ('attempt_id','execution_id','candidate_id','execution_epoch','boundary'):
            atomic(cleanup_path,{**original,field:'wrong-owner'})
            with self.assertRaises(CandidateError):
                promoter._task(job,child,result)
        atomic(cleanup_path,original)
        atomic(promoter.root/(task.execution_id+'-intent.json'),{'state':'PUBLISHED'})
        with self.assertRaisesRegex(CandidateError,'PUBLICATION_REPLAY_DENIED'):
            promoter(job,child,result)
        self.assertEqual('value = 1\n',(self.workspace/'app/a.py').read_text())

    def test_original_profile_drift_remains_visible(self):
        child = self.allocate()
        (self.profile/'rules.md').write_text('changed source profile')
        with self.assertRaisesRegex(CandidateError,'PROFILE_DRIFT'):
            self.task(child)

    def test_effective_git_worktree_escape_rejects_before_canonical_git_writes(self):
        import subprocess
        self.schema6()
        app = self.workspace/'app'
        def git(*args):
            return subprocess.run(['git','-C',str(app),*args],check=True,capture_output=True)
        git('config','core.autocrlf','false')
        git('add','a.py')
        git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgsign=false','commit','-qm','fixture')
        (app/'a.py').write_text('accepted dirty baseline\n')
        for include in (False, True):
            if include:
                git('config','--unset','core.worktree')
                external = self.root/'included.gitconfig'
                external.write_text('[core]\n\tworktree = '+app.as_posix()+'\n')
                git('config','include.path',str(external))
            else:
                git('config','core.worktree',str(app))
            before = inventory(self.workspace,include_git=True)
            with self.assertRaisesRegex(CandidateError,'CANDIDATE_GIT_OWNERSHIP_INVALID'):
                self.control.isolation({'job_id':'RDY-ESCAPE','request':{'worker':'opencode','model':MODEL}},self.control.store.repository)
            self.assertEqual(before,inventory(self.workspace,include_git=True))
            self.assertEqual({},self.control.isolation.promoter.allocations)

    def test_nested_module_escape_is_rejected_when_workspace_has_git(self):
        import subprocess
        subprocess.run(['git','-C',str(self.workspace),'init','-q'],check=True,capture_output=True)
        # Exercise both direct and included configuration with a dirty nested
        # canonical module, preserving root and module Git/source inventories.
        self.test_effective_git_worktree_escape_rejects_before_canonical_git_writes()

    def test_monorepo_modules_and_valid_independent_nested_repo_are_admitted(self):
        import subprocess
        subprocess.run(['git','-C',str(self.workspace),'init','-q'],check=True,capture_output=True)
        # app remains an independent nested repository; lib belongs to root.
        (self.workspace/'lib').mkdir()
        (self.workspace/'lib/b.py').write_bytes(b'value = 1\n')
        manifest = read(self.profile/'project.json')
        manifest['modules'].append({'name':'lib','path':'lib','kind':'backend','protected':False})
        atomic(self.profile/'project.json',manifest)
        before = inventory(self.workspace,include_git=True)
        child = self.allocate()
        self.assertEqual(before,inventory(self.workspace,include_git=True))
        self.assertEqual(before,inventory(child.working_dir,include_git=True))
        for rel, expected in (('app',child.working_dir/'app'),('lib',child.working_dir)):
            top = subprocess.run(['git','-C',str(child.working_dir/rel),'rev-parse','--show-toplevel'],
                                 check=True,capture_output=True,text=True).stdout.strip()
            self.assertEqual(expected,Path(top).resolve())

    def test_existing_eol_repair_reseals_only_authorized_normalization(self):
        import subprocess
        from git_collector import GitCollector
        app = self.workspace/'app'
        (app/'a.py').write_bytes(b'value = 1\n')
        for args in (['config','core.autocrlf','false'],['add','a.py'],
                ['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgsign=false','commit','-qm','fixture']):
            subprocess.run(['git','-C',str(app),*args],check=True,capture_output=True)
        child = self.allocate()
        task = self.task(child)
        task.changed_files = ['app/a.py']
        profile,_ = child._load_profile()
        collector = GitCollector(str(child.working_dir),profile=profile)
        baseline,error = collector.capture_baseline(task)
        self.assertIsNotNone(baseline,error)
        class Worker:
            def __init__(self, **kwargs): pass
            def execute(self, requirement, working_dir, task):
                (Path(working_dir)/'app/a.py').write_bytes(b'value = 2\r\n')
                return SimpleNamespace(success=True)
        custody = child.isolated_execution
        with self.fake_adapter(custody):
            custody.work(Worker,{},'',task)
        old = read(custody.root/'cleanup.json')
        self.assertEqual(b'value = 2\r\n',(child.working_dir/'app/a.py').read_bytes())
        repaired = task._isolated_eol_repair(task,collector,baseline)
        self.assertTrue(repaired.success)
        self.assertEqual(['app/a.py'],repaired.repaired_files)
        self.assertEqual(b'value = 2\n',(child.working_dir/'app/a.py').read_bytes())
        sealed = read(custody.root/'cleanup.json')
        self.assertNotEqual(old['host_input'],sealed['host_input'])
        self.assertEqual('EXISTING_REPAIR_EOL_TO_INDEX',sealed['normalizations'][0]['policy'])
        task.save(child.task_root)
        job = {'job_id':task.job_id,'materialized_execution':{'execution_id':task.execution_id}}
        result = {'task_id':task.task_id,'success':True}
        self.control.isolation.promoter._task(job,child,result)  # custody eligibility, no fabricated gates
        (child.working_dir/'app/a.py').write_bytes(b'unrelated drift\n')
        with self.assertRaisesRegex(CandidateError,'CUSTODY_PROOF_INVALID'):
            self.control.isolation.promoter._task(job,child,result)
        with self.assertRaisesRegex(CandidateError,'CUSTODY_PROOF_INVALID'):
            task._isolated_eol_repair(task,collector,baseline)

    def test_isolated_dispositions_survive_roundtrip_unknown_still_rejected(self):
        task = TaskState(task_id='RDY-STATE',requirement='fixture')
        for disposition in ('BATCH_COMMIT_PENDING','CANONICAL_COMMITTED','ISOLATED_CANDIDATE_PRESERVED',
                            'REJECTED_ISOLATED_CANDIDATE_PRESERVED','UNRECOGNIZED'):
            task.worktree_disposition = disposition
            task.save(self.control.harness.task_root)
            actual = TaskState.load(task.task_id,self.control.harness.task_root).worktree_disposition
            self.assertEqual('UNKNOWN' if disposition=='UNRECOGNIZED' else disposition,actual)

    def test_human_reject_receipt_keeps_public_response_envelope(self):
        from operator_actions import result_projection
        finalized = {'job_id':'RDY-A','status':'CANCELLED','revision':4,
                     'request':{'requirement':'fixture-private-requirement'}}
        result = result_projection({'resolution':'HUMAN_REJECT','replayed':False,
            'decision_id':'RDY-D','status':'CANCELLED','job':finalized})
        self.assertEqual({'resolution':'HUMAN_REJECT','replayed':False,
            'decision_id':'RDY-D','status':'CANCELLED',
            'job':{'job_id':'RDY-A','revision':4,'status':'CANCELLED'}},result)
        self.assertNotIn('request', result['job'])

    def test_generated_output_identity_preserves_declared_and_tracked_source(self):
        import subprocess
        app = self.workspace/'app'
        (app/'target').mkdir()
        (app/'target/source.py').write_text('tracked source')
        (app/'target/generated.bin').write_text('output')
        subprocess.run(['git','-C',str(app),'init','-q'],check=True,capture_output=True)
        subprocess.run(['git','-C',str(app),'add','target/source.py'],check=True,capture_output=True)
        profile,_ = self.control.harness._load_profile()
        result = inventory(self.workspace, profile=profile)
        self.assertIn('app/target/source.py',result)
        self.assertNotIn('app/target/generated.bin',result)
        self.assertIn('app/target/generated.bin',inventory(self.workspace,profile=profile,preserve=['app/target/generated.bin']))
        (app/'target/.env').write_text('fixture config')
        self.assertIn('app/target/.env',inventory(self.workspace,profile=profile))
        (app/'.quasar').mkdir()
        (app/'.quasar/app.js').write_text('generated quasar bootstrap')
        self.assertNotIn('app/.quasar/app.js', inventory(self.workspace, profile=profile))
        self.assertIn('app/.quasar/app.js', inventory(
            self.workspace, profile=profile, preserve=['app/.quasar/app.js']))

    def test_preserved_reopen_ignores_only_known_undeclared_quasar_output(self):
        import subprocess
        app = self.workspace/'app'
        (app/'.quasar').mkdir()
        (app/'.quasar/app.js').write_text('generated bootstrap')
        (app/'src').mkdir()
        (app/'src/A.vue').write_text('source')
        (app/'test').mkdir()
        (app/'test/A.test.mjs').write_text('test')
        (app/'unexpected.txt').write_text('unexpected')
        subprocess.run(['git','-C',str(app),'init','-q'],check=True,capture_output=True)
        profile,_ = self.control.harness._load_profile()
        recorded = {
            'app/.quasar/app.js': 'old-generated',
            'app/src/A.vue': 'source-hash',
            'app/test/A.test.mjs': 'test-hash',
            'app/unexpected.txt': 'unexpected-hash',
        }
        preserve = ['app/src/A.vue', 'app/test/A.test.mjs']
        first = normalized_preserved_inventory(
            self.workspace, recorded, profile=profile, preserve=preserve)
        second = normalized_preserved_inventory(
            self.workspace, recorded, profile=profile, preserve=preserve)
        self.assertEqual(first, second)
        self.assertNotIn('app/.quasar/app.js', first)
        self.assertEqual('source-hash', first['app/src/A.vue'])
        self.assertEqual('test-hash', first['app/test/A.test.mjs'])
        self.assertEqual('unexpected-hash', first['app/unexpected.txt'])

    def test_owned_native_server_closes_on_success_and_failure(self):
        from opencode_runtime_adapter import OpenCodeRuntimeAdapter, OpenCodeAdapterError
        from runtime_adapter import RuntimeDescriptor
        from worker import OpenCodeWorker
        class Adapter(OpenCodeRuntimeAdapter):
            def __init__(self, fail=False):
                super().__init__()
                self.fail, self.stops = fail, 0
            def preflight(self): return RuntimeDescriptor(runtime='opencode',version='fixture')
            def start_local_server(self, **kwargs): self.base_url='http://127.0.0.1:1'
            def stop_local_server(self): self.stops += 1
            def execute_invocation(self, invocation):
                if self.fail: raise OpenCodeAdapterError('OPENCODE_TRANSPORT_ERROR')
                return {'id':'ses_fixture'}
        for fail in (False, True):
            adapter = Adapter(fail)
            task = TaskState(task_id='RDY-NATIVE',requirement='fixture',worker='opencode',opencode_model=MODEL)
            with patch('worker.build_prompt',return_value='fixture'), patch.object(OpenCodeWorker,'_wait_for_response',return_value=('msg_result','done')):
                result = OpenCodeWorker(model=MODEL,adapter_factory=lambda:adapter,ephemeral_session=True).execute('fixture',str(self.workspace),task)
            self.assertEqual(not fail,result.success)
            self.assertEqual(1,adapter.stops)

    def test_normal_queue_reaches_worker_and_runtime_failure_cannot_publish(self):
        import subprocess
        from runtime_adapter import RuntimeDescriptor
        from worker import OpenCodeWorker
        self.schema6()
        app = self.workspace/'app'
        (app/'.gitignore').write_text('.env\n')
        for args in (['init','-q'],['add','.'],['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgsign=false','commit','-qm','fixture']):
            subprocess.run(['git','-C',str(app),*args],check=True,capture_output=True)
        queued = self.control.enqueue_jobs('RDY-QUEUE',[{
            'client_job_id':'RDY-A','requirement':'MODIFICATION: Change app/a.py value to 2.',
            'worker':'opencode','model':MODEL,'target_modules':['app'],
            'target_resources':['app/a.py'],'max_outer_attempts':1,
        }])
        job_id = queued['jobs'][0]['job_id']
        descriptor = RuntimeDescriptor(runtime='opencode',version='fixture',schema_compatible=True)
        class Adapter:
            def __init__(self,*args): self.stopped=False
            def preflight(self): return descriptor
            def stop_local_server(self): self.stopped=True
            def confirm_quiescence(self): return self.stopped
            def boundary_receipt(self): return {'boundary':'docker-opencode/1','quiesced':True}
        def failed_work(worker, requirement, working_dir, task=None):
            (Path(working_dir)/'app/a.py').write_text('failed untrusted edit')
            return OpenCodeWorker._result(task,code='OPENCODE_TRANSPORT_ERROR')
        with patch.object(self.control.isolation,'adapter',side_effect=Adapter), patch.object(OpenCodeWorker,'execute',autospec=True,side_effect=failed_work) as invoke:
            result = self.control.supervisor.start()
            self.assertTrue(result['started'],result)
            self.assertTrue(self.control.supervisor.wait(30))
            self.assertEqual(1,invoke.call_count,self.control.store.get_job(job_id))
        job = self.control.store.get_job(job_id)
        self.assertNotEqual('SUCCEEDED',job['status'])
        self.assertEqual('value = 1\n',(self.workspace/'app/a.py').read_text())
        self.assertFalse(list(self.control.isolation.promoter.root.glob('*-promotion.json')))


if __name__ == '__main__':
    unittest.main()
