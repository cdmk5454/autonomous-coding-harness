"""Cold real-control operator flow; only native provider transports are fixtures.

Real disposable Git, profile, runtime snapshots, source provenance, Manager,
Build/Test/Review gates, decisions and journalled publication are required.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class ColdCanonicalOperatorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='cold-op-',dir=r'D:\upload')
        self.root=Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.work=self.root/'canonical';self.app=self.work/'app';self.app.mkdir(parents=True)
        self.profile=self.root/'profile'/'cold';self.profile.mkdir(parents=True)
        self.control_root=self.root/'.control';self.tasks=self.root/'.tasks';self.custody=self.root/'custody'
        self.agents=self.root/'agents';self.user=self.root/'user'
        values={'AGENTS_DIR':str(self.agents),'USERPROFILE':str(self.user),'CODEX_HOME':str(self.user/'.codex'),
            'KKM_TG_ENABLED':'0','KKM_TELEGRAM_ENABLED':'0','KKM_DISCORD_ENABLED':'0',
            'KKM_HARNESS_DISABLE_AUTO_START':'1','PYTHONDONTWRITEBYTECODE':'1'}
        env=patch.dict(os.environ,values);env.start();self.addCleanup(env.stop)
        from run import runtime_rule_paths
        for path,_ in runtime_rule_paths():
            path.parent.mkdir(parents=True,exist_ok=True);path.write_text('# Disposable synchronized policy\n',encoding='utf-8')
        for name in ('a','b'):
            (self.app/f'{name}.py').write_text('value = 0\n',encoding='utf-8')
            (self.app/f'test_{name}.py').write_text(
                f'from pathlib import Path\nassert Path(__file__).with_name("{name}.py").read_text() == "value = 1\\n"\nprint("CHECK_{name.upper()}_PASS")\n',encoding='utf-8')
        (self.app/'.gitignore').write_text('__pycache__/\n',encoding='utf-8')
        for args in [('init','-q'),('config','core.autocrlf','false'),('add','.'),
                     ('-c','user.name=Fixture','-c','user.email=fixture@example.invalid',
                      '-c','commit.gpgsign=false','commit','-qm','fixture baseline')]:self.git(*args)
        self.initial_head=self.git('rev-parse','HEAD')
        (self.profile/'ui_map').mkdir()
        for rel in ['rules.md','analysis.md','ui_map/index.md']:
            (self.profile/rel).write_text('# Disposable minimal Python module\n',encoding='utf-8')
        from hermes_boundary import atomic
        atomic(self.profile/'project.json',dict(schema_version=1,id='cold',display_name='Cold fixture',
            workspace_roots=[str(self.work)],context=dict(project_rules='rules.md',analysis='analysis.md',ui_map='ui_map'),
            modules=[dict(name='app',path='app',kind='backend',protected=False,
                build=dict(cwd='app',argv=[sys.executable,'-B','-c',
                    "from pathlib import Path; [compile(p.read_text(),str(p),'exec') for p in Path('.').glob('*.py')]"]))],
            rules=dict(always=['rules.md'],backend=[],frontend=[],sql=[],architecture=[],egov=[]),
            area_patterns=dict(backend=['app/**'],frontend=[],sql=[],architecture=[]),
            protected_paths=[],local_dev_config_excludes=[],
            control_policy=dict(commit_policy='BATCH_FINAL_COMMIT',cumulative_worktree=True,
                auto_continue_without_commit=True,pre_job_snapshot_supported=True,
                scoped_rollback_supported=True,final_commit_required=True,
                task_owned_baseline_files=[],external_frozen_baseline_files=[])))
        from control_repository import ControlRepository
        from job_queue import JobStore
        from bounded_execution import migrate_disposable
        self.control_root.mkdir()
        self.repo=ControlRepository(self.control_root/'control.sqlite3',create=True)
        self.repo.bootstrap(JobStore._empty_queue(),{}, {})
        atomic(self.control_root/'sqlite-authority.json',{'authority':'SQLITE','schema_version':5})
        migrate_disposable(self.repo.path,allocation_root=self.root,backup_path=self.root/'schema5.bak')
        import gc;gc.collect()
        self.control=self.new_control()
        self.calls=[]
        self.native_script=self.root/'model_transport_fixture.py'
        self.native_script.write_text('''import json,re,sys
from pathlib import Path
mode=sys.argv[1]
if mode=='worker':
 Path(sys.argv[2]).write_text('value = 1\\n',encoding='utf-8')
 print('Implemented exactly the approved single-file change.')
elif mode=='tester':
 print('TEST_RESULT: PASS\\nSUMMARY: Native fixture inspected current input\\nSTEPS:\\n- Frozen source and required tests inspected')
elif mode=='reviewer':
 prompt=sys.stdin.read()
 found=re.findall(r'"current_task_changed_files"\\s*:\\s*(\\[[^\\]]*\\])',prompt)
 files=json.loads(found[-1]) if found else []
 assert files, 'review fixture requires actual bound file coverage'
 answer=json.dumps({'result':'PASS','reason':'Current approved fixture source and receipts verified','details':[],
  'risk':'NONE','violation_codes':[],'reviewed_files':files,
  'reviewed_task_files':files,'inspected_unchanged_target_files':[]})
 Path(sys.argv[2]).write_text(answer,encoding='utf-8')
 print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':answer}}))
''',encoding='utf-8')
        self.original_run=subprocess.run
        import worker,reviewer,tester
        self.original_activity=worker._run_with_activity_timeout
        self.real_descriptor=self.control.isolation.descriptor()
        test=self
        class RuntimeBoundary:
            def __init__(self,root,execution,epoch,candidate,job,attempt):
                self.stopped=False
                self.binding=dict(execution_id=execution,execution_epoch=epoch,candidate_id=candidate,job_id=job,attempt_id=attempt)
            def preflight(self):return test.real_descriptor
            def stop_local_server(self):self.stopped=True
            def confirm_quiescence(self):return self.stopped
            def boundary_receipt(self):return {**self.binding,'quiesced':self.stopped,'boundary':'docker-opencode/1'}
        def worker_transport(instance,requirement,working_dir,task=None):
            from worker import WorkerResult,TimeoutConfig
            rel=task.target_resources[0]
            self.calls.append(('WORKER',task.execution_id,task.attempt_id))
            cmd=[sys.executable,'-B',str(self.native_script),'worker',str(Path(working_dir)/rel)]
            code,out,err,reason=self.original_activity(cmd=cmd,stdin_data='',cwd=working_dir,
                cfg=TimeoutConfig(analysis_max=15,modification_max=15,hard_limit=15),
                requirement=requirement,task_mode='MODIFICATION',runtime_event=task._runtime_event_callback,
                execution_id='INV-FIXTURE-'+task.attempt_id,execution_role='WORKER',
                attempt_id=task.attempt_id,runtime_name='opencode')
            return WorkerResult(code==0 and not reason,code,out,err,cmd,timed_out=bool(reason))
        def subprocess_transport(cmd,*args,**kwargs):
            if isinstance(cmd,(list,tuple)) and 'droid' in Path(str(cmd[0])).name.lower():
                self.calls.append(('TESTER_PROVIDER',str(kwargs.get('cwd','')),''))
                cmd=[sys.executable,'-B',str(self.native_script),'tester']
            return self.original_run(cmd,*args,**kwargs)
        def fixture_command(kwargs):
            cmd=kwargs['cmd'];base=Path(str(cmd[0])).name.lower()
            if 'codex' in base:
                self.calls.append(('REVIEWER_PROVIDER',kwargs.get('execution_id',''),kwargs.get('attempt_id','')))
                kwargs['cmd']=[sys.executable,'-B',str(self.native_script),'reviewer',cmd[cmd.index('-o')+1]]
            elif 'droid' in base:
                self.calls.append(('TESTER_PROVIDER',kwargs.get('execution_id',''),kwargs.get('attempt_id','')))
                kwargs['cmd']=[sys.executable,'-B',str(self.native_script),'tester']
            return kwargs
        def activity_transport(**kwargs):
            return self.original_activity(**fixture_command(kwargs))
        import owned_role_transport
        original_owned=owned_role_transport.run
        def owned_transport(**kwargs):
            return original_owned(**fixture_command(kwargs))
        for p in [patch.object(self.control.isolation,'adapter',side_effect=RuntimeBoundary),
                  patch.object(worker.OpenCodeWorker,'execute',new=worker_transport),
                  patch.object(subprocess,'run',side_effect=subprocess_transport),
                  patch.object(worker,'_run_with_activity_timeout',side_effect=activity_transport),
                  patch.object(reviewer,'_run_with_activity_timeout',side_effect=activity_transport),
                  patch.object(owned_role_transport,'run',side_effect=owned_transport),
                  patch.object(socket.socket,'connect',side_effect=AssertionError('NETWORK_FORBIDDEN'))]:
            p.start();self.addCleanup(p.stop)

    def git(self,*args):
        return subprocess.run(['git','-C',str(self.app),*args],check=True,capture_output=True).stdout.decode().strip()

    def new_control(self):
        from chatgpt_control import ChatGPTControlService
        return ChatGPTControlService(self.work,self.profile,control_root=self.control_root,
            task_root=self.tasks,state_base=self.root,isolation_root=self.custody)

    def prepare_parents(self):
        from production_isolation import MODEL
        requests=[dict(client_job_id=name.upper(),requirement=f'MODIFICATION: Set app/{name}.py value to 1. Preserve all other source. Run app/test_{name}.py.',
            worker='opencode',model=MODEL,target_modules=['app'],target_resources=[f'app/{name}.py'],depends_on=[],
            test_plan=dict(required_tests=[f'app/test_{name}.py'],command=f'"{Path(sys.executable).as_posix()}" -B app/test_{name}.py',mandatory=True,verification_depth='TARGETED')) for name in ('a','b')]
        jobs=self.control.enqueue_jobs('cold-approved-ab',requests)['jobs']
        claims=[]
        for name in ('a','b'):
            job=self.control.store.claim_next('owner-'+name)
            self.assertIsNotNone(job)
            child=self.control.isolation(job,self.control.store.repository)
            claims.append((job,child,'owner-'+name))
        self.parents=[]
        for job,child,owner in claims:
            snapshot=child.prepare_job_snapshot(job,self.control.store.queue_snapshot())
            job=self.control.store.record_pre_job_snapshot(job['job_id'],owner,snapshot)
            execution=child.materialize_execution(job,self.control.store.repository)
            bound={**job,'materialized_execution':execution,'execution_context':execution['execution_context'],
                   'request':execution['request'],'current_requirement':execution['current_requirement']}
            def started(task_id,job=job,owner=owner,execution=execution):
                self.control.store.mark_task_started(job['job_id'],owner,task_id)
                return self.repo.start_attempt(execution,attempt_id=task_id+'-WORKER',role='WORKER',model=MODEL)
            self.assertTrue(child.acquire_execution_lease())
            try:result=child.execute(bound,on_task_started=started)
            finally:child.release_execution_lease()
            self.assertTrue(result['success'],json.dumps(result,ensure_ascii=False,default=str)[-18000:])
            self.parents.append((bound,child,result,owner))
        # Both candidates originated at exactly the same canonical baseline.
        # Hold B first, then publish A through the actual journal owner.
        a,b=self.parents
        held=self.control.isolation.promoter.quarantine(b[0],b[1],b[2])
        self.control.store.record_result(b[0]['job_id'],b[3],held)
        published=self.control.isolation.promoter(a[0],a[1],a[2])
        self.assertEqual('PROMOTED',published['canonical_promotion']['status'])
        self.control.store.record_result(a[0]['job_id'],a[3],published)
        return a,b

    def test_cold_operator_revalidate_fresh_decision_and_real_publication(self):
        from canonical_revalidation import CONFIRMATION
        from task_state import TaskState
        from hermes_boundary import read
        a,b=self.prepare_parents()
        old_job=self.repo.read_job(b[0]['job_id']);old_task=TaskState.load(b[2]['task_id'],self.tasks)
        old_path=TaskState.task_date_dir(old_task.task_id,self.tasks)/(old_task.task_id+'.json')
        old_bytes=old_path.read_bytes();old_execution=self.repo.execution(old_task.execution_id)
        worker_count=len([x for x in self.calls if x[0]=='WORKER'])
        # The approved driver is one continuous cold-started controller. Its
        # verified owned allocations remain authority through A -> B -> QA.
        cold=self.control
        self.assertIn(b[0]['job_id'],cold.isolation.promoter.allocations)
        result=cold.revalidate_canonical_candidate(b[0]['job_id'],expected_revision=old_job['revision'],
            request_id='cold-post-a-revalidation',predecessor_ids=[a[0]['job_id']],confirmation=CONFIRMATION)
        self.assertFalse(result['published']);new_job=cold.store.get_job(b[0]['job_id'])
        self.assertEqual('AWAITING_QA',new_job['status'])
        self.assertNotEqual(old_job['candidate_id'],new_job['candidate_id'])
        self.assertEqual(old_bytes,old_path.read_bytes());self.assertEqual(old_execution,self.repo.execution(old_task.execution_id))
        self.assertEqual(worker_count,len([x for x in self.calls if x[0]=='WORKER']))
        fresh_execution=self.repo.execution(new_job['last_result']['execution_id'])
        self.assertNotEqual(old_execution['profile_revision'],fresh_execution['profile_revision'])
        self.assertEqual(old_execution['policy_revision'],fresh_execution['policy_revision'])
        context=cold.get_operator_action_context(new_job['job_id'])
        self.assertIn('HUMAN_ACCEPT',context['legal_next_actions'])
        accepted=cold.resolve_qa_decision(new_job['job_id'],resolution='HUMAN_ACCEPT',
            expected_revision=new_job['revision'],request_id='cold-fresh-qa-accept',
            confirmation='I_CONFIRM_QA_HUMAN_ACCEPT',candidate_id=new_job['candidate_id'],decision_context=context)
        self.assertEqual('SUCCEEDED',accepted['status'],accepted)
        self.assertEqual('value = 1\n',(self.app/'a.py').read_text())
        self.assertEqual('value = 1\n',(self.app/'b.py').read_text())
        self.assertEqual(self.initial_head,self.git('rev-parse','HEAD'))
        final=cold.store.get_job(new_job['job_id']);pub=final['last_result']['canonical_promotion']
        from qa_quarantine import QACandidateStore
        store=QACandidateStore(cold.isolation.promoter.root/'candidates',self.work,None)
        journal=store.load_publication(pub['publication_id'])
        self.assertEqual(('COMPLETED','PROMOTED'),(journal['state'],journal['disposition']))
        self.assertEqual([],store.unresolved_publications())
        with self.repo.hold():
            self.assertEqual(0,self.repo._connection.execute('SELECT count(*) FROM execution_slots').fetchone()[0])
            self.assertEqual(0,self.repo._connection.execute("SELECT count(*) FROM attempt_outcomes WHERE status IN ('RUNNING','UNKNOWN')").fetchone()[0])
            self.assertEqual(0,self.repo._connection.execute("SELECT count(*) FROM runtime_processes WHERE state NOT IN ('EXITED','KILLED')").fetchone()[0])

    def test_changed_policy_after_parent_materialization_blocks_before_admission(self):
        from canonical_revalidation import CONFIRMATION
        from qa_quarantine import CandidateError
        a,b=self.prepare_parents()
        old_job=self.repo.read_job(b[0]['job_id'])
        cold=self.control
        with self.repo.hold():
            before=self.repo._connection.execute('SELECT count(*) FROM executions').fetchone()[0]
        from run import runtime_rule_paths
        changed=[]
        for path,label in runtime_rule_paths():
            if label in {'common_agents','codex_global_agents','factory_global_agents'}:
                changed.append((path,path.read_bytes()))
                path.write_text('# Changed disposable synchronized policy\n',encoding='utf-8')
        try:
            with self.assertRaisesRegex(CandidateError,'REVALIDATION_POLICY_DRIFT'):
                cold.revalidate_canonical_candidate(b[0]['job_id'],expected_revision=old_job['revision'],
                    request_id='cold-policy-drift-negative',predecessor_ids=[a[0]['job_id']],confirmation=CONFIRMATION)
            with self.repo.hold():
                after=self.repo._connection.execute('SELECT count(*) FROM executions').fetchone()[0]
            self.assertEqual(before,after)
            self.assertEqual(old_job,self.repo.read_job(old_job['job_id']))
        finally:
            for path,original in changed:path.write_bytes(original)


if __name__=='__main__':unittest.main(verbosity=2)
