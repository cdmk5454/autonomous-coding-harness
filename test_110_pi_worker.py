"""Pi Worker through schema7 Attempt/process custody with a real mock RPC child."""
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import unittest

from harness_11.core import FenceProof, ProcessIdentity
from job_contract import JobRequest
from pi_process_boundary import DockerPiProcessBoundary, PI_MODEL
from pi_worker import PiWorker
from task_state import TaskState


MOCK_RPC = r'''
import json,sys,time
mode=sys.argv[1]
for line in sys.stdin:
    try: command=json.loads(line)
    except ValueError: break
    if command['type']=='get_state':
        if mode=='startup_timeout': time.sleep(30); continue
        print(json.dumps({'type':'response','id':command['id'],'command':'get_state',
              'success':True,'data':{'sessionId':'mock-session'}}),flush=True)
    elif command['type']=='prompt':
        if mode=='malformed': print('{bad',flush=True); continue
        if mode=='linger': time.sleep(30); continue
        if mode=='complete_delta':
            open('app/a.py','w',encoding='utf-8').write('value=2\n')
        print(json.dumps({'type':'agent_start'}),flush=True)
        print(json.dumps({'type':'agent_settled'}),flush=True)
        print(json.dumps({'type':'response','id':command['id'],'command':'prompt','success':mode!='false_result'}),flush=True)
    elif command['type']=='abort':
        print(json.dumps({'type':'response','id':command['id'],'command':'abort','success':True}),flush=True)
'''


class MockPiDocker(DockerPiProcessBoundary):
    def __init__(self,candidate,canonical,script,mode):
        self.candidate=Path(candidate).resolve();self.canonical=Path(canonical).resolve()
        self.script=Path(script);self.mode=mode;self.process=None;self.client=None
        self._quiesced=False;self.events=queue.Queue();self.binding=None
    def preflight(self): return {'runtime':'pi','version':'0.87.1'}
    def verify_scope(self,binding,scope): return scope.candidate_root==str(self.candidate)
    def start(self,binding,scope,*,timeout):
        self.binding=binding
        self.client=subprocess.Popen([sys.executable,str(self.script),self.mode],
            cwd=self.candidate,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.process=ProcessIdentity(self.client.pid,f'{self.client.pid}:mock-birth',
            'disposable-mock','opencode-go','pi')
        def read():
            for line in self.client.stdout: self.events.put(line)
        threading.Thread(target=read,daemon=True).start()
        return self.process
    def send(self,raw,*,timeout):
        self.client.stdin.write(raw);self.client.stdin.flush()
    def read(self,*,timeout,maximum):
        try: return self.events.get(timeout=timeout),b''
        except queue.Empty: raise TimeoutError()
    def poll(self): return self.client.poll() if self.client else None
    def fence(self,binding,process,token,*,timeout):
        self.stop_local_server()
        return FenceProof(binding.key,process.key,token,True,True,'MOCK_CHILD','disposable-fence')
    def stop_local_server(self,timeout=None):
        if self.client and self.client.poll() is None:
            self.client.kill();self.client.wait(timeout=5)
        self._quiesced=True
        if self.client:
            for pipe in (self.client.stdin,self.client.stdout,self.client.stderr): pipe.close()
    def confirm_quiescence(self): return self._quiesced and self.client.poll() is not None
    def boundary_receipt(self): return {'boundary':'docker-pi/1','quiesced':self.confirm_quiescence()}


class PiWorkerIntegrated(unittest.TestCase):
    def setUp(self):
        from test_110_nworker_dispatch import NWorkerDispatchTest
        self.fixture=NWorkerDispatchTest('test_explicit_2_to_3_migration_and_three_real_claims')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.script=self.fixture.root/'mock_pi_rpc.py'
        self.script.write_text(MOCK_RPC,encoding='utf-8')
        (self.fixture.root/'candidate').mkdir();(self.fixture.root/'canonical').mkdir()
        (self.fixture.root/'candidate'/'app').mkdir()
        (self.fixture.root/'candidate'/'app'/'a.py').write_text('value=1\n')

    def task(self):
        req=JobRequest.from_mapping({'client_job_id':'mock-pi','requirement':'Change a.py',
            'worker':'pi','model':PI_MODEL,'pi_candidate':True,
            'target_modules':['app'],'target_resources':['app/a.py']},allowed_modules=['app'])
        job=self.fixture.enqueue([req])[0]
        claimed=self.fixture.store.claim_next('pi-owner')
        execution=self.fixture.materialize(claimed)
        attempt=self.fixture.repo.start_attempt(execution,attempt_id='PI-MOCK-ATTEMPT',role='WORKER',model=PI_MODEL)
        task=TaskState(task_id='PI-MOCK-TASK',requirement='Change a.py',worker='pi',pi_model=PI_MODEL)
        task.job_id=job['job_id'];task.execution_id=execution['execution_id'];task.attempt_id=attempt['attempt_id']
        task.candidate_id='PI-MOCK-CANDIDATE';task.execution_epoch='PI-MOCK-EPOCH'
        task.target_resources=['app/a.py'];task.materialized_execution=execution
        task.control_repository_path=str(self.fixture.repo.path)
        return task

    def worker(self,mode):
        boundary=MockPiDocker(self.fixture.root/'candidate',self.fixture.root/'canonical',self.script,mode)
        return PiWorker(adapter_factory=lambda:boundary,model=PI_MODEL,timeout=1),boundary

    def test_native_completion_is_only_worker_evidence(self):
        task=self.task();worker,boundary=self.worker('complete')
        result=worker.execute(task.requirement,boundary.candidate,task)
        self.assertTrue(result.success,result)
        self.assertTrue(boundary.confirm_quiescence())
        self.assertEqual('RUNNING',self.fixture.store.get_job(task.job_id)['status'])
        self.assertEqual('RUNNING',self.fixture.repo.attempt_outcome(task.attempt_id)['status'])
        self.assertFalse(list(self.fixture.root.glob('*promotion*')))

    def test_malformed_event_fences_child(self):
        task=self.task();worker,boundary=self.worker('malformed')
        result=worker.execute(task.requirement,boundary.candidate,task)
        self.assertFalse(result.success)
        self.assertTrue(boundary.confirm_quiescence())

    def test_false_native_result_is_not_product_pass(self):
        task=self.task();worker,boundary=self.worker('false_result')
        result=worker.execute(task.requirement,boundary.candidate,task)
        self.assertFalse(result.success)
        self.assertTrue(boundary.confirm_quiescence())
        self.assertEqual('RUNNING',self.fixture.store.get_job(task.job_id)['status'])

    def test_startup_timeout_fences_child(self):
        task=self.task();worker,boundary=self.worker('startup_timeout')
        result=worker.execute(task.requirement,boundary.candidate,task)
        self.assertFalse(result.success)
        self.assertTrue(boundary.confirm_quiescence())

    def test_cancel_lingering_child_keeps_slot(self):
        from bounded_execution import request_cancel, slots
        task=self.task();worker,boundary=self.worker('linger')
        # Cancellation is set after Process registration/slot binding.
        original_send=boundary.send
        def send(raw,*,timeout):
            original_send(raw,timeout=timeout)
            if json.loads(raw)['type']=='prompt':
                request_cancel(self.fixture.repo,task.job_id,'pi-owner')
        boundary.send=send
        result=worker.execute(task.requirement,boundary.candidate,task)
        self.assertFalse(result.success)
        self.assertTrue(boundary.confirm_quiescence())
        self.assertEqual(1,len(slots(self.fixture.repo)))

    def test_actual_service_manager_factory_and_candidate_custody(self):
        from runtime_identity import RuntimeIdentityError, verify_source_manifest
        try: verify_source_manifest(Path(__file__).resolve().parent)
        except RuntimeIdentityError as exc:
            if exc.code in {'RUNTIME_SOURCE_DRIFT','RUNTIME_SOURCE_INVENTORY_DRIFT'}:
                self.skipTest('Live edited source awaits candidate manifest')
            raise
        from test_production_isolation import CompositionTests
        from bounded_execution import bind_candidate, migrate_capacity_disposable
        from harness_11.capacity import CapacityPolicy
        from manager import Manager
        from unittest.mock import patch
        fixture=CompositionTests('test_wrong_route_rejects_without_allocation')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.schema6()
        fixture.control.store.pause('disposable Pi schema7')
        migrate_capacity_disposable(fixture.repo.path,allocation_root=fixture.root,
            backup_path=fixture.root/'schema6-pi.bak',policy=CapacityPolicy(workers=3,wip=3),
            quiescence_probe=lambda _:True)
        fixture.control.store.resume_queue()
        queued=fixture.control.enqueue_jobs('PI-INTEGRATED',[{
            'client_job_id':'PI-INTEGRATED-1','requirement':'MODIFICATION: Change app/a.py',
            'worker':'pi','model':PI_MODEL,'pi_candidate':True,
            'target_modules':['app'],'target_resources':['app/a.py']}])
        claimed=fixture.control.store.claim_next('pi-integrated-owner')
        child=fixture.control.isolation(claimed,fixture.control.store.repository)
        bind_candidate(fixture.repo,claimed['job_id'],'pi-integrated-owner',str(child.working_dir))
        execution=child.materialize_execution(claimed,fixture.repo)
        attempt=fixture.repo.start_attempt(execution,attempt_id='PI-INTEGRATED-ATTEMPT',
            role='WORKER',model=PI_MODEL)
        task=TaskState(task_id='PI-INTEGRATED-TASK',requirement='Change app/a.py',
            worker='pi',pi_model=PI_MODEL,target_module=['app'])
        task.job_id=claimed['job_id'];task.execution_id=execution['execution_id']
        task.attempt_id=attempt['attempt_id'];task.materialized_execution=execution
        task.control_repository_path=str(fixture.repo.path);task.target_resources=['app/a.py']
        child.isolated_execution.bind(task)
        manager=Manager.__new__(Manager)
        manager.choose_worker=lambda _: 'pi'
        manager.isolated_pi_worker_factory=child.isolated_execution.worker
        worker,route=manager._get_worker(task)
        self.assertEqual('pi',route)
        def adapter(root,*_args,**_kwargs):
            return MockPiDocker(root/'candidate',fixture.workspace,self.script,'complete_delta')
        with patch.object(fixture.control.isolation,'adapter',side_effect=adapter):
            result=worker.execute(task.requirement,child.working_dir,task)
        self.assertTrue(result.success,result)
        self.assertEqual('value=2\n',(child.working_dir/'app/a.py').read_text())
        self.assertEqual('value = 1\n',(fixture.workspace/'app/a.py').read_text())
        self.assertEqual('RUNNING',fixture.control.store.get_job(task.job_id)['status'])


if __name__=='__main__': unittest.main()
