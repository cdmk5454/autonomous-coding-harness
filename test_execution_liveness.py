"""Step 7 virtual-time and durable identity regression tests."""
import unittest,copy,json
from dataclasses import asdict,replace
from execution_liveness import ExecutionSignals,SignalError
import test_hermes_runtime_adapter as adapter_fixtures
from hermes_runtime_adapter import HermesError
from control_repository import RepositoryError

class Clock:
    def __init__(self):self.value=0.
    def __call__(self):return self.value
    def add(self,s):self.value+=s

class SignalTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.identity={'job_id':'j','execution_id':'e','attempt_id':'a','binding_id':'b','execution_epoch':'epoch','native_run_id':'r','native_session_id':'s','host_boundary_identity':'pid:ctime'}
        self.s=ExecutionSignals(self.identity,clock=self.clock);self.n=0
    def beat(self,identity=None,**kw):
        args=dict(sequence=self.n,observed=self.clock(),gateway_alive=True,run_alive=True,containers_alive=True);args.update(kw)
        self.s.heartbeat(identity or self.identity,**args);self.n+=1
    def test_defaults(self):self.assertEqual((15,45,900),(self.s.heartbeat_seconds,self.s.lease_seconds,self.s.progress_seconds))
    def test_long_job_no_absolute_deadline(self):
        for i in range(1000):
            self.clock.add(10);self.beat()
            if i%50==0:self.s.progress('CANDIDATE_DIFF',str(i),verified=True)
        self.assertGreater(self.clock(),900);self.assertEqual('ACTIVE',self.s.check())
    def test_alive_stall_timeout(self):
        for _ in range(89):self.clock.add(10);self.beat()
        self.clock.add(10)
        with self.assertRaisesRegex(SignalError,'PROGRESS_TIMEOUT'):self.s.check()
    def test_lease_expiry_45(self):
        self.clock.add(44.9);self.assertEqual('ACTIVE',self.s.check());self.clock.add(.1)
        with self.assertRaisesRegex(SignalError,'LEASE_EXPIRED'):self.s.check()
    def test_wrong_identity_fields(self):
        for field in self.identity:
            wrong={**self.identity,field:'foreign'}
            with self.assertRaisesRegex(SignalError,'IDENTITY_MISMATCH'):self.beat(wrong)
        self.assertEqual(45,self.s.lease_until)
    def test_duplicate_heartbeat(self):
        self.beat();self.clock.add(1)
        with self.assertRaisesRegex(SignalError,'DUPLICATE'):self.beat(sequence=0)
        self.assertEqual(45,self.s.lease_until)
    def test_delayed_heartbeat(self):
        self.clock.add(16)
        with self.assertRaisesRegex(SignalError,'DELAYED'):self.beat(observed=0)
    def test_future_heartbeat(self):
        with self.assertRaisesRegex(SignalError,'DELAYED'):self.beat(observed=1)
    def test_renew_after_fence(self):
        self.s.state='FENCING'
        with self.assertRaisesRegex(SignalError,'FENCING'):self.beat()
    def test_dead_gateway_live_container(self):
        with self.assertRaisesRegex(SignalError,'LIVENESS_LOST'):self.beat(gateway_alive=False)
    def test_live_gateway_dead_container(self):
        with self.assertRaisesRegex(SignalError,'LIVENESS_LOST'):self.beat(containers_alive=False)
    def test_false_progress(self):
        for kind in ('HEARTBEAT','TOKEN','SSE_KEEPALIVE','RUNNING','TIMER'):
            with self.assertRaisesRegex(SignalError,'MEANINGFUL'):self.s.progress(kind,'x',verified=True)
        with self.assertRaises(SignalError):self.s.progress('TOOL_COMPLETED','x')
        self.assertEqual(0,self.s.last_progress)
    def test_duplicate_progress_not_extend(self):
        self.assertTrue(self.s.progress('CANDIDATE_DIFF','hash',verified=True));self.clock.add(20)
        self.assertFalse(self.s.progress('CANDIDATE_DIFF','hash',verified=True));self.assertEqual(0,self.s.last_progress)
    def test_qa_has_no_lease_or_watchdog(self):
        self.s.stop('QA_WAIT',quiesced=True);self.clock.add(100000)
        self.assertEqual('QA_WAIT',self.s.check());self.assertIsNone(self.s.lease_until)
    def test_qa_requires_quiescence(self):
        with self.assertRaisesRegex(SignalError,'QUIESCENCE'):self.s.stop('QA_WAIT')
    def test_expired_lease_not_resurrected(self):
        self.clock.add(46)
        with self.assertRaisesRegex(SignalError,'LEASE_EXPIRED'):self.beat()

    def test_qa_heartbeat_cannot_reactivate(self):
        self.s.stop('QA_WAIT',quiesced=True)
        with self.assertRaisesRegex(SignalError,'NOT_ACTIVE'):self.beat()
    def test_old_progress_cannot_hide_expiry(self):
        self.clock.add(46)
        with self.assertRaisesRegex(SignalError,'LEASE_EXPIRED'):self.s.progress('CANDIDATE_DIFF','new',verified=True)
    def test_status_and_tokens_never_extend_progress(self):
        for i in range(89):
            self.clock.add(10);self.beat()
        self.assertEqual(0,self.s.last_progress)
    def test_user_projection_ignores_heartbeat_identity_details(self):
        from task_state import TaskState
        from progress_projection import project_execution_health
        task=TaskState(task_id='qa',requirement='fixture',status='AWAITING_QA')
        task.execution_runtime={'execution_id':'e','lease_expires_at':'2000-01-01T00:00:00+00:00'}
        self.assertEqual('NOT_RUNNING',project_execution_health(task)['runtime_health'])

class DurableTests(unittest.TestCase):
    def setUp(self):
        self.f=adapter_fixtures.HermesAdapterTests();self.f.setUp();self.addCleanup(self.f.doCleanups);self.f.start()
        self.r=self.f.repo;self.b=self.f.b
        self.process=self.r.record_runtime_process(self.b.attempt_id,runtime='hermes',pid=1234,process_identity='1234:ctime')
        self.identity={**asdict(self.b),'native_run_id':self.f.a.run_id,'native_session_id':self.f.a.session_id}
    def persist(self,state='ACTIVE',identity=None):
        identity=identity or self.identity
        return self.r.record_execution_observation(self.b.attempt_id,identity=identity,observation={'identity':identity,'state':state})
    def test_persistence_new_connection(self):
        self.persist()
        from control_repository import ControlRepository
        other=ControlRepository(self.r.path)
        try:self.assertEqual(self.identity,other.runtime_process(self.process['process_id'])['execution_observation']['identity'])
        finally:pass  # Repository.hold closes its own connection.
    def test_stale_epoch_receipt(self):
        self.persist()
        with self.assertRaisesRegex(RepositoryError,'IDENTITY'):self.persist(identity={**self.identity,'execution_epoch':'old'})
    def test_revival_after_fencing_denied(self):
        self.persist('FENCING')
        with self.assertRaisesRegex(RepositoryError,'FENCING'):self.persist()
    def test_receipt_does_not_finish_attempt(self):
        self.persist('QUIESCED');self.assertEqual('RUNNING',self.r.attempt_outcome(self.b.attempt_id)['status'])
    def test_wrong_attempt_denied(self):
        with self.assertRaisesRegex(RepositoryError,'FOREIGN'):self.persist(identity={**self.identity,'attempt_id':'other'})
    def test_newer_attempt_rejects_old_heartbeat(self):
        self.r.start_attempt(self.f.f.execution,attempt_id='new',role='WORKER',model=self.b.model)
        with self.assertRaisesRegex(RepositoryError,'FOREIGN'):self.persist()
    def test_candidate_changed_after_quiescence(self):
        from pathlib import Path
        from hermes_boundary import snapshot,atomic
        from hermes_supervision import verify_recovery_candidate
        root=Path(self.f.f.temp.name);candidate=root/'candidate';candidate.mkdir();(candidate/'f').write_text('old')
        atomic(root/'quiescence.json',{'state':'QUIESCED','samples':[{'candidate':snapshot(candidate)}]})
        verify_recovery_candidate(root,candidate);(candidate/'f').write_text('new')
        with self.assertRaisesRegex(HermesError,'CANDIDATE_CHANGED'):verify_recovery_candidate(root,candidate)
    def test_no_absolute_worker_timeout(self):
        from hermes_worker import HermesWorker
        with self.assertRaisesRegex(HermesError,'ABSOLUTE_TIMEOUT'):HermesWorker(self.f.a,timeout=240)

    def test_recovery_requires_old_attempt_finished(self):
        with self.assertRaisesRegex(RepositoryError,'OLD_ATTEMPT'):
            self.r.authorize_runtime_recovery(self.b.execution_id,self.b.attempt_id,candidate_disposition='PRESERVE_FOR_REVERIFICATION',candidate_digest='d'*64,confirmation='AUTHORIZE_NEW_ATTEMPT_AFTER_RECONCILIATION')
    def test_recovery_requires_quiescence_and_command_reconciliation(self):
        self.r.finish_attempt(self.b.attempt_id,status='INTERRUPTED',retry_domain='RUNTIME_RECOVERY')
        self.r.suspend_writer_authority(self.b.execution_id,reason='HERMES_FAULT')
        kwargs=dict(candidate_disposition='PRESERVE_FOR_REVERIFICATION',candidate_digest='d'*64,confirmation='AUTHORIZE_NEW_ATTEMPT_AFTER_RECONCILIATION')
        with self.assertRaisesRegex(RepositoryError,'QUIESCENCE'):self.r.authorize_runtime_recovery(self.b.execution_id,self.b.attempt_id,**kwargs)
        self.r.record_runtime_recovery(self.b.execution_id,self.b.attempt_id,failure_code='FAULT',evidence={'binding':asdict(self.b),'writer_quiescence':{'quiesced':True},'candidate_digest':'d'*64})
        with self.assertRaisesRegex(RepositoryError,'COMMAND_RECONCILIATION'):self.r.authorize_runtime_recovery(self.b.execution_id,self.b.attempt_id,**kwargs)
        self.r.mark_command_aborted(self.b.command_id,reason='old epoch fenced; no resend')
        self.assertEqual('ACTIVE',self.r.authorize_runtime_recovery(self.b.execution_id,self.b.attempt_id,**kwargs)['state'])
        self.assertEqual('INTERRUPTED',self.r.attempt_outcome(self.b.attempt_id)['status'])

class SupervisionTests(unittest.TestCase):
    def setUp(self):
        DurableTests.setUp(self)
        from hermes_supervision import HermesSupervision
        from pathlib import Path
        self.root=Path(self.f.f.temp.name)/'boundary';self.root.mkdir()
        candidate=Path(self.f.f.temp.name)/'candidate';candidate.mkdir();(candidate/'file').write_text('a')
        self.f.boundary.root=self.root;self.f.boundary.candidate=candidate
        self.f.boundary.server_identity={'pid':1234}
        self.f.boundary.process.members.return_value=[]
        self.clock=Clock();self.monitor=HermesSupervision(self.f.a,clock=self.clock)
        self.monitor.containers=lambda:[{'container_id':'owned','running':True}]
    def test_status_poll_and_heartbeat_are_not_progress(self):
        for _ in range(20):self.clock.add(10);self.monitor.tick(self.f.status)
        self.assertEqual(0,self.monitor.signals.last_progress)
        self.assertGreater(self.monitor.signals.last_heartbeat,0)
    def test_verified_candidate_change_progress(self):
        self.clock.add(10);(self.f.boundary.candidate/'file').write_text('b');self.monitor.tick(self.f.status)
        self.assertEqual(10,self.monitor.signals.last_progress)
        self.clock.add(10);self.monitor.tick(self.f.status);self.assertEqual(10,self.monitor.signals.last_progress)
    def test_mtime_only_not_progress(self):
        self.clock.add(10);(self.f.boundary.candidate/'file').touch();self.monitor.tick(self.f.status)
        self.assertEqual(0,self.monitor.signals.last_progress)
    def test_duplicate_tool_receipt_not_progress(self):
        from hermes_boundary import atomic
        atomic(self.root/'exec-completed-1.json',{'binding':asdict(self.b),'exit_code':0})
        self.clock.add(10);self.monitor.tick(self.f.status)
        self.clock.add(10);self.monitor.tick(self.f.status);self.assertEqual(10,self.monitor.signals.last_progress)
    def test_foreign_tool_receipt_denied(self):
        from hermes_boundary import atomic
        atomic(self.root/'exec-completed-1.json',{'binding':{},'exit_code':0})
        with self.assertRaisesRegex(HermesError,'FOREIGN_TOOL'):self.monitor.tick(self.f.status)
    def test_terminal_without_sse_is_reconciliation_only(self):
        self.monitor.tick({**self.f.status,'status':'completed'})
        self.assertEqual('RUNNING',self.r.attempt_outcome(self.b.attempt_id)['status'])
        self.assertEqual('STATUS_RECONCILIATION',json.loads(self.monitor.path.read_text())['event'])
    def test_restart_never_revives_same_binding(self):
        from hermes_supervision import HermesSupervision
        with self.assertRaisesRegex(HermesError,'RESTART_RECONCILIATION'):HermesSupervision(self.f.a,clock=self.clock)
    def test_old_native_run_cannot_renew(self):
        with self.assertRaisesRegex(HermesError,'FOREIGN_RUN'):self.monitor.tick({**self.f.status,'run_id':'old'})
    def test_owner_unavailable_denied(self):
        self.f.boundary.assert_server.side_effect=HermesError('SERVER_INCARNATION_LOST')
        with self.assertRaisesRegex(HermesError,'SERVER_INCARNATION'):self.monitor.tick(self.f.status)

    def test_revoked_writer_authority_cannot_renew(self):
        self.r.suspend_writer_authority(self.b.execution_id,reason='fixture revoke')
        with self.assertRaisesRegex(HermesError,'AUTHORITY_REVOKED'):self.monitor.tick(self.f.status)
        self.assertEqual(45,self.monitor.signals.lease_until)
    def test_stale_container_receipt(self):
        from hermes_boundary import atomic
        from hermes_supervision import HermesSupervision
        atomic(self.root/'inspect-fake.json',{'binding':{},'container_id':'fake'})
        with self.assertRaisesRegex(HermesError,'STALE_CONTAINER'):HermesSupervision.containers(self.monitor)
    def test_container_observation_timeout_is_unknown(self):
        from hermes_boundary import atomic
        from hermes_supervision import HermesSupervision
        from unittest.mock import patch
        import subprocess
        atomic(self.root/'inspect-fake.json',{'binding':asdict(self.b),'container_id':'fake'})
        with patch('hermes_supervision.subprocess.run',side_effect=subprocess.TimeoutExpired(['docker','inspect'],10)):
            with self.assertRaisesRegex(HermesError,'OBSERVATION_UNKNOWN'):HermesSupervision.containers(self.monitor)

    def test_worker_closes_process_ledger_after_quiescence(self):
        from hermes_worker import HermesWorker
        from hermes_boundary import atomic
        from worker import WorkerResult
        from unittest.mock import Mock
        atomic(self.root/'quiescence.json',{'state':'QUIESCED'})
        worker=HermesWorker(self.f.a);worker._execute=Mock(return_value=WorkerResult(True,0,'','',[]))
        self.assertTrue(worker._bounded_execute('fixture','unused',None).success)
        self.assertIsNone(self.r.active_runtime_process(self.b.attempt_id))
        self.assertEqual('RUNNING',self.r.attempt_outcome(self.b.attempt_id)['status'])
    def test_worker_failure_closes_process_without_semantic_retry(self):
        from hermes_worker import HermesWorker
        from hermes_boundary import atomic
        from unittest.mock import Mock
        from types import SimpleNamespace
        atomic(self.root/'quiescence.json',{'state':'QUIESCED'})
        worker=HermesWorker(self.f.a);worker._execute=Mock(side_effect=HermesError('TRANSPORT_UNKNOWN'))
        task=SimpleNamespace(retry_domain='')
        result=worker._bounded_execute('fixture','unused',task)
        self.assertFalse(result.success);self.assertEqual('RUNTIME_RECOVERY',task.retry_domain)
        self.assertIsNone(self.r.active_runtime_process(self.b.attempt_id))

class ProgressUXTests(unittest.TestCase):
    def test_repeated_heartbeat_silent_but_progress_delivered(self):
        from manager import Manager
        from task_state import TaskState
        from progress_projection import project_execution_health
        from datetime import datetime,timezone,timedelta
        from unittest.mock import Mock
        import threading
        manager=Manager.__new__(Manager);manager._state_lock=threading.RLock();manager.task_root='unused-disposable'
        manager._notify_progress=Mock(return_value={'delivered':True})
        now=datetime.now(timezone.utc)
        task=TaskState(task_id='ux',requirement='fixture',status='RUNNING',stage='WORKING')
        task.save=Mock()
        task.execution_runtime={'execution_id':'e','execution_role':'WORKER','execution_owner':'owned',
          'last_runtime_heartbeat_at':now.isoformat(),'lease_expires_at':(now+timedelta(seconds=45)).isoformat(),
          'last_progress_at':now.isoformat(),'progress_timeout_seconds':900,'process_alive':True}
        task.progress_snapshot={'execution_health':project_execution_health(task)}
        task.notification_evidence=[{'attempted_at':(now-timedelta(hours=1)).isoformat()}]
        callback=manager._runtime_event_callback(task)
        event={**task.execution_runtime,'runtime':'hermes','event':'HEARTBEAT','observed_at':now.isoformat()}
        callback(event);callback(event)
        manager._notify_progress.assert_not_called()
        callback({**event,'event':'PROGRESS'})
        manager._notify_progress.assert_called_once()

if __name__=='__main__':unittest.main()

