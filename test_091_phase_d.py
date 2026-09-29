"""Developer report provenance and notification regressions M through Q."""
from pathlib import Path
import copy
import json
import subprocess
import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
import harness_temp
from control_repository import ControlRepository
from operator_notifications import terminal_projection, deliver_semantic, render_compact
from developer_change_report import capture, save, freshness
from git_collector import GitCollector
from task_state import TaskState
from gate_evidence import identity
from test_plan import derive_test_plan
from test_job_control import request
from progress_projection import should_emit, queue_progress_snapshot


class PhaseDTests(unittest.TestCase):
    def setUp(self):
        self.temp=harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.repo=ControlRepository(self.root/'control.sqlite3',create=True)
        self.sent=[]

    def sender(self,message):
        self.sent.append(message)
        return {'delivered':True,'delivery_status':'SUCCESS'}

    def job(self):
        return {'job_id':'JOB-ROOT','client_job_id':'E4U-3-J03-R1-EMPLOYMENT-SETUP',
                'status':'AWAITING_QA','revision':14,'batch_id':'BATCH-X','sequence':1,
                'request':{'requirement':'very long requirement '*2000,'operator_label':'J03-R1 — 취업설정'},
                'last_result':{'failure_stage':'CONTROL','failure_code':'PROFILE_DRIFT',
                               'failure_origin':'HARNESS_CAUSED','worktree_disposition':'PRESERVED'}}

    def queue(self):
        return {'blocked_by_job_id':'JOB-ROOT','gate_reason':'AWAITING_QA','revision':488}

    def test_root_alert_suppresses_three_dependent_alerts_and_false_human_actions(self):
        root=self.job(); queue=self.queue()
        root_projection=terminal_projection(root,queue)
        self.assertTrue(root_projection['user_action_required'])
        deliver_semantic(root_projection,self.repo,self.sender)
        for index in range(3):
            child={'job_id':'CHILD-'+str(index),'status':'BLOCKED_BY_DEPENDENCY','blocked_by_job_id':'JOB-ROOT',
                   'root_failure_code':'PROFILE_DRIFT','last_result':{'failure_code':'BLOCKED_BY_DEPENDENCY'}}
            projected=terminal_projection(child,queue)
            self.assertFalse(projected['user_action_required'])
            self.assertTrue(deliver_semantic(projected,self.repo,self.sender)['dependent_suppressed'])
        self.assertEqual(1,len(self.sent))

    def test_revision_restart_reconciliation_dedup_and_distinct_failure_alert(self):
        job=self.job(); queue=self.queue()
        job['last_result']['failure_code']='CONTROL_EXECUTION_ERROR'
        deliver_semantic(terminal_projection(job,queue),self.repo,self.sender)
        job['revision']+=10; queue['revision']+=20
        restarted=ControlRepository(self.repo.path)
        self.assertTrue(deliver_semantic(terminal_projection(job,queue),restarted,self.sender)['duplicate_suppressed'])
        job['last_result']['failure_code']='PROFILE_DRIFT'
        deliver_semantic(terminal_projection(job,queue),restarted,self.sender)
        self.assertEqual(2,len(self.sent))

    def test_long_requirement_compact_output_and_unknown_batch_total(self):
        projection=terminal_projection(self.job(),self.queue())
        self.assertNotIn('very long requirement',projection['message'])
        self.assertLess(len(projection['message']),700)
        self.assertIn('Batch BATCH-X',projection['message'])
        self.assertNotIn('/?',projection['message'])
        snapshot={'job':{'id':'ROOT','client_job_id':'J03-R1','display_name':'very long requirement '*2000},
                  'stage':{'name':'REVIEW'},'batch':{'id':'BATCH-X','current':1,'total':0}}
        rendered=render_compact(snapshot)
        self.assertNotIn('very long requirement',rendered)
        self.assertIn('Batch BATCH-X',rendered)

    def test_failed_delivery_is_retried_and_notification_error_is_non_product(self):
        job=self.job(); before=copy.deepcopy(job)
        projection=terminal_projection(job,self.queue())
        failed=deliver_semantic(projection,self.repo,lambda message:{'delivered':False})
        self.assertEqual('NOTIFICATION',failed['failure_origin'])
        self.assertTrue(deliver_semantic(projection,self.repo,self.sender)['delivered'])
        self.assertEqual(before,job)
        with patch.object(self.repo,'notification',side_effect=OSError('injected')):
            self.assertEqual('NOTIFICATION',deliver_semantic(projection,self.repo,self.sender)['failure_origin'])

    def test_success_terminal_is_delivered_once_by_supervisor(self):
        from types import SimpleNamespace
        from queue_supervisor import QueueSupervisor
        supervisor = object.__new__(QueueSupervisor)
        supervisor.store = SimpleNamespace(repository=self.repo)
        job = self.job()
        job.update(status='SUCCEEDED', last_result={'stage':'DONE','worktree_disposition':'VERIFIED_CLEAN'})
        queue = {'gate_reason':'', 'blocked_by_job_id':''}
        with patch('queue_supervisor.send_notify_evidence', return_value={'delivered':True}) as sender:
            self.assertTrue(supervisor.notify_terminal_transition(job,queue)['delivered'])
            self.assertTrue(supervisor.notify_terminal_transition(job,queue)['duplicate_suppressed'])
            sender.assert_called_once()
            self.assertEqual('Harness completed',sender.call_args.kwargs['title'])

    def test_terminal_job_overrides_stale_running_task_and_never_heartbeats(self):
        job=self.job(); job['batch_id']='BATCH-X'
        task={'job':{'status':'RUNNING'},'progress_revision':1,'execution_health':{'runtime_health':'VALID'}}
        snapshot=queue_progress_snapshot(self.queue(),[job],task)
        self.assertEqual('AWAITING_QA',snapshot['job']['status'])
        now=datetime.now(timezone.utc)
        self.assertEqual((False,'SILENT'),should_emit(snapshot,snapshot,now=now,last_emitted_at=now-timedelta(hours=2)))

    def test_exact_report_sources_and_candidate_or_contract_change_stales_report(self):
        project=self.root/'project'; project.mkdir()
        def git(*args):
            return subprocess.run(['git','-C',str(project),*args],capture_output=True,check=True)
        git('init','-q')
        source=project/'A.py'; source.write_text('def number():\n    return 1\n',encoding='utf-8')
        git('add','A.py'); git('-c','user.name=Harness Test','-c','user.email=harness-test@example.invalid','commit','-qm','input')
        task=TaskState(task_id='TASK-REPORT',job_id='JOB-REPORT',requirement='return the new number',working_dir=str(project),
            task_mode='MODIFICATION',materialized_execution={'contract_revision':1,'execution_surface_hash':'surface'},
            test_plan={'production_scope':['A.py'],'scope_hash':'scope'},changed_files=['A.py'])
        collector=GitCollector(str(project))
        baseline,error=collector.capture_baseline(task)
        self.assertFalse(error)
        source.write_text('def number():\n    return 2\n',encoding='utf-8')
        collector.collect(task,baseline)
        capture(task,baseline,collector,project,self.root/'.tasks')
        saved=save(task,self.root/'.tasks')
        text=saved.read_text(encoding='utf-8')
        self.assertIn('    return 1',text); self.assertIn('    return 2',text)
        self.assertEqual('CURRENT',task.developer_change_report['freshness'])
        current=dict(task.developer_change_report['binding'],contract_revision=2)
        self.assertEqual('STALE',freshness(task.developer_change_report,current))
        source.write_text('def number():\n    return 3\n',encoding='utf-8')
        save(task,self.root/'.tasks')
        self.assertEqual('STALE',task.developer_change_report['freshness'])
        self.assertNotIn('    return 3',saved.read_text(encoding='utf-8'))


if __name__=='__main__':
    unittest.main()
