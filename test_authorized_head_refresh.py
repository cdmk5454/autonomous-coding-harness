import copy
import hashlib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from authorized_head_refresh import refresh, CONFIRMATION
from control_repository import RepositoryError
from test_operator_reanchor import OperatorReanchorTests
J4='JOB-20260922-120000-12345678'
J5='JOB-20260922-120000-87654321'


class HeadRefreshTests(unittest.TestCase):
    def setUp(self):
        self.fx = OperatorReanchorTests()
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx.invoke()
        self.r = self.fx.repository
        self.q = self.r.read_queue()
        self.root = self.fx.fx.workspace
        self.old = self.fx.manager.workspace_inventory()['repos'][0]['head']
        self.candidate = self.fx.root/'candidate'
        self.candidate.mkdir()
        (self.candidate/'source.java').write_text('preserved')
        self.sha = hashlib.sha256(b'preserved').hexdigest()
        self.job = dict(schema_version=1,job_id=J4,status='AWAITING_QA',revision=15,candidate_id='C4',
            owner_id='',request={'target_resources':['server/j4.java']},last_result={'task_id':'T4'})
        self.r.write_job(self.job,expected_revision=0)
        q=self.r.read_queue();q['order'].append(J4);q['revision']+=1
        self.r.write_queue(q,expected_revision=self.q['revision'])
        self.q=self.r.read_queue()
        (self.root/'server/pulled.java').write_text('external')
        self.fx.git(self.root/'server','add','pulled.java')
        self.fx.git(self.root/'server','commit','-qm','pull fixture')
        self.new=self.fx.manager.workspace_inventory()['repos'][0]['head']
        self.args=dict(job_id=J4,candidate_id='C4',expected_revision=self.q['revision'],
            expected_job_revision=15,request_id='refresh',confirmation=CONFIRMATION,
            modules={'server':dict(old=self.old,new=self.new,changed_files=['server/pulled.java'])})
        t=SimpleNamespace(candidate_id='C4',working_dir=str(self.candidate),
            build_evidence={'details':{'source_sha256':{'source.java':self.sha}}})
        p=patch('authorized_head_refresh.TaskState.load',return_value=t)
        p.start();self.addCleanup(p.stop)

    def call(self,**changes):
        args=copy.deepcopy(self.args);args.update(changes)
        return refresh(self.fx.harness,**args)

    def test_exact_refresh_and_replay_preserve_baseline_candidate_and_other_heads(self):
        before=self.fx.manager.load_batch_manifest(self.q['active_baseline_id'])
        result=self.call();self.assertFalse(result['replayed'])
        self.assertTrue(self.call()['replayed'])
        q=self.r.read_queue();self.assertEqual(15,q['generation'])
        self.assertEqual(self.q['active_baseline_id'],q['active_baseline_id'])
        self.assertEqual(before,self.fx.manager.load_batch_manifest(q['active_baseline_id']))
        status=self.fx.harness.cumulative_policy_status(active_baseline_id=q['active_baseline_id'],read_only=True)
        self.assertTrue(status['external_frozen_integrity'])
        self.assertEqual(b'preserved',(self.candidate/'source.java').read_bytes())
        self.assertEqual(self.job,self.r.read_job(J4))
        import chatgpt_control_mcp
        args=SimpleNamespace(working_dir=str(self.root),profile_dir=str(self.fx.fx.profile))
        with patch.object(chatgpt_control_mcp,'HARNESS_ROOT',self.fx.root):
            checked=chatgpt_control_mcp._check_control(args)
        self.assertTrue(checked['external_frozen_integrity'])

    def test_confirmation(self):
        with self.assertRaisesRegex(RepositoryError,'CONFIRMATION'):self.call(confirmation='')

    def test_revision(self):
        with self.assertRaisesRegex(RepositoryError,'REVISION'):self.call(expected_revision=1)

    def test_wrong_old_head(self):
        args=copy.deepcopy(self.args['modules']);args['server']['old']='0'*40
        with self.assertRaisesRegex(RepositoryError,'HEAD_MISMATCH'):self.call(modules=args)

    def test_overlap(self):
        j=dict(self.job,job_id=J5,request={'target_resources':['server/pulled.java']})
        self.r.write_job(j,expected_revision=0)
        q=self.r.read_queue();rev=q['revision'];q['order'].append(J5);q['revision']+=1
        self.r.write_queue(q,expected_revision=rev)
        with self.assertRaisesRegex(RepositoryError,'OVERLAP'):self.call(expected_revision=rev+1)

    def test_dirty_adoption_denied(self):
        (self.root/'server/a.txt').write_text('not authorized')
        with self.assertRaisesRegex(RepositoryError,'DIRTY_BYTES'):self.call()

    def test_candidate_change_denied(self):
        (self.candidate/'source.java').write_text('changed')
        with self.assertRaisesRegex(RepositoryError,'CANDIDATE_CHANGED'):self.call()

    def test_wrong_paths(self):
        args=copy.deepcopy(self.args['modules']);args['server']['changed_files']=[]
        with self.assertRaisesRegex(RepositoryError,'PATH_INVENTORY'):self.call(modules=args)

    def test_non_fast_forward_denied(self):
        import subprocess
        real=subprocess.run
        def run(argv,**kwargs):
            if 'merge-base' in argv:
                raise subprocess.CalledProcessError(1,argv)
            return real(argv,**kwargs)
        with patch('authorized_head_refresh.subprocess.run',side_effect=run):
            with self.assertRaisesRegex(RepositoryError,'NOT_FAST_FORWARD'):self.call()
