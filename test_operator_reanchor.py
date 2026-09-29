"""Deterministic operator re-anchor tests; all Product repositories are disposable."""
import json
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import harness_temp
from control_repository import ControlRepository, RepositoryError
from cumulative_policy import CumulativePolicyManager
from harness_service import HarnessService
from job_queue import JobStore
from operator_reanchor import reanchor, current_delta
from project_profile import load_project_profile
from test_project_profile import ProfileFixture


class OperatorReanchorTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fx = ProfileFixture(self.root)
        self.fx.data['control_policy'] = dict(commit_policy='BATCH_FINAL_COMMIT', cumulative_worktree=True,
            auto_continue_without_commit=True, pre_job_snapshot_supported=True, scoped_rollback_supported=True,
            final_commit_required=True, task_owned_baseline_files=[], external_frozen_baseline_files=[])
        self.fx.write()
        self.profile = load_project_profile(self.fx.profile, self.fx.workspace)
        for name in ('server', 'web'):
            repo = self.fx.workspace / name
            self.git(repo, 'init', '-q')
            self.git(repo, 'config', 'user.email', 'fixture@example.invalid')
            self.git(repo, 'config', 'user.name', 'Fixture')
            self.git(repo, 'config', 'core.autocrlf', 'false')
            (repo / 'a.txt').write_bytes(b'committed\n')
            self.git(repo, 'add', '.')
            self.git(repo, 'commit', '-qm', 'fixture')
        (self.fx.workspace / 'server/a.txt').write_bytes(b'preexisting\r\n')
        (self.fx.workspace / 'web/new.txt').write_bytes(b'untracked\n')
        self.control = self.root / '.control'
        self.repository = ControlRepository(self.control / 'control.sqlite3', create=True)
        q = JobStore(self.root / 'seed')._empty_queue()
        q.update(generation=14, revision=803, active_baseline_id='BASELINE-OLD')
        self.repository.bootstrap(q, {}, {})
        (self.control / 'sqlite-authority.json').write_text(json.dumps({'authority':'SQLITE','schema_version':5}))
        self.harness = HarnessService(self.fx.workspace, self.fx.profile, task_root=self.root / '.tasks')
        self.manager = CumulativePolicyManager(self.fx.workspace, self.control, self.profile)
        self.job = {'job_id':'JOB-20260905-120000-12345678','client_job_id':'old','status':'FAILED_FINAL',
                    'revision':2,'generation':13,'request':{},'last_result':{'success':False},'owner_id':''}
        self.repository.write_job(self.job, expected_revision=0)
        with self.repository.hold():
            self.repository._connection.execute('INSERT INTO ownership VALUES(?,?,?,?,?)',
                ('server/a.txt',self.job['job_id'],'a'*64,'PRE_EXISTING_UNVERIFIED_CANDIDATE','old-evidence'))
        self.before = self.manager.workspace_inventory()

    @staticmethod
    def git(root, *args):
        subprocess.run(['git','-C',str(root),*args],check=True,capture_output=True)

    def invoke(self, **changes):
        args=dict(expected_revision=803,request_id='test-reanchor',accept_current_workspace=True,
                  confirmation='REANCHOR SAMPLE',pid_probe=lambda pid:False,writer_probe=lambda workspace:[])
        args.update(changes)
        return reanchor(self.harness,**args)

    def test_acknowledgement_required(self):
        for args in ({'accept_current_workspace':False},{'confirmation':'YES'}):
            with self.assertRaisesRegex(RepositoryError,'ACKNOWLEDGEMENT_REQUIRED'):
                self.invoke(**args)
        self.assertEqual(803,self.repository.read_queue()['revision'])

    def test_active_writer_lease_rejected(self):
        with patch.object(self.harness,'acquire_execution_lease',return_value=False):
            with self.assertRaisesRegex(RepositoryError,'ACTIVE_WRITER'):
                self.invoke()

    def test_local_product_runtime_rejected(self):
        with self.assertRaisesRegex(RepositoryError,'LOCAL_PRODUCT_PROCESS_ACTIVE'):
            self.invoke(writer_probe=lambda workspace:[{'pid':123,'name':'java.exe'}])

    def execution(self, phase):
        with self.repository.hold():
            self.repository._connection.execute('INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('EX1',self.job['job_id'],'old',1,phase,'p','p','h','b','s','now','{}',None))

    def test_active_execution_rejected(self):
        self.execution('RUNNING')
        with self.assertRaisesRegex(RepositoryError,'ACTIVE_EXECUTION'):
            self.invoke()

    def test_live_registered_runtime_rejected(self):
        self.execution('MATERIALIZED')
        with self.repository.hold():
            c=self.repository._connection
            c.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)',('A1','EX1','WORKER','m','p','p','s','c','now','{}'))
            c.execute('INSERT INTO runtime_processes VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                ('P1','A1',None,'opencode',123,'known','RUNNING','now','',None,'{}'))
            c.execute("UPDATE executions SET phase='TERMINAL' WHERE execution_id='EX1'")
        with self.assertRaisesRegex(RepositoryError,'LIVE_RUNTIME'):
            self.invoke(pid_probe=lambda pid:True)

    def test_existing_dirty_workspace_adopted(self):
        result=self.invoke();q=self.repository.read_queue()
        self.assertEqual(15,q['generation']);self.assertEqual(804,q['revision'])
        self.assertEqual(2,result['receipt']['accepted_dirty_path_count'])
        status=self.manager.public_status(q['active_baseline_id'])
        self.assertTrue(status['external_frozen_integrity'])
        self.assertEqual([],status['unexpected_runtime_dirty_files'])

    def test_old_ownership_archived_and_history_preserved(self):
        result=self.invoke();self.assertEqual(self.job,self.repository.read_job(self.job['job_id']))
        self.assertEqual([],self.repository.candidate_ownership(self.fx.workspace)['files'])
        self.assertEqual(1,len(result['receipt']['archived_ownership']))
        with self.repository.hold():
            payload=self.repository._connection.execute('SELECT payload FROM control_state WHERE key=?',
                (result['receipt']['receipt_key'],)).fetchone()[0]
        self.assertEqual(self.job['job_id'],json.loads(payload)['archived_ownership'][0]['job_id'])

    def test_product_bytes_and_git_state_unchanged(self):
        self.invoke()
        self.assertEqual(self.before,self.manager.workspace_inventory())

    def test_atomic_rollback_after_ownership_retirement(self):
        # Failure at the last CAS proves receipt insertion and custody deletion roll back too.
        with patch.object(ControlRepository,'write_queue',side_effect=RuntimeError('crash-before-commit')):
            with self.assertRaisesRegex(RuntimeError,'crash-before-commit'):
                self.invoke()
        self.assertEqual(803,self.repository.read_queue()['revision'])
        self.assertEqual(1,len(self.repository.candidate_ownership(self.fx.workspace)['files']))
        with self.repository.hold():
            self.assertEqual(0,self.repository._connection.execute("SELECT count(*) FROM control_state WHERE key LIKE 'operator_reanchor:%'").fetchone()[0])
        self.assertEqual(self.before,self.manager.workspace_inventory())

    def test_zero_post_reanchor_delta(self):
        result=self.invoke()
        self.assertEqual({'changed_files':[],'repository_state_equal':True},result['product_delta'])

    def test_subsequent_modification_is_new_delta(self):
        result=self.invoke()
        (self.fx.workspace/'server/a.txt').write_bytes(b'future work\n')
        self.assertEqual(['server/a.txt'],current_delta(self.manager,result['receipt']['baseline_id'])['changed_files'])

    def test_new_untracked_file_is_new_delta(self):
        result=self.invoke()
        (self.fx.workspace/'web/future.txt').write_bytes(b'future work\n')
        self.assertEqual(['web/future.txt'],current_delta(self.manager,result['receipt']['baseline_id'])['changed_files'])

    def test_normal_git_delta_excludes_preexisting_dirty_bytes(self):
        from git_collector import GitCollector, GitRepoBaseline, GitTaskBaseline
        result=self.invoke();manifest=self.manager.load_batch_manifest(result['receipt']['baseline_id'])
        baseline=GitTaskBaseline(tuple(GitRepoBaseline(Path(r['repo']),r['module'],r['head'],r['worktree_tree'],r['index_tree']) for r in manifest['repos']))
        proof=GitCollector(str(self.fx.workspace),profile=self.profile).prove_no_task_delta(baseline)
        self.assertTrue(proof['passed'],proof)

    def test_expected_revision_conflict_no_change(self):
        with self.assertRaisesRegex(RepositoryError,'CONTROL_REVISION_CONFLICT'):
            self.invoke(expected_revision=802)
        self.assertEqual(803,self.repository.read_queue()['revision'])
        self.assertEqual(1,len(self.repository.candidate_ownership(self.fx.workspace)['files']))

    def test_normal_check_control_passes(self):
        self.invoke()
        import chatgpt_control_mcp
        args=SimpleNamespace(working_dir=str(self.fx.workspace),profile_dir=str(self.fx.profile))
        with patch.object(chatgpt_control_mcp,'HARNESS_ROOT',self.root):
            result=chatgpt_control_mcp._check_control(args)
        self.assertTrue(result['external_frozen_integrity'])
        self.assertTrue(result['candidate_preservation_integrity'])

    def test_retry_is_idempotent(self):
        a=self.invoke();b=self.invoke()
        self.assertTrue(b['replayed']);self.assertEqual(a['receipt'],b['receipt'])
        self.assertEqual(15,self.repository.read_queue()['generation'])

    def test_clean_workspace_supported(self):
        # Fixture writes only; never runs on the Product repositories.
        (self.fx.workspace/'server/a.txt').write_bytes(b'committed\n')
        (self.fx.workspace/'web/new.txt').unlink()
        result=self.invoke();self.assertEqual(0,result['receipt']['accepted_dirty_path_count'])

    def test_clean_reanchor_does_not_reapply_old_profile_frozen_hashes(self):
        (self.fx.workspace/'server/a.txt').write_bytes(b'committed\n')
        (self.fx.workspace/'web/new.txt').unlink()
        self.fx.data['control_policy']['external_frozen_baseline_files']=[{
            'path':'server/a.txt','sha256':'a'*64,'evidence':'obsolete profile declaration'}]
        self.fx.write()
        result=self.invoke()
        self.assertEqual([],result['product_delta']['changed_files'])

    def test_deleted_tracked_file_supported(self):
        (self.fx.workspace/'server/a.txt').unlink()
        result=self.invoke()
        self.assertEqual([],result['product_delta']['changed_files'])

    def test_unresolved_publication_rejected(self):
        root=self.control/'candidates/publications';root.mkdir(parents=True)
        (root/'PUB-test.json').write_text(json.dumps({'state':'APPLYING'}))
        with self.assertRaisesRegex(RepositoryError,'PUBLICATION_UNRESOLVED'):
            self.invoke()


if __name__=='__main__':
    unittest.main()
