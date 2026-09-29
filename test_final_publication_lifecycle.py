"""Real journalled publication after the amended preserved-candidate lifecycle."""
from dataclasses import replace
import shutil
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_final_blocker_lifecycle as lifecycle
from harness_service import HarnessService
from hermes_boundary import atomic, read
from isolated_promotion import IsolatedCanonicalPromotion, module_heads
from production_isolation import ExecutionCustody, inventory
from project_profile import ControlPolicyProfile
from qa_quarantine import CandidateError, QACandidateStore
from test_checkpoint import accepted


class FinalPublicationLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.flow = lifecycle.FinalBlockerLifecycleTests()
        self.flow.setUp()
        self.addCleanup(self.flow.doCleanups)
        self.fx = self.flow.fixture
        self.task = self.flow.task
        self.source = self.fx.root/'source'
        self.canonical = self.fx.root/'canonical'
        app = self.fx.workspace/'app'
        original_candidate = (app/'a.py').read_bytes()
        (app/'a.py').write_bytes(b'value = 0\n')
        self.git(app, 'init', '-q')
        self.git(app, 'config', 'core.autocrlf', 'false')
        self.git(app, 'add', '.')
        self.git(app, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                 '-c', 'commit.gpgsign=false', 'commit', '-qm', 'base')
        shutil.copytree(self.fx.workspace, self.source)
        shutil.copytree(self.fx.workspace, self.canonical)
        (app/'a.py').write_bytes(original_candidate)
        # Real integration build uses a synchronous, read-only Python process.
        module = replace(self.fx.profile.modules[0], build_cwd='app',
            build_argv=(sys.executable, '-B', '-c',
                "from pathlib import Path; assert Path('a.py').read_text() == 'value = 1\\n'"))
        self.profile = replace(self.fx.profile, modules=(module,),
            control_policy=ControlPolicyProfile(commit_policy='BATCH_FINAL_COMMIT'))
        self.service = SimpleNamespace(working_dir=self.fx.workspace,
            task_root=self.flow.root, snapshot_root=self.fx.root/'snapshots',
            _load_profile=lambda: (self.profile, None))
        self.job = {**self.fx.repo.read_job(self.task.job_id),
                    'materialized_execution': self.fx.execution, 'execution_context': {}}
        self.promoter = IsolatedCanonicalPromotion(self.canonical, self.fx.root/'publication', self.fx.repo)
        preserve = ['app/a.py', 'app/test_a.py']
        atomic(self.promoter.root/(self.task.job_id+'-base.json'), dict(
            job_id=self.task.job_id, candidate=str(self.fx.workspace), source=str(self.source),
            base=inventory(self.source, profile=self.profile, preserve=preserve),
            source_heads=module_heads(self.source, self.profile)))
        self.promoter.register_preserved(self.job, self.service, self.source)
        record = {**read(self.fx.root/'allocation.json'), 'source': str(self.source),
            'profile_origin': str(self.profile.profile_dir),
            'profile_origin_inventory': inventory(self.profile.profile_dir, include_git=True)}
        atomic(self.fx.root/'allocation.json', record)
        self.task.candidate_manifest = str(self.fx.root/'allocation.json')
        cleanup = {**read(self.fx.root/'cleanup.json'), 'boundary': 'docker-opencode/1',
            'execution_id': self.task.execution_id, 'job_id': self.task.job_id,
            'candidate_id': self.task.candidate_id, 'attempt_id': self.task.attempt_id,
            'execution_epoch': self.task.execution_epoch}
        atomic(self.fx.root/'cleanup.json', cleanup)
        self.custody = ExecutionCustody(SimpleNamespace(), self.fx.root, record, self.profile)
        self.service.isolated_execution = self.custody
        self.task.save(self.flow.root)

    @staticmethod
    def git(repo, *args):
        return subprocess.run(['git', '-C', str(repo), *args], capture_output=True,
                              check=True).stdout.decode().strip()

    def verified(self):
        outcome = self.flow.run_manager()
        self.assertTrue(outcome.success, self.task.failure_reason)
        self.assertIsNotNone(accepted(self.task, self.fx.workspace))
        # HarnessService persists the finished Manager projection before its
        # canonical promoter reloads it.
        self.task.save(self.flow.root)
        result = HarnessService._safe_outcome(self.task, outcome, self.flow.root/'task.json', None)
        self.assertTrue(result['success'], result)
        return result

    def publish(self, result):
        # Only runtime snapshot retrieval is supplied by this programmatic
        # fixture; all custody, gates, integration build and publication run.
        with patch('runtime_snapshot.load_frozen_runtime_snapshot',
                   return_value=SimpleNamespace(profile=self.profile)):
            return self.promoter(self.job, self.service, result)

    def test_amend_checkpoint_successor_verify_real_publish_and_quiescence(self):
        initial_head = module_heads(self.canonical, self.profile)
        result = self.verified()
        proof = self.task.test_checkpoint.copy()
        successor = self.task.reviewer_execution.copy()
        promoted = self.publish(result)
        self.assertEqual('PROMOTED', promoted['canonical_promotion']['status'])
        self.assertEqual('BATCH_COMMIT_PENDING', promoted['worktree_disposition'])
        self.assertEqual((self.fx.workspace/'app/a.py').read_bytes(),
                         (self.canonical/'app/a.py').read_bytes())
        self.assertEqual(inventory(self.fx.workspace, profile=self.profile),
                         inventory(self.canonical, profile=self.profile))
        self.assertEqual(initial_head, module_heads(self.canonical, self.profile))
        self.assertEqual(1, self.flow.tester_calls)
        self.assertEqual(proof, self.task.test_checkpoint)
        self.assertEqual(successor, self.task.reviewer_execution)
        store = QACandidateStore(self.promoter.root/'candidates', self.canonical, self.profile)
        self.assertEqual([], store.unresolved_publications())
        self.assertEqual('PROMOTED', store.load(promoted['candidate_id'])['state'])
        self.custody.close()
        self.assertEqual('CLOSED', read(self.fx.root/'allocation.json')['state'])
        self.assertTrue(read(self.fx.root/'cleanup.json')['quiesced'])
        self.assertEqual('PUBLISHED', read(self.promoter.root/(self.task.execution_id+'-intent.json'))['state'])

    def test_before_verify_cannot_publish(self):
        before = inventory(self.canonical, include_git=True)
        with patch('reviewer_successor.prepare', side_effect=RuntimeError('stop-before-review')):
            with self.assertRaisesRegex(RuntimeError, 'stop-before-review'):
                self.flow.run_manager()
        self.assertEqual('PASS', accepted(self.task, self.fx.workspace)['disposition'])
        self.assertEqual(1, self.flow.tester_calls)
        result = dict(task_id=self.task.task_id, success=False, build_status='PASS',
            test_status=self.task.test_status, review_status='', verification_status='PENDING')
        with self.assertRaisesRegex(CandidateError, 'CANONICAL_MACHINE_GATES_REQUIRED'):
            self.publish(result)
        self.assertEqual(before, inventory(self.canonical, include_git=True))
        self.assertFalse(list(self.promoter.root.glob('*-intent.json')))

    def test_stale_candidate_after_verify_cannot_publish(self):
        result = self.verified()
        (self.fx.workspace/'app/a.py').write_text('value = 99\n')
        with self.assertRaisesRegex(CandidateError, 'CANDIDATE_CUSTODY_PROOF_INVALID'):
            self.publish(result)
        self.assertEqual(b'value = 0\n', (self.canonical/'app/a.py').read_bytes())

    def test_unquiesced_candidate_cannot_publish(self):
        result = self.verified()
        atomic(self.fx.root/'cleanup.json', {**read(self.fx.root/'cleanup.json'), 'quiesced': False})
        with self.assertRaisesRegex(CandidateError, 'CANDIDATE_CUSTODY_PROOF_INVALID'):
            self.publish(result)
        self.assertEqual(b'value = 0\n', (self.canonical/'app/a.py').read_bytes())

    def test_existing_human_hold_alias_reuses_accepted_successor_review(self):
        import copy
        from control_repository import RepositoryError
        from reviewer_successor import verify_accepted_review
        from task_state import TaskState
        result = self.verified()
        original_candidate = self.task.candidate_id
        build_proof = copy.deepcopy(self.task.gate_evidence['BUILD'])
        role_proof = copy.deepcopy(self.task.reviewer_execution)
        # Exercise the established quarantine path, including the host-owned
        # held_verification receipt and actual quarantine bundle.
        held_result = self.promoter.quarantine(self.job, self.service, result)
        self.assertNotEqual(original_candidate, held_result['candidate_id'])
        job = self.fx.repo.read_job(self.task.job_id)
        revision = job['revision']
        job.update(revision=revision+1, candidate_id=held_result['candidate_id'],
                   candidate_manifest=held_result['candidate_manifest'],
                   status='AWAITING_QA', last_result=held_result)
        self.fx.repo.write_job(job, expected_revision=revision)
        held_job = {**self.job, **job}
        restored, allocation = self.promoter._task(held_job, self.service, result)
        self.assertEqual(original_candidate, restored.candidate_id)
        self.assertEqual(held_result['candidate_id'], job['candidate_id'])
        self.assertEqual(original_candidate, allocation['held_verification']['runtime_candidate_id'])
        self.assertEqual(self.task.reviewer_execution, restored.reviewer_execution)
        self.assertEqual(b'value = 0\n', (self.canonical/'app/a.py').read_bytes())
        # The verified host alias must not invalidate the unchanged durable
        # TEST checkpoint/Reviewer verdict during approved human-hold resume.
        for invalid_alias in ('', 'unrelated-quarantine-candidate'):
            with self.subTest(alias=invalid_alias), self.assertRaisesRegex(
                    RepositoryError, 'TEST_CHECKPOINT_CONTRACT_STALE'):
                verify_accepted_review(self.fx.repo, restored, candidate_alias=invalid_alias)
        verify_accepted_review(self.fx.repo, restored,
                               candidate_alias=held_result['candidate_id'])
        self.job = held_job
        promoted = self.publish(result)
        self.assertEqual('PROMOTED', promoted['canonical_promotion']['status'])
        self.assertEqual(held_result['candidate_id'], promoted['candidate_id'])
        self.assertEqual((self.fx.workspace/'app/a.py').read_bytes(),
                         (self.canonical/'app/a.py').read_bytes())
        final_task = TaskState.load(self.task.task_id, self.flow.root)
        self.assertEqual(build_proof, final_task.gate_evidence['BUILD'])
        self.assertEqual(role_proof, final_task.reviewer_execution)
        self.assertEqual(self.fx.execution, self.fx.repo.execution(self.task.execution_id))
        self.assertEqual(1, self.flow.tester_calls)
        store = QACandidateStore(self.promoter.root/'candidates', self.canonical, self.profile)
        self.assertEqual([], store.unresolved_publications())
        self.custody.close()
        self.assertEqual('CLOSED', read(self.fx.root/'allocation.json')['state'])
        self.assertTrue(read(self.fx.root/'cleanup.json')['quiesced'])

    def test_disjoint_and_overlapping_canonical_drift_hold_publication(self):
        result = self.verified()
        for relative in ('app/disjoint.py', 'app/a.py'):
            with self.subTest(relative=relative):
                path = self.canonical/relative
                prior = path.read_bytes() if path.exists() else None
                path.write_bytes(b'foreign canonical bytes')
                held = self.publish(result)
                self.assertEqual('CANDIDATE_FOCUSED_REBASE_REQUIRED', held['failure_code'])
                self.assertEqual(b'foreign canonical bytes', path.read_bytes())
                if prior is None:
                    path.unlink()
                else:
                    path.write_bytes(prior)
                # _hold persists a quarantine alias; each attempted publication
                # starts from the same verified runtime task fixture.
                self.task.save(self.flow.root)


if __name__ == '__main__':
    unittest.main()
