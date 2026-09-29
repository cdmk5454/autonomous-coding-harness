"""Disposable regressions for the real isolated-publication canary blockers."""

from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_final_blocker_lifecycle as lifecycle
import test_final_publication_lifecycle as publication
from chatgpt_control import ChatGPTControlService
from hermes_boundary import atomic, read
from isolated_promotion import content
from production_isolation import inventory
from qa_quarantine import CandidateError, QACandidateStore


class RuntimeCheckpointTests(unittest.TestCase):
    def test_d1_bound_open_handle_survives_review_without_clone(self):
        flow = lifecycle.FinalBlockerLifecycleTests()
        flow.setUp()
        self.addCleanup(flow.doCleanups)
        candidate_id = flow.task.candidate_id
        handle = (flow.fixture.root / 'runtime-handle').open('w+b')
        self.addCleanup(handle.close)

        class Custody:
            def __init__(self, file):
                self.file = file

            def repair(self, *_args):
                return None

            def __deepcopy__(self, _memo):
                raise AssertionError('runtime custody must not be cloned')

        custody = Custody(handle)
        flow.task._isolated_eol_repair = custody.repair
        result = flow.run_manager()
        self.assertTrue(result.success, flow.task.failure_reason)
        self.assertIs(flow.task._isolated_eol_repair.__self__, custody)
        self.assertFalse(handle.closed)
        self.assertEqual(candidate_id, flow.task.candidate_id)
        self.assertEqual('PASS', flow.task.test_checkpoint['disposition'])
        self.assertEqual('PASS', flow.task.test_status)
        self.assertEqual('REVIEWER', flow.task.reviewer_execution['role'])
        self.assertEqual(1, flow.tester_calls)


class PublicationHotfixTests(unittest.TestCase):
    def setUp(self):
        self.flow = publication.FinalPublicationLifecycleTests()
        self.flow.setUp()
        self.addCleanup(self.flow.doCleanups)

    def hold_one_candidate(self, result):
        flow = self.flow
        held = flow.promoter.quarantine(flow.job, flow.service, result)
        job = flow.fx.repo.read_job(flow.task.job_id)
        revision = job['revision']
        job.update(revision=revision + 1, candidate_id=held['candidate_id'],
                   candidate_manifest=held['candidate_manifest'], status='AWAITING_QA',
                   last_result=held)
        flow.fx.repo.write_job(job, expected_revision=revision)
        flow.job = {**flow.job, **job}
        return held['candidate_id']

    def test_d2_editor_drift_is_excluded_but_source_and_protected_drift_block(self):
        flow = self.flow
        result = flow.verified()
        editor = flow.canonical / '.idea/workspace.xml'
        editor.parent.mkdir()
        editor.write_text('editor changed after allocation')
        self.assertNotIn('.idea/workspace.xml', content(flow.canonical, flow.profile))
        protected_profile = replace(flow.profile, protected_paths=('.idea/workspace.xml',))
        self.assertIn('.idea/workspace.xml', inventory(flow.canonical, profile=protected_profile))
        flow.promoter.allocations.pop(flow.task.job_id)
        flow.promoter.register_preserved(flow.job, flow.service, flow.source)
        self.assertEqual('PROMOTED', flow.publish(result)['canonical_promotion']['status'])
        self.assertEqual('editor changed after allocation', editor.read_text())

        # Fresh disposable fixture for the negative boundaries, with an
        # unmodified canonical base before each attempt.
        other = publication.FinalPublicationLifecycleTests()
        other.setUp()
        self.addCleanup(other.doCleanups)
        negative_result = other.verified()
        for relative in ('app/disjoint.py', 'app/a.py', 'app/protected.py'):
            with self.subTest(path=relative):
                target = other.canonical / relative
                previous = target.read_bytes() if target.exists() else None
                target.write_bytes(b'foreign source drift')
                held = other.publish(negative_result)
                self.assertEqual('CANDIDATE_FOCUSED_REBASE_REQUIRED', held['failure_code'])
                self.assertEqual(b'foreign source drift', target.read_bytes())
                if previous is None:
                    target.unlink()
                else:
                    target.write_bytes(previous)
                other.task.save(other.flow.root)

    def test_d3_and_combined_clean_rollback_retries_same_candidate_once(self):
        flow = self.flow
        runtime_candidate = flow.task.candidate_id
        result = flow.verified()  # WORK fixture -> Build -> TEST checkpoint -> Reviewer PASS.
        reviewer = flow.task.reviewer_execution.copy()
        checkpoint = flow.task.test_checkpoint.copy()
        held_candidate = self.hold_one_candidate(result)
        editor = flow.canonical / '.idea/workspace.xml'
        editor.parent.mkdir()
        editor.write_text('ephemeral editor state')
        base = (flow.canonical / 'app/a.py').read_bytes()
        intent_path = flow.promoter.root / (flow.task.execution_id + '-intent.json')
        original_apply = QACandidateStore._apply_publication_entry

        def fail_after_apply(store, journal, entry):
            self.assertEqual('PUBLISHING', read(intent_path)['state'])
            original_apply(store, journal, entry)
            raise RuntimeError('injected apply failure')

        with patch.object(QACandidateStore, '_apply_publication_entry', fail_after_apply):
            with self.assertRaisesRegex(RuntimeError, 'injected apply failure'):
                flow.publish(result)
        self.assertEqual(base, (flow.canonical / 'app/a.py').read_bytes())
        self.assertEqual('ROLLED_BACK', read(intent_path)['state'])
        first_publication = read(intent_path)['publication_id']
        store = QACandidateStore(flow.promoter.root / 'candidates', flow.canonical, flow.profile)
        self.assertEqual('ROLLED_BACK', store.load_publication(first_publication)['disposition'])

        # The real canary left a pre-hotfix PUBLISHING intent without a journal
        # ID. Exact single-journal recovery must resolve it without replaying.
        atomic(intent_path, {'job_id': flow.task.job_id,
                             'execution_id': flow.task.execution_id, 'state': 'PUBLISHING'})
        flow.promoter.recover()
        self.assertEqual('ROLLED_BACK', read(intent_path)['state'])
        self.assertEqual(first_publication, read(intent_path)['publication_id'])

        promoted = flow.publish(result)
        self.assertEqual(held_candidate, promoted['candidate_id'])
        self.assertEqual('PUBLISHED', read(intent_path)['state'])
        self.assertEqual([first_publication], read(intent_path)['rolled_back_publications'])
        self.assertEqual('ephemeral editor state', editor.read_text())
        self.assertEqual(1, flow.flow.tester_calls)
        self.assertEqual(checkpoint, flow.task.test_checkpoint)
        self.assertEqual(reviewer, flow.task.reviewer_execution)
        self.assertEqual(runtime_candidate, flow.promoter.allocations[flow.task.job_id]['held_verification']['runtime_candidate_id'])
        journals = [store.load_publication(path.stem) for path in store._publications_dir().glob('PUB-*.json')]
        self.assertEqual(1, sum(j['disposition'] == 'PROMOTED' for j in journals))
        with self.assertRaisesRegex(CandidateError, 'CANONICAL_PUBLICATION_REPLAY_DENIED'):
            flow.publish(result)

    def test_d3_foreign_rollback_stays_fenced(self):
        flow = self.flow
        result = flow.verified()
        self.hold_one_candidate(result)
        original_apply = QACandidateStore._apply_publication_entry

        def foreign_after_apply(store, journal, entry):
            original_apply(store, journal, entry)
            (flow.canonical / entry['path']).write_bytes(b'foreign postimage')
            raise RuntimeError('injected foreign write')

        with patch.object(QACandidateStore, '_apply_publication_entry', foreign_after_apply):
            with self.assertRaisesRegex(CandidateError, 'CANONICAL_PUBLICATION_RECONCILIATION_REQUIRED'):
                flow.publish(result)
        intent = flow.promoter.root / (flow.task.execution_id + '-intent.json')
        self.assertEqual('PUBLISHING', read(intent)['state'])
        self.assertEqual(b'foreign postimage', (flow.canonical / 'app/a.py').read_bytes())
        with self.assertRaisesRegex(CandidateError, 'CANONICAL_PUBLICATION_REPLAY_DENIED'):
            flow.publish(result)
        with self.assertRaisesRegex(CandidateError, 'CANONICAL_PUBLICATION_RECOVERY_REQUIRED'):
            flow.promoter.recover()

    def test_d4_review_recovery_uses_materialized_frozen_context(self):
        flow = self.flow
        result = flow.verified()
        execution = dict(flow.fx.execution)
        frozen_context = dict(execution['execution_context'])
        mutable_context = {**frozen_context, 'profile_sha256': 'later-profile'}
        self.assertNotEqual(frozen_context, mutable_context)
        job = {**flow.job, 'status': 'AWAITING_QA', 'revision': 7,
               'execution_context': mutable_context,
               'candidate_manifest': str(flow.fx.root / 'allocation.json'),
               'last_result': result, 'review_only_actions': {}}
        captured = []

        def probe(bound):
            if bound['execution_context'] != frozen_context:
                raise CandidateError('PROFILE_DRIFT')
            captured.append(bound)
            raise RuntimeError('captured bound job')

        service = object.__new__(ChatGPTControlService)
        service._assert_writable = lambda: None
        service._assert_configuration_current = lambda: None
        service.store = SimpleNamespace(
            action_key=lambda value: value, get_job=lambda _id: job,
            repository=SimpleNamespace(execution=lambda _id: execution))
        service.harness = SimpleNamespace(task_root=flow.flow.root,
            acquire_execution_lease=lambda: True, release_execution_lease=lambda: None)
        service.supervisor = SimpleNamespace(is_running=lambda: False)
        service.isolation = SimpleNamespace(reopen_preserved=probe)
        with patch('test_checkpoint.can_resume_review', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'captured bound job'):
                service.retry_review_only(flow.task.job_id, expected_revision=7,
                    request_id='review-retry', confirmation='I_CONFIRM_RETRY_REVIEW')
        self.assertEqual(frozen_context, captured[0]['execution_context'])
        self.assertEqual(execution, captured[0]['materialized_execution'])

        execution['job_id'] = 'incompatible-job'
        with patch('test_checkpoint.can_resume_review', return_value=True):
            with self.assertRaisesRegex(Exception, 'REVIEW_ONLY_EXECUTION_MISSING'):
                service.retry_review_only(flow.task.job_id, expected_revision=7,
                    request_id='review-retry', confirmation='I_CONFIRM_RETRY_REVIEW')
        self.assertEqual(1, len(captured))


if __name__ == '__main__':
    unittest.main()
